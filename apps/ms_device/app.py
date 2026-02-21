import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import asyncpg
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "device-db"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "device_service"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password"),
}
DEPS = {}

class DeviceType(str, Enum):
    TEMPERATURE_SENSOR = "temperature_sensor"
    THERMOSTAT = "thermostat"
    LIGHT = "light"
    LOCK = "lock"
    CAMERA = "camera"
    HUMIDITY_SENSOR = "humidity_sensor"


class DeviceStatus(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    ERROR = "error"


class DeviceCreate(BaseModel):
    name: str
    device_type: DeviceType
    location: Optional[str] = None
    configuration: Optional[Dict[str, Any]] = {}
    metadata: Optional[Dict[str, Any]] = {}


class DeviceUpdate(BaseModel):
    name: Optional[str] = None
    location: Optional[str] = None
    configuration: Optional[Dict[str, Any]] = None
    status: Optional[DeviceStatus] = None
    metadata: Optional[Dict[str, Any]] = None


class DeviceCommand(BaseModel):
    command: str
    parameters: Optional[Dict[str, Any]] = None
    priority: str = "normal"

def get_db():
    return DEPS["db"]


async def init_db():
    conn = await asyncpg.connect(**DB_CONFIG)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            id VARCHAR(36) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            device_type VARCHAR(50) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'offline',
            location VARCHAR(255),
            configuration JSONB DEFAULT '{}',
            metadata JSONB DEFAULT '{}',
            last_seen TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_device_type ON devices(device_type);
        CREATE INDEX IF NOT EXISTS idx_device_status ON devices(status);
        CREATE INDEX IF NOT EXISTS idx_device_location ON devices(location);
    """)
    await conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    db_pool = await asyncpg.create_pool(**DB_CONFIG)
    DEPS["db"] = db_pool
    yield
    await db_pool.close()


app = FastAPI(
    title="Device Service",
    description="Сервис управления устройствами",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.get("/devices")
async def get_devices(
    pool=Depends(get_db),
    device_type: Optional[str] = None,
    location: Optional[str] = None,
    status: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
):
    query = "SELECT * FROM devices"
    conditions = []
    params = []
    idx = 1
    if device_type:
        conditions.append(f"device_type = ${idx}")
        params.append(device_type)
        idx += 1
    if location:
        conditions.append(f"location = ${idx}")
        params.append(location)
        idx += 1
    if status:
        conditions.append(f"status = ${idx}")
        params.append(status)
        idx += 1
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += f" ORDER BY created_at DESC LIMIT ${idx} OFFSET ${idx + 1}"
    params.extend([limit, skip])
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]


@app.get("/devices/{device_id}")
async def get_device(device_id: str, pool=Depends(get_db)):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM devices WHERE id = $1", device_id)
        if not row:
            raise HTTPException(status_code=404, detail="Device not found")
        return dict(row)


@app.post("/devices", status_code=201)
async def create_device(device: DeviceCreate, pool=Depends(get_db)):
    device_id = str(uuid.uuid4())
    now = datetime.utcnow()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO devices (id, name, device_type, status, location, configuration, metadata, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            device_id,
            device.name,
            device.device_type.value,
            "offline",
            device.location,
            json.dumps(device.configuration or {}),
            json.dumps(device.metadata or {}),
            now,
            now,
        )
    return {
        "id": device_id,
        "name": device.name,
        "device_type": device.device_type,
        "status": "offline",
        "location": device.location,
        "configuration": device.configuration or {},
        "metadata": device.metadata or {},
        "created_at": now,
        "updated_at": now,
        "last_seen": None,
    }


@app.patch("/devices/{device_id}")
async def update_device(device_id: str, update: DeviceUpdate, pool=Depends(get_db)):
    async with pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT * FROM devices WHERE id = $1", device_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Device not found")
        updates = []
        values = []
        idx = 1
        update_dict = update.model_dump(exclude_unset=True)
        for field, value in update_dict.items():
            if field in ["configuration", "metadata"] and value is not None:
                updates.append(f"{field} = ${idx}")
                values.append(json.dumps(value))
            elif value is not None:
                updates.append(f"{field} = ${idx}")
                values.append(value)
            idx += 1
        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")
        values.append(device_id)
        values.append(datetime.utcnow())
        query = f"""
            UPDATE devices SET {", ".join(updates)}, updated_at = ${idx}
            WHERE id = ${idx + 1} RETURNING *
        """
        updated = await conn.fetchrow(query, *values)
        return dict(updated)


@app.delete("/devices/{device_id}")
async def delete_device(device_id: str, pool=Depends(get_db)):
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM devices WHERE id = $1", device_id)
        if result.split()[-1] == "0":
            raise HTTPException(status_code=404, detail="Device not found")
    return {"message": "Device deleted successfully"}


@app.post("/devices/{device_id}/command")
async def send_command(device_id: str, command: DeviceCommand, pool=Depends(get_db)):
    async with pool.acquire() as conn:
        device = await conn.fetchrow("SELECT * FROM devices WHERE id = $1", device_id)
        if not device:
            raise HTTPException(status_code=404, detail="Device not found")
        await conn.execute(
            "UPDATE devices SET status = 'online', last_seen = $1 WHERE id = $2",
            datetime.utcnow(),
            device_id,
        )
    return {
        "command_id": str(uuid.uuid4()),
        "status": "accepted",
        "message": f"Command '{command.command}' sent to device {device_id}",
    }
