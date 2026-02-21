import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, List, Optional

import asyncpg
import httpx
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "device-manager-db"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "device_manager"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password"),
}
DEVICE_SERVICE_URL = os.getenv("DEVICE_SERVICE_URL", "http://device-service:8002")
DEPS = {}


class ScenarioAction(BaseModel):
    device_id: str
    command: str
    parameters: Optional[Dict] = None
    delay_seconds: int = 0


class ScenarioCreate(BaseModel):
    name: str
    description: Optional[str] = None
    trigger_type: str
    cron_schedule: Optional[str] = None
    actions: List[ScenarioAction]


def get_db():
    return DEPS["db"]


def get_client():
    return DEPS["client"]


async def send_device_command(
    device_id: str,
    command: str,
    parameters: Optional[Dict] = None,
):
    try:
        url = f"{DEVICE_SERVICE_URL}/devices/{device_id}/command"
        payload = {"command": command, "parameters": parameters or {}}
        http_client = get_client()
        response = await http_client.post(url, json=payload, timeout=10.0)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Failed to send command to device {device_id}: {e}")
        raise


async def execute_scenario_async(scenario_id: str, db_pool):
    async with db_pool.acquire() as conn:
        scenario = await conn.fetchrow("SELECT * FROM scenarios WHERE id = $1", scenario_id)
        if not scenario:
            logger.error(f"Scenario {scenario_id} not found")
            return
        execution_id = str(uuid.uuid4())
        await conn.execute(
            "INSERT INTO scenario_executions (id, scenario_id, status) VALUES ($1, $2, $3)",
            execution_id, scenario_id, "running",
        )
        await conn.execute("UPDATE scenarios SET last_executed = $1 WHERE id = $2", datetime.utcnow(), scenario_id)
        actions = scenario["actions"]
        if isinstance(actions, str):
            actions = json.loads(actions)
        for action in actions:
            try:
                await send_device_command(
                    action["device_id"],
                    action["command"],
                    action.get("parameters"),
                )
                action_id = str(uuid.uuid4())
                await conn.execute(
                    "INSERT INTO executed_actions (id, execution_id, device_id, command, status) VALUES ($1, $2, $3, $4, $5)",
                    action_id, execution_id, action["device_id"], action["command"], "success",
                )
            except Exception as e:
                logger.error(f"Action failed: {e}")
                action_id = str(uuid.uuid4())
                await conn.execute(
                    "INSERT INTO executed_actions (id, execution_id, device_id, command, status) VALUES ($1, $2, $3, $4, $5)",
                    action_id, execution_id, action["device_id"], action["command"], "failed",
                )
        await conn.execute(
            "UPDATE scenario_executions SET status = $1, completed_at = $2 WHERE id = $3",
            "completed", datetime.utcnow(), execution_id,
        )


async def init_database():
    conn = await asyncpg.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
    )
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS scenarios (
            id VARCHAR(36) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            description TEXT,
            trigger_type VARCHAR(50) NOT NULL,
            cron_schedule VARCHAR(100),
            actions JSONB NOT NULL,
            enabled BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_executed TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS scenario_executions (
            id VARCHAR(36) PRIMARY KEY,
            scenario_id VARCHAR(36) REFERENCES scenarios(id),
            status VARCHAR(20) NOT NULL,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            error_message TEXT
        );
        CREATE TABLE IF NOT EXISTS executed_actions (
            id VARCHAR(36) PRIMARY KEY,
            execution_id VARCHAR(36) REFERENCES scenario_executions(id),
            device_id VARCHAR(255) NOT NULL,
            command TEXT NOT NULL,
            status VARCHAR(20) NOT NULL,
            executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    await conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_database()
    db_pool = await asyncpg.create_pool(**DB_CONFIG)
    http_client = httpx.AsyncClient(timeout=30.0)
    DEPS["db"] = db_pool
    DEPS["client"] = http_client
    yield
    await db_pool.close()
    await http_client.aclose()


app = FastAPI(
    title="Device Manager Service",
    description="Управление сценариями и автоматизацией устройств",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "device-manager-service", "database": "connected"}


@app.get("/scenarios")
async def get_scenarios(db_pool=Depends(get_db), enabled: Optional[bool] = None):
    async with db_pool.acquire() as conn:
        query = "SELECT * FROM scenarios"
        params = []
        if enabled is not None:
            query += " WHERE enabled = $1"
            params.append(enabled)
        query += " ORDER BY created_at DESC"
        rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]


@app.get("/scenarios/{scenario_id}")
async def get_scenario(scenario_id: str, db_pool=Depends(get_db)):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM scenarios WHERE id = $1", scenario_id)
        if not row:
            raise HTTPException(status_code=404, detail="Scenario not found")
        return dict(row)


@app.post("/scenarios")
async def create_scenario(scenario: ScenarioCreate, db_pool=Depends(get_db)):
    scenario_id = str(uuid.uuid4())
    now = datetime.utcnow()
    actions_json = json.dumps([a.model_dump() for a in scenario.actions])
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO scenarios (id, name, description, trigger_type, cron_schedule, actions, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            scenario_id,
            scenario.name,
            scenario.description,
            scenario.trigger_type,
            scenario.cron_schedule,
            actions_json,
            now,
            now,
        )
    return {
        "id": scenario_id,
        "name": scenario.name,
        "description": scenario.description,
        "trigger_type": scenario.trigger_type,
        "actions": [a.model_dump() for a in scenario.actions],
        "created_at": now,
    }


@app.post("/scenarios/{scenario_id}/execute")
async def execute_scenario(scenario_id: str, db_pool=Depends(get_db)):
    async with db_pool.acquire() as conn:
        scenario = await conn.fetchrow("SELECT * FROM scenarios WHERE id = $1 AND enabled = TRUE", scenario_id)
        if not scenario:
            raise HTTPException(status_code=404, detail="Scenario not found or disabled")
    await execute_scenario_async(scenario_id, db_pool)
    return {
        "status": "accepted",
        "message": "Scenario execution started",
        "scenario_id": scenario_id,
        "started_at": datetime.utcnow().isoformat(),
    }
