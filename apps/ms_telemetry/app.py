import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, Optional

import aiohttp
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


CLICKHOUSE_URL = f"http://{os.getenv('CLICKHOUSE_HOST', 'clickhouse')}:8123"
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "telemetry")


class TelemetryData(BaseModel):
    device_id: str
    timestamp: datetime
    metrics: Dict[str, float]
    tags: Optional[Dict[str, str]] = {}


async def execute_clickhouse_query(query: str):
    try:
        async with aiohttp.ClientSession() as session:
            params = {"database": CLICKHOUSE_DB, "query": query}
            async with session.post(CLICKHOUSE_URL, params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
                text = await resp.text()
                logger.error(f"ClickHouse error: {text}")
                return None
    except Exception as e:
        logger.error(f"ClickHouse connection error: {e}")
        return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{CLICKHOUSE_URL}/ping") as resp:
                if resp.status == 200:
                    logger.info("Connected to ClickHouse")
                else:
                    logger.error("ClickHouse is not available")
    except Exception as e:
        logger.error(f"Cannot connect to ClickHouse: {e}")
    yield


app = FastAPI(
    title="Telemetry Service",
    description="Сервис сбора и хранения телеметрии",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health_check():
    try:
        await execute_clickhouse_query("SELECT 1")
        return {"status": "healthy"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.post("/telemetry")
async def save_telemetry(data: TelemetryData):
    try:
        metrics_json = json.dumps(data.metrics)
        tags_json = json.dumps(data.tags or {})
        query = f"""
            INSERT INTO telemetry (device_id, timestamp, metrics, tags)
            VALUES (
                '{data.device_id}',
                '{data.timestamp.isoformat()}',
                '{metrics_json}',
                '{tags_json}'
            )
        """
        await execute_clickhouse_query(query)
        return {
            "status": "success",
            "message": "Telemetry saved",
            "device_id": data.device_id,
            "timestamp": data.timestamp,
        }
    except Exception as e:
        logger.error(f"Failed to save telemetry: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/telemetry/latest")
async def get_latest_telemetry(device_id: Optional[str] = None, limit: int = 10):
    try:
        if device_id:
            query = f"""
                SELECT device_id, timestamp, metrics, tags
                FROM telemetry WHERE device_id = '{device_id}'
                ORDER BY timestamp DESC LIMIT {limit}
            """
        else:
            query = f"""
                SELECT device_id, timestamp, metrics, tags
                FROM telemetry ORDER BY timestamp DESC LIMIT {limit}
            """
        result = await execute_clickhouse_query(query)
        if result and "data" in result:
            return result["data"]
        return []
    except Exception as e:
        logger.error(f"Failed to fetch telemetry: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/telemetry/history")
async def get_telemetry_history(
    device_id: str,
    start_date: str,
    end_date: Optional[str] = None,
    interval: str = "raw",
    limit: int = 1000,
):
    try:
        if interval != "raw":
            interval_func = {"hour": "toStartOfHour", "day": "toStartOfDay", "minute": "toStartOfMinute"}.get(
                interval, "toStartOfHour"
            )
            end_clause = f" AND timestamp <= '{end_date}'" if end_date else ""
            query = f"""
                SELECT
                    {interval_func}(timestamp) as interval_start,
                    avg(JSONExtractFloat(metrics, 'temperature')) as avg_temperature,
                    min(JSONExtractFloat(metrics, 'temperature')) as min_temperature,
                    max(JSONExtractFloat(metrics, 'temperature')) as max_temperature,
                    count() as data_points
                FROM telemetry
                WHERE device_id = '{device_id}' AND timestamp >= '{start_date}'{end_clause}
                GROUP BY interval_start ORDER BY interval_start DESC LIMIT {limit}
            """
        else:
            end_clause = f" AND timestamp <= '{end_date}'" if end_date else ""
            query = f"""
                SELECT timestamp, metrics FROM telemetry
                WHERE device_id = '{device_id}' AND timestamp >= '{start_date}'{end_clause}
                ORDER BY timestamp DESC LIMIT {limit}
            """
        result = await execute_clickhouse_query(query)
        if result and "data" in result:
            return result["data"]
        return []
    except Exception as e:
        logger.error(f"Failed to fetch history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/telemetry/devices/{device_id}/stats")
async def get_device_stats(device_id: str):
    try:
        query = f"""
            SELECT
                count() as total_records,
                min(timestamp) as first_record,
                max(timestamp) as last_record,
                avg(JSONExtractFloat(metrics, 'temperature')) as avg_temperature
            FROM telemetry WHERE device_id = '{device_id}'
        """
        result = await execute_clickhouse_query(query)
        if result and "data" in result and len(result["data"]) > 0:
            return result["data"][0]
        return {}
    except Exception as e:
        logger.error(f"Failed to get device stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))
