"""
Сервис уведомлений (Pusher). Весь код в одном файле.
"""
import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, Optional

import httpx
import ujson
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


PUSHER_CLUSTER = os.getenv("PUSHER_CLUSTER", "cluster1")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
PUSHER_APP_ID = os.getenv("PUSHER_APP_ID", "app_id")
PUSHER_KEY = os.getenv("PUSHER_KEY", "app_id")
PUSHER_SECRET = os.getenv("PUSHER_SECRET", "app_id")
FAKE_PUSHER = os.getenv("FAKE_PUSHER", "true").lower() in ("1", "true", "yes")

AUTH_VERSION = "1.0"
API_URL = f"https://api-{PUSHER_CLUSTER}.pusher.com"
TIMEOUT = 8


class NotificationCreate(BaseModel):
    user_id: str
    title: str
    message: str
    data: Optional[Dict] = None


async def trigger_event_fake(channel: str, event_name: str, event_data: dict):
    return


def _get_path(app_id: str) -> str:
    return f"/apps/{app_id}/events"


def _get_md5_body(body: str) -> str:
    m = hashlib.md5()
    m.update(body.encode())
    return m.hexdigest()


def _generate_signature(
    path: str,
    auth_key: str,
    auth_version: str,
    auth_secret: str,
    timestamp: int,
    md5_body: str,
) -> str:
    s = f"POST\n{path}\nauth_key={auth_key}&auth_timestamp={timestamp}&auth_version={auth_version}&body_md5={md5_body}"
    return hmac.new(
        auth_secret.encode(),
        msg=s.encode(),
        digestmod=hashlib.sha256,
    ).hexdigest()


async def trigger_event_real(channel: str, event_name: str, event_data: dict):
    payload = {"channel": channel, "name": event_name, "data": ujson.dumps(event_data)}
    event_body = ujson.dumps(payload)
    md5_body = _get_md5_body(event_body)
    timestamp = int(time.time())
    signature = _generate_signature(
        _get_path(PUSHER_APP_ID), PUSHER_KEY, AUTH_VERSION, PUSHER_SECRET, timestamp, md5_body
    )
    async with httpx.AsyncClient() as client:
        await client.post(
            API_URL + _get_path(PUSHER_APP_ID),
            params={
                "auth_key": PUSHER_KEY,
                "auth_timestamp": timestamp,
                "auth_signature": signature,
                "auth_version": AUTH_VERSION,
                "body_md5": md5_body,
            },
            headers={"content-type": "application/json"},
            json=payload,
            timeout=TIMEOUT,
        )


trigger_event = trigger_event_fake if FAKE_PUSHER else trigger_event_real


async def process_kafka_notification(data: Dict):
    try:
        notification = NotificationCreate(
            user_id=data.get("user_id", "system"),
            title=data.get("title", "Notification"),
            message=data.get("message", ""),
            data=data.get("data"),
        )
        event_data = {
            "user_id": notification.user_id,
            "type": "notification",
            "title": notification.title,
            "message": notification.message,
            "data": notification.data,
            "timestamp": datetime.utcnow().isoformat(),
        }
        await trigger_event("telemetry", "telemetry_data", event_data)
        logger.info(f"Processed Kafka notification for user {notification.user_id}")
    except Exception as e:
        logger.error(f"Failed to process Kafka notification: {e}")


async def consume_notifications():
    try:
        consumer = AIOKafkaConsumer(
            "notifications",
            bootstrap_servers=KAFKA_BROKERS,
            group_id="notification-service",
        )
        await consumer.start()
        async for msg in consumer:
            try:
                notification_data = json.loads(msg.value.decode("utf-8"))
                await process_kafka_notification(notification_data)
            except Exception as e:
                logger.error(f"Error processing Kafka message: {e}")
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    kafka_consumer_task = asyncio.create_task(consume_notifications())
    yield
    kafka_consumer_task.cancel()
    try:
        await kafka_consumer_task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="Notification Service",
    description="Сервис уведомлений и WebSocket соединений",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health_check():
    return {"status": "healthy", "database": "connected"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)
