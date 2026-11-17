import logging
import os
from typing import Any, Dict, Optional

import httpx
from fastapi import Body, Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SERVICES = {
    "auth": os.getenv("AUTH_SERVICE_URL", "http://auth-service:8001"),
    "device": os.getenv("DEVICE_SERVICE_URL", "http://device-service:8002"),
    "device_manager": os.getenv("DEVICE_MANAGER_URL", "http://device-manager:8003"),
    "telemetry": os.getenv("TELEMETRY_SERVICE_URL", "http://telemetry-service:8004"),
    "notification": os.getenv("NOTIFICATION_SERVICE_URL", "http://notification-service:8005"),
}


class ServiceClient:
    def __init__(self):
        self.timeout = 30.0
        self.services = SERVICES

    async def _make_request(
        self,
        service_name: str,
        method: str,
        path: str,
        data: Optional[dict] = None,
        headers: Optional[dict] = None,
    ):
        base_url = self.services.get(service_name)
        if not base_url:
            raise HTTPException(status_code=500, detail=f"Service {service_name} not configured")
        url = f"{base_url}{path}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            method_func = getattr(client, method.lower())
            if data is not None:
                response = await method_func(url, json=data, headers=headers)
            else:
                response = await method_func(url, headers=headers)
            if response.status_code >= 400:
                logger.error(f"Service {service_name} error: {response.status_code} - {response.text}")
            return response


async def get_service_client():
    return ServiceClient()


async def validate_token(request: Request, client: ServiceClient = Depends(get_service_client)):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="No token provided")
    token = auth_header.split(" ")[1]
    try:
        response = await client._make_request("auth", "POST", "/auth/internal/validate", {"token": token})
        if response.status_code != 200:
            raise HTTPException(status_code=401, detail="Invalid token")
        return response.json()
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token validation failed: {str(e)}")


# --- Приложение ---
app = FastAPI(
    title="Smart House API Gateway",
    description="Основной шлюз для системы умного дома",
    version="1.0.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Incoming request: {request.method} {request.url}")
    response = await call_next(request)
    logger.info(f"Response status: {response.status_code}")
    return response


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


def _proxy_response(response: httpx.Response):
    return JSONResponse(
        content=response.json() if response.content else {},
        status_code=response.status_code,
    )


@app.post("/auth/login")
async def login(request: dict, client: ServiceClient = Depends(get_service_client)):
    response = await client._make_request("auth", "POST", "/auth/login", request)
    return _proxy_response(response)


@app.post("/auth/register")
async def register(request: dict, client: ServiceClient = Depends(get_service_client)):
    response = await client._make_request("auth", "POST", "/auth/register", request)
    return _proxy_response(response)


@app.post("/auth/users/me")
async def user_me(request: dict, client: ServiceClient = Depends(get_service_client)):
    response = await client._make_request("auth", "GET", "/auth/users/me", request)
    return _proxy_response(response)


@app.post("/auth/logout")
async def logout(request: dict, client: ServiceClient = Depends(get_service_client)):
    response = await client._make_request("auth", "POST", "/auth/logout", request)
    return _proxy_response(response)


@app.get("/device")
async def get_devices(
    device_type: Optional[str] = None,
    location: Optional[str] = None,
    client: ServiceClient = Depends(get_service_client),
    _: dict = Depends(validate_token),
):
    params = []
    if device_type:
        params.append(f"device_type={device_type}")
    if location:
        params.append(f"location={location}")
    query = "?" + "&".join(params) if params else ""
    response = await client._make_request("device", "GET", f"/devices{query}")
    return _proxy_response(response)


@app.get("/device/{device_id}")
async def get_device(
    device_id: str,
    client: ServiceClient = Depends(get_service_client),
    _: dict = Depends(validate_token),
):
    response = await client._make_request("device", "GET", f"/devices/{device_id}")
    return _proxy_response(response)


@app.post("/device")
async def create_device(
    request: dict,
    client: ServiceClient = Depends(get_service_client),
    _: dict = Depends(validate_token),
):
    response = await client._make_request("device", "POST", "/devices", request)
    return _proxy_response(response)


@app.post("/device/{device_id}/command")
async def send_device_command(
    device_id: str,
    request: dict,
    client: ServiceClient = Depends(get_service_client),
    _: dict = Depends(validate_token),
):
    response = await client._make_request("device", "POST", f"/devices/{device_id}/command", request)
    return _proxy_response(response)


@app.get("/scenarios")
async def get_scenarios(
    client: ServiceClient = Depends(get_service_client),
    _: dict = Depends(validate_token),
):
    response = await client._make_request("device_manager", "GET", "/scenarios")
    return _proxy_response(response)


@app.get("/scenarios/{scenario_id}")
async def get_scenario(
    scenario_id: str,
    client: ServiceClient = Depends(get_service_client),
    _: dict = Depends(validate_token),
):
    response = await client._make_request("device_manager", "GET", f"/scenarios/{scenario_id}")
    return _proxy_response(response)


@app.post("/scenarios")
async def create_scenario(
    request_data: dict,
    client: ServiceClient = Depends(get_service_client),
    _: dict = Depends(validate_token),
):
    response = await client._make_request("device_manager", "POST", "/scenarios", request_data)
    return _proxy_response(response)


@app.post("/scenarios/{scenario_id}/execute")
async def execute_scenario(
    scenario_id: str,
    request: Optional[dict] = None,
    client: ServiceClient = Depends(get_service_client),
    _: dict = Depends(validate_token),
):
    response = await client._make_request(
        "device_manager", "POST", f"/scenarios/{scenario_id}/execute", request
    )
    return _proxy_response(response)


@app.get("/telemetry/latest")
async def get_latest_telemetry(
    device_id: Optional[str] = None,
    limit: int = 10,
    client: ServiceClient = Depends(get_service_client),
    _: dict = Depends(validate_token),
):
    params = [f"limit={limit}"]
    if device_id:
        params.append(f"device_id={device_id}")
    query = "?" + "&".join(params)
    response = await client._make_request("telemetry", "GET", f"/telemetry/latest{query}")
    return _proxy_response(response)


@app.get("/telemetry/history")
async def get_telemetry_history(
    device_id: str,
    start_date: str,
    end_date: Optional[str] = None,
    interval: str = "raw",
    client: ServiceClient = Depends(get_service_client),
    _: dict = Depends(validate_token),
):
    params = [f"device_id={device_id}", f"start_date={start_date}", f"interval={interval}"]
    if end_date:
        params.append(f"end_date={end_date}")
    query = "?" + "&".join(params)
    response = await client._make_request("telemetry", "GET", f"/telemetry/history{query}")
    return _proxy_response(response)


@app.get("/telemetry/devices/{device_id}/stats")
async def get_telemetry_stats(
    device_id: str,
    client: ServiceClient = Depends(get_service_client),
    _: dict = Depends(validate_token),
):
    response = await client._make_request("telemetry", "GET", f"/telemetry/devices/{device_id}/stats")
    return _proxy_response(response)


def _device_to_sensor(device: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": device["id"],
        "name": device["name"],
        "type": device.get("device_type"),
        "location": device.get("location", ""),
        "value": device.get("configuration", {}).get("last_value", 0),
        "unit": device.get("configuration", {}).get("unit", ""),
        "status": device.get("status"),
        "last_updated": device.get("last_seen"),
        "created_at": device.get("created_at"),
    }


def _sensor_type_to_device_type(sensor_type: str) -> str:
    if sensor_type == "temperature":
        return "temperature_sensor"
    if sensor_type == "humidity":
        return "humidity_sensor"
    return sensor_type


@app.get("/api/v1/sensors")
async def legacy_get_sensors(
    location: Optional[str] = None,
    client: ServiceClient = Depends(get_service_client),
):
    response = await client._make_request("device", "GET", "/devices")
    devices = response.json()
    sensors = []
    for device in devices:
        if device.get("device_type") in ["temperature_sensor", "humidity_sensor"]:
            sensors.append(_device_to_sensor(device))
    if location:
        sensors = [s for s in sensors if s.get("location") == location]
    return JSONResponse(content=sensors, status_code=200)


@app.get("/api/v1/sensors/{sensor_id}")
async def legacy_get_sensor_by_id(sensor_id: str, client: ServiceClient = Depends(get_service_client)):
    response = await client._make_request("device", "GET", f"/devices/{sensor_id}")
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return JSONResponse(content=_device_to_sensor(response.json()), status_code=200)


@app.post("/api/v1/sensors")
async def legacy_create_sensor(
    payload: Dict[str, Any] = Body(...),
    client: ServiceClient = Depends(get_service_client),
):
    device_payload = {
        "name": payload.get("name"),
        "device_type": _sensor_type_to_device_type(payload.get("type", "temperature")),
        "location": payload.get("location"),
        "configuration": {"unit": payload.get("unit", ""), "last_value": payload.get("value", 0)},
        "metadata": {},
    }
    response = await client._make_request("device", "POST", "/devices", device_payload)
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return JSONResponse(content=_device_to_sensor(response.json()), status_code=201)


@app.put("/api/v1/sensors/{sensor_id}")
async def legacy_update_sensor(
    sensor_id: str,
    payload: Dict[str, Any] = Body(...),
    client: ServiceClient = Depends(get_service_client),
):
    configuration = {}
    if "value" in payload and payload["value"] is not None:
        configuration["last_value"] = payload["value"]
    if "unit" in payload and payload["unit"]:
        configuration["unit"] = payload["unit"]
    device_update = {}
    if "name" in payload:
        device_update["name"] = payload["name"]
    if "location" in payload:
        device_update["location"] = payload["location"]
    if "status" in payload:
        device_update["status"] = payload["status"]
    if configuration:
        device_update["configuration"] = configuration
    if not device_update:
        raise HTTPException(status_code=400, detail="No fields to update")
    response = await client._make_request("device", "PATCH", f"/devices/{sensor_id}", device_update)
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="Sensor not found")
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return JSONResponse(content=_device_to_sensor(response.json()), status_code=200)


@app.delete("/api/v1/sensors/{sensor_id}")
async def legacy_delete_sensor(sensor_id: str, client: ServiceClient = Depends(get_service_client)):
    response = await client._make_request("device", "DELETE", f"/devices/{sensor_id}")
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="Sensor not found")
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return JSONResponse(content={"message": "Sensor deleted successfully"}, status_code=200)


@app.patch("/api/v1/sensors/{sensor_id}/value")
async def legacy_update_sensor_value(
    sensor_id: str,
    payload: Dict[str, Any] = Body(...),
    client: ServiceClient = Depends(get_service_client),
):
    if "value" not in payload or "status" not in payload:
        raise HTTPException(status_code=400, detail="value and status are required")
    device_update = {"status": payload["status"], "configuration": {"last_value": payload["value"]}}
    response = await client._make_request("device", "PATCH", f"/devices/{sensor_id}", device_update)
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="Sensor not found")
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return JSONResponse(content={"message": "Sensor value updated successfully"}, status_code=200)


@app.get("/api/v1/sensors/temperature/{location}")
async def legacy_get_temperature_by_location(location: str, client: ServiceClient = Depends(get_service_client)):
    response = await client._make_request("device", "GET", "/devices")
    devices = response.json()
    candidates = [
        d for d in devices
        if d.get("device_type") == "temperature_sensor" and d.get("location") == location
    ]
    if not candidates:
        raise HTTPException(status_code=404, detail="No temperature sensors for this location")
    device = candidates[0]
    config = device.get("configuration", {}) or {}
    result = {
        "location": location,
        "value": config.get("last_value", 0),
        "unit": config.get("unit", "C"),
        "status": device.get("status", "unknown"),
        "timestamp": device.get("last_seen"),
        "description": f"Temperature from device {device.get('id')}",
    }
    return JSONResponse(content=result, status_code=200)
