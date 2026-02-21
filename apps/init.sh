#!/bin/bash

# Exit on any error
set -e

echo "Starting the Smart Home Sensor API..."
echo "Building and starting containers..."
docker-compose up --build -d

echo "Waiting for services to be ready..."
# Wait for PostgreSQL to be ready
for i in {1..30}; do
  if docker exec smarthome-postgres pg_isready -U postgres > /dev/null 2>&1; then
    echo "PostgreSQL is ready!"
    break
  fi
  echo "Waiting for PostgreSQL to start... ($i/30)"
  sleep 1
done

# Check if PostgreSQL is ready
if ! docker exec smarthome-postgres pg_isready -U postgres > /dev/null 2>&1; then
  echo "Error: PostgreSQL did not start within the expected time."
  exit 1
fi

echo "All services are up and running!"
echo "The API is available at http://localhost:8080"
echo ""
echo "To view logs, run: docker-compose logs -f"
echo "To stop the services, run: docker-compose down"
echo "Services:"
echo "- API Gateway: http://localhost:8000"
echo "- Auth Service: http://localhost:8001"
echo "- Device Service: http://localhost:8002"
echo "- Device Manager: http://localhost:8003"
echo "- Telemetry Service: http://localhost:8004"
echo "- Notification Service: http://localhost:8005"
echo "- Kafka: localhost:9092"
echo "- Kafka-ui: http://localhost:7777"

echo "- ClickHouse: http://localhost:8123"
echo ""
echo "API Documentation: http://localhost:8000/docs"
echo ""
echo "Test command: curl http://localhost:8000/health"