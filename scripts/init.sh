#!/bin/bash

# Initialize AI Logistics Platform - Setup script
# This script prepares the environment for running the data lake

set -e

echo "========================================="
echo "AI Logistics Platform - Initialization"
echo "========================================="

# Create directories
echo "Creating directories..."
mkdir -p data logs airflow/logs airflow/plugins

# Load environment variables
if [ -f .env ]; then
    echo "Loading environment from .env..."
    export $(cat .env | grep -v '#' | xargs)
else
    echo "Warning: .env file not found. Using defaults."
fi

# Check Docker
echo "Checking Docker installation..."
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed"
    exit 1
fi

echo "Docker version: $(docker --version)"

# Check Docker Compose
echo "Checking Docker Compose installation..."
if ! command -v docker-compose &> /dev/null; then
    echo "ERROR: Docker Compose is not installed"
    exit 1
fi

echo "Docker Compose version: $(docker-compose --version)"

# Start services
echo "Starting Docker Compose services..."
docker-compose up -d

# Wait for services to be healthy
echo "Waiting for services to be healthy (this may take a few minutes)..."
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    echo "Checking service health... (attempt $((attempt+1))/$max_attempts)"
    
    # Check PostgreSQL
    if docker-compose exec postgres pg_isready -U ${POSTGRES_USER:-postgres} &> /dev/null; then
        echo "✓ PostgreSQL is ready"
    else
        echo "⏳ Waiting for PostgreSQL..."
        sleep 5
        ((attempt++))
        continue
    fi
    
    # Check MinIO
    if docker-compose exec minio mcli status local &> /dev/null; then
        echo "✓ MinIO is ready"
    else
        echo "⏳ Waiting for MinIO..."
        sleep 5
        ((attempt++))
        continue
    fi
    
    # Check Airflow
    if curl -s http://localhost:8080 > /dev/null; then
        echo "✓ Airflow is ready"
    else
        echo "⏳ Waiting for Airflow..."
        sleep 5
        ((attempt++))
        continue
    fi
    
    echo "All services are healthy!"
    break
done

if [ $attempt -eq $max_attempts ]; then
    echo "ERROR: Services did not become healthy within timeout"
    exit 1
fi

# Initialize Airflow database
echo "Initializing Airflow database..."
docker-compose exec -T airflow-webserver airflow db init || true

# Create MinIO buckets
echo "Creating MinIO buckets..."
docker-compose exec minio mcli mb local/${MINIO_BUCKET_BRONZE:-logistics-bronze} || true
docker-compose exec minio mcli mb local/${MINIO_BUCKET_SILVER:-logistics-silver} || true
docker-compose exec minio mcli mb local/${MINIO_BUCKET_GOLD:-logistics-gold} || true
docker-compose exec minio mcli mb local/${MINIO_BUCKET_FEATURES:-logistics-features} || true

echo ""
echo "========================================="
echo "✓ Initialization Complete!"
echo "========================================="
echo ""
echo "Web Interfaces:"
echo "  Airflow: http://localhost:8080 (admin/admin)"
echo "  MinIO: http://localhost:9001 (minioadmin/minioadmin)"
echo "  MLflow: http://localhost:5000"
echo ""
echo "Next steps:"
echo "  1. Run data pipeline: python3 main.py"
echo "  2. Trigger Airflow DAG: http://localhost:8080"
echo "  3. Check logs: docker-compose logs -f"
echo ""
