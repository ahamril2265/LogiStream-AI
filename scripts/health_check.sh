#!/bin/bash

# Health check script - Verify all services are running

echo "AI Logistics Platform - Health Check"
echo "======================================"
echo ""

# Check Docker
echo "🔍 Docker status:"
docker ps --filter "label=com.docker.compose.project=ai-logistics-platform" \
  --format "table {{.Names}}\t{{.Status}}" || \
  docker-compose ps

echo ""
echo "🔍 PostgreSQL:"
docker-compose exec postgres pg_isready -U postgres && echo "✓ Connected" || echo "✗ Connection failed"

echo ""
echo "🔍 MinIO:"
docker-compose exec minio mcli status local > /dev/null && echo "✓ Health check passed" || echo "✗ Health check failed"

echo ""
echo "🔍 Airflow Webserver:"
curl -s http://localhost:8080/health > /dev/null && echo "✓ Running" || echo "✗ Not responding"

echo ""
echo "🔍 MLflow:"
curl -s http://localhost:5000 > /dev/null && echo "✓ Running" || echo "✗ Not responding"

echo ""
echo "🔍 DuckDB:"
ls -lh data/logistics.duckdb 2>/dev/null && echo "✓ Database file exists" || echo "⚠ Database not initialized yet"

echo ""
echo "======================================"
echo "Health check complete"
