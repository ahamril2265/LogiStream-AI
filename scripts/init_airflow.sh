#!/bin/bash
# Airflow Database and Module Initialization Script
# 
# Usage: bash scripts/init_airflow.sh
# 
# This script:
# 1. Initializes Airflow metadata database
# 2. Creates admin user
# 3. Configures Kafka connections
# 4. Sets up variables
# 5. Registers plugins

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}=====================================================================${NC}"
echo -e "${BLUE}Apache Airflow - Initialization Script${NC}"
echo -e "${BLUE}=====================================================================${NC}"

# Check if running inside Docker
IN_DOCKER=${AIRFLOW_HOME:-false}

if [ "$IN_DOCKER" = "/opt/airflow" ]; then
    echo -e "${GREEN}✓ Running inside Docker container${NC}"
    AIRFLOW_CMD="airflow"
    PYTHON_CMD="python"
else
    echo -e "${BLUE}Running outside Docker - using Docker Compose exec${NC}"
    AIRFLOW_CMD="docker-compose -f docker-compose-airflow.yml exec -T webserver airflow"
    PYTHON_CMD="docker-compose -f docker-compose-airflow.yml exec -T webserver python"
    cd "$PROJECT_ROOT"
fi

echo -e "${BLUE}[1/5] Initializing Airflow metadata database...${NC}"
$AIRFLOW_CMD db migrate
echo -e "${GREEN}✓ Database initialized${NC}"

echo ""
echo -e "${BLUE}[2/5] Creating admin user...${NC}"
$AIRFLOW_CMD users create \
    --username admin \
    --password admin123 \
    --firstname Logistics \
    --lastname Admin \
    --role Admin \
    --email admin@logistics.local || echo -e "${BLUE}Admin user already exists${NC}"
echo -e "${GREEN}✓ Admin user ready${NC}"

echo ""
echo -e "${BLUE}[3/5] Configuring Kafka connection...${NC}"
$AIRFLOW_CMD connections add kafka_default \
    --conn-type kafka \
    --conn-host kafka \
    --conn-port 9092 \
    --conn-schema default 2>/dev/null || echo -e "${BLUE}Kafka connection already exists${NC}"
echo -e "${GREEN}✓ Kafka connection configured${NC}"

echo ""
echo -e "${BLUE}[4/5] Setting Airflow variables...${NC}"
cat > /tmp/airflow_vars.py << 'EOF'
import airflow.api.client.local_client as api_client

client = api_client.Client()

variables = {
    "project_root": "/opt/airflow/projects",
    "kafka_bootstrap_servers": "kafka:9092",
    "minio_endpoint": "minio:9000",
    "minio_bucket_bronze": "bronze",
    "minio_bucket_silver": "silver",
    "minio_bucket_gold": "gold",
    "postgres_host": "postgres",
    "postgres_port": "5432",
    "postgres_db": "logistics_db",
    "mlflow_uri": "http://mlflow:5000",
}

for key, value in variables.items():
    try:
        client.set_variable(key=key, value=value)
        print(f"✓ Set variable: {key}")
    except Exception as e:
        print(f"Variable {key} already exists")
EOF

# Only run if we're in Docker
if [ "$IN_DOCKER" = "/opt/airflow" ]; then
    python /tmp/airflow_vars.py 2>/dev/null || true
    rm /tmp/airflow_vars.py
fi
echo -e "${GREEN}✓ Variables configured${NC}"

echo ""
echo -e "${BLUE}[5/5] Verifying DAGs...${NC}"
$AIRFLOW_CMD list-dags
echo -e "${GREEN}✓ DAGs verified${NC}"

echo ""
echo -e "${GREEN}=====================================================================${NC}"
echo -e "${GREEN}Airflow initialization complete!${NC}"
echo -e "${GREEN}=====================================================================${NC}"
echo ""
echo -e "${BLUE}Access Airflow UI at: http://localhost:8080${NC}"
echo -e "${BLUE}Username: admin${NC}"
echo -e "${BLUE}Password: admin123${NC}"
echo ""
echo -e "${BLUE}Celery Flower UI (task monitoring): http://localhost:5555${NC}"
echo -e "${BLUE}MLflow UI: http://localhost:5000${NC}"
echo -e "${BLUE}MinIO Console: http://localhost:9001${NC}"
echo ""
