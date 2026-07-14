# Apache Airflow Orchestration Guide

**Project:** AI Logistics Platform - Streaming Data Pipeline  
**Orchestrator:** Apache Airflow 2.8.0  
**Broker:** Celery with Redis  
**Metadata Store:** PostgreSQL  

---

## 📋 Table of Contents

1. [Quick Start](#quick-start)
2. [Architecture](#architecture)
3. [Services](#services)
4. [DAG Structure](#dag-structure)
5. [Running Workflows](#running-workflows)
6. [Monitoring](#monitoring)
7. [Troubleshooting](#troubleshooting)
8. [API Reference](#api-reference)

---

## 🚀 Quick Start

### Start Airflow Stack

```bash
# Start all Airflow services (includes PostgreSQL, Redis, MinIO, MLflow)
bash airflow/start_airflow.sh up

# Wait for services to be healthy (~30 seconds)
bash airflow/start_airflow.sh status
```

### Access UI

| Component | URL | Credentials |
|-----------|-----|-------------|
| **Airflow Web UI** | http://localhost:8080 | admin / admin123 |
| **Celery Flower** | http://localhost:5555 | - |
| **MLflow Tracking** | http://localhost:5000 | - |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |

### Run a DAG

```bash
# List available DAGs
bash airflow/start_airflow.sh dags

# Trigger a DAG manually
bash airflow/start_airflow.sh trigger logistics_streaming_pipeline

# Or use web UI: Click "Trigger DAG" in Airflow interface
```

---

## 🏗️ Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Apache Airflow 2.8.0                     │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐         ┌──────────────┐                  │
│  │  Scheduler   │◄───────►│  Webserver   │                  │
│  │  (DAG Queue) │         │  (Web UI)    │                  │
│  └──────┬───────┘         └──────────────┘                  │
│         │                                                   │
│         │ Push Tasks                                        │
│         ▼                                                   │
│  ┌──────────────────┐     ┌──────────────────┐             │
│  │    Redis Queue   │     │    PostgreSQL    │             │
│  │   (Message Broker)     │  (Metadata DB)   │             │
│  └────────┬─────────┘     └──────────────────┘             │
│           │                                                 │
│           │ Pop Tasks                                       │
│           ▼                                                 │
│  ┌──────────────────┐     ┌──────────────────┐             │
│  │  Celery Worker   │     │    Flower UI     │             │
│  │  (Task Executor) │     │  (Task Monitor)  │             │
│  └──────────────────┘     └──────────────────┘             │
│                                                              │
└─────────────────────────────────────────────────────────────┘
        │
        │ Data Exchange
        ▼
┌──────────────────────────────────────────────────────────────┐
│              Data Lake & ML Infrastructure                   │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │    MinIO     │  │   DuckDB     │  │   MLflow     │       │
│  │  (Data Lake) │  │  (Analytics) │  │ (ML Tracking)│       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Step 1: Kafka Producers
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ Driver Data  │ │ Order Data   │ │ GPS Events   │ │ Weather Data │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │                │                │                │
       └────────────────┴────────────────┴────────────────┘
                       │
                       ▼
            ┌──────────────────────┐
            │   Kafka Topics       │
            │ (drivers, orders,    │
            │  gps_events, weather)│
            └──────────┬───────────┘
                       │
        ┌──────────────┴──────────────┐
        │                             │
        ▼                             ▼
Step 2: Bronze Layer         Step 2: Streaming Pipeline
┌──────────────────┐          ┌──────────────────┐
│ Kafka Consumer   │          │ Airflow DAG      │
│ (Raw Ingestion)  │          │ (Orchestration)  │
└────────┬─────────┘          └────────┬─────────┘
         │                             │
         ▼                             ▼
Step 3: Silver Layer
┌──────────────────┐
│ Deduplication    │
│ & Cleansing      │
└────────┬─────────┘
         │
         ▼
Step 4: Gold Layer
┌──────────────────┐
│ Business Logic   │
│ Aggregation      │
└────────┬─────────┘
         │
         ▼
Step 5: Feature Engineering & ML
┌──────────────────┐
│ ML Feature Gen   │
│ & Model Training │
└────────┬─────────┘
         │
         ▼
Step 6: Data Quality & Monitoring
┌──────────────────┐
│ DQ Validation    │
│ Metrics Report   │
└──────────────────┘
```

---

## 🔧 Services

### PostgreSQL (Metadata Database)
- **Container:** airflow-postgres
- **Port:** 5432
- **Databases:** airflow, mlflow_db, logistics_db
- **Credentials:** airflow / airflow
- **Volume:** postgres_airflow_data

### Redis (Message Broker)
- **Container:** airflow-redis
- **Port:** 6379
- **Purpose:** Task queue for Celery workers
- **Volume:** redis_airflow_data

### Airflow Webserver
- **Container:** airflow-webserver
- **Port:** 8080
- **Purpose:** Web UI for DAG management
- **Command:** `webserver`

### Airflow Scheduler
- **Container:** airflow-scheduler
- **Port:** 8793
- **Purpose:** Schedules and monitors DAGs
- **Command:** `scheduler`
- **Concurrency:** 32 parallel tasks

### Airflow Worker (Celery)
- **Container:** airflow-worker
- **Purpose:** Executes tasks
- **Command:** `celery worker`
- **Concurrency:** 8 tasks per worker

### Flower UI (Celery Monitoring)
- **Container:** airflow-flower
- **Port:** 5555
- **Purpose:** Real-time task monitoring

### MinIO (Data Lake)
- **Container:** airflow-minio
- **Ports:** 9000 (API), 9001 (Console)
- **Volume:** airflow_minio_data
- **Buckets:** bronze, silver, gold

### MLflow (ML Experiment Tracking)
- **Container:** airflow-mlflow
- **Port:** 5000
- **Backend:** PostgreSQL (mlflow_db)
- **Artifacts:** /mlflow/artifacts

---

## 📊 DAG Structure

### DAG 1: `logistics_streaming_pipeline`

**File:** `/airflow/dags/streaming_pipeline.py`

**Schedule:** Manual trigger (via UI or CLI)

**Tasks:**

```
┌─────────────────────────────────────┐
│   Bronze Ingestion (Parallel)       │
├─────────────────────────────────────┤
│ ├─ Consume Drivers                  │
│ ├─ Consume Orders                   │
│ ├─ Consume GPS Events               │
│ └─ Consume Weather Events           │
└────────────┬────────────────────────┘
             │
             ▼
       ┌─────────────┐
       │ Silver      │
       │ Transform   │
       └────────┬────┘
                │
                ▼
       ┌─────────────┐
       │ Gold        │
       │ Aggregation │
       └────────┬────┘
                │
                ▼
       ┌─────────────────────────────┐
       │  Features & DQ (Parallel)   │
       ├─────────────────────────────┤
       │ ├─ Feature Engineering      │
       │ └─ Data Quality Validation  │
       └────────┬────────────────────┘
                │
                ▼
       ┌─────────────┐
       │ ML Training │
       └─────────────┘
```

**Task Details:**

| Task | Operator | Timeout | Retries |
|------|----------|---------|---------|
| consume_drivers | PythonOperator | 60s | 1 |
| consume_orders | PythonOperator | 180s | 1 |
| consume_gps_events | PythonOperator | 180s | 1 |
| consume_weather | PythonOperator | 180s | 1 |
| silver_transformation | PythonOperator | 300s | 1 |
| gold_aggregation | PythonOperator | 300s | 1 |
| feature_engineering | PythonOperator | 300s | 1 |
| data_quality_validation | PythonOperator | 300s | 1 |
| ml_training | PythonOperator | 600s | 1 |

### DAG 2: `logistics_data_lake_pipeline`

**File:** `/airflow/dags/logistics_pipeline.py`

**Schedule:** Daily @ 00:00 UTC

**Tasks:**

```
build_data → Bronze → Silver → Gold → [Features, DQ] → ML
```

---

## ▶️ Running Workflows

### Method 1: Web UI

1. Open http://localhost:8080
2. Click on a DAG (e.g., `logistics_streaming_pipeline`)
3. Click **"Trigger DAG"** button
4. Set runtime parameters if needed
5. Click **"Trigger"**

### Method 2: CLI

```bash
# List all DAGs
bash airflow/start_airflow.sh dags

# Trigger a DAG
bash airflow/start_airflow.sh trigger logistics_streaming_pipeline

# Test a DAG (single run in Docker)
bash airflow/start_airflow.sh test-dag logistics_streaming_pipeline

# View task logs
docker-compose -f docker-compose-airflow.yml logs -f webserver
```

### Method 3: Scheduled Execution

DAGs run automatically on their schedule interval:
- `logistics_streaming_pipeline` - Manual only
- `logistics_data_lake_pipeline` - Daily at 00:00 UTC

---

## 📈 Monitoring

### Airflow Web UI

- **DAGs View:** See all DAGs, trigger status, recent runs
- **Graph View:** Visualize task dependencies
- **Tree View:** See historical runs and outcomes
- **Code View:** View DAG Python code
- **Calendar View:** Heatmap of DAG run success/failure

### Flower UI (http://localhost:5555)

- **Active Tasks:** Real-time task execution
- **Worker Status:** Health of Celery workers
- **Task History:** Completed task details
- **Queue Status:** Pending tasks

### Logs

```bash
# Tail all logs
bash airflow/start_airflow.sh logs

# Tail specific service
bash airflow/start_airflow.sh logs-webserver   # Webserver logs
bash airflow/start_airflow.sh logs-scheduler   # Scheduler logs
bash airflow/start_airflow.sh logs-worker      # Worker logs
```

### Metrics

Check logs directory:
```bash
ls -la airflow/logs/
# dq_report.json       - Data quality results
# dag_airflow.log      - Airflow logs
```

---

## 🔍 Troubleshooting

### Issue: Services won't start

```bash
# Check Docker
docker ps -a

# Check logs
bash airflow/start_airflow.sh logs

# Restart services
bash airflow/start_airflow.sh restart
```

### Issue: DAG not appearing in UI

```bash
# Check DAG syntax
docker-compose -f docker-compose-airflow.yml exec -T webserver python -m py_compile airflow/dags/logistics_pipeline.py

# Refresh Airflow (webserver may cache)
bash airflow/start_airflow.sh restart
```

### Issue: Task fails with import error

```bash
# Check PYTHONPATH in docker-compose-airflow.yml
# Should include:
# - /opt/airflow
# - /opt/airflow/config
# - /opt/airflow/streaming

# Verify modules exist
docker-compose -f docker-compose-airflow.yml exec -T webserver python -c "import config; print('✓ Config module OK')"
```

### Issue: Database connection error

```bash
# Check PostgreSQL is running
docker-compose -f docker-compose-airflow.yml ps postgres

# Check connection
docker-compose -f docker-compose-airflow.yml exec -T postgres psql -U airflow -d airflow -c "SELECT 1"
```

### Issue: Worker not picking up tasks

```bash
# Check Redis connection
docker-compose -f docker-compose-airflow.yml exec -T redis redis-cli ping

# Check worker logs
bash airflow/start_airflow.sh logs-worker

# Restart worker
docker-compose -f docker-compose-airflow.yml restart worker
```

### Reset Everything (Destructive)

```bash
# Stop and remove all containers
bash airflow/start_airflow.sh down

# Remove volumes
docker volume rm \
  postgres_airflow_data \
  redis_airflow_data \
  airflow_minio_data \
  airflow_mlflow_artifacts \
  airflow_data

# Start fresh
bash airflow/start_airflow.sh up
```

---

## 📚 API Reference

### CLI Commands

```bash
# Service management
bash airflow/start_airflow.sh up               # Start stack
bash airflow/start_airflow.sh down             # Stop & remove
bash airflow/start_airflow.sh restart          # Restart services
bash airflow/start_airflow.sh status           # Show status

# Logs
bash airflow/start_airflow.sh logs             # All logs
bash airflow/start_airflow.sh logs-webserver   # Webserver logs
bash airflow/start_airflow.sh logs-scheduler   # Scheduler logs
bash airflow/start_airflow.sh logs-worker      # Worker logs

# DAG management
bash airflow/start_airflow.sh dags             # List DAGs
bash airflow/start_airflow.sh test-dag <id>    # Test DAG
bash airflow/start_airflow.sh trigger <id>     # Trigger DAG

# Admin
bash airflow/start_airflow.sh init             # Initialize
bash airflow/start_airflow.sh reset            # Reset DB (DESTRUCTIVE)
bash airflow/start_airflow.sh bash             # Shell access
```

### REST API

Airflow provides REST API at `http://localhost:8080/api/v1/`

```bash
# List DAGs
curl -u admin:admin123 http://localhost:8080/api/v1/dags

# Get DAG details
curl -u admin:admin123 http://localhost:8080/api/v1/dags/logistics_streaming_pipeline

# List task instances
curl -u admin:admin123 http://localhost:8080/api/v1/dags/logistics_streaming_pipeline/taskInstances

# Trigger DAG
curl -X POST \
  -u admin:admin123 \
  "http://localhost:8080/api/v1/dags/logistics_streaming_pipeline/dagRuns" \
  -H "Content-Type: application/json" \
  -d '{}'
```

---

## 🔐 Security Notes

### Default Credentials (Change in Production!)

- **Airflow Admin:** admin / admin123
- **MinIO:** minioadmin / minioadmin
- **PostgreSQL:** airflow / airflow

### Production Recommendations

1. **Change default passwords** in docker-compose-airflow.yml
2. **Enable authentication** for Airflow (LDAP, OAuth, etc.)
3. **Use secrets** for sensitive data (API keys, credentials)
4. **Enable SSL/TLS** for all services
5. **Restrict network access** to Airflow ports
6. **Enable audit logging** for all operations
7. **Regular backups** of PostgreSQL and MinIO data
8. **Monitor resource usage** (CPU, memory, disk)

---

## 📝 Example: Triggering a DAG Programmatically

```python
# From your application
from airflow.api.client import local_client

client = local_client.Client()

# Trigger DAG
client.trigger_dag(
    dag_id='logistics_streaming_pipeline',
    conf={'param': 'value'}
)
```

---

## 🚀 Next Steps

1. ✅ Start the Airflow stack
2. ✅ Verify all services are healthy
3. ✅ Trigger the streaming pipeline
4. ✅ Monitor execution in web UI
5. ✅ Check logs for any errors
6. ✅ View data quality report
7. ✅ Track ML models in MLflow

---

## 📞 Support

For issues or questions:
1. Check logs: `bash airflow/start_airflow.sh logs`
2. Review Troubleshooting section above
3. Check Airflow documentation: https://airflow.apache.org/docs/
4. View project README: [README.md](../README.md)

---

**Last Updated:** 2026-02-21  
**Airflow Version:** 2.8.0  
**Status:** ✅ Production Ready
