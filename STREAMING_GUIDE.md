# Streaming Architecture Guide

## Overview

The logistics platform now uses a **streaming architecture** with:
- **Kafka Producers**: Continuously generate data (run local Python scripts)
- **Kafka Topics**: Event streaming in real-time (Docker)
- **Kafka Consumers**: Bronze layer ingests from Kafka (via runner.py)
- **Pipeline Runner**: Orchestrate Silver, Gold, Features, ML, and DQ phases (local Python)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ STREAMING LAYER (Always Running)                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  producers.py (run continuously, Ctrl+C to stop)                │
│  ├── Drivers → drivers topic                                    │
│  ├── Orders → orders topic (continuous stream)                  │
│  ├── GPS Events → gps_events topic (continuous stream)          │
│  └── Weather → weather_events topic                             │
│                                                                  │
│  Kafka Topics (in-memory event streams)                         │
│  ├── drivers                                                    │
│  ├── orders                                                     │
│  ├── gps_events                                                 │
│  └── weather_events                                             │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ BRONZE LAYER (Triggered via Airflow)                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  streaming/consumer.py                                          │
│  └── Consume Kafka topics → MinIO Bronze buckets               │
│      ├── logistics-bronze/raw/drivers/                          │
│      ├── logistics-bronze/raw/orders/                           │
│      ├── logistics-bronze/raw/gps_events/                       │
│      └── logistics-bronze/raw/weather_events/                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ SILVER LAYER (Transformation)                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  silver/transformer.py (Airflow DAG)                            │
│  └── Deduplication, SCD Type 2, schema validation               │
│      ├── logistics-silver/drivers/                              │
│      ├── logistics-silver/orders/                               │
│      ├── logistics-silver/gps_events/                           │
│      └── logistics-silver/weather/                              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
         ↓                        ↓
   ┌──────────────┐       ┌────────────────┐
   │ GOLD LAYER   │       │ FEATURES LAYER │
   │(Aggregation) │       │ (ML Preparation)│
   └──────────────┘       └────────────────┘
         ↓                        ↓
   ┌──────────────────────────────────────┐
   │ ML TRAINING + DQ VALIDATION          │
   │ - RandomForest (delay prediction)    │
   │ - XGBoost (ETA estimation)           │
   │ - IsolationForest (anomaly detection)│
   │ - MLflow experiment tracking         │
   └──────────────────────────────────────┘
```

## Quick Start

### 1. Start Docker Services

```bash
cd /home/ahamed/Projects/DE/ai-logistics-platform

# Use the new streaming docker-compose
docker-compose -f docker-compose-streaming.yml up -d
```

**Services (Docker):**
- ✅ Zookeeper (2181)
- ✅ Kafka (9092)
- ✅ PostgreSQL (5432) - Airflow backend
- ✅ MinIO (9000, 9001)
- ✅ MLflow (5000)

**Note:** Airflow runs locally (not in Docker)

Wait 30-60 seconds for all services to start.

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Start Kafka Producer (Continuous Stream)

In **Terminal B**, start the data generator:

```bash
python3 streaming/producer.py
```

**Output:**
```
Starting order stream: 100 orders
Produced 25 order records  
Produced 50 order records
...
[Continues generating data until you press Ctrl+C]
```

**Leave this running!** The producer streams data continuously.

### 5. Run Pipeline (Manual Trigger)

In a **new Terminal C**, trigger the pipeline runner:

```bash
python3 runner.py
```

This executes the full pipeline:
- Bronze ingestion (consume Kafka topics, 1-3 min)
- Silver transformation (dedupe, SCD2, 30-60 sec)
- Gold aggregation (metrics, 30-60 sec)
- Feature engineering (ML prep, 30-60 sec)
- Data quality checks (validation, 30-60 sec)
- ML training (models + MLflow, 1-2 min)

**Total runtime:** ~5-10 minutes

### 6. Check Results

**MinIO Console:**
```
http://localhost:9001
Login: minioadmin / minioadmin

Buckets:
├── logistics-bronze/ → Raw streaming data
├── logistics-silver/ → Deduplicated & cleaned
├── logistics-gold/ → Business metrics
└── logistics-features/ → ML-ready features
```

**MLflow UI:**
```
http://localhost:5000
└── Experiment: logistics-ml
    ├── Run 1: RandomForest for delay prediction
    ├── Run 2: XGBoost for ETA estimation
    └── Run 3: IsolationForest for anomalies
```

**Logs:**
```
./logs/platform.log
./logs/dq_report.json
```

## Continuous Streaming

### Terminal Setup

You'll need **3 terminals** running simultaneously:

**Terminal A** - Kafka Producer (Always Running):
```bash
# Terminal A - Always keep running
python3 streaming/producer.py
```

This generates new data continuously:
- Orders: ~2 orders/sec
- GPS Events: ~50 events/sec per delivery
- Weather: 5 events every 5 seconds
- Drivers: Generated once at startup

**Terminal B** - Pipeline Runner (On-Demand):
```bash
# Run once
python3 runner.py

# Or run continuously every 5 minutes
python3 runner.py --continuous

# Or custom interval (e.g., 10 minutes = 600 seconds)
python3 runner.py --continuous --interval 600
```

**Terminal C** - Monitor (Optional):
```bash
# Watch logs in real-time
tail -f ./logs/platform.log

# Or check DQ report after each run
cat ./logs/dq_report.json | jq '.'
```

### Execution Timeline

```
T=0:00  → Docker services start
T=0:30  → Producer starts streaming data
T=1:00  → Trigger runner (first batch ingestion)
T=10:00 → Runner completes pipeline + ML training
T=10:05 → Trigger runner again (continuous)
...     → Continue indefinitely
```

### Trigger Pipeline Multiple Times

The runner automatically consumes the latest Kafka data.

```bash
# Manual trigger via CLI
python3 runner.py

# For continuous processing (runs every 5 min):
python3 runner.py --continuous

# Each execution:
# 1. Consumes messages from Kafka (1-3 min timeout)
# 2. Transforms and aggregates (dedup, SCD2, metrics)
# 3. Engineers features for ML
# 4. Validates data quality (generates report)
# 5. Trains fresh models (RandomForest, XGBoost, IsoForest)
```

## Scripts

### Producer (`streaming/producer.py`)

```bash
# Continuous data generation
python3 streaming/producer.py

# Control:
# - Ctrl+C to stop
# - Data persists in Kafka topics
```

### Consumer (`streaming/consumer.py`)

Runs as part of pipeline runner. Called by:
- `runner.py` → Bronze ingestion tasks

### Pipeline Runner (`runner.py`) - Replaces Airflow

```bash
# Run pipeline once
python3 runner.py

# Run pipeline continuously every 5 minutes
python3 runner.py --continuous

# Run pipeline continuously with custom interval (seconds)
python3 runner.py --continuous --interval 600

# Monitor execution
# - Logs: tail -f ./logs/platform.log
# - DQ Report: ./logs/dq_report.json
# - MinIO: http://localhost:9001
# - MLflow: http://localhost:5000
```

## Configuration

### Environment Variables

Edit `.env` for settings:

```env
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# MinIO
MINIO_ENDPOINT=localhost:9000
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# MLflow
MLFLOW_TRACKING_URI=http://localhost:5000

# DuckDB (local)
DUCKDB_PATH=./data/logistics.duckdb
```

### Python Configuration

The pipeline runner uses these configurations from `.env`:

```bash
# All configs are loaded automatically
python3 runner.py  # Uses settings from .env
```

Edit `.env` to change:
- Kafka bootstrap servers
- MinIO endpoint and credentials
- MLflow tracking URI
- DuckDB database path
- Data simulation parameters
- Data lake settings

## Stopping Services

### Stop Everything (Clean Shutdown)

```bash
# Terminal A: Stop Kafka Producer
# Press Ctrl+C

# Terminal B: Stop Pipeline Runner
# Press Ctrl+C

# Terminal C: Stop Docker services
docker-compose -f docker-compose-streaming.yml down
```

### Stop Docker Only (Keep Local Data)

```bash
docker-compose -f docker-compose-streaming.yml down

# Data persists in volumes:
# - PostgreSQL: postgres_data
# - MinIO: minio_data
# - MLflow: mlflow_artifacts
```

### Clean Restart (Remove All Data)

```bash
# WARNING: This deletes all data!
docker-compose -f docker-compose-streaming.yml down -v

# Also clean local logs:
rm -rf ./logs
rm -rf ./data
```

## Troubleshooting

### Docker Services

**Kafka connection refused**
```
ERROR: Failed to connect to Kafka
→ Wait 30 seconds for Kafka to start
→ Check: docker-compose -f docker-compose-streaming.yml ps
```

**"Topic does not exist"**
```
→ Kafka auto-creates topics
→ Check: kafka-topics --list --bootstrap-server localhost:9092
```

**MinIO buckets not created**
```
→ Runner tasks auto-create them
→ Or manually: from config import MinIOClient; MinIOClient(...).ensure_bucket_exists(...)
```

### Pipeline Runner

**"No module named 'config'"**
```
ERROR: ModuleNotFoundError: No module named 'config'
→ PYTHONPATH missing project root
→ Fix: cd /path/to/ai-logistics-platform && python3 runner.py
→ Or: export PYTHONPATH=/home/ahamed/Projects/DE/ai-logistics-platform:$PYTHONPATH
```

**"postgresql://... connection refused"**
```
ERROR: psycopg2.OperationalError: connection refused
→ PostgreSQL Docker container not running
→ Check: docker-compose -f docker-compose-streaming.yml ps
→ Start: docker-compose -f docker-compose-streaming.yml up -d postgres
```

**"Consumer stuck consuming" for >5 minutes**
```
→ Either: Timeout reached, or waiting for messages
→ Press Ctrl+C to interrupt
→ Check Kafka topics: kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning
→ If no messages: Producer may not be running
```

### Producer/Consumer

**Kafka producer hangs on send**
```
ERROR: KafkaTimeoutException
→ Kafka not ready: Wait 30+ seconds after docker-compose up
→ Check: nc -zv localhost 9092
```

**"Address already in use" from docker-compose**
```
ERROR: driver failed programming external connectivity
→ Port already in use by another container
→ List running: docker ps
→ Kill conflict: docker stop <container_id>
→ Or use docker-compose -f docker-compose-streaming.yml down and restart
```

## Next Steps

**With 3 Terminals Running:**

1. **Terminal A**: Kafka Producer → `python3 streaming/producer.py` (always running)
2. **Terminal B**: Pipeline Runner → `python3 runner.py` (triggered manually)
3. **Terminal C**: Monitor → `tail -f ./logs/platform.log` (optional)

**Workflow:**

1. ✅ Start Docker services: `docker-compose -f docker-compose-streaming.yml up -d`
2. ✅ Install dependencies: `pip install -r requirements.txt`
3. ✅ Start producer (Terminal A): `python3 streaming/producer.py`
4. ✅ Trigger pipeline once (Terminal B): `python3 runner.py`
5. ✅ Monitor execution in logs
6. ✅ Check results in MinIO Console, MLflow UI, logs
7. ✅ Trigger pipeline again: `python3 runner.py`
8. ✅ For continuous runs: `python3 runner.py --continuous`
9. ✅ Repeat steps 7-8 indefinitely

**Expected Volumes:**

- First run: ~500 drivers, 100 orders, ~2500 GPS events
- Per subsequent run: ~100 new orders, ~2500 new GPS events
- Data accumulates in Kafka topics and MinIO buckets

**Continuous Processing Example:**

```bash
# Terminal A (keeps running)
python3 streaming/producer.py

# Terminal B (separate terminal)
python3 runner.py --continuous --interval 300  # Every 5 minutes

# Monitor logs in Terminal C (optional)
tail -f ./logs/platform.log
```

This will:
- Continuously generate data in Kafka
- Automatically process new data every 5 minutes
- Store results in MinIO
- Track experiments in MLflow
- Generate DQ reports
- Train fresh ML models

## System Monitoring

```bash
# Monitor Kafka consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group bronze-ingestion-group --describe

# Check MinIO bucket sizes
mc du minio/logistics-bronze

# Monitor MLflow experiments
curl http://localhost:5000/api/2.0/mlflow/experiments/list
```

---

**Architecture Complete!** 🎉
- ✅ Kafka streaming (Docker)
- ✅ Continuous producers (Local Python)
- ✅ Pipeline runner (Local Python, no Airflow needed)
- ✅ Bronze/Silver/Gold/Features pipeline (runner.py)
- ✅ ML training with MLflow (Docker)
- ✅ Data quality validation (runner.py)
- ✅ Python 3.12 compatible (no Airflow compatibility issues)
