# Complete Run Commands Reference

This document contains all commands needed to run the AI Logistics Platform streaming pipeline with various configurations.

## Table of Contents
1. [Setup & Infrastructure](#setup--infrastructure)
2. [Data Producer](#data-producer)
3. [Pipeline Runner](#pipeline-runner)
4. [Monitoring](#monitoring)
5. [Common Scenarios](#common-scenarios)
6. [Troubleshooting](#troubleshooting)

---

## Setup & Infrastructure

### One-Time Initial Setup
This should be run once when you first clone the project.

```bash
# Create Python virtual environment and install dependencies
./run.sh setup

# Start Docker services (Kafka, MinIO, MLflow, PostgreSQL, Zookeeper)
./run.sh docker-up
```

### Stopping & Restarting

```bash
# Stop Docker services (preserves data for restart)
./run.sh docker-down

# Complete cleanup (deletes all Kafka messages and MinIO data)
./run.sh docker-clean

# Check service status
./run.sh status
```

---

## Data Producer

The producer generates realistic logistics data and streams it to Kafka topics continuously.

### Run Producer (Interactive)
Runs in the foreground, useful for viewing real-time output.

```bash
./run.sh producer

# Output:
#   ✓ Connected to Kafka
#   ✓ Simulation date range: 2024-02-22 to 2026-02-21
#   → Generating driver profiles...
#   → All 500 driver records produced successfully
#   → Streaming Batch 1: 100 orders + 2500 GPS events...
#   (Ctrl+C to stop)
```

### Run Producer (Background)
Runs in the background, useful for parallel pipeline execution.

```bash
./run.sh producer-bg

# Returns immediately with background process ID
# Logs to: logs/producer.log
# Stop with: pkill -f "python3 streaming/producer.py"
```

### Data Generated

| Topic | Records | Rate | Duration |
|-------|---------|------|----------|
| drivers | 500 | Once per run | ~1 second |
| orders | 100/batch | ~1 per sec | Continuous |
| gps_events | ~2500/batch | ~50/sec | Per order batch |
| weather_events | ~5/day | ~1 per 5 hours | Continuous |

---

## Pipeline Runner

The runner executes the full data transformation pipeline: Bronze → Silver → Gold → Features → ML.

### Run Pipeline Once

```bash
./run.sh runner

# Executes:
#   1. Bronze Ingestion (Kafka → MinIO)
#   2. Silver Transformation (Dedup & Clean)
#   3. Gold Aggregation (Metrics)
#   4. Feature Engineering (ML Features)
#   5. Data Quality Validation
#   6. ML Model Training
# Duration: ~2-3 minutes
```

### Run Pipeline Continuously (Default: 5-minute intervals)

```bash
./run.sh runner-continuous

# Repeats pipeline every 5 minutes
# Logs appended to: logs/runner.log
# Stop with: Ctrl+C or pkill -f "python3 runner.py"
```

### Run Pipeline with Custom Interval

```bash
# Every 2 minutes (for testing)
./run.sh runner-custom 120

# Every 10 minutes
./run.sh runner-custom 600

# Every 30 minutes (production)
./run.sh runner-custom 1800
```

### Run Pipeline in Background

```bash
# Run continuously in background with default interval (5 min)
./run.sh runner-bg

# Run with custom interval (e.g., 10 min = 600 seconds)
./run.sh runner-bg 600

# Returns immediately, logs to: logs/runner.log
# Stop with: pkill -f "python3 runner.py"
```

### Pipeline Output

```bash
# View logs
./run.sh logs-runner

# View data quality report
cat logs/dq_report.json

# Check MLflow experiments
# Open: http://localhost:5000
```

---

## Monitoring

### System Status

```bash
# Overall status of Docker, processes, and Kafka
./run.sh status

# Output:
# Docker Services:
# ✓ logistics-zookeeper   (UP)
# ✓ logistics-kafka       (HEALTHY)
# ✓ logistics-postgres    (HEALTHY)
# ✓ logistics-minio       (UP)
# ✓ logistics-mlflow      (UP)
#
# Running Processes:
# → Producer: PID 12345 (running)
# → Runner: Not running
#
# Kafka Topics:
# drivers        (500 messages)
# orders         (1250 messages)
# gps_events     (31000 messages)
# weather_events (0 messages)
```

### Monitor Kafka Topics

```bash
# Watch messages in real-time (from drivers topic)
./run.sh monitor-kafka

# Output:
# {"driver_id": "DRV_000001", "vehicle_type": "bike", ...}
# {"driver_id": "DRV_000002", "vehicle_type": "van", ...}
```

### View Logs

```bash
# Producer logs
./run.sh logs-producer

# Runner logs
./run.sh logs-runner

# Both logs
./run.sh logs-all

# Follow logs in real-time
tail -f logs/producer.log
tail -f logs/runner.log
```

### Web Dashboards

```bash
# Show all available URLs
./run.sh web-urls

# Direct URLs:
# MLflow:    http://localhost:5000  (Track ML experiments)
# MinIO:     http://localhost:9001  (View uploaded data)
# Kafka API: localhost:9092
```

---

## Common Scenarios

### Scenario 1: Quick Demo (5 minutes)

Run complete end-to-end test in one command.

```bash
./run.sh full-demo

# This will:
# 1. Start Docker services
# 2. Start producer in background
# 3. Run full pipeline
# 4. Show results and DQ report
# 5. Stop producer
# 6. Done!
```

### Scenario 2: Development Mode

Keep everything running and monitor logs.

```bash
# Terminal 1: Start everything and watch logs
./run.sh dev-mode

# Terminal 2: Monitor Kafka
./run.sh monitor-kafka

# Terminal 3: Check status
./run.sh status

# Terminal 4: View logs
./run.sh logs-all
```

### Scenario 3: Manual Step-by-Step

Manually control each component.

```bash
# Terminal 1: Setup and start Docker
./run.sh docker-up

# Terminal 2: Start producer (foreground, watch output)
./run.sh producer
# (runs until Ctrl+C)

# Terminal 3: In another window, run pipeline
./run.sh runner

# Monitor in Terminal 4
./run.sh status
./run.sh monitor-kafka
```

### Scenario 4: Production Mode (24/7)

Run everything continuously in background.

```bash
# Start producer in background
nohup ./run.sh producer-bg &

# Start runner every 5 minutes in background
nohup ./run.sh runner-continuous &

# Check status periodically
./run.sh status

# View logs
tail -f logs/runner.log

# Stop everything
pkill -f "python3 streaming/producer.py"
pkill -f "python3 runner.py"
```

### Scenario 5: Testing with Custom Interval

Test with different pipeline run frequencies.

```bash
# Every 2 minutes (for fast testing)
./run.sh docker-up
./run.sh producer-bg
./run.sh runner-custom 120

# Every 30 minutes (for long-term testing)
./run.sh runner-custom 1800
```

### Scenario 6: Integration Testing

Validate all components are working.

```bash
./run.sh setup
./run.sh docker-up
./run.sh test-integration

# This will:
# - Test Kafka connectivity
# - Test MinIO accessibility
# - Test DuckDB operations
# - Test Python imports
# - Confirm all components are ready
```

### Scenario 7: ML Experiment Tracking

Run multiple pipeline executions and track experiments.

```bash
./run.sh docker-up
./run.sh producer-bg

# Run multiple times to create different experiments
./run.sh runner
sleep 60
./run.sh runner
sleep 60
./run.sh runner

# View all runs in MLflow
# Open: http://localhost:5000
# Compare models and metrics

pkill -f "python3 streaming/producer.py"
```

---

## Troubleshooting

### Producer won't start

**Error:** `ModuleNotFoundError: No module named 'kafka'`

**Solution:**
```bash
./run.sh setup  # Install all dependencies
```

**Error:** `Connection refused` to Kafka

**Solution:**
```bash
./run.sh docker-up     # Ensure Docker is running
./run.sh status        # Check service health
docker logs logistics-kafka  # Check Kafka logs
```

### Runner fails with database errors

**Error:** `database is locked`

**Solution:**
```bash
pkill -f "python3 runner.py"  # Kill any running runner
sleep 2
./run.sh runner                # Try again
```

**Error:** `DuckDBConnector object has no attribute 'execute'`

**Solution:**
```bash
./run.sh reset-duckdb  # Delete and recreate database
./run.sh runner        # Run again
```

### Kafka topics are empty

**Problem:** Producer started but no messages in Kafka

**Solution:**
```bash
# Check producer is actually running
pgrep -f "python3 streaming/producer.py"

# Check logs
./run.sh logs-producer | tail -50

# Check Docker Kafka is healthy
./run.sh status
```

### MinIO bucket not found

**Problem:** Pipeline fails with MinIO bucket errors

**Solution:**
```bash
# Buckets are created by runner on first execution
./run.sh runner

# Or manually create:
docker exec logistics-minio mc mb minio/logistics-bronze
```

### Out of memory errors

**Problem:** Pipeline dies with memory errors

**Solution:**
```bash
# Reduce batch sizes in streaming/producer.py
# Or increase DuckDB memory in config/settings.py
# Or reduce data scale:
export NUM_DRIVERS=100
export NUM_DELIVERIES=10000
./run.sh runner
```

### High CPU usage

**Problem:** CPU maxed out during pipeline run

**Solution:**
```bash
# Increase interval between pipeline runs
./run.sh runner-custom 900  # Instead of 300 seconds

# Or stop one component
pkill -f "python3 streaming/producer.py"  # Stop producer
./run.sh runner                           # Run only pipeline
```

### Logs show errors but pipeline completes

**Common issues:**
```bash
# Warning: "Topic not available during auto-create initialization"
# → This is normal, Kafka will auto-create topics

# Warning: "Could not read from MinIO path"
# → Normal on first run before data exists

# Check overall status
./run.sh logs-runner | grep "Pipeline completed"  # Look for success message
```

---

## Configuration

### Environment Variables

Configure behavior by setting environment variables:

```bash
# Data scale
export NUM_DRIVERS=1000           # Default: 500
export NUM_DELIVERIES=500000      # Default: 100000
export SIM_YEARS=5                # Default: 2

# Streaming
export GPS_EVENTS_PER_DELIVERY=50 # Default: 25
export RANDOM_SEED=123            # Default: 42

# Logging
export LOG_LEVEL=DEBUG            # Default: INFO

# Infrastructure
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export MINIO_ENDPOINT=localhost:9000
export DUCKDB_PATH=./data/logistics.duckdb

# Run with custom config
./run.sh runner
```

### Configuration Files

Persistent settings in `config/settings.py`:

```python
# Data Simulation
NUM_DRIVERS = 500
NUM_DELIVERIES = 100000
SIM_YEARS = 2
GPS_EVENTS_PER_DELIVERY = 25

# Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# MinIO
MINIO_ENDPOINT = "localhost:9000"
MINIO_ROOT_USER = "minioadmin"
MINIO_ROOT_PASSWORD = "minioadmin"

# DuckDB
DUCKDB_PATH = "./data/logistics.duckdb"
```

---

## Advanced Usage

### Running with systemd (Linux)

Create `/etc/systemd/system/logistics-producer.service`:

```ini
[Unit]
Description=Logistics Data Producer
After=network.target

[Service]
Type=simple
User=ahamed
WorkingDirectory=/home/ahamed/Projects/DE/ai-logistics-platform
ExecStart=/home/ahamed/Projects/DE/ai-logistics-platform/run.sh producer
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable logistics-producer
sudo systemctl start logistics-producer
```

### Running with Docker

Create containerized runner:

```bash
docker run -it \
  -v $(pwd):/app \
  -w /app \
  -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  python:3.12 \
  bash -c "./run.sh setup && ./run.sh runner"
```

### CI/CD Integration

GitHub Actions example:

```yaml
name: Pipeline Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Setup Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.12'
      - name: Run Tests
        run: |
          ./run.sh setup
          ./run.sh docker-up
          ./run.sh test-integration
```

---

## Summary

| Task | Command | Time |
|------|---------|------|
| First-time setup | `./run.sh setup` | 2 min |
| Start Docker | `./run.sh docker-up` | 30 sec |
| Quick demo | `./run.sh full-demo` | 5 min |
| Single pipeline run | `./run.sh runner` | 3 min |
| Data quality check | `cat logs/dq_report.json` | 5 sec |
| Check ML experiments | `http://localhost:5000` | Browser |
| Continuous monitoring | `./run.sh dev-mode` | Ongoing |
| Complete reset | `./run.sh reset-all` | 1 min |

---

## Help

For detailed information on any command:

```bash
./run.sh help
```

For command-specific help, grep the output:

```bash
./run.sh help | grep -A 10 "runner-custom"
```
