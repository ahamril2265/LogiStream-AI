# AI Logistics Platform - Streaming Data Pipeline

A complete end-to-end data engineering solution for real-time logistics data processing with Kafka streaming, data transformation, and ML-powered insights.

## 🚀 Quick Start

```bash
# One-line quick demo (5 minutes)
./run.sh full-demo

# Or step-by-step:
./run.sh setup        # First time only: Install dependencies
./run.sh docker-up    # Start Docker services
./run.sh producer     # Terminal 1: Start data producer
./run.sh runner       # Terminal 2: Run pipeline
```

**Expected Output:**
- ✅ 500 driver profiles generated
- ✅ Real-time order streaming to Kafka
- ✅ GPS events and weather data ingestion
- ✅ Full pipeline execution (Bronze → Gold → ML)
- ✅ Data quality validation and ML model training

---

## 📋 Table of Contents

1. [Overview & Architecture](#overview--architecture)
2. [Installation & Setup](#installation--setup)
3. [All Available Commands](#all-available-commands)
4. [Usage Scenarios](#usage-scenarios)
5. [Data Generation & Rates](#data-generation--rates)
6. [Configuration](#configuration)
7. [Troubleshooting](#troubleshooting)
8. [Project Structure](#project-structure)

---

## Overview & Architecture

### What is this system?

This is a **production-ready streaming analytics platform** that:
- 🔄 **Generates realistic logistics data** - 500 drivers, 100K+ orders, millions of GPS events
- 📨 **Streams data via Kafka** - Real-time event-driven architecture
- 💾 **Stores in MinIO** - S3-compatible object storage with Bronze/Silver/Gold/Features layers
- 🔍 **Transforms with DuckDB** - OLAP queries for data quality and feature engineering
- 🤖 **Trains ML models** - XGBoost predictions with MLflow experiment tracking
- 📊 **Validates quality** - Automated data quality checks and reporting

### System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                  DATA PRODUCERS                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │   Drivers    │  │   Orders     │  │  GPS Events  │  Weather  │
│  │   (500x)     │  │   (100K+)    │  │  (2-5M+)     │           │
│  └──────────────┘  └──────────────┘  └──────────────┘           │
└────────────┬────────────────────────────────────────────────────┘
             │
             v
┌─────────────────────────────────────────────────────────────────┐
│                    KAFKA (Message Broker)                        │
│  Topics: drivers | orders | gps_events | weather_events         │
└────────────┬────────────────────────────────────────────────────┘
             │
             v
┌─────────────────────────────────────────────────────────────────┐
│                  DATA TRANSFORMATION                             │
│  Bronze      → Silver       → Gold        → Features             │
│  (Raw Data)    (Cleaned)      (Aggregated) (ML-ready)            │
│                                                                   │
│  MinIO: logistics-bronze / logistics-silver / logistics-gold     │
│  DuckDB: ./data/logistics.duckdb                                │
└────────────┬────────────────────────────────────────────────────┘
             │
             v
┌─────────────────────────────────────────────────────────────────┐
│              DATA QUALITY & ML TRAINING                          │
│  ✓ Schema validation        ✓ Data profiling                     │
│  ✓ Completeness checks      ✓ XGBoost model training             │
│  ✓ Quality reports (JSON)   ✓ MLflow experiment tracking         │
└─────────────────────────────────────────────────────────────────┘
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Message Broker** | Apache Kafka 7.5.0 | Real-time event streaming |
| **Object Storage** | MinIO (S3-compatible) | Bronze/Silver/Gold data layers |
| **Analytics DB** | DuckDB (OLAP) | SQL transformations & aggregations |
| **ML Ops** | MLflow 2.10.0 | Experiment tracking & model registry |
| **ML Framework** | scikit-learn, XGBoost | Predictive models |
| **Orchestration** | Python scripts + Bash | Data pipeline automation |
| **Python Version** | 3.12 | Data science & engineering |

---

## Installation & Setup

### Prerequisites

- **OS**: Linux/macOS (Windows with WSL2)
- **Docker**: 20.10+ with Docker Compose
- **Python**: 3.12+
- **Disk Space**: 5GB minimum
- **Memory**: 8GB RAM recommended

### Step 1: Clone & Navigate

```bash
cd /home/ahamed/Projects/DE/ai-logistics-platform
```

### Step 2: Initial Setup (First Time Only)

```bash
./run.sh setup

# This will:
# ✓ Create Python virtual environment
# ✓ Install all dependencies (kafka-python, pandas, duckdb, minio, mlflow, sklearn, xgboost)
# ✓ Create data directories
# ✓ Validate Python version
```

### Step 3: Start Docker Services

```bash
./run.sh docker-up

# This will start 5 containers:
# ✓ Zookeeper (Kafka coordination)
# ✓ Kafka (Message broker on port 9092)
# ✓ PostgreSQL (Metadata - port 5432)
# ✓ MinIO (S3 storage on port 9001)
# ✓ MLflow (ML tracking on port 5000)
```

### Step 4: Verify Installation

```bash
./run.sh status

# Expected output:
# Docker Services:
# ✓ logistics-zookeeper (UP)
# ✓ logistics-kafka (HEALTHY)
# ✓ logistics-postgres (HEALTHY)
# ✓ logistics-minio (UP)
# ✓ logistics-mlflow (UP)
```

✅ **You're ready!** Proceed to [Usage Scenarios](#usage-scenarios)

---

## All Available Commands

### Quick Reference Table

| Command | Purpose | Time | Terminal |
|---------|---------|------|----------|
| `./run.sh help` | List all commands | 1 sec | Any |
| `./run.sh setup` | First-time setup | 2 min | Any |
| `./run.sh docker-up` | Start services | 30 sec | Any |
| `./run.sh docker-down` | Stop services | 10 sec | Any |
| `./run.sh docker-clean` | Delete all data | 5 sec | Any |
| `./run.sh status` | Check health | 5 sec | Any |
| `./run.sh producer` | Stream data (foreground) | ∞ | Dedicated |
| `./run.sh producer-bg` | Stream data (background) | Instant | Any |
| `./run.sh runner` | Run pipeline once | 3 min | Any |
| `./run.sh runner-continuous` | Run every 5 min | ∞ | Dedicated |
| `./run.sh runner-custom 600` | Run every N seconds | ∞ | Dedicated |
| `./run.sh runner-bg 600` | Run in background | Instant | Any |
| `./run.sh monitor-kafka` | Watch Kafka messages | ∞ | Dedicated |
| `./run.sh logs-producer` | View producer logs | 1 sec | Any |
| `./run.sh logs-runner` | View runner logs | 1 sec | Any |
| `./run.sh logs-all` | View all logs | 1 sec | Any |
| `./run.sh web-urls` | Show dashboard URLs | 1 sec | Any |
| `./run.sh full-demo` | 5-minute demo | 5 min | Any |
| `./run.sh dev-mode` | Development setup | 3 min | Any |
| `./run.sh test-integration` | Run tests | 2 min | Any |
| `./run.sh cleanup` | Stop processes | 5 sec | Any |
| `./run.sh reset-duckdb` | Reset database | 10 sec | Any |
| `./run.sh reset-all` | Full factory reset | 1 min | Any |

### Setup & Infrastructure

```bash
# One-time setup (creates venv, installs dependencies)
./run.sh setup

# Start all Docker containers
./run.sh docker-up

# Stop containers (preserves data)
./run.sh docker-down

# Delete all data (containers, volumes, Kafka messages)
./run.sh docker-clean

# Check status of services
./run.sh status
```

### Data Producer

**Generates and streams realistic logistics data to Kafka**

```bash
# Run in foreground (see real-time output)
./run.sh producer
# Output:
#   ✓ Connected to Kafka bootstrap-servers=localhost:9092
#   → Generating 500 driver profiles...
#   → Producing 100 orders per batch
#   → Generating 2500 GPS events per batch
#   → Every message: orders=100, gps=2500 (Ctrl+C to stop)

# Run in background (continue working, logs to logs/producer.log)
./run.sh producer-bg
```

**What it produces:**
| Topic | Count | Rate | Duration |
|-------|-------|------|----------|
| drivers | 500 | Once | ~1 sec |
| orders | 100/batch | ~1 per sec | Continuous |
| gps_events | ~2500/batch | ~50/sec | Per batch |
| weather_events | ~5/day | ~1 per 5 hrs | Continuous |

### Pipeline Runner

**Executes full data transformation pipeline**

```bash
# Run once (transforms all current data)
./run.sh runner
# Duration: 2-3 minutes
# Executes: Bronze → Silver → Gold → Features → DQ → ML

# Run every 5 minutes (default)
./run.sh runner-continuous
# Stop with: Ctrl+C or pkill -f "python3 runner.py"

# Run every N seconds (e.g., 600 = 10 minutes)
./run.sh runner-custom 600
./run.sh runner-custom 120  # Every 2 minutes (testing)
./run.sh runner-custom 1800 # Every 30 minutes (production)

# Run in background with custom interval
./run.sh runner-bg 600
# Logs to: logs/runner.log
# Stop with: pkill -f "python3 runner.py"
```

### Monitoring

```bash
# Overall system status
./run.sh status

# Watch Kafka messages (real-time from all topics)
./run.sh monitor-kafka

# View logs
./run.sh logs-producer      # Producer output
./run.sh logs-runner        # Pipeline output
./run.sh logs-all          # Both logs

# Show dashboard URLs
./run.sh web-urls
# MLflow:    http://localhost:5000  (ML experiments)
# MinIO:     http://localhost:9001  (Data storage)
```

### Workflows

```bash
# 5-minute complete demo
./run.sh full-demo

# Development mode (everything with monitoring)
./run.sh dev-mode

# Run integration tests
./run.sh test-integration
```

### Cleanup

```bash
# Stop running producer/runner
./run.sh cleanup

# Reset DuckDB database
./run.sh reset-duckdb

# Complete reset (everything including Docker volumes)
./run.sh reset-all

# Help
./run.sh help
```

---

## Usage Scenarios

### Scenario 1: Quick 5-Minute Demo

Perfect for seeing everything working end-to-end.

```bash
./run.sh full-demo

# Automatically:
# 1. Starts Docker services
# 2. Starts data producer
# 3. Runs full pipeline
# 4. Shows results
# 5. Stops producer
# Total time: 5 minutes
```

### Scenario 2: Interactive Development

Monitor all components in real-time across terminals.

```bash
# Terminal 1: Start everything
./run.sh dev-mode
# Logs from all processes

# Terminal 2: Monitor Kafka
./run.sh monitor-kafka
# Watch messages flowing in real-time

# Terminal 3: Watch status
watch "./run.sh status"
# Refreshes every 2 seconds

# Terminal 4: View pipeline results
tail -f logs/runner.log
```

### Scenario 3: Manual Step-by-Step Control

Complete control over each component.

```bash
# Terminal 1: Start Docker
./run.sh docker-up

# Terminal 2: Start data producer (foreground)
./run.sh producer
# Watch driver IDs, orders, GPS events being generated

# Terminal 3: While producer is running, run pipeline
./run.sh runner

# Terminal 4: Monitor Kafka
./run.sh monitor-kafka

# Terminal 5: Check status
./run.sh status
```

### Scenario 4: Continuous Production Mode (24/7)

Run everything in background continuously.

```bash
# Start producer in background
nohup ./run.sh producer-bg &

# Start pipeline every 5 minutes in background
nohup ./run.sh runner-continuous &

# Periodically check status
./run.sh status

# View logs (last 20 lines)
./run.sh logs-runner | tail -20

# Stop everything
pkill -f "python3 streaming/producer.py"
pkill -f "python3 runner.py"
```

### Scenario 5: Custom Intervals

Test different pipeline frequencies.

```bash
# Every 2 minutes (for testing/development)
./run.sh docker-up
./run.sh producer-bg
./run.sh runner-custom 120

# Every 30 minutes (production-like)
./run.sh runner-custom 1800

# View metrics
./run.sh status
tail -f logs/runner.log
```

### Scenario 6: ML Experiment Comparison

Run multiple pipelines and compare ML models.

```bash
./run.sh docker-up
./run.sh producer-bg

# Run multiple times to generate different experiments
./run.sh runner
sleep 60
./run.sh runner
sleep 60
./run.sh runner

# Compare in MLflow UI
# Open: http://localhost:5000
# Select experiments to compare metrics and models
```

### Scenario 7: Troubleshooting & Testing

Validate all components are working.

```bash
# Run integration tests
./run.sh test-integration

# Check individual services
./run.sh status

# View detailed logs
./run.sh logs-all

# Test Kafka
./run.sh monitor-kafka

# Test MinIO
# Open: http://localhost:9001 (user: minioadmin, pw: minioadmin)

# Test MLflow
# Open: http://localhost:5000
```

---

## Data Generation & Rates

### Data Model

**Drivers** (500 total, generated once)
- Driver ID, name, vehicle type, capacity
- Performance metrics (success rate, avg rating)

**Orders** (100+ per minute)
- Order ID, customer, delivery address
- Weight, pickup/delivery time
- Status (pending → assigned → started → completed)

**GPS Events** (50+ per second per delivery)
- Order ID, driver ID, latitude, longitude, timestamp
- Altitude, speed, accuracy

**Weather Events** (~5 per day)
- Location, temperature, humidity, precipitation
- Condition (clear, rain, snow, fog)

### Data Rates

| Stream | Volume | Rate | Frequency | Duration |
|--------|--------|------|-----------|----------|
| **drivers** | 500 | Once | On startup | ~1 sec |
| **orders** | 100/batch | ~1 order/sec | Continuous | ∞ |
| **gps_events** | 2,500/batch | ~50 events/sec | Per order batch | ∞ |
| **weather_events** | ~5/day | ~1 per 5 hours | Continuous | ∞ |

### Pipeline Processing

| Phase | Input | Output | Time |
|-------|-------|--------|------|
| **Bronze** | Kafka topics | Raw data in MinIO | 20 sec |
| **Silver** | Bronze data | Deduplicated, cleaned | 30 sec |
| **Gold** | Silver data | Aggregated metrics | 40 sec |
| **Features** | Gold data | ML-ready features | 30 sec |
| **DQ** | All layers | Quality metrics | 20 sec |
| **ML** | Features | Trained model | 20 sec |
| **TOTAL** | — | — | **2-3 min** |

---

## Configuration

### Environment Variables

Override defaults by exporting variables before running:

```bash
# Data Scale
export NUM_DRIVERS=1000           # Default: 500
export NUM_DELIVERIES=500000      # Default: 100000
export SIM_YEARS=3                # Default: 2
export GPS_EVENTS_PER_DELIVERY=50 # Default: 25

# Streaming
export RANDOM_SEED=123            # Default: 42

# Then run
./run.sh runner
```

### Configuration File

Edit `config/settings.py` for persistent settings:

```python
# Data Simulation
NUM_DRIVERS = 500
NUM_DELIVERIES = 100000
SIM_YEARS = 2
GPS_EVENTS_PER_DELIVERY = 25

# Infrastructure
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
MINIO_ENDPOINT = "localhost:9000"
MINIO_ROOT_USER = "minioadmin"
MINIO_ROOT_PASSWORD = "minioadmin"
DUCKDB_PATH = "./data/logistics.duckdb"

# Logging
LOG_LEVEL = "INFO"  # or "DEBUG"
```

### Storage Locations

```
Data Storage:
├── Kafka topics (in-memory)
│   ├── drivers
│   ├── orders
│   ├── gps_events
│   └── weather_events
│
├── MinIO buckets (S3-compatible)
│   ├── logistics-bronze/          (Raw data)
│   ├── logistics-silver/          (Cleaned data)
│   ├── logistics-gold/            (Aggregated data)
│   ├── logistics-features/        (ML features)
│   └── logistics-ml/              (Models)
│
├── DuckDB local database
│   └── ./data/logistics.duckdb
│
└── Logs
    ├── logs/producer.log
    ├── logs/runner.log
    └── logs/dq_report.json
```

---

## Troubleshooting

### Producer Issues

**Problem: Producer won't start**
```
Error: ModuleNotFoundError: No module named 'kafka'
```
**Solution:**
```bash
./run.sh setup  # Install all dependencies
source venv/bin/activate
pip install kafka-python
```

**Problem: Connection refused to Kafka**
```
Error: [Errno 111] Connection refused
```
**Solution:**
```bash
# Check Docker is running
./run.sh status

# If not healthy, restart
./run.sh docker-down
./run.sh docker-up
./run.sh status
```

**Problem: No messages in Kafka (topics empty)**
```
Error: Kafka topics created but no data flowing
```
**Solution:**
```bash
# Check producer is running
pgrep -f "python3 streaming/producer.py"

# Check producer logs
./run.sh logs-producer | tail -50

# Start producer explicitly
./run.sh producer
```

### Pipeline Runner Issues

**Problem: Database is locked**
```
Error: database is locked
```
**Solution:**
```bash
# Kill any running runner
pkill -f "python3 runner.py"
sleep 2

# Reset database
./run.sh reset-duckdb

# Try again
./run.sh runner
```

**Problem: MinIO bucket not found**
```
Error: Bucket not found: logistics-bronze
```
**Solution:**
```bash
# Buckets are created automatically on first run
./run.sh runner

# Or manually create
docker exec logistics-minio mc mb minio/logistics-bronze
```

**Problem: Pipeline runs but no output files**
```
Error: No data in MinIO, DuckDB empty
```
**Solution:**
```bash
# Check producer is streaming data
./run.sh monitor-kafka

# Check logs
./run.sh logs-runner

# Ensure producer ran before pipeline
./run.sh producer-bg
sleep 5
./run.sh runner
```

**Problem: Out of memory errors**
```
Error: MemoryError or DuckDB memory exceeded
```
**Solution:**
```bash
# Reduce data scale
export NUM_DRIVERS=100
export NUM_DELIVERIES=10000

# Or increase DuckDB memory in config/settings.py
# DUCKDB_MEMORY_LIMIT = "8GB"

./run.sh runner
```

### Docker Issues

**Problem: Docker containers not starting**
```
Error: Cannot connect to Docker daemon
```
**Solution:**
```bash
# Start Docker service (macOS)
open -a Docker

# Or Linux
sudo systemctl start docker

# Verify
docker ps
```

**Problem: Port already in use**
```
Error: port 9092 is already allocated
```
**Solution:**
```bash
# Check what's using the port
lsof -i :9092
# or
netstat -tlnp | grep 9092

# Stop the process or use different port
# Edit docker-compose-streaming.yml and change ports
```

**Problem: Volumes mounting incorrectly**
```
Error: Cannot find data files, permission denied
```
**Solution:**
```bash
# Check Docker has permission to access paths
chmod 755 ./data ./logs

# Restart Docker
./run.sh docker-down
./run.sh docker-up
```

### Common Issues

| Issue | Check | Fix |
|-------|-------|-----|
| Nothing working | `./run.sh status` | `./run.sh docker-up` |
| No data flowing | `./run.sh monitor-kafka` | `./run.sh producer` |
| Pipeline fails | `cat logs/runner.log` | `./run.sh reset-duckdb` |
| Memory errors | Check DuckDB memory limit | Reduce data scale or increase RAM |
| Kafka timeout | Check network, Docker health | Restart services: `./run.sh docker-clean && ./run.sh docker-up` |

### Get Help

```bash
# Show all commands and help
./run.sh help

# View logs with context
./run.sh logs-all | head -100

# Check system health
./run.sh status

# Run tests
./run.sh test-integration
```

---

## Project Structure

```
ai-logistics-platform/
├── run.sh                          # Main command dispatcher (26 commands)
├── README.md                       # This file
├── QUICKSTART.sh                   # Copy-paste examples
├── RUN_COMMANDS.md                 # Detailed command reference
├── RUNTIME_GUIDE.md                # Getting started guide
├── QUICK_REFERENCE.md              # One-page cheat sheet
│
├── config/
│   ├── settings.py                 # Configuration values
│   └── __init__.py
│
├── streaming/
│   ├── producer.py                 # Kafka data producer
│   ├── consumer.py                 # Kafka → MinIO consumer
│   └── __init__.py
│
├── data_simulation/
│   ├── __init__.py                 # Generator exports
│   ├── driver_generator.py          # Driver profile generation
│   ├── order_generator.py           # Order generation
│   ├── gps_generator.py             # GPS event generation
│   └── weather_generator.py         # Weather event generation
│
├── pipeline/
│   ├── runner.py                   # Main pipeline orchestrator
│   ├── bronze.py                   # Kafka → MinIO ingestion
│   ├── silver.py                   # Data cleaning & deduplication
│   ├── gold.py                     # Data aggregation
│   ├── features.py                 # ML feature engineering
│   ├── quality.py                  # Data quality validation
│   └── ml_models.py                # ML model training
│
├── data/
│   ├── logistics.duckdb            # DuckDB database (created on first run)
│   └── [Order data files]
│
├── logs/
│   ├── producer.log                # Producer output
│   ├── runner.log                  # Pipeline output
│   └── dq_report.json              # Data quality report
│
├── docker-compose-streaming.yml    # Docker services composition
├── requirements.txt                # Python dependencies
└── .gitignore

├── venv/                           # Python virtual environment (created by setup)
└── [Python packages]
```

### Key Files

| File | Purpose |
|------|---------|
| `run.sh` | Single entry point for all operations (26 commands) |
| `streaming/producer.py` | Generates realistic logistics data and streams to Kafka |
| `streaming/consumer.py` | Consumes Kafka messages and writes to MinIO |
| `pipeline/runner.py` | Orchestrates all transformation phases |
| `data_simulation/` | Data generators (drivers, orders, GPS, weather) |
| `pipeline/` | Transformation phases (Bronze → Gold → ML) |
| `config/settings.py` | Configuration for data scale, infrastructure, logging |
| `docker-compose-streaming.yml` | Docker services (Kafka, MinIO, MLflow, Zookeeper, PostgreSQL) |

---

## Next Steps

### For New Users

1. **Installation** (5 minutes)
   ```bash
   ./run.sh setup
   ./run.sh docker-up
   ```

2. **Quick Demo** (5 minutes)
   ```bash
   ./run.sh full-demo
   ```

3. **Explore Data** (5 minutes)
   - MinIO: http://localhost:9001
   - MLflow: http://localhost:5000
   - Data files: `logs/dq_report.json`

4. **Run Continuously** (ongoing)
   ```bash
   ./run.sh producer-bg
   ./run.sh runner-continuous
   ```

### For Developers

1. **Understand Architecture**
   - Read: `RUNTIME_GUIDE.md`
   - Run: `./run.sh dev-mode`

2. **Modify Generators**
   - Edit: `data_simulation/`
   - Test: `./run.sh producer` (watch output)

3. **Enhance Pipeline**
   - Edit: `pipeline/` phase files
   - Test: `./run.sh runner` (check logs)

4. **Configure for Your Data**
   - Edit: `config/settings.py`
   - Export: Environment variables
   - Run: Custom intervals with `./run.sh runner-custom N`

### For Operators

1. **Monitor System**
   ```bash
   ./run.sh status              # Health check
   ./run.sh monitor-kafka       # Watch data flow
   tail -f logs/runner.log      # Pipeline progress
   ```

2. **Adjust Frequency**
   ```bash
   ./run.sh runner-custom 1800  # Every 30 minutes
   ./run.sh runner-custom 600   # Every 10 minutes
   ```

3. **Scale Data**
   ```bash
   # Edit config/settings.py to increase NUM_DRIVERS, NUM_DELIVERIES
   # Or export environment variables before running
   ./run.sh runner
   ```

4. **Troubleshoot Issues**
   ```bash
   ./run.sh status
   ./run.sh logs-all
   ./run.sh test-integration
   ```

---

## Support & Resources

### Quick Links

- **Documentation**: See [All Available Commands](#all-available-commands) section
- **Examples**: See [Usage Scenarios](#usage-scenarios) section
- **Troubleshooting**: See [Troubleshooting](#troubleshooting) section
- **Configuration**: See [Configuration](#configuration) section

### Help Commands

```bash
# List all commands
./run.sh help

# View specific command help
./run.sh help | grep runner

# Check system status
./run.sh status

# Run tests
./run.sh test-integration

# View all logs
./run.sh logs-all
```

### Useful URLs

Once system is running:

- **MLflow Experiments**: http://localhost:5000
- **MinIO Storage**: http://localhost:9001 (credentials: minioadmin/minioadmin)
- **Data Quality Report**: `cat logs/dq_report.json`

---

## Performance Expectations

### Timing

| Operation | Duration | Notes |
|-----------|----------|-------|
| Initial Setup | 2 minutes | One-time only |
| Docker startup | 30 seconds | Per session |
| Producer (500 drivers) | ~1 second | Drivers created once |
| Full pipeline run | 2-3 minutes | All transformation phases |
| Continuous runs | Every 5 min | Configurable |

### Resource Usage

| Component | CPU | Memory | Disk |
|-----------|-----|--------|------|
| Kafka | 5-10% | 512 MB | 1 GB (with data) |
| Producer | 10-20% | 256 MB | — |
| Pipeline | 20-30% | 2-4 GB | 1-2 GB (data storage) |
| MLflow | 1-5% | 512 MB | 500 MB |
| Total System | 30-50% | 4-6 GB | 3-5 GB |

### Data Volume

| Scenario | Drivers | Orders | GPS Events | ML Features | Disk |
|----------|---------|--------|-----------|-------------|------|
| Demo | 500 | 10K | 200K | 10K | 500 MB |
| Standard | 500 | 100K | 2M | 100K | 2 GB |
| Large | 1000 | 500K | 10M | 500K | 5 GB |
| XL | 5000 | 2M | 50M | 2M | 20 GB |

---

## Known Limitations

### Current Scope

- **Single-node system**: Designed for up to 10M records (not petabyte-scale)
- **DuckDB local**: In-process OLAP database (4GB memory limit)
- **Default ML models**: Hyperparameters not tuned for production
- **Weather data**: Sparse (by design: ~5 events per day)
- **No real-time serving**: Models trained offline

### Future Enhancements

- [ ] Distributed Kafka cluster configuration
- [ ] Snowflake/BigQuery backend
- [ ] Real-time model serving (FastAPI)
- [ ] Feature store integration (Feast)
- [ ] Advanced schema validation (Great Expectations)
- [ ] Data lineage tracking (OpenLineage)
- [ ] Kubernetes deployment
- [ ] CI/CD integration (GitHub Actions)

---

## License & Credits

**Project**: AI Logistics Platform  
**Status**: Production-ready for development and testing  
**Maintainer**: Engineering Team  

---

## Getting Started Now

Ready to run? Execute this command to get started:

```bash
./run.sh full-demo
```

This single command will:
1. ✅ Start Docker services
2. ✅ Generate 500 drivers
3. ✅ Stream orders and GPS events
4. ✅ Run complete transformation pipeline
5. ✅ Validate data quality
6. ✅ Train ML models
7. ✅ Display results

**Total time: 5 minutes**

For more detailed information, see [All Available Commands](#all-available-commands) or run:

```bash
./run.sh help
```

---

Made with ❤️ for real-time data engineering
