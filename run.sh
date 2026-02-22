#!/bin/bash
#
# AI Logistics Platform - Streaming Data Pipeline
# Comprehensive run script with multiple configurations
#
# Usage:
#   ./run.sh setup              # One-time environment setup
#   ./run.sh docker-up          # Start Docker services
#   ./run.sh docker-down        # Stop Docker services
#   ./run.sh producer           # Start producer (continuous)
#   ./run.sh runner             # Run pipeline once
#   ./run.sh runner-continuous  # Run pipeline continuously (5 min intervals)
#   ./run.sh runner-custom 300  # Run pipeline with custom interval (seconds)
#   ./run.sh monitor            # Monitor Kafka topics
#   ./run.sh full-demo          # Complete demo (docker + producer + runner)
#   ./run.sh cleanup            # Stop all processes
#   ./run.sh logs               # View logs
#   ./run.sh help               # Show all options
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${PROJECT_DIR}/venv"
LOG_DIR="${PROJECT_DIR}/logs"
PRODUCER_LOG="${LOG_DIR}/producer.log"
RUNNER_LOG="${LOG_DIR}/runner.log"

# Create logs directory
mkdir -p "${LOG_DIR}"

# Functions
print_header() {
    echo -e "\n${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}→ $1${NC}"
}

activate_venv() {
    if [ ! -d "${VENV_DIR}" ]; then
        print_error "Virtual environment not found. Run './run.sh setup' first."
        exit 1
    fi
    source "${VENV_DIR}/bin/activate"
    print_success "Virtual environment activated"
}

# Commands
cmd_help() {
    cat << 'EOF'

═══════════════════════════════════════════════════════════════════════════════
  AI LOGISTICS PLATFORM - RUN COMMAND REFERENCE
═══════════════════════════════════════════════════════════════════════════════

SETUP & INFRASTRUCTURE
──────────────────────────────────────────────────────────────────────────────

  ./run.sh setup
    • Create Python 3.12 virtual environment
    • Install all dependencies from requirements.txt
    • Verify installations
    • One-time setup only

  ./run.sh docker-up
    • Start Docker containers (Zookeeper, Kafka, PostgreSQL, MinIO, MLflow)
    • Verify all services are healthy
    • Initialize MinIO buckets

  ./run.sh docker-down
    • Stop all Docker containers gracefully
    • Preserve data volumes
    • Can be restarted with 'docker-up'

  ./run.sh docker-clean
    • Stop and remove all containers and volumes
    • WARNING: Deletes all Kafka messages and MinIO data
    • Use only for complete reset


DATA PRODUCTION (Kafka Streaming)
──────────────────────────────────────────────────────────────────────────────

  ./run.sh producer
    • Start Kafka data producer
    • Generates driver profiles (one-time: 500 records)
    • Continuously streams orders (~2 orders/sec)
    • Continuously streams GPS events (~50 events/sec/delivery)
    • Continuously streams weather events
    • Runs indefinitely until Ctrl+C
    • Logs to: logs/producer.log
    
    Example:
      $ ./run.sh producer
      → Generates 500 driver profiles
      → Batch 1: Streaming 100 orders + 2500 GPS events...
      → Batch 2: Streaming 100 orders + 2500 GPS events...
      (Ctrl+C to stop)

  ./run.sh producer-bg
    • Start producer in background
    • Returns immediately, logs to logs/producer.log
    • Use with runner commands for full pipeline testing
    • Kill with: pkill -f "python3 streaming/producer.py"
    
    Example:
      $ ./run.sh producer-bg
      $ ./run.sh runner
      $ pkill -f "python3 streaming/producer.py"


DATA TRANSFORMATION (Pipeline Runner)
──────────────────────────────────────────────────────────────────────────────

  ./run.sh runner
    • Execute full pipeline ONCE
    • Phases: Bronze → Silver → Gold → Features → DQ → ML
    • Duration: ~2-3 minutes
    • Logs to: logs/runner.log
    • Creates data quality report: logs/dq_report.json
    
    Example:
      $ ./run.sh runner
      Pipeline started: 2026-02-21T22:47:22
      [Bronze Ingestion] Consuming 4 Kafka topics...
      [Silver Transformation] Deduplicating & cleaning...
      [Gold Aggregation] Business metrics...
      [Feature Engineering] ML-ready features...
      [Data Quality] Validation...
      [ML Training] RandomForest, XGBoost, IsolationForest...
      Pipeline completed in 2m 45s

  ./run.sh runner-continuous
    • Execute pipeline CONTINUOUSLY with 5-minute intervals
    • Suitable for production monitoring
    • Logs to: logs/runner.log (appended)
    • Keep running 24/7 with: nohup ./run.sh runner-continuous &
    • Kill with: pkill -f "python3 runner.py"
    
    Example:
      $ ./run.sh runner-continuous
      [Pipeline Run 1] Started at 22:47:22, completed in 2m 45s
      [Pipeline Run 2] Started at 22:52:22, completed in 2m 42s
      [Pipeline Run 3] Started at 22:57:22, completed in 2m 48s
      (Ctrl+C to stop)

  ./run.sh runner-custom <seconds>
    • Execute pipeline continuously with CUSTOM interval
    • Example: ./run.sh runner-custom 600  (10 min intervals)
    • Example: ./run.sh runner-custom 120  (2 min intervals)
    • Example: ./run.sh runner-custom 1800 (30 min intervals)
    
    Example:
      $ ./run.sh runner-custom 600
      [Pipeline Run 1] Next run in 600 seconds (10.0 minutes)...
      [Pipeline Run 2] Next run in 600 seconds (10.0 minutes)...

  ./run.sh runner-bg <seconds>
    • Run pipeline in background with custom interval
    • Default interval: 300 seconds (5 minutes)
    • Example: ./run.sh runner-bg 600
    • Kill with: pkill -f "python3 runner.py"
    
    Example:
      $ ./run.sh runner-bg 300
      Runner started in background (PID: 12345)


MONITORING & DEBUGGING
──────────────────────────────────────────────────────────────────────────────

  ./run.sh monitor-kafka
    • Watch Kafka messages in real-time
    • Reads from 'drivers' topic
    • Shows newest messages
    
    Example:
      $ ./run.sh monitor-kafka
      {"driver_id": "DRV_000001", "vehicle_type": "bike", ...}
      {"driver_id": "DRV_000002", "vehicle_type": "van", ...}

  ./run.sh logs-producer
    • View producer logs
    • Shows: Kafka connections, data generation, stream stats
    
    Example:
      $ ./run.sh logs-producer
      2026-02-21 22:44:56 - Connected to Kafka: localhost:9092
      2026-02-21 22:44:56 - Generating driver profiles...
      2026-02-21 22:44:57 - Produced 500 driver records

  ./run.sh logs-runner
    • View runner logs
    • Shows: All pipeline phases, execution times, errors
    
    Example:
      $ ./run.sh logs-runner
      2026-02-21 22:47:22 - BRONZE INGESTION
      2026-02-21 22:47:45 - Consumed drivers: 500 records
      2026-02-21 22:48:25 - SILVER TRANSFORMATION

  ./run.sh logs-all
    • View combined producer and runner logs

  ./run.sh status
    • Show status of all components
    • Docker containers status
    • Running processes (producer, runner)
    • Kafka topics and message counts
    • MinIO buckets
    
    Example:
      $ ./run.sh status
      
      Docker Services:
      ✓ logistics-zookeeper   (UP)
      ✓ logistics-kafka       (HEALTHY)
      ✓ logistics-postgres    (HEALTHY)
      ✓ logistics-minio       (UP)
      ✓ logistics-mlflow      (UP)
      
      Running Processes:
      → Producer: PID 12345 (running since 22:44)
      → Runner: Not running
      
      Kafka Topics:
      → drivers: 500 messages
      → orders: 1250 messages
      → gps_events: 31000 messages
      → weather_events: 0 messages

  ./run.sh web-urls
    • Show URLs for web dashboards
    
    Example:
      $ ./run.sh web-urls
      
      MLflow Experiments:  http://localhost:5000
      MinIO Console:       http://localhost:9001
      Kafka UI (optional): http://localhost:8080


COMPLETE WORKFLOWS
──────────────────────────────────────────────────────────────────────────────

  ./run.sh full-demo
    • One-command complete demo
    • Steps:
      1. Start Docker services
      2. Wait for Kafka to be ready
      3. Start producer in background
      4. Run pipeline once
      5. Show results
      6. Stop producer
      7. Display logs and statistics
    
    Example:
      $ ./run.sh full-demo
      [1/7] Starting Docker services...
      [2/7] Waiting for Kafka (30 sec)...
      [3/7] Starting producer...
      [4/7] Running pipeline...
      [5/7] Pipeline completed!
      [6/7] Results summary...
      [7/7] Demo complete!

  ./run.sh dev-mode
    • Developer workflow: docker + producer + runner + logs
    • Starts everything and watches logs
    • Keeps all processes running for manual testing
    
    Example:
      $ ./run.sh dev-mode
      [Setup] Starting Docker...
      [Setup] Starting producer...
      [Setup] Running first pipeline...
      [Watch] Tailing logs... (Ctrl+C to exit)

  ./run.sh test-integration
    • Run integration tests
    • Validates data flow end-to-end
    • Checks Kafka connectivity
    • Verifies MinIO storage
    • Validates DuckDB transformations


CLEANUP & RESET
──────────────────────────────────────────────────────────────────────────────

  ./run.sh cleanup
    • Kill all running processes (producer, runner)
    • Does NOT stop Docker
    • Keeps data intact

  ./run.sh docker-clean
    • Full reset: Stop Docker and remove volumes
    • WARNING: Deletes all data
    • Start fresh with: ./run.sh docker-up

  ./run.sh reset-all
    • Complete reset: Stop everything, delete data
    • WARNING: Nuclear option
    • Then run: ./run.sh full-demo

  ./run.sh reset-duckdb
    • Delete DuckDB database (./data/logistics.duckdb)
    • Next pipeline run will recreate from scratch


CONFIGURATION & ENVIRONMENT
──────────────────────────────────────────────────────────────────────────────

  Environment variables (in config/settings.py or .env):
  
  NUM_DRIVERS=500                 # Driver profiles to generate
  NUM_DELIVERIES=100000           # Orders to simulate (historical)
  SIM_YEARS=2                     # Years of historical data
  GPS_EVENTS_PER_DELIVERY=25      # GPS tracking points per order
  RANDOM_SEED=42                  # Reproducibility seed
  
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  MINIO_ENDPOINT=localhost:9000
  MLFLOW_TRACKING_URI=http://localhost:5000
  DUCKDB_PATH=./data/logistics.duckdb
  
  ENVIRONMENT=development         # or 'production'
  LOG_LEVEL=INFO                  # DEBUG, INFO, WARNING, ERROR
  DEBUG=false


EXAMPLE WORKFLOWS
──────────────────────────────────────────────────────────────────────────────

1. QUICK DEMO (5 minutes)
   ────────────────────────────────────────────────────────────
   $ ./run.sh docker-up
   $ ./run.sh producer-bg      # Start in background
   $ sleep 10                   # Let producer generate data
   $ ./run.sh runner            # Run full pipeline
   $ ./run.sh logs-runner       # See results
   $ pkill -f "python3 streaming/producer.py"  # Stop producer

2. DEVELOPMENT MODE
   ────────────────────────────────────────────────────────────
   $ ./run.sh dev-mode          # Starts everything
   # (in another terminal)
   $ ./run.sh logs-all          # Watch logs
   $ ./run.sh monitor-kafka     # Monitor Kafka
   $ ./run.sh status            # Check status

3. PRODUCTION MODE (24/7)
   ────────────────────────────────────────────────────────────
   $ nohup ./run.sh producer-bg > /tmp/producer.out 2>&1 &
   $ nohup ./run.sh runner-continuous > /tmp/runner.out 2>&1 &
   # Monitor with:
   $ ./run.sh status
   $ tail -f /tmp/runner.out

4. TESTING & VALIDATION
   ────────────────────────────────────────────────────────────
   $ ./run.sh setup
   $ ./run.sh docker-up
   $ ./run.sh test-integration  # Full validation
   $ ./run.sh docker-clean

5. CUSTOM PIPELINE INTERVAL
   ────────────────────────────────────────────────────────────
   $ ./run.sh docker-up
   $ ./run.sh producer-bg
   $ ./run.sh runner-custom 600  # Run every 10 minutes
   # Stop with Ctrl+C


DATA FLOW DIAGRAM
──────────────────────────────────────────────────────────────────────────────

Producer → Kafka Topics      Consumer → MinIO Bronze    Runner → Transforms
(streaming) →                (ingestion) →              (pipeline) →
├─ drivers                  ├─ logistics-bronze/       ├─ Silver (dedup)
├─ orders                   │  drivers/                ├─ Gold (metrics)
├─ gps_events               │  orders/                 ├─ Features (ML)
└─ weather_events           │  gps_events/             ├─ DuckDB (local)
                            │  weather_events/         └─ MLflow (tracking)
                            └─ Y/M/D partitions


TROUBLESHOOTING
──────────────────────────────────────────────────────────────────────────────

Q: Producer won't start
A: ./run.sh setup && ./run.sh docker-up

Q: Kafka connection error
A: docker-compose -f docker-compose-streaming.yml logs kafka

Q: DuckDB locked error
A: pkill -f runner.py && sleep 2 && ./run.sh runner

Q: MinIO bucket not created
A: First pipeline run automatically creates buckets

Q: How to see detailed logs?
A: tail -f logs/runner.log

Q: How to reset everything?
A: ./run.sh reset-all && ./run.sh full-demo

═══════════════════════════════════════════════════════════════════════════════

EOF
}

cmd_setup() {
    print_header "SETUP: Creating Python Environment"
    
    if [ -d "${VENV_DIR}" ]; then
        print_info "Virtual environment already exists at ${VENV_DIR}"
        read -p "Recreate? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            return
        fi
        rm -rf "${VENV_DIR}"
    fi
    
    print_info "Creating virtual environment..."
    python3 -m venv "${VENV_DIR}"
    print_success "Virtual environment created"
    
    print_info "Activating virtual environment..."
    source "${VENV_DIR}/bin/activate"
    
    print_info "Upgrading pip..."
    pip install --upgrade pip -q
    print_success "Pip upgraded"
    
    print_info "Installing dependencies from requirements.txt..."
    pip install -r requirements.txt -q
    print_success "Dependencies installed"
    
    print_info "Verifying installations..."
    python3 -c "import kafka; print('  ✓ kafka-python')" 2>/dev/null || echo "  ✗ kafka-python failed"
    python3 -c "import pandas; print('  ✓ pandas')" 2>/dev/null || echo "  ✗ pandas failed"
    python3 -c "import duckdb; print('  ✓ duckdb')" 2>/dev/null || echo "  ✗ duckdb failed"
    python3 -c "import minio; print('  ✓ minio')" 2>/dev/null || echo "  ✗ minio failed"
    python3 -c "import mlflow; print('  ✓ mlflow')" 2>/dev/null || echo "  ✗ mlflow failed"
    
    print_success "Setup complete! Virtual environment ready at ${VENV_DIR}"
}

cmd_docker_up() {
    print_header "DOCKER: Starting Services"
    
    print_info "Pulling latest images..."
    docker-compose -f docker-compose-streaming.yml pull -q
    
    print_info "Starting containers..."
    docker-compose -f docker-compose-streaming.yml up -d
    
    print_info "Waiting for services to be healthy (30 seconds)..."
    for i in {1..30}; do
        if docker-compose -f docker-compose-streaming.yml ps | grep -q "healthy\|Up"; then
            echo -ne "\r  Progress: ${i}/30 seconds"
        fi
        sleep 1
    done
    echo
    
    print_success "Docker services started!"
    
    echo -e "\n${BLUE}Service Status:${NC}"
    docker-compose -f docker-compose-streaming.yml ps
}

cmd_docker_down() {
    print_header "DOCKER: Stopping Services"
    
    print_info "Stopping containers..."
    docker-compose -f docker-compose-streaming.yml down
    
    print_success "Docker services stopped (data preserved)"
}

cmd_docker_clean() {
    print_header "DOCKER: Full Cleanup"
    
    print_error "WARNING: This will delete all Kafka messages and MinIO data!"
    read -p "Continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Cancelled"
        return
    fi
    
    print_info "Removing containers and volumes..."
    docker-compose -f docker-compose-streaming.yml down -v
    
    print_success "Complete cleanup done"
}

cmd_producer() {
    print_header "PRODUCER: Starting Kafka Data Stream"
    
    activate_venv
    
    print_info "Starting data producer..."
    print_info "This will stream data until you press Ctrl+C"
    
    cd "${PROJECT_DIR}"
    python3 streaming/producer.py
}

cmd_producer_bg() {
    print_header "PRODUCER: Starting in Background"
    
    activate_venv
    
    print_info "Starting producer in background..."
    cd "${PROJECT_DIR}"
    nohup python3 streaming/producer.py > "${PRODUCER_LOG}" 2>&1 &
    local pid=$!
    
    print_success "Producer started (PID: $pid)"
    print_info "Logs: ${PRODUCER_LOG}"
    print_info "To stop: pkill -f 'python3 streaming/producer.py'"
}

cmd_runner() {
    print_header "RUNNER: Executing Full Pipeline (Once)"
    
    activate_venv
    
    print_info "Executing pipeline phases:"
    print_info "  1. Bronze Ingestion (Kafka → MinIO)"
    print_info "  2. Silver Transformation (Dedup & Clean)"
    print_info "  3. Gold Aggregation (Metrics)"
    print_info "  4. Feature Engineering (ML Features)"
    print_info "  5. Data Quality Validation"
    print_info "  6. ML Model Training"
    
    cd "${PROJECT_DIR}"
    python3 runner.py 2>&1 | tee -a "${RUNNER_LOG}"
}

cmd_runner_continuous() {
    print_header "RUNNER: Continuous Mode (5 minute intervals)"
    
    activate_venv
    
    print_info "Running pipeline continuously..."
    print_info "Interval: 5 minutes (300 seconds)"
    print_info "Press Ctrl+C to stop"
    
    cd "${PROJECT_DIR}"
    python3 runner.py --continuous --interval 300 2>&1 | tee -a "${RUNNER_LOG}"
}

cmd_runner_custom() {
    local interval=${1:-300}
    
    if ! [[ "$interval" =~ ^[0-9]+$ ]]; then
        print_error "Invalid interval. Must be a number (seconds)"
        print_info "Example: ./run.sh runner-custom 600"
        exit 1
    fi
    
    print_header "RUNNER: Continuous Mode (Custom Interval)"
    
    activate_venv
    
    print_info "Running pipeline continuously..."
    print_info "Interval: ${interval} seconds ($(echo "scale=1; ${interval}/60" | bc) minutes)"
    print_info "Press Ctrl+C to stop"
    
    cd "${PROJECT_DIR}"
    python3 runner.py --continuous --interval ${interval} 2>&1 | tee -a "${RUNNER_LOG}"
}

cmd_runner_bg() {
    local interval=${1:-300}
    
    activate_venv
    
    print_header "RUNNER: Starting in Background"
    
    print_info "Starting runner in background..."
    print_info "Interval: ${interval} seconds"
    
    cd "${PROJECT_DIR}"
    nohup python3 runner.py --continuous --interval ${interval} > "${RUNNER_LOG}" 2>&1 &
    local pid=$!
    
    print_success "Runner started (PID: $pid)"
    print_info "Logs: ${RUNNER_LOG}"
    print_info "To stop: pkill -f 'python3 runner.py'"
}

cmd_monitor_kafka() {
    print_header "MONITOR: Kafka Messages"
    
    print_info "Watching 'drivers' topic (latest messages)..."
    print_info "Press Ctrl+C to stop"
    
    docker exec logistics-kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic drivers \
        --from-beginning \
        --max-messages 10
}

cmd_logs_producer() {
    print_header "LOGS: Producer"
    
    if [ ! -f "${PRODUCER_LOG}" ]; then
        print_error "Producer log not found at ${PRODUCER_LOG}"
        return
    fi
    
    tail -100 "${PRODUCER_LOG}"
}

cmd_logs_runner() {
    print_header "LOGS: Runner"
    
    if [ ! -f "${RUNNER_LOG}" ]; then
        print_error "Runner log not found at ${RUNNER_LOG}"
        return
    fi
    
    tail -100 "${RUNNER_LOG}"
}

cmd_logs_all() {
    print_header "LOGS: All"
    
    echo -e "${BLUE}=== Producer Logs ===${NC}"
    if [ -f "${PRODUCER_LOG}" ]; then
        tail -30 "${PRODUCER_LOG}"
    else
        print_info "No producer logs yet"
    fi
    
    echo -e "\n${BLUE}=== Runner Logs ===${NC}"
    if [ -f "${RUNNER_LOG}" ]; then
        tail -30 "${RUNNER_LOG}"
    else
        print_info "No runner logs yet"
    fi
}

cmd_status() {
    print_header "STATUS: System Overview"
    
    echo -e "${BLUE}Docker Services:${NC}"
    docker-compose -f docker-compose-streaming.yml ps 2>/dev/null || print_error "Docker not running"
    
    echo -e "\n${BLUE}Running Processes:${NC}"
    if pgrep -f "python3 streaming/producer.py" > /dev/null; then
        local pid=$(pgrep -f "python3 streaming/producer.py")
        print_success "Producer: PID $pid (running)"
    else
        print_info "Producer: Not running"
    fi
    
    if pgrep -f "python3 runner.py" > /dev/null; then
        local pid=$(pgrep -f "python3 runner.py")
        print_success "Runner: PID $pid (running)"
    else
        print_info "Runner: Not running"
    fi
    
    echo -e "\n${BLUE}Kafka Topics:${NC}"
    docker exec logistics-kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null || print_error "Kafka not accessible"
}

cmd_web_urls() {
    print_header "WEB DASHBOARDS"
    
    echo -e "${BLUE}Access URLs:${NC}"
    echo -e "  MLflow Experiments:  ${GREEN}http://localhost:5000${NC}"
    echo -e "  MinIO Console:       ${GREEN}http://localhost:9001${NC}"
    echo -e "  (MinIO API: localhost:9000)"
    
    echo -e "\n${BLUE}Credentials:${NC}"
    echo "  MinIO Username: minioadmin"
    echo "  MinIO Password: minioadmin"
}

cmd_full_demo() {
    print_header "FULL DEMO: Complete End-to-End Test"
    
    print_info "[1/7] Starting Docker services..."
    cmd_docker_up
    
    print_info "[2/7] Waiting for Kafka to be ready..."
    sleep 15
    
    print_info "[3/7] Starting producer in background..."
    cmd_producer_bg
    sleep 5
    
    print_info "[4/7] Running full pipeline..."
    cmd_runner
    
    print_info "[5/7] Displaying results..."
    sleep 2
    if [ -f "${LOG_DIR}/dq_report.json" ]; then
        echo -e "\n${BLUE}Data Quality Report:${NC}"
        cat "${LOG_DIR}/dq_report.json" | head -20
    fi
    
    print_info "[6/7] Stopping producer..."
    pkill -f "python3 streaming/producer.py" || true
    
    print_success "[7/7] Demo complete!"
    echo -e "\n${BLUE}Summary:${NC}"
    echo "  • Kafka topics created and populated with data"
    echo "  • Full pipeline executed: Bronze → Silver → Gold → Features → DQ → ML"
    echo "  • Results saved in logs/ and data/ directories"
    echo "  • View MLflow: http://localhost:5000"
}

cmd_dev_mode() {
    print_header "DEV MODE: Development Workflow"
    
    print_info "Starting Docker..."
    cmd_docker_up
    sleep 10
    
    print_info "Starting producer in background..."
    cmd_producer_bg
    sleep 5
    
    print_info "Running first pipeline..."
    cmd_runner
    
    print_info "Dev mode ready. Watching logs..."
    print_info "Open new terminals for:"
    print_info "  • Logs: tail -f logs/producer.log"
    print_info "  • Logs: tail -f logs/runner.log"
    print_info "  • Monitor: ./run.sh monitor-kafka"
    print_info "  • Dashboard: http://localhost:5000 (MLflow)"
    
    tail -f "${RUNNER_LOG}"
}

cmd_test_integration() {
    print_header "TEST: Integration Validation"
    
    activate_venv
    
    print_info "Running integration tests..."
    
    # Check Kafka
    print_info "  Testing Kafka connectivity..."
    docker exec logistics-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null && print_success "Kafka is accessible"
    
    # Check MinIO
    print_info "  Testing MinIO connectivity..."
    docker exec logistics-minio mc ls minio/logistics-bronze > /dev/null 2>&1 && print_success "MinIO is accessible"
    
    # Check DuckDB
    print_info "  Testing DuckDB..."
    python3 -c "import duckdb; db = duckdb.connect('./data/logistics.duckdb'); print('  ✓ DuckDB is working')"
    
    # Check Python imports
    print_info "  Testing Python imports..."
    python3 -c "from streaming.producer import main; print('  ✓ Producer module')"
    python3 -c "from streaming.consumer import LogisticsBronzeConsumer; print('  ✓ Consumer module')"
    python3 -c "from runner import main; print('  ✓ Runner module')"
    
    print_success "All integration tests passed!"
}

cmd_cleanup() {
    print_header "CLEANUP: Stopping All Processes"
    
    print_info "Stopping producer..."
    pkill -f "python3 streaming/producer.py" || print_info "Producer not running"
    
    print_info "Stopping runner..."
    pkill -f "python3 runner.py" || print_info "Runner not running"
    
    print_success "All processes stopped"
    print_info "Docker containers still running (use 'docker-down' to stop them)"
}

cmd_reset_all() {
    print_header "RESET: Complete System Reset"
    
    print_error "WARNING: This will delete ALL data and processes!"
    read -p "Continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Cancelled"
        return
    fi
    
    print_info "Stopping processes..."
    pkill -f "python3 streaming/producer.py" || true
    pkill -f "python3 runner.py" || true
    
    print_info "Stopping Docker..."
    docker-compose -f docker-compose-streaming.yml down -v 2>/dev/null || true
    
    print_info "Deleting data..."
    rm -rf ./data/logistics.duckdb || true
    rm -rf ./logs/* || true
    
    print_success "Reset complete!"
    print_info "To start fresh: ./run.sh setup && ./run.sh full-demo"
}

cmd_reset_duckdb() {
    print_header "RESET: DuckDB Database"
    
    local db_path="${PROJECT_DIR}/data/logistics.duckdb"
    
    if [ ! -f "${db_path}" ]; then
        print_info "Database not found, nothing to delete"
        return
    fi
    
    print_info "Deleting DuckDB database..."
    rm -f "${db_path}"
    
    print_success "DuckDB reset! (will be recreated on next pipeline run)"
}

# Main script logic
case "${1:-help}" in
    setup)
        cmd_setup
        ;;
    docker-up)
        cmd_docker_up
        ;;
    docker-down)
        cmd_docker_down
        ;;
    docker-clean)
        cmd_docker_clean
        ;;
    producer)
        cmd_producer
        ;;
    producer-bg)
        cmd_producer_bg
        ;;
    runner)
        cmd_runner
        ;;
    runner-continuous)
        cmd_runner_continuous
        ;;
    runner-custom)
        cmd_runner_custom "${2:-300}"
        ;;
    runner-bg)
        cmd_runner_bg "${2:-300}"
        ;;
    monitor-kafka)
        cmd_monitor_kafka
        ;;
    logs-producer)
        cmd_logs_producer
        ;;
    logs-runner)
        cmd_logs_runner
        ;;
    logs-all)
        cmd_logs_all
        ;;
    logs)
        cmd_logs_all
        ;;
    status)
        cmd_status
        ;;
    web-urls)
        cmd_web_urls
        ;;
    full-demo)
        cmd_full_demo
        ;;
    dev-mode)
        cmd_dev_mode
        ;;
    test-integration)
        cmd_test_integration
        ;;
    cleanup)
        cmd_cleanup
        ;;
    reset-all)
        cmd_reset_all
        ;;
    reset-duckdb)
        cmd_reset_duckdb
        ;;
    help)
        cmd_help
        ;;
    *)
        print_error "Unknown command: $1"
        echo -e "\nRun '${0} help' for usage information"
        exit 1
        ;;
esac
