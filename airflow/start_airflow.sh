#!/bin/bash
# Airflow Stack Controller Script
#
# Usage: bash airflow/start_airflow.sh [command]
#
# Commands:
#   up              Start all Airflow services
#   down            Stop all Airflow services
#   start           Start Airflow without logs
#   stop            Stop Airflow without removing containers
#   restart         Restart Airflow services
#   logs            Tail logs from all services
#   logs-webserver  Tail webserver logs
#   logs-scheduler  Tail scheduler logs
#   logs-worker     Tail worker logs
#   status          Show service status
#   reset           Reset database (DESTRUCTIVE)
#   init            Initialize Airflow
#   bash            Enter webserver bash shell
#   dags            List all DAGs
#   test-dag        Test a DAG
#   trigger         Manually trigger a DAG

set -e

PROJECT_ROOT="$(dirname "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")")"
COMPOSE_FILE="${PROJECT_ROOT}/docker-compose-airflow.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Helper functions
print_header() {
    echo -e "${BLUE}=====================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}=====================================================================${NC}"
}

print_status() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

# Check if Docker is running
check_docker() {
    if ! docker ps > /dev/null 2>&1; then
        print_error "Docker is not running!"
        exit 1
    fi
}

# Start Airflow
start_airflow() {
    print_header "Starting Airflow Stack"
    check_docker
    
    docker-compose -f "$COMPOSE_FILE" up -d
    
    print_status "Waiting for services to be healthy..."
    sleep 15
    
    print_status "Initializing Airflow..."
    docker-compose -f "$COMPOSE_FILE" exec -T webserver airflow db migrate
    docker-compose -f "$COMPOSE_FILE" exec -T webserver airflow users create \
        --username admin --password admin123 --firstname Admin \
        --lastname User --role Admin --email admin@localhost || true
    
    print_header "Airflow Stack Started!"
    print_info "Web UI:        http://localhost:8080 (admin/admin123)"
    print_info "Flower UI:     http://localhost:5555"
    print_info "MLflow UI:     http://localhost:5000"
    print_info "MinIO Console: http://localhost:9001"
    print_info "PostgreSQL:    localhost:5432 (airflow/airflow)"
    print_info "Redis:         localhost:6379"
}

# Stop Airflow
stop_airflow() {
    print_header "Stopping Airflow Stack"
    check_docker
    docker-compose -f "$COMPOSE_FILE" down
    print_status "Airflow stack stopped"
}

# Restart Airflow
restart_airflow() {
    print_header "Restarting Airflow Stack"
    stop_airflow
    sleep 5
    start_airflow
}

# Tail logs
tail_logs() {
    check_docker
    docker-compose -f "$COMPOSE_FILE" logs -f
}

# Tail webserver logs
tail_webserver_logs() {
    check_docker
    docker-compose -f "$COMPOSE_FILE" logs -f webserver
}

# Tail scheduler logs
tail_scheduler_logs() {
    check_docker
    docker-compose -f "$COMPOSE_FILE" logs -f scheduler
}

# Tail worker logs
tail_worker_logs() {
    check_docker
    docker-compose -f "$COMPOSE_FILE" logs -f worker
}

# Show status
show_status() {
    print_header "Airflow Services Status"
    check_docker
    docker-compose -f "$COMPOSE_FILE" ps
    echo ""
    print_info "Helpful URLs:"
    echo "  Web UI:        http://localhost:8080"
    echo "  Flower UI:     http://localhost:5555"
    echo "  MLflow UI:     http://localhost:5000"
    echo "  MinIO Console: http://localhost:9001"
}

# Reset database
reset_airflow() {
    print_header "Resetting Airflow Database (DESTRUCTIVE)"
    echo -e "${RED}WARNING: This will delete all Airflow metadata!${NC}"
    read -p "Are you sure? (type 'yes' to confirm): " confirm
    
    if [ "$confirm" = "yes" ]; then
        check_docker
        docker-compose -f "$COMPOSE_FILE" exec -T webserver airflow db reset --yes
        print_status "Database reset complete"
    else
        print_info "Reset cancelled"
    fi
}

# Initialize Airflow
init_airflow() {
    print_header "Initializing Airflow"
    check_docker
    bash "${PROJECT_ROOT}/scripts/init_airflow.sh"
}

# Enter bash shell
enter_bash() {
    check_docker
    docker-compose -f "$COMPOSE_FILE" exec webserver bash
}

# List DAGs
list_dags() {
    check_docker
    docker-compose -f "$COMPOSE_FILE" exec -T webserver airflow dags list
}

# Test DAG
test_dag() {
    if [ -z "$1" ]; then
        print_error "Usage: $0 test-dag <dag_id>"
        exit 1
    fi
    check_docker
    docker-compose -f "$COMPOSE_FILE" exec -T webserver airflow dags test "$1" "2026-02-21"
}

# Trigger DAG
trigger_dag() {
    if [ -z "$1" ]; then
        print_error "Usage: $0 trigger <dag_id>"
        exit 1
    fi
    check_docker
    docker-compose -f "$COMPOSE_FILE" exec -T webserver airflow dags trigger "$1"
    print_status "DAG '$1' triggered"
}

# Start without logs
start_background() {
    print_header "Starting Airflow Stack (Background)"
    check_docker
    docker-compose -f "$COMPOSE_FILE" up -d
    print_status "Airflow started in background"
    print_info "Use 'bash airflow/start_airflow.sh logs' to see logs"
}

# Stop without removing containers
stop_containers() {
    print_header "Stopping Airflow Services"
    check_docker
    docker-compose -f "$COMPOSE_FILE" stop
    print_status "Airflow services stopped"
}

# Main command handler
COMMAND="${1:-status}"

case "$COMMAND" in
    up|start)
        start_airflow
        ;;
    down|stop)
        stop_airflow
        ;;
    stop-only)
        stop_containers
        ;;
    start-bg)
        start_background
        ;;
    restart)
        restart_airflow
        ;;
    logs)
        tail_logs
        ;;
    logs-webserver)
        tail_webserver_logs
        ;;
    logs-scheduler)
        tail_scheduler_logs
        ;;
    logs-worker)
        tail_worker_logs
        ;;
    status)
        show_status
        ;;
    reset)
        reset_airflow
        ;;
    init)
        init_airflow
        ;;
    bash)
        enter_bash
        ;;
    dags|list-dags)
        list_dags
        ;;
    test-dag)
        test_dag "$2"
        ;;
    trigger)
        trigger_dag "$2"
        ;;
    *)
        echo -e "${BLUE}Airflow Stack Controller${NC}"
        echo ""
        echo "Usage: bash airflow/start_airflow.sh [command]"
        echo ""
        echo "Commands:"
        echo "  up                   Start all Airflow services"
        echo "  down                 Stop and remove containers"
        echo "  start                Start Airflow without logs"
        echo "  stop                 Stop without removing"
        echo "  restart              Restart services"
        echo "  logs                 Tail all logs"
        echo "  logs-webserver       Tail webserver logs"
        echo "  logs-scheduler       Tail scheduler logs"
        echo "  logs-worker          Tail worker logs"
        echo "  status               Show service status"
        echo "  reset                Reset database (DESTRUCTIVE)"
        echo "  init                 Initialize Airflow"
        echo "  bash                 Enter webserver shell"
        echo "  dags                 List all DAGs"
        echo "  test-dag <id>        Test a specific DAG"
        echo "  trigger <id>         Trigger DAG run"
        echo ""
        exit 1
        ;;
esac
