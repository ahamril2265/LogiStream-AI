#!/bin/bash
#
# QUICK START GUIDE - AI Logistics Platform
# Copy-paste ready commands for common scenarios
#

# ═════════════════════════════════════════════════════════════════════════════
# FIRST TIME SETUP
# ═════════════════════════════════════════════════════════════════════════════

# 1. Initialize environment (one-time only)
./run.sh setup

# 2. Start Docker services
./run.sh docker-up

# ═════════════════════════════════════════════════════════════════════════════
# QUICK DEMO (5 MINUTES) - Run all at once
# ═════════════════════════════════════════════════════════════════════════════

./run.sh full-demo

# What happens:
# 1. Starts Docker (Zookeeper, Kafka, PostgreSQL, MinIO, MLflow)
# 2. Starts data producer (generates 500 drivers + streams orders/GPS/weather)
# 3. Runs full pipeline (Bronze → Silver → Gold → Features → DQ → ML)
# 4. Stops producer and shows results
# 5. Opens MLflow dashboard (http://localhost:5000)

# ═════════════════════════════════════════════════════════════════════════════
# DEVELOPMENT MODE - Keep everything running
# ═════════════════════════════════════════════════════════════════════════════

# Terminal 1: Start everything
./run.sh dev-mode

# Terminal 2: View logs in real-time
./run.sh logs-all

# Terminal 3: Monitor Kafka
./run.sh monitor-kafka

# Terminal 4: Check system status
./run.sh status

# ═════════════════════════════════════════════════════════════════════════════
# MANUAL CONTROL - For step-by-step execution
# ═════════════════════════════════════════════════════════════════════════════

# Terminal 1: Start Docker
./run.sh docker-up

# Terminal 2: Start producer (streaming data continuously)
./run.sh producer

# Terminal 3: In another terminal, run pipeline
./run.sh runner

# Stop producer when done
# Ctrl+C in Terminal 2

# ═════════════════════════════════════════════════════════════════════════════
# PRODUCTION MODE - 24/7 continuous operation
# ═════════════════════════════════════════════════════════════════════════════

# Start everything in the background
nohup ./run.sh producer-bg > /tmp/producer.out 2>&1 &
nohup ./run.sh runner-continuous > /tmp/runner.out 2>&1 &

# Monitor status
./run.sh status

# View logs
tail -f /tmp/producer.out
tail -f /tmp/runner.out

# Stop everything
pkill -f "python3 streaming/producer.py"
pkill -f "python3 runner.py"

# ═════════════════════════════════════════════════════════════════════════════
# CUSTOM PIPELINE INTERVAL - Run every N seconds
# ═════════════════════════════════════════════════════════════════════════════

# Every 5 minutes (300 seconds) - default
./run.sh runner-continuous

# Every 10 minutes (600 seconds)
./run.sh runner-custom 600

# Every 2 minutes (120 seconds) - for testing
./run.sh runner-custom 120

# Every 30 minutes (1800 seconds) - for production
./run.sh runner-custom 1800

# In background with custom interval
./run.sh runner-bg 600

# ═════════════════════════════════════════════════════════════════════════════
# TESTING & VALIDATION
# ═════════════════════════════════════════════════════════════════════════════

# Full integration test
./run.sh test-integration

# Just run producer for 30 seconds, then check data
./run.sh producer-bg
sleep 30
./run.sh monitor-kafka
pkill -f "python3 streaming/producer.py"

# Check data quality report
cat logs/dq_report.json

# ═════════════════════════════════════════════════════════════════════════════
# MONITORING & DEBUGGING
# ═════════════════════════════════════════════════════════════════════════════

# View system status
./run.sh status

# Watch Kafka messages in real-time
./run.sh monitor-kafka

# View producer logs
./run.sh logs-producer
tail -f logs/producer.log

# View runner logs
./run.sh logs-runner
tail -f logs/runner.log

# View both logs
./run.sh logs-all

# ═════════════════════════════════════════════════════════════════════════════
# WEB DASHBOARDS & TOOLS
# ═════════════════════════════════════════════════════════════════════════════

# Show all available URLs
./run.sh web-urls

# Access dashboards:
# MLflow (ML Experiments):    http://localhost:5000
# MinIO Console:             http://localhost:9001
#   Username: minioadmin
#   Password: minioadmin

# ═════════════════════════════════════════════════════════════════════════════
# CLEANUP & RESET
# ═════════════════════════════════════════════════════════════════════════════

# Stop all running processes (keep Docker)
./run.sh cleanup

# Stop Docker services (preserve data)
./run.sh docker-down

# Complete reset - delete everything
./run.sh docker-clean

# Delete only DuckDB database
./run.sh reset-duckdb

# Nuclear option - everything
./run.sh reset-all && ./run.sh full-demo

# ═════════════════════════════════════════════════════════════════════════════
# HELP & REFERENCE
# ═════════════════════════════════════════════════════════════════════════════

# Show all available commands
./run.sh help

# Show specific command details
./run.sh help | grep -A 5 "runner-custom"

# ═════════════════════════════════════════════════════════════════════════════
# COMMON WORKFLOWS
# ═════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# Workflow 1: Quick 5-minute Demo
# ─────────────────────────────────────────────────────────────────────────────
./run.sh docker-up
./run.sh full-demo
# Take a look at http://localhost:5000 (MLflow)
./run.sh docker-down

# ─────────────────────────────────────────────────────────────────────────────
# Workflow 2: Test New Code Changes
# ─────────────────────────────────────────────────────────────────────────────
./run.sh docker-up
./run.sh producer-bg
sleep 10
./run.sh runner          # Run once to test
./run.sh logs-runner     # Check for errors
pkill -f "python3 streaming/producer.py"

# ─────────────────────────────────────────────────────────────────────────────
# Workflow 3: Performance Testing (30 min continuous run)
# ─────────────────────────────────────────────────────────────────────────────
./run.sh docker-up
./run.sh producer-bg
./run.sh runner-custom 1800  # Run every 30 minutes
# Let it run for several hours, then stop:
pkill -f "python3"
./run.sh logs-runner | grep "Pipeline completed"  # Check stats

# ─────────────────────────────────────────────────────────────────────────────
# Workflow 4: Debug Producer Issues
# ─────────────────────────────────────────────────────────────────────────────
./run.sh docker-up
./run.sh producer      # Run in foreground to see real-time output
# Ctrl+C when done
./run.sh docker-down

# ─────────────────────────────────────────────────────────────────────────────
# Workflow 5: Monitor Production Daily
# ─────────────────────────────────────────────────────────────────────────────
./run.sh status
./run.sh monitor-kafka | head -20
./run.sh logs-runner | tail -30
# Check MLflow experiments: http://localhost:5000

# ─────────────────────────────────────────────────────────────────────────────
# Workflow 6: Full End-to-End Test Suite
# ─────────────────────────────────────────────────────────────────────────────
./run.sh setup              # Initialize environment
./run.sh docker-up          # Start infrastructure
./run.sh test-integration   # Run all validation tests
./run.sh full-demo          # Complete demo
./run.sh docker-down        # Cleanup

# ═════════════════════════════════════════════════════════════════════════════
# TROUBLESHOOTING TIPS
# ═════════════════════════════════════════════════════════════════════════════

# Producer won't connect to Kafka
#   → Check Docker: docker ps
#   → Check Kafka logs: docker-compose logs kafka
#   → Restart: ./run.sh docker-clean && ./run.sh docker-up

# DuckDB says "database is locked"
#   → Kill runner: pkill -f "python3 runner.py"
#   → Wait 2 seconds, try again

# MinIO bucket not found
#   → It's created automatically on first pipeline run
#   → Check: docker exec logistics-minio mc ls minio/logistics-bronze

# Can't see MLflow experiments
#   → Start MLflow: ./run.sh docker-up
#   → Visit: http://localhost:5000
#   → Make sure runner has executed at least once

# Out of memory errors
#   → DuckDB uses 4GB max (config/settings.py)
#   → Reduce batch sizes in streaming/producer.py
#   → Run runner less frequently

# ═════════════════════════════════════════════════════════════════════════════
# ENVIRONMENT VARIABLES (Optional Customization)
# ═════════════════════════════════════════════════════════════════════════════

# Set custom number of drivers
export NUM_DRIVERS=1000
./run.sh full-demo

# Set custom simulation period
export SIM_YEARS=5
./run.sh full-demo

# Set debug logging
export LOG_LEVEL=DEBUG
./run.sh runner

# Reset to defaults
unset NUM_DRIVERS SIM_YEARS LOG_LEVEL

# ═════════════════════════════════════════════════════════════════════════════
# QUICK COMMAND REFERENCE
# ═════════════════════════════════════════════════════════════════════════════

# Setup
./run.sh setup                    # Initialize environment once
./run.sh docker-up                # Start Docker
./run.sh docker-down              # Stop Docker

# Producer
./run.sh producer                 # Run producer (foreground)
./run.sh producer-bg              # Run producer (background)

# Runner / Pipeline
./run.sh runner                   # Run once
./run.sh runner-continuous        # Run every 5 min
./run.sh runner-custom 600        # Run every 10 min
./run.sh runner-bg 300            # Run in background

# Monitoring
./run.sh status                   # System overview
./run.sh monitor-kafka            # Watch Kafka messages
./run.sh logs-producer            # Producer logs
./run.sh logs-runner              # Runner logs
./run.sh logs-all                 # All logs
./run.sh web-urls                 # Dashboard links

# Combined
./run.sh full-demo                # Everything at once
./run.sh dev-mode                 # Dev workflow with tailing

# Cleanup
./run.sh cleanup                  # Stop processes
./run.sh reset-duckdb             # Delete database
./run.sh reset-all                # Nuclear reset

# ═════════════════════════════════════════════════════════════════════════════

# For detailed help on any command:
#   ./run.sh help
