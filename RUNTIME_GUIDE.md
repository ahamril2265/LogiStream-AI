# AI Logistics Platform - Complete Runtime Guide

## Overview

This project provides a **comprehensive run command system** for managing the AI-Optimized Logistics Data Lake streaming pipeline. All operations are controlled through a single shell script with structured commands, eliminating the need to remember complex Docker, Python, and Kafka commands.

## Files Provided

### 1. **run.sh** (Main Script)
- **Type**: Executable bash script (~850 lines)
- **Location**: `/home/ahamed/Projects/DE/ai-logistics-platform/run.sh`
- **Purpose**: Universal command dispatcher for all pipeline operations
- **Usage**: `./run.sh <command> [options]`

### 2. **QUICKSTART.sh** (Examples)
- **Type**: Bash script with copy-paste examples
- **Location**: `/home/ahamed/Projects/DE/ai-logistics-platform/QUICKSTART.sh`
- **Purpose**: Quick reference for common scenarios
- **Usage**: View and copy specific command combinations

### 3. **RUN_COMMANDS.md** (Complete Documentation)
- **Type**: Markdown reference
- **Location**: `/home/ahamed/Projects/DE/ai-logistics-platform/RUN_COMMANDS.md`
- **Purpose**: Detailed documentation with examples
- **Usage**: Browse for command help and troubleshooting

---

## Quick Start (3 steps)

### Step 1: Initial Setup (one-time, ~2 minutes)
```bash
cd /home/ahamed/Projects/DE/ai-logistics-platform
./run.sh setup
```

### Step 2: Start Infrastructure (30 seconds)
```bash
./run.sh docker-up
```

### Step 3: Run Complete Demo (5 minutes)
```bash
./run.sh full-demo
```

Done! Your pipeline is running. Check MLflow at http://localhost:5000

---

## Command Categories

### 🔧 Setup & Infrastructure
```bash
./run.sh setup              # One-time environment setup
./run.sh docker-up          # Start Docker services
./run.sh docker-down        # Stop Docker services
./run.sh docker-clean       # Full cleanup (delete all data)
```

### 📤 Data Producer (Kafka Streaming)
```bash
./run.sh producer           # Run producer (interactive)
./run.sh producer-bg        # Run producer (background)
```

### 🔄 Pipeline Runner (Transformations)
```bash
./run.sh runner             # Run pipeline once (~3 minutes)
./run.sh runner-continuous  # Run every 5 minutes
./run.sh runner-custom 600  # Run every 10 minutes (custom interval)
./run.sh runner-bg 300      # Run in background with custom interval
```

### 📊 Monitoring & Logs
```bash
./run.sh status             # System status overview
./run.sh monitor-kafka      # Watch Kafka messages
./run.sh logs-producer      # View producer logs
./run.sh logs-runner        # View runner/pipeline logs
./run.sh logs-all           # View all logs
./run.sh web-urls           # Show dashboard URLs
```

### 🎯 Combined Workflows
```bash
./run.sh full-demo          # End-to-end demo (complete in 5 min)
./run.sh dev-mode           # Development mode with log tailing
./run.sh test-integration   # Validate all components
```

### 🧹 Cleanup
```bash
./run.sh cleanup            # Stop all processes
./run.sh reset-duckdb       # Delete database
./run.sh reset-all          # Complete reset
```

### ℹ️ Help
```bash
./run.sh help               # Show all available commands
```

---

## Execution Modes

### Mode 1: Quick Demo (Easiest)
Perfect for first-time testing or demonstrations.

```bash
./run.sh full-demo
```
**What happens:**
- Starts Docker services
- Runs producer and pipeline
- Shows results
- Takes ~5 minutes total

---

### Mode 2: Development (Interactive)
Perfect for testing code changes and debugging.

```bash
# Terminal 1
./run.sh dev-mode

# Terminal 2 (in new window)
./run.sh logs-all
./run.sh monitor-kafka
```
**What happens:**
- Starts everything
- Keeps processes running
- You manually control pipeline execution
- View logs in real-time

---

### Mode 3: Manual Control (Step-by-Step)
Perfect for learning and understanding the system.

```bash
# Terminal 1: Start infrastructure
./run.sh docker-up

# Terminal 2: Start data producer
./run.sh producer

# Terminal 3: Run pipeline
./run.sh runner

# Terminal 4: Monitor
./run.sh status
./run.sh monitor-kafka
```
**What happens:**
- You control each component independently
- Can stop and restart each component
- Full visibility into what's happening

---

### Mode 4: Production (24/7 Continuous)
Perfect for continuous data pipeline operation.

```bash
# Start producer in background
nohup ./run.sh producer-bg > /tmp/producer.log 2>&1 &

# Start pipeline runner continuously every 5 minutes
nohup ./run.sh runner-continuous > /tmp/runner.log 2>&1 &

# Check status periodically
./run.sh status

# Check logs
tail -f /tmp/runner.log
```
**What happens:**
- Producer runs indefinitely
- Pipeline executes every 5 minutes automatically
- Both run in background
- Logs saved to files

---

## Common Scenarios

### Scenario A: "I want to see everything working in 5 minutes"
```bash
./run.sh full-demo
```

### Scenario B: "I want to manually control each component"
```bash
./run.sh docker-up
./run.sh producer       # Terminal 1
./run.sh runner         # Terminal 2 (in new window)
```

### Scenario C: "I want to test my code changes"
```bash
./run.sh docker-up
./run.sh producer-bg
./run.sh runner
./run.sh logs-runner    # Check for errors
```

### Scenario D: "I want to run continuously like production"
```bash
nohup ./run.sh producer-bg &
nohup ./run.sh runner-continuous &
./run.sh status         # Monitor periodically
```

### Scenario E: "I want to understand what's happening"
```bash
./run.sh dev-mode       # Terminal 1
# + new terminals:
tail -f logs/producer.log
tail -f logs/runner.log
./run.sh monitor-kafka
```

### Scenario F: "I want to run every 10 minutes instead of 5"
```bash
./run.sh docker-up
./run.sh producer-bg
./run.sh runner-custom 600  # 600 seconds = 10 minutes
```

---

## Component Architecture

```
┌─────────────────────────────────────────────────┐
│        SINGLE ENTRY POINT: ./run.sh             │
├─────────────────────────────────────────────────┤
│                                                 │
│  ├─ setup                    (Initialize)       │
│  ├─ docker-up                (Start services)   │
│  ├─ docker-down              (Stop services)    │
│  │                                             │
│  ├─ producer                 (Stream data)      │
│  ├─ producer-bg              (Stream in bg)     │
│  │                                             │
│  ├─ runner                   (Single execution) │
│  ├─ runner-continuous        (5-min intervals)  │
│  ├─ runner-custom 600        (Custom interval)  │
│  ├─ runner-bg 300            (Background run)   │
│  │                                             │
│  ├─ status                   (System overview)  │
│  ├─ monitor-kafka            (Watch messages)   │
│  ├─ logs-*                   (View logs)        │
│  ├─ web-urls                 (Show dashboards)  │
│  │                                             │
│  ├─ full-demo                (Complete demo)   │
│  ├─ dev-mode                 (Dev workflow)     │
│  ├─ test-integration         (Validation)       │
│  │                                             │
│  ├─ cleanup                  (Stop processes)   │
│  ├─ reset-*                  (Reset state)      │
│  │                                             │
│  └─ help                     (This help)        │
│                                                 │
└─────────────────────────────────────────────────┘
```

---

## Data Flow Visualization

```
Producer                 Kafka                 Consumer              MinIO
─────────                ─────                 ────────              ─────

Drivers ───────────────► drivers ◄────────────── Bronze Ingestion ──► logistics-bronze/
                         (500 msg)                (consumes all)        drivers/

Orders  ───────────────► orders  ◄─────────────────────────────────► logistics-bronze/
                         (100+/batch)                                  orders/

GPS     ───────────────► gps_events ◄──────────────────────────────► logistics-bronze/
Events                   (2500+/batch)                               gps_events/

Weather ───────────────► weather_events ◄────────────────────────► logistics-bronze/
                         (0-5/batch)                                 weather_events/


Bronze ──────────────────┐
         Silver ─────────┤
                  Gold ──┤ (Pipeline Runner executes these sequentially)
               Features ┤
                    ML ─┘

└──► MLflow Experiments (http://localhost:5000)
```

---

## Key Features

✅ **Single Command Dispatcher** - No need to remember Docker/Python commands
✅ **Color-coded Output** - Green for success, red for errors, yellow for info
✅ **Background/Foreground Modes** - Run interactively or in background
✅ **Custom Intervals** - Run pipeline any interval you want (seconds)
✅ **Integrated Logging** - All logs captured automatically
✅ **Status Monitoring** - See what's running at a glance
✅ **Error Handling** - Graceful error messages and recovery suggestions
✅ **Help System** - Comprehensive documentation built-in
✅ **Development Friendly** - Easy to test code changes
✅ **Production Ready** - Can run 24/7 with background processes

---

## File Structure

```
project/
├── run.sh                    # ← Main command script (30 KB)
├── QUICKSTART.sh             # ← Copy-paste examples
├── RUN_COMMANDS.md           # ← Full documentation
├── README.md                 # ← Project overview
├── STATUS.md                 # ← Current system status
│
├── streaming/
│   ├── producer.py           # Data streaming
│   └── consumer.py           # MinIO ingestion
│
├── runner.py                 # Pipeline orchestrator
│
├── config/
│   └── settings.py           # Configuration
│
├── data_simulation/
│   └── generator.py          # Data generators
│
├── docker-compose-streaming.yml  # Docker services
├── requirements.txt          # Python dependencies
│
├── logs/                     # Execution logs
│   ├── producer.log
│   ├── runner.log
│   └── dq_report.json
│
├── data/
│   └── logistics.duckdb      # Local database
│
└── venv/                     # Python environment
```

---

## Typical Workflow

### First Time Users
```
1. ./run.sh setup               (2 min, one-time)
2. ./run.sh docker-up           (30 sec)
3. ./run.sh full-demo           (5 min)
4. View http://localhost:5000   (MLflow)
Done!
```

### Developers Testing Changes
```
1. Make code changes
2. ./run.sh docker-up           (if not running)
3. ./run.sh producer-bg         (start data stream)
4. ./run.sh runner              (test pipeline)
5. ./run.sh logs-runner         (check output)
6. Fix issues and repeat step 4
```

### Production Operations
```
1. ./run.sh setup               (one-time)
2. ./run.sh docker-up           (one-time)
3. nohup ./run.sh producer-bg &         (start producer)
4. nohup ./run.sh runner-continuous &  (start pipeline)
5. ./run.sh status              (check periodically)
6. tail -f logs/runner.log      (monitor)
```

---

## Environment Variables

```bash
# Control behavior without editing code
export NUM_DRIVERS=1000              # More drivers
export NUM_DELIVERIES=500000         # More orders
export SIM_YEARS=5                   # More history
export LOG_LEVEL=DEBUG               # Verbose logging
export RANDOM_SEED=42                # Reproducibility

# Then run commands as usual
./run.sh runner
```

---

## Support & Troubleshooting

### "I don't know which command to run"
```bash
./run.sh help         # Shows all commands with descriptions
```

### "Something isn't working"
```bash
./run.sh status       # Check what's running
./run.sh logs-all     # View all logs
./run.sh test-integration  # Validate all components
```

### "I want to start over"
```bash
./run.sh reset-all    # Complete cleanup
./run.sh full-demo    # Start fresh
```

### "I need detailed help on specific topics"
```bash
# Read the documentation
cat RUN_COMMANDS.md
cat QUICKSTART.sh
```

---

## Performance Expectations

| Operation | Time | Resource Usage |
|-----------|------|---|
| `setup` | 2 min | Network (pip) |
| `docker-up` | 30 sec | 2 CPU, 2 GB RAM |
| `full-demo` | 5 min | 100% CPU |
| `runner` (once) | 3 min | 100% CPU |
| `producer` | Infinite | 10-20% CPU |
| Memory usage | Steady state | ~4 GB |
| Disk usage | After first run | ~5 GB |

---

## What This Solves

**Before**: Remember dozens of Docker, Python, Kafka, and custom commands
```bash
docker-compose -f docker-compose-streaming.yml up -d
source venv/bin/activate
python3 streaming/producer.py &
python3 runner.py --continuous --interval 300 &
# ... etc
```

**After**: Simple, intuitive commands
```bash
./run.sh docker-up
./run.sh producer-bg
./run.sh runner-continuous
```

---

## Key Takeaways

1. **All commands** go through `./run.sh`
2. **Help is built-in**: `./run.sh help`
3. **Quick start**: `./run.sh full-demo`
4. **Choose your mode**: Interactive, background, or continuous
5. **Everything is logged**: Check `logs/` directory
6. **Safe to experiment**: Use `./run.sh reset-all` to start over

---

## Next Steps

1. Run the setup: `./run.sh setup`
2. Start Docker: `./run.sh docker-up`
3. Try the demo: `./run.sh full-demo`
4. Explore the dashboards: http://localhost:5000 (MLflow)
5. Check the logs: `./run.sh logs-all`
6. Read detailed docs: `RUN_COMMANDS.md`

---

**Happy streaming!** 🚀

For more information, run: `./run.sh help`
