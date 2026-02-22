# AI Logistics Platform - Quick Reference Card

## 🎯 Everything You Need To Know

### ✅ What Was Created

| File | Lines | Size | Purpose |
|------|-------|------|---------|
| **run.sh** | 930 | 30 KB | Main command dispatcher - use this! |
| **QUICKSTART.sh** | 321 | 17 KB | Copy-paste ready examples |
| **RUN_COMMANDS.md** | 618 | 12 KB | Complete documentation |
| **RUNTIME_GUIDE.md** | 493 | 15 KB | Getting started guide |

### 📍 Location
All files are in: `/home/ahamed/Projects/DE/ai-logistics-platform/`

---

## ⚡ Quick Start (Copy & Paste)

```bash
# Step 1: Initialize (one-time, 2 min)
cd /home/ahamed/Projects/DE/ai-logistics-platform
./run.sh setup

# Step 2: Start Docker (30 sec)
./run.sh docker-up

# Step 3: Run Demo (5 min)
./run.sh full-demo
```

Done! Check: http://localhost:5000 (MLflow)

---

## 🎮 All Commands At A Glance

### Setup
```bash
./run.sh setup              # Initialize (one-time)
./run.sh docker-up          # Start Docker
./run.sh docker-down        # Stop Docker
```

### Run The System
```bash
./run.sh full-demo          # Everything at once (5 min)
./run.sh producer           # Stream data (foreground)
./run.sh runner             # Pipeline run (foreground)
```

### Background Mode
```bash
./run.sh producer-bg        # Stream in background
./run.sh runner-continuous  # Pipeline every 5 minutes
./run.sh runner-custom 600  # Pipeline every N seconds
./run.sh runner-bg 600      # Pipeline in background
```

### Monitor
```bash
./run.sh status             # What's running?
./run.sh logs-producer      # Producer output
./run.sh logs-runner        # Pipeline output
./run.sh logs-all           # All logs
./run.sh monitor-kafka      # Watch messages
./run.sh web-urls           # Dashboard URLs
```

### Development
```bash
./run.sh dev-mode           # Full dev workflow
./run.sh test-integration   # Validate components
```

### Cleanup
```bash
./run.sh cleanup            # Stop processes
./run.sh reset-duckdb       # Delete database
./run.sh reset-all          # Complete reset
./run.sh help               # Show all commands
```

---

## 🎯 5 Ways To Run The System

### 1️⃣ Demo (Easiest - 5 minutes)
```bash
./run.sh full-demo
```
Starts everything, runs, stops. Perfect for first-time testing.

### 2️⃣ Interactive (Manual Control)
```bash
./run.sh producer           # Terminal 1
./run.sh runner             # Terminal 2 (new window)
```
You control each piece. Great for learning.

### 3️⃣ Production (24/7)
```bash
nohup ./run.sh producer-bg &
nohup ./run.sh runner-continuous &
```
Runs in background. Check with `./run.sh status`

### 4️⃣ Custom Interval (Testing)
```bash
./run.sh runner-custom 120  # Run every 2 minutes
```
Test with any frequency you want.

### 5️⃣ Development (With Monitoring)
```bash
./run.sh dev-mode
# (also open in new terminals)
tail -f logs/producer.log
tail -f logs/runner.log
```
Everything running with visible logs.

---

## 📊 What Happens When You Run Them

| Command | Duration | What It Does |
|---------|----------|--------------|
| `full-demo` | 5 min | Starts Docker → Producer → Pipeline → Results |
| `producer` | ∞ | Generates 500 drivers, streams orders/GPS/weather |
| `runner` | 3 min | Bronze→Silver→Gold→Features→DQ→ML in sequence |
| `runner-continuous` | ∞ | Runs pipeline every 5 minutes forever |
| `status` | 1 sec | Shows running processes and Kafka topics |
| `logs-all` | 1 sec | Shows last 30 lines of all logs |

---

## 🌐 Dashboards

| URL | Purpose | Credentials |
|-----|---------|-------------|
| http://localhost:5000 | MLflow Experiments | (open) |
| http://localhost:9001 | MinIO Console | minioadmin/minioadmin |
| localhost:9092 | Kafka API | N/A |

---

## 🔄 Data Flow Summary

```
Producer                Kafka                      MinIO
────────────────────────────────────────────────────────────
Driver profiles (500) ──→ drivers topic ──→ Bronze/drivers/
Orders (100+/batch) ───→ orders topic ────→ Bronze/orders/
GPS events (2500+) ────→ gps_events topic ─→ Bronze/gps_events/
Weather events ────────→ weather topic ────→ Bronze/weather/

                     Pipeline Runner
                     ───────────────
            Bronze ──────→ Silver ──────→ Gold ──────→ Features
                                               ↓
                                          ML Training
                                          (MLflow)
```

---

## 💾 Storage Locations

```
Logs:              ./logs/
  Producer:        ./logs/producer.log
  Runner:          ./logs/runner.log
  DQ Report:       ./logs/dq_report.json

Database:          ./data/logistics.duckdb (4GB max)

Python:            ./venv/ (auto-created by setup)

Docker Data:       (managed by Docker, survives restart)
```

---

## ⚙️ Configuration

No config file needed! But you can override defaults:

```bash
# Set these before running commands
export NUM_DRIVERS=1000
export NUM_DELIVERIES=500000
export LOG_LEVEL=DEBUG

# Run normally
./run.sh runner
```

---

## 🆘 Troubleshooting In 30 Seconds

**"Nothing works"**
```bash
./run.sh reset-all && ./run.sh full-demo
```

**"Kafka connection failed"**
```bash
./run.sh docker-clean && ./run.sh docker-up
```

**"Database locked"**
```bash
pkill -f "python3 runner.py" && ./run.sh runner
```

**"Unknown command"**
```bash
./run.sh help
```

---

## 📚 Documentation Files

- **This file**: Quick reference
- **RUNTIME_GUIDE.md**: Getting started guide
- **RUN_COMMANDS.md**: Complete documentation
- **QUICKSTART.sh**: Copy-paste examples
- **run.sh**: Source code (beautifully commented)

---

## 🎓 Learning Path

1. **New User**: `./run.sh full-demo`
2. **Explore**: `./run.sh help` → check dashboards
3. **Test Code**: `./run.sh dev-mode` + make changes
4. **Experiment**: `./run.sh runner-custom 120`
5. **Deploy**: `nohup ./run.sh runner-continuous &`

---

## ✨ Key Features

✅ Single entry point: `./run.sh`
✅ No Docker/Python commands needed!
✅ Color-coded output (green=success, red=error, yellow=info)
✅ Background & foreground modes
✅ Custom intervals
✅ Automatic logging
✅ Built-in help system
✅ Production-ready
✅ 0-configuration

---

## 🚀 Next Steps

1. Open Terminal
2. Run: `cd /home/ahamed/Projects/DE/ai-logistics-platform`
3. Run: `./run.sh full-demo`
4. Wait 5 minutes
5. Open: http://localhost:5000
6. Enjoy! 🎉

---

## 📞 Need Help?

```bash
./run.sh help                    # All commands
./run.sh help | grep runner      # Help on specific topic
cat RUNTIME_GUIDE.md             # Getting started
cat RUN_COMMANDS.md              # Complete reference
```

---

**Created:** February 21, 2026
**Location:** `/home/ahamed/Projects/DE/ai-logistics-platform/`
**Ready to use:** Yes! Start with `./run.sh full-demo`
