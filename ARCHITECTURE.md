# Architecture and Design Documentation

## Table of Contents
1. [Medallion Data Architecture](#medallion-data-architecture)
2. [Data Flow](#data-flow)
3. [Storage Strategy](#storage-strategy)
4. [Transformation Logic](#transformation-logic)
5. [Feature Engineering](#feature-engineering)
6. [ML Pipeline](#ml-pipeline)
7. [Data Quality Framework](#data-quality-framework)
8. [Scalability Considerations](#scalability-considerations)
9. [Production Readiness](#production-readiness)

## Medallion Data Architecture

The platform implements the **Medallion Architecture** with three data layers optimized for different use cases:

### Bronze Layer (Raw Data)
**Purpose**: Immutable raw data storage

**Characteristics**:
- Append-only pattern (no updates/deletes)
- Partitioned by `year/month/day` for efficient pruning
- Minimal transformation (adds only `ingestion_timestamp` and `ingestion_id`)
- Format: Parquet (columnar compression)
- Location: MinIO S3-compatible storage

**Datasets**:
- `orders`: Order metadata and expected vs actual delivery times
- `gps_events`: Real-time tracking points with late arrivals and duplicates
- `driver_profile`: Driver attributes (vehicle type, experience, risk metrics)
- `weather_events`: Weather conditions affecting deliveries

**Key Design Decisions**:
- Parquet format chosen for columnar compression and schema evolution support
- Partitioning by date enables partition pruning for large-scale queries
- Immutable storage prevents accidental data loss
- `ingestion_id` enables exact replay of failed ingestions

### Silver Layer (Cleaned Data)
**Purpose**: High-quality, standardized data for analytics

**Characteristics**:
- Deduplicates GPS events by `event_id`
- Handles late arrivals (events arriving > 15 min after delivery)
- Schema enforced (column types validated)
- Timestamps normalized to UTC (idempotent transformation)
- SCD Type 2 for driver dimensions (tracks historical changes)

**Datasets**:
- `silver_gps_events`: Deduplicated, sorted by timestamp
- `silver_orders`: Schema-validated orders
- `silver_driver_dim_scd2`: Slowly changing dimension tracking
- `silver_weather`: Normalized weather events

**Deduplication Strategy**:
- Keep first ingestion of event by `ingestion_timestamp`
- Maintains auditability of duplicates
- Removes ~1-2% of GPS events (realistic duplication ratio)

**SCD Type 2 for Drivers**:
```sql
SELECT 
    driver_id,
    vehicle_type,
    experience_years,
    effective_date,      -- When this record became valid
    end_date,            -- When superseded (NULL if current)
    is_current           -- 1 = current version, 0 = historical
FROM silver_driver_dim_scd2
ORDER BY driver_id, effective_date DESC
```

### Gold Layer (Aggregations)
**Purpose**: Business-ready metrics and KPIs

**Datasets**:
- `gold_deliveries`: Order-level metrics
  - `delay_minutes`: actual - expected delivery duration
  - `delay_flag`: binary indicator (delay > 5 min)
  - `route_distance_km`: haversine distance calculation
  - `weather_impact_score`: (0-10) impact on delivery
  - `traffic_score`: inferred from time variance

- `gold_driver_daily_metrics`: Daily aggregations per driver
  - `deliveries_completed`: count of deliveries
  - `on_time_ratio`: percentage of on-time deliveries
  - `avg_delay_minutes`: average delay across deliveries
  - `gps_points_count`: quality metric for tracking

- `gold_region_hourly_metrics`: Regional traffic patterns
  - `active_drivers`: unique drivers in region per hour
  - `gps_events_count`: hourly event volume
  - Used for capacity planning and congestion detection

## Data Flow

```
┌─────────────────────────────────────────────────┐
│  Data Simulation                                 │
│  - Orders (100k)                                 │
│  - GPS Events (2-5M with late/duplicates)       │
│  - Weather (realistic distribution)              │
│  - Driver Profiles (500 drivers)                 │
└────────────┬────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────┐
│  BRONZE Layer (Raw)                              │
│  - Append-only                                   │
│  - Partitioned by year/month/day                │
│  - Added ingestion_timestamp                    │
└────────────┬────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────┐
│  SILVER Layer (Cleaned)                          │
│  - Deduplicate GPS by event_id                  │
│  - Remove late event anomalies                  │
│  - Normalize timestamps (UTC)                   │
│  - Schema validation                            │
│  - SCD Type 2 for dimensions                    │
└────────────┬────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────┐
│  GOLD Layer (Business Logic)                     │
│  - Deliveries: delay_flag, distance, traffic    │
│  - Driver Daily: performance aggregations       │
│  - Region Hourly: traffic patterns              │
└────────────┬────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────┐
│  FEATURES Layer (ML-Ready)                       │
│  - Point-in-Time correct features               │
│  - No data leakage (event_ts < order_created_ts)│
│  - Dataset versioning & hashing                 │
│  - Multiple feature sets for different models   │
└────────────┬────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────┐
│  ML Training                                     │
│  - Delay: RandomForest classifier               │
│  - ETA: XGBoost regressor                       │
│  - Risk: IsolationForest anomaly detection      │
│  - MLflow: experiment tracking & registry       │
└─────────────────────────────────────────────────┘
```

## Storage Strategy

### Partitioning Strategy
```
s3://logistics-bronze/raw/gps_events/
├── year=2024/
│   ├── month=01/
│   │   ├── day=01/data.parquet
│   │   ├── day=02/data.parquet
│   │   └── ...
│   ├── month=02/
│   │   └── ...
```

**Benefits**:
- Partition pruning: query only relevant partitions
- Parallel processing: separate files per partition
- Incremental loads: only process recent days
- Time-based retention: easy to delete old data

### File Format Rationale
- **Parquet** chosen for:
  - Columnar compression (50-80% space savings vs CSV)
  - Schema evolution support
  - Splittable: can read partial files in parallel
  - Native support in Spark, DuckDB, Pandas
  
- **Not CSV**:
  - No compression (5x larger)
  - Requires full row parse
  - Limited type handling

- **Not Delta/Iceberg**:
  - Complexity not needed for append-only bronze
  - Added for Silver layer as complexity increases

## Transformation Logic

### GPS Deduplication
```python
SELECT 
    event_id,
    order_id,
    driver_id,
    latitude,
    longitude,
    accuracy,
    event_timestamp,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingestion_timestamp) as rn
FROM bronze_gps_events
WHERE rn = 1  -- Keep first occurrence
```

**Why this approach**:
- `ROW_NUMBER()` ensures deterministic results
- Preserves audit trail (know exact ingestion_timestamp)
- Scalable to billions of events
- Handles late arrivals naturally (sorted by timestamp)

### Timestamp Normalization
```python
# Convert all timestamps to UTC
event_timestamp = pd.to_datetime(event_timestamp, utc=True)

# Benefits:
# - Eliminates timezone confusion
# - Enables consistent joins across regions
# - Required for ML (features are timezone-agnostic)
```

## Feature Engineering

### Point-in-Time (PIT) Correctness
**Critical for avoiding data leakage in ML**

```python
# WRONG - Includes future information
historical_avg = driver_historical_avg_delay  # Computed over entire history including test period

# CORRECT - Only uses data before order creation
historical_avg = driver_historical_avg_delay_BEFORE_ORDER_CREATED_TS  # Data <= order_created_at
```

### Feature Categories

**1. Order Features**
- `order_type`: {standard, express, scheduled, frozen}
- `expected_delivery_minutes`: baseline (0.5-4 hours)
- `route_complexity`: normalized distance (0-10 scale)

**2. Driver Features**
- `experience_years`: cumulative experience
- `accident_history`: count of accidents (0-5+)
- `weather_sensitivity`: coefficient (0.01-0.15)
- `historical_on_time_ratio`: performance metric

**3. Temporal Features**
- `order_hour`: hour of day (0-23)
- `order_dayofweek`: day of week (0-6)
- `order_month`: month (1-12)
- `is_peak_hour`: flag for 7-9am or 5-7pm

**4. Environmental Features**
- `weather_impact_score`: 0-10 scale
- `traffic_score`: inferred from actual/expected ratio

### Feature Dataset Versioning
```python
# Snapshot feature dataset with hash
feature_df = engineer.create_delay_model_features(...)
dataset_hash = compute_dataset_hash(feature_df)

# MLflow logs:
mlflow.log_param("feature_dataset_hash", dataset_hash)
mlflow.log_param("feature_columns", len(feature_df.columns))
mlflow.log_param("training_rows", len(feature_df))

# Enables:
# - Reproducibility: regenerate exact features
# - Drift detection: compare hashes across runs
# - Model lineage: trace features -> model
```

## ML Pipeline

### Model Selection Rationale

**1. Delay Prediction - RandomForest**
- Problem: Binary classification (delay/no-delay)
- Why RF:
  - Handles mixed feature types (categorical + numeric)
  - Non-linear relationships (experience/weather interaction)
  - Feature importance interpretable (explain delays)
  - Robust to outliers (weather extremes)

**2. ETA Prediction - XGBoost**
- Problem: Regression (predict actual delivery minutes)
- Why XGBoost:
  - State-of-the-art for structured data
  - Handles missing values natively
  - Fast inference (low latency serving)
  - Gradient boosting captures complex patterns

**3. Driver Risk - IsolationForest**
- Problem: Anomaly detection (flag risky drivers)
- Why IsolationForest:
  - Unsupervised (no labeled anomalies needed)
  - Efficient (linear complexity)
  - Works in high dimensions
  - Output is anomaly score (0-1)

### Model Training Workflow
```
1. Load features from FEATURES layer
2. Train/test split (80/20)
3. Feature scaling (StandardScaler for RF/IF)
4. Train model with hyperparameters
5. Evaluate on test set
6. Log to MLflow:
   - Metrics (accuracy, precision, recall, F1)
   - Parameters (max_depth, n_estimators, etc.)
   - Model artifact
   - Feature dataset hash
7. Register model version
8. Snapshot training data
```

## Data Quality Framework

### Row Count Lineage
Tracks row counts through pipeline to detect data loss:
```
bronze_orders: 100,000 rows
  ↓ (SQL dedup)
silver_orders: 99,998 rows (-2 duplicates, acceptable)
  ↓ (join with deliveries)
gold_deliveries: 99,850 rows (-148 rows with NULL delivery, acceptable)
```

Acceptable variance configured in `.env`:
```env
ROW_COUNT_VARIANCE=0.1  # Allow 10% variance
```

### Null Percentage Checks
Per-column null ratio monitoring:
```python
null_pct = field.isnull().sum() / len(df)
if null_pct > threshold:
    raise DataQualityError(f"{field}: {null_pct:.1%} nulls (threshold: {threshold})")
```

Configuration:
```env
NULL_CHECK_THRESHOLD=0.05  # Alert if >5% nulls
```

### Feature Drift Detection
Uses Kolmogorov-Smirnov (KS) test to detect distribution changes:
```python
# KS test: compare baseline vs current distributions
ks_statistic, p_value = scipy.stats.ks_2samp(baseline, current)

# If p_value < 0.05 (default), distribution has significantly changed
# Action: Retrain model or alert for investigation
```

Typical drifts:
- New weather pattern → higher variance in delay features
- Driver turnover → different experience distribution
- Route changes → altered distance distribution

## Scalability Considerations

### Current Scale
- 500 drivers
- 100,000 deliveries
- 2-5M GPS events
- 2-year simulation = ~1GB raw data

### Scaling to Production (1000x)

**Data Volume**:
- 500k drivers → 1M+ drivers (logistics at scale)
- 100M deliveries/year → 100M-1B
- 2-5B GPS events → 2-5T events
- Storage: 100GB+ → 10-100TB raw

**Optimization Strategies**:

1. **Partitioning**
   - Current: year/month/day (good for 1-2 years)
   - Scale: year/month/day/region (partition by geography too)
   - Reduces partition size: 1GB → 100MB per partition

2. **Incremental Processing**
   - Current: reprocess all data daily
   - Scale: only process new data (delta)
   - SQL: `WHERE created_at >= CURRENT_DATE`

3. **Caching**
   - Cache frequently-accessed dimensions
   - Redis for driver profiles, region definitions
   - 100ms → 1ms lookup

4. **Archival**
   - S3 Intelligent-Tiering: auto-move cold data
   - 90+ days old → cheaper tier (50% cost reduction)

### Parallel Processing
Current implementation uses single-threaded DuckDB.

For scale:
```python
# Use Spark instead of DuckDB
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("logistics") \
    .config("spark.executor.cores", "8") \
    .config("spark.executor.memory", "16g") \
    .getOrCreate()

# Automatic parallelization across partitions
df = spark.read.parquet("s3://logistics-bronze/raw/gps_events/")
df.filter(df.delay_flag == 1).write.parquet("s3://logistics-gold/...")
```

## Production Readiness

### Missing Components (TODO)
1. **Service Mesh**: Istio for service-to-service communication
2. **Feature Store**: Feast/Tecton for feature versioning
3. **Model Serving**: KServe for low-latency predictions
4. **Monitoring**: Prometheus + Grafana for metrics
5. **Governance**: Apache Atlas for lineage & compliance
6. **Backup**: Automated snapshots and cross-region replication

### Security Hardening
1. TLS/SSL for all services
2. Service account isolation (RBAC)
3. Secrets management (HashiCorp Vault)
4. Row-level security (RLS) in Postgres
5. Audit logging for all data access

### Performance Tuning
1. Query caching (Redis for hot data)
2. Index optimization (PostgreSQL indexes)
3. Connection pooling (PgBouncer)
4. Query parallelization (Spark cluster)
5. Memory management (DuckDB → Spark)

---

**Last Updated**: 2024-01-15
**Architecture Version**: 1.0 (Medallion with ML)
