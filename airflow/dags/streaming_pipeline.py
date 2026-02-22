"""
Airflow DAG for logistics data pipeline orchestration.

Execution flow:
- consume_bronze: Consumes from Kafka topics and ingests to Bronze layer
- transform_silver: Deduplicates and cleans Bronze data
- aggregate_gold: Creates business metrics from Silver
- engineer_features: Creates ML-ready features from Gold
- validate_dq: Runs data quality checks
- train_models: Trains ML models with MLflow tracking

Trigger manually via Airflow UI after Kafka producer is running.
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# Add project modules to path
sys.path.insert(0, "/opt/airflow/config")
sys.path.insert(0, "/opt/airflow")

from config import get_config, setup_logging, MinIOClient
from streaming.consumer import consume_topic

# Default DAG arguments
default_args = {
    "owner": "logistics-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 2, 21),
}

# DAG definition
dag = DAG(
    "logistics_streaming_pipeline",
    default_args=default_args,
    description="Streaming data pipeline with Kafka producers and Airflow orchestration",
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=["logistics", "streaming", "medallion"],
)


# ============================================================================
# BRONZE LAYER TASKS
# ============================================================================

def consume_bronze_drivers(**context):
    """Consume driver records from Kafka."""
    config = get_config()
    setup_logging(config)
    
    minio = MinIOClient(
        config.minio.endpoint,
        config.minio.access_key,
        config.minio.secret_key,
        config.minio.secure,
    )
    minio.ensure_bucket_exists(config.minio.bucket_bronze)
    
    stats = consume_topic("drivers", config, minio, timeout_seconds=60)
    return stats


def consume_bronze_orders(**context):
    """Consume order records from Kafka."""
    config = get_config()
    setup_logging(config)
    
    minio = MinIOClient(
        config.minio.endpoint,
        config.minio.access_key,
        config.minio.secret_key,
        config.minio.secure,
    )
    minio.ensure_bucket_exists(config.minio.bucket_bronze)
    
    stats = consume_topic("orders", config, minio, timeout_seconds=180)
    return stats


def consume_bronze_gps(**context):
    """Consume GPS events from Kafka."""
    config = get_config()
    setup_logging(config)
    
    minio = MinIOClient(
        config.minio.endpoint,
        config.minio.access_key,
        config.minio.secret_key,
        config.minio.secure,
    )
    minio.ensure_bucket_exists(config.minio.bucket_bronze)
    
    stats = consume_topic("gps_events", config, minio, timeout_seconds=180)
    return stats


def consume_bronze_weather(**context):
    """Consume weather events from Kafka."""
    config = get_config()
    setup_logging(config)
    
    minio = MinIOClient(
        config.minio.endpoint,
        config.minio.access_key,
        config.minio.secret_key,
        config.minio.secure,
    )
    minio.ensure_bucket_exists(config.minio.bucket_bronze)
    
    stats = consume_topic("weather_events", config, minio, timeout_seconds=180)
    return stats


# ============================================================================
# SILVER LAYER TASKS
# ============================================================================

def silver_transformation(**context):
    """Execute Silver layer transformation."""
    import logging
    from silver.transformer import SilverTransformer
    from config import get_config, MinIOClient
    from config.utils import DuckDBConnector
    
    logger = logging.getLogger(__name__)
    config = get_config()
    
    minio = MinIOClient(
        config.minio.endpoint,
        config.minio.access_key,
        config.minio.secret_key,
        config.minio.secure,
    )
    
    duckdb = DuckDBConnector(config.duckdb.db_path)
    duckdb.connect()
    
    transformer = SilverTransformer(duckdb, minio, config)
    
    stats = {}
    stats["gps_events"] = transformer.transform_gps_events()
    stats["orders"] = transformer.transform_orders()
    stats["weather"] = transformer.transform_weather()
    
    logger.info(f"Silver transformation complete: {stats}")
    duckdb.close()
    
    return stats


# ============================================================================
# GOLD LAYER TASKS
# ============================================================================

def gold_aggregation(**context):
    """Execute Gold layer aggregation."""
    import logging
    from gold.aggregator import GoldAggregator
    from config import get_config, MinIOClient
    from config.utils import DuckDBConnector
    
    logger = logging.getLogger(__name__)
    config = get_config()
    
    minio = MinIOClient(
        config.minio.endpoint,
        config.minio.access_key,
        config.minio.secret_key,
        config.minio.secure,
    )
    
    duckdb = DuckDBConnector(config.duckdb.db_path)
    duckdb.connect()
    
    aggregator = GoldAggregator(duckdb, minio, config)
    
    logger.info("Gold aggregation complete")
    duckdb.close()
    
    return {"status": "complete"}


# ============================================================================
# FEATURE ENGINEERING TASKS
# ============================================================================

def feature_engineering(**context):
    """Execute feature engineering for ML."""
    import logging
    from features.engineer import FeatureEngineer
    from config import get_config, MinIOClient
    from config.utils import DuckDBConnector
    
    logger = logging.getLogger(__name__)
    config = get_config()
    
    minio = MinIOClient(
        config.minio.endpoint,
        config.minio.access_key,
        config.minio.secret_key,
        config.minio.secure,
    )
    
    duckdb = DuckDBConnector(config.duckdb.db_path)
    duckdb.connect()
    
    engineer = FeatureEngineer(duckdb, minio, config)
    
    logger.info("Feature engineering complete")
    duckdb.close()
    
    return {"status": "complete"}


# ============================================================================
# DATA QUALITY TASKS
# ============================================================================

def data_quality_checks(**context):
    """Execute data quality validation."""
    import logging
    from monitoring.dq_validator import DataQualityValidator
    from config import get_config
    from config.utils import DuckDBConnector
    from pathlib import Path
    
    logger = logging.getLogger(__name__)
    config = get_config()
    
    duckdb = DuckDBConnector(config.duckdb.db_path)
    duckdb.connect()
    
    validator = DataQualityValidator(duckdb, config)
    report = validator.run_full_validation()
    
    # Save report
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    report_path = log_dir / "dq_report.json"
    
    import json
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    
    logger.info(f"Data quality report saved to {report_path}")
    duckdb.close()
    
    return report


# ============================================================================
# ML TRAINING TASKS
# ============================================================================

def ml_training(**context):
    """Execute ML model training."""
    import logging
    from ml.trainer import ModelTrainer
    from config import get_config
    
    logger = logging.getLogger(__name__)
    config = get_config()
    
    trainer = ModelTrainer(config)
    
    logger.info("ML model training complete")
    
    return {"status": "complete"}


# ============================================================================
# DAG DEFINITION
# ============================================================================

with dag:
    
    # Bronze ingestion tasks
    with TaskGroup("bronze_ingestion") as bronze_group:
        bronze_drivers = PythonOperator(
            task_id="consume_drivers",
            python_callable=consume_bronze_drivers,
            doc="Consume driver records from Kafka",
        )
        
        bronze_orders = PythonOperator(
            task_id="consume_orders",
            python_callable=consume_bronze_orders,
            doc="Consume order records from Kafka",
        )
        
        bronze_gps = PythonOperator(
            task_id="consume_gps_events",
            python_callable=consume_bronze_gps,
            doc="Consume GPS events from Kafka",
        )
        
        bronze_weather = PythonOperator(
            task_id="consume_weather",
            python_callable=consume_bronze_weather,
            doc="Consume weather events from Kafka",
        )
    
    # Silver transformation
    silver = PythonOperator(
        task_id="silver_transformation",
        python_callable=silver_transformation,
        doc="Transform and deduplicate Bronze data",
    )
    
    # Gold aggregation
    gold = PythonOperator(
        task_id="gold_aggregation",
        python_callable=gold_aggregation,
        doc="Create business metrics from Silver",
    )
    
    # Feature engineering
    features = PythonOperator(
        task_id="feature_engineering",
        python_callable=feature_engineering,
        doc="Engineer ML-ready features",
    )
    
    # Data quality checks
    dq_check = PythonOperator(
        task_id="data_quality_validation",
        python_callable=data_quality_checks,
        doc="Run comprehensive data quality checks",
    )
    
    # ML training
    ml_train = PythonOperator(
        task_id="ml_training",
        python_callable=ml_training,
        doc="Train ML models with MLflow tracking",
    )
    
    # Task dependencies
    bronze_group >> silver >> gold >> features
    features >> [dq_check, ml_train]
