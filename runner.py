"""
Standalone pipeline runner - replaces Airflow for local testing.

Executes the full streaming pipeline without needing Airflow.
Run manually or via scheduler like APScheduler.

Usage:
  python3 runner.py              # Run once
  python3 runner.py --continuous  # Run continuously every 5 min
"""

import sys
import argparse
import logging
import time
from datetime import datetime
from pathlib import Path

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

from config import get_config, setup_logging, MinIOClient
from config.utils import DuckDBConnector
from streaming.consumer import consume_topic
from silver.transformer import SilverTransformer
from gold.aggregator import GoldAggregator
from features.engineer import FeatureEngineer
from monitoring.dq_validator import DataQualityValidator
from ml.trainer import ModelTrainer
import json

logger = logging.getLogger(__name__)


def run_bronze_ingestion(config):
    """Step 1: Consume Kafka topics to Bronze."""
    logger.info("=" * 80)
    logger.info("BRONZE INGESTION: Consuming Kafka topics")
    logger.info("=" * 80)
    
    minio = MinIOClient(
        config.minio.endpoint,
        config.minio.access_key,
        config.minio.secret_key,
        config.minio.secure,
    )
    minio.ensure_bucket_exists(config.minio.bucket_bronze)
    
    # Consume all topics
    topics = ["drivers", "orders", "gps_events", "weather_events"]
    for topic in topics:
        logger.info(f"Consuming: {topic}")
        stats = consume_topic(topic, config, minio, timeout_seconds=60)
        logger.info(f"  {stats['total_records']} records ingested")
    
    logger.info("Bronze ingestion complete")


def run_silver_transformation(config):
    """Step 2: Transform Bronze → Silver."""
    logger.info("=" * 80)
    logger.info("SILVER TRANSFORMATION: Deduplication & Cleaning")
    logger.info("=" * 80)
    
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
    
    logger.info(f"Silver transformation stats: {stats}")
    duckdb.close()


def run_gold_aggregation(config):
    """Step 3: Aggregate Silver → Gold."""
    logger.info("=" * 80)
    logger.info("GOLD AGGREGATION: Business Metrics")
    logger.info("=" * 80)
    
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


def run_feature_engineering(config):
    """Step 4: Engineer features for ML."""
    logger.info("=" * 80)
    logger.info("FEATURE ENGINEERING: ML-Ready Features")
    logger.info("=" * 80)
    
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


def run_data_quality_checks(config):
    """Step 5: Validate data quality."""
    logger.info("=" * 80)
    logger.info("DATA QUALITY VALIDATION")
    logger.info("=" * 80)
    
    duckdb = DuckDBConnector(config.duckdb.db_path)
    duckdb.connect()
    
    validator = DataQualityValidator(config)
    report = validator.run_full_validation()
    
    # Save report
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    report_path = log_dir / "dq_report.json"
    
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    
    logger.info(f"DQ report saved: {report_path}")
    duckdb.close()
    
    return report


def run_ml_training(config):
    """Step 6: Train ML models."""
    logger.info("=" * 80)
    logger.info("ML MODEL TRAINING: RandomForest, XGBoost, IsolationForest")
    logger.info("=" * 80)
    
    trainer = ModelTrainer(config)
    logger.info("ML training complete - check MLflow at http://localhost:5000")


def run_pipeline(config):
    """Execute complete pipeline."""
    try:
        start_time = datetime.utcnow()
        logger.info(f"Pipeline started: {start_time.isoformat()}")
        
        run_bronze_ingestion(config)
        run_silver_transformation(config)
        run_gold_aggregation(config)
        run_feature_engineering(config)
        run_data_quality_checks(config)
        run_ml_training(config)
        
        elapsed = (datetime.utcnow() - start_time).total_seconds()
        logger.info("=" * 80)
        logger.info("PIPELINE COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Elapsed time: {elapsed:.1f} seconds")
        logger.info(f"Check results:")
        logger.info(f"  - MinIO: http://localhost:9001")
        logger.info(f"  - MLflow: http://localhost:5000")
        logger.info(f"  - Logs: ./logs/")
        
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Standalone pipeline runner (Airflow alternative)"
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run continuously every 5 minutes"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Interval between runs in seconds (default: 300)"
    )
    
    args = parser.parse_args()
    
    # Setup
    config = get_config()
    setup_logging(config)
    
    logger.info("=" * 80)
    logger.info("STREAMING PIPELINE RUNNER")
    logger.info("=" * 80)
    logger.info(f"Mode: {'CONTINUOUS' if args.continuous else 'ONCE'}")
    if args.continuous:
        logger.info(f"Interval: {args.interval} seconds")
    
    if args.continuous:
        # Continuous mode
        run_count = 0
        try:
            while True:
                run_count += 1
                logger.info(f"\n>>> RUN #{run_count}")
                run_pipeline(config)
                
                if run_count < 99999:  # Sanity check
                    logger.info(f"Next run in {args.interval} seconds (Ctrl+C to stop)")
                    time.sleep(args.interval)
        except KeyboardInterrupt:
            logger.info("\nStopped by user")
            return 0
    else:
        # Single run
        return run_pipeline(config)


if __name__ == "__main__":
    sys.exit(main())
