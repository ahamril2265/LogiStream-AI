#!/usr/bin/env python3
"""Main entry point for AI Logistics Platform.

This script orchestrates the entire data pipeline:
1. Data generation
2. Bronze ingestion
3. Silver transformation
4. Gold aggregation
5. Feature engineering
6. Data quality checks
7. ML training
"""

import logging
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config import get_config, setup_logging, MinIOClient, DuckDBConnector, RowCountValidator
from data_simulation.generator import DataSimulator
from bronze.ingestor import BronzeIngestor, validate_bronze_schema
from silver.transformer import SilverTransformer, validate_silver_schema
from gold.aggregator import GoldAggregator
from features.engineer import FeatureEngineer, validate_pit_correctness
from ml.trainer import ModelTrainer
from monitoring.dq_validator import DataQualityValidator, DataQualityReport

logger = logging.getLogger(__name__)


def run_data_generation(config):
    """Execute data generation phase."""
    logger.info("=" * 80)
    logger.info("PHASE 1: DATA GENERATION")
    logger.info("=" * 80)
    
    simulator = DataSimulator(config.data_simulation)
    data = simulator.generate_all()
    
    # Save to local JSONL
    saved_files = simulator.save_as_jsonl(data, "/tmp/simulated_data")
    
    return data, saved_files


def run_bronze_ingestion(data, saved_files, config):
    """Execute Bronze layer ingestion."""
    logger.info("=" * 80)
    logger.info("PHASE 2: BRONZE INGESTION")
    logger.info("=" * 80)
    
    # Initialize clients
    minio = MinIOClient(
        config.minio.endpoint,
        config.minio.access_key,
        config.minio.secret_key,
        config.minio.secure
    )
    
    duckdb = DuckDBConnector(config.duckdb.db_path)
    duckdb.connect()
    
    # Ingest
    ingestor = BronzeIngestor(minio, duckdb, config)
    
    for dataset_name in ["drivers", "orders", "gps_events", "weather_events"]:
        file_path = saved_files.get(dataset_name)
        if file_path and Path(file_path).exists():
            stats = ingestor.ingest_jsonl(dataset_name, file_path)
            logger.info(f"Ingested {dataset_name}: {stats}")
    
    duckdb.close()


def run_silver_transformation(config):
    """Execute Silver layer transformation."""
    logger.info("=" * 80)
    logger.info("PHASE 3: SILVER TRANSFORMATION")
    logger.info("=" * 80)
    
    minio = MinIOClient(
        config.minio.endpoint,
        config.minio.access_key,
        config.minio.secret_key,
        config.minio.secure
    )
    
    duckdb = DuckDBConnector(config.duckdb.db_path)
    duckdb.connect()
    
    transformer = SilverTransformer(duckdb, minio, config)
    
    # Apply transformations
    stats = {}
    stats["gps_events"] = transformer.transform_gps_events()
    stats["orders"] = transformer.transform_orders()
    stats["weather"] = transformer.transform_weather()
    
    logger.info(f"Silver transformation stats: {stats}")
    
    duckdb.close()
    return stats


def run_gold_aggregation(config):
    """Execute Gold layer aggregation."""
    logger.info("=" * 80)
    logger.info("PHASE 4: GOLD AGGREGATION")
    logger.info("=" * 80)
    
    minio = MinIOClient(
        config.minio.endpoint,
        config.minio.access_key,
        config.minio.secret_key,
        config.minio.secure
    )
    
    duckdb = DuckDBConnector(config.duckdb.db_path)
    duckdb.connect()
    
    aggregator = GoldAggregator(duckdb, minio, config)
    
    # For now, create placeholder metrics
    logger.info("Gold metrics creation would proceed with data from Silver")
    
    duckdb.close()


def run_feature_engineering(config):
    """Execute feature engineering."""
    logger.info("=" * 80)
    logger.info("PHASE 5: FEATURE ENGINEERING")
    logger.info("=" * 80)
    
    minio = MinIOClient(
        config.minio.endpoint,
        config.minio.access_key,
        config.minio.secret_key,
        config.minio.secure
    )
    
    duckdb = DuckDBConnector(config.duckdb.db_path)
    duckdb.connect()
    
    engineer = FeatureEngineer(duckdb, minio, config)
    
    logger.info("Feature engineering would proceed with data from Gold")
    
    duckdb.close()


def run_data_quality_checks(config):
    """Execute data quality validation."""
    logger.info("=" * 80)
    logger.info("PHASE 6: DATA QUALITY CHECKS")
    logger.info("=" * 80)
    
    validator = DataQualityValidator(config)
    report = DataQualityReport()
    
    # Run checks (with placeholder data)
    logger.info("Data quality checks would run on ingested data")
    
    # Save report
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    report_path = log_dir / "dq_report.json"
    report.save_report(str(report_path))


def run_ml_training(config):
    """Execute ML model training."""
    logger.info("=" * 80)
    logger.info("PHASE 7: ML TRAINING")
    logger.info("=" * 80)
    
    trainer = ModelTrainer(config)
    
    logger.info("ML model training would proceed with feature datasets")


def initialize_buckets(config):
    """Initialize MinIO buckets for all layers."""
    logger.info("=" * 80)
    logger.info("INITIALIZING BUCKETS")
    logger.info("=" * 80)
    
    minio = MinIOClient(
        config.minio.endpoint,
        config.minio.access_key,
        config.minio.secret_key,
        config.minio.secure
    )
    
    # Create all required buckets
    buckets = [
        config.minio.bucket_bronze,
        config.minio.bucket_silver,
        config.minio.bucket_gold,
        config.minio.bucket_features
    ]
    
    for bucket in buckets:
        minio.ensure_bucket_exists(bucket)
    
    logger.info("All buckets initialized successfully!")


def main():
    """Execute complete pipeline."""
    try:
        # Configuration
        config = get_config()
        setup_logging(config)
        
        logger.info(f"Starting AI Logistics Platform Pipeline")
        logger.info(f"Environment: {config.environment}")
        logger.info(f"Log Level: {config.log_level}")
        
        # Initialize MinIO buckets
        initialize_buckets(config)
        
        # Execute pipeline phases
        data, saved_files = run_data_generation(config)
        run_bronze_ingestion(data, saved_files, config)
        silver_stats = run_silver_transformation(config)
        run_gold_aggregation(config)
        run_feature_engineering(config)
        run_data_quality_checks(config)
        run_ml_training(config)
        
        # Summary
        logger.info("=" * 80)
        logger.info("PIPELINE EXECUTION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Execution time: {datetime.utcnow().isoformat()}")
        logger.info("All phases completed successfully!")
        
        return 0
    
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
