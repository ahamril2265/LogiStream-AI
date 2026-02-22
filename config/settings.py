"""
Configuration management for AI Logistics Platform.

All configuration is managed through environment variables with sensible defaults.
For production, use a secrets management system (e.g., HashiCorp Vault).
"""

import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class MinIOConfig:
    """MinIO object storage configuration."""
    endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key: str = os.getenv("MINIO_ROOT_USER", "minioadmin")
    secret_key: str = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    bucket_bronze: str = os.getenv("MINIO_BUCKET_BRONZE", "logistics-bronze")
    bucket_silver: str = os.getenv("MINIO_BUCKET_SILVER", "logistics-silver")
    bucket_gold: str = os.getenv("MINIO_BUCKET_GOLD", "logistics-gold")
    bucket_features: str = os.getenv("MINIO_BUCKET_FEATURES", "logistics-features")
    secure: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"


@dataclass
class PostgresConfig:
    """PostgreSQL database configuration."""
    host: str = os.getenv("POSTGRES_HOST", "postgres")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    user: str = os.getenv("POSTGRES_USER", "postgres")
    password: str = os.getenv("POSTGRES_PASSWORD", "postgres")
    db_name: str = os.getenv("POSTGRES_DB", "logistics_db")
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"


@dataclass
class AirflowConfig:
    """Airflow configuration."""
    dags_folder: str = "/opt/airflow/dags"
    logs_folder: str = "/opt/airflow/logs"
    plugins_folder: str = "/opt/airflow/plugins"
    base_log_folder: str = "/opt/airflow/logs"
    executor: str = os.getenv("AIRFLOW_EXECUTOR", "LocalExecutor")
    sql_alchemy_conn: Optional[str] = None  # Set during initialization


@dataclass
class MLflowConfig:
    """MLflow configuration."""
    tracking_uri: str = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    artifact_location: str = os.getenv("MLFLOW_ARTIFACT_LOCATION", "/mlflow/artifacts")
    backend_store_uri: Optional[str] = None  # Set during initialization
    experiment_name: str = os.getenv("MLFLOW_EXPERIMENT_NAME", "logistics-ml")


@dataclass
class DataSimulationConfig:
    """Data simulation configuration."""
    # Scale
    num_drivers: int = int(os.getenv("NUM_DRIVERS", "500"))
    num_deliveries: int = int(os.getenv("NUM_DELIVERIES", "100000"))
    sim_years: int = int(os.getenv("SIM_YEARS", "2"))
    
    # GPS events: realistic 2-5M based on frequency
    gps_events_per_delivery: int = int(os.getenv("GPS_EVENTS_PER_DELIVERY", "25"))
    
    # Late data and duplicates
    late_event_ratio: float = float(os.getenv("LATE_EVENT_RATIO", "0.08"))  # 5-15% → 8% avg
    late_event_delay_min: int = int(os.getenv("LATE_EVENT_DELAY_MIN", "5"))  # minutes
    late_event_delay_max: int = int(os.getenv("LATE_EVENT_DELAY_MAX", "15"))  # minutes
    duplicate_event_ratio: float = float(os.getenv("DUPLICATE_EVENT_RATIO", "0.02"))  # 1-3% → 2% avg
    out_of_order_ratio: float = float(os.getenv("OUT_OF_ORDER_RATIO", "0.01"))  # 1% out of order
    
    # Output
    output_batch_size: int = int(os.getenv("OUTPUT_BATCH_SIZE", "10000"))
    seed: int = int(os.getenv("RANDOM_SEED", "42"))


@dataclass
class DuckDBConfig:
    """DuckDB configuration for local transformations."""
    db_path: str = os.getenv("DUCKDB_PATH", "./data/logistics.duckdb")
    max_memory: str = "4GB"  # DuckDB memory limit
    threads: int = int(os.getenv("DUCKDB_THREADS", "4"))


@dataclass
class DataLakeConfig:
    """Data Lake configuration."""
    # Partitioning
    partition_depth: str = "year/month/day"  # Year/Month/Day partitioning
    retention_days: int = int(os.getenv("RETENTION_DAYS", "730"))  # 2 years
    
    # Formats
    storage_format: str = "parquet"  # Primary format
    
    # Data quality
    null_check_threshold: float = float(os.getenv("NULL_CHECK_THRESHOLD", "0.05"))  # 5% nulls threshold
    row_count_variance_threshold: float = float(os.getenv("ROW_COUNT_VARIANCE", "0.1"))  # 10% variance
    
    # Feature engineering
    point_in_time_window_days: int = int(os.getenv("PIT_WINDOW_DAYS", "90"))


@dataclass
class Config:
    """Master configuration aggregator."""
    minio: MinIOConfig
    postgres: PostgresConfig
    airflow: AirflowConfig
    mlflow: MLflowConfig
    duckdb: DuckDBConfig
    data_simulation: DataSimulationConfig
    datalake: DataLakeConfig
    
    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    
    # Execution mode
    environment: str = os.getenv("ENVIRONMENT", "development")
    debug: bool = os.getenv("DEBUG", "false").lower() == "true"


def get_config() -> Config:
    """Get global configuration object.
    
    TODO (Production):
    - Load secrets from HashiCorp Vault
    - Add configuration validation
    - Support config file (YAML/TOML)
    - Add schema versioning
    """
    # Initialize derived configs
    airflow_cfg = AirflowConfig()
    postgres_cfg = PostgresConfig()
    airflow_cfg.sql_alchemy_conn = postgres_cfg.connection_string
    
    mlflow_cfg = MLflowConfig()
    mlflow_cfg.backend_store_uri = postgres_cfg.connection_string
    
    return Config(
        minio=MinIOConfig(),
        postgres=postgres_cfg,
        airflow=airflow_cfg,
        mlflow=mlflow_cfg,
        duckdb=DuckDBConfig(),
        data_simulation=DataSimulationConfig(),
        datalake=DataLakeConfig(),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        environment=os.getenv("ENVIRONMENT", "development"),
        debug=os.getenv("DEBUG", "false").lower() == "true",
    )


def setup_logging(config: Config) -> None:
    """Configure logging for the application."""
    from pathlib import Path
    
    # Create logs directory in project root (not system root)
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_level = getattr(logging, config.log_level)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_dir / "platform.log"),
            logging.StreamHandler(),
        ],
    )
