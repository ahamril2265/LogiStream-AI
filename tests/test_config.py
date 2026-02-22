"""Unit tests for configuration module."""

import pytest
from config import get_config, MinIOConfig, PostgresConfig


def test_config_initialization():
    """Test that configuration initializes correctly."""
    config = get_config()
    
    assert config is not None
    assert config.minio is not None
    assert config.postgres is not None
    assert config.duckdb is not None
    assert config.data_simulation is not None


def test_minio_config():
    """Test MinIO configuration."""
    minio_cfg = MinIOConfig()
    
    assert minio_cfg.endpoint is not None
    assert minio_cfg.access_key is not None
    assert minio_cfg.secret_key is not None
    assert len(minio_cfg.bucket_bronze) > 0


def test_postgres_connection_string():
    """Test PostgreSQL connection string generation."""
    postgres_cfg = PostgresConfig()
    conn_str = postgres_cfg.connection_string
    
    assert "postgresql://" in conn_str
    assert "@" in conn_str
    assert ":" in conn_str


def test_data_simulation_config():
    """Test data simulation configuration."""
    config = get_config()
    sim_cfg = config.data_simulation
    
    assert sim_cfg.num_drivers == 500
    assert sim_cfg.num_deliveries == 100000
    assert sim_cfg.sim_years == 2
    assert sim_cfg.gps_events_per_delivery == 25
    assert 0 <= sim_cfg.late_event_ratio <= 1
    assert 0 <= sim_cfg.duplicate_event_ratio <= 1
