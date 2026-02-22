"""Unit tests for utilities module."""

import pytest
import pandas as pd
from config.utils import (
    RowCountValidator,
    generate_data_signature,
    get_partition_date_path,
)
from datetime import datetime


def test_row_count_validator():
    """Test row count validator."""
    validator = RowCountValidator()
    
    validator.record("stage1", 1000)
    validator.record("stage2", 900)
    
    assert validator.get_report()["stage1"] == 1000
    assert validator.get_report()["stage2"] == 900
    
    # Within threshold
    is_valid = validator.validate_lineage("stage1", "stage2", variance_threshold=0.2)
    assert is_valid
    
    # Exceeds threshold
    is_valid = validator.validate_lineage("stage1", "stage2", variance_threshold=0.05)
    assert not is_valid


def test_generate_data_signature():
    """Test data signature generation."""
    data1 = {"name": "order1", "value": 100}
    data2 = {"name": "order1", "value": 100}
    data3 = {"name": "order2", "value": 100}
    
    sig1 = generate_data_signature(data1)
    sig2 = generate_data_signature(data2)
    sig3 = generate_data_signature(data3)
    
    # Same data should produce same signature
    assert sig1 == sig2
    
    # Different data should produce different signature
    assert sig1 != sig3
    
    # Signature should be deterministic
    assert isinstance(sig1, str)
    assert len(sig1) > 0


def test_get_partition_date_path():
    """Test partition path generation."""
    date = datetime(2024, 1, 15)
    
    # Full path
    path = get_partition_date_path(date, "year/month/day")
    assert path == "year=2024/month=01/day=15"
    
    # Year only
    path = get_partition_date_path(date, "year")
    assert path == "year=2024"
    
    # Year and month
    path = get_partition_date_path(date, "year/month")
    assert path == "year=2024/month=01"


def test_row_count_validator_lineage_report():
    """Test row count validator report."""
    validator = RowCountValidator()
    
    validator.record("bronze_orders", 100000)
    validator.record("silver_orders", 99998)
    validator.record("gold_deliveries", 99900)
    
    report = validator.get_report()
    
    assert len(report) == 3
    assert report["bronze_orders"] == 100000
    assert report["silver_orders"] == 99998
    assert report["gold_deliveries"] == 99900
