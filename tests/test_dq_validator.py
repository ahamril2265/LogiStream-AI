"""Unit tests for data quality monitoring."""

import pytest
import pandas as pd
import numpy as np
from monitoring.dq_validator import DataQualityValidator, DataQualityReport
from datetime import datetime


@pytest.fixture
def dq_validator(config):
    """Provide DQ validator."""
    return DataQualityValidator(config)


@pytest.fixture
def sample_df():
    """Provide sample DataFrame."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "value": [10.0, 20.0, None, 40.0, 50.0],
        "timestamp": pd.date_range("2024-01-01", periods=5),
    })


def test_row_count_validation_pass(dq_validator, sample_df):
    """Test row count validation passes."""
    result = dq_validator.validate_row_count(
        sample_df,
        stage="test_stage",
        expected_min=4,
        expected_max=6
    )
    
    assert result["status"] == "PASS"
    assert result["row_count"] == 5


def test_row_count_validation_fail_min(dq_validator, sample_df):
    """Test row count validation fails on minimum."""
    result = dq_validator.validate_row_count(
        sample_df,
        stage="test_stage",
        expected_min=10
    )
    
    assert result["status"] == "FAIL"


def test_row_count_validation_fail_max(dq_validator, sample_df):
    """Test row count validation fails on maximum."""
    result = dq_validator.validate_row_count(
        sample_df,
        stage="test_stage",
        expected_max=3
    )
    
    assert result["status"] == "FAIL"


def test_null_percentage_validation(dq_validator, sample_df):
    """Test null percentage validation."""
    result = dq_validator.validate_null_percentage(
        sample_df,
        stage="test_stage",
        threshold=0.5
    )
    
    assert result["status"] == "PASS"
    assert "value" in result["null_stats"]
    # Column 'value' has 1 null out of 5
    assert result["null_stats"]["value"]["null_pct"] == 0.2


def test_data_freshness_validation(dq_validator):
    """Test data freshness validation."""
    df = pd.DataFrame({
        "timestamp": [datetime.utcnow()] * 5,
    })
    
    result = dq_validator.validate_data_freshness(
        df,
        timestamp_col="timestamp",
        stage="test_stage",
        max_age_hours=1
    )
    
    assert result["status"] == "PASS"
    assert result["age_hours"] < 1


def test_dq_report():
    """Test DQ report aggregation."""
    report = DataQualityReport()
    
    # Add some checks
    report.add_check({"check": "test1", "status": "PASS"})
    report.add_check({"check": "test2", "status": "PASS"})
    report.add_check({"check": "test3", "status": "WARN"})
    
    summary = report.get_summary()
    
    assert summary["total_checks"] == 3
    assert summary["passed"] == 2
    assert summary["warned"] == 1
    assert summary["overall_status"] == "WARN"


def test_dq_report_with_failure():
    """Test DQ report with failures."""
    report = DataQualityReport()
    
    report.add_check({"check": "test1", "status": "PASS"})
    report.add_check({"check": "test2", "status": "FAIL"})
    
    summary = report.get_summary()
    
    assert summary["failed"] == 1
    assert summary["overall_status"] == "FAIL"
