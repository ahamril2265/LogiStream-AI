"""Data Quality and Observability Module.

Implements comprehensive data quality checks:
- Row count validation  
- Schema validation
- Null percentage checks
- Feature drift detection (KS test)
- Late arrival ratio monitoring
- Data freshness checks
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
from scipy import stats

logger = logging.getLogger(__name__)


class DataQualityValidator:
    """Comprehensive data quality validation framework."""
    
    def __init__(self, config):
        """Initialize DQ validator.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.dq_report = {}
    
    def validate_row_count(self, df: pd.DataFrame, stage: str,
                          expected_min: Optional[int] = None,
                          expected_max: Optional[int] = None) -> Dict[str, Any]:
        """Validate row count is within expected range.
        
        Args:
            df: DataFrame to validate
            stage: Data pipeline stage (e.g., 'bronze_orders')
            expected_min: Minimum expected rows
            expected_max: Maximum expected rows
        
        Returns:
            Validation result and statistics
        """
        row_count = len(df)
        
        result = {
            "check": "row_count",
            "stage": stage,
            "row_count": row_count,
            "status": "PASS",
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        if expected_min is not None and row_count < expected_min:
            result["status"] = "FAIL"
            result["message"] = f"Row count {row_count} below minimum {expected_min}"
        
        if expected_max is not None and row_count > expected_max:
            result["status"] = "FAIL"
            result["message"] = f"Row count {row_count} above maximum {expected_max}"
        
        logger.info(f"Row count validation - {stage}: {row_count:,} rows [{result['status']}]")
        return result
    
    def validate_schema(self, df: pd.DataFrame, expected_schema: Dict[str, str],
                       stage: str) -> Dict[str, Any]:
        """Validate DataFrame schema against expected.
        
        Args:
            df: DataFrame to validate
            expected_schema: Dict mapping column names to expected types
            stage: Pipeline stage
        
        Returns:
            Validation result
        """
        result = {
            "check": "schema",
            "stage": stage,
            "status": "PASS",
            "missing_columns": [],
            "unexpected_columns": [],
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        # Check for missing columns
        for col in expected_schema:
            if col not in df.columns:
                result["missing_columns"].append(col)
                result["status"] = "FAIL"
        
        # Check for unexpected columns (warning only)
        expected_cols = set(expected_schema.keys())
        actual_cols = set(df.columns)
        
        unexpected = actual_cols - expected_cols
        if unexpected:
            result["unexpected_columns"] = list(unexpected)
            logger.warning(f"Unexpected columns in {stage}: {unexpected}")
        
        if result["status"] == "PASS":
            logger.info(f"Schema validation passed - {stage}")
        else:
            logger.error(f"Schema validation failed - {stage}: {result}")
        
        return result
    
    def validate_null_percentage(self, df: pd.DataFrame, 
                                stage: str,
                                threshold: Optional[float] = None) -> Dict[str, Any]:
        """Validate null percentages don't exceed threshold.
        
        Args:
            df: DataFrame to validate
            stage: Pipeline stage
            threshold: Maximum allowed null ratio (default from config)
        
        Returns:
            Per-column null statistics and validation result
        """
        if threshold is None:
            threshold = self.config.datalake.null_check_threshold
        
        null_stats = {}
        violations = {}
        
        for col in df.columns:
            null_count = df[col].isnull().sum()
            null_pct = null_count / len(df)
            null_stats[col] = {"null_count": int(null_count), "null_pct": round(null_pct, 4)}
            
            if null_pct > threshold:
                violations[col] = null_pct
        
        result = {
            "check": "null_percentage",
            "stage": stage,
            "columns_checked": len(df.columns),
            "threshold": threshold,
            "violations": violations,
            "null_stats": null_stats,
            "status": "PASS" if not violations else "FAIL",
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        if violations:
            logger.warning(
                f"Null percentage violations in {stage}: {violations}"
            )
        else:
            logger.info(f"Null percentage validation passed - {stage}")
        
        return result
    
    def detect_feature_drift(self, current_df: pd.DataFrame,
                            baseline_df: pd.DataFrame,
                            numeric_columns: List[str],
                            stage: str,
                            alpha: float = 0.05) -> Dict[str, Any]:
        """Detect feature drift using Kolmogorov-Smirnov test.
        
        KS test compares distributions:
        - H0: distributions are identical
        - If p-value < alpha, reject H0 (drift detected)
        
        Args:
            current_df: Current feature data
            baseline_df: Baseline/training data
            numeric_columns: Columns to check for drift
            stage: Pipeline stage
            alpha: Significance level (default 5%)
        
        Returns:
            KS test results for each column
        """
        logger.info(f"Starting feature drift detection - {stage}")
        
        drift_results = {}
        drifted_columns = []
        
        for col in numeric_columns:
            if col not in current_df.columns or col not in baseline_df.columns:
                continue
            
            # Handle missing values
            current_values = current_df[col].dropna()
            baseline_values = baseline_df[col].dropna()
            
            if len(current_values) == 0 or len(baseline_values) == 0:
                continue
            
            # Perform KS test
            ks_stat, p_value = stats.ks_2samp(baseline_values, current_values)
            
            is_drifted = p_value < alpha
            drift_results[col] = {
                "ks_statistic": float(ks_stat),
                "p_value": float(p_value),
                "is_drifted": bool(is_drifted),
                "baseline_mean": float(baseline_values.mean()),
                "current_mean": float(current_values.mean()),
                "baseline_std": float(baseline_values.std()),
                "current_std": float(current_values.std()),
            }
            
            if is_drifted:
                drifted_columns.append(col)
        
        result = {
            "check": "feature_drift",
            "stage": stage,
            "alpha": alpha,
            "columns_checked": len(numeric_columns),
            "drifted_columns": drifted_columns,
            "drift_results": drift_results,
            "status": "PASS" if not drifted_columns else "WARN",
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        if drifted_columns:
            logger.warning(
                f"Feature drift detected in {stage}: {drifted_columns}"
            )
        else:
            logger.info(f"No feature drift detected - {stage}")
        
        return result
    
    def validate_late_arrival_ratio(self, gps_df: pd.DataFrame,
                                   orders_df: pd.DataFrame,
                                   threshold: float = 0.15) -> Dict[str, Any]:
        """Monitor late arrival ratio in GPS events.
        
        Late arrivals occur when GPS events arrive > 15 minutes after delivery.
        
        Args:
            gps_df: GPS events from Silver
            orders_df: Orders from Silver
            threshold: Maximum acceptable late ratio (15% default)
        
        Returns:
            Late arrival statistics
        """
        logger.info("Validating late arrival ratio...")
        
        if gps_df.empty or orders_df.empty:
            return {"status": "skipped", "reason": "no data"}
        
        # Merge to get delivery times
        merged = gps_df.merge(
            orders_df[["order_id", "created_at", "actual_delivery_minutes"]],
            on="order_id",
            how="left"
        )
        
        # Calculate expected delivery time
        merged["created_at"] = pd.to_datetime(merged["created_at"])
        merged["event_timestamp"] = pd.to_datetime(merged["event_timestamp"])
        merged["expected_delivery_ts"] = (
            merged["created_at"] + 
            pd.to_timedelta(merged["actual_delivery_minutes"], unit="minutes")
        )
        
        # Identify late events
        merged["is_late"] = merged["event_timestamp"] > merged["expected_delivery_ts"]
        
        late_ratio = merged["is_late"].mean()
        late_count = merged["is_late"].sum()
        
        result = {
            "check": "late_arrival_ratio",
            "late_count": int(late_count),
            "total_events": len(merged),
            "late_ratio": float(late_ratio),
            "threshold": threshold,
            "status": "PASS" if late_ratio <= threshold else "WARN",
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        if late_ratio > threshold:
            logger.warning(
                f"Late arrival ratio {late_ratio:.1%} exceeds threshold {threshold:.1%}"
            )
        else:
            logger.info(f"Late arrival ratio {late_ratio:.1%} within threshold")
        
        return result
    
    def validate_data_freshness(self, df: pd.DataFrame, 
                               timestamp_col: str,
                               stage: str,
                               max_age_hours: int = 24) -> Dict[str, Any]:
        """Validate that data is recent (not stale).
        
        Args:
            df: DataFrame to validate
            timestamp_col: Name of timestamp column
            stage: Pipeline stage
            max_age_hours: Maximum acceptable age in hours
        
        Returns:
            Freshness validation result
        """
        if timestamp_col not in df.columns or df.empty:
            return {"status": "skipped", "reason": "no timestamp data"}
        
        max_timestamp = pd.to_datetime(df[timestamp_col]).max()
        age_hours = (datetime.utcnow() - max_timestamp.replace(tzinfo=None)).total_seconds() / 3600
        
        result = {
            "check": "data_freshness",
            "stage": stage,
            "max_timestamp": max_timestamp.isoformat(),
            "age_hours": float(age_hours),
            "max_age_hours": max_age_hours,
            "status": "PASS" if age_hours <= max_age_hours else "WARN",
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        if age_hours > max_age_hours:
            logger.warning(f"Data in {stage} is {age_hours:.1f} hours old (threshold: {max_age_hours}h)")
        else:
            logger.info(f"Data freshness OK - {stage} ({age_hours:.1f}h old)")
        
        return result


class DataQualityReport:
    """Aggregates and reports data quality checks."""
    
    def __init__(self):
        self.checks: List[Dict[str, Any]] = []
    
    def add_check(self, result: Dict[str, Any]) -> None:
        """Add DQ check result to report."""
        self.checks.append(result)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all checks."""
        passed = sum(1 for c in self.checks if c.get("status") == "PASS")
        warned = sum(1 for c in self.checks if c.get("status") == "WARN")
        failed = sum(1 for c in self.checks if c.get("status") == "FAIL")
        
        return {
            "total_checks": len(self.checks),
            "passed": passed,
            "warned": warned,
            "failed": failed,
            "overall_status": "FAIL" if failed > 0 else ("WARN" if warned > 0 else "PASS"),
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    def save_report(self, file_path: str) -> None:
        """Save report to JSON file."""
        import json
        with open(file_path, "w") as f:
            json.dump({
                "summary": self.get_summary(),
                "checks": self.checks,
            }, f, indent=2)
        logger.info(f"Data quality report saved: {file_path}")
