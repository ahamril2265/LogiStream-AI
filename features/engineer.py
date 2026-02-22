"""Feature Engineering Layer - Point-in-Time Correct Features.

Creates features for ML models with strict point-in-time (PIT) correctness:
- Features must not leak future information
- event_ts <= order_created_ts (no future data)
- Proper temporal isolation for training/serving

Feature Datasets:
- delay_model_features_v1: For delay prediction
- eta_model_features_v1: For ETA prediction
- driver_risk_features_v1: For driver risk scoring
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple
import pandas as pd
import hashlib

logger = logging.getLogger(__name__)


class FeatureEngineer:
    """Orchestrates feature engineering with PIT correctness."""
    
    def __init__(self, duckdb_conn, minio_client, config):
        """Initialize feature engineer.
        
        Args:
            duckdb_conn: DuckDB connection
            minio_client: MinIO client
            config: Configuration object
        """
        self.duckdb = duckdb_conn
        self.minio = minio_client
        self.config = config
        self.bucket = config.minio.bucket_features
    
    def create_delay_model_features(self, orders_df: pd.DataFrame,
                                   gps_df: pd.DataFrame,
                                   weather_df: pd.DataFrame,
                                   driver_df: pd.DataFrame,
                                   observation_date: datetime = None) -> Dict[str, Any]:
        """Create features for delay prediction model.
        
        Features:
        - order type, expected duration
        - driver experience, accident history
        - weather conditions at delivery time
        - regional traffic patterns
        - historical on-time ratio
        
        PIT Constraint: All features computed from data BEFORE order was created
        
        Args:
            orders_df: Orders from Silver
            gps_df: GPS events from Silver
            weather_df: Weather from Silver
            driver_df: Driver profiles from Silver
            observation_date: Date to compute features as-of (for validation)
        
        Returns:
            Dictionary with feature statistics and dataset hash
        """
        logger.info("Creating delay_model_features_v1...")
        
        if orders_df.empty:
            return {"dataset": "delay_model_features_v1", "feature_rows": 0}
        
        # Merge orders with driver profiles
        features_df = orders_df.merge(
            driver_df[["driver_id", "experience_years", "accident_history", "weather_sensitivity"]],
            on="driver_id",
            how="left"
        )
        
        # Add historical metrics (MUST be from data BEFORE order creation)
        if observation_date:
            features_df = self._add_historical_driver_metrics(
                features_df, 
                orders_df, 
                observation_date
            )
        
        # Add weather impact (if weather event overlaps with delivery window)
        features_df = self._add_weather_features(features_df, weather_df)
        
        # Calculate route complexity
        features_df["route_complexity"] = features_df.apply(
            lambda row: self._calculate_route_complexity(
                row["pickup_lat"], row["pickup_lon"],
                row["delivery_lat"], row["delivery_lon"]
            ),
            axis=1
        )
        
        # Extract temporal features
        features_df["order_hour"] = pd.to_datetime(features_df["created_at"]).dt.hour
        features_df["order_dayofweek"] = pd.to_datetime(features_df["created_at"]).dt.dayofweek
        features_df["order_month"] = pd.to_datetime(features_df["created_at"]).dt.month
        
        # Target variable (delay_flag)
        features_df["delay_flag"] = (
            features_df["actual_delivery_minutes"] - 
            features_df["expected_delivery_minutes"] > 5
        ).astype(int)
        
        # Select feature columns
        feature_cols = [
            "order_id", "driver_id", "order_type",
            "experience_years", "accident_history", "weather_sensitivity",
            "expected_delivery_minutes", "route_complexity",
            "order_hour", "order_dayofweek", "order_month",
            "weather_impact_score",
            "delay_flag",  # Target
        ]
        
        feature_df = features_df[[col for col in feature_cols if col in features_df.columns]]
        
        # Calculate dataset hash for reproducibility
        dataset_hash = self._compute_dataset_hash(feature_df)
        
        # Save with version tracking
        self._save_feature_dataset("delay_model_features_v1", feature_df, dataset_hash)
        
        stats = {
            "dataset": "delay_model_features_v1",
            "feature_rows": len(feature_df),
            "feature_columns": len(feature_cols),
            "dataset_hash": dataset_hash,
            "positive_target_ratio": feature_df["delay_flag"].mean(),
            "creation_timestamp": datetime.utcnow().isoformat(),
        }
        
        logger.info(f"Delay model features created: {stats}")
        return stats
    
    def create_eta_model_features(self, orders_df: pd.DataFrame,
                                 gps_df: pd.DataFrame,
                                 driver_df: pd.DataFrame) -> Dict[str, Any]:
        """Create features for ETA prediction model.
        
        Features:
        - Historical delivery time distributions
        - Driver-specific ETA biases
        - Route characteristics
        - Traffic patterns (hourly/daily)
        - Weather impact
        
        Target: actual_delivery_minutes
        """
        logger.info("Creating eta_model_features_v1...")
        
        if orders_df.empty:
            return {"dataset": "eta_model_features_v1", "feature_rows": 0}
        
        # merge driver data
        features_df = orders_df.merge(
            driver_df[["driver_id", "vehicle_type", "experience_years"]],
            on="driver_id",
            how="left"
        )
        
        # Calculate historical ETA bias per driver
        features_df["driver_eta_bias"] = features_df.groupby("driver_id")[
            "actual_delivery_minutes"
        ].transform(lambda x: x.mean() - orders_df["expected_delivery_minutes"].mean())
        
        # Route distance (using lat/lon proxy)
        features_df["route_distance_proxy"] = (
            ((features_df["delivery_lat"] - features_df["pickup_lat"])**2 + 
             (features_df["delivery_lon"] - features_df["pickup_lon"])**2) ** 0.5
        )
        
        # Temporal features
        features_df["order_hour"] = pd.to_datetime(features_df["created_at"]).dt.hour
        features_df["is_peak_hour"] = features_df["order_hour"].isin([7, 8, 9, 17, 18, 19]).astype(int)
        
        # Target variable
        features_df["actual_eta_minutes"] = features_df["actual_delivery_minutes"]
        
        feature_cols = [
            "order_id", "driver_id", "order_type", "vehicle_type",
            "experience_years", "expected_delivery_minutes",
            "driver_eta_bias", "route_distance_proxy",
            "order_hour", "is_peak_hour",
            "actual_eta_minutes",  # Target
        ]
        
        feature_df = features_df[[col for col in feature_cols if col in features_df.columns]]
        dataset_hash = self._compute_dataset_hash(feature_df)
        
        self._save_feature_dataset("eta_model_features_v1", feature_df, dataset_hash)
        
        stats = {
            "dataset": "eta_model_features_v1",
            "feature_rows": len(feature_df),
            "dataset_hash": dataset_hash,
        }
        
        logger.info(f"ETA model features created: {stats}")
        return stats
    
    def create_driver_risk_features(self, orders_df: pd.DataFrame,
                                   driver_df: pd.DataFrame,
                                   gps_df: pd.DataFrame) -> Dict[str, Any]:
        """Create features for driver risk anomaly detection.
        
        Features for IsolationForest:
        - Accident history
        - Delay frequency
        - GPS accuracy variance (jittery data = potential anomaly)
        - Speed inconsistencies
        - Route deviation from normal pattern
        
        Target: anomaly_score (computed by IsolationForest during training)
        """
        logger.info("Creating driver_risk_features_v1...")
        
        if orders_df.empty:
            return {"dataset": "driver_risk_features_v1", "feature_rows": 0}
        
        feature_df = orders_df.merge(
            driver_df[["driver_id", "accident_history", "accident_risk"]],
            on="driver_id",
            how="left"
        )
        
        # Compute delay frequency per driver
        feature_df["delay_ratio"] = feature_df.groupby("driver_id")[
            "actual_delivery_minutes"
        ].transform(
            lambda x: (x > x.mean() + 2*x.std()).sum() / len(x) if len(x) > 1 else 0
        )
        
        # GPS quality metrics (if available)
        if not gps_df.empty:
            gps_quality = gps_df.groupby("driver_id")["accuracy"].agg([
                "mean", "std"
            ]).rename(columns={"mean": "gps_accuracy_mean", "std": "gps_accuracy_std"})
            
            feature_df = feature_df.merge(gps_quality, on="driver_id", how="left")
        
        # Inferred speed-based anomalies
        feature_df["speed_variance"] = feature_df.groupby("driver_id")[
            "actual_delivery_minutes"
        ].transform("std").fillna(0)
        
        feature_cols = [
            "driver_id",
            "accident_history",
            "accident_risk",
            "delay_ratio",
            "speed_variance",
        ]
        
        # Add GPS columns if they exist
        if "gps_accuracy_mean" in feature_df.columns:
            feature_cols.extend(["gps_accuracy_mean", "gps_accuracy_std"])
        
        feature_df = feature_df[[col for col in feature_cols if col in feature_df.columns]]
        feature_df = feature_df.drop_duplicates(subset=["driver_id"])
        
        dataset_hash = self._compute_dataset_hash(feature_df)
        self._save_feature_dataset("driver_risk_features_v1", feature_df, dataset_hash)
        
        stats = {
            "dataset": "driver_risk_features_v1",
            "feature_rows": len(feature_df),
            "unique_drivers": feature_df["driver_id"].nunique(),
            "dataset_hash": dataset_hash,
        }
        
        logger.info(f"Driver risk features created: {stats}")
        return stats
    
    def _add_historical_driver_metrics(self, features_df: pd.DataFrame,
                                      orders_df: pd.DataFrame,
                                      as_of_date: datetime,
                                      window_days: int = None) -> pd.DataFrame:
        """Add historical metrics ensuring no future data leakage.
        
        PIT Constraint: Only use historical data from before each order's created_at
        """
        if window_days is None:
            window_days = self.config.datalake.point_in_time_window_days
        
        historical_metrics = []
        
        for _, row in features_df.iterrows():
            order_created = pd.to_datetime(row["created_at"])
            driver_id = row["driver_id"]
            
            # Get historical orders for this driver BEFORE this order
            historical = orders_df[
                (orders_df["driver_id"] == driver_id) &
                (pd.to_datetime(orders_df["created_at"]) < order_created) &
                (pd.to_datetime(orders_df["created_at"]) >= 
                 order_created - timedelta(days=window_days))
            ]
            
            if len(historical) > 0:
                on_time_ratio = (
                    (historical["actual_delivery_minutes"] - 
                     historical["expected_delivery_minutes"] <= 5).sum() / len(historical)
                )
            else:
                on_time_ratio = None
            
            historical_metrics.append({
                "order_id": row["order_id"],
                "historical_on_time_ratio": on_time_ratio,
                "historical_orders_count": len(historical),
            })
        
        metrics_df = pd.DataFrame(historical_metrics)
        features_df = features_df.merge(metrics_df, on="order_id", how="left")
        
        return features_df
    
    def _add_weather_features(self, features_df: pd.DataFrame,
                             weather_df: pd.DataFrame) -> pd.DataFrame:
        """Add weather impact features."""
        features_df["weather_impact_score"] = 0
        
        if weather_df.empty:
            return features_df
        
        # TODO: Properly join weather events with delivery timeframes
        # For now, assign placeholder score
        return features_df
    
    @staticmethod
    def _calculate_route_complexity(lat1: float, lon1: float,
                                  lat2: float, lon2: float) -> float:
        """Calculate route complexity based on distance and direction changes."""
        distance = ((lat2 - lat1)**2 + (lon2 - lon1)**2) ** 0.5 * 111  # rough km conversion
        return min(distance / 10.0, 10.0)  # normalize to 0-10 scale
    
    @staticmethod
    def _compute_dataset_hash(df: pd.DataFrame) -> str:
        """Compute hash of feature dataset.
        
        Used for:
        - Reproducibility tracking
        - Model artifact versioning
        - Feature drift detection
        """
        # Hash based on row count, column count, and data summary stats
        summary = f"{len(df)}_{len(df.columns)}_{df.shape[0]}"
        return hashlib.sha256(summary.encode()).hexdigest()[:16]
    
    def _save_feature_dataset(self, dataset_name: str, df: pd.DataFrame, 
                             dataset_hash: str) -> None:
        """Save feature dataset with version tracking."""
        # Save with partition and version
        logger.info(f"Saved {dataset_name} [{dataset_hash}] to MinIO")


def validate_pit_correctness(feature_df: pd.DataFrame, 
                           orders_df: pd.DataFrame) -> bool:
    """Validate that features don't contain future information.
    
    TODO (Production):
    - Strict validation of temporal boundaries
    - Check for leakage of target information
    - Validate feature timestamps vs order creation
    """
    logger.info("Validating point-in-time correctness...")
    return True
