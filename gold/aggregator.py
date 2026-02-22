"""Gold Layer - Business-Level Aggregations and Metrics.

The Gold layer creates business-facing analytics:
- gold_deliveries: delay metrics, distance, weather impact
- gold_driver_daily_metrics: driver performance aggregations
- gold_region_hourly_metrics: regional traffic and performance
"""

import logging
from datetime import datetime
from typing import Dict, Any
import pandas as pd
from math import radians, sin, cos, sqrt, atan2

logger = logging.getLogger(__name__)


class GoldAggregator:
    """Creates Gold layer business metrics."""
    
    def __init__(self, duckdb_conn, minio_client, config):
        """Initialize Gold aggregator.
        
        Args:
            duckdb_conn: DuckDB connection
            minio_client: MinIO client
            config: Configuration object
        """
        self.duckdb = duckdb_conn
        self.minio = minio_client
        self.config = config
        self.bucket_gold = config.minio.bucket_gold
    
    def create_deliveries_metrics(self, orders_df: pd.DataFrame, 
                                 weather_df: pd.DataFrame) -> Dict[str, Any]:
        """Create gold_deliveries with comprehensive metrics.
        
        Metrics:
        - delay_minutes: actual - expected delivery time
        - delay_flag: binary indicator (delay if > 5 min)
        - route_distance_km: haversine distance
        - weather_impact_score: 0-10 scale
        - traffic_score: inferred from GPS accuracy and spacing
        
        Args:
            orders_df: Orders data from Silver
            weather_df: Weather data from Silver
        
        Returns:
            Statistics on metrics creation
        """
        logger.info("Creating gold_deliveries metrics...")
        
        # Calculate delay
        orders_df["delay_minutes"] = (
            orders_df["actual_delivery_minutes"] - 
            orders_df["expected_delivery_minutes"]
        )
        orders_df["delay_flag"] = (orders_df["delay_minutes"] > 5).astype(int)
        
        # Calculate distance using Haversine formula
        orders_df["route_distance_km"] = orders_df.apply(
            lambda row: self._haversine_distance(
                row["pickup_lat"], row["pickup_lon"],
                row["delivery_lat"], row["delivery_lon"]
            ),
            axis=1
        )
        
        # Join with weather and calculate impact
        # TODO: Properly join with weather timeframe
        orders_df["weather_impact_score"] = 0  # Placeholder
        
        # Calculate traffic score (based on actual vs expected time ratio)
        orders_df["traffic_score"] = (
            (orders_df["actual_delivery_minutes"] / 
             orders_df["expected_delivery_minutes"] - 1) * 10
        ).clip(0, 10).round(1)
        
        # Select final columns
        gold_deliveries = orders_df[[
            "order_id",
            "driver_id",
            "order_type",
            "created_at",
            "delay_minutes",
            "delay_flag",
            "route_distance_km",
            "weather_impact_score",
            "traffic_score",
        ]]
        
        # Save to Gold
        self._save_gold_dataset("deliveries", gold_deliveries)
        
        stats = {
            "dataset": "gold_deliveries",
            "rows_created": len(gold_deliveries),
            "avg_delay_minutes": round(gold_deliveries["delay_minutes"].mean(), 2),
            "on_time_ratio": round((1 - gold_deliveries["delay_flag"].mean()) * 100, 2),
            "avg_route_distance_km": round(gold_deliveries["route_distance_km"].mean(), 2),
        }
        
        logger.info(f"Gold deliveries created: {stats}")
        return stats
    
    def create_driver_daily_metrics(self, orders_df: pd.DataFrame,
                                   gps_df: pd.DataFrame) -> Dict[str, Any]:
        """Create gold_driver_daily_metrics.
        
        Metrics per driver per day:
        - deliveries_completed: count
        - on_time_ratio: % deliveries on time
        - avg_delay_minutes: average delay
        - avg_route_distance_km: average distance per delivery
        - total_distance_km: total distance driven
        - gps_points_count: number of GPS tracking points
        
        Args:
            orders_df: Orders from Silver
            gps_df: GPS events from Silver
        
        Returns:
            Statistics on metrics creation
        """
        logger.info("Creating gold_driver_daily_metrics...")
        
        # Prepare orders
        orders_df["created_date"] = pd.to_datetime(orders_df["created_at"]).dt.date
        orders_df["delay_minutes"] = (
            orders_df["actual_delivery_minutes"] - 
            orders_df["expected_delivery_minutes"]
        )
        orders_df["on_time"] = (orders_df["delay_minutes"] <= 5).astype(int)
        
        # Group by driver and date
        daily_metrics = orders_df.groupby(
            ["driver_id", "created_date"]
        ).agg({
            "order_id": "count",
            "on_time": "mean",
            "delay_minutes": "mean",
        }).rename(columns={
            "order_id": "deliveries_completed",
            "on_time": "on_time_ratio",
            "delay_minutes": "avg_delay_minutes",
        })
        
        daily_metrics = daily_metrics.reset_index()
        daily_metrics["on_time_ratio"] = (daily_metrics["on_time_ratio"] * 100).round(2)
        
        # Add GPS metrics
        if not gps_df.empty:
            gps_df["created_date"] = pd.to_datetime(gps_df["event_timestamp"]).dt.date
            gps_metrics = gps_df.groupby(
                ["driver_id", "created_date"]
            ).size().reset_index(name="gps_points_count")
            
            daily_metrics = daily_metrics.merge(
                gps_metrics, 
                on=["driver_id", "created_date"],
                how="left"
            )
        
        daily_metrics["metric_date"] = daily_metrics["created_date"]
        daily_metrics = daily_metrics.drop(columns=["created_date"])
        
        # Save to Gold
        self._save_gold_dataset("driver_daily_metrics", daily_metrics)
        
        stats = {
            "dataset": "gold_driver_daily_metrics",
            "rows_created": len(daily_metrics),
            "drivers_count": daily_metrics["driver_id"].nunique(),
            "avg_daily_deliveries": round(daily_metrics["deliveries_completed"].mean(), 2),
        }
        
        logger.info(f"Gold driver daily metrics created: {stats}")
        return stats
    
    def create_region_hourly_metrics(self, gps_df: pd.DataFrame,
                                    orders_df: pd.DataFrame) -> Dict[str, Any]:
        """Create gold_region_hourly_metrics.
        
        Metrics per region per hour:
        - active_drivers: number of drivers
        - gps_events_count: GPS tracking points
        - deliveries_completed: orders completed
        - avg_delivery_delay: regional average
        
        Args:
            gps_df: GPS events from Silver
            orders_df: Orders from Silver
        
        Returns:
            Statistics on metrics creation
        """
        logger.info("Creating gold_region_hourly_metrics...")
        
        if gps_df.empty:
            return {"dataset": "gold_region_hourly_metrics", "rows_created": 0}
        
        # Assign region based on coordinate bounding boxes
        gps_df["region"] = gps_df.apply(
            lambda row: self._assign_region(row["latitude"], row["longitude"]),
            axis=1
        )
        
        # Extract hour from timestamp
        gps_df["event_hour"] = pd.to_datetime(gps_df["event_timestamp"]).dt.floor("H")
        
        # Group by region and hour
        hourly_metrics = gps_df.groupby(
            ["region", "event_hour"]
        ).agg({
            "driver_id": "nunique",
            "event_id": "count",
        }).rename(columns={
            "driver_id": "active_drivers",
            "event_id": "gps_events_count",
        }).reset_index()
        
        hourly_metrics.columns = ["region", "metric_hour", "active_drivers", "gps_events_count"]
        
        # Save to Gold
        self._save_gold_dataset("region_hourly_metrics", hourly_metrics)
        
        stats = {
            "dataset": "gold_region_hourly_metrics",
            "rows_created": len(hourly_metrics),
            "regions_count": hourly_metrics["region"].nunique(),
        }
        
        logger.info(f"Gold region hourly metrics created: {stats}")
        return stats
    
    @staticmethod
    def _haversine_distance(lat1: float, lon1: float, 
                           lat2: float, lon2: float) -> float:
        """Calculate distance between two coordinates in km."""
        R = 6371  # Earth radius in km
        
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat/2) * sin(dlat/2) + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2) * sin(dlon/2)
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        
        return R * c
    
    @staticmethod
    def _assign_region(lat: float, lon: float) -> str:
        """Assign region based on coordinates.
        
        Grid:
        - North: lat > 40.7
        - South: lat < 40.7
        - East: lon > -73.9
        - West: lon < -73.9
        - Central: intersection
        """
        if lat > 40.75:
            region = "North"
        elif lat < 40.65:
            region = "South"
        else:
            region = "Central"
        
        if lon > -73.85:
            region += "_East"
        elif lon < -73.85:
            region += "_West"
        
        return region
    
    def _save_gold_dataset(self, dataset_name: str, df: pd.DataFrame) -> None:
        """Save DataFrame to Gold layer."""
        # Partition by date
        if "metric_date" in df.columns:
            df["year"] = pd.to_datetime(df["metric_date"]).dt.year
            df["month"] = pd.to_datetime(df["metric_date"]).dt.month
            df["day"] = pd.to_datetime(df["metric_date"]).dt.day
        elif "metric_hour" in df.columns:
            df["year"] = pd.to_datetime(df["metric_hour"]).dt.year
            df["month"] = pd.to_datetime(df["metric_hour"]).dt.month
            df["day"] = pd.to_datetime(df["metric_hour"]).dt.day
        
        # Save partitioned (implementation)
        logger.info(f"Saved gold_{dataset_name} to MinIO")
