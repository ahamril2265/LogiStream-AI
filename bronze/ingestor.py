"""Bronze Layer - Raw Data Ingestion.

The Bronze layer stores raw data as-is with minimal transformation:
- Append-only pattern
- Stored in Parquet format in MinIO
- Partitioned by year/month/day
- Adds ingestion_timestamp
- Row count tracking for data quality
"""

import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

logger = logging.getLogger(__name__)


class BronzeIngestor:
    """Handles raw data ingestion into Bronze layer."""
    
    def __init__(self, minio_client, duckdb_conn, config):
        """Initialize Bronze ingestor.
        
        Args:
            minio_client: MinIO client for storage
            duckdb_conn: DuckDB connection
            config: Configuration object
        """
        self.minio = minio_client
        self.duckdb = duckdb_conn
        self.config = config
        self.bucket = config.minio.bucket_bronze
    
    def ingest_jsonl(self, dataset_name: str, file_path: str) -> Dict[str, Any]:
        """Ingest JSONL file into Bronze layer.
        
        Process:
        1. Read JSONL file
        2. Add ingestion_timestamp
        3. Partition by year/month/day
        4. Save as Parquet to MinIO
        5. Record row count
        
        Args:
            dataset_name: Name of the dataset (e.g., 'orders', 'gps_events')
            file_path: Path to input JSONL file
        
        Returns:
            Dictionary with ingestion statistics
        """
        logger.info(f"Starting Bronze ingestion: {dataset_name}")
        
        # Read JSONL file
        records = []
        with open(file_path, "r") as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
        
        if not records:
            logger.warning(f"No records found in {file_path}")
            return {"dataset": dataset_name, "rows_ingested": 0}
        
        # Add ingestion timestamp and ingestion_id
        ingestion_ts = datetime.utcnow()
        ingestion_id = ingestion_ts.strftime("%Y%m%d%H%M%S")
        
        for record in records:
            record["ingestion_timestamp"] = ingestion_ts.isoformat()
            record["ingestion_id"] = ingestion_id
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Extract partition key from the first date-like column
        partition_col = self._infer_partition_column(df, dataset_name)
        
        # Partition and save
        partition_stats = self._save_partitioned(dataset_name, df, partition_col)
        
        # Ensure MinIO bucket exists
        self.minio.ensure_bucket_exists(self.bucket)
        
        # Record statistics
        stats = {
            "dataset": dataset_name,
            "rows_ingested": len(df),
            "ingestion_timestamp": ingestion_ts.isoformat(),
            "ingestion_id": ingestion_id,
            "partitions": partition_stats,
            "schema": {col: str(df[col].dtype) for col in df.columns},
        }
        
        logger.info(
            f"Bronze ingestion complete: {dataset_name} - "
            f"{len(df):,} rows in {len(partition_stats)} partitions"
        )
        
        return stats
    
    def _infer_partition_column(self, df: pd.DataFrame, dataset_name: str) -> str:
        """Infer partition column from dataset."""
        # Common date column patterns
        date_columns = [
            f"{dataset_name.rstrip('s')}_timestamp",  # e.g., order_timestamp
            f"{dataset_name.rstrip('s')}_at",          # e.g., order_at
            "event_timestamp",
            "created_at",
            "start_time",
        ]
        
        for col in date_columns:
            if col in df.columns:
                return col
        
        # Fallback: use first datetime column
        for col in df.columns:
            if "date" in col.lower() or "time" in col.lower():
                return col
        
        # Fallback: use ingestion_timestamp
        return "ingestion_timestamp"
    
    def _save_partitioned(self, dataset_name: str, df: pd.DataFrame, 
                         partition_col: str) -> Dict[str, int]:
        """Save DataFrame partitioned by date.
        
        Returns:
            Dictionary mapping partition path to row count
        """
        partition_stats = {}
        
        # Convert partition column to datetime if needed
        if partition_col in df.columns:
            df[partition_col] = pd.to_datetime(df[partition_col])
        
        # Group by year/month/day
        df["_year"] = df[partition_col].dt.year
        df["_month"] = df[partition_col].dt.month
        df["_day"] = df[partition_col].dt.day
        
        for (year, month, day), group_df in df.groupby(["_year", "_month", "_day"]):
            # Create partition path: year=2024/month=01/day=15
            partition_path = f"year={year}/month={month:02d}/day={day:02d}"
            
            # Remove temporary columns
            group_df = group_df.drop(columns=["_year", "_month", "_day"])
            
            # Save as Parquet
            local_temp_path = f"/tmp/{dataset_name}_{year}_{month:02d}_{day:02d}.parquet"
            group_df.to_parquet(local_temp_path, index=False)
            
            # Upload to MinIO
            minio_path = f"raw/{dataset_name}/{partition_path}/data.parquet"
            self.minio.put_object(self.bucket, minio_path, local_temp_path)
            
            partition_stats[partition_path] = len(group_df)
            logger.debug(f"Saved partition: {minio_path} ({len(group_df):,} rows)")
            
            # Cleanup
            Path(local_temp_path).unlink()
        
        return partition_stats


def validate_bronze_schema(df: pd.DataFrame, dataset_name: str) -> bool:
    """Validate that Bronze data has expected columns.
    
    TODO (Production):
    - Load schema from schema registry
    - Validate data types more strictly
    - Check for required fields
    """
    required_cols = ["ingestion_timestamp", "ingestion_id"]
    
    for col in required_cols:
        if col not in df.columns:
            logger.error(f"Missing required column: {col}")
            return False
    
    logger.info(f"Schema validation passed for {dataset_name}")
    return True


def get_bronze_stats(minio_client, bucket: str, dataset_name: str) -> Dict[str, Any]:
    """Get statistics about a Bronze dataset."""
    try:
        objects = minio_client.list_objects(
            bucket, 
            prefix=f"raw/{dataset_name}/"
        )
        
        total_files = len(objects)
        total_size_bytes = sum([
            minio_client.client.stat_object(bucket, obj).size 
            for obj in objects
        ])
        
        return {
            "dataset": dataset_name,
            "files": total_files,
            "size_bytes": total_size_bytes,
            "size_mb": round(total_size_bytes / (1024**2), 2),
        }
    except Exception as e:
        logger.error(f"Error getting Bronze stats: {e}")
        return {}
