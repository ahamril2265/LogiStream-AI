"""
Common utilities for database and storage operations.
"""

import os
import logging
import hashlib
import json
from typing import Dict, Any, Optional
from datetime import datetime
from pathlib import Path

import duckdb
import psycopg2
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


class MinIOClient:
    """MinIO S3-compatible object storage client wrapper."""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool = False):
        """Initialize MinIO client.
        
        Args:
            endpoint: MinIO endpoint (e.g., 'minio:9000')
            access_key: Access key
            secret_key: Secret key
            secure: Use HTTPS
        """
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        self.endpoint = endpoint
        
    def ensure_bucket_exists(self, bucket_name: str) -> None:
        """Create bucket if it doesn't exist."""
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"Created bucket: {bucket_name}")
            else:
                logger.debug(f"Bucket exists: {bucket_name}")
        except S3Error as e:
            logger.error(f"Error creating bucket {bucket_name}: {e}")
            raise
    
    def put_object(self, bucket: str, object_name: str, file_path: str) -> None:
        """Upload file to MinIO."""
        try:
            self.client.fput_object(bucket, object_name, file_path)
            logger.debug(f"Uploaded {object_name} to {bucket}")
        except S3Error as e:
            logger.error(f"Error uploading to MinIO: {e}")
            raise
    
    def get_object(self, bucket: str, object_name: str, file_path: str) -> None:
        """Download file from MinIO."""
        try:
            self.client.fget_object(bucket, object_name, file_path)
            logger.debug(f"Downloaded {object_name} from {bucket}")
        except S3Error as e:
            logger.error(f"Error downloading from MinIO: {e}")
            raise
    
    def list_objects(self, bucket: str, prefix: str = "") -> list:
        """List objects in bucket with optional prefix."""
        try:
            objects = self.client.list_objects(bucket, prefix=prefix, recursive=True)
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing objects in {bucket}: {e}")
            raise


class PostgresConnector:
    """PostgreSQL database connector."""
    
    def __init__(self, connection_string: str):
        """Initialize PostgreSQL connector.
        
        Args:
            connection_string: PostgreSQL connection string
        """
        self.connection_string = connection_string
        self.conn = None
    
    def connect(self):
        """Establish connection to PostgreSQL."""
        try:
            self.conn = psycopg2.connect(self.connection_string)
            logger.info("Connected to PostgreSQL")
            return self.conn
        except psycopg2.Error as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise
    
    def close(self):
        """Close PostgreSQL connection."""
        if self.conn:
            self.conn.close()
            logger.info("Closed PostgreSQL connection")
    
    def execute_query(self, query: str) -> Optional[list]:
        """Execute query and return results."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            if query.strip().upper().startswith("SELECT"):
                results = cursor.fetchall()
                cursor.close()
                return results
            else:
                self.conn.commit()
                cursor.close()
                return None
        except psycopg2.Error as e:
            logger.error(f"Error executing query: {e}")
            self.conn.rollback()
            raise


class DuckDBConnector:
    """DuckDB connector for local transformations."""
    
    def __init__(self, db_path: str, max_memory: str = "4GB", threads: int = 4):
        """Initialize DuckDB connector.
        
        Args:
            db_path: Path to DuckDB database file
            max_memory: Memory limit for DuckDB
            threads: Number of threads to use
        """
        self.db_path = db_path
        self.max_memory = max_memory
        self.threads = threads
        self.conn = None
    
    def connect(self):
        """Connect to DuckDB."""
        try:
            # Create parent directory if it doesn't exist
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            
            self.conn = duckdb.connect(self.db_path)
            
            # Set configuration
            self.conn.execute(f"SET memory_limit='{self.max_memory}'")
            self.conn.execute(f"SET threads={self.threads}")
            
            logger.info(f"Connected to DuckDB: {self.db_path}")
            return self.conn
        except Exception as e:
            logger.error(f"Error connecting to DuckDB: {e}")
            raise
    
    def close(self):
        """Close DuckDB connection."""
        if self.conn:
            self.conn.close()
            logger.info("Closed DuckDB connection")
    
    def execute_query(self, query: str):
        """Execute query in DuckDB."""
        try:
            return self.conn.execute(query)
        except Exception as e:
            logger.error(f"DuckDB query error: {e}")
            raise
    
    def read_parquet(self, path: str):
        """Read Parquet file with DuckDB."""
        try:
            return self.conn.execute(f"SELECT * FROM '{path}'").df()
        except Exception as e:
            logger.error(f"Error reading Parquet: {e}")
            raise
    
    def write_parquet(self, df, path: str) -> None:
        """Write DataFrame to Parquet with DuckDB."""
        try:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            self.conn.execute(f"COPY (SELECT * FROM df) TO '{path}' (FORMAT PARQUET)")
            logger.debug(f"Written Parquet file: {path}")
        except Exception as e:
            logger.error(f"Error writing Parquet: {e}")
            raise


def generate_file_hash(file_path: str) -> str:
    """Generate SHA256 hash of a file.
    
    Useful for detecting duplicates and tracking data lineage.
    """
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


def generate_data_signature(data_dict: Dict[str, Any]) -> str:
    """Generate hash signature for a data object.
    
    Used for tracking dataset versions and detecting changes.
    """
    json_str = json.dumps(data_dict, sort_keys=True, default=str)
    return hashlib.sha256(json_str.encode()).hexdigest()


def get_partition_date_path(date: datetime, depth: str = "year/month/day") -> str:
    """Generate partition path for given date.
    
    Args:
        date: Date to partition
        depth: Partition depth (e.g., 'year/month/day')
    
    Returns:
        Partition path (e.g., 'year=2024/month=01/day=15')
    """
    parts = []
    if "year" in depth:
        parts.append(f"year={date.year}")
    if "month" in depth:
        parts.append(f"month={date.month:02d}")
    if "day" in depth:
        parts.append(f"day={date.day:02d}")
    return "/".join(parts)


class RowCountValidator:
    """Validates row counts across data transformations.
    
    Maintains lineage of row counts to detect data loss.
    """
    
    def __init__(self):
        self.counts: Dict[str, int] = {}
    
    def record(self, stage: str, count: int) -> None:
        """Record row count for a stage."""
        self.counts[stage] = count
        logger.info(f"Row count recorded - {stage}: {count:,} rows")
    
    def validate_lineage(self, source_stage: str, target_stage: str, 
                         variance_threshold: float = 0.1) -> bool:
        """Validate row count doesn't exceed variance threshold.
        
        Args:
            source_stage: Source stage name
            target_stage: Target stage name
            variance_threshold: Allowed variance (default 10%)
        
        Returns:
            True if valid, False otherwise
        """
        if source_stage not in self.counts or target_stage not in self.counts:
            return False
        
        source_count = self.counts[source_stage]
        target_count = self.counts[target_stage]
        
        # Row count should generally not increase (except in certain aggregations)
        # Allow for expected increases in join operations
        variance = abs(target_count - source_count) / max(source_count, 1)
        
        if variance > variance_threshold:
            logger.warning(
                f"High variance in row count: {source_stage} ({source_count:,}) -> "
                f"{target_stage} ({target_count:,}) [variance: {variance:.1%}]"
            )
            return False
        
        return True
    
    def get_report(self) -> Dict[str, int]:
        """Get row count report."""
        return self.counts.copy()
