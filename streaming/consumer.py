"""
Kafka consumer for logistics data streams.

Consumes from Kafka topics and writes to MinIO Bronze layer:
- drivers: Driver profile records
- orders: Order records  
- gps_events: GPS tracking events
- weather_events: Weather conditions

Run as a background service, triggered by Airflow DAG.
"""

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import pandas as pd

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_config, MinIOClient
from config.utils import generate_data_signature, get_partition_date_path

logger = logging.getLogger(__name__)


class LogisticsBronzeConsumer:
    """Kafka consumer that writes to MinIO Bronze layer."""

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        """Initialize consumer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
        """
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                group_id="bronze-ingestion-group",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                max_poll_records=1000,
                session_timeout_ms=10000,
                request_timeout_ms=30000,
                heartbeat_interval_ms=3000
            )
            logger.info(f"Connected to Kafka: {bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def consume_and_ingest(
        self,
        topic: str,
        minio: MinIOClient,
        bucket: str,
        batch_size: int = 1000,
        timeout_seconds: int = 60,
    ) -> dict:
        """Consume messages from Kafka and write to MinIO.
        
        Args:
            topic: Kafka topic to consume from
            minio: MinIO client
            bucket: Target MinIO bucket
            batch_size: Messages to batch before writing
            timeout_seconds: How long to consume (0 = until empty queue)
        
        Returns:
            Ingestion statistics
        """
        logger.info(f"Starting consumption from topic: {topic}")
        
        records = []
        stats = {
            "topic": topic,
            "total_records": 0,
            "batches_written": 0,
            "errors": 0,
            "ingestion_timestamp": datetime.utcnow().isoformat(),
        }
        
        start_time = time.time()
        last_message_time = time.time()
        
        try:
            self.consumer.subscribe([topic])
            
            while True:
                # Check timeout
                elapsed = time.time() - start_time
                if timeout_seconds > 0 and elapsed > timeout_seconds:
                    logger.info(f"Timeout reached ({timeout_seconds}s)")
                    break
                
                # Check idle timeout (no messages received)
                if time.time() - last_message_time > 5:
                    logger.info(f"No messages for 5 seconds, stopping consumption")
                    break
                
                # Poll messages
                messages = self.consumer.poll(timeout_ms=1000, max_records=batch_size)
                
                if not messages:
                    continue
                
                last_message_time = time.time()
                
                # Process messages from all partitions
                for topic_partition, msgs in messages.items():
                    for msg in msgs:
                        records.append(msg.value)
                        stats["total_records"] += 1
                
                # Write batch if full
                if len(records) >= batch_size:
                    self._write_batch_to_minio(
                        records, topic, minio, bucket, stats
                    )
                    records = []
        
        except Exception as e:
            logger.error(f"Error consuming from {topic}: {e}")
            stats["errors"] += 1
            raise
        
        finally:
            # Write remaining records
            if records:
                self._write_batch_to_minio(
                    records, topic, minio, bucket, stats
                )
        
        logger.info(f"Consumption complete from {topic}: {stats['total_records']} records")
        return stats

    def _write_batch_to_minio(
        self,
        records: list,
        topic: str,
        minio: MinIOClient,
        bucket: str,
        stats: dict,
    ) -> None:
        """Write a batch of records to MinIO.
        
        Args:
            records: List of record dictionaries
            topic: Kafka topic name
            minio: MinIO client
            bucket: Target bucket
            stats: Statistics dictionary to update
        """
        try:
            # Convert to DataFrame
            df = pd.DataFrame(records)
            
            # Infer partition column (date, timestamp)
            partition_col = self._infer_partition_column(df)
            
            # Group by date and write partitions
            if partition_col:
                groups = df.groupby(pd.to_datetime(df[partition_col]).dt.date)
                for date, group in groups:
                    self._write_partition(
                        group,
                        topic,
                        date,
                        minio,
                        bucket
                    )
            else:
                # No date column, write as single file
                self._write_partition(
                    df,
                    topic,
                    datetime.utcnow().date(),
                    minio,
                    bucket
                )
            
            stats["batches_written"] += 1
            logger.info(f"Batch written to {bucket}/{topic}")
            
        except Exception as e:
            logger.error(f"Error writing batch to MinIO: {e}")
            stats["errors"] += 1
            raise

    def _write_partition(
        self,
        df: pd.DataFrame,
        topic: str,
        date,
        minio: MinIOClient,
        bucket: str,
    ) -> None:
        """Write a date partition to MinIO.
        
        Args:
            df: DataFrame to write
            topic: Kafka topic name
            date: Partition date
            minio: MinIO client
            bucket: Target bucket
        """
        # Create partition path: year=YYYY/month=MM/day=DD
        partition_path = get_partition_date_path(date)
        
        # Save to temporary file
        local_file = f"/tmp/{topic}_{date.isoformat()}.parquet"
        df.to_parquet(local_file, index=False, engine="pyarrow")
        
        # Upload to MinIO
        minio_path = f"raw/{topic}/{partition_path}/data.parquet"
        minio.put_object(bucket, minio_path, local_file)
        
        logger.debug(f"Uploaded {len(df)} records to {bucket}/{minio_path}")

    def _infer_partition_column(self, df: pd.DataFrame) -> str:
        """Infer date/timestamp column for partitioning.
        
        Args:
            df: DataFrame to inspect
        
        Returns:
            Column name if found, else None
        """
        candidates = [
            "order_timestamp",
            "event_timestamp",
            "created_at",
            "start_time",
            "timestamp",
            "date",
        ]
        
        for col in candidates:
            if col in df.columns:
                return col
        
        return None

    def close(self) -> None:
        """Close the consumer."""
        self.consumer.close()
        logger.info("Consumer closed")


def consume_topic(
    topic: str,
    config,
    minio: MinIOClient,
    batch_size: int = 1000,
    timeout_seconds: int = 60,
) -> dict:
    """Consume a Kafka topic and write to Bronze layer.
    
    Args:
        topic: Kafka topic name
        config: Configuration object
        minio: MinIO client
        batch_size: Batch size for writes
        timeout_seconds: Consumption timeout
    
    Returns:
        Ingestion statistics
    """
    consumer = LogisticsBronzeConsumer(bootstrap_servers="localhost:9092")
    
    topic_to_bucket = {
        "drivers": config.minio.bucket_bronze,
        "orders": config.minio.bucket_bronze,
        "gps_events": config.minio.bucket_bronze,
        "weather_events": config.minio.bucket_bronze,
    }
    
    bucket = topic_to_bucket.get(topic, config.minio.bucket_bronze)
    
    try:
        stats = consumer.consume_and_ingest(
            topic,
            minio,
            bucket,
            batch_size=batch_size,
            timeout_seconds=timeout_seconds,
        )
        return stats
    finally:
        consumer.close()


def main():
    """Run the Bronze consumer service."""
    try:
        # Configuration
        config = get_config()
        
        logger.info("=" * 80)
        logger.info("BRONZE CONSUMER SERVICE")
        logger.info("=" * 80)
        
        # Initialize MinIO
        minio = MinIOClient(
            config.minio.endpoint,
            config.minio.access_key,
            config.minio.secret_key,
            config.minio.secure
        )
        
        # Ensure bucket exists
        minio.ensure_bucket_exists(config.minio.bucket_bronze)
        
        # Topics to consume
        topics = ["drivers", "orders", "gps_events", "weather_events"]
        
        # Consume all topics
        for topic in topics:
            logger.info(f"\nConsuming from: {topic}")
            stats = consume_topic(topic, config, minio, timeout_seconds=300)
            logger.info(f"Statistics: {stats}")
        
        logger.info("=" * 80)
        logger.info("BRONZE CONSUMER COMPLETED")
        logger.info("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error(f"Consumer failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
