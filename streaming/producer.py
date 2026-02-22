"""
Kafka producer for logistics data streams.

Generates and streams:
- Driver profiles (once per batch)
- Orders (continuous stream)
- GPS events (continuous stream)
- Weather events (continuous stream)

Run until interrupted (Ctrl+C).
"""

import json
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_config, setup_logging
from data_simulation import (
    DriverProfileGenerator,
    OrderGenerator,
    GPSEventGenerator,
    WeatherEventGenerator,
)

logger = logging.getLogger(__name__)


class LogisticsProducer:
    """Kafka producer for logistics data streams."""

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        """Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
        """
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                request_timeout_ms=10000
            )
            logger.info(f"Connected to Kafka: {bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def produce_drivers(self, drivers: list, batch_size: int = 1000) -> None:
        """Produce driver profile records to Kafka.
        
        Args:
            drivers: List of driver records
            batch_size: Records per batch
        """
        logger.info(f"Producing {len(drivers)} driver records")
        
        for i, driver in enumerate(drivers):
            try:
                future = self.producer.send(
                    "drivers",
                    value=driver,
                    key=str(driver["driver_id"]).encode("utf-8")
                )
                # Block for 'synchronous' sends
                record_metadata = future.get(timeout=10)
                
                if (i + 1) % batch_size == 0:
                    logger.info(f"Produced {i + 1}/{len(drivers)} driver records")
                    
            except KafkaError as e:
                logger.error(f"Error producing driver {driver['driver_id']}: {e}")
                raise

        logger.info(f"All {len(drivers)} driver records produced successfully")

    def produce_orders_stream(
        self,
        orders: list,
        batch_size: int = 100,
    ) -> None:
        """Stream order records to Kafka.
        
        Args:
            orders: List of order records to produce
            batch_size: Orders per batch before logging
        """
        logger.info(f"Starting order stream: {len(orders)} orders")
        
        for i, order in enumerate(orders):
            try:
                future = self.producer.send(
                    "orders",
                    value=order,
                    key=str(order["order_id"]).encode("utf-8")
                )
                record_metadata = future.get(timeout=10)
                
                # Simulate real-time delay (1-2 orders per second)
                time.sleep(0.5 + (hash(order["order_id"]) % 10) / 10)
                
                if (i + 1) % batch_size == 0:
                    logger.info(f"Produced {i + 1}/{len(orders)} order records")
                    
            except KafkaError as e:
                logger.error(f"Error producing order: {e}")
                raise
            except KeyboardInterrupt:
                logger.info(f"Order stream interrupted after {i + 1} records")
                break

        logger.info(f"Order stream completed: {len(orders)} records")

    def produce_gps_events_stream(
        self,
        gps_events: list,
        batch_size: int = 500,
    ) -> None:
        """Stream GPS events to Kafka.
        
        Args:
            gps_events: List of GPS event records to produce
            batch_size: Events per batch before logging
        """
        logger.info(f"Starting GPS event stream: {len(gps_events)} events")
        
        for i, event in enumerate(gps_events):
            try:
                future = self.producer.send(
                    "gps_events",
                    value=event,
                    key=str(event.get("event_id")).encode("utf-8")
                )
                record_metadata = future.get(timeout=10)
                
                # GPS events are frequent (simulate ~50 events/second per delivery)
                time.sleep(0.02)
                
                if (i + 1) % batch_size == 0:
                    logger.info(f"Produced {i + 1}/{len(gps_events)} GPS events")
                    
            except KafkaError as e:
                logger.error(f"Error producing GPS event: {e}")
                raise
            except KeyboardInterrupt:
                logger.info(f"GPS event stream interrupted after {i + 1} events")
                break

        logger.info(f"GPS event stream completed: {len(gps_events)} events")

    def produce_weather_stream(
        self,
        weather_events: list,
        batch_size: int = 10,
    ) -> None:
        """Stream weather events to Kafka.
        
        Args:
            weather_events: List of weather event records to produce
            batch_size: Events per batch before logging
        """
        logger.info(f"Starting weather event stream: {len(weather_events)} events")
        
        try:
            for i, event in enumerate(weather_events):
                try:
                    future = self.producer.send(
                        "weather_events",
                        value=event,
                        key=str(event.get("weather_event_id")).encode("utf-8")
                    )
                    record_metadata = future.get(timeout=10)
                    
                    if (i + 1) % batch_size == 0:
                        logger.info(f"Produced {i + 1}/{len(weather_events)} weather events")
                        
                except KafkaError as e:
                    logger.error(f"Error producing weather event: {e}")
                    raise
                
        except KeyboardInterrupt:
            logger.info(f"Weather stream interrupted after {i + 1} events")

        logger.info(f"Weather stream completed: {len(weather_events)} events")

    def flush(self) -> None:
        """Flush all pending messages."""
        self.producer.flush()
        logger.info("All messages flushed")

    def close(self) -> None:
        """Close the producer."""
        self.producer.close()
        logger.info("Producer closed")


def main():
    """Run continuous data streaming."""
    try:
        # Configuration
        config = get_config()
        setup_logging(config)
        
        logger.info("=" * 80)
        logger.info("STREAMING DATA PRODUCER")
        logger.info("=" * 80)
        logger.info(f"Kafka bootstrap servers: localhost:9092")
        logger.info("Press Ctrl+C to stop streaming")
        
        # Initialize producer
        producer = LogisticsProducer(bootstrap_servers="localhost:9092")
        
        # Calculate date range from sim_years
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365 * config.data_simulation.sim_years)
        
        logger.info(f"Simulation date range: {start_date.date()} to {end_date.date()}")
        logger.info(f"Random seed: {config.data_simulation.seed}")
        
        # Initialize generators with correct parameters
        driver_gen = DriverProfileGenerator(
            num_drivers=config.data_simulation.num_drivers,
            seed=config.data_simulation.seed
        )
        order_gen = OrderGenerator(
            num_orders=config.data_simulation.num_deliveries,
            num_drivers=config.data_simulation.num_drivers,
            start_date=start_date,
            end_date=end_date,
            seed=config.data_simulation.seed
        )
        weather_gen = WeatherEventGenerator(
            start_date=start_date,
            end_date=end_date,
            seed=config.data_simulation.seed
        )
        
        # Generate initial driver profiles (only once)
        logger.info("Generating driver profiles...")
        drivers = driver_gen.generate()
        producer.produce_drivers(drivers)
        
        # Stream orders and GPS events continuously
        try:
            # For demo, stream smaller batches in a loop
            batch_num = 0
            while True:
                batch_num += 1
                logger.info(f"\n--- Streaming Batch {batch_num} ---")
                
                # Generate and stream orders for this batch
                batch_order_gen = OrderGenerator(
                    num_orders=100,
                    num_drivers=config.data_simulation.num_drivers,
                    start_date=start_date,
                    end_date=end_date,
                    seed=config.data_simulation.seed + batch_num
                )
                orders = batch_order_gen.generate()
                producer.produce_orders_stream(
                    orders,
                    batch_size=25
                )
                
                # Generate GPS events for the orders we just created
                gps_gen = GPSEventGenerator(
                    orders=orders,
                    events_per_delivery=config.data_simulation.gps_events_per_delivery,
                    late_ratio=config.data_simulation.late_event_ratio,
                    duplicate_ratio=config.data_simulation.duplicate_event_ratio,
                    out_of_order_ratio=config.data_simulation.out_of_order_ratio,
                    seed=config.data_simulation.seed + batch_num
                )
                gps_events = gps_gen.generate()
                producer.produce_gps_events_stream(
                    gps_events,
                    batch_size=100
                )
                
                # Generate and stream weather events
                weather_events = weather_gen.generate()
                producer.produce_weather_stream(
                    weather_events,
                    batch_size=10
                )
                
                # Brief pause between batches
                logger.info("Waiting before next batch...")
                time.sleep(10)
                
        except KeyboardInterrupt:
            logger.info("\nStreaming interrupted by user")
        
        producer.flush()
        producer.close()
        
        logger.info("=" * 80)
        logger.info("STREAMING STOPPED")
        logger.info("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error(f"Producer failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
