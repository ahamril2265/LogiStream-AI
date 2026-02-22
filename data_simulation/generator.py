"""Data Simulation Module - Generates realistic logistics data.

Key features:
- Generates orders, GPS events, weather, and driver profiles
- Simulates late arrivals (5-15 min delay)
- Inject GPS duplicates (1-3%)
- Out-of-order timestamp simulation
- Realistic spatial distribution (100km x 100km city grid)
- Multiple delivery types with different patterns
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Tuple
import random
import uuid

import numpy as np
from numpy.random import RandomState

logger = logging.getLogger(__name__)

# Geographic bounds for simulation (city-level)
LAT_MIN, LAT_MAX = 40.5, 40.9  # ~45 km
LON_MIN, LON_MAX = -74.1, -73.7  # ~40 km


class DriverProfileGenerator:
    """Generate realistic driver profiles."""
    
    DRIVER_TYPES = ["van", "truck", "bike", "car"]
    WEATHER_SENSITIVITY = {"van": 0.05, "truck": 0.02, "bike": 0.15, "car": 0.08}
    ACCIDENT_RISK = {"van": 0.001, "truck": 0.002, "bike": 0.005, "car": 0.001}
    
    def __init__(self, num_drivers: int, seed: int = 42):
        self.num_drivers = num_drivers
        self.rng = RandomState(seed)
    
    def generate(self) -> List[Dict[str, Any]]:
        """Generate driver profiles."""
        drivers = []
        for i in range(self.num_drivers):
            vehicle_type = self.rng.choice(self.DRIVER_TYPES)
            drivers.append({
                "driver_id": f"DRV_{i+1:06d}",
                "vehicle_type": vehicle_type,
                "experience_years": int(self.rng.gamma(2, 3)),  # 1-15 years
                "accident_history": int(self.rng.poisson(0.5)),
                "weather_sensitivity": self.WEATHER_SENSITIVITY[vehicle_type],
                "accident_risk": self.ACCIDENT_RISK[vehicle_type],
                "region": self.rng.choice(["North", "South", "East", "West", "Central"]),
                "onboard_timestamp": datetime.now().isoformat(),
            })
        return drivers


class OrderGenerator:
    """Generate order data with realistic patterns."""
    
    ORDER_TYPES = ["standard", "express", "scheduled", "frozen"]
    TYPE_DURATIONS = {
        "standard": (0.5, 2.0),    # 30 min - 2 hours
        "express": (0.25, 1.0),    # 15 min - 1 hour
        "scheduled": (0.5, 4.0),   # 30 min - 4 hours
        "frozen": (0.5, 1.5),      # 30 min - 1.5 hours (time-critical)
    }
    
    def __init__(self, num_orders: int, num_drivers: int, 
                 start_date: datetime, end_date: datetime, seed: int = 42):
        self.num_orders = num_orders
        self.num_drivers = num_drivers
        self.start_date = start_date
        self.end_date = end_date
        self.rng = RandomState(seed)
        self.total_seconds = (end_date - start_date).total_seconds()
    
    def generate(self) -> List[Dict[str, Any]]:
        """Generate orders."""
        orders = []
        for i in range(self.num_orders):
            order_type = self.rng.choice(self.ORDER_TYPES)
            created_at = self.start_date + timedelta(
                seconds=self.rng.uniform(0, self.total_seconds)
            )
            
            # Expected duration based on type
            duration_min, duration_max = self.TYPE_DURATIONS[order_type]
            expected_duration = self.rng.uniform(duration_min, duration_max)
            
            # Assign driver
            driver_id = f"DRV_{self.rng.randint(1, self.num_drivers+1):06d}"
            
            # Random locations (100km x 100km)
            pickup_lat = self.rng.uniform(LAT_MIN, LAT_MAX)
            pickup_lon = self.rng.uniform(LON_MIN, LON_MAX)
            
            # Delivery typically within 20km
            delivery_lat = pickup_lat + self.rng.normal(0, 0.15)
            delivery_lon = pickup_lon + self.rng.normal(0, 0.15)
            
            delivery_lat = np.clip(delivery_lat, LAT_MIN, LAT_MAX)
            delivery_lon = np.clip(delivery_lon, LON_MIN, LON_MAX)
            
            orders.append({
                "order_id": f"ORD_{i+1:08d}",
                "driver_id": driver_id,
                "order_type": order_type,
                "status": "completed",
                "created_at": created_at.isoformat(),
                "pickup_lat": round(pickup_lat, 6),
                "pickup_lon": round(pickup_lon, 6),
                "delivery_lat": round(delivery_lat, 6),
                "delivery_lon": round(delivery_lon, 6),
                "expected_delivery_minutes": round(expected_duration * 60),
                "actual_delivery_minutes": int(expected_duration * 60) 
                                           + int(self.rng.normal(0, 10)),
            })
        
        return orders


class GPSEventGenerator:
    """Generate GPS tracking events for deliveries.
    
    Features:
    - Realistic route sampling
    - Late arrivals (5-15 min)
    - Duplicate events (1-3%)
    - Out-of-order timestamps
    """
    
    def __init__(self, orders: List[Dict[str, Any]], 
                 events_per_delivery: int = 25,
                 late_ratio: float = 0.08,
                 duplicate_ratio: float = 0.02,
                 out_of_order_ratio: float = 0.01,
                 seed: int = 42):
        self.orders = orders
        self.events_per_delivery = events_per_delivery
        self.late_ratio = late_ratio
        self.duplicate_ratio = duplicate_ratio
        self.out_of_order_ratio = out_of_order_ratio
        self.rng = RandomState(seed)
    
    def generate(self) -> List[Dict[str, Any]]:
        """Generate GPS events for all orders."""
        events = []
        event_id_counter = 0
        
        for order in self.orders:
            order_events = self._generate_order_events(order, event_id_counter)
            events.extend(order_events)
            event_id_counter += len(order_events)
        
        return events
    
    def _generate_order_events(self, order: Dict[str, Any], 
                               base_id: int) -> List[Dict[str, Any]]:
        """Generate GPS events for a single order."""
        # Parse timestamps
        created_at = datetime.fromisoformat(order["created_at"])
        duration_minutes = order["actual_delivery_minutes"]
        
        events = []
        
        # Regular events along the route
        for i in range(self.events_per_delivery):
            event_id = f"GPS_{base_id + i:010d}"
            progress = i / self.events_per_delivery
            
            # Interpolate location along route
            lat = (order["pickup_lat"] + 
                   progress * (order["delivery_lat"] - order["pickup_lat"]))
            lon = (order["pickup_lon"] + 
                   progress * (order["delivery_lon"] - order["pickup_lon"]))
            
            # Add noise to simulate actual GPS jitter
            lat += self.rng.normal(0, 0.0005)
            lon += self.rng.normal(0, 0.0005)
            
            event_timestamp = created_at + timedelta(
                minutes=progress * duration_minutes
            )
            
            events.append({
                "event_id": event_id,
                "order_id": order["order_id"],
                "driver_id": order["driver_id"],
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "accuracy": round(self.rng.uniform(5, 20), 1),
                "event_timestamp": event_timestamp.isoformat(),
            })
        
        # Inject late events (~8% of orders get 1-2 late events)
        if self.rng.random() < self.late_ratio:
            num_late = self.rng.randint(1, 3)
            for j in range(num_late):
                event_id = f"GPS_{base_id + self.events_per_delivery + j:010d}"
                # Late arrival: 5-15 minutes after actual delivery
                late_delay_min = self.rng.randint(5, 15)
                late_timestamp = (created_at + 
                                timedelta(minutes=duration_minutes + late_delay_min))
                
                events.append({
                    "event_id": event_id,
                    "order_id": order["order_id"],
                    "driver_id": order["driver_id"],
                    "latitude": round(order["delivery_lat"], 6),
                    "longitude": round(order["delivery_lon"], 6),
                    "accuracy": round(self.rng.uniform(5, 20), 1),
                    "event_timestamp": late_timestamp.isoformat(),
                })
        
        # Inject duplicate events (~2% of events)
        duplicates_to_inject = int(len(events) * self.duplicate_ratio)
        for _ in range(duplicates_to_inject):
            original = self.rng.choice(events)
            duplicate = original.copy()
            duplicate["event_id"] = f"GPS_{self.rng.randint(0, 10**10):010d}"
            events.append(duplicate)
        
        # Inject out-of-order timestamps (~1% of events)
        if self.rng.random() < self.out_of_order_ratio and len(events) > 1:
            idx = self.rng.randint(1, len(events))
            # Swap order of two events
            events[idx]["event_timestamp"] = events[idx-1]["event_timestamp"]
        
        return events


class WeatherEventGenerator:
    """Generate weather events affecting deliveries."""
    
    WEATHER_TYPES = ["clear", "rain", "snow", "fog", "storm", "wind"]
    SEVERITY = {
        "clear": 0,
        "rain": 2,
        "snow": 4,
        "fog": 1,
        "storm": 5,
        "wind": 1,
    }
    
    def __init__(self, start_date: datetime, end_date: datetime, seed: int = 42):
        self.start_date = start_date
        self.end_date = end_date
        self.rng = RandomState(seed)
        self.total_hours = (end_date - start_date).total_seconds() / 3600
    
    def generate(self) -> List[Dict[str, Any]]:
        """Generate weather events."""
        events = []
        
        # ~5 weather events per day on average
        num_events = int(self.total_hours / 24 * 5)
        
        for i in range(num_events):
            weather_type = self.rng.choice(self.WEATHER_TYPES, p=[0.4, 0.2, 0.1, 0.1, 0.1, 0.1])
            event_time = self.start_date + timedelta(
                seconds=self.rng.uniform(0, self.total_hours * 3600)
            )
            
            # Duration: 1-8 hours depending on type
            duration_hours = int(self.rng.gamma(2, 0.5)) + 1
            
            events.append({
                "weather_event_id": f"WE_{i+1:06d}",
                "weather_type": weather_type,
                "severity": self.SEVERITY[weather_type],
                "start_time": event_time.isoformat(),
                "end_time": (event_time + timedelta(hours=duration_hours)).isoformat(),
                "affected_region": self.rng.choice(["North", "South", "East", "West", "Central"]),
            })
        
        return events


class DataSimulator:
    """Main simulator orchestrating all data generation."""
    
    def __init__(self, config_obj, seed: int = 42):
        """Initialize simulator.
        
        Args:
            config_obj: Configuration object from config.settings.DataSimulationConfig
            seed: Random seed for reproducibility
        """
        self.config = config_obj
        self.seed = seed
        random.seed(seed)
        np.random.seed(seed)
    
    def generate_all(self) -> Dict[str, List[Dict[str, Any]]]:
        """Generate all datasets for the simulation."""
        logger.info("Starting data generation...")
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365 * self.config.sim_years)
        
        # Generate in order (dependencies)
        logger.info("Generating driver profiles...")
        drivers = DriverProfileGenerator(
            self.config.num_drivers, 
            seed=self.seed
        ).generate()
        
        logger.info("Generating orders...")
        orders = OrderGenerator(
            self.config.num_deliveries,
            self.config.num_drivers,
            start_date,
            end_date,
            seed=self.seed + 1
        ).generate()
        
        logger.info("Generating GPS events...")
        gps_events = GPSEventGenerator(
            orders,
            self.config.gps_events_per_delivery,
            self.config.late_event_ratio,
            self.config.duplicate_event_ratio,
            self.config.out_of_order_ratio,
            seed=self.seed + 2
        ).generate()
        
        logger.info("Generating weather events...")
        weather = WeatherEventGenerator(
            start_date, 
            end_date, 
            seed=self.seed + 3
        ).generate()
        
        logger.info(
            f"Data generation complete:\n"
            f"  - Drivers: {len(drivers):,}\n"
            f"  - Orders: {len(orders):,}\n"
            f"  - GPS events: {len(gps_events):,}\n"
            f"  - Weather events: {len(weather):,}"
        )
        
        return {
            "drivers": drivers,
            "orders": orders,
            "gps_events": gps_events,
            "weather_events": weather,
        }
    
    def save_as_jsonl(self, data: Dict[str, List[Dict]], 
                      output_dir: str) -> Dict[str, str]:
        """Save datasets as JSON Lines format.
        
        Args:
            data: Dictionary of datasets
            output_dir: Output directory path
        
        Returns:
            Dictionary mapping dataset name to file path
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        saved_files = {}
        
        for dataset_name, records in data.items():
            file_path = output_path / f"{dataset_name}.jsonl"
            
            with open(file_path, "w") as f:
                for record in records:
                    f.write(json.dumps(record) + "\n")
            
            saved_files[dataset_name] = str(file_path)
            logger.info(f"Saved {dataset_name}: {len(records):,} records to {file_path}")
        
        return saved_files
