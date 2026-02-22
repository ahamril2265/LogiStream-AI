"""Unit tests for data simulation module."""

import pytest
from datetime import datetime, timedelta
from data_simulation.generator import (
    DriverProfileGenerator,
    OrderGenerator,
    GPSEventGenerator,
    WeatherEventGenerator,
)


def test_driver_profile_generation():
    """Test driver profile generator."""
    generator = DriverProfileGenerator(num_drivers=10, seed=42)
    drivers = generator.generate()
    
    assert len(drivers) == 10
    assert all("driver_id" in d for d in drivers)
    assert all("vehicle_type" in d for d in drivers)
    assert all(d["vehicle_type"] in ["van", "truck", "bike", "car"] for d in drivers)
    assert all("experience_years" in d for d in drivers)


def test_order_generation():
    """Test order generator."""
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 31)
    
    generator = OrderGenerator(
        num_orders=100,
        num_drivers=10,
        start_date=start_date,
        end_date=end_date,
        seed=42
    )
    orders = generator.generate()
    
    assert len(orders) == 100
    assert all("order_id" in o for o in orders)
    assert all("driver_id" in o for o in orders)
    assert all("order_type" in o for o in orders)
    assert all(o["order_type"] in ["standard", "express", "scheduled", "frozen"] for o in orders)


def test_gps_event_generation():
    """Test GPS event generator."""
    # Create sample orders
    orders = [
        {
            "order_id": "ORD_001",
            "driver_id": "DRV_001",
            "created_at": datetime(2024, 1, 1, 10, 0).isoformat(),
            "pickup_lat": 40.7,
            "pickup_lon": -74.0,
            "delivery_lat": 40.71,
            "delivery_lon": -73.99,
            "actual_delivery_minutes": 30,
        }
    ]
    
    generator = GPSEventGenerator(
        orders=orders,
        events_per_delivery=10,
        seed=42
    )
    events = generator.generate()
    
    assert len(events) > 0
    assert all("event_id" in e for e in events)
    assert all("latitude" in e for e in events)
    assert all("longitude" in e for e in events)


def test_weather_event_generation():
    """Test weather event generator."""
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 31)
    
    generator = WeatherEventGenerator(
        start_date=start_date,
        end_date=end_date,
        seed=42
    )
    events = generator.generate()
    
    assert len(events) > 0
    assert all("weather_event_id" in e for e in events)
    assert all("weather_type" in e for e in events)
    assert all(e["weather_type"] in 
              ["clear", "rain", "snow", "fog", "storm", "wind"] 
              for e in events)
