"""
Streaming module for Kafka-based data pipeline.
"""

from .producer import LogisticsProducer, main as producer_main
from .consumer import LogisticsBronzeConsumer, consume_topic, main as consumer_main

__all__ = [
    "LogisticsProducer",
    "LogisticsBronzeConsumer",
    "consume_topic",
    "producer_main",
    "consumer_main",
]
