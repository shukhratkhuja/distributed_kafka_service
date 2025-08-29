# src/__init__.py
"""
Event Processing Pipeline - High-volume event data processing and analytics
"""

__version__ = "1.0.0"
__author__ = "Event Processing Team"
__description__ = "High-volume event data processing and analytics pipeline"

# src/producer/__init__.py
"""
Event Producer Module - Generates and publishes events to Kafka
"""

from .event_generator import EventGenerator, EventData
from .kafka_producer import KafkaEventProducer

__all__ = [
    'EventGenerator',
    'EventData', 
    'KafkaEventProducer'
]

# src/consumer/__init__.py
"""
Event Consumer Module - Consumes, transforms and stores events
"""

from .kafka_consumer import EventProcessor, ClickHouseWriter, MongoDBWriter
from .transformer import EventTransformer, TransformedEvent, EventDeduplicator
from .database_writer import DualWriter, database_writer_context

__all__ = [
    'EventProcessor',
    'ClickHouseWriter',
    'MongoDBWriter',
    'EventTransformer',
    'TransformedEvent',
    'EventDeduplicator',
    'DualWriter',
    'database_writer_context'
]

# src/utils/__init__.py
"""
Utility Module - Configuration, monitoring and shared utilities
"""

from .config import AppConfig, ConfigManager, get_config
from .monitoring import MetricsCollector, HealthChecker, MonitoringService

__all__ = [
    'AppConfig',
    'ConfigManager', 
    'get_config',
    'MetricsCollector',
    'HealthChecker',
    'MonitoringService'
]

# tests/__init__.py
"""
Test Suite for Event Processing Pipeline
"""

# tests/conftest.py
import pytest
import asyncio
import os
from unittest.mock import AsyncMock, MagicMock

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(autouse=True)
def setup_test_environment():
    """Set up test environment variables"""
    os.environ['TESTING'] = '1'
    os.environ['CLICKHOUSE_HOST'] = 'localhost'
    os.environ['CLICKHOUSE_PORT'] = '8123'
    os.environ['MONGODB_URL'] = 'mongodb://localhost:27017/test_events'
    yield
    # Cleanup if needed

# Mock fixtures for external dependencies
@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer for testing"""
    producer = AsyncMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.send_and_wait = AsyncMock(return_value=True)
    return producer

# src/__init__.py
"""
Event Processing Pipeline - High-volume event data processing and analytics
"""

__version__ = "1.0.0"
__author__ = "Event Processing Team"
__description__ = "High-volume event data processing and analytics pipeline"

# src/producer/__init__.py
"""
Event Producer Module - Generates and publishes events to Kafka
"""

from .event_generator import EventGenerator, EventData
from .kafka_producer import KafkaEventProducer

__all__ = [
    'EventGenerator',
    'EventData', 
    'KafkaEventProducer'
]

# src/consumer/__init__.py
"""
Event Consumer Module - Consumes, transforms and stores events
"""

from .kafka_consumer import EventProcessor, ClickHouseWriter, MongoDBWriter
from .transformer import EventTransformer, TransformedEvent, EventDeduplicator
from .database_writer import DualWriter, database_writer_context

__all__ = [
    'EventProcessor',
    'ClickHouseWriter',
    'MongoDBWriter',
    'EventTransformer',
    'TransformedEvent',
    'EventDeduplicator',
    'DualWriter',
    'database_writer_context'
]

# src/utils/__init__.py
"""
Utility Module - Configuration, monitoring and shared utilities
"""

from .config import AppConfig, ConfigManager, get_config
from .monitoring import MetricsCollector, HealthChecker, MonitoringService

__all__ = [
    'AppConfig',
    'ConfigManager', 
    'get_config',
    'MetricsCollector',
    'HealthChecker',
    'MonitoringService'
]

# tests/__init__.py
"""
Test Suite for Event Processing Pipeline
"""

# tests/conftest.py
import pytest
import asyncio
import os
from unittest.mock import AsyncMock, MagicMock

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(autouse=True)
def setup_test_environment():
    """Set up test environment variables"""
    os.environ['TESTING'] = '1'
    os.environ['CLICKHOUSE_HOST'] = 'localhost'
    os.environ['CLICKHOUSE_PORT'] = '8123'
    os.environ['MONGODB_URL'] = 'mongodb://localhost:27017/test_events'
    yield
    # Cleanup if needed

# Mock fixtures for external dependencies
@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer for testing"""
    producer = AsyncMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.send_and_wait = AsyncMock(return_value=True)
    return producer

@pytest.fixture 
def mock_kafka_consumer():
    """Mock Kafka consumer for testing"""
    consumer = AsyncMock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.getmany = AsyncMock(return_value={})
    return consumer

@pytest.fixture
def mock_clickhouse_client():
    """Mock ClickHouse client for testing"""
    client = MagicMock()
    client.command = MagicMock(return_value=1)
    client.insert = MagicMock()
    client.query = MagicMock()
    return client

@pytest.fixture
def mock_mongodb_client():
    """Mock MongoDB client for testing"""
    client = AsyncMock()
    client.admin.command = AsyncMock(return_value={'ok': 1})
    
    # Mock collection
    collection = AsyncMock()
    collection.insert_many = AsyncMock()
    collection.create_index = AsyncMock()
    collection.find = AsyncMock()
    collection.count_documents = AsyncMock()
    
    client.__getitem__.return_value.__getitem__.return_value = collection
    return client

@pytest.fixture
def sample_raw_event():
    """Sample raw event for testing"""
    return {
        "event_id": "test-event-123",
        "user_id": 12345,
        "event_type": "page_view",
        "timestamp": "2024-01-15T10:30:00.000Z",
        "session_id": "session-abc-123",
        "ip_address": "192.168.1.100",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "referrer": "https://google.com",
        "page_url": "https://example.com/home",
        "properties": {
            "page_title": "Home Page",
            "load_time": 1.23
        }
    }

@pytest.fixture
def sample_transformed_events():
    """Sample transformed events for testing"""
    from datetime import datetime
    from src.consumer.transformer import TransformedEvent
    
    return [
        TransformedEvent(
            event_id="test-1",
            user_id=123,
            event_type="page_view",
            event_date="2024-01-15",
            timestamp=datetime(2024, 1, 15, 10, 30, 0),
            hour_of_day=10,
            day_of_week=0,
            is_weekend=False,
            event_category="engagement",
            session_id="session-123",
            ip_address="192.168.1.1",
            user_agent="Mozilla/5.0",
            referrer=None,
            page_url="https://example.com",
            properties={"page_title": "Home"},
            browser="Chrome",
            os="Windows",
            device_type="desktop",
            country="US",
            region="CA",
            city="San Francisco",
            processed_at=datetime(2024, 1, 15, 10, 30, 1)
        )
    ]

# Pytest markers for different test types
def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "kafka: mark test as requiring Kafka"
    )
    config.addinivalue_line(
        "markers", "clickhouse: mark test as requiring ClickHouse"
    )
    config.addinivalue_line(
        "markers", "mongodb: mark test as requiring MongoDB"
    )