# tests/test_transformer.py
import pytest
import json
from datetime import datetime
from src.consumer.transformer import EventTransformer, EventDeduplicator

class TestEventDeduplicator:
    
    def test_duplicate_detection(self):
        """Test duplicate event detection"""
        deduplicator = EventDeduplicator(max_size=10)
        
        # First occurrence should not be duplicate
        assert not deduplicator.is_duplicate("event_1")
        
        # Second occurrence should be duplicate
        assert deduplicator.is_duplicate("event_1")
        
        # Different event should not be duplicate
        assert not deduplicator.is_duplicate("event_2")
        
    def test_cache_size_limit(self):
        """Test cache size management"""
        deduplicator = EventDeduplicator(max_size=5)
        
        # Add more events than max_size
        for i in range(10):
            deduplicator.is_duplicate(f"event_{i}")
            
        # Cache should be managed (not exceed max_size by much)
        assert len(deduplicator.seen_events) <= 10  # Some cleanup tolerance

class TestEventTransformer:
    
    @pytest.fixture
    def mongodb_writer(self):
        """Create MongoDBWriter instance for testing"""
        return MongoDBWriter(
            connection_string='mongodb://localhost:27017',
            database='test_events',
            collection='test_raw_events'
        )
        
    @pytest.fixture
    def sample_mongo_events(self):
        """Sample MongoDB events for testing"""
        return [
            {
                'event_id': 'test-1',
                'user_id': 123,
                'event_type': 'page_view',
                'timestamp': '2024-01-15T10:30:00.000Z',
                'session_id': 'session-123',
                'ip_address': '192.168.1.1',
                'user_agent': 'Mozilla/5.0',
                'referrer': None,
                'page_url': 'https://example.com',
                'properties': {'page_title': 'Home'},
                'event_category': 'engagement',
                'geo': {'country': 'US', 'region': 'CA', 'city': 'SF'},
                'device': {'browser': 'Chrome', 'os': 'Windows'},
                'processed_at': '2024-01-15T10:30:01.000Z'
            }
        ]
        
    @patch('motor.motor_asyncio.AsyncIOMotorClient')
    async def test_connect_success(self, mock_motor_client, mongodb_writer):
        """Test successful MongoDB connection"""
        mock_client_instance = AsyncMock()
        mock_client_instance.admin.command = AsyncMock(return_value={'ok': 1})
        mock_motor_client.return_value = mock_client_instance
        
        # Mock collection for index creation
        mock_collection = AsyncMock()
        mock_collection.create_index = AsyncMock()
        mock_client_instance.__getitem__.return_value.__getitem__.return_value = mock_collection
        
        await mongodb_writer.connect()
        
        assert mongodb_writer.client is not None
        mock_motor_client.assert_called_once_with('mongodb://localhost:27017')
        
    async def test_write_batch_empty(self, mongodb_writer):
        """Test writing empty batch"""
        result = await mongodb_writer.write_batch([])
        assert result is True
        
    @patch('motor.motor_asyncio.AsyncIOMotorClient')
    async def test_write_batch_success(self, mock_motor_client, mongodb_writer, sample_mongo_events):
        """Test successful batch write"""
        mock_client_instance = AsyncMock()
        mock_client_instance.admin.command = AsyncMock(return_value={'ok': 1})
        
        # Mock successful insert
        mock_collection = AsyncMock()
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_ids = ['id1']
        mock_collection.insert_many = AsyncMock(return_value=mock_insert_result)
        mock_collection.create_index = AsyncMock()
        
        mock_client_instance.__getitem__.return_value.__getitem__.return_value = mock_collection
        mock_motor_client.return_value = mock_client_instance
        
        await mongodb_writer.connect()
        result = await mongodb_writer.write_batch(sample_mongo_events)
        
        assert result is True
        assert mongodb_writer.stats['total_written'] == 1
        assert mongodb_writer.stats['batches_written'] == 1
        
    def test_get_stats(self, mongodb_writer):
        """Test getting writer statistics"""
        stats = mongodb_writer.get_stats()
        
        assert 'total_written' in stats
        assert 'batches_written' in stats
        assert 'errors' in stats
        assert 'last_write_time' in stats

# Integration test helpers
@pytest.mark.integration
class TestEventProcessorIntegration:
    """Integration tests requiring actual services"""
    
    @pytest.fixture(scope="session")
    def docker_compose_file(self, pytestconfig):
        return "docker-compose.yml"
        
    @pytest.mark.asyncio
    async def test_end_to_end_processing(self):
        """Test complete event processing pipeline"""
        # This test would require running services
        # Implementation depends on pytest-docker-compose or similar
        pass

if __name__ == "__main__":
    pytest.main([__file__])

---

# tests/conftest.py
import pytest
import asyncio
import os

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
    from unittest.mock import AsyncMock
    producer = AsyncMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.send_and_wait = AsyncMock(return_value=True)
    return producer

@pytest.fixture 
def mock_kafka_consumer():
    """Mock Kafka consumer for testing"""
    from unittest.mock import AsyncMock
    consumer = AsyncMock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.getmany = AsyncMock(return_value={})
    return consumer.fixture
    def transformer(self):
        """Create transformer instance for testing"""
        return EventTransformer()
        
    @pytest.fixture
    def sample_event(self):
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
        
    def test_validate_event_valid(self, transformer, sample_event):
        """Test validation of valid event"""
        assert transformer._validate_event(sample_event) is True
        
    def test_validate_event_missing_required_field(self, transformer, sample_event):
        """Test validation with missing required field"""
        del sample_event['event_id']
        assert transformer._validate_event(sample_event) is False
        
    def test_validate_event_invalid_user_id(self, transformer, sample_event):
        """Test validation with invalid user_id"""
        sample_event['user_id'] = "not_a_number"
        assert transformer._validate_event(sample_event) is False
        
    def test_normalize_timestamp_iso_format(self, transformer):
        """Test timestamp normalization with ISO format"""
        timestamp_str = "2024-01-15T10:30:00.000Z"
        result = transformer._normalize_timestamp(timestamp_str)
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10
        
    def test_derive_time_fields(self, transformer):
        """Test time field derivation"""
        timestamp = datetime(2024, 1, 15, 10, 30, 0)  # Monday
        time_fields = transformer._derive_time_fields(timestamp)
        
        assert time_fields['event_date'] == '2024-01-15'
        assert time_fields['hour_of_day'] == 10
        assert time_fields['day_of_week'] == 0  # Monday
        assert time_fields['is_weekend'] is False
        
    def test_derive_time_fields_weekend(self, transformer):
        """Test time field derivation for weekend"""
        timestamp = datetime(2024, 1, 13, 15, 0, 0)  # Saturday
        time_fields = transformer._derive_time_fields(timestamp)
        
        assert time_fields['day_of_week'] == 5  # Saturday
        assert time_fields['is_weekend'] is True
        
    def test_get_event_category(self, transformer):
        """Test event category mapping"""
        assert transformer._get_event_category('page_view') == 'engagement'
        assert transformer._get_event_category('purchase') == 'revenue'
        assert transformer._get_event_category('unknown_event') == 'other'
        
    def test_transform_event_success(self, transformer, sample_event):
        """Test successful event transformation"""
        transformed = transformer.transform_event(sample_event)
        
        assert transformed is not None
        assert transformed.event_id == sample_event['event_id']
        assert transformed.user_id == sample_event['user_id']
        assert transformed.event_type == sample_event['event_type']
        assert transformed.event_category == 'engagement'
        assert transformed.hour_of_day == 10
        assert transformed.is_weekend is False
        
    def test_transform_event_duplicate(self, transformer, sample_event):
        """Test duplicate event handling"""
        # First transformation should succeed
        transformed1 = transformer.transform_event(sample_event)
        assert transformed1 is not None
        
        # Second transformation of same event should return None (duplicate)
        transformed2 = transformer.transform_event(sample_event)
        assert transformed2 is None
        
    def test_transform_event_invalid(self, transformer):
        """Test transformation of invalid event"""
        invalid_event = {"invalid": "event"}
        transformed = transformer.transform_event(invalid_event)
        assert transformed is None
        
    def test_transform_batch(self, transformer, sample_event):
        """Test batch transformation"""
        # Create batch with multiple events
        events = []
        for i in range(5):
            event = sample_event.copy()
            event['event_id'] = f"event-{i}"
            event['user_id'] = 1000 + i
            events.append(event)
            
        transformed_events = transformer.transform_batch(events)
        
        assert len(transformed_events) == 5
        assert all(event.event_category == 'engagement' for event in transformed_events)
        
    def test_stats_tracking(self, transformer, sample_event):
        """Test statistics tracking"""
        initial_stats = transformer.get_stats()
        
        # Transform some events
        transformer.transform_event(sample_event)
        transformer.transform_event(sample_event)  # Duplicate
        
        invalid_event = {"invalid": "event"}
        transformer.transform_event(invalid_event)  # Invalid
        
        final_stats = transformer.get_stats()
        
        assert final_stats['processed'] == initial_stats['processed'] + 3
        assert final_stats['duplicates'] == initial_stats['duplicates'] + 1
        assert final_stats['errors'] == initial_stats['errors'] + 1

---

# tests/test_database_writer.py
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from src.consumer.kafka_consumer import ClickHouseWriter, MongoDBWriter

class TestClickHouseWriter:
    
    @pytest.fixture
    def clickhouse_writer(self):
        """Create ClickHouseWriter instance for testing"""
        return ClickHouseWriter(
            host='localhost',
            port=8123,
            database='test_analytics'
        )
        
    @pytest.fixture
    def sample_events(self):
        """Sample events for testing"""
        return [
            {
                'event_id': 'test-1',
                'user_id': 123,
                'event_type': 'page_view',
                'event_date': '2024-01-15',
                'timestamp': '2024-01-15 10:30:00.000',
                'hour_of_day': 10,
                'day_of_week': 0,
                'is_weekend': 0,
                'event_category': 'engagement',
                'session_id': 'session-123',
                'ip_address': '192.168.1.1',
                'user_agent': 'Mozilla/5.0',
                'referrer': '',
                'page_url': 'https://example.com',
                'properties': '{}',
                'processed_at': '2024-01-15 10:30:01.000'
            },
            {
                'event_id': 'test-2',
                'user_id': 124,
                'event_type': 'button_click',
                'event_date': '2024-01-15',
                'timestamp': '2024-01-15 10:31:00.000',
                'hour_of_day': 10,
                'day_of_week': 0,
                'is_weekend': 0,
                'event_category': 'engagement',
                'session_id': 'session-124',
                'ip_address': '192.168.1.2',
                'user_agent': 'Mozilla/5.0',
                'referrer': '',
                'page_url': 'https://example.com/page',
                'properties': '{"button": "subscribe"}',
                'processed_at': '2024-01-15 10:31:01.000'
            }
        ]
        
    @patch('clickhouse_connect.get_client')
    async def test_connect_success(self, mock_get_client, clickhouse_writer):
        """Test successful ClickHouse connection"""
        mock_client = MagicMock()
        mock_client.command.return_value = 1
        mock_get_client.return_value = mock_client
        
        await clickhouse_writer.connect()
        
        assert clickhouse_writer.client is not None
        mock_get_client.assert_called_once_with(
            host='localhost',
            port=8123,
            database='test_analytics',
            username='default',
            password=''
        )
        
    @patch('clickhouse_connect.get_client')
    async def test_connect_failure(self, mock_get_client, clickhouse_writer):
        """Test ClickHouse connection failure"""
        mock_get_client.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception, match="Connection failed"):
            await clickhouse_writer.connect()
            
    async def test_write_batch_empty(self, clickhouse_writer):
        """Test writing empty batch"""
        result = await clickhouse_writer.write_batch([])
        assert result is True
        
    @patch('clickhouse_connect.get_client')
    async def test_write_batch_success(self, mock_get_client, clickhouse_writer, sample_events):
        """Test successful batch write"""
        mock_client = MagicMock()
        mock_client.command.return_value = 1
        mock_client.insert.return_value = None
        mock_get_client.return_value = mock_client
        
        await clickhouse_writer.connect()
        
        result = await clickhouse_writer.write_batch(sample_events)
        
        assert result is True
        assert clickhouse_writer.stats['total_written'] == 2
        assert clickhouse_writer.stats['batches_written'] == 1
        mock_client.insert.assert_called_once()
        
    def test_get_stats(self, clickhouse_writer):
        """Test getting writer statistics"""
        stats = clickhouse_writer.get_stats()
        
        assert 'total_written' in stats
        assert 'batches_written' in stats
        assert 'errors' in stats
        assert 'last_write_time' in stats

class TestMongoDBWriter:
    
    @pytest