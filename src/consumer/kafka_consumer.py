"""
Kafka Consumer and Database Writer
Consumes events from Kafka, transforms them, and writes to both MongoDB and ClickHouse
"""

import asyncio
import json
import logging
import os
import time
from typing import List, Dict, Any, Optional
import signal
import sys
from dataclasses import asdict

from aiokafka import AIOKafkaConsumer, ConsumerRecord
import motor.motor_asyncio
import clickhouse_connect
from pymongo.errors import BulkWriteError
import yaml

# Import our modules
from .transformer import EventTransformer, event_to_clickhouse_dict, event_to_mongodb_dict
from .database_writer import DualWriter, ClickHouseWriter, MongoDBWriter
from ..utils.monitoring import MonitoringService
from database_writer import DualWriter, ClickHouseWriter, MongoDBWriter
from monitoring import MonitoringService, monitoring_context

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ClickHouseWriter:
    """Async ClickHouse writer with connection pooling"""
    
    def __init__(self, host: str = 'localhost', port: int = 8123, 
                 database: str = 'analytics', username: str = 'default', password: str = ''):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.client = None
        
        # Statistics
        self.stats = {
            'total_written': 0,
            'batches_written': 0,
            'errors': 0,
            'last_write_time': None
        }
        
    async def connect(self):
        """Initialize ClickHouse client"""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password
            )
            
            # Test connection
            result = self.client.command('SELECT 1')
            logger.info(f"ClickHouse connected successfully: {self.host}:{self.port}")
            
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
            
    async def write_batch(self, events: List[Dict[str, Any]]) -> bool:
        """Write batch of events to ClickHouse"""
        if not events:
            return True
            
        try:
            # Convert events to format expected by ClickHouse
            data = []
            for event in events:
                data.append(list(event.values()))
                
            columns = list(events[0].keys())
            
            # Insert data
            self.client.insert(
                'events_distributed',
                data,
                column_names=columns
            )
            
            # Update statistics
            self.stats['total_written'] += len(events)
            self.stats['batches_written'] += 1
            self.stats['last_write_time'] = time.time()
            
            logger.debug(f"Wrote {len(events)} events to ClickHouse")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write batch to ClickHouse: {e}")
            self.stats['errors'] += 1
            return False
            
    def get_stats(self) -> Dict[str, Any]:
        """Get writer statistics"""
        return self.stats.copy()

class MongoDBWriter:
    """Async MongoDB writer with connection pooling"""
    
    def __init__(self, connection_string: str, database: str = 'events', collection: str = 'raw_events'):
        self.connection_string = connection_string
        self.database_name = database
        self.collection_name = collection
        self.client = None
        self.database = None
        self.collection = None
        
        # Statistics
        self.stats = {
            'total_written': 0,
            'batches_written': 0,
            'errors': 0,
            'last_write_time': None
        }
        
    async def connect(self):
        """Initialize MongoDB client"""
        try:
            self.client = motor.motor_asyncio.AsyncIOMotorClient(self.connection_string)
            self.database = self.client[self.database_name]
            self.collection = self.database[self.collection_name]
            
            # Test connection
            await self.client.admin.command('ismaster')
            logger.info(f"MongoDB connected successfully")
            
            # Create indexes
            await self._create_indexes()
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
            
    async def _create_indexes(self):
        """Create necessary indexes for the collection"""
        try:
            # Create compound indexes for common query patterns
            await self.collection.create_index([
                ("user_id", 1),
                ("timestamp", -1)
            ])
            
            await self.collection.create_index([
                ("event_type", 1),
                ("timestamp", -1)
            ])
            
            await self.collection.create_index([
                ("event_id", 1)
            ], unique=True)
            
            await self.collection.create_index([
                ("session_id", 1),
                ("timestamp", 1)
            ])
            
            logger.info("MongoDB indexes created successfully")
            
        except Exception as e:
            logger.warning(f"Failed to create some indexes: {e}")
            
    async def write_batch(self, events: List[Dict[str, Any]]) -> bool:
        """Write batch of events to MongoDB"""
        if not events:
            return True
            
        try:
            # Use ordered=False for better performance, but handle duplicates
            result = await self.collection.insert_many(events, ordered=False)
            
            # Update statistics
            self.stats['total_written'] += len(result.inserted_ids)
            self.stats['batches_written'] += 1
            self.stats['last_write_time'] = time.time()
            
            logger.debug(f"Wrote {len(result.inserted_ids)} events to MongoDB")
            return True
            
        except BulkWriteError as e:
            # Handle duplicate key errors gracefully
            inserted_count = e.details.get('nInserted', 0)
            self.stats['total_written'] += inserted_count
            self.stats['batches_written'] += 1
            self.stats['last_write_time'] = time.time()
            
            logger.warning(f"Bulk write with some errors: inserted {inserted_count}/{len(events)}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write batch to MongoDB: {e}")
            self.stats['errors'] += 1
            return False
            
    def get_stats(self) -> Dict[str, Any]:
        """Get writer statistics"""
        return self.stats.copy()

class EventProcessor:
    """Main event processing class that coordinates consumer, transformer, and writers"""
    
    def __init__(self, 
                 kafka_servers: str,
                 kafka_topic: str = 'events_raw',
                 kafka_group: str = 'event-processing-group',
                 batch_size: int = 1000,
                 max_workers: int = 4):
        
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.kafka_group = kafka_group
        self.batch_size = batch_size
        self.max_workers = max_workers
        
        # Components
        self.consumer = None
        self.transformer = EventTransformer()
        self.clickhouse_writer = None
        self.mongodb_writer = None
        
        # Control
        self.running = False
        self.shutdown_event = asyncio.Event()
        
        # Statistics
        self.stats = {
            'messages_consumed': 0,
            'events_processed': 0,
            'events_written_ch': 0,
            'events_written_mongo': 0,
            'processing_errors': 0,
            'start_time': None
        }
        
    async def initialize(self):
        """Initialize all components"""
        logger.info("Initializing event processor...")
        
        # Initialize Kafka consumer
        self.consumer = AIOKafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_servers,
            group_id=self.kafka_group,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            max_poll_records=self.batch_size,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        # Initialize database writers
        clickhouse_config = {
            'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
            'port': int(os.getenv('CLICKHOUSE_PORT', '8123')),
            'database': os.getenv('CLICKHOUSE_DB', 'analytics'),
            'username': os.getenv('CLICKHOUSE_USER', 'default'),
            'password': os.getenv('CLICKHOUSE_PASSWORD', '')
        }
        
        self.clickhouse_writer = ClickHouseWriter(**clickhouse_config)
        await self.clickhouse_writer.connect()
        
        mongodb_url = os.getenv('MONGODB_URL', 'mongodb://localhost:27017/events')
        self.mongodb_writer = MongoDBWriter(mongodb_url)
        await self.mongodb_writer.connect()
        
        # Start consumer
        await self.consumer.start()
        logger.info(f"Kafka consumer started, subscribed to: {self.kafka_topic}")
        
        self.stats['start_time'] = time.time()
        
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down event processor...")
        self.running = False
        self.shutdown_event.set()
        
        if self.consumer:
            await self.consumer.stop()
            
        logger.info("Event processor shutdown complete")
        
    async def process_batch(self, messages: List[ConsumerRecord]) -> Dict[str, int]:
        """Process a batch of Kafka messages"""
        batch_stats = {
            'consumed': len(messages),
            'transformed': 0,
            'written_ch': 0,
            'written_mongo': 0,
            'errors': 0
        }
        
        try:
            # Extract raw events from messages
            raw_events = []
            for message in messages:
                try:
                    raw_events.append(message.value)
                except Exception as e:
                    logger.warning(f"Failed to deserialize message: {e}")
                    batch_stats['errors'] += 1
                    
            # Transform events
            transformed_events = self.transformer.transform_batch(raw_events)
            batch_stats['transformed'] = len(transformed_events)
            
            if not transformed_events:
                return batch_stats
                
            # Prepare data for each database
            clickhouse_events = [event_to_clickhouse_dict(event) for event in transformed_events]
            mongodb_events = [event_to_mongodb_dict(event) for event in transformed_events]
            
            # Write to databases concurrently
            write_tasks = [
                self.clickhouse_writer.write_batch(clickhouse_events),
                self.mongodb_writer.write_batch(mongodb_events)
            ]
            
            results = await asyncio.gather(*write_tasks, return_exceptions=True)
            
            # Handle results
            if results[0] is True:
                batch_stats['written_ch'] = len(clickhouse_events)
            else:
                logger.error("Failed to write batch to ClickHouse")
                batch_stats['errors'] += 1
                
            if results[1] is True:
                batch_stats['written_mongo'] = len(mongodb_events)
            else:
                logger.error("Failed to write batch to MongoDB")
                batch_stats['errors'] += 1
                
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            batch_stats['errors'] += 1
            
        return batch_stats
        
    async def run(self):
        """Main processing loop"""
        self.running = True
        logger.info("Starting event processing loop...")
        
        batch_buffer = []
        last_stats_time = time.time()
        
        try:
            while self.running:
                try:
                    # Consume messages with timeout
                    msg_pack = await asyncio.wait_for(
                        self.consumer.getmany(timeout_ms=1000, max_records=self.batch_size),
                        timeout=2.0
                    )
                    
                    # Process messages from all partitions
                    for tp, messages in msg_pack.items():
                        batch_buffer.extend(messages)
                        self.stats['messages_consumed'] += len(messages)
                        
                    # Process batch when buffer is full or timeout
                    if len(batch_buffer) >= self.batch_size or \
                       (batch_buffer and time.time() - last_stats_time > 5):
                        
                        batch_stats = await self.process_batch(batch_buffer)
                        
                        # Update global statistics
                        self.stats['events_processed'] += batch_stats['transformed']
                        self.stats['events_written_ch'] += batch_stats['written_ch']
                        self.stats['events_written_mongo'] += batch_stats['written_mongo']
                        self.stats['processing_errors'] += batch_stats['errors']
                        
                        # Clear buffer
                        batch_buffer = []
                        
                        # Log progress
                        if time.time() - last_stats_time > 10:  # Every 10 seconds
                            await self._log_stats()
                            last_stats_time = time.time()
                            
                except asyncio.TimeoutError:
                    # Process any remaining messages in buffer
                    if batch_buffer:
                        batch_stats = await self.process_batch(batch_buffer)
                        
                        self.stats['events_processed'] += batch_stats['transformed']
                        self.stats['events_written_ch'] += batch_stats['written_ch']
                        self.stats['events_written_mongo'] += batch_stats['written_mongo']
                        self.stats['processing_errors'] += batch_stats['errors']
                        
                        batch_buffer = []
                        
                except Exception as e:
                    logger.error(f"Error in processing loop: {e}")
                    await asyncio.sleep(1)  # Brief pause before retrying
                    
        except Exception as e:
            logger.error(f"Fatal error in event processor: {e}")
            raise
        finally:
            # Process any remaining messages
            if batch_buffer:
                await self.process_batch(batch_buffer)
                
    async def _log_stats(self):
        """Log current statistics"""
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['events_processed'] / elapsed if elapsed > 0 else 0
        
        logger.info(f"Processing Stats:")
        logger.info(f"  Messages consumed: {self.stats['messages_consumed']}")
        logger.info(f"  Events processed: {self.stats['events_processed']}")
        logger.info(f"  Events/sec: {rate:.2f}")
        logger.info(f"  Written to ClickHouse: {self.stats['events_written_ch']}")
        logger.info(f"  Written to MongoDB: {self.stats['events_written_mongo']}")
        logger.info(f"  Processing errors: {self.stats['processing_errors']}")
        
        # Log transformer stats
        transform_stats = self.transformer.get_stats()
        logger.info(f"  Transformer - Duplicates: {transform_stats['duplicates']}")
        logger.info(f"  Transformer - Enriched: {transform_stats['enriched']}")
        
        # Log database writer stats
        ch_stats = self.clickhouse_writer.get_stats()
        mongo_stats = self.mongodb_writer.get_stats()
        
        logger.info(f"  ClickHouse - Batches: {ch_stats['batches_written']}, Errors: {ch_stats['errors']}")
        logger.info(f"  MongoDB - Batches: {mongo_stats['batches_written']}, Errors: {mongo_stats['errors']}")

class HealthChecker:
    """Health check service for monitoring"""
    
    def __init__(self, processor: EventProcessor):
        self.processor = processor
        self.last_health_check = time.time()
        
    async def start_health_server(self, port: int = 8080):
        """Start simple HTTP health check server"""
        from aiohttp import web
        
        async def health_check(request):
            """Health check endpoint"""
            try:
                # Check if processor is running
                if not self.processor.running:
                    return web.json_response(
                        {"status": "unhealthy", "reason": "processor_stopped"}, 
                        status=503
                    )
                    
                # Check if we're receiving messages
                current_time = time.time()
                if current_time - self.processor.stats.get('start_time', current_time) > 60:
                    # Check if we've processed messages recently
                    last_processed = self.processor.stats.get('last_process_time', 0)
                    if current_time - last_processed > 120:  # No processing in 2 minutes
                        return web.json_response(
                            {"status": "degraded", "reason": "no_recent_processing"}, 
                            status=200
                        )
                
                return web.json_response({
                    "status": "healthy",
                    "stats": self.processor.stats,
                    "uptime": current_time - self.processor.stats.get('start_time', current_time)
                })
                
            except Exception as e:
                return web.json_response(
                    {"status": "error", "error": str(e)}, 
                    status=500
                )
                
        app = web.Application()
        app.router.add_get('/health', health_check)
        app.router.add_get('/metrics', health_check)  # Alias for Prometheus
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        
        logger.info(f"Health check server started on port {port}")

async def main():
    """Main function"""
    # Configuration from environment
    KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'events_raw')
    KAFKA_GROUP = os.getenv('KAFKA_GROUP', 'event-processing-group')
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
    MAX_WORKERS = int(os.getenv('MAX_WORKERS', '4'))
    HEALTH_PORT = int(os.getenv('HEALTH_PORT', '8080'))
    
    # Create processor
    processor = EventProcessor(
        kafka_servers=KAFKA_SERVERS,
        kafka_topic=KAFKA_TOPIC,
        kafka_group=KAFKA_GROUP,
        batch_size=BATCH_SIZE,
        max_workers=MAX_WORKERS
    )
    
    # Setup graceful shutdown
    def signal_handler():
        logger.info("Received shutdown signal")
        asyncio.create_task(processor.shutdown())
        
    # Register signal handlers
    for sig in [signal.SIGTERM, signal.SIGINT]:
        signal.signal(sig, lambda s, f: signal_handler())
    
    try:
        # Initialize components
        await processor.initialize()
        
        # Start health check server
        health_checker = HealthChecker(processor)
        await health_checker.start_health_server(HEALTH_PORT)
        
        logger.info("Event processor initialized successfully")
        logger.info(f"Configuration:")
        logger.info(f"  Kafka servers: {KAFKA_SERVERS}")
        logger.info(f"  Topic: {KAFKA_TOPIC}")
        logger.info(f"  Consumer group: {KAFKA_GROUP}")
        logger.info(f"  Batch size: {BATCH_SIZE}")
        logger.info(f"  Max workers: {MAX_WORKERS}")
        logger.info(f"  Health port: {HEALTH_PORT}")
        
        # Run processor
        await processor.run()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        await processor.shutdown()
        logger.info("Event processor stopped")

if __name__ == "__main__":
    asyncio.run(main())