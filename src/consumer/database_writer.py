"""
Database Writer Module - Handles writes to both MongoDB and ClickHouse
"""

import asyncio
import logging
import time
from typing import List, Dict, Any, Optional
import json
from contextlib import asynccontextmanager

import motor.motor_asyncio
import clickhouse_connect
from pymongo.errors import BulkWriteError, ServerSelectionTimeoutError, ConnectionFailure
from clickhouse_connect.driver.exceptions import ClickHouseError

logger = logging.getLogger(__name__)

class DatabaseWriter:
    """Base class for database writers"""
    
    def __init__(self):
        self.stats = {
            'total_written': 0,
            'batches_written': 0,
            'errors': 0,
            'last_write_time': None,
            'connection_errors': 0,
            'retry_count': 0
        }
        self.connected = False
    
    async def connect(self):
        """Initialize database connection"""
        raise NotImplementedError
    
    async def disconnect(self):
        """Close database connection"""
        raise NotImplementedError
    
    async def write_batch(self, events: List[Dict[str, Any]]) -> bool:
        """Write batch of events"""
        raise NotImplementedError
    
    def get_stats(self) -> Dict[str, Any]:
        """Get writer statistics"""
        return self.stats.copy()
    
    async def health_check(self) -> bool:
        """Check if connection is healthy"""
        raise NotImplementedError

class ClickHouseWriter(DatabaseWriter):
    """Async ClickHouse writer with connection pooling and retry logic"""
    
    def __init__(self, 
                 host: str = 'localhost', 
                 port: int = 8123, 
                 database: str = 'analytics', 
                 username: str = 'default', 
                 password: str = '',
                 table: str = 'events_distributed',
                 max_retries: int = 3,
                 retry_delay: float = 1.0):
        super().__init__()
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.table = table
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.client = None
        
        # Performance settings
        self.insert_settings = {
            'async_insert': 1,
            'wait_for_async_insert': 0,
            'async_insert_max_data_size': 10485760,  # 10MB
            'async_insert_busy_timeout_ms': 200
        }
        
    async def connect(self):
        """Initialize ClickHouse client"""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password,
                connect_timeout=30,
                send_receive_timeout=300,
                compress=True
            )
            
            # Test connection with simple query
            result = self.client.command('SELECT 1 as test')
            if result != 1:
                raise Exception("Connection test failed")
                
            self.connected = True
            logger.info(f"ClickHouse connected successfully: {self.host}:{self.port}/{self.database}")
            
        except Exception as e:
            self.stats['connection_errors'] += 1
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
            
    async def disconnect(self):
        """Close ClickHouse connection"""
        if self.client:
            self.client.close()
            self.connected = False
            logger.info("ClickHouse connection closed")
            
    async def health_check(self) -> bool:
        """Check ClickHouse connection health"""
        try:
            if not self.client:
                return False
            result = self.client.command('SELECT 1')
            return result == 1
        except Exception as e:
            logger.warning(f"ClickHouse health check failed: {e}")
            return False
    
    async def _prepare_data_for_insert(self, events: List[Dict[str, Any]]) -> tuple:
        """Prepare data for ClickHouse insertion"""
        if not events:
            return [], []
            
        # Use the first event to determine column order
        columns = list(events[0].keys())
        
        # Convert events to list of lists for efficient insertion
        data_rows = []
        for event in events:
            row = []
            for col in columns:
                value = event.get(col)
                
                # Handle special data type conversions
                if value is None:
                    if col in ['referrer', 'user_agent', 'page_url', 'properties']:
                        value = ''
                    elif col in ['user_id', 'hour_of_day', 'day_of_week', 'is_weekend']:
                        value = 0
                    else:
                        value = ''
                        
                # Ensure strings are not None
                if isinstance(value, str) and value is None:
                    value = ''
                    
                row.append(value)
            data_rows.append(row)
            
        return columns, data_rows
    
    async def write_batch(self, events: List[Dict[str, Any]]) -> bool:
        """Write batch of events to ClickHouse with retry logic"""
        if not events:
            return True
            
        for attempt in range(self.max_retries):
            try:
                # Check connection health
                if not await self.health_check():
                    if attempt == 0:  # Only reconnect on first attempt
                        await self.connect()
                
                # Prepare data for insertion
                columns, data_rows = await self._prepare_data_for_insert(events)
                
                # Insert data with optimized settings
                self.client.insert(
                    self.table,
                    data_rows,
                    column_names=columns,
                    settings=self.insert_settings
                )
                
                # Update statistics
                self.stats['total_written'] += len(events)
                self.stats['batches_written'] += 1
                self.stats['last_write_time'] = time.time()
                
                logger.debug(f"Successfully wrote {len(events)} events to ClickHouse")
                return True
                
            except ClickHouseError as e:
                self.stats['errors'] += 1
                logger.error(f"ClickHouse error on attempt {attempt + 1}: {e}")
                
                # Don't retry on certain errors
                if 'SYNTAX_ERROR' in str(e) or 'TYPE_MISMATCH' in str(e):
                    logger.error(f"Non-retryable ClickHouse error: {e}")
                    return False
                    
            except Exception as e:
                self.stats['errors'] += 1
                logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
            
            # Wait before retry
            if attempt < self.max_retries - 1:
                await asyncio.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                self.stats['retry_count'] += 1
                
        logger.error(f"Failed to write batch after {self.max_retries} attempts")
        return False

class MongoDBWriter(DatabaseWriter):
    """Async MongoDB writer with connection pooling and retry logic"""
    
    def __init__(self, 
                 connection_string: str,
                 database: str = 'events', 
                 collection: str = 'raw_events',
                 max_retries: int = 3,
                 retry_delay: float = 1.0):
        super().__init__()
        self.connection_string = connection_string
        self.database_name = database
        self.collection_name = collection
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        self.client = None
        self.database = None
        self.collection = None
        
        # Connection settings
        self.client_settings = {
            'maxPoolSize': 50,
            'minPoolSize': 5,
            'maxIdleTimeMS': 30000,
            'waitQueueTimeoutMS': 5000,
            'serverSelectionTimeoutMS': 30000,
            'connectTimeoutMS': 20000,
            'socketTimeoutMS': 20000,
            'retryWrites': True,
            'retryReads': True
        }
        
    async def connect(self):
        """Initialize MongoDB client"""
        try:
            self.client = motor.motor_asyncio.AsyncIOMotorClient(
                self.connection_string,
                **self.client_settings
            )
            
            self.database = self.client[self.database_name]
            self.collection = self.database[self.collection_name]
            
            # Test connection
            await self.client.admin.command('ping')
            
            # Create indexes for better performance
            await self._ensure_indexes()
            
            self.connected = True
            logger.info(f"MongoDB connected successfully: database={self.database_name}, collection={self.collection_name}")
            
        except Exception as e:
            self.stats['connection_errors'] += 1
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
            
    async def disconnect(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            self.connected = False
            logger.info("MongoDB connection closed")
    
    async def health_check(self) -> bool:
        """Check MongoDB connection health"""
        try:
            if not self.client:
                return False
            await self.client.admin.command('ping')
            return True
        except Exception as e:
            logger.warning(f"MongoDB health check failed: {e}")
            return False
    
    async def _ensure_indexes(self):
        """Create necessary indexes for optimal performance"""
        try:
            indexes_to_create = [
                # Compound index for user queries
                ([("user_id", 1), ("timestamp", -1)], {"background": True}),
                
                # Event type queries
                ([("event_type", 1), ("timestamp", -1)], {"background": True}),
                
                # Unique constraint on event_id
                ([("event_id", 1)], {"unique": True, "background": True}),
                
                # Session-based queries
                ([("session_id", 1), ("timestamp", 1)], {"background": True}),
                
                # Time-based queries
                ([("timestamp", -1)], {"background": True}),
                
                # Category-based queries
                ([("event_category", 1), ("timestamp", -1)], {"background": True}),
                
                # Geo-based queries
                ([("country", 1), ("timestamp", -1)], {"background": True, "sparse": True}),
            ]
            
            for index_spec, options in indexes_to_create:
                try:
                    await self.collection.create_index(index_spec, **options)
                except Exception as e:
                    logger.warning(f"Failed to create index {index_spec}: {e}")
            
            logger.info("MongoDB indexes created successfully")
            
        except Exception as e:
            logger.warning(f"Failed to create some indexes: {e}")
    
    async def write_batch(self, events: List[Dict[str, Any]]) -> bool:
        """Write batch of events to MongoDB with retry logic"""
        if not events:
            return True
            
        for attempt in range(self.max_retries):
            try:
                # Check connection health
                if not await self.health_check():
                    if attempt == 0:  # Only reconnect on first attempt
                        await self.connect()
                
                # Use ordered=False for better performance and handle duplicates gracefully
                result = await self.collection.insert_many(
                    events, 
                    ordered=False,
                    bypass_document_validation=False
                )
                
                # Update statistics
                inserted_count = len(result.inserted_ids)
                self.stats['total_written'] += inserted_count
                self.stats['batches_written'] += 1
                self.stats['last_write_time'] = time.time()
                
                logger.debug(f"Successfully wrote {inserted_count} events to MongoDB")
                return True
                
            except BulkWriteError as e:
                # Handle partial success with duplicate key errors
                details = e.details
                inserted_count = details.get('nInserted', 0)
                write_errors = details.get('writeErrors', [])
                
                # Count non-duplicate errors
                non_duplicate_errors = [
                    error for error in write_errors 
                    if error.get('code') != 11000  # Duplicate key error code
                ]
                
                if inserted_count > 0:
                    self.stats['total_written'] += inserted_count
                    self.stats['batches_written'] += 1
                    self.stats['last_write_time'] = time.time()
                
                if non_duplicate_errors:
                    self.stats['errors'] += len(non_duplicate_errors)
                    logger.warning(f"Bulk write with {len(non_duplicate_errors)} non-duplicate errors")
                    
                    # If too many non-duplicate errors, consider it a failure
                    if len(non_duplicate_errors) > len(events) * 0.5:
                        logger.error(f"Too many write errors: {len(non_duplicate_errors)}/{len(events)}")
                        return False
                
                logger.info(f"Wrote {inserted_count}/{len(events)} events (duplicates skipped)")
                return True
                
            except (ServerSelectionTimeoutError, ConnectionFailure) as e:
                self.stats['connection_errors'] += 1
                logger.error(f"MongoDB connection error on attempt {attempt + 1}: {e}")
                
            except Exception as e:
                self.stats['errors'] += 1
                logger.error(f"Unexpected MongoDB error on attempt {attempt + 1}: {e}")
            
            # Wait before retry
            if attempt < self.max_retries - 1:
                await asyncio.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                self.stats['retry_count'] += 1
                
        logger.error(f"Failed to write MongoDB batch after {self.max_retries} attempts")
        return False

class DualWriter:
    """Coordinates writes to both ClickHouse and MongoDB"""
    
    def __init__(self, clickhouse_writer: ClickHouseWriter, mongodb_writer: MongoDBWriter):
        self.clickhouse_writer = clickhouse_writer
        self.mongodb_writer = mongodb_writer
        self.stats = {
            'total_batches': 0,
            'successful_batches': 0,
            'ch_failures': 0,
            'mongo_failures': 0,
            'dual_failures': 0
        }
    
    async def connect_all(self):
        """Connect to both databases"""
        tasks = [
            self.clickhouse_writer.connect(),
            self.mongodb_writer.connect()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check if any connections failed
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                db_name = "ClickHouse" if i == 0 else "MongoDB"
                logger.error(f"Failed to connect to {db_name}: {result}")
                raise result
    
    async def disconnect_all(self):
        """Disconnect from both databases"""
        tasks = [
            self.clickhouse_writer.disconnect(),
            self.mongodb_writer.disconnect()
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def write_dual_batch(self, 
                              clickhouse_events: List[Dict[str, Any]], 
                              mongodb_events: List[Dict[str, Any]]) -> Dict[str, bool]:
        """Write to both databases concurrently"""
        self.stats['total_batches'] += 1
        
        # Execute writes concurrently
        tasks = [
            self.clickhouse_writer.write_batch(clickhouse_events),
            self.mongodb_writer.write_batch(mongodb_events)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Analyze results
        ch_success = results[0] is True
        mongo_success = results[1] is True
        
        # Update statistics
        if not ch_success:
            self.stats['ch_failures'] += 1
            if isinstance(results[0], Exception):
                logger.error(f"ClickHouse write failed: {results[0]}")
        
        if not mongo_success:
            self.stats['mongo_failures'] += 1
            if isinstance(results[1], Exception):
                logger.error(f"MongoDB write failed: {results[1]}")
        
        if ch_success and mongo_success:
            self.stats['successful_batches'] += 1
        elif not ch_success and not mongo_success:
            self.stats['dual_failures'] += 1
        
        return {
            'clickhouse': ch_success,
            'mongodb': mongo_success,
            'overall': ch_success or mongo_success  # Success if at least one succeeds
        }
    
    async def health_check_all(self) -> Dict[str, bool]:
        """Check health of both databases"""
        tasks = [
            self.clickhouse_writer.health_check(),
            self.mongodb_writer.health_check()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return {
            'clickhouse': results[0] is True,
            'mongodb': results[1] is True
        }
    
    def get_combined_stats(self) -> Dict[str, Any]:
        """Get combined statistics from both writers"""
        ch_stats = self.clickhouse_writer.get_stats()
        mongo_stats = self.mongodb_writer.get_stats()
        
        return {
            'dual_writer': self.stats.copy(),
            'clickhouse': ch_stats,
            'mongodb': mongo_stats,
            'total_events_written': ch_stats['total_written'] + mongo_stats['total_written'],
            'combined_error_rate': (ch_stats['errors'] + mongo_stats['errors']) / 
                                 max(1, ch_stats['batches_written'] + mongo_stats['batches_written'])
        }

@asynccontextmanager
async def database_writer_context(clickhouse_config: Dict[str, Any], 
                                mongodb_config: Dict[str, Any]):
    """Context manager for database writers"""
    
    # Create writers
    ch_writer = ClickHouseWriter(**clickhouse_config)
    mongo_writer = MongoDBWriter(**mongodb_config)
    dual_writer = DualWriter(ch_writer, mongo_writer)
    
    try:
        # Connect to both databases
        await dual_writer.connect_all()
        yield dual_writer
        
    finally:
        # Clean up connections
        await dual_writer.disconnect_all()

# Utility functions for configuration
def create_clickhouse_config(host: str = 'localhost', 
                           port: int = 8123,
                           database: str = 'analytics',
                           username: str = 'default',
                           password: str = '') -> Dict[str, Any]:
    """Create ClickHouse configuration dictionary"""
    return {
        'host': host,
        'port': port,
        'database': database,
        'username': username,
        'password': password
    }

def create_mongodb_config(connection_string: str,
                        database: str = 'events',
                        collection: str = 'raw_events') -> Dict[str, Any]:
    """Create MongoDB configuration dictionary"""
    return {
        'connection_string': connection_string,
        'database': database,
        'collection': collection
    }