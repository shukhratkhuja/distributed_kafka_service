"""
Dedicated Kafka Producer Module
High-performance Kafka producer optimized for event streaming
"""

import asyncio
import json
import logging
import time
from typing import Dict, List, Any, Optional, Callable
from dataclasses import asdict
import aiokafka
from aiokafka.errors import KafkaError, KafkaTimeoutError
import hashlib

logger = logging.getLogger(__name__)

class KafkaProducerConfig:
    """Configuration for Kafka producer"""
    
    def __init__(self,
                 bootstrap_servers: str = "localhost:9092",
                 topic: str = "events_raw",
                 batch_size: int = 16384,
                 linger_ms: int = 5,
                 compression_type: str = "lz4",
                 acks: str = "all",
                 retries: int = 3,
                 max_in_flight_requests: int = 5,
                 buffer_memory: int = 33554432,  # 32MB
                 request_timeout_ms: int = 30000,
                 delivery_timeout_ms: int = 120000):
        
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.batch_size = batch_size
        self.linger_ms = linger_ms
        self.compression_type = compression_type
        self.acks = acks
        self.retries = retries
        self.max_in_flight_requests = max_in_flight_requests
        self.buffer_memory = buffer_memory
        self.request_timeout_ms = request_timeout_ms
        self.delivery_timeout_ms = delivery_timeout_ms

class AdvancedKafkaProducer:
    """Advanced Kafka producer with monitoring, partitioning, and error handling"""
    
    def __init__(self, config: KafkaProducerConfig):
        self.config = config
        self.producer = None
        self.running = False
        
        # Statistics
        self.stats = {
            'messages_sent': 0,
            'messages_failed': 0,
            'bytes_sent': 0,
            'batches_sent': 0,
            'avg_batch_size': 0,
            'errors': {},
            'start_time': None,
            'last_send_time': None
        }
        
        # Rate limiting
        self.rate_limiter = None
        self.partition_strategy = self._default_partition_strategy
        
        # Callbacks
        self.on_success_callback = None
        self.on_error_callback = None
        
    def set_partition_strategy(self, strategy: Callable[[Any], int]):
        """Set custom partitioning strategy"""
        self.partition_strategy = strategy
        
    def set_callbacks(self, 
                     on_success: Optional[Callable] = None, 
                     on_error: Optional[Callable] = None):
        """Set success and error callbacks"""
        self.on_success_callback = on_success
        self.on_error_callback = on_error
        
    def _default_partition_strategy(self, event: Dict[str, Any]) -> int:
        """Default partitioning strategy based on user_id"""
        user_id = event.get('user_id', 0)
        if user_id:
            return hash(str(user_id)) % 6  # Assuming 6 partitions
        return 0
        
    def _serialize_event(self, event: Any) -> bytes:
        """Serialize event to bytes"""
        if hasattr(event, '__dict__'):
            event_dict = asdict(event)
        elif isinstance(event, dict):
            event_dict = event
        else:
            event_dict = {'data': str(event)}
            
        return json.dumps(event_dict, default=str, ensure_ascii=False).encode('utf-8')
        
    async def start(self):
        """Initialize and start the Kafka producer"""
        try:
            self.producer = aiokafka.AIOKafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers,
                value_serializer=self._serialize_event,
                batch_size=self.config.batch_size,
                linger_ms=self.config.linger_ms,
                compression_type=self.config.compression_type,
                acks=self.config.acks,
                retries=self.config.retries,
                max_in_flight_requests_per_connection=self.config.max_in_flight_requests,
                buffer_memory=self.config.buffer_memory,
                request_timeout_ms=self.config.request_timeout_ms,
                delivery_timeout_ms=self.config.delivery_timeout_ms
            )
            
            await self.producer.start()
            self.running = True
            self.stats['start_time'] = time.time()
            
            logger.info(f"Kafka producer started successfully")
            logger.info(f"  Bootstrap servers: {self.config.bootstrap_servers}")
            logger.info(f"  Target topic: {self.config.topic}")
            logger.info(f"  Compression: {self.config.compression_type}")
            logger.info(f"  Batch size: {self.config.batch_size}")
            
        except Exception as e:
            logger.error(f"Failed to start Kafka producer: {e}")
            raise
            
    async def stop(self):
        """Stop the Kafka producer"""
        if self.producer and self.running:
            try:
                await self.producer.stop()
                self.running = False
                logger.info("Kafka producer stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping Kafka producer: {e}")
                
    async def send_event(self, 
                        event: Any, 
                        topic: Optional[str] = None,
                        partition: Optional[int] = None,
                        key: Optional[str] = None,
                        headers: Optional[Dict[str, bytes]] = None) -> bool:
        """Send a single event to Kafka"""
        if not self.running or not self.producer:
            logger.error("Producer is not running")
            return False
            
        target_topic = topic or self.config.topic
        
        try:
            # Determine partition if not specified
            if partition is None and isinstance(event, dict):
                partition = self.partition_strategy(event)
                
            # Create key if not provided
            if key is None and isinstance(event, dict):
                event_id = event.get('event_id')
                if event_id:
                    key = str(event_id)
            
            # Convert key to bytes if string
            if isinstance(key, str):
                key = key.encode('utf-8')
                
            # Send message
            future = await self.producer.send_and_wait(
                topic=target_topic,
                value=event,
                partition=partition,
                key=key,
                headers=headers
            )
            
            # Update statistics
            self.stats['messages_sent'] += 1
            self.stats['last_send_time'] = time.time()
            
            # Calculate message size
            serialized_size = len(self._serialize_event(event))
            self.stats['bytes_sent'] += serialized_size
            
            # Execute success callback
            if self.on_success_callback:
                try:
                    await self.on_success_callback(event, future)
                except Exception as e:
                    logger.warning(f"Success callback error: {e}")
                    
            return True
            
        except KafkaTimeoutError as e:
            logger.error(f"Kafka timeout error: {e}")
            self._update_error_stats('timeout_error')
            return False
            
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
            self._update_error_stats('kafka_error')
            return False
            
        except Exception as e:
            logger.error(f"Unexpected error sending event: {e}")
            self._update_error_stats('unexpected_error')
            
            # Execute error callback
            if self.on_error_callback:
                try:
                    await self.on_error_callback(event, e)
                except Exception as callback_error:
                    logger.warning(f"Error callback error: {callback_error}")
                    
            return False
            
    async def send_batch(self, 
                        events: List[Any],
                        topic: Optional[str] = None,
                        partition_strategy: Optional[Callable] = None) -> Dict[str, int]:
        """Send a batch of events to Kafka"""
        if not events:
            return {'sent': 0, 'failed': 0}
            
        target_topic = topic or self.config.topic
        strategy = partition_strategy or self.partition_strategy
        
        # Group events by partition for efficiency
        partitioned_events = {}
        for event in events:
            if isinstance(event, dict):
                partition = strategy(event)
                if partition not in partitioned_events:
                    partitioned_events[partition] = []
                partitioned_events[partition].append(event)
            else:
                # Default partition for non-dict events
                if 0 not in partitioned_events:
                    partitioned_events[0] = []
                partitioned_events[0].append(event)
        
        # Send events concurrently by partition
        send_tasks = []
        for partition, partition_events in partitioned_events.items():
            for event in partition_events:
                task = asyncio.create_task(
                    self.send_event(event, target_topic, partition)
                )
                send_tasks.append(task)
                
        # Wait for all sends to complete
        results = await asyncio.gather(*send_tasks, return_exceptions=True)
        
        # Count results
        sent_count = sum(1 for result in results if result is True)
        failed_count = len(results) - sent_count
        
        # Update batch statistics
        self.stats['batches_sent'] += 1
        if self.stats['batches_sent'] > 0:
            self.stats['avg_batch_size'] = (
                self.stats['messages_sent'] / self.stats['batches_sent']
            )
            
        logger.debug(f"Batch send completed: {sent_count} sent, {failed_count} failed")
        
        return {
            'sent': sent_count,
            'failed': failed_count,
            'total': len(events)
        }
        
    def _update_error_stats(self, error_type: str):
        """Update error statistics"""
        self.stats['messages_failed'] += 1
        if error_type not in self.stats['errors']:
            self.stats['errors'][error_type] = 0
        self.stats['errors'][error_type] += 1
        
    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics"""
        current_time = time.time()
        elapsed = current_time - self.stats['start_time'] if self.stats['start_time'] else 0
        
        stats = self.stats.copy()
        stats['elapsed_seconds'] = elapsed
        stats['messages_per_second'] = self.stats['messages_sent'] / elapsed if elapsed > 0 else 0
        stats['bytes_per_second'] = self.stats['bytes_sent'] / elapsed if elapsed > 0 else 0
        stats['error_rate'] = (
            self.stats['messages_failed'] / 
            max(1, self.stats['messages_sent'] + self.stats['messages_failed'])
        ) * 100
        
        return stats
        
    async def flush(self, timeout: Optional[float] = None):
        """Flush any pending messages"""
        if self.producer and self.running:
            try:
                if timeout:
                    await asyncio.wait_for(self.producer.flush(), timeout=timeout)
                else:
                    await self.producer.flush()
                logger.debug("Producer flush completed")
            except asyncio.TimeoutError:
                logger.warning("Producer flush timed out")
            except Exception as e:
                logger.error(f"Error during producer flush: {e}")
                
    async def get_metadata(self) -> Dict[str, Any]:
        """Get topic metadata"""
        if not self.producer or not self.running:
            return {}
            
        try:
            cluster = self.producer.client.cluster
            topic_metadata = cluster.topics()
            
            metadata = {
                'topics': list(topic_metadata.keys()),
                'brokers': [
                    {'id': broker.nodeId, 'host': broker.host, 'port': broker.port}
                    for broker in cluster.brokers()
                ]
            }
            
            # Get partition info for target topic
            if self.config.topic in topic_metadata:
                topic_info = topic_metadata[self.config.topic]
                metadata['target_topic'] = {
                    'name': self.config.topic,
                    'partitions': len(topic_info.partitions),
                    'partition_info': [
                        {
                            'partition': p.partition,
                            'leader': p.leader,
                            'replicas': p.replicas,
                            'isr': p.isr
                        }
                        for p in topic_info.partitions.values()
                    ]
                }
                
            return metadata
            
        except Exception as e:
            logger.error(f"Error getting metadata: {e}")
            return {}

# Convenience function for simple use cases
async def send_events_to_kafka(events: List[Any], 
                              bootstrap_servers: str = "localhost:9092",
                              topic: str = "events_raw",
                              **config_kwargs) -> Dict[str, int]:
    """
    Convenience function to send events to Kafka
    
    Args:
        events: List of events to send
        bootstrap_servers: Kafka bootstrap servers
        topic: Target topic
        **config_kwargs: Additional configuration options
        
    Returns:
        Dictionary with send results
    """
    config = KafkaProducerConfig(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        **config_kwargs
    )
    
    producer = AdvancedKafkaProducer(config)
    
    try:
        await producer.start()
        result = await producer.send_batch(events, topic)
        await producer.flush()
        return result
    finally:
        await producer.stop()

# Example usage and testing
if __name__ == "__main__":
    import asyncio
    
    async def test_producer():
        """Test the Kafka producer"""
        config = KafkaProducerConfig(
            bootstrap_servers="localhost:9092",
            topic="test_events",
            batch_size=1000,
            linger_ms=5
        )
        
        producer = AdvancedKafkaProducer(config)
        
        # Set callbacks
        async def on_success(event, metadata):
            print(f"Event sent successfully: {event.get('event_id', 'unknown')}")
            
        async def on_error(event, error):
            print(f"Event failed: {event.get('event_id', 'unknown')}, Error: {error}")
            
        producer.set_callbacks(on_success, on_error)
        
        try:
            await producer.start()
            
            # Send test events
            test_events = [
                {
                    'event_id': f'test-{i}',
                    'user_id': i % 100,
                    'event_type': 'test_event',
                    'timestamp': time.time(),
                    'data': f'Test event {i}'
                }
                for i in range(10)
            ]
            
            result = await producer.send_batch(test_events)
            print(f"Batch result: {result}")
            
            # Print statistics
            stats = producer.get_stats()
            print(f"Producer stats: {stats}")
            
        finally:
            await producer.stop()
    
    # Run test
    asyncio.run(test_producer())