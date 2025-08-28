"""
Event Generator and Kafka Producer
Generates realistic high-volume event data and sends to Kafka
"""

import asyncio
import json
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging
from dataclasses import dataclass, asdict
import os
from faker import Faker
from aiokafka import AIOKafkaProducer
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

@dataclass
class EventData:
    """Structure for generated events"""
    event_id: str
    user_id: int
    event_type: str
    timestamp: str
    session_id: str
    ip_address: str
    user_agent: str
    referrer: Optional[str]
    page_url: str
    properties: Dict[str, Any]

class EventGenerator:
    """Generates realistic event data for testing"""
    
    def __init__(self, config_path: str = "configs/event-categories.yml"):
        self.config = self._load_config(config_path)
        self.user_pool = list(range(1, 100000))  # 100k unique users
        self.session_pool = {}  # Track sessions per user
        self.event_types = self._get_all_event_types()
        self.websites = [
            "https://example-ecommerce.com",
            "https://example-blog.com", 
            "https://example-app.com",
            "https://example-saas.com"
        ]
        
    def _load_config(self, config_path: str) -> Dict:
        """Load event categories configuration"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config file {config_path} not found, using defaults")
            return self._default_config()
    
    def _default_config(self) -> Dict:
        """Default configuration if file not found"""
        return {
            'categories': {
                'engagement': {'events': ['page_view', 'button_click', 'scroll']},
                'conversion': {'events': ['signup', 'form_submit']},
                'revenue': {'events': ['purchase', 'subscription_start']},
                'ecommerce': {'events': ['cart_add', 'checkout_start']},
                'social': {'events': ['share', 'like', 'comment']},
                'authentication': {'events': ['login', 'logout']},
                'technical': {'events': ['error', 'api_call']}
            }
        }
    
    def _get_all_event_types(self) -> List[str]:
        """Extract all event types from config"""
        event_types = []
        for category in self.config.get('categories', {}).values():
            event_types.extend(category.get('events', []))
        return event_types
    
    def _get_session_id(self, user_id: int) -> str:
        """Get or create session for user"""
        if user_id not in self.session_pool:
            self.session_pool[user_id] = str(uuid.uuid4())
        
        # 10% chance to start new session
        if random.random() < 0.1:
            self.session_pool[user_id] = str(uuid.uuid4())
            
        return self.session_pool[user_id]
    
    def _generate_properties(self, event_type: str) -> Dict[str, Any]:
        """Generate event-specific properties"""
        base_properties = {
            "source": "web",
            "version": "1.0",
            "environment": "production"
        }
        
        # Event-specific properties
        if event_type == "page_view":
            base_properties.update({
                "page_title": fake.catch_phrase(),
                "page_category": random.choice(["home", "product", "blog", "about"]),
                "load_time": round(random.uniform(0.5, 3.0), 2)
            })
        elif event_type == "purchase":
            base_properties.update({
                "product_id": random.randint(1000, 9999),
                "product_name": fake.commerce.product_name(),
                "amount": round(random.uniform(10.0, 500.0), 2),
                "currency": "USD",
                "payment_method": random.choice(["credit_card", "paypal", "bank_transfer"])
            })
        elif event_type == "button_click":
            base_properties.update({
                "button_text": random.choice(["Buy Now", "Learn More", "Sign Up", "Download"]),
                "button_position": random.choice(["header", "sidebar", "footer", "content"]),
                "click_coordinates": {"x": random.randint(0, 1920), "y": random.randint(0, 1080)}
            })
        elif event_type == "search":
            base_properties.update({
                "search_query": fake.word(),
                "results_count": random.randint(0, 100),
                "search_category": random.choice(["products", "articles", "users"])
            })
        elif event_type == "error":
            base_properties.update({
                "error_type": random.choice(["404", "500", "timeout", "validation"]),
                "error_message": fake.sentence(),
                "stack_trace": fake.text(100)
            })
            
        return base_properties
    
    def generate_event(self, timestamp: Optional[datetime] = None) -> EventData:
        """Generate a single realistic event"""
        if timestamp is None:
            timestamp = datetime.utcnow()
            
        user_id = random.choice(self.user_pool)
        event_type = random.choice(self.event_types)
        
        # Weight certain events more heavily
        event_weights = {
            'page_view': 0.4,
            'button_click': 0.2,
            'scroll': 0.15,
            'search': 0.1,
            'purchase': 0.02,
            'signup': 0.03,
            'login': 0.05,
            'error': 0.05
        }
        
        if event_type in event_weights:
            if random.random() > event_weights[event_type]:
                event_type = 'page_view'  # Default fallback
        
        event = EventData(
            event_id=str(uuid.uuid4()),
            user_id=user_id,
            event_type=event_type,
            timestamp=timestamp.isoformat(),
            session_id=self._get_session_id(user_id),
            ip_address=fake.ipv4(),
            user_agent=fake.user_agent(),
            referrer=fake.url() if random.random() > 0.3 else None,
            page_url=f"{random.choice(self.websites)}/{fake.uri_path()}",
            properties=self._generate_properties(event_type)
        )
        
        return event
    
    def generate_batch(self, size: int, start_time: Optional[datetime] = None) -> List[EventData]:
        """Generate a batch of events"""
        if start_time is None:
            start_time = datetime.utcnow()
            
        events = []
        for i in range(size):
            # Spread events over time with some randomness
            event_time = start_time + timedelta(seconds=i + random.uniform(-0.5, 0.5))
            events.append(self.generate_event(event_time))
            
        return events

class KafkaEventProducer:
    """High-performance Kafka producer for events"""
    
    def __init__(self, 
                 bootstrap_servers: str = "localhost:9092",
                 topic: str = "events_raw",
                 batch_size: int = 1000,
                 linger_ms: int = 5):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.batch_size = batch_size
        self.linger_ms = linger_ms
        self.producer = None
        self.stats = {
            'total_sent': 0,
            'errors': 0,
            'start_time': None
        }
        
    async def start(self):
        """Initialize Kafka producer"""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(asdict(x), default=str).encode('utf-8'),
            compression_type='lz4',
            batch_size=self.batch_size,
            linger_ms=self.linger_ms,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=5
        )
        await self.producer.start()
        self.stats['start_time'] = time.time()
        logger.info(f"Kafka producer started, sending to topic: {self.topic}")
        
    async def stop(self):
        """Stop Kafka producer"""
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped")
            
    async def send_event(self, event: EventData) -> bool:
        """Send single event to Kafka"""
        try:
            await self.producer.send_and_wait(self.topic, event)
            self.stats['total_sent'] += 1
            return True
        except Exception as e:
            logger.error(f"Failed to send event: {e}")
            self.stats['errors'] += 1
            return False
            
    async def send_batch(self, events: List[EventData]) -> int:
        """Send batch of events to Kafka"""
        sent_count = 0
        tasks = []
        
        for event in events:
            task = asyncio.create_task(self.send_event(event))
            tasks.append(task)
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if result is True:
                sent_count += 1
                
        return sent_count
    
    def get_stats(self) -> Dict:
        """Get producer statistics"""
        elapsed = time.time() - self.stats['start_time'] if self.stats['start_time'] else 0
        rate = self.stats['total_sent'] / elapsed if elapsed > 0 else 0
        
        return {
            'total_sent': self.stats['total_sent'],
            'errors': self.stats['errors'],
            'rate_per_second': round(rate, 2),
            'elapsed_seconds': round(elapsed, 2)
        }

async def main():
    """Main function to run event generation"""
    # Configuration from environment
    KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    EVENTS_PER_SECOND = int(os.getenv('EVENTS_PER_SECOND', '1000'))
    TOTAL_EVENTS = int(os.getenv('TOTAL_EVENTS', '1000000'))
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
    
    # Initialize components
    generator = EventGenerator()
    producer = KafkaEventProducer(
        bootstrap_servers=KAFKA_SERVERS,
        batch_size=BATCH_SIZE
    )
    
    try:
        await producer.start()
        
        logger.info(f"Starting event generation:")
        logger.info(f"  Target rate: {EVENTS_PER_SECOND} events/sec")
        logger.info(f"  Total events: {TOTAL_EVENTS}")
        logger.info(f"  Batch size: {BATCH_SIZE}")
        
        events_sent = 0
        batch_interval = BATCH_SIZE / EVENTS_PER_SECOND  # Time between batches
        
        while events_sent < TOTAL_EVENTS:
            batch_start = time.time()
            
            # Generate batch
            remaining = min(BATCH_SIZE, TOTAL_EVENTS - events_sent)
            events = generator.generate_batch(remaining)
            
            # Send batch
            sent_count = await producer.send_batch(events)
            events_sent += sent_count
            
            # Rate limiting
            batch_duration = time.time() - batch_start
            if batch_duration < batch_interval:
                await asyncio.sleep(batch_interval - batch_duration)
            
            # Progress logging
            if events_sent % (EVENTS_PER_SECOND * 10) == 0:  # Every 10 seconds
                stats = producer.get_stats()
                logger.info(f"Progress: {events_sent}/{TOTAL_EVENTS} events "
                          f"({stats['rate_per_second']} events/sec)")
        
        # Final statistics
        final_stats = producer.get_stats()
        logger.info(f"Event generation completed!")
        logger.info(f"  Total sent: {final_stats['total_sent']}")
        logger.info(f"  Errors: {final_stats['errors']}")
        logger.info(f"  Average rate: {final_stats['rate_per_second']} events/sec")
        logger.info(f"  Duration: {final_stats['elapsed_seconds']} seconds")
        
    except Exception as e:
        logger.error(f"Error in event generation: {e}")
        raise
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())