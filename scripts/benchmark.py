#!/usr/bin/env python3
"""
Performance Benchmark Script for Event Processing Pipeline
Measures throughput, latency, and resource usage across the entire system
"""

import asyncio
import time
import json
import logging
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests
import psutil
import argparse
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import clickhouse_connect
import pymongo
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from faker import Faker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
fake = Faker()

@dataclass
class BenchmarkConfig:
    """Benchmark configuration settings"""
    duration_seconds: int = 300  # 5 minutes
    events_per_second: int = 1000
    batch_size: int = 100
    num_users: int = 10000
    kafka_servers: str = "localhost:9092"
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    mongodb_url: str = "mongodb://localhost:27017"
    health_endpoint: str = "http://localhost:8080/health"

@dataclass
class BenchmarkResults:
    """Benchmark results"""
    total_events_generated: int = 0
    total_events_processed: int = 0
    events_per_second_avg: float = 0.0
    events_per_second_max: float = 0.0
    latency_avg_ms: float = 0.0
    latency_p95_ms: float = 0.0
    latency_p99_ms: float = 0.0
    error_rate: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    clickhouse_query_time_ms: float = 0.0
    mongodb_query_time_ms: float = 0.0
    kafka_lag: int = 0
    pipeline_health: str = "unknown"
    
class EventGenerator:
    """Generates realistic events for benchmarking"""
    
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.user_ids = list(range(1, config.num_users + 1))
        
    def generate_event(self) -> Dict[str, Any]:
        """Generate a single event"""
        return {
            "event_id": fake.uuid4(),
            "user_id": fake.random_element(self.user_ids),
            "event_type": fake.random_element([
                "page_view", "button_click", "purchase", "signup", 
                "login", "search", "cart_add", "video_play"
            ]),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "session_id": fake.uuid4(),
            "ip_address": fake.ipv4(),
            "user_agent": fake.user_agent(),
            "referrer": fake.url() if fake.random.random() > 0.3 else None,
            "page_url": fake.url(),
            "properties": {
                "page_title": fake.catch_phrase(),
                "category": fake.word(),
                "value": fake.random.randint(1, 1000)
            }
        }

class KafkaBenchmark:
    """Kafka-specific benchmarking"""
    
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.producer = None
        self.consumer = None
        self.sent_events = []
        self.received_events = []
        
    async def setup(self):
        """Initialize Kafka clients"""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.config.kafka_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            compression_type='lz4',
            batch_size=self.config.batch_size * 1000,
            linger_ms=5
        )
        
        self.consumer = AIOKafkaConsumer(
            'events_raw',
            bootstrap_servers=self.config.kafka_servers,
            group_id='benchmark_consumer',
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        await self.producer.start()
        await self.consumer.start()
        
    async def cleanup(self):
        """Cleanup Kafka clients"""
        if self.producer:
            await self.producer.stop()
        if self.consumer:
            await self.consumer.stop()
            
    async def produce_events(self, generator: EventGenerator, duration: int):
        """Produce events for specified duration"""
        start_time = time.time()
        events_sent = 0
        
        while time.time() - start_time < duration:
            batch_start = time.time()
            
            # Send batch
            tasks = []
            for _ in range(self.config.batch_size):
                event = generator.generate_event()
                self.sent_events.append({
                    'event_id': event['event_id'],
                    'sent_at': time.time()
                })
                task = self.producer.send('events_raw', event)
                tasks.append(task)
                
            await asyncio.gather(*tasks)
            events_sent += self.config.batch_size
            
            # Rate limiting
            batch_duration = time.time() - batch_start
            target_duration = self.config.batch_size / self.config.events_per_second
            if batch_duration < target_duration:
                await asyncio.sleep(target_duration - batch_duration)
                
        return events_sent

class DatabaseBenchmark:
    """Database-specific benchmarking"""
    
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.clickhouse_client = None
        self.mongodb_client = None
        
    async def setup(self):
        """Initialize database connections"""
        self.clickhouse_client = clickhouse_connect.get_client(
            host=self.config.clickhouse_host,
            port=self.config.clickhouse_port,
            database='analytics'
        )
        
        self.mongodb_client = pymongo.MongoClient(self.config.mongodb_url)
        
    def cleanup(self):
        """Cleanup database connections"""
        if self.clickhouse_client:
            self.clickhouse_client.close()
        if self.mongodb_client:
            self.mongodb_client.close()
            
    def benchmark_clickhouse_queries(self) -> Dict[str, float]:
        """Benchmark ClickHouse query performance"""
        queries = [
            "SELECT count() FROM events_distributed WHERE event_date = today()",
            "SELECT event_type, count() FROM events_distributed WHERE event_date >= today() - 1 GROUP BY event_type",
            "SELECT uniq(user_id) FROM events_distributed WHERE event_date = today()",
            "SELECT toHour(timestamp) as hour, count() FROM events_distributed WHERE event_date = today() GROUP BY hour ORDER BY hour"
        ]
        
        results = {}
        for i, query in enumerate(queries):
            start_time = time.time()
            try:
                result = self.clickhouse_client.query(query)
                duration = (time.time() - start_time) * 1000
                results[f'query_{i+1}_ms'] = duration
                logger.info(f"ClickHouse Query {i+1}: {duration:.2f}ms, {len(result.result_rows)} rows")
            except Exception as e:
                logger.error(f"ClickHouse Query {i+1} failed: {e}")
                results[f'query_{i+1}_ms'] = -1
                
        return results
    
    def benchmark_mongodb_queries(self) -> Dict[str, float]:
        """Benchmark MongoDB query performance"""
        db = self.mongodb_client.events
        collection = db.raw_events
        
        queries = [
            lambda: collection.count_documents({}),
            lambda: list(collection.aggregate([
                {"$group": {"_id": "$event_type", "count": {"$sum": 1}}}
            ])),
            lambda: collection.distinct("user_id"),
            lambda: list(collection.find({"event_type": "page_view"}).limit(100))
        ]
        
        results = {}
        for i, query_func in enumerate(queries):
            start_time = time.time()
            try:
                result = query_func()
                duration = (time.time() - start_time) * 1000
                results[f'query_{i+1}_ms'] = duration
                count = len(result) if isinstance(result, list) else (result if isinstance(result, int) else len(list(result)))
                logger.info(f"MongoDB Query {i+1}: {duration:.2f}ms, {count} items")
            except Exception as e:
                logger.error(f"MongoDB Query {i+1} failed: {e}")
                results[f'query_{i+1}_ms'] = -1
                
        return results

class SystemMonitor:
    """System resource monitoring"""
    
    def __init__(self):
        self.cpu_samples = []
        self.memory_samples = []
        self.monitoring = False
        
    async def start_monitoring(self, interval: float = 1.0):
        """Start system monitoring"""
        self.monitoring = True
        while self.monitoring:
            cpu_percent = psutil.cpu_percent(interval=None)
            memory_info = psutil.virtual_memory()
            
            self.cpu_samples.append(cpu_percent)
            self.memory_samples.append(memory_info.used / 1024 / 1024)  # MB
            
            await asyncio.sleep(interval)
            
    def stop_monitoring(self):
        """Stop system monitoring"""
        self.monitoring = False
        
    def get_stats(self) -> Dict[str, float]:
        """Get monitoring statistics"""
        return {
            'cpu_avg': statistics.mean(self.cpu_samples) if self.cpu_samples else 0,
            'cpu_max': max(self.cpu_samples) if self.cpu_samples else 0,
            'memory_avg_mb': statistics.mean(self.memory_samples) if self.memory_samples else 0,
            'memory_max_mb': max(self.memory_samples) if self.memory_samples else 0
        }

class HealthChecker:
    """Pipeline health monitoring"""
    
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        
    def check_health(self) -> Dict[str, Any]:
        """Check pipeline health"""
        try:
            response = requests.get(self.config.health_endpoint, timeout=5)
            if response.status_code == 200:
                return response.json()
            else:
                return {"status": "unhealthy", "code": response.status_code}
        except Exception as e:
            return {"status": "error", "error": str(e)}

class PipelineBenchmark:
    """Main benchmark orchestrator"""
    
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.generator = EventGenerator(config)
        self.kafka_benchmark = KafkaBenchmark(config)
        self.db_benchmark = DatabaseBenchmark(config)
        self.system_monitor = SystemMonitor()
        self.health_checker = HealthChecker(config)
        
    async def run_benchmark(self) -> BenchmarkResults:
        """Run complete benchmark"""
        logger.info(f"Starting benchmark - Duration: {self.config.duration_seconds}s, Rate: {self.config.events_per_second} events/s")
        
        results = BenchmarkResults()
        
        try:
            # Setup
            await self.kafka_benchmark.setup()
            await self.db_benchmark.setup()
            
            # Start system monitoring
            monitor_task = asyncio.create_task(self.system_monitor.start_monitoring())
            
            # Wait for system to stabilize
            await asyncio.sleep(5)
            
            # Check initial health
            initial_health = self.health_checker.check_health()
            logger.info(f"Initial health: {initial_health.get('status', 'unknown')}")
            
            # Generate events
            logger.info("Starting event generation...")
            start_time = time.time()
            events_sent = await self.kafka_benchmark.produce_events(
                self.generator, 
                self.config.duration_seconds
            )
            generation_time = time.time() - start_time
            
            # Wait for processing to complete
            logger.info("Waiting for processing to complete...")
            await asyncio.sleep(30)
            
            # Stop monitoring
            self.system_monitor.stop_monitoring()
            monitor_task.cancel()
            
            # Check final health
            final_health = self.health_checker.check_health()
            logger.info(f"Final health: {final_health.get('status', 'unknown')}")
            
            # Database benchmarks
            logger.info("Running database benchmarks...")
            ch_results = self.db_benchmark.benchmark_clickhouse_queries()
            mongo_results = self.db_benchmark.benchmark_mongodb_queries()
            
            # Compile results
            system_stats = self.system_monitor.get_stats()
            
            results.total_events_generated = events_sent
            results.events_per_second_avg = events_sent / generation_time
            results.events_per_second_max = self.config.events_per_second
            results.memory_usage_mb = system_stats['memory_avg_mb']
            results.cpu_usage_percent = system_stats['cpu_avg']
            results.clickhouse_query_time_ms = statistics.mean([
                v for v in ch_results.values() if v > 0
            ]) if ch_results else 0
            results.mongodb_query_time_ms = statistics.mean([
                v for v in mongo_results.values() if v > 0
            ]) if mongo_results else 0
            results.pipeline_health = final_health.get('status', 'unknown')
            
            # Get processing stats if available
            if 'stats' in final_health:
                stats = final_health['stats']
                results.total_events_processed = stats.get('events_processed', 0)
                errors = stats.get('processing_errors', 0)
                results.error_rate = errors / max(1, results.total_events_processed) * 100
                
        except Exception as e:
            logger.error(f"Benchmark failed: {e}")
            raise
        finally:
            # Cleanup
            await self.kafka_benchmark.cleanup()
            self.db_benchmark.cleanup()
            
        return results
    
    def print_results(self, results: BenchmarkResults):
        """Print benchmark results"""
        print("\n" + "="*60)
        print("BENCHMARK RESULTS")
        print("="*60)
        print(f"Events Generated:     {results.total_events_generated:,}")
        print(f"Events Processed:     {results.total_events_processed:,}")
        print(f"Generation Rate:      {results.events_per_second_avg:.1f} events/sec")
        print(f"Error Rate:           {results.error_rate:.2f}%")
        print(f"Pipeline Health:      {results.pipeline_health}")
        print("\n" + "-"*30 + " PERFORMANCE " + "-"*30)
        print(f"Avg Memory Usage:     {results.memory_usage_mb:.1f} MB")
        print(f"Avg CPU Usage:        {results.cpu_usage_percent:.1f}%")
        print(f"ClickHouse Avg Query: {results.clickhouse_query_time_ms:.1f} ms")
        print(f"MongoDB Avg Query:    {results.mongodb_query_time_ms:.1f} ms")
        print("="*60)
        
        # Determine overall grade
        grade = self._calculate_grade(results)
        print(f"OVERALL GRADE:        {grade}")
        print("="*60)
        
    def _calculate_grade(self, results: BenchmarkResults) -> str:
        """Calculate overall performance grade"""
        score = 0
        max_score = 100
        
        # Throughput (30 points)
        throughput_ratio = results.events_per_second_avg / self.config.events_per_second
        score += min(30, throughput_ratio * 30)
        
        # Error rate (25 points)
        if results.error_rate < 1:
            score += 25
        elif results.error_rate < 5:
            score += 15
        elif results.error_rate < 10:
            score += 5
            
        # Query performance (25 points)
        if results.clickhouse_query_time_ms < 100:
            score += 12.5
        elif results.clickhouse_query_time_ms < 500:
            score += 7.5
        elif results.clickhouse_query_time_ms < 1000:
            score += 2.5
            
        if results.mongodb_query_time_ms < 50:
            score += 12.5
        elif results.mongodb_query_time_ms < 200:
            score += 7.5
        elif results.mongodb_query_time_ms < 500:
            score += 2.5
            
        # Resource usage (20 points)
        if results.cpu_usage_percent < 50:
            score += 10
        elif results.cpu_usage_percent < 80:
            score += 5
            
        if results.memory_usage_mb < 1000:
            score += 10
        elif results.memory_usage_mb < 2000:
            score += 5
        
        percentage = (score / max_score) * 100
        
        if percentage >= 90:
            return "A+ (Excellent)"
        elif percentage >= 80:
            return "A (Very Good)"
        elif percentage >= 70:
            return "B (Good)"
        elif percentage >= 60:
            return "C (Fair)"
        else:
            return "F (Needs Improvement)"

async def main():
    """Main benchmark function"""
    parser = argparse.ArgumentParser(description='Event Processing Pipeline Benchmark')
    parser.add_argument('--duration', type=int, default=300, help='Benchmark duration in seconds')
    parser.add_argument('--rate', type=int, default=1000, help='Events per second')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size')
    parser.add_argument('--kafka-servers', default='localhost:9092', help='Kafka servers')
    parser.add_argument('--clickhouse-host', default='localhost', help='ClickHouse host')
    parser.add_argument('--mongodb-url', default='mongodb://localhost:27017', help='MongoDB URL')
    
    args = parser.parse_args()
    
    config = BenchmarkConfig(
        duration_seconds=args.duration,
        events_per_second=args.rate,
        batch_size=args.batch_size,
        kafka_servers=args.kafka_servers,
        clickhouse_host=args.clickhouse_host,
        mongodb_url=args.mongodb_url
    )
    
    benchmark = PipelineBenchmark(config)
    
    try:
        results = await benchmark.run_benchmark()
        benchmark.print_results(results)
        
        # Save results to file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        with open(f'benchmark_results_{timestamp}.json', 'w') as f:
            json.dump(results.__dict__, f, indent=2, default=str)
            
        logger.info(f"Results saved to benchmark_results_{timestamp}.json")
        
    except Exception as e:
        logger.error(f"Benchmark failed: {e}")
        return 1
        
    return 0

if __name__ == "__main__":
    exit(asyncio.run(main()))