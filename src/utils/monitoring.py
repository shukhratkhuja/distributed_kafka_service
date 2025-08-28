"""
Monitoring and Metrics Module - Prometheus metrics, health checks, and performance monitoring
"""

import time
import asyncio
import logging
import psutil
import json
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from contextlib import asynccontextmanager
import threading
from collections import defaultdict, deque

from prometheus_client import Counter, Histogram, Gauge, Info, start_http_server
from aiohttp import web
import structlog

logger = logging.getLogger(__name__)

# Prometheus Metrics
EVENTS_PROCESSED_TOTAL = Counter(
    'events_processed_total',
    'Total number of events processed',
    ['status', 'event_type', 'source']
)

EVENTS_PROCESSING_DURATION = Histogram(
    'events_processing_duration_seconds',
    'Time spent processing events',
    ['operation', 'database'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

DATABASE_OPERATIONS_TOTAL = Counter(
    'database_operations_total',
    'Total database operations',
    ['database', 'operation', 'status']
)

KAFKA_MESSAGES_CONSUMED = Counter(
    'kafka_messages_consumed_total',
    'Total Kafka messages consumed',
    ['topic', 'partition']
)

KAFKA_CONSUMER_LAG = Gauge(
    'kafka_consumer_lag',
    'Kafka consumer lag',
    ['topic', 'partition', 'consumer_group']
)

SYSTEM_MEMORY_USAGE = Gauge(
    'system_memory_usage_bytes',
    'System memory usage in bytes'
)

SYSTEM_CPU_USAGE = Gauge(
    'system_cpu_usage_percent',
    'System CPU usage percentage'
)

APPLICATION_INFO = Info(
    'application_info',
    'Application information'
)

ACTIVE_CONNECTIONS = Gauge(
    'active_database_connections',
    'Number of active database connections',
    ['database']
)

ERROR_RATE = Gauge(
    'error_rate_percent',
    'Error rate percentage',
    ['component']
)

@dataclass
class HealthStatus:
    """Health status information"""
    healthy: bool
    status: str
    details: Dict[str, Any]
    timestamp: float
    uptime: float

@dataclass
class ComponentHealth:
    """Health status of individual components"""
    kafka_consumer: bool = False
    clickhouse: bool = False
    mongodb: bool = False
    processor: bool = False
    memory_ok: bool = True
    cpu_ok: bool = True
    last_check: float = 0.0

class MetricsCollector:
    """Collects and manages application metrics"""
    
    def __init__(self, app_info: Dict[str, str]):
        self.app_info = app_info
        self.start_time = time.time()
        self.stats_history = defaultdict(lambda: deque(maxlen=100))
        self.error_counts = defaultdict(int)
        self.total_counts = defaultdict(int)
        
        # Set application info
        APPLICATION_INFO.info(app_info)
        
        # Start system metrics collection
        self._system_metrics_task = None
        
    async def start_system_metrics_collection(self, interval: int = 10):
        """Start collecting system metrics periodically"""
        self._system_metrics_task = asyncio.create_task(
            self._collect_system_metrics_loop(interval)
        )
        
    async def stop_system_metrics_collection(self):
        """Stop collecting system metrics"""
        if self._system_metrics_task:
            self._system_metrics_task.cancel()
            try:
                await self._system_metrics_task
            except asyncio.CancelledError:
                pass
                
    async def _collect_system_metrics_loop(self, interval: int):
        """Continuously collect system metrics"""
        while True:
            try:
                await self._collect_system_metrics()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error collecting system metrics: {e}")
                await asyncio.sleep(interval)
                
    async def _collect_system_metrics(self):
        """Collect system resource metrics"""
        try:
            # Memory usage
            memory = psutil.virtual_memory()
            SYSTEM_MEMORY_USAGE.set(memory.used)
            
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=None)
            SYSTEM_CPU_USAGE.set(cpu_percent)
            
            # Store in history
            self.stats_history['memory_usage'].append(memory.used)
            self.stats_history['cpu_usage'].append(cpu_percent)
            
        except Exception as e:
            logger.error(f"Failed to collect system metrics: {e}")
    
    def record_event_processed(self, event_type: str, status: str, source: str = "kafka"):
        """Record an event processing metric"""
        EVENTS_PROCESSED_TOTAL.labels(
            status=status, 
            event_type=event_type, 
            source=source
        ).inc()
        
        # Update error rates
        component = f"{source}_processor"
        self.total_counts[component] += 1
        if status == "error":
            self.error_counts[component] += 1
            
        self._update_error_rate(component)
    
    def record_processing_duration(self, operation: str, database: str, duration: float):
        """Record processing duration"""
        EVENTS_PROCESSING_DURATION.labels(
            operation=operation,
            database=database
        ).observe(duration)
    
    def record_database_operation(self, database: str, operation: str, status: str):
        """Record database operation"""
        DATABASE_OPERATIONS_TOTAL.labels(
            database=database,
            operation=operation,
            status=status
        ).inc()
        
        # Update error rates
        component = f"{database}_writer"
        self.total_counts[component] += 1
        if status == "error":
            self.error_counts[component] += 1
            
        self._update_error_rate(component)
    
    def record_kafka_message_consumed(self, topic: str, partition: int):
        """Record Kafka message consumption"""
        KAFKA_MESSAGES_CONSUMED.labels(
            topic=topic,
            partition=str(partition)
        ).inc()
    
    def update_kafka_consumer_lag(self, topic: str, partition: int, 
                                consumer_group: str, lag: int):
        """Update Kafka consumer lag"""
        KAFKA_CONSUMER_LAG.labels(
            topic=topic,
            partition=str(partition),
            consumer_group=consumer_group
        ).set(lag)
    
    def update_active_connections(self, database: str, count: int):
        """Update active database connections"""
        ACTIVE_CONNECTIONS.labels(database=database).set(count)
    
    def _update_error_rate(self, component: str):
        """Update error rate for component"""
        total = self.total_counts[component]
        errors = self.error_counts[component]
        error_rate = (errors / total * 100) if total > 0 else 0
        ERROR_RATE.labels(component=component).set(error_rate)
    
    def get_uptime(self) -> float:
        """Get application uptime in seconds"""
        return time.time() - self.start_time
    
    def get_stats_summary(self) -> Dict[str, Any]:
        """Get comprehensive stats summary"""
        uptime = self.get_uptime()
        
        # Get latest system metrics
        memory_usage = list(self.stats_history['memory_usage'])[-1] if self.stats_history['memory_usage'] else 0
        cpu_usage = list(self.stats_history['cpu_usage'])[-1] if self.stats_history['cpu_usage'] else 0
        
        return {
            'uptime_seconds': uptime,
            'system': {
                'memory_usage_bytes': memory_usage,
                'cpu_usage_percent': cpu_usage
            },
            'error_rates': {
                component: (self.error_counts[component] / self.total_counts[component] * 100)
                if self.total_counts[component] > 0 else 0
                for component in self.total_counts.keys()
            },
            'total_operations': dict(self.total_counts),
            'total_errors': dict(self.error_counts)
        }

class HealthChecker:
    """Comprehensive health checking for all components"""
    
    def __init__(self, processor=None, clickhouse_writer=None, mongodb_writer=None):
        self.processor = processor
        self.clickhouse_writer = clickhouse_writer
        self.mongodb_writer = mongodb_writer
        self.start_time = time.time()
        self.component_health = ComponentHealth()
        
        # Health check thresholds
        self.memory_threshold_mb = 1024  # 1GB
        self.cpu_threshold_percent = 80.0
        self.error_rate_threshold = 0.05  # 5%
        
    async def check_all_components(self) -> HealthStatus:
        """Check health of all components"""
        checks = [
            self._check_kafka_consumer(),
            self._check_clickhouse(),
            self._check_mongodb(),
            self._check_processor(),
            self._check_system_resources()
        ]
        
        results = await asyncio.gather(*checks, return_exceptions=True)
        
        # Aggregate results
        all_healthy = True
        details = {}
        
        component_names = ['kafka_consumer', 'clickhouse', 'mongodb', 'processor', 'system']
        for i, (name, result) in enumerate(zip(component_names, results)):
            if isinstance(result, Exception):
                all_healthy = False
                details[name] = {'healthy': False, 'error': str(result)}
            else:
                details[name] = result
                if not result.get('healthy', False):
                    all_healthy = False
        
        status = "healthy" if all_healthy else "degraded"
        if not any(details.get(name, {}).get('healthy', False) for name in component_names):
            status = "unhealthy"
        
        self.component_health.last_check = time.time()
        
        return HealthStatus(
            healthy=all_healthy,
            status=status,
            details=details,
            timestamp=time.time(),
            uptime=time.time() - self.start_time
        )
    
    async def _check_kafka_consumer(self) -> Dict[str, Any]:
        """Check Kafka consumer health"""
        try:
            if not self.processor or not hasattr(self.processor, 'consumer'):
                return {'healthy': False, 'reason': 'processor_not_available'}
            
            consumer = self.processor.consumer
            if not consumer:
                return {'healthy': False, 'reason': 'consumer_not_initialized'}
            
            # Check if consumer is running and receiving messages
            last_processed = getattr(self.processor, 'last_message_time', 0)
            time_since_last = time.time() - last_processed
            
            if time_since_last > 300:  # 5 minutes
                return {
                    'healthy': False,
                    'reason': 'no_messages_received',
                    'last_message_seconds_ago': time_since_last
                }
            
            self.component_health.kafka_consumer = True
            return {
                'healthy': True,
                'last_message_seconds_ago': time_since_last,
                'topics': getattr(consumer, 'subscription', set())
            }
            
        except Exception as e:
            self.component_health.kafka_consumer = False
            return {'healthy': False, 'error': str(e)}
    
    async def _check_clickhouse(self) -> Dict[str, Any]:
        """Check ClickHouse health"""
        try:
            if not self.clickhouse_writer:
                return {'healthy': False, 'reason': 'writer_not_available'}
            
            healthy = await self.clickhouse_writer.health_check()
            self.component_health.clickhouse = healthy
            
            if healthy:
                stats = self.clickhouse_writer.get_stats()
                return {
                    'healthy': True,
                    'stats': stats
                }
            else:
                return {'healthy': False, 'reason': 'health_check_failed'}
                
        except Exception as e:
            self.component_health.clickhouse = False
            return {'healthy': False, 'error': str(e)}
    
    async def _check_mongodb(self) -> Dict[str, Any]:
        """Check MongoDB health"""
        try:
            if not self.mongodb_writer:
                return {'healthy': False, 'reason': 'writer_not_available'}
            
            healthy = await self.mongodb_writer.health_check()
            self.component_health.mongodb = healthy
            
            if healthy:
                stats = self.mongodb_writer.get_stats()
                return {
                    'healthy': True,
                    'stats': stats
                }
            else:
                return {'healthy': False, 'reason': 'health_check_failed'}
                
        except Exception as e:
            self.component_health.mongodb = False
            return {'healthy': False, 'error': str(e)}
    
    async def _check_processor(self) -> Dict[str, Any]:
        """Check processor health"""
        try:
            if not self.processor:
                return {'healthy': False, 'reason': 'processor_not_available'}
            
            # Check if processor is running
            running = getattr(self.processor, 'running', False)
            if not running:
                return {'healthy': False, 'reason': 'processor_not_running'}
            
            # Check processing stats
            stats = getattr(self.processor, 'stats', {})
            errors = stats.get('processing_errors', 0)
            processed = stats.get('events_processed', 0)
            
            error_rate = (errors / processed) if processed > 0 else 0
            if error_rate > self.error_rate_threshold:
                return {
                    'healthy': False,
                    'reason': 'high_error_rate',
                    'error_rate': error_rate,
                    'threshold': self.error_rate_threshold
                }
            
            self.component_health.processor = True
            return {
                'healthy': True,
                'stats': stats,
                'error_rate': error_rate
            }
            
        except Exception as e:
            self.component_health.processor = False
            return {'healthy': False, 'error': str(e)}
    
    async def _check_system_resources(self) -> Dict[str, Any]:
        """Check system resource health"""
        try:
            # Check memory usage
            memory = psutil.virtual_memory()
            memory_mb = memory.used / (1024 * 1024)
            memory_ok = memory_mb < self.memory_threshold_mb
            
            # Check CPU usage
            cpu_percent = psutil.cpu_percent(interval=0.1)
            cpu_ok = cpu_percent < self.cpu_threshold_percent
            
            self.component_health.memory_ok = memory_ok
            self.component_health.cpu_ok = cpu_ok
            
            healthy = memory_ok and cpu_ok
            
            return {
                'healthy': healthy,
                'memory': {
                    'usage_mb': memory_mb,
                    'threshold_mb': self.memory_threshold_mb,
                    'ok': memory_ok
                },
                'cpu': {
                    'usage_percent': cpu_percent,
                    'threshold_percent': self.cpu_threshold_percent,
                    'ok': cpu_ok
                }
            }
            
        except Exception as e:
            self.component_health.memory_ok = False
            self.component_health.cpu_ok = False
            return {'healthy': False, 'error': str(e)}

class MonitoringService:
    """Main monitoring service that coordinates metrics and health checks"""
    
    def __init__(self, app_info: Dict[str, str], prometheus_port: int = 8000):
        self.app_info = app_info
        self.prometheus_port = prometheus_port
        self.metrics_collector = MetricsCollector(app_info)
        self.health_checker = None
        self.prometheus_server = None
        
    async def start(self, health_check_port: int = 8080):
        """Start monitoring services"""
        # Start Prometheus metrics server
        self.prometheus_server = start_http_server(self.prometheus_port)
        logger.info(f"Prometheus metrics server started on port {self.prometheus_port}")
        
        # Start system metrics collection
        await self.metrics_collector.start_system_metrics_collection()
        
        # Start health check server
        await self.start_health_server(health_check_port)
        
        logger.info("Monitoring service started successfully")
    
    async def stop(self):
        """Stop monitoring services"""
        await self.metrics_collector.stop_system_metrics_collection()
        logger.info("Monitoring service stopped")
    
    def set_components(self, processor=None, clickhouse_writer=None, mongodb_writer=None):
        """Set components for health checking"""
        self.health_checker = HealthChecker(processor, clickhouse_writer, mongodb_writer)
    
    async def start_health_server(self, port: int):
        """Start HTTP health check server"""
        app = web.Application()
        app.router.add_get('/health', self._health_endpoint)
        app.router.add_get('/health/detailed', self._detailed_health_endpoint)
        app.router.add_get('/metrics/summary', self._metrics_summary_endpoint)
        app.router.add_get('/status', self._status_endpoint)
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        
        logger.info(f"Health check server started on port {port}")
    
    async def _health_endpoint(self, request):
        """Basic health check endpoint"""
        try:
            if not self.health_checker:
                return web.json_response({'status': 'unknown', 'reason': 'health_checker_not_initialized'}, status=503)
            
            health = await self.health_checker.check_all_components()
            
            status_code = 200 if health.healthy else 503
            return web.json_response({
                'status': health.status,
                'healthy': health.healthy,
                'timestamp': health.timestamp,
                'uptime': health.uptime
            }, status=status_code)
            
        except Exception as e:
            return web.json_response({
                'status': 'error',
                'error': str(e)
            }, status=500)
    
    async def _detailed_health_endpoint(self, request):
        """Detailed health check endpoint"""
        try:
            if not self.health_checker:
                return web.json_response({'error': 'health_checker_not_initialized'}, status=503)
            
            health = await self.health_checker.check_all_components()
            
            response_data = {
                'status': health.status,
                'healthy': health.healthy,
                'timestamp': health.timestamp,
                'uptime': health.uptime,
                'components': health.details
            }
            
            status_code = 200 if health.healthy else 503
            return web.json_response(response_data, status=status_code)
            
        except Exception as e:
            return web.json_response({
                'status': 'error',
                'error': str(e)
            }, status=500)
    
    async def _metrics_summary_endpoint(self, request):
        """Metrics summary endpoint"""
        try:
            summary = self.metrics_collector.get_stats_summary()
            return web.json_response(summary)
            
        except Exception as e:
            return web.json_response({
                'error': str(e)
            }, status=500)
    
    async def _status_endpoint(self, request):
        """Combined status endpoint"""
        try:
            # Get health status
            health_data = {'status': 'unknown'}
            if self.health_checker:
                health = await self.health_checker.check_all_components()
                health_data = {
                    'status': health.status,
                    'healthy': health.healthy,
                    'uptime': health.uptime
                }
            
            # Get metrics summary
            metrics_summary = self.metrics_collector.get_stats_summary()
            
            return web.json_response({
                'application': self.app_info,
                'health': health_data,
                'metrics': metrics_summary,
                'timestamp': time.time()
            })
            
        except Exception as e:
            return web.json_response({
                'error': str(e)
            }, status=500)
    
    # Convenience methods for recording metrics
    def record_event_processed(self, event_type: str, status: str, source: str = "kafka"):
        """Record event processing metric"""
        self.metrics_collector.record_event_processed(event_type, status, source)
    
    def record_processing_duration(self, operation: str, database: str, duration: float):
        """Record processing duration metric"""
        self.metrics_collector.record_processing_duration(operation, database, duration)
    
    def record_database_operation(self, database: str, operation: str, status: str):
        """Record database operation metric"""
        self.metrics_collector.record_database_operation(database, operation, status)

@asynccontextmanager
async def monitoring_context(app_info: Dict[str, str], 
                           prometheus_port: int = 8000,
                           health_port: int = 8080):
    """Context manager for monitoring service"""
    monitoring = MonitoringService(app_info, prometheus_port)
    
    try:
        await monitoring.start(health_port)
        yield monitoring
    finally:
        await monitoring.stop()

# Performance monitoring decorators
def monitor_duration(operation: str, database: str = ""):
    """Decorator to monitor function execution duration"""
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                EVENTS_PROCESSING_DURATION.labels(
                    operation=operation,
                    database=database
                ).observe(duration)
                return result
            except Exception as e:
                duration = time.time() - start_time
                EVENTS_PROCESSING_DURATION.labels(
                    operation=f"{operation}_error",
                    database=database
                ).observe(duration)
                raise
        
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                EVENTS_PROCESSING_DURATION.labels(
                    operation=operation,
                    database=database
                ).observe(duration)
                return result
            except Exception as e:
                duration = time.time() - start_time
                EVENTS_PROCESSING_DURATION.labels(
                    operation=f"{operation}_error",
                    database=database
                ).observe(duration)
                raise
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator