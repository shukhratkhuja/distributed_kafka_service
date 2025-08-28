"""
Configuration Module - Centralized configuration management
"""

import os
import yaml
import json
from typing import Dict, Any, Optional, Union
from dataclasses import dataclass, field
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

@dataclass
class KafkaConfig:
    """Kafka configuration settings"""
    bootstrap_servers: str = "localhost:9092"
    topic: str = "events_raw"
    consumer_group: str = "event-processing-group"
    batch_size: int = 1000
    max_poll_records: int = 5000
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 1000
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 3000
    max_poll_interval_ms: int = 300000
    compression_type: str = "lz4"
    acks: str = "all"
    retries: int = 3
    linger_ms: int = 5
    buffer_memory: int = 33554432  # 32MB

@dataclass
class ClickHouseConfig:
    """ClickHouse configuration settings"""
    host: str = "localhost"
    port: int = 8123
    database: str = "analytics"
    username: str = "default"
    password: str = ""
    table: str = "events_distributed"
    connect_timeout: int = 30
    send_receive_timeout: int = 300
    compress: bool = True
    max_retries: int = 3
    retry_delay: float = 1.0
    
    # Performance settings
    async_insert: bool = True
    wait_for_async_insert: bool = False
    async_insert_max_data_size: int = 10485760  # 10MB
    async_insert_busy_timeout_ms: int = 200

@dataclass
class MongoDBConfig:
    """MongoDB configuration settings"""
    connection_string: str = "mongodb://localhost:27017"
    database: str = "events"
    collection: str = "raw_events"
    max_pool_size: int = 50
    min_pool_size: int = 5
    max_idle_time_ms: int = 30000
    wait_queue_timeout_ms: int = 5000
    server_selection_timeout_ms: int = 30000
    connect_timeout_ms: int = 20000
    socket_timeout_ms: int = 20000
    retry_writes: bool = True
    retry_reads: bool = True
    max_retries: int = 3
    retry_delay: float = 1.0

@dataclass
class ProcessingConfig:
    """Event processing configuration"""
    batch_size: int = 1000
    max_workers: int = 4
    max_concurrent_batches: int = 10
    health_check_port: int = 8080
    stats_interval: int = 10  # seconds
    deduplication_cache_size: int = 100000
    enable_enrichment: bool = True
    enable_validation: bool = True
    
    # Retry settings
    max_processing_retries: int = 3
    processing_retry_delay: float = 1.0
    
    # Performance tuning
    consumer_timeout_ms: int = 1000
    processing_timeout: float = 30.0

@dataclass
class MonitoringConfig:
    """Monitoring and metrics configuration"""
    enable_prometheus: bool = True
    prometheus_port: int = 9090
    enable_health_checks: bool = True
    health_check_interval: int = 30
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Alerting thresholds
    error_rate_threshold: float = 0.05  # 5%
    latency_threshold_ms: float = 1000.0
    memory_threshold_mb: int = 1024

@dataclass
class ProducerConfig:
    """Event producer configuration"""
    events_per_second: int = 1000
    total_events: int = 1000000
    batch_size: int = 100
    num_users: int = 100000
    session_duration_minutes: int = 30
    event_distribution: Dict[str, float] = field(default_factory=lambda: {
        'page_view': 0.4,
        'button_click': 0.2,
        'scroll': 0.15,
        'search': 0.1,
        'purchase': 0.02,
        'signup': 0.03,
        'login': 0.05,
        'error': 0.05
    })

@dataclass
class AppConfig:
    """Main application configuration"""
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    clickhouse: ClickHouseConfig = field(default_factory=ClickHouseConfig)
    mongodb: MongoDBConfig = field(default_factory=MongoDBConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    producer: ProducerConfig = field(default_factory=ProducerConfig)
    
    # Environment settings
    environment: str = "development"
    debug: bool = False
    testing: bool = False

class ConfigManager:
    """Configuration manager for loading and validating settings"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or self._find_config_file()
        self._config = None
        self._env_overrides = {}
        
    def _find_config_file(self) -> Optional[str]:
        """Find configuration file in common locations"""
        possible_paths = [
            "config.yml",
            "config.yaml", 
            "configs/config.yml",
            "configs/config.yaml",
            os.path.expanduser("~/.event-processor/config.yml"),
            "/etc/event-processor/config.yml"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                return path
                
        return None
    
    def _load_from_file(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if not self.config_path or not os.path.exists(self.config_path):
            logger.warning(f"Config file not found: {self.config_path}")
            return {}
            
        try:
            with open(self.config_path, 'r') as f:
                config_data = yaml.safe_load(f) or {}
                logger.info(f"Loaded configuration from: {self.config_path}")
                return config_data
        except Exception as e:
            logger.error(f"Error loading config file {self.config_path}: {e}")
            return {}
    
    def _load_from_env(self) -> Dict[str, Any]:
        """Load configuration from environment variables"""
        env_config = {}
        
        # Kafka configuration
        if 'KAFKA_BOOTSTRAP_SERVERS' in os.environ:
            env_config.setdefault('kafka', {})['bootstrap_servers'] = os.environ['KAFKA_BOOTSTRAP_SERVERS']
        if 'KAFKA_TOPIC' in os.environ:
            env_config.setdefault('kafka', {})['topic'] = os.environ['KAFKA_TOPIC']
        if 'KAFKA_GROUP' in os.environ:
            env_config.setdefault('kafka', {})['consumer_group'] = os.environ['KAFKA_GROUP']
        
        # ClickHouse configuration
        if 'CLICKHOUSE_HOST' in os.environ:
            env_config.setdefault('clickhouse', {})['host'] = os.environ['CLICKHOUSE_HOST']
        if 'CLICKHOUSE_PORT' in os.environ:
            env_config.setdefault('clickhouse', {})['port'] = int(os.environ['CLICKHOUSE_PORT'])
        if 'CLICKHOUSE_DB' in os.environ:
            env_config.setdefault('clickhouse', {})['database'] = os.environ['CLICKHOUSE_DB']
        if 'CLICKHOUSE_USER' in os.environ:
            env_config.setdefault('clickhouse', {})['username'] = os.environ['CLICKHOUSE_USER']
        if 'CLICKHOUSE_PASSWORD' in os.environ:
            env_config.setdefault('clickhouse', {})['password'] = os.environ['CLICKHOUSE_PASSWORD']
        
        # MongoDB configuration
        if 'MONGODB_URL' in os.environ:
            env_config.setdefault('mongodb', {})['connection_string'] = os.environ['MONGODB_URL']
        if 'MONGODB_DB' in os.environ:
            env_config.setdefault('mongodb', {})['database'] = os.environ['MONGODB_DB']
        if 'MONGODB_COLLECTION' in os.environ:
            env_config.setdefault('mongodb', {})['collection'] = os.environ['MONGODB_COLLECTION']
        
        # Processing configuration
        if 'BATCH_SIZE' in os.environ:
            batch_size = int(os.environ['BATCH_SIZE'])
            env_config.setdefault('processing', {})['batch_size'] = batch_size
            env_config.setdefault('kafka', {})['batch_size'] = batch_size
        if 'MAX_WORKERS' in os.environ:
            env_config.setdefault('processing', {})['max_workers'] = int(os.environ['MAX_WORKERS'])
        if 'HEALTH_PORT' in os.environ:
            env_config.setdefault('processing', {})['health_check_port'] = int(os.environ['HEALTH_PORT'])
        
        # Producer configuration
        if 'EVENTS_PER_SECOND' in os.environ:
            env_config.setdefault('producer', {})['events_per_second'] = int(os.environ['EVENTS_PER_SECOND'])
        if 'TOTAL_EVENTS' in os.environ:
            env_config.setdefault('producer', {})['total_events'] = int(os.environ['TOTAL_EVENTS'])
        
        # Environment settings
        if 'ENVIRONMENT' in os.environ:
            env_config['environment'] = os.environ['ENVIRONMENT']
        if 'DEBUG' in os.environ:
            env_config['debug'] = os.environ['DEBUG'].lower() in ('true', '1', 'yes')
        if 'TESTING' in os.environ:
            env_config['testing'] = os.environ['TESTING'].lower() in ('true', '1', 'yes')
            
        return env_config
    
    def _merge_configs(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge configuration dictionaries"""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
                
        return result
    
    def load_config(self) -> AppConfig:
        """Load and return application configuration"""
        # Load from file
        file_config = self._load_from_file()
        
        # Load from environment
        env_config = self._load_from_env()
        
        # Merge configurations (env overrides file)
        merged_config = self._merge_configs(file_config, env_config)
        
        # Create configuration objects
        try:
            kafka_config = KafkaConfig(**merged_config.get('kafka', {}))
            clickhouse_config = ClickHouseConfig(**merged_config.get('clickhouse', {}))
            mongodb_config = MongoDBConfig(**merged_config.get('mongodb', {}))
            processing_config = ProcessingConfig(**merged_config.get('processing', {}))
            monitoring_config = MonitoringConfig(**merged_config.get('monitoring', {}))
            producer_config = ProducerConfig(**merged_config.get('producer', {}))
            
            app_config = AppConfig(
                kafka=kafka_config,
                clickhouse=clickhouse_config,
                mongodb=mongodb_config,
                processing=processing_config,
                monitoring=monitoring_config,
                producer=producer_config,
                environment=merged_config.get('environment', 'development'),
                debug=merged_config.get('debug', False),
                testing=merged_config.get('testing', False)
            )
            
            self._config = app_config
            logger.info(f"Configuration loaded successfully (environment: {app_config.environment})")
            return app_config
            
        except TypeError as e:
            logger.error(f"Configuration validation error: {e}")
            raise ValueError(f"Invalid configuration: {e}")
    
    def get_config(self) -> AppConfig:
        """Get cached configuration or load if not cached"""
        if self._config is None:
            self._config = self.load_config()
        return self._config
    
    def reload_config(self) -> AppConfig:
        """Force reload configuration"""
        self._config = None
        return self.load_config()
    
    def save_config(self, config: AppConfig, output_path: str):
        """Save configuration to YAML file"""
        config_dict = {
            'kafka': config.kafka.__dict__,
            'clickhouse': config.clickhouse.__dict__,
            'mongodb': config.mongodb.__dict__,
            'processing': config.processing.__dict__,
            'monitoring': config.monitoring.__dict__,
            'producer': config.producer.__dict__,
            'environment': config.environment,
            'debug': config.debug,
            'testing': config.testing
        }
        
        with open(output_path, 'w') as f:
            yaml.dump(config_dict, f, default_flow_style=False, indent=2)
            
        logger.info(f"Configuration saved to: {output_path}")

class ConfigValidator:
    """Validates configuration settings"""
    
    @staticmethod
    def validate_config(config: AppConfig) -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []
        
        # Validate Kafka settings
        if not config.kafka.bootstrap_servers:
            errors.append("Kafka bootstrap_servers is required")
        if not config.kafka.topic:
            errors.append("Kafka topic is required")
        if config.kafka.batch_size <= 0:
            errors.append("Kafka batch_size must be positive")
        
        # Validate ClickHouse settings
        if not config.clickhouse.host:
            errors.append("ClickHouse host is required")
        if config.clickhouse.port <= 0 or config.clickhouse.port > 65535:
            errors.append("ClickHouse port must be between 1 and 65535")
        if not config.clickhouse.database:
            errors.append("ClickHouse database is required")
        
        # Validate MongoDB settings
        if not config.mongodb.connection_string:
            errors.append("MongoDB connection_string is required")
        if not config.mongodb.database:
            errors.append("MongoDB database is required")
        if not config.mongodb.collection:
            errors.append("MongoDB collection is required")
        
        # Validate processing settings
        if config.processing.batch_size <= 0:
            errors.append("Processing batch_size must be positive")
        if config.processing.max_workers <= 0:
            errors.append("Processing max_workers must be positive")
        
        return errors

# Global configuration instance
_config_manager = None

def get_config() -> AppConfig:
    """Get global configuration instance"""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager()
    return _config_manager.get_config()

def setup_logging(config: MonitoringConfig):
    """Setup logging based on configuration"""
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper()),
        format=config.log_format
    )
    
    # Set specific logger levels
    logging.getLogger('kafka').setLevel(logging.WARNING)
    logging.getLogger('clickhouse_connect').setLevel(logging.WARNING)
    logging.getLogger('motor').setLevel(logging.WARNING)

def create_sample_config(output_path: str = "config.yml"):
    """Create a sample configuration file"""
    config = AppConfig()
    manager = ConfigManager()
    manager.save_config(config, output_path)
    print(f"Sample configuration created at: {output_path}")

if __name__ == "__main__":
    # CLI for configuration management
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "create-sample":
        output_path = sys.argv[2] if len(sys.argv) > 2 else "config.yml"
        create_sample_config(output_path)
    else:
        # Load and validate configuration
        try:
            config = get_config()
            errors = ConfigValidator.validate_config(config)
            
            if errors:
                print("Configuration errors:")
                for error in errors:
                    print(f"  - {error}")
                sys.exit(1)
            else:
                print("Configuration is valid!")
                print(f"Environment: {config.environment}")
                print(f"Kafka: {config.kafka.bootstrap_servers}")
                print(f"ClickHouse: {config.clickhouse.host}:{config.clickhouse.port}")
                print(f"MongoDB: {config.mongodb.database}")
                
        except Exception as e:
            print(f"Configuration error: {e}")
            sys.exit(1)