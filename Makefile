.PHONY: help up down logs clean init-db start-pipeline stop-pipeline test benchmark status health

# Default target
help: ## Show this help message
	@echo "Event Processing Pipeline - Available Commands:"
	@echo ""
	@echo "MongoDB Replica Set:"
	@docker exec mongodb-1 mongosh --quiet --eval "db.runCommand('ping').ok" > /dev/null 2>&1 && echo "  ✅ MongoDB-1: Healthy" || echo "  ❌ MongoDB-1: Unhealthy"
	@docker exec mongodb-2 mongosh --quiet --eval "db.runCommand('ping').ok" > /dev/null 2>&1 && echo "  ✅ MongoDB-2: Healthy" || echo "  ❌ MongoDB-2: Unhealthy"
	@docker exec mongodb-3 mongosh --quiet --eval "db.runCommand('ping').ok" > /dev/null 2>&1 && echo "  ✅ MongoDB-3: Healthy" || echo "  ❌ MongoDB-3: Unhealthy"

# Utility Commands
clean: ## Clean up all containers, volumes, and networks
	@echo "🧹 Cleaning up..."
	@docker compose down -v --remove-orphans
	@docker system prune -f
	@echo "✅ Cleanup complete"

shell-clickhouse: ## Open ClickHouse client shell
	@docker exec -it clickhouse-s1r1 clickhouse-client

shell-mongo: ## Open MongoDB shell
	@docker exec -it mongodb-1 mongosh

shell-kafka: ## Open Kafka shell
	@docker exec -it kafka-1 bash

# Scaling
scale-consumers: ## Scale consumer instances (usage: make scale-consumers REPLICAS=3)
	@docker compose up -d --scale event-consumer=$(or $(REPLICAS),2)

# Data Operations
create-topics: ## Create additional Kafka topics
	@docker exec kafka-1 kafka-topics --create --topic events_transformed --bootstrap-server localhost:29092 --partitions 6 --replication-factor 3 --if-not-exists
	@docker exec kafka-1 kafka-topics --create --topic events_errors --bootstrap-server localhost:29092 --partitions 3 --replication-factor 3 --if-not-exists

list-topics: ## List all Kafka topics
	@docker exec kafka-1 kafka-topics --list --bootstrap-server localhost:29092

reset-offsets: ## Reset consumer group offsets (DANGEROUS - use with caution)
	@echo "⚠️  This will reset consumer offsets. Press Ctrl+C to cancel..."
	@sleep 5
	@docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:29092 --group event-processing-group --reset-offsets --to-earliest --topic events_raw --execute

# Performance Monitoring
perf-kafka: ## Show Kafka performance metrics
	@echo "📊 Kafka Topic Performance:"
	@docker exec kafka-1 kafka-topics --describe --bootstrap-server localhost:29092 --topic events_raw

perf-clickhouse: ## Show ClickHouse query performance
	@echo "📊 ClickHouse Performance:"
	@docker exec clickhouse-s1r1 clickhouse-client --query "SELECT event, query_duration_ms FROM system.query_log WHERE type = 'QueryFinish' ORDER BY event_time DESC LIMIT 10"

perf-mongo: ## Show MongoDB performance stats
	@echo "📊 MongoDB Performance:"
	@docker exec mongodb-1 mongosh --eval "db.serverStatus().opcounters"

# Development
format: ## Format Python code
	@echo "🎨 Formatting code..."
	@docker run --rm -v $(PWD):/app -w /app python:3.11-slim bash -c "pip install black isort && black src/ tests/ && isort src/ tests/"

lint: ## Lint Python code
	@echo "🔍 Linting code..."
	@docker run --rm -v $(PWD):/app -w /app python:3.11-slim bash -c "pip install flake8 pylint && flake8 src/ tests/ && pylint src/"

# Backup & Restore
backup-mongo: ## Backup MongoDB data
	@echo "💾 Backing up MongoDB..."
	@mkdir -p ./backups
	@docker exec mongodb-1 mongodump --out /tmp/backup
	@docker cp mongodb-1:/tmp/backup ./backups/mongodb-$(shell date +%Y%m%d_%H%M%S)
	@echo "✅ MongoDB backup completed"

backup-clickhouse: ## Backup ClickHouse data
	@echo "💾 Backing up ClickHouse..."
	@mkdir -p ./backups
	@docker exec clickhouse-s1r1 clickhouse-client --query "BACKUP DATABASE analytics TO Disk('backups', 'clickhouse-$(shell date +%Y%m%d_%H%M%S)')"
	@echo "✅ ClickHouse backup completed"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Quick Start:"
	@echo "  1. make up          # Start all services"
	@echo "  2. make init-db     # Initialize databases (wait 2-3 minutes after 'up')"
	@echo "  3. make start-pipeline # Start data processing"
	@echo "  4. make benchmark   # Run performance tests"

# Infrastructure Management
up: ## Start all services with docker compose
	@echo "🚀 Starting event processing pipeline..."
	@docker compose up -d
	@echo "✅ Services starting... Use 'make logs' to monitor startup"
	@echo "⏳ Wait 2-3 minutes before running 'make init-db'"

down: ## Stop all services
	@echo "🛑 Stopping all services..."
	@docker compose down
	@echo "✅ All services stopped"

restart: down up ## Restart all services

logs: ## Show logs from all services
	@docker compose logs -f

logs-service: ## Show logs for specific service (usage: make logs-service SERVICE=kafka-1)
	@docker compose logs -f $(SERVICE)

# Database Management
init-db: ## Initialize database schemas and configs
	@echo "🗄️  Initializing databases..."
	@echo "📊 Setting up ClickHouse cluster..."
	@sleep 10  # Wait for ClickHouse to be ready
	@docker exec clickhouse-s1r1 clickhouse-client --query "SELECT 'ClickHouse is ready'" || (echo "❌ ClickHouse not ready, waiting..."; sleep 30)
	@docker exec clickhouse-s1r1 clickhouse-client --multiline < configs/clickhouse/init-cluster.sql
	@echo "🍃 Initializing MongoDB replica set..."
	@sleep 5
	@docker exec mongodb-1 mongosh --eval "rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'mongodb-1:27017'}, {_id: 1, host: 'mongodb-2:27017'}, {_id: 2, host: 'mongodb-3:27017'}]})" || echo "Replica set may already be initialized"
	@sleep 10
	@docker exec mongodb-1 mongosh --eval "db.createUser({user: 'events_user', pwd: 'events_pass', roles: [{role: 'readWrite', db: 'events'}]})" events
	@echo "📝 Creating Kafka topics..."
	@docker exec kafka-1 kafka-topics --create --topic events_raw --bootstrap-server localhost:29092 --partitions 6 --replication-factor 3 --if-not-exists
	@docker exec kafka-1 kafka-topics --create --topic events_dlq --bootstrap-server localhost:29092 --partitions 3 --replication-factor 3 --if-not-exists
	@echo "✅ Database initialization complete!"

# Pipeline Management
start-pipeline: ## Start event producer and consumer
	@echo "🏃 Starting event processing pipeline..."
	@docker compose up -d event-producer event-consumer
	@echo "✅ Pipeline started. Monitor with 'make logs-pipeline'"

stop-pipeline: ## Stop event producer and consumer
	@echo "⏹️  Stopping pipeline..."
	@docker compose stop event-producer event-consumer
	@echo "✅ Pipeline stopped"

logs-pipeline: ## Show pipeline logs (producer and consumer)
	@docker compose logs -f event-producer event-consumer

# Development & Testing
test: ## Run unit tests
	@echo "🧪 Running unit tests..."
	@docker run --rm -v $(PWD):/app -w /app python:3.11-slim bash -c "pip install -r requirements.txt && python -m pytest tests/ -v"

benchmark: ## Run performance benchmarks
	@echo "📈 Running performance benchmarks..."
	@python scripts/benchmark.py

# Monitoring & Health
status: ## Show status of all services
	@echo "📊 Service Status:"
	@docker compose ps

health: ## Check health of all services
	@echo "🏥 Health Check Results:"
	@echo ""
	@echo "Kafka Cluster:"
	@docker exec kafka-1 kafka-broker-api-versions --bootstrap-server localhost:29092 > /dev/null 2>&1 && echo "  ✅ Kafka-1: Healthy" || echo "  ❌ Kafka-1: Unhealthy"
	@docker exec kafka-2 kafka-broker-api-versions --bootstrap-server localhost:29092 > /dev/null 2>&1 && echo "  ✅ Kafka-2: Healthy" || echo "  ❌ Kafka-2: Unhealthy"
	@docker exec kafka-3 kafka-broker-api-versions --bootstrap-server localhost:29092 > /dev/null 2>&1 && echo "  ✅ Kafka-3: Healthy" || echo "  ❌ Kafka-3: Unhealthy"
	@echo ""
	@echo "ClickHouse Cluster:"
	@docker exec clickhouse-s1r1 clickhouse-client --query "SELECT 1" > /dev/null 2>&1 && echo "  ✅ ClickHouse S1R1: Healthy" || echo "  ❌ ClickHouse S1R1: Unhealthy"
	@docker exec clickhouse-s1r2 clickhouse-client --query "SELECT 1" > /dev/null 2>&1 && echo "  ✅ ClickHouse S1R2: Healthy" || echo "  ❌ ClickHouse S1R2: Unhealthy"
	@docker exec clickhouse-s2r1 clickhouse-client --query "SELECT 1" > /dev/null 2>&1 && echo "  ✅ ClickHouse S2R1: Healthy" || echo "  ❌ ClickHouse S2R1: Unhealthy"
	@echo ""