# High-Volume Event Data Processing & Analytics Pipeline

## Architecture Overview

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   Load Generator│───▶│  Kafka KRaft │───▶│  Python Consumer│
│   (Producer)    │    │  (3 Nodes)   │    │  (Transformer)  │
└─────────────────┘    └──────────────┘    └─────────────────┘
                                                     │
                                          ┌──────────┴──────────┐
                                          ▼                     ▼
                                   ┌─────────────┐    ┌──────────────┐
                                   │  MongoDB    │    │  ClickHouse  │
                                   │ (3 Replica) │    │ (3 Keepers,  │
                                   │             │    │  3 Shards,   │
                                   │             │    │  2 Replicas) │
                                   └─────────────┘    └──────────────┘
                                                             │
                                                             ▼
                                                   ┌──────────────┐
                                                   │ Materialized │
                                                   │    Views     │
                                                   └──────────────┘
```

## Project Structure

```
event-processing-pipeline/
├── 📄 docker-compose.yml                    
├── 📄 Dockerfile.consumer                    
├── 📄 Dockerfile.producer                   
├── 📄 Makefile                              
├── 📄 README.md                             
├── 📄 requirements.txt                      
│
├── 📁 configs/
│   ├── 📄 event-categories.yml              
│   ├── 📁 clickhouse/
│   │   ├── 📄 config.xml                    
│   │   ├── 📄 init-db.sql                   
│   │   ├── 📄 keeper-1.xml                  
│   │   ├── 📄 keeper-2.xml                  
│   │   ├── 📄 keeper-3.xml                  
│   │   ├── 📄 s1r1-config.xml               
│   │   ├── 📄 s1r1-users.xml                
│   │   ├── 📄 s1r2-config.xml               
│   │   ├── 📄 s1r2-users.xml                
│   │   ├── 📄 s2r1-config.xml               
│   │   ├── 📄 s2r1-users.xml                
│   │   ├── 📄 s2r2-config.xml               
│   │   ├── 📄 s2r2-users.xml                
│   │   ├── 📄 s3r1-config.xml               
│   │   ├── 📄 s3r1-users.xml                
│   │   ├── 📄 s3r2-config.xml               
│   │   └── 📄 s3r2-users.xml                
│   ├── 📁 kafka/
│   │   └── 📄 kafka.properties              
│   ├── 📁 prometheus/                       
│   │   └── 📄 prometheus.yml                
│   ├── 📁 grafana/                          
│   │   ├── 📁 dashboards/                   
│   │   └── 📁 datasources/                  
│   │       └── 📄 prometheus.yml            
│   └── 📁 mongodb/                          
│       └── 📄 init-replica.js               
│
├── 📁 src/
│   ├── 📄 __init__.py                       
│   ├── 📁 producer/
│   │   ├── 📄 __init__.py                   
│   │   ├── 📄 event_generator.py            
│   │   └── 📄 kafka_producer.py             
│   ├── 📁 consumer/
│   │   ├── 📄 __init__.py                   
│   │   ├── 📄 kafka_consumer.py             
│   │   ├── 📄 transformer.py                
│   │   └── 📄 database_writer.py            
│   └── 📁 utils/
│       ├── 📄 __init__.py                   
│       ├── 📄 config.py                     
│       └── 📄 monitoring.py                 
│
├── 📁 tests/
│   ├── 📄 __init__.py                       
│   ├── 📄 conftest.py                       
│   └── 📄 test_transformer.py               
│
├── 📁 scripts/
│   ├── 📄 benchmark.py                      
│   └── 📄 setup.sh                          
│
├── 📁 logs/                                 
├── 📁 backups/                              
└── 📁 data/                                 
    ├── 📁 clickhouse/                       
    ├── 📁 mongodb/                          
    └── 📁 kafka/                            
```

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.9+
- At least 8GB RAM for local testing

### Running the Pipeline

1. **Clone and setup:**
```bash
git clone <repository-url>
cd event-processing-pipeline
```

2. **Start all services:**
```bash
make up
```

3. **Initialize databases:**
```bash
make init-db
```

4. **Start data generation:**
```bash
make start-pipeline
```

5. **Run benchmarks:**
```bash
make benchmark
```

### Makefile Commands

- `make up` - Start all services
- `make down` - Stop all services
- `make logs` - View logs
- `make init-db` - Initialize database schemas
- `make start-pipeline` - Start producer and consumer
- `make test` - Run unit tests
- `make benchmark` - Run performance tests
- `make clean` - Clean up volumes and containers

## Performance Targets

- **Ingestion Rate:** 10k events/sec
- **Query Performance:** <300ms for 100M+ rows
- **Daily Volume:** 100M+ events
- **Availability:** 99.9% uptime

## Database Access

### ClickHouse
- URL: `http://localhost:8123`
- User: `default`
- Database: `analytics`

### MongoDB
- URL: `mongodb://localhost:27017`
- Database: `events`
- Collection: `raw_events`

### Kafka
- Bootstrap Servers: `localhost:9092,localhost:9093,localhost:9094`
- Topic: `events_raw`

## Example Queries

### ClickHouse Analytics

```sql
-- Event counts per type per hour (last 24h)
SELECT 
    event_type,
    toHour(timestamp) as hour,
    count() as event_count
FROM events_distributed 
WHERE event_date >= today() - 1
GROUP BY event_type, hour
ORDER BY event_count DESC;

-- Unique active users per day
SELECT 
    event_date,
    uniqExact(user_id) as unique_users
FROM events_distributed 
WHERE event_date >= today() - 7
GROUP BY event_date
ORDER BY event_date;

-- Top N event types (last 24h)
SELECT 
    event_type,
    count() as total_events
FROM events_distributed 
WHERE timestamp >= now() - INTERVAL 24 HOUR
GROUP BY event_type
ORDER BY total_events DESC
LIMIT 10;
```

### MongoDB Raw Data Queries

```javascript
// Find events by user
db.raw_events.find({user_id: 12345}).sort({timestamp: -1}).limit(100);

// Events by type in time range
db.raw_events.find({
  event_type: "purchase",
  timestamp: {$gte: new Date("2024-01-01"), $lt: new Date("2024-01-02")}
});
```

## Monitoring & Metrics

The pipeline includes built-in monitoring for:
- Kafka consumer lag
- Database write performance  
- Error rates and failed events
- Memory and CPU usage

Access metrics at: `http://localhost:8080/metrics`

## Architecture Decisions

### Data Flow
1. **Event Generator** produces realistic JSON events to Kafka
2. **Kafka Consumer** processes events in batches with transformation
3. **Dual Writes** to MongoDB (raw) and ClickHouse (analytics)
4. **Materialized Views** in ClickHouse for real-time aggregations

### Performance Optimizations
- **Batch Processing:** 1000-10000 events per batch to ClickHouse
- **Connection Pooling:** Reuse database connections
- **Async Processing:** Non-blocking I/O with asyncio
- **Partitioning:** ClickHouse partitioned by month, MongoDB sharded by user_id

### Fault Tolerance
- **Kafka Replication:** 3 brokers with replication factor 2
- **Database Replication:** MongoDB replica set, ClickHouse replicas
- **Dead Letter Queue:** Failed events sent to separate topic
- **Health Checks:** All services have health endpoints

## Benchmarking Results

Expected performance on modern hardware:
- **Ingestion:** 15k+ events/sec
- **ClickHouse Queries:** 50-200ms for 100M rows
- **MongoDB Queries:** 10-50ms for indexed lookups
- **End-to-end Latency:** <500ms from producer to storage