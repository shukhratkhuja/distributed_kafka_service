-- ClickHouse Cluster Initialization Script
-- This script sets up the distributed analytics database

-- Create the analytics database on all nodes
CREATE DATABASE IF NOT EXISTS analytics ON CLUSTER cluster_3shards_2replicas;

-- Create the main events table on each shard with replicas
CREATE TABLE IF NOT EXISTS analytics.events_local ON CLUSTER cluster_3shards_2replicas
(
    event_id String,
    user_id UInt64,
    event_type LowCardinality(String),
    event_date Date,
    timestamp DateTime64(3),
    hour_of_day UInt8,
    day_of_week UInt8,
    is_weekend UInt8,
    event_category LowCardinality(String),
    session_id String,
    ip_address IPv4,
    user_agent String,
    referrer String,
    page_url String,
    properties String, -- JSON properties as string
    processed_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type, user_id, timestamp)
TTL event_date + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 8192;

-- Create the distributed table that spans all shards
CREATE TABLE IF NOT EXISTS analytics.events_distributed ON CLUSTER cluster_3shards_2replicas
(
    event_id String,
    user_id UInt64,
    event_type LowCardinality(String),
    event_date Date,
    timestamp DateTime64(3),
    hour_of_day UInt8,
    day_of_week UInt8,
    is_weekend UInt8,
    event_category LowCardinality(String),
    session_id String,
    ip_address IPv4,
    user_agent String,
    referrer String,
    page_url String,
    properties String,
    processed_at DateTime64(3)
) ENGINE = Distributed(cluster_3shards_2replicas, analytics, events_local, cityHash64(user_id));

-- Create materialized view for hourly event counts
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.events_hourly_mv ON CLUSTER cluster_3shards_2replicas
(
    event_date Date,
    event_hour DateTime,
    event_type LowCardinality(String),
    event_category LowCardinality(String),
    event_count UInt64,
    unique_users UInt64
) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/events_hourly', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_hour, event_type, event_category)
AS SELECT
    event_date,
    toStartOfHour(timestamp) as event_hour,
    event_type,
    event_category,
    count() as event_count,
    uniq(user_id) as unique_users
FROM analytics.events_local
GROUP BY event_date, event_hour, event_type, event_category;

-- Create distributed table for hourly aggregations
CREATE TABLE IF NOT EXISTS analytics.events_hourly_distributed ON CLUSTER cluster_3shards_2replicas
(
    event_date Date,
    event_hour DateTime,
    event_type LowCardinality(String),
    event_category LowCardinality(String),
    event_count UInt64,
    unique_users UInt64
) ENGINE = Distributed(cluster_3shards_2replicas, analytics, events_hourly_mv, cityHash64(event_type));

-- Create materialized view for daily user activity
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.daily_users_mv ON CLUSTER cluster_3shards_2replicas
(
    event_date Date,
    user_id UInt64,
    first_event_time DateTime64(3),
    last_event_time DateTime64(3),
    event_count UInt32,
    unique_event_types UInt16,
    session_count UInt16
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/daily_users', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id)
AS SELECT
    event_date,
    user_id,
    min(timestamp) as first_event_time,
    max(timestamp) as last_event_time,
    count() as event_count,
    uniq(event_type) as unique_event_types,
    uniq(session_id) as session_count
FROM analytics.events_local
GROUP BY event_date, user_id;

-- Create distributed table for daily users
CREATE TABLE IF NOT EXISTS analytics.daily_users_distributed ON CLUSTER cluster_3shards_2replicas
(
    event_date Date,
    user_id UInt64,
    first_event_time DateTime64(3),
    last_event_time DateTime64(3),
    event_count UInt32,
    unique_event_types UInt16,
    session_count UInt16
) ENGINE = Distributed(cluster_3shards_2replicas, analytics, daily_users_mv, cityHash64(user_id));

-- Create table for event categories lookup
CREATE TABLE IF NOT EXISTS analytics.event_categories ON CLUSTER cluster_3shards_2replicas
(
    event_type String,
    category String,
    description String,
    is_active UInt8 DEFAULT 1,
    created_at DateTime DEFAULT now()
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/event_categories', '{replica}')
ORDER BY event_type;

-- Insert default event categories
INSERT INTO analytics.event_categories VALUES
('page_view', 'engagement', 'User viewed a page', 1, now()),
('button_click', 'engagement', 'User clicked a button', 1, now()),
('form_submit', 'conversion', 'User submitted a form', 1, now()),
('purchase', 'revenue', 'User made a purchase', 1, now()),
('signup', 'conversion', 'User signed up', 1, now()),
('login', 'authentication', 'User logged in', 1, now()),
('logout', 'authentication', 'User logged out', 1, now()),
('search', 'engagement', 'User performed a search', 1, now()),
('video_play', 'engagement', 'User played a video', 1, now()),
('download', 'engagement', 'User downloaded a file', 1, now()),
('share', 'social', 'User shared content', 1, now()),
('like', 'social', 'User liked content', 1, now()),
('comment', 'social', 'User posted a comment', 1, now()),
('cart_add', 'ecommerce', 'User added item to cart', 1, now()),
('cart_remove', 'ecommerce', 'User removed item from cart', 1, now()),
('checkout_start', 'ecommerce', 'User started checkout', 1, now()),
('payment_failed', 'ecommerce', 'Payment failed', 1, now()),
('subscription_start', 'revenue', 'User started subscription', 1, now()),
('subscription_cancel', 'revenue', 'User cancelled subscription', 1, now()),
('error', 'technical', 'Application error occurred', 1, now());

-- Create performance monitoring views
CREATE VIEW IF NOT EXISTS analytics.performance_summary ON CLUSTER cluster_3shards_2replicas AS
SELECT
    'Events per second (last hour)' as metric,
    toString(round(count() / 3600, 2)) as value
FROM analytics.events_distributed
WHERE timestamp >= now() - INTERVAL 1 HOUR
UNION ALL
SELECT
    'Total events today' as metric,
    toString(count()) as value
FROM analytics.events_distributed
WHERE event_date = today()
UNION ALL
SELECT
    'Unique users today' as metric,
    toString(uniq(user_id)) as value
FROM analytics.events_distributed
WHERE event_date = today()
UNION ALL
SELECT
    'Average events per user today' as metric,
    toString(round(count() / uniq(user_id), 2)) as value
FROM analytics.events_distributed
WHERE event_date = today();

-- Create indexes for common query patterns
-- Note: ClickHouse automatically creates indexes based on ORDER BY

-- Grant permissions (if users exist)
-- GRANT SELECT, INSERT ON analytics.* TO events_user;

-- Show tables to verify creation
SHOW TABLES FROM analytics;