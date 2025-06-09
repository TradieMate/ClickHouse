-- ClickHouse Advanced Analytics Platform - Core Tables
-- Data cleaning, validation, and core event storage

-- Create analytics database
CREATE DATABASE IF NOT EXISTS analytics;

-- Raw events table with comprehensive data cleaning and validation
CREATE TABLE IF NOT EXISTS analytics.events (
    -- Event identification
    event_id String,
    event_time DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(event_time),
    event_type LowCardinality(String),
    
    -- User identification with cross-session tracking
    user_id String DEFAULT '',
    anonymous_id String,
    session_id String,
    visit_id UInt32 DEFAULT 1,
    
    -- Device and browser fingerprinting for cross-device tracking
    device_fingerprint String DEFAULT '',
    user_agent String DEFAULT '',
    ip_address IPv4 DEFAULT toIPv4('0.0.0.0'),
    ip_anonymized IPv4 MATERIALIZED toIPv4(bitAnd(toUInt32(ip_address), 0xFFFFFF00)), -- Anonymize last octet
    
    -- Page and referrer information
    page_url String DEFAULT '',
    page_title String DEFAULT '',
    page_path String MATERIALIZED if(page_url = '', '', extractURLPath(page_url)),
    page_domain String MATERIALIZED if(page_url = '', '', domain(page_url)),
    referrer_url String DEFAULT '',
    referrer_domain String MATERIALIZED if(referrer_url = '', '', domain(referrer_url)),
    
    -- UTM and campaign tracking
    utm_source LowCardinality(String) DEFAULT '',
    utm_medium LowCardinality(String) DEFAULT '',
    utm_campaign String DEFAULT '',
    utm_content String DEFAULT '',
    utm_term String DEFAULT '',
    
    -- Google Ads specific tracking
    gclid String DEFAULT '',
    gbraid String DEFAULT '',
    wbraid String DEFAULT '',
    
    -- E-commerce and conversion data
    revenue Decimal(10,2) DEFAULT 0,
    currency LowCardinality(String) DEFAULT 'USD',
    order_id String DEFAULT '',
    product_id String DEFAULT '',
    product_category String DEFAULT '',
    quantity UInt32 DEFAULT 0,
    
    -- Custom event properties (JSON for flexibility)
    custom_properties String DEFAULT '{}',
    
    -- Session context
    session_start_time DateTime64(3, 'UTC') DEFAULT event_time,
    session_duration UInt32 DEFAULT 0,
    page_views_in_session UInt32 DEFAULT 1,
    is_bounce UInt8 DEFAULT 0,
    
    -- Data quality and validation flags
    is_valid UInt8 DEFAULT 1,
    validation_errors String DEFAULT '',
    data_source LowCardinality(String) DEFAULT 'web',
    
    -- Technical metadata
    created_at DateTime64(3, 'UTC') DEFAULT now64(),
    updated_at DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id, session_id, event_time)
SAMPLE BY sipHash64(user_id)
TTL event_date + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- Data cleaning and validation triggers
CREATE TABLE IF NOT EXISTS analytics.events_buffer AS analytics.events
ENGINE = Buffer(analytics, events, 16, 10, 100, 10000, 1000000, 10000000, 100000000);

-- User profiles table for cross-session tracking
CREATE TABLE IF NOT EXISTS analytics.user_profiles (
    user_id String,
    anonymous_ids Array(String),
    device_fingerprints Array(String),
    first_seen DateTime64(3, 'UTC'),
    last_seen DateTime64(3, 'UTC'),
    total_sessions UInt32,
    total_events UInt32,
    total_revenue Decimal(10,2),
    lifetime_value Decimal(10,2),
    
    -- User classification
    user_segment LowCardinality(String) DEFAULT 'unknown',
    is_customer UInt8 DEFAULT 0,
    customer_tier LowCardinality(String) DEFAULT 'none',
    
    -- Acquisition data
    first_utm_source LowCardinality(String),
    first_utm_medium LowCardinality(String),
    first_utm_campaign String,
    first_referrer_domain String,
    
    -- Behavioral data
    avg_session_duration Float32,
    avg_pages_per_session Float32,
    bounce_rate Float32,
    conversion_rate Float32,
    
    created_at DateTime64(3, 'UTC') DEFAULT now64(),
    updated_at DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id
SETTINGS index_granularity = 8192;

-- Session tracking table
CREATE TABLE IF NOT EXISTS analytics.sessions (
    session_id String,
    user_id String,
    anonymous_id String,
    visit_id UInt32,
    
    -- Session timing
    session_start DateTime64(3, 'UTC'),
    session_end DateTime64(3, 'UTC'),
    session_duration UInt32,
    session_date Date MATERIALIZED toDate(session_start),
    
    -- Session metrics
    page_views UInt32,
    events_count UInt32,
    is_bounce UInt8,
    has_conversion UInt8,
    session_revenue Decimal(10,2),
    
    -- Attribution data
    utm_source LowCardinality(String),
    utm_medium LowCardinality(String),
    utm_campaign String,
    referrer_domain String,
    
    -- Device and location
    device_fingerprint String,
    user_agent String,
    ip_address IPv4,
    
    -- Entry and exit pages
    landing_page String,
    exit_page String,
    
    created_at DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(session_date)
ORDER BY (session_date, user_id, session_start)
SETTINGS index_granularity = 8192;

-- Google Ads performance data
CREATE TABLE IF NOT EXISTS analytics.google_ads_performance (
    date Date,
    account_id String,
    campaign_id String,
    campaign_name String,
    ad_group_id String,
    ad_group_name String,
    keyword_id String,
    keyword_text String,
    
    -- Performance metrics
    impressions UInt64,
    clicks UInt64,
    cost Decimal(10,2),
    conversions UInt32,
    conversion_value Decimal(10,2),
    
    -- Quality metrics
    quality_score Float32,
    ctr Float32,
    avg_cpc Decimal(10,2),
    avg_position Float32,
    
    -- Sync metadata
    sync_timestamp DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(sync_timestamp)
PARTITION BY toYYYYMM(date)
ORDER BY (date, campaign_id, ad_group_id, keyword_id)
SETTINGS index_granularity = 8192;

-- Data quality monitoring table
CREATE TABLE IF NOT EXISTS analytics.data_quality_log (
    check_timestamp DateTime64(3, 'UTC'),
    check_type LowCardinality(String),
    table_name LowCardinality(String),
    issue_type LowCardinality(String),
    issue_count UInt64,
    issue_details String,
    severity LowCardinality(String) -- 'low', 'medium', 'high', 'critical'
)
ENGINE = MergeTree()
ORDER BY (check_timestamp, table_name, check_type)
TTL check_timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;