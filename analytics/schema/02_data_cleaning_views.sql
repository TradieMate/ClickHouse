-- ClickHouse Advanced Analytics Platform - Data Cleaning & Aggregation Views
-- Real-time data cleaning, validation, and aggregation materialized views

-- Real-time session aggregation with data cleaning
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.user_sessions_mv
TO analytics.sessions
AS SELECT
    session_id,
    any(user_id) as user_id,
    any(anonymous_id) as anonymous_id,
    any(visit_id) as visit_id,
    
    -- Session timing with validation
    min(event_time) as session_start,
    max(event_time) as session_end,
    if(max(event_time) > min(event_time), 
       toUInt32(max(event_time) - min(event_time)), 
       0) as session_duration,
    
    -- Session metrics
    countIf(event_type = 'page_view') as page_views,
    count() as events_count,
    if(countIf(event_type = 'page_view') <= 1 AND session_duration < 30, 1, 0) as is_bounce,
    if(sum(revenue) > 0, 1, 0) as has_conversion,
    sum(revenue) as session_revenue,
    
    -- Attribution (first non-empty values)
    anyIf(utm_source, utm_source != '') as utm_source,
    anyIf(utm_medium, utm_medium != '') as utm_medium,
    anyIf(utm_campaign, utm_campaign != '') as utm_campaign,
    anyIf(referrer_domain, referrer_domain != '') as referrer_domain,
    
    -- Device info
    any(device_fingerprint) as device_fingerprint,
    any(user_agent) as user_agent,
    any(ip_address) as ip_address,
    
    -- Entry and exit pages
    argMin(page_url, event_time) as landing_page,
    argMax(page_url, event_time) as exit_page,
    
    now64() as created_at
FROM analytics.events
WHERE is_valid = 1  -- Only process valid events
GROUP BY session_id;

-- Real-time user profile aggregation with cross-session tracking
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.user_profiles_mv
TO analytics.user_profiles
AS SELECT
    user_id,
    groupUniqArray(anonymous_id) as anonymous_ids,
    groupUniqArray(device_fingerprint) as device_fingerprints,
    min(event_time) as first_seen,
    max(event_time) as last_seen,
    uniq(session_id) as total_sessions,
    count() as total_events,
    sum(revenue) as total_revenue,
    
    -- Calculate lifetime value with predictive component
    sum(revenue) + (sum(revenue) / uniq(session_id) * 0.3) as lifetime_value,
    
    -- User segmentation based on behavior
    multiIf(
        sum(revenue) > 1000, 'high_value',
        sum(revenue) > 100, 'medium_value',
        sum(revenue) > 0, 'low_value',
        uniq(session_id) > 5, 'engaged',
        'new'
    ) as user_segment,
    
    if(sum(revenue) > 0, 1, 0) as is_customer,
    multiIf(
        sum(revenue) > 1000, 'premium',
        sum(revenue) > 500, 'gold',
        sum(revenue) > 100, 'silver',
        sum(revenue) > 0, 'bronze',
        'none'
    ) as customer_tier,
    
    -- First touch attribution
    argMin(utm_source, event_time) as first_utm_source,
    argMin(utm_medium, event_time) as first_utm_medium,
    argMin(utm_campaign, event_time) as first_utm_campaign,
    argMin(referrer_domain, event_time) as first_referrer_domain,
    
    -- Behavioral metrics
    avg(session_duration) as avg_session_duration,
    avg(page_views_in_session) as avg_pages_per_session,
    avgIf(is_bounce, is_bounce = 1) as bounce_rate,
    countIf(revenue > 0) / uniq(session_id) as conversion_rate,
    
    min(event_time) as created_at,
    max(event_time) as updated_at
FROM analytics.events
WHERE is_valid = 1 AND user_id != ''
GROUP BY user_id;

-- Attribution touchpoints for multi-touch attribution
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.attribution_touchpoints_mv
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id, touchpoint_time)
AS SELECT
    user_id,
    session_id,
    event_date,
    event_time as touchpoint_time,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term,
    referrer_domain,
    page_url as touchpoint_url,
    
    -- Touchpoint sequencing
    row_number() OVER (PARTITION BY user_id ORDER BY event_time) as touchpoint_sequence,
    
    -- Time to conversion (if any)
    minIf(event_time, revenue > 0) OVER (PARTITION BY user_id) as conversion_time,
    if(conversion_time > 0, conversion_time - event_time, 0) as time_to_conversion,
    
    -- Revenue attribution (will be calculated by attribution functions)
    sumIf(revenue, revenue > 0) OVER (PARTITION BY user_id) as total_user_revenue,
    
    created_at
FROM analytics.events
WHERE is_valid = 1 
  AND user_id != ''
  AND (utm_source != '' OR referrer_domain != '');

-- Daily cohorts for retention analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.daily_cohorts_mv
ENGINE = MergeTree()
PARTITION BY toYYYYMM(cohort_date)
ORDER BY (cohort_date, user_id)
AS SELECT
    user_id,
    toDate(min(event_time)) as cohort_date,
    min(event_time) as first_event_time,
    argMin(utm_source, event_time) as acquisition_source,
    argMin(utm_medium, event_time) as acquisition_medium,
    argMin(utm_campaign, event_time) as acquisition_campaign,
    sumIf(revenue, toDate(event_time) = toDate(min(event_time))) as day_0_revenue,
    now64() as created_at
FROM analytics.events
WHERE is_valid = 1 AND user_id != ''
GROUP BY user_id;

-- Real-time metrics aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.real_time_metrics_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(metric_hour)
ORDER BY (metric_hour, metric_type)
AS SELECT
    toStartOfHour(event_time) as metric_hour,
    'hourly' as metric_type,
    count() as events,
    uniq(user_id) as unique_users,
    uniq(session_id) as sessions,
    sum(revenue) as revenue,
    countIf(event_type = 'purchase') as conversions,
    countIf(event_type = 'page_view') as pageviews,
    countIf(is_bounce = 1) as bounces,
    avg(session_duration) as avg_session_duration
FROM analytics.events
WHERE is_valid = 1
GROUP BY metric_hour;

-- Data quality monitoring view
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.data_quality_mv
TO analytics.data_quality_log
AS SELECT
    now64() as check_timestamp,
    'real_time' as check_type,
    'events' as table_name,
    multiIf(
        event_id = '', 'missing_event_id',
        anonymous_id = '', 'missing_anonymous_id',
        session_id = '', 'missing_session_id',
        event_time > now64() + INTERVAL 1 HOUR, 'future_event',
        event_time < now64() - INTERVAL 7 DAY, 'old_event',
        page_url != '' AND NOT match(page_url, '^https?://'), 'invalid_url',
        'valid'
    ) as issue_type,
    1 as issue_count,
    concat('Event ID: ', event_id, ', Time: ', toString(event_time)) as issue_details,
    multiIf(
        issue_type IN ('missing_event_id', 'missing_anonymous_id'), 'critical',
        issue_type IN ('future_event', 'invalid_url'), 'high',
        issue_type = 'old_event', 'medium',
        'low'
    ) as severity
FROM analytics.events
WHERE issue_type != 'valid';

-- Page performance aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.page_performance_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, page_path)
AS SELECT
    event_date,
    page_path,
    page_domain,
    count() as page_views,
    uniq(user_id) as unique_users,
    uniq(session_id) as unique_sessions,
    sum(revenue) as page_revenue,
    countIf(event_type = 'purchase') as conversions,
    avg(session_duration) as avg_time_on_page,
    countIf(is_bounce = 1) / count() as bounce_rate
FROM analytics.events
WHERE is_valid = 1 AND event_type = 'page_view' AND page_path != ''
GROUP BY event_date, page_path, page_domain;

-- Campaign performance aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.campaign_performance_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, utm_source, utm_medium, utm_campaign)
AS SELECT
    event_date,
    utm_source,
    utm_medium,
    utm_campaign,
    count() as events,
    uniq(user_id) as unique_users,
    uniq(session_id) as sessions,
    sum(revenue) as revenue,
    countIf(event_type = 'purchase') as conversions,
    countIf(is_bounce = 1) / uniq(session_id) as bounce_rate,
    avg(session_duration) as avg_session_duration
FROM analytics.events
WHERE is_valid = 1 AND utm_source != ''
GROUP BY event_date, utm_source, utm_medium, utm_campaign;