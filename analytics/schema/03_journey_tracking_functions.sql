-- ClickHouse Advanced Analytics Platform - Journey Tracking Functions
-- Advanced user journey analysis, funnel tracking, and behavioral analytics

-- User Journey Analysis Function
-- Tracks complete user lifecycle with session progression and behavioral patterns
CREATE OR REPLACE FUNCTION user_journey_analysis(user_id_param String)
RETURNS TABLE (
    session_number UInt32,
    session_id String,
    session_start DateTime64(3),
    session_end DateTime64(3),
    session_duration UInt32,
    page_views UInt32,
    events_count UInt32,
    session_revenue Decimal(10,2),
    utm_source String,
    utm_campaign String,
    referrer_domain String,
    landing_page String,
    exit_page String,
    is_bounce UInt8,
    has_conversion UInt8,
    days_since_first_visit UInt32,
    lifecycle_stage String,
    session_value_score Float32
)
AS $$
WITH user_sessions AS (
    SELECT 
        session_id,
        min(event_time) as session_start,
        max(event_time) as session_end,
        toUInt32(max(event_time) - min(event_time)) as session_duration,
        countIf(event_type = 'page_view') as page_views,
        count() as events_count,
        sum(revenue) as session_revenue,
        anyIf(utm_source, utm_source != '') as utm_source,
        anyIf(utm_campaign, utm_campaign != '') as utm_campaign,
        anyIf(referrer_domain, referrer_domain != '') as referrer_domain,
        argMin(page_url, event_time) as landing_page,
        argMax(page_url, event_time) as exit_page,
        if(countIf(event_type = 'page_view') <= 1 AND session_duration < 30, 1, 0) as is_bounce,
        if(sum(revenue) > 0, 1, 0) as has_conversion
    FROM analytics.events
    WHERE user_id = user_id_param AND is_valid = 1
    GROUP BY session_id
),
user_first_visit AS (
    SELECT min(session_start) as first_visit_time
    FROM user_sessions
),
session_sequence AS (
    SELECT 
        *,
        row_number() OVER (ORDER BY session_start) as session_number,
        dateDiff('day', (SELECT first_visit_time FROM user_first_visit), toDate(session_start)) as days_since_first_visit,
        
        -- Calculate session value score based on multiple factors
        (page_views * 0.1 + 
         events_count * 0.05 + 
         session_duration * 0.001 + 
         session_revenue * 0.1 + 
         if(has_conversion = 1, 5, 0) + 
         if(is_bounce = 0, 2, 0)) as session_value_score,
         
        -- Determine lifecycle stage
        multiIf(
            session_revenue > 0, 'customer',
            session_number = 1, 'new_visitor',
            session_number <= 3, 'returning_visitor',
            session_number <= 10, 'engaged_user',
            'loyal_user'
        ) as lifecycle_stage
    FROM user_sessions
)
SELECT 
    session_number,
    session_id,
    session_start,
    session_end,
    session_duration,
    page_views,
    events_count,
    session_revenue,
    utm_source,
    utm_campaign,
    referrer_domain,
    landing_page,
    exit_page,
    is_bounce,
    has_conversion,
    days_since_first_visit,
    lifecycle_stage,
    session_value_score
FROM session_sequence
ORDER BY session_number
$$;

-- Advanced Funnel Analysis with Time Windows
-- Uses ClickHouse's windowFunnel function for sophisticated conversion tracking
CREATE OR REPLACE FUNCTION conversion_funnel_analysis(time_window_hours UInt32)
RETURNS TABLE (
    funnel_step UInt8,
    step_name String,
    users_count UInt64,
    conversion_rate Float32,
    avg_time_to_next_step Float32,
    drop_off_rate Float32
)
AS $$
WITH funnel_events AS (
    SELECT 
        user_id,
        event_time,
        multiIf(
            event_type = 'page_view', 1,
            event_type = 'add_to_cart', 2,
            event_type = 'checkout_start', 3,
            event_type = 'purchase', 4,
            0
        ) as funnel_level
    FROM analytics.events
    WHERE event_time >= now() - INTERVAL time_window_hours HOUR
      AND is_valid = 1
      AND funnel_level > 0
),
funnel_analysis AS (
    SELECT
        user_id,
        windowFunnel(time_window_hours * 3600)(event_time, funnel_level = 1, funnel_level = 2, funnel_level = 3, funnel_level = 4) as funnel_step
    FROM funnel_events
    GROUP BY user_id
),
step_counts AS (
    SELECT
        1 as step,
        'page_view' as step_name,
        countIf(funnel_step >= 1) as users_count
    FROM funnel_analysis
    
    UNION ALL
    
    SELECT
        2 as step,
        'add_to_cart' as step_name,
        countIf(funnel_step >= 2) as users_count
    FROM funnel_analysis
    
    UNION ALL
    
    SELECT
        3 as step,
        'checkout_start' as step_name,
        countIf(funnel_step >= 3) as users_count
    FROM funnel_analysis
    
    UNION ALL
    
    SELECT
        4 as step,
        'purchase' as step_name,
        countIf(funnel_step >= 4) as users_count
    FROM funnel_analysis
),
time_between_steps AS (
    SELECT
        user_id,
        argMin(event_time, funnel_level) as step1_time,
        argMinIf(event_time, funnel_level, funnel_level = 2) as step2_time,
        argMinIf(event_time, funnel_level, funnel_level = 3) as step3_time,
        argMinIf(event_time, funnel_level, funnel_level = 4) as step4_time
    FROM funnel_events
    GROUP BY user_id
)
SELECT 
    toUInt8(step) as funnel_step,
    step_name,
    users_count,
    if(step = 1, 100.0, users_count * 100.0 / lag(users_count) OVER (ORDER BY step)) as conversion_rate,
    
    -- Calculate average time to next step
    multiIf(
        step = 1, avgIf(step2_time - step1_time, step2_time > 0),
        step = 2, avgIf(step3_time - step2_time, step3_time > 0),
        step = 3, avgIf(step4_time - step3_time, step4_time > 0),
        0
    ) as avg_time_to_next_step,
    
    if(step = 1, 0.0, 100.0 - conversion_rate) as drop_off_rate
FROM step_counts
CROSS JOIN time_between_steps
GROUP BY step, step_name, users_count
ORDER BY step
$$;

-- Multi-Touch Attribution Analysis
-- Implements multiple attribution models for marketing analysis
CREATE OR REPLACE FUNCTION attribution_analysis(lookback_days UInt32)
RETURNS TABLE (
    utm_source String,
    utm_medium String,
    utm_campaign String,
    touchpoints UInt64,
    conversions UInt64,
    total_revenue Decimal(10,2),
    first_touch_revenue Decimal(10,2),
    last_touch_revenue Decimal(10,2),
    linear_attribution_revenue Decimal(10,2),
    time_decay_revenue Decimal(10,2),
    position_based_revenue Decimal(10,2)
)
AS $$
WITH user_touchpoints AS (
    SELECT 
        user_id,
        utm_source,
        utm_medium,
        utm_campaign,
        event_time,
        revenue,
        row_number() OVER (PARTITION BY user_id ORDER BY event_time) as touchpoint_order,
        count() OVER (PARTITION BY user_id) as total_touchpoints,
        
        -- Time decay weight (more recent = higher weight)
        exp(-0.1 * dateDiff('hour', event_time, now())) as time_decay_weight
    FROM analytics.events
    WHERE event_time >= now() - INTERVAL lookback_days DAY
      AND is_valid = 1
      AND utm_source != ''
      AND user_id != ''
),
user_conversions AS (
    SELECT 
        user_id,
        sum(revenue) as user_total_revenue,
        count() as conversion_events
    FROM analytics.events
    WHERE event_time >= now() - INTERVAL lookback_days DAY
      AND is_valid = 1
      AND revenue > 0
      AND user_id != ''
    GROUP BY user_id
),
attribution_data AS (
    SELECT
        tp.utm_source,
        tp.utm_medium,
        tp.utm_campaign,
        tp.user_id,
        tp.touchpoint_order,
        tp.total_touchpoints,
        tp.time_decay_weight,
        uc.user_total_revenue,
        
        -- First touch attribution
        if(tp.touchpoint_order = 1, uc.user_total_revenue, 0) as first_touch_value,
        
        -- Last touch attribution
        if(tp.touchpoint_order = tp.total_touchpoints, uc.user_total_revenue, 0) as last_touch_value,
        
        -- Linear attribution
        uc.user_total_revenue / tp.total_touchpoints as linear_value,
        
        -- Time decay attribution
        (uc.user_total_revenue * tp.time_decay_weight) / 
        sum(tp.time_decay_weight) OVER (PARTITION BY tp.user_id) as time_decay_value,
        
        -- Position-based attribution (40% first, 40% last, 20% middle)
        multiIf(
            tp.total_touchpoints = 1, uc.user_total_revenue,
            tp.touchpoint_order = 1, uc.user_total_revenue * 0.4,
            tp.touchpoint_order = tp.total_touchpoints, uc.user_total_revenue * 0.4,
            uc.user_total_revenue * 0.2 / (tp.total_touchpoints - 2)
        ) as position_based_value
    FROM user_touchpoints tp
    JOIN user_conversions uc ON tp.user_id = uc.user_id
)
SELECT
    utm_source,
    utm_medium,
    utm_campaign,
    count() as touchpoints,
    uniq(user_id) as conversions,
    sum(user_total_revenue) / uniq(user_id) as total_revenue,
    sum(first_touch_value) as first_touch_revenue,
    sum(last_touch_value) as last_touch_revenue,
    sum(linear_value) as linear_attribution_revenue,
    sum(time_decay_value) as time_decay_revenue,
    sum(position_based_value) as position_based_revenue
FROM attribution_data
GROUP BY utm_source, utm_medium, utm_campaign
ORDER BY total_revenue DESC
$$;

-- Cohort Retention Analysis
-- Advanced cohort analysis with revenue progression
CREATE OR REPLACE FUNCTION cohort_retention_analysis(analysis_days UInt32)
RETURNS TABLE (
    cohort_date Date,
    cohort_size UInt64,
    days_since_first_visit UInt32,
    active_users UInt64,
    retention_rate Float32,
    revenue_per_user Decimal(10,2),
    cumulative_revenue_per_user Decimal(10,2)
)
AS $$
WITH cohort_users AS (
    SELECT 
        user_id,
        toDate(min(event_time)) as cohort_date,
        min(event_time) as first_visit_time
    FROM analytics.events
    WHERE event_time >= now() - INTERVAL analysis_days DAY
      AND is_valid = 1
      AND user_id != ''
    GROUP BY user_id
),
cohort_activity AS (
    SELECT 
        cu.cohort_date,
        cu.user_id,
        toDate(e.event_time) as activity_date,
        dateDiff('day', cu.first_visit_time, e.event_time) as days_since_first_visit,
        sum(e.revenue) as user_revenue
    FROM cohort_users cu
    JOIN analytics.events e ON cu.user_id = e.user_id
    WHERE e.is_valid = 1
      AND e.event_time >= cu.first_visit_time
      AND e.event_time <= now()
    GROUP BY cu.cohort_date, cu.user_id, activity_date, days_since_first_visit
),
cohort_sizes AS (
    SELECT 
        cohort_date,
        count() as cohort_size
    FROM cohort_users
    GROUP BY cohort_date
),
retention_data AS (
    SELECT
        ca.cohort_date,
        ca.days_since_first_visit,
        uniq(ca.user_id) as active_users,
        sum(ca.user_revenue) as period_revenue,
        
        -- Calculate cumulative revenue per user
        sum(ca.user_revenue) / uniq(ca.user_id) as revenue_per_user
    FROM cohort_activity ca
    GROUP BY ca.cohort_date, ca.days_since_first_visit
),
cumulative_revenue AS (
    SELECT
        cohort_date,
        days_since_first_visit,
        active_users,
        revenue_per_user,
        sum(revenue_per_user) OVER (
            PARTITION BY cohort_date 
            ORDER BY days_since_first_visit 
            ROWS UNBOUNDED PRECEDING
        ) as cumulative_revenue_per_user
    FROM retention_data
)
SELECT
    cr.cohort_date,
    cs.cohort_size,
    cr.days_since_first_visit,
    cr.active_users,
    cr.active_users * 100.0 / cs.cohort_size as retention_rate,
    cr.revenue_per_user,
    cr.cumulative_revenue_per_user
FROM cumulative_revenue cr
JOIN cohort_sizes cs ON cr.cohort_date = cs.cohort_date
WHERE cr.days_since_first_visit <= analysis_days
ORDER BY cr.cohort_date, cr.days_since_first_visit
$$;

-- User Segmentation Analysis
-- Advanced behavioral segmentation with RFM analysis
CREATE OR REPLACE FUNCTION user_segmentation_analysis()
RETURNS TABLE (
    user_id String,
    segment String,
    recency_score UInt8,
    frequency_score UInt8,
    monetary_score UInt8,
    rfm_score String,
    total_revenue Decimal(10,2),
    total_sessions UInt32,
    avg_session_value Decimal(10,2),
    days_since_last_visit UInt32,
    predicted_ltv Decimal(10,2)
)
AS $$
WITH user_metrics AS (
    SELECT
        user_id,
        max(event_time) as last_visit,
        min(event_time) as first_visit,
        dateDiff('day', max(event_time), now()) as days_since_last_visit,
        dateDiff('day', min(event_time), max(event_time)) as customer_lifespan,
        uniq(session_id) as total_sessions,
        sum(revenue) as total_revenue,
        count() as total_events,
        sum(revenue) / uniq(session_id) as avg_session_value
    FROM analytics.events
    WHERE is_valid = 1 AND user_id != ''
    GROUP BY user_id
),
rfm_scores AS (
    SELECT
        *,
        -- Recency score (1-5, 5 = most recent)
        multiIf(
            days_since_last_visit <= 7, 5,
            days_since_last_visit <= 30, 4,
            days_since_last_visit <= 90, 3,
            days_since_last_visit <= 180, 2,
            1
        ) as recency_score,
        
        -- Frequency score (1-5, 5 = most frequent)
        multiIf(
            total_sessions >= 20, 5,
            total_sessions >= 10, 4,
            total_sessions >= 5, 3,
            total_sessions >= 2, 2,
            1
        ) as frequency_score,
        
        -- Monetary score (1-5, 5 = highest value)
        multiIf(
            total_revenue >= 1000, 5,
            total_revenue >= 500, 4,
            total_revenue >= 100, 3,
            total_revenue >= 10, 2,
            1
        ) as monetary_score
    FROM user_metrics
),
segmented_users AS (
    SELECT
        *,
        concat(toString(recency_score), toString(frequency_score), toString(monetary_score)) as rfm_score,
        
        -- Predict lifetime value based on current behavior
        total_revenue + (avg_session_value * frequency_score * 2) as predicted_ltv,
        
        -- Segment users based on RFM scores
        multiIf(
            recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4, 'champions',
            recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3, 'loyal_customers',
            recency_score >= 4 AND frequency_score <= 2, 'new_customers',
            recency_score >= 3 AND monetary_score >= 3, 'potential_loyalists',
            frequency_score >= 3 AND monetary_score >= 3, 'at_risk',
            recency_score <= 2 AND frequency_score >= 3, 'cannot_lose_them',
            recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 3, 'hibernating',
            'others'
        ) as segment
    FROM rfm_scores
)
SELECT
    user_id,
    segment,
    recency_score,
    frequency_score,
    monetary_score,
    rfm_score,
    total_revenue,
    total_sessions,
    avg_session_value,
    days_since_last_visit,
    predicted_ltv
FROM segmented_users
ORDER BY predicted_ltv DESC
$$;

-- Campaign ROI Analysis with Attribution
-- Combines Google Ads data with website analytics for comprehensive ROI analysis
CREATE OR REPLACE FUNCTION campaign_roi_analysis(analysis_days UInt32)
RETURNS TABLE (
    campaign_name String,
    utm_source String,
    utm_medium String,
    ad_spend Decimal(10,2),
    impressions UInt64,
    clicks UInt64,
    website_sessions UInt64,
    conversions UInt64,
    revenue Decimal(10,2),
    roas Decimal(10,2),
    roi_percent Decimal(10,2),
    cost_per_acquisition Decimal(10,2),
    customer_lifetime_value Decimal(10,2)
)
AS $$
WITH campaign_costs AS (
    SELECT 
        campaign_name,
        'google' as utm_source,
        'cpc' as utm_medium,
        sum(cost) as ad_spend,
        sum(impressions) as impressions,
        sum(clicks) as clicks
    FROM analytics.google_ads_performance
    WHERE date >= today() - analysis_days
    GROUP BY campaign_name
),
campaign_performance AS (
    SELECT 
        utm_campaign as campaign_name,
        utm_source,
        utm_medium,
        uniq(session_id) as website_sessions,
        countIf(revenue > 0) as conversions,
        sum(revenue) as revenue,
        
        -- Calculate customer lifetime value for acquired users
        avg(sumIf(revenue, user_id != '') / uniqIf(user_id, user_id != '')) as customer_lifetime_value
    FROM analytics.events
    WHERE event_date >= today() - analysis_days
      AND is_valid = 1
      AND utm_campaign != ''
    GROUP BY utm_campaign, utm_source, utm_medium
)
SELECT 
    coalesce(cc.campaign_name, cp.campaign_name) as campaign_name,
    coalesce(cc.utm_source, cp.utm_source) as utm_source,
    coalesce(cc.utm_medium, cp.utm_medium) as utm_medium,
    coalesce(cc.ad_spend, 0) as ad_spend,
    coalesce(cc.impressions, 0) as impressions,
    coalesce(cc.clicks, 0) as clicks,
    coalesce(cp.website_sessions, 0) as website_sessions,
    coalesce(cp.conversions, 0) as conversions,
    coalesce(cp.revenue, 0) as revenue,
    
    -- Return on Ad Spend
    if(cc.ad_spend > 0, cp.revenue / cc.ad_spend, 0) as roas,
    
    -- ROI Percentage
    if(cc.ad_spend > 0, (cp.revenue - cc.ad_spend) / cc.ad_spend * 100, 0) as roi_percent,
    
    -- Cost per Acquisition
    if(cp.conversions > 0, cc.ad_spend / cp.conversions, 0) as cost_per_acquisition,
    
    coalesce(cp.customer_lifetime_value, 0) as customer_lifetime_value
FROM campaign_costs cc
FULL OUTER JOIN campaign_performance cp 
    ON cc.campaign_name = cp.campaign_name 
    AND cc.utm_source = cp.utm_source 
    AND cc.utm_medium = cp.utm_medium
WHERE cc.ad_spend > 0 OR cp.revenue > 0
ORDER BY roi_percent DESC
$$;