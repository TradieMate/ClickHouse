# 🚀 Advanced Analytics Platform - Complete Setup Guide

This comprehensive guide will walk you through setting up the advanced analytics platform, configuring integrations, verifying data pipelines, and running test queries.

## 📋 Table of Contents

1. [Prerequisites](#prerequisites)
2. [Initial Setup](#initial-setup)
3. [Google Ads Integration Setup](#google-ads-integration-setup)
4. [Website Tracking Integration](#website-tracking-integration)
5. [Data Pipeline Verification](#data-pipeline-verification)
6. [Test Queries](#test-queries)
7. [Troubleshooting](#troubleshooting)
8. [Production Deployment](#production-deployment)

## 🔧 Prerequisites

### System Requirements
- **Docker** 20.10+ and **Docker Compose** 2.0+
- **Minimum 4GB RAM** (8GB+ recommended for production)
- **10GB free disk space** (for ClickHouse data)
- **Linux/macOS/Windows** with WSL2

### Required Accounts & Credentials
- **Google Ads Account** with API access
- **Google Cloud Project** for API credentials
- **Domain/Website** for tracking implementation

### Verify Prerequisites
```bash
# Check Docker installation
docker --version
docker-compose --version

# Check available resources
docker system df
free -h  # Linux/macOS
```

## 🚀 Initial Setup

### 1. Clone and Setup Repository
```bash
# Clone the repository
git clone https://github.com/TradieMate/ClickHouse.git
cd ClickHouse

# Switch to the analytics branch
git checkout feature/advanced-analytics-platform

# Make setup script executable
chmod +x setup.sh
```

### 2. Environment Configuration
```bash
# Copy environment template
cp .env.example .env

# Edit configuration (see detailed configuration below)
nano .env
```

### 3. Basic Environment Configuration
```bash
# .env file configuration
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_DATABASE=analytics
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_secure_password_here

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=4
JWT_SECRET=your_super_secret_jwt_key_here

# Leave Google Ads empty for now - we'll configure this next
GOOGLE_ADS_DEVELOPER_TOKEN=
GOOGLE_ADS_CLIENT_ID=
GOOGLE_ADS_CLIENT_SECRET=
GOOGLE_ADS_REFRESH_TOKEN=
GOOGLE_ADS_CUSTOMER_IDS=
```

### 4. Initial Platform Deployment
```bash
# Run the setup script
./setup.sh

# This will:
# - Start ClickHouse and API services
# - Initialize database schema
# - Create materialized views
# - Set up advanced analytics functions
```

### 5. Verify Basic Setup
```bash
# Check service status
docker-compose ps

# Test ClickHouse connection
curl http://localhost:8123/ping

# Test API health
curl http://localhost:8000/api/health

# Access ClickHouse web interface
# Open: http://localhost:8123/play
```

## 🎯 Google Ads Integration Setup

### 1. Create Google Cloud Project
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing one
3. Enable the **Google Ads API**

### 2. Create OAuth 2.0 Credentials
```bash
# In Google Cloud Console:
# 1. Go to "APIs & Services" > "Credentials"
# 2. Click "Create Credentials" > "OAuth 2.0 Client ID"
# 3. Choose "Desktop Application"
# 4. Note down Client ID and Client Secret
```

### 3. Get Google Ads Developer Token
1. Go to [Google Ads API Center](https://ads.google.com/nav/selectaccount?authuser=0&dst=/aw/apicenter)
2. Apply for API access (may take 24-48 hours for approval)
3. Get your Developer Token

### 4. Generate Refresh Token
```bash
# Install Google Ads Python library
pip install google-ads

# Create token generation script
cat > generate_refresh_token.py << 'EOF'
from google_auth_oauthlib.flow import Flow
import json

# Your OAuth 2.0 credentials
CLIENT_ID = "your_client_id_here"
CLIENT_SECRET = "your_client_secret_here"
SCOPES = ['https://www.googleapis.com/auth/adwords']

# Create flow
flow = Flow.from_client_config(
    {
        "web": {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost:8080"]
        }
    },
    scopes=SCOPES
)
flow.redirect_uri = "http://localhost:8080"

# Get authorization URL
auth_url, _ = flow.authorization_url(prompt='consent')
print(f"Visit this URL to authorize: {auth_url}")

# After visiting URL and getting code, paste it here
authorization_code = input("Enter authorization code: ")

# Exchange code for tokens
flow.fetch_token(code=authorization_code)
credentials = flow.credentials

print(f"Refresh Token: {credentials.refresh_token}")
EOF

# Run the script
python generate_refresh_token.py
```

### 5. Get Customer IDs
```bash
# Create customer ID finder script
cat > get_customer_ids.py << 'EOF'
from google.ads.googleads.client import GoogleAdsClient

# Configure with your credentials
client = GoogleAdsClient.load_from_dict({
    "developer_token": "YOUR_DEVELOPER_TOKEN",
    "client_id": "YOUR_CLIENT_ID",
    "client_secret": "YOUR_CLIENT_SECRET",
    "refresh_token": "YOUR_REFRESH_TOKEN",
    "use_proto_plus": True
})

customer_service = client.get_service("CustomerService")
accessible_customers = customer_service.list_accessible_customers()

print("Accessible Customer IDs:")
for customer_resource in accessible_customers.resource_names:
    customer_id = customer_resource.split("/")[1]
    print(f"Customer ID: {customer_id}")
EOF

# Run to get your customer IDs
python get_customer_ids.py
```

### 6. Update Environment with Google Ads Credentials
```bash
# Update .env file with your credentials
GOOGLE_ADS_DEVELOPER_TOKEN=your_developer_token_here
GOOGLE_ADS_CLIENT_ID=your_client_id_here
GOOGLE_ADS_CLIENT_SECRET=your_client_secret_here
GOOGLE_ADS_REFRESH_TOKEN=your_refresh_token_here
GOOGLE_ADS_CUSTOMER_IDS=1234567890,0987654321

# Restart services to pick up new configuration
docker-compose restart
```

### 7. Test Google Ads Integration
```bash
# Test the Google Ads sync manually
docker-compose exec google-ads-sync python google_ads_sync.py

# Check if data is being synced
docker-compose exec clickhouse clickhouse-client --query "
SELECT count() as total_rows, max(date) as latest_date 
FROM analytics.google_ads_performance"
```

## 🌐 Website Tracking Integration

### 1. Basic Tracking Implementation
```html
<!-- Add to your website's <head> section -->
<script>
// Analytics configuration
window.analyticsConfig = {
    endpoint: 'https://your-domain.com/api/events',
    batchSize: 10,
    flushInterval: 5000
};
</script>

<!-- Load tracking script -->
<script src="https://your-domain.com/js/tracker.js" async></script>

<!-- Initialize tracking -->
<script>
document.addEventListener('DOMContentLoaded', function() {
    // Track initial pageview
    analytics.track('page_view');
    
    // Set up user identification (when user logs in)
    if (window.currentUserId) {
        analytics.identify(window.currentUserId);
    }
});
</script>
```

### 2. E-commerce Tracking
```javascript
// Track purchases
function trackPurchase(orderId, revenue, currency, products) {
    analytics.trackPurchase(orderId, revenue, currency, products);
}

// Track add to cart
function trackAddToCart(productId, category, value) {
    analytics.trackAddToCart(productId, category, value);
}

// Track sign ups
function trackSignUp(method) {
    analytics.trackSignUp(method);
}

// Example usage
trackPurchase('order_123', 99.99, 'USD', [
    {id: 'prod_1', name: 'Product 1', price: 49.99},
    {id: 'prod_2', name: 'Product 2', price: 49.99}
]);
```

### 3. Custom Event Tracking
```javascript
// Track custom events
analytics.track('video_play', {
    video_id: 'intro_video',
    duration: 120,
    custom: {
        video_title: 'Product Introduction',
        video_category: 'onboarding'
    }
});

// Track form submissions
analytics.track('form_submit', {
    custom: {
        form_name: 'contact_form',
        form_type: 'lead_generation'
    }
});
```

### 4. UTM Parameter Setup
```html
<!-- Ensure UTM parameters are captured -->
<script>
// Example landing page URLs with UTM parameters:
// https://yoursite.com/?utm_source=google&utm_medium=cpc&utm_campaign=summer_sale&utm_content=ad_variant_a&utm_term=running_shoes

// The tracker automatically captures these parameters
// and attributes them to all subsequent events in the session
</script>
```

## 🔍 Data Pipeline Verification

### 1. Verify Event Collection
```bash
# Send test event via API
curl -X POST http://localhost:8000/api/events \
  -H "Content-Type: application/json" \
  -d '{
    "events": [{
      "event_id": "test_001",
      "event_time": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
      "anonymous_id": "anon_123",
      "session_id": "session_456",
      "visit_id": 1,
      "event_type": "test_event",
      "page_url": "https://example.com/test",
      "utm_source": "test",
      "utm_campaign": "verification"
    }]
  }'

# Check if event was recorded
docker-compose exec clickhouse clickhouse-client --query "
SELECT * FROM analytics.events 
WHERE event_type = 'test_event' 
ORDER BY event_time DESC 
LIMIT 5"
```

### 2. Verify Materialized Views
```bash
# Check if materialized views are updating
docker-compose exec clickhouse clickhouse-client --query "
SELECT 
    'user_sessions_mv' as view_name,
    count() as total_rows,
    max(event_date) as latest_date
FROM analytics.user_sessions_mv

UNION ALL

SELECT 
    'attribution_touchpoints_mv' as view_name,
    count() as total_rows,
    max(event_date) as latest_date
FROM analytics.attribution_touchpoints_mv

UNION ALL

SELECT 
    'daily_cohorts_mv' as view_name,
    count() as total_rows,
    max(cohort_date) as latest_date
FROM analytics.daily_cohorts_mv"
```

### 3. Verify Google Ads Data Pipeline
```bash
# Check Google Ads sync status
docker-compose logs google-ads-sync

# Verify Google Ads data
docker-compose exec clickhouse clickhouse-client --query "
SELECT 
    date,
    campaign_name,
    sum(impressions) as total_impressions,
    sum(clicks) as total_clicks,
    sum(cost) as total_cost
FROM analytics.google_ads_performance 
WHERE date >= today() - 7
GROUP BY date, campaign_name
ORDER BY date DESC, total_cost DESC
LIMIT 10"
```

### 4. Test Real-time Processing
```bash
# Send multiple events to test batching
for i in {1..20}; do
  curl -X POST http://localhost:8000/api/events \
    -H "Content-Type: application/json" \
    -d '{
      "events": [{
        "event_id": "batch_test_'$i'",
        "event_time": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
        "anonymous_id": "anon_batch_test",
        "session_id": "session_batch_test",
        "visit_id": 1,
        "event_type": "page_view",
        "page_url": "https://example.com/page'$i'"
      }]
    }' &
done

# Wait and check processing
sleep 10
docker-compose exec clickhouse clickhouse-client --query "
SELECT count() as events_processed 
FROM analytics.events 
WHERE event_id LIKE 'batch_test_%'"
```

## 🧪 Test Queries

### 1. Basic Analytics Verification
```sql
-- Test in ClickHouse web interface: http://localhost:8123/play

-- 1. Check data freshness
SELECT 
    'events' as table_name,
    count() as total_rows,
    min(event_time) as earliest_event,
    max(event_time) as latest_event,
    uniq(user_id) as unique_users,
    uniq(session_id) as unique_sessions
FROM analytics.events

UNION ALL

SELECT 
    'google_ads_performance' as table_name,
    count() as total_rows,
    min(date) as earliest_date,
    max(date) as latest_date,
    uniq(campaign_id) as unique_campaigns,
    0 as unique_sessions
FROM analytics.google_ads_performance;
```

### 2. Real-time Analytics Functions
```sql
-- Test advanced analytics functions

-- User journey analysis (replace with actual user_id)
SELECT * FROM user_journey_analysis('test_user_123') LIMIT 10;

-- Attribution analysis for last 30 days
SELECT * FROM attribution_analysis(30) LIMIT 10;

-- Cohort retention analysis
SELECT * FROM cohort_retention_analysis(90) 
WHERE cohort_date >= today() - 30 
LIMIT 10;

-- Conversion funnel analysis (24-hour window)
SELECT * FROM conversion_funnel_analysis(24);

-- User segmentation
SELECT * FROM user_segmentation_analysis() LIMIT 10;

-- Campaign ROI analysis
SELECT * FROM campaign_roi_analysis(7) LIMIT 10;
```

### 3. Real-time Dashboard Metrics
```sql
-- Live metrics for monitoring dashboard
SELECT 
    'last_hour' as period,
    count() as events,
    uniq(user_id) as unique_users,
    uniq(session_id) as sessions,
    sum(revenue) as revenue,
    countIf(event_type = 'purchase') as conversions,
    countIf(event_type = 'page_view') as pageviews
FROM analytics.events 
WHERE event_time >= now() - INTERVAL 1 HOUR

UNION ALL

SELECT 
    'last_24h' as period,
    count() as events,
    uniq(user_id) as unique_users,
    uniq(session_id) as sessions,
    sum(revenue) as revenue,
    countIf(event_type = 'purchase') as conversions,
    countIf(event_type = 'page_view') as pageviews
FROM analytics.events 
WHERE event_time >= now() - INTERVAL 24 HOUR

UNION ALL

SELECT 
    'last_7d' as period,
    count() as events,
    uniq(user_id) as unique_users,
    uniq(session_id) as sessions,
    sum(revenue) as revenue,
    countIf(event_type = 'purchase') as conversions,
    countIf(event_type = 'page_view') as pageviews
FROM analytics.events 
WHERE event_date >= today() - 7;
```

### 4. Attribution Analysis Verification
```sql
-- Multi-touch attribution test
WITH user_touchpoints AS (
    SELECT 
        user_id,
        utm_source,
        utm_campaign,
        event_time,
        row_number() OVER (PARTITION BY user_id ORDER BY event_time) as touchpoint_order,
        count() OVER (PARTITION BY user_id) as total_touchpoints
    FROM analytics.events 
    WHERE event_time >= now() - INTERVAL 7 DAY
    AND utm_source != ''
    AND user_id != ''
),
conversions AS (
    SELECT 
        user_id,
        sum(revenue) as user_revenue
    FROM analytics.events
    WHERE event_time >= now() - INTERVAL 7 DAY
    AND event_type = 'purchase'
    GROUP BY user_id
)
SELECT 
    tp.utm_source,
    tp.utm_campaign,
    count(DISTINCT tp.user_id) as attributed_users,
    
    -- First-touch attribution
    sumIf(c.user_revenue, tp.touchpoint_order = 1) as first_touch_revenue,
    
    -- Last-touch attribution
    sumIf(c.user_revenue, tp.touchpoint_order = tp.total_touchpoints) as last_touch_revenue,
    
    -- Linear attribution
    sum(c.user_revenue / tp.total_touchpoints) as linear_attribution_revenue
    
FROM user_touchpoints tp
JOIN conversions c USING (user_id)
GROUP BY tp.utm_source, tp.utm_campaign
ORDER BY first_touch_revenue DESC
LIMIT 10;
```

### 5. Google Ads Integration Verification
```sql
-- Campaign performance with website attribution
WITH campaign_costs AS (
    SELECT 
        campaign_name,
        sum(cost) as total_spend,
        sum(clicks) as total_clicks,
        sum(impressions) as total_impressions
    FROM analytics.google_ads_performance
    WHERE date >= today() - 7
    GROUP BY campaign_name
),
campaign_conversions AS (
    SELECT 
        utm_campaign as campaign_name,
        count(DISTINCT user_id) as unique_users,
        sum(revenue) as total_revenue,
        countIf(event_type = 'purchase') as conversions
    FROM analytics.events
    WHERE event_date >= today() - 7
    AND utm_campaign != ''
    GROUP BY utm_campaign
)
SELECT 
    coalesce(costs.campaign_name, conv.campaign_name) as campaign,
    costs.total_spend,
    costs.total_clicks,
    costs.total_impressions,
    conv.unique_users,
    conv.total_revenue,
    conv.conversions,
    
    -- Performance metrics
    (conv.total_revenue - costs.total_spend) as profit,
    (conv.total_revenue - costs.total_spend) / costs.total_spend * 100 as roi_percent,
    costs.total_spend / conv.conversions as cost_per_conversion,
    conv.total_revenue / conv.conversions as revenue_per_conversion
    
FROM campaign_costs costs
FULL OUTER JOIN campaign_conversions conv USING (campaign_name)
WHERE costs.total_spend > 0 OR conv.total_revenue > 0
ORDER BY roi_percent DESC;
```

### 6. Data Quality Checks
```sql
-- Data quality verification queries

-- Check for missing required fields
SELECT 
    'Missing event_id' as issue,
    count() as count
FROM analytics.events 
WHERE event_id = '' OR event_id IS NULL

UNION ALL

SELECT 
    'Missing anonymous_id' as issue,
    count() as count
FROM analytics.events 
WHERE anonymous_id = '' OR anonymous_id IS NULL

UNION ALL

SELECT 
    'Missing session_id' as issue,
    count() as count
FROM analytics.events 
WHERE session_id = '' OR session_id IS NULL

UNION ALL

SELECT 
    'Future events' as issue,
    count() as count
FROM analytics.events 
WHERE event_time > now() + INTERVAL 1 HOUR;

-- Check event distribution
SELECT 
    event_type,
    count() as event_count,
    count() / (SELECT count() FROM analytics.events) * 100 as percentage
FROM analytics.events 
WHERE event_date >= today() - 7
GROUP BY event_type
ORDER BY event_count DESC;

-- Check session duration distribution
SELECT 
    CASE 
        WHEN session_duration < 30 THEN '0-30 seconds'
        WHEN session_duration < 60 THEN '30-60 seconds'
        WHEN session_duration < 300 THEN '1-5 minutes'
        WHEN session_duration < 1800 THEN '5-30 minutes'
        ELSE '30+ minutes'
    END as duration_bucket,
    count() as sessions
FROM analytics.user_sessions_mv
WHERE event_date >= today() - 7
GROUP BY duration_bucket
ORDER BY sessions DESC;
```

## 🔧 Troubleshooting

### Common Issues and Solutions

#### 1. ClickHouse Connection Issues
```bash
# Check ClickHouse logs
docker-compose logs clickhouse

# Restart ClickHouse
docker-compose restart clickhouse

# Check disk space
df -h

# Check memory usage
docker stats
```

#### 2. Google Ads API Issues
```bash
# Check Google Ads sync logs
docker-compose logs google-ads-sync

# Test credentials manually
docker-compose exec google-ads-sync python -c "
from google.ads.googleads.client import GoogleAdsClient
import os
client = GoogleAdsClient.load_from_dict({
    'developer_token': os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN'),
    'client_id': os.getenv('GOOGLE_ADS_CLIENT_ID'),
    'client_secret': os.getenv('GOOGLE_ADS_CLIENT_SECRET'),
    'refresh_token': os.getenv('GOOGLE_ADS_REFRESH_TOKEN'),
    'use_proto_plus': True
})
print('Google Ads client initialized successfully')
"

# Common Google Ads API errors:
# - Invalid developer token: Check token and API access approval
# - Invalid refresh token: Regenerate using OAuth flow
# - Customer ID not accessible: Verify customer ID and permissions
```

#### 3. Event Collection Issues
```bash
# Check API logs
docker-compose logs analytics-api

# Test event collection endpoint
curl -v -X POST http://localhost:8000/api/events \
  -H "Content-Type: application/json" \
  -d '{"events": [{"event_id": "test", "event_time": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'", "anonymous_id": "test", "session_id": "test", "visit_id": 1, "event_type": "test", "page_url": "test"}]}'

# Check for CORS issues (if accessing from browser)
# Add your domain to CORS_ORIGINS in docker-compose.yml
```

#### 4. Materialized View Issues
```bash
# Check materialized view status
docker-compose exec clickhouse clickhouse-client --query "
SELECT 
    database,
    table,
    engine,
    total_rows,
    total_bytes
FROM system.tables 
WHERE database = 'analytics' 
AND engine LIKE '%MaterializedView%'"

# Refresh materialized views manually if needed
docker-compose exec clickhouse clickhouse-client --query "
SYSTEM FLUSH LOGS;
OPTIMIZE TABLE analytics.user_sessions_mv;
OPTIMIZE TABLE analytics.attribution_touchpoints_mv;
OPTIMIZE TABLE analytics.daily_cohorts_mv;"
```

### Performance Optimization

#### 1. ClickHouse Optimization
```sql
-- Check table sizes
SELECT 
    table,
    formatReadableSize(sum(bytes)) as size,
    sum(rows) as rows
FROM system.parts 
WHERE database = 'analytics'
GROUP BY table
ORDER BY sum(bytes) DESC;

-- Optimize tables
OPTIMIZE TABLE analytics.events;
OPTIMIZE TABLE analytics.google_ads_performance;

-- Check query performance
SELECT 
    query,
    query_duration_ms,
    read_rows,
    read_bytes
FROM system.query_log 
WHERE event_time >= now() - INTERVAL 1 HOUR
ORDER BY query_duration_ms DESC
LIMIT 10;
```

#### 2. API Performance Monitoring
```bash
# Monitor API response times
curl -w "@curl-format.txt" -o /dev/null -s http://localhost:8000/api/health

# Create curl-format.txt
cat > curl-format.txt << 'EOF'
     time_namelookup:  %{time_namelookup}\n
        time_connect:  %{time_connect}\n
     time_appconnect:  %{time_appconnect}\n
    time_pretransfer:  %{time_pretransfer}\n
       time_redirect:  %{time_redirect}\n
  time_starttransfer:  %{time_starttransfer}\n
                     ----------\n
          time_total:  %{time_total}\n
EOF
```

## 🚀 Production Deployment

### 1. Production Environment Setup
```bash
# Production environment variables
cat > .env.production << 'EOF'
# ClickHouse Production Configuration
CLICKHOUSE_HOST=clickhouse-prod
CLICKHOUSE_PORT=8123
CLICKHOUSE_DATABASE=analytics
CLICKHOUSE_USER=analytics_user
CLICKHOUSE_PASSWORD=secure_production_password

# API Production Configuration
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=8
JWT_SECRET=production_jwt_secret_key

# Google Ads Production Configuration
GOOGLE_ADS_DEVELOPER_TOKEN=prod_developer_token
GOOGLE_ADS_CLIENT_ID=prod_client_id
GOOGLE_ADS_CLIENT_SECRET=prod_client_secret
GOOGLE_ADS_REFRESH_TOKEN=prod_refresh_token
GOOGLE_ADS_CUSTOMER_IDS=prod_customer_ids

# Security Configuration
CORS_ORIGINS=https://yourdomain.com,https://www.yourdomain.com
RATE_LIMIT_PER_MINUTE=10000

# Monitoring
SENTRY_DSN=your_sentry_dsn
LOG_LEVEL=INFO
EOF
```

### 2. Production Docker Compose
```yaml
# docker-compose.prod.yml
version: '3.8'

services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    container_name: analytics_clickhouse_prod
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - clickhouse_data_prod:/var/lib/clickhouse
      - ./analytics/schema:/docker-entrypoint-initdb.d
    environment:
      CLICKHOUSE_DB: analytics
      CLICKHOUSE_USER: analytics_user
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD}
    restart: unless-stopped
    deploy:
      resources:
        limits:
          memory: 4G
        reservations:
          memory: 2G

  analytics-api:
    build: ./analytics/api
    container_name: analytics_api_prod
    ports:
      - "8000:8000"
    depends_on:
      - clickhouse
    environment:
      CLICKHOUSE_HOST: clickhouse
      ENVIRONMENT: production
    restart: unless-stopped
    deploy:
      replicas: 2
      resources:
        limits:
          memory: 1G
        reservations:
          memory: 512M

  nginx:
    image: nginx:alpine
    container_name: analytics_nginx_prod
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx/nginx.prod.conf:/etc/nginx/nginx.conf
      - ./nginx/ssl:/etc/nginx/ssl
    depends_on:
      - analytics-api
    restart: unless-stopped

volumes:
  clickhouse_data_prod:
```

### 3. SSL/TLS Setup
```bash
# Generate SSL certificates (use Let's Encrypt for production)
mkdir -p nginx/ssl

# For testing (self-signed)
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout nginx/ssl/analytics.key \
  -out nginx/ssl/analytics.crt \
  -subj "/C=US/ST=State/L=City/O=Organization/CN=yourdomain.com"

# For production, use Let's Encrypt
# certbot certonly --webroot -w /var/www/html -d yourdomain.com
```

### 4. Monitoring Setup
```bash
# Add monitoring stack
cat >> docker-compose.prod.yml << 'EOF'
  prometheus:
    image: prom/prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana_data:/var/lib/grafana

volumes:
  grafana_data:
EOF
```

### 5. Backup Strategy
```bash
# Create backup script
cat > backup.sh << 'EOF'
#!/bin/bash
BACKUP_DIR="/backups/$(date +%Y%m%d_%H%M%S)"
mkdir -p $BACKUP_DIR

# Backup ClickHouse data
docker-compose exec clickhouse clickhouse-client --query "
BACKUP DATABASE analytics TO Disk('backups', '$BACKUP_DIR/analytics.zip')"

# Backup configuration
cp -r analytics/ $BACKUP_DIR/
cp .env $BACKUP_DIR/
cp docker-compose.yml $BACKUP_DIR/

echo "Backup completed: $BACKUP_DIR"
EOF

chmod +x backup.sh

# Set up daily backups
echo "0 2 * * * /path/to/backup.sh" | crontab -
```

## ✅ Verification Checklist

### Initial Setup ✓
- [ ] Docker and Docker Compose installed
- [ ] Repository cloned and setup script executed
- [ ] ClickHouse web interface accessible (http://localhost:8123/play)
- [ ] Analytics API health check passes (http://localhost:8000/api/health)
- [ ] Database schema and materialized views created

### Google Ads Integration ✓
- [ ] Google Cloud project created with Ads API enabled
- [ ] OAuth 2.0 credentials generated
- [ ] Developer token obtained and approved
- [ ] Refresh token generated successfully
- [ ] Customer IDs identified and configured
- [ ] Google Ads sync service running without errors
- [ ] Google Ads data appearing in analytics.google_ads_performance table

### Website Tracking ✓
- [ ] Tracking script implemented on website
- [ ] Test events successfully sent to API
- [ ] Events appearing in analytics.events table
- [ ] UTM parameters being captured correctly
- [ ] User identification working for logged-in users
- [ ] E-commerce events tracking properly

### Data Pipeline ✓
- [ ] Real-time event collection working
- [ ] Materialized views updating automatically
- [ ] Advanced analytics functions returning results
- [ ] Attribution analysis showing multi-touch data
- [ ] Cohort analysis generating retention metrics
- [ ] Campaign ROI analysis combining ads and website data

### Production Readiness ✓
- [ ] SSL/TLS certificates configured
- [ ] Environment variables secured
- [ ] Monitoring and alerting set up
- [ ] Backup strategy implemented
- [ ] Performance optimization applied
- [ ] Security measures in place

## 📞 Support

If you encounter issues during setup:

1. **Check the logs**: `docker-compose logs [service-name]`
2. **Verify configuration**: Ensure all environment variables are set correctly
3. **Test connectivity**: Use the provided test queries and API calls
4. **Review troubleshooting section**: Common issues and solutions are documented above
5. **Performance monitoring**: Use the provided monitoring queries to identify bottlenecks

For additional support, refer to the main README.md or create an issue in the repository.

---

🎉 **Congratulations!** You now have a fully functional advanced analytics platform with cross-session tracking, multi-touch attribution, and Google Ads integration!