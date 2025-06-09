#!/bin/bash

# ClickHouse Advanced Analytics Platform Setup Script
# Automated deployment with data cleaning and journey tracking

set -e

echo "🚀 Setting up ClickHouse Advanced Analytics Platform..."

# Check if Docker and Docker Compose are installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Create environment file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating environment configuration..."
    cat > .env << EOF
# ClickHouse Configuration
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=analytics_secure_password

# Google Ads Integration (fill in your credentials)
GOOGLE_ADS_DEVELOPER_TOKEN=
GOOGLE_ADS_CLIENT_ID=
GOOGLE_ADS_CLIENT_SECRET=
GOOGLE_ADS_REFRESH_TOKEN=
GOOGLE_ADS_CUSTOMER_IDS=

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
EOF
    echo "✅ Environment file created. Please update .env with your Google Ads credentials."
fi

# Create nginx configuration
mkdir -p nginx
cat > nginx/nginx.conf << 'EOF'
events {
    worker_connections 1024;
}

http {
    upstream analytics_api {
        server analytics-api:8000;
    }

    server {
        listen 80;
        
        # Serve tracking script
        location /js/ {
            root /usr/share/nginx/html;
            add_header Access-Control-Allow-Origin *;
            add_header Cache-Control "public, max-age=3600";
        }
        
        # Proxy API requests
        location /api/ {
            proxy_pass http://analytics_api;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
        
        # Health check
        location /health {
            proxy_pass http://analytics_api/api/health;
        }
    }
}
EOF

echo "🐳 Starting Docker services..."

# Build and start services
docker-compose up -d --build

echo "⏳ Waiting for services to be ready..."

# Wait for ClickHouse to be ready
echo "Waiting for ClickHouse..."
timeout=60
while ! docker-compose exec -T clickhouse clickhouse-client --query "SELECT 1" &> /dev/null; do
    sleep 2
    timeout=$((timeout - 2))
    if [ $timeout -le 0 ]; then
        echo "❌ ClickHouse failed to start within 60 seconds"
        exit 1
    fi
done

echo "✅ ClickHouse is ready"

# Wait for API to be ready
echo "Waiting for Analytics API..."
timeout=60
while ! curl -f http://localhost:8000/api/health &> /dev/null; do
    sleep 2
    timeout=$((timeout - 2))
    if [ $timeout -le 0 ]; then
        echo "❌ Analytics API failed to start within 60 seconds"
        exit 1
    fi
done

echo "✅ Analytics API is ready"

# Initialize database schema
echo "🗄️ Initializing database schema..."
docker-compose exec -T clickhouse clickhouse-client --multiquery < analytics/schema/01_core_tables.sql
docker-compose exec -T clickhouse clickhouse-client --multiquery < analytics/schema/02_data_cleaning_views.sql
docker-compose exec -T clickhouse clickhouse-client --multiquery < analytics/schema/03_journey_tracking_functions.sql

echo "✅ Database schema initialized"

# Test the setup
echo "🧪 Testing the setup..."

# Test API health
if curl -f http://localhost:8000/api/health > /dev/null 2>&1; then
    echo "✅ API health check passed"
else
    echo "❌ API health check failed"
fi

# Test event collection
echo "Testing event collection..."
curl -X POST http://localhost:8000/api/events \
  -H "Content-Type: application/json" \
  -d '{
    "events": [{
      "event_id": "test_setup_001",
      "event_time": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
      "event_type": "setup_test",
      "anonymous_id": "setup_test_user",
      "session_id": "setup_test_session",
      "page_url": "https://example.com/setup-test"
    }]
  }' > /dev/null 2>&1

if [ $? -eq 0 ]; then
    echo "✅ Event collection test passed"
else
    echo "❌ Event collection test failed"
fi

# Show service status
echo ""
echo "📊 Service Status:"
docker-compose ps

echo ""
echo "🎉 Setup completed successfully!"
echo ""
echo "📍 Access Points:"
echo "   • ClickHouse Web Interface: http://localhost:8123/play"
echo "   • Analytics API: http://localhost:8000"
echo "   • API Documentation: http://localhost:8000/docs"
echo "   • Tracking Script: http://localhost/js/tracker.js"
echo ""
echo "📝 Next Steps:"
echo "   1. Update .env file with your Google Ads credentials"
echo "   2. Restart services: docker-compose restart google-ads-sync"
echo "   3. Add tracking script to your website"
echo "   4. Check SETUP_GUIDE.md for detailed configuration"
echo ""
echo "🔍 Useful Commands:"
echo "   • View logs: docker-compose logs -f [service-name]"
echo "   • Stop services: docker-compose down"
echo "   • Restart services: docker-compose restart"
echo ""