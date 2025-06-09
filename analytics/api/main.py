"""
ClickHouse Advanced Analytics Platform - API Server
Real-time event collection with data cleaning, validation, and processing
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator, Field
from typing import List, Optional, Dict, Any
import clickhouse_connect
import json
import hashlib
import time
from datetime import datetime, timezone
import logging
import os
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'analytics')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')

# Data validation and cleaning models
class EventData(BaseModel):
    event_id: str = Field(..., min_length=1, max_length=255)
    event_time: datetime
    event_type: str = Field(..., min_length=1, max_length=100)
    
    # User identification
    user_id: Optional[str] = Field(default='', max_length=255)
    anonymous_id: str = Field(..., min_length=1, max_length=255)
    session_id: str = Field(..., min_length=1, max_length=255)
    visit_id: int = Field(default=1, ge=1)
    
    # Device and browser info
    device_fingerprint: Optional[str] = Field(default='', max_length=255)
    user_agent: Optional[str] = Field(default='', max_length=1000)
    ip_address: Optional[str] = Field(default='0.0.0.0')
    
    # Page information
    page_url: Optional[str] = Field(default='', max_length=2000)
    page_title: Optional[str] = Field(default='', max_length=500)
    referrer_url: Optional[str] = Field(default='', max_length=2000)
    
    # UTM parameters
    utm_source: Optional[str] = Field(default='', max_length=255)
    utm_medium: Optional[str] = Field(default='', max_length=255)
    utm_campaign: Optional[str] = Field(default='', max_length=255)
    utm_content: Optional[str] = Field(default='', max_length=255)
    utm_term: Optional[str] = Field(default='', max_length=255)
    
    # Google Ads tracking
    gclid: Optional[str] = Field(default='', max_length=255)
    gbraid: Optional[str] = Field(default='', max_length=255)
    wbraid: Optional[str] = Field(default='', max_length=255)
    
    # E-commerce data
    revenue: Optional[float] = Field(default=0.0, ge=0)
    currency: Optional[str] = Field(default='USD', max_length=3)
    order_id: Optional[str] = Field(default='', max_length=255)
    product_id: Optional[str] = Field(default='', max_length=255)
    product_category: Optional[str] = Field(default='', max_length=255)
    quantity: Optional[int] = Field(default=0, ge=0)
    
    # Custom properties
    custom_properties: Optional[Dict[str, Any]] = Field(default_factory=dict)
    
    # Session context
    session_start_time: Optional[datetime] = None
    session_duration: Optional[int] = Field(default=0, ge=0)
    page_views_in_session: Optional[int] = Field(default=1, ge=1)
    is_bounce: Optional[bool] = Field(default=False)
    
    @validator('event_time')
    def validate_event_time(cls, v):
        """Validate event time is not too far in the future or past"""
        now = datetime.now(timezone.utc)
        if v > now.replace(hour=23, minute=59, second=59):
            raise ValueError('Event time cannot be in the future')
        if (now - v).days > 7:
            logger.warning(f"Event time is more than 7 days old: {v}")
        return v
    
    @validator('page_url', 'referrer_url')
    def validate_urls(cls, v):
        """Basic URL validation"""
        if v and not (v.startswith('http://') or v.startswith('https://')):
            if v.startswith('//'):
                v = 'https:' + v
            elif not v.startswith('/'):
                v = 'https://' + v
        return v
    
    @validator('custom_properties')
    def validate_custom_properties(cls, v):
        """Ensure custom properties can be serialized to JSON"""
        try:
            json.dumps(v)
            return v
        except (TypeError, ValueError):
            raise ValueError('Custom properties must be JSON serializable')

class EventBatch(BaseModel):
    events: List[EventData] = Field(..., min_items=1, max_items=1000)

class UserIdentification(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=255)
    anonymous_id: str = Field(..., min_length=1, max_length=255)
    traits: Optional[Dict[str, Any]] = Field(default_factory=dict)

# Data cleaning and validation functions
class DataCleaner:
    @staticmethod
    def clean_string(value: str, max_length: int = 255) -> str:
        """Clean and truncate string values"""
        if not value:
            return ''
        # Remove null bytes and control characters
        cleaned = ''.join(char for char in value if ord(char) >= 32 or char in '\t\n\r')
        return cleaned[:max_length].strip()
    
    @staticmethod
    def validate_ip_address(ip: str) -> str:
        """Validate and clean IP address"""
        if not ip or ip == '0.0.0.0':
            return '0.0.0.0'
        
        # Basic IPv4 validation
        parts = ip.split('.')
        if len(parts) != 4:
            return '0.0.0.0'
        
        try:
            for part in parts:
                if not 0 <= int(part) <= 255:
                    return '0.0.0.0'
            return ip
        except ValueError:
            return '0.0.0.0'
    
    @staticmethod
    def generate_device_fingerprint(user_agent: str, ip: str) -> str:
        """Generate device fingerprint for cross-device tracking"""
        if not user_agent and not ip:
            return ''
        
        fingerprint_data = f"{user_agent}:{ip}"
        return hashlib.md5(fingerprint_data.encode()).hexdigest()
    
    @staticmethod
    def detect_bot_traffic(user_agent: str) -> bool:
        """Detect bot traffic based on user agent"""
        if not user_agent:
            return False
        
        bot_indicators = [
            'bot', 'crawler', 'spider', 'scraper', 'curl', 'wget',
            'python-requests', 'java/', 'go-http-client'
        ]
        
        user_agent_lower = user_agent.lower()
        return any(indicator in user_agent_lower for indicator in bot_indicators)

# ClickHouse connection manager
class ClickHouseManager:
    def __init__(self):
        self.client = None
        self.connect()
    
    def connect(self):
        """Establish connection to ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                database=CLICKHOUSE_DATABASE,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD
            )
            logger.info("Connected to ClickHouse successfully")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def insert_events(self, events: List[EventData]):
        """Insert events into ClickHouse with data cleaning"""
        if not events:
            return
        
        cleaned_events = []
        validation_errors = []
        
        for event in events:
            try:
                # Data cleaning and validation
                cleaned_event = self._clean_event(event)
                
                # Detect and flag bot traffic
                if DataCleaner.detect_bot_traffic(cleaned_event.get('user_agent', '')):
                    cleaned_event['is_valid'] = 0
                    cleaned_event['validation_errors'] = 'bot_traffic'
                
                cleaned_events.append(cleaned_event)
                
            except Exception as e:
                logger.error(f"Error cleaning event {event.event_id}: {e}")
                validation_errors.append({
                    'event_id': event.event_id,
                    'error': str(e)
                })
        
        if cleaned_events:
            try:
                # Insert into buffer table for real-time processing
                self.client.insert(
                    'analytics.events_buffer',
                    cleaned_events,
                    column_names=[
                        'event_id', 'event_time', 'event_type', 'user_id', 'anonymous_id',
                        'session_id', 'visit_id', 'device_fingerprint', 'user_agent', 'ip_address',
                        'page_url', 'page_title', 'referrer_url', 'utm_source', 'utm_medium',
                        'utm_campaign', 'utm_content', 'utm_term', 'gclid', 'gbraid', 'wbraid',
                        'revenue', 'currency', 'order_id', 'product_id', 'product_category',
                        'quantity', 'custom_properties', 'session_start_time', 'session_duration',
                        'page_views_in_session', 'is_bounce', 'is_valid', 'validation_errors'
                    ]
                )
                logger.info(f"Inserted {len(cleaned_events)} events successfully")
                
            except Exception as e:
                logger.error(f"Failed to insert events: {e}")
                raise
        
        if validation_errors:
            logger.warning(f"Validation errors for {len(validation_errors)} events")
    
    def _clean_event(self, event: EventData) -> Dict[str, Any]:
        """Clean and prepare event data for insertion"""
        # Generate device fingerprint if not provided
        device_fingerprint = event.device_fingerprint
        if not device_fingerprint:
            device_fingerprint = DataCleaner.generate_device_fingerprint(
                event.user_agent or '', event.ip_address or ''
            )
        
        # Set session start time if not provided
        session_start_time = event.session_start_time or event.event_time
        
        return {
            'event_id': DataCleaner.clean_string(event.event_id),
            'event_time': event.event_time,
            'event_type': DataCleaner.clean_string(event.event_type, 100),
            'user_id': DataCleaner.clean_string(event.user_id or ''),
            'anonymous_id': DataCleaner.clean_string(event.anonymous_id),
            'session_id': DataCleaner.clean_string(event.session_id),
            'visit_id': event.visit_id,
            'device_fingerprint': DataCleaner.clean_string(device_fingerprint),
            'user_agent': DataCleaner.clean_string(event.user_agent or '', 1000),
            'ip_address': DataCleaner.validate_ip_address(event.ip_address or '0.0.0.0'),
            'page_url': DataCleaner.clean_string(event.page_url or '', 2000),
            'page_title': DataCleaner.clean_string(event.page_title or '', 500),
            'referrer_url': DataCleaner.clean_string(event.referrer_url or '', 2000),
            'utm_source': DataCleaner.clean_string(event.utm_source or ''),
            'utm_medium': DataCleaner.clean_string(event.utm_medium or ''),
            'utm_campaign': DataCleaner.clean_string(event.utm_campaign or ''),
            'utm_content': DataCleaner.clean_string(event.utm_content or ''),
            'utm_term': DataCleaner.clean_string(event.utm_term or ''),
            'gclid': DataCleaner.clean_string(event.gclid or ''),
            'gbraid': DataCleaner.clean_string(event.gbraid or ''),
            'wbraid': DataCleaner.clean_string(event.wbraid or ''),
            'revenue': round(event.revenue or 0, 2),
            'currency': DataCleaner.clean_string(event.currency or 'USD', 3),
            'order_id': DataCleaner.clean_string(event.order_id or ''),
            'product_id': DataCleaner.clean_string(event.product_id or ''),
            'product_category': DataCleaner.clean_string(event.product_category or ''),
            'quantity': event.quantity or 0,
            'custom_properties': json.dumps(event.custom_properties or {}),
            'session_start_time': session_start_time,
            'session_duration': event.session_duration or 0,
            'page_views_in_session': event.page_views_in_session or 1,
            'is_bounce': 1 if event.is_bounce else 0,
            'is_valid': 1,
            'validation_errors': ''
        }
    
    def identify_user(self, identification: UserIdentification):
        """Handle user identification and profile merging"""
        try:
            # Update user profile
            self.client.command(f"""
                INSERT INTO analytics.user_profiles 
                (user_id, anonymous_ids, first_seen, last_seen, created_at, updated_at)
                VALUES (
                    '{identification.user_id}',
                    ['{identification.anonymous_id}'],
                    now64(),
                    now64(),
                    now64(),
                    now64()
                )
            """)
            
            # Update existing events with user_id
            self.client.command(f"""
                ALTER TABLE analytics.events 
                UPDATE user_id = '{identification.user_id}'
                WHERE anonymous_id = '{identification.anonymous_id}' AND user_id = ''
            """)
            
            logger.info(f"User identification completed: {identification.user_id}")
            
        except Exception as e:
            logger.error(f"Failed to identify user: {e}")
            raise

# Initialize ClickHouse manager
ch_manager = ClickHouseManager()

# FastAPI app initialization
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting Analytics API server")
    yield
    # Shutdown
    logger.info("Shutting down Analytics API server")

app = FastAPI(
    title="ClickHouse Advanced Analytics Platform",
    description="Real-time event collection with data cleaning and journey tracking",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API endpoints
@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test ClickHouse connection
        result = ch_manager.client.command("SELECT 1")
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "clickhouse": "connected",
            "version": "1.0.0"
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {e}")

@app.post("/api/events")
async def collect_events(
    event_batch: EventBatch,
    background_tasks: BackgroundTasks
):
    """Collect and process events with real-time data cleaning"""
    try:
        # Process events in background for better performance
        background_tasks.add_task(ch_manager.insert_events, event_batch.events)
        
        return {
            "status": "accepted",
            "events_count": len(event_batch.events),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    except Exception as e:
        logger.error(f"Error processing events: {e}")
        raise HTTPException(status_code=500, detail="Failed to process events")

@app.post("/api/identify")
async def identify_user(identification: UserIdentification):
    """Handle user identification for cross-session tracking"""
    try:
        ch_manager.identify_user(identification)
        
        return {
            "status": "success",
            "user_id": identification.user_id,
            "anonymous_id": identification.anonymous_id,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    except Exception as e:
        logger.error(f"Error identifying user: {e}")
        raise HTTPException(status_code=500, detail="Failed to identify user")

@app.get("/api/stats")
async def get_basic_stats():
    """Get basic analytics statistics"""
    try:
        # Get real-time metrics
        stats = ch_manager.client.query("""
            SELECT 
                'last_hour' as period,
                count() as events,
                uniq(user_id) as unique_users,
                uniq(session_id) as sessions,
                sum(revenue) as revenue,
                countIf(event_type = 'purchase') as conversions
            FROM analytics.events 
            WHERE event_time >= now() - INTERVAL 1 HOUR
            
            UNION ALL
            
            SELECT 
                'last_24h' as period,
                count() as events,
                uniq(user_id) as unique_users,
                uniq(session_id) as sessions,
                sum(revenue) as revenue,
                countIf(event_type = 'purchase') as conversions
            FROM analytics.events 
            WHERE event_time >= now() - INTERVAL 24 HOUR
        """).result_rows
        
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metrics": [
                {
                    "period": row[0],
                    "events": row[1],
                    "unique_users": row[2],
                    "sessions": row[3],
                    "revenue": float(row[4]),
                    "conversions": row[5]
                }
                for row in stats
            ]
        }
    
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to get statistics")

@app.get("/api/data-quality")
async def get_data_quality_report():
    """Get data quality monitoring report"""
    try:
        quality_report = ch_manager.client.query("""
            SELECT 
                check_type,
                issue_type,
                sum(issue_count) as total_issues,
                max(check_timestamp) as last_check,
                severity
            FROM analytics.data_quality_log
            WHERE check_timestamp >= now() - INTERVAL 24 HOUR
            GROUP BY check_type, issue_type, severity
            ORDER BY severity DESC, total_issues DESC
        """).result_rows
        
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "quality_issues": [
                {
                    "check_type": row[0],
                    "issue_type": row[1],
                    "total_issues": row[2],
                    "last_check": row[3].isoformat() if row[3] else None,
                    "severity": row[4]
                }
                for row in quality_report
            ]
        }
    
    except Exception as e:
        logger.error(f"Error getting data quality report: {e}")
        raise HTTPException(status_code=500, detail="Failed to get data quality report")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )