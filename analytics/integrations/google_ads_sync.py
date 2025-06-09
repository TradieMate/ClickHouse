"""
ClickHouse Advanced Analytics Platform - Google Ads Integration
Automated sync of Google Ads performance data with attribution analysis
"""

import os
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import clickhouse_connect
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import schedule

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GoogleAdsSync:
    """Google Ads data synchronization with ClickHouse"""
    
    def __init__(self):
        self.clickhouse_client = None
        self.google_ads_client = None
        self.customer_ids = []
        self.setup_connections()
    
    def setup_connections(self):
        """Initialize ClickHouse and Google Ads connections"""
        try:
            # ClickHouse connection
            self.clickhouse_client = clickhouse_connect.get_client(
                host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
                port=int(os.getenv('CLICKHOUSE_PORT', '8123')),
                database=os.getenv('CLICKHOUSE_DATABASE', 'analytics'),
                username=os.getenv('CLICKHOUSE_USER', 'default'),
                password=os.getenv('CLICKHOUSE_PASSWORD', '')
            )
            logger.info("Connected to ClickHouse successfully")
            
            # Google Ads connection
            google_ads_config = {
                'developer_token': os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN'),
                'client_id': os.getenv('GOOGLE_ADS_CLIENT_ID'),
                'client_secret': os.getenv('GOOGLE_ADS_CLIENT_SECRET'),
                'refresh_token': os.getenv('GOOGLE_ADS_REFRESH_TOKEN'),
                'use_proto_plus': True
            }
            
            if not all([google_ads_config['developer_token'], 
                       google_ads_config['client_id'],
                       google_ads_config['client_secret'],
                       google_ads_config['refresh_token']]):
                raise ValueError("Missing required Google Ads API credentials")
            
            self.google_ads_client = GoogleAdsClient.load_from_dict(google_ads_config)
            
            # Parse customer IDs
            customer_ids_str = os.getenv('GOOGLE_ADS_CUSTOMER_IDS', '')
            if customer_ids_str:
                self.customer_ids = [cid.strip() for cid in customer_ids_str.split(',')]
            else:
                raise ValueError("No Google Ads customer IDs provided")
            
            logger.info(f"Connected to Google Ads API for {len(self.customer_ids)} customers")
            
        except Exception as e:
            logger.error(f"Failed to setup connections: {e}")
            raise
    
    def sync_campaign_performance(self, days_back: int = 7) -> None:
        """Sync campaign performance data from Google Ads"""
        try:
            end_date = datetime.now(timezone.utc).date()
            start_date = end_date - timedelta(days=days_back)
            
            logger.info(f"Syncing Google Ads data from {start_date} to {end_date}")
            
            all_performance_data = []
            
            for customer_id in self.customer_ids:
                try:
                    performance_data = self._fetch_campaign_performance(
                        customer_id, start_date, end_date
                    )
                    all_performance_data.extend(performance_data)
                    logger.info(f"Fetched {len(performance_data)} records for customer {customer_id}")
                    
                except GoogleAdsException as ex:
                    logger.error(f"Google Ads API error for customer {customer_id}: {ex}")
                    continue
                except Exception as e:
                    logger.error(f"Error fetching data for customer {customer_id}: {e}")
                    continue
            
            if all_performance_data:
                self._insert_performance_data(all_performance_data)
                logger.info(f"Successfully synced {len(all_performance_data)} performance records")
            else:
                logger.warning("No performance data to sync")
                
        except Exception as e:
            logger.error(f"Error in sync_campaign_performance: {e}")
            raise
    
    def _fetch_campaign_performance(self, customer_id: str, start_date, end_date) -> List[Dict[str, Any]]:
        """Fetch campaign performance data from Google Ads API"""
        ga_service = self.google_ads_client.get_service("GoogleAdsService")
        
        query = f"""
            SELECT 
                segments.date,
                customer.id,
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group_criterion.keyword.text,
                ad_group_criterion.criterion_id,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value,
                metrics.search_impression_share,
                metrics.quality_score,
                metrics.ctr,
                metrics.average_cpc,
                metrics.average_position
            FROM keyword_view 
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                AND campaign.status = 'ENABLED'
                AND ad_group.status = 'ENABLED'
                AND ad_group_criterion.status IN ('ENABLED', 'PAUSED')
        """
        
        performance_data = []
        
        try:
            search_request = self.google_ads_client.get_type("SearchGoogleAdsRequest")
            search_request.customer_id = customer_id
            search_request.query = query
            search_request.page_size = 10000
            
            results = ga_service.search(request=search_request)
            
            for row in results:
                performance_data.append({
                    'date': str(row.segments.date),
                    'account_id': str(row.customer.id),
                    'campaign_id': str(row.campaign.id),
                    'campaign_name': self._clean_string(row.campaign.name),
                    'ad_group_id': str(row.ad_group.id),
                    'ad_group_name': self._clean_string(row.ad_group.name),
                    'keyword_id': str(row.ad_group_criterion.criterion_id),
                    'keyword_text': self._clean_string(row.ad_group_criterion.keyword.text),
                    'impressions': int(row.metrics.impressions or 0),
                    'clicks': int(row.metrics.clicks or 0),
                    'cost': round(float(row.metrics.cost_micros or 0) / 1000000, 2),
                    'conversions': int(row.metrics.conversions or 0),
                    'conversion_value': round(float(row.metrics.conversions_value or 0), 2),
                    'quality_score': float(row.metrics.quality_score or 0),
                    'ctr': round(float(row.metrics.ctr or 0) * 100, 4),
                    'avg_cpc': round(float(row.metrics.average_cpc or 0) / 1000000, 2),
                    'avg_position': round(float(row.metrics.average_position or 0), 2),
                    'sync_timestamp': datetime.now(timezone.utc).isoformat()
                })
                
        except GoogleAdsException as ex:
            logger.error(f"Google Ads API exception: {ex}")
            raise
        except Exception as e:
            logger.error(f"Error fetching performance data: {e}")
            raise
        
        return performance_data
    
    def _insert_performance_data(self, performance_data: List[Dict[str, Any]]) -> None:
        """Insert performance data into ClickHouse"""
        try:
            if not performance_data:
                return
            
            # Prepare data for insertion
            columns = [
                'date', 'account_id', 'campaign_id', 'campaign_name',
                'ad_group_id', 'ad_group_name', 'keyword_id', 'keyword_text',
                'impressions', 'clicks', 'cost', 'conversions', 'conversion_value',
                'quality_score', 'ctr', 'avg_cpc', 'avg_position', 'sync_timestamp'
            ]
            
            # Convert data to list of tuples
            rows = []
            for record in performance_data:
                row = tuple(record.get(col, '') for col in columns)
                rows.append(row)
            
            # Insert into ClickHouse
            self.clickhouse_client.insert(
                'analytics.google_ads_performance',
                rows,
                column_names=columns
            )
            
            logger.info(f"Inserted {len(rows)} performance records into ClickHouse")
            
        except Exception as e:
            logger.error(f"Error inserting performance data: {e}")
            raise
    
    def sync_attribution_data(self) -> None:
        """Sync and analyze attribution data between Google Ads and website analytics"""
        try:
            logger.info("Starting attribution data sync")
            
            # Get Google Ads clicks with gclid
            attribution_query = """
                WITH google_ads_clicks AS (
                    SELECT 
                        date,
                        campaign_name,
                        sum(clicks) as ad_clicks,
                        sum(cost) as ad_spend
                    FROM analytics.google_ads_performance
                    WHERE date >= today() - 30
                    GROUP BY date, campaign_name
                ),
                website_conversions AS (
                    SELECT 
                        toDate(event_time) as date,
                        utm_campaign as campaign_name,
                        count(DISTINCT user_id) as unique_users,
                        sum(revenue) as website_revenue,
                        countIf(event_type = 'purchase') as conversions
                    FROM analytics.events
                    WHERE event_date >= today() - 30
                      AND utm_source = 'google'
                      AND utm_medium = 'cpc'
                      AND utm_campaign != ''
                      AND is_valid = 1
                    GROUP BY date, campaign_name
                )
                SELECT 
                    coalesce(gac.date, wc.date) as date,
                    coalesce(gac.campaign_name, wc.campaign_name) as campaign_name,
                    coalesce(gac.ad_clicks, 0) as ad_clicks,
                    coalesce(gac.ad_spend, 0) as ad_spend,
                    coalesce(wc.unique_users, 0) as website_users,
                    coalesce(wc.website_revenue, 0) as website_revenue,
                    coalesce(wc.conversions, 0) as website_conversions,
                    
                    -- Calculate attribution metrics
                    if(gac.ad_spend > 0, wc.website_revenue / gac.ad_spend, 0) as roas,
                    if(wc.conversions > 0, gac.ad_spend / wc.conversions, 0) as cost_per_conversion,
                    if(gac.ad_clicks > 0, wc.unique_users / gac.ad_clicks * 100, 0) as click_to_visit_rate
                FROM google_ads_clicks gac
                FULL OUTER JOIN website_conversions wc 
                    ON gac.date = wc.date AND gac.campaign_name = wc.campaign_name
                WHERE gac.ad_spend > 0 OR wc.website_revenue > 0
                ORDER BY date DESC, roas DESC
            """
            
            attribution_results = self.clickhouse_client.query(attribution_query).result_rows
            
            # Log attribution insights
            logger.info("Attribution Analysis Results:")
            for row in attribution_results[:10]:  # Top 10 results
                date, campaign, clicks, spend, users, revenue, conversions, roas, cpc, ctr = row
                logger.info(
                    f"Campaign: {campaign}, Date: {date}, "
                    f"Spend: ${spend:.2f}, Revenue: ${revenue:.2f}, "
                    f"ROAS: {roas:.2f}, Conversions: {conversions}"
                )
            
            # Store attribution results for dashboard
            self._store_attribution_results(attribution_results)
            
        except Exception as e:
            logger.error(f"Error in attribution data sync: {e}")
            raise
    
    def _store_attribution_results(self, attribution_results: List[tuple]) -> None:
        """Store attribution analysis results"""
        try:
            # Create attribution summary table if not exists
            create_table_query = """
                CREATE TABLE IF NOT EXISTS analytics.attribution_summary (
                    date Date,
                    campaign_name String,
                    ad_clicks UInt64,
                    ad_spend Decimal(10,2),
                    website_users UInt64,
                    website_revenue Decimal(10,2),
                    website_conversions UInt64,
                    roas Decimal(10,4),
                    cost_per_conversion Decimal(10,2),
                    click_to_visit_rate Decimal(10,4),
                    sync_timestamp DateTime64(3, 'UTC') DEFAULT now64()
                )
                ENGINE = ReplacingMergeTree(sync_timestamp)
                PARTITION BY toYYYYMM(date)
                ORDER BY (date, campaign_name)
            """
            
            self.clickhouse_client.command(create_table_query)
            
            # Insert attribution results
            if attribution_results:
                columns = [
                    'date', 'campaign_name', 'ad_clicks', 'ad_spend',
                    'website_users', 'website_revenue', 'website_conversions',
                    'roas', 'cost_per_conversion', 'click_to_visit_rate'
                ]
                
                self.clickhouse_client.insert(
                    'analytics.attribution_summary',
                    attribution_results,
                    column_names=columns
                )
                
                logger.info(f"Stored {len(attribution_results)} attribution records")
            
        except Exception as e:
            logger.error(f"Error storing attribution results: {e}")
            raise
    
    def run_data_quality_checks(self) -> None:
        """Run data quality checks on Google Ads data"""
        try:
            logger.info("Running Google Ads data quality checks")
            
            quality_checks = [
                {
                    'check_name': 'missing_campaign_names',
                    'query': """
                        SELECT count() as issue_count
                        FROM analytics.google_ads_performance
                        WHERE campaign_name = '' AND date >= today() - 7
                    """
                },
                {
                    'check_name': 'zero_cost_with_clicks',
                    'query': """
                        SELECT count() as issue_count
                        FROM analytics.google_ads_performance
                        WHERE cost = 0 AND clicks > 0 AND date >= today() - 7
                    """
                },
                {
                    'check_name': 'high_cost_per_click',
                    'query': """
                        SELECT count() as issue_count
                        FROM analytics.google_ads_performance
                        WHERE avg_cpc > 50 AND date >= today() - 7
                    """
                },
                {
                    'check_name': 'future_dates',
                    'query': """
                        SELECT count() as issue_count
                        FROM analytics.google_ads_performance
                        WHERE date > today()
                    """
                }
            ]
            
            for check in quality_checks:
                try:
                    result = self.clickhouse_client.query(check['query']).result_rows[0][0]
                    
                    if result > 0:
                        # Log quality issue
                        self.clickhouse_client.insert(
                            'analytics.data_quality_log',
                            [(
                                datetime.now(timezone.utc).isoformat(),
                                'google_ads_sync',
                                'google_ads_performance',
                                check['check_name'],
                                result,
                                f"Found {result} records with {check['check_name']}",
                                'medium' if result < 100 else 'high'
                            )],
                            column_names=[
                                'check_timestamp', 'check_type', 'table_name',
                                'issue_type', 'issue_count', 'issue_details', 'severity'
                            ]
                        )
                        
                        logger.warning(f"Data quality issue: {check['check_name']} - {result} records")
                    
                except Exception as e:
                    logger.error(f"Error running quality check {check['check_name']}: {e}")
            
        except Exception as e:
            logger.error(f"Error in data quality checks: {e}")
    
    def _clean_string(self, value: str, max_length: int = 255) -> str:
        """Clean string values for database insertion"""
        if not value:
            return ''
        
        # Remove null bytes and control characters
        cleaned = ''.join(char for char in str(value) if ord(char) >= 32 or char in '\t\n\r')
        return cleaned[:max_length].strip()
    
    def run_full_sync(self) -> None:
        """Run complete synchronization process"""
        try:
            logger.info("Starting full Google Ads sync")
            
            # Sync performance data
            self.sync_campaign_performance(days_back=7)
            
            # Sync attribution data
            self.sync_attribution_data()
            
            # Run quality checks
            self.run_data_quality_checks()
            
            logger.info("Full Google Ads sync completed successfully")
            
        except Exception as e:
            logger.error(f"Error in full sync: {e}")
            raise

def main():
    """Main function to run Google Ads sync"""
    try:
        sync = GoogleAdsSync()
        
        # Schedule regular syncs
        schedule.every(6).hours.do(sync.run_full_sync)
        schedule.every().day.at("02:00").do(lambda: sync.sync_campaign_performance(days_back=30))
        
        logger.info("Google Ads sync scheduler started")
        
        # Run initial sync
        sync.run_full_sync()
        
        # Keep running scheduled tasks
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        logger.info("Google Ads sync stopped by user")
    except Exception as e:
        logger.error(f"Fatal error in Google Ads sync: {e}")
        raise

if __name__ == "__main__":
    main()