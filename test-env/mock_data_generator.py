#!/usr/bin/env python3
"""
Mock data generator for Pontoon test environment.
Generates realistic mock data for leads, campaigns, and multitouch_attribution tables.
"""

import psycopg2
import random
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'source-postgres'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'analytics'),
    'user': os.getenv('DB_USER', 'source'),
    'password': os.getenv('DB_PASSWORD', 'test')
}

# Mock data constants
CUSTOMER_IDS = [f"Customer{i}" for i in range(1, 11)]  # Customer1 - Customer10
LEAD_SOURCES = [
    'Webinar', 'Google Ads', 'LinkedIn Ads', 'Facebook Ads', 'Email Campaign',
    'Organic Search', 'Direct Traffic', 'Referral', 'Trade Show', 'Cold Outreach',
    'Content Marketing', 'Social Media', 'Partner Referral', 'Press Release'
]
LIFECYCLE_STAGES = ['MQL', 'SQL', 'Customer', 'Prospect', 'Lead', 'Opportunity']
LEAD_STATUSES = ['Open', 'Qualified', 'Contacted', 'Meeting Scheduled', 'Proposal Sent', 'Negotiation', 'Closed Won', 'Closed Lost']
CAMPAIGN_CHANNELS = ['email', 'social', 'search', 'display', 'video', 'affiliate', 'retargeting', 'influencer']
CAMPAIGN_STATUSES = ['active', 'paused', 'completed', 'draft', 'scheduled']
ATTRIBUTION_MODELS = ['linear', 'first_touch', 'last_touch', 'time_decay', 'position_based', 'data_driven']

def get_db_connection():
    """Create and return a database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def generate_mock_leads(count: int = 100) -> List[Dict[str, Any]]:
    """Generate mock leads data."""
    leads = []
    for _ in range(count):
        lead = {
            'customer_id': random.choice(CUSTOMER_IDS),
            'last_modified': datetime.now() - timedelta(days=random.randint(0, 365)),
            'lead_source': random.choice(LEAD_SOURCES),
            'campaign_id': random.randint(1, 50),
            'lifecycle_stage': random.choice(LIFECYCLE_STAGES),
            'lead_status': random.choice(LEAD_STATUSES),
            'lead_score': random.randint(0, 100),
            'is_converted': random.choice([True, False]),
            'created_at': datetime.now() - timedelta(days=random.randint(0, 730))
        }
        leads.append(lead)
    return leads

def generate_mock_campaigns(count: int = 100) -> List[Dict[str, Any]]:
    """Generate mock campaigns data."""
    campaigns = []
    for _ in range(count):
        start_date = datetime.now() - timedelta(days=random.randint(0, 365))
        end_date = start_date + timedelta(days=random.randint(30, 180))
        
        campaign = {
            'customer_id': random.choice(CUSTOMER_IDS),
            'last_modified': datetime.now() - timedelta(days=random.randint(0, 365)),
            'name': f"Campaign {random.randint(1000, 9999)} - {random.choice(['Q1', 'Q2', 'Q3', 'Q4'])} {random.randint(2022, 2024)}",
            'start_date': start_date.date(),
            'end_date': end_date.date(),
            'budget': round(random.uniform(1000, 50000), 2),
            'channel': random.choice(CAMPAIGN_CHANNELS),
            'status': random.choice(CAMPAIGN_STATUSES),
            'created_at': start_date - timedelta(days=random.randint(1, 30))
        }
        campaigns.append(campaign)
    return campaigns

def generate_mock_attribution(count: int = 100) -> List[Dict[str, Any]]:
    """Generate mock multitouch attribution data."""
    attributions = []
    for _ in range(count):
        touchpoint_time = datetime.now() - timedelta(days=random.randint(0, 365))
        
        attribution = {
            'customer_id': random.choice(CUSTOMER_IDS),
            'last_modified': datetime.now() - timedelta(days=random.randint(0, 365)),
            'lead_id': random.randint(1, 1000),
            'campaign_id': random.randint(1, 50),
            'touchpoint_order': random.randint(1, 10),
            'touchpoint_time': touchpoint_time,
            'attribution_model': random.choice(ATTRIBUTION_MODELS),
            'attribution_value': round(random.uniform(0.05, 1.0), 2),
            'created_at': touchpoint_time + timedelta(minutes=random.randint(1, 60))
        }
        attributions.append(attribution)
    return attributions

def insert_leads(conn, leads: List[Dict[str, Any]]):
    """Insert leads data into the database."""
    cursor = conn.cursor()
    try:
        for lead in leads:
            cursor.execute("""
                INSERT INTO pontoon_data.leads 
                (customer_id, last_modified, lead_source, campaign_id, lifecycle_stage, 
                 lead_status, lead_score, is_converted, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                lead['customer_id'], lead['last_modified'], lead['lead_source'],
                lead['campaign_id'], lead['lifecycle_stage'], lead['lead_status'],
                lead['lead_score'], lead['is_converted'], lead['created_at']
            ))
        conn.commit()
        logger.info(f"Inserted {len(leads)} leads records")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert leads: {e}")
        raise
    finally:
        cursor.close()

def insert_campaigns(conn, campaigns: List[Dict[str, Any]]):
    """Insert campaigns data into the database."""
    cursor = conn.cursor()
    try:
        for campaign in campaigns:
            cursor.execute("""
                INSERT INTO pontoon_data.campaigns 
                (customer_id, last_modified, name, start_date, end_date, 
                 budget, channel, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                campaign['customer_id'], campaign['last_modified'], campaign['name'],
                campaign['start_date'], campaign['end_date'], campaign['budget'],
                campaign['channel'], campaign['status'], campaign['created_at']
            ))
        conn.commit()
        logger.info(f"Inserted {len(campaigns)} campaigns records")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert campaigns: {e}")
        raise
    finally:
        cursor.close()

def insert_attributions(conn, attributions: List[Dict[str, Any]]):
    """Insert multitouch attribution data into the database."""
    cursor = conn.cursor()
    try:
        for attribution in attributions:
            cursor.execute("""
                INSERT INTO pontoon_data.multitouch_attribution 
                (customer_id, last_modified, lead_id, campaign_id, touchpoint_order,
                 touchpoint_time, attribution_model, attribution_value, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                attribution['customer_id'], attribution['last_modified'],
                attribution['lead_id'], attribution['campaign_id'],
                attribution['touchpoint_order'], attribution['touchpoint_time'],
                attribution['attribution_model'], attribution['attribution_value'],
                attribution['created_at']
            ))
        conn.commit()
        logger.info(f"Inserted {len(attributions)} attribution records")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert attributions: {e}")
        raise
    finally:
        cursor.close()

def generate_and_insert_mock_data():
    """Generate and insert mock data for all tables."""
    conn = get_db_connection()
    try:
        logger.info("Starting mock data generation...")
        
        # Generate mock data
        leads = generate_mock_leads(100)
        campaigns = generate_mock_campaigns(100)
        attributions = generate_mock_attribution(100)
        
        # Insert data
        insert_leads(conn, leads)
        insert_campaigns(conn, campaigns)
        insert_attributions(conn, attributions)
        
        logger.info("Mock data generation completed successfully")
        
    except Exception as e:
        logger.error(f"Error during mock data generation: {e}")
        raise
    finally:
        conn.close()

def main():
    """Main function to run the mock data generator continuously."""
    logger.info("Starting mock data generator service...")
    
    while True:
        try:
            generate_and_insert_mock_data()
            logger.info("Waiting 5 minutes before next generation...")
            time.sleep(300)  # 5 minutes
        except KeyboardInterrupt:
            logger.info("Mock data generator stopped by user")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            logger.info("Retrying in 1 minute...")
            time.sleep(60)

if __name__ == "__main__":
    main() 