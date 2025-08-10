"""
Data collection scheduler for API integrations
"""
import os
import json
import logging
import time
import schedule
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session

from db import SessionLocal
from api_integrations.base.credentials import CredentialsManager
from api_integrations.shopify import ShopifyClient, ShopifyDataProcessor
from api_integrations.meta import MetaClient, MetaDataProcessor
from api_integrations.google import GoogleAnalyticsClient, GoogleAnalyticsDataProcessor
from api_integrations.tiktok import TikTokAdsClient, TikTokAdsDataProcessor
from api_integrations.twitter import TwitterClient, TwitterDataProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_collector.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("data_collector")

# Initialize credentials manager
CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "credentials", "api_credentials.enc")
os.makedirs(os.path.dirname(CREDENTIALS_PATH), exist_ok=True)

# Use environment variable for secret key or default to a secure value
SECRET_KEY = os.environ.get("API_CREDENTIALS_KEY", "change_this_in_production_environment")
credentials_manager = CredentialsManager(CREDENTIALS_PATH, SECRET_KEY)

def get_db_session() -> Session:
    """Get a database session"""
    return SessionLocal()

def collect_shopify_data():
    """Collect data from Shopify API"""
    logger.info("Starting Shopify data collection")
    
    try:
        # Get credentials
        credentials = credentials_manager.load_credentials("shopify")
        if not credentials:
            logger.warning("No Shopify credentials found")
            return
        
        # Initialize client
        client = ShopifyClient(credentials)
        
        # Validate credentials
        if not client.validate_credentials():
            logger.error("Invalid Shopify credentials")
            return
        
        # Get database session
        db_session = get_db_session()
        
        try:
            # Initialize data processor
            processor = ShopifyDataProcessor(db_session)
            
            # Process data
            success = processor.process(client)
            
            if success:
                logger.info("Shopify data collection completed successfully")
            else:
                logger.error("Shopify data collection failed")
        finally:
            db_session.close()
            
    except Exception as e:
        logger.exception(f"Error collecting Shopify data: {str(e)}")

def collect_meta_data():
    """Collect data from Meta (Facebook/Instagram) API"""
    logger.info("Starting Meta data collection")
    
    try:
        # Get credentials
        credentials = credentials_manager.load_credentials("meta")
        if not credentials:
            logger.warning("No Meta credentials found")
            return
        
        # Initialize client
        client = MetaClient(credentials)
        
        # Validate credentials
        if not client.validate_credentials():
            logger.error("Invalid Meta credentials")
            return
        
        # Get database session
        db_session = get_db_session()
        
        try:
            # Initialize data processor
            processor = MetaDataProcessor(db_session)
            
            # Process data
            success = processor.process(client)
            
            if success:
                logger.info("Meta data collection completed successfully")
            else:
                logger.error("Meta data collection failed")
        finally:
            db_session.close()
            
    except Exception as e:
        logger.exception(f"Error collecting Meta data: {str(e)}")

def collect_google_analytics_data():
    """Collect data from Google Analytics API"""
    logger.info("Starting Google Analytics data collection")
    
    try:
        # Get credentials
        credentials = credentials_manager.load_credentials("google_analytics")
        if not credentials:
            logger.warning("No Google Analytics credentials found")
            return
        
        # Initialize client
        client = GoogleAnalyticsClient(credentials)
        
        # Validate credentials
        if not client.validate_credentials():
            logger.error("Invalid Google Analytics credentials")
            return
        
        # Get database session
        db_session = get_db_session()
        
        try:
            # Initialize data processor
            processor = GoogleAnalyticsDataProcessor(db_session)
            
            # Process data
            success = processor.process(client)
            
            if success:
                logger.info("Google Analytics data collection completed successfully")
            else:
                logger.error("Google Analytics data collection failed")
        finally:
            db_session.close()
            
    except Exception as e:
        logger.exception(f"Error collecting Google Analytics data: {str(e)}")

def collect_tiktok_ads_data():
    """Collect data from TikTok Ads API"""
    logger.info("Starting TikTok Ads data collection")
    
    try:
        # Get credentials
        credentials = credentials_manager.load_credentials("tiktok_ads")
        if not credentials:
            logger.warning("No TikTok Ads credentials found")
            return
        
        # Initialize client
        client = TikTokAdsClient(credentials)
        
        # Validate credentials
        if not client.validate_credentials():
            logger.error("Invalid TikTok Ads credentials")
            return
        
        # Get database session
        db_session = get_db_session()
        
        try:
            # Initialize data processor
            processor = TikTokAdsDataProcessor(db_session)
            
            # Process data
            success = processor.process(client)
            
            if success:
                logger.info("TikTok Ads data collection completed successfully")
            else:
                logger.error("TikTok Ads data collection failed")
        finally:
            db_session.close()
            
    except Exception as e:
        logger.exception(f"Error collecting TikTok Ads data: {str(e)}")

def collect_twitter_data():
    """Collect data from Twitter API"""
    logger.info("Starting Twitter data collection")
    
    try:
        # Get credentials
        credentials = credentials_manager.load_credentials("twitter")
        if not credentials:
            logger.warning("No Twitter credentials found")
            return
        
        # Initialize client
        client = TwitterClient(credentials)
        
        # Validate credentials
        if not client.validate_credentials():
            logger.error("Invalid Twitter credentials")
            return
        
        # Get database session
        db_session = get_db_session()
        
        try:
            # Initialize data processor
            processor = TwitterDataProcessor(db_session)
            
            # Process data
            success = processor.process(client)
            
            if success:
                logger.info("Twitter data collection completed successfully")
            else:
                logger.error("Twitter data collection failed")
        finally:
            db_session.close()
            
    except Exception as e:
        logger.exception(f"Error collecting Twitter data: {str(e)}")

def collect_all_data():
    """Collect data from all configured APIs"""
    logger.info("Starting data collection for all platforms")
    
    # Get all platforms with credentials
    platforms = credentials_manager.list_platforms()
    logger.info(f"Found credentials for platforms: {platforms}")
    
    # Collect data for each platform
    if "shopify" in platforms:
        collect_shopify_data()
    
    if "meta" in platforms:
        collect_meta_data()
    
    if "google_analytics" in platforms:
        collect_google_analytics_data()
    
    if "tiktok_ads" in platforms:
        collect_tiktok_ads_data()
    
    if "twitter" in platforms:
        collect_twitter_data()
    
    logger.info("Completed data collection for all platforms")

def run_scheduler():
    """Run the scheduler in a separate thread"""
    while True:
        schedule.run_pending()
        time.sleep(1)

def start_scheduler():
    """Start the scheduler"""
    # Schedule data collection
    schedule.every().day.at("01:00").do(collect_all_data)  # Run daily at 1 AM
    
    # Also schedule individual platform collections at different times
    schedule.every().day.at("02:00").do(collect_shopify_data)
    schedule.every().day.at("03:00").do(collect_meta_data)
    schedule.every().day.at("04:00").do(collect_google_analytics_data)
    schedule.every().day.at("05:00").do(collect_tiktok_ads_data)
    schedule.every().day.at("06:00").do(collect_twitter_data)
    
    # Start the scheduler in a separate thread
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()
    
    logger.info("Scheduler started")

if __name__ == "__main__":
    # Collect data immediately on startup
    collect_all_data()
    
    # Start the scheduler
    start_scheduler()
    
    # Keep the main thread alive
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Data collector stopped")