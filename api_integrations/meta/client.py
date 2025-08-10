"""
Meta API client implementation for Facebook and Instagram
"""
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from ..base.client import BaseAPIClient

class MetaClient(BaseAPIClient):
    """Meta API client for interacting with Facebook and Instagram APIs"""
    
    def __init__(self, credentials: Dict[str, Any]):
        """
        Initialize the Meta API client
        
        Args:
            credentials: Dictionary containing 'access_token'
        """
        super().__init__('meta', credentials)
        
        # Validate required credentials
        if 'access_token' not in credentials:
            raise ValueError("Missing required credential: access_token")
        
        self.access_token = credentials['access_token']
        self.page_id = credentials.get('page_id')  # Optional, can be discovered
        
        # Set up API connection details
        self.base_url = "https://graph.facebook.com/v18.0"
        self.headers = {
            "Content-Type": "application/json"
        }
    
    def validate_credentials(self) -> bool:
        """
        Validate that the provided credentials are valid
        
        Returns:
            True if credentials are valid, False otherwise
        """
        try:
            # Try to get basic account info
            me_data = self.make_request("me", params={'fields': 'id,name', 'access_token': self.access_token})
            return bool(me_data and 'id' in me_data)
        except Exception as e:
            self.logger.error(f"Credential validation failed: {str(e)}")
            return False
    
    def get_account_info(self) -> Dict[str, Any]:
        """
        Get basic account information to verify API access
        
        Returns:
            Dictionary with account information
        """
        return self.make_request("me", params={'fields': 'id,name', 'access_token': self.access_token})
    
    def get_page_id(self) -> Optional[str]:
        """
        Get page ID from token
        
        Returns:
            Page ID if found, None otherwise
        """
        if self.page_id:
            return self.page_id
            
        # Try to get page ID from me endpoint
        me_data = self.make_request("me", params={'fields': 'id,name', 'access_token': self.access_token})
        if me_data and 'id' in me_data:
            self.page_id = me_data['id']
            self.logger.info(f"Found page ID: {self.page_id} ({me_data.get('name', 'N/A')})")
            return self.page_id
        
        # If me doesn't work, try accounts endpoint
        accounts_data = self.make_request("me/accounts", 
                                         params={'fields': 'id,name,access_token', 'access_token': self.access_token})
        
        if accounts_data and 'data' in accounts_data and accounts_data['data']:
            first_page = accounts_data['data'][0]
            self.page_id = first_page['id']
            self.logger.info(f"Selected first page: {first_page.get('name', 'N/A')} (ID: {self.page_id})")
            
            # Update access token if page token is available
            if 'access_token' in first_page:
                self.access_token = first_page['access_token']
                self.logger.info("Using page access token")
                
            return self.page_id
            
        self.logger.error("Could not find page ID")
        return None
    
    def get_page_info(self) -> Dict[str, Any]:
        """
        Get page information
        
        Returns:
            Dictionary with page details
        """
        page_id = self.get_page_id()
        if not page_id:
            return {}
            
        fields = 'name,followers_count,fan_count,engagement,about,website,category,link,picture'
        return self.make_request(page_id, params={'fields': fields, 'access_token': self.access_token})
    
    def get_instagram_account(self) -> Dict[str, Any]:
        """
        Get connected Instagram account
        
        Returns:
            Dictionary with Instagram account info
        """
        page_id = self.get_page_id()
        if not page_id:
            return {}
            
        return self.make_request(page_id, params={'fields': 'instagram_business_account', 'access_token': self.access_token})
    
    def get_recent_posts(self, limit: int = 25) -> Dict[str, Any]:
        """
        Get recent posts
        
        Args:
            limit: Maximum number of posts
            
        Returns:
            Dictionary with posts data
        """
        page_id = self.get_page_id()
        if not page_id:
            return {}
            
        fields = 'message,created_time,likes.summary(true),comments.summary(true),shares,reactions.summary(true)'
        return self.make_request(f"{page_id}/posts", 
                               params={'fields': fields, 'limit': limit, 'access_token': self.access_token})
    
    def get_page_insights(self, days: int = 30) -> Dict[str, Any]:
        """
        Get page insights
        
        Args:
            days: Number of days to look back
            
        Returns:
            Dictionary with insights data
        """
        page_id = self.get_page_id()
        if not page_id:
            return {}
            
        since = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        until = datetime.now().strftime('%Y-%m-%d')
        
        metrics = [
            'page_impressions',
            'page_reach',
            'page_engaged_users',
            'page_post_engagements',
            'page_fans',
            'page_fan_adds',
            'page_fan_removes'
        ]
        
        return self.make_request(f"{page_id}/insights", 
                               params={
                                   'metric': ','.join(metrics),
                                   'period': 'day',
                                   'since': since,
                                   'until': until,
                                   'access_token': self.access_token
                               })
    
    def get_instagram_media(self, instagram_account_id: str, limit: int = 25) -> Dict[str, Any]:
        """
        Get Instagram media
        
        Args:
            instagram_account_id: Instagram account ID
            limit: Maximum number of media items
            
        Returns:
            Dictionary with media data
        """
        fields = 'id,caption,media_type,media_url,permalink,timestamp,like_count,comments_count'
        return self.make_request(f"{instagram_account_id}/media", 
                               params={'fields': fields, 'limit': limit, 'access_token': self.access_token})
    
    def get_instagram_insights(self, instagram_account_id: str, days: int = 30) -> Dict[str, Any]:
        """
        Get Instagram insights
        
        Args:
            instagram_account_id: Instagram account ID
            days: Number of days to look back
            
        Returns:
            Dictionary with insights data
        """
        since = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        until = datetime.now().strftime('%Y-%m-%d')
        
        metrics = [
            'impressions',
            'reach',
            'profile_views',
            'follower_count'
        ]
        
        return self.make_request(f"{instagram_account_id}/insights", 
                               params={
                                   'metric': ','.join(metrics),
                                   'period': 'day',
                                   'since': since,
                                   'until': until,
                                   'access_token': self.access_token
                               })
    
    def collect_metrics(self, start_date: Optional[datetime] = None, 
                       end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Collect metrics from Meta platforms
        
        Args:
            start_date: Start date for data collection
            end_date: End date for data collection
            
        Returns:
            Dictionary with collected metrics
        """
        if not start_date:
            start_date = datetime.now() - timedelta(days=30)
        if not end_date:
            end_date = datetime.now()
            
        days = (end_date - start_date).days
        self.logger.info(f"Collecting Meta metrics for the past {days} days")
        
        # Find page ID
        page_id = self.get_page_id()
        if not page_id:
            self.logger.error("Could not find page ID, aborting metrics collection")
            return {}
            
        # Get page information
        page_info = self.get_page_info()
        
        # Get recent posts
        posts_data = self.get_recent_posts()
        
        # Get page insights
        insights_data = self.get_page_insights(days=days)
        
        # Check for Instagram account
        instagram_data = {}
        instagram_info = self.get_instagram_account()
        
        if instagram_info and 'instagram_business_account' in instagram_info:
            instagram_account_id = instagram_info['instagram_business_account']['id']
            self.logger.info(f"Found Instagram account: {instagram_account_id}")
            
            # Get Instagram media
            instagram_data['media'] = self.get_instagram_media(instagram_account_id)
            
            # Get Instagram insights
            instagram_data['insights'] = self.get_instagram_insights(instagram_account_id, days=days)
        
        # Compile all data
        return {
            "page_info": page_info,
            "posts": posts_data,
            "insights": insights_data,
            "instagram": instagram_data,
            "collected_at": datetime.now().isoformat(),
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            }
        }