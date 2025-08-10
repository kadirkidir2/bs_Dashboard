"""
TikTok Ads API client implementation
"""
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import hashlib
import time
import hmac
from ..base.client import BaseAPIClient

class TikTokAdsClient(BaseAPIClient):
    """TikTok Ads API client for interacting with the TikTok Ads API"""
    
    def __init__(self, credentials: Dict[str, Any]):
        """
        Initialize the TikTok Ads API client
        
        Args:
            credentials: Dictionary containing 'app_id', 'secret', 'access_token', and 'advertiser_id'
        """
        super().__init__('tiktok_ads', credentials)
        
        # Validate required credentials
        required_keys = ['app_id', 'secret', 'access_token', 'advertiser_id']
        for key in required_keys:
            if key not in credentials:
                raise ValueError(f"Missing required credential: {key}")
        
        self.app_id = credentials['app_id']
        self.secret = credentials['secret']
        self.access_token = credentials['access_token']
        self.advertiser_id = credentials['advertiser_id']
        
        # Set up API connection details
        self.base_url = "https://business-api.tiktok.com/open_api/v1.3"
        self.headers = {
            "Access-Token": self.access_token,
            "Content-Type": "application/json"
        }
    
    def _generate_signature(self, http_method: str, url: str, timestamp: int, params: Dict[str, Any]) -> str:
        """
        Generate signature for TikTok API requests
        
        Args:
            http_method: HTTP method (GET, POST, etc.)
            url: Request URL
            timestamp: Current timestamp
            params: Request parameters
            
        Returns:
            Signature string
        """
        # Sort parameters by key
        sorted_params = sorted(params.items(), key=lambda x: x[0])
        
        # Create parameter string
        param_str = ""
        for key, value in sorted_params:
            param_str += f"{key}={value}&"
        param_str = param_str.rstrip("&")
        
        # Create signature string
        signature_str = f"{http_method}\n{url}\n{timestamp}\n{param_str}"
        
        # Generate HMAC-SHA256 signature
        signature = hmac.new(
            self.secret.encode(),
            signature_str.encode(),
            hashlib.sha256
        ).hexdigest()
        
        return signature
    
    def validate_credentials(self) -> bool:
        """
        Validate that the provided credentials are valid
        
        Returns:
            True if credentials are valid, False otherwise
        """
        try:
            # Try to get advertiser info to validate credentials
            advertiser_info = self.get_advertiser_info()
            return bool(advertiser_info and 'data' in advertiser_info)
        except Exception as e:
            self.logger.error(f"Credential validation failed: {str(e)}")
            return False
    
    def get_account_info(self) -> Dict[str, Any]:
        """
        Get basic account information to verify API access
        
        Returns:
            Dictionary with account information
        """
        return self.get_advertiser_info()
    
    def get_advertiser_info(self) -> Dict[str, Any]:
        """
        Get advertiser information
        
        Returns:
            Dictionary with advertiser details
        """
        endpoint = f"advertiser/info/"
        params = {
            "advertiser_ids": f"[{self.advertiser_id}]"
        }
        return self.make_request(endpoint, params=params)
    
    def get_campaign_list(self, page_size: int = 100, page: int = 1) -> Dict[str, Any]:
        """
        Get list of campaigns
        
        Args:
            page_size: Number of items per page
            page: Page number
            
        Returns:
            Dictionary with campaign list
        """
        endpoint = f"campaign/get/"
        params = {
            "advertiser_id": self.advertiser_id,
            "page_size": page_size,
            "page": page
        }
        return self.make_request(endpoint, params=params)
    
    def get_ad_list(self, page_size: int = 100, page: int = 1) -> Dict[str, Any]:
        """
        Get list of ads
        
        Args:
            page_size: Number of items per page
            page: Page number
            
        Returns:
            Dictionary with ad list
        """
        endpoint = f"ad/get/"
        params = {
            "advertiser_id": self.advertiser_id,
            "page_size": page_size,
            "page": page
        }
        return self.make_request(endpoint, params=params)
    
    def get_ad_reports(self, start_date: str, end_date: str, metrics: List[str]) -> Dict[str, Any]:
        """
        Get ad reports
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            metrics: List of metrics to retrieve
            
        Returns:
            Dictionary with ad reports
        """
        endpoint = f"report/integrated/get/"
        params = {
            "advertiser_id": self.advertiser_id,
            "report_type": "BASIC",
            "dimensions": '["ad_id"]',
            "metrics": json.dumps(metrics),
            "start_date": start_date,
            "end_date": end_date
        }
        return self.make_request(endpoint, params=params)
    
    def collect_metrics(self, start_date: Optional[datetime] = None, 
                       end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Collect metrics from TikTok Ads
        
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
            
        # Format dates for TikTok API
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        self.logger.info(f"Collecting TikTok Ads metrics from {start_date_str} to {end_date_str}")
        
        # Get advertiser info
        advertiser_info = self.get_advertiser_info()
        
        # Get campaigns
        campaigns = self.get_campaign_list()
        
        # Get ads
        ads = self.get_ad_list()
        
        # Get ad reports
        metrics = [
            "spend", "impressions", "clicks", "conversion", 
            "cost_per_conversion", "ctr", "cpc", "reach"
        ]
        reports = self.get_ad_reports(start_date_str, end_date_str, metrics)
        
        # Compile all data
        return {
            "advertiser_info": advertiser_info,
            "campaigns": campaigns,
            "ads": ads,
            "reports": reports,
            "collected_at": datetime.now().isoformat(),
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            }
        }