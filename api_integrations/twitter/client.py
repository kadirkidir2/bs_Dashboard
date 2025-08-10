"""
Twitter (X) API client implementation
"""
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import base64
import hmac
import hashlib
import time
import urllib.parse
from ..base.client import BaseAPIClient

class TwitterClient(BaseAPIClient):
    """Twitter API client for interacting with the Twitter API v2"""
    
    def __init__(self, credentials: Dict[str, Any]):
        """
        Initialize the Twitter API client
        
        Args:
            credentials: Dictionary containing 'api_key', 'api_secret', 'access_token', 'access_token_secret'
                         and optionally 'bearer_token'
        """
        super().__init__('twitter', credentials)
        
        # Validate required credentials
        required_keys = ['api_key', 'api_secret', 'access_token', 'access_token_secret']
        for key in required_keys:
            if key not in credentials:
                raise ValueError(f"Missing required credential: {key}")
        
        self.api_key = credentials['api_key']
        self.api_secret = credentials['api_secret']
        self.access_token = credentials['access_token']
        self.access_token_secret = credentials['access_token_secret']
        self.bearer_token = credentials.get('bearer_token')
        
        # Set up API connection details
        self.base_url = "https://api.twitter.com/2"
        self.headers = {}
        
        # Use bearer token if available, otherwise use OAuth 1.0a
        if self.bearer_token:
            self.headers["Authorization"] = f"Bearer {self.bearer_token}"
        
    def _generate_oauth_signature(self, method: str, url: str, params: Dict[str, Any]) -> str:
        """
        Generate OAuth 1.0a signature
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            params: Request parameters
            
        Returns:
            OAuth signature
        """
        # Create parameter string
        param_string = "&".join([f"{urllib.parse.quote(k)}={urllib.parse.quote(str(v))}" for k, v in sorted(params.items())])
        
        # Create signature base string
        base_string = f"{method}&{urllib.parse.quote(url)}&{urllib.parse.quote(param_string)}"
        
        # Create signing key
        signing_key = f"{urllib.parse.quote(self.api_secret)}&{urllib.parse.quote(self.access_token_secret)}"
        
        # Generate signature
        signature = base64.b64encode(
            hmac.new(
                signing_key.encode(),
                base_string.encode(),
                hashlib.sha1
            ).digest()
        ).decode()
        
        return signature
    
    def _get_oauth_header(self, method: str, url: str, params: Dict[str, Any]) -> Dict[str, str]:
        """
        Get OAuth 1.0a header
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            params: Request parameters
            
        Returns:
            OAuth header dictionary
        """
        oauth_params = {
            "oauth_consumer_key": self.api_key,
            "oauth_nonce": hashlib.md5(str(time.time()).encode()).hexdigest(),
            "oauth_signature_method": "HMAC-SHA1",
            "oauth_timestamp": str(int(time.time())),
            "oauth_token": self.access_token,
            "oauth_version": "1.0"
        }
        
        # Combine with request params for signature
        all_params = {**params, **oauth_params}
        
        # Generate signature
        oauth_params["oauth_signature"] = self._generate_oauth_signature(method, url, all_params)
        
        # Create header string
        header_string = "OAuth " + ", ".join([f'{urllib.parse.quote(k)}="{urllib.parse.quote(v)}"' for k, v in oauth_params.items()])
        
        return {"Authorization": header_string}
    
    def make_oauth_request(self, endpoint: str, method: str = "GET", params: Dict = None, 
                          data: Dict = None, retry_count: int = 3) -> Dict:
        """
        Make a request with OAuth 1.0a authentication
        
        Args:
            endpoint: API endpoint to call
            method: HTTP method (GET, POST, etc.)
            params: URL parameters
            data: Request body for POST/PUT requests
            retry_count: Number of retry attempts for failed requests
            
        Returns:
            API response as dictionary
        """
        if params is None:
            params = {}
            
        url = f"{self.base_url}/{endpoint}"
        
        # Get OAuth header
        oauth_header = self._get_oauth_header(method, url, params)
        
        # Make request with OAuth header
        return self.make_request(endpoint, method, params, data, oauth_header, retry_count)
    
    def validate_credentials(self) -> bool:
        """
        Validate that the provided credentials are valid
        
        Returns:
            True if credentials are valid, False otherwise
        """
        try:
            # Try to get user info to validate credentials
            user_info = self.get_user_info()
            return bool(user_info and 'data' in user_info)
        except Exception as e:
            self.logger.error(f"Credential validation failed: {str(e)}")
            return False
    
    def get_account_info(self) -> Dict[str, Any]:
        """
        Get basic account information to verify API access
        
        Returns:
            Dictionary with account information
        """
        return self.get_user_info()
    
    def get_user_info(self) -> Dict[str, Any]:
        """
        Get authenticated user information
        
        Returns:
            Dictionary with user details
        """
        if self.bearer_token:
            # Use bearer token
            endpoint = "users/me"
            params = {
                "user.fields": "id,name,username,created_at,description,public_metrics"
            }
            return self.make_request(endpoint, params=params)
        else:
            # Use OAuth 1.0a
            endpoint = "users/me"
            params = {
                "user.fields": "id,name,username,created_at,description,public_metrics"
            }
            return self.make_oauth_request(endpoint, params=params)
    
    def get_user_tweets(self, user_id: str, max_results: int = 100) -> Dict[str, Any]:
        """
        Get tweets for a user
        
        Args:
            user_id: Twitter user ID
            max_results: Maximum number of results to return
            
        Returns:
            Dictionary with tweets
        """
        endpoint = f"users/{user_id}/tweets"
        params = {
            "max_results": max_results,
            "tweet.fields": "id,text,created_at,public_metrics,entities"
        }
        
        if self.bearer_token:
            return self.make_request(endpoint, params=params)
        else:
            return self.make_oauth_request(endpoint, params=params)
    
    def get_user_followers(self, user_id: str, max_results: int = 100) -> Dict[str, Any]:
        """
        Get followers for a user
        
        Args:
            user_id: Twitter user ID
            max_results: Maximum number of results to return
            
        Returns:
            Dictionary with followers
        """
        endpoint = f"users/{user_id}/followers"
        params = {
            "max_results": max_results,
            "user.fields": "id,name,username,public_metrics"
        }
        
        if self.bearer_token:
            return self.make_request(endpoint, params=params)
        else:
            return self.make_oauth_request(endpoint, params=params)
    
    def get_user_following(self, user_id: str, max_results: int = 100) -> Dict[str, Any]:
        """
        Get accounts a user is following
        
        Args:
            user_id: Twitter user ID
            max_results: Maximum number of results to return
            
        Returns:
            Dictionary with following accounts
        """
        endpoint = f"users/{user_id}/following"
        params = {
            "max_results": max_results,
            "user.fields": "id,name,username,public_metrics"
        }
        
        if self.bearer_token:
            return self.make_request(endpoint, params=params)
        else:
            return self.make_oauth_request(endpoint, params=params)
    
    def collect_metrics(self, start_date: Optional[datetime] = None, 
                       end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Collect metrics from Twitter
        
        Args:
            start_date: Start date for data collection (not used for Twitter API)
            end_date: End date for data collection (not used for Twitter API)
            
        Returns:
            Dictionary with collected metrics
        """
        self.logger.info("Collecting Twitter metrics")
        
        # Get user info
        user_info = self.get_user_info()
        
        if not user_info or 'data' not in user_info:
            self.logger.error("Failed to get user info")
            return {}
            
        user_id = user_info['data']['id']
        
        # Get user tweets
        tweets = self.get_user_tweets(user_id)
        
        # Get user followers
        followers = self.get_user_followers(user_id)
        
        # Get user following
        following = self.get_user_following(user_id)
        
        # Compile all data
        return {
            "user_info": user_info,
            "tweets": tweets,
            "followers": followers,
            "following": following,
            "collected_at": datetime.now().isoformat()
        }