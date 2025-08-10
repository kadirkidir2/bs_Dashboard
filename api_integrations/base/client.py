"""
Base API client class that all platform-specific clients will inherit from
"""
import requests
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class BaseAPIClient(ABC):
    """Base API client class with common functionality"""
    
    def __init__(self, platform_name: str, credentials: Dict[str, Any]):
        """
        Initialize the base API client
        
        Args:
            platform_name: Name of the platform (e.g., 'shopify', 'meta')
            credentials: Dictionary containing API credentials
        """
        self.platform_name = platform_name
        self.credentials = credentials
        self.logger = logging.getLogger(f"api.{platform_name}")
        self.base_url = None
        self.headers = {}
        self.session = requests.Session()
        
    def make_request(self, endpoint: str, method: str = "GET", params: Dict = None, 
                    data: Dict = None, headers: Dict = None, retry_count: int = 3) -> Dict:
        """
        Make an API request with error handling and retries
        
        Args:
            endpoint: API endpoint to call
            method: HTTP method (GET, POST, PUT, DELETE)
            params: URL parameters
            data: Request body for POST/PUT requests
            headers: Additional headers to include
            retry_count: Number of retry attempts for failed requests
            
        Returns:
            API response as dictionary
        """
        if not self.base_url:
            raise ValueError("Base URL is not set. Initialize the client properly.")
            
        url = f"{self.base_url}/{endpoint}" if not endpoint.startswith('http') else endpoint
        request_headers = {**self.headers}
        if headers:
            request_headers.update(headers)
            
        self.logger.debug(f"Making {method} request to {url}")
        
        for attempt in range(retry_count):
            try:
                if method == "GET":
                    response = self.session.get(url, params=params, headers=request_headers)
                elif method == "POST":
                    response = self.session.post(url, params=params, json=data, headers=request_headers)
                elif method == "PUT":
                    response = self.session.put(url, params=params, json=data, headers=request_headers)
                elif method == "DELETE":
                    response = self.session.delete(url, params=params, headers=request_headers)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                # Check for rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 1))
                    self.logger.warning(f"Rate limited. Waiting {retry_after} seconds before retry.")
                    import time
                    time.sleep(retry_after)
                    continue
                    
                # Raise for other HTTP errors
                response.raise_for_status()
                
                # Return JSON response if available
                if response.content:
                    try:
                        return response.json()
                    except ValueError:
                        return {"raw_content": response.text}
                return {}
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed (attempt {attempt+1}/{retry_count}): {str(e)}")
                if attempt == retry_count - 1:
                    self.logger.error(f"All retry attempts failed for {url}")
                    raise
                import time
                time.sleep(2 ** attempt)  # Exponential backoff
    
    @abstractmethod
    def validate_credentials(self) -> bool:
        """
        Validate that the provided credentials are valid
        
        Returns:
            True if credentials are valid, False otherwise
        """
        pass
    
    @abstractmethod
    def get_account_info(self) -> Dict[str, Any]:
        """
        Get basic account information to verify API access
        
        Returns:
            Dictionary with account information
        """
        pass
    
    @abstractmethod
    def collect_metrics(self, start_date: Optional[datetime] = None, 
                       end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Collect metrics from the platform
        
        Args:
            start_date: Start date for data collection
            end_date: End date for data collection
            
        Returns:
            Dictionary with collected metrics
        """
        pass
    
    def get_date_range(self, days: int = 30) -> tuple:
        """
        Get start and end dates for a given time range
        
        Args:
            days: Number of days to look back
            
        Returns:
            Tuple of (start_date, end_date) as datetime objects
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        return start_date, end_date
    
    def format_date(self, date: datetime, format_str: str = "%Y-%m-%d") -> str:
        """
        Format a datetime object as string
        
        Args:
            date: Datetime object
            format_str: Date format string
            
        Returns:
            Formatted date string
        """
        return date.strftime(format_str)