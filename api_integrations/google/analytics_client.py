"""
Google Analytics API client implementation
"""
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import json
import pandas as pd
from ..base.client import BaseAPIClient
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account

class GoogleAnalyticsClient(BaseAPIClient):
    """Google Analytics API client for interacting with the Google Analytics Data API"""
    
    def __init__(self, credentials: Dict[str, Any]):
        """
        Initialize the Google Analytics API client
        
        Args:
            credentials: Dictionary containing 'service_account_info' and 'property_id'
        """
        super().__init__('google_analytics', credentials)
        
        # Validate required credentials
        required_keys = ['service_account_info', 'property_id']
        for key in required_keys:
            if key not in credentials:
                raise ValueError(f"Missing required credential: {key}")
        
        self.service_account_info = credentials['service_account_info']
        self.property_id = credentials['property_id']
        
        # Initialize the Google Analytics client
        self._init_client()
    
    def _init_client(self):
        """Initialize the Google Analytics client"""
        try:
            self.credentials = service_account.Credentials.from_service_account_info(
                self.service_account_info,
                scopes=['https://www.googleapis.com/auth/analytics.readonly']
            )
            self.client = BetaAnalyticsDataClient(credentials=self.credentials)
            self.logger.info("Google Analytics client initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Google Analytics client: {str(e)}")
            raise
    
    def validate_credentials(self) -> bool:
        """
        Validate that the provided credentials are valid
        
        Returns:
            True if credentials are valid, False otherwise
        """
        try:
            # Try to get basic report to validate credentials
            self.get_basic_report(start_date="7daysAgo", end_date="today", row_limit=1)
            return True
        except Exception as e:
            self.logger.error(f"Credential validation failed: {str(e)}")
            return False
    
    def get_account_info(self) -> Dict[str, Any]:
        """
        Get basic account information to verify API access
        
        Returns:
            Dictionary with account information
        """
        # GA4 doesn't have a direct endpoint for account info
        # We'll return property ID as a basic check
        return {"property_id": self.property_id}
    
    def validate_date_format(self, date_str: str) -> bool:
        """
        Validate date format for GA4 API
        
        Args:
            date_str: Date string to validate
            
        Returns:
            True if valid, False otherwise
        """
        if date_str in ["yesterday", "today", "7daysAgo", "30daysAgo"]:
            return True
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return True
        except ValueError:
            return False
    
    def get_basic_report(self, start_date: str = "7daysAgo", end_date: str = "today", 
                        row_limit: int = 100) -> pd.DataFrame:
        """
        Get basic report with sessions, users, page views
        
        Args:
            start_date: Start date (YYYY-MM-DD or GA4 relative date)
            end_date: End date (YYYY-MM-DD or GA4 relative date)
            row_limit: Maximum number of rows to return
            
        Returns:
            DataFrame with report data
        """
        if not self.validate_date_format(start_date) or not self.validate_date_format(end_date):
            raise ValueError("Invalid date format")
        
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="country"),
                    Dimension(name="deviceCategory")
                ],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="activeUsers"),
                    Metric(name="screenPageViews"),
                    Metric(name="bounceRate"),
                    Metric(name="averageSessionDuration")
                ],
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                limit=row_limit
            )
            response = self.client.run_report(request=request)
            return self._response_to_dataframe(response)
        except Exception as e:
            self.logger.error(f"Failed to get basic report: {str(e)}")
            return pd.DataFrame()
    
    def get_traffic_sources(self, start_date: str = "7daysAgo", end_date: str = "today", 
                           simple: bool = False, row_limit: int = 100) -> pd.DataFrame:
        """
        Get traffic sources report
        
        Args:
            start_date: Start date (YYYY-MM-DD or GA4 relative date)
            end_date: End date (YYYY-MM-DD or GA4 relative date)
            simple: Use simplified dimensions
            row_limit: Maximum number of rows to return
            
        Returns:
            DataFrame with report data
        """
        if not self.validate_date_format(start_date) or not self.validate_date_format(end_date):
            raise ValueError("Invalid date format")
        
        try:
            dimensions = [
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium")
            ]
            metrics = [
                Metric(name="sessions"),
                Metric(name="activeUsers")
            ]
            if not simple:
                dimensions.append(Dimension(name="sessionCampaignId"))
                metrics.append(Metric(name="totalUsers"))
            
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                dimensions=dimensions,
                metrics=metrics,
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                limit=row_limit
            )
            response = self.client.run_report(request=request)
            return self._response_to_dataframe(response)
        except Exception as e:
            self.logger.error(f"Failed to get traffic sources: {str(e)}")
            return pd.DataFrame()
    
    def get_page_analytics(self, start_date: str = "7daysAgo", end_date: str = "today", 
                          row_limit: int = 100) -> pd.DataFrame:
        """
        Get page analytics report
        
        Args:
            start_date: Start date (YYYY-MM-DD or GA4 relative date)
            end_date: End date (YYYY-MM-DD or GA4 relative date)
            row_limit: Maximum number of rows to return
            
        Returns:
            DataFrame with report data
        """
        if not self.validate_date_format(start_date) or not self.validate_date_format(end_date):
            raise ValueError("Invalid date format")
        
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                dimensions=[
                    Dimension(name="pagePath"),
                    Dimension(name="pageTitle")
                ],
                metrics=[
                    Metric(name="screenPageViews"),
                    Metric(name="userEngagementDuration"),
                    Metric(name="bounceRate")
                ],
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                limit=row_limit
            )
            response = self.client.run_report(request=request)
            return self._response_to_dataframe(response)
        except Exception as e:
            self.logger.error(f"Failed to get page analytics: {str(e)}")
            return pd.DataFrame()
    
    def get_realtime_data(self, row_limit: int = 100) -> pd.DataFrame:
        """
        Get realtime data
        
        Args:
            row_limit: Maximum number of rows to return
            
        Returns:
            DataFrame with realtime data
        """
        from google.analytics.data_v1beta.types import RunRealtimeReportRequest
        
        try:
            request = RunRealtimeReportRequest(
                property=f"properties/{self.property_id}",
                dimensions=[
                    Dimension(name="country"),
                    Dimension(name="deviceCategory")
                ],
                metrics=[
                    Metric(name="activeUsers")
                ],
                limit=row_limit
            )
            response = self.client.run_realtime_report(request=request)
            return self._response_to_dataframe(response)
        except Exception as e:
            self.logger.error(f"Failed to get realtime data: {str(e)}")
            return pd.DataFrame()
    
    def _response_to_dataframe(self, response) -> pd.DataFrame:
        """
        Convert API response to pandas DataFrame
        
        Args:
            response: GA4 API response
            
        Returns:
            DataFrame with response data
        """
        if not response or not response.rows or not response.dimension_headers or not response.metric_headers:
            return pd.DataFrame()
        
        dimension_names = [dim.name for dim in response.dimension_headers]
        metric_names = [metric.name for metric in response.metric_headers]
        
        data = []
        for row in response.rows:
            row_data = {}
            for i, dim_value in enumerate(row.dimension_values):
                row_data[dimension_names[i]] = dim_value.value
            
            for i, metric_value in enumerate(row.metric_values):
                value = metric_value.value
                if metric_names[i] in ["sessions", "activeUsers", "screenPageViews", "totalUsers", "userEngagementDuration"]:
                    row_data[metric_names[i]] = int(value) if value else 0
                elif metric_names[i] in ["bounceRate", "averageSessionDuration"]:
                    row_data[metric_names[i]] = float(value) if value else 0.0
                else:
                    row_data[metric_names[i]] = value
            
            data.append(row_data)
        
        return pd.DataFrame(data)
    
    def collect_metrics(self, start_date: Optional[datetime] = None, 
                       end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Collect metrics from Google Analytics
        
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
            
        # Format dates for GA4 API
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        self.logger.info(f"Collecting Google Analytics metrics from {start_date_str} to {end_date_str}")
        
        # Get basic report
        basic_report = self.get_basic_report(start_date=start_date_str, end_date=end_date_str)
        
        # Get traffic sources
        traffic_sources = self.get_traffic_sources(start_date=start_date_str, end_date=end_date_str)
        
        # Get page analytics
        page_analytics = self.get_page_analytics(start_date=start_date_str, end_date=end_date_str)
        
        # Get realtime data
        realtime_data = self.get_realtime_data()
        
        # Convert DataFrames to dictionaries
        return {
            "basic_report": basic_report.to_dict(orient="records") if not basic_report.empty else [],
            "traffic_sources": traffic_sources.to_dict(orient="records") if not traffic_sources.empty else [],
            "page_analytics": page_analytics.to_dict(orient="records") if not page_analytics.empty else [],
            "realtime_data": realtime_data.to_dict(orient="records") if not realtime_data.empty else [],
            "collected_at": datetime.now().isoformat(),
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            }
        }