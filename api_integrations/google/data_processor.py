"""
Google Analytics data processor for ETL operations
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import Session
from ..base.data_processor import BaseDataProcessor
from .analytics_client import GoogleAnalyticsClient
from db.models import (
    WebsitePerformanceMetric,
    CustomerAnalyticsData
)

class GoogleAnalyticsDataProcessor(BaseDataProcessor):
    """Data processor for Google Analytics API data"""
    
    def __init__(self, db_session: Session):
        """
        Initialize the Google Analytics data processor
        
        Args:
            db_session: SQLAlchemy database session
        """
        super().__init__('google_analytics', db_session)
    
    def extract(self, api_client: GoogleAnalyticsClient, start_date: Optional[datetime] = None, 
               end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Extract data from the Google Analytics API
        
        Args:
            api_client: Google Analytics API client instance
            start_date: Start date for data extraction
            end_date: End date for data extraction
            
        Returns:
            Raw data from the Google Analytics API
        """
        return api_client.collect_metrics(start_date, end_date)
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw Google Analytics API data into a format suitable for database storage
        
        Args:
            raw_data: Raw data from the Google Analytics API
            
        Returns:
            Transformed data ready for database storage
        """
        transformed_data = {
            "website_metrics": self._transform_website_metrics(raw_data),
            "customer_metrics": self._transform_customer_metrics(raw_data)
        }
        
        # Calculate derived metrics
        transformed_data = self.calculate_derived_metrics(transformed_data)
        
        return transformed_data
    
    def _transform_website_metrics(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform website metrics from raw data"""
        website_metrics = []
        
        # Process basic report
        basic_report = raw_data.get("basic_report", [])
        if basic_report:
            # Convert to DataFrame for easier aggregation
            df = pd.DataFrame(basic_report)
            
            # Calculate totals
            total_sessions = df["sessions"].sum() if "sessions" in df else 0
            total_users = df["activeUsers"].sum() if "activeUsers" in df else 0
            total_pageviews = df["screenPageViews"].sum() if "screenPageViews" in df else 0
            avg_bounce_rate = df["bounceRate"].mean() if "bounceRate" in df else 0
            avg_session_duration = df["averageSessionDuration"].mean() if "averageSessionDuration" in df else 0
            
            # Add metrics
            website_metrics.append({
                "category": "traffic",
                "sub_category": "overview",
                "value": str(total_sessions),
                "unit": "sessions",
                "trend_value": None,
                "trend_unit": None,
                "icon": "chart-line",
                "color": "primary",
                "display_order": 1,
                "status": "normal"
            })
            
            website_metrics.append({
                "category": "traffic",
                "sub_category": "overview",
                "value": str(total_users),
                "unit": "users",
                "trend_value": None,
                "trend_unit": None,
                "icon": "users",
                "color": "info",
                "display_order": 2,
                "status": "normal"
            })
            
            website_metrics.append({
                "category": "traffic",
                "sub_category": "overview",
                "value": str(total_pageviews),
                "unit": "views",
                "trend_value": None,
                "trend_unit": None,
                "icon": "eye",
                "color": "success",
                "display_order": 3,
                "status": "normal"
            })
            
            website_metrics.append({
                "category": "engagement",
                "sub_category": "overview",
                "value": f"{avg_bounce_rate:.2f}",
                "unit": "%",
                "trend_value": None,
                "trend_unit": None,
                "icon": "sign-out-alt",
                "color": "warning",
                "display_order": 4,
                "status": "normal"
            })
            
            website_metrics.append({
                "category": "engagement",
                "sub_category": "overview",
                "value": f"{avg_session_duration:.2f}",
                "unit": "seconds",
                "trend_value": None,
                "trend_unit": None,
                "icon": "clock",
                "color": "info",
                "display_order": 5,
                "status": "normal"
            })
            
            # Device breakdown
            if "deviceCategory" in df.columns:
                device_counts = df.groupby("deviceCategory")["sessions"].sum().reset_index()
                for _, row in device_counts.iterrows():
                    device = row["deviceCategory"]
                    sessions = row["sessions"]
                    percentage = (sessions / total_sessions * 100) if total_sessions > 0 else 0
                    
                    website_metrics.append({
                        "category": "devices",
                        "sub_category": device.lower(),
                        "value": str(sessions),
                        "unit": "sessions",
                        "trend_value": f"{percentage:.1f}",
                        "trend_unit": "%",
                        "icon": "mobile-alt" if device.lower() == "mobile" else ("tablet" if device.lower() == "tablet" else "desktop"),
                        "color": "primary",
                        "display_order": 10,
                        "status": "normal"
                    })
        
        # Process traffic sources
        traffic_sources = raw_data.get("traffic_sources", [])
        if traffic_sources:
            # Convert to DataFrame for easier aggregation
            df = pd.DataFrame(traffic_sources)
            
            # Group by source and medium
            if "sessionSource" in df.columns and "sessionMedium" in df.columns and "sessions" in df.columns:
                source_medium = df.groupby(["sessionSource", "sessionMedium"])["sessions"].sum().reset_index()
                source_medium = source_medium.sort_values("sessions", ascending=False).head(10)
                
                for i, row in enumerate(source_medium.iterrows()):
                    _, data = row
                    source = data["sessionSource"]
                    medium = data["sessionMedium"]
                    sessions = data["sessions"]
                    
                    website_metrics.append({
                        "category": "traffic_sources",
                        "sub_category": f"{source} / {medium}",
                        "value": str(sessions),
                        "unit": "sessions",
                        "trend_value": None,
                        "trend_unit": None,
                        "icon": "link",
                        "color": "info",
                        "display_order": 20 + i,
                        "status": "normal"
                    })
        
        # Process page analytics
        page_analytics = raw_data.get("page_analytics", [])
        if page_analytics:
            # Convert to DataFrame for easier aggregation
            df = pd.DataFrame(page_analytics)
            
            # Top pages by views
            if "pagePath" in df.columns and "screenPageViews" in df.columns:
                top_pages = df.sort_values("screenPageViews", ascending=False).head(10)
                
                for i, row in enumerate(top_pages.iterrows()):
                    _, data = row
                    page_path = data["pagePath"]
                    page_title = data.get("pageTitle", page_path)
                    views = data["screenPageViews"]
                    
                    website_metrics.append({
                        "category": "top_pages",
                        "sub_category": page_title[:50],  # Truncate long titles
                        "value": str(views),
                        "unit": "views",
                        "trend_value": None,
                        "trend_unit": None,
                        "icon": "file",
                        "color": "success",
                        "display_order": 30 + i,
                        "status": "normal"
                    })
        
        return website_metrics
    
    def _transform_customer_metrics(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform customer metrics from raw data"""
        customer_metrics = []
        
        # Process basic report for user metrics
        basic_report = raw_data.get("basic_report", [])
        if basic_report:
            # Convert to DataFrame for easier aggregation
            df = pd.DataFrame(basic_report)
            
            # Calculate totals
            total_users = df["activeUsers"].sum() if "activeUsers" in df else 0
            
            # Add metrics
            customer_metrics.append({
                "type": "top_metrics",
                "name": "Website Visitors",
                "display_value": f"{total_users:,}",
                "numeric_value": total_users,
                "trend_value": None,
                "trend_status": None,
                "color": "primary",
                "icon": "users"
            })
            
            # Country breakdown for segments
            if "country" in df.columns and "activeUsers" in df.columns:
                country_users = df.groupby("country")["activeUsers"].sum().reset_index()
                country_users = country_users.sort_values("activeUsers", ascending=False).head(5)
                
                for _, row in country_users.iterrows():
                    country = row["country"]
                    users = row["activeUsers"]
                    percentage = (users / total_users * 100) if total_users > 0 else 0
                    
                    customer_metrics.append({
                        "type": "segments",
                        "name": country,
                        "display_value": f"{percentage:.1f}%",
                        "numeric_value": percentage,
                        "trend_value": None,
                        "trend_status": None,
                        "color": "info",
                        "icon": "globe"
                    })
            
            # Device breakdown for segments
            if "deviceCategory" in df.columns and "activeUsers" in df.columns:
                device_users = df.groupby("deviceCategory")["activeUsers"].sum().reset_index()
                
                for _, row in device_users.iterrows():
                    device = row["deviceCategory"]
                    users = row["activeUsers"]
                    percentage = (users / total_users * 100) if total_users > 0 else 0
                    
                    customer_metrics.append({
                        "type": "channels",
                        "name": device,
                        "display_value": f"{percentage:.1f}%",
                        "numeric_value": percentage,
                        "trend_value": None,
                        "trend_status": None,
                        "color": "success",
                        "icon": "mobile-alt" if device.lower() == "mobile" else ("tablet" if device.lower() == "tablet" else "desktop")
                    })
        
        # Process traffic sources for channel metrics
        traffic_sources = raw_data.get("traffic_sources", [])
        if traffic_sources:
            # Convert to DataFrame for easier aggregation
            df = pd.DataFrame(traffic_sources)
            
            # Group by medium
            if "sessionMedium" in df.columns and "activeUsers" in df.columns:
                medium_users = df.groupby("sessionMedium")["activeUsers"].sum().reset_index()
                medium_users = medium_users.sort_values("activeUsers", ascending=False).head(5)
                
                total_medium_users = medium_users["activeUsers"].sum()
                
                for _, row in medium_users.iterrows():
                    medium = row["sessionMedium"]
                    users = row["activeUsers"]
                    percentage = (users / total_medium_users * 100) if total_medium_users > 0 else 0
                    
                    customer_metrics.append({
                        "type": "activities",
                        "name": medium,
                        "display_value": f"{percentage:.1f}%",
                        "numeric_value": percentage,
                        "trend_value": None,
                        "trend_status": None,
                        "color": "warning",
                        "icon": "chart-bar"
                    })
        
        return customer_metrics
    
    def load(self, transformed_data: Dict[str, Any]) -> bool:
        """
        Load transformed data into the database
        
        Args:
            transformed_data: Transformed data ready for database storage
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Load website metrics
            for metric in transformed_data.get("website_metrics", []):
                website_metric = WebsitePerformanceMetric(
                    category=metric["category"],
                    sub_category=metric["sub_category"],
                    value=metric["value"],
                    unit=metric["unit"],
                    trend_value=metric["trend_value"],
                    trend_unit=metric["trend_unit"],
                    icon=metric["icon"],
                    color=metric["color"],
                    display_order=metric["display_order"],
                    status=metric["status"]
                )
                self.db_session.add(website_metric)
            
            # Load customer metrics
            for metric in transformed_data.get("customer_metrics", []):
                customer_metric = CustomerAnalyticsData(
                    type=metric["type"],
                    name=metric["name"],
                    display_value=metric["display_value"],
                    numeric_value=metric["numeric_value"],
                    trend_value=metric["trend_value"],
                    trend_status=metric["trend_status"],
                    color=metric["color"],
                    icon=metric["icon"]
                )
                self.db_session.add(customer_metric)
            
            # Commit all changes
            self.db_session.commit()
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load data: {str(e)}")
            self.db_session.rollback()
            return False
    
    def calculate_derived_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate derived metrics from existing metrics
        
        Args:
            metrics: Dictionary of existing metrics
            
        Returns:
            Dictionary with additional derived metrics
        """
        # Add derived website metrics
        website_metrics = metrics.get("website_metrics", [])
        
        # Find sessions and pageviews
        total_sessions = 0
        total_pageviews = 0
        
        for metric in website_metrics:
            if metric["category"] == "traffic" and metric["sub_category"] == "overview":
                if metric["unit"] == "sessions":
                    try:
                        total_sessions = int(metric["value"])
                    except (ValueError, TypeError):
                        pass
                elif metric["unit"] == "views":
                    try:
                        total_pageviews = int(metric["value"])
                    except (ValueError, TypeError):
                        pass
        
        # Calculate pages per session
        if total_sessions > 0 and total_pageviews > 0:
            pages_per_session = total_pageviews / total_sessions
            website_metrics.append({
                "category": "engagement",
                "sub_category": "derived",
                "value": f"{pages_per_session:.2f}",
                "unit": "pages/session",
                "trend_value": None,
                "trend_unit": None,
                "icon": "file-alt",
                "color": "info",
                "display_order": 6,
                "status": "normal"
            })
        
        # Update the metrics dictionary
        metrics["website_metrics"] = website_metrics
        
        return metrics