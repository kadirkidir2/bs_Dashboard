"""
Meta data processor for ETL operations
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from ..base.data_processor import BaseDataProcessor
from .client import MetaClient
from db.models import (
    SocialCommerceMetric,
    MarketingRoiData
)

class MetaDataProcessor(BaseDataProcessor):
    """Data processor for Meta API data"""
    
    def __init__(self, db_session: Session):
        """
        Initialize the Meta data processor
        
        Args:
            db_session: SQLAlchemy database session
        """
        super().__init__('meta', db_session)
    
    def extract(self, api_client: MetaClient, start_date: Optional[datetime] = None, 
               end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Extract data from the Meta API
        
        Args:
            api_client: Meta API client instance
            start_date: Start date for data extraction
            end_date: End date for data extraction
            
        Returns:
            Raw data from the Meta API
        """
        return api_client.collect_metrics(start_date, end_date)
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw Meta API data into a format suitable for database storage
        
        Args:
            raw_data: Raw data from the Meta API
            
        Returns:
            Transformed data ready for database storage
        """
        transformed_data = {
            "facebook_metrics": self._transform_facebook_metrics(raw_data),
            "instagram_metrics": self._transform_instagram_metrics(raw_data),
            "marketing_metrics": self._transform_marketing_metrics(raw_data)
        }
        
        # Calculate derived metrics
        transformed_data = self.calculate_derived_metrics(transformed_data)
        
        return transformed_data
    
    def _transform_facebook_metrics(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform Facebook metrics from raw data"""
        facebook_metrics = []
        
        # Extract page info
        page_info = raw_data.get("page_info", {})
        if not page_info:
            return facebook_metrics
            
        # Basic page metrics
        facebook_metrics.append({
            "category": "facebook",
            "metric_name": "Page Followers",
            "metric_value": str(page_info.get("followers_count", 0)),
            "trend": None,
            "unit": "followers",
            "display_order": 1
        })
        
        facebook_metrics.append({
            "category": "facebook",
            "metric_name": "Page Likes",
            "metric_value": str(page_info.get("fan_count", 0)),
            "trend": None,
            "unit": "likes",
            "display_order": 2
        })
        
        # Extract post data
        posts_data = raw_data.get("posts", {}).get("data", [])
        if posts_data:
            # Calculate engagement metrics
            total_likes = sum(post.get("likes", {}).get("summary", {}).get("total_count", 0) for post in posts_data)
            total_comments = sum(post.get("comments", {}).get("summary", {}).get("total_count", 0) for post in posts_data)
            total_shares = sum(post.get("shares", {}).get("count", 0) for post in posts_data if "shares" in post)
            total_reactions = sum(post.get("reactions", {}).get("summary", {}).get("total_count", 0) for post in posts_data)
            
            post_count = len(posts_data)
            
            facebook_metrics.append({
                "category": "facebook",
                "metric_name": "Total Posts",
                "metric_value": str(post_count),
                "trend": None,
                "unit": "posts",
                "display_order": 3
            })
            
            if post_count > 0:
                facebook_metrics.append({
                    "category": "facebook",
                    "metric_name": "Avg Likes per Post",
                    "metric_value": f"{total_likes / post_count:.1f}",
                    "trend": None,
                    "unit": "likes",
                    "display_order": 4
                })
                
                facebook_metrics.append({
                    "category": "facebook",
                    "metric_name": "Avg Comments per Post",
                    "metric_value": f"{total_comments / post_count:.1f}",
                    "trend": None,
                    "unit": "comments",
                    "display_order": 5
                })
                
                facebook_metrics.append({
                    "category": "facebook",
                    "metric_name": "Avg Shares per Post",
                    "metric_value": f"{total_shares / post_count:.1f}",
                    "trend": None,
                    "unit": "shares",
                    "display_order": 6
                })
        
        # Extract insights data
        insights_data = raw_data.get("insights", {}).get("data", [])
        if insights_data:
            # Process each metric
            for metric in insights_data:
                metric_name = metric.get("name", "")
                values = metric.get("values", [])
                
                if not values:
                    continue
                    
                # Calculate total and average
                total_value = sum(item.get("value", 0) for item in values if item.get("value") is not None)
                avg_value = total_value / len(values) if values else 0
                
                # Map API metric names to display names
                metric_display_names = {
                    "page_impressions": "Page Impressions",
                    "page_reach": "Page Reach",
                    "page_engaged_users": "Engaged Users",
                    "page_post_engagements": "Post Engagements",
                    "page_fans": "Total Fans",
                    "page_fan_adds": "New Fans",
                    "page_fan_removes": "Lost Fans"
                }
                
                display_name = metric_display_names.get(metric_name, metric_name)
                
                facebook_metrics.append({
                    "category": "facebook",
                    "metric_name": f"{display_name} (Total)",
                    "metric_value": str(int(total_value)),
                    "trend": None,
                    "unit": "",
                    "display_order": 10
                })
                
                facebook_metrics.append({
                    "category": "facebook",
                    "metric_name": f"{display_name} (Avg)",
                    "metric_value": f"{avg_value:.1f}",
                    "trend": None,
                    "unit": "per day",
                    "display_order": 11
                })
        
        return facebook_metrics
    
    def _transform_instagram_metrics(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform Instagram metrics from raw data"""
        instagram_metrics = []
        
        # Check if Instagram data exists
        instagram_data = raw_data.get("instagram", {})
        if not instagram_data:
            return instagram_metrics
            
        # Extract media data
        media_data = instagram_data.get("media", {}).get("data", [])
        if media_data:
            # Calculate engagement metrics
            total_likes = sum(item.get("like_count", 0) for item in media_data)
            total_comments = sum(item.get("comments_count", 0) for item in media_data)
            media_count = len(media_data)
            
            # Group by media type
            media_types = {}
            for item in media_data:
                media_type = item.get("media_type", "UNKNOWN")
                media_types[media_type] = media_types.get(media_type, 0) + 1
            
            instagram_metrics.append({
                "category": "instagram",
                "metric_name": "Total Posts",
                "metric_value": str(media_count),
                "trend": None,
                "unit": "posts",
                "display_order": 1
            })
            
            if media_count > 0:
                instagram_metrics.append({
                    "category": "instagram",
                    "metric_name": "Avg Likes per Post",
                    "metric_value": f"{total_likes / media_count:.1f}",
                    "trend": None,
                    "unit": "likes",
                    "display_order": 2
                })
                
                instagram_metrics.append({
                    "category": "instagram",
                    "metric_name": "Avg Comments per Post",
                    "metric_value": f"{total_comments / media_count:.1f}",
                    "trend": None,
                    "unit": "comments",
                    "display_order": 3
                })
            
            # Add media type breakdown
            for media_type, count in media_types.items():
                instagram_metrics.append({
                    "category": "instagram",
                    "metric_name": f"{media_type} Posts",
                    "metric_value": str(count),
                    "trend": None,
                    "unit": "posts",
                    "display_order": 10
                })
        
        # Extract insights data
        insights_data = instagram_data.get("insights", {}).get("data", [])
        if insights_data:
            # Process each metric
            for metric in insights_data:
                metric_name = metric.get("name", "")
                values = metric.get("values", [])
                
                if not values:
                    continue
                    
                # Calculate total and average
                total_value = sum(item.get("value", 0) for item in values if item.get("value") is not None)
                avg_value = total_value / len(values) if values else 0
                
                # Map API metric names to display names
                metric_display_names = {
                    "impressions": "Impressions",
                    "reach": "Reach",
                    "profile_views": "Profile Views",
                    "follower_count": "Followers"
                }
                
                display_name = metric_display_names.get(metric_name, metric_name)
                
                instagram_metrics.append({
                    "category": "instagram",
                    "metric_name": f"{display_name} (Total)",
                    "metric_value": str(int(total_value)),
                    "trend": None,
                    "unit": "",
                    "display_order": 20
                })
                
                instagram_metrics.append({
                    "category": "instagram",
                    "metric_name": f"{display_name} (Avg)",
                    "metric_value": f"{avg_value:.1f}",
                    "trend": None,
                    "unit": "per day",
                    "display_order": 21
                })
        
        return instagram_metrics
    
    def _transform_marketing_metrics(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform marketing metrics from raw data"""
        marketing_metrics = []
        
        # Extract page info for basic metrics
        page_info = raw_data.get("page_info", {})
        if page_info:
            marketing_metrics.append({
                "type": "social_media",
                "name": "Facebook Followers",
                "value": page_info.get("followers_count", 0),
                "display_value": f"{page_info.get('followers_count', 0):,}",
                "trend": None,
                "color": "primary",
                "date": datetime.now()
            })
        
        # Extract insights data for engagement metrics
        insights_data = raw_data.get("insights", {}).get("data", [])
        if insights_data:
            # Find engagement metrics
            for metric in insights_data:
                if metric.get("name") == "page_engaged_users":
                    values = metric.get("values", [])
                    if values:
                        total_engaged = sum(item.get("value", 0) for item in values if item.get("value") is not None)
                        marketing_metrics.append({
                            "type": "engagement",
                            "name": "Facebook Engaged Users",
                            "value": total_engaged,
                            "display_value": f"{total_engaged:,}",
                            "trend": None,
                            "color": "info",
                            "date": datetime.now()
                        })
                
                if metric.get("name") == "page_post_engagements":
                    values = metric.get("values", [])
                    if values:
                        total_engagements = sum(item.get("value", 0) for item in values if item.get("value") is not None)
                        marketing_metrics.append({
                            "type": "engagement",
                            "name": "Facebook Post Engagements",
                            "value": total_engagements,
                            "display_value": f"{total_engagements:,}",
                            "trend": None,
                            "color": "success",
                            "date": datetime.now()
                        })
        
        # Extract Instagram metrics if available
        instagram_data = raw_data.get("instagram", {})
        if instagram_data:
            insights_data = instagram_data.get("insights", {}).get("data", [])
            if insights_data:
                for metric in insights_data:
                    if metric.get("name") == "follower_count":
                        values = metric.get("values", [])
                        if values and values[-1].get("value") is not None:
                            # Use the most recent value
                            follower_count = values[-1].get("value", 0)
                            marketing_metrics.append({
                                "type": "social_media",
                                "name": "Instagram Followers",
                                "value": follower_count,
                                "display_value": f"{follower_count:,}",
                                "trend": None,
                                "color": "purple",
                                "date": datetime.now()
                            })
                    
                    if metric.get("name") == "impressions":
                        values = metric.get("values", [])
                        if values:
                            total_impressions = sum(item.get("value", 0) for item in values if item.get("value") is not None)
                            marketing_metrics.append({
                                "type": "reach",
                                "name": "Instagram Impressions",
                                "value": total_impressions,
                                "display_value": f"{total_impressions:,}",
                                "trend": None,
                                "color": "pink",
                                "date": datetime.now()
                            })
        
        return marketing_metrics
    
    def load(self, transformed_data: Dict[str, Any]) -> bool:
        """
        Load transformed data into the database
        
        Args:
            transformed_data: Transformed data ready for database storage
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Load Facebook and Instagram metrics
            for metric in transformed_data.get("facebook_metrics", []) + transformed_data.get("instagram_metrics", []):
                social_metric = SocialCommerceMetric(
                    category=metric["category"],
                    metric_name=metric["metric_name"],
                    metric_value=metric["metric_value"],
                    trend=metric["trend"],
                    unit=metric["unit"],
                    display_order=metric["display_order"]
                )
                self.db_session.add(social_metric)
            
            # Load marketing metrics
            for metric in transformed_data.get("marketing_metrics", []):
                marketing_metric = MarketingRoiData(
                    type=metric["type"],
                    name=metric["name"],
                    value=metric["value"],
                    display_value=metric["display_value"],
                    trend=metric["trend"],
                    color=metric["color"],
                    date=metric["date"]
                )
                self.db_session.add(marketing_metric)
            
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
        # Add engagement rate metrics for Facebook
        facebook_metrics = metrics.get("facebook_metrics", [])
        
        # Find followers count
        followers_count = 0
        for metric in facebook_metrics:
            if metric["metric_name"] == "Page Followers":
                try:
                    followers_count = int(metric["metric_value"])
                    break
                except (ValueError, TypeError):
                    pass
        
        # Find engagement metrics
        total_engagements = 0
        for metric in facebook_metrics:
            if metric["metric_name"] == "Post Engagements (Total)":
                try:
                    total_engagements = int(metric["metric_value"])
                    break
                except (ValueError, TypeError):
                    pass
        
        # Calculate engagement rate if we have both values
        if followers_count > 0 and total_engagements > 0:
            engagement_rate = (total_engagements / followers_count) * 100
            facebook_metrics.append({
                "category": "facebook",
                "metric_name": "Engagement Rate",
                "metric_value": f"{engagement_rate:.2f}",
                "trend": None,
                "unit": "%",
                "display_order": 30
            })
        
        # Update the metrics dictionary
        metrics["facebook_metrics"] = facebook_metrics
        
        # Do the same for Instagram
        instagram_metrics = metrics.get("instagram_metrics", [])
        
        # Find followers count
        instagram_followers = 0
        for metric in instagram_metrics:
            if metric["metric_name"] == "Followers (Total)":
                try:
                    instagram_followers = int(metric["metric_value"])
                    break
                except (ValueError, TypeError):
                    pass
        
        # Find engagement metrics (likes + comments)
        instagram_likes = 0
        instagram_comments = 0
        instagram_posts = 0
        
        for metric in instagram_metrics:
            if metric["metric_name"] == "Total Posts":
                try:
                    instagram_posts = int(metric["metric_value"])
                except (ValueError, TypeError):
                    pass
            elif metric["metric_name"] == "Avg Likes per Post":
                try:
                    instagram_likes = float(metric["metric_value"]) * instagram_posts
                except (ValueError, TypeError):
                    pass
            elif metric["metric_name"] == "Avg Comments per Post":
                try:
                    instagram_comments = float(metric["metric_value"]) * instagram_posts
                except (ValueError, TypeError):
                    pass
        
        # Calculate engagement rate if we have the values
        instagram_engagements = instagram_likes + instagram_comments
        if instagram_followers > 0 and instagram_engagements > 0:
            instagram_engagement_rate = (instagram_engagements / instagram_followers) * 100
            instagram_metrics.append({
                "category": "instagram",
                "metric_name": "Engagement Rate",
                "metric_value": f"{instagram_engagement_rate:.2f}",
                "trend": None,
                "unit": "%",
                "display_order": 30
            })
        
        # Update the metrics dictionary
        metrics["instagram_metrics"] = instagram_metrics
        
        return metrics