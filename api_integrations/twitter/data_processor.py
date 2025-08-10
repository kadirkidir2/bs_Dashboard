"""
Twitter (X) data processor for ETL operations
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from ..base.data_processor import BaseDataProcessor
from .client import TwitterClient
from db.models import (
    SocialCommerceMetric,
    MarketingRoiData
)

class TwitterDataProcessor(BaseDataProcessor):
    """Data processor for Twitter API data"""
    
    def __init__(self, db_session: Session):
        """
        Initialize the Twitter data processor
        
        Args:
            db_session: SQLAlchemy database session
        """
        super().__init__('twitter', db_session)
    
    def extract(self, api_client: TwitterClient, start_date: Optional[datetime] = None, 
               end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Extract data from the Twitter API
        
        Args:
            api_client: Twitter API client instance
            start_date: Start date for data extraction
            end_date: End date for data extraction
            
        Returns:
            Raw data from the Twitter API
        """
        return api_client.collect_metrics(start_date, end_date)
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw Twitter API data into a format suitable for database storage
        
        Args:
            raw_data: Raw data from the Twitter API
            
        Returns:
            Transformed data ready for database storage
        """
        transformed_data = {
            "social_metrics": self._transform_social_metrics(raw_data),
            "marketing_metrics": self._transform_marketing_metrics(raw_data)
        }
        
        # Calculate derived metrics
        transformed_data = self.calculate_derived_metrics(transformed_data)
        
        return transformed_data
    
    def _transform_social_metrics(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform social metrics from raw data"""
        social_metrics = []
        
        # Extract user info
        user_info = raw_data.get("user_info", {}).get("data", {})
        if user_info:
            # Get public metrics
            public_metrics = user_info.get("public_metrics", {})
            
            # Add metrics
            social_metrics.append({
                "category": "twitter",
                "metric_name": "Followers",
                "metric_value": str(public_metrics.get("followers_count", 0)),
                "trend": None,
                "unit": "followers",
                "display_order": 1
            })
            
            social_metrics.append({
                "category": "twitter",
                "metric_name": "Following",
                "metric_value": str(public_metrics.get("following_count", 0)),
                "trend": None,
                "unit": "accounts",
                "display_order": 2
            })
            
            social_metrics.append({
                "category": "twitter",
                "metric_name": "Tweet Count",
                "metric_value": str(public_metrics.get("tweet_count", 0)),
                "trend": None,
                "unit": "tweets",
                "display_order": 3
            })
            
            social_metrics.append({
                "category": "twitter",
                "metric_name": "Listed Count",
                "metric_value": str(public_metrics.get("listed_count", 0)),
                "trend": None,
                "unit": "lists",
                "display_order": 4
            })
        
        # Extract tweets data
        tweets = raw_data.get("tweets", {}).get("data", [])
        if tweets:
            # Calculate engagement metrics
            total_likes = sum(tweet.get("public_metrics", {}).get("like_count", 0) for tweet in tweets)
            total_retweets = sum(tweet.get("public_metrics", {}).get("retweet_count", 0) for tweet in tweets)
            total_replies = sum(tweet.get("public_metrics", {}).get("reply_count", 0) for tweet in tweets)
            total_quotes = sum(tweet.get("public_metrics", {}).get("quote_count", 0) for tweet in tweets)
            
            tweet_count = len(tweets)
            
            # Add metrics
            social_metrics.append({
                "category": "twitter",
                "metric_name": "Recent Tweets",
                "metric_value": str(tweet_count),
                "trend": None,
                "unit": "tweets",
                "display_order": 5
            })
            
            if tweet_count > 0:
                social_metrics.append({
                    "category": "twitter",
                    "metric_name": "Avg Likes per Tweet",
                    "metric_value": f"{total_likes / tweet_count:.1f}",
                    "trend": None,
                    "unit": "likes",
                    "display_order": 6
                })
                
                social_metrics.append({
                    "category": "twitter",
                    "metric_name": "Avg Retweets per Tweet",
                    "metric_value": f"{total_retweets / tweet_count:.1f}",
                    "trend": None,
                    "unit": "retweets",
                    "display_order": 7
                })
                
                social_metrics.append({
                    "category": "twitter",
                    "metric_name": "Avg Replies per Tweet",
                    "metric_value": f"{total_replies / tweet_count:.1f}",
                    "trend": None,
                    "unit": "replies",
                    "display_order": 8
                })
                
                social_metrics.append({
                    "category": "twitter",
                    "metric_name": "Avg Quotes per Tweet",
                    "metric_value": f"{total_quotes / tweet_count:.1f}",
                    "trend": None,
                    "unit": "quotes",
                    "display_order": 9
                })
        
        return social_metrics
    
    def _transform_marketing_metrics(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform marketing metrics from raw data"""
        marketing_metrics = []
        
        # Extract user info
        user_info = raw_data.get("user_info", {}).get("data", {})
        if user_info:
            # Get public metrics
            public_metrics = user_info.get("public_metrics", {})
            
            # Add metrics
            marketing_metrics.append({
                "type": "social_media",
                "name": "Twitter Followers",
                "value": public_metrics.get("followers_count", 0),
                "display_value": f"{public_metrics.get('followers_count', 0):,}",
                "trend": None,
                "color": "info",
                "date": datetime.now()
            })
        
        # Extract tweets data
        tweets = raw_data.get("tweets", {}).get("data", [])
        if tweets:
            # Calculate engagement metrics
            total_likes = sum(tweet.get("public_metrics", {}).get("like_count", 0) for tweet in tweets)
            total_retweets = sum(tweet.get("public_metrics", {}).get("retweet_count", 0) for tweet in tweets)
            total_replies = sum(tweet.get("public_metrics", {}).get("reply_count", 0) for tweet in tweets)
            total_quotes = sum(tweet.get("public_metrics", {}).get("quote_count", 0) for tweet in tweets)
            
            total_engagement = total_likes + total_retweets + total_replies + total_quotes
            
            marketing_metrics.append({
                "type": "engagement",
                "name": "Twitter Engagement",
                "value": total_engagement,
                "display_value": f"{total_engagement:,}",
                "trend": None,
                "color": "primary",
                "date": datetime.now()
            })
            
            marketing_metrics.append({
                "type": "engagement",
                "name": "Twitter Likes",
                "value": total_likes,
                "display_value": f"{total_likes:,}",
                "trend": None,
                "color": "danger",
                "date": datetime.now()
            })
            
            marketing_metrics.append({
                "type": "engagement",
                "name": "Twitter Retweets",
                "value": total_retweets,
                "display_value": f"{total_retweets:,}",
                "trend": None,
                "color": "success",
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
            # Load social metrics
            for metric in transformed_data.get("social_metrics", []):
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
        # Add derived social metrics
        social_metrics = metrics.get("social_metrics", [])
        
        # Find followers and engagement metrics
        followers_count = 0
        likes_per_tweet = 0
        retweets_per_tweet = 0
        replies_per_tweet = 0
        quotes_per_tweet = 0
        
        for metric in social_metrics:
            if metric["metric_name"] == "Followers":
                try:
                    followers_count = int(metric["metric_value"])
                except (ValueError, TypeError):
                    pass
            elif metric["metric_name"] == "Avg Likes per Tweet":
                try:
                    likes_per_tweet = float(metric["metric_value"])
                except (ValueError, TypeError):
                    pass
            elif metric["metric_name"] == "Avg Retweets per Tweet":
                try:
                    retweets_per_tweet = float(metric["metric_value"])
                except (ValueError, TypeError):
                    pass
            elif metric["metric_name"] == "Avg Replies per Tweet":
                try:
                    replies_per_tweet = float(metric["metric_value"])
                except (ValueError, TypeError):
                    pass
            elif metric["metric_name"] == "Avg Quotes per Tweet":
                try:
                    quotes_per_tweet = float(metric["metric_value"])
                except (ValueError, TypeError):
                    pass
        
        # Calculate engagement rate if we have followers
        if followers_count > 0:
            avg_engagement_per_tweet = likes_per_tweet + retweets_per_tweet + replies_per_tweet + quotes_per_tweet
            engagement_rate = (avg_engagement_per_tweet / followers_count) * 100
            
            social_metrics.append({
                "category": "twitter",
                "metric_name": "Engagement Rate",
                "metric_value": f"{engagement_rate:.2f}",
                "trend": None,
                "unit": "%",
                "display_order": 10
            })
        
        # Update the metrics dictionary
        metrics["social_metrics"] = social_metrics
        
        return metrics