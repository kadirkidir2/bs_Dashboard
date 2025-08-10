"""
TikTok Ads data processor for ETL operations
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from ..base.data_processor import BaseDataProcessor
from .client import TikTokAdsClient
from db.models import (
    MarketingRoiData,
    SocialCommerceMetric
)

class TikTokAdsDataProcessor(BaseDataProcessor):
    """Data processor for TikTok Ads API data"""
    
    def __init__(self, db_session: Session):
        """
        Initialize the TikTok Ads data processor
        
        Args:
            db_session: SQLAlchemy database session
        """
        super().__init__('tiktok_ads', db_session)
    
    def extract(self, api_client: TikTokAdsClient, start_date: Optional[datetime] = None, 
               end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Extract data from the TikTok Ads API
        
        Args:
            api_client: TikTok Ads API client instance
            start_date: Start date for data extraction
            end_date: End date for data extraction
            
        Returns:
            Raw data from the TikTok Ads API
        """
        return api_client.collect_metrics(start_date, end_date)
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw TikTok Ads API data into a format suitable for database storage
        
        Args:
            raw_data: Raw data from the TikTok Ads API
            
        Returns:
            Transformed data ready for database storage
        """
        transformed_data = {
            "marketing_metrics": self._transform_marketing_metrics(raw_data),
            "social_metrics": self._transform_social_metrics(raw_data)
        }
        
        # Calculate derived metrics
        transformed_data = self.calculate_derived_metrics(transformed_data)
        
        return transformed_data
    
    def _transform_marketing_metrics(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform marketing metrics from raw data"""
        marketing_metrics = []
        
        # Extract reports data
        reports = raw_data.get("reports", {}).get("data", {}).get("list", [])
        
        if reports:
            # Calculate totals
            total_spend = sum(float(report.get("metrics", {}).get("spend", 0)) for report in reports)
            total_impressions = sum(int(report.get("metrics", {}).get("impressions", 0)) for report in reports)
            total_clicks = sum(int(report.get("metrics", {}).get("clicks", 0)) for report in reports)
            total_conversions = sum(int(report.get("metrics", {}).get("conversion", 0)) for report in reports)
            
            # Add metrics
            marketing_metrics.append({
                "type": "tiktok_ads",
                "name": "TikTok Ad Spend",
                "value": total_spend,
                "display_value": f"${total_spend:.2f}",
                "trend": None,
                "color": "danger",
                "date": datetime.now()
            })
            
            marketing_metrics.append({
                "type": "tiktok_ads",
                "name": "TikTok Impressions",
                "value": total_impressions,
                "display_value": f"{total_impressions:,}",
                "trend": None,
                "color": "primary",
                "date": datetime.now()
            })
            
            marketing_metrics.append({
                "type": "tiktok_ads",
                "name": "TikTok Clicks",
                "value": total_clicks,
                "display_value": f"{total_clicks:,}",
                "trend": None,
                "color": "info",
                "date": datetime.now()
            })
            
            marketing_metrics.append({
                "type": "tiktok_ads",
                "name": "TikTok Conversions",
                "value": total_conversions,
                "display_value": f"{total_conversions:,}",
                "trend": None,
                "color": "success",
                "date": datetime.now()
            })
            
            # Calculate averages
            if total_impressions > 0:
                ctr = (total_clicks / total_impressions) * 100
                marketing_metrics.append({
                    "type": "tiktok_ads",
                    "name": "TikTok CTR",
                    "value": ctr,
                    "display_value": f"{ctr:.2f}%",
                    "trend": None,
                    "color": "warning",
                    "date": datetime.now()
                })
            
            if total_clicks > 0:
                cpc = total_spend / total_clicks
                marketing_metrics.append({
                    "type": "tiktok_ads",
                    "name": "TikTok CPC",
                    "value": cpc,
                    "display_value": f"${cpc:.2f}",
                    "trend": None,
                    "color": "secondary",
                    "date": datetime.now()
                })
            
            if total_conversions > 0:
                cpa = total_spend / total_conversions
                marketing_metrics.append({
                    "type": "tiktok_ads",
                    "name": "TikTok CPA",
                    "value": cpa,
                    "display_value": f"${cpa:.2f}",
                    "trend": None,
                    "color": "dark",
                    "date": datetime.now()
                })
        
        # Extract campaign data
        campaigns = raw_data.get("campaigns", {}).get("data", {}).get("list", [])
        
        if campaigns:
            # Count campaigns by status
            status_counts = {}
            for campaign in campaigns:
                status = campaign.get("operation_status", "unknown")
                status_counts[status] = status_counts.get(status, 0) + 1
            
            for status, count in status_counts.items():
                marketing_metrics.append({
                    "type": "tiktok_campaigns",
                    "name": f"TikTok {status.capitalize()} Campaigns",
                    "value": count,
                    "display_value": str(count),
                    "trend": None,
                    "color": "primary" if status.lower() == "enable" else "secondary",
                    "date": datetime.now()
                })
        
        return marketing_metrics
    
    def _transform_social_metrics(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform social metrics from raw data"""
        social_metrics = []
        
        # Extract reports data
        reports = raw_data.get("reports", {}).get("data", {}).get("list", [])
        
        if reports:
            # Calculate totals
            total_spend = sum(float(report.get("metrics", {}).get("spend", 0)) for report in reports)
            total_impressions = sum(int(report.get("metrics", {}).get("impressions", 0)) for report in reports)
            total_clicks = sum(int(report.get("metrics", {}).get("clicks", 0)) for report in reports)
            total_conversions = sum(int(report.get("metrics", {}).get("conversion", 0)) for report in reports)
            total_reach = sum(int(report.get("metrics", {}).get("reach", 0)) for report in reports)
            
            # Add metrics
            social_metrics.append({
                "category": "tiktok",
                "metric_name": "Ad Spend",
                "metric_value": f"${total_spend:.2f}",
                "trend": None,
                "unit": "USD",
                "display_order": 1
            })
            
            social_metrics.append({
                "category": "tiktok",
                "metric_name": "Impressions",
                "metric_value": f"{total_impressions:,}",
                "trend": None,
                "unit": "views",
                "display_order": 2
            })
            
            social_metrics.append({
                "category": "tiktok",
                "metric_name": "Clicks",
                "metric_value": f"{total_clicks:,}",
                "trend": None,
                "unit": "clicks",
                "display_order": 3
            })
            
            social_metrics.append({
                "category": "tiktok",
                "metric_name": "Conversions",
                "metric_value": f"{total_conversions:,}",
                "trend": None,
                "unit": "conversions",
                "display_order": 4
            })
            
            social_metrics.append({
                "category": "tiktok",
                "metric_name": "Reach",
                "metric_value": f"{total_reach:,}",
                "trend": None,
                "unit": "users",
                "display_order": 5
            })
            
            # Calculate averages
            if total_impressions > 0:
                ctr = (total_clicks / total_impressions) * 100
                social_metrics.append({
                    "category": "tiktok",
                    "metric_name": "CTR",
                    "metric_value": f"{ctr:.2f}",
                    "trend": None,
                    "unit": "%",
                    "display_order": 6
                })
            
            if total_clicks > 0:
                cpc = total_spend / total_clicks
                social_metrics.append({
                    "category": "tiktok",
                    "metric_name": "CPC",
                    "metric_value": f"{cpc:.2f}",
                    "trend": None,
                    "unit": "USD",
                    "display_order": 7
                })
            
            if total_conversions > 0:
                cpa = total_spend / total_conversions
                social_metrics.append({
                    "category": "tiktok",
                    "metric_name": "CPA",
                    "metric_value": f"{cpa:.2f}",
                    "trend": None,
                    "unit": "USD",
                    "display_order": 8
                })
        
        return social_metrics
    
    def load(self, transformed_data: Dict[str, Any]) -> bool:
        """
        Load transformed data into the database
        
        Args:
            transformed_data: Transformed data ready for database storage
            
        Returns:
            True if successful, False otherwise
        """
        try:
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
        # Add derived marketing metrics
        marketing_metrics = metrics.get("marketing_metrics", [])
        
        # Find spend and conversions
        total_spend = 0
        total_conversions = 0
        
        for metric in marketing_metrics:
            if metric["name"] == "TikTok Ad Spend":
                total_spend = metric["value"]
            elif metric["name"] == "TikTok Conversions":
                total_conversions = metric["value"]
        
        # Calculate ROAS if we have both values
        if total_spend > 0 and total_conversions > 0:
            # Assume average order value of $50 for ROAS calculation
            # In a real implementation, this would come from actual order data
            avg_order_value = 50
            revenue = total_conversions * avg_order_value
            roas = revenue / total_spend
            
            marketing_metrics.append({
                "type": "tiktok_ads",
                "name": "TikTok ROAS",
                "value": roas,
                "display_value": f"{roas:.2f}x",
                "trend": None,
                "color": "success",
                "date": datetime.now()
            })
        
        # Update the metrics dictionary
        metrics["marketing_metrics"] = marketing_metrics
        
        return metrics