"""
Data processing module for API integrations
"""
import logging
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
from sqlalchemy.orm import Session
from abc import ABC, abstractmethod

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class BaseDataProcessor(ABC):
    """Base data processor class for ETL operations"""
    
    def __init__(self, platform_name: str, db_session: Session):
        """
        Initialize the data processor
        
        Args:
            platform_name: Name of the platform
            db_session: SQLAlchemy database session
        """
        self.platform_name = platform_name
        self.db_session = db_session
        self.logger = logging.getLogger(f"data.{platform_name}")
        
    @abstractmethod
    def extract(self, api_client: Any, start_date: Optional[datetime] = None, 
               end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Extract data from the API
        
        Args:
            api_client: API client instance
            start_date: Start date for data extraction
            end_date: End date for data extraction
            
        Returns:
            Raw data from the API
        """
        pass
    
    @abstractmethod
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw API data into a format suitable for database storage
        
        Args:
            raw_data: Raw data from the API
            
        Returns:
            Transformed data ready for database storage
        """
        pass
    
    @abstractmethod
    def load(self, transformed_data: Dict[str, Any]) -> bool:
        """
        Load transformed data into the database
        
        Args:
            transformed_data: Transformed data ready for database storage
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    def process(self, api_client: Any, start_date: Optional[datetime] = None, 
               end_date: Optional[datetime] = None) -> bool:
        """
        Run the full ETL process
        
        Args:
            api_client: API client instance
            start_date: Start date for data extraction
            end_date: End date for data extraction
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(f"Starting ETL process for {self.platform_name}")
            
            # Extract
            self.logger.info("Extracting data...")
            raw_data = self.extract(api_client, start_date, end_date)
            
            # Transform
            self.logger.info("Transforming data...")
            transformed_data = self.transform(raw_data)
            
            # Load
            self.logger.info("Loading data into database...")
            success = self.load(transformed_data)
            
            if success:
                self.logger.info(f"ETL process completed successfully for {self.platform_name}")
            else:
                self.logger.error(f"ETL process failed during load phase for {self.platform_name}")
                
            return success
            
        except Exception as e:
            self.logger.error(f"ETL process failed for {self.platform_name}: {str(e)}")
            return False
            
    def calculate_derived_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate derived metrics from existing metrics
        
        Args:
            metrics: Dictionary of existing metrics
            
        Returns:
            Dictionary with additional derived metrics
        """
        # Base implementation does nothing, subclasses should override
        return metrics