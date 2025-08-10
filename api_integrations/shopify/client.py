"""
Shopify API client implementation
"""
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from ..base.client import BaseAPIClient

class ShopifyClient(BaseAPIClient):
    """Shopify API client for interacting with the Shopify Admin API"""
    
    def __init__(self, credentials: Dict[str, Any]):
        """
        Initialize the Shopify API client
        
        Args:
            credentials: Dictionary containing 'shop_name' and 'access_token'
        """
        super().__init__('shopify', credentials)
        
        # Validate required credentials
        required_keys = ['shop_name', 'access_token']
        for key in required_keys:
            if key not in credentials:
                raise ValueError(f"Missing required credential: {key}")
        
        self.shop_name = credentials['shop_name']
        self.access_token = credentials['access_token']
        
        # Set up API connection details
        self.base_url = f"https://{self.shop_name}.myshopify.com/admin/api/2023-10"
        self.headers = {
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json"
        }
    
    def validate_credentials(self) -> bool:
        """
        Validate that the provided credentials are valid
        
        Returns:
            True if credentials are valid, False otherwise
        """
        try:
            shop_info = self.get_shop_info()
            return bool(shop_info and 'id' in shop_info)
        except Exception as e:
            self.logger.error(f"Credential validation failed: {str(e)}")
            return False
    
    def get_account_info(self) -> Dict[str, Any]:
        """
        Get basic account information to verify API access
        
        Returns:
            Dictionary with shop information
        """
        return self.get_shop_info()
    
    def get_shop_info(self) -> Dict[str, Any]:
        """
        Get shop information
        
        Returns:
            Dictionary with shop details
        """
        response = self.make_request("shop.json")
        return response.get("shop", {})
    
    def get_products(self, limit: int = 50, since_id: int = None) -> List[Dict]:
        """
        Get products
        
        Args:
            limit: Maximum number of products (1-250)
            since_id: Get products after this ID
            
        Returns:
            List of product dictionaries
        """
        endpoint = f"products.json?limit={limit}"
        if since_id:
            endpoint += f"&since_id={since_id}"
        
        response = self.make_request(endpoint)
        return response.get("products", [])
    
    def get_all_products(self) -> List[Dict]:
        """
        Get all products (with pagination)
        
        Returns:
            List of all product dictionaries
        """
        all_products = []
        since_id = None
        
        while True:
            products = self.get_products(limit=250, since_id=since_id)
            if not products:
                break
            
            all_products.extend(products)
            since_id = products[-1]["id"]
            self.logger.info(f"Retrieved {len(all_products)} products...")
            
            # Rate limiting
            time.sleep(0.5)
        
        return all_products
    
    def get_orders(self, status: str = "any", limit: int = 50, 
                  since_id: int = None, created_at_min: str = None) -> List[Dict]:
        """
        Get orders
        
        Args:
            status: Order status (open, closed, cancelled, any)
            limit: Maximum number of orders
            since_id: Get orders after this ID
            created_at_min: Get orders created after this timestamp
            
        Returns:
            List of order dictionaries
        """
        endpoint = f"orders.json?status={status}&limit={limit}"
        if since_id:
            endpoint += f"&since_id={since_id}"
        if created_at_min:
            endpoint += f"&created_at_min={created_at_min}"
        
        response = self.make_request(endpoint)
        return response.get("orders", [])
    
    def get_all_orders(self, days: int = 30, status: str = "any") -> List[Dict]:
        """
        Get all orders within a time period
        
        Args:
            days: Number of days to look back
            status: Order status
            
        Returns:
            List of all order dictionaries
        """
        all_orders = []
        since_id = None
        created_at_min = (datetime.now() - timedelta(days=days)).isoformat()
        
        while True:
            orders = self.get_orders(
                status=status, 
                limit=250, 
                since_id=since_id, 
                created_at_min=created_at_min
            )
            
            if not orders:
                break
            
            all_orders.extend(orders)
            since_id = orders[-1]["id"]
            self.logger.info(f"Retrieved {len(all_orders)} orders...")
            
            # Rate limiting
            time.sleep(0.5)
        
        return all_orders
    
    def get_customers(self, limit: int = 50, since_id: int = None) -> List[Dict]:
        """
        Get customers
        
        Args:
            limit: Maximum number of customers
            since_id: Get customers after this ID
            
        Returns:
            List of customer dictionaries
        """
        endpoint = f"customers.json?limit={limit}"
        if since_id:
            endpoint += f"&since_id={since_id}"
        
        response = self.make_request(endpoint)
        return response.get("customers", [])
    
    def get_all_customers(self) -> List[Dict]:
        """
        Get all customers (with pagination)
        
        Returns:
            List of all customer dictionaries
        """
        all_customers = []
        since_id = None
        
        while True:
            customers = self.get_customers(limit=250, since_id=since_id)
            if not customers:
                break
            
            all_customers.extend(customers)
            if not customers:
                break
                
            since_id = customers[-1]["id"]
            self.logger.info(f"Retrieved {len(all_customers)} customers...")
            
            # Rate limiting
            time.sleep(0.5)
        
        return all_customers
    
    def get_inventory_levels(self, location_id: int = None) -> List[Dict]:
        """
        Get inventory levels
        
        Args:
            location_id: Filter by location ID
            
        Returns:
            List of inventory level dictionaries
        """
        endpoint = "inventory_levels.json"
        if location_id:
            endpoint += f"?location_ids={location_id}"
        
        response = self.make_request(endpoint)
        return response.get("inventory_levels", [])
    
    def collect_metrics(self, start_date: Optional[datetime] = None, 
                       end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Collect metrics from Shopify
        
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
            
        # Format dates for Shopify API
        created_at_min = start_date.isoformat()
        
        self.logger.info(f"Collecting Shopify metrics from {start_date.date()} to {end_date.date()}")
        
        # Get shop information
        shop_info = self.get_shop_info()
        
        # Get recent orders
        orders = self.get_all_orders(
            days=(end_date - start_date).days,
            status="any"
        )
        
        # Get products
        products = self.get_all_products()
        
        # Get customers
        customers = self.get_all_customers()
        
        # Get inventory levels
        inventory = self.get_inventory_levels()
        
        # Compile all data
        return {
            "shop_info": shop_info,
            "orders": orders,
            "products": products,
            "customers": customers,
            "inventory": inventory,
            "collected_at": datetime.now().isoformat(),
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            }
        }