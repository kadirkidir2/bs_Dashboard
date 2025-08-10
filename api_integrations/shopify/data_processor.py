"""
Shopify data processor for ETL operations
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from ..base.data_processor import BaseDataProcessor
from .client import ShopifyClient
from db.models import (
    SalesRevenueData, 
    InventoryData, 
    ProductAnalysisData,
    CustomerAnalyticsData
)

class ShopifyDataProcessor(BaseDataProcessor):
    """Data processor for Shopify API data"""
    
    def __init__(self, db_session: Session):
        """
        Initialize the Shopify data processor
        
        Args:
            db_session: SQLAlchemy database session
        """
        super().__init__('shopify', db_session)
    
    def extract(self, api_client: ShopifyClient, start_date: Optional[datetime] = None, 
               end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Extract data from the Shopify API
        
        Args:
            api_client: Shopify API client instance
            start_date: Start date for data extraction
            end_date: End date for data extraction
            
        Returns:
            Raw data from the Shopify API
        """
        return api_client.collect_metrics(start_date, end_date)
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw Shopify API data into a format suitable for database storage
        
        Args:
            raw_data: Raw data from the Shopify API
            
        Returns:
            Transformed data ready for database storage
        """
        transformed_data = {
            "sales_metrics": self._transform_sales_metrics(raw_data),
            "inventory_metrics": self._transform_inventory_metrics(raw_data),
            "product_metrics": self._transform_product_metrics(raw_data),
            "customer_metrics": self._transform_customer_metrics(raw_data)
        }
        
        # Calculate derived metrics
        transformed_data = self.calculate_derived_metrics(transformed_data)
        
        return transformed_data
    
    def _transform_sales_metrics(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform sales metrics from raw data"""
        sales_metrics = []
        
        orders = raw_data.get("orders", [])
        if not orders:
            return sales_metrics
            
        # Calculate total revenue
        total_revenue = sum(float(order.get("total_price", 0)) for order in orders)
        sales_metrics.append({
            "type": "main_metric",
            "name": "Total Revenue",
            "value": total_revenue,
            "display_value": f"${total_revenue:.2f}",
            "trend": None,  # Will be calculated later if historical data is available
            "color": "success",
            "date": datetime.now()
        })
        
        # Calculate order count
        order_count = len(orders)
        sales_metrics.append({
            "type": "main_metric",
            "name": "Order Count",
            "value": order_count,
            "display_value": str(order_count),
            "trend": None,
            "color": "primary",
            "date": datetime.now()
        })
        
        # Calculate average order value
        avg_order_value = total_revenue / order_count if order_count > 0 else 0
        sales_metrics.append({
            "type": "main_metric",
            "name": "Average Order Value",
            "value": avg_order_value,
            "display_value": f"${avg_order_value:.2f}",
            "trend": None,
            "color": "info",
            "date": datetime.now()
        })
        
        # Group orders by financial status
        status_counts = {}
        for order in orders:
            status = order.get("financial_status", "unknown")
            status_counts[status] = status_counts.get(status, 0) + 1
            
        for status, count in status_counts.items():
            sales_metrics.append({
                "type": "order_status",
                "name": f"{status.capitalize()} Orders",
                "value": count,
                "display_value": str(count),
                "trend": None,
                "color": "secondary",
                "date": datetime.now()
            })
        
        return sales_metrics
    
    def _transform_inventory_metrics(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform inventory metrics from raw data"""
        inventory_metrics = []
        
        products = raw_data.get("products", [])
        inventory_levels = raw_data.get("inventory", [])
        
        if not products:
            return inventory_metrics
            
        # Calculate total inventory count
        total_inventory = 0
        for product in products:
            for variant in product.get("variants", []):
                inventory_quantity = variant.get("inventory_quantity", 0)
                if inventory_quantity is not None:
                    total_inventory += inventory_quantity
        
        inventory_metrics.append({
            "name": "Total Inventory",
            "value": total_inventory,
            "display_value": str(total_inventory),
            "trend": None,
            "type": "main_metric",
            "color": "primary"
        })
        
        # Calculate out of stock items
        out_of_stock = 0
        for product in products:
            for variant in product.get("variants", []):
                inventory_quantity = variant.get("inventory_quantity", 0)
                if inventory_quantity is not None and inventory_quantity <= 0:
                    out_of_stock += 1
        
        inventory_metrics.append({
            "name": "Out of Stock Items",
            "value": out_of_stock,
            "display_value": str(out_of_stock),
            "trend": None,
            "type": "main_metric",
            "color": "danger"
        })
        
        # Calculate low stock items (less than 5)
        low_stock = 0
        for product in products:
            for variant in product.get("variants", []):
                inventory_quantity = variant.get("inventory_quantity", 0)
                if inventory_quantity is not None and 0 < inventory_quantity < 5:
                    low_stock += 1
        
        inventory_metrics.append({
            "name": "Low Stock Items",
            "value": low_stock,
            "display_value": str(low_stock),
            "trend": None,
            "type": "main_metric",
            "color": "warning"
        })
        
        return inventory_metrics
    
    def _transform_product_metrics(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform product metrics from raw data"""
        product_metrics = []
        
        products = raw_data.get("products", [])
        orders = raw_data.get("orders", [])
        
        if not products:
            return product_metrics
            
        # Calculate product counts
        product_count = len(products)
        
        # Count active products
        active_products = sum(1 for p in products if p.get("status") == "active")
        
        # Calculate total variants
        variant_count = sum(len(p.get("variants", [])) for p in products)
        
        # Extract product types
        product_types = {}
        for product in products:
            product_type = product.get("product_type", "Uncategorized")
            if not product_type:
                product_type = "Uncategorized"
            product_types[product_type] = product_types.get(product_type, 0) + 1
            
        # Add metrics
        product_metrics.append({
            "product_id": "total_count",
            "product_name": "Total Products",
            "category": "metrics",
            "price": 0,
            "sales_volume": 0,
            "stock_quantity": product_count,
            "view_count": 0,
            "conversion_rate": 0,
            "average_rating": 0,
            "review_count": 0,
            "profit_margin": 0,
            "status": "active"
        })
        
        product_metrics.append({
            "product_id": "active_count",
            "product_name": "Active Products",
            "category": "metrics",
            "price": 0,
            "sales_volume": 0,
            "stock_quantity": active_products,
            "view_count": 0,
            "conversion_rate": 0,
            "average_rating": 0,
            "review_count": 0,
            "profit_margin": 0,
            "status": "active"
        })
        
        product_metrics.append({
            "product_id": "variant_count",
            "product_name": "Total Variants",
            "category": "metrics",
            "price": 0,
            "sales_volume": 0,
            "stock_quantity": variant_count,
            "view_count": 0,
            "conversion_rate": 0,
            "average_rating": 0,
            "review_count": 0,
            "profit_margin": 0,
            "status": "active"
        })
        
        # Add top products (by inventory value)
        for product in products[:10]:  # Top 10 products
            variants = product.get("variants", [])
            if not variants:
                continue
                
            # Use the first variant for simplicity
            variant = variants[0]
            
            product_metrics.append({
                "product_id": str(product["id"]),
                "product_name": product["title"],
                "category": product.get("product_type", "Uncategorized") or "Uncategorized",
                "price": float(variant.get("price", 0)),
                "sales_volume": 0,  # Would need to calculate from orders
                "stock_quantity": variant.get("inventory_quantity", 0) or 0,
                "view_count": 0,  # Not available from basic API
                "conversion_rate": 0,  # Would need to calculate
                "average_rating": 0,  # Not available from basic API
                "review_count": 0,  # Not available from basic API
                "profit_margin": 0,  # Not available from basic API
                "status": product.get("status", "active")
            })
        
        return product_metrics
    
    def _transform_customer_metrics(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform customer metrics from raw data"""
        customer_metrics = []
        
        customers = raw_data.get("customers", [])
        orders = raw_data.get("orders", [])
        
        if not customers:
            return customer_metrics
            
        # Calculate total customers
        total_customers = len(customers)
        customer_metrics.append({
            "type": "top_metrics",
            "name": "Total Customers",
            "display_value": str(total_customers),
            "numeric_value": total_customers,
            "trend_value": None,
            "trend_status": None,
            "color": "primary",
            "icon": "users"
        })
        
        # Calculate new customers (in the last 30 days)
        thirty_days_ago = datetime.now() - datetime.timedelta(days=30)
        new_customers = sum(1 for c in customers if datetime.fromisoformat(c["created_at"].replace("Z", "+00:00")) > thirty_days_ago)
        customer_metrics.append({
            "type": "top_metrics",
            "name": "New Customers (30d)",
            "display_value": str(new_customers),
            "numeric_value": new_customers,
            "trend_value": None,
            "trend_status": None,
            "color": "success",
            "icon": "user-plus"
        })
        
        # Calculate customers with orders
        customers_with_orders = sum(1 for c in customers if c.get("orders_count", 0) > 0)
        customer_metrics.append({
            "type": "top_metrics",
            "name": "Customers with Orders",
            "display_value": str(customers_with_orders),
            "numeric_value": customers_with_orders,
            "trend_value": None,
            "trend_status": None,
            "color": "info",
            "icon": "shopping-cart"
        })
        
        # Calculate average orders per customer
        total_orders = sum(c.get("orders_count", 0) for c in customers)
        avg_orders = total_orders / total_customers if total_customers > 0 else 0
        customer_metrics.append({
            "type": "top_metrics",
            "name": "Avg Orders per Customer",
            "display_value": f"{avg_orders:.2f}",
            "numeric_value": avg_orders,
            "trend_value": None,
            "trend_status": None,
            "color": "warning",
            "icon": "chart-line"
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
            # Load sales metrics
            for metric in transformed_data.get("sales_metrics", []):
                sales_metric = SalesRevenueData(
                    type=metric["type"],
                    subtype=None,
                    name=metric["name"],
                    value=metric["value"],
                    display_value=metric["display_value"],
                    trend=metric["trend"],
                    color=metric["color"],
                    date=metric["date"]
                )
                self.db_session.add(sales_metric)
            
            # Load inventory metrics
            for metric in transformed_data.get("inventory_metrics", []):
                inventory_metric = InventoryData(
                    name=metric["name"],
                    value=metric["value"],
                    display_value=metric["display_value"],
                    trend=metric["trend"],
                    type=metric["type"],
                    color=metric["color"]
                )
                self.db_session.add(inventory_metric)
            
            # Load product metrics
            for metric in transformed_data.get("product_metrics", []):
                product_metric = ProductAnalysisData(
                    product_id=metric["product_id"],
                    product_name=metric["product_name"],
                    category=metric["category"],
                    price=metric["price"],
                    sales_volume=metric["sales_volume"],
                    stock_quantity=metric["stock_quantity"],
                    view_count=metric["view_count"],
                    conversion_rate=metric["conversion_rate"],
                    average_rating=metric["average_rating"],
                    review_count=metric["review_count"],
                    profit_margin=metric["profit_margin"],
                    status=metric["status"]
                )
                self.db_session.add(product_metric)
            
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
        # Add derived sales metrics
        sales_metrics = metrics.get("sales_metrics", [])
        
        # Find total revenue and order count
        total_revenue = next((m["value"] for m in sales_metrics if m["name"] == "Total Revenue"), 0)
        order_count = next((m["value"] for m in sales_metrics if m["name"] == "Order Count"), 0)
        
        # Calculate revenue per order if not already present
        if order_count > 0 and not any(m["name"] == "Revenue per Order" for m in sales_metrics):
            revenue_per_order = total_revenue / order_count
            sales_metrics.append({
                "type": "derived_metric",
                "name": "Revenue per Order",
                "value": revenue_per_order,
                "display_value": f"${revenue_per_order:.2f}",
                "trend": None,
                "color": "info",
                "date": datetime.now()
            })
        
        # Update the metrics dictionary
        metrics["sales_metrics"] = sales_metrics
        
        return metrics