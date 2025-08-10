from sqlalchemy import Column, Integer, String, DECIMAL, DateTime, BigInteger, Float, Enum, TIMESTAMP, Numeric, text
from datetime import datetime
from db import Base
from sqlalchemy.sql import func

# Mevcut modelinizi buraya taşıyın
class CustomerAnalyticsData(Base):
    __tablename__ = 'customer_analitics_data'
    
    id = Column(Integer, primary_key=True)
    type = Column(String(255), nullable=False)
    name = Column(String(255), nullable=False)
    display_value = Column(String(255), nullable=True)
    numeric_value = Column(DECIMAL(10, 2), nullable=True)
    trend_value = Column(DECIMAL(10, 2), nullable=True)
    trend_status = Column(String(50), nullable=True)
    color = Column(String(50), nullable=True)
    icon = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
# # Yeni bir tablo eklemek isterseniz (örnek)
# class SalesReportData(Base):
#     __tablename__ = 'sales_reports_data'
    
#     id = Column(Integer, primary_key=True)
#     product_name = Column(String(255))
#     revenue = Column(DECIMAL(10, 2))
#     date = Column(DateTime)

class SalesRevenueData(Base):
    __tablename__ = 'sales_revenue_data'

    id = Column(Integer, primary_key=True)
    type = Column(String(50), nullable=False)
    subtype = Column(String(50), nullable=True)
    name = Column(String(100), nullable=False)
    value = Column(DECIMAL(12, 2), nullable=True)
    display_value = Column(String(50), nullable=True)
    trend = Column(DECIMAL(5, 2), nullable=True)
    color = Column(String(20), nullable=True)
    date = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.now)

    def to_dict(self):
        return {
            "id": self.id,
            "type": self.type,
            "subtype": self.subtype,
            "name": self.name,
            "value": float(self.value) if self.value else None,
            "display_value": self.display_value,
            "trend": float(self.trend) if self.trend else None,
            "color": self.color,
            "date": self.date.isoformat() if self.date else None
        }
    
class MarketingRoiData(Base):
    __tablename__ = 'marketing_roi_data'

    id = Column(Integer, primary_key=True)
    type = Column(String(50), nullable=False)
    name = Column(String(100), nullable=False)
    value = Column(DECIMAL(12, 2), nullable=True)
    display_value = Column(String(50), nullable=True)
    trend = Column(DECIMAL(5, 2), nullable=True)
    color = Column(String(20), nullable=True)
    date = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.now)

    def to_dict(self):
        return {
            "id": self.id,
            "type": self.type,
            "name": self.name,
            "value": float(self.value) if self.value else None,
            "display_value": self.display_value,
            "trend": float(self.trend) if self.trend else None,
            "color": self.color,
            "date": self.date.isoformat() if self.date else None,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }

    def __repr__(self):
        return f'<MarketingRoiData {self.name}: {self.display_value}>'
    
class InventoryData(Base):
    __tablename__ = 'inventory_data'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    value = Column(Float(15, 2))
    display_value = Column(String(50))
    trend = Column(Float(5, 2))
    type = Column(Enum('main_metric', 'category_stock', 'fast_moving', 'slow_moving', 
                    'warehouse_metric', 'supply_chain', 'stock_trend'), nullable=False)
    color = Column(String(20))
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'value': self.value,
            'display_value': self.display_value,
            'trend': self.trend,
            'type': self.type,
            'color': self.color,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
from sqlalchemy import Column, Integer, String, Float, TIMESTAMP, Enum
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

class WebsitePerformanceMetric(Base):
    __tablename__ = 'site_performance_metrics'

    # Sütun tanımları (veritabanı yapısıyla birebir eşleşecek şekilde)
    id = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(String(255), nullable=False)  # NOT NULL
    sub_category = Column(String(255))  # NULL olabilir
    value = Column(String(255), nullable=False)  # NOT NULL
    unit = Column(String(50))  # NULL olabilir
    trend_value = Column(String(255))  # NULL olabilir
    trend_unit = Column(String(50))  # NULL olabilir
    icon = Column(String(255))  # NULL olabilir
    color = Column(String(50))  # NULL olabilir
    display_order = Column(Integer)  # NULL olabilir
    status = Column(String(50))  # NULL olabilir

    def to_dict(self):
        """Veritabanı kaydını Python dict'ine dönüştürür"""
        return {
            'id': self.id,
            'category': self.category,
            'sub_category': self.sub_category,
            'value': self.value,
            'unit': self.unit,
            'trend_value': self.trend_value,
            'trend_unit': self.trend_unit,
            'icon': self.icon,
            'color': self.color,
            'display_order': self.display_order,
            'status': self.status
        }

    def __repr__(self):
        return f"<WebsitePerformanceMetric(id={self.id}, category='{self.category}', sub_category='{self.sub_category}')>"
    

class ProductAnalysisData(Base):
    __tablename__ = 'product_analysis'

    id = Column(Integer, primary_key=True)
    product_id = Column(String(50), nullable=False, unique=True)
    product_name = Column(String(200), nullable=False)
    category = Column(String(50), nullable=False)
    price = Column(Numeric(10, 2), nullable=False)
    sales_volume = Column(Integer, default=0)
    stock_quantity = Column(Integer, default=0)
    view_count = Column(Integer, default=0)
    conversion_rate = Column(Numeric(5, 2), default=0.0)
    average_rating = Column(Numeric(3, 2), default=0.0)
    review_count = Column(Integer, default=0)
    profit_margin = Column(Numeric(5, 2), nullable=True)
    status = Column(Enum('active', 'inactive', 'discontinued'), default='active')

    def to_dict(self):
        return {
            "id": self.id,
            "product_id": self.product_id,
            "product_name": self.product_name,
            "category": self.category,
            "price": float(self.price),
            "sales_volume": self.sales_volume,
            "stock_quantity": self.stock_quantity,
            "view_count": self.view_count,
            "conversion_rate": float(self.conversion_rate),
            "average_rating": float(self.average_rating),
            "review_count": self.review_count,
            "profit_margin": float(self.profit_margin) if self.profit_margin is not None else None,
            "status": self.status
        }
    
class CustomerServiceMetric(Base):
    __tablename__ = 'customer_service_metrics'

    id = Column(Integer, primary_key=True)
    metric_category = Column(String(50), nullable=False)
    metric_name = Column(String(100), nullable=False)
    metric_value = Column(String(100), nullable=False)
    metric_unit = Column(String(20))
    trend_value = Column(String(20))
    display_order = Column(Integer)
    created_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(
        TIMESTAMP,
        server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')
    )

class FinancialMetric(Base):
    __tablename__ = 'financial_metrics'

    id = Column(Integer, primary_key=True)
    category = Column(String(50), nullable=False)
    metric_name = Column(String(100), nullable=False)
    metric_value = Column(String(50), nullable=False)
    trend = Column(String(20))
    unit = Column(String(20))
    display_order = Column(Integer)
    created_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(
        TIMESTAMP,
        server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')
    )

    def to_dict(self):
        return {
            "id": self.id,
            "category": self.category,
            "metric_name": self.metric_name,
            "metric_value": self.metric_value,
            "trend": self.trend,
            "unit": self.unit,
            "display_order": self.display_order
        }
    
class MobileCommerceMetric(Base):
    __tablename__ = 'mobile_commerce_metrics'

    id = Column(Integer, primary_key=True)
    category = Column(String(50), nullable=False)
    metric_name = Column(String(100), nullable=False)
    metric_value = Column(String(50), nullable=False)
    trend = Column(String(20))
    unit = Column(String(20))
    display_order = Column(Integer)
    created_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = Column(
        TIMESTAMP,
        server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')
    )

    def to_dict(self):
        return {
            "id": self.id,
            "category": self.category,
            "metric_name": self.metric_name,
            "metric_value": self.metric_value,
            "trend": self.trend,
            "unit": self.unit,
            "display_order": self.display_order
        }
    
class SocialCommerceMetric(Base):
    __tablename__ = 'social_commerce_metrics'   

    id = Column(Integer, primary_key=True)
    category = Column(String(50), nullable=False)
    metric_name = Column(String(100), nullable=False)
    metric_value = Column(String(50), nullable=False)
    trend = Column(String(20))
    unit = Column(String(20))
    display_order = Column(Integer)

    def to_dict(self):
        return {
            "id": self.id,
            "category": self.category,
            "metric_name": self.metric_name,
            "metric_value": self.metric_value,
            "trend": self.trend,
            "unit": self.unit,
            "display_order": self.display_order
        }
    
class OperationalMetric(Base):
    __tablename__ = 'operational_metrics'   

    id = Column(Integer, primary_key=True)
    category = Column(String(50), nullable=False)
    metric_name = Column(String(100), nullable=False)
    metric_value = Column(String(50), nullable=False)
    trend = Column(String(20))
    unit = Column(String(20))
    display_order = Column(Integer)

    def to_dict(self):
        return {
            "id": self.id,
            "category": self.category,
            "metric_name": self.metric_name,
            "metric_value": self.metric_value,
            "trend": self.trend,
            "unit": self.unit,
            "display_order": self.display_order
        }
    
class CompetitiveAnalysisMetric(Base):
    __tablename__ = 'competitive_analysis_metrics'
    
    id = Column(Integer, primary_key=True)
    category = Column(String(50), nullable=False)
    metric_name = Column(String(100), nullable=False)
    metric_value = Column(String(50), nullable=False)
    trend = Column(String(20))
    unit = Column(String(10))
    display_order = Column(Integer, nullable=False)

    def to_dict(self):
        return {
            "id": self.id,
            "category": self.category,
            "metric_name": self.metric_name,
            "metric_value": self.metric_value,
            "trend": self.trend,
            "unit": self.unit,
            "display_order": self.display_order
        }