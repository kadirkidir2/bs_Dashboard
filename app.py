from flask import Flask
from flask_cors import CORS
from routes.customer_analytics.routes import bp as customer_analytics_bp
from routes.sales_revenue.routes import bp as sales_revenue_bp
from routes.marketing_roi.routes import bp as marketing_roi_bp
from routes.inventory.routes import bp as inventory_bp
from routes.website_performance.routes import bp as websitePerformance_bp
from routes.product_analysis.routes import bp as productAnalysis_bp
from routes.customer_service.routes import bp as customerService_bp
from routes.financial_metrics.routes import bp as financialMetrics_bp
from routes.mobile_commerce.routes import bp as mobileCommerce_bp
from routes.social_commerce.routes import bp as socialCommerce_bp
from routes.operational_metrics.routes import bp as operational_bp
from routes.competitive_analysis.routes import bp as competitive_bp

app = Flask(__name__)

CORS(app)

app.register_blueprint(customer_analytics_bp, url_prefix='/api/dashboard')
app.register_blueprint(sales_revenue_bp, url_prefix='/api/sales_revenue')
app.register_blueprint(marketing_roi_bp, url_prefix='/api/marketing_roi')
app.register_blueprint(inventory_bp, url_prefix='/api/inventory')
app.register_blueprint(websitePerformance_bp, url_prefix='/api/website_performance')
app.register_blueprint(productAnalysis_bp, url_prefix='/api/product_analysis')
app.register_blueprint(customerService_bp, url_prefix='/api/customer_service')
app.register_blueprint(financialMetrics_bp, url_prefix='/api/financial_metrics')
app.register_blueprint(mobileCommerce_bp, url_prefix='/api/mobile')
app.register_blueprint(socialCommerce_bp, url_prefix='/api/social')
app.register_blueprint(operational_bp, url_prefix='/api/operational')
app.register_blueprint(competitive_bp, url_prefix='/api/competitive')

if __name__ == '__main__':
    app.run(debug=True)