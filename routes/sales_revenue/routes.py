from flask import Blueprint, jsonify
from db.models import SalesRevenueData
from db import get_db

bp = Blueprint('sales_revenue', __name__)

# Tüm verileri listeleme (Debug için)
@bp.route('/data')
def get_all_data():
    db = next(get_db())
    data = db.query(SalesRevenueData).all()
    return jsonify([d.to_dict() for d in data])

@bp.route('/top_metrics')
def get_top_metrics():
    db = next(get_db())
    metrics = db.query(SalesRevenueData).filter_by(type='top_metrics').all()
    return jsonify([m.to_dict() for m in metrics])


@bp.route('/revenue_trend')
def get_revenue_trend():
    db = next(get_db())
    trends = db.query(SalesRevenueData).filter_by(type='revenue_trend').all()
    return jsonify([t.to_dict() for t in trends])

@bp.route('/top_products')
def get_top_products():
    db = next(get_db())
    products = db.query(SalesRevenueData)\
                .filter_by(type='top_products')\
                .order_by(SalesRevenueData.value.desc())\
                .limit(5)\
                .all()
    return jsonify([p.to_dict() for p in products])