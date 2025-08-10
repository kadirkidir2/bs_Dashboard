from flask import Blueprint, jsonify
from db.models import CustomerServiceMetric
from db import get_db

bp = Blueprint('customer_service', __name__, url_prefix='/api/customer_service')

# Tüm veriler
@bp.route('/data')
def get_all_data():
    db = next(get_db())
    data = db.query(CustomerServiceMetric).all()
    return jsonify([{
        'id': item.id,
        'category': item.metric_category,
        'name': item.metric_name,
        'value': item.metric_value,
        'unit': item.metric_unit,
        'trend': item.trend_value,
        'order': item.display_order
    } for item in data])

@bp.route('/<category>')
def get_by_category(category):
    db = next(get_db())
    data = db.query(CustomerServiceMetric).filter(
        CustomerServiceMetric.metric_category == category
    ).order_by(
        CustomerServiceMetric.display_order
    ).all()
    
    return jsonify([{
        'name': item.metric_name,
        'value': item.metric_value,
        'unit': item.metric_unit,
        'trend': item.trend_value
    } for item in data])