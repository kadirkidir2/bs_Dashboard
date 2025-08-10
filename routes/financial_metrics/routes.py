from flask import Blueprint, jsonify
from db.models import FinancialMetric
from db import get_db

bp = Blueprint('financial', __name__, url_prefix='/api/financial')

# Tüm finansal veriler
@bp.route('/data')
def get_all_data():
    db = next(get_db())
    data = db.query(FinancialMetric).order_by(FinancialMetric.display_order).all()
    return jsonify([item.to_dict() for item in data])

# Kategoriye göre filtreleme
@bp.route('/<category>')
def get_by_category(category):
    db = next(get_db())
    data = db.query(FinancialMetric).filter(
        FinancialMetric.category == category
    ).order_by(
        FinancialMetric.display_order
    ).all()
    
    return jsonify([item.to_dict() for item in data])

# Özel endpoint: Karlılık metrikleri
@bp.route('/profitability')
def get_profitability():
    db = next(get_db())
    data = db.query(FinancialMetric).filter(
        FinancialMetric.category.in_(['profitability', 'profitability_metrics'])
    ).order_by(
        FinancialMetric.display_order
    ).all()
    
    return jsonify([item.to_dict() for item in data])