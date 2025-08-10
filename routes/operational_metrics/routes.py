from flask import Blueprint, jsonify
from db.models import OperationalMetric
from db import get_db

bp = Blueprint('operational', __name__, url_prefix='/api/operational')

# Tüm operasyonel metrikleri listele
@bp.route('/data')
def get_all_operational_data():
    db = next(get_db())
    data = db.query(OperationalMetric).order_by(OperationalMetric.display_order).all()
    return jsonify([d.to_dict() for d in data])

# Belirli bir kategoriye ait metrikleri getir
@bp.route('/<category>')
def get_category_data(category):
    db = next(get_db())
    data = db.query(OperationalMetric)\
             .filter(OperationalMetric.category == category)\
             .order_by(OperationalMetric.display_order)\
             .all()
    return jsonify([d.to_dict() for d in data])

# Özel endpoint'ler (opsiyonel)
@bp.route('/order_processing')
def get_order_processing():
    return get_category_data('order_processing')

@bp.route('/order_steps')
def get_order_steps():
    return get_category_data('order_steps')

@bp.route('/facility_performance')
def get_facility_performance():
    return get_category_data('facility_performance')

@bp.route('/fulfillment')
def get_fulfillment():
    return get_category_data('fulfillment')

@bp.route('/returns')
def get_returns():
    return get_category_data('returns')

@bp.route('/supply_chain')
def get_supply_chain():
    return get_category_data('supply_chain')