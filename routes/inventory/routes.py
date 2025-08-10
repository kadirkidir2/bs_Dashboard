from flask import Blueprint, jsonify
from db.models import InventoryData
from db import get_db

bp = Blueprint('inventory', __name__, url_prefix='http://localhost:5000/api/inventory')

# Tüm verileri listeleme (Debug için)
@bp.route('/data')
def get_all_data():
    db = next(get_db())
    data = db.query(InventoryData).all()
    return jsonify([d.to_dict() for d in data])

@bp.route('/main_metrics')
def get_main_metrics():
    db = next(get_db())
    metrics = db.query(InventoryData).filter_by(type='main_metric').all()
    return jsonify([m.to_dict() for m in metrics])

@bp.route('/category_stocks')
def get_category_stocks():
    db = next(get_db())
    stocks = db.query(InventoryData).filter_by(type='category_stock').all()
    return jsonify([s.to_dict() for s in stocks])

@bp.route('/fast_moving')
def get_fast_moving():
    db = next(get_db())
    items = db.query(InventoryData).filter_by(type='fast_moving').all()
    return jsonify([i.to_dict() for i in items])

@bp.route('/slow_moving')
def get_slow_moving():
    db = next(get_db())
    items = db.query(InventoryData).filter_by(type='slow_moving').all()
    return jsonify([i.to_dict() for i in items])

@bp.route('/warehouse_metrics')
def get_warehouse_metrics():
    db = next(get_db())
    metrics = db.query(InventoryData).filter_by(type='warehouse_metric').all()
    return jsonify([m.to_dict() for m in metrics])

@bp.route('/supply_chain')
def get_supply_chain():
    db = next(get_db())
    metrics = db.query(InventoryData).filter_by(type='supply_chain').all()
    return jsonify([m.to_dict() for m in metrics])

@bp.route('/stock_trends')
def get_stock_trends():
    db = next(get_db())
    trends = db.query(InventoryData).filter_by(type='stock_trend').order_by(InventoryData.id).all()
    return jsonify([t.to_dict() for t in trends])