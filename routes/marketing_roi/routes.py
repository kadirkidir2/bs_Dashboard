from flask import Blueprint, jsonify
from db.models import MarketingRoiData
from db import get_db

bp = Blueprint('marketing_roi', __name__)

# Tüm verileri listeleme (Debug için)
@bp.route('/data')
def get_all_data():
    db = next(get_db())
    data = db.query(MarketingRoiData).all()
    return jsonify([d.to_dict() for d in data])

@bp.route('/main_metric')
def get_main_metrics():
    db = next(get_db())
    metrics = db.query(MarketingRoiData).filter_by(type='main_metric').all()
    return jsonify([m.to_dict() for m in metrics])


@bp.route('/channel_roas')
def get_channel_roas():
    db = next(get_db())
    roas = db.query(MarketingRoiData).filter_by(type='channel_roas').all()
    return jsonify([t.to_dict() for t in roas])

@bp.route('/best_campaign')
def get_best_campaign():
    db = next(get_db())
    trends = db.query(MarketingRoiData).filter_by(type='best_campaign').all()
    return jsonify([t.to_dict() for t in trends])

@bp.route('/channel_performance')
def get_channel_performance():
    db = next(get_db())
    performance = db.query(MarketingRoiData).filter_by(type='channel_performance').all()
    return jsonify([t.to_dict() for t in performance])

@bp.route('/key_metric')
def get_key_metric():
    db = next(get_db())
    keymetric = db.query(MarketingRoiData).filter_by(type='key_metric').all()
    return jsonify([t.to_dict() for t in keymetric])

@bp.route('/budget_distribution')
def get_budget_distribution():
    db = next(get_db())
    budget = db.query(MarketingRoiData).filter_by(type='budget_distribution').all()
    return jsonify([t.to_dict() for t in budget])
