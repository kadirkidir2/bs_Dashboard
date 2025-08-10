from flask import Blueprint, jsonify
from db.models import CustomerAnalyticsData
from db import get_db

bp = Blueprint('customer_analytics', __name__)

# Tüm verileri listeleme (Debug için)
@bp.route('/data')
def get_all_data():
    db = next(get_db())
    data = db.query(CustomerAnalyticsData).all()
    return jsonify([d.to_dict() for d in data])

@bp.route('/top-metrics')
def get_top_metrics():
    db = next(get_db())  # DB session'ı al
    metrics = db.query(CustomerAnalyticsData).filter_by(type='top_metrics').all()
    return jsonify([m.to_dict() for m in metrics])

# Diğer endpoint'leri de buraya taşıyın (segments, journey vb.)
@bp.route('/segments')
def get_segments():
    db = next(get_db())
    segments = db.query(CustomerAnalyticsData).filter_by(type='segments').all()
    labels = [s.name for s in segments]
    values = [float(s.numeric_value) for s in segments]
    colors = [s.color for s in segments]
    return jsonify({"labels": labels, "values": values, "colors":colors})

# journey verilerini döndürür (Bar grafik veya funnel için)
@bp.route('/journey')
def get_journey():
    db= next(get_db())
    journey_data = db.query(CustomerAnalyticsData).filter_by(type='journey').order_by(CustomerAnalyticsData.id).all()
    labels = [j.name for j in journey_data]
    values = [float(j.numeric_value) for j in journey_data]
    colors = [j.color for j in journey_data]
    return jsonify({"labels": labels, "values": values, "colors": colors})

# channels verilerini döndürür (Bar veya pasta grafik için)
@bp.route('/channels')
def get_channels():
    db = next(get_db())
    channels_data = db.query(CustomerAnalyticsData).filter_by(type='channels').order_by(CustomerAnalyticsData.id).all()
    labels = [c.name for c in channels_data]
    values = [float(c.numeric_value) for c in channels_data]
    return jsonify({"labels": labels, "values": values})

# satisfaction verilerini döndürür
@bp.route('/satisfaction')
def get_satisfaction():
    db = next(get_db())
    satisfaction_metrics = db.query(CustomerAnalyticsData).filter_by(type='satisfaction').all()
    return jsonify([s.to_dict() for s in satisfaction_metrics])

# activities verilerini döndürür
@bp.route('/activities')
def get_activities():
    db = next(get_db())
    activities_data = db.query(CustomerAnalyticsData).filter_by(type='activities').all()
    labels = [a.name for a in activities_data]
    values = [float(a.numeric_value) for a in activities_data]
    return jsonify({"labels": labels, "values": values})
