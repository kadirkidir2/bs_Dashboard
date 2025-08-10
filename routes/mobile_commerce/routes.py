from flask import Blueprint, jsonify
from db.models import MobileCommerceMetric
from db import get_db

bp = Blueprint('mobile_commerce', __name__, url_prefix='/api/mobile')

# Tüm veriler (Debug için)
@bp.route('/data')
def get_all_data():
    db = next(get_db())
    data = db.query(MobileCommerceMetric).order_by(MobileCommerceMetric.display_order).all()
    return jsonify([d.to_dict() for d in data])

# Trafik Metrikleri
@bp.route('/traffic')
def get_traffic_metrics():
    db = next(get_db())
    metrics = db.query(MobileCommerceMetric).filter_by(category='traffic').order_by(MobileCommerceMetric.display_order).all()
    return jsonify([m.to_dict() for m in metrics])

# Platform Performansı
@bp.route('/platform_performance')
def get_platform_performance():
    db = next(get_db())
    metrics = db.query(MobileCommerceMetric).filter_by(category='platform_performance').order_by(MobileCommerceMetric.display_order).all()
    return jsonify([m.to_dict() for m in metrics])

# Uygulama Metrikleri
@bp.route('/app_metrics')
def get_app_metrics():
    db = next(get_db())
    metrics = db.query(MobileCommerceMetric).filter_by(category='app_metrics').order_by(MobileCommerceMetric.display_order).all()
    return jsonify([m.to_dict() for m in metrics])

# UX Metrikleri
@bp.route('/ux_metrics')
def get_ux_metrics():
    db = next(get_db())
    metrics = db.query(MobileCommerceMetric).filter_by(category='ux_metrics').order_by(MobileCommerceMetric.display_order).all()
    return jsonify([m.to_dict() for m in metrics])

# Bildirim Metrikleri
@bp.route('/notifications')
def get_notification_metrics():
    db = next(get_db())
    metrics = db.query(MobileCommerceMetric).filter_by(category='notifications').order_by(MobileCommerceMetric.display_order).all()
    return jsonify([m.to_dict() for m in metrics])

# Özel Gruplama: Ana Performans Göstergeleri
@bp.route('/key_metrics')
def get_key_metrics():
    db = next(get_db())
    metrics = db.query(MobileCommerceMetric).filter(
        MobileCommerceMetric.metric_name.in_([
            'mobile_traffic_ratio',
            'mobile_conversion_rate',
            'app_downloads',
            'active_users'
        ])
    ).order_by(MobileCommerceMetric.display_order).all()
    return jsonify([m.to_dict() for m in metrics])

# Özel Gruplama: Platform Karşılaştırması
@bp.route('/platform_comparison')
def get_platform_comparison():
    db = next(get_db())
    metrics = db.query(MobileCommerceMetric).filter(
        MobileCommerceMetric.metric_name.in_([
            'ios_app_performance',
            'android_app_performance',
            'tablet_performance'
        ])
    ).order_by(MobileCommerceMetric.display_order).all()
    return jsonify([m.to_dict() for m in metrics])