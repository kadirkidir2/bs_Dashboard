from flask import Blueprint, jsonify
from db.models import WebsitePerformanceMetric
from db import get_db

bp = Blueprint('website_performance', __name__)

# Tüm verileri listeleme (Debug için)
@bp.route('/data')
def get_all_data():
    db = next(get_db())
    data = db.query(WebsitePerformanceMetric).all()
    return jsonify([d.to_dict() for d in data])

# @bp.route('/top-metrics')
# def get_top_metrics():
#     db = next(get_db())  # DB session'ı al
#     metrics = db.query(CustomerAnalyticsData).filter_by(type='top_metrics').all()
#     return jsonify([m.to_dict() for m in metrics])

