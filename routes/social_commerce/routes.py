from flask import Blueprint, jsonify
from db.models import SocialCommerceMetric
from db import get_db

bp = Blueprint('social_commerce', __name__)


# Tüm metrikleri listele
@bp.route('/data')
def get_all_social_data():
    db = next(get_db())
    data = db.query(SocialCommerceMetric).order_by(SocialCommerceMetric.display_order).all()
    return jsonify([d.to_dict() for d in data])


# Belirli bir kategoriye ait metrikleri getir
@bp.route('/<category>')
def get_category_data(category):
    db = next(get_db())
    data = db.query(SocialCommerceMetric)\
             .filter(SocialCommerceMetric.category == category)\
             .order_by(SocialCommerceMetric.display_order)\
             .all()
    return jsonify([d.to_dict() for d in data])
