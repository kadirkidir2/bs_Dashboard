from flask import Blueprint, jsonify
from db.models import CompetitiveAnalysisMetric
from db import get_db

bp = Blueprint('competitive_analysis', __name__)

# Tüm metrikleri listele
@bp.route('/data')
def get_all_metrics():
    db = next(get_db())
    data = db.query(CompetitiveAnalysisMetric).order_by(CompetitiveAnalysisMetric.display_order).all()
    return jsonify([d.to_dict() for d in data])

# Kategoriye göre metrikleri getir
@bp.route('/<category>')
def get_category_metrics(category):
    db = next(get_db())
    data = db.query(CompetitiveAnalysisMetric)\
             .filter(CompetitiveAnalysisMetric.category == category)\
             .order_by(CompetitiveAnalysisMetric.display_order)\
             .all()
    return jsonify([d.to_dict() for d in data])

# Özel endpoint'ler
@bp.route('/market_position')
def get_market_position():
    return get_category_metrics('market_position')

@bp.route('/price_competitiveness')
def get_price_competitiveness():
    return get_category_metrics('price_competitiveness')

@bp.route('/competitors')
def get_competitors():
    return get_category_metrics('competitors')

@bp.route('/seo')
def get_seo_metrics():
    return get_category_metrics('seo')

@bp.route('/social_media')
def get_social_media():
    return get_category_metrics('social_media')

@bp.route('/advantages')
def get_competitive_advantages():
    return get_category_metrics('competitive_advantages')