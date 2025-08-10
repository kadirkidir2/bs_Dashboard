from flask import Blueprint, jsonify
from db.models import ProductAnalysisData
from db import get_db
from sqlalchemy import func

bp = Blueprint('product_analysis', __name__)

# Tüm ürünleri döndür
@bp.route('/data')
def get_all_data():
    db = next(get_db())
    data = db.query(ProductAnalysisData).all()
    return jsonify([p.to_dict() for p in data])

# Kategori performansını döndür
@bp.route('/category-performance')
def get_category_performance():
    db = next(get_db())
    result = db.query(
        ProductAnalysisData.category,
        func.count(ProductAnalysisData.id).label('product_count'),
        func.sum(ProductAnalysisData.sales_volume).label('total_sales'),
        func.avg(ProductAnalysisData.average_rating).label('avg_rating'),
        func.sum(ProductAnalysisData.view_count).label('total_views'),
        func.avg(ProductAnalysisData.price).label('avg_price')
    ).group_by(ProductAnalysisData.category).all()
    
    return jsonify([{
        'category': r.category,
        'product_count': r.product_count,
        'total_sales': r.total_sales or 0,
        'avg_rating': float(r.avg_rating) if r.avg_rating else 0,
        'total_views': r.total_views or 0,
        'avg_price': float(r.avg_price) if r.avg_price else 0
    } for r in result])

# Ürün yaşam döngüsü dağılımı
@bp.route('/lifecycle-distribution')
def get_lifecycle_distribution():
    db = next(get_db())
    result = db.query(
        ProductAnalysisData.status,
        func.count(ProductAnalysisData.id).label('count')
    ).group_by(ProductAnalysisData.status).all()
    
    return jsonify({
        'active': next((r.count for r in result if r.status == 'active'), 0),
        'inactive': next((r.count for r in result if r.status == 'inactive'), 0),
        'discontinued': next((r.count for r in result if r.status == 'discontinued'), 0)
    })

# En çok satan ürünler
@bp.route('/top-products/<int:limit>')
def get_top_products(limit):
    db = next(get_db())
    data = db.query(ProductAnalysisData).order_by(
        ProductAnalysisData.sales_volume.desc()
    ).limit(limit).all()
    return jsonify([p.to_dict() for p in data])

# Dashboard özet verileri
@bp.route('/dashboard-summary')
def get_dashboard_summary():
    db = next(get_db())
    
    # Aktif ürün sayısı
    active_count = db.query(ProductAnalysisData).filter(
        ProductAnalysisData.status == 'active'
    ).count()
    
    # Toplam ürün sayısı
    total_count = db.query(ProductAnalysisData).count()
    
    # Ortalama puan
    avg_rating = db.query(
        func.avg(ProductAnalysisData.average_rating)
    ).scalar() or 0
    
    # Ortalama fiyat
    avg_price = db.query(
        func.avg(ProductAnalysisData.price)
    ).filter(
        ProductAnalysisData.status == 'active'
    ).scalar() or 0
    
    # Toplam görüntülenme
    total_views = db.query(
        func.sum(ProductAnalysisData.view_count)
    ).scalar() or 0
    
    # Toplam satış hacmi
    total_sales = db.query(
        func.sum(ProductAnalysisData.sales_volume)
    ).scalar() or 0
    
    # Yüksek puanlı ürün sayısı (4.5 ve üzeri)
    high_rated_count = db.query(ProductAnalysisData).filter(
        ProductAnalysisData.average_rating >= 4.5
    ).count()
    
    # Geçen ay ile karşılaştırma için örnek değişim oranları
    # Gerçek uygulamada bunlar tarih bazlı sorgularla hesaplanmalı
    product_change = "+8.4%" if active_count > 0 else "0%"
    rating_change = "+0.2" if avg_rating > 0 else "0"
    price_change = "+15.7%" if avg_price > 0 else "0%"
    views_change = "+12.3%" if total_views > 0 else "0%"
    
    return jsonify({
        'aktif_urun_sayisi': active_count,
        'toplam_urun_sayisi': total_count,
        'aktif_urun_degisim': product_change,
        'ortalama_urun_puani': float(avg_rating),
        'ortalama_puan_degisim': rating_change,
        'ortalama_urun_fiyati': float(avg_price),
        'ortalama_fiyat_degisim': price_change,
        'toplam_urun_goruntulenme': total_views,
        'goruntulenme_degisim': views_change,
        'toplam_satis_hacmi': total_sales,
        'yuksek_puanli_urun_sayisi': high_rated_count
    })

# Kategori bazında detaylı analiz
@bp.route('/category-analysis/<string:category>')
def get_category_analysis(category):
    db = next(get_db())
    
    # Kategori bazında ürünler
    products = db.query(ProductAnalysisData).filter(
        ProductAnalysisData.category == category
    ).all()
    
    if not products:
        return jsonify({'error': 'Kategori bulunamadı'}), 404
    
    # Kategori istatistikleri
    total_products = len(products)
    active_products = len([p for p in products if p.status == 'active'])
    avg_rating = sum(p.average_rating for p in products if p.average_rating) / len([p for p in products if p.average_rating])
    avg_price = sum(p.price for p in products if p.price) / len([p for p in products if p.price])
    total_sales = sum(p.sales_volume for p in products if p.sales_volume)
    total_views = sum(p.view_count for p in products if p.view_count)
    
    return jsonify({
        'category': category,
        'total_products': total_products,
        'active_products': active_products,
        'avg_rating': avg_rating,
        'avg_price': avg_price,
        'total_sales': total_sales,
        'total_views': total_views,
        'products': [p.to_dict() for p in products[:10]]  # İlk 10 ürün
    })

# Ürün durumu bazlı istatistikler
@bp.route('/status-statistics')
def get_status_statistics():
    db = next(get_db())
    
    result = db.query(
        ProductAnalysisData.status,
        func.count(ProductAnalysisData.id).label('count'),
        func.avg(ProductAnalysisData.price).label('avg_price'),
        func.avg(ProductAnalysisData.average_rating).label('avg_rating'),
        func.sum(ProductAnalysisData.sales_volume).label('total_sales')
    ).group_by(ProductAnalysisData.status).all()
    
    return jsonify([{
        'status': r.status,
        'count': r.count,
        'avg_price': float(r.avg_price) if r.avg_price else 0,
        'avg_rating': float(r.avg_rating) if r.avg_rating else 0,
        'total_sales': r.total_sales or 0
    } for r in result])

# Fiyat aralığı analizi
@bp.route('/price-range-analysis')
def get_price_range_analysis():
    db = next(get_db())
    
    # Fiyat aralıklarını tanımla
    price_ranges = [
        (0, 100, '0-100₺'),
        (100, 500, '100-500₺'),
        (500, 1000, '500-1000₺'),
        (1000, 5000, '1000-5000₺'),
        (5000, float('inf'), '5000₺+')
    ]
    
    result = []
    for min_price, max_price, label in price_ranges:
        if max_price == float('inf'):
            count = db.query(ProductAnalysisData).filter(
                ProductAnalysisData.price >= min_price
            ).count()
        else:
            count = db.query(ProductAnalysisData).filter(
                ProductAnalysisData.price >= min_price,
                ProductAnalysisData.price < max_price
            ).count()
        
        result.append({
            'range': label,
            'count': count,
            'min_price': min_price,
            'max_price': max_price if max_price != float('inf') else None
        })
    
    return jsonify(result)

# En yüksek puanlı ürünler
@bp.route('/highest-rated/<int:limit>')
def get_highest_rated_products(limit):
    db = next(get_db())
    data = db.query(ProductAnalysisData).filter(
        ProductAnalysisData.average_rating.isnot(None)
    ).order_by(
        ProductAnalysisData.average_rating.desc()
    ).limit(limit).all()
    
    return jsonify([p.to_dict() for p in data])

# En çok görüntülenen ürünler
@bp.route('/most-viewed/<int:limit>')
def get_most_viewed_products(limit):
    db = next(get_db())
    data = db.query(ProductAnalysisData).filter(
        ProductAnalysisData.view_count.isnot(None)
    ).order_by(
        ProductAnalysisData.view_count.desc()
    ).limit(limit).all()
    
    return jsonify([p.to_dict() for p in data])