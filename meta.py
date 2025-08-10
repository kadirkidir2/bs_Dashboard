
import requests
import json
from datetime import datetime, timedelta
import time

# Konfigürasyon
ACCESS_TOKEN = "EAAKTyfl7uPIBPN2uwAYiZCypL8uoRnEEzOukOzzKhIPs4hzurCYZCEivfduOZAZAZC7nFoIyaQuW2Rp0hZC8NOZC0yUtakDSM7dlMft8w2i6FH6sPn6bSsAXPL4X4AIBkjlxLQ2QEldGm60xymiuZAtAI7TMbVUqxgj6S9IEV75u4meXOqCBbcnZAt4ZAcnZAkF3AZDZD"  # Buraya token'ınızı yazın

class MetaKPICollector:
    def __init__(self, access_token):
        self.access_token = access_token
        self.page_id = None
        self.base_url = "https://graph.facebook.com/v18.0"
        
    def get_page_id(self):
        """Token'dan sayfa ID'sini otomatik bul"""
        print("🔍 Sayfa ID'si bulunuyor...")
        
        # Önce me endpoint'ini dene
        me_data = self.make_request("me", {'fields': 'id,name'})
        if me_data and 'id' in me_data:
            self.page_id = me_data['id']
            print(f"✅ Sayfa bulundu: {me_data.get('name', 'N/A')} (ID: {self.page_id})")
            return self.page_id
        
        # Eğer me çalışmazsa, accounts endpoint'ini dene
        accounts_data = self.make_request("me/accounts", {'fields': 'id,name,access_token'})
        if accounts_data and 'data' in accounts_data and accounts_data['data']:
            first_page = accounts_data['data'][0]
            self.page_id = first_page['id']
            print(f"✅ İlk sayfa seçildi: {first_page.get('name', 'N/A')} (ID: {self.page_id})")
            
            # Sayfa access token'ı varsa güncelle
            if 'access_token' in first_page:
                self.access_token = first_page['access_token']
                print("🔄 Sayfa access token'ı kullanılıyor")
            
            return self.page_id
        
        print("❌ Sayfa ID'si bulunamadı!")
        return None
        
    def make_request(self, endpoint, params=None):
        """API isteği yap"""
        if params is None:
            params = {}
        params['access_token'] = self.access_token
        
        try:
            response = requests.get(f"{self.base_url}/{endpoint}", params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API Error: {e}")
            return {}
    
    def get_page_info(self):
        """Sayfa temel bilgileri"""
        print("📊 Sayfa bilgileri alınıyor...")
        fields = 'name,followers_count,fan_count,engagement,about,website,category,link,picture'
        return self.make_request(self.page_id, {'fields': fields})
    
    def get_instagram_account(self):
        """Instagram hesap bilgisi"""
        print("📱 Instagram hesap bilgisi kontrol ediliyor...")
        return self.make_request(self.page_id, {'fields': 'instagram_business_account'})
    
    def get_recent_posts(self, limit=25):
        """Son postları çek"""
        print(f"📝 Son {limit} post alınıyor...")
        fields = 'message,created_time,likes.summary(true),comments.summary(true),shares,reactions.summary(true)'
        return self.make_request(f"{self.page_id}/posts", {
            'fields': fields,
            'limit': limit
        })
    
    def get_page_insights(self, days=30):
        """Sayfa insights (son 30 gün)"""
        print(f"📈 Son {days} günün insights verileri alınıyor...")
        since = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        until = datetime.now().strftime('%Y-%m-%d')
        
        metrics = [
            'page_impressions',
            'page_reach',
            'page_engaged_users',
            'page_post_engagements',
            'page_fans',
            'page_fan_adds',
            'page_fan_removes'
        ]
        
        return self.make_request(f"{self.page_id}/insights", {
            'metric': ','.join(metrics),
            'period': 'day',
            'since': since,
            'until': until
        })
    
    def get_instagram_media(self, instagram_account_id, limit=25):
        """Instagram medya verilerini çek"""
        print(f"📸 Instagram medya verileri alınıyor...")
        fields = 'id,caption,media_type,media_url,permalink,timestamp,like_count,comments_count'
        return self.make_request(f"{instagram_account_id}/media", {
            'fields': fields,
            'limit': limit
        })
    
    def get_instagram_insights(self, instagram_account_id, days=30):
        """Instagram insights"""
        print("📊 Instagram insights alınıyor...")
        since = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        until = datetime.now().strftime('%Y-%m-%d')
        
        metrics = [
            'impressions',
            'reach',
            'profile_views',
            'follower_count'
        ]
        
        return self.make_request(f"{instagram_account_id}/insights", {
            'metric': ','.join(metrics),
            'period': 'day',
            'since': since,
            'until': until
        })
    
    def calculate_facebook_kpis(self, posts_data, page_info, insights_data):
        """Facebook KPI'larını hesapla"""
        kpis = {
            'facebook': {
                'page_info': {},
                'engagement_metrics': {},
                'content_performance': {},
                'audience_growth': {}
            }
        }
        
        # Sayfa bilgileri
        if page_info:
            kpis['facebook']['page_info'] = {
                'name': page_info.get('name', ''),
                'followers': page_info.get('followers_count', 0),
                'fans': page_info.get('fan_count', 0),
                'category': page_info.get('category', ''),
                'website': page_info.get('website', '')
            }
        
        # Post analizi
        if posts_data and 'data' in posts_data:
            posts = posts_data['data']
            total_likes = sum(post.get('likes', {}).get('summary', {}).get('total_count', 0) for post in posts)
            total_comments = sum(post.get('comments', {}).get('summary', {}).get('total_count', 0) for post in posts)
            total_shares = sum(post.get('shares', {}).get('count', 0) for post in posts)
            total_reactions = sum(post.get('reactions', {}).get('summary', {}).get('total_count', 0) for post in posts)
            
            post_count = len(posts)
            total_engagement = total_likes + total_comments + total_shares
            
            kpis['facebook']['content_performance'] = {
                'total_posts': post_count,
                'avg_likes_per_post': round(total_likes / post_count, 2) if post_count > 0 else 0,
                'avg_comments_per_post': round(total_comments / post_count, 2) if post_count > 0 else 0,
                'avg_shares_per_post': round(total_shares / post_count, 2) if post_count > 0 else 0,
                'avg_reactions_per_post': round(total_reactions / post_count, 2) if post_count > 0 else 0,
                'total_engagement': total_engagement
            }
            
            # Engagement rate hesapla
            followers = page_info.get('followers_count', 0)
            if followers > 0:
                kpis['facebook']['engagement_metrics']['engagement_rate'] = round((total_engagement / followers) * 100, 2)
        
        # Insights analizi
        if insights_data and 'data' in insights_data:
            insights = insights_data['data']
            
            # Metrics'i organize et
            metrics_summary = {}
            for metric in insights:
                metric_name = metric['name']
                values = metric.get('values', [])
                if values:
                    total_value = sum(item['value'] for item in values if item['value'] is not None)
                    avg_value = total_value / len(values)
                    metrics_summary[metric_name] = {
                        'total': total_value,
                        'average': round(avg_value, 2)
                    }
            
            kpis['facebook']['engagement_metrics'].update(metrics_summary)
        
        return kpis
    
    def calculate_instagram_kpis(self, media_data, insights_data):
        """Instagram KPI'larını hesapla"""
        kpis = {
            'instagram': {
                'content_performance': {},
                'engagement_metrics': {},
                'media_types': {}
            }
        }
        
        if media_data and 'data' in media_data:
            media = media_data['data']
            
            total_likes = sum(item.get('like_count', 0) for item in media)
            total_comments = sum(item.get('comments_count', 0) for item in media)
            media_count = len(media)
            
            # Media türlerine göre grupla
            media_types = {}
            for item in media:
                media_type = item.get('media_type', 'UNKNOWN')
                if media_type not in media_types:
                    media_types[media_type] = 0
                media_types[media_type] += 1
            
            kpis['instagram']['content_performance'] = {
                'total_posts': media_count,
                'avg_likes_per_post': round(total_likes / media_count, 2) if media_count > 0 else 0,
                'avg_comments_per_post': round(total_comments / media_count, 2) if media_count > 0 else 0,
                'total_engagement': total_likes + total_comments
            }
            
            kpis['instagram']['media_types'] = media_types
        
        if insights_data and 'data' in insights_data:
            insights = insights_data['data']
            
            metrics_summary = {}
            for metric in insights:
                metric_name = metric['name']
                values = metric.get('values', [])
                if values:
                    total_value = sum(item['value'] for item in values if item['value'] is not None)
                    avg_value = total_value / len(values)
                    metrics_summary[metric_name] = {
                        'total': total_value,
                        'average': round(avg_value, 2)
                    }
            
            kpis['instagram']['engagement_metrics'] = metrics_summary
        
        return kpis
    
    def collect_all_data(self):
        """Tüm verileri topla ve KPI'ları hesapla"""
        print("🚀 Meta KPI verileri toplanıyor...\n")
        
        # İlk önce sayfa ID'sini bul
        if not self.get_page_id():
            print("❌ Sayfa ID'si bulunamadığı için işlem durduruluyor")
            return {}, {}
        
        all_data = {}
        
        # Facebook verileri
        page_info = self.get_page_info()
        all_data['page_info'] = page_info
        
        posts_data = self.get_recent_posts()
        all_data['posts'] = posts_data
        
        insights_data = self.get_page_insights()
        all_data['insights'] = insights_data
        
        # Instagram kontrolü
        instagram_info = self.get_instagram_account()
        instagram_account_id = None
        
        if instagram_info and 'instagram_business_account' in instagram_info:
            instagram_account_id = instagram_info['instagram_business_account']['id']
            print(f"✅ Instagram hesabı bulundu: {instagram_account_id}")
            
            instagram_media = self.get_instagram_media(instagram_account_id)
            all_data['instagram_media'] = instagram_media
            
            instagram_insights = self.get_instagram_insights(instagram_account_id)
            all_data['instagram_insights'] = instagram_insights
        else:
            print("❌ Instagram hesabı bulunamadı")
        
        # KPI hesaplamaları
        print("\n📊 KPI'lar hesaplanıyor...")
        
        facebook_kpis = self.calculate_facebook_kpis(posts_data, page_info, insights_data)
        all_kpis = facebook_kpis
        
        if instagram_account_id:
            instagram_kpis = self.calculate_instagram_kpis(
                all_data.get('instagram_media', {}),
                all_data.get('instagram_insights', {})
            )
            all_kpis.update(instagram_kpis)
        
        return all_data, all_kpis
    
    def print_kpis(self, kpis):
        """KPI'ları güzel formatta yazdır"""
        print("\n" + "="*60)
        print("📊 META KPI RAPORU")
        print("="*60)
        
        # Facebook KPI'ları
        if 'facebook' in kpis:
            fb = kpis['facebook']
            print("\n🔵 FACEBOOK KPI'LARI:")
            print("-" * 40)
            
            if 'page_info' in fb:
                info = fb['page_info']
                print(f"📄 Sayfa Adı: {info.get('name', 'N/A')}")
                print(f"👥 Takipçi Sayısı: {info.get('followers', 0):,}")
                print(f"👍 Beğeni Sayısı: {info.get('fans', 0):,}")
            
            if 'content_performance' in fb:
                content = fb['content_performance']
                print(f"\n📝 İçerik Performansı:")
                print(f"   • Toplam Post: {content.get('total_posts', 0)}")
                print(f"   • Ortalama Beğeni/Post: {content.get('avg_likes_per_post', 0)}")
                print(f"   • Ortalama Yorum/Post: {content.get('avg_comments_per_post', 0)}")
                print(f"   • Ortalama Paylaşım/Post: {content.get('avg_shares_per_post', 0)}")
                print(f"   • Toplam Engagement: {content.get('total_engagement', 0):,}")
            
            if 'engagement_metrics' in fb:
                engagement = fb['engagement_metrics']
                if 'engagement_rate' in engagement:
                    print(f"   • Engagement Rate: %{engagement['engagement_rate']}")
        
        # Instagram KPI'ları
        if 'instagram' in kpis:
            ig = kpis['instagram']
            print("\n🔴 INSTAGRAM KPI'LARI:")
            print("-" * 40)
            
            if 'content_performance' in ig:
                content = ig['content_performance']
                print(f"📝 İçerik Performansı:")
                print(f"   • Toplam Post: {content.get('total_posts', 0)}")
                print(f"   • Ortalama Beğeni/Post: {content.get('avg_likes_per_post', 0)}")
                print(f"   • Ortalama Yorum/Post: {content.get('avg_comments_per_post', 0)}")
                print(f"   • Toplam Engagement: {content.get('total_engagement', 0):,}")
            
            if 'media_types' in ig:
                media_types = ig['media_types']
                print(f"\n📸 Medya Türleri:")
                for media_type, count in media_types.items():
                    print(f"   • {media_type}: {count}")
        
        print("\n" + "="*60)

def main():
    # KPI Collector'ı başlat
    collector = MetaKPICollector(ACCESS_TOKEN)
    
    # Tüm verileri topla
    raw_data, kpis = collector.collect_all_data()
    
    # Eğer veri yoksa çık
    if not kpis:
        print("❌ Veri alınamadı, işlem sonlandırılıyor")
        return
    
    # KPI'ları yazdır
    collector.print_kpis(kpis)
    
    # JSON olarak kaydet
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    with open(f'meta_kpis_{timestamp}.json', 'w', encoding='utf-8') as f:
        json.dump(kpis, f, indent=2, ensure_ascii=False)
    
    with open(f'meta_raw_data_{timestamp}.json', 'w', encoding='utf-8') as f:
        json.dump(raw_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Veriler kaydedildi:")
    print(f"   • KPI'lar: meta_kpis_{timestamp}.json")
    print(f"   • Ham veri: meta_raw_data_{timestamp}.json")

if __name__ == "__main__":
    main()