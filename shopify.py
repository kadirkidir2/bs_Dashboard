import requests
import json
from typing import Dict, List, Optional
import time

class ShopifyAPI:
    def __init__(self, shop_name: str, access_token: str):
        """
        Shopify API client'ını başlatır
        
        Args:
            shop_name: Shopify mağaza adı (örn: 'mystore' for mystore.myshopify.com)
            access_token: Admin API access token (shpat_ ile başlayan)
        """
        self.shop_name = shop_name
        self.access_token = access_token
        self.base_url = f"https://{shop_name}.myshopify.com/admin/api/2023-10"
        self.headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json"
        }
    
    def _make_request(self, endpoint: str, method: str = "GET", data: dict = None) -> dict:
        """
        API isteği yapar ve sonucu döndürür
        """
        url = f"{self.base_url}/{endpoint}"
        
        try:
            if method == "GET":
                response = requests.get(url, headers=self.headers)
            elif method == "POST":
                response = requests.post(url, headers=self.headers, json=data)
            elif method == "PUT":
                response = requests.put(url, headers=self.headers, json=data)
            elif method == "DELETE":
                response = requests.delete(url, headers=self.headers)
            
            # Rate limiting kontrolü (Shopify 2 req/sec limiti)
            if response.status_code == 429:
                print("Rate limit aşıldı, 1 saniye bekleniyor...")
                time.sleep(1)
                return self._make_request(endpoint, method, data)
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"API isteği hatası: {e}")
            return {}
    
    def get_products(self, limit: int = 50, since_id: int = None) -> List[Dict]:
        """
        Ürünleri getirir
        
        Args:
            limit: Maksimum ürün sayısı (1-250)
            since_id: Bu ID'den sonraki ürünleri getir
        """
        endpoint = f"products.json?limit={limit}"
        if since_id:
            endpoint += f"&since_id={since_id}"
        
        response = self._make_request(endpoint)
        return response.get("products", [])
    
    def get_all_products(self) -> List[Dict]:
        """
        Tüm ürünleri getirir (pagination ile)
        """
        all_products = []
        since_id = None
        
        while True:
            products = self.get_products(limit=250, since_id=since_id)
            if not products:
                break
            
            all_products.extend(products)
            since_id = products[-1]["id"]
            print(f"{len(all_products)} ürün alındı...")
            
            # Rate limiting için kısa bekleme
            time.sleep(0.5)
        
        return all_products
    
    def get_orders(self, status: str = "any", limit: int = 50) -> List[Dict]:
        """
        Siparişleri getirir
        
        Args:
            status: open, closed, cancelled, any
            limit: Maksimum sipariş sayısı
        """
        endpoint = f"orders.json?status={status}&limit={limit}"
        response = self._make_request(endpoint)
        return response.get("orders", [])
    
    def get_customers(self, limit: int = 50) -> List[Dict]:
        """
        Müşterileri getirir
        """
        endpoint = f"customers.json?limit={limit}"
        response = self._make_request(endpoint)
        return response.get("customers", [])
    
    def get_inventory_levels(self, location_id: int = None) -> List[Dict]:
        """
        Stok seviyelerini getirir
        """
        endpoint = "inventory_levels.json"
        if location_id:
            endpoint += f"?location_ids={location_id}"
        
        response = self._make_request(endpoint)
        return response.get("inventory_levels", [])
    
    def get_shop_info(self) -> Dict:
        """
        Mağaza bilgilerini getirir
        """
        response = self._make_request("shop.json")
        return response.get("shop", {})

# Kullanım örneği
def main():
    # API bilgilerinizi buraya girin
    SHOP_NAME = "roboskop-test-store"  # .myshopify.com olmadan
    ACCESS_TOKEN = "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"
    
    # API client'ını oluştur
    shopify = ShopifyAPI(SHOP_NAME, ACCESS_TOKEN)
    
    try:
        # Mağaza bilgilerini al
        print("=== Mağaza Bilgileri ===")
        shop_info = shopify.get_shop_info()
        if shop_info:
            print(f"Mağaza Adı: {shop_info.get('name', 'N/A')}")
            print(f"Email: {shop_info.get('email', 'N/A')}")
            print(f"Domain: {shop_info.get('domain', 'N/A')}")
            print(f"Para Birimi: {shop_info.get('currency', 'N/A')}")
        
        # Ürünleri al
        print("\n=== Ürünler ===")
        products = shopify.get_products(limit=5)  # İlk 5 ürün
        for product in products:
            print(f"ID: {product['id']}")
            print(f"Başlık: {product['title']}")
            print(f"Fiyat: {product['variants'][0]['price'] if product['variants'] else 'N/A'}")
            print(f"Stok: {product['variants'][0]['inventory_quantity'] if product['variants'] else 'N/A'}")
            print("-" * 40)
        
        # Siparişleri al
        print("\n=== Son Siparişler ===")
        orders = shopify.get_orders(limit=3)
        for order in orders:
            print(f"Sipariş ID: {order['id']}")
            print(f"Müşteri: {order.get('customer', {}).get('first_name', 'N/A')} {order.get('customer', {}).get('last_name', '')}")
            print(f"Toplam: {order['total_price']} {order['currency']}")
            print(f"Durum: {order['financial_status']}")
            print("-" * 40)
        
        # Müşterileri al
        print("\n=== Müşteriler ===")
        customers = shopify.get_customers(limit=3)
        for customer in customers:
            print(f"ID: {customer['id']}")
            print(f"Ad: {customer['first_name']} {customer['last_name']}")
            print(f"Email: {customer['email']}")
            print(f"Toplam Sipariş: {customer['orders_count']}")
            print("-" * 40)
    
    except Exception as e:
        print(f"Hata oluştu: {e}")

# Veri analizi için pandas kullanımı
def analyze_with_pandas():
    """
    Pandas ile veri analizi örneği
    """
    try:
        import pandas as pd
        
        SHOP_NAME = "roboskop-test-store"
        ACCESS_TOKEN = "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"
        
        shopify = ShopifyAPI(SHOP_NAME, ACCESS_TOKEN)
        
        # Ürünleri DataFrame'e çevir
        products = shopify.get_products(limit=50)
        
        if products:
            # Ürün verilerini düzenle
            product_data = []
            for product in products:
                for variant in product.get('variants', []):
                    product_data.append({
                        'product_id': product['id'],
                        'title': product['title'],
                        'variant_id': variant['id'],
                        'variant_title': variant.get('title', ''),
                        'price': float(variant['price']),
                        'inventory_quantity': variant.get('inventory_quantity', 0),
                        'created_at': product['created_at'],
                        'product_type': product.get('product_type', ''),
                        'vendor': product.get('vendor', ''),
                        'status': product['status']
                    })
            
            df = pd.DataFrame(product_data)
            
            print("=== Ürün Analizi ===")
            print(f"Toplam ürün varyasyonu: {len(df)}")
            print(f"Ortalama fiyat: {df['price'].mean():.2f}")
            print(f"En pahalı ürün: {df['price'].max():.2f}")
            print(f"En ucuz ürün: {df['price'].min():.2f}")
            print(f"Toplam stok: {df['inventory_quantity'].sum()}")
            
            # Kategori analizi
            print("\n=== Kategori Dağılımı ===")
            print(df['product_type'].value_counts().head())
            
            # CSV'ye kaydet
            df.to_csv('shopify_products.csv', index=False)
            print("\nVeriler 'shopify_products.csv' dosyasına kaydedildi.")
            
    except ImportError:
        print("Pandas kurulu değil. 'pip install pandas' ile kurabilirsiniz.")
    except Exception as e:
        print(f"Analiz hatası: {e}")

if __name__ == "__main__":
    main()
    print("\n" + "="*50)
    analyze_with_pandas()