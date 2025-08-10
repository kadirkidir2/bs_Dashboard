import json
import os
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime

try:
    with open('roboskop_ga_test.json', 'r') as f:
        SERVICE_ACCOUNT_INFO = json.load(f)
except FileNotFoundError:
    raise FileNotFoundError("Service account JSON dosyası bulunamadı: roboskop_ga_test.json")
except json.JSONDecodeError:
    raise ValueError("Service account JSON dosyası geçersiz formatta")

PROPERTY_ID = os.getenv("GA_PROPERTY_ID", "481831072")

class GoogleAnalyticsDataExtractor:
    def __init__(self, service_account_info, property_id):
        self.property_id = property_id
        self.credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=['https://www.googleapis.com/auth/analytics.readonly']
        )
        self.client = BetaAnalyticsDataClient(credentials=self.credentials)
    
    def validate_date_format(self, date_str):
        """Tarih formatını doğrular"""
        if date_str in ["yesterday", "today", "7daysAgo", "30daysAgo"]:
            return True
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return True
        except ValueError:
            return False
    
    def get_basic_report(self, start_date="7daysAgo", end_date="today"):
        """Temel rapor alır - oturumlar, kullanıcılar, sayfa görünümleri"""
        if not self.validate_date_format(start_date) or not self.validate_date_format(end_date):
            raise ValueError("Geçersiz tarih formatı")
        
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="country"),
                    Dimension(name="deviceCategory")
                ],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="activeUsers"),
                    Metric(name="screenPageViews"),
                    Metric(name="bounceRate"),
                    Metric(name="averageSessionDuration")
                ],
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            )
            response = self.client.run_report(request=request)
            return self._response_to_dataframe(response)
        except Exception as e:
            print(f"Temel rapor alınırken hata: {e}")
            return pd.DataFrame()
    
    def get_traffic_sources(self, start_date="7daysAgo", end_date="today", simple=False):
        """Trafik kaynaklarını analiz eder"""
        if not self.validate_date_format(start_date) or not self.validate_date_format(end_date):
            raise ValueError("Geçersiz tarih formatı")
        
        try:
            dimensions = [
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium")
            ]
            metrics = [
                Metric(name="sessions"),
                Metric(name="activeUsers")
            ]
            if not simple:
                dimensions.append(Dimension(name="sessionCampaignId"))
                metrics.append(Metric(name="totalUsers"))
            
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                dimensions=dimensions,
                metrics=metrics,
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            )
            response = self.client.run_report(request=request)
            return self._response_to_dataframe(response)
        except Exception as e:
            print(f"Trafik kaynakları alınırken hata: {e}")
            return pd.DataFrame()
    
    def get_page_analytics(self, start_date="7daysAgo", end_date="today"):
        """Sayfa bazlı analiz"""
        if not self.validate_date_format(start_date) or not self.validate_date_format(end_date):
            raise ValueError("Geçersiz tarih formatı")
        
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                dimensions=[
                    Dimension(name="pagePath"),
                    Dimension(name="pageTitle")
                ],
                metrics=[
                    Metric(name="screenPageViews"),
                    Metric(name="userEngagementDuration"),
                    Metric(name="bounceRate")
                ],
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                limit=100
            )
            response = self.client.run_report(request=request)
            return self._response_to_dataframe(response)
        except Exception as e:
            print(f"Sayfa analizi alınırken hata: {e}")
            return pd.DataFrame()
    
    def get_realtime_data(self):
        """Gerçek zamanlı veriler"""
        from google.analytics.data_v1beta.types import RunRealtimeReportRequest
        
        try:
            request = RunRealtimeReportRequest(
                property=f"properties/{self.property_id}",
                dimensions=[
                    Dimension(name="country"),
                    Dimension(name="deviceCategory")
                ],
                metrics=[
                    Metric(name="activeUsers")
                ]
            )
            response = self.client.run_realtime_report(request=request)
            return self._response_to_dataframe(response)
        except Exception as e:
            print(f"Gerçek zamanlı veri alınırken hata: {e}")
            return pd.DataFrame()
    
    def _response_to_dataframe(self, response):
        """API yanıtını pandas DataFrame'e çevirir"""
        if not response or not response.rows or not response.dimension_headers or not response.metric_headers:
            return pd.DataFrame()
        
        dimension_names = [dim.name for dim in response.dimension_headers]
        metric_names = [metric.name for metric in response.metric_headers]
        
        data = []
        for row in response.rows:
            row_data = {}
            for i, dim_value in enumerate(row.dimension_values):
                row_data[dimension_names[i]] = dim_value.value
            
            for i, metric_value in enumerate(row.metric_values):
                value = metric_value.value
                if metric_names[i] in ["sessions", "activeUsers", "screenPageViews", "totalUsers", "userEngagementDuration"]:
                    row_data[metric_names[i]] = int(value) if value else 0
                elif metric_names[i] in ["bounceRate", "averageSessionDuration"]:
                    row_data[metric_names[i]] = float(value) if value else 0.0
                else:
                    row_data[metric_names[i]] = value
            
            data.append(row_data)
        
        return pd.DataFrame(data)

# Kullanım örneği
if __name__ == "__main__":
    # Analytics extractor'ı başlat
    extractor = GoogleAnalyticsDataExtractor(SERVICE_ACCOUNT_INFO, PROPERTY_ID)
    
    # Temel rapor al
    print("Temel rapor alınıyor...")
    basic_report = extractor.get_basic_report(start_date="30daysAgo", end_date="today")
    print(f"Temel rapor: {len(basic_report)} satır")
    if not basic_report.empty:
        print(basic_report.head())
    
    # Trafik kaynakları
    print("\nTrafik kaynakları analizi...")
    traffic_sources = extractor.get_traffic_sources(start_date="30daysAgo", end_date="today")
    print(f"Trafik kaynakları: {len(traffic_sources)} satır")
    if not traffic_sources.empty:
        print(traffic_sources.head())
    else:
        print("Trafik kaynakları alınamadı, basit analiz deneniyor...")
        traffic_sources = extractor.get_traffic_sources(start_date="30daysAgo", end_date="today", simple=True)
        print(f"Basit trafik kaynakları: {len(traffic_sources)} satır")
        if not traffic_sources.empty:
            print(traffic_sources.head())
    
    # Sayfa analizi
    print("\nSayfa analizi...")
    page_analytics = extractor.get_page_analytics(start_date="30daysAgo", end_date="today")
    print(f"Sayfa analizi: {len(page_analytics)} satır")
    if not page_analytics.empty:
        print(page_analytics.head())
    
    # Gerçek zamanlı veriler
    print("\nGerçek zamanlı veriler...")
    realtime_data = extractor.get_realtime_data()
    print(f"Gerçek zamanlı: {len(realtime_data)} satır")
    if not realtime_data.empty:
        print(realtime_data.head())
    
    # Verileri kaydet
    if not basic_report.empty:
        try:
            basic_report.to_csv('ga_basic_report.csv', index=False)
            print("\nTemel rapor 'ga_basic_report.csv' olarak kaydedildi")
        except Exception as e:
            print(f"Hata: Temel rapor CSV dosyası kaydedilemedi: {e}")
    
    if not traffic_sources.empty:
        try:
            traffic_sources.to_csv('ga_traffic_sources.csv', index=False)
            print("Trafik kaynakları 'ga_traffic_sources.csv' olarak kaydedildi")
        except Exception as e:
            print(f"Hata: Trafik kaynakları CSV dosyası kaydedilemedi: {e}")
    
    if not page_analytics.empty:
        try:
            page_analytics.to_csv('ga_page_analytics.csv', index=False)
            print("Sayfa analizi 'ga_page_analytics.csv' olarak kaydedildi")
        except Exception as e:
            print(f"Hata: Sayfa analizi CSV dosyası kaydedilemedi: {e}")