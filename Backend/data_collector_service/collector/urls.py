# data-collector/collector/urls.py
from django.urls import path
from collector import views

urlpatterns = [
    path('fetch_daily_data/', views.fetch_daily_data, name='fetch_daily_data'),
    path('fetch_15min_data/', views.fetch_15min_data, name='fetch_15min_data'),
    path('fetch_option_data/', views.fetch_option_data, name='fetch_option_data'),
    path('fetch_historical_data/', views.fetch_historical_data, name='fetch_historical_data'),
]