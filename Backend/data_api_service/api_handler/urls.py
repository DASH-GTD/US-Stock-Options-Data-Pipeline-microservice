from django.urls import path
from .views import StockDataView, OptionsDataView, SearchStockView, SearchStockName

urlpatterns = [
    path('api/stock-data/', StockDataView.as_view(), name='stock-data'),
    path('api/options-data/', OptionsDataView.as_view(), name='options-data'),
    path('api/search/', SearchStockView.as_view(), name='search-stock'),
    path('api/stocknames/', SearchStockName.as_view(), name='stock-names'),
]