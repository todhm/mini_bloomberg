from django.urls import path 
from .import worker_api
from .import views
from django.urls import re_path

app_name = 'crawlapp'

urlpatterns = [
    path('crawl_bok_metadata', worker_api.CrawlBokMetaData.as_view(), name='crawl_bok_metadata'),
    path('launch_daily_stock', worker_api.CrawlDailyStock.as_view(), name='launch_daily_stock'),
    path('crawl_quandl_data', worker_api.CrawlQuandlData.as_view(), name='crawl_quandl_data'),
    path('save_report_data', worker_api.SaverReportData.as_view(), name='save_report_data'),

    re_path(r'^(?P<nk>.+)/$', views.GetTaskView.as_view( {'get': 'retrieve'}), name='task'),
]