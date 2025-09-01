from django.conf import settings
from django.contrib import admin
from django.urls import path
from django.conf.urls.static import static

from core import views
from core.views_api import ClientsListAPI
from core.views_clients import (
    clients_table_view,
    buckets_list_api,
    client_detail_view,
    client_heatmap_view,
)
from core.views_geo import HeatmapAPI
from core.views_geo_homework import HomeWorkAPI
from core.views_llm import plan_meeting_view

urlpatterns = [
    # Admin
    path('admin/', admin.site.urls),

    # Pages
    path('', views.index, name='index'),
    path('clients/', views.clients_page, name='clients'),
    path('upload/multi/', views.upload_multi_page, name='upload_multi'),

    # Download templates
    path('download/template/cs/', views.download_template_cs, name='download-template-cs'),
    path('download/template/c/', views.download_template_c, name='download-template-c'),
    path('download/template/tr/', views.download_template_tr, name='download-template-tr'),
    path('download/template/so/', views.download_template_so, name='download-template-so'),
    path('download/template/dog/', views.download_template_dog, name='download-template-dog'),

    # Clients list + data
    path('clients/table/', clients_table_view, name='clients_table'),
    path('clients/buckets/', buckets_list_api, name='clients_buckets'),

    # Client detail and heatmap (фикс путей)
    path('clients/<int:pk>/', client_detail_view, name='client-detail'),
    path('clients/<int:pk>/heatmap/', client_heatmap_view, name='client-heatmap'),

    # APIs
    path('api/clients/', ClientsListAPI.as_view(), name='api_clients'),
    path('api/geo/heatmap/', HeatmapAPI.as_view(), name='geo-heatmap'),
    path('api/geo/homework/', HomeWorkAPI.as_view(), name='geo-homework'),

    # LLM API (имя совпадает с шаблоном)
    path('api/llm/plan-meeting/', plan_meeting_view, name='plan_meeting'),
]

# Static/media в DEV
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
