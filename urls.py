from django.urls import path
from .views import list_hdfs_files, upload_to_hdfs, download_from_hdfs , hadoop_overview
from .views import hdfs_usage
from . import views
urlpatterns = [
    path('list-files/', list_hdfs_files, name='list_hdfs_files'),
    path('upload-file/', upload_to_hdfs, name='upload_to_hdfs'),
    path('download-file/', download_from_hdfs, name='download_from_hdfs'),
    path('hdfs-usage/', hdfs_usage, name='hdfs_usage'),  # Add this URL
    path('', hadoop_overview, name='hadoop'),  # This is the Hadoop overview page
    
    path('list-files/<path:path>/', views.list_hdfs_files, name='list_hdfs_files'),
    path('read/<path:file_path>/', views.read_file, name='read_file'),
]
