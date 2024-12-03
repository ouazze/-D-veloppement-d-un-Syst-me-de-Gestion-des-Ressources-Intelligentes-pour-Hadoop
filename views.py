# hadoop_integration/views.py
from django.shortcuts import render

def home_view(request):
    return render(request,'home.html')  # Specify the correct folder path
