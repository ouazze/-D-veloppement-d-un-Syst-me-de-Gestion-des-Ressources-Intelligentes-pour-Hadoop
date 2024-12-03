from hdfs import InsecureClient
from django.http import HttpResponse
from django.conf import settings
from django.shortcuts import render
import psutil
import json

'''# Function to list files in HDFS
def list_hdfs_files(request):
    try:
        client = InsecureClient(f'http://{settings.HDFS_HOST}:{settings.HDFS_PORT}', user=settings.HDFS_USER)
        files = client.list('/')
        return HttpResponse(f"Files in HDFS root: {', '.join(files)}")
    except Exception as e:
        return HttpResponse(f"Error connecting to HDFS: {str(e)}")  '''
'''
def list_hdfs_files(request):
    try:
        client = InsecureClient(f'http://{settings.HDFS_HOST}:{settings.HDFS_PORT}', user=settings.HDFS_USER)
        files = client.list('/')
        return render(request, 'hadoop_integration/list_hdfs_files.html', {'files': files})
    except Exception as e:
        return render(request, 'hadoop_integration/list_hdfs_files.html', {'error': str(e), 'files': []})

'''
'''
def list_hdfs_files(request, path='/'):
    try:
        client = InsecureClient(f'http://{settings.HDFS_HOST}:{settings.HDFS_PORT}', user=settings.HDFS_USER)
        files = client.list(path, True)  # True for recursive listing
        return render(request, 'hadoop_integration/list_hdfs_files.html', {'files': files, 'current_path': path})
    except Exception as e:
        return render(request, 'hadoop_integration/list_hdfs_files.html', {'error': str(e), 'files': [], 'current_path': path})
'''
from django.shortcuts import render
from hdfs import InsecureClient
from django.conf import settings

def list_hdfs_files(request, path='/'):
    try:
        client = InsecureClient(f'http://{settings.HDFS_HOST}:{settings.HDFS_PORT}', user=settings.HDFS_USER)
        
        # List files in the directory with status (returns a list of tuples)
        files_with_status = client.list(path, status=True)
        
        file_list = []
        for file_name, metadata in files_with_status:
            file_info = {
                'name': file_name,
                'isdir': metadata['type'] == 'DIRECTORY',
                'path': f"{path.rstrip('/')}/{file_name}"
            }
            file_list.append(file_info)

        context = {
            'files': file_list,
            'current_path': path
        }

        return render(request, 'hadoop_integration/list_hdfs_files.html', context)

    except Exception as e:
        return render(request, 'hadoop_integration/list_hdfs_files.html', {
            'error': str(e),
            'files': [],
            'current_path': path
        })


def read_file(request, file_path):
    try:
        client = InsecureClient(f'http://{settings.HDFS_HOST}:{settings.HDFS_PORT}', user=settings.HDFS_USER)

        # Open and read the file
        with client.read(file_path) as reader:
            file_content = reader.read().decode('utf-8')

        # Check if the file is a JSON file to display it properly
        if file_path.endswith('.json'):
            try:
                file_content = json.dumps(json.loads(file_content), indent=4)
            except json.JSONDecodeError:
                pass  # If the file is not valid JSON, just display it as plain text.

        context = {
            'file_path': file_path,
            'file_content': file_content,
        }
        return render(request, 'hadoop_integration/read_file.html', context)

    except Exception as e:
        return HttpResponse(f"Error reading file: {str(e)}")

# Function to upload a file to HDFS

def upload_to_hdfs(request):
    if request.method == 'POST' and request.FILES['file']:
        uploaded_file = request.FILES['file']
        client = InsecureClient(f'http://{settings.HDFS_HOST}:{settings.HDFS_PORT}', user=settings.HDFS_USER)
        
        # Write the file to HDFS
        try:
            client.write(f'/uploaded_files/{uploaded_file.name}', uploaded_file, overwrite=True)
            return HttpResponse(f"File '{uploaded_file.name}' uploaded successfully to HDFS.")
        except Exception as e:
            return HttpResponse(f"Error uploading file to HDFS: {str(e)}")
    
    # Render the upload form template
    return render(request, 'hadoop_integration/upload.html')

# Function to download a file from HDFS
def download_from_hdfs(request):
    if request.method == 'POST':
        file_name = request.POST.get('file_name')  # Get the file name from the form
        client = InsecureClient(f'http://{settings.HDFS_HOST}:{settings.HDFS_PORT}', user=settings.HDFS_USER)
        
        # Attempt to download the specified file from HDFS
        try:
            with client.read(f'/uploaded_files/{file_name}') as reader:
                data = reader.read()
            # Return the file content in the response
            return HttpResponse(data, content_type='text/plain')
        except Exception as e:
            # Pass the error message to the template
            return render(request, 'hadoop_integration/download.html', {
                'error_message': f"Error downloading file from HDFS: {str(e)}"
            })
    
    # Render the form if it's a GET request
    return render(request, 'hadoop_integration/download.html')

def hdfs_usage(request):
    # HDFS client setup
    client = InsecureClient(f'http://{settings.HDFS_HOST}:{settings.HDFS_PORT}', user=settings.HDFS_USER)
    
    # HDFS storage details
    try:
        hdfs_report = client.status('/')
        hdfs_capacity = hdfs_report['capacity']
        hdfs_used = hdfs_report['used']
        hdfs_remaining = hdfs_report['remaining']
    except Exception as e:
        hdfs_capacity = hdfs_used = hdfs_remaining = None

    # System memory and CPU details
    memory = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=1)
    
    context = {
        'hdfs_capacity': hdfs_capacity,
        'hdfs_used': hdfs_used,
        'hdfs_remaining': hdfs_remaining,
        'system_memory': memory,
        'cpu_usage': cpu
    }

    return render(request, 'hadoop_integration/hdfs_usage.html', context)

def hadoop_overview(request):
    return render(request, 'hadoop_integration/hadoop_overview.html')