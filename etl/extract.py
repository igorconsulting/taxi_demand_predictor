import requests
import sys
from pathlib import Path

from src.paths import RAW_DATA_DIR

def fetch_raw_data(url, file_name, repo_dir=RAW_DATA_DIR):
    """
    Downloads the file from the provided URL if it doesn't already exist in the directory.
    
    Args:
    - url: The URL from which the file will be downloaded.
    - file_name: The name with which the file will be saved.
    - repo_dir: The directory where the file will be saved (default: RAW_DATA_DIR).
    
    Returns:
    - None
    """
    file_path = Path(repo_dir) / file_name

    # Check if the file already exists
    if file_path.exists():
        print(f'{file_name} already exists. Skipping download.')
        return

    # Attempt to download the file
    response = requests.get(url)
    
    # Check if the download was successful
    if response.status_code == 200:
        Path(repo_dir).mkdir(parents=True, exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f'{file_name} downloaded successfully to {repo_dir}')
    else:
        raise Exception(f'Status code error: {response.status_code} - File not available!')


def download_yellow_monthly_data(year, month):
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet'
    file_name = f'yellow_tripdata_{year}-{month}.parquet'
    fetch_raw_data(url, file_name)

def download_green_monthly_data(year, month):
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet'
    file_name = f'green_tripdata_{year}-{month}.parquet'
    fetch_raw_data(url, file_name)

def download_fhv_monthly_data(year, month):
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year}-{month}.parquet'
    file_name = f'fhv_tripdata_{year}-{month}.parquet'
    fetch_raw_data(url, file_name)

def download_fhvhv_monthly_data(year, month):
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year}-{month}.parquet'
    file_name = f'fhvhv_tripdata_{year}-{month}.parquet'
    fetch_raw_data(url, file_name)

def nyc_data_extrator(year, month):
    download_yellow_monthly_data(year, month)
    download_green_monthly_data(year, month)
    download_fhv_monthly_data(year, month)
    download_fhvhv_monthly_data(year, month)