import requests
from pathlib import Path

def fetch_raw_data(url, file_name, repo_dir='../data/raw'):
    """
    Faz o download do arquivo da URL e salva no diretório especificado.
    
    Args:
    - url: URL do arquivo a ser baixado.
    - file_name: Nome com o qual o arquivo será salvo.
    - repo_dir: Diretório onde o arquivo será salvo. Padrão é '../data/raw'.
    
    Returns:
    - None
    """
    response = requests.get(url)

    if response.status_code == 200:
        Path(repo_dir).mkdir(parents=True, exist_ok=True)
        with open(f'{repo_dir}/{file_name}', 'wb') as f:
            f.write(response.content)
        print(f'{file_name} downloaded successfully to {repo_dir}')
    else:
        raise Exception(f'Status code error: {response.status_code} - File not available!')

def download_yellow_monthly_data(year, month):
    """
    Faz o download dos dados de táxi amarelo para o mês e ano especificados.
    
    Args:
    - year: Ano no formato 'YYYY'.
    - month: Mês no formato 'MM'.
    
    Returns:
    - None
    """
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet'
    file_name = f'yellow_tripdata_{year}-{month}.parquet'
    fetch_raw_data(url, file_name)

def download_green_monthly_data(year, month):
    """
    Faz o download dos dados de táxi verde para o mês e ano especificados.
    """
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet'
    file_name = f'green_tripdata_{year}-{month}.parquet'
    fetch_raw_data(url, file_name)

def download_fhv_monthly_data(year, month):
    """
    Faz o download dos dados de táxi FHV para o mês e ano especificados.
    """
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year}-{month}.parquet'
    file_name = f'fhv_tripdata_{year}-{month}.parquet'
    fetch_raw_data(url, file_name)

def download_fhvhv_monthly_data(year, month):
    """
    Faz o download dos dados de táxi FHVHV para o mês e ano especificados.
    """
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year}-{month}.parquet'
    file_name = f'fhvhv_tripdata_{year}-{month}.parquet'
    fetch_raw_data(url, file_name)

def nyc_data_extrator(year, month):
    download_yellow_monthly_data(year, month)
    download_green_monthly_data(year, month)
    download_fhv_monthly_data(year, month)
    download_fhvhv_monthly_data(year, month)