from pathlib import Path
import os

# Define diretório base e subdiretórios de dados
PARENT_DIR = Path(__file__).resolve().parent
DATA_DIR = (PARENT_DIR / '../data').resolve()
RAW_DATA_DIR = DATA_DIR / 'raw'
FILTERED_DATA_DIR = DATA_DIR / 'filtered'
TRANSFORMED_DATA_DIR = DATA_DIR / 'transformed'
TIME_SERIES_DATA_DIR = DATA_DIR / 'time_series_data'

# Configurações de link e constantes
MAIN_PATH_LINK = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
YELLOW = 'yellow_tripdata'
GREEN = 'green_tripdata'
FHV = 'fhv_tripdata'
FHVHV = 'fhvhv_tripdata'

YELLOW_DATETIME = 'tpep_pickup_datetime'
GREEN_DATETIME = 'lpep_pickup_datetime'
FHV_DATETIME = 'pickup_datetime'
FHVHV_DATETIME = 'pickup_datetime'

# Mapeamento de data e hora para cada tipo de arquivo
PATH_DATETIME = {
    YELLOW: YELLOW_DATETIME,
    GREEN: GREEN_DATETIME,
    FHV: FHV_DATETIME,
    FHVHV: FHVHV_DATETIME
}

# Cria todos os diretórios se não existirem
for directory in [DATA_DIR, RAW_DATA_DIR, FILTERED_DATA_DIR, TRANSFORMED_DATA_DIR, TIME_SERIES_DATA_DIR]:
    directory.mkdir(parents=True, exist_ok=True)
