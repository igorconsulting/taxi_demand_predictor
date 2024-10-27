from pathlib import Path
import os

# Defines base directory and data subdirectories
PARENT_DIR = Path(__file__).resolve().parent
DATA_DIR = (PARENT_DIR / '../data').resolve()
RAW_DATA_DIR = DATA_DIR / 'raw'
FILTERED_DATA_DIR = DATA_DIR / 'filtered'
TRANSFORMED_DATA_DIR = DATA_DIR / 'transformed'
TIME_SERIES_DATA_DIR = DATA_DIR / 'time_series_data'
MODELS_DIR = PARENT_DIR / 'models'

# Link Settings and Constants
MAIN_PATH_LINK = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
YELLOW = 'yellow_tripdata'
GREEN = 'green_tripdata'
FHV = 'fhv_tripdata'
FHVHV = 'fhvhv_tripdata'

YELLOW_DATETIME = 'tpep_pickup_datetime'
GREEN_DATETIME = 'lpep_pickup_datetime'
FHV_DATETIME = 'pickup_datetime'
FHVHV_DATETIME = 'pickup_datetime'

# Date and time mapping for each file type
PATH_DATETIME = {
    YELLOW: YELLOW_DATETIME,
    GREEN: GREEN_DATETIME,
    FHV: FHV_DATETIME,
    FHVHV: FHVHV_DATETIME
}

# Creates all directories if they do not exist
for directory in [DATA_DIR, RAW_DATA_DIR, FILTERED_DATA_DIR, TRANSFORMED_DATA_DIR, TIME_SERIES_DATA_DIR, MODELS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)
