from pathlib import Path
import os

PARENT_DIR = Path(__file__).resolve().parent
DATA_DIR = PARENT_DIR / '../data'
RAW_DATA_DIR = DATA_DIR / 'raw'
FILTERED_DATA_DIR = DATA_DIR / 'filtered'
TRANSFORMED_DATA_DIR = DATA_DIR / 'transformed'
TIME_SERIES_DATA_DIR = DATA_DIR / 'time_series_data'

MAIN_PATH_LINK = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
YELLOW = 'yellow_tripdata'
GREEN = 'green_tripdata'
FHV = 'fhv_tripdata'
FHVHV = 'fhvhv_tripdata'

YELLOW_DATETIME = 'tpep_pickup_datetime'
GREEN_DATETIME = 'lpep_pickup_datetime'
FHV_DATETIME = 'pickup_datetime'
FHVHV_DATETIME = 'pickup_datetime'

PATH_DATETIME = {
    YELLOW: YELLOW_DATETIME,
    GREEN: GREEN_DATETIME,
    FHV: FHV_DATETIME,
    FHVHV: FHVHV_DATETIME
}

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

if not os.path.exists(RAW_DATA_DIR):
    os.makedirs(RAW_DATA_DIR)

if not os.path.exists(FILTERED_DATA_DIR):
    os.makedirs(FILTERED_DATA_DIR)

if not os.path.exists(TRANSFORMED_DATA_DIR):
    os.makedirs(TRANSFORMED_DATA_DIR)

if not os.path.exists(TIME_SERIES_DATA_DIR):
    os.makedirs(TIME_SERIES_DATA_DIR)
