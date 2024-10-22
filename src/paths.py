from pathlib import Path
import os

PARENT_DIR = Path(__file__).resolve().parents
DATA_DIR = PARENT_DIR / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
FILTERED_DATA_DIR = DATA_DIR / 'filtered'
TRANSFORMED_DATA_DIR = DATA_DIR / 'transformed'
TIME_SERIES_DATA_DIR = DATA_DIR / 'time_series'

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
