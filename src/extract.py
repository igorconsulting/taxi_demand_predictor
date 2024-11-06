import requests
from pathlib import Path
import pandas as pd
from src.logger import get_logger
from src.filtering import filter_by_date_range, select_important_columns
from src.paths import (RAW_DATA_DIR, MAIN_PATH_LINK, FILTERED_DATA_DIR, TIME_SERIES_DATA_DIR, PATH_DATETIME)

logger = get_logger()

def download_file(url, file_path):
    """Auxiliary function to download a file and save it to a specific path."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(response.content)
        logger.info(f'{file_path.name} downloaded successfully to {file_path.parent}')
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from {file_path.name}: {e}")
        return False

def check_file_exists(path, year, month):
    """Check if the file for a specific year and month exists."""
    file_path = Path(RAW_DATA_DIR) / f"{path}_{year}-{str(month).zfill(2)}.parquet"
    return file_path.exists()

def  fetch_data_if_not_exists(path, year, month):
    """Download data if it doesn't already exist."""
    if check_file_exists(path, year, month):
        logger.info(f'{path}_{year}-{str(month).zfill(2)}.parquet already exists. Skipping download.')
        return
    url = f"{MAIN_PATH_LINK}{path}_{year}-{str(month).zfill(2)}.parquet"
    file_name = f"{path}_{year}-{str(month).zfill(2)}.parquet"
    download_file(url, Path(RAW_DATA_DIR) / file_name)

def validate_and_process_data(path, year, month):
    """Validates, filters, and processes data for a specific year and month."""
    file_path = Path(RAW_DATA_DIR) / f"{path}_{year}-{str(month).zfill(2)}.parquet"
    filtered_file_path = Path(FILTERED_DATA_DIR) / f"{path}_{year}-{str(month).zfill(2)}.parquet"
    
    if filtered_file_path.exists():
        logger.info(f"{filtered_file_path} already exists. Skipping validation.")
        return

    try:
        df = pd.read_parquet(file_path)
        filtered_df = filter_by_date_range(df, year, month, PATH_DATETIME[path])
        filtered_df = select_important_columns(filtered_df, path).dropna()
        filtered_df['type'] = path.split('_')[0]
        filtered_file_path.parent.mkdir(parents=True, exist_ok=True)
        filtered_df.to_parquet(filtered_file_path)
        logger.info(f'Saved filtered data to {filtered_file_path}')
    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")

def concatenate_filtered_data(path):
    """Concatenate filtered data for a specific path and save it to a single file."""
    dataframes = []
    for year in range(2022, 2025):
        for month in range(1, 13):
            file_path = Path(FILTERED_DATA_DIR) / f"{path}_{year}-{str(month).zfill(2)}.parquet"
            if file_path.exists():
                try:
                    df = pd.read_parquet(file_path)
                    dataframes.append(df)
                except Exception as e:
                    logger.error(f"Error reading {file_path}: {e}")
    
    if dataframes:
        concatenated_df = pd.concat(dataframes, ignore_index=True)
        return concatenated_df
    else:
        logger.warning("No data available to concatenate.")
        return pd.DataFrame()
