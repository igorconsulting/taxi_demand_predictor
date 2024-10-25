import requests
import sys
from pathlib import Path
import pandas as pd
from src.filtering import filter_by_date_range, select_important_columns
from src.paths import (RAW_DATA_DIR, 
                       MAIN_PATH_LINK, 
                       PATH_DATETIME, 
                       FILTERED_DATA_DIR, 
                       TIME_SERIES_DATA_DIR, 
                       )



def fetch_raw_data(url, file_name, repo_dir=RAW_DATA_DIR):
    file_path = Path(repo_dir) / file_name
    try:
        response = requests.get(url)
        response.raise_for_status()
        Path(repo_dir).mkdir(parents=True, exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f'{file_name} downloaded successfully to {repo_dir}')
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return False


def verify_data_exists(PATH, year, month):
    # Ensure PATH includes the underscore
    file_path = f'{RAW_DATA_DIR}/{PATH}_{year}-{str(month).zfill(2)}.parquet'
    return Path(file_path).exists()


def download_monthly_data(PATH, year, month):
    if verify_data_exists(PATH, year, month):
        print(f'{PATH}_{year}-{str(month).zfill(2)}.parquet already exists. Skipping download.')
        return True
    # Ensure PATH includes the underscore
    url = f'{MAIN_PATH_LINK}{PATH}_{year}-{str(month).zfill(2)}.parquet'
    file_name = f'{PATH}_{year}-{str(month).zfill(2)}.parquet'
    fetch_raw_data(url, file_name)


def validate_and_filter_year_month(PATH, year, month):
    file_path = f'{RAW_DATA_DIR}/{PATH}_{year}-{str(month).zfill(2)}.parquet'
    filtered_file_path = f'{FILTERED_DATA_DIR}/{PATH}_{year}-{str(month).zfill(2)}.parquet'
    
    if Path(filtered_file_path).exists():
        print(f"{filtered_file_path} already exists. Skipping validation.")
        return None  # Skip if already processed and saved
    
    try:
        df = pd.read_parquet(file_path)
        print(f'Processing {file_path}')
        filtered_df = filter_by_date_range(df, year, month, PATH_DATETIME[PATH])
        filtered_df = select_important_columns(filtered_df, PATH)
        filtered_df = filtered_df.dropna()
        filtered_df['type'] = PATH.split('_')[0]
        
        # Save the validated, filtered data
        Path(FILTERED_DATA_DIR).mkdir(parents=True, exist_ok=True)
        filtered_df.to_parquet(filtered_file_path)
        print(f'Saved filtered data to {filtered_file_path}')
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")


def concat_filtered_data(PATH):
    dataframes = []
    
    for year in range(2019, 2022):
        for month in range(1, 13):
            filtered_file_path = f'{FILTERED_DATA_DIR}/{PATH}_{year}-{str(month).zfill(2)}.parquet'
            if Path(filtered_file_path).exists():
                try:
                    df = pd.read_parquet(filtered_file_path)
                    dataframes.append(df)
                except Exception as e:
                    print(f"Error reading {filtered_file_path}: {e}")
    
    if dataframes:
        df = pd.concat(dataframes, ignore_index=True)
        output_path = f'{TIME_SERIES_DATA_DIR}/{PATH}.parquet'
        df.to_parquet(output_path)
        print(f'Saved concatenated data to {output_path}')
    else:
        print("No data available to concatenate.")
