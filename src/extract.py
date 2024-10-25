import requests
import sys
from pathlib import Path
import pandas as pd
from src.filtering import filter_by_date_range, select_important_columns
from src.paths import (RAW_DATA_DIR, MAIN_PATH_LINK, PATH_DATETIME, FILTERED_DATA_DIR, YELLOW, GREEN, FHV, FHVHV)


def fetch_raw_data(url, file_name, repo_dir=RAW_DATA_DIR):
    file_path = Path(repo_dir) / file_name

    if file_path.exists():
        print(f'{file_name} already exists. Skipping download.')
        return True

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


def download_monthly_data(PATH, year, month):
    # Ensure PATH includes the underscore
    url = f'{MAIN_PATH_LINK}{PATH}_{year}-{str(month).zfill(2)}.parquet'
    file_name = f'{PATH}_{year}-{str(month).zfill(2)}.parquet'
    fetch_raw_data(url, file_name)


def validate_and_filter_year_month(PATH, year, month):
    # Ensure PATH includes the underscore
    file_path = f'{RAW_DATA_DIR}/{PATH}_{year}-{str(month).zfill(2)}.parquet'
    try:
        df = pd.read_parquet(file_path)
        print(f'Processing {file_path}')
        filtered_df = filter_by_date_range(df, year, month, PATH_DATETIME[PATH])
        filtered_df = select_important_columns(filtered_df, PATH)
        filtered_df = filtered_df.dropna()
        filtered_df['type'] = PATH.split('_')[0]
        return filtered_df
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return pd.DataFrame()


def concat_filtered_data(PATH):
    # Ensure PATH includes the underscore
    dataframes = []  # List to store all DataFrames

    for year in range(2019, 2022):
        for month in range(1, 13):
            try:
                monthly_df = validate_and_filter_year_month(PATH, year, month)
                if not monthly_df.empty:
                    dataframes.append(monthly_df)
            except Exception as e:
                print(f'Error in {PATH}_{year}-{str(month).zfill(2)}.parquet: {e}')
                continue

    if dataframes:
        df = pd.concat(dataframes, ignore_index=True)
        return df
    else:
        print("No data available to concatenate.")
        return pd.DataFrame()


def main():
    # Ensure PATH includes the underscore in each iteration
    for PATH in [YELLOW]:#, GREEN, FHV, FHVHV]:
        for year in range(2019, 2022):
            for month in range(1, 13):
                download_monthly_data(PATH, year, month)
        
        # Concatenate all processed data and save
        df = concat_filtered_data(PATH)
        if not df.empty:
            output_path = f'{FILTERED_DATA_DIR}/{PATH}.parquet'
            df.to_parquet(output_path)
            print(f'Saved concatenated data to {output_path}')
        else:
            print(f"No data to save for {PATH}.")

if __name__ == '__main__':
    main()

# how to run this script
# python src/etl/extract.py yellow_tripdata