import pandas as pd
import os
from pathlib import Path
from src.paths import RAW_DATA_DIR, FILTERED_DATA_DIR, FHV, PATH_DATETIME
from src.logger import get_logger

def filter_by_date_range(df, year, month, date_column):
    """
    Filter the DataFrame to include only rows within the specified date range.
    """
    df[date_column] = pd.to_datetime(df[date_column])
    filtered_df = df[(df[date_column].dt.year == year) & (df[date_column].dt.month == month)].copy()
    return filtered_df

def select_important_columns(df, PATH):
    """
    Selects important columns for the analysis, renaming columns where necessary,
    and removes rows with missing values.
    """
    # Rename columns only if PATH is FHV, which uses 'PUlocationID'
    if PATH == FHV:
        filtered_df = df.rename(columns={PATH_DATETIME[PATH]: 'pickup_datetime',
                                         'PUlocationID': 'PULocationID'}).copy()
    else:
        filtered_df = df.rename(columns={PATH_DATETIME[PATH]: 'pickup_datetime'}).copy()
    
    # Keep only the important columns ('pickup_datetime' and 'PULocationID')
    filtered_df = filtered_df[['pickup_datetime', 'PULocationID']].copy()
    filtered_df = filtered_df.dropna(subset=['pickup_datetime', 'PULocationID'])
    
    return filtered_df

def save_filtered_data(df, original_name, output_dir=FILTERED_DATA_DIR):
    """
    Saves the filtered DataFrame to a parquet file with a prefix 'filtered_'.
    """
    os.makedirs(output_dir, exist_ok=True)
    filtered_filename = f'filtered_{original_name}.parquet'
    output_path = os.path.join(output_dir, filtered_filename)
    df.to_parquet(output_path)
    print(f'Filtered data saved to {output_path}')

def process_file(filename, input_dir=RAW_DATA_DIR, output_dir=FILTERED_DATA_DIR):
    """
    Processes a single parquet file by filtering data based on date, selecting important columns, 
    and saving the result.
    """
    logger = get_logger()
    try:
        filtered_filename = f'filtered_{filename}'
        filtered_file_path = os.path.join(output_dir, filtered_filename)

        if Path(filtered_file_path).exists():
            logger.info(f'{filtered_filename} already exists. Skipping processing.')
            return
        
        for pattern in PATH_DATETIME:
            if filename.startswith(pattern):
                try:
                    parts = filename.split('_')
                    file_year, file_month = parts[-1].split('.')[0].split('-')
                    file_year = int(file_year)
                    file_month = int(file_month)

                    file_path = os.path.join(input_dir, filename)
                    df = pd.read_parquet(file_path)
                    df = filter_by_date_range(df, file_year, file_month, PATH_DATETIME[pattern])
                    df = select_important_columns(df, pattern)
                    save_filtered_data(df, filename.replace('.parquet', ''), output_dir)
                except Exception as e:
                    logger.info(f"Error processing file {filename}: {e}")
                break
    except FileNotFoundError:
        logger.info(f"File {filename} not found.")
    except Exception as e:
        logger.info(f"Unexpected error processing {filename}: {e}")

def process_all_parquet_files_in_directory(input_dir=RAW_DATA_DIR, output_dir=FILTERED_DATA_DIR):
    """
    Processes all parquet files in the input directory that follow the known patterns.
    """
    logger = get_logger()
    for filename in os.listdir(input_dir):
        if filename.endswith('.parquet'):
            try:
                process_file(filename, input_dir, output_dir)
            except Exception as e:
                logger.info(f"Failed to process {filename}: {e}")
                continue
