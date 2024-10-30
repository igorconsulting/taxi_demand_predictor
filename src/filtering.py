import pandas as pd
import os
from pathlib import Path
from src.paths import RAW_DATA_DIR, FILTERED_DATA_DIR, FHV, PATH_DATETIME
from src.logger import get_logger

logger = get_logger()

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
    logger.info(f'Filtered data saved to {output_path}')
