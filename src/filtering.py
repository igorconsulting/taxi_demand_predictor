import pandas as pd
import os
from pathlib import Path

from src.paths import RAW_DATA_DIR, FILTERED_DATA_DIR

def filter_by_date_range(df, year, month, date_column):
    """
    Filters rows in the DataFrame that match the specified year and month in the given date column.
    
    Args:
    - df: The pandas DataFrame containing the data.
    - year: The expected year for the data.
    - month: The expected month for the data.
    - date_column: The column name that contains the datetime data (default is 'pickup_datetime').
    
    Returns:
    - filtered_df: DataFrame containing rows that match the specified year and month.
    """
    
    # Rename the datetime column to 'pickup_datetime'
    df = df.rename(columns={date_column: 'pickup_datetime'})

    # Convert 'pickup_datetime' to datetime
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])

    # Filter rows outside the specified year and month
    outside_range = df[
        (df['pickup_datetime'].dt.year != year) | 
        (df['pickup_datetime'].dt.month != month)
    ]
    
    if not outside_range.empty:
        print(f"Found {outside_range.shape[0]} rows outside the specified year-month. Filtering them out.")
        
        # Keep only the rows within the specified year and month
        filtered_df = df[
            (df['pickup_datetime'].dt.year == year) & 
            (df['pickup_datetime'].dt.month == month)
        ]
    else:
        print('All data within the expected date range.')
        filtered_df = df  # No filtering needed if all rows match

    return filtered_df


def select_important_columns(df, file_type):
    """
    Selects important columns for the analysis, renaming columns where necessary,
    and removes rows with missing values.
    
    Args:
    - df: The pandas DataFrame.
    - file_type: The type of file being processed (e.g., 'fhv_tripdata', 'yellow_tripdata').
    
    Returns:
    - df: DataFrame with only the important columns and no NaN values.
    """
    # Handle the special case for 'fhv_tripdata' where the column name is 'PUlocationID' instead of 'PULocationID'
    if file_type == 'fhv_tripdata':
        df = df.rename(columns={'PUlocationID': 'PULocationID'})
    
    # Keep only the important columns ('pickup_datetime' and 'PULocationID')
    df = df[['pickup_datetime', 'PULocationID']]
    
    # Remove rows with NaN values in the important columns
    df = df.dropna(subset=['pickup_datetime', 'PULocationID'])
    
    return df


def save_filtered_data(df, original_name, output_dir=FILTERED_DATA_DIR):
    """
    Saves the filtered DataFrame to a parquet file with a prefix 'filtered_'.
    
    Args:
    - df: The filtered pandas DataFrame.
    - original_name: The original name of the data file (used for constructing the output filename).
    - output_dir: The directory where the filtered file will be saved (default is '../data/filtered').
    
    Returns:
    - None
    """
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Construct the new filename by adding the prefix 'filtered_' to the original filename
    filtered_filename = f'filtered_{original_name}.parquet'
    output_path = os.path.join(output_dir, filtered_filename)
    
    # Save the DataFrame to a parquet file
    df.to_parquet(output_path)
    print(f'Filtered data saved to {output_path}')


def process_file(filename, input_dir=RAW_DATA_DIR, output_dir=FILTERED_DATA_DIR):
    """
    Processes a single parquet file by filtering data based on date, selecting important columns, 
    and saving the result.
    
    Args:
    - filename: The name of the file to process.
    - input_dir: The directory where the parquet file is located.
    - output_dir: The directory where the filtered parquet file will be saved.
    
    Returns:
    - None
    """
    try:
        # Construct the filtered file name and path
        filtered_filename = f'filtered_{filename}'
        filtered_file_path = os.path.join(output_dir, filtered_filename)

        # Check if the filtered file already exists
        if Path(filtered_file_path).exists():
            print(f'{filtered_filename} already exists. Skipping processing.')
            return  # Skip this file since it's already processed
        
        # Known patterns and corresponding original date columns
        file_patterns = {
            'yellow_tripdata': 'tpep_pickup_datetime',
            'green_tripdata': 'lpep_pickup_datetime',
            'fhv_tripdata': 'pickup_datetime',   # Corrected for fhv files
            'fhvhv_tripdata': 'pickup_datetime'
        }
        
        for pattern, original_date_column in file_patterns.items():
            if filename.startswith(pattern):
                try:
                    # Extract year and month from the filename
                    parts = filename.split('_')
                    file_year, file_month = parts[-1].split('.')[0].split('-')
                    file_year = int(file_year)
                    file_month = int(file_month)

                    # Load the parquet file
                    file_path = os.path.join(input_dir, filename)
                    df = pd.read_parquet(file_path)

                    # Filter rows by date range (year and month)
                    df = filter_by_date_range(df, file_year, file_month, original_date_column)
                    
                    # Select important columns for analysis
                    df = select_important_columns(df, pattern)  # Handle column name differences
                    
                    # Save the filtered DataFrame
                    save_filtered_data(df, filename.replace('.parquet', ''), output_dir)
                
                except Exception as e:
                    print(f"Error processing file {filename}: {e}")
                break

    except FileNotFoundError:
        print(f"File {filename} not found.")
    except Exception as e:
        print(f"Unexpected error processing {filename}: {e}")


def process_all_parquet_files_in_directory(input_dir=RAW_DATA_DIR, output_dir=FILTERED_DATA_DIR):
    """
    Processes all parquet files in the input directory that follow the known patterns.
    
    Args:
    - input_dir: The directory where the raw parquet files are located.
    - output_dir: The directory where the filtered parquet files will be saved.
    
    Returns:
    - None
    """
    for filename in os.listdir(input_dir):
        if filename.endswith('.parquet'):
            try:
                process_file(filename, input_dir, output_dir)
            except Exception as e:
                print(f"Failed to process {filename}: {e}")
                continue
