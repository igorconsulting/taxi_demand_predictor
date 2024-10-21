import pandas as pd
import os

def check_date_range(df, year, month, date_column):
    """
    Checks if the dates in the specified column are within the given year and month.
    
    Args:
    - df: The pandas DataFrame containing the data.
    - year: The expected year for the data.
    - month: The expected month for the data.
    - date_column: The column name that contains the datetime data (default is 'pickup_datetime').
    
    Returns:
    - filtered_df: DataFrame containing rows that match the specified year and month.
    """
    
    # Step 1: Rename the datetime column to 'pickup_datetime'
    df = df.rename(columns={date_column: 'pickup_datetime'})

    # Convert the renamed 'pickup_datetime' column to datetime
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])

    # Find rows where the year and month do not match
    outside_range = df[
        (df['pickup_datetime'].dt.year != year) | 
        (df['pickup_datetime'].dt.month != month)
    ]
    
    if not outside_range.empty:
        print(True)  # Found data outside the expected range
        
        # Filter to keep only the rows within the expected year and month
        filtered_df = df[
            (df['pickup_datetime'].dt.year == year) & 
            (df['pickup_datetime'].dt.month == month)
        ]
        print(f'{outside_range.shape[0]} rows were outside the specified year-month and have been filtered.')
    else:
        print(False)  # No data outside the expected range
        filtered_df = df  # No filtering needed if all rows match

    return filtered_df


def data_filter(df):
    """
    Filters important columns, and drops rows with NaN values.
    
    Args:
    - df: The pandas DataFrame.
    
    Returns:
    - Filtered DataFrame with no NaN values in important columns.
    """
    # Step 1: Filter to keep only the important columns ('pickup_datetime' and 'PULocationID')
    df = df[['pickup_datetime', 'PULocationID']]
    
    # Step 2: Drop rows with NaN values in 'pickup_datetime' or 'PULocationID'
    df = df.dropna(subset=['pickup_datetime', 'PULocationID'])
    
    return df


def save_filtered_data(df, original_name, output_dir='../data/filtered'):
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


def process_file(filename, input_dir='../data/raw', output_dir='../data/filtered'):
    """
    Processes a single parquet file by filtering data, removing NaN values, and saving the result.
    
    Args:
    - filename: The name of the file to process.
    - input_dir: The directory where the parquet file is located.
    - output_dir: The directory where the filtered parquet file will be saved.
    
    Returns:
    - None
    """
    try:
        # Known patterns and corresponding original date columns
        file_patterns = {
            'yellow_tripdata': 'tpep_pickup_datetime',
            'green_tripdata': 'lpep_pickup_datetime',
            'fhv_tripdata': 'pickup_datetime',   # Processing fhv_tripdata
            'fhvhv_tripdata': 'pickup_datetime'
        }
        
        # Check each pattern
        processed = False
        for pattern, original_date_column in file_patterns.items():
            if filename.startswith(pattern):
                processed = True  # File matched a pattern
                try:
                    # Extract year and month from the filename
                    parts = filename.split('_')
                    file_year, file_month = parts[-1].split('.')[0].split('-')
                    file_year = int(file_year)
                    file_month = int(file_month)

                    # Load the parquet file
                    file_path = os.path.join(input_dir, filename)
                    df = pd.read_parquet(file_path)

                    # Check if the dates are within the expected range
                    df = check_date_range(df, file_year, file_month, original_date_column)
                    
                    # Apply data filtering (filter columns, drop NaN)
                    df = data_filter(df)
                    
                    # Save the filtered DataFrame
                    save_filtered_data(df, filename.replace('.parquet', ''), output_dir)
                
                except Exception as e:
                    print(f"Error processing file {filename}: {e}")
                break

        if not processed:
            print(f"File {filename} did not match any known patterns.")
    
    except FileNotFoundError:
        print(f"File {filename} not found.")
    except Exception as e:
        print(f"Unexpected error processing {filename}: {e}")


def process_all_parquet_files_in_directory(input_dir='../data/raw', output_dir='../data/filtered'):
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
                continue  # Ensure the loop continues to the next file
