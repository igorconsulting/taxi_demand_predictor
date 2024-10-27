import os
import pandas as pd
from tqdm import tqdm
import numpy as np
from src.logger import get_logger

from src.paths import (PARENT_DIR,
                       FILTERED_DATA_DIR, 
                       TRANSFORMED_DATA_DIR, 
                       TIME_SERIES_DATA_DIR
                       )

# Step 1: Function to add the 'pickup_hour' column and group data by 'pickup_hour' and 'PULocationID'
def process_filtered_dataframe(df) -> pd.DataFrame:
    """
    Process a filtered DataFrame by adding a 'pickup_hour' column and grouping the data by 'pickup_hour' and 'PULocationID'.

    Args:

    - df: A pandas DataFrame containing the filtered data with columns 'pickup_datetime' and 'PULocationID'.

    Returns:

    - A DataFrame with the number of rides for each 'pickup_hour' and 'PULocationID'.

    """
    # Add a column for the pickup hour
    df['pickup_hour'] = df['pickup_datetime'].dt.floor('h')

    # Group by pickup hour and PULocationID, count the number of rides
    df_grouped = df.groupby(['pickup_hour', 'PULocationID']).size().reset_index(name='rides')
    
    return df_grouped

# Step 2: Function to add missing slots (time series transformation)
def add_missing_slots(df_grouped) -> pd.DataFrame:
    location_ids = df_grouped['PULocationID'].unique()
    full_range = pd.date_range(
        start=df_grouped['pickup_hour'].min(), end=df_grouped['pickup_hour'].max(), freq='h'
    )
    
    output_list = []  # Use a list for better performance
    
    for location_id in tqdm(location_ids):
        # Filter only rides for this location
        df_location = df_grouped.loc[df_grouped['PULocationID'] == location_id, ['pickup_hour', 'rides']]
        
        # Add missing dates with 0 in rides
        df_location = df_location.set_index('pickup_hour').reindex(full_range).fillna(0).reset_index()
        df_location = df_location.rename(columns={'index': 'pickup_hour'})  # Rename the reindexed column
        
        # Add the location ID back
        df_location['PULocationID'] = location_id
        
        # Append to the list instead of concatenating in each iteration
        output_list.append(df_location)
    
    # Concatenate all at once
    output = pd.concat(output_list, ignore_index=True)

    # Ensure the DataFrame is well-formed and reset index correctly
    output = output.reset_index(drop=True)
    
    return output

# Step 3: Function to process all parquet files in a folder
def process_all_filtered_files(input_dir=FILTERED_DATA_DIR, output_dir=TRANSFORMED_DATA_DIR):
    logger = get_logger()
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Loop through each file in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith('.parquet'):
            output_file = f"transformed_{filename}"
            output_path = os.path.join(output_dir, output_file)
            
            # Check if the transformed file already exists
            if os.path.exists(output_path):
                print(f"{output_file} already exists. Skipping processing.")
                continue  # Skip this file if already processed

            file_path = os.path.join(input_dir, filename)
            logger.info(f"Processing file: {filename}")

            # Read the parquet file
            df = pd.read_parquet(file_path)
            
            # Process the dataframe (add pickup_hour, group by, and add missing slots)
            df_grouped = process_filtered_dataframe(df)
            complete_df_grouped = add_missing_slots(df_grouped)

            # Save the transformed dataframe as a parquet file in the output directory
            complete_df_grouped.to_parquet(output_path)
            logger.info(f"Saved transformed file: {output_file}")


# Step 4: Function to generate the feature matrix and target vector
def get_cutoff_indices(df: pd.DataFrame, n_features: int, step_size: int = 1) -> list:
    """
    Generate cutoff indices for creating features and targets from time series data.
    
    Args:
    - df: DataFrame with time series data.
    - n_features: Number of time steps to use as features.
    - step_size: Number of steps to shift the window after each iteration.

    Returns:
    - List of tuples with indices for each sliding window (start, mid, end).
    """
    
    stop_position = len(df) - 1  # Define o limite para a última posição
    subseq_first_idx = 0
    subseq_mid_idx = n_features
    subseq_last_idx = n_features + 1
    indices = []

    # Loop para gerar os índices de corte com controle de step_size
    while subseq_last_idx <= stop_position:
        indices.append((subseq_first_idx, subseq_mid_idx, subseq_last_idx))
        subseq_first_idx += step_size
        subseq_mid_idx += step_size
        subseq_last_idx += step_size

    return indices


# Step 5: Function to create the feature matrix and target vector
import numpy as np

def create_feature_matrix_and_target(df, cutoff_indices):
    """
    Creates a feature matrix and a target vector from a DataFrame using sliding window indices.
    
    Args:
    - df: A pandas DataFrame containing the time series data (e.g., column 'rides').
    - cutoff_indices: A list of tuples (first_index, mid_index, last_index) generated by the `get_cutoff_indices()` function.

    Returns:
    - feature_matrix: A NumPy array where each row contains a sliding window of values.
    - target_vector: A NumPy array containing the target value (the value at `last_index` for each window).
    """
    
    n_examples = len(cutoff_indices)
    n_features = cutoff_indices[0][1] - cutoff_indices[0][0]  # Número de recursos na janela

    # allocate matrix and vector
    feature_matrix = np.ndarray((n_examples, n_features), dtype=np.float32)
    target_vector = np.ndarray((n_examples,), dtype=np.float32)

    for i, (first_index, mid_index, last_index) in enumerate(cutoff_indices):
        feature_matrix[i, :] = df['rides'].iloc[first_index:mid_index].values
        target_vector[i] = df['rides'].iloc[last_index]

    return feature_matrix, target_vector



# Step 6: Function to process a parquet file by PULocationID
def process_feature_target_by_PULocationID(df, n_features, step_size=1):
    """
    Process data by PULocationID and apply sliding window transformation for each PULocationID.
    
    Args:
    - df: DataFrame containing the time series data with columns ['PULocationID', 'rides', 'pickup_hour'].
    - n_features: Number of previous time steps to use as features.
    - step_size: Step size for sliding window.
    
    Returns:
    - A DataFrame with PULocationID, features, and target.
    """
    logger = get_logger()

    unique_pulocation_ids = df['PULocationID'].unique()
    rows = []

    logger.info(f"Processing {len(unique_pulocation_ids)} unique PULocationIDs...")
    
    for pulocation_id in unique_pulocation_ids:
        df_location = df[df['PULocationID'] == pulocation_id].sort_values('pickup_hour')

        if len(df_location) < n_features:
            continue

        # Get cut indices with `step_size`
        cutoff_indices = get_cutoff_indices(df_location, n_features, step_size)

        # Pre-allocate arrays for features and targets
        n_examples = len(cutoff_indices)
        feature_matrix = np.ndarray((n_examples, n_features), dtype=np.float32)
        target_vector = np.ndarray((n_examples,), dtype=np.float32)
        pickup_hours = []

        # Assembles `feature_matrix` and `target_vector`
        for i, (start_idx, mid_idx, end_idx) in enumerate(cutoff_indices):
            feature_matrix[i, :] = df_location.iloc[start_idx:mid_idx]['rides'].values
            target_vector[i] = df_location.iloc[end_idx]['rides']
            pickup_hours.append(df_location.iloc[end_idx]['pickup_hour'])

        # Creates DataFrames of features and targets
        features_one_location = pd.DataFrame(
            feature_matrix,
            columns=[f'feature_{i}' for i in range(n_features)]
        )
        features_one_location['pickup_hour'] = pickup_hours
        features_one_location['PULocationID'] = pulocation_id

        targets_one_location = pd.DataFrame(target_vector, columns=['target'])

        # Concatenates features and target
        rows.append(pd.concat([features_one_location, targets_one_location], axis=1))

    final_df = pd.concat(rows).reset_index(drop=True)
    return final_df


# Step 7: Function to process all transformed files
def process_all_transformed_files(input_dir=TRANSFORMED_DATA_DIR, output_dir=TIME_SERIES_DATA_DIR, n_features=24):
    """
    Process all files in the 'data/transformed' folder that follow the naming pattern
    'transformed_filtered_{type}_{year}-{month}.parquet' and save them to 'data/time_series' 
    with the name 'ts_{type}_{year}-{month}.parquet'.
    
    Args:
    - input_dir: Directory where the transformed parquet files are located.
    - output_dir: Directory where the time series parquet files will be saved.
    - n_features: Number of previous time steps to use as features (default is 24).
    
    Returns:
    - None: The function saves the processed files in the output directory.
    """
    logger = get_logger()
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Iterate over all parquet files in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith('.parquet') and filename.startswith('transformed_filtered_'):
            try:
                # Remove the prefix and suffix to get the core file name
                file_core = filename.replace('transformed_filtered_', '').replace('.parquet', '')

                # Use the last underscore to separate the type from the year-month
                data_type, year_month = file_core.rsplit('_', 1)
                
                # Extract year and month from the year_month string
                year, month = year_month.split('-')
                # Create the new filename following the pattern ts_{type}_{year}-{month}.parquet
                output_filename = f'ts_{data_type}_{year}-{month}.parquet'
                output_path = os.path.join(output_dir, output_filename)

                # Skip the file if it already exists
                if os.path.exists(output_path):
                    logger.info(f"{output_filename} already exists. Skipping.")
                    continue

                # Full path to the current input file
                input_path = os.path.join(input_dir, filename)

                # Process the file to get the time series data for each PULocationID
                logger.info(f"Processing {filename}...")
                final_df = process_feature_target_by_PULocationID(input_path, n_features)

                # Save the processed DataFrame as a parquet file
                final_df.to_parquet(output_path)
                logger.info(f"Saved {output_filename} to {output_dir}")

            except Exception as e:
                logger.info(f"Error processing {filename}: {e}")