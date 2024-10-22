import os
import pandas as pd
from tqdm import tqdm

# Step 1: Function to add the 'pickup_hour' column and group data by 'pickup_hour' and 'PULocationID'
def process_filtered_dataframe(df):
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
def process_all_filtered_files(input_dir='../data/filtered', output_dir='../data/transformed'):
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
            print(f"Processing file: {filename}")

            # Read the parquet file
            df = pd.read_parquet(file_path)
            
            # Process the dataframe (add pickup_hour, group by, and add missing slots)
            df_grouped = process_filtered_dataframe(df)
            complete_df_grouped = add_missing_slots(df_grouped)

            # Save the transformed dataframe as a parquet file in the output directory
            complete_df_grouped.to_parquet(output_path)
            print(f"Saved transformed file: {output_file}")
