from src.extract import *
from src.transform import *
from src.filtering import *
from src.paths import *

def run_pipeline(n_features=24, step_size=1):
    """
    Runs the data processing pipeline for each PATH, using n_features for the sliding window and step_size for window shifts.
    
    Args:
    - n_features: Number of previous time steps to use as features.
    - step_size: Number of steps to shift the window after each iteration.
    """
    for PATH in [YELLOW]:  # Add other paths as needed, e.g., GREEN, FHV, FHVHV
        # Step 1: Download and validate data for each month
        for year in range(2019, 2022):
            for month in range(1, 13):
                download_monthly_data(PATH, year, month)
                validate_and_filter_year_month(PATH, year, month)
        
        # Step 2: Concatenate all filtered data and proceed if not empty
        df_filtered = concat_filtered_data(PATH)
        if df_filtered.empty:
            print(f"No filtered data to concatenate for {PATH}.")
            continue  # Skip to the next PATH if no data
        df_filtered.to_parquet(f'{TIME_SERIES_DATA_DIR}/{PATH}_filtered.parquet')
        print(f'Saved concatenated filtered data to {TIME_SERIES_DATA_DIR}/{PATH}_filtered.parquet')

        # Step 3: Process the data to fill missing slots and save grouped data
        df_grouped = process_filtered_dataframe(df_filtered)
        df_grouped = add_missing_slots(df_grouped)
        grouped_path = f'{TIME_SERIES_DATA_DIR}/{PATH}_grouped.parquet'
        df_grouped.to_parquet(grouped_path)
        print(f'Saved grouped data to {grouped_path}')

        # Step 4: Transform data to feature-target format using sliding window and step size
        feature_target_df = process_feature_target_by_PULocationID(
            df_grouped, n_features=n_features, step_size=step_size
        )
        transformed_path = f'{TRANSFORMED_DATA_DIR}/{PATH}_features_target.parquet'
        feature_target_df.to_parquet(transformed_path)
        print(f'Saved transformed feature-target data to {transformed_path}')

if __name__ == '__main__':
    # Run the pipeline with specific parameters for n_features and step_size
    run_pipeline(n_features=24, step_size=1)