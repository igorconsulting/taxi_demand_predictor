from src.extract import download_monthly_data, validate_and_filter_year_month, concat_filtered_data
from src.transform import process_filtered_dataframe, add_missing_slots, process_feature_target_by_PULocationID
from src.paths import *
from src.logger import get_logger
import pandas as pd

def run_pipeline(n_features=24, step_size=1):
    """
    Runs the data processing pipeline for each PATH, using n_features for the sliding window and step_size for window shifts.
    
    Args:
    - n_features: Number of previous time steps to use as features.
    - step_size: Number of steps to shift the window after each iteration.
    """
    logger = get_logger()
    for PATH in [YELLOW]:  # Add other paths as needed, e.g., GREEN, FHV, FHVHV
        # Step 1: Download and validate data for each month
        for year in range(2019, 2022):
            for month in range(1, 13):
                logger.info(f"Processing {PATH} data for {year}-{month}")
                download_monthly_data(PATH, year, month)
                logger.info(f"Downloaded {PATH} data for {year}-{month}")
                validate_and_filter_year_month(PATH, year, month)
                logger.info(f"Validated and filtered {PATH} data for {year}-{month}")
        
        # Step 2: Concatenate all filtered data and proceed if not empty
        df_filtered = concat_filtered_data(PATH)
        if df_filtered.empty:
            logger.info(f"No filtered data to concatenate for {PATH}.")
            continue  # Skip to the next PATH if no data
        df_filtered.to_parquet(f'{TIME_SERIES_DATA_DIR}/{PATH}_filtered.parquet')
        logger.info(f'Saved concatenated filtered data to {TIME_SERIES_DATA_DIR}/{PATH}_filtered.parquet')

        # Step 3: Process the data to fill missing slots and save grouped data
        df_grouped = process_filtered_dataframe(df_filtered)
        df_grouped = add_missing_slots(df_grouped)
        grouped_path = f'{TIME_SERIES_DATA_DIR}/{PATH}_grouped.parquet'
        df_grouped.to_parquet(grouped_path)
        logger.info(f'Saved grouped data to {grouped_path}')

        # Step 4: Transform data to feature-target format using sliding window and step size
        feature_df, target_df = process_feature_target_by_PULocationID(
            df_grouped, n_features=n_features, step_size=step_size
        )
        target_df = pd.DataFrame(target_df, columns=['target_rides_next_hour'])
        features_transformed_path = f'{TRANSFORMED_DATA_DIR}/{PATH}_features_target.parquet'
        # join feature and target data
        feature_target_df = feature_df.join(target_df)

        feature_target_df.to_parquet(features_transformed_path)
        logger.info(f'Saved transformed feature-target data to {features_transformed_path}')

if __name__ == '__main__':
    # Run the pipeline with specific parameters for n_features and step_size
    run_pipeline(n_features=24*28, step_size=24)