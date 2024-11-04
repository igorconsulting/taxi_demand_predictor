from src.extract import fetch_data_if_not_exists, validate_and_process_data
from src.transform import process_feature_target_by_PULocationID, transform_to_time_series_data
from src.paths import *
from src.logger import get_logger
import pandas as pd
from pathlib import Path

def save_dataframe(df, path, filename, message, logger):
    """Helper function to save a DataFrame and log the action."""
    file_path = Path(path) / filename
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(file_path)
    logger.info(f'{message} {file_path}')
    return file_path

def run_pipeline(n_features=24, step_size=1):
    """Runs the data processing pipeline for each PATH."""
    logger = get_logger()
    for path in [YELLOW]:  # Expand paths as needed
        for year in range(2022, 2025):
            for month in range(1, 13):
                fetch_data_if_not_exists(path, year, month)
                validate_and_process_data(path, year, month)
        
        # Transform data to time-series format
        df_time_series = transform_to_time_series_data(path, logger)
        if df_time_series.empty:
            continue

        # Save the final transformed time-series data
        save_dataframe(df_time_series, TIME_SERIES_DATA_DIR, f"{path}_time_series.parquet", "Saved final time-series data to", logger)

        # Transform to feature-target format and save
        feature_df, target_df = process_feature_target_by_PULocationID(df_time_series, n_features=n_features, step_size=step_size)
        feature_target_df = feature_df.join(pd.DataFrame(target_df, columns=['target_rides_next_hour']))
        save_dataframe(feature_target_df, TRANSFORMED_DATA_DIR, f"{path}_features_target.parquet", "Saved transformed feature-target data to", logger)
        
        logger.info(f"Pipeline completed for {path}.")

if __name__ == '__main__':
    run_pipeline(n_features=24*28, step_size=23)
