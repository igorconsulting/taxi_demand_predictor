from src.extract import fetch_data_if_not_exists, validate_and_process_data, concatenate_filtered_data
from src.transform import process_filtered_dataframe, add_missing_slots, process_feature_target_by_PULocationID
from src.paths import *
from src.logger import get_logger
import pandas as pd

def save_and_log_dataframe(df, path, logger, message):
    """Saves a DataFrame to a specified path and logs the action."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)
    logger.info(f'{message} {path}')

def run_pipeline(n_features=24, step_size=1):
    """Runs the data processing pipeline for each PATH."""
    logger = get_logger()
    for path in [YELLOW]:  # Expand paths as needed
        for year in range(2022, 2025):
            for month in range(1, 13):
                fetch_data_if_not_exists(path, year, month)
                validate_and_process_data(path, year, month)
        
        df_filtered = concatenate_filtered_data(path)
        if df_filtered.empty:
            logger.info(f"No filtered data to concatenate for {path}.")
            continue

        # Save concatenated data
        filtered_path = Path(TIME_SERIES_DATA_DIR) / f"{path}.parquet"
        save_and_log_dataframe(df_filtered, filtered_path, logger, "Saved concatenated data to")

        # Process and save grouped data
        df_grouped = add_missing_slots(process_filtered_dataframe(df_filtered))
        grouped_path = Path(TIME_SERIES_DATA_DIR) / f"{path}_grouped.parquet"
        save_and_log_dataframe(df_grouped, grouped_path, logger, "Saved grouped data to")

        # Transform to feature-target format and save
        feature_df, target_df = process_feature_target_by_PULocationID(df_grouped, n_features=n_features, step_size=step_size)
        feature_target_df = feature_df.join(pd.DataFrame(target_df, columns=['target_rides_next_hour']))
        feature_target_path = Path(TRANSFORMED_DATA_DIR) / f"{path}_features_target.parquet"
        save_and_log_dataframe(feature_target_df, feature_target_path, logger, "Saved transformed feature-target data to")
        
        logger.info(f"Pipeline completed for {path}.")

if __name__ == '__main__':
    run_pipeline(n_features=24*28, step_size=23)