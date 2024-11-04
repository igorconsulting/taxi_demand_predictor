from pathlib import Path
from src.extract import fetch_data_if_not_exists, validate_and_process_data 
from src.transform import process_feature_target_by_PULocationID,transform_to_time_series_data
from src.paths import *
from src.logger import get_logger
import pandas as pd
from metaflow import FlowSpec, step

class DataPipelineFlow(FlowSpec):
    
    def save_dataframe(self, df, path, filename, message):
        """Helper function to save a DataFrame and log the action."""
        file_path = Path(path) / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(file_path)
        self.logger.info(f'{message} {file_path}')
        return file_path

    @step
    def start(self):
        """Initializes parameters and starts the pipeline."""
        self.n_features = 24 * 28
        self.step_size = 23
        self.paths = [YELLOW]  # Expand paths as needed
        self.years = range(2022, 2025)
        self.months = range(1, 13)
        self.logger = get_logger()
        self.next(self.download_and_validate_data)

    @step
    def download_and_validate_data(self):
        """Downloads and validates data for each PATH, year, and month."""
        for path in self.paths:
            for year in self.years:
                for month in self.months:
                    fetch_data_if_not_exists(path, year, month)
                    validate_and_process_data(path, year, month)
        self.next(self.transform_data_to_time_series)

    @step
    def transform_data_to_time_series(self):
        """Transforms filtered data into time-series format for each PATH."""
        self.time_series_data_dict = {}
        for path in self.paths:
            df_time_series = transform_to_time_series_data(path, self.logger)
            if df_time_series.empty:
                self.logger.info(f"No data available for transformation for {path}.")
            else:
                # Save the final transformed time-series data
                self.save_dataframe(df_time_series, TIME_SERIES_DATA_DIR, f"{path}_time_series.parquet", "Saved final time-series data to")
                self.time_series_data_dict[path] = df_time_series
        self.next(self.transform_to_feature_target)

    @step
    def transform_to_feature_target(self):
        """Transforms time-series data into feature-target format for modeling."""
        self.transformed_data = {}
        for path, df_time_series in self.time_series_data_dict.items():
            feature_df, target_df = process_feature_target_by_PULocationID(
                df_time_series, n_features=self.n_features, step_size=self.step_size
            )
            feature_target_df = feature_df.join(pd.DataFrame(target_df, columns=['target_rides_next_hour']))
            self.save_dataframe(feature_target_df, TRANSFORMED_DATA_DIR, f"{path}_features_target.parquet", "Saved transformed feature-target data to")
            self.transformed_data[path] = feature_target_df
        self.next(self.end)

    @step
    def end(self):
        """Finalizes the pipeline and logs completion."""
        self.logger.info("Data pipeline completed successfully.")

if __name__ == '__main__':
    DataPipelineFlow()
