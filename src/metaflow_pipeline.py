from pathlib import Path
from src.extract import concatenate_filtered_data
from src.transform import process_filtered_dataframe, add_missing_slots, process_feature_target_by_PULocationID
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
        self.next(self.concatenate_data)

    @step
    def concatenate_data(self):
        """Concatenates filtered data for each PATH into a single DataFrame."""
        self.df_concatenated_dict = {}
        for path in self.paths:
            df_concatenated = concatenate_filtered_data(path)
            if df_concatenated.empty:
                self.logger.info(f"No filtered data to concatenate for {path}.")
            else:
                self.save_dataframe(df_concatenated, TIME_SERIES_DATA_DIR, f"{path}.parquet", "Saved concatenated data to")
                self.df_concatenated_dict[path] = df_concatenated
        self.next(self.process_data)

    @step
    def process_data(self):
        """Processes data by grouping and filling missing slots."""
        self.grouped_dfs = {}
        for path, df_filtered in self.df_concatenated_dict.items():
            df_grouped = add_missing_slots(process_filtered_dataframe(df_filtered))
            self.save_dataframe(df_grouped, TIME_SERIES_DATA_DIR, f"{path}_grouped.parquet", "Saved grouped data to")
            self.grouped_dfs[path] = df_grouped
        self.next(self.transform_data)

    @step
    def transform_data(self):
        """Transforms data into a feature-target format for modeling."""
        self.transformed_data = {}
        for path, df_grouped in self.grouped_dfs.items():
            feature_df, target_df = process_feature_target_by_PULocationID(
                df_grouped, n_features=self.n_features, step_size=self.step_size
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
