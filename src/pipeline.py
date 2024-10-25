from src.extract import *
from src.transform import *
from src.filtering import *
from src.paths import *

def run_pipeline():
    # Ensure PATH includes the underscore in each iteration
    for PATH in [YELLOW]:#, GREEN, FHV, FHVHV]:
        for year in range(2019, 2022):
            for month in range(1, 13):
                download_monthly_data(PATH, year, month)
                validate_and_filter_year_month(PATH, year, month)
        
        # Concatenate all processed data and save
        concat_filtered_data(PATH)

        df_grouped = process_filtered_dataframe(PATH)
        df_grouped = add_missing_slots(df_grouped)
        df_grouped.to_parquet(f'{TIME_SERIES_DATA_DIR}/{PATH}_grouped.parquet')
        print(f'Saved grouped data to {TIME_SERIES_DATA_DIR}/{PATH}_grouped.parquet')
        # get_cutoff_indices(df_grouped, PATH)

        #cutoff_indices = get_cutoff_indices(df_grouped, n_features=24)
        feature_target_df = process_feature_target_by_PULocationID(
                            df_grouped, n_features=24
                            )
        feature_target_df.to_parquet(f'{TRANSFORMED_DATA_DIR}/{PATH}.parquet')
        print(f'Saved transformed data to {TRANSFORMED_DATA_DIR}/{PATH}.parquet')


if __name__ == '__main__':
    run_pipeline()