from src.extract import download_monthly_data, validate_and_filter_year_month, concat_filtered_data
from src.transform import process_filtered_dataframe, add_missing_slots, process_feature_target_by_PULocationID
from src.filtering import filter_by_date_range, select_important_columns
from src.paths import ( 
                       TIME_SERIES_DATA_DIR, 
                       TRANSFORMED_DATA_DIR, 
                       YELLOW
                       )

def run_pipeline(n_features=24, step_size=1):
    """
    Executa a pipeline para cada PATH, considerando o número de features e o passo da janela.
    """
    for PATH in [YELLOW]:  # Pode incluir GREEN, FHV, FHVHV conforme necessário
        for year in range(2019, 2022):
            for month in range(1, 13):
                # Download e validação dos dados mensais
                download_monthly_data(PATH, year, month)
                validate_and_filter_year_month(PATH, year, month)
        
        # Concatena os dados filtrados e salva no diretório de séries temporais
        df_filtered = concat_filtered_data(PATH)
        if not df_filtered.empty:
            df_filtered.to_parquet(f'{TIME_SERIES_DATA_DIR}/{PATH}_filtered.parquet')
            print(f'Saved concatenated filtered data to {TIME_SERIES_DATA_DIR}/{PATH}_filtered.parquet')
        else:
            print(f"No filtered data to concatenate for {PATH}.")
            continue
        
        # Processamento adicional: agrupamento e preenchimento de slots
        df_grouped = process_filtered_dataframe(df_filtered)
        df_grouped = add_missing_slots(df_grouped)
        df_grouped.to_parquet(f'{TIME_SERIES_DATA_DIR}/{PATH}_grouped.parquet')
        print(f'Saved grouped data to {TIME_SERIES_DATA_DIR}/{PATH}_grouped.parquet')

        # Geração de features e target considerando step_size
        feature_target_df = process_feature_target_by_PULocationID(df_grouped, n_features=n_features, step_size=step_size)
        feature_target_df.to_parquet(f'{TRANSFORMED_DATA_DIR}/{PATH}_features_target.parquet')
        print(f'Saved transformed feature-target data to {TRANSFORMED_DATA_DIR}/{PATH}_features_target.parquet')

if __name__ == '__main__':
    # Executando com parâmetros configuráveis
    run_pipeline(n_features=24, step_size=1)