from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
from pdb import set_trace as stop
from src.extract import fetch_data_if_not_exists
from src.filtering import select_important_columns,filter_by_date_range

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

from src.paths import RAW_DATA_DIR, TRANSFORMED_DATA_DIR

#refactoring the load_raw_data function
def load_raw_data(
        PATH,
        year: int,
        months: Optional[List[int]] = None
    ) -> pd.DataFrame:
    """
    Loads raw data from local storage or downloads it from the NYC website, and
    then loads it into a Pandas DataFrame

    Args:
        year: year of the data to download
        months: months of the data to download. If `None`, download all months

    Returns:
        pd.DataFrame: DataFrame with the following columns:
            - pickup_datetime: datetime of the pickup
            - pickup_location_id: ID of the pickup location
    """  
    rides = pd.DataFrame()
    
    if months is None:
        # download data for the entire year (all months)
        months = list(range(1, 13))
    elif isinstance(months, int):
        # download data only for the month specified by the int `month`
        months = [months]

    for month in months:
        
        local_file = RAW_DATA_DIR / f'{PATH}_{year}-{month:02d}.parquet'
        if not local_file.exists():
            try:
                # download the file from the NYC website
                print(f'Downloading file {year}-{month:02d}')
                fetch_data_if_not_exists(year, month)
            except:
                print(f'{year}-{month:02d} file is not available')
                continue
        else:
            print(f'File {year}-{month:02d} was already in local storage') 

        # load the file into Pandas
        rides_one_month = pd.read_parquet(local_file)

        # rename columns
        rides_one_month = select_important_columns(rides_one_month, PATH)
        
        rides_one_month = filter_by_date_range(rides_one_month, year, month, date_column='pickup_datetime')

        # append to existing data
        rides = pd.concat([rides, rides_one_month])

    if rides.empty:
        # no data, so we return an empty dataframe
        return pd.DataFrame()
    else:
        # keep only time and origin of the ride
        rides = rides[['pickup_datetime', 'pickup_location_id']]
        return rides