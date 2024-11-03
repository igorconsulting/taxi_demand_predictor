import pandas as pd
from src.paths import FHV, PATH_DATETIME
from src.logger import get_logger

logger = get_logger()

def filter_by_date_range(df, year, month, date_column):
    """Filter DataFrame to include only rows within the specified date range."""
    df[date_column] = pd.to_datetime(df[date_column])
    return df[(df[date_column].dt.year == year) & (df[date_column].dt.month == month)].copy()

def select_important_columns(df, path):
    """Select and rename columns based on the path type."""
    columns_map = {PATH_DATETIME[path]: 'pickup_datetime'}
    if path == FHV:
        columns_map['PUlocationID'] = 'PULocationID'
    df = df.rename(columns=columns_map).copy()
    return df[['pickup_datetime', 'PULocationID']].dropna()
