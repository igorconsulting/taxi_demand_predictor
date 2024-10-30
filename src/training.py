from datetime import datetime
from typing import Tuple
import pandas as pd
from src.logger import get_logger

def train_test_split(
        df: pd.DataFrame,
        cutoff_date: datetime,
        target_column_name: str):
    """
    Split the data into training and testing sets
    :param df: the input DataFrame
    :param cutoff_date: the cutoff date
    :param target_column_name: the name of the target column
    :return: a tuple of two DataFrames: (training, testing)
    """
    training = df[df['pickup_hour'] < cutoff_date].reset_index(drop=True)
    test = df[df['pickup_hour'] >= cutoff_date].reset_index(drop=True)

    X_train = training.drop(columns=[target_column_name])
    y_train = training[target_column_name]

    X_test = test.drop(columns=[target_column_name])
    y_test = test[target_column_name]

    return X_train, y_train, X_test, y_test


def create_training_sets(PATH, cutoff_date, target_column_name): 
    """
    Generate separate training and testing sets for each PULocationID.
    
    :param PATH: Path to the data file.
    :param cutoff_date: The cutoff date for splitting into train/test sets.
    :param target_column_name: The name of the target column.
    :return: A dictionary where each PULocationID has its (X_train, X_test, y_train, y_test).
    """
    logger = get_logger()
    data = pd.read_parquet(PATH)
    
    # Dictionary to store training/testing sets for each PULocationID
    training_sets = {}

    for pulocation_id in data['PULocationID'].unique():
        logger.info(f"Processing PULocationID: {pulocation_id}")
        
        # Filter data specific to each PULocationID
        location_data = data[data['PULocationID'] == pulocation_id].copy()
        location_data.drop(columns=['PULocationID'], inplace=True)
        # Perform train/test split for this location
        X_train, X_test, y_train, y_test = train_test_split(
            location_data,
            cutoff_date,
            target_column_name
        )

        # Store the result in the dictionary
        training_sets[pulocation_id] = (X_train, X_test, y_train, y_test)

    return training_sets

def get_cutoff_training_date(data: pd.DataFrame, n_months=6) -> pd.Timestamp:
    """
    Determines the cutoff date for training data based on a specified number of months before the latest date in the dataset.

    This function calculates a cutoff date by subtracting a given number of months (default is 6) from the latest date in the `pickup_hour` column.
    This cutoff date can be used to split the dataset into training and testing periods, allowing the model to be trained only on data up to that point.

    Parameters:
    -----------
    data : pd.DataFrame
        The input DataFrame containing a 'pickup_hour' column with date and time values, which must be in datetime format.
    n_months : int, optional
        The number of months before the latest date to set as the cutoff for training data. Default is 6 months.

    Returns:
    --------
    pd.Timestamp
        The cutoff date for training, which is `n_months` months before the latest date in the 'pickup_hour' column.

    Example:
    --------
    >>> cutoff_date = get_cutoff_training_date(data, n_months=6)
    >>> training_data = data[data['pickup_hour'] < cutoff_date]
    >>> testing_data = data[data['pickup_hour'] >= cutoff_date]
    """
    
    # Ensure the 'pickup_hour' column is in datetime format
    data['pickup_hour'] = pd.to_datetime(data['pickup_hour'])

    # Find the latest date in the 'pickup_hour' column
    last_date = data['pickup_hour'].max()

    # Calculate the cutoff date by subtracting n_months from last_date
    cutoff_training_date = last_date - pd.DateOffset(months=n_months)

    return cutoff_training_date