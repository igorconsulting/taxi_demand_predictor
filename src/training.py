from datetime import datetime
from typing import Tuple
import pandas as pd

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

    return X_train, X_test, y_train, y_test