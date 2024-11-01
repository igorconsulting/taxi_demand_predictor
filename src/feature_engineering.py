import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

class TemporalFeatures(BaseEstimator, TransformerMixin):

    def fit(self, X, y=None):
        return self
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X_ = X.copy()
        X_['hour'] = X_.pickup_hour.dt.hour
        X_['day_of_week'] = X_.pickup_hour.dt.dayofweek
        X_['day_of_month'] = X_.pickup_hour.dt.day
        X_['month'] = X_.pickup_hour.dt.month
        X_['year'] = X_.pickup_hour.dt.year

        return X_.drop(columns=['pickup_hour'])

# Function to calculate the average rides over the last 4 weeks
def average_rides_last_4_weeks(X: pd.DataFrame) -> pd.DataFrame:
    avg_rides_last_4_weeks = X[[f'rides_previous_{24*7*i}' for i in range(1, 5)]].mean(axis=1)
    return X.assign(avg_rides_last_4_weeks=avg_rides_last_4_weeks)
