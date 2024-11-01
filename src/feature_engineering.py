from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import FunctionTransformer
import pandas as pd

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

# Function that builds and returns the complete pipeline
def get_pipeline(model, **hyperparameter) -> Pipeline:
    # Transformer for average rides calculation
    add_feature_average_rides_last_4_weeks = FunctionTransformer(
        average_rides_last_4_weeks, validate=False
        )
    
    # FunctionTransformer for time-based features
    add_temporal_features = TemporalFeatures()

    # Instantiate model with or without hyperparameters
    if hyperparameter:
        model_instance = model(**hyperparameter)
    else:
        model_instance = model()

    # Create and return the pipeline with the specified transformers and model
    return make_pipeline(
        add_feature_average_rides_last_4_weeks,
        add_temporal_features,
        model_instance
    )


