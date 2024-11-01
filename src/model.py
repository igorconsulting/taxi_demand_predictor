from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import FunctionTransformer
from src.feature_engineering import TemporalFeatures, average_rides_last_4_weeks
from sklearn.pipeline import Pipeline
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
