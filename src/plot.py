from typing import Optional
from datetime import timedelta
import plotly.express as px
import pandas as pd

def plot_one_sample(
        features: pd.DataFrame,
        target: pd.Series,
        sample_idx: int,
        prediction: Optional[pd.Series] = None,
        display_title: Optional[bool] = True
) -> None:
    """
    Plots a time series sample for a specific pickup location and time. This function generates a line plot for 
    past values (features), along with optional actual target and predicted target values.

    Args:
    - features (pd.DataFrame): DataFrame containing the features (historical ride data).
        Must include columns starting with "feature_" for time series data and "pickup_hour" for timestamp.
    - target (pd.Series): Series with actual target values for each sample (number of rides).
    - sample_idx (int): Index of the sample to plot within the DataFrame and Series.
    - prediction (Optional[pd.Series], optional): Series containing predicted values, if available. 
        Defaults to None.
    - display_title (Optional[bool], optional): Boolean flag to display the title in the plot. 
        Defaults to True.

    Returns:
    - fig (plotly.graph_objects.Figure): Plotly figure containing the time series plot.
    
    The plot displays:
    - Historical values (features) for the selected sample in a line plot.
    - Actual target value as a green dot, if provided.
    - Predicted target value as a red "X" marker, if provided.
    """
    # PULocationID is the location_id
    feature_sample = features.iloc[sample_idx]
    target_sample = target.iloc[sample_idx]
    
    ts_columns = [column for column in features.columns if column.startswith('rides_previous_')]
    ts_values = [feature_sample[column] for column in ts_columns] + [target_sample]

    ts_dates = pd.date_range(
        start = feature_sample['pickup_hour'] - timedelta(hours = len(ts_columns)),
        end = feature_sample['pickup_hour'],
        freq='h'
        )
    
    # line plot with past values
    title = f'Pick up hour = {feature_sample["pickup_hour"]}, location_id={feature_sample["PULocationID"]}'  if display_title else None
    fig = px.line(x=ts_dates,
                  y=ts_values,
                  template = 'plotly_dark',
                  markers=True,
                  title=title)
    
    if target_sample is not None:
        # green dot for the value we want to predict
        fig.add_scatter(x=[ts_dates[-1]],
                        y=[target_sample],
                        line_color='green',
                        mode='markers',
                        marker_size=10,
                        name='actual value')
    
    if prediction is not None:
        # red dot for the predicted value
        fig.add_scatter(x=[ts_dates[-1]],
                        y=[prediction.iloc[sample_idx]],
                        line_color='red',
                        mode='markers',
                        marker_size=15,
                        marker_symbol='x',
                        name='Predicted value')
        
    return fig


def plot_ts(
        ts_data: pd.DataFrame,
        locations: Optional[list] = None
) -> None:
    """
    Plots time series data for rides grouped by pickup location.

    Args:
    - ts_data (pd.DataFrame): DataFrame containing time series data with columns "pickup_hour", "rides", and "PULocationID".
    - locations (Optional[list], optional): List of pickup location IDs to filter the data for specific locations.
      If None, all locations in `ts_data` are plotted. Defaults to None.

    Returns:
    - None: Displays an interactive Plotly line plot of the time series data for the specified locations.
    
    The plot includes:
    - A line plot of ride counts over time ("pickup_hour") for each specified "PULocationID".
    - Color-coded lines for each unique "PULocationID" in the data.
    """

    if locations is not None:
        ts_data_to_plot = ts_data[ts_data['PULocationID'].isin(locations)].copy()
    else:
        ts_data_to_plot = ts_data.copy()

    fig = px.line(
        ts_data_to_plot,
        x='pickup_hour',
        y='target_rides_next_hour',
        color='PULocationID',
        template='none'
    )

    fig.show()
