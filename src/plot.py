from typing import Optional
from datetime import timedelta
import plotly.express as px
import pandas as pd

def plot_one_sample(
        features: pd.DataFrame,
        target: pd.Series,
        sample_idx: int,
        predictions: Optional[pd.Series] = None,
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
    - predictions (Optional[pd.Series], optional): Series containing predicted values, if available. 
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

    # Retrieve specific feature sample based on the sample index
    feature_sample = features.iloc[sample_idx].copy()
    
    # Retrieve target sample value if available
    target_sample = target.iloc[sample_idx]

    # Extract columns that represent time series features (assumed to start with "feature_")
    ts_columns = [column for column in features.columns if column.startswith('feature_')]
    
    # Combine feature values and target sample into a single list for plotting
    ts_values = [feature_sample[column] for column in ts_columns] + [target_sample]

    # Create a range of dates based on the length of the time series columns, ending with `pickup_hour`
    ts_dates = pd.date_range(
        start=feature_sample['pickup_hour'] - timedelta(hours=len(ts_columns)),
        end=feature_sample['pickup_hour'],
        freq='H'
    )

    # Generate the base line plot with past values from the time series
    title = (
        f'Pick up hour = {feature_sample["pickup_hour"]}, location_id={feature_sample["PULocationID"]}'
        if display_title else None
    )
    fig = px.line(
        x=ts_dates,
        y=ts_values,
        template='plotly_dark',
        markers=True,
        title=title
    )
    
    # If target is provided, add a green dot for the actual target value to indicate the prediction target
    if target_sample is not None:
        fig.add_scatter(
            x=[ts_dates[-1]],
            y=[target_sample],
            line_color='green',
            mode='markers',
            marker_size=10,
            name='Actual value'
        )
    
    # If predictions are provided, add a red "X" marker to indicate the predicted value
    if predictions is not None:
        fig.add_scatter(
            x=[ts_dates[-1]],
            y=[predictions.iloc[sample_idx]],
            line_color='red',
            mode='markers',
            marker_size=15,
            marker_symbol='x',
            name='Predicted value'
        )
    
    return fig  # Return the Plotly figure for further display or processing


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

    # Filter data for specified locations if provided
    if locations is not None:
        ts_data_to_plot = ts_data[ts_data['PULocationID'].isin(locations)].copy()
    else:
        ts_data_to_plot = ts_data.copy()  # Use the full dataset if no specific locations are given

    # Create a line plot of rides over time, grouped by location (PULocationID)
    fig = px.line(
        ts_data_to_plot,
        x='pickup_hour',
        y='rides',
        color='PULocationID',  # Color lines by PULocationID for distinct representation
        template='none'  # Use a minimal Plotly template for a clean look
    )

    # Display the interactive plot
    fig.show()
