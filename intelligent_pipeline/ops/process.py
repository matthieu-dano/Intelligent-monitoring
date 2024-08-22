from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM

import json
import numpy as np
import pandas as pd

def generate_query(metrics, start_absolute, end_absolute="", tag={}, resample=0, limit=None):
    metrics_part = []  # Initialize the metrics_part as a list
    tags_name = list(tag.keys())  # Get the list of tag names

    # Iterate through each metric and format it into the metrics_part
    for metric in metrics:
        metric_dict = {"name": metric}

        # Add tags if they exist
        if tags_name:
            metric_dict["tags"] = {key: [tag[key]] for key in tags_name}

        # Add aggregator if resample is different from 0
        if resample != 0:
            metric_dict["aggregators"] = [
                {
                    "name": "first",
                    "align_end_time": False,
                    "align_start_time": True,
                    "sampling": {"value": str(resample), "unit": "SECONDS"},
                    "trim": False,
                }
            ]
        # Add limit if it is not None
        if limit is not None:
            metric_dict["limit"] = limit

        metrics_part.append(metric_dict)

    # Construct the query dictionary based on whether end_absolute is provided
    query_dict = {"metrics": metrics_part, "time_zone": "UCT", "start_absolute": start_absolute}

    if end_absolute:
        query_dict["end_absolute"] = end_absolute

    # Convert the query dictionary to a formatted JSON string
    query = json.dumps(query_dict, indent=4)

    return query


def isolation_forest(
    df, window_size="30D", sigma=3, min_periods=5, widow_size_norm=None, min_periods_norm=None
):
    """
    Combines rolling isolation forest and normalization for anomaly detection.

    Parameters:
    - df: DataFrame, input data containing the time series.
    - window_size: str or DateOffset, size of the rolling window for isolation
    forest (e.g., '30D').
    - sigma: int or float, number of standard deviations for anomaly threshold.
    - min_periods: int or None, minimum number of observations required for
    isolation forest.
    - widow_size_norm: str or DateOffset, size of the rolling window for
    isolation forest (e.g., '30D').
    - min_periods_norm: int or None, minimum number of observations required
    for normalization.

    Returns:
    - result_df: DataFrame, normalized anomaly scores indexed by time. When score
    > 1 this is an anomaly.
    """
    if widow_size_norm is None:
        widow_size_norm = window_size
    if min_periods_norm is None:
        min_periods_norm = min_periods
        
    
    score = rolling_isolation_forest(df, window_size, min_periods)
    result_df = rolling_normalisation(score, sigma, widow_size_norm, min_periods_norm)
    df[score.name] = result_df[score.name]
    return df


def rolling_isolation_forest(df, window_size="30D", min_periods=5):
    """
    Calculates anomaly scores using rolling isolation forest method.

    Parameters:
    - df: DataFrame, input data containing the time series.
    - window_size: str or DateOffset, size of the rolling window for
    isolation forest (e.g., '30D').
    - min_periods: int or None, minimum number of observations
    required to have a valid result.

    Returns:
    - result_df: Series, anomaly scores indexed by time.

    Notes:
    - Uses IsolationForest from sklearn. The anomaly score is
    negated and appended
      to isolation_forest_scores if the window size is sufficient.
    """
    df = df.dropna()
    isolation_forest_scores = []
    for end in range(len(df)):
        end_date = df.index[end]
        start_date = max(end_date - pd.Timedelta(window_size), df.index[0])
        start = df.index.get_loc(df.index[df.index <= start_date][-1])
        window = df.iloc[start:end]
        if len(window) >= min_periods:
            model = IsolationForest()
            model.fit(window)
            current_score = model.decision_function([df.iloc[end]])[0]
            current_score *= -1  # Invert score to get anomaly score
            isolation_forest_scores.append(current_score)

    len_df = len(isolation_forest_scores)
    if len_df > 0:
        result_series = pd.Series(isolation_forest_scores, index=df.index[-len_df:])
    else:
        result_series = pd.Series(isolation_forest_scores)
    result_series.name = df.columns[0].split("_dagster")[0] + "_dagster_iso_score"
    return result_series


def rolling_normalisation(score, sigma, window, min_periods=None):
    """
    Performs rolling normalization of anomaly scores.

    Parameters:
    - score: Series
    Anomaly scores to be normalized.
    - sigma: int or float
    Number of standard deviations for the anomaly threshold.
    - window: str or DateOffset
    Size of the rolling window for normalization (e.g., '30D').
    - min_periods: int or None
    Minimum number of observations required for a valid result.

    Returns:
    - result_df: DataFrame
    Normalized anomaly scores indexed by time. Scores greater than 1 indicate an anomaly.
    """
    rolling = score.rolling(window=window, min_periods=min_periods)
    std_res = rolling.std().shift(1)
    mean_res = rolling.mean().shift(1)
    anomaly_score = ((score - mean_res) / (std_res + 1e-8)) / sigma  # Normalize scores
    result_df = pd.DataFrame({score.name: anomaly_score}, index=score.index)
    result_df = result_df.dropna()
    return result_df


def process_one_class_svm(df, metric, last_date):
    df = df.bfill().ffill()
    ocsvm = OneClassSVM(kernel="rbf", gamma="auto", nu=0.1)
    ocsvm.fit(df)
    scores = ocsvm.decision_function(df)
    df[f"{metric}_dagster_OCSVM_anomaly_score"] = scores

    ref_date = pd.to_datetime(last_date, unit="ms")
    new_df = df[df.index > ref_date]

    return new_df


def resample(df, metric):
    frequency = "10s"
    df = df.resample(frequency).first()
    df = df.bfill().ffill()
    df = df.rename(columns={metric: metric + "_dagster_lttb_resample_" + frequency})
    return df


def calculate_statistics(df, target, frequency="60T"):
    # Calculate energy over a one-day window

    # Group data by day and calculate other aggregated statistics
    daily_metrics = df.resample(frequency, label='right').agg(
        {target: ["mean", "count", "median", "std", "skew", "min", "max"]}
    )

    # Calculate energy over a one-day window
    def calculate_energy(x):
        if len(x) > 0:
            return np.sum(x**2) / len(x)
        else:
            return np.nan

    daily_metrics["energy"] = df[target].resample(frequency, label='right').apply(calculate_energy)

    # Calculate kurtosis over a one-day window
    daily_metrics["kurt"] = df[target].resample(frequency, label='right').apply(lambda x: x.kurt())

    daily_metrics['max_diff'] = df[target].resample(frequency, label='right').apply(lambda x: abs(x.diff()).max())

    # Rename columns for better readability
    daily_metrics.columns = [
        f"{target}_dagster_{stat}_{frequency}"
        for stat in ["mean", "count", "median", "std", "skew", "kurt", "min",
                     "max", "energy", "max_diff"]
    ]

    return daily_metrics


def get_list_metrics_aggregated(df):
    """
    Get the list of metrics to be pushed to the Skyminer API.

    Parameters:
    df (pd.DataFrame): The input DataFrame.

    Returns:
    list: The list of metrics to be pushed to the Skyminer API.
    """
    return [metric for metric in df.columns if metric not in ["date", "satellite"]]

def ma(df: pd.DataFrame,
       window: str = '30D',
       min_periods: int = 48,
       sigma: float = 4,
       delta: float = 0.01,
       return_aggregated: bool = False,
       return_bounds: bool = True,):
    """
    Perform moving average and anomaly detection on a DataFrame.

    Parameters:
    df (pd.DataFrame): The input DataFrame containing time-series data.
    resampling_window (str, optional): The time window for resampling the data.
    Default is '1H'
    (Y year, D day, H hour, T minute, s second).
    window (str, optional): The rolling window size for calculating the mean and
    standard deviation.
    Default is '7H'
    (Y year, D day, H hour, T minute, s second).
    min_periods (int, optional): The minimum number of observations in a
    window required to have a value. Default is 1.
    sigma (float, optional): The number of standard deviations to consider as a threshold
    for anomalies. Default is 3.
    delta (float, optional): A small constant added to the upper and lower bounds to
    control the threshold. Default is
     0.01.
    return_aggregated (bool, optional): Whether to return the dataframe if you used skyminer
    aggregator in query.
     Default is False.
    return_bounds (bool, optional): Whether to return upper and lower anomaly bounds.
    Default is False.

    Returns:
    pd.DataFrame: A DataFrame containing the following columns:
        - 'column_name_anomaly_score': Anomaly scores for each column in the input DataFrame.
        - 'column_name_anomaly': Anomaly values for each column in the input DataFrame.
        - 'column_name_upper_bound': Upper bounds for anomaly detection (if return_bounds is True).
        - 'column_name_lower_bound': Lower bounds for anomaly detection (if return_bounds is True).
        - 'column_name_resampled': The resampled DataFrame (if return_aggregated is True).
    """
    rolling = df.rolling(window=window, min_periods=min_periods, center=False)
    mean = rolling.mean().shift(1)
    residuals = abs(df - mean)
    rolling = residuals.rolling(window=window, min_periods=min_periods, center=False)
    std_res = rolling.std().shift(1)
    mean_res = rolling.mean().shift(1)
    anomaly_score = ((residuals - mean_res - delta)/(std_res + 1e-8))/sigma
    anomaly = df[anomaly_score > 1]
    upper_bound = sigma*(std_res + 1e-8) + mean_res + mean + delta
    lower_bound = -sigma*(std_res + 1e-8) - mean_res + mean - delta
    if return_aggregated:
        if return_bounds:
            list_results = [anomaly_score, anomaly, upper_bound, lower_bound, df]
            list_extension = ['_ma_anomaly_score', '_ma_anomaly', '_ma_upper_bound',
                              '_ma_lower_bound', '_ma_aggregated']
        else:
            list_results = [anomaly_score, anomaly, df]
            list_extension = ['_ma_anomaly_score', '_ma_anomaly', '_ma_aggregated']
    else:
        if return_bounds:
            list_results = [anomaly_score, anomaly, upper_bound, lower_bound]
            list_extension = ['_ma_anomaly_score', '_ma_anomaly', '_ma_upper_bound',
                              '_ma_lower_bound']
        else:
            list_results = [anomaly_score, anomaly]
            list_extension = ['_ma_anomaly_score', '_ma_anomaly']
    # list to dataframe
    dataframe = pd.DataFrame()
    for result in list_results:
        dataframe = pd.concat([dataframe, result], axis=1)
    dataframe.columns = [col + extension for extension in list_extension for col in df.columns]
    return dataframe