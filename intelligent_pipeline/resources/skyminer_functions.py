import datetime
import gc
import json
import sys
import time
import warnings
from numbers import Number
from textwrap import wrap

import numpy as np
import pandas as pd
from SkyminerTS import STSAPI, QueriesToDataframe
from SkyminerTS.DataPoint import DataPointBuilder
from SkyminerTS.DataPoint.DataPointsPayloadBuilder import DataPointsPayloadBuilder

warnings.filterwarnings("ignore")


def to_unix_timestamp(date):
    if isinstance(date, str):
        dt = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        return int(dt.timestamp() * 1000)
    else:
        return int(date.timestamp() * 1000)


def simple_process_data(data, factor=2):
    df = data / factor
    df.columns = [f"sch.TOTO_processed_div{factor}"]
    df["time"] = df.index
    print(df)
    return df


def actualsize(input_obj):
    memory_size = 0
    ids = set()
    objects = [input_obj]
    while objects:
        new = []
        for obj in objects:
            if id(obj) not in ids:
                ids.add(id(obj))
                memory_size += sys.getsizeof(obj)
                new.append(obj)
        objects = gc.get_referents(*new)
    return memory_size


def powers_to_trace(list_powers):
    """
    Transform a list of powers in a trace
    :param list_powers: list of powers in db
    """
    list_powers = np.array(list_powers)
    min_val = min(list_powers) * 100
    max_val = max(list_powers) * 100
    trace = str()
    list_powers = (list_powers * 100 - min_val) * 255 / (max_val - min_val)
    list_powers = np.around(list_powers).astype(int)
    list_powers = [f"{x:02x}".upper() for x in list_powers]
    for power in list_powers:
        trace += power
    return trace


def trace_to_powers(trace, min_val, max_val):
    """
    Transform a list of powers in a trace
    :param max_val: float, The maximum value of the trace
    :param min_val: float, The minimum value of the trace
    :param trace: str of hexadecimals
    """
    list_power = np.array([int(x, 16) for x in wrap(trace, 2)])
    list_power = (min_val + list_power * (max_val - min_val) / 255) / 100
    return list_power


def powers_to_spectrum(list_powers, start_freq, end_freq):
    """
    Transform a list of powers in a spectrum
    :param list_powers: list of powers in db
    :param start_freq: start frequency in Hz
    :param end_freq: end frequency in Hz
    """
    x = np.linspace(start_freq, end_freq, len(list_powers)) / 1e6
    df = pd.DataFrame(list_powers, columns=["amplitude"], index=x)
    return df


def check_dataframe(dataframe, data_type, extension):
    r"""
    Check if the dataframe is in good format.

    Parameters:
        dataframe: DataFrame
            If `data_type` is 'value', a dataframe with the timestamp in the
            index and columns with values. Column names will be the metrics
            names.
                timestamp: Epoch timestamp (s, ms, or ns) or UTC date.
                values: Values of the metrics.
            If `data_type` is 'trace', a dataframe with 'timestamp' in the
            index and 7 columns: 'trace', 'start_frequency', 'stop_frequency',
            'res_bw', 'video_bw'.
                timestamp: Epoch timestamp (s, ms, or ns) or UTC date.
                trace: List of powers in dB.
                start_frequency: Start frequency in Hz.
                stop_frequency: Stop frequency in Hz.
                res_bw: Resolution bandwidth in Hz.
                video_bw: Video bandwidth in Hz.
            If `data_type` is 'constellation', a dataframe with 'timestamp' in
            the index and 2 columns: 'constellation', 'sweep_time_seconds'.
                timestamp: Epoch timestamp (s, ms, or ns) or UTC date.
                constellation: List of complex numbers (IQ data).
                sweep_time_seconds: Sweep time in seconds.

        data_type: str
            Must be 'value', 'trace', or 'constellation'.

        extension: str
            Add an extension to metric names.
    """

    if data_type == "value":
        dataframe = pd.DataFrame(dataframe)
        metric_name = [extension + col for col in dataframe.columns]
    elif data_type == "constellation":
        dataframe = pd.DataFrame(dataframe)
        # Check number of column
        if len(dataframe.columns) != 2:
            raise ValueError(
                f"To push constellation data, the function needs a dataframe with 2 columns: "
                f"'constellation' and 'sweep_time_seconds'. Your dataframe has "
                f"{len(dataframe.columns)} columns!"
            )
        # Rename columns
        metric_name = (extension + dataframe.columns[:1])[0]
        dataframe.columns = ["constellation", "sweep_time_seconds"]
    else:
        dataframe = pd.DataFrame(dataframe)
        # Check number of column
        if len(dataframe.columns) != 7:
            raise ValueError(
                f"To push traces, the function needs a dataframe with 7 columns: "
                f"'trace', 'start_frequency', 'stop_frequency', 'res_bw', 'video_bw', "
                f"'min_val', 'max_val'. Your dataframe has {len(dataframe.columns)} columns!"
            )
        # Rename columns
        metric_name = (extension + dataframe.columns[:1])[0]
        dataframe.columns = [
            "trace",
            "start_frequency",
            "stop_frequency",
            "res_bw",
            "video_bw",
            "min_val",
            "max_val",
        ]
    return dataframe, metric_name


def check_timestamp(dataframe):
    """
    Check and format the timestamp index of the dataframe.

    :param dataframe: DataFrame with timestamp index
    :return: DataFrame with formatted timestamp index
    """
    # Transform date to timestamp if the index is in datetime format
    if not isinstance(dataframe.index[0], Number):
        dataframe.index = pd.DatetimeIndex(dataframe.index).asi8

    # Number of digits in the timestamp to be in the correct format
    # for Skyminer is 12 or 13 (in milliseconds)
    number_of_digits = len(str(int(dataframe.index[0])))
    if number_of_digits % 3 == 1:
        if number_of_digits > 13:
            dataframe.index = dataframe.index / (10 ** (number_of_digits - 13))
        elif number_of_digits < 13:
            dataframe.index = dataframe.index * (10 ** (13 - number_of_digits))
    elif number_of_digits % 3 == 0:
        if number_of_digits > 12:
            dataframe.index = dataframe.index / (10 ** (number_of_digits - 12))
        elif number_of_digits < 12:
            dataframe.index = dataframe.index * (10 ** (12 - number_of_digits))
    return dataframe


def reformat_datapoints(dataframe, tags, metric_name):
    r"""
    Push datapoints to skyminer

    Parameters:
        dataframe: DataFrame
            If data_type is 'value', dataframe with timestamp in index and columns with values.
            /!\ Columns name will be metrics name
                timestamp: Epoch timestamp (s, ms or ns) or UTC date
                values: Values of the metrics

        tags or tags: list[dict]
            {'group1': 'label1', 'group2': 'label2'}

        metric_name: str
            The name of the metric in skyminer
    """
    data_to_push = list()
    for columns, name in zip(dataframe.columns, metric_name):
        # Skyminer can't support 'nan' we drop it
        series = dataframe[columns]
        if series.isna().sum() != 0:
            series = series.dropna()
            print(f"There was nan in {name} for tag {tags}")
        else:
            print(f"There was not nan in {name} for tag {tags}")
        datapoints = [[timestamp, value] for timestamp, value in zip(series.index, series)]
        # Reformat datapoints and tag to use add_data_points
        datapointbuilders = DataPointBuilder(name, datapoints=datapoints, tags=tags)

        col_to_push = DataPointsPayloadBuilder([datapointbuilders]).build()
        col_to_push = json.loads(col_to_push[1:-1])
        data_to_push.append(col_to_push)
    return data_to_push


def trace_builder(dataframe, tags, metric_name):
    """
    Reformat a dataframe to push trace on skyminer
    :param tags: dict
    :param metric_name: str
    :param dataframe: dataframe with 'timestamp' in index and 7 columns
    ['trace', 'start_frequency',
    'stop_frequency', 'res_bw', 'video_bw'].
    timestamp: Epoch timestamp
    trace: list of powers in db
    start_frequency: the start frequency in Hz
    stop_frequency: the stop frequency in Hz
    res_bw: resolution bandwidth in Hz
    video_bw: video bandwidth in Hz
    """
    # Skyminer can't support 'nan'
    if dataframe.isna().sum().sum() > 0:
        dataframe = dataframe.dropna()
        print(f"There was nan in {metric_name} for tag {tags}")
    else:
        print(f"There was not nan in {metric_name} for tag {tags}")
    datapoints = list()
    for index, row in dataframe.iterrows():
        ref_level = np.ceil(row["max_val"] / 10)
        scale = np.ceil((ref_level - row["min_val"]) / 10)
        datapoint = [
            index,
            {
                "trace": row["trace"],
                "min_val": row["min_val"],
                "max_val": row["max_val"],
                "start_frequency": row["start_frequency"],
                "stop_frequency": row["stop_frequency"],
                "res_bw": row["res_bw"],
                "video_bw": row["video_bw"],
                "scale": scale,
                "ref_level": ref_level,
                "type": "spectrum_trace",
            },
        ]
        datapoints.append(datapoint)
    return datapoints


def reformat_traces(dataframe, tags, metric_name):
    """
    Push trace on skyminer
    :param tags: dict
    :param metric_name: str
    :param dataframe: dataframe with in index 'timestamp' and 5 columns
    ['trace', 'start_frequency',
    'stop_frequency', 'res_bw', 'video_bw'].
    timestamp: Epoch timestamp (s, ms or ns) or UTC date
    trace: list of powers in db
    start_frequency: the start frequency in Hz
    stop_frequency: the stop frequency in Hz
    res_bw: resolution bandwidth in Hz
    video_bw: video bandwidth in Hz
    """
    datapoints = trace_builder(dataframe, tags, metric_name)
    data_to_push = {
        "datapoints": datapoints,
        "name": metric_name,
        "type": "spectrum_trace",
        "tags": tags,
    }
    return data_to_push


def constellation_builder(dataframe, tags, metric_name):
    """
    Push trace on skyminer
    :param tags: dict
    :param metric_name: str
    :param dataframe: dataframe with 'timestamp' in index and 2 columns
    ['constellation' 'sweep_time_seconds'].
    timestamp: Epoch timestamp
    sweep_time_seconds: float in seconds
    constellation: list of complex number
    sweep_time_seconds: sweep time in seconds
    """
    # Skyminer can't support 'nan'
    if dataframe.isna().sum().sum() > 0:
        dataframe = dataframe.dropna()
        print(f"There was nan in {metric_name} for tag {tags}")
    else:
        print(f"There was not nan in {metric_name} for tag {tags}")
    datapoints = list()
    for index, row in dataframe.iterrows():
        num_pts_constellation = len(row["constellation"])
        datapoint = [
            index,
            {
                "sweep_time_seconds": row["sweep_time_seconds"],
                "num_pts_constellation": num_pts_constellation,
                "constellation_y": [x.real for x in row["constellation"]],
                "constellation_x": [x.imag for x in row["constellation"]],
            },
        ]
        datapoints.append(datapoint)
    return datapoints


def reformat_constellations(dataframe, tags, metric_name):
    """
    Push trace on skyminer
    :param tags: dict
    :param metric_name: str
    :param dataframe: dataframe with 'timestamp' in index and 2 columns
    ['constellation' 'sweep_time_seconds'].
    timestamp: Epoch timestamp
    sweep_time_seconds: float in seconds
    constellation: list of complex number
    sweep_time_seconds: sweep time in seconds
    """
    datapoints = constellation_builder(dataframe, tags, metric_name)
    data_to_push = {
        "datapoints": datapoints,
        "name": metric_name,
        "type": "constellation_diagram",
        "tags": tags,
    }
    return data_to_push


def push_pipeline(
    dataframe, tags, data_type, list_data_to_push, extension, verbose, i, last_index, api, sleep
):
    """
    The function processes the input DataFrame, checks and reformats the data,
    and appends it to the list of data to be pushed. The data is then converted
    to JSON format and the memory usage
    is checked. If the memory usage exceeds a threshold or loop end is reached,
    the data is pushed to Skyminer.
    The function also includes print statements for verbose output and adds a
    sleep period
    after each data push.
    """
    dataframe, metric_name = check_dataframe(dataframe, data_type, extension)
    dataframe = check_timestamp(dataframe)
    if data_type == "value":
        data_to_push = reformat_datapoints(dataframe, tags, metric_name)
        list_data_to_push += data_to_push
    elif data_type == "trace":
        data_to_push = reformat_traces(dataframe, tags, metric_name)
        list_data_to_push.append(data_to_push)
    elif data_type == "constellation":
        data_to_push = reformat_constellations(dataframe, tags, metric_name)
        list_data_to_push.append(data_to_push)
    list_data_to_push_dumped = json.dumps(list_data_to_push)
    query_mem = int(sys.getsizeof(list_data_to_push_dumped) // 1e3)
    # Push on skyminer
    if (query_mem > 2e4) | (last_index == i):
        if verbose > 0:
            print(f"query pushed: memory {query_mem}KB")
        api.add_data_points(list_data_to_push_dumped)
        list_data_to_push.clear()
        print("Data pushed!")
        time.sleep(sleep)


class SkyminerPostProcessingUtils:
    def __init__(self, url):
        self.url = url
        self.api = STSAPI.init(url + "/api/v1", disable_ssl_certificate_validation=True)

    def delete_metric(self, metric):
        """
        Delete a metric on skyminer
        :param metric: str, the metric to delete, example: "TM1"
        """
        self.api.delete_metric(metric)

    def get_data(self, query, file_format="JSON", timestamp=False):
        """
        import data from skyminer to dataframe format one row per timestamp
        :param file_format:
        :param query: str or dict, skyminer query
        :return dataframe, with one row per timestamp and the index as a UTC date
        """
        # sting to dict
        if isinstance(query, str):
            query = json.loads(query)

        valid_formats = ["JSON", "ONE_ROW_PER_TIMESTAMP", "ONE_ROW_PER_VALUE"]
        if file_format not in valid_formats:
            raise ValueError(
                "Unknown format %s. " "Valid formats are %s" % (file_format, valid_formats)
            )
        # dict to string
        for key, value in query.items():
            if isinstance(value, np.number):
                query[key] = float(value)
        query = json.dumps(query)
        dataframe = self.api.get_data_points(query)
        if file_format != "JSON":
            dataframe = QueriesToDataframe(dataframe)
            if file_format == "ONE_ROW_PER_TIMESTAMP":
                # Modify dataframe to have one row per timestamp
                dataframe = self.convert_to_one_row_per_timestamp(dataframe)
        # dataframe = dataframe.astype('float32')
        return dataframe

    def convert_to_one_row_per_timestamp(self, df):
        def extract_titles(row):
            if "tag" in row and isinstance(row["tag"], dict):
                return list(row["tag"].keys())
            return []

        if "group_by" in df.columns:
            unique_titles = set()
            df["group_by"].apply(lambda row: unique_titles.update(extract_titles(row)))
        else:
            unique_titles = set()

        columns_to_include = sorted(list(unique_titles))

        def extract_value(value):
            """Extract the value from a list if present."""
            if isinstance(value, list) and len(value) == 1:
                return value[0]  # Return the single element of the list
            else:
                return value  # Return the value as is

        def combine_values(row):
            """Combine non-empty column-value pairs into a formatted string."""
            other_columns = []
            metric_name = row["metricname"]

            # Iterate over the columns to include and add formatted col=value
            # pairs to other_columns
            for col in columns_to_include:
                value = row[col]

                # Check if the value is not empty (or evaluates to True)
                if value:
                    formatted_value = extract_value(value)
                    other_columns.append(f"({col}: {formatted_value})")

            # If at least one non-empty col=value pair was added
            if other_columns:
                # Concatenate the result with parentheses
                combined_value = f"{metric_name} " + ", ".join(other_columns)
            else:
                # Otherwise, use only the metric name without parentheses
                combined_value = metric_name

            return combined_value

        df["pivot_col"] = df.apply(combine_values, axis=1)
        pivot_table = df.pivot(columns="pivot_col", values="value")
        pivot_table.columns.name = None
        pivot_table.index.name = "Epoch Time"
        return pivot_table

    def push_dataset(
        self,
        df,
        time_name,
        list_tag_names=None,
        list_metrics=None,
        trace=None,
        constellation=None,
        extension="",
        mapping=None,
        sleep=0.2,
        verbose=1,
    ):
        """
        Push a dataset to Skyminer. Group metrics that correspond to the same
        object in a dataframe. All metrics in a dataframe need to be time-aligned
        and have the same tags.

        :param data: list of dataframes, the data you want to push. Each dataframe
                    should have one column 'time_name' with a timestamp or date,
                    columns for each tag name with tag values, and one column per
                    metric to push. The name of the column will be the name of the
                    metric in Skyminer. For 'trace' and 'constellation,' you need
                    additional columns.
        :param time_name: str, name of the column with the time (new name after
                        mapping).
        :param list_tag_names: list[str], names of the tags (new names after mapping).
        :param list_metrics: list[str], names of metrics (new names after mapping).
        :param trace: list[str], name of the trace metric and all other needed
                    columns (minval, maxval, vid_bw, res_bw, start_freq, stop_freq)
                    (new names after mapping).
        :param constellation: list[str], name of the constellation metric and
                            sweep_time_seconds (new names after mapping).
        :param mapping: dict, with old names of your columns as keys and new ones
                        as values.
        :param extension: str, add an extension to all the metrics.
        :param sleep: time to wait after each push.
        """
        if trace is None and list_metrics is None and constellation is None:
            print("Push traces or values!")
        if list_tag_names is None:
            df["tag"] = "tag_1"
            list_tag_names = ["tag"]
        if mapping is not None:
            df = df.rename(columns=mapping)
        list_values = list()
        list_tags = list()
        list_traces = list()
        list_constellations = list()
        print(df.columns)
        df = df.set_index(time_name)
        for tags, values in df.groupby(list_tag_names):
            tags = list(tags) if isinstance(tags, tuple) else [tags]
            tags = {list_tag_names[i]: str(tags[i]) for i in range(len(list_tag_names))}
            list_tags.append(tags)
            if list_metrics is not None:
                list_values.append(values[list_metrics])
            if trace is not None:
                list_traces.append(values[trace])
            if constellation is not None:
                list_constellations.append(values[constellation])

        if len(list_constellations) == 0:
            list_constellations = None
        if len(list_traces) == 0:
            list_traces = None
        if len(list_values) == 0:
            list_values = None
        self.push_data(
            list_tags=list_tags,
            list_values=list_values,
            list_constellations=list_constellations,
            list_traces=list_traces,
            extension=extension,
            sleep=sleep,
            verbose=verbose,
        )

    def push_data(
        self,
        list_values=None,
        list_traces=None,
        list_constellations=None,
        list_tags=None,
        extension="",
        sleep=0.2,
        verbose=0,
    ):
        """
        Push data in skyminer
        :param extension:
        :param list_values: list[dataframe] with in index 'timestamp' and 1 column ['value'].
        timestamp: Epoch timestamp (s, ms or ns) or UTC date
        value: the values of the metric
        :param list_traces: list[dataframe] with in index 'timestamp' and 5 columns ['trace',
        'start_frequency', 'stop_frequency', 'res_bw', 'video_bw'].
        timestamp: Epoch timestamp (s, ms or ns) or UTC date
        trace: list of powers in db
        start_frequency: the start frequency in Hz
        stop_frequency: the stop frequency in Hz
        res_bw: resolution bandwidth in Hz
        video_bw: video bandwidth in Hz
        :param list_tags: list[dictionary], {'group1': 'label1', 'group2': 'label2'}
        :param sleep: int, waiting time between pushes. Avoid vm bugs.
        """
        if list_tags is None:
            list_tags = [{"tag": "tag_1"} for _ in range(max(len(list_values), len(list_traces)))]
        # Reformat the datapoint in list of list as str.
        if not isinstance(list_values, list) and list_values is not None:
            list_values = [list_values]
        if not isinstance(list_traces, list) and list_traces is not None:
            list_traces = [list_traces]
        if not isinstance(list_constellations, list) and list_constellations is not None:
            list_constellations = [list_constellations]
        if not isinstance(list_tags, list) and list_tags is not None:
            list_tags = [list_tags]

        # Create push query
        list_data_to_push = list()
        i = 0
        j = 0
        k = 0
        value_max = 1000000
        constellation_max = 200
        trace_max = 1000
        if list_values is not None:
            for tags, dataframe in zip(list_tags, list_values):
                row_i = 0
                while row_i < len(dataframe):
                    if row_i + value_max >= len(dataframe):
                        i += 1
                    push_pipeline(
                        dataframe.iloc[row_i : min(row_i + value_max, len(dataframe)), :],
                        tags,
                        "value",
                        list_data_to_push,
                        extension,
                        verbose,
                        i,
                        len(list_tags),
                        self.api,
                        sleep,
                    )
                    row_i += int(value_max // len(dataframe.columns))
        if list_traces is not None:
            for tags, dataframe in zip(list_tags, list_traces):
                row_j = 0
                while row_j < len(dataframe):
                    if row_j + trace_max >= len(dataframe):
                        j += 1
                    push_pipeline(
                        dataframe.iloc[row_j : min(row_j + trace_max, len(dataframe)), :],
                        tags,
                        "trace",
                        list_data_to_push,
                        extension,
                        verbose,
                        j,
                        len(list_tags),
                        self.api,
                        sleep,
                    )
                    row_j += trace_max
        if list_constellations is not None:
            for tags, dataframe in zip(list_tags, list_constellations):
                row_k = 0
                while row_k < len(dataframe):
                    if row_k + constellation_max >= len(dataframe):
                        k += 1
                    push_pipeline(
                        dataframe.iloc[row_k : min(row_k + constellation_max, len(dataframe)), :],
                        tags,
                        "constellation",
                        list_data_to_push,
                        extension,
                        verbose,
                        k,
                        len(list_tags),
                        self.api,
                        sleep,
                    )
                    row_k += constellation_max

    def get_tags(self, query, group):
        """
        Get tags of a group in a query
        :param query: dict, skyminer query
        :param group: str, the name of the group, example: "satellite"
        """
        # Import the 2 first point
        for i in range(len(query["metrics"])):
            query["metrics"][i]["order"] = "asc"
            query["metrics"][i]["limit"] = 2
        skyminer_result = self.get_data(query)
        if skyminer_result["queries"][0]["sample_size"] == 0:
            return None, None

        list_tag = list()
        for result in skyminer_result["queries"][0]["results"]:
            if result["tags"] != dict():
                # Get the tag for each time series
                tag = result["tags"][group][0]
                list_tag.append(tag)

        # Save all tags for the pushing phase
        push_tag = skyminer_result["queries"][0]["results"][0]["tags"]
        # Remove tag with multi value (filter by this tag in parameters if you want to keep it)
        for key in list(push_tag.keys()):
            if len(push_tag[key]) > 1:
                del push_tag[key]
        # Transform list into single values
        for key in push_tag:
            push_tag[key] = push_tag[key][0]
        return list_tag, push_tag
