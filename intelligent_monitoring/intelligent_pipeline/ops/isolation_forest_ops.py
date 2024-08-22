import pandas as pd
from dagster import Field, In, Out, String, op

from . import process
from .basic_ops import BasicOps
from .logger import logging_decorator


class IsolationForest(BasicOps):
    @staticmethod
    def create_build_query_op(brick_name: str, op_name: str, time_delta: str):
        @op(
            name=op_name,
            required_resource_keys={"checkpoint"},
            out={
                "query": Out(
                    str, description="Query string to retrieve aggregation data from Skyminer API"
                )
            },
            description="Builds a query string to retrieve aggregation data from Skyminer API using checkpoint.",
            tags={"operation": "query_building"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(list, description="Metric name used in the query"),
                "tag": Field(dict, description="Dictionary of the tags used in the query"),
            },
        )
        @logging_decorator
        def build_query(context) -> str:
            metric = context.op_config["metric"]
            tag = context.op_config["tag"]
            checkpoint = context.resources.checkpoint.get(brick_name, metric=metric, tag=tag)
            td = pd.Timedelta(time_delta)
            checkpoint -= int(td.total_seconds() * 1000)
            query = process.generate_query(metrics=metric, start_absolute=checkpoint, tag=tag)
            context.log.info(query)
            return query

        return build_query

    @staticmethod
    def create_process_data_op(brick_name: str, op_name: str, window_size: str, min_periods: int):
        @op(
            name=op_name,
            required_resource_keys={"checkpoint"},
            ins={
                "dataframe": In(
                    pd.DataFrame, description="Skyminer aggregated data to be analyzed"
                )
            },
            out={
                "processed_dataframe": Out(
                    pd.DataFrame, description=f"Anomaly detection results with {brick_name} method"
                )
            },
            description=f"Detect anomaly using {brick_name} method in processes Skyminer aggregated data.",
            tags={"operation": "data_processing", "outlier_detection": f"{brick_name}"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(list, description="Metric name used in the query"),
                "tag" : Field(dict, description="Tag used in the query")
            },
        )
        @logging_decorator
        def process_data(context, dataframe: pd.DataFrame) -> pd.DataFrame:
            context.log.info(f"Initial dataframe : {dataframe}")
            metric = context.op_config["metric"]
            tag = context.op_config["tag"]
            processed_dataframe = process.isolation_forest(dataframe, window_size, min_periods)
            context.log.info(f"Processed dataframe before checkpoint : {processed_dataframe.head()}")
            checkpoint = context.resources.checkpoint.get(brick_name, metric=metric, tag=tag)
            context.log.info(f"Checkpoint : {checkpoint}")
            ref_date = pd.to_datetime(checkpoint, unit="ms")
            context.log.info(f"Ref date : {ref_date}")
            processed_dataframe = processed_dataframe[processed_dataframe.index > ref_date]
            context.log.info(f"Processed dataframe after checkpoint : {processed_dataframe.head()}")
            return processed_dataframe

        return process_data

class RollingAverage(BasicOps) :
    @staticmethod
    def create_process_data_op(brick_name: str, op_name: str, window, min_periods):
        @op(
            name=op_name,
            required_resource_keys={"checkpoint"},
            ins={
                "dataframe": In(
                    pd.DataFrame, description="Skyminer aggregated data to be analyzed"
                )
            },
            out={
                "processed_dataframe": Out(
                    pd.DataFrame, description=f"Anomaly detection results with {brick_name} method"
                )
            },
            description=f"Detect anomaly using {brick_name} method in processes Skyminer aggregated data.",
            tags={"operation": "data_processing", "outlier_detection": f"{brick_name}"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(list, description="Metric name used in the query"),
                "tag" : Field(dict, description="Tag used in the query")
            },
        )
        @logging_decorator
        def process_data(context, dataframe: pd.DataFrame) -> pd.DataFrame:
            context.log.info(f"Initial dataframe : {dataframe}")
            metric = context.op_config["metric"]
            tag = context.op_config["tag"]
            processed_dataframe = process.ma(dataframe, window, min_periods)
            checkpoint = context.resources.checkpoint.get(brick_name, metric=metric, tag=tag)
            ref_date = pd.to_datetime(checkpoint, unit="ms")
            processed_dataframe = processed_dataframe[processed_dataframe.index > ref_date]
            return processed_dataframe

        return process_data
