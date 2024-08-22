import pandas as pd
from dagster import Field, In, Out, String, op

from . import process
from .basic_ops import BasicOps
from .logger import logging_decorator


class AggregationOps(BasicOps):
    @staticmethod
    def create_process_data_op(brick_name: str, op_name: str):
        @op(
            name=op_name,
            ins={"dataframe": In(pd.DataFrame, description="Skyminer data to be processed")},
            out={
                "processed_dataframe": Out(pd.DataFrame, description=f"{brick_name} Skyminer data")
            },
            description="Processes Skyminer data using basic statistical methods (mean, max, min, kurt...).",
            tags={"operation": "data_processing", "method": f"{brick_name}"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(list, description="Metric name used in the query"),
            },
        )
        @logging_decorator
        def process_data(context, dataframe: pd.DataFrame) -> pd.DataFrame:
            processed_dataframe = process.calculate_statistics(
                dataframe, context.op_config["metric"][0]
            )
            processed_dataframe = processed_dataframe[:-1]
            return processed_dataframe

        return process_data


class LttbOps(BasicOps):
    @staticmethod
    def create_process_data_op(brick_name: str, op_name: str):
        @op(
            name=op_name,
            ins={"dataframe": In(pd.DataFrame, description="Skyminer data to be processed")},
            out={
                "processed_dataframe": Out(pd.DataFrame, description=f"{brick_name} Skyminer data")
            },
            description=f"Processes Skyminer data using a {brick_name} method.",
            tags={"operation": "data_processing", "method": f"{brick_name}"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(String, description="Metric name used in the query"),
            },
        )
        @logging_decorator
        def process_data(context, dataframe: pd.DataFrame) -> pd.DataFrame:
            processed_dataframe = process.resample(dataframe, context.op_config["metric"])
            return processed_dataframe

        return process_data


class OneClassSvmOps(BasicOps):
    @staticmethod
    def create_build_query_op(brick_name: str, op_name: str):
        @op(
            name=op_name,
            required_resource_keys={"checkpoint"},
            out={
                "query": Out(
                    str,
                    description=f"Query string to retrieve {brick_name} data from Skyminer API",
                )
            },
            description=f"Builds a query string to retrieve {brick_name} data from Skyminer API using checkpoint.",
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
            checkpoint = context.resources.checkpoint.get(brick_name, metric, tag=tag)
            query = process.generate_query(metric, checkpoint, tag=tag)
            context.log.info(query)
            context.log.info(f"Query formatted: {query}")
            return query

        return build_query

    @staticmethod
    def create_process_data_op(brick_name: str, op_name: str):
        @op(
            name=op_name,
            ins={"dataframe": In(pd.DataFrame, description="Skyminer data to be analyzed")},
            out={
                "processed_dataframe": Out(
                    pd.DataFrame, description=f"Anomaly detection results with {brick_name} method"
                )
            },
            description=f"Detect outlier using a {brick_name} method.",
            tags={"operation": "data_processing", "outlier_detection": f"{brick_name}"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(String, description="Metric name used in the query"),
            },
        )
        @logging_decorator
        def process_data(context, dataframe: pd.DataFrame) -> pd.DataFrame:
            processed_dataframe = process.process_one_class_svm(
                dataframe, context.op_config["metric"]
            )
            return processed_dataframe

        return process_data
