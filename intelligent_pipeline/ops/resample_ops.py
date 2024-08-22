import pandas as pd
from dagster import Field, In, Out, String, op

from . import process
from .basic_ops import BasicOps
from .logger import logging_decorator


class ResampleOps(BasicOps):
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
            description=f"Builds a query string to retrieve {brick_name} data from Skyminer"
            f"API using checkpoint.",
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
            query = process.generate_query([metric[0]], checkpoint, tag=tag, resample=10)
            context.log.info(query)
            context.log.info(f"Query formatted: {query}")
            return query

        return build_query

    @staticmethod
    def create_build_query_lb_ub_op(brick_name: str, op_name: str):
        @op(
            name=op_name,
            required_resource_keys={"checkpoint"},
            out={
                "query": Out(
                    str,
                    description=f"Query string to retrieve {brick_name} data from Skyminer API",
                )
            },
            description=f"Builds a query string to retrieve {brick_name} data from Skyminer"
            f"API using checkpoint.",
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
            query = process.generate_query(
                [metric[0] + ".prediction_ML_UB_dagster", metric[0] + ".prediction_ML_LB_dagster"],
                checkpoint,
                tag=tag,
                resample=10,
            )
            context.log.info(query)
            context.log.info(f"Query formatted: {query}")
            return query

        return build_query

    @staticmethod
    def create_process_data_op(brick_name: str, op_name: str):
        @op(
            name=op_name,
            ins={
                "dataframe_resample": In(pd.DataFrame, description="Skyminer data to be analyzed"),
                "dataframe_lb_ub": In(
                    pd.DataFrame, description="Upper bound and lower bound of the data"
                ),
            },
            out={
                "processed_dataframe": Out(
                    pd.DataFrame, description=f"Anomaly detection results with {brick_name} method"
                )
            },
            description=f"Detect outlier using a {brick_name} method.",
            tags={"operation": "data_processing", "outlier_detection": f"{brick_name}"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(list, description="Metric name used in the query"),
            },
        )
        @logging_decorator
        def process_data(
            context, dataframe_resample: pd.DataFrame, dataframe_lb_ub
        ) -> pd.DataFrame:
            metric = context.op_config["metric"]
            ub = dataframe_lb_ub.columns[1]
            lb = dataframe_lb_ub.columns[0]
            common_index = dataframe_resample.index.intersection(dataframe_lb_ub.index)
            dataframe_resample = dataframe_resample.loc[common_index]
            dataframe_lb_ub = dataframe_lb_ub.loc[common_index]
            context.log.info(dataframe_lb_ub[ub])
            context.log.info(dataframe_resample)
            dataframe_resample[metric[0] + ".prediction_anomaly_score_dagster"] = (
                dataframe_resample.apply(
                    lambda row: max(
                        dataframe_lb_ub.loc[row.name, lb] - row[metric[0]],
                        row[metric[0]] - dataframe_lb_ub.loc[row.name, ub],
                    ),
                    axis=1,
                )
            )
            dataframe_resample = dataframe_resample.drop(metric[0], axis=1)
            context.log.info(dataframe_resample)
            return dataframe_resample

        return process_data
