import pandas as pd

from dagster import Field, In, Nothing, Out, String, op
from . import process
from .logger import logging_decorator


class BasicOps:
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
                "tag": Field(dict, description="Tags for the query"),
            },
        )
        @logging_decorator
        def build_query(context) -> str:
            metric = context.op_config["metric"]
            tag = context.op_config["tag"]
            checkpoint = context.resources.checkpoint.get(brick_name, metric)
            query = process.generate_query(metric, checkpoint, tag=tag)
            context.log.info(f"Query: {query}")
            return query

        return build_query

    @staticmethod
    def create_get_data_op(brick_name: str, op_name: str):
        @op(
            name=op_name,
            required_resource_keys={"skyminer_api"},
            ins={
                "query": In(
                    str,
                    description=f"Query string to retrieve {brick_name} data from Skyminer API",
                )
            },
            out={
                "dataframe": Out(pd.DataFrame, description=f"Retrieved Skyminer {brick_name} data")
            },
            description=f"Retrieves {brick_name} data from checkpoint or the last two days if no checkpoint.",
            tags={"operation": "data_retrieval", "source": "skyminer"},
            config_schema={"date": Field(String, description="Date for the execution of the op")},
        )
        @logging_decorator
        def get_data(context, query: str) -> pd.DataFrame:
            if query == "":
                context.log.info("Query is empty, returning empty dataframe")
                return pd.DataFrame()
            dataframe = context.resources.skyminer_api.get_data(query, "ONE_ROW_PER_TIMESTAMP")
            return dataframe

        return get_data

    @staticmethod
    def create_push_data_op(brick_name: str, op_name: str):
        @op(
            name=op_name,
            required_resource_keys={"checkpoint", "skyminer_api"},
            ins={
                "processed_dataframe": In(
                    pd.DataFrame, description=f"{brick_name} Skyminer data to be pushed"
                )
            },
            out={"result": Out(Nothing, description="Indicates completion of pushing data")},
            description=f"Pushes {brick_name} Skyminer data to the Skyminer API.",
            tags={"operation": "data_pushing", "target": "skyminer"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(list, description="Metric name used in the query"),
                "tag": Field(dict, description="Tags for the query"),
                "extension": Field(str, description="Extension for the name pushed", default_value="")
            },
        )
        @logging_decorator
        def push_data(context, processed_dataframe: pd.DataFrame) -> None:
            metric = context.op_config["metric"]
            tag = context.op_config["tag"]
            extension = context.op_config["extension"]
            processed_dataframe["date"] = processed_dataframe.index
            if extension != "":
                for metric_name in metric:
                    processed_dataframe = processed_dataframe.rename(columns={metric_name: metric_name+extension})
            tags_name = list(tag.keys())
            for tag_name in tags_name:
                processed_dataframe[tag_name] = tag[tag_name]
            context.log.info(f"Processed dataframe before push : {processed_dataframe}")
            if not processed_dataframe.empty:
                list_metrics = process.get_list_metrics_aggregated(processed_dataframe)
                context.log.info(f"Metrics to be pushed : ${list_metrics}")
                context.log.info(processed_dataframe.head())
                context.resources.skyminer_api.push_dataset(
                    processed_dataframe,
                    time_name="date",
                    list_metrics=list_metrics,
                    list_tag_names=list(tag.keys()),
                )
                context.log.info(processed_dataframe["date"].astype("int64") // 10**6)
                checkpoint = (processed_dataframe["date"].astype("int64") // 10**6).iloc[-1]
                message = context.resources.checkpoint.set(checkpoint, brick_name, metric, tag=tag)
                context.log.info(message)
            return None

        return push_data
