import time
from typing import Tuple

import pandas as pd
from dagster import Array, Field, In, Out, String, op

from . import process
from .basic_ops import BasicOps
from .logger import logging_decorator
from .mltraining_process import PipelineMLForecast


class MLpredict(BasicOps):
    @staticmethod
    def create_get_model_op(brick_name: str, op_name: str):
        @op(
            name=op_name,
            required_resource_keys={"model_manager"},
            out={"model": Out(object, description="Get a model for the metric given")},
            description=f"Get the model for the mtric given for {brick_name}",
            tags={"operation": "search", "target": "models"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(String, description="Metric name used in the query"),
                "metrics_multivariate": Field(
                    Array(String), description="Metric multivariate used for the model"
                ),
                "tag": Field(dict, description="Dictionary of the tags used in the query"),
            },
        )
        @logging_decorator
        def get_existing_model(context) -> object:
            metric = context.op_config["metric"]
            metrics_multivariate = context.op_config["metrics_multivariate"]
            tag = context.op_config["tag"]
            file_path = context.resources.model_manager.get_model_file_path([metric], metrics_multivariate, tag=tag)
            context.log.info(f"Loading model from {file_path}")
            model = context.resources.model_manager.get_model(
                file_path, metric, metrics_multivariate, tag=tag
            )
            context.log.info(f"{type(model)}")
            return model

        return get_existing_model

    @staticmethod
    def create_build_query_op_main_metric(brick_name: str, op_name: str):
        @op(
            name=op_name,
            required_resource_keys={"checkpoint"},
            ins={"model": In(object, description="ML pipeline instance")},
            out={"query": Out(str, description=f"Query string to retrieve metric data to {brick_name} from Skyminer API"),
                 "checkpoint": Out(int, description=f"Checkpoint"),
                 "start_absolute": Out(int, description=f"Start absolute")},
            description=f"Builds a query string to retrieve metric data to {brick_name}"
                        f" from Skyminer API using checkpoint.",
            tags={"operation": "query_building"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(String, description="Metric name used in the query"),
                "metrics_multivariate": Field(
                    Array(String), description="Metric multivariate used for the model"
                ),
                "tag": Field(dict, description="Dictionary of the tags used in the query"),
            },
        )
        @logging_decorator
        def build_query(context, model : object) -> Tuple[str, int, int]:
            metric = context.op_config["metric"]
            metrics_multivariate = context.op_config["metrics_multivariate"]
            tag = context.op_config["tag"]
            checkpoint = context.resources.checkpoint.get(
                brick_name, [metric], metrics_multivariate, tag=tag
            )
            now = int(time.time() * 1000)
            reference_ts = min(checkpoint, now)
            start_absolute = reference_ts - model.get_length_needed() * 1000 - 1000 * 60 * 3
            end_absolute = reference_ts
            context.log.info("Query length : " + str((end_absolute - start_absolute) / 1000) + "s")
            context.log.info(
                "Length needed by the model : " + str(model.get_length_needed()) + "s"
            )
            if checkpoint - now > 1000 * 3600:
                return "", checkpoint, 0
            query = process.generate_query(
                [metric], start_absolute, end_absolute=end_absolute, tag=tag, resample=10
            )
            context.log.info(query)
            return query, checkpoint, start_absolute

        return build_query

    @staticmethod
    def create_build_query_op_multi(brick_name: str, op_name: str):
        @op(
            name=op_name,
            required_resource_keys={"checkpoint"},
            ins={"model": In(object, description="ML pipeline instance"),
                 "start_absolute" : In(int, description="Start absolute")},
            out={"query": Out(str, description=f"Query string to retrieve metrics "
                                               f"multivariated data used for the {brick_name} from Skyminer API")},
            description=f"Builds a query string to retrieve multivariate data used from "
                        f"Skyminer API using checkpoint for the {brick_name}.",
            tags={"operation": "query_building"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(String, description="Metric name used in the query"),
                "metrics_multivariate": Field(
                    Array(String), description="Metric multivariate used for the model"
                ),
                "tag": Field(dict, description="Dictionary of the tags used in the query"),
            },
        )
        @logging_decorator
        def build_query(context, model : object, start_absolute: int) -> str:
            if start_absolute == 0:
                return ""
            metric = context.op_config["metric"]
            metrics_multivariate = context.op_config["metrics_multivariate"]
            tag = context.op_config["tag"]
            if metrics_multivariate == []:
                return ""
            checkpoint = context.resources.checkpoint.get(
                brick_name, [metric], metrics_multivariate, tag
            )
            prediction_length = model.get_length_prediction() * 1000
            end_absolute = checkpoint + prediction_length
            context.log.info(f"Dataframe : {metrics_multivariate}")
            query = process.generate_query(
                metrics_multivariate, start_absolute, end_absolute, tag=tag, resample=10
            )
            context.log.info(query)
            return query

        return build_query

    @staticmethod
    def create_forecast_model_op(brick_name: str, op_name: str):
        @op(
            name=op_name,
            ins={"dataframe": In(pd.DataFrame, description="training dataframe"),
                 'dataframe_multi': In(pd.DataFrame, description="Future mutivariate dataframe"),
                 "model": In(object, description="ML pipeline instance"),
                 "checkpoint": In(int, description="checkpoint")},
            out={"forecasts": Out(pd.DataFrame, description="forecasts dataframe")},
            description=f"Building a model fro {brick_name}",
            tags={"operation": "training"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(String, description="Metric name used in the query"),
                "tag": Field(dict, description="Tags for the query"),
            },
        )
        @logging_decorator
        def forecast_model(context, dataframe: pd.DataFrame, dataframe_multi: pd.DataFrame,
                           model: object, checkpoint: int) -> pd.DataFrame:
            metric = context.op_config["metric"]
            columns = [
                "unique_id",
                "date",
                metric + ".prediction_ML_dagster",
                metric + ".prediction_ML_LB_dagster",
                metric + ".prediction_ML_UB_dagster",
            ]
            if dataframe.empty:
                return pd.DataFrame(columns=columns)
            dataframe["ds"] = pd.to_datetime(dataframe.index, unit="ms")
            dataframe_multi["ds"] = pd.to_datetime(dataframe_multi.index, unit="ms")
            forecasts = model.predict(dataframe, x_df=dataframe_multi)
            forecasts.columns = columns
            forecasts.drop("unique_id", axis=1, inplace=True)
            ref_date = pd.to_datetime(checkpoint, unit="ms")
            new_forecasts = forecasts[forecasts["date"] > ref_date]
            new_forecasts.index = new_forecasts["date"]
            new_forecasts.drop("date", axis=1, inplace=True)
            context.log.info(f"Predictions : {new_forecasts}")
            return new_forecasts

        return forecast_model
