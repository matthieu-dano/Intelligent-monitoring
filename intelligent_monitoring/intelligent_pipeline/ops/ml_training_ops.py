from dagster import (
    Array,
    Failure,
    Field,
    In,
    Nothing,
    Out,
    String,
    op,
)

from . import process
from .basic_ops import BasicOps
from .logger import logging_decorator
import pandas as pd
from sklearn.linear_model import LinearRegression
import time
from .mltraining_process import PipelineMLForecast


class MLtraining(BasicOps):

    @staticmethod
    def create_check_op(brick_name: str, op_name: str):
        @op(
            required_resource_keys={"model"},
            name=op_name,
            ins={
                "job_in_progress": In(
                    bool, description="Indicate if a same job is already running"
                )
            },
            out={"model_file_path": Out(str, description="Path to the model if he exist")},
            description=f"Check if a model already exist for the metric and tags given and skip the {brick_name}",
            tags={"operation": "search", "target": "models"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(list, description="Metric name used in the query"),
                "metrics_multivariate": Field(Array(String), description="Metric multivariate used for the model"),
                "tag" : Field(dict, description="Dictionary of the tags used in the query")
            },
        )
        @logging_decorator
        def check_existing_model(context, job_in_progress=False) -> str:
            metric = context.op_config["metric"]
            metrics_multivariate = context.op_config["metrics_multivariate"]
            tag = context.op_config["tag"]
            context.log.info(f"Checking : {job_in_progress}")
            try:
                context.log.info(metric)
                if job_in_progress:
                    raise Failure(
                        f"Training is already in course for this metric {metric}, "
                        f"stopping the job execution"
                    )
                context.log.info(f"No training is in course for the metric {metric}")
                return context.resources.model.get_model_file_path(
                    metric, metrics_multivariate, tag=tag
                )
            except Exception as e:
                context.log.error(f"A training is already in course for the metric {metric}: {e}")
                raise Failure(f"An error occurred in check_existing_model: {e}")

        return check_existing_model

    @staticmethod
    def create_build_query_first_date_op(brick_name: str, op_name: str):
        @op(
            name=op_name,
            required_resource_keys={"checkpoint"},
            ins={"model_file_path": In(str, description="Path to the model if he exist")},
            out={
                "query": Out(
                    str,
                    description=f"Query string to retrieve {brick_name} data from Skyminer API",
                )
            },
            description=f"Builds a query string to retrieve {brick_name} data from Skyminer"
                        f"API using checkpoint, if no model exist",
            tags={"operation": "query_building"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(list, description="Metric name used in the query"),
                "metrics_multivariate": Field(Array(String), description="Metric multivariate used for the model"),
                "tag" : Field(dict, description="Dictionary of the tags used in the query")
            }
        )
        @logging_decorator
        def build_query(context, model_file_path=""):
            if model_file_path != "":
                context.log.info(
                    f"Model already exist for the metric {context.op_config['metric']}"
                )
                return ""
            metric = context.op_config["metric"]
            metrics_multivariate = context.op_config["metrics_multivariate"]
            tag = context.op_config["tag"]
            start_time = 1
            query = process.generate_query(metric+metrics_multivariate, start_time, tag=tag, limit=1)
            context.log.info(query)
            return query

        return build_query

    @staticmethod
    def create_build_query_op(brick_name: str, op_name: str):
        @op(
            name=op_name,
            required_resource_keys={"checkpoint"},
            ins={"dataframe" : In(pd.DataFrame, description = "Dataframe with only the first data"),
            "model_file_path": In(str, description="Path to the model if he exist")},
            out={
                "queries": Out(
                    list,
                    description=f"Query string to retrieve {brick_name} data from Skyminer API",
                )
            },
            description=f"Builds a query string to retrieve {brick_name} data from Skyminer"
            f"API using checkpoint, if no model exist",
            tags={"operation": "query_building"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(list, description="Metric name used in the query"),
                "metrics_multivariate": Field(Array(String), description="Metric multivariate used for the model"),
                "tag" : Field(dict, description="Dictionary of the tags used in the query")
            }
        )
        @logging_decorator
        def build_query(context, dataframe=pd.DataFrame(), model_file_path="") -> list:
            if model_file_path != "":
                context.log.info(
                    f"Model already exist for the metric {context.op_config['metric']}"
                )
                return [""]
            if dataframe.empty :
                first_date = (time.time()-3600-24*30*4)
            else :
                first_date = dataframe.index[0].timestamp()*1000
            metric = context.op_config["metric"]
            metrics_multivariate = context.op_config["metrics_multivariate"]
            metrics = metric+metrics_multivariate
            tag = context.op_config["tag"]
            #We take the first date or one year before
            now = time.time()*1000
            queries = []
            window = 3600*24*30*1000
            for metr in metrics:
                start_time = int(max(first_date, int(time.time()-3600*24*30*12)*1000))
                end_time = start_time+window
                while end_time <= now:
                    query = process.generate_query([metr], start_time,
                                                   end_absolute=end_time-1, tag=tag, resample=10)
                    queries.append(query)
                    start_time = end_time
                    end_time = start_time+window
                query = process.generate_query([metr], start_time,
                                               end_absolute=now, tag=tag, resample=10)
                queries.append(query)

            context.log.info(f"Length of queries: {len(queries)}")
            context.log.info(queries)
            return queries

        return build_query

    @staticmethod
    def create_get_data_op(brick_name: str, op_name: str):
        @op(
            name=op_name,
            required_resource_keys={"skyminer_api"},
            ins={
                "queries": In(
                    list,
                    description=f"Query string to retrieve {brick_name} data from Skyminer API",
                ),
                "model_file_path": In(str, description="Path to the model if he exist"),
            },
            out={
                "dataframe": Out(
                    pd.DataFrame,
                    description=f"Retrieved Skyminer {brick_name} data necessary for the model",
                )
            },
            description=f"Retrieves {brick_name} data from checkpoint or the last two days"
            f"if no checkpoint and if no model exist",
            tags={"operation": "data_retrieval", "source": "skyminer"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(list, description="Metric name used in the query"),
                "metrics_multivariate": Field(Array(String), description="Metric multivariate used for the model")
            }
        )
        @logging_decorator
        def get_data(context, queries, model_file_path="") -> pd.DataFrame:
            if model_file_path != "":
                context.log.info(
                    f"Model already exist for the metric {context.op_config['metric']}"
                    f"and multivariate {context.op_config['metrics_multivariate']}"
                )
                return pd.DataFrame()
            dataframe = pd.DataFrame()
            for index, query in enumerate(queries):
                context.log.info(f"Processing query {index} on {len(queries)}")
                data = context.resources.skyminer_api.get_data(query, "ONE_ROW_PER_TIMESTAMP")
                if dataframe.empty:
                    dataframe = data
                else:
                    for column in data.columns:
                        if column in dataframe.columns:
                            dataframe[column].update(data[column])
                        else :
                            dataframe[column] = data[column]
                    dataframe = dataframe.combine_first(data)
            dataframe = dataframe.resample("10S").first().bfill().ffill()
            context.log.info(dataframe)
            return dataframe
        return get_data

    @staticmethod
    def create_model_op(brick_name: str, op_name: str):
        @op(
            required_resource_keys={"model"},
            name=op_name,
            ins={
                "dataframe": In(pd.DataFrame, description=f"Dataframe for {brick_name}"),
                "model_file_path": In(
                    str, description=f"Path to the model if he exist for {brick_name}"
                ),
            },
            out={
                "model": Out(
                    PipelineMLForecast, description="Create a model for the metric and tag given"
                )
            },
            description=f"Building a model for {brick_name} if no model exist",
            tags={"operation": "training"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(list, description="Metric name used in the query"),
                "metrics_multivariate": Field(Array(String), description="Metric multivariate used for the model"),
                "tag" : Field(dict, description="Dictionary of the tags used in the query")
            },
        )
        @logging_decorator
        def create_model(context, dataframe: pd.DataFrame, model_file_path="") -> PipelineMLForecast:
            if model_file_path != "":
                metric = context.op_config["metric"]
                metrics_multivariate = context.op_config["metrics_multivariate"]
                tag = context.op_config["tag"]
                context.log.info(
                    f"Model already exist for the metric {metric}"
                    f"and multivariate {metrics_multivariate}"
                )
                return context.resources.model.get_model(
                    model_file_path, metric, metrics_multivariate, tag=tag
                )
            dataframe["ds"] = pd.to_datetime(dataframe.index, unit="ms")
            dataframe = dataframe.reset_index(drop=True)
            context.log.info(f"Target : {dataframe.columns[0]}")
            model = PipelineMLForecast(dataframe, target=dataframe.columns[0], freq=10)
            model.fit()
            return model

        return create_model

    @staticmethod
    def create_save_op(brick_name: str, op_name: str):
        @op(
            required_resource_keys={"model"},
            name=op_name,
            ins={
                "pipeline_ml": In(
                    PipelineMLForecast,
                    description=f"ML pipeline instance for the model {brick_name}",
                ),
                "model_file_path": In(
                    str, description=f"Path to the model if he exist for the metric {brick_name}"
                ),
            },
            out={
                "result": Out(
                    Nothing,
                    description=f"Indicates if a model has been saved for the metric {brick_name}",
                )
            },
            description=f"Save model in a .pkl format for {brick_name} if no model exist",
            tags={"operation": "save", "target": "models"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(list, description="Metric name used in the query"),
                "metrics_multivariate": Field(Array(String), description="Metric multivariate used for the model"),
                "tag" : Field(dict, description="Dictionary of the tags used in the query")
            },
        )
        @logging_decorator
        def save_model(context, pipeline_ml: PipelineMLForecast, model_file_path="") -> None:
            metric = context.op_config["metric"]
            metrics_multivariate = context.op_config["metrics_multivariate"]
            tag = context.op_config["tag"]
            if model_file_path != "":
                context.log.info(
                    f"Model already exist for the metric {context.op_config['metric']}"
                )
                return None
            date = context.op_config["date"]
            folder, name = context.resources.model.save_model(
                pipeline_ml, date, metric, metrics_multivariate, tag=tag
            )
            context.log.info(f"folder {folder}, name {name}")
            return None

        return save_model


    @staticmethod
    def create_linear_op(brick_name: str, op_name: str):
        @op(
            name=op_name,
            ins={"dataframe": In(pd.DataFrame, description=f"Dataframe for {brick_name}")},
            out={"forecast": Out(pd.DataFrame, description=f"Dataframe with the predictions")},
            description=f"Building a model for {brick_name} if no model exist",
            tags={"operation": "training"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(list, description="Metric name used in the query")
            },
        )
        @logging_decorator
        def create_model(context, dataframe: pd.DataFrame) -> pd.DataFrame:
            metric = context.op_config["metric"]
            dataframe['ds'] = pd.to_datetime(dataframe.index, unit='ms')
            dataframe = dataframe.reset_index(drop=True)
            context.log.info(f"Target : {dataframe.columns[0]}")
            model = PipelineMLForecast(dataframe, target=metric[0], freq=10, custom_model=LinearRegression())
            model.fit()
            forecasts = model.predict()
            columns = ['unique_id', 'date', metric[0] + '.prediction_ML_linear_dagster', metric[0] + '.prediction_ML_linear_LB_dagster',
                       metric[0] + '.prediction_ML_linear_UB_dagster']
            forecasts.columns = columns
            forecasts.index = forecasts['date']
            forecasts.drop(['unique_id', 'date'], axis=1, inplace=True)
            context.log.info(f"Forecast : {forecasts}")
            return forecasts

        return create_model
