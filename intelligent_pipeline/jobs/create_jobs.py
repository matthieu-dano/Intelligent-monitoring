from dagster import job
from dagstermill import ConfigurableLocalOutputNotebookIOManager
from ..resources import (
    get_checkpoint_resource,
    get_model_resource,
    get_skyminer_api_resource
)

class CreateJob:
    @staticmethod
    def create_job(name, ops, timeout):
        @job(
            name=name,
            description=f"Job for {name}",
            tags={
                "category": "intelligent_pipeline_v1",
                "subcategory": name,
                "owner": "datascience_team",
                "job": "my_unique_job",
                "dagster/max_runtime": timeout,
            },
            resource_defs={
                "skyminer_api": get_skyminer_api_resource,
                "checkpoint": get_checkpoint_resource,
            },
        )
        def created_job():
            build_query_op = ops["build_query"]
            get_data_op = ops["get_data"]
            process_data_op = ops["process"]
            push_data_op = ops["push"]

            query = build_query_op()
            dataframe = get_data_op(query)
            processed_dataframe = process_data_op(dataframe)
            push_data_op(processed_dataframe)

        return created_job

    @staticmethod
    def create_job_training(name, ops):
        @job(
            name=name,
            description=f"Job for {name}",
            tags={
                "category": "intelligent_pipeline_v1",
                "subcategory": name,
                "owner": "datascience_team",
                "training": "my_unique_training",
            },
            resource_defs={
                "skyminer_api": get_skyminer_api_resource,
                "checkpoint": get_checkpoint_resource,
                "model": get_model_resource,
            },
        )
        def created_job():
            check_op = ops['check']
            build_query_first_date_op = ops['build_query_first_date']
            get_data_first_date_op = ops['get_data_first_date']
            build_query_op = ops['build_query']
            get_data_op = ops['get_data']
            process_ml_op = ops['process_ml']
            save_model_op = ops['save_model']

            path_to_model = check_op()
            query_first_date = build_query_first_date_op(path_to_model)
            dataframe_first_date = get_data_first_date_op([query_first_date], path_to_model)
            queries = build_query_op(dataframe_first_date, path_to_model)
            dataframe = get_data_op(queries, path_to_model)
            model = process_ml_op(dataframe, path_to_model)
            save_model_op(model, path_to_model)

        return created_job

    @staticmethod
    def create_job_prediction(name, ops, timeout):
        @job(
            name=name,
            description=f"Job for {name}",
            tags={
                "category": "intelligent_pipeline_v1",
                "subcategory": name,
                "owner": "datascience_team",
                "predict": "my_unique_predict",
                "dagster/max_runtime": timeout,
            },
            resource_defs={
                "skyminer_api": get_skyminer_api_resource,
                "checkpoint": get_checkpoint_resource,
                "model_manager": get_model_resource,
            },
        )
        def created_job():
            get_op = ops["get_model"]
            build_query_op = ops["build_query"]
            get_data_op = ops["get_data"]
            build_query_multi_op = ops["build_query_multi"]
            get_data_multi_op = ops["get_data_multi"]
            predict_op = ops["predict"]
            push_op = ops["push"]

            model = get_op()
            query, checkpoint, start_absolute = build_query_op(model)
            dataframe = get_data_op(query)
            query_multi = build_query_multi_op(model, start_absolute)
            dataframe_multi = get_data_multi_op(query_multi)
            forecasts = predict_op(dataframe, dataframe_multi, model, checkpoint)
            push_op(forecasts)

        return created_job

    @staticmethod
    def create_notebook_run_job(name, ops, timeout):
        @job(
            name=name,
            description=f"Job for {name}",
            tags={
                "category": "intelligent_pipeline_v1",
                "subcategory": name,
                "owner": "datascience_team",
                "notebook": "my_unique_notebook",
                "dagster/max_runtime": timeout,
            },
            resource_defs={
                "skyminer_api": get_skyminer_api_resource,
                "checkpoint": get_checkpoint_resource,
                "output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager(),
            },
        )
        def created_job():
            notebook_run_op = ops["notebook_run"]
            notebook_run_op()

        return created_job

    @staticmethod
    def create_notebook_export_job(name, ops, timeout):
        @job(
            name=name,
            description=f"Job for {name}",
            tags={
                "category": "intelligent_pipeline_v1",
                "subcategory": name,
                "owner": "datascience_team",
                "notebook": "my_unique_notebook",
                "dagster/max_runtime": timeout,
            },
            resource_defs={
                "skyminer_api": get_skyminer_api_resource,
                "checkpoint": get_checkpoint_resource,
            },
        )
        def created_job():
            notebook_export_op = ops["notebook_export"]
            notebook_export_op()

        return created_job

    @staticmethod
    def create_lower_part_job(name, ops, timeout):
        @job(
            name=name,
            description=f"Job for {name}",
            tags={
                "category": "intelligent_pipeline_v1",
                "subcategory": name,
                "owner": "datascience_team",
                "lower_part": "my_unique_lower_part",
                "dagster/max_runtime": timeout,
            },
            resource_defs={
                "skyminer_api": get_skyminer_api_resource,
                "checkpoint": get_checkpoint_resource,
            },
        )
        def created_job():
            build_query_aggregation_op = ops["build_query"]
            get_data_aggregation_op = ops["get_data"]
            process_data_aggregation_op = ops["process_aggregation"]
            process_data_if_op = ops["process_if"]
            process_data_ra_op = ops["process_ra"]
            push_data_op = ops["push_if"]
            push_data_ra_op = ops["push_ra"]

            query = build_query_aggregation_op()
            dataframe = get_data_aggregation_op(query)
            processed_dataframe = process_data_aggregation_op(dataframe)
            process_df_if = process_data_if_op(processed_dataframe)
            push_data_op(process_df_if)
            process_df_ra = process_data_ra_op(processed_dataframe)
            push_data_ra_op(process_df_ra)

        return created_job

    @staticmethod
    def create_resample_anomaly_job(name, ops, timeout):
        @job(
            name=name,
            description=f"Job for {name}",
            tags={
                "category": "intelligent_pipeline_v1",
                "subcategory": name,
                "owner": "datascience_team",
                "resample_anomaly": "my_unique_resample_anomaly",
                "dagster/max_runtime": timeout,
            },
            resource_defs={
                "skyminer_api": get_skyminer_api_resource,
                "checkpoint": get_checkpoint_resource,
            },
        )
        def created_job():
            build_query_resample_op = ops["build_query"]
            get_data_resample_op = ops["get_data"]
            build_query_lb_ub_ops = ops["build_query_lb_ub"]
            get_data_lb_ub_ops = ops["get_data_lb_ub"]
            process_data_anomaly_op = ops["process_anomaly"]
            push_data_resample_op = ops["push_resample"]
            push_data_anomaly_op = ops["push_anomaly"]

            query = build_query_resample_op()
            data = get_data_resample_op(query)
            push_data_resample_op(data)
            query_ub_lb = build_query_lb_ub_ops()
            data_ub_lb = get_data_lb_ub_ops(query_ub_lb)
            process_data = process_data_anomaly_op(data, data_ub_lb)
            push_data_anomaly_op(process_data)

        return created_job

    @staticmethod
    def create_linear_prediction_job(name, ops, timeout):
        @job(
            name=name,
            description=f"Job for {name}",
            tags={"category": "intelligent_pipeline_v1", "subcategory": name,
                  "owner": "datascience_team", "linear_prediction": "my_unique_linear_prediction",
                  "dagster/max_runtime": timeout},
            resource_defs={"skyminer_api": get_skyminer_api_resource, "checkpoint": get_checkpoint_resource}
        )
        def create_job():
            build_query_resample_op = ops['build_query']
            get_data_resample_op = ops['get_data']
            linear_prediction_op = ops['prediction']
            push_data_op = ops['push']
            query = build_query_resample_op()
            data = get_data_resample_op(query)
            forecast = linear_prediction_op(data)
            push_data_op(forecast)
        return create_job
    
    @staticmethod
    def create_maintenance_job(name, ops, timeout):
        @job(
            name=name,
            description=f"Job for {name}",
            tags={"category": "intelligent_pipeline_v1", "subcategory": name, "owner": "datascience_team", 
                "maintenance_job": "my_unique_maintenance_job",
                "dagster/max_runtime": timeout}
        )
        def create_job():
            purge_op = ops['purge']
            purge_op()
        return create_job
