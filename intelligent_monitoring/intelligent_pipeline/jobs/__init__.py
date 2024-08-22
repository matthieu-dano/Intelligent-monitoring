from ..ops import AggregationOps, LttbOps, OneClassSvmOps
from ..ops.isolation_forest_ops import IsolationForest, RollingAverage
from ..ops.ml_predict_ops import MLpredict
from ..ops.ml_training_ops import MLtraining
from ..ops.notebooks_ops import NotebookOps
from ..ops.resample_ops import ResampleOps
from ..ops.maintenance_ops import MaintenanceOps
from .create_jobs import CreateJob

windows_size_if= "3D"
time_delta = "6D"
min_periods_if = 1
brick_name_if = "isolation_forest"
brick_name_ra = "rolling_average"

isolation_forest_ops = {
    "build_query": IsolationForest.create_build_query_op(
        brick_name_if, "build_query_" + brick_name_if, time_delta
    ),
    "get_data": IsolationForest.create_get_data_op(brick_name_if, "get_data_" + brick_name_if),
    "process": IsolationForest.create_process_data_op(
        brick_name_if, "process_data_" + brick_name_if, windows_size_if, min_periods_if
    ),
    "push": IsolationForest.create_push_data_op(brick_name_if, "push_data_" + brick_name_if),
}

brick_name_lttb_resample = "lttb_resample"

lttb_resample_ops = {
    "build_query": LttbOps.create_build_query_op(
        brick_name_lttb_resample, "build_query_" + brick_name_lttb_resample
    ),
    "get_data": LttbOps.create_get_data_op(
        brick_name_lttb_resample, "get_data_" + brick_name_lttb_resample
    ),
    "process": LttbOps.create_process_data_op(
        brick_name_lttb_resample, "process_data_" + brick_name_lttb_resample
    ),
    "push": LttbOps.create_push_data_op(
        brick_name_lttb_resample, "push_data_" + brick_name_lttb_resample
    ),
}

aggregation_frequency = "T"
brick_name_aggregation = "aggregation"


aggregation_ops = {
    "build_query": AggregationOps.create_build_query_op(
        brick_name_aggregation, "build_query_" + brick_name_aggregation
    ),
    "get_data": AggregationOps.create_get_data_op(
        brick_name_aggregation, "get_data_" + brick_name_aggregation
    ),
    "process": AggregationOps.create_process_data_op(
        brick_name_aggregation, "process_data_" + brick_name_aggregation
    ),
    "push": AggregationOps.create_push_data_op(
        brick_name_aggregation, "push_data_" + brick_name_aggregation
    ),
}

brick_name_ocsvm = "ocsvm"

one_class_svm_ops = {
    "build_query": OneClassSvmOps.create_build_query_op(
        brick_name_ocsvm, "build_query_" + brick_name_ocsvm
    ),
    "get_data": OneClassSvmOps.create_get_data_op(
        brick_name_ocsvm, "get_data_" + brick_name_ocsvm
    ),
    "process": OneClassSvmOps.create_process_data_op(
        brick_name_ocsvm, "process_data_" + brick_name_ocsvm
    ),
    "push": OneClassSvmOps.create_push_data_op(brick_name_ocsvm, "push_data_" + brick_name_ocsvm),
}

notebook_name = "report"
notebook_run_ops = {"notebook_run": NotebookOps.create_notebook_run_op(notebook_name)}
notebook_export_ops = {"notebook_export": NotebookOps.create_notebook_export_op(notebook_name)}

brick_name_mltraining = "training"

mltraining_ops = {
    "check": MLtraining.create_check_op(
        brick_name_mltraining, "check_model_" + brick_name_mltraining
    ),
    "build_query_first_date" : MLtraining.create_build_query_first_date_op(
        brick_name_mltraining, "build_query_first_date_" + brick_name_mltraining
    ),
    "get_data_first_date" : MLtraining.create_get_data_op(
        brick_name_mltraining, "get_data_first_date_" + brick_name_mltraining
    ),
    "build_query": MLtraining.create_build_query_op(
        brick_name_mltraining, "build_query_" + brick_name_mltraining
    ),
    "get_data": MLtraining.create_get_data_op(
        brick_name_mltraining, "get_data_" + brick_name_mltraining
    ),
    "process_ml": MLtraining.create_model_op(
        brick_name_mltraining, "model_" + brick_name_mltraining
    ),
    "save_model": MLtraining.create_save_op(
        brick_name_mltraining, "save_model_" + brick_name_mltraining
    ),
}

brick_name_mlpredict = "predict"

mlpredict_ops = {
    "get_model": MLpredict.create_get_model_op(
        brick_name_mlpredict, "get_model_" + brick_name_mlpredict
    ),
    "build_query": MLpredict.create_build_query_op_main_metric(
        brick_name_mlpredict, "build_query_" + brick_name_mlpredict
    ),
    "get_data": MLpredict.create_get_data_op(
        brick_name_mlpredict, "get_data_" + brick_name_mlpredict
    ),
    "build_query_multi": MLpredict.create_build_query_op_multi(
        brick_name_mlpredict, "build_query_multi_" + brick_name_mlpredict
    ),
    "get_data_multi": MLpredict.create_get_data_op(
        brick_name_mlpredict, "get_data_multi_" + brick_name_mlpredict
    ),
    "predict": MLpredict.create_forecast_model_op(
        brick_name_mlpredict, "using_model_" + brick_name_mlpredict
    ),
    "push": MLpredict.create_push_data_op(brick_name_mlpredict, "pushing_" + brick_name_mlpredict),
}

brick_name_lower_part = "lower_part"
windows_size_ra = "3D"
min_periods_ra = 1

lower_part_ops = {
    "build_query": IsolationForest.create_build_query_op(
        brick_name_aggregation, "build_query_" + brick_name_lower_part,
        time_delta
    ),
    "get_data": AggregationOps.create_get_data_op(
        brick_name_aggregation, "get_data_" + brick_name_lower_part
    ),
    "process_aggregation": AggregationOps.create_process_data_op(
        brick_name_aggregation,
        "process_data_" + brick_name_lower_part + "_" + brick_name_aggregation,
    ),
    "process_if": IsolationForest.create_process_data_op(
        brick_name_if,
        "process_data_" + brick_name_lower_part + "_" + brick_name_if,
        windows_size_if,
        min_periods_if,
    ),
    "process_ra": RollingAverage.create_process_data_op(
        brick_name_ra,
        "process_data_" + brick_name_lower_part + "_" + brick_name_ra,
        windows_size_ra,
        min_periods_ra
    ),
    "push_if": IsolationForest.create_push_data_op(
        brick_name_if, "push_data_" + brick_name_lower_part + "_" + brick_name_if
    ),
    "push_ra": RollingAverage.create_push_data_op(
        brick_name_ra, "push_data_" + brick_name_lower_part + "_" + brick_name_ra
    )
}

brick_name_resample = "resample"

resample_anomaly_ops = {
    "build_query": ResampleOps.create_build_query_op(
        brick_name_resample, "build_query_" + brick_name_resample
    ),
    "get_data": ResampleOps.create_get_data_op(brick_name_resample, "get_data_" + brick_name_resample),
    "build_query_lb_ub": ResampleOps.create_build_query_lb_ub_op(
        brick_name_resample, "build_query_lb_ub_" + brick_name_resample
    ),
    "get_data_lb_ub": ResampleOps.create_get_data_op(
        brick_name_resample, "get_data_lb_ub_" + brick_name_resample
    ),
    "process_anomaly": ResampleOps.create_process_data_op(
        brick_name_resample, "process_data_" + brick_name_resample
    ),
    "push_anomaly": ResampleOps.create_push_data_op(brick_name_resample,
                                                    "push_data_anomaly_" + brick_name_resample),
    "push_resample": ResampleOps.create_push_data_op(brick_name_resample,
                                                     "push_data_resample_" + brick_name_resample),
}

brick_name_linear = "linear"
linear_predict_ops = {
    "build_query": MLtraining.create_build_query_op(brick_name_linear, "build_query_"+brick_name_linear),
    "get_data": MLtraining.create_get_data_op(brick_name_linear, "get_data_"+brick_name_linear),
    "prediction": MLtraining.create_linear_op(brick_name_linear, "prediction_"+brick_name_linear),
    "push": MLtraining.create_push_data_op(brick_name_linear, "push_data_"+brick_name_linear)
}

brick_name_maintenance = "maintenance"
maintenance_ops = {
    "purge": MaintenanceOps.create_purge_old_runs_op(brick_name_maintenance, "purge_old_runs_"+brick_name_maintenance)
}

timeout_every_minute = 90
timeout_every_five_minute = 400
timeout_every_hours = 4000
timeout_every_thirty_min = 60*30

# Create jobs using CreateJob class
lttb_resample_job = CreateJob.create_job(
    brick_name_lttb_resample, lttb_resample_ops, timeout_every_five_minute
)
aggregation_job = CreateJob.create_job(
    brick_name_aggregation, aggregation_ops, timeout_every_five_minute
)
one_class_svm_job = CreateJob.create_job(
    brick_name_ocsvm, one_class_svm_ops, timeout_every_five_minute
)
notebook_run_job = CreateJob.create_notebook_run_job(
    "notebook_run_"+notebook_name, notebook_run_ops, timeout_every_hours
)
notebook_export_job = CreateJob.create_notebook_export_job(
    "notebook_export_"+notebook_name, notebook_export_ops, timeout_every_hours
)
mltraining_job = CreateJob.create_job_training(brick_name_mltraining, mltraining_ops)

linear_prediction_job = CreateJob.create_linear_prediction_job(brick_name_linear, linear_predict_ops, timeout_every_hours)

mlprediction_job = CreateJob.create_job_prediction(
    brick_name_mlpredict, mlpredict_ops, timeout_every_thirty_min
)
isolation_forest_job = CreateJob.create_job(
    brick_name_if, isolation_forest_ops, timeout_every_thirty_min
)
lower_part_job = CreateJob.create_lower_part_job(
    brick_name_lower_part, lower_part_ops, timeout_every_hours
)
resample_anomaly_job = CreateJob.create_resample_anomaly_job(
    brick_name_resample, resample_anomaly_ops, timeout_every_five_minute
)
maintenance_job = CreateJob.create_maintenance_job(brick_name_maintenance, maintenance_ops, timeout_every_hours)
