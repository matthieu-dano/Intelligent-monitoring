from dagster import Definitions

from .jobs import (
    aggregation_job,
    isolation_forest_job,
    lower_part_job,
    lttb_resample_job,
    mlprediction_job,
    mltraining_job,
    notebook_run_job,
    one_class_svm_job,
    resample_anomaly_job,
    maintenance_job,
    linear_prediction_job,
    notebook_export_job
)
from .schedules import (
    every_day_notebook,
    every_five_minute_anomaly,
    every_day_lower_part,
    every_hour_training,
    daily_purge,
    every_minute_linear_prediction
)
from .sensors import export_sensor, mlpredict_sensor

defs = Definitions(
    jobs=[
        mltraining_job,
        lttb_resample_job,
        aggregation_job,
        one_class_svm_job,
        mlprediction_job,
        notebook_run_job,
        isolation_forest_job,
        lower_part_job,
        resample_anomaly_job,
        maintenance_job,
        linear_prediction_job,
        notebook_export_job
    ],
    schedules=[
        every_hour_training,
        every_day_notebook,
        every_day_lower_part,
        every_five_minute_anomaly,
        daily_purge,
        every_minute_linear_prediction
    ],
    sensors= [export_sensor, mlpredict_sensor],
)
