from dagster import schedule

from ..jobs import (
    aggregation_job,
    isolation_forest_job,
    lower_part_job,
    lttb_resample_job,
    mlprediction_job,
    mltraining_job,
    notebook_run_job,
    resample_anomaly_job,
    linear_prediction_job,
    maintenance_job
)
from ..jobs.generate_run_configs import GenerateRunConfig
from ..settings import metrics, tags, days_to_keep


@schedule(
    cron_schedule="* * * * *",
    job=lttb_resample_job,
    execution_timezone="Europe/Paris",
)
def every_minute_resample(context):
    return GenerateRunConfig.basic_run_configs(context, "lttb", metrics, tags)


@schedule(
    cron_schedule="* * * * *",
    job=aggregation_job,
    execution_timezone="Europe/Paris",
)
def every_minute_aggregation(context):
    """Schedule to run the aggregation pipeline every minute."""
    return GenerateRunConfig.basic_run_configs(context, "aggregation", metrics, tags)


@schedule(
    cron_schedule="0 1 * * *",
    job=isolation_forest_job,
    execution_timezone="Europe/Paris",
)
def every_minute_isolation_forest(context):
    """Schedule to run the isolation_forest pipeline every minute."""
    return GenerateRunConfig.basic_run_configs(context, "isolation_forest", metrics, tags)


@schedule(
    cron_schedule="5 * * * *",
    job=notebook_run_job,
    execution_timezone="Europe/Paris",
)
def every_day_notebook(context):
    """Schedule to run the notebook pipeline every minute."""
    return GenerateRunConfig.notebook_run_configs(context, "report", metrics, tags)


@schedule(
    cron_schedule="0 * * * *",
    job=mltraining_job,
    execution_timezone="Europe/Paris",
)
def every_hour_training(context):
    """Schedule to run the aggregation pipeline every minute."""
    return GenerateRunConfig.training_run_configs(context, "training", metrics, tags)


@schedule(
    cron_schedule="* * * * *",
    job=mlprediction_job,
    execution_timezone="Europe/Paris",
)
def every_day_predict(context):
    """Schedule to run the aggregation pipeline every minute."""
    return GenerateRunConfig.predict_run_configs(context, "predict", metrics, tags)


@schedule(
    cron_schedule="5 * * * *",
    job=lower_part_job,
    execution_timezone="Europe/Paris",
)
def every_day_lower_part(context):
    """Schedule to run the aggregation pipeline every minute."""
    return GenerateRunConfig.lower_part_run_config(context, "lower_part", metrics, tags)


@schedule(cron_schedule="*/5 * * * *", job=resample_anomaly_job, execution_timezone="Europe/Paris")
def every_five_minute_anomaly(context):
    return GenerateRunConfig.resample_anomaly_run_config(context, "resample", metrics,
                                                         tags, extension="_dagster_resample")

@schedule(cron_schedule="0 * * * *", job=linear_prediction_job, execution_timezone="Europe/Paris")
def every_minute_linear_prediction(context):
    return GenerateRunConfig.linear_prediction_run_config(context, "linear", metrics, tags)

@schedule(cron_schedule="0 6 * * *", job=maintenance_job, execution_timezone="UTC")
def daily_purge(context):
    return GenerateRunConfig.maintenance_run_config(context, "maintenance", days_to_keep)
