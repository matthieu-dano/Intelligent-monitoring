from dagster import sensor

from ..jobs import mlprediction_job, notebook_export_job, one_class_svm_job
from ..jobs.generate_run_configs_sensor import GenerateRunConfigSensor
from ..settings import metrics, tags


@sensor(job=one_class_svm_job, minimum_interval_seconds=30)
def aggregation_sensor(context):
    return GenerateRunConfigSensor.basic_requests(context, "aggregation", "ocsvm", metrics, tags)


@sensor(job=mlprediction_job, minimum_interval_seconds=900)
def mlpredict_sensor(context):
    return GenerateRunConfigSensor.ml_predict_requests(
        context, "training", "predict", metrics, tags
    )


@sensor(job=notebook_export_job, minimum_interval_seconds=120)
def export_sensor(context):
    return GenerateRunConfigSensor.export_notebook_requests(
        context, "notebook_run_report", "report", metrics, tags
    )
