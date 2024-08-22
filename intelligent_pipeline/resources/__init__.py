import os

from dagster import resource
from ..checkpoints import MetricCheckpoint
from ..models import ModelCheck
from . import skyminer_functions as sf


@resource
def get_skyminer_api_resource(_):
    server_url = os.getenv("SKYMINER_API_URL")
    if not server_url:
        raise ValueError("SERVER_URL environment variable is not set.")
    return sf.SkyminerPostProcessingUtils(server_url)


@resource
def get_checkpoint_resource(_):
    return MetricCheckpoint()


@resource
def get_model_resource(_):
    return ModelCheck()


