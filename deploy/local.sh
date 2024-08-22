#!/bin/bash

# Check if the script received the required argument
if [ -z "$1" ]; then
  echo "Usage: $0 <intelligent_monitoring_path>"
  exit 1
fi

# Set the intelligent monitoring path from the first argument
INTELLIGENT_MONITORING_PATH="$1"
DAGSTER_HOME_PATH="${INTELLIGENT_MONITORING_PATH}/dagster_home"

DAGSTER_HOME_PATH="${INTELLIGENT_MONITORING_PATH}/dagster_home"

SKYMINER_API_URL="http://192.168.48.101"
DAGSTER_URL="http://localhost:3000"
GRAPHQL_URL="http://localhost:3000/graphql"

# Create necessary directories and files
mkdir -p "$INTELLIGENT_MONITORING_PATH/dagster_home"

# Create .env file
cat <<EOL > "$INTELLIGENT_MONITORING_PATH/.env"
SKYMINER_API_URL="$SKYMINER_API_URL"
DAGSTER_URL="$DAGSTER_URL"
GRAPHQL_URL="$GRAPHQL_URL"
CHECKPOINT_PATH="./intelligent_pipeline/checkpoints"
MODEL_PATH="./intelligent_pipeline/models"
NOTEBOOK_PATH="./intelligent_pipeline/notebooks"
TRAINING_PATH="./intelligent_pipeline/training"
DAGSTER_HOME="$DAGSTER_HOME_PATH"
EOL

# Create dagster.yaml file
cat <<EOL > "$INTELLIGENT_MONITORING_PATH/dagster_home/dagster.yaml"
local_artifact_storage:
  module: dagster._core.storage.root
  class: LocalArtifactStorage
  config:
    base_dir: $DAGSTER_HOME_PATH

run_storage:
  module: dagster._core.storage.runs
  class: SqliteRunStorage
  config:
    base_dir: $DAGSTER_HOME_PATH/storage

event_log_storage:
  module: dagster._core.storage.event_log
  class: SqliteEventLogStorage
  config:
    base_dir: $DAGSTER_HOME_PATH/history

compute_logs:
  module: dagster._core.storage.local_compute_log_manager
  class: LocalComputeLogManager
  config:
    base_dir: $DAGSTER_HOME_PATH/logs

schedule_storage:
  module: dagster._core.storage.schedules
  class: SqliteScheduleStorage
  config:
    base_dir: $DAGSTER_HOME_PATH/schedules

scheduler:
  module: dagster._core.scheduler
  class: DagsterDaemonScheduler
  config: {}

run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    tag_concurrency_limits:
      - key: "job"
        value: "my_unique_job"
        limit: 5
      - key: "training"
        value: "my_unique_training"
        limit: 1
      - key: "predict"
        value: "my_unique_predict"
        limit: 1
      - key: "notebook"
        value: "my_unique_notebook"
        limit: 1
      - key: "lower_part"
        value: "my_unique_lower_part"
        limit: 1
      - key: "resample_anomaly"
        value: "my_unique_resample_anomaly"
        limit: 1
      - key: "linear_prediction"
        value: "my_unique_linear_prediction"
        limit: 1
      - key: "maintenance_job"
        value: "my_unique_maintenance_job"
        limit: 1
    max_concurrent_runs: -1

run_launcher:
  module: dagster
  class: DefaultRunLauncher
  config: {}

# Opt in to run monitoring
run_monitoring:
  enabled: true
  
telemetry:
  enabled: false

# Data retention policies
retention:
  schedule:
    purge_after_days: 7  # Retain schedule ticks for 7 days
  sensor:
    purge_after_days:
      skipped: 7  # Retain skipped sensor ticks for 7 days
      failure: 30  # Retain failed sensor ticks for 30 days
      success: -1  # Retain successful sensor ticks indefinitely
EOL

# Create workspace.yaml file
cat <<EOL > "$INTELLIGENT_MONITORING_PATH/dagster_home/workspace.yaml"
load_from:
  - python_module:
    module_name: intelligent_pipeline
EOL

# Check Python version and install dependencies
PYTHON_VERSION=$(python --version 2>&1)
if [[ $PYTHON_VERSION != *"Python 3.10"* ]]; then
  echo "Error: Python 3.10 is required. Please install Python 3.10 and try again."
  exit 1
fi

# Create and activate virtual environment
python -m venv "$INTELLIGENT_MONITORING_PATH/venv"
source "$INTELLIGENT_MONITORING_PATH/venv/bin/activate"

# Install dependencies
pip install -r "$INTELLIGENT_MONITORING_PATH/requirements.txt"

# Start Dagster
cd "$INTELLIGENT_MONITORING_PATH"
dagster dev

echo "Dagster is now running in development mode. Access the UI at http://localhost:3000"
