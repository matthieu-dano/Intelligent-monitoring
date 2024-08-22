# Intelligent Monitoring

## Overview

The Intelligent Monitoring project aims to detect anomalies in real-time on satellite telemetry data. It sends this information to the customer and a virtual assistant capable of answering any related questions. The assistant provides precise dates of anomalies and possible explanations using various in-house developed analysis and detection tools. Dagster is used to orchestrate the multiple processes in this pipeline.

## Features

- **Real-time Anomaly Detection**: Monitor satellite telemetry data for anomalies in real-time.
- **Customer Notifications (In progress)**: Automatically notify customers of detected anomalies.
- **Virtual Assistant (In progress)**: A virtual assistant that can answer questions about anomalies, including dates and possible explanations.
- **Dagster Orchestration**: Use Dagster to orchestrate and manage the data pipeline processes.

## Prerequisites

Before you begin, ensure you have met the following requirements:

- Python 3.10
- Docker and Docker Compose

## Running the Project Locally

To run Dagster in development mode with full UI support, follow these steps. This exposes the UI on [http://localhost:3000](http://localhost:3000) by default.

1. **Environment variables**

Create a dagster_home directory at the root of the intelligent monitoring project:

```bash
cd file_path_to_intelligent_monitoring_project
mkdir dagster_home
```

Add a file names .env at the root of intelligent monitoring project, **DON'T FORGET TO REPLACE DAGSTER_HOME WITH THE TRUE LOCATION** :

```yaml
SKYMINER_API_URL = "http://192.168.48.101"

DAGSTER_URL = "http://localhost:3000"
GRAPHQL_URL = "http://localhost:3000/graphql"

CHECKPOINT_PATH = "./intelligent_pipeline/checkpoints"
MODEL_PATH = "./intelligent_pipeline/models"
NOTEBOOK_PATH = "./intelligent_pipeline/notebooks"
TRAINING_PATH = "./intelligent_pipeline/training"
DAGSTER_HOME = "**ABSOLUTE_PATH_TO_DAGSTER_HOME**"
```

2. **Dagster Configuration File**

```bash
cd dagster_home
```

Create a dagster.yaml file inside the dagster_home directory with the following configuration **DON'T FORGET TO REPLACE base_dir WITH THE TRUE LOCATION**:

```yaml
local_artifact_storage:
  module: dagster._core.storage.root
  class: LocalArtifactStorage
  config:
    base_dir:**PATH_TO_DAGSTER_HOME**

run_storage:
  module: dagster._core.storage.runs
  class: SqliteRunStorage
  config:
    base_dir: **PATH_TO_DAGSTER_HOME/storage**

event_log_storage:
  module: dagster._core.storage.event_log
  class: SqliteEventLogStorage
  config:
    base_dir: **PATH_TO_DAGSTER_HOME/history**

compute_logs:
  module: dagster._core.storage.local_compute_log_manager
  class: LocalComputeLogManager
  config:
    base_dir: **PATH_TO_DAGSTER_HOME/logs**

schedule_storage:
  module: dagster._core.storage.schedules
  class: SqliteScheduleStorage
  config:
    base_dir: **PATH_TO_DAGSTER_HOME/schedules**

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
```

Create a workspace.yaml file inside the dagster_home directory with the following configuration :
```yaml
load_from:
  - python_module:
    module_name: intelligent_pipeline
```

3. **Install dependencies**

**DON'T FORGET PYTHON 3.10**

On Linux :

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

On Windows :

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
``` 

4. **Start Dagster**

```bash
cd root_intelligent_monitoring
dagster dev
```

This exposes the UI on [http://localhost:3000](http://localhost:3000) by default.

## Deploying the Application

To deploy the application using Docker Compose, follow these steps:

1. **Volumes and location project**

```bash
# Create the directory /home/dagster if it doesn't already exist
mkdir -p /home/dagster

# Paste the project inside /home/dagster (Assuming you mean to copy the project from a source directory)
cp -r path_to_intelligent_monitoring /home/dagster/

# Create the volumes directory inside /home/dagster
mkdir -p /home/dagster/volumes

# Copy checkpoints from /home/dagster/intelligent_monitoring/intelligent_pipeline/checkpoints to /home/dagster/volumes
cp -r /home/dagster/intelligent_monitoring/intelligent_pipeline/checkpoints /home/dagster/volumes/

# Copy checkpoints from /home/dagster/intelligent_monitoring/intelligent_pipeline/notebooks to /home/dagster/volumes
cp -r /home/dagster/intelligent_monitoring/intelligent_pipeline/notebooks /home/dagster/volumes/

# Rename /home/dagster/volumes/notebooks to /home/dagster/volumes/reports
mv /home/dagster/volumes/notebooks /home/dagster/volumes/reports
```

2. **Environment variables**

Create a dagster_home directory at the root of the intelligent monitoring project:

```yaml
SKYMINER_API_URL = "http://192.168.48.101"

DAGSTER_URL = "http://localhost:4000"
GRAPHQL_URL = "http://localhost:4000/graphql"

CHECKPOINT_PATH = "./intelligent_pipeline/checkpoints"
MODEL_PATH = "./intelligent_pipeline/models"
NOTEBOOK_PATH = "./intelligent_pipeline/notebooks"
TRAINING_PATH = "./intelligent_pipeline/training"
DAGSTER_HOME = "/opt/dagster/dagster_home"
```

3. **Build and Start the Docker Containers**

    ```bash
    docker-compose build
    docker-compose up 
    ```

4. **Access the Application**

    The application should now be running and accessible. Check the Docker Compose logs for the exact URL and port. By default [http://localhost:4000](http://localhost:4000).

## Project Structure

Briefly describe the main directories and files in your project:

```plaintext
.
├── dagster_home/           # Directory for the local conf of dagster
├── docs/                   # Complete docs about the project and the operations defined
├── intelligent_pipeline/   # Code source of the pipeline
│   ├── assets/             # Asset definitions
│   ├── checkpoints/        # Directory used to stored checkpoint for jobs, metrics and tags, it's used in query
│   ├── graphql/            # Directory containing example of graphql queries
│   ├── jobs/               # Job defintions
│   ├── models/             # Directory to store model in used to predict
│   ├── notebooks/          # Directory to store notebook used
│   ├── ops/                # Operation definitions
│   ├── partitions/         # Partition definitions
│   ├── ressources/         # Resource definitions
│   ├── schedules/          # Schedule definitions
│   ├── sensors/            # Sensor definitions 
│   ├── __init__.py         # The definition of object used in the pipeline 
│   ├── settings.py         # Python file containing the metrics and tags used in the pipeline
├── tests/                  # Units tests used in CI
├── .env                    # Environment variables defined in a file used by dagster for local environment
├── Dockerfile_dagster      # Dockerfile for the webserver part of dagster
├── Dockerfile_grpc_server  # Dockerfile for the code locations part using grpc server
├── SkyminerTS-1.0....whl   # Python librairie to use Skyminer API
├── dagster.yaml             # Dagster configuration files (scheduling, execution, logs...)
├── docker-compose.yml      # Docker Compose configuration
├── requirements.txt        # Python dependencies
└── workspace.yaml          # Dagster configurations file to define the locations of the repositories and pipelines
```

**Metrics**

This file inside the intelligent_pipeline folder contains the metrics and tags uses by dagster to launch the pipeline and build associated queries to Skyminer with theses values in parameters :

```python
metrics = [["SCH.M1", "SCH.M15"]]  # Metrics
tags = [{"satellite": "SAT2"}] # Tag used for the predic

#Number of days the old runs op logs are kept
days_to_keep = 7
```

