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

**DON'T FORGET TO REPLACE THE ABSOLUTE_PATH_TO_INTELLIGENT_MONITORING**

On Linux :

```bash
cd file_path_to_intelligent_monitoring/deploy
chmod u+x docker.sh
./local.sh **ABSOLUTE_PATH_TO_INTELLIGENT_MONITORING**
```

## Running the project Docker

To run Dagster in docker mode with full UI support, follow these steps. This exposes the UI on [http://localhost:4000](http://localhost:4000) by default.

**DON'T FORGET TO REPLACE THE ABSOLUTE_PATH_TO_INTELLIGENT_MONITORING**

On Linux :
```bash
cd file_path_to_intelligent_monitoring/deploy
chmod u+x local.sh
./docker.sh **ABSOLUTE_PATH_TO_INTELLIGENT_MONITORING**
```

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

