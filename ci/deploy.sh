#!/bin/bash
set -e

POSTGRES_USER=$1
POSTGRES_PASSWORD=$2
POSTGRES_DB=$3
DAGSTER_HOME=$4
SKYMINER_API_URL=$5
NOTEBOOK_PATH=$6
MODEL_PATH=$7
CHECKPOINT_PATH=$8
PROJECT_NAME=$9
GRAPHQL_URL=$10


# Project directory
PROJECT_DIR="/home/dagster/${PROJECT_NAME}"

# Change directory to the project directory
cd "$PROJECT_DIR"

export POSTGRES_USER
export POSTGRES_PASSWORD
export POSTGRES_DB
export DAGSTER_HOME
export SKYMINER_API_URL
export NOTEBOOK_PATH
export MODEL_PATH
export CHECKPOINT_PATH
export GRAPHQL_URL

# Rebuild the Dockerfile_intelligent_pipeline image
echo "Rebuilding the dagster_intelligent_pipeline image."
docker-compose build dagster_intelligent_pipeline 

# Rebuild the Dockerfile_dagster image
echo "Rebuilding the dagster_daemon image."
docker-compose build dagster_daemon

# Rebuild the Dockerfile_dagster webserver image
echo "Rebuilding the dagster_webserver image."
docker-compose build dagster_webserver 

# Relaunch the updated project
echo "Relaunching the project with docker-compose."
docker-compose up -d

# Remove the deployment scripts
rm *.sh

