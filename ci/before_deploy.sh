#!/bin/bash
set -e

PROJECT_NAME=$1
CI_COMMIT_TAG=$2

# Paths and file names
ZIP_FILE="/home/dagster/${PROJECT_NAME}-${CI_COMMIT_TAG}.zip"
PROJECT_DIR="/home/dagster/${PROJECT_NAME}"
CONTAINER_NAME="dagster_intelligent_pipeline"

# Stop and remove the current deployment if it exists
if [ -d "$PROJECT_DIR" ]; then
  echo "Stopping the current deployment."

  # Attempt to stop and remove the dagster_intelligent_pipeline container
  if [ "$(docker ps -q -f name=${CONTAINER_NAME})" ]; then
    echo "Stopping the container ${CONTAINER_NAME}..."
    docker stop ${CONTAINER_NAME}
    echo "Removing the container ${CONTAINER_NAME}..."
    docker rm ${CONTAINER_NAME}
  fi

  # Attempt to bring down the docker-compose stack
  docker-compose -f "${PROJECT_DIR}/docker-compose.yaml" down || true
  
  # Remove the project directory
  rm -rf "$PROJECT_DIR"
fi

# Cleanup old zip files except the current one
rm -rf /home/dagster/*.zip
