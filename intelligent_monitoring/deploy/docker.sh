#!/bin/bash

# Check if the script received the required argument
if [ -z "$1" ]; then
  echo "Usage: $0 <project_source_path>"
  exit 1
fi

# Define paths
PROJECT_SOURCE_PATH=$1

DAGSTER_HOME="/home/dagster"
VOLUMES_PATH="$DAGSTER_HOME/volumes"
INTELLIGENT_MONITORING_PATH="$DAGSTER_HOME/intelligent_monitoring"
PIPELINE_PATH="$INTELLIGENT_MONITORING_PATH/intelligent_pipeline"

# Step 1: Create the /home/dagster directory if it doesn't already exist
echo "Creating /home/dagster directory..."
mkdir -p "$DAGSTER_HOME"

# Step 2: Copy the project to /home/dagster
echo "Copying project files to /home/dagster..."
cp -r "$PROJECT_SOURCE_PATH" "$DAGSTER_HOME/"

# Step 3: Rename the copied project directory
# Assuming the original project directory name is 'intelligent_monitoring-main'
if [ -d "$DAGSTER_HOME/intelligent_monitoring-main" ]; then
  echo "Renaming directory from 'intelligent_monitoring-main' to 'intelligent_monitoring'..."
  mv "$DAGSTER_HOME/intelligent_monitoring-main" "$INTELLIGENT_MONITORING_PATH"
else
  echo "Directory 'intelligent_monitoring-main' not found in $DAGSTER_HOME. Skipping rename."
fi

# Step 3: Create the volumes directory inside /home/dagster
echo "Creating volumes directory..."
mkdir -p "$VOLUMES_PATH"

# Step 4: Copy checkpoints to the volumes directory
echo "Copying checkpoints..."
cp -r "$PIPELINE_PATH/checkpoints" "$VOLUMES_PATH/"

# Step 4: Copy checkpoints to the volumes directory
echo "Copying checkpoints..."
cp -r "$PIPELINE_PATH/models" "$VOLUMES_PATH/"

# Step 5: Copy notebooks to the volumes directory
echo "Copying notebooks..."
cp -r "$PIPELINE_PATH/notebooks" "$VOLUMES_PATH/"

# Step 6: Rename notebooks to reports
echo "Renaming notebooks to reports..."
mv "$VOLUMES_PATH/notebooks" "$VOLUMES_PATH/reports"

# Step 7: Set environment variables (assuming these need to be exported for Docker Compose)
export SKYMINER_API_URL="http://192.168.48.101"
export DAGSTER_URL="http://localhost:4000"
export GRAPHQL_URL="http://localhost:4000/graphql"
export CHECKPOINT_PATH="./intelligent_pipeline/checkpoints"
export MODEL_PATH="./intelligent_pipeline/models"
export NOTEBOOK_PATH="./intelligent_pipeline/notebooks"
export TRAINING_PATH="./intelligent_pipeline/training"
export DAGSTER_HOME="/opt/dagster/dagster_home"
export POSTGRES_USER="postgres"
export POSTGRES_PASSWORD="postgres"
export POSTGRES_DB="postgres"

# Step 8: Build and start the Docker containers
echo "Building Docker containers..."
docker compose build

echo "Starting Docker containers..."
docker compose up -d

echo "Deployment complete. The application should now be accessible at http://localhost:4000."

# End of script