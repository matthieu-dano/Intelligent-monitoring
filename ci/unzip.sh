#!/bin/bash
set -e

PROJECT_NAME=$1
CI_COMMIT_TAG=$2


# Paths and file names
ZIP_FILE="/home/dagster/${PROJECT_NAME}-${CI_COMMIT_TAG}.zip"
PROJECT_DIR="/home/dagster/${PROJECT_NAME}"

# Unzip the project
unzip -o "$ZIP_FILE" -d "$PROJECT_DIR"
