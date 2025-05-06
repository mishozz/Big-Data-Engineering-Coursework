#!/bin/bash

# Setup script for ML Stack with Docker Compose

set -e  # Exit on error

echo "Creating directory structure..."

# Create main directories
mkdir -p airflow/dags
mkdir -p airflow/logs
mkdir -p airflow/plugins
mkdir -p airflow/scripts
mkdir -p airflow/data
mkdir -p airflow/secrets

# Create MongoDB directory for persistent storage
mkdir -p mongodb_data

# Make Airflow DAGs directory writable
chmod -R 777 airflow/dags
chmod -R 777 airflow/logs
chmod -R 777 airflow/plugins
chmod -R 777 airflow/scripts
chmod -R 777 airflow/data
chmod -R 777 airflow/secrets
