#!/bin/bash

# Exit on error
set -e

# Get the directory where this script is located (project root)
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"

# echo "Step: Downloading Elasticsearch IK plugin..."
# bash "$BASE_DIR/scripts/download-es-analysis-ik.sh"

echo -e "\nStep: Starting Docker containers..."
docker compose up -d

echo -e "\nStep: Updating /etc/hosts in containers..."
bash "$BASE_DIR/scripts/update-docker-hosts.sh"

echo -e "\nStep: Running Spark submission script..."
bash "$BASE_DIR/scripts/submit_to_master.sh"

echo -e "\n[✓] All steps completed successfully."
