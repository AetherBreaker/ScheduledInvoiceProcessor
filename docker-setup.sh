#!/bin/bash

# Docker setup script for Scheduled Order Middleman
# This script helps set up the secrets directory structure

set -e

echo "Setting up Docker secrets directory..."

# Create secrets directory
mkdir -p secrets

# Check if secret files exist in current directory
if [ -f "db-key.json" ]; then
    echo "Found db-key.json, copying to secrets/"
    cp db-key.json secrets/
fi

if [ -f "sas_ftp_creds.json" ]; then
    echo "Found sas_ftp_creds.json, copying to secrets/"
    cp sas_ftp_creds.json secrets/
fi

if [ -f "sft_creds.json" ]; then
    echo "Found sft_creds.json, copying to secrets/"
    cp sft_creds.json secrets/
fi

# Create directories for volumes
mkdir -p logs
mkdir -p queue_backups

echo ""
echo "Setup complete!"
echo ""
echo "Please ensure the following files exist in the secrets/ directory:"
echo "  - secrets/db-key.json"
echo "  - secrets/sas_ftp_creds.json"
echo "  - secrets/sft_creds.json"
echo ""
echo "To start the application:"
echo "  docker-compose up -d"
echo ""
echo "To view logs:"
echo "  docker-compose logs -f"
echo ""
echo "To stop the application:"
echo "  docker-compose down"
