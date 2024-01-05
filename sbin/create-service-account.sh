#!/bin/bash

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --project-id)
            PROJECT_ID="$2"
            shift
            ;;
        *)
            echo "Unknown parameter: $1"
            exit 1
            ;;
    esac
    shift
done

# Check if PROJECT_ID is provided
if [ -z "$PROJECT_ID" ]; then
    echo "Error: --project-id is a required argument."
    exit 1
fi

# Variables
SERVICE_ACCOUNT_NAME="dataslush-waltlabs-movielens"
DESCRIPTION="This service account is used for waltlabs movielens interview problem"
DISPLAY_NAME="DataSlush Waltlabs Movielens"
ROLE_STORAGE="roles/storage.admin"
ROLE_BIGQUERY="roles/bigquery.admin"
CURRENT_FOLDER=$(pwd)
SERVICE_ACCOUNT_FOLDER="$CURRENT_FOLDER/service-account"
KEY_FILE_PATH="$SERVICE_ACCOUNT_FOLDER/$SERVICE_ACCOUNT_NAME.json"

# Create service-account folder if it doesn't exist
mkdir -p $SERVICE_ACCOUNT_FOLDER

gcloud config set project $PROJECT_ID

# Create Service Account
gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
  --description="$DESCRIPTION" \
  --display-name="$DISPLAY_NAME"

# Assign Roles to the Service Account
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="$ROLE_STORAGE"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="$ROLE_BIGQUERY"

# Create JSON Key File
gcloud iam service-accounts keys create $KEY_FILE_PATH \
  --iam-account="$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"

# Set GOOGLE_APPLICATION_CREDENTIALS environment variable
# export GOOGLE_APPLICATION_CREDENTIALS=$KEY_FILE_PATH

echo "Service account created successfully."
