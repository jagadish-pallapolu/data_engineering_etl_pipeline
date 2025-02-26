"""
Description
----------------
This Script is used to get data from sftp server and upload to gcs
This Script can be deployed to cloud function and use Cloud Scheduler triggers the function on the 1st of every month

How It Works
--------------
Cloud Scheduler triggers the function on the 1st of every month.
The function downloads the macroeconomic dataset from the SFTP server.
The file is uploaded to a Google Cloud Storage bucket with a timestamped name

Deployment Instructions
=======================

Create the Cloud Function:
-------------------------
gcloud functions deploy sftp_to_gcs \
    --runtime python39 \
    --trigger-topic macroeconomic-transfer \
    --memory 512MB \
    --timeout 300s \
    --region us-central1 \
    --set-env-vars GCS_BUCKET_NAME=your-gcs-bucket,SFTP_HOST=sftp.example.com,SFTP_USERNAME=your_username,SFTP_PASSWORD=your_password


Set Up a Cloud Scheduler Job (Runs Monthly):
-------------------------------------------
gcloud scheduler jobs create pubsub macroeconomic-sftp-job \
    --schedule="0 0 1 * *" \
    --time-zone="UTC" \
    --topic=macroeconomic-transfer \
    --message-body="{}"

=================================================

"""

import os
import pysftp
from google.cloud import storage
from datetime import datetime

# SFTP Credentials (Replace with actual credentials)
SFTP_HOST = "sftp.example.com"
SFTP_USERNAME = "your_username"
SFTP_PASSWORD = "your_password"
SFTP_REMOTE_PATH = "/data/macro_data.csv"  # File path on SFTP server

# GCS Details
GCS_BUCKET_NAME = "your-gcs-bucket"
GCS_DESTINATION_PATH = "macroeconomic_data/macro_data_{date}.csv"

def download_from_sftp():
    """Download file from SFTP server."""
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # Disable host key verification (not recommended for production)

    with pysftp.Connection(SFTP_HOST, username=SFTP_USERNAME, password=SFTP_PASSWORD, cnopts=cnopts) as sftp:
        local_file = f"/tmp/macro_data_{datetime.now().strftime('%Y-%m')}.csv"
        sftp.get(SFTP_REMOTE_PATH, local_file)
        return local_file

def upload_to_gcs(local_file):
    """Upload file to Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)

    destination_blob = GCS_DESTINATION_PATH.format(date=datetime.now().strftime('%Y-%m'))
    blob = bucket.blob(destination_blob)

    blob.upload_from_filename(local_file)
    print(f"File {local_file} uploaded to {destination_blob} in {GCS_BUCKET_NAME}")

def sftp_to_gcs(event, context):
    """Cloud Function to transfer file from SFTP to GCS."""
    try:
        local_file = download_from_sftp()
        upload_to_gcs(local_file)
        print("SFTP to GCS transfer completed successfully!")
    except Exception as e:
        print(f"Error: {e}")
