# --------------------------
# Terraform Configuration
# --------------------------
provider "google" {
  project = "your-gcp-project-id"
  region  = "australia-southeast1"
}

# --------------------------
# Create Google Cloud Storage Buckets
# --------------------------
resource "google_storage_bucket" "transaction_data_bucket" {
  name     = "bank-retail-transactions"
  location = "AU"
  uniform_bucket_level_access = true
}

# Folders for raw and processed data
resource "google_storage_bucket_object" "raw_folder" {
  name   = "raw/"
  bucket = google_storage_bucket.transaction_data_bucket.name
}

resource "google_storage_bucket_object" "processed_folder" {
  name   = "processed/"
  bucket = google_storage_bucket.transaction_data_bucket.name
}

# --------------------------
# BigQuery Dataset & Table
# --------------------------
resource "google_bigquery_dataset" "bank_dataset" {
  dataset_id = "bank_dataset"
  location   = "AU"
}

resource "google_bigquery_table" "retail_transactions" {
  dataset_id = google_bigquery_dataset.bank_dataset.dataset_id
  table_id   = "retail_transactions"
  schema     = file("schema.json")
  time_partitioning {
    type = "DAY"
  }
}

# --------------------------
# Cloud Composer Environment (Airflow)
# --------------------------
resource "google_composer_environment" "transaction_data_pipeline" {
  name   = "transaction-data-pipeline"
  region = "australia-southeast1"

  config {
    node_count = 3
    software_config {
      image_version = "composer-2.0.5-airflow-2.2.3"
      python_version = "3"
      env_variables = {
        BUCKET_NAME = google_storage_bucket.transaction_data_bucket.name
      }
    }
  }
}

# --------------------------
# IAM Permissions
# --------------------------
resource "google_project_iam_member" "gcs_permissions" {
  project = "your-gcp-project-id"
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_composer_environment.transaction_data_pipeline.config[0].node_config.service_account}"
}

resource "google_project_iam_member" "bigquery_permissions" {
  project = "your-gcp-project-id"
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_composer_environment.transaction_data_pipeline.config[0].node_config.service_account}"
}

# --------------------------
# SFTP Transfer Setup (Optional placeholder)
# --------------------------
# Add configuration for SFTP ingestion if required later
