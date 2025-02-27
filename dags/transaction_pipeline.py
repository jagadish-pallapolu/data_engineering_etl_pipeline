# Import necessary libraries
from airflow import DAG
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage, bigquery
import pandas as pd
from datetime import datetime, timedelta

# Default arguments for Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'modern_data_stack_pipeline',
    default_args=default_args,
    schedule_interval='@daily',  # Runs daily for transaction data
    catchup=False
)

# Step 1: Ingest Retail Transaction Data from On-Prem to GCS
upload_transactions = LocalFilesystemToGCSOperator(
    task_id='upload_transactions_to_gcs',
    src='/path/to/transactions/*.csv',  # Assuming CSV files are generated daily
    dst='raw/transactions/',
    bucket='your-gcs-bucket',
    gcp_conn_id='google_cloud_default',
    dag=dag
)

# Step 2: Ingest Macroeconomic Data from SFTP to GCS
upload_macro_data = SFTPToGCSOperator(
    task_id='upload_macro_data_to_gcs',
    source_path='/sftp/path/macro_data.csv',
    destination_bucket='your-gcs-bucket',
    destination_path='raw/macro/',
    sftp_conn_id='sftp_default',
    gcp_conn_id='google_cloud_default',
    dag=dag
)

# Step 3: Load Data into BigQuery
load_transactions_to_bq = BigQueryInsertJobOperator(
    task_id='load_transactions_to_bq',
    sql="""
    CREATE OR REPLACE TABLE `your_project.dataset.transactions`
    AS SELECT * FROM `your_project.dataset.raw_transactions`
    """,
    use_legacy_sql=False,
    gcp_conn_id='google_cloud_default',
    dag=dag
)

load_macro_to_bq = BigQueryInsertJobOperator(
    task_id='load_macro_to_bq',
    sql="""
    CREATE OR REPLACE TABLE `your_project.dataset.macro`
    AS SELECT * FROM `your_project.dataset.raw_macro`
    """,
    use_legacy_sql=False,
    gcp_conn_id='google_cloud_default',
    dag=dag
)

# Step 4: Validate Data Before Transformation
def validate_data():
    client = bigquery.Client()
    query = """
    SELECT COUNT(*) as row_count FROM `your_project.dataset.transactions`
    WHERE transaction_date IS NULL OR amount IS NULL;
    """
    results = client.query(query).result()
    for row in results:
        if row.row_count > 0:
            raise ValueError("Data validation failed: NULL values detected in transactions table")

validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag
)

# Step 5: Transform Data in BigQuery using dbt or SQL
transform_data = BigQueryInsertJobOperator(
    task_id='transform_data',
    sql="""
    CREATE OR REPLACE TABLE `your_project.dataset.enriched_transactions` AS
    SELECT t.*, m.cash_rate, m.unemployment_rate
    FROM `your_project.dataset.transactions` t
    LEFT JOIN `your_project.dataset.macro` m
    ON t.transaction_date = m.date;
    """,
    use_legacy_sql=False,
    gcp_conn_id='google_cloud_default',
    dag=dag
)

# Step 6: Define Task Dependencies
upload_transactions >> load_transactions_to_bq >> validate_data_task >> transform_data
upload_macro_data >> load_macro_to_bq >> transform_data
