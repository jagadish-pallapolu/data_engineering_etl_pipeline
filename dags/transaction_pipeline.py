# Import necessary libraries
from google.cloud import storage, bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import paramiko
import os

# --------------------------
# Configuration Variables
# --------------------------
BUCKET_NAME = 'bank-retail-transactions'
RAW_FOLDER = 'raw/'
PROCESSED_FOLDER = 'processed/'
UNIX_SERVER = 'unix.server.address'
USERNAME = 'your_username'
PRIVATE_KEY_PATH = '/path/to/private/key'
SFTP_PORT = 22
REMOTE_PATH = '/path/to/transactions/'
LOCAL_TEMP_PATH = '/tmp/'
DATASET_ID = 'bank_dataset'
TABLE_ID = 'retail_transactions'

# ------------------------------------------
# You can also import variables from airflow
# ------------------------------------------
# from airflow.models import Variable

# BUCKET_NAME = Variable.get("gcs_bucket_name")
# RAW_FOLDER = Variable.get("raw_folder")
# UNIX_SERVER = Variable.get("unix_server")
# BIGQUERY_DATASET = Variable.get("bigquery_dataset")
#
# print(f"Data will be stored in {BUCKET_NAME}/{RAW_FOLDER}")


# --------------------------
# Ingestion Function: Upload from UNIX to GCS
# --------------------------
def ingest_transaction_data():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(UNIX_SERVER, port=SFTP_PORT, username=USERNAME, key_filename=PRIVATE_KEY_PATH)
        sftp = ssh.open_sftp()
        files = sftp.listdir(REMOTE_PATH)
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        for file in files:
            local_file_path = os.path.join(LOCAL_TEMP_PATH, file)
            remote_file_path = os.path.join(REMOTE_PATH, file)
            sftp.get(remote_file_path, local_file_path)
            blob = bucket.blob(f'{RAW_FOLDER}{file}')
            blob.upload_from_filename(local_file_path)
            os.remove(local_file_path)

        sftp.close()
    finally:
        ssh.close()

# --------------------------
# Transformation Function: Load and Process in BigQuery
# --------------------------
def transform_data():
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )

    uri = f'gs://{BUCKET_NAME}/{RAW_FOLDER}*'
    load_job = client.load_table_from_uri(uri, f'{DATASET_ID}.{TABLE_ID}', job_config=job_config)
    load_job.result()  # Waits for the job to finish

# --------------------------
# Airflow DAG Definition
# --------------------------
def create_dag():
    default_args = {
        'owner': 'airflow',
        'start_date': days_ago(1),
        'retries': 1
    }

    dag = DAG(
        'transaction_data_pipeline',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False
    )

    ingestion_task = PythonOperator(
        task_id='ingest_transaction_data',
        python_callable=ingest_transaction_data,
        dag=dag
    )

    transformation_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        dag=dag
    )

    ingestion_task >> transformation_task
    return dag

# Instantiate DAG
dag = create_dag()
