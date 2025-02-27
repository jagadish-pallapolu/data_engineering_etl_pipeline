
import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator
)

BUCKET_NAME =""
RESOURCE_DATA_BUCKET=""
GCS_TMP = f"gs://{BUCKET_NAME}/temp/"
GCS_STAGING = f"gs://{BUCKET_NAME}/staging/"
GCS_OUTPUT = f"gs://{BUCKET_NAME}/output"
GCS_PYTHON_SCRIPT = f"gs://{RESOURCE_DATA_BUCKET}/dataflow/python/wordcount_debugging.py"
LOCATION = "australia-southeast1"

DATASET_NAME = ""
DATA_SAMPLE_GCS_OBJECT_NAME = ""
DATA_SAMPLE_GCS_BUCKET_NAME =""
INSERT_ROWS_QUERY =""

with DAG(
    dag_id="transaction_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
):
    # This task will use dataflow to convert csv to parquet by reading and writing to GCS bucket
    start_python_job = BeamRunPythonPipelineOperator(
        runner=BeamRunnerType.DataflowRunner,
        task_id="start_python_job",
        py_file=GCS_PYTHON_SCRIPT,
        py_options=[],
        pipeline_options={
            "output": GCS_OUTPUT,
        },
        py_requirements=["apache-beam[gcp]==2.59.0"],
        py_interpreter="python3",
        py_system_site_packages=False,
        dataflow_config={"location": LOCATION, "job_name": "start_python_job"},
    )

    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        destination_project_dataset_table=f"{DATASET_NAME}.external_table",
        bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        source_objects=[DATA_SAMPLE_GCS_OBJECT_NAME],
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )

    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION,
    )

    start_python_job >> create_external_table >> create_table >> insert_query_job
