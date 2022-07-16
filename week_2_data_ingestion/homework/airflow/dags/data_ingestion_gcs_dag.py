import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# URL prefix for yellow and fhv taxi data
URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'

# Arguments for yellow taxi dag
YELLOW_TAXI_URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_GCS_PATH_TEMPLATE = "raw/yellow_tripdata/{{ execution_date.strftime(\'%Y\') }}/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

# Arguments for fhv taxi dag
FHV_URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_GCS_PATH_TEMPLATE = "raw/fhv_tripdata/{{ execution_date.strftime(\'%Y\') }}/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

# Arguments for zone table
ZONES_URL_TEMPLATE = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.parquet'
ZONES_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + "/taxi_zone_lookup.parquet"
ZONES_GCS_PATH_TEMPLATE = "raw/taxi_zone/taxi_zone_lookup.parquet"

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
def download_parquetize_upload_dag(
    dag,
    url_template,
    local_parquet_path_template,
    gcs_path_template
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            # -f flag causes url to fail if the url does not exist, we don't want a green box if something fails
            bash_command=f"curl -sSLf {url_template} > {local_parquet_path_template}"
        )

        # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_parquet_path_template,
            },
        )

        # Remove file for local folder to reduce storage
        rm_task = BashOperator(
            task_id = "rm_task",
            bash_command = f"rm {local_parquet_path_template}"
        )

        download_dataset_task >> local_to_gcs_task >> rm_task

# Run dag for yellow taxi
yellow_taxi_data_dag = DAG(
    dag_id="yellow_taxi_data",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020,12,31),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_parquetize_upload_dag(
    dag=yellow_taxi_data_dag,
    url_template=YELLOW_TAXI_URL_TEMPLATE,
    local_parquet_path_template=YELLOW_TAXI_PARQUET_FILE_TEMPLATE,
    gcs_path_template=YELLOW_TAXI_GCS_PATH_TEMPLATE
)

# Run dag for fhv taxi
fhv_taxi_data_dag = DAG(
    dag_id="fhv_taxi_data",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019,12,31),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_parquetize_upload_dag(
    dag=fhv_taxi_data_dag,
    url_template=FHV_URL_TEMPLATE,
    local_parquet_path_template=FHV_PARQUET_FILE_TEMPLATE,
    gcs_path_template=FHV_GCS_PATH_TEMPLATE
)

# Run dag for zones file
zones_data_dag = DAG(
    dag_id="zones_data",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_parquetize_upload_dag(
    dag=zones_data_dag,
    url_template=ZONES_URL_TEMPLATE,
    local_parquet_path_template=ZONES_PARQUET_FILE_TEMPLATE,
    gcs_path_template=ZONES_GCS_PATH_TEMPLATE
)