import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
# GCS to GCS transfer service
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

# Project and Bucket ID for GCP
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

DATASET = "tripdata"
YELLOW_COLOUR_RANGE = {'yellow': 'tpep_pickup_datetime'}#, 'green': 'lpep_pickup_datetime'}
GREEN_COLOUR_RANGE = {'green': 'lpep_pickup_datetime'}
INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
# with DAG(
#     dag_id="gcs_2_bq_dag_yellow",
#     schedule_interval="@daily",
#     default_args=default_args,
#     catchup=False,
#     max_active_runs=1,
#     tags=['dtc-de'],
# ) as dag:

#     for colour, ds_col in YELLOW_COLOUR_RANGE.items():
#         # GCS to GCS task
#         move_files_gcs_task = GCSToGCSOperator(
#             task_id=f'move_{colour}_{DATASET}_files_task',
#             source_bucket=BUCKET,
#             source_object=f'{INPUT_PART}/{colour}_{DATASET}*.{INPUT_FILETYPE}',
#             destination_bucket=BUCKET,
#             destination_object=f'{colour}/{colour}_{DATASET}',
#             move_object=True
#         )

#         # Creating BigQuery External Table
#         bigquery_external_table_task = BigQueryCreateExternalTableOperator(
#             task_id=f"bq_{colour}_{DATASET}_external_table_task",
#             table_resource={
#                 "tableReference": {
#                     "projectId": PROJECT_ID,
#                     "datasetId": BIGQUERY_DATASET,
#                     "tableId": f"{colour}_{DATASET}_external_table",
#                 },
#                 "externalDataConfiguration": {
#                     "autodetect": "True",
#                     "sourceFormat": f"{INPUT_FILETYPE.upper()}",
#                     "sourceUris": [f"gs://{BUCKET}/{colour}/*"],
#                 },
#             },
#         )

#         CREATE_BQ_TBL_QUERY = (
#             f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{colour}_{DATASET} \
#             PARTITION BY DATE({ds_col}) \
#             AS \
#             SELECT * REPLACE(NULL AS airport_fee) FROM {BIGQUERY_DATASET}.{colour}_{DATASET}_external_table;"
#         )

#         # Create a partitioned table from external table, performs within BQ query
#         bq_create_partitioned_table_job = BigQueryInsertJobOperator(
#             task_id=f"bq_create_{colour}_{DATASET}_partitioned_table_task",
#             configuration={
#                 "query": {
#                     "query": CREATE_BQ_TBL_QUERY,
#                     "useLegacySql": False,
#                 }
#             }
#         )

#         move_files_gcs_task >> bigquery_external_table_task >> bq_create_partitioned_table_job

# NOTE: DAG declaration - using a Context Manager (an implicit way)
# with DAG(
#     dag_id="gcs_2_bq_dag_green",
#     schedule_interval="@daily",
#     default_args=default_args,
#     catchup=False,
#     max_active_runs=1,
#     tags=['dtc-de'],
# ) as dag:

#     for colour, ds_col in GREEN_COLOUR_RANGE.items():
#         # GCS to GCS task
#         move_files_gcs_task = GCSToGCSOperator(
#             task_id=f'move_{colour}_{DATASET}_files_task',
#             source_bucket=BUCKET,
#             source_object=f'{INPUT_PART}/{colour}_{DATASET}*.{INPUT_FILETYPE}',
#             destination_bucket=BUCKET,
#             destination_object=f'{colour}/{colour}_{DATASET}',
#             move_object=True
#         )

#         # Creating BigQuery External Table
#         bigquery_external_table_task = BigQueryCreateExternalTableOperator(
#             task_id=f"bq_{colour}_{DATASET}_external_table_task",
#             table_resource={
#                 "tableReference": {
#                     "projectId": PROJECT_ID,
#                     "datasetId": BIGQUERY_DATASET,
#                     "tableId": f"{colour}_{DATASET}_external_table",
#                 },
#                 "externalDataConfiguration": {
#                     "autodetect": "True",
#                     "sourceFormat": f"{INPUT_FILETYPE.upper()}",
#                     "sourceUris": [f"gs://{BUCKET}/{colour}/*"],
#                 },
#             },
#         )

#         CREATE_BQ_TBL_QUERY = (
#             f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{colour}_{DATASET} \
#             PARTITION BY DATE({ds_col}) \
#             AS \
#             SELECT * REPLACE(NULL as ehail_fee) FROM {BIGQUERY_DATASET}.{colour}_{DATASET}_external_table;"
#         )

#         # Create a partitioned table from external table, performs within BQ query
#         bq_create_partitioned_table_job = BigQueryInsertJobOperator(
#             task_id=f"bq_create_{colour}_{DATASET}_partitioned_table_task",
#             configuration={
#                 "query": {
#                     "query": CREATE_BQ_TBL_QUERY,
#                     "useLegacySql": False,
#                 }
#             }
#         )

#         move_files_gcs_task >> bigquery_external_table_task >> bq_create_partitioned_table_job
        
#########################################################################################################################################
        
# NOTE: DAG declaration - using a Context Manager (an implicit way)
def move_table_parition_dag(
    dag,
    colour_range
):
    with dag:
        for colour, ds_col in color_range.items():
            # GCS to GCS task
            move_files_gcs_task = GCSToGCSOperator(
                task_id=f'move_{colour}_{DATASET}_files_task',
                source_bucket=BUCKET,
                source_object=f'{INPUT_PART}/{colour}_{DATASET}*.{INPUT_FILETYPE}',
                destination_bucket=BUCKET,
                destination_object=f'{colour}/{colour}_{DATASET}',
                move_object=True
            )

            # Creating BigQuery External Table
            bigquery_external_table_task = BigQueryCreateExternalTableOperator(
                task_id=f"bq_{colour}_{DATASET}_external_table_task",
                table_resource={
                    "tableReference": {
                        "projectId": PROJECT_ID,
                        "datasetId": BIGQUERY_DATASET,
                        "tableId": f"{colour}_{DATASET}_external_table",
                    },
                    "externalDataConfiguration": {
                        "autodetect": "True",
                        "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                        "sourceUris": [f"gs://{BUCKET}/{colour}/*"],
                    },
                },
            )

            if color == 'yellow':
                CREATE_BQ_TBL_QUERY = (
                    f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{colour}_{DATASET} \
                    PARTITION BY DATE({ds_col}) \
                    AS \
                    SELECT * REPLACE(NULL AS airport_fee) FROM {BIGQUERY_DATASET}.{colour}_{DATASET}_external_table;"
                )
            elif color == 'green':
                CREATE_BQ_TBL_QUERY = (
                    f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{colour}_{DATASET} \
                    PARTITION BY DATE({ds_col}) \
                    AS \
                    SELECT * REPLACE(NULL as ehail_fee) FROM {BIGQUERY_DATASET}.{colour}_{DATASET}_external_table;"
                )

            # Create a partitioned table from external table, performs within BQ query
            bq_create_partitioned_table_job = BigQueryInsertJobOperator(
                task_id=f"bq_create_{colour}_{DATASET}_partitioned_table_task",
                configuration={
                    "query": {
                        "query": CREATE_BQ_TBL_QUERY,
                        "useLegacySql": False,
                    }
                }
            )

            move_files_gcs_task >> bigquery_external_table_task >> bq_create_partitioned_table_job
        
# Run dag for yellow taxi
yellow_taxi_data_dag = DAG(
    dag_id="gcs_2_bq_dag_yellow",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
)

move_table_parition_dag(
    dag = yellow_taxi_data_dag,
    colour_range = YELLOW_COLOUR_RANGE
)

# Run dag for green taxi
green_taxi_data_dag = DAG(
    dag_id="gcs_2_bq_dag_green",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
)

move_table_parition_dag(
    dag = green_taxi_data_dag,
    colour_range = GREEN_COLOUR_RANGE
)
