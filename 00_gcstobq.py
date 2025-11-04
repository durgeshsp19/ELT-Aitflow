"""Simple DAG to load CSV data from Google Cloud Storage to BigQuery."""

from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='load_gcs_to_bq',
    default_args=default_args,
    description='Load a CSV file from GCS to BigQuery',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigquery', 'gcs', 'csv'],
) as dag:

    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket='bkt-src-global-data_1',
        source_objects=['global_health_data.csv'],
        destination_project_dataset_table='user-omqnclorqszb.staging_dataset.global_data',
        source_format='CSV', 
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        field_delimiter=',',
        autodetect=True,
    )
    load_csv_to_bigquery
