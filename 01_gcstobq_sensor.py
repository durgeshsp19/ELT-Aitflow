"""DAG to check file existence in GCS before loading to BigQuery."""
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# DAG definition
with DAG(
    dag_id='check_load_csv_to_bigquery',
    default_args=default_args,
    description='Load a CSV file from GCS to BigQuery',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigquery', 'gcs', 'csv'],
) as dag:

    check_file_exists = GCSObjectExistenceSensor(
        task_id='check_file_exists',
        bucket='bkt-src-global-data',
        object='global_health_data.csv',
        timeout=300,
        poke_interval=30,
        mode='poke',
    )

    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket='bkt-src-global-data',
        source_objects=['global_health_data.csv'],
        destination_project_dataset_table='tt-dev-02.staging_dataset.global_data',
        source_format='CSV', 
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        field_delimiter=',',
        autodetect=True,
    )

    # Define task dependencies
    check_file_exists >> load_csv_to_bigquery