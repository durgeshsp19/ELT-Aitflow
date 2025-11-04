"""DAG to create filtered views for country-specific health data analysis.

This DAG implements a robust ELT pipeline that:
1. Validates source data in GCS
2. Performs data quality checks
3. Loads data to BigQuery staging
4. Creates country-specific transformed tables
5. Generates analytical views with monitoring
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.exceptions import AirflowException
from google.cloud import bigquery

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def log_task_failure(context):
    """Log task failures with detailed information."""
    task_instance = context['task_instance']
    logger.error(
        f"Task Failed: {task_instance.task_id}\n"
        f"Execution Date: {context.get('execution_date')}\n"
        f"Error: {context.get('exception')}"
    )

def alert_on_failure(context):
    """Send alerts on task failures."""
    task_instance = context['task_instance']
    logger.error(f"Task {task_instance.task_id} failed. Sending alert...")
    # Add your alerting logic here (e.g., email, Slack, etc.)

# Enhanced DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'on_failure_callback': log_task_failure,
    'execution_timeout': timedelta(hours=1),
}

# Project configuration
project_id = 'tt-dev-02'
dataset_id = 'staging_dataset'
transform_dataset_id = 'transform_dataset'
reporting_dataset_id = 'reporting_dataset'
source_table = f'{project_id}.{dataset_id}.global_data'
countries = ['USA', 'India', 'Germany', 'Japan', 'France', 'Canada', 'Italy']
# DAG documentation
dag_doc_md = """
# Global Health Data ELT Pipeline

## Overview
This DAG implements a robust Extract-Load-Transform pipeline for global health data analysis.

## Flow
1. **Validation**: Check source file existence and format
2. **Loading**: Import data to BigQuery staging
3. **Transformation**: Create country-specific tables
4. **Analytics**: Generate filtered views with metrics

## Features
- Comprehensive data validation
- Error handling and retries
- Performance optimization
- Monitoring and alerting

## Dependencies
- GCS Bucket: `{bucket}`
- Source File: `{source}`
- Datasets:
  - Staging: `{staging}`
  - Transform: `{transform}`
  - Reporting: `{reporting}`
""".format(
    bucket=config['bucket_name'],
    source=config['source_file'],
    staging=dataset_id,
    transform=transform_dataset_id,
    reporting=reporting_dataset_id
)

# DAG definition with enhanced configuration
with DAG(
    dag_id='load_and_transform_view',
    default_args=default_args,
    description='Load and transform global health data with validation and monitoring',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigquery', 'gcs', 'csv', 'health-data', 'etl'],
    doc_md=dag_doc_md,
    max_active_runs=1,
    concurrency=3,  # Limit concurrent tasks
    user_defined_filters={'format_date': lambda d: d.strftime('%Y-%m-%d')},
    render_template_as_native_obj=True
) as dag:

    # Task Group 1: Data Validation and Loading
    
    # Task 1.1: Check if the source file exists in GCS
    check_file_exists = GCSObjectExistenceSensor(
        task_id='check_file_exists',
        bucket=config['bucket_name'],
        object=config['source_file'],
        timeout=600,  # 10 minutes
        poke_interval=30,
        mode='reschedule',  # More efficient than 'poke' for longer intervals
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=alert_on_failure,
    )

    # Task 1.2: Validate source file format
    def validate_source_file(**context):
        """Validate source file format and basic structure."""
        from google.cloud import storage
        
        client = storage.Client()
        bucket = client.bucket(config['bucket_name'])
        blob = bucket.blob(config['source_file'])
        
        # Download first few lines to validate format
        content = blob.download_as_text(start=0, end=1024)
        lines = content.split('\n')
        
        # Validate header
        if len(lines) < 2:
            raise AirflowException("File is empty or too short")
            
        headers = lines[0].strip().split(',')
        required_columns = config['table_config']['required_columns']
        
        missing_columns = set(required_columns) - set(headers)
        if missing_columns:
            raise AirflowException(f"Missing required columns: {missing_columns}")
        
        logger.info("Source file validation successful")
        
    validate_file = PythonOperator(
        task_id='validate_source_file',
        python_callable=validate_source_file,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=2),
        on_failure_callback=alert_on_failure,
    )

    # Task 1.3: Load CSV to BigQuery with enhanced error handling
    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket=config['bucket_name'],
        source_objects=[config['source_file']],
        destination_project_dataset_table=source_table,
        source_format='CSV',
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        field_delimiter=',',
        autodetect=True,
        on_failure_callback=alert_on_failure,
        labels={
            'env': 'production',
            'data_type': 'health_metrics',
            'process': 'etl_pipeline'
        }
    )

    # Task 1.4: Validate loaded data
    validate_loaded_data = PythonOperator(
        task_id='validate_loaded_data',
        python_callable=validate_bigquery_data,
        op_kwargs={'table_ref': source_table, 'min_rows': 100},
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=2),
        on_failure_callback=alert_on_failure,
    )

    # Task 1.5: Monitor loading process
    monitor_load = PythonOperator(
        task_id='monitor_load_process',
        python_callable=monitor_task_progress,
        provide_context=True,
        trigger_rule='all_done',
    )

    # Task Group 2: Country-specific Processing
    
    create_table_tasks = []
    create_view_tasks = []
    validation_tasks = []
    
    for country in countries:
        # Task 2.1: Create country-specific tables with partitioning and clustering
        create_table_task = BigQueryInsertJobOperator(
            task_id=f'create_table_{country.lower()}',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE TABLE `{project_id}.{transform_dataset_id}.{country.lower()}_table`
                        PARTITION BY DATE_TRUNC(PARSE_DATE('%Y', CAST(Year AS STRING)), YEAR)
                        CLUSTER BY Disease_Category, Disease_Name
                        OPTIONS(
                            description="Health data for {country}",
                            labels=[("country", "{country.lower()}"), ("data_type", "health_metrics")]
                        )
                        AS
                        SELECT 
                            *,
                            -- Add derived columns for analysis
                            PARSE_DATE('%Y', CAST(Year AS STRING)) AS date_year,
                            CASE 
                                WHEN Prevalence_Rate > AVG(Prevalence_Rate) OVER() THEN 'High'
                                ELSE 'Low'
                            END AS prevalence_category,
                            ROUND(Prevalence_Rate / NULLIF(Incidence_Rate, 0), 2) AS prevalence_incidence_ratio
                        FROM `{source_table}`
                        WHERE country = '{country}'
                    """,
                    "useLegacySql": False,
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
            retries=2,
            retry_delay=timedelta(minutes=5),
            on_failure_callback=alert_on_failure,
        )

        # Task 2.2: Validate country-specific data
        validate_country_task = PythonOperator(
            task_id=f'validate_{country.lower()}_data',
            python_callable=validate_bigquery_data,
            op_kwargs={
                'table_ref': f'{project_id}.{transform_dataset_id}.{country.lower()}_table',
                'min_rows': 1
            },
            provide_context=True,
            retries=2,
            on_failure_callback=alert_on_failure,
        )

        # Task 2.3: Create optimized analytical view
        create_view_task = BigQueryInsertJobOperator(
            task_id=f'create_view_{country.lower()}_view',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE VIEW `{project_id}.{reporting_dataset_id}.{country.lower()}_view` AS
                        WITH yearly_metrics AS (
                            SELECT 
                                date_year,
                                Disease_Name,
                                Disease_Category,
                                Prevalence_Rate,
                                Incidence_Rate,
                                prevalence_category,
                                prevalence_incidence_ratio,
                                -- Add year-over-year calculations
                                LAG(Prevalence_Rate) OVER(
                                    PARTITION BY Disease_Name 
                                    ORDER BY date_year
                                ) AS prev_year_prevalence,
                                LAG(Incidence_Rate) OVER(
                                    PARTITION BY Disease_Name 
                                    ORDER BY date_year
                                ) AS prev_year_incidence
                            FROM `{project_id}.{transform_dataset_id}.{country.lower()}_table`
                            WHERE Availability_of_Vaccines_Treatment = False
                        )
                        SELECT 
                            *,
                            ROUND((Prevalence_Rate - prev_year_prevalence) / NULLIF(prev_year_prevalence, 0) * 100, 2) 
                                AS prevalence_yoy_change,
                            ROUND((Incidence_Rate - prev_year_incidence) / NULLIF(prev_year_incidence, 0) * 100, 2) 
                                AS incidence_yoy_change
                        FROM yearly_metrics
                        ORDER BY date_year DESC, Disease_Category, Disease_Name
                    """,
                    "useLegacySql": False,
                }
            },
            retries=2,
            retry_delay=timedelta(minutes=5),
            on_failure_callback=alert_on_failure,
        )

        # Store tasks in lists
        create_table_tasks.append(create_table_task)
        validation_tasks.append(validate_country_task)
        create_view_tasks.append(create_view_task)
        
        # Set task dependencies
        create_table_task.set_upstream(validate_loaded_data)
        validate_country_task.set_upstream(create_table_task)
        create_view_task.set_upstream(validate_country_task)

    # Task Group 3: Pipeline Completion and Monitoring

    def monitor_pipeline_completion(**context):
        """Monitor overall pipeline completion and generate metrics."""
        try:
            # Get execution metrics
            dag_run = context['dag_run']
            execution_date = dag_run.execution_date
            end_date = datetime.now()
            duration = (end_date - execution_date).total_seconds()

            # Collect task statistics
            task_instances = dag_run.get_task_instances()
            total_tasks = len(task_instances)
            successful_tasks = len([t for t in task_instances if t.state == 'success'])
            failed_tasks = len([t for t in task_instances if t.state == 'failed'])
            
            # Log pipeline metrics
            logger.info(
                f"\nPipeline Execution Summary:\n"
                f"Total Duration: {duration:.2f} seconds\n"
                f"Start Time: {execution_date}\n"
                f"End Time: {end_date}\n"
                f"Total Tasks: {total_tasks}\n"
                f"Successful Tasks: {successful_tasks}\n"
                f"Failed Tasks: {failed_tasks}\n"
                f"Success Rate: {(successful_tasks/total_tasks)*100:.2f}%"
            )

            # Check for data quality
            for country in countries:
                table_ref = f"{project_id}.{transform_dataset_id}.{country.lower()}_table"
                view_ref = f"{project_id}.{reporting_dataset_id}.{country.lower()}_view"
                
                client = bigquery.Client()
                table = client.get_table(table_ref)
                
                logger.info(
                    f"\nCountry: {country}\n"
                    f"Records Processed: {table.num_rows}\n"
                    f"Table Size: {table.num_bytes/1024/1024:.2f} MB"
                )

        except Exception as e:
            logger.error(f"Error in pipeline monitoring: {str(e)}")
            raise AirflowException(f"Pipeline monitoring failed: {str(e)}")

    # Final monitoring task
    pipeline_monitoring = PythonOperator(
        task_id='monitor_pipeline_completion',
        python_callable=monitor_pipeline_completion,
        provide_context=True,
        trigger_rule='all_done',
        on_failure_callback=alert_on_failure,
    )

    # Success task
    success_task = DummyOperator(
        task_id='pipeline_success',
        trigger_rule='all_success',
        on_success_callback=lambda ctx: logger.info("Pipeline completed successfully!")
    )

    # Define task dependencies
    check_file_exists >> validate_file >> load_csv_to_bigquery >> validate_loaded_data >> monitor_load

    # Country-specific task dependencies
    for create_table_task, validate_task, create_view_task in zip(
        create_table_tasks, validation_tasks, create_view_tasks
    ):
        monitor_load >> create_table_task >> validate_task >> create_view_task >> pipeline_monitoring

    # Final pipeline status
    pipeline_monitoring >> success_task
