"""Unit tests for the ELT pipeline DAGs."""

import os
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from airflow.models import DagBag
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

class TestELTPipeline(unittest.TestCase):
    """Test cases for ELT pipeline."""

    def setUp(self):
        """Set up test fixtures."""
        self.dag_path = os.path.dirname(os.path.dirname(__file__))
        self.dagbag = DagBag(dag_folder=self.dag_path, include_examples=False)
        self.test_dag_id = "test_dag"
        self.default_args = {
            'owner': 'airflow',
            'start_date': datetime(2024, 1, 1),
            'depends_on_past': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
        }

    def test_dag_loading(self):
        """Test that all DAGs can be imported without errors."""
        self.assertFalse(
            len(self.dagbag.import_errors),
            f"DAG import errors: {self.dagbag.import_errors}"
        )

    def test_dag_structure(self):
        """Test the structure of all DAGs."""
        for dag_id, dag in self.dagbag.dags.items():
            # Check DAG attributes
            self.assertIsNotNone(dag.description)
            self.assertIsNotNone(dag.default_args.get('owner'))
            self.assertIsInstance(dag.start_date, datetime)
            
            # Check task dependencies
            for task in dag.tasks:
                self.assertTrue(
                    len(task.upstream_list) > 0 or len(task.downstream_list) > 0,
                    f"Task {task.task_id} in DAG {dag_id} has no dependencies"
                )

    @patch('airflow.providers.google.cloud.sensors.gcs.GCSObjectExistenceSensor')
    def test_file_sensor(self, mock_sensor):
        """Test GCS file sensor configuration."""
        mock_sensor.return_value = MagicMock()
        mock_sensor.return_value.execute.return_value = True
        
        # Test sensor configuration
        sensor = GCSObjectExistenceSensor(
            task_id='test_sensor',
            bucket='bkt-src-global-data',
            object='global_health_data.csv',
            timeout=300,
            mode='poke'
        )
        
        self.assertEqual(sensor.bucket, 'bkt-src-global-data')
        self.assertEqual(sensor.object, 'global_health_data.csv')
        self.assertEqual(sensor.timeout, 300)

    @patch('airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSToBigQueryOperator')
    def test_gcs_to_bq_operator(self, mock_operator):
        """Test GCS to BigQuery operator configuration."""
        mock_operator.return_value = MagicMock()
        mock_operator.return_value.execute.return_value = True
        
        # Test operator configuration
        operator = GCSToBigQueryOperator(
            task_id='test_load',
            bucket='bkt-src-global-data',
            source_objects=['global_health_data.csv'],
            destination_project_dataset_table='project.dataset.table',
            source_format='CSV',
            autodetect=True
        )
        
        self.assertEqual(operator.bucket, 'bkt-src-global-data')
        self.assertEqual(operator.source_objects, ['global_health_data.csv'])
        self.assertEqual(operator.source_format, 'CSV')
        self.assertTrue(operator.autodetect)

    def test_country_transformations(self):
        """Test country-specific transformation configurations."""
        dag = self.dagbag.get_dag('load_and_transform')
        self.assertIsNotNone(dag)
        
        # Check for country-specific tasks
        countries = ['USA', 'India', 'Germany', 'Japan', 'France', 'Canada', 'Italy']
        for country in countries:
            task_id = f'create_table_{country.lower()}'
            task = dag.get_task(task_id)
            self.assertIsNotNone(task)
            self.assertIsInstance(task, BigQueryInsertJobOperator)

if __name__ == '__main__':
    unittest.main()