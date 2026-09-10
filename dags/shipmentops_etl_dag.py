from datetime import datetime, timedelta
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# Add plugins to path
sys.path.insert(0, '/opt/airflow/plugins')

from shipmentops.extract import extract_all_sources, generate_synthetic_data
from shipmentops.validate import validate_all_sources
from shipmentops.transform import transform_all_sources
from shipmentops.load import load_all
from shipmentops.quality_checks import run_quality_checks


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}


def push_xcom(**context):
    """Helper to push xcom values from extract task."""
    result = extract_all_sources(**context)
    context['ti'].xcom_push(key='extracted_files', value=result)
    return result


def validate_and_push(**context):
    """Helper to validate and push xcom."""
    result = validate_all_sources(**context)
    context['ti'].xcom_push(key='validated_files', value=result)
    return result


def transform_and_push(**context):
    """Helper to transform and push xcom."""
    result = transform_all_sources(**context)
    context['ti'].xcom_push(key='transformed_files', value=result)
    return result


# Define the DAG
with DAG(
    dag_id='shipmentops_etl',
    default_args=default_args,
    description='Batch ETL pipeline for ShipmentOps logistics data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'logistics', 'warehouse', 'star-schema'],
    doc_md=__doc__,
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    #synthetic data (for demo purposes)
    generate_data = PythonOperator(
        task_id='generate_synthetic_data',
        python_callable=generate_synthetic_data,
        op_kwargs={'num_days': 30},
        doc_md="Generate synthetic logistics data for testing the pipeline."
    )
    
    #extract from landing zone
    extract = PythonOperator(
        task_id='extract',
        python_callable=push_xcom,
        provide_context=True,
        doc_md="Read raw CSV files from landing folder and convert to Parquet."
    )
    
    #validate schema and nulls, quarantine bad records
    validate = PythonOperator(
        task_id='validate',
        python_callable=validate_and_push,
        provide_context=True,
        doc_md="Validate schema, check nulls, quarantine bad records with reason."
    )
    
    #transform - clean, dedupe, standardize
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_and_push,
        provide_context=True,
        doc_md="Clean data, deduplicate, standardize dates and currency."
    )
    
    #load into star schema warehouse
    load = PythonOperator(
        task_id='load',
        python_callable=load_all,
        provide_context=True,
        doc_md="Load data into PostgreSQL warehouse using star schema with incremental loading."
    )
    
    #run data quality checks
    quality_check = PythonOperator(
        task_id='quality_checks',
        python_callable=run_quality_checks,
        provide_context=True,
        doc_md="Run data quality checks on loaded data: completeness, referential integrity, value ranges."
    )
    
    # End marker
    end = EmptyOperator(task_id='end')
    
    # Define task dependencies
    start >> generate_data >> extract >> validate >> transform >> load >> quality_check >> end