from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sys

# Add pipeline to Python path for imports
sys.path.append('/opt/airflow/pipeline')

from extract import Extract
from transform import Transform
from load import Load

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 1, 1),
}

def run_extract(**kwargs):
    date = kwargs['ds']
    extract = Extract(
        pipeline_env_file='/opt/airflow/pipeline/.env',       # if you have it
        docker_env_file='/opt/airflow/.env'
    )
    extract.extract_and_process_data(date)

def run_transform(**kwargs):
    date = kwargs['ds']
    transform = Transform(
        pipeline_env_file='/opt/airflow/pipeline/.env',
        docker_env_file='/opt/airflow/.env'
    )
    stations_df, readings_df = transform.transform_data(date)

    # Save intermediate data for load step
    stations_df.to_pickle(f'/tmp/stations_{date}.pkl')
    readings_df.to_pickle(f'/tmp/readings_{date}.pkl')

def run_load(**kwargs):
    date = kwargs['ds']
    load = Load(
        pipeline_env_file='/opt/airflow/pipeline/.env',
        docker_env_file='/opt/airflow/.env'
    )
    stations_df = pd.read_pickle(f'/tmp/stations_{date}.pkl')
    readings_df = pd.read_pickle(f'/tmp/readings_{date}.pkl')

    load.process_and_load_data(stations_df, readings_df)

with DAG(
    dag_id='etl_pipeline_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'rainfall'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=run_extract,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=run_transform,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=run_load,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task
