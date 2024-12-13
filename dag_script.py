from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from extract import main
from transform import transformation
from azure_load import azure_load
from postgres_load import postgres_load
import pandas as pd

default_args= {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2024, 11, 6),
    'email': 'imarr_emarr@yahoo.com',
    'email_on_failure': True,
    'email_on_retry' : True,
    'retries': 1,
    'retries_delay': timedelta(minutes=	1)
}

dag = DAG(
    'velocity_railway_pipeline',
    default_args = default_args,
    description= 'This represents velocity railway data management pipeline'
)

extraction = PythonOperator(
    task_id= 'extraction_layer',
    python_callable=main,
    provide_context=True,
    dag=dag
)

transform = PythonOperator(
    task_id= 'transformation_layer',
    python_callable=transformation,
    dag=dag
)

az_load = PythonOperator(
    task_id= 'azure_load_layer',
    python_callable=azure_load,
    dag=dag
)

pg_load = PythonOperator(
    task_id= 'postgres_load_layer',
    python_callable=postgres_load,
    dag=dag
)

extraction >> transform >> [az_load, pg_load]