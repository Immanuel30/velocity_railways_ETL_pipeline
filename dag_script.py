from datetime import datetime, timedelta
from airflow import DAG
from airflow.operator.python_operator import pythonOperator
from extract import run_extract
from transform import run_transform
from azure_load import run_azure_load
from postgres_load import run_postgres_load

default_args= {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 21),
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

extraction = pythonOperator(
    task_id= 'extraction_layer',
    python_callable=run_extract,
    dag=dag,
)

extraction = pythonOperator(
    task_id= 'transformation_layer',
    python_callable=run_transform,
    dag=dag,
)

extraction = pythonOperator(
    task_id= 'azure_load_layer'
    python_callable=run_azure_load,
    dag=dag,
)

extraction = pythonOperator(
    task_id= 'postgres_load_layer'
    python_callable=run_postgres_load,
    dag=dag,
)

extract >> transform >> [azure_load, postges_load]