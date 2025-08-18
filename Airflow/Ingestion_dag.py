import sys
sys.path.insert(0, '/home/vboxuser/airflow/scripts')  # MUST come first

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from data_ingestion import fetch_stock_data 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='mongo_data_ingestion',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    start = PythonOperator(
        task_id='upload_to_mongodb',
        python_callable=fetched_stock_data
    )

    start
