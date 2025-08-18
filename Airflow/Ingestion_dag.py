from airflow import DAG
from airflow.operators.bash import BashOperator 
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='stock_data_ingestion',
    default_args=default_args,
    description='Fetch stock data and store in MongoDB',
    schedule_interval='@daily',
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello Airflow!"'
    )

    task1

    