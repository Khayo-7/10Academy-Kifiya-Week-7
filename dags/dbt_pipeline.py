from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_automation',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
)

dbt_run = BashOperator(
    task_id='run_dbt_models',
    bash_command='cd ./medical_dwh && dbt run',
    dag=dag,
)

dbt_test = BashOperator(
    task_id='test_dbt_models',
    bash_command='cd ./medical_dwh && dbt test',
    dag=dag,
)

dbt_run >> dbt_test  # Run models first, then test

# airflow scheduler