from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from scripts.data_utils.elt import extract_telegram_channels, load_data_mongo, transform

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='dbt_automation',
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


# extract = PythonOperator(
#     task_id='extract',
#     provide_context=True,
#     python_callable=extract_telegram_channels,
#     dag=dag)

# load = PythonOperator(
#     task_id='load_to_mongodb',
#     provide_context=True,
#     python_callable=load_data_mongo,
#     dag=dag)

# transform = PythonOperator(
#     task_id='load_to_mongodb',
#     provide_context=True,
#     python_callable=load_data_mongo,
#     dag=dag)

# extract >> load >> transform

# airflow scheduler