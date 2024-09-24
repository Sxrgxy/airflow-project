from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG('daily_script_run', 
         start_date=datetime(2024,9,10), 
         default_args=default_args,
         description='A DAG to run a Python script', 
         tags=['vk_de_task'],
         schedule_interval='0 7 * * *',
         catchup=False) as dag:
        
        run_script_task = BashOperator(
            task_id='run_script',
            bash_command='python3 /opt/airflow/dags/script.py {{ ds }}'
        )
        
        run_script_task