from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG('daily_script_run_spark', 
         start_date=datetime(2024, 9, 10), 
         default_args=default_args,
         description='A DAG to run a Spark job', 
         tags=['vk_de_task'],
         schedule_interval='0 7 * * *',
         catchup=False) as dag:
        
        run_spark_job = SparkSubmitOperator(
            task_id='run_spark_job',
            application='/opt/airflow/dags/spark_script.py',
            name='weekly_aggregation',
            conn_id='spark_default',
            application_args=[
                '{{ ds }}',
                '--input_dir', '/opt/airflow/dags/data/input',
                '--output_tmp_dir', '/opt/airflow/dags/data/output_tmp',
                '--output_dir', '/opt/airflow/dags/data/output'
            ],
            conf={'spark.master': 'local[*]'},
            executor_cores=2,
            executor_memory='2g',
            driver_memory='1g',
            verbose=True
        )

        run_spark_job