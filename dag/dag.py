from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.refreshing import job  # Import job from refreshing.py

default_args = {
    'owner': 'Alejandro',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email': ['alegior7@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sf_police_incidents_etl', 
    default_args=default_args, 
    description='ETL job for SF Police Incidents data',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='etl_job',
    python_callable=job,
    dag=dag,
)

t1
