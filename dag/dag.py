from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.ETL import (create_redshift_engine, create_table, fetch_data_from_api,
                               transform_data, load_data_to_redshift, check_threshold_and_alert,
                               close_connection)

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

# Tareas
t1 = PythonOperator(
    task_id='create_redshift_engine',
    python_callable=create_redshift_engine,
    dag=dag,
)

t2 = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    op_kwargs={'conn': '{{ ti.xcom_pull(task_ids="create_redshift_engine") }}'},
    dag=dag,
)

t3 = PythonOperator(
    task_id='fetch_data_from_api',
    python_callable=fetch_data_from_api,
    dag=dag,
)

t4 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={'df': '{{ ti.xcom_pull(task_ids="fetch_data_from_api") }}'},
    dag=dag,
)

t5 = PythonOperator(
    task_id='load_data_to_redshift',
    python_callable=load_data_to_redshift,
    op_kwargs={
        'df': '{{ ti.xcom_pull(task_ids="transform_data") }}',
        'engine': '{{ ti.xcom_pull(task_ids="create_redshift_engine") }}'
    },
    dag=dag,
)

t6 = PythonOperator(
    task_id='check_threshold_and_alert',
    python_callable=check_threshold_and_alert,
    op_kwargs={'df': '{{ ti.xcom_pull(task_ids="transform_data") }}'},
    dag=dag,
)

t7 = PythonOperator(
    task_id='close_connection',
    python_callable=close_connection,
    op_kwargs={'conn': '{{ ti.xcom_pull(task_ids="create_redshift_engine") }}'},
    trigger_rule='all_done',  # Se ejecuta independientemente del éxito de las tareas anteriores
    dag=dag,
)

# Definir el orden de las tareas
t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7