from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.abspath("/opt/airflow/dags/"))
from etls.grammy_etl import extract_grammy_data, transform_grammy_data
from etls.spotify_etl import extract_spotify_data, transform_spotify_data
from etls.merge_load_data import merge_data, save_to_postgres, upload_to_drive

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 4),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'etl_dag',
    default_args=default_args,
    description='A DAG for extracting and transforming Spotify data',
    schedule_interval=timedelta(days=1), 
    catchup=False, 
) as dag:

    spotify_extract_task = PythonOperator(
        task_id='extract_spotify',
        python_callable=extract_spotify_data,
    )

    spotify_transform_task = PythonOperator(
        task_id='transform_spotify',
        python_callable=transform_spotify_data,
        op_kwargs={'df_spotify': '{{ ti.xcom_pull(task_ids="extract_spotify") }}'}
    )

    grammy_extract_task = PythonOperator(
        task_id='extract_grammy_data_task',
        python_callable=extract_grammy_data
    )

    grammy_transform_task = PythonOperator(
        task_id='transform_grammy_data_task',
        python_callable=transform_grammy_data,
        op_kwargs={'df_grammys': '{{ ti.xcom_pull(task_ids="extract_grammy_data_task") }}'}
    )

    
    merge_task = PythonOperator(
        task_id='merge_data',
        python_callable=merge_data
    ) 

    load_db = PythonOperator(
        task_id='load_db_task',
        python_callable=save_to_postgres
    ) 

    upload_to_drive_task = PythonOperator(
    task_id='upload_to_drive',
    python_callable=upload_to_drive,
    provide_context=True,
    dag=dag,
    )
    grammy_extract_task >> grammy_transform_task >> merge_task >> load_db
    spotify_extract_task >> spotify_transform_task >> merge_task >> load_db
    merge_task >> upload_to_drive_task 