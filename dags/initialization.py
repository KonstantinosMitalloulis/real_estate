from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.postgres_operator import PostgresOperator
import requests
from dag_helpers import (read_sql_file, read_csv_initialization, transform_dataframe_initialization,create_directories_exchange_files_initialization,insert_data_into_german_geography_initialization,
                         send_discord_message_initialization, insert_data_into_stage_initialization,cleanup_csvs_created_under_run_initialization,transformation_german_geography_initialization)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'intialization_database',
    default_args=default_args,
    description='Merge csvs from after scraping,transform,load into postgres',
    schedule_interval=None,
)


create_directories_exchange_files_task = PythonOperator(
    task_id='create_directories_exchange_files_initialization',
    python_callable=create_directories_exchange_files_initialization,
    dag=dag,
)

# Task to create all tables
create_all_tables_task = PostgresOperator(
    task_id='create_all_tables_initialization',
    postgres_conn_id='real_estate_germany',  # Replace with your Postgres connection ID
    sql=read_sql_file('/opt/airflow/dags/sql_scripts/create_scripts.sql'),
    dag=dag,
)

read_csv_task = PythonOperator(
    task_id='read_csv_initialization',
    python_callable=read_csv_initialization,
    dag=dag,
)



transform_dataframe_task = PythonOperator(
    task_id='transform_dataframe_initialization',
    python_callable=transform_dataframe_initialization,
    provide_context=True,
    dag=dag,
)

transform_dataframe_german_geography_task = PythonOperator(
    task_id='transform_dataframe_german_geography_initialization',
    python_callable=transformation_german_geography_initialization,
    provide_context=True,
    dag=dag,
)

insert_data_into_stage_task = PythonOperator(
    task_id='insert_data_into_stage_initialization',
    python_callable=insert_data_into_stage_initialization,
    provide_context=True,
    dag=dag,
)


insert_data_dimensions_fact_task = PostgresOperator(
    task_id='insert_data_dimensions_fact_initialization',
    postgres_conn_id='real_estate_germany',  # Replace with your Postgres connection ID
    sql=read_sql_file('/opt/airflow/dags/sql_scripts/insert_data_to_dimensions_fact.sql'),
    dag=dag,
)

insert_dim_german_geography_task = PythonOperator(
    task_id='insert_data_into_german_geography_initialization',
    python_callable=insert_data_into_german_geography_initialization,
    provide_context=True,
    dag=dag,
)



# Task to send success message to Discord
discord_webhook_url = 'https://discord.com/api/webhooks/1255880045684064286/vjIH6LVAyTSUgI-qc98zRIxDMnLUua3pa5Tigl2xp9DCVqKuN0eEmq_PjPm5egeC61ej' 

send_success_message_task = PythonOperator(
    task_id='send_success_message_initialization',
    python_callable=send_discord_message_initialization,
    op_kwargs={
        'webhook_url': discord_webhook_url,
        'message': 'DAG:intialization_database completed successfully!'
    },
    dag=dag,
)

cleanup_csvs_created_under_run_task = PythonOperator(
    task_id='cleanup_csvs_created_under_run_initialization',
    python_callable=cleanup_csvs_created_under_run_initialization,
    provide_context=True,
    dag=dag,
)

create_directories_exchange_files_task>>create_all_tables_task>> read_csv_task >> transform_dataframe_task   >> transform_dataframe_german_geography_task >>insert_data_into_stage_task >> insert_data_dimensions_fact_task >>insert_dim_german_geography_task >>send_success_message_task >> cleanup_csvs_created_under_run_task