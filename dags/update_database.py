"""
Update-DAG: Er aktualisiert unsere Datenbank mit den aktuellen Immobilien, die zum Verkauf stehen. 
Gleichzeitig werden in der Tabelle dim_historical_data alle Webseiten von Immobilien archiviert, die nicht mehr aktuell sind.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import requests
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.postgres_operator import PostgresOperator
from dag_helpers import (read_sql_file, insert_data_into_stage_dag_update, insert_data_into_table_all_current_webpages_update,webpages_before_update,
                         export_webpages_before_update,run_web_scraper_update,transform_new_entries_df_update,send_discord_message_update,cleanup_csvs_created_under_run_update)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'update_database',
    default_args=default_args,
    description='Update database daily with web scraper',
    schedule_interval=None,
)

discord_webhook_url = 'https://discord.com/api/webhooks/1255880045684064286/vjIH6LVAyTSUgI-qc98zRIxDMnLUua3pa5Tigl2xp9DCVqKuN0eEmq_PjPm5egeC61ej' 





read_before_update_fact_table_task = PythonOperator(
    task_id='read_before_update_fact_table_update',
    python_callable=webpages_before_update,
    provide_context=True,
    dag=dag,
)

export_webpages_before_update_task = PythonOperator(
    task_id='export_existing_webpages_update',
    python_callable=export_webpages_before_update,
    provide_context=True,
    dag=dag,
)


run_web_scraper_task = PythonOperator(
    task_id='run_web_scraper_update',
    python_callable=run_web_scraper_update,
    provide_context=True,
    dag=dag,
)


create_table_all_current_webpages_task = PostgresOperator(
    task_id='create_table_all_current_webpages_update',
    postgres_conn_id='real_estate_germany',  
    sql=read_sql_file('/opt/airflow/dags/sql_scripts/create_table_all_current_webpages.sql'),
    dag=dag,
)


insert_data_into_all_current_webpages_table_task = PythonOperator(
    task_id='insert_data_into_all_current_webpages_table_update',
    python_callable=insert_data_into_table_all_current_webpages_update,
    provide_context=True,
    dag=dag,
)


create_table_historical_data_if_not_exists_task = PostgresOperator(
    task_id='create_table_historical_data_if_not_exists_update',
    postgres_conn_id='real_estate_germany', 
    sql=read_sql_file('/opt/airflow/dags/sql_scripts/create_dim_historical_data_if_not_exists.sql'),
    dag=dag,
)


insert_into_dim_historical_data_task = PostgresOperator(
    task_id='insert_into_dim_historical_data_update',
    postgres_conn_id='real_estate_germany',  
    sql=read_sql_file('/opt/airflow/dags/sql_scripts/insert_into_dim_historical_data.sql'),
    dag=dag,
)


transform_new_entries_dataframe_task = PythonOperator(
    task_id='transform_new_entries_dataframe_update',
    python_callable=transform_new_entries_df_update,
    provide_context=True,
    dag=dag,
)


insert_data_into_staging_table_task = PythonOperator(
    task_id='insert_data_into_staging_table_update',
    python_callable=insert_data_into_stage_dag_update,
    provide_context=True,
    dag=dag,
)


insert_data_dimensions_fact_task = PostgresOperator(
    task_id='insert_data_dimensions_fact_update',
    postgres_conn_id='real_estate_germany',  
    sql=read_sql_file('/opt/airflow/dags/sql_scripts/insert_data_to_dimensions_fact.sql'),
    dag=dag,
)



send_success_message_task = PythonOperator(
    task_id='send_success_message_update',
    python_callable=send_discord_message_update,
    op_kwargs={
        'webhook_url': discord_webhook_url,
        'message': 'DAG:update_database completed successfully!'
    },
    dag=dag,
)

cleanup_csvs_created_under_run_task = PythonOperator(
    task_id='cleanup_csvs_created_under_run_update',
    python_callable=cleanup_csvs_created_under_run_update,
    provide_context=True,
    dag=dag,
)



read_before_update_fact_table_task >> export_webpages_before_update_task >>run_web_scraper_task >> create_table_all_current_webpages_task >>insert_data_into_all_current_webpages_table_task >> create_table_historical_data_if_not_exists_task>>insert_into_dim_historical_data_task  >> transform_new_entries_dataframe_task >>insert_data_into_staging_table_task>>insert_data_dimensions_fact_task >> send_success_message_task >> cleanup_csvs_created_under_run_task