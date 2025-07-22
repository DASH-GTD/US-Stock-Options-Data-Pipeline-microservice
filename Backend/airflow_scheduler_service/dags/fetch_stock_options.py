from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def log_response(**context):
    task_id = context['task'].task_id.replace('log_', 'fetch_')
    response = context['task_instance'].xcom_pull(task_ids=task_id)
    logging.info(f"API Response for {task_id}: {response}")

with DAG(
    'fetch_stock_options',
    default_args=default_args,
    description='DAG to fetch stock and options data periodically',
    schedule=timedelta(minutes=5),
    start_date=datetime(2025, 6, 20),
    catchup=False,
) as dag:
    
    fetch_each_day_data = HttpOperator(
        task_id='fetch_each_day_data',
        http_conn_id='data_collector_service',
        endpoint='/api/fetch_each_day_data',
        method='POST',
        headers={'Content-Type': 'application/json'},
        data='{}',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

    fetch_last_15min_data = HttpOperator(
        task_id='fetch_last_15min_data',
        http_conn_id='data_collector_service',
        endpoint='/api/fetch_last_15min_data',
        method='POST',
        headers={'Content-Type': 'application/json'},
        data='{}',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

    fetch_option_data = HttpOperator(
        task_id='fetch_option_data',
        http_conn_id='data_collector_service',
        endpoint='/api/fetch_option_data',
        method='POST',
        headers={'Content-Type': 'application/json'},
        data='{}',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

    fetch_historical_data = HttpOperator(
        task_id='fetch_historical_data',
        http_conn_id='data_collector_service',
        endpoint='/api/fetch_historical_data',
        method='POST',
        headers={'Content-Type': 'application/json'},
        data='{}',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

    log_each_day_data = PythonOperator(
        task_id='log_each_day_data',
        python_callable=log_response,
        dag=dag,
    )

    log_last_15min_data = PythonOperator(
        task_id='log_last_15min_data',
        python_callable=log_response,
        dag=dag,
    )

    log_option_data = PythonOperator(
        task_id='log_option_data',
        python_callable=log_response,
        dag=dag,
    )

    log_historical_data = PythonOperator(
        task_id='log_historical_data',
        python_callable=log_response,
        dag=dag,
    )

    fetch_each_day_data >> log_each_day_data
    fetch_last_15min_data >> log_last_15min_data
    fetch_option_data >> log_option_data
    fetch_historical_data >> log_historical_data