from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import logging
import pendulum

# Set timezone to US Eastern Time (ET)
local_tz = pendulum.timezone("America/New_York")

# Centralized configuration
API_BASE_URL = Variable.get("data_collector_base_url", default_var="http://localhost:8000/api")
HTTP_CONN_ID = "data_collector_service"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def log_response(**context):
    """Log the API response for a given task."""
    task_id = context["task"].task_id.replace("log_", "fetch_")
    response = context["task_instance"].xcom_pull(task_ids=task_id)
    try:
        logging.info(f"API Response for {task_id}: {response}")
    except Exception as e:
        logging.error(f"Failed to log response for {task_id}: {str(e)}")
        raise

def create_http_task(task_id, endpoint, dag):
    """Factory function to create HttpOperator tasks."""
    return HttpOperator(
        task_id=task_id,
        http_conn_id=HTTP_CONN_ID,
        endpoint=f"{API_BASE_URL}/{endpoint}/",
        method="POST",
        headers={"Content-Type": "application/json"},
        data="{}",
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

def create_log_task(task_id, dag):
    """Factory function to create PythonOperator log tasks."""
    return PythonOperator(
        task_id=task_id,
        python_callable=log_response,
        dag=dag,
    )

with DAG(
    "fetch_last_15min_data",
    default_args=default_args,
    description="DAG to fetch stock data every 15 minutes during market hours",
    schedule_interval="*/15 9-16 * * 1-5",
    start_date=datetime(2025, 6, 20, tzinfo=local_tz),
    catchup=False,
) as dag_15min:
    fetch_last_15min_data = create_http_task("fetch_last_15min_data", "fetch_last_15min_data", dag_15min)
    log_last_15min_data = create_log_task("log_last_15min_data", dag_15min)
    fetch_last_15min_data >> log_last_15min_data


with DAG(
    "fetch_historical_data",
    default_args=default_args,
    description="DAG to fetch historical stock data once",
    schedule="@once",
    start_date=datetime(2025, 6, 20, tzinfo=local_tz),
    catchup=False,
) as dag_historical:
    fetch_historical_data = create_http_task("fetch_historical_data", "fetch_historical_data", dag_historical)
    log_historical_data = create_log_task("log_historical_data", dag_historical)
    fetch_historical_data >> log_historical_data

with DAG(
    "fetch_stock_daily",
    default_args=default_args,
    description="DAG for daily stock and options data fetch after market close",
    schedule_interval="30 17 * * 1-5",
    start_date=datetime(2025, 6, 20, tzinfo=local_tz),
    catchup=False,
) as dag_daily:
    start = DummyOperator(task_id="start")
    fetch_each_day_data = create_http_task("fetch_each_day_data", "fetch_each_day_data", dag_daily)
    fetch_option_data = create_http_task("fetch_option_data", "fetch_option_data", dag_daily)
    log_each_day_data = create_log_task("log_each_day_data", dag_daily)
    log_option_data = create_log_task("log_option_data", dag_daily)

    start >> [fetch_each_day_data, fetch_option_data]
    fetch_each_day_data >> log_each_day_data
    fetch_option_data >> log_option_data