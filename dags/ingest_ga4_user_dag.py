import sys
import os
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from dotenv import load_dotenv

# Add root directory to Python path so Airflow can find scripts.*
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Imports AFTER path append
from scripts.ingest_ga4_user import ingest_ga4_user_data
from scripts.ga4_checks import check_ga4_user_data, check_ga4_user_schema

# Load .env from project root
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path=env_path)

def run_ingestion(**context):
    # Ensure date environment variables are set consistently
    yesterday = (context["execution_date"] - timedelta(days=1)).strftime('%Y-%m-%d')
    os.environ["GA4_START_DATE"] = yesterday
    os.environ["GA4_END_DATE"] = yesterday

    ingest_ga4_user_data(
        start_date=os.environ["GA4_START_DATE"],
        end_date=os.environ["GA4_END_DATE"]
    )

default_args = {
    "owner": "Alex Perrine",
    "retries": 0,
    "execution_timeout": timedelta(minutes=30),
}

@dag(
    dag_id="Ingest_GA4_user_views_daily",
    description="A DAG that ingests Google Analytics user data into Iceberg",
    start_date=days_ago(1),
    schedule='@daily',
    catchup=False,
    default_args=default_args,
    tags=["ga4", "iceberg", "analytics"]
)
def ingest_ga4_user_dag():

    test_connection = BashOperator(
        task_id="test_airflow_connection",
        bash_command="echo 'Airflow environment is reachable.'"
    )

    pre_ingestion = EmptyOperator(task_id="pre_ingestion_marker")

    check_data_task = PythonOperator(
        task_id="check_ga4_user_data",
        python_callable=check_ga4_user_data,
        provide_context=True,
    )

    check_user_schema_task = PythonOperator(
        task_id="check_ga4_user_schema",
        python_callable=check_ga4_user_schema,
        provide_context=True,
    )

    ingestion_task = PythonOperator(
        task_id="run_users_ingestion",
        python_callable=run_ingestion,
        provide_context=True,
    )

    post_ingestion = EmptyOperator(task_id="post_ingestion_successful")

    test_connection >> pre_ingestion >> check_data_task >> check_user_schema_task >> ingestion_task >> post_ingestion

ingest_ga4_user_dag()
