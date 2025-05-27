import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv


# Root directory to Python path for Airflow
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Load .env from project root
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path=env_path)

default_args={
    "owner":"Alex Perrine",
    "retries":0,
    "execution_timeout":timedelta(minutes=15)
}

@dag(
    dag_id="refresh_iceberg_tables_daily",
    description="A DAG that refreshes Iceberg tables in Snowflake daily",
    start_date=days_ago(1),
    schedule='@daily',
    catchup=False,
    default_args=default_args,
    tags=["snowflake", "iceberg", "refresh"]
)

def refresh_iceberg_tables_dag():
    test_connection = BashOperator(
        task_id="test_airflow_connection",
        bash_command="echo 'Airflow environment is reachable for Iceberg table refresh.'"
    )

    start_refresh = EmptyOperator(task_id="start_refresh_marker")

    def debug_airflow_connection():
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection("snowflake_conn")
        print("Airflow Connection Debug:")
        print("Host:", conn.host)
        print("Schema:", conn.schema)
        print("Login:", conn.login)
        print("Password:", conn.password)
        print("Extra JSON:", conn.extra_dejson)

    debug_connection = PythonOperator(
        task_id="debug_airflow_connection",
        python_callable=debug_airflow_connection
    )

    refresh_engagement = SQLExecuteQueryOperator(
        task_id="refresh_engagement_table",
        conn_id="snowflake_conn",  # Uses AIRFLOW_CONN_SNOWFLAKE_CONN from .env
        sql="ALTER ICEBERG TABLE alexperrine.raw_ga_engagement REFRESH;"
    )
    refresh_pageviews = SQLExecuteQueryOperator(
        task_id="refresh_pageviews_table",
        conn_id="snowflake_conn",  # Uses AIRFLOW_CONN_SNOWFLAKE_CONN from .env
        sql="ALTER ICEBERG TABLE alexperrine.raw_ga_pageviews REFRESH;"
    )
    refresh_users = SQLExecuteQueryOperator(
        task_id="refresh_users_table",
        conn_id="snowflake_conn",  # Uses AIRFLOW_CONN_SNOWFLAKE_CONN from .env
        sql="ALTER ICEBERG TABLE alexperrine.raw_ga_users REFRESH;"
    )

    end_refresh = EmptyOperator(task_id="end_refresh_marker")

    test_connection >> start_refresh >> debug_connection >> refresh_engagement >> refresh_pageviews >> refresh_users >> end_refresh

refresh_iceberg_tables_dag()
