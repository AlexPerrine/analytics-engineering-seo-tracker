import os
from datetime import datetime, timedelta
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag
from dotenv import load_dotenv

# Load environment vairbales
dbt_env_path = os.path.join(os.environ["AIRFLOW_HOME"], 'dbt_project','dbt.env')
load_dotenv(dbt_env_path)

# Paths for dbt project and profiles
airflow_home = os.getenv('AIRFLOW_HOME')
PATH_TO_DBT_PROJECTS = f'{airflow_home}/dbt_project'
PATH_TO_DBT_PROFILES = f'{airflow_home}/dbt_project/profiles.yml'

# dbt profile configuration
profile_config = ProfileConfig(
    profile_name= "ae_capstone",
    target_name="dev",
    profiles_yml_filepath=PATH_TO_DBT_PROFILES,
)

# Defaul DAG Arguments
default_args = {
    "owner":"Alex Perrine",
    "retries":0,
    "execution_timeout": timedelta(hours=1),
}

@dag(
    dag_id="dbt_run_pipeline",
    start_date=datetime(2025,1,1),
    schedule='@daily',
    catchup=False,
    default_args=default_args,
    tags=["dbt","WAP","AE_capstone"]
)

def dbt_capstone_WAP_dag():
    pre_dbt_workflow = EmptyOperator(task_id="pre_dbt_workflow")

    dbt_run_marts = DbtTaskGroup(
        group_id="dbt_marts_build",
        project_config=ProjectConfig(PATH_TO_DBT_PROJECTS),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f'{airflow_home}/dbt_venv/bin/dbt',
        ),
        render_config = RenderConfig(
            select=[
                "+fact_blog_engagement",
                "+fact_user_sessions",
                "+fact_event_engagement"
            ],
        ),
    )

    post_dbt_workflow = EmptyOperator(task_id="post_dbt_workflow", trigger_rule="all_done")

    pre_dbt_workflow >> dbt_run_marts >> post_dbt_workflow

dbt_capstone_WAP_dag()