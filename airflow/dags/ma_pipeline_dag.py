from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# Repo root = airflow/..
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))

with DAG(
    dag_id="ma_value_impact_pipeline",
    start_date=datetime(2020, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    bronze = BashOperator(
        task_id="load_bronze",
        bash_command=f"cd {PROJECT_ROOT} && docker exec ma_project_app python src/extract_wrds.py",
    )

    silver = BashOperator(
        task_id="build_silver",
        bash_command=f"cd {PROJECT_ROOT} && docker exec ma_project_app python src/transform_clean.py",
    )

    gold = BashOperator(
        task_id="build_gold",
        bash_command=f"cd {PROJECT_ROOT} && docker exec ma_project_app python src/gold_layer.py",
    )

    bronze >> silver >> gold
