"""Optional Airflow DAG (free, open source) that runs the same stages as the CLI.

To try it locally (not required for the lab, and not covered by the CI tests):
    pip install "apache-airflow>=2.9"
    export AIRFLOW_HOME=$PWD/.airflow
    export AIRFLOW__CORE__DAGS_FOLDER=$PWD/orchestration
    airflow standalone
The DAG shells out to the CLI, so it uses the same code path you test locally.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2
    from airflow.operators.bash import BashOperator

PROJECT = Path(__file__).resolve().parents[1]
STAGES = ["bronze", "silver", "gold", "warehouse"]

with DAG(
    dag_id="retail_lakehouse",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1},
    tags=["lab", "lakehouse"],
) as dag:
    previous = None
    for stage in STAGES:
        task = BashOperator(
            task_id=stage,
            bash_command=f"cd {PROJECT} && python -m lakehouse.pipeline {stage}",
        )
        if previous:
            previous >> task
        previous = task
