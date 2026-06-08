from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "aqil",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SCRIPTS_DIR = "/opt/airflow/dags/scripts"

with DAG(
    dag_id="mci2026_dustiniadelixiagroceria_pipeline",
    default_args=default_args,
    description="Dataset Pipeline",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    fetch_dataset = BashOperator(
        task_id="fetch_datasets",
        bash_command=(
            f"python {SCRIPTS_DIR}/fetch_datasets.py"
        )
    )

    process_datasets = BashOperator(
        task_id="process_datasets_spark",
        bash_command=(
            f"python {SCRIPTS_DIR}/process_datasets_spark.py"
        )
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> fetch_dataset >> process_datasets >> end