from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "kelompok28",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SCRIPTS_DIR = "/opt/airflow/dags/scripts"

with DAG(
    dag_id="mci2026_orders_pipeline",
    default_args=default_args,
    description="Orders ETL Pipeline",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    fetch_orders = BashOperator(
        task_id="fetch_orders",
        bash_command=(
            f"python {SCRIPTS_DIR}/fetch_orders.py"
        )
    )

    process_orders = BashOperator(
        task_id="process_orders_spark",
        bash_command=(
            f"python {SCRIPTS_DIR}/process_orders_spark.py"
        )
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> fetch_orders >> process_orders >> end