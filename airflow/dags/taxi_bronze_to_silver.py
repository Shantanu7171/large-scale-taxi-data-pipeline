import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def run_bronze_to_silver():
    import runpy

    runpy.run_path(
        "/opt/airflow/processing/jobs/bronze_to_silver.py",
        run_name="__main__",
    )


def load_silver_to_snowflake():
    import sys

    sys.path.insert(0, "/opt/airflow/snowflake")

    import load_silver

    load_silver.main()


with DAG(
    dag_id="taxi_bronze_to_silver",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Kolkata"),
    schedule="0 0 * * *",   # Every day at 12:00 AM IST
    catchup=False,
    tags=["taxi", "minio", "polars", "snowflake"],
) as dag:

    bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_bronze_to_silver,
    )

    load_to_snowflake = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_silver_to_snowflake,
    )

    bronze_to_silver >> load_to_snowflake