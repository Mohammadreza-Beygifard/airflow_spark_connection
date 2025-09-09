from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from scripts.spark.example_standalone_spark_job import run_pyspark_on_standalone


with DAG(
    dag_id="pyspark_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Run on demand
    catchup=False,
    tags=["practice", "branching"],
) as dag:

    start = EmptyOperator(task_id="start")
    spark_task = PythonOperator(
        task_id="spark_task", python_callable=run_pyspark_on_standalone
    )

    start >> spark_task
