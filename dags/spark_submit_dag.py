from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime


with DAG(
    dag_id="spark_submit_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Run on demand
    catchup=False,
    tags=["practice", "branching"],
) as dag:

    start = EmptyOperator(task_id="start")
    spark_task = SparkSubmitOperator(
        task_id="spark_task",
        application="/opt/airflow/dags/scripts/spark/example_spark_submit_job.py",  # This is mounted inside containers
        conn_id="spark_conn",  # gets connected to spark://spark-master:7077
        verbose=True,
    )

    start >> spark_task
