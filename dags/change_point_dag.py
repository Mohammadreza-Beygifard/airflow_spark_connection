from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id="change_point_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Run on demand
    catchup=False,
    tags=["practice", "branching"],
) as dag:
    generate = BashOperator(
        task_id="generate_data",
        bash_command=(
            "python /opt/airflow/dags/tools/generate_data.py "
            "--out /opt/airflow/data/secondary "
            "--ds {{ ds }} "
            "--users 20000 "
            "--min_points 60 "
            "--max_points 240 "
            "--skew_user_prob 0.02 "
            "--skew_multiplier 40 "
            "--seed 42"
        ),
    )

    spark_task = SparkSubmitOperator(
        task_id="spark_task",
        application="/opt/airflow/dags/scripts/spark/change_point_job.py",  # This is mounted inside containers
        conn_id="spark_conn",  # gets connected to spark://spark-master:7077
        verbose=True,
        application_args=["--ds", "{{ ds }}"],
    )

    generate >> spark_task
