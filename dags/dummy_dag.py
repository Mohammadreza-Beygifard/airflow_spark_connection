from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import random


def choose_branch():
    number = random.randint(1, 100)
    print(f"Generated number: {number}")
    if number % 2 == 0:
        return ["even_task", "dummy_task_one"]
    else:
        return "odd_task"


def print_even():
    print("Number was even")


def print_odd():
    print("Number was odd")


with DAG(
    dag_id="branching_example_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Run on demand
    catchup=False,
    tags=["practice", "branching"],
) as dag:

    start = EmptyOperator(task_id="start")

    branch = BranchPythonOperator(
        task_id="branching",
        python_callable=choose_branch,
    )

    even_task = PythonOperator(
        task_id="even_task",
        python_callable=print_even,
    )

    odd_task = PythonOperator(
        task_id="odd_task",
        python_callable=print_odd,
    )

    dummy_task_one = EmptyOperator(
        task_id="dummy_task_one",
        trigger_rule="none_failed_min_one_success",  # ensures DAG continues after branch
    )

    dummy_task_two = EmptyOperator(
        task_id="dummy_task_two",
        trigger_rule="none_failed_min_one_success",  # ensures DAG continues after branch
    )

    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",  # ensures DAG continues after branch
    )

    start >> branch
    branch >> [even_task, odd_task, dummy_task_one, dummy_task_two] >> join
