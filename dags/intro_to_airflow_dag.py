from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime
from include.custom_operators import MyBasicMathOperator


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
)
def intro_to_airflow_dag():

    @task
    def pick_a_number() -> int:
        "Return a random number between 1 and 100."
        import random

        return random.randint(1, 100)

    pick_a_number_obj = pick_a_number()

    add_23 = MyBasicMathOperator(
        task_id="add_23",
        first_number=pick_a_number_obj,
        second_number=23,
        operation="+",
    )

    chain(pick_a_number_obj, add_23)


intro_to_airflow_dag()
