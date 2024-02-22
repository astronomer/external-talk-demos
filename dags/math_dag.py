"""
### DAG that performs basic math operations and writes the result to a table

This DAG is a simple example of how to use a custom operator to perform basic 
math operations and write the result to a table in a PostgreSQL database.
Demo: dag.test() and Airflow testing
"""

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime
import logging

# local imports
from include.custom_operators import MyBasicMathOperator
from include.utils import get_random_number_from_api

task_logger = logging.getLogger("airflow.task")

POSTGRES_CONN_ID = "postgres_demo"


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    params={
        "upper_limit": Param(100, type="integer"),
        "lower_limit": Param(1, type="integer"),
    },
)
def math_dag():

    @task
    def pick_a_random_number(**context) -> int:
        "Return a random number within the limits."
        minimum = context["params"]["lower_limit"]
        maximum = context["params"]["upper_limit"]

        num = get_random_number_from_api(
            min=minimum,
            max=maximum,
            count=1,
        )

        return num

    pick_a_random_number_obj = pick_a_random_number()

    @task
    def retrieve_operation_from_variable():
        from airflow.models.variable import Variable

        op = Variable.get("operation", default_var=None)

        if not op:
            raise ValueError("Operation not found in the Airflow Variables")

        return op

    retrieve_operation_from_variable_obj = retrieve_operation_from_variable()

    operate_with_23 = MyBasicMathOperator(
        task_id="operate_with_23",
        first_number=pick_a_random_number_obj,
        second_number=23,
        operation=retrieve_operation_from_variable_obj,
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=POSTGRES_CONN_ID,
        database="postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS numbers (
                number INTEGER
            )""",
    )

    write_to_table = SQLExecuteQueryOperator(
        task_id="write_to_table",
        conn_id=POSTGRES_CONN_ID,
        database="postgres",
        sql="INSERT INTO numbers VALUES ({{ task_instance.xcom_pull(task_ids='operate_with_23') }})",
    )

    chain(
        [retrieve_operation_from_variable_obj, pick_a_random_number_obj],
        operate_with_23,
    )
    chain([create_table, operate_with_23], write_to_table)


dag_obj = math_dag()


if __name__ == "__main__":
    conn_path = "dag_test/connections.yaml"
    variables_path = "dag_test/variables.yaml"
    upper_limit = 20
    lower_limit = 10

    dag_obj.test(
        execution_date=datetime(2025, 2, 1),
        conn_file_path=conn_path,
        variable_file_path=variables_path,
        run_conf={"upper_limit": upper_limit, "lower_limit": lower_limit},
    )
