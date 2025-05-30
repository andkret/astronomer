import json
from pendulum import datetime

# added a comment 2
# and another one

from airflow.decorators import (
    dag,
    task,
)

@dag(
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,  # If a task fails, it will retry 2 times.
    },
    tags=["example"],
)  # If set, this tag is shown in the DAG view of the Airflow UI
def example_dag_basic():

    @task()
    def extract():

        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        order_data_dict = json.loads(data_string)
        return order_data_dict

    @task(
        multiple_outputs=True
    )  # multiple_outputs=True unrolls dictionaries into separate XCom values
    def transform(order_data_dict: dict):

        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    @task()
    def load(total_order_value: float):

        print(f"Total order value is: {total_order_value:.2f}")

    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])


example_dag_basic()
