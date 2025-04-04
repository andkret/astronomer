import json
from airflow.decorators import asset
from pendulum import datetime

@asset(schedule="@daily", name="order_data", group="etl")
def extract():
    """
    Extracts raw order data from a hardcoded JSON string.
    """
    data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
    return json.loads(data_string)

@asset(name="order_summary", group="etl")
def transform(order_data: dict):
    """
    Transforms raw order data by calculating total order value.
    """
    total_order_value = sum(order_data.values())
    return {"total_order_value": total_order_value}

@asset(group="etl")
def load(order_summary: dict):
    """
    Loads the transformed summary by printing the result.
    """
    print(f"Total order value is: {order_summary['total_order_value']:.2f}")
