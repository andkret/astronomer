import pytest
from airflow.models import DagBag
from airflow.utils.dates import days_ago

@pytest.fixture
def dag():
    return DagBag().get_dag('example_dag_basic')

# def test_transform():
#     from example_dag_basic import transform

#     result = transform(order_data_dict='{"1001": 1, "1002": 2, "1003": 3}')
#     assert result == 5

def test_task_add(dag):
    transform = dag.get_task('transform')
    assert transform is not None

    # Test the output of the task (depends on the implementation of add_numbers)
    ti = TaskInstance(task=transform, execution_date='2023-07-28')
    result = ti.run(order_data_dict={"1001": 1, "1002": 2, "1003": 3})
    assert result == 5