"""
Example DAG demonstrating the usage of ``@task.branch`` TaskFlow API decorator with depends_on_past=True,
where tasks may be run or skipped on alternating runs.
"""
import pendulum

from airflow import DAG
from airflow.decorators import task
#from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator


@task.branch()
def should_run(**kwargs):
    """
    Determine which empty_task should be run based on if the execution date minute is even or odd.
    :param dict kwargs: Context
    :return: Id of the task to run
    :rtype: str
    """
    print(
        f"------------- exec dttm = {kwargs['execution_date']} and minute = {kwargs['execution_date'].minute}"
    )
    if kwargs['execution_date'].minute % 2 == 0:
        return "empty_task_1"
    else:
        return "empty_task_2"

with DAG(
    dag_id='example_branch_dop_operator',
    schedule_interval='*/1 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    default_args={'depends_on_past': True, 'owner': 'customer success'},
    tags=['example'],
) as dag:
    cond = should_run()


    empty_task_1 = DummyOperator(task_id='empty_task_1')
    empty_task_2 = DummyOperator(task_id='empty_task_2')
    cond >> [empty_task_1, empty_task_2]