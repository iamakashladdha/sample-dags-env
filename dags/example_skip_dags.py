"""Example DAG demonstrating the EmptyOperator and a custom EmptySkipOperator which skips by default."""

from os import abort
import pendulum

from airflow import DAG
from airflow.exceptions import AirflowSkipException
#from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule


# Create some placeholder operators
class EmptySkipOperator(DummyOperator):
    """Empty operator which always skips the task."""

    ui_color = '#e8b7e4'

    def execute(self, context: Context):
        raise AirflowSkipException


def create_test_pipeline(suffix, trigger_rule):
    """
    Instantiate a number of operators for the given DAG.
    :param str suffix: Suffix to append to the operator task_ids
    :param str trigger_rule: TriggerRule for the join task
    :param DAG dag_: The DAG to run the operators on
    """
    skip_operator = EmptySkipOperator(task_id=f'skip_operator_{suffix}')
    always_true = DummyOperator(task_id=f'always_true_{suffix}')
    join = DummyOperator(task_id=trigger_rule, trigger_rule=trigger_rule)
    final = DummyOperator(task_id=f'final_{suffix}')

    skip_operator >> join
    always_true >> join
    join >> final


with DAG(
    dag_id='example_skip_dag',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
    default_args={'owner':'community'}
) as dag:
    create_test_pipeline('1', TriggerRule.ALL_SUCCESS)
    create_test_pipeline('2', TriggerRule.ONE_SUCCESS)