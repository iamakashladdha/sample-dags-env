"""Example DAG demonstrating the usage of the ShortCircuitOperator."""
import pendulum

from airflow import DAG
from airflow.models.baseoperator import chain
#from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id='example_short_circuit_operator',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
) as dag:
    cond_true = ShortCircuitOperator(
        task_id='condition_is_True',
        python_callable=lambda: True,
    )

    cond_false = ShortCircuitOperator(
        task_id='condition_is_False',
        python_callable=lambda: False,
    )

    ds_true = [DummyOperator(task_id='true_' + str(i)) for i in [1, 2]]
    ds_false = [DummyOperator(task_id='false_' + str(i)) for i in [1, 2]]

    chain(cond_true, *ds_true)
    chain(cond_false, *ds_false)

    [task_1, task_2, task_3, task_4, task_5, task_6] = [
        DummyOperator(task_id=f"task_{i}") for i in range(1, 7)
    ]

    task_7 = DummyOperator(task_id="task_7", trigger_rule=TriggerRule.ALL_DONE)

    short_circuit = ShortCircuitOperator(
        task_id="short_circuit", ignore_downstream_trigger_rules=False, python_callable=lambda: False
    )

    chain(task_1, [task_2, short_circuit], [task_3, task_4], [task_5, task_6], task_7)