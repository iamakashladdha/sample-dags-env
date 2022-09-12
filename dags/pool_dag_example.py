from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
#from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


def random_sum():
    import random
    import time

    r = random.randint(0, 100)
    add_random = 2 + r
    print(add_random)
    time.sleep(15)

with DAG(dag_id='pool_dag_example',
         start_date=datetime(2022, 5, 1),
         schedule_interval="@daily",
         default_args={'owner': 'customer success'},
         tags=['pools', 'example', 'chain'],
         description='''
             This DAG demonstrates pools which allow you to limit parallelism
             for an arbitrary set ot tasks.
         ''',
         catchup=False
         ) as dag:

    sum1 = PythonOperator(task_id='sum1', python_callable=random_sum, pool='pool_example')  # pool_2 is set to 1

    sum2 = PythonOperator(task_id='sum2', python_callable=random_sum, pool='pool_example')

    complete = DummyOperator(
        task_id='complete',
        pool='default_pool',  # If a task is not given a pool, it is assigned to default_pool (128 slots).
    )

    chain([sum1, sum2], complete)  # This is equivalent to [sum1, sum2] >> complete
    # chain() is useful when setting single direction relationships to many operators
    # https://airflow.apache.org/docs/apache-airflow/1.10.6/concepts.html?highlight=branch#relationship-helper