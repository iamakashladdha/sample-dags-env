from airflow import DAG
#from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'owner': 'customer success'
}

with DAG(dag_id='deferrable_sleep_dag',
         default_args=default_args,
         schedule_interval="@daily",
         start_date=datetime(2022, 1, 1),
         tags=["deferrable", "async"],
         catchup=False) as dag:

    start = DummyOperator(
        task_id='start')

    sleep_task_1 = BashOperator(
        task_id='sleep_task_1',
        bash_command='sleep 10s'
    )

    sleep_task_2 = BashOperator(
        task_id='sleep_task_2',
        bash_command='sleep 10s'
    )

    sleep_task_3 = BashOperator(
        task_id='sleep_task_3',
        bash_command='sleep 10s'
    )

    finish = DummyOperator(
        task_id='finish')

    start >> sleep_task_1 >> sleep_task_2 >> sleep_task_3 >> finish