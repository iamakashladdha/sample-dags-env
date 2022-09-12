from airflow import DAG
from datetime import timedelta
#from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.timezone import datetime

from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync

default_args = {
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'owner':'customer success'
}

with DAG(
    dag_id="deferrable_operator_dag",
    start_date=datetime(2022, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=["deferrable", "async"],
) as dag:

    start = DummyOperator(task_id="start")

    external_task_async = ExternalTaskSensorAsync(
        task_id="external_task_async",
        external_task_id="sleep_task_2",
        external_dag_id="deferrable_sleep_dag",
    )

    finish = DummyOperator(task_id="finish")

    start >> external_task_async >> finish