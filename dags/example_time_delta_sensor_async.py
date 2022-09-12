"""
Example DAG demonstrating ``TimeDeltaSensorAsync``, a drop in replacement for ``TimeDeltaSensor`` that
defers and doesn't occupy a worker slot while it waits
"""

import datetime

import pendulum

from airflow import DAG
#from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.time_delta import TimeDeltaSensorAsync

with DAG(
    dag_id="example_time_delta_sensor_async",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    default_args={'owner':'community'}
) as dag:
    wait = TimeDeltaSensorAsync(task_id="wait", delta=datetime.timedelta(seconds=10))
    finish = DummyOperator(task_id="finish")
    wait >> finish
