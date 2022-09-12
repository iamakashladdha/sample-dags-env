import datetime
import time

import pendulum

from airflow.decorators import dag, task

"""Example DAG demonstrating SLA use in Tasks"""


# [START howto_task_sla]
def sla_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(
        "The callback arguments are: ",
        {
            "dag": dag,
            "task_list": task_list,
            "blocking_task_list": blocking_task_list,
            "slas": slas,
            "blocking_tis": blocking_tis,
        },
    )


@dag(
    schedule_interval="*/2 * * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    sla_miss_callback=sla_callback,
    tags=['example','sla'],
    default_args={'email': "email@example.com"},
)
def example_sla_dag():
    @task(sla=datetime.timedelta(seconds=10))
    def sleep_20():
        """Sleep for 20 seconds"""
        time.sleep(20)

    @task
    def sleep_30():
        """Sleep for 30 seconds"""
        time.sleep(30)

    sleep_20() >> sleep_30()


example_dag = example_sla_dag()

# [END howto_task_sla]