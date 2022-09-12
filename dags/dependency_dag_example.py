
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import cross_downstream, chain
from datetime import datetime

default_args = {
    'start_date':datetime(2022,2,5),
    'owner': 'customer success'
}

with DAG("dependency_example_dag",
    schedule_interval="@hourly",
    catchup=False, 
    tags =["example","cross_downstream"], 
    default_args=default_args,
    concurrency=2, 
    max_active_runs=2) as dag:

    t1 = DummyOperator(task_id ="t1")
    t2 = DummyOperator(task_id = "t2")
    t3 = DummyOperator(task_id = "t3")
    t4 = DummyOperator(task_id = "t4")
    t5 = DummyOperator(task_id = "t5")
    t6 = DummyOperator(task_id = "t6")
    t7 = DummyOperator(task_id = "t7")

    cross_downstream([t1,t2,t3],[t4,t5,t6])
    [t4,t5,t6] >> t7 