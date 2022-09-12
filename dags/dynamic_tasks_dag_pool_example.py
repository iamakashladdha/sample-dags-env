from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator, BranchPythonOperator
from groups.process_tasks import process_tasks
from airflow.operators.dummy import DummyOperator
from airflow.sensors.date_time import DateTimeSensor
from airflow.models.baseoperator import chain
import time

partners =  {
    "partner_snowflake":{
        "name":"snowflake",
        "path" : "/partners/snowflake",
        "priority": 2

    },
    "partner_netflix":{
        "name":"netflix",
        "path" : "/partners/netflix",
        "priority": 3
    },
    "partner_astronomer":{
        "name":"astronomer",
        "path" : "/partners/astronomer",
        "priority": 1
    }
}

default_args={
    'start_date':datetime(2022,2,1),
    'retries':0,
    'owner' : 'customer success'
}

def _success_callback(context):
    print(context)
    print("Dag is successful")

def _failure_callback(context):
    print(context)

def _extract_data_callback_success(context):
    print("SUCCESS CALLBACK")

from airflow.exceptions import AirflowTaskTimeout, AirflowSensorTimeout

def _extract_data_callback_failure(context):
    if (context['exception']):
        if (isinstance(context['exception'], AirflowTaskTimeout)):
            print("AirflowTaskTimeout")
        if (isinstance(context['exception'], AirflowSensorTimeout)):
            print("Airflow SensorTimeOut")
    print("FAILURE CALLBACK")

def _extract_data_callback_retry(context):
    print("RETRY CALLBACK")

def _sla_miss_callback(dag, task_list, blocking_task_list,slas,blocking_tis):
    print(task_list)
    print(blocking_task_list)
    print(slas)
    print(blocking_tis)

@dag(
    description="Dag for Dynamic Task with Branching for processing customer data",
    default_args=default_args,
    schedule_interval=timedelta(minutes=30),
    dagrun_timeout=timedelta(minutes=10),
    tags=["example", "task_groups","pools"],
    catchup=False,
    on_success_callback = _success_callback,
    on_failure_callback = _failure_callback,
    sla_miss_callback = _sla_miss_callback)

def dynamic_tasks_branch_pool_dag():

    start = DummyOperator(
        task_id="start",
        execution_timeout=timedelta(minutes=10),
        trigger_rule='dummy',
        pool='default_pool'
    )

    storing = DummyOperator(
        task_id="store",
        trigger_rule='none_failed_or_skipped'
    )

    end = DummyOperator(
        task_id="end"
    )
    
    for partner, details in partners.items():
        
        @task.python(task_id=f"extract_data_{partner}",
        on_success_callback= _extract_data_callback_success,
        on_failure_callback= _extract_data_callback_failure,
        on_retry_callback = _extract_data_callback_retry,
        depends_on_past=False, 
        pool = 'partner_pool',
        priority_weight = details['priority'],
        multiple_outputs=True, 
        do_xcom_push=False,
        sla=timedelta(minutes=10),
        )
        def extract_data(partner_name,partner_path):
            return {"partner_name": partner_name, "partner_path" : partner_path}

        extracted_values = extract_data(details['name'],details['path'])
        start >> extracted_values 
        process_tasks(extracted_values) >> storing >> end
        
dag = dynamic_tasks_branch_pool_dag()