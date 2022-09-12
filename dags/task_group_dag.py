from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from groups.process_tasks import process_tasks


@task.python(task_id="extract_data_partner",multiple_outputs=True, do_xcom_push=False)
def extract_data():
    partner_name = "netflix"
    partner_path = "/path/netlix"
    return {"partner_name": partner_name, "partner_path" : partner_path}

default_args={
    'start_date':datetime(2022,2,1),
    'owner':'customer success'
}

@dag(description="this is a sample dag for processing customer data",default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    dagrun_timeout=timedelta(minutes=10),
    tags=["example", "task_groups"],
    catchup=False)

def task_group_dag():
    partner_setting = extract_data()
    process_tasks(partner_setting)

dag = task_group_dag()