from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models.baseoperator import chain
from airflow.decorators import task,dag
from typing import Dict

@task.python (task_id="extract_partners", do_xcom_push=False,multiple_outputs=True)
def extract():
    partner_name = "netflix"
    partner_path = "/partner/netflix"
    return {
        "partner_name": partner_name,
        "partner_path": partner_path
    }

@task.python
def process(partner_name, partner_path):
    print(partner_name)
    print(partner_path)

@dag(description="Dag to test XCOM to exchange data between tasks",
    start_date = datetime(2022,2,6),
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    catchup=False,
    tags=["example","xcom"],
    max_active_runs=2,
    default_args={'owner':'customer success'})

def xcom_dag_example():
    partner_settings = extract()
    process(partner_settings['partner_name'],partner_settings['partner_path'])

dag = xcom_dag_example()