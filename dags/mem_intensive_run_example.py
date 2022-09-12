from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.sensors.date_time import DateTimeSensorAsync
from airflow.operators.dummy import DummyOperator
from multiprocessing import cpu_count
import time, random, signal, threading, psutil

'''
    Steps to Execute Memory Max Out
    In the pop-up, select Trigger DAG w/ config to enter a configuration for the DAG run. 
    This DAG run's default running time is set to 60 seconds, but in many cases, this may be a bit short. 
    Therefore, in the config JSON, add a parameter duration and set it to 600 seconds. 
    Also, add another parameter called step and set it to 50. 
    Step is how much memory (in MegaBytes) the memory usage will increase per every second of interval.

    JSON should look like 
    { "duration": 600, "step": 50}
'''


# example DAG to promote MEM load during the DAG run
stop_loop = 0
mem_chunks = []
MEGABYTE = 1024 * 1024

def exit_load():
    global stop_loop
    stop_loop = 1

def memory_fill(mbytes):
    # consume: MB of reserved memory
    dummy_buffer = []
    dummy_buffer = ['A' * MEGABYTE for _ in range(0, int(mbytes))]
    return dummy_buffer

def induce_mem_load(size):
    global stop_loop
    global mem_chunks
    print(f'starting mem load')
    while not stop_loop:
        # get memory available in MB
        mem_avail = psutil.virtual_memory().available >> 20
        print(f'available mem: {mem_avail}')
        if(mem_avail > int(size) + 10):
            mem_chunk = memory_fill(size)
            mem_chunks.append(mem_chunk)
            print(f'mem_chunks size : {len(mem_chunks)} x {len(mem_chunk)}MB')
        elif mem_avail > 15:
            mem_chunk = memory_fill(mem_avail - 15)
            mem_chunks.append(mem_chunk)
            print(f'mem_chunks size : {len(mem_chunks)} x {len(mem_chunk)}MB')
        else:
            print(f'available memory hit the ceiling - stopped incrementing')
        time.sleep(1)

with DAG(
    f"mem_intensive_run",
    start_date=days_ago(0),
    schedule_interval=None,
    catchup=False,
    max_active_runs=32,
    max_active_tasks=32,
    tags=[
        'example',
        'mem-run'
    ],
    default_args = {'owner':'customer success'}
) as dag:

    # simple init task that initializes this dag run
    @task(task_id="init")
    def init(**kwargs):
        dag_run = kwargs['dag_run']
        # try getting duration from dag run config, if any
        duration = dag_run.conf.get('duration', 60)
        # step is the MegaByte fill size of each
        # step iteration of the load. By default, 
        # each step interval is 1 second, and default
        # step size is 10MB. This means per 1 second, 
        # the program will increment 10MB each.
        step = dag_run.conf.get('step', 10)
        return { 'duration': duration, 'step': step }

    complete = DummyOperator(task_id="complete")

    mem_loads = []

    @task(task_id=f"mem_intensive")
    def mem_intensive(**kwargs):
        print(kwargs)
        ti = kwargs['ti']
        # looks like xcom_pull returns 'array', so choosing the first one.
        config = ti.xcom_pull(key='return_value', task_ids=['init'])[0]
        print(config)
        load = threading.Thread(target=induce_mem_load, args=(config['step'],))
        load.start()

        # start the sleep
        time.sleep(config['duration'])
        exit_load()
        print(f'finishing load after {config["duration"]} seconds.')
        
    init() >> mem_intensive() >> complete