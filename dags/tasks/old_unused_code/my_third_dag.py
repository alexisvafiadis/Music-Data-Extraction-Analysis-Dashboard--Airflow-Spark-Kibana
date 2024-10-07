from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
       'My_third_dag',
       params={'fake_param_to_have_conf_in_ui' : 123},
       default_args={
           'depends_on_past': False,
           'email': ['airflow@example.com'],
           'email_on_failure': False,
           'email_on_retry': False,
           'retries': 1,
           'retry_delay': timedelta(seconds=15),
       },
       description='A first DAG',
       schedule_interval=None,
       start_date=datetime(2021, 1, 1),
       catchup=False,
       tags=['example'],
) as dag:
   dag.doc_md = """
       This is my first DAG in airflow.
       I can write documentation in Markdown here with **bold text** or __bold text__.
   """


   def launch_task(**kwargs):
       print("Hello Airflow - This is Task with task_number:", kwargs['task_number'])
       print("kwargs", kwargs)
       print("kwargs['conf']", kwargs["conf"])
       print("kwargs['dag_run']", kwargs["dag_run"])
       print("kwargs['dag_run'].conf", kwargs["dag_run"].conf)

   tasks = []
   for i in range(6):
       task = PythonOperator(
           task_id='task' + str(i),
           python_callable=launch_task,
           provide_context=True,
           op_kwargs={'task_number': 'task' + str(i)}
       )
       tasks.append(task)
       if i > 0:
           tasks[i - 1].set_downstream(tasks[i])
