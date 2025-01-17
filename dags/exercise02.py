from datetime import timedelta, datetime

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


args = {
    'owner': 'Airflow',
    'start_date': datetime(2019, 12, 1),
    'dagrun_timeout': timedelta(minutes=60)
}

dag = DAG(
    dag_id='example03',
    default_args=args,
    # schedule_interval='0 0 * * *' ## every day
    # schedule_interval='@daily' ## every day
    schedule_interval='45 13 * *  MON,WED,FRI' ## every day
    # schedule_interval= timedelta(hours=2, minutes=30)  ## every 2.5 hours; difficult in crone

)

t1 = DummyOperator(task_id="do_sth_01", dag=dag)
t2 = DummyOperator(task_id="do_sth_02", dag=dag)
t3 = DummyOperator(task_id="do_sth_03", dag=dag)
t4 = DummyOperator(task_id="do_sth_04", dag=dag)
t5 = DummyOperator(task_id="do_sth_05", dag=dag)

t1 >> t2 >> [t3, t4] >> t5

