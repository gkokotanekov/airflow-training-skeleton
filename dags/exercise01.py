from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='example01',
    default_args=args
)

t1 = DummyOperator(task_id="do_sth_01", dag=dag)
t2 = DummyOperator(task_id="do_sth_02", dag=dag)
t3 = DummyOperator(task_id="do_sth_03", dag=dag)
t4 = DummyOperator(task_id="do_sth_04", dag=dag)
t5 = DummyOperator(task_id="do_sth_05", dag=dag)

t1 >> t2 >> [t3, t4] >> t5

