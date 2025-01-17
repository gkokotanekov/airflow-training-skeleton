from datetime import timedelta, datetime

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def _print_exec_date(**context):
    print(context["execution_date"])


# noinspection PyUnresolvedReferences
args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'dagrun_timeout': timedelta(minutes=60)
}

dag = DAG(
    dag_id='example04',
    default_args=args,
    # schedule_interval='0 0 * * *' ## every day
    schedule_interval='@daily' ## every day
    # schedule_interval='45 13 * *  MON,WED,FRI' ## every day
    # schedule_interval= timedelta(hours=2, minutes=30)  ## every 2.5 hours; difficult in crone

)

print_exec_date = PythonOperator(
    task_id="print_exec_date",
    python_callable=_print_exec_date,
    provide_context=True,
    dag=dag,
)

the_end = DummyOperator(task_id="the_end", dag=dag)

sleep_values = [5, 1, 10]
for i in sleep_values:
    wait = BashOperator(
        task_id='sleep_' + str(i),
        bash_command="sleep " + str(i),
        dag=dag,
    )


    print_exec_date >> wait >> the_end




