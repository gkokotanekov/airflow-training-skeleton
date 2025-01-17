from datetime import timedelta, datetime

import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator


def _print_weekday(**context):
    print(context["execution_date"].weekday())


def return_branch(**context):
    branches = {1: 'email_bob', 2: 'email_alice', 3: 'email_joe', 4: 'email_joe', 5: 'email_alice', 6: 'email_joe', 0: 'email_bob'}
    return branches[context["execution_date"].weekday()]


# noinspection PyUnresolvedReferences
args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(5),
    'dagrun_timeout': timedelta(minutes=60)
}

dag = DAG(
    dag_id='example06',
    default_args=args,
    # schedule_interval='0 0 * * *' ## every day
    schedule_interval='@daily' ## every day
    # schedule_interval='45 13 * *  MON,WED,FRI' ## every day
    # schedule_interval= timedelta(hours=2, minutes=30)  ## every 2.5 hours; difficult in crone

)

print_weekday = PythonOperator(
    task_id="print_weekday",
    python_callable=_print_weekday,
    provide_context=True,
    dag=dag,
)

final_task = DummyOperator(task_id="final_task",
                          dag=dag)

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=return_branch,
    provide_context=True,
    dag=dag)

people = ['bob', 'alice', 'joe']
emails_tasks_array = []
for i in people:
    email = DummyOperator(
        task_id='email_' + str(i),
        dag=dag,
    )
    emails_tasks_array.append(email)

    # branching >> email >> final_task


print_weekday >> branching >> emails_tasks_array >> final_task

