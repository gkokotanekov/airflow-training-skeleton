from datetime import timedelta, datetime

import airflow
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator


## bucket
## gkokotanekov_airflow_training

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
    dag_id='example07',
    default_args=args,
    # schedule_interval='0 0 * * *' ## every day
    schedule_interval='@daily' ## every day
    # schedule_interval='45 13 * *  MON,WED,FRI' ## every day
    # schedule_interval= timedelta(hours=2, minutes=30)  ## every 2.5 hours; difficult in crone

)


get_data = PostgresToGoogleCloudStorageOperator(
    task_id='get_data',
    filename= 'gdk_data',
    bucket = 'gkokotanekov_airflow_training',
    postgres_conn_id = 'gddconnection',
    google_cloud_storage_conn_id = 'google_cloud_storage_default',
    sql = 'select transfer_date FROM land_registry_price_paid_uk WHERE transfer_date = "{{ execution_date.strftime("%d-%m-%Y") }}"',
    dag=dag)
