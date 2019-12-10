import airflow
from airflow.models import DAG

from operators.launch_operator import LaunchToGcsOperator


args = {
    "owner": "your name",
    "start_date": airflow.utils.dates.days_ago(14)
}

with DAG(
        dag_id="exercise-launch",
        default_args=args,
        schedule_interval="0 0 * * *"
) as dag:
    output_bucket = "airflow-training-data-jrderuiter"
    output_path = "launches/{{ ds }}.json"

    LaunchToGcsOperator(
        task_id="launch_to_gcs",
        start_date="{{ds}}",
        end_date="{{tomorrow_ds}}",
        output_bucket=output_bucket,
        output_path=output_path,
    )