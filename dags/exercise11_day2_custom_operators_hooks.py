import json
import os
import pathlib
import posixpath
import tempfile

import airflow
import requests
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.base_hook import BaseHook
# from airflow.hooks.http_hook import HttpHook
from airflow.models import DAG, BaseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults



# class LaunchHook(HttpHook):
from sys import api_version


class LaunchHook(BaseHook):

    base_url = 'https://launchlibrary.net'

    def __init__(self, conn_id):
        super().__init__(source=None)
        self._conn_id = conn_id
        self._api_version = api_version
        self._conn = None

    def get_conn(self):
        if self._conn is None:
            self._conn = requests.Session()  # create connection instance here
        return self._conn

    def get_launches(self, start_date, end_date):
        session = self.get_conn()
        response = session.get(
            "{self.base_url}/{self._api_version}/launches",
            params = {"start_date": start_date, "end_date" : end_date }
        )
        response.raise_for_status()
        return response.json()["launches"]

        # session.do_stuff(# perform some action...)


# def _download_rocket_launches(ds, tomorrow_ds, **context):
#     query = f"https://launchlibrary.net/1.4/launch?startdate={ds}&enddate={tomorrow_ds}"
#     result_path = f"/data/rocket_launches/ds={ds}"
#     pathlib.Path(result_path).mkdir(parents=True, exist_ok=True)
#     response = requests.get(query)
#     with open(posixpath.join(result_path, "launches.json"), "w") as f:
#         f.write(response.text)


# noinspection PyUnresolvedReferences
args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(10)}

dag = DAG(
    dag_id="download_rocket_launches02",
    default_args=args,
    description="DAG downloading rocket launches from Launch Library.",
    schedule_interval="0 0 * * *",
)


class LaunchToGcsOperator(BaseOperator):

    @apply_defaults
    def __init__(self, start_date, output_bucket, output_path, end_date = None,
                 launch_conn_id = None, gcp_conn_id = "google_cloud_default" , **kwargs):
        super().__init__(*args, **kwargs)
        self._output_bucket = output_bucket
        self._output_path = output_path

        self._start_date = start_date
        self._end_date = end_date

        self._launch_conn_id = launch_conn_id
        self._gcp_conn_id = gcp_conn_id


    def execute(self, context):

        self.log.info("Fetching log date")
        launch_hook = LaunchHook(conn_id=self._launch_conn_id)
        result = launch_hook.get_launches(
            start_date=self._start_date,
            end_date=self._end_date
        )

        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self._gcp_conn_id
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = os.path.join(tmp_dir, "result.json")
            with open(tmp_path, "w") as file_:
                json.dump(result, file_)

            gcs_hook.upload(
                bucket=self._output_bucket,
                object=self._output_path,
                filename=tmp_path
            )

# def _print_stats(ds, **context):
#     with open(f"/data/rocket_launches/ds={ds}/launches.json") as f:
#         data = json.load(f)
#         rockets_launched = [launch["name"] for launch in data["launches"]]
#         rockets_str = ""
#         if rockets_launched:
#             rockets_str = f" ({' & '.join(rockets_launched)})"
#             print(f"{len(rockets_launched)} rocket launch(es) on {ds}{rockets_str}.")



get_stats = LaunchToGcsOperator(
    start_date = '2019-12-08',
    output_bucket = 'gkokotanekov_airflow_training',
    output_path = 'stats_data{{ds}}_{}',
    end_date = '2019-12-10',
    # launch_conn_id = ''
    provide_context=True,
    dag=dag,
)


# download_rocket_launches = PythonOperator(
#     task_id="download_rocket_launches",
#     python_callable=_download_rocket_launches,
#     provide_context=True,
#     dag=dag,
# )


# print_stats = PythonOperator(
#     task_id="print_stats", python_callable=_print_stats, provide_context=True, dag=dag
# )

# get_stats >> print_stats
