from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.operators.dummy_operator import DummyOperator
from operators.gcp_container_operator import GKEPodOperator

default_args = {
    "owner": "elin@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2018, 11, 26),
    "email": ["elin@mozilla.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=30),
}


def taipei_etl(
    task_id,
    gcp_conn_id="google_cloud_derived_datasets",
    location="us-central1-a",
    cluster_name="bq-load-gke-1",
    name="tapei-etl",
    namespace="default",
    image="gcr.io/rocket-dev01/taipei-bi-etl",
    image_pull_policy="Always",
    arguments=[],
    **kwargs,
):
    return GKEPodOperator(
        task_id=task_id,
        gcp_conn_id=gcp_conn_id,
        project_id=GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id).project_id,
        location=location,
        cluster_name=cluster_name,
        name=name,
        namespace=namespace,
        image=image,
        image_pull_policy=image_pull_policy,
        arguments=[
            "--debug",
            "--date",
            "{{ds}}",
            "--next_execution_date",
            "{{next_execution_date}}",
        ]
        + arguments,
        **kwargs,
    )


with DAG("taipei_etl", default_args=default_args, schedule_interval=None) as dag:

    gcp_conn_id = "google_cloud_derived_datasets"
    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

    mango_events = taipei_etl(
        "mango_events",
        arguments=["--task", "bigquery", "--subtask", "mango_events"],
        dag=dag,
    )

    mango_events_unnested = taipei_etl(
        "mango_events_unnested",
        arguments=["--task", "bigquery", "--subtask", "mango_events_unnested"],
        dag=dag,
    )

    mango_events_feature_mapping = taipei_etl(
        "mango_events_feature_mapping",
        arguments=["--task", "bigquery", "--subtask", "mango_events_feature_mapping"],
        dag=dag,
    )

    channel_mapping = taipei_etl(
        "channel_mapping",
        arguments=["--task", "bigquery", "--subtask", "channel_mapping"],
        dag=dag,
    )

    user_channels = taipei_etl(
        "user_channels",
        arguments=["--task", "bigquery", "--subtask", "user_channels"],
        dag=dag,
    )

    mango_events >> mango_events_unnested >> mango_events_feature_mapping
    mango_events >> user_channels
    channel_mapping >> user_channels
