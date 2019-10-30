from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.operators.dummy_operator import DummyOperator
from operators.gcp_container_operator import GKEPodOperator

default_args = {
    "owner": "elin@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 9, 1),
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


with DAG(
    "taipei_etl",
    default_args=default_args,
    schedule_interval="0 23 * * *",
    catchup=True,
) as dag:

    gcp_conn_id = "google_cloud_derived_datasets"
    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

    adjust = taipei_etl("adjust", arguments=["--task", "adjust"], dag=dag)

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

    mango_channel_mapping = taipei_etl(
        "mango_channel_mapping",
        arguments=["--task", "bigquery", "--subtask", "mango_channel_mapping"],
        dag=dag,
    )

    mango_user_channels = taipei_etl(
        "mango_user_channels",
        arguments=["--task", "bigquery", "--subtask", "mango_user_channels"],
        dag=dag,
    )

    [mango_events, mango_channel_mapping] >> mango_user_channels

    mango_events >> mango_events_unnested >> mango_events_feature_mapping
    adjust >> mango_channel_mapping
