from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.kubernetes.secret import Secret
from airflow.operators.dummy_operator import DummyOperator
from operators.gcp_container_operator import GKEPodOperator

default_args = {
    "owner": "elin@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 9, 1),
    "email": ["elin@mozilla.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def taipei_etl(
    task_id,
    gcp_conn_id="google_cloud_derived_datasets",    # same one in airflow web UI(composer) admin -> connection setting
    location="us-central1-a",                       # same location as the GKE cluster
    cluster_name="bq-load-gke-1",                   # same name when creating GKE cluster
    name="taipei-etl",                               # used for kubernetes job ID, no need to change
    namespace="default",                            # same namespace when creating GKE cluster
    image="gcr.io/taipei-bi/taipei-bi-etl",         # reference the image tag you pushed to GCR
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
        startup_timeout_seconds=300,
        arguments=[
            "--config",
            "staging",     # MUST verify
            "--date",
            "{{ds}}",
            "--next_execution_date",
            "{{next_execution_date}}",
        ]
        + arguments,
        **kwargs,
    )


with DAG(
    "taipei_etl-staging",   # used in airflow web ui/command line to identify tasks, can have multiple DAGs in one python file
    catchup=False,
    default_args=default_args,
    schedule_interval="0 23 * * *",
) as dag:

    gcp_conn_id = "google_cloud_derived_datasets"
    connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

    adjust = taipei_etl(
        "adjust",
        arguments=["--task", "adjust"],
        dag=dag,
        secrets=[Secret("env", "ADJUST_API_KEY", "adjust-api-key", "ADJUST_API_KEY")],
        depends_on_past=True,
        # Secret("env", {local env var name}, {secret name}, {env var name in secret})
    )

    mango_core = taipei_etl(
        "mango_core",
        arguments=["--task", "bigquery", "--subtask", "mango_core"],
        dag=dag,
    )

    mango_core_normalized = taipei_etl(
        "mango_core_normalized",
        arguments=["--task", "bigquery", "--subtask", "mango_core_normalized"],
        dag=dag,
    )

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
        depends_on_past=True,
    )

    mango_user_channels = taipei_etl(
        "mango_user_channels",
        arguments=["--task", "bigquery", "--subtask", "mango_user_channels"],
        dag=dag,
        depends_on_past=True,
    )

    mango_user_rfe_partial = taipei_etl(
        "mango_user_rfe_partial",
        arguments=["--task", "bigquery", "--subtask", "mango_user_rfe_partial"],
        dag=dag,
    )

    mango_user_rfe_session = taipei_etl(
        "mango_user_rfe_session",
        arguments=["--task", "bigquery", "--subtask", "mango_user_rfe_session"],
        dag=dag,
        depends_on_past=True,
    )

    mango_user_rfe = taipei_etl(
        "mango_user_rfe",
        arguments=["--task", "bigquery", "--subtask", "mango_user_rfe"],
        dag=dag,
    )

    mango_feature_cohort_date = taipei_etl(
        "mango_feature_cohort_date",
        arguments=["--task", "bigquery", "--subtask", "mango_feature_cohort_date"],
        dag=dag,
        depends_on_past=True,
    )
    #deprecated
    mango_user_occurrence = DummyOperator(
        task_id="mango_user_occurrence",
        # arguments=["--task", "bigquery", "--subtask", "mango_user_occurrence"],
        dag=dag,
    )
    #merge with user_occurrence
    mango_user_feature_occurrence = taipei_etl(
        "mango_user_feature_occurrence",
        arguments=["--task", "bigquery", "--subtask", "mango_user_feature_occurrence"],
        dag=dag,
    )

    mango_cohort_user_occurrence = taipei_etl(
        "mango_cohort_user_occurrence",
        arguments=["--task", "bigquery", "--subtask", "mango_cohort_user_occurrence"],
        dag=dag,
    )

    mango_cohort_retained_users = taipei_etl(
        "mango_cohort_retained_users",
        arguments=["--task", "bigquery", "--subtask", "mango_cohort_retained_users"],
        dag=dag,
        depends_on_past=True,
    )

    #deprecated
    mango_feature_active_new_user_count = DummyOperator(
        task_id="mango_feature_active_new_user_count",
        # arguments=[
        #     "--task",
        #     "bigquery",
        #     "--subtask",
        #     "mango_feature_active_new_user_count",
        # ],
        dag=dag,
    )

    #deprecated, merge with active new user count
    mango_feature_active_user_count = DummyOperator(
        task_id="mango_feature_active_user_count",
        # arguments=[
        #     "--task",
        #     "bigquery",
        #     "--subtask",
        #     "mango_feature_active_user_count",
        # ],
        dag=dag,
    )

    mango_active_user_count = taipei_etl(
        "mango_active_user_count",
        arguments=[
            "--task",
            "bigquery",
            "--subtask",
            "mango_active_user_count",
        ],
        dag=dag,
        depends_on_past=True,
    )

    mango_feature_roi = taipei_etl(
        "mango_feature_roi",
        arguments=["--task", "bigquery", "--subtask", "mango_feature_roi"],
        dag=dag,
    )

    mango_channel_roi = taipei_etl(
        "mango_channel_roi",
        arguments=["--task", "bigquery", "--subtask", "mango_channel_roi"],
        dag=dag,
    )

    mango_revenue_google = taipei_etl(
        "mango_revenue_google",
        arguments=["--task", "bigquery", "--subtask", "mango_revenue_google"],
        dag=dag,
    )

    [mango_events, mango_channel_mapping] >> mango_user_channels

    mango_events >> mango_events_unnested >> mango_events_feature_mapping
    adjust >> mango_channel_mapping

    mango_events_feature_mapping >> [
        mango_user_rfe_session,
        mango_user_rfe_partial,
    ] >> mango_user_rfe
    mango_events >> mango_user_rfe
    mango_user_channels >> mango_user_rfe

    mango_core >> mango_core_normalized
    mango_feature_cohort_date >> mango_user_rfe_partial
    mango_core_normalized >> [
        mango_user_rfe_session,
        mango_revenue_google,
    ]

    mango_events_feature_mapping >> mango_feature_cohort_date >> mango_user_feature_occurrence
    mango_events_feature_mapping >> mango_user_feature_occurrence

    [
        mango_user_feature_occurrence,
        mango_user_channels,
    ] >> mango_cohort_user_occurrence

    mango_cohort_user_occurrence >> mango_cohort_retained_users

    mango_cohort_user_occurrence >> mango_active_user_count

    [
        mango_user_rfe,
        mango_cohort_retained_users,
        mango_active_user_count,
    ] >> mango_feature_roi

    [
        mango_user_rfe,
        mango_cohort_retained_users,
        mango_active_user_count,
    ] >> mango_channel_roi
