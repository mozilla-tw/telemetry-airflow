from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

from operators.gcp_container_operator import GKEPodOperator  # noqa

gke_cluster_name = "bq-load-gke-1"

# Defined in Airflow's UI -> Admin -> Connections
gcp_conn_id = "google_cloud_derived_datasets"
connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

# Dataproc connection to GCP
gcpdataproc_conn_id = "google_cloud_airflow_dataproc"


taar_aws_conn_id = "airflow_taar_rw_s3"
taar_aws_access_key, taar_aws_secret_key, session = AwsHook(taar_aws_conn_id).get_credentials()

default_args = {
    "owner": "vng@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 7),
    "email": ["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG("taar_amodump", default_args=default_args, schedule_interval="@daily")

amodump = GKEPodOperator(
    task_id="taar_amodump",
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    location="us-central1-a",
    cluster_name=gke_cluster_name,
    name="taar-amodump",
    namespace="default",
    # This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
    image="gcr.io/moz-fx-data-airflow-prod-88e0/taar_gcp_etl:0.1",
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com", "hwoo@mozilla.com"],
    arguments=["-m", "taar_etl.taar_amodump", "--date", "{{ ds_nodash }}"],
    env_vars={
        "AWS_ACCESS_KEY_ID": taar_aws_access_key,
        "AWS_SECRET_ACCESS_KEY": taar_aws_secret_key,
    },
    dag=dag,
    is_delete_operator_pod=True,
)

amowhitelist = GKEPodOperator(
    task_id="taar_amowhitelist",
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    location="us-central1-a",
    cluster_name=gke_cluster_name,
    name="taar-amowhitelist",
    namespace="default",
    # This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
    image="gcr.io/moz-fx-data-airflow-prod-88e0/taar_gcp_etl:0.1",
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com", "hwoo@mozilla.com"],
    # We are extracting addons from the AMO server's APIs which don't
    # support date based queries, so no date parameter is required
    # here.
    arguments=["-m", "taar_etl.taar_amowhitelist"],
    env_vars={
        "AWS_ACCESS_KEY_ID": taar_aws_access_key,
        "AWS_SECRET_ACCESS_KEY": taar_aws_secret_key,
    },
    dag=dag,
    is_delete_operator_pod=True,
)

editorial_whitelist = GKEPodOperator(
    task_id="taar_update_whitelist",
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    location="us-central1-a",
    cluster_name=gke_cluster_name,
    name="taar-update-whitelist",
    namespace="default",
    # This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
    image="gcr.io/moz-fx-data-airflow-prod-88e0/taar_gcp_etl:0.1",
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com", "hwoo@mozilla.com"],
    arguments=["-m", "taar_etl.taar_update_whitelist", "--date", "{{ ds_nodash }}"],
    env_vars={
        "AWS_ACCESS_KEY_ID": taar_aws_access_key,
        "AWS_SECRET_ACCESS_KEY": taar_aws_secret_key,
    },
    dag=dag,
    is_delete_operator_pod=True,
)


# Set a dependency on amodump from amowhitelist
amowhitelist.set_upstream(amodump)

# Set a dependency on amodump for the editorial reviewed whitelist of
# addons
editorial_whitelist.set_upstream(amodump)
