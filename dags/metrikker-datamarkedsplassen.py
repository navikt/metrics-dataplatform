from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators import kubernetes_pod_operator

TASK_MAX_RETRIES = 2
TASK_RETRY_DELAY = timedelta(seconds=5)
TASK_STARTUP_TIMEOUT = 360
DELETE_POD_ON_COMPLETED = True
IMAGE = "navikt/nada-metrics:b8f38c698babe0f71be2faed160be2b76ffef6a2"
ENVS = {
    "COMPOSER_LAND": "true",
    "GCP_PROJECT": "nada-prod-6977",
    "GSM_SECRET": "datamarkedsplassen-metrikker",
    "CLOUD_SQL_INSTANCE": "nada-backend",
    "AUDIT_LOG_TABLE": "nada-prod-6977.bigquery_audit_logs_org.cloudaudit_googleapis_com_data_acces",
    "STAGE_TABLE": "nada-prod-6977.bq_metrics_datamarkedsplassen.stage",
    "DATAPRODUCTS_TABLE": "nada-prod-6977.bq_metrics_datamarkedsplassen.dataproducts"
}

with DAG('metrikker-datamarkedsplassen',
         start_date=datetime(year=2022, month=5, day=20),
         schedule_interval="0 5 * * *",
         max_active_runs=1,
         catchup=False) as dag:

    stage = kubernetes_pod_operator.KubernetesPodOperator(
        task_id="stage",
        name="stage",
        cmds=["python", "scripts/source_stage.py"],
        env_vars=ENVS,
        image=IMAGE,
        retries=TASK_MAX_RETRIES,
        retry_delay=TASK_RETRY_DELAY,
        namespace="default",
        startup_timeout_seconds=TASK_STARTUP_TIMEOUT,
        is_delete_operator_pod=False,
    )

    dataproducts = kubernetes_pod_operator.KubernetesPodOperator(
        task_id="dataproducts",
        name="dataproducts",
        cmds=["python", "scripts/stage_to_dp.py"],
        env_vars=ENVS,
        image=IMAGE,
        retries=TASK_MAX_RETRIES,
        retry_delay=TASK_RETRY_DELAY,
        namespace="default",
        startup_timeout_seconds=TASK_STARTUP_TIMEOUT,
        is_delete_operator_pod=DELETE_POD_ON_COMPLETED,
    )

    stage >> dataproducts
