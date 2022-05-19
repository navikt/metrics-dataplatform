from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators import kubernetes_pod_operator

TASK_MAX_RETRIES = 2
TASK_RETRY_DELAY = timedelta(seconds=5)
TASK_STARTUP_TIMEOUT = 360
DELETE_POD_ON_COMPLETED = True
IMAGE = "ghcr.io/navikt/metrics-dataplatform:36ba92ddf6b159a3d8cda0c280818db6c1b4e1eb"
ENVS = {
    "COMPOSER_LAND": "true"
}

with DAG('metrikker-datamarkedsplassen',
         start_date=datetime(year=2022, month=2, day=26, hour=14),
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
        startup_timeout_seconds=TASK_STARTUP_TIMEOUT,
        is_delete_operator_pod=DELETE_POD_ON_COMPLETED,
    )

    dataproducts = kubernetes_pod_operator.KubernetesPodOperator(
        task_id="stage",
        name="stage",
        cmds=["python", "scripts/stage_to_dp.py"],
        env_vars=ENVS,
        image=IMAGE,
        retries=TASK_MAX_RETRIES,
        retry_delay=TASK_RETRY_DELAY,
        startup_timeout_seconds=TASK_STARTUP_TIMEOUT,
        is_delete_operator_pod=DELETE_POD_ON_COMPLETED,
    )

    stage >> dataproducts
