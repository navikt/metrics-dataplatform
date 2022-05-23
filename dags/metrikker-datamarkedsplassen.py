import os
from re import S
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

PIPELINE_NAME = "metrikker-datamarkedsplassen"
SLACK_CHANNEL = "#nada-composer-info"
TASK_MAX_RETRIES = 2
TASK_RETRY_DELAY = timedelta(seconds=5)
TASK_STARTUP_TIMEOUT = 360
DELETE_POD_ON_COMPLETED = True
IMAGE = "navikt/nada-metrics:3228e9ba94b4fd29024514ce72984abac346b830"
ENVS = {
    "COMPOSER_LAND": "true",
    "GCP_PROJECT": "nada-prod-6977",
    "GSM_SECRET": "datamarkedsplassen-metrikker",
    "CLOUD_SQL_INSTANCE": "nada-backend",
    "AUDIT_LOG_TABLE": "nada-prod-6977.bigquery_audit_logs_org.cloudaudit_googleapis_com_data_access",
    "STAGE_TABLE": "nada-prod-6977.bq_metrics_datamarkedsplassen.stage",
    "DATAPRODUCTS_TABLE": "nada-prod-6977.bq_metrics_datamarkedsplassen.dataproducts"
}


def on_task_error(context):
    slack_notification = SlackWebhookOperator(
        task_id="airflow_task_failed",
        http_conn_id=None,
        webhook_token=os.environ["SLACK_WEBHOOK_TOKEN"],
        message=f"@here DAG {PIPELINE_NAME} feilet kl. {datetime.now().isoformat()}.",
        channel=SLACK_CHANNEL,
        link_names=True,
        icon_emoji=":sadpanda:",
    )
    slack_notification.execute(context)


with DAG(PIPELINE_NAME,
         start_date=datetime(year=2022, month=5, day=20),
         schedule_interval="0 5 * * *",
         max_active_runs=1,
         catchup=False) as dag:

    start_notify = SlackWebhookOperator(
        dag=dag,
        http_conn_id=None,
        task_id="slack-start-notification",
        webhook_token=os.environ["SLACK_WEBHOOK_TOKEN"],
        message=f"Pipeline _*{PIPELINE_NAME}*_ startet",
        channel=SLACK_CHANNEL,
        link_names=True,
        icon_emoji=":dataverk:",
        retries=2,
    )

    stage = kubernetes_pod_operator.KubernetesPodOperator(
        task_id="stage",
        name="stage",
        cmds=["python", "scripts/source_stage.py"],
        env_vars=ENVS,
        image=IMAGE,
        retries=TASK_MAX_RETRIES,
        retry_delay=TASK_RETRY_DELAY,
        namespace="composer-2-0-13-airflow-2-2-5-3b8c2b10",
        startup_timeout_seconds=TASK_STARTUP_TIMEOUT,
        on_failure_callback=on_task_error,
        is_delete_operator_pod=DELETE_POD_ON_COMPLETED,
    )

    dataproducts = kubernetes_pod_operator.KubernetesPodOperator(
        task_id="dataproducts",
        name="dataproducts",
        cmds=["python", "scripts/stage_to_dp.py"],
        env_vars=ENVS,
        image=IMAGE,
        retries=TASK_MAX_RETRIES,
        retry_delay=TASK_RETRY_DELAY,
        namespace="composer-2-0-13-airflow-2-2-5-3b8c2b10",
        startup_timeout_seconds=TASK_STARTUP_TIMEOUT,
        on_failure_callback=on_task_error,
        is_delete_operator_pod=DELETE_POD_ON_COMPLETED,
    )

    success_notify = SlackWebhookOperator(
        dag=dag,
        http_conn_id=None,
        task_id="success_notification",
        webhook_token=os.environ["SLACK_WEBHOOK_TOKEN"],
        message=f"Vellykket kjøring av pipeline _*{PIPELINE_NAME}*_",
        channel=SLACK_CHANNEL,
        link_names=True,
        icon_emoji=":dataverk:",
        retries=3,
    )

    start_notify >> stage >> dataproducts >> success_notify
