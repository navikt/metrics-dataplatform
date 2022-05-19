from datetime import date, timedelta
import pandas as pd
import numpy as np
import os

import pg8000
import re
import sqlalchemy
import pandas as pd
from google.cloud.sql.connector import connector


def read_secrets() -> None:
    from google.cloud import secretmanager
    secrets = secretmanager.SecretManagerServiceClient()

    resource_name = f"projects/knada-gcp/secrets/datamarkedsplassen-metrikker/versions/latest"
    secret = secrets.access_secret_version(name=resource_name)
    os.environ.update(dict(
        [line.split("=") for line in secret.payload.data.decode('UTF-8').splitlines()]))


def read_dataproducts_from_nada() -> pd.DataFrame:
    def getconn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector.connect(
            f"nada-prod-6977:europe-north1:nada-backend",
            "pg8000",
            user=os.environ["NAIS_DATABASE_NADA_BACKEND_NADA_USERNAME"],
            password=os.environ["NAIS_DATABASE_NADA_BACKEND_NADA_PASSWORD"],
            db=os.environ["NAIS_DATABASE_NADA_BACKEND_NADA_DATABASE"],
        )
        return conn

    engine = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )

    df_dp = pd.read_sql("SELECT id, name FROM dataproducts", engine)

    df_dp.rename(columns={
        "id": "dataproduct_id",
        "name": "dataproduct"
    }, inplace=True)

    df_ds = pd.read_sql(
        "SELECT dataproduct_id, project_id, dataset, created, table_name FROM datasource_bigquery", engine)

    engine.dispose()

    df_nada = df_dp.merge(df_ds, on="dataproduct_id")
    df_nada["table_uri"] = df_nada.apply(
        lambda row: f"{row['project_id']}.{row['dataset']}.{row['table_name']}", axis=1)

    return df_nada


def read_audit_log_data() -> pd.DataFrame:
    yesterday = date.today() - timedelta(days=1)

    insert_job_query = f"""
    SELECT
        resource.labels.project_id,
        protopayload_auditlog.authenticationInfo.principalEmail,
        timestamp,
        REGEXP_EXTRACT(protopayload_auditlog.metadataJson, 'projects/([^/]+)/jobs/.+') as job_project_id,
        JSON_EXTRACT(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobConfig.queryConfig.query') as sql_query,
        JSON_EXTRACT(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobName') as job_name,
    FROM `nada-prod-6977.bigquery_audit_logs_org.cloudaudit_googleapis_com_data_access`
    WHERE protopayload_auditlog.methodName = 'google.cloud.bigquery.v2.JobService.InsertJob'
    AND protopayload_auditlog.status IS NULL
    AND JSON_VALUE(protopayload_auditlog.metadataJson,'$.jobInsertion.job.jobConfig.queryConfig.statementType') = 'SELECT'
    AND DATE(timestamp) = "{yesterday}"
    """
    df_insert = pd.read_gbq(
        insert_job_query, project_id='nada-prod-6977', location='europe-north1')
    df_insert = df_insert[~df_insert["sql_query"].isna()]
    df_insert.drop_duplicates(subset=["job_name"], inplace=True)

    query_job_query = f"""
    SELECT 
        resource.labels.project_id,
        protopayload_auditlog.authenticationInfo.principalEmail,
        timestamp,
        REGEXP_EXTRACT(protopayload_auditlog.metadataJson, 'projects/([^/]+)/jobs/.+') as job_project_id,
        JSON_EXTRACT(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobConfig.queryConfig.query') as sql_query,
        JSON_EXTRACT(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobName') as job_name,
    FROM `nada-prod-6977.bigquery_audit_logs_org.cloudaudit_googleapis_com_data_access`
    WHERE protopayload_auditlog.methodName = 'google.cloud.bigquery.v2.JobService.Query'
    AND protopayload_auditlog.status IS NULL
    AND DATE(timestamp) = "{yesterday}"
    """
    df_query = pd.read_gbq(
        query_job_query, project_id='nada-prod-6977', location='europe-north1')
    df_query = df_query[~df_query["sql_query"].isna()]
    df_query.drop_duplicates(subset=["job_name"], inplace=True)

    df_audit_raw = df_query.append(df_insert).reset_index()
    df_audit_raw.drop(columns=["index"], inplace=True)
    df_audit_raw["table_uris"] = df_audit_raw.apply(
        extract_dataset_and_table, axis=1)

    # Trenger å splitte ut og lage flere rader når et query har bestått av lesinger fra flere tabeller
    # Dette kan sikkert gjøres bedre
    df_audit = pd.DataFrame()
    for _, row in df_audit_raw.iterrows():
        for table in row["table_uris"]:
            row_copy = row.copy()
            row_copy["table_uri"] = table
            df_audit = df_audit.append(row_copy)

    df_audit = df_audit.reset_index()
    df_audit["metabase"] = df_audit["sql_query"].apply(
        lambda query: True if query.startswith("\"-- Metabase") else False)
    df_audit["service_account"] = df_audit["principalEmail"].apply(
        lambda email: True if ".gserviceaccount.com" in email else False)
    df_audit['week'] = df_audit['timestamp'].dt.isocalendar().week.astype(str)
    df_audit['year'] = df_audit['timestamp'].dt.isocalendar().year.astype(str)
    df_audit['date'] = df_audit['timestamp'].dt.date.astype(str)

    df_audit.drop(columns=["table_uris",
                           "project_id", "index"], inplace=True)

    return df_audit


def extract_dataset_and_table(row):
    tables = []
    query = row["sql_query"]
    regex_pattern = "(FROM|JOIN|from|join)(\s+`?)(\w|-|`|\.)+`?"
    matched = True
    while matched:
        try:
            table = re.search(regex_pattern, r"{}".format(query))[0].strip()
            table_orig = table
            # noen ganger mangler prosjektid i querien, så da legges den på her
            if len(table.split(".")) < 3:
                table = f"{row['project_id']}.{table}"
            tables.append(table.replace("`", "")
                          .replace(" ", "")
                          .replace("FROM", "")
                          .replace("JOIN", "")
                          .replace("from", "")
                          .replace("join", ""))
            query = query.replace(table_orig, '')
        except TypeError:  # Which is the case when re.search returns None
            matched = False

    return tables


def merge_nada_and_audit_logs(df_nada: pd.DataFrame, df_audit: pd.DataFrame) -> pd.DataFrame:
    df_stage = df_audit.merge(df_nada, how="inner", on="table_uri")

    df_stage.rename(columns={
        "principalEmail": "user",
        "timestamp": "query_timestamp",
        "created": "dp_created"
    }, inplace=True)

    df_stage['intra_team'] = False
    df_stage.loc[df_stage['project_id'] ==
                 df_stage['job_project_id'], 'intra_team'] = True

    df_stage['source'] = df_stage['project_id'].str.split(
        '-').apply(lambda x: "-".join(x[:-2]))
    df_stage['target'] = df_stage['job_project_id'].str.split('-').str.get(0)
    df_stage = df_stage.astype({"dataproduct_id": str})

    df_stage = df_stage[["user",
                         "service_account",
                         "metabase",
                         "dataproduct_id",
                         "dataproduct",
                         "dp_created",
                         "query_timestamp",
                         "year",
                         "week",
                         "date",
                         "intra_team",
                         "job_name",
                         "job_project_id",
                         "project_id",
                         "dataset",
                         "table_name",
                         "table_uri",
                         "sql_query",
                         "source",
                         "target"]]

    return df_stage


def publish(df_stage: pd.DataFrame) -> None:
    df_stage.to_gbq(project_id="nada-prod-6977",
                    destination_table="nada-prod-6977.bq_metrics_datamarkedsplassen.stage",
                    if_exists='append',
                    location='europe-north1')


if __name__ == "__main__":
    if os.environ["COMPOSER_LAND"]:
        read_secrets()
    df_nada = read_dataproducts_from_nada()
    df_audit = read_audit_log_data()
    df_stage = merge_nada_and_audit_logs(df_nada=df_nada, df_audit=df_audit)
    publish(df_stage)
