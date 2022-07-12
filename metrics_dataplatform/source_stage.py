import pandas as pd
import os
import re
from nada_backend import read_dataproducts_from_nada


def read_audit_log_data(time_range) -> pd.DataFrame:
    insert_job_query = f"""
    SELECT
        resource.labels.project_id,
        protopayload_auditlog.authenticationInfo.principalEmail,
        timestamp,
        REGEXP_EXTRACT(protopayload_auditlog.metadataJson, 'projects/([^/]+)/jobs/.+') as job_project_id,
        JSON_EXTRACT(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobConfig.queryConfig.query') as sql_query,
        JSON_EXTRACT(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobName') as job_name,
    FROM `{os.environ["SOURCE_AUDIT_LOGS_TABLE"]}`
    WHERE protopayload_auditlog.methodName = 'google.cloud.bigquery.v2.JobService.InsertJob'
    AND protopayload_auditlog.status IS NULL
    AND JSON_VALUE(protopayload_auditlog.metadataJson,'$.jobInsertion.job.jobConfig.queryConfig.statementType') = 'SELECT'
    AND DATE(timestamp) BETWEEN {time_range}
    """
    df_insert = pd.read_gbq(
        insert_job_query, project_id=os.environ["GCP_TEAM_PROJECT_ID"], location='europe-north1')
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
    FROM `{os.environ["SOURCE_AUDIT_LOGS_TABLE"]}`
    WHERE protopayload_auditlog.methodName = 'google.cloud.bigquery.v2.JobService.Query'
    AND protopayload_auditlog.status IS NULL
    AND DATE(timestamp) BETWEEN {time_range}
    """
    df_query = pd.read_gbq(
        query_job_query, project_id=os.environ["GCP_TEAM_PROJECT_ID"], location='europe-north1')
    df_query = df_query[~df_query["sql_query"].isna()]
    df_query.drop_duplicates(subset=["job_name"], inplace=True)

    df_audit_raw = pd.concat([df_query, df_insert], ignore_index=True)
    df_audit_raw["table_uris"] = df_audit_raw.apply(
        extract_dataset_and_table, axis=1)

    # Splitter ut og lager flere rader når et query har bestått av lesinger fra flere tabeller
    df_audit = df_audit_raw.explode("table_uris", ignore_index=True).rename(columns={'table_uris': 'table_uri'})

    # Dropper rader uten tabeller (gjelder tilsynelatende bare scheduled queries)
    df_audit = df_audit[df_audit.table_uri.notna()]

    df_audit["metabase"] = df_audit["sql_query"].apply(
        lambda query: True if query.startswith("\"-- Metabase") else False)
    df_audit["service_account"] = df_audit["principalEmail"].apply(
        lambda email: True if ".gserviceaccount.com" in email else False)
    df_audit['week'] = df_audit['timestamp'].dt.isocalendar().week.astype(str)
    df_audit['year'] = df_audit['timestamp'].dt.isocalendar().year.astype(str)
    df_audit['date'] = df_audit['timestamp'].dt.date.astype(str)

    df_audit.drop(columns=["project_id"], inplace=True)

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
    df_stage.to_gbq(project_id=os.environ["GCP_TEAM_PROJECT_ID"],
                    destination_table=os.environ["STAGE_TABLE"],
                    if_exists='append',
                    location='europe-north1')

    print(f"Uploaded {len(df_stage)} rows to {os.environ['STAGE_TABLE']}")


def run_source_stage(time_range: str):
    df_nada = read_dataproducts_from_nada()
    df_audit = read_audit_log_data(time_range)
    df_stage = merge_nada_and_audit_logs(df_nada=df_nada, df_audit=df_audit)
    publish(df_stage)
