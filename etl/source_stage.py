import pandas as pd

from transformations import extract_tables_from_query


def run_etl_general():
    query = """
    SELECT 
        resource.labels.dataset_id, 
        resource.labels.project_id, 
        SPLIT(REGEXP_EXTRACT(protopayload_auditlog.resourceName, '^projects/[^/]+/datasets/[^/]+/tables/(.*)$'), '$')[OFFSET(0)] AS table,
        protopayload_auditlog.authenticationInfo.principalEmail,
        timestamp,
        REGEXP_EXTRACT(protopayload_auditlog.metadataJson, 'projects/([^/]+)/jobs/.+') as job_project_id,
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.tableDataRead.reason') as reason,
        JSON_EXTRACT(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.queryConfig.query') as sql_query
    FROM `nada-prod-6977.bigquery_audit_logs.cloudaudit_googleapis_com_data_access` 
    """
    df = pd.read_gbq(query)

    # Extracting the team names for the producers and the consumers
    df['source'] = df['project_id'].str.split('-').str.get(0)
    df['target'] = df['job_project_id'].str.split('-').str.get(0)

    # Typically want to evaluate performance per week (?)
    df['year'] = df['timestamp'].dt.isocalendar().year
    df['week'] = df['timestamp'].dt.isocalendar().week

    # For measuring the network effect: How much of the conusmption is "outside" the team?
    df['intra_team'] = False
    df.loc[df['source'] == df['target'], 'intra_team'] = True

    # Since unique users is misguiding us in case there is a large share of reads from service users
    df['service'] = True
    df.loc[df['principalEmail'].str.contains('@nav.no'), 'service'] = False

    # Not sure if this makes sense
    df['tables_in_query'] = df['sql_query'].apply(lambda x: extract_tables_from_query(x))

    # Need to load the data to a staging layer


    return None


def run_etl_platform():
    return None



def run_etl_producer():
    return None