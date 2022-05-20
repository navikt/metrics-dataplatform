import uuid
import pandas as pd

from .transformations import extract_tables_from_query


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
    FROM `nada-prod-6977.bigquery_audit_logs_org.cloudaudit_googleapis_com_data_access`
    """
    df = pd.read_gbq(query, project_id='nada-prod-6977', location='europe-north1')

    # Behandler datetimes for groupby i senere del av flyt
    df['week'] = df['timestamp'].dt.isocalendar().week.astype(str)
    df['year'] = df['timestamp'].dt.isocalendar().year.astype(str)
    df['date'] = df['timestamp'].dt.date.astype(str)

    # Since unique users is misguiding us in case there is a large share of reads from service users
    df['service'] = True
    df.loc[df['principalEmail'].str.contains('@nav.no'), 'service'] = False

    # Anonymize the principalEmail
    df_right = pd.DataFrame([{'principalEmail': mail, 'identifier_anonym': str(uuid.uuid4())} for mail in df['principalEmail'].unique()])
    df = df.merge(df_right, how='left', on=['principalEmail'])
    df.drop('principalEmail', axis=1, inplace=True)

    # Extracting the team names for the producers and the consumers
    df['source'] = df['project_id'].str.split('-').apply(lambda x: "-".join(x[:-2]))
    df['target'] = df['job_project_id'].str.split('-').str.get(0)

    # For measuring the network effect: How much of the conusmption is "outside" the team?
    df['intra_team'] = False
    df.loc[df['source'] == df['target'], 'intra_team'] = True

    # Not sure if this makes sense
    df['tables_in_query'] = df['sql_query'].apply(lambda x: extract_tables_from_query(x))

    # Need to load the data to a staging layer
    project_id = 'nada-prod-6977'
    dataset = 'bq_metrics_org'

    DESTINATION_DATASET = f'{project_id}.{dataset}'
    destination_table = f'{DESTINATION_DATASET}.stage'

    df.to_gbq(
        project_id = project_id,
        destination_table=destination_table,
        if_exists='replace', 
        location='europe-north1'
    )

    return None

if __name__ == '__main__':
    run_etl_general()
