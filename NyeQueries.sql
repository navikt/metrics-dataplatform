--- methodName: InsertJob
SELECT 
    resource.labels.dataset_id, 
    resource.labels.project_id, 
    protopayload_auditlog.authenticationInfo.principalEmail,
    timestamp,
    REGEXP_EXTRACT(protopayload_auditlog.metadataJson, 'projects/([^/]+)/jobs/.+') as job_project_id,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.tableDataRead.reason') as reason,
    JSON_EXTRACT(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobConfig.queryConfig.query') as sql_query,
    JSON_VALUE_ARRAY(protopayload_auditlog.metadataJson,'$.jobInsertion.job.jobStats.queryStats.referencedTables') as tables,
FROM `nada-prod-6977.bigquery_audit_logs_org.cloudaudit_googleapis_com_data_access`
WHERE protopayload_auditlog.methodName = 'google.cloud.bigquery.v2.JobService.InsertJob'
AND protopayload_auditlog.status IS NULL
AND JSON_VALUE(protopayload_auditlog.metadataJson,'$.jobInsertion.job.jobConfig.queryConfig.statementType') = 'SELECT'
AND JSON_VALUE(protopayload_auditlog.metadataJson,'$.jobInsertion.job.jobStatus.jobState') = 'DONE'
AND DATE(timestamp) = "2022-05-9"

--- methodName: Query
SELECT 
    resource.labels.dataset_id, 
    resource.labels.project_id, 
    protopayload_auditlog.authenticationInfo.principalEmail,
    timestamp,
    REGEXP_EXTRACT(protopayload_auditlog.metadataJson, 'projects/([^/]+)/jobs/.+') as job_project_id,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.tableDataRead.reason') as reason,
    JSON_EXTRACT(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobConfig.queryConfig.query') as sql_query,
    JSON_VALUE_ARRAY(protopayload_auditlog.metadataJson,'$.jobInsertion.job.jobStats.queryStats.referencedTables') as tables,
FROM `nada-prod-6977.bigquery_audit_logs_org.cloudaudit_googleapis_com_data_access`
WHERE protopayload_auditlog.methodName = 'google.cloud.bigquery.v2.JobService.Query'
AND protopayload_auditlog.status IS NULL
AND JSON_VALUE(protopayload_auditlog.metadataJson,'$.jobInsertion.job.jobStatus.jobState') = 'DONE'
AND DATE(timestamp) = "2022-05-9"
