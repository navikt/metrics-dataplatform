from etl import source_stage, stage_to_dp


# ETL for the connection to the audit logs
source_stage.run_etl_general()

# ETL for the various dataproducts
stage_to_dp.run_etl_aggregate()