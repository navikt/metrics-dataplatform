import os
import pandas_gbq
from datetime import datetime


def run_stage_to_dp(time_range: str):
    df_stage = pandas_gbq.read_gbq(f"""SELECT user, query_timestamp, table_uri, service_account, metabase, intra_team, dataproduct, source, target, dataproduct_id
    FROM {os.environ['STAGE_TABLE']} WHERE date BETWEEN {time_range}""", project_id=os.environ["GCP_TEAM_PROJECT_ID"], location='europe-north1')

    df_stage["date"] = df_stage["query_timestamp"].apply(
        lambda t: datetime.combine(t.date(), datetime.min.time()))
    df_stage.drop(columns=["query_timestamp"], inplace=True)

    df_dataproducts = df_stage.groupby(["source", "table_uri", "date", "dataproduct", "dataproduct_id"])[
        "user"].agg(["count", "nunique"]).reset_index()

    # Calculating share of unique consumers coming from same team as producers, share of service-users and share of metabase users
    for column in ['intra_team', 'service_account', 'metabase']:
        groupby_temp = ['source', 'table_uri'] + [column, 'date']
        df_temp = df_stage.copy()
        df_temp = df_temp.groupby(groupby_temp)['user'].agg(
            ['nunique']).reset_index()
        groupby_temp.remove(column)
        df_temp[f'share_{column}'] = df_temp['nunique'].div(
            df_temp.groupby(groupby_temp)['nunique'].transform('sum'))
        df_temp = df_temp[df_temp[column] == 1]
        df_temp.drop([column, 'nunique'], axis=1, inplace=True)

        df_dataproducts = df_dataproducts.merge(
            df_temp, how='left', on=groupby_temp)

    df_dataproducts.fillna(0, inplace=True)

    pandas_gbq.to_gbq(df_dataproducts,
                      project_id=os.environ['GCP_TEAM_PROJECT_ID'],
                      destination_table=os.environ["DATAPRODUCTS_TABLE"],
                      if_exists='append',
                      location='europe-north1')

    print(
        f"Uploaded {len(df_dataproducts)} rows to {os.environ['DATAPRODUCTS_TABLE']}")
