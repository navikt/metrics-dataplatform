import pandas as pd


def dataproducts():
    df_stage = pd.read_gbq("""SELECT user, date, table_uri, service_account, metabase, intra_team, dataproduct, source, target
    FROM nada-prod-6977.bq_metrics_datamarkedsplassen.stage""", location='europe-north1')

    df_dataproducts = df_stage.groupby(["source", "table_uri", "date"])[
        "user"].agg(["count", "nunique"]).reset_index()

    # Calculating share of unique consumers coming from same team as producers and share of service-users
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

    df_dataproducts.to_gbq(project_id="nada-prod-6977",
                           destination_table="nada-prod-6977.bq_metrics_datamarkedsplassen.dataproducts",
                           if_exists='append',
                           location='europe-north1')
