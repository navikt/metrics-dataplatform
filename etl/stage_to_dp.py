import datetime

import pandas as pd

from .transformations import calculate_retention_weekly


def run_etl_to_dataproduct(groupby_columns, DESTINATION_TABLE):
    project_id = 'nada-dev-db2e'
    dataset = 'metrics'
    DESTINATION_DATASET = f'{project_id}.{dataset}'
    read_table = f'{DESTINATION_DATASET}.stage'

    query = f'select * from `{read_table}`'

    df = pd.read_gbq(query, project_id)

    df['count'] = 1

    ## Calculating total number of reads + nunique reads + retention
    df_main = df.groupby(groupby_columns + ['date'])['identifier_anonym'].agg(['count', 'nunique']).reset_index()
    df_retention = calculate_retention_weekly(df.copy(), 'identifier_anonym', groupby_columns[::], 'timestamp')
    df_main = df_main.merge(df_retention, on=groupby_columns + ['date'], how='outer')

    # Calculating share of unique consumers coming from same team as producers and share of service-users
    for column in ['intra_team', 'service']:
        groupby_temp = groupby_columns + [column, 'date']
        df_temp = df.copy()
        df_temp = df_temp.groupby(groupby_temp)['identifier_anonym'].agg(['nunique']).reset_index()
        groupby_temp.remove(column)
        df_temp[f'share_{column}'] = df_temp['nunique'].div(df_temp.groupby(groupby_temp)['nunique'].transform('sum'))
        df_temp = df_temp[df_temp[column] == 1]
        df_temp.drop([column, 'nunique'], axis=1, inplace=True)

        df_main = df_main.merge(df_temp, how='left', on=groupby_temp)

    seven_days_ago = datetime.datetime.now() - pd.Timedelta(7, 'days')

    df_main.loc[pd.to_datetime(df_main['date']) >= seven_days_ago, 'retention'] = None

    load_table = f'{DESTINATION_DATASET}.{DESTINATION_TABLE}'

    df_main.to_gbq(
        project_id = project_id,
        destination_table=load_table,
        if_exists='replace'
    )

def run_etl_aggregate():
    run_etl_to_dataproduct([], 'plattform')
    run_etl_to_dataproduct(['source'], 'team')
    run_etl_to_dataproduct(['source', 'table'], 'dataproduct')




if __name__ == '__main__':
    run_etl_aggregate()
