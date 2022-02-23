import pandas as pd
from transformations import calculate_retention_weekly


def run_etl_platform():


    project_id = 'nada-dev-db2e'
    dataset = 'metrics'
    DESTINATION_DATASET = f'{project_id}.{dataset}'
    read_table = f'{DESTINATION_DATASET}.stage'

    query = f'select * from `{read_table}`'

    df = pd.read_gbq(query, project_id)


    df['count'] = 1

    ## Calculating total number of reads + nunique reads + retention
    df_main = df.groupby('date')['identifier_anonym'].agg(['count', 'nunique']).reset_index()

    df_retention = calculate_retention_weekly(df.copy(), 'identifier_anonym', [], 'timestamp')

    df_main = df_main.merge(df_retention, on='date', how='outer')

    # Calculating share of unique consumers coming from same team as producers and share of service-users
    for column in ['intra_team', 'service']:
        groupby_temp = [column, 'date']
        df_temp = df.copy()
        df_temp = df_temp.groupby(groupby_temp)['identifier_anonym'].agg(['nunique']).reset_index()
        df_temp[f'share_{column}'] = df_temp['nunique'].div(df_temp.groupby('date')['nunique'].transform('sum'))
        df_temp = df_temp[df_temp[column] == 1]
        df_temp.drop([column, 'nunique'], axis=1, inplace=True)

        df_main = df_main.merge(df_temp, how='left', on=['date'])

    load_table = f'{DESTINATION_DATASET}.plattform'

    df.to_gbq(
        project_id = project_id,
        destination_table=load_table,
        if_exists='replace'
    )



if __name__ == '__main__':
    run_etl_platform()