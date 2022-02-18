import pandas as pd
from transformations import calculate_retention_weekly


def run_etl_platform():


    project_id = 'nada-dev-db2e'
    dataset = 'metrics'
    DESTINATION_DATASET = f'{project_id}.{dataset}'
    read_table = f'{DESTINATION_DATASET}.stage'

    query = f'select * from `{read_table}`'

    df = pd.read_gbq(query, project_id)

    df.drop_duplicates(['identifier_anonym', 'year', 'week']).to_csv('inspect.csv', index=False)


    df['count'] = 1

    ## Calculating total number of reads + nunique reads
    df_main = df.groupby('timestamp')['identifier_anonym'].agg(['count', 'nunique']).reset_index()

    for column in ['intra_team', 'service']:
        groupby_temp = groupby_index + [column]
        df_temp = df.groupby(groupby_temp)['count'].count().reset_index()
        df_temp[f'share_{column}'] = df_temp['count'].div(df_temp.groupby(groupby_index)['count'].transform('sum'))
        df_temp = df_temp[df_temp[column]]
        df_temp.drop([column, 'count'], axis=1, inplace=True)
        df_main = df_main.merge(df_temp, how='left', on=groupby_index)

    print(df_main)


if __name__ == '__main__':
    run_etl_platform()