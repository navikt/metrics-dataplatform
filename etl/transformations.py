import re
import pandas as pd



def extract_tables_from_query(query):
    """
    Denne funksjonen finner og returnerer tabeller inkludert i en spørring

    :param query: str: spørringen
    :return: str: Tabeller identifisert, skilt med pipe
    """
    regex_pattern = "(?<=FROM|JOIN|from|join)(\s`)+(\w|-|.)+?(`)+"
    # Hvorfor funker ikke re.findall som re.search??
    tables = []
    matched = True
    while matched:
        try:
            table = re.search(regex_pattern, query)[0].strip()
            tables.append(table)
            query = query.replace(table, '')
        except TypeError: # Which is the case when re.search returns None
            matched = False
    tables = '|'.join(tables)

    return tables



def flag_retention_observations_weekly(df, identifier_column, groupby_columns, time_column):
    """

    :param df:
    :param identifier_column:
    :param groupby_columns:
    :param time_column:
    :return:
    """
    df['date'] = df[time_column].dt.date

    groupby_columns = groupby_columns[::]
    groupby_columns.append(identifier_column)
    groupby_columns.append('date')
    df.sort_values(by=groupby_columns, ascending=False)
    df.drop_duplicates(subset=groupby_columns, inplace=True)
    groupby_columns.remove('date')

    # Resample to days
    df['observation'] = 1
    df_right = df.copy() # for merging later
    df_right.drop(time_column, axis=1, inplace=True)
    df.set_index(time_column, inplace=True)
    df = df.groupby(groupby_columns)['observation'].resample('d').sum().reset_index()
    df.loc[df['observation'].isnull(), 'observation'] = 0

    # Flag activity in periods
    df.sort_values(time_column, ascending=False, inplace=True)
    df.set_index(time_column, inplace=True)
    df = df.groupby(groupby_columns)['observation'].rolling(7, min_periods=1, closed='left').sum().reset_index()
    df.rename({'observation': 'obs_next_window'}, axis=1, inplace=True)

    # Merge and flag retention
    merge_columns = groupby_columns + ['date']
    df['date'] = df[time_column].dt.date
    df.drop(time_column, axis=1, inplace=True)
    df = df.merge(df_right, on=merge_columns, how='left')
    df[time_column] = pd.to_datetime(df['date'])

    df['retention'] = 0
    df.loc[(df['obs_next_window'] > 0) & (df['observation'] == 1), 'retention'] = 1

    return df


def calculate_retention_weekly(df, identifier_column, groupby_columns, time_column):
    """
    This function calculates retention per week for the groups included in groupby_columns

    :param df:
    :param identifier_column:
    :param groupby_columns:
    :param time_column:
    :return:
    """
    df = flag_retention_observations_weekly(df, identifier_column, groupby_columns, time_column)
    groupby_columns.append(time_column)
    df = df.groupby(groupby_columns).sum().reset_index()
    df.set_index(time_column, inplace=True)
    df = df[['observation', 'retention']].rolling(7).sum().reset_index()
    df['retention_share'] = df['retention'].div(df['observation'])
    print(df)

    return df


