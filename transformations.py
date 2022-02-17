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
    df['week'] = df[time_column].dt.isocalendar().week
    df['last_week'] = (df[time_column] - pd.Timedelta(7, 'days')).dt.isocalendar().week

    groupby_columns = groupby_columns[::]
    groupby_columns.append(identifier_column)
    groupby_columns.append('week')
    df.sort_values(by=groupby_columns, ascending=False)
    df.drop_duplicates(subset=groupby_columns, inplace=True)

    groupby_columns.remove('week')
    df['next_activity_week'] = df.groupby(groupby_columns)['last_week'].shift(-1)
    df.drop('last_week', axis=1, inplace=True)

    df['retention'] = 0
    df.loc[df['week'] == df['next_activity_week'], 'retention'] = 1

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
    df['constant'] = 1

    groupby_columns.append('week')
    df = df.groupby(groupby_columns)[['retention', 'constant']].sum().reset_index()
    df['retention_share'] = df['retention'].div(df['constant'])

    return df


