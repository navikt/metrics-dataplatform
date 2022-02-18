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
    df['constant'] = 1
    df.set_index(time_column, inplace=True)
    df = df.groupby(groupby_columns)['constant'].resample('d').sum().reset_index()
    df.loc[df['constant'].isnull(), 'constant'] = 0
    df.set_index(time_column, inplace=True)
    print(df)
    # Flag retention
    df = df.groupby(groupby_columns)['constant'].rolling(7).sum().reset_index()

    df['retention'] = 0
    df.loc[df['constant'] > 1, 'retention'] = 1

    print(df)

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


