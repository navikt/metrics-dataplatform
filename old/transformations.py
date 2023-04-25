import re
import pandas as pd
import pg8000
import os
import sqlalchemy
from google.cloud.sql.connector import connector


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
        except TypeError:  # Which is the case when re.search returns None
            matched = False
    tables = '|'.join(tables)

    return tables


def flag_observation_in_window(df, identifier_column, groupby_columns, time_column, window_days=7,
                               forward_window=True):
    """

    :param df:
    :param identifier_column:
    :param groupby_columns:
    :param time_column:
    :return:
    """
    df = df.copy()
    df['date'] = df[time_column].dt.date
    groupby_columns.append(identifier_column)
    subset_duplicates = groupby_columns + ['date']
    df.sort_values(by=time_column, inplace=True)
    df.drop_duplicates(subset=subset_duplicates, inplace=True)
    df['constant'] = 1
    max_period = df[time_column].max()

    # In order to have correct flagging of edge-observations, that is: The last observations within a group, we must
    # append observations n number of days forward
    df_to_append = df.copy()
    df_to_append.drop_duplicates(groupby_columns)
    df_to_append[time_column] = df_to_append[time_column] + pd.Timedelta(window_days, 'days')
    df_to_append['constant'] = None
    df = df.append(df_to_append)

    # ...and we can resample!
    df.set_index(time_column, inplace=True)
    df = df.groupby(groupby_columns)['constant'].resample('d', origin='end').sum().reset_index()
    if forward_window:
        df.sort_values(time_column, ascending=False, inplace=True)
        df.set_index(time_column, inplace=True)
        df = df.groupby(groupby_columns)['constant'].rolling(window_days, closed='left',
                                                             min_periods=window_days).sum().reset_index(
            name='observations_next_window')
    else:
        df.sort_values(time_column, ascending=True, inplace=True)
        df.set_index(time_column, inplace=True)
        df = df.groupby(groupby_columns)['constant'].rolling(window_days, closed='right',
                                                             min_periods=1).sum().reset_index(
            name='observations_this_window')

    df = df[df[time_column] <= max_period]
    df['date'] = df[time_column].dt.date.astype(str)
    df.drop(time_column, axis=1, inplace=True)

    return df


def calculate_retention_weekly(df, identifier_column, groupby_columns, time_column, window_days=7):
    """

    :param df:
    :param identifier_column:
    :param groupby_columns:
    :param time_column:
    :param window_days:
    :return:
    """
    df_forward = flag_observation_in_window(df, identifier_column, groupby_columns[::], time_column, window_days)
    df_backward = flag_observation_in_window(df, identifier_column, groupby_columns[::], time_column, window_days,
                                             forward_window=False)

    merge_columns = [col for col in df_forward.columns if col in df_backward.columns]
    df = df_forward.merge(df_backward, on=merge_columns, how='outer')

    df.loc[df['observations_this_window'] > 1, 'observations_this_window'] = 1 # We only count one observation per window
    df.loc[df['observations_next_window'] > 1, 'observations_next_window'] = 1
    df = df[df['observations_this_window'] > 0]
    groupby_columns.append('date')
    df = df.groupby(groupby_columns)[['observations_this_window', 'observations_next_window']].sum().reset_index()

    df['retention'] = df['observations_next_window'].div(df['observations_this_window'])
    df.loc[df['observations_this_window'] == 0, 'retention'] = 0
    df.drop(['observations_this_window', 'observations_next_window'], axis=1, inplace=True)

    return df

def flag_dataproduct(df):
    """
    Går gjennom df og lager en ny kolonne med is_dataproduct flagg
    0 hvis raden mangler fullstendig tabellrefereranse
    """

    def getconn() -> pg8000.dbapi.Connection:
        print(os.environ['DB_USER'])
        conn: pg8000.dbapi.Connection = connector.connect(
            f"{os.environ['GCP_TEAM_PROJECT_ID']}:europe-north1:nada-backend",
            "pg8000",
            user=f"{os.environ['DB_USER']}",
            password=f"{os.environ['DB_PASS']}",
            db=f"{os.environ['DB_DB']}",
        )
        return conn

    engine = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )

    df_ref = pd.read_sql("SELECT project_id, dataset, table_name FROM datasource_bigquery", engine)

    engine.dispose()

    table_refs = list(df_ref.project_id + '.' + df_ref.dataset + '.' + df_ref.table_name)

    def is_dataproduct(p_id, ds_id, t_id):
        try:
            ref = p_id + '.' + ds_id + '.' + t_id
        except TypeError:
            # If one or more ids are None (usually dataset_id and table_id)
            # TODO: Handle missing ids better.
            return 0
        if ref in table_refs:
            return 1
        else:
            return 0
    df["dataproduct_flag"] = df.apply(lambda row: is_dataproduct(row["project_id"], row["dataset_id"], row["table"]), axis=1)
    
    return df