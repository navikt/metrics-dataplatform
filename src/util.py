import os
import pandas as pd
import pandas_gbq
from datetime import datetime, date, timedelta


def find_next_upload_date(table_uri: str):
    last_uploaded_date = pandas_gbq.read_gbq(f"""SELECT max(date) as date FROM `{table_uri}`""",
                                     project_id=os.environ["GCP_TEAM_PROJECT_ID"],
                                     location="europe-north1")
    try:
        return datetime.strptime(last_uploaded_date.iloc[0]["date"], "%Y-%m-%d").date() + timedelta(days=1)
    except TypeError:
        return last_uploaded_date.iloc[0]["date"].to_pydatetime().date() + timedelta(days=1)


def determine_time_range(table_uri: str) -> str:
    next_upload_date = find_next_upload_date(table_uri)
    yesterday = date.today() - timedelta(days=1)
    delta = (yesterday - next_upload_date).days
    if delta >= 0:
        return f"'{next_upload_date}' AND '{yesterday}'"
    else:
        print(f"{table_uri} allready up to date")
        return None
