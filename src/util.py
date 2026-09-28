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


def determine_dates_to_upload(table_uri: str) -> list[date]:
    next_upload_date = find_next_upload_date(table_uri)
    yesterday = date.today() - timedelta(days=1)
    delta = (yesterday - next_upload_date).days
    if delta < 0:
        print(f"{table_uri} allready up to date")
        return []
    return [next_upload_date + timedelta(days=i) for i in range(delta + 1)]


def to_time_range(day: date) -> str:
    return f"'{day}' AND '{day}'"
