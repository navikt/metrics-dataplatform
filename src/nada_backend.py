import os
import time
from datetime import datetime

import pandas as pd
import requests


def get_datasets_from_dmp() -> list:
    dss = []

    res = requests.get(f"{os.environ['NADA_BACKEND_URL']}/internal/api/datasets/")

    for ds in res.json():
        try:
            created = datetime.strptime(ds["created"], "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            created = datetime.strptime(ds["created"], "%Y-%m-%dT%H:%M:%S%fZ")

        dss.append({
            "dataset_id": ds["id"],
            "dataset_name": ds["name"],
            "project_id": ds["project"],
            "dataset": ds["dataset"],
            "table_name": ds["table"],
            "created": created,
        })

    return dss

def read_datasets_from_nada() -> pd.DataFrame:
    retries = [5, 15, 45, 135]
    for retry in retries:
        datasets = get_datasets_from_dmp()
        if datasets is not None:
            break
        time.sleep(retry)

    if datasets is None:
        print("Got 'None' from Datamarkedsplassen, failing hard")
        exit(1)

    df_nada = pd.DataFrame.from_dict(datasets)
    df_nada["table_uri"] = df_nada.apply(
        lambda row: f"{row['project_id']}.{row['dataset']}.{row['table_name']}", axis=1)

    return df_nada
