import os
import pandas as pd
import requests
import time
from datetime import datetime


# using v1 definition of dataproduct (dataproduct = table)
def unpack(ds: dict):
    ds["dataproduct_id"] = ds.pop("id")
    ds["dataproduct"] = ds.pop("name")
    ds["project_id"] = ds["datasource"]["projectID"]
    ds["dataset"] = ds["datasource"]["dataset"]
    created = ds["datasource"]["created"]
    try:
        ds["created"] = datetime.strptime(created, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        ds["created"] = datetime.strptime(created, "%Y-%m-%dT%H:%M:%S%fZ")
    ds["table_name"] = ds["datasource"]["table"]
    del ds["datasource"]
    return ds

def get_dataproducts_from_graphql() -> list:
    dss = []

    res = requests.get(f"{os.environ['NADA_BACKEND_URL']}/datasets")

    for ds in res.json():
        try:
            created = datetime.strptime(ds["created"], "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            created = datetime.strptime(ds["created"], "%Y-%m-%dT%H:%M:%S%fZ")

        dss.append({
            "dataproduct_id": ds["id"],
            "dataproduct": ds["name"],
            "project_id": ds["projectID"],
            "dataset": ds["dataset"],
            "table_name": ds["table"],
            "created": created,
        })

    return dss

def read_dataproducts_from_nada() -> pd.DataFrame:
    retries = [5, 15, 45, 135]
    for retry in retries:
        datasets = get_dataproducts_from_graphql()
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
