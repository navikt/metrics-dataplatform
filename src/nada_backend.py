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
    ds["created"] = datetime.strptime(
        ds["datasource"]["created"], "%Y-%m-%dT%H:%M:%S.%fZ")
    ds["table_name"] = ds["datasource"]["table"]
    del ds["datasource"]
    return ds

def get_dataproducts_from_graphql(offset: int, limit: int):
    dps = []
    while True:
        query = """query ($limit: Int, $offset: Int) {
                     dataproducts(limit: $limit, offset: $offset) {
                       datasets {
                         id
                         name
                         datasource {
                           ... on BigQuery {
                             projectID
                             dataset
                             created
                             table
                           }
                         }
                       }
                     }
                   }"""

        res = requests.post(os.environ["NADA_BACKEND_URL"],
                            json={"query": query, "variables": {"limit": limit, "offset": offset}})
        res.raise_for_status()

        if res.json()["data"] is None:
            return None

        if res.json()["data"]["dataproducts"] is None:
            return None

        dataproducts = res.json()["data"]["dataproducts"]
        if len(dataproducts) == 0:
            break

        dps += dataproducts
        offset += limit

    return dps

def read_dataproducts_from_nada() -> pd.DataFrame:
    retries = [5, 15, 45, 135]
    for retry in retries:
        dps = get_dataproducts_from_graphql(offset=0, limit=15)
        if dps is not None:
            break
        time.sleep(retry)

    if dps is None:
        print("Got 'None' from Datamarkedsplassen, failing hard")
        exit(1)

    datasets = []
    for dp in dps:
        datasets += [unpack(ds) for ds in dp["datasets"]]

    df_nada = pd.DataFrame.from_dict(datasets)
    df_nada["table_uri"] = df_nada.apply(
        lambda row: f"{row['project_id']}.{row['dataset']}.{row['table_name']}", axis=1)

    return df_nada
