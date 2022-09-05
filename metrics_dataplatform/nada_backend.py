import os
import pandas as pd
import requests
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


def read_dataproducts_from_nada() -> pd.DataFrame:
    dps = []
    offset = 0
    limit = 15
    done = False
    while not done:
        query = """query ($limit: Int, $offset: Int){
            dataproducts(limit: $limit, offset: $offset){
            id
            name
            datasets{
              datasource{
              ...on BigQuery{
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

        new = res.json()["data"]["dataproducts"]
        if len(new) == 0:
            done = True
        else:
            dps += new
            offset += limit

    ds = []
    for dp in dps:
        ds += [unpack(ds) for ds in dp["datasets"]]

    df_nada = pd.DataFrame.from_dict(ds)
    df_nada["table_uri"] = df_nada.apply(
        lambda row: f"{row['project_id']}.{row['dataset']}.{row['table_name']}", axis=1)

    return df_nada
