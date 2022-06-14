import os
import pandas as pd
import requests
from datetime import datetime


def unpack(dp: dict):
    dp["dataproduct_id"] = dp.pop("id")
    dp["dataproduct"] = dp.pop("name")
    dp["project_id"] = dp["datasource"]["projectID"]
    dp["dataset"] = dp["datasource"]["dataset"]
    dp["created"] = datetime.strptime(
        dp["datasource"]["created"], "%Y-%m-%dT%H:%M:%S.%fZ")
    dp["table_name"] = dp["datasource"]["table"]
    del dp["datasource"]
    return dp


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
            datasource{
            ...on BigQuery{
                projectID
                dataset
                created
                table
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

    dps = [unpack(dp) for dp in dps]
    df_nada = pd.DataFrame.from_dict(dps)
    df_nada["table_uri"] = df_nada.apply(
        lambda row: f"{row['project_id']}.{row['dataset']}.{row['table_name']}", axis=1)

    return df_nada
