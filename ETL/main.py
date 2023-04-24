import pandas as pd
from google.cloud import bigquery


def run_target_group():

    def extract_cluster_ids(item):
        """
        Denne funksjonen henter ut alle cluster-idene fra en json-lignende struktur
        """
        list_of_ids = item.replace("'", "").replace("[", "").replace("]", "").replace(" ", "").split(",")

        return list_of_ids

    # Henter data fra teamkatalogen
    df = pd.read_gbq('select clusterIds, id as team_id, name, naisTeams, productAreaId from org-prod-1016.teamkatalogen.teams')

    # Beholder oversikten over naisteams til å koble med source aligned
    df_nais_teams = df.explode("naisTeams")

    ### Henter ut source-aligned teams
    df_source_team = pd.read_gbq("select dato, team, cluster, name from aura-prod-d7e3.dataproduct_apps.dataproduct_apps_unique")

    df_source_team.drop_duplicates(inplace=True) # Vil kun ha en per dag

    # Teller antall apper
    df_source_team = df_source_team.groupby(["dato", "team", "cluster"])["name"].count().reset_index(name="antall_apper")

    # Merger inn info fra teamkatalogen for de som har rapportertert nais-team der
    df_source_team = df_source_team.merge(df_nais_teams, how="left", right_on="naisTeams", left_on="team")

    df_source_team["source_aligned"] = True


    ## Datateam fra teamkatalogen
    # Lager en rad per cluster-id
    df["cluster_id"] = df["clusterIds"].apply(extract_cluster_ids)
    df = df.explode("cluster_id")

    # Og beholder kun de som matcher DVH-clusteret
    df_data_team = df[df["cluster_id"] == "eeba714e-fbae-4102-bad3-5e41cc275c6e"]
    df_data_team["source_aligned"] = False

    df_data_team = df_data_team.explode("naisTeams")
    df_data_team["dato"] = max(df_source_team["dato"])


    # Slår alt sammen til slutt
    df_total = pd.concat([df_source_team, df_data_team])
    df_total.drop(["clusterIds", "cluster_id"], axis=1, inplace=True)

    # Caster til datetime
    df_total["dato"] = pd.to_datetime(df_total["dato"])

    # Schema
    table_schema=[
        bigquery.SchemaField("dato", bigquery.enums.SqlTypeNames.DATE, description="Tidspunkt for innsamling av data fra Kubernetes (for team med apper). Info om datateam er siste oppdaterte fra Teamkatalogen."),
        bigquery.SchemaField("team", bigquery.enums.SqlTypeNames.STRING, description="Nais-team fra Kubernetes"),
        bigquery.SchemaField("cluster", bigquery.enums.SqlTypeNames.STRING, description="Hvilket cluster appene kjører i"),
        bigquery.SchemaField("antall_apper", bigquery.enums.SqlTypeNames.FLOAT, description="Antall apper i Kubernetes"),
        bigquery.SchemaField("team_id", bigquery.enums.SqlTypeNames.STRING, description="Team-id fra Teamkatalogen"),
        bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING, description="Navn fra Teamkatalogen"),
        bigquery.SchemaField("naisTeams", bigquery.enums.SqlTypeNames.STRING, description="Nais-team fra Teamkatalogen"),
        bigquery.SchemaField("productAreaId", bigquery.enums.SqlTypeNames.STRING, description="Produktområde-id fra Teamkatalogen"),
        bigquery.SchemaField("source_aligned", bigquery.enums.SqlTypeNames.BOOLEAN, description="True hvis teamet har apper der data oppstår")
        ]

    # Skriver data
    client = bigquery.Client()

    project, dataset, table_name = "nada-prod-6977", "platform_users", "target_group"
    table_id = f"{project}.{dataset}.{table_name}"

    job_config = bigquery.job.LoadJobConfig(schema=table_schema, write_disposition="WRITE_TRUNCATE")

    job = client.load_table_from_dataframe(df_total, table_id, job_config=job_config)



if __name__ == "__main__":
    run_target_group()
