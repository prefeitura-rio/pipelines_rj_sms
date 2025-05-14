# -*- coding: utf-8 -*-
import urllib.parse
from datetime import datetime, timedelta

import geopandas as gpd
import pandas as pd
import requests
from google.cloud import bigquery
from prefeitura_rio.pipelines_utils.logging import log
from shapely.geometry import Point

from pipelines.utils.credential_injector import authenticated_task as task


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def get_estabelecimentos_sem_coordenadas(env):
    log("Instanciando cliente Big Query")
    client = bigquery.Client()
    log("Cliente Big Query OK")

    sql = """
        SELECT DISTINCT
            id_cnes,
            endereco_cep,
            endereco_bairro,
            endereco_logradouro,
            endereco_numero
        FROM
            rj-sms.saude_cnes.estabelecimento_sus_rio_historico
        WHERE
            nome_fantasia IS NOT NULL
    """

    log("Executando query")
    df = client.query_and_wait(sql).to_dataframe()
    log("Query executada com sucesso")

    log(f"{df.sample(5)}")
    return df


@task(max_retries=3, retry_delay=timedelta(minutes=2))
def get_coordinates_from_cep(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recebe um DataFrame, tenta obter coordenadas via CEP para cada linha.
    Retorna um novo DF com colunas 'latitude_cep' e 'longitude_cep'.
    """
    log(f"[get_coordinates_from_cep] START | rows={len(df)}")

    CEP_URL_TEMPLATE = (
        "https://pgeo3.rio.rj.gov.br/arcgis/rest/services/Geocode/Geocode_CEP/GeocodeServer/"
        "findAddressCandidates?Postal={cep}&outFields=Match_addr,Addr_type,SingleLine,City,Region"
        "&matchOutOfRange=true&langCode=pt&f=pjson"
    )

    df_cep = df.copy()
    df_cep["latitude_cep"] = None
    df_cep["longitude_cep"] = None

    for idx, row in df_cep.iterrows():
        cep = (row["endereco_cep"] or "").strip()
        if not cep:
            continue

        url = CEP_URL_TEMPLATE.format(cep=urllib.parse.quote(cep))
        try:
            resp = requests.get(url, timeout=10)
            data = resp.json()
            candidates = data.get("candidates", [])
            if candidates:
                location = candidates[0].get("location", {})
                lon = location.get("x")
                lat = location.get("y")
                if lat and lon:
                    df_cep.at[idx, "latitude_cep"] = lat
                    df_cep.at[idx, "longitude_cep"] = lon
        except Exception as e:
            log(f"[get_coordinates_from_cep] ERROR | CEP={cep} | {e}")

    log(f"[get_coordinates_from_cep] DONE | rows={len(df_cep)}")
    return df_cep


@task(max_retries=3, retry_delay=timedelta(minutes=2))
def get_coordinates_from_address(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recebe um DataFrame, tenta obter coordenadas via endereço completo
    (logradouro, número, bairro) para cada linha.
    Retorna um novo DF com colunas 'latitude_addr' e 'longitude_addr'.
    """
    log(f"[get_coordinates_from_address] START | rows={len(df)}")

    LOGRADOURO_URL_TEMPLATE = (
        "https://pgeo3.rio.rj.gov.br"
        "/arcgis/rest/services/Geocode/Geocode_Logradouros/GeocodeServer/"
        "findAddressCandidates?SingleLine={single_line}"
        "&outFields=Match_addr,Addr_type,SingleLine,City,Region"
        "&matchOutOfRange=true&langCode=pt&f=pjson"
    )

    df_addr = df.copy()
    df_addr["latitude_addr"] = None
    df_addr["longitude_addr"] = None

    for idx, row in df_addr.iterrows():
        logradouro = (row["endereco_logradouro"] or "").strip()
        numero = (row["endereco_numero"] or "").strip()
        bairro = (row["endereco_bairro"] or "").strip()

        parts = ["RIO DE JANEIRO"]

        if bairro:
            parts.append(bairro)
        if logradouro:
            parts.append(logradouro)
        if numero and numero.lower() != "s/n":
            parts.append(numero)

        single_line = " ".join(parts)
        if not single_line:
            continue

        url = LOGRADOURO_URL_TEMPLATE.format(single_line=urllib.parse.quote(single_line))
        try:
            resp = requests.get(url, timeout=10)
            data = resp.json()
            candidates = data.get("candidates", [])
            if candidates:
                location = candidates[0].get("location", {})
                lon = location.get("x")
                lat = location.get("y")
                if lat and lon:
                    df_addr.at[idx, "latitude_addr"] = lat
                    df_addr.at[idx, "longitude_addr"] = lon
        except Exception as e:
            log(f"[get_coordinates_from_address] ERROR | address='{single_line}' | {e}")

    log(f"[get_coordinates_from_address] DONE | rows={len(df_addr)}")
    return df_addr


@task()
def enrich_coordinates(df_cep: pd.DataFrame, df_addr: pd.DataFrame) -> pd.DataFrame:
    """
    Recebe dois DataFrames (mesmas dimensões):
      - df_cep  : com colunas (latitude_cep, longitude_cep)
      - df_addr : com colunas (latitude_addr, longitude_addr)

      - Se latitude_addr/longitude_addr não forem nulos, usa esses.
      - Caso contrário, usa latitude_cep/longitude_cep.
    Retorna DataFrame final com colunas 'latitude' e 'longitude'.
    """
    log("[enrich_coordinates] START")

    if len(df_cep) != len(df_addr):
        raise ValueError("DataFrames with different lengths — cannot merge row-by-row reliably.")

    df_final = df_cep.copy()
    df_final = pd.concat([df_final, df_addr[["latitude_addr", "longitude_addr"]]], axis=1)
    df_final["latitude_api"] = None
    df_final["longitude_api"] = None

    for idx in df_final.index:
        lat_cep = df_final.at[idx, "latitude_cep"]
        lon_cep = df_final.at[idx, "longitude_cep"]

        lat_addr = df_addr.at[idx, "latitude_addr"]
        lon_addr = df_addr.at[idx, "longitude_addr"]

        if lat_addr and lon_addr:
            df_final.at[idx, "latitude_api"] = lat_addr
            df_final.at[idx, "longitude_api"] = lon_addr
        else:
            df_final.at[idx, "latitude_api"] = lat_cep
            df_final.at[idx, "longitude_api"] = lon_cep

    log("[enrich_coordinates] DONE")
    log(f"{df_final.sample(5)}")
    return df_final


@task
def transform_coordinates_geopandas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma as coordenadas de 'latitude_api' (y) e 'longitude_api' (x)
    utilizando GeoPandas para converter de SRS 31983 para 4326.
    """

    log("[transform_coordinates_geopandas] START")

    valid_mask = df["longitude_api"].notnull() & df["latitude_api"].notnull()
    if valid_mask.any():
        sub_df = df.loc[valid_mask].copy()

        sub_df["geometry"] = [
            Point(xy) for xy in zip(sub_df["longitude_api"], sub_df["latitude_api"])
        ]

        gdf = gpd.GeoDataFrame(sub_df, geometry="geometry", crs="EPSG:31983")

        gdf = gdf.to_crs("EPSG:4326")

        df.loc[valid_mask, "longitude_api"] = gdf.geometry.x
        df.loc[valid_mask, "latitude_api"] = gdf.geometry.y

    df["data_extracao"] = datetime.now()

    log("[transform_coordinates_geopandas] DONE")
    log(f"{df.sample(5)}")
    return df
