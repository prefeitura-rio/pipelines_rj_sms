# -*- coding: utf-8 -*-
import re
from typing import Tuple

import numpy as np
import pandas as pd
import requests
from prefeitura_rio.pipelines_utils.logging import log
from tqdm import tqdm
from unidecode import unidecode
from weighted_levenshtein import lev

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import get_secret_key, load_file_from_bigquery


@task
def load_std_dataset(
    project_name: str, dataset_name: str, table_name: str, environment: str, is_historical: bool
) -> pd.DataFrame:

    if is_historical is False:
        std_dataframe = load_file_from_bigquery.run(
            project_name=project_name,
            dataset_name=dataset_name,
            table_name=table_name,
            environment=environment,
        )
    else:
        std_dataframe = pd.DataFrame()

    return std_dataframe


@task(nout=2)
def create_allergie_list(
    dataframe_allergies_vitai: pd.DataFrame, std_allergies: pd.DataFrame
) -> Tuple[pd.DataFrame, list]:
    """
    Create a list of allergies from a dataframe
    Args:
        dataframe (pd.DataFrame): Dataframe with the allergies
    """

    def clean_allergies_field(allergies_field: str) -> str:
        allergies_field = unidecode(allergies_field)
        allergies_field = allergies_field.upper()
        allergies_field_clean = re.sub(
            r"INTOLERANCIA A{0,1}|((REFERE )|(RELATA ){0,1}ALERGIA A{0,1})|\
            (PACIENTE ){0,1}AL[É|E]RGIC[A|O] AO{0,1} ",
            "",
            allergies_field,
        )
        allergies_field_clean = re.sub(
            r".*N[Ã|A]O [(SABE)|(RECORDA)|(LEMBRA))].*", "", allergies_field_clean
        )
        allergies_field_clean = re.sub(r"^A ", "", allergies_field_clean)
        allergies_field_clean = re.sub(r"ALERGIC[A|O]", "", allergies_field_clean)
        allergies_field_clean = re.sub(r".*COMENTARIO.*", "", allergies_field_clean)
        allergies_field_clean = re.sub(r".*LISTA.*", "", allergies_field_clean)
        allergies_field_clean = re.sub(r" E |\/|\n", ",", allergies_field_clean)
        allergies_field_clean = re.sub(r"(\\\\)", ",", allergies_field_clean)
        allergies_field_clean = re.sub(r"\(\)", ",", allergies_field_clean)
        allergies_field_clean = allergies_field_clean.strip()

        return allergies_field_clean

    list_of_lists_alergias = dataframe_allergies_vitai["alergias"].values
    flat_list_alergias = [x for xs in list_of_lists_alergias for x in xs]
    allergies_vitai = [d["descricao_raw"] for d in flat_list_alergias if d["padronizado"] == 0]

    allergies_unique = []
    for i in allergies_vitai:
        allergies_unique.extend(i)
    allergies_unique = np.unique(allergies_unique)
    log(f"Loading {len(allergies_unique)} allergies")

    if not std_allergies.empty:
        allergies_unique = [
            i for i in allergies_unique if i not in std_allergies["alergias_raw"].values
        ]

    df_allergies = pd.DataFrame(data=allergies_unique, columns=["alergias_raw"])
    df_allergies = df_allergies[0:100]
    log(f"Standardizing {len(allergies_unique)} allergies")

    df_allergies["alergias_limpo"] = df_allergies["alergias_raw"].apply(clean_allergies_field)
    alergias_join = ",".join(df_allergies["alergias_limpo"].values)
    alergias_lista = alergias_join.split(",")
    alergias_lista = list(
        set([alergia.strip() for alergia in alergias_lista if alergia.strip() != ""])
    )
    return df_allergies, alergias_lista


@task(nout=2)
def get_similar_allergie_levenshtein(
    allergies_dataset_reference: list, allergie_list: str, threshold: float
) -> Tuple[list, list]:
    """
    Get similar allergie using levenshtein distance
    Args:
        allergies_dataset_reference (list): List of allergies to be used as reference
        allergie_list (list): List of allergie to be standardized
        threshold (float): Threshold for the levenshtein distance
    """

    # Defining weighted-levenshtein function
    def dist(c1, c2, key_positions):
        pos1 = key_positions[c1]
        pos2 = key_positions[c2]
        return abs(pos1[0] - pos2[0]) + abs(pos1[1] - pos2[1])

    key_positions = {
        "Q": (0, 0),
        "W": (0, 1),
        "E": (0, 2),
        "R": (0, 3),
        "T": (0, 4),
        "Y": (0, 5),
        "U": (0, 6),
        "I": (0, 7),
        "O": (0, 8),
        "P": (0, 9),
        "A": (1, 0),
        "S": (1, 1),
        "D": (1, 2),
        "F": (1, 3),
        "G": (1, 4),
        "H": (1, 5),
        "J": (1, 6),
        "K": (1, 7),
        "L": (1, 8),
        "Z": (2, 0),
        "X": (2, 1),
        "C": (2, 2),
        "V": (2, 3),
        "B": (2, 4),
        "N": (2, 5),
        "M": (2, 6),
    }

    substitute_costs = np.ones((128, 128), dtype=np.float64)
    for key_1 in key_positions.keys():
        for key_2 in key_positions.keys():
            if dist(key_1, key_2, key_positions) == 1:
                substitute_costs[ord(key_1), ord(key_2)] = 0.5

    # Calculating weighted-levenshtein similarity

    standardized = []
    not_standardized = []
    for allergie in allergie_list:
        candidates = []
        similaritys = []
        for ref in allergies_dataset_reference["nome"].values:
            lev_result = lev(ref, allergie, substitute_costs=substitute_costs)
            max_length = max(len(ref), len(allergie))
            lev_weighted_similarity = 1 - (lev_result / max_length)

            if lev_weighted_similarity > threshold:
                candidates.append(ref)
                similaritys.append(lev_weighted_similarity)
        result = pd.DataFrame(
            {
                "input": [allergie] * len(candidates),
                "output": candidates,
                "similaridade": similaritys,
                "metodo": "levenshetein",
            }
        )
        result.sort_values(by="similaridade", ascending=False, inplace=True)
        if result.head(1).empty:
            not_standardized.append(allergie)
        else:
            standardized.append(
                result[["input", "output", "metodo"]].head(1).to_dict(orient="records")[0]
            )

    log(f"{len(standardized)} allergies standardized using Levenshtein")
    log(f"{len(not_standardized)} allergies remaining")

    return standardized, not_standardized


@task(nout=2)
def get_api_token(
    environment: str,
    infisical_path: str,
    infisical_api_url: str,
    infisical_api_username: str,
    infisical_api_password: str,
) -> Tuple[str, str]:
    """
    Retrieves the authentication token for AI Models API.

    Args:
        environment (str): The environment for which to retrieve the API token.
        infisical_path (str): The path of infisical secrets
        infisical_api_url (str): The secret name of API_url in infisical
        infisical_api_username (str): The secret name of username in infisical
        infisical_api_password (str): The secret name os password in infisical

    Returns:
        str: The API token.

    Raises:
        Exception: If there is an error getting the API token.
    """
    api_url = get_secret_key.run(
        secret_path=infisical_path, secret_name=infisical_api_url, environment=environment
    )
    api_username = get_secret_key.run(
        secret_path=infisical_path, secret_name=infisical_api_username, environment=environment
    )
    api_password = get_secret_key.run(
        secret_path=infisical_path, secret_name=infisical_api_password, environment=environment
    )
    response = requests.post(
        url=f"{api_url}v1/login",
        timeout=180,
        headers={
            "Content-Type": "application/json",
        },
        json={"username": api_username, "password": api_password},
    )

    if response.status_code == 200:
        return response.json()["access_token"], api_url
    else:
        raise Exception(f"Error getting API token ({response.status_code}) - {response.text}")


@task
def get_similar_allergie_gemini(allergies_list: list, api_url: str, api_token: str) -> list:
    """
    Get similar allergie using gemini agent
    Args:
        allergies_list (list): List of allergies to be standardized
        api_url (str): URL used to call AI models
        api_token (str): Models API key
    """
    log(f"{len(allergies_list)} allergies to be standardized using Gemini")
    batch_size = 20
    result_list = []
    for i in tqdm(range(0, len(allergies_list), batch_size)):
        allergies_batch = allergies_list[i : i + batch_size]
        response = requests.post(
            url=f"{api_url}v1/allergy/standardize",
            timeout=500,
            headers={
                "Authorization": "Bearer {}".format(api_token),
                "Content-Type": "application/json",
            },
            json={"allergies_list": allergies_batch},
        )

        if response.status_code == 200:
            result_batch = response.json()["corrections"]
            result_batch = [
                {"input": i["input"], "output": i["output"], "metodo": "gemini"}
                for i in result_list
            ]
            result_list.extend(result_batch)
        else:
            raise Exception(
                f"Error getting gemini standardization ({response.status_code}) - {response.text}"
            )

    return result_list


@task
def saving_results(
    raw_allergies: pd.DataFrame, gemini_result: list, levenshtein_result: list
) -> dict:
    """
    Concatenate results from both gemini and levenshtein models and save into a csv file
    Args:
        gemini_result (list): List of gemini result
        levenshtein_result (list): List of levenshtein result
    """

    def find_std(allergie_raw, from_to_dict):
        allergie_raw_list = allergie_raw.split(",")
        allergie_std_list = [from_to_dict.get(i.strip()) for i in allergie_raw_list]
        allergie_std_list = [i for i in allergie_std_list if i is not None]
        allergie_std = ",".join(allergie_std_list)
        return allergie_std

    levenshtein_result.extend(gemini_result)
    table = pd.DataFrame(levenshtein_result)
    if table.empty:
        log("No allergies were standardized")
        destination_file_path = ""
    else:
        from_to_dict = dict(zip(table["input"], table["output"]))
        raw_allergies["alergias_padronizado"] = raw_allergies["alergias_limpo"].apply(
            lambda x: find_std(x, from_to_dict)
        )
        raw_allergies["_extracted_at"] = pd.to_datetime("now")

    return raw_allergies
