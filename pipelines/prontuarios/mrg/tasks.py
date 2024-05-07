# -*- coding: utf-8 -*-
import asyncio

from httpx import AsyncClient, AsyncHTTPTransport, ReadTimeout
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.prontuarios.mrg.functions import (
    final_merge,
    first_merge,
    load_ranking,
    normalize_payload_list,
    sanity_check,
)
from pipelines.utils.credential_injector import authenticated_task as task


@task
def get_params(start_datetime: str, end_datetime: str) -> dict:
    """
    Creating params
    Args:
        start_datetime (str) : initial date extraction

    Returns:
        dict : params dictionary
    """
    # start_datetime = '2024-03-14 17:03:25'
    # end_datetime = '2024-03-14 17:03:26' #start_datetime + timedelta(days=1)
    log(
        f"""
        Standardizing from {start_datetime}
        to {end_datetime}"""
    )
    return {
        "start_datetime": start_datetime,  # .strftime("%Y-%m-%d 00:00:00"),
        "end_datetime": end_datetime,  # .strftime("%Y-%m-%d 00:00:00")
    }


@task
def print_n_patients(data: list):
    """
    Print number of patients to perform merge

    Args:
        data (list): List of patients to perform merge.
    """
    log(f"Merging registers for {len(data)} patients")

    return data


@task
def load_mergeable_data(url: str, cpfs: list, credentials: str, batch_size: int = 100) -> list:
    async def get_single_data(cpf):
        transport = AsyncHTTPTransport(retries=3)
        async with AsyncClient(transport=transport, timeout=180) as client:
            try:
                response = await client.get(
                    url=url + "/" + cpf,
                    headers={"Authorization": f"Bearer {credentials}"},
                    timeout=180,
                )
            except ReadTimeout:
                return None
            if response.status_code not in [200]:
                return None
            else:
                return response.json()

    async def main():
        awaitables = [get_single_data(cpf) for cpf in cpfs]
        awaitables = [
            awaitables[i : i + batch_size] for i in range(0, len(awaitables), batch_size)  # noqa
        ]
        log(f"Sending {len(awaitables)} request batches (BATCH_SIZE={batch_size})")
        data = []
        for i, awaitables_batch in enumerate(awaitables):
            responses = await asyncio.gather(*awaitables_batch)

            log(f"[{i}/{len(awaitables)}] {responses.count(None)} failed out of {len(responses)}")
            data.extend(responses)

        log(f"{data.count(None)} failed requests out of {len(data)}")
        return data

    data = asyncio.run(main())
    data = list(filter(None, data))

    return data


@task
def merge(data_to_merge) -> dict:
    """
    Loads mergeable data of patient from std patient API endpoint.

    Args:
        data_to_merge (list): Standardized data registers related to patient_code ready to merge

    Returns:
        sanitized_data (list): Merged data
    """
    register_list = list(map(normalize_payload_list, data_to_merge))
    log("Trying first merge")
    ranking_df = load_ranking()
    merged_n_not_merged_data = list(map(lambda x: first_merge(x, ranking_df), register_list))
    log("Final merge")
    merged_data = list(map(final_merge, merged_n_not_merged_data))
    log("Sanity check")
    sanitized_data = list(map(sanity_check, merged_data))
    return sanitized_data


@task()
def put_to_api(
    payloads: dict, api_url: str, endpoint_name: str, api_token: str, batch_size: int = 100
) -> None:
    """
    Sends multiple payloads to an API endpoint using the PUT method.

    Args:
        payloads (dict): A dictionary containing the payloads to be sent.
        api_url (str): The base URL of the API.
        endpoint_name (str): The name of the API endpoint.
        api_token (str): The API token for authentication.
        batch_size (int, optional): The number of payloads to send in each batch.

    Returns:
        None
    """

    async def put_single_patient(payload):
        transport = AsyncHTTPTransport(retries=3)
        async with AsyncClient(transport=transport, timeout=180) as client:
            try:
                response = await client.put(
                    url=f"{api_url}{endpoint_name}",
                    headers={"Authorization": f"Bearer {api_token}"},
                    timeout=180,
                    json=[payload],
                )
            except ReadTimeout:
                return False
            if response.status_code not in [200, 201]:
                return False
            else:
                return True

    async def main():
        awaitables = [put_single_patient(payload) for payload in payloads]
        awaitables = [awaitables[i: i + batch_size] for i in range(0, len(awaitables), batch_size)] #noqa
        log(f"Sending {len(awaitables)} request batches (BATCH_SIZE={batch_size})")
        status = []
        for i, awaitables_batch in enumerate(awaitables):
            responses = await asyncio.gather(*awaitables_batch)
            log(f"[{i}/{len(awaitables)}] {sum(responses)} successful out of {len(responses)}")

            status.extend(responses)

        log(f"{sum(status)} successful requests out of {len(status)}")

    asyncio.run(main())
