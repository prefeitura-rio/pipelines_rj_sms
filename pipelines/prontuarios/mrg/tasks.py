# -*- coding: utf-8 -*-
import asyncio

import httpcore
import httpx
from httpx import AsyncClient, AsyncHTTPTransport
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
def get_patient_count(data: list):
    """
    Print number of patients to perform merge

    Args:
        data (list): List of patients to perform merge.
    """
    log(f"Merging registers for {len(data)} patients")

    return len(data)


@task
def get_mergeable_records_from_api(
    api_base_url: str,
    api_token: str,
    start_datetime: str,
    end_datetime: str,
    page_size: int = 10000,
) -> list:
    """
    Retrieves mergeable records from an API within a specified time range.

    Args:
        api_base_url (str): The base URL of the API.
        api_token (str): The API token for authentication.
        start_datetime (str): The start datetime for the time range.
        end_datetime (str): The end datetime for the time range.
        page_size (int, optional): The number of records to retrieve per page.

    Returns:
        list: A list of mergeable records retrieved from the API.
    """

    async def get_mergeable_records_batch_from_api(
        page: int, page_size: int, start_datetime: str, end_datetime: str
    ) -> dict:
        transport = AsyncHTTPTransport(retries=3)
        async with AsyncClient(transport=transport, timeout=300) as client:
            try:
                response = await client.get(
                    url=f"{api_base_url}std/patientrecords/updated",
                    headers={"Authorization": f"Bearer {api_token}"},
                    params={
                        "page": page,
                        "page_size": page_size,
                        "start_datetime": start_datetime,
                        "end_datetime": end_datetime,
                    },
                    timeout=300,
                )
            except httpx.ReadTimeout:
                return None
            except httpcore.ReadTimeout:
                return None
            if response.status_code not in [200]:
                return None
            else:
                return response.json()

    async def main():
        log("Now retrieving first page of data")
        first_response = await get_mergeable_records_batch_from_api(
            1, page_size, start_datetime, end_datetime
        )
        page_count = first_response.get("page_count")

        log(f"Planning retrieval of {page_count-1} pages of data")
        awaitables = []
        for page in range(2, page_count + 1):
            awaitables.append(
                get_mergeable_records_batch_from_api(page, page_size, start_datetime, end_datetime)
            )

        batch_size = 2
        awaitables_batch = [
            awaitables[i : i + batch_size] for i in range(0, len(awaitables), batch_size)
        ]
        log(f"Retrieving data in {len(awaitables_batch)} batches")

        responses = [first_response]
        for i, batch in enumerate(awaitables_batch):
            log(f"Retrieving batch {i+1} of data")
            other_responses = await asyncio.gather(*batch)
            responses.extend(other_responses)

        log(f"{responses.count(None)} failed requests out of {len(responses)}")

        return responses

    data = asyncio.run(main())
    data = list(filter(None, data))

    return data


@task
def flatten_page_data(data_in_pages: list[dict]) -> list:
    """
    Flattens a list of dictionaries containing data in pages.

    Args:
        data_in_pages (list[dict]): A list of dictionaries where each
        dictionary represents a page of data.

    Returns:
        list: A flattened list of all items from all pages.

    """
    flattened_data = []
    for page in data_in_pages:
        flattened_data.extend(page["items"])
    return flattened_data


@task
def merge(data_to_merge) -> dict:
    """
    Loads mergeable data of patient from std patient API endpoint.

    Args:
        data_to_merge (list): Standardized data registers related to patient_code ready to merge

    Returns:
        sanitized_data (list): Merged data
    """
    log(f"Merging data for {len(data_to_merge)} patients")
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
def put_to_api(payload_in_batch: list, api_url: str, endpoint_name: str, api_token: str) -> None:
    """
    Sends the payload in batches to the specified API endpoint using the PUT method.

    Args:
        payload_in_batch (list): A list of batches containing the payload data to be sent.
        api_url (str): The base URL of the API.
        endpoint_name (str): The name of the API endpoint to send the data to.
        api_token (str): The API token for authentication.

    Returns:
        None
    """

    async def send_data_batch(merged_patient_batch):
        transport = AsyncHTTPTransport(retries=3)
        async with AsyncClient(transport=transport, timeout=180) as client:
            try:
                response = await client.put(
                    url=f"{api_url}{endpoint_name}",
                    headers={"Authorization": f"Bearer {api_token}"},
                    timeout=180,
                    json=merged_patient_batch,
                )
            except httpx.ReadTimeout:
                return False
            except httpcore.ReadTimeout:
                return False
            if response.status_code not in [200, 201]:
                return False
            else:
                return True

    async def main():
        awaitables = [send_data_batch(batch) for batch in payload_in_batch]
        responses = await asyncio.gather(*awaitables)
        log(f"{sum(responses)} successful requests out of {len(responses)}")

    asyncio.run(main())