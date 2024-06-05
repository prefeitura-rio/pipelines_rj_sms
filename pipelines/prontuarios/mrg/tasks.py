# -*- coding: utf-8 -*-
from datetime import timedelta
import asyncio
import httpcore
import httpx
import prefect
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
def parse_date(date: str) -> str:
    """
    Parses the given date string and returns it in the format "%Y-%m-%d".

    Args:
        date (str): The date string to be parsed. Can be '', 'today', 'tomorrow', 'yesterday', or any other valid date string.

    Returns:
        str: The parsed date string in the format "%Y-%m-%d".
    """
    scheduled_datetime = prefect.context.get("scheduled_start_time")

    if date == 'today':
        date_value = scheduled_datetime.date()
        return date_value.strftime("%Y-%m-%d")
    elif date == 'tomorrow':
        date_value = scheduled_datetime.date() + timedelta(days=1)
        return date_value.strftime("%Y-%m-%d")
    elif date == 'yesterday':
        date_value = scheduled_datetime.date() - timedelta(days=1)
        return date_value.strftime("%Y-%m-%d")
    else:
        return date


@task
def get_mergeable_records_from_api(
    api_base_url: str,
    api_token: str,
    start_datetime: str,
    end_datetime: str,
    page_size: int = 1000,
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
        logger = prefect.context.get("logger")
        logger.info(f"Retrieving page {page} of data")

        transport = AsyncHTTPTransport(retries=3)
        async with AsyncClient(transport=transport, timeout=300) as client:
            try:
                response = await client.get(
                    url=f"{api_base_url}std/patientrecords/updated",
                    headers={"Authorization": f"Bearer {api_token}"},
                    params={
                        "page": page,
                        "size": page_size,
                        "start_datetime": start_datetime,
                        "end_datetime": end_datetime,
                    },
                    timeout=300,
                )
            except httpx.ReadTimeout:
                logger.info("HTTPX Read Timed Out")
                return None
            except httpcore.ReadTimeout:
                logger.info("HTTPCore Read Timed Out")
                return None
            if response.status_code not in [200]:
                logger.info(f"Failed request with status code {response.status_code}")
                return None
            else:
                logger.info("Successful request")
                return response.json()

    async def main():
        log("Now retrieving first page of data")
        first_response = await get_mergeable_records_batch_from_api(
            1, page_size, start_datetime, end_datetime
        )
        log("First page of data retrieved.")
        log(first_response)
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
    logger = prefect.context.get("logger")

    async def send_data_batch(merged_patient_batch):
            
        transport = AsyncHTTPTransport(retries=3)
        async with AsyncClient(transport=transport, timeout=500) as client:
            try:
                response = await client.put(
                    url=f"{api_url}{endpoint_name}",
                    headers={"Authorization": f"Bearer {api_token}"},
                    timeout=500,
                    json=merged_patient_batch,
                )
            except httpx.ReadTimeout:
                logger.info("HTTPX Read Timed Out")
                return False
            except httpcore.ReadTimeout:
                logger.info("HTTPCore Read Timed Out")
                return False
            if response.status_code not in [200, 201]:
                logger.info(f"Failed request with status code {response.status_code}")
                return False
            else:
                logger.info("Successful request")
                return True

    async def main():
        awaitables = [send_data_batch(batch) for batch in payload_in_batch]
        responses = await asyncio.gather(*awaitables)
        log(f"{sum(responses)} successful requests out of {len(responses)}")

    asyncio.run(main())
