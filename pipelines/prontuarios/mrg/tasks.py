# -*- coding: utf-8 -*-
import asyncio
from datetime import timedelta

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
        date (str): The date string to be parsed. Can be '', 'today', 'tomorrow',
            'yesterday', or any other valid date string.

    Returns:
        str: The parsed date string in the format "%Y-%m-%d".
    """
    scheduled_datetime = prefect.context.get("scheduled_start_time")

    if date == "today":
        date_value = scheduled_datetime.date()
        return date_value.strftime("%Y-%m-%d")
    elif date == "tomorrow":
        date_value = scheduled_datetime.date() + timedelta(days=1)
        return date_value.strftime("%Y-%m-%d")
    elif date == "yesterday":
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
        page: int, page_count_str: str, page_size: int, start_datetime: str, end_datetime: str
    ) -> dict:

        log(f"Beginning page {page}/{page_count_str}")

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
                log(f"Failed Retrieval of page {page}/{page_count_str}: Read Timeout", level="error") #noqa
                return None
            if response.status_code not in [200]:
                log(f"Failed Retrieval of page {page}/{page_count_str}: {response.status_code} {response.text}", level="error") #noqa
                return None
            else:
                log(f"Sucessful Retrieval of page {page}/{page_count_str}: {len(response.json()['items'])} registers") #noqa
                return response.json()

    async def main():
        first_response = await get_mergeable_records_batch_from_api(
            1, '?', page_size, start_datetime, end_datetime
        )
        log(f"First page of data retrieved with length {len(first_response['items'])}.")
        page_count = first_response.get("page_count")

        awaitables = []
        for page in range(2, page_count + 1):
            awaitables.append(
                get_mergeable_records_batch_from_api(
                    page, str(page_count),
                    page_size, start_datetime, end_datetime
                )
            )

        batch_size = 2
        awaitables_batch = [
            awaitables[i : i + batch_size] for i in range(0, len(awaitables), batch_size)
        ]

        responses = [first_response]
        for i, batch in enumerate(awaitables_batch):
            log(f"BATCH [{i+1}/{len(awaitables_batch)}]")
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


@task(nout=4)
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

    log(f"Applying First merge in {len(register_list)} patients")
    ranking_df = load_ranking()
    merged_n_not_merged_data = list(map(lambda x: first_merge(x, ranking_df), register_list))
    
    log(f"Applying Final merge in {len(merged_n_not_merged_data)} patients")
    merged_data = list(map(final_merge, merged_n_not_merged_data))

    log(f"Applying Sanity check in {len(merged_data)} patients")
    patient_data = list(map(sanity_check, merged_data))

    log(f"Removing null fields in {len(patient_data)} patients")
    patient_data = [{k: v for k, v in record.items() if v is not None} for record in patient_data]

    log(f"Splitting entities from {len(patient_data)} patients")
    addresses, telecoms, cnss = [], [], []
    for record in patient_data:
        # Address
        address_list = record.pop("address_list", [])
        for address in address_list:
            address["patient_code"] = record["patient_code"]
        addresses.extend(address_list)

        # Telecom
        telecom_list = record.pop("telecom_list", [])
        for telecom in telecom_list:
            telecom["patient_code"] = record["patient_code"]
        telecoms.extend(telecom_list)

        # CNS
        cns_list = record.pop("cns_list", [])
        for cns in cns_list:
            cns["patient_code"] = record["patient_code"]
        cnss.extend(cns_list)
    
    log(f"{len(patient_data)} patients merged successfully")
    log(f"{len(addresses)} addresses to insert")
    log(f"{len(telecoms)} telecoms to insert")
    log(f"{len(cnss)} CNS to insert")

    return patient_data, addresses, telecoms, cnss
