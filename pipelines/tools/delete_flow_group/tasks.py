# -*- coding: utf-8 -*-
from prefect.client import Client
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.credential_injector import authenticated_task as task


@task
def delete_flow_group(environment:str, flow_group_id: str):
    """
    Deletes a flow group from Prefect Cloud.

    :param flow_group_id: Flow group id.
    """
    client = Client()
    response = client.graphql(
        query="""
        mutation {
            delete_flow_group(input:{flow_group_id:"<FLOW_GROUP_ID>"}){
                success
            }
        }
        """.replace(
            "<FLOW_GROUP_ID>", flow_group_id
        ),
        raise_on_error=True,
    )
    log(response)
    return
