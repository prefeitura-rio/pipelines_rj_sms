# -*- coding: utf-8 -*-
import asyncio
from typing import Literal

import aiohttp
import prefect
from discord import Embed, File, Webhook
from prefeitura_rio.pipelines_utils.infisical import get_secret


async def send_discord_webhook(
    text_content: str,
    file_path: str = None,
    username: str = None,
    monitor_slug: str = Literal["endpoint-health", "dbt-runs", "data-ingestion"],
):
    """
    Sends a message to a Discord webhook.

    Args:
        message (str): The message to send.
        username (str, optional): The username to use when sending the message. Defaults to None.
    """
    secret_name = f"DISCORD_WEBHOOK_URL_{monitor_slug.upper()}"
    webhook_url = get_secret(secret_name=secret_name, environment="prod").get(secret_name)

    async with aiohttp.ClientSession() as session:
        kwargs = {"content": text_content}
        if username:
            kwargs["username"] = username
        if file_path:
            file = File(file_path, filename=file_path)

            if ".png" in file_path:
                embed = Embed()
                embed.set_image(url=f"attachment://{file_path}")
                kwargs["embed"] = embed

            kwargs["file"] = file

        webhook = Webhook.from_url(webhook_url, session=session)
        await webhook.send(**kwargs)


def send_message(title, message, monitor_slug, file_path=None, username=None):
    """
    Sends a message with the given title and content to a webhook.

    Args:
        title (str): The title of the message.
        message (str): The content of the message.
        username (str, optional): The username to be used for the webhook. Defaults to None.
    """
    environment = prefect.context.get("parameters").get("environment")
    flow_name = prefect.context.get("flow_name")
    flow_run_id = prefect.context.get("flow_run_id")
    task_name = prefect.context.get("task_full_name")
    task_run_id = prefect.context.get("task_run_id")

    content = f"""
## {title}
> Environment: {environment}
> Flow Run: [{flow_name}](https://pipelines.dados.rio/flow-run/{flow_run_id})
> Task Run: [{task_name}](https://pipelines.dados.rio/task-run/{task_run_id})

{message}
    """
    asyncio.run(
        send_discord_webhook(
            text_content=content, file_path=file_path, username=username, monitor_slug=monitor_slug
        )
    )
