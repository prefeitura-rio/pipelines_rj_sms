# -*- coding: utf-8 -*-
import asyncio

import aiohttp
import prefect
from discord import Embed, File, Webhook
from prefeitura_rio.pipelines_utils.infisical import get_secret


async def send_discord_webhook(text_content, fig_path=None, username=None):
    """
    Sends a message to a Discord webhook.

    Args:
        message (str): The message to send.
        username (str, optional): The username to use when sending the message. Defaults to None.
    """
    webhook_url = get_secret(secret_name="DISCORD_WEBHOOK_URL", environment="prod").get(
        "DISCORD_WEBHOOK_URL"
    )

    async with aiohttp.ClientSession() as session:
        kwargs = {"content": text_content}
        if username:
            kwargs["username"] = username
        if fig_path:
            file = File(fig_path, filename="image.png")
            embed = Embed()
            embed.set_image(url="attachment://image.png")
            kwargs["file"] = file
            kwargs["embed"] = embed

        webhook = Webhook.from_url(webhook_url, session=session)
        await webhook.send(**kwargs)


def send_message(title, message, fig_path=None, username=None):
    """
    Sends a message with the given title and content to a webhook.

    Args:
        title (str): The title of the message.
        message (str): The content of the message.
        username (str, optional): The username to be used for the webhook. Defaults to None.
    """
    flow_name = prefect.context.get("flow_name")
    flow_run_id = prefect.context.get("flow_run_id")
    task_name = prefect.context.get("task_full_name")
    task_run_id = prefect.context.get("task_run_id")

    content = f"""
## {title}
> Flow Run: [{flow_name}](https://pipelines.dados.rio/flow-run/{flow_run_id})
> Task Run: [{task_name}](https://pipelines.dados.rio/task-run/{task_run_id})

{message}
    """
    asyncio.run(send_discord_webhook(text_content=content, fig_path=fig_path, username=username))
