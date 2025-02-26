# -*- coding: utf-8 -*-
import asyncio
from typing import Literal

import aiohttp
import prefect
from discord import AllowedMentions, Embed, File, Webhook
from prefeitura_rio.pipelines_utils.infisical import get_secret


def get_environment():
    return prefect.context.get("parameters").get("environment")


async def send_discord_webhook(
    text_content: str,
    file_path: str = None,
    username: str = None,
    monitor_slug: str = Literal["endpoint-health", "dbt-runs", "data-ingestion", "warning"],
):
    """
    Sends a message to a Discord webhook

    Args:
        message (str): The message to send.
        username (str, optional): The username to use when sending the message. Defaults to None.
    """
    environment = get_environment()
    secret_name = f"DISCORD_WEBHOOK_URL_{monitor_slug.upper()}"
    webhook_url = get_secret(secret_name=secret_name, environment=environment).get(secret_name)

    if len(text_content) > 2000:
        raise ValueError(f"Message content is too long: {len(text_content)} > 2000 characters.")

    async with aiohttp.ClientSession() as session:
        kwargs = {"content": text_content, "allowed_mentions": AllowedMentions(users=True)}
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
        try:
            await webhook.send(**kwargs)
        except RuntimeError:
            raise ValueError(f"Error sending message to Discord webhook: {webhook_url}")


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

    header_content = f"""
## {title}
> Environment: {environment}
> Flow Run: [{flow_name}](https://pipelines.dados.rio/flow-run/{flow_run_id})
> Task Run: [{task_name}](https://pipelines.dados.rio/task-run/{task_run_id})
    """
    # Calculate max char count for message
    message_max_char_count = 2000 - len(header_content)

    # Split message into lines
    message_lines = message.split("\n")

    # Split message into pages
    pages = []
    current_page = ""
    for line in message_lines:
        if len(current_page) + 2 + len(line) < message_max_char_count:
            current_page += "\n" + line
        else:
            pages.append(current_page)
            current_page = line

    # Append last page
    pages.append(current_page)

    # Build message content using Header in first page
    message_contents = []
    for page_idx, page in enumerate(pages):
        if page_idx == 0:
            message_contents.append(header_content + page)
        else:
            message_contents.append(page)

    # Send message to Discord
    async def main(contents):
        for content in contents:
            await send_discord_webhook(
                text_content=content,
                file_path=file_path,
                username=username,
                monitor_slug=monitor_slug,
            )

    asyncio.run(main(message_contents))
