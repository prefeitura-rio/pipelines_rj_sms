import aiohttp
from discord import Webhook
import asyncio

from prefeitura_rio.pipelines_utils.infisical import get_secret


def send_message(message, username=None):
    async def send_message(message, username=None):
        webhook_url = get_secret(
            secret_name="DISCORD_WEBHOOK_URL",
            environment='prod'
        )
        async with aiohttp.ClientSession() as session:
            webhook = Webhook.from_url(webhook_url, session=session)
            if username:
                await webhook.send(message, username=username)
            else:
                await webhook.send(message)

    asyncio.run(send_message(message, username))