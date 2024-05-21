import prefect
import logging

from typing_extensions import Literal
from pipelines.utils.monitor import send_message


LEVELS_CONFIG = {
    "debug": {"type": logging.DEBUG, "discord_forwarding": False, "icon": "🟦"},
    "info": {"type": logging.INFO, "discord_forwarding": False, "icon": "🟩"},
    "warning": {"type": logging.WARNING, "discord_forwarding": False, "icon": "⚠️"},
    "error": {"type": logging.ERROR, "discord_forwarding": True, "icon": "❌"},
    "critical": {"type": logging.CRITICAL, "discord_forwarding": True, "icon": "🔴"},
}


def log(
    msg: str,
    level: Literal["debug", "info", "warning", "error", "critical"] = "info",
    force_discord_forwarding: bool = False,
) -> None:
    """
    Logs a message with the specified log level. In addition to logging the message,
        it also sends the message to a Discord webhook if the log level is configured
        to do so.

    Args:
        msg (str): The message to be logged.
        level (Literal["debug", "info", "warning", "error", "critical"]): The log level.

    Returns:
        None
    """
    prefect.context.get("logger").log(LEVELS_CONFIG[level]["type"], msg)

    if LEVELS_CONFIG[level]["discord_forwarding"] or force_discord_forwarding:
        icon = LEVELS_CONFIG[level]["icon"]
        title = f"{icon} Log {level.capitalize()}"
        send_message(
            title=title,
            message=msg,
            monitor_slug="warning"
        )
