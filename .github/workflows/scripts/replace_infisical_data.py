# -*- coding: utf-8 -*-
from pathlib import Path
from sys import argv, exit

FILE_PATH = Path("./pipelines/constants.py")
REPLACE_INFISICAL_ADDRESS = "AUTO_REPLACE_INFISICAL_ADDRESS"
REPLACE_INFISICAL_TOKEN = "AUTO_REPLACE_INFISICAL_TOKEN"


def replace_in_text(orig_text: str, find_text: str, replace_text: str) -> str:
    """
    Replaces the `find_text` with `replace_text` in the `orig_text`.
    """
    return orig_text.replace(find_text, replace_text)


def update_file(file_path: Path, address: str, token: str) -> None:
    """
    Updates the `DOCKER_TAG` variable in the `constants.py` file.
    """
    with file_path.open("r") as file:
        text = file.read()
    text = replace_in_text(text, REPLACE_INFISICAL_ADDRESS, address)
    text = replace_in_text(text, REPLACE_INFISICAL_TOKEN, token)
    with file_path.open("w") as file:
        file.write(text)


if __name__ == "__main__":
    if len(argv) != 3:
        print("Usage: replace_infisical_data.py <address> <token>")
        exit(1)
    address, token = argv[1], argv[2]
    update_file(FILE_PATH, address, token)
