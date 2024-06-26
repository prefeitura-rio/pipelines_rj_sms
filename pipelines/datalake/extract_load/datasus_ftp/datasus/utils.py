# -*- coding: utf-8 -*-
# pylint: disable=C0103, R0913, C0301, W3101
# flake8: noqa: E501
"""
Utilities for DataSUS pipelines
"""

import re
import shutil
import tempfile
from ftplib import FTP

from prefeitura_rio.pipelines_utils.logging import log


def check_newest_file_version(host: str, directory: str, base_file_name: str):
    """
    Check the newest version of a file in a given FTP directory.

    Args:
        host (str): FTP server hostname.
        user (str): FTP server username.
        password (str): FTP server password.
        directory (str): FTP directory path.
        file_name (str): Base name of the file to check.

    Returns:
        str: The name of the newest version of the file.
    """

    # list all files in the directory
    ftp = FTP(host)
    ftp.login()
    ftp.cwd(directory)

    files = ftp.nlst()
    ftp.quit()

    # filter a list of files that contains the base file name
    matching_files = [file for file in files if base_file_name in file]
    log(f"Matching files: {matching_files}", level="debug")

    # sort list descending
    matching_files.sort(reverse=True)
    newest_file = matching_files[0]

    # extract snapshot date from file
    snapshot_date = re.findall(r"\d{6}", newest_file)[0]
    snapshot_date = f"{snapshot_date[:4]}-{snapshot_date[-2:]}"

    log(f"Newest file: {newest_file}, snapshot_date: {snapshot_date}")
    return {"file": newest_file, "snapshot": snapshot_date}


def fix_cnes_header(file_path: str):
    """
    Modifies the header of a file located at `file_path` to conform to BigQuery requirements.

    Args:
        file_path (str): The path to the file to be modified.

    Returns:
        str: The path of the modified file.

    Raises:
        None
    """

    log(f"Conforming file: {file_path}", level="debug")

    # create a temporary file
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as tf:
        # open the original file in read mode
        with open(file_path, "r", encoding="iso8859-1") as f:
            # read the first line
            first_line = f.readline()

            # modify the first line
            modified_first_line = first_line.replace("TO_CHAR(", "")
            modified_first_line = modified_first_line.replace(",'DD/MM/YYYY')", "")
            modified_first_line = modified_first_line.replace("A.DT", "DT")
            modified_first_line = modified_first_line.replace("'CO_CPF'", "CO_CPF")

            # write the modified first line to the temporary file
            tf.write(modified_first_line)

            # copy the rest of the lines from the original file to the temporary file
            shutil.copyfileobj(f, tf)

    # replace the original file with the temporary file
    shutil.move(tf.name, file_path)

    log(f"Conformed: {file_path}", level="debug")
    return file_path
