#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import shlex
import threading
from typing import List

import requests
from paramiko import AutoAddPolicy, SSHClient, SSHException

from viaa.configuration import ConfigParser
from viaa.observability import logging

config_parser = ConfigParser()
config = config_parser.app_cfg
log = logging.get_logger(__name__, config=config_parser)
dest_conf = config["destination"]
NUMBER_PARTS = 4


def calculate_ranges(size_bytes: int, number_parts: int) -> List[str]:
    """Split the filesize up in multiple ranges.

    Args:
        size_bytes: The size of the file in bytes.
        number_parts: The amount of parts.

    Returns:
        List of ranges, with the range in format "{x}-{y}".
        With x and y integers and x <= y.
        Format of list: ["0-{x}", "{x+1}-{y}", ... ,"{z+1}-{size_bytes}"].

    Raises:
        ValueError: If the amount of parts is greater than the size.
    """
    if number_parts > size_bytes:
        raise ValueError(
            f"Amount of parts '{number_parts}' is greater than the size '{size_bytes}'"
        )
    ranges = []
    part_size = size_bytes / number_parts
    for i in range(number_parts):
        if i == 0:
            ranges.append(f"{i}-{round(part_size)}")
        else:
            ranges.append(f"{round(i * part_size) + 1}-{round(((i + 1) * part_size))}")
    return ranges


def build_curl_command(
    destination: str, source_url: str, s3_domain: str, part_range: str
) -> str:
    """Build the cURL command.

    The args "-S -s" are used so that the progress bar is not shown but errors are.
    In combination with "-w", it will output information of the download after
    completion.

    Args:
        destination: Full filename path of destination file.
        source_url: The URL to fetch the file from.
        s3_domain: The S3 domain to pass as header.
        part_range: The range of the part to fetch in format "{x}-{y}"
            with x, y integers and x<=y.

    Returns:
        The cURL command shell-escaped
    """
    command = [
        "curl",
        "-w",
        "%{speed_download},%{http_code},%{size_download},%{url_effective},%{time_total}",
        "-L",
        "-H",
        f"host: {s3_domain}",
        "-H",
        f"range: bytes={part_range}",
        "-r",
        part_range,
        "-S",
        "-s",
        "-o",
        destination,
        source_url,
    ]
    return shlex.join(command)


def build_assemble_command(
    dest_folder_dirname: str, dest_file_basename: str, parts: int
) -> str:
    """Build the assemble command.

    Command consists of changing into directory (cd) and (&&) assembling the
    parts (cat) into a filename with suffix ".tmp".

    Args:
        dest_folder_dirname: The dirname of the destination folder.
        dest_file_basename: The basename of the destination file.
        parts: The amount of parts.

    Returns:
        The assemble command shell-escaped.
    """
    assemble_command = ["cd", dest_folder_dirname, "&&", "cat"]
    for i in range(parts):
        assemble_command.append(calculate_filename_part(dest_file_basename, i))
    assemble_command.extend([">", f"{dest_file_basename}.tmp"])
    return shlex.join(assemble_command).replace("'&&'", "&&").replace("'>'", ">")


def calculate_filename_part(file: str, idx: int) -> str:
    """Convenience method for calculating the filename of a part."""
    return f"{file}.part{idx}"


def transfer_part(
    dest_file_full: str,
    source_url: str,
    s3_domain: str,
    part_range: str,
):
    """Connect to a remote server via SSH and download a part via cURL.

    Connecting to the server is via user/pass. The host keys will be automatically
    added.

    Args:
        dest_file_full: The full filename of the destination file.
        source_url: The URL to fetch the part from.
        s3_domain: The S3 domain to pass as HTTP header.
        part_range: The range of the part to fetch in format "{x}-{y}"
            with x, y integers and x<=y.
    """
    # Build the cURL command
    curl_cmd = build_curl_command(
        dest_file_full,
        source_url,
        s3_domain,
        part_range,
    )
    with SSHClient() as remote_client:
        try:
            remote_client.set_missing_host_key_policy(AutoAddPolicy())
            remote_client.connect(
                dest_conf["host"],
                port=22,
                username=dest_conf["user"],
                password=dest_conf["password"],
            )
            # Execute the cURL command and read examine results
            _stdin, stdout, stderr = remote_client.exec_command(curl_cmd)
            results = []
            out = stdout.readlines()
            err = stderr.readlines()
            if err:
                log.error(
                    f"Error occurred when cURLing part: {err}",
                    destination=dest_file_full,
                )
                # TODO: raise exception
                return
            if out:
                try:
                    results = out[0].split(",")
                    status_code = results[1]
                    if int(status_code) >= 400:
                        log.error(
                            f"Error occurred when cURLing part with status code: {status_code}",
                            destination=dest_file_full,
                        )
                        # TODO: raise exception
                    log.info("Successfully cURLed part", destination=dest_file_full)
                except IndexError as i_e:
                    log.error(
                        f"Error occurred cURLing part: {i_e}",
                        destination=dest_file_full,
                    )
                    # TODO: raise exception
        except SSHException as ssh_e:
            log.error(
                f"SSH Error occurred when cURLing part: {ssh_e}",
                destination=dest_file_full,
            )
            # TODO: raise exception


def transfer(message: dict):
    """Transfer a file to a remote server

    First we'll make the tmp dir to transfer the parts to.
    Then, fetch the size of the file to determine how to split it up in parts.
    Split up in parts and each part will be separately transferred in its own Thread.
    When the threads are done, assemble the file.

    Rename/move the assembled file to its correct destination folder.
    Lastly, remove the parts and tmp folder.

    Args:
        message: Contains the information of the source file and the destination
            filename.
    """
    domain = message["source"]["domain"]["name"]
    destination_path = message["destination"]["path"]

    dest_folder_dirname = os.path.dirname(destination_path)
    dest_file_basename = os.path.basename(destination_path)

    dest_file_tmp_basename = f"{dest_file_basename}.tmp"
    dest_folder_tmp_basename = f".{dest_file_basename}"
    dest_folder_tmp_dirname = os.path.join(
        dest_folder_dirname, dest_folder_tmp_basename
    )

    bucket = message["source"]["bucket"]["name"]
    key = message["source"]["object"]["key"]
    source_url = f"http://{config['source']['swarmurl']}/{bucket}/{key}"

    log.info(f"Start transferring of file: {source_url}")

    # Fetch size of the file to transfer
    size_in_bytes = requests.head(
        source_url,
        allow_redirects=True,
        headers={"host": domain, "Accept-Encoding": "identity"},
    ).headers.get("content-length", None)

    if not size_in_bytes:
        log.error("Failed to get size of file on Castor", source_url=source_url)
        # TODO: raise exception

    # Make the tmp dir
    with SSHClient() as remote_client:
        try:
            remote_client.set_missing_host_key_policy(AutoAddPolicy())
            remote_client.connect(
                dest_conf["host"],
                port=22,
                username=dest_conf["user"],
                password=dest_conf["password"],
            )
            sftp = remote_client.open_sftp()
            try:
                sftp.mkdir(dest_folder_tmp_dirname)
            except OSError as os_e:
                log.error(
                    f"Error occurred when creating tmp folder: {os_e}",
                    tmp_folder=dest_folder_tmp_dirname,
                )
                raise
        except SSHException as ssh_e:
            log.error(
                f"SSH Error occurred creating tmp folder: {ssh_e}",
                tmp_folder=dest_folder_tmp_dirname,
            )
            # TODO: raise exception

    # Transfer the parts
    parts = calculate_ranges(int(size_in_bytes), NUMBER_PARTS)
    threads = []
    for idx, part in enumerate(parts):
        thread = threading.Thread(
            target=transfer_part,
            args=(
                os.path.join(
                    dest_folder_tmp_dirname,
                    calculate_filename_part(dest_file_basename, idx),
                ),
                source_url,
                domain,
                part,
            ),
        )
        threads.append(thread)
        thread.start()
        log.debug(f"Thread started for {destination_path}")

    for thread in threads:
        thread.join()

    # Assemble the parts
    with SSHClient() as remote_client:
        try:
            remote_client.set_missing_host_key_policy(AutoAddPolicy())
            remote_client.connect(
                dest_conf["host"],
                port=22,
                username=dest_conf["user"],
                password=dest_conf["password"],
            )
            _stdin, stdout, stderr = remote_client.exec_command(
                build_assemble_command(
                    dest_folder_tmp_dirname, dest_file_basename, NUMBER_PARTS
                )
            )
            # This waits for assembling to finish?
            _ = stdout.readlines()
            _ = stderr.readlines()

            sftp = remote_client.open_sftp()
            sftp.chdir(dest_folder_tmp_dirname)

            # Check if file has the correct size
            file_attrs = sftp.stat(dest_file_tmp_basename)
            if file_attrs.st_size != int(size_in_bytes):
                log.error(
                    f"Size of assembled file: {file_attrs.st_size}, expected size: {size_in_bytes}",
                    source_url=source_url,
                    destination_basename=dest_file_tmp_basename,
                )
                # TODO: raise exception
            # Rename and move file destination folder
            sftp.rename(
                os.path.join(dest_folder_tmp_dirname, dest_file_tmp_basename),
                os.path.join(dest_folder_dirname, dest_file_basename),
            )
            # Delete the parts
            for idx in range(NUMBER_PARTS):
                try:
                    sftp.remove(calculate_filename_part(dest_file_basename, idx))
                except FileNotFoundError:
                    # Only delete parts that exist
                    pass
            # Delete the tmp folder
            sftp.rmdir(dest_folder_tmp_dirname)
            log.info("File successfully transferred", destination=destination_path)
        except IOError as io_e:
            log.error(
                f"Error occurred when assembling parts: {io_e}",
                destination=destination_path,
            )
            # TODO: raise exception
