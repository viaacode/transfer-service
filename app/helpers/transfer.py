#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import shlex
from typing import List

from paramiko import AutoAddPolicy, SSHClient, SSHException

from viaa.configuration import ConfigParser


configParser = ConfigParser()
config = configParser.app_cfg
dest_conf = config["destination"]


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
                # TODO: log error
                # TODO: raise exception
                return
            if out:
                try:
                    results = out[0].split(",")
                    status_code = results[1]
                    if int(status_code) >= 400:
                        # TODO: log error
                        # TODO: raise exception
                        pass
                    # TODO: log success
                except IndexError:
                    # TODO: log error
                    # TODO: raise exception
                    pass
        except SSHException:
            # TODO: log error
            # TODO: raise exception
            pass
