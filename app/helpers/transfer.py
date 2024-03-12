#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import shlex
import threading
import time
from socket import gaierror
from ftplib import FTP, error_perm
from typing import List
from urllib.parse import urlparse

import requests
from paramiko import AutoAddPolicy, SSHClient, SSHException
from retry import retry
from viaa.configuration import ConfigParser
from viaa.observability import logging
from hvac.exceptions import InvalidPath, Forbidden

from app.services.vault import VaultClient


config_parser = ConfigParser()
config = config_parser.app_cfg
log = logging.get_logger(__name__, config=config_parser)
dest_conf = config["destination"]
NUMBER_PARTS = 4


class TransferPartException(Exception):
    pass


class TransferException(Exception):
    pass


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
    destination: str,
    source_url: str,
    s3_domain: str,
    part_range: str,
    source_username: str = None,
    source_password: str = None,
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
        source_username: The username for fetching the source file (optional).
        source_password: The password for fetching the source file (optional).

    Returns:
        The cURL command shell-escaped
    """
    command = [
        "curl",
        "-w",
        "%{http_code},time: %{time_total}s,size: %{size_download} bytes,speed: %{speed_download}b/s",
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
    ]
    if source_username and source_password:
        command.extend(["-u", f"{source_username}:{source_password}"])
    command.append(source_url)
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


def calculate_filename_part(file: str, idx: int, directory: str = None) -> str:
    """Convenience method for calculating the filename of a part."""
    part = f"{file}.part{idx}"
    if directory:
        return os.path.join(directory, part)
    else:
        return part


class Transfer:
    def __init__(self, message: dict, vault_client: VaultClient):
        """Initialize a Transfer.

        Args:
            message: Contains the information of the source file and the destination
                filename."""
        self.domain = message["source"]["headers"].get("host")
        self.destination_path = message["destination"]["path"]

        self.dest_folder_dirname = os.path.dirname(self.destination_path)
        self.dest_file_basename = os.path.basename(self.destination_path)

        self.dest_file_tmp_basename = f"{self.dest_file_basename}.tmp"
        dest_folder_tmp_basename = f"{self.dest_file_basename}.part"
        self.dest_folder_tmp_dirname = os.path.join(
            self.dest_folder_dirname, dest_folder_tmp_basename
        )

        self.source_url = message["source"]["url"]
        self.size_in_bytes = 0

        # SSH client
        self.remote_client = None
        # SFTP client
        self.sftp = None

        self.remote_server_host = message["destination"]["host"]

        # Credentials (secret) for destination
        secret_path_destination = message["destination"]["credentials"]
        try:
            vault_client.fetch_secret(secret_path_destination)
        except (InvalidPath, Forbidden) as vault_error:
            raise TransferException(
                f"Can not retrieve secret for path: '{secret_path_destination}'. Error: '{vault_error}'"
            )

        self.host_username = vault_client.get_username(secret_path_destination)
        self.host_password = vault_client.get_password(secret_path_destination)

        # Credentials (secret) for source if provided
        try:
            secret_path_source = message["source"]["credentials"]
        except KeyError:
            self.source_username = None
            self.source_password = None
        else:
            try:
                vault_client.fetch_secret(secret_path_source)
            except (InvalidPath, Forbidden) as vault_error:
                raise TransferException(
                    f"Can not retrieve secret for path: '{secret_path_source}'. Error: '{vault_error}'"
                )
            self.source_username = vault_client.get_username(secret_path_source)
            self.source_password = vault_client.get_password(secret_path_source)

    def _init_remote_client(self):
        # SSH client
        self.remote_client = SSHClient()
        self.remote_client.set_missing_host_key_policy(AutoAddPolicy())
        self.remote_client.connect(
            self.remote_server_host,
            port=22,
            username=self.host_username,
            password=self.host_password,
        )
        # SFTP client
        self.sftp = self.remote_client.open_sftp()

    @retry(TransferPartException, tries=3, delay=3, logger=log)
    def _transfer_part(
        self,
        dest_file_full: str,
        part_range: str,
    ):
        """Connect to a remote server via SSH and download a part via cURL.

        Connecting to the server is via user/pass. The host keys will be automatically
        added.

        Args:
            dest_file_full: The full filename of the destination file.
            part_range: The range of the part to fetch in format "{x}-{y}"
                with x, y integers and x<=y.
        """
        # Build the cURL command
        curl_cmd = build_curl_command(
            dest_file_full,
            self.source_url,
            self.domain,
            part_range,
            source_username=self.source_username,
            source_password=self.source_password,
        )
        with SSHClient() as remote_client:
            try:
                remote_client.set_missing_host_key_policy(AutoAddPolicy())
                remote_client.connect(
                    self.remote_server_host,
                    port=22,
                    username=self.host_username,
                    password=self.host_password,
                )
                # Execute the cURL command and examine results
                _stdin, stdout, stderr = remote_client.exec_command(curl_cmd)
                results = []
                out = stdout.readlines()
                err = stderr.readlines()
                if err:
                    log.error(
                        f"Error occurred when cURLing part: {err}",
                        destination=dest_file_full,
                    )
                    raise TransferPartException
                if out:
                    try:
                        results = out[0].split(",")
                        status_code = results[0]
                        if int(status_code) >= 400:
                            log.error(
                                f"Error occurred when cURLing part with status code: {status_code}",
                                destination=dest_file_full,
                            )
                            raise TransferPartException
                        log.info(
                            "Successfully cURLed part",
                            destination=dest_file_full,
                            results=results,
                        )
                    except IndexError as i_e:
                        log.error(
                            f"Error occurred cURLing part: {i_e}",
                            destination=dest_file_full,
                        )
                        raise TransferPartException
            except SSHException as ssh_e:
                log.error(
                    f"SSH Error occurred when cURLing part: {ssh_e}",
                    destination=dest_file_full,
                )
                raise TransferPartException

    def _fetch_size(self) -> int:
        """Fetch the size of the file on Castor.

        Depending on the protocol the logic is different:
            HTTP(s): The size is in the "content-length" response header.
            FTP: Open a FTP connection to retrieve the size of the file.

        Returns:
            The size of the file in bytes.

        Raises:
            TransferException: If it was not possible to get the size of the file,
                e.g. a 404.
            ValueError: If the source url contains an unknown protocol.
        """

        source_url_parsed = urlparse(self.source_url)
        if source_url_parsed.scheme in ("http", "https"):
            size_in_bytes = requests.head(
                self.source_url,
                allow_redirects=True,
                headers={"host": self.domain, "Accept-Encoding": "identity"},
            ).headers.get("content-length", None)

            if not size_in_bytes:
                log.error(
                    "Failed to get size of file on Castor", source_url=self.source_url
                )
                raise TransferException
        elif source_url_parsed.scheme in ("ftp",):
            try:
                with FTP(host=source_url_parsed.netloc, encoding="utf-8") as ftp:
                    ftp.login(
                        user=self.source_username,
                        passwd=self.source_password,
                    )
                    size_in_bytes = ftp.size(source_url_parsed.path)
            except (gaierror, error_perm) as e:
                raise TransferException(f"Failed to get size of file on the FTP: {e}")
        else:
            raise ValueError(f"Protocol not supported: {self.source_url}")
        return size_in_bytes

    def _check_target_folder(self):
        """Check if target folder exists.

        Raises:
            OSError: When target folder does not exist.
        """
        try:
            self.sftp.stat(self.dest_folder_dirname)
        except FileNotFoundError:
            raise OSError(f"Target folder does not exist: {self.dest_folder_dirname}")

    def _check_free_space(self):
        """Check if there is sufficient free space on the remote server.

        The free space needs to be a higher than a given percentage in order
        to be allowed to send the file over. If the space is lower, then it
        will retry until the space is freed.

        This free space check is optional, in the sense that if the
        `free_space_percentage` config var is empty, it will assume that
        a transfer is always allowed.
        """
        try:
            percentage_limit = int(dest_conf["free_space_percentage"])
        except ValueError:
            percentage_limit = ""

        # If percentage limit is not filled in, skip the check.
        if percentage_limit:
            while True:
                # Check the used space in percentage
                _stdin, stdout, _stderr = self.remote_client.exec_command(
                    f"df --output=pcent {self.dest_folder_dirname} | tail -1"
                )
                out = stdout.readlines()
                # Parse the used percentage as an int.
                try:
                    percentage_used = int(
                        out[0].strip().split("%")[0]  # Output example: [' 12%\n']
                    )
                except ValueError:
                    log.warning("Could not get used percentage")
                    break

                free_percentage = 100 - percentage_used
                log.info(
                    f"Free space: {free_percentage}%. Space needed: {percentage_limit}%"
                )
                if free_percentage > percentage_limit:
                    break
                else:
                    time.sleep(120)

    def _prepare_target_transfer(self):
        """Prepare for transferring the file to the remote server.

        Do the following:
        - Check if the file does not exist yet.
        - Create the tmp folder if it does not exist yet. Note that if the tmp folder
          already exists, we'll just continue.

        Raises:
            OSError:
                -The file already exists.
                -The tmp folder couldn't be created.
            TransferException: When a SSH error occurred.
        """
        # Check if file doesn't exist yet and make the tmp dir

        try:
            # Check if the file does not exist yet
            try:
                self.sftp.stat(self.destination_path)
            except FileNotFoundError:
                # Continue
                pass
            else:
                # If the file exists stop.
                log.error("File already exists", destination=self.destination_path)
                raise OSError

            # Create tmp folder if it doesn't exist yet
            try:
                self.sftp.mkdir(self.dest_folder_tmp_dirname)
            except OSError as os_e:
                # If the folder already exists, just continue
                try:
                    self.sftp.stat(self.dest_folder_tmp_dirname)
                except FileNotFoundError:
                    log.error(
                        f"Error occurred when creating tmp folder: {os_e}",
                        tmp_folder=self.dest_folder_tmp_dirname,
                    )
                    raise os_e
        except SSHException as ssh_e:
            log.error(
                f"SSH Error occurred: {ssh_e}",
                tmp_folder=self.dest_folder_tmp_dirname,
            )
            raise TransferException

    def _transfer_parts(self):
        """Transfer the file in separate parts.

        Split up a file in a certain amount of parts. Transfer each part simultaneously
        in a separate thread. Wait for the threads to finish, thus wait for all the
        parts to finish transferring.
        """
        parts = calculate_ranges(int(self.size_in_bytes), NUMBER_PARTS)
        threads = []
        for idx, part in enumerate(parts):
            dest_file_part_full = calculate_filename_part(
                self.dest_file_basename, idx, directory=self.dest_folder_tmp_dirname
            )
            thread = threading.Thread(
                target=self._transfer_part,
                args=(
                    dest_file_part_full,
                    part,
                ),
            )
            threads.append(thread)
            thread.start()
            log.debug(f"Thread started for: {dest_file_part_full}")

        # Wait for the parts to finish transferring
        for thread in threads:
            thread.join()

    def _assemble_parts(self):
        """Assemble the parts into the destination file.

        The parts are first concatenated to a tmp file in the tmp folder.
        If the size of the assembled file is correct, the tmp file will be renamed
        as the destination file in the correct folder.

        The parts and the tmp folder will be removed.

        Raises:
            TransferException: If an OSError occurs.
        """
        log.info("Start assembling the parts", destination=self.destination_path)
        try:
            _stdin, stdout, stderr = self.remote_client.exec_command(
                build_assemble_command(
                    self.dest_folder_tmp_dirname,
                    self.dest_file_basename,
                    NUMBER_PARTS,
                )
            )
            # This waits for assembling to finish?
            _ = stdout.readlines()
            _ = stderr.readlines()

            self.dest_file_tmp_filename = os.path.join(
                self.dest_folder_tmp_dirname, self.dest_file_tmp_basename
            )

            # Check if file has the correct size
            file_attrs = self.sftp.stat(self.dest_file_tmp_filename)
            if file_attrs.st_size != int(self.size_in_bytes):
                log.error(
                    f"Size of assembled file: {file_attrs.st_size}, expected size: {self.size_in_bytes}",
                    source_url=self.source_url,
                    destination_filename=self.dest_file_tmp_filename,
                )
                raise TransferException
            # Rename and move file destination folder
            self.sftp.rename(
                self.dest_file_tmp_filename,
                self.destination_path,
            )
            # Touch the file so MH picks it up
            # Explicitly use a `SSH touch` as `SFTP utime` doesn't work
            self.remote_client.exec_command(f"touch '{self.destination_path}'")

            # Delete the parts
            for idx in range(NUMBER_PARTS):
                try:
                    self.sftp.remove(
                        calculate_filename_part(
                            self.dest_file_basename,
                            idx,
                            directory=self.dest_folder_tmp_dirname,
                        )
                    )
                except FileNotFoundError:
                    # Only delete parts that exist
                    pass
            # Delete the tmp folder
            self.sftp.rmdir(self.dest_folder_tmp_dirname)
            log.info("File successfully transferred", destination=self.destination_path)
        except OSError as os_e:
            log.error(
                f"Error occurred when assembling parts: {os_e}",
                destination=self.destination_path,
            )
            raise TransferException

    @retry(TransferException, tries=3, delay=3, logger=log)
    def transfer(self):
        """Transfer a file to a remote server.

        First we'll make the tmp dir to transfer the parts to.
        Then, fetch the size of the file to determine how to split it up in parts.
        Split up in parts and each part will be separately transferred in its own Thread.
        When the threads are done, assemble the file.

        Rename/move the assembled file to its correct destination folder.
        Lastly, remove the parts and tmp folder.
        """

        try:
            log.info(f"Start transferring of file: {self.source_url}")

            # initialize the SSH client
            self._init_remote_client()

            # Check if target folder exists
            self._check_target_folder()

            # Check freespace
            self._check_free_space()

            # Fetch size of the file to transfer
            self.size_in_bytes = self._fetch_size()

            # Check if file doesn't exist yet and make the tmp dir
            self._prepare_target_transfer()
        finally:
            self.remote_client.close()

        # Transfer the parts
        self._transfer_parts()

        try:
            # Re-initialize the SSH client
            self._init_remote_client()
            # Assemble the parts
            self._assemble_parts()
        finally:
            self.remote_client.close()
