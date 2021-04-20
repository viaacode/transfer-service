#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pytest
from unittest.mock import MagicMock, patch

from paramiko import SSHException

from app.helpers.transfer import (
    build_curl_command,
    calculate_filename_part,
    calculate_ranges,
    Transfer,
    TransferException,
    TransferPartException,
)


@pytest.mark.parametrize(
    "size, number_parts, expected",
    [
        (1303, 4, ["0-326", "327-652", "653-977", "978-1303"]),
        (6, 6, ["0-1", "2-2", "3-3", "4-4", "5-5", "6-6"]),
        (1000, 1, ["0-1000"]),
    ],
)
def test_calculate_ranges(size, number_parts, expected):
    assert calculate_ranges(size, number_parts) == expected


@pytest.mark.parametrize(
    "size, number_parts, side_effect, message",
    [
        (1000, 0, ZeroDivisionError, "division by zero"),
        (2, 4, ValueError, "Amount of parts '4' is greater than the size '2'"),
    ],
)
def test_calculate_ranges_error(size, number_parts, side_effect, message):
    with pytest.raises(side_effect) as e:
        calculate_ranges(size, number_parts)
    assert str(e.value) == message


def test_build_curl_command():
    dest = "dest file"
    src = "source file"
    domain = "S3 domain"
    r = "0-100"
    w_params = "%{http_code},time: %{time_total}s,size: %{size_download} bytes,speed: %{speed_download}b/s"
    curl_command = build_curl_command(dest, src, domain, r)
    assert (
        curl_command
        == f"curl -w '{w_params}' -L -H 'host: {domain}' -H 'range: bytes={r}' -r {r} -S -s -o '{dest}' '{src}'"
    )


def test_calculate_filename_part():
    assert calculate_filename_part("file.mxf", 0) == "file.mxf.part0"


class TestTransfer:
    @pytest.fixture()
    def transfer(self) -> Transfer:
        msg = {
            "source": {
                "domain": {"name": "domain"},
                "object": {"key": "file.mxf"},
                "bucket": {"name": "bucket"},
            },
            "destination": {"path": "/s3-transfer-test/file.mxf"},
        }

        return Transfer(msg)

    @patch("app.helpers.transfer.build_curl_command", return_value="curl")
    @patch("app.helpers.transfer.SSHClient")
    def test_transfer_part(
        self, ssh_client_mock, build_curl_command_mock, transfer, caplog
    ):
        """Successful transfer of a part."""
        stdin_mock, stdout_mock, stderr_mock = (MagicMock(), MagicMock(), MagicMock())
        # Mock the stdout result of the cURL command
        stdout_result = ["206,time: 5s,size: 1000 bytes,speed: 200b/s"]
        stdout_mock.readlines.return_value = stdout_result
        # Mock stderr to be empty
        stderr_mock.readlines.return_value = []

        # Mock exec command
        client_mock = ssh_client_mock().__enter__()
        client_mock.exec_command.return_value = (stdin_mock, stdout_mock, stderr_mock)

        transfer._transfer_part("dest", "0-100")

        assert client_mock.set_missing_host_key_policy.call_count == 1
        assert client_mock.connect.call_count == 1
        assert client_mock.connect.call_args.args == ("ssh_host",)
        assert client_mock.connect.call_args.kwargs == {
            "port": 22,
            "username": "ssh_user",
            "password": "ssh_pass",
        }

        # Check if curl command gets called with the correct arguments
        build_curl_command_mock.assert_called_once_with(
            "dest", "http://url/bucket/file.mxf", "domain", "0-100"
        )

        assert client_mock.exec_command() == (stdin_mock, stdout_mock, stderr_mock)
        assert "Successfully cURLed part" in caplog.messages

    @patch("app.helpers.transfer.SSHClient")
    @patch("time.sleep", MagicMock())
    def test_transfer_part_status_code(self, ssh_client_mock, transfer, caplog):
        """HTTP error occurs when transferring a part."""
        stdin_mock, stdout_mock, stderr_mock = (MagicMock(), MagicMock(), MagicMock())
        # Mock the stdout result of the cURL command
        stdout_result = ["416,time: 5s,size: 1000 bytes,speed: 200b/s"]
        stdout_mock.readlines.return_value = stdout_result
        # Mock stderr to be empty
        stderr_mock.readlines.return_value = []

        # Mock exec command
        client_mock = ssh_client_mock().__enter__()
        client_mock.exec_command.return_value = (stdin_mock, stdout_mock, stderr_mock)
        with pytest.raises(TransferPartException):
            transfer._transfer_part("dest", "0-100")

        assert client_mock.set_missing_host_key_policy.call_count == 3
        assert client_mock.connect.call_count == 3
        assert client_mock.connect.call_args.args == ("ssh_host",)
        assert client_mock.connect.call_args.kwargs == {
            "port": 22,
            "username": "ssh_user",
            "password": "ssh_pass",
        }
        assert client_mock.exec_command() == (stdin_mock, stdout_mock, stderr_mock)
        assert (
            "Error occurred when cURLing part with status code: 416" in caplog.messages
        )

    @patch("app.helpers.transfer.SSHClient")
    @patch("time.sleep", MagicMock())
    def test_transfer_part_stderr(self, ssh_client_mock, transfer, caplog):
        """Transferring a part resulting in stderr output."""
        stdin_mock, stdout_mock, stderr_mock = (MagicMock(), MagicMock(), MagicMock())
        # Mock the stderr result of the cURL command
        stderr_result = ["Error"]
        stderr_mock.readlines.return_value = stderr_result

        # Mock exec command
        client_mock = ssh_client_mock().__enter__()
        client_mock.exec_command.return_value = (stdin_mock, stdout_mock, stderr_mock)
        with pytest.raises(TransferPartException):
            transfer._transfer_part("dest", "0-100")
        assert "Error occurred when cURLing part: ['Error']" in caplog.messages

    @patch("app.helpers.transfer.SSHClient")
    @patch("time.sleep", MagicMock())
    def test_transfer_part_ssh_exception(self, ssh_client_mock, transfer, caplog):
        """SSH Exception occurs when connecting."""
        client_mock = ssh_client_mock().__enter__()
        client_mock.connect.side_effect = SSHException("Connection error")
        with pytest.raises(TransferPartException):
            transfer._transfer_part("dest", "0-100")
        assert not client_mock.exec_command.call_count
        assert (
            "SSH Error occurred when cURLing part: Connection error" in caplog.messages
        )
