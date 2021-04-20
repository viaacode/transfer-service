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

    @patch("requests.head")
    def test_fetch_size(self, head_mock, transfer):
        """Response contains a "content-length" response header with the size."""
        # Mock return size of file
        head_mock().headers = {"content-length": 1000}

        size = transfer._fetch_size()
        assert size == 1000

    @patch("requests.head")
    def test_fetch_size_error(self, head_mock, transfer, caplog):
        """No "content-length" response header."""
        # Mock return size of file
        head_mock().headers = {}
        with pytest.raises(TransferException):
            transfer._fetch_size()
        log_record = caplog.records[0]
        assert log_record.level == "error"
        assert log_record.message == "Failed to get size of file on Castor"

    @patch("app.helpers.transfer.SSHClient")
    def test_prepare_target_transfer(self, ssh_client_mock, transfer):
        """File does not exist and folder is created"""
        client_mock = ssh_client_mock().__enter__()
        client_mock.open_sftp().stat.side_effect = FileNotFoundError

        transfer._prepare_target_transfer()

        sftp_mock = client_mock.open_sftp()

        sftp_mock.stat.assert_called_once_with("/s3-transfer-test/file.mxf")
        sftp_mock.mkdir.assert_called_once_with("/s3-transfer-test/.file.mxf")

    @patch("app.helpers.transfer.SSHClient")
    def test__prepare_target_transfer_file_exists(
        self, ssh_client_mock, transfer, caplog
    ):
        """File already exist."""
        client_mock = ssh_client_mock().__enter__()

        with pytest.raises(OSError):
            transfer._prepare_target_transfer()

        sftp_mock = client_mock.open_sftp()
        log_record = caplog.records[0]
        assert log_record.level == "error"
        assert log_record.message == "File already exists"
        assert log_record.destination == "/s3-transfer-test/file.mxf"

        sftp_mock.stat.assert_called_once_with("/s3-transfer-test/file.mxf")
        sftp_mock.mkdir.assert_not_called()

    @patch("app.helpers.transfer.SSHClient")
    def test_prepare_target_transfer_folder_exists(self, ssh_client_mock, transfer):
        """File does not exist and tmp folder already exists."""
        client_mock = ssh_client_mock().__enter__()
        # File not found but folder is found.
        client_mock.open_sftp().stat.side_effect = (FileNotFoundError, MagicMock)
        # mkdir results in OSError
        client_mock.open_sftp().mkdir.side_effect = OSError("error")

        transfer._prepare_target_transfer()

        sftp_mock = client_mock.open_sftp()
        assert sftp_mock.stat.call_count == 2
        sftp_mock.mkdir.assert_called_once_with("/s3-transfer-test/.file.mxf")

    @patch("app.helpers.transfer.SSHClient")
    def test_prepare_target_transfer_folder_error(
        self, ssh_client_mock, transfer, caplog
    ):
        """File does not exist but tmp folder can't be created."""
        client_mock = ssh_client_mock().__enter__()
        # File not found and folder not found. Gets called twice.
        client_mock.open_sftp().stat.side_effect = FileNotFoundError
        # mkdir results in OSError
        client_mock.open_sftp().mkdir.side_effect = OSError("error")

        with pytest.raises(OSError):
            transfer._prepare_target_transfer()

        sftp_mock = client_mock.open_sftp()
        log_record = caplog.records[0]
        assert log_record.level == "error"
        assert log_record.message == "Error occurred when creating tmp folder: error"
        assert log_record.tmp_folder == "/s3-transfer-test/.file.mxf"

        assert sftp_mock.stat.call_count == 2
        sftp_mock.mkdir.assert_called_once_with("/s3-transfer-test/.file.mxf")

    @patch("app.helpers.transfer.Transfer._transfer_part")
    @patch("app.helpers.transfer.calculate_ranges", return_value=["0-1", "1-2"])
    def test_transfer_parts(
        self, calculate_ranges_mock, transfer_part_mock, transfer, caplog
    ):
        """Transfer parts successfully."""
        transfer._transfer_parts()

        log_records = caplog.records
        assert len(log_records) == 2
        for log_record in log_records:
            assert log_record.level == "debug"

        assert (
            "Thread started for: /s3-transfer-test/.file.mxf/file.mxf.part0"
            in caplog.messages
        )
        assert (
            "Thread started for: /s3-transfer-test/.file.mxf/file.mxf.part1"
            in caplog.messages
        )

        assert transfer_part_mock.call_count == 2
        call_args = [call_args.args for call_args in transfer_part_mock.call_args_list]
        assert (
            "/s3-transfer-test/.file.mxf/file.mxf.part1",
            "1-2",
        ) in call_args

        assert (
            "/s3-transfer-test/.file.mxf/file.mxf.part0",
            "0-1",
        ) in call_args
