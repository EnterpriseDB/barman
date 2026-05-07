# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2026
#
# Client Utilities for Barman, Backup and Recovery Manager for PostgreSQL
#
# Barman is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Barman is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Barman.  If not, see <http://www.gnu.org/licenses/>.

import logging

import mock
import pytest

from barman.clients import cloud_walrestore


class TestMain(object):
    """
    Test the main method of barman_cloud_wal_restore.
    """

    @mock.patch("barman.clients.cloud_walrestore.get_cloud_interface")
    def test_exits_on_connectivity_test(self, get_cloud_interface_mock):
        """If connectivity test fails we exit with status 2."""
        cloud_interface_mock = get_cloud_interface_mock.return_value
        with pytest.raises(SystemExit) as exc:
            cloud_walrestore.main(
                [
                    "s3://test-bucket/testfolder/",
                    "test-server",
                    "000000080000ABFF000000C1",
                    "/tmp/000000080000ABFF000000C1",
                    "-t",
                ]
            )
        assert exc.value.code == 0
        cloud_interface_mock.verify_cloud_connectivity_and_bucket_existence.assert_called_once()

    @mock.patch("barman.clients.cloud_walrestore.get_cloud_interface")
    def test_succeeds_if_wal_is_found(self, get_cloud_interface_mock, caplog):
        """If the WAL is found we exit with status 0."""
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.path = "testfolder/"
        cloud_interface_mock.list_bucket.return_value = [
            "testfolder/test-server/wals/000000080000ABFF/000000080000ABFF000000C1"
        ]
        cloud_walrestore.main(
            [
                "s3://test-bucket/testfolder/",
                "test-server",
                "000000080000ABFF000000C1",
                "/tmp/000000080000ABFF000000C1",
            ]
        )
        assert caplog.text == ""
        cloud_interface_mock.download_file.assert_called_once()

    @mock.patch("barman.clients.cloud_walrestore.get_cloud_interface")
    def test_succeeds_if_wal_is_found_partial(self, get_cloud_interface_mock, caplog):
        """If the WAL is found as partial we exit with status 0."""
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.path = "testfolder/"
        cloud_interface_mock.list_bucket.return_value = [
            "testfolder/test-server/wals/000000080000ABFF/000000080000ABFF000000C1.partial"
        ]
        cloud_walrestore.main(
            [
                "s3://test-bucket/testfolder/",
                "test-server",
                "000000080000ABFF000000C1",
                "/tmp/000000080000ABFF000000C1",
            ]
        )
        assert caplog.text == ""
        cloud_interface_mock.download_file.assert_called_once()

    @mock.patch("barman.clients.cloud_walrestore.get_cloud_interface")
    def test_fails_if_wal_is_found_partial_but_nopartial(
        self, get_cloud_interface_mock, caplog
    ):
        """If the WAL is found as partial we exit with status 0."""
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.path = "testfolder/"
        cloud_interface_mock.list_bucket.return_value = [
            "testfolder/test-server/wals/000000080000ABFF/000000080000ABFF000000C1.partial"
        ]
        caplog.set_level(logging.INFO)
        with pytest.raises(SystemExit) as exc:
            cloud_walrestore.main(
                [
                    "--no-partial",
                    "s3://test-bucket/testfolder/",
                    "test-server",
                    "000000080000ABFF000000C1",
                    "/tmp/000000080000ABFF000000C1",
                ]
            )
        assert exc.value.code == 1
        assert (
            "WAL file 000000080000ABFF000000C1 for server test-server does not exist\n"
            in caplog.text
        )

    @mock.patch("barman.clients.cloud_walrestore.get_cloud_interface")
    def test_fails_if_wal_not_found(self, get_cloud_interface_mock, caplog):
        """If the WAL cannot be found we exit with status 1."""
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.path = "testfolder/"
        cloud_interface_mock.list_bucket.return_value = []
        caplog.set_level(logging.INFO)
        with pytest.raises(SystemExit) as exc:
            cloud_walrestore.main(
                [
                    "s3://test-bucket/testfolder/",
                    "test-server",
                    "000000080000ABFF000000C0",
                    "/tmp/000000080000ABFF000000C0",
                ]
            )
        assert exc.value.code == 1
        assert (
            "WAL file 000000080000ABFF000000C0 for server test-server does not exist\n"
            in caplog.text
        )

    @mock.patch("barman.clients.cloud_walrestore.get_cloud_interface")
    def test_fails_on_invalid_wal_name(self, _get_cloud_interface_mock, caplog):
        """If an invalid wal name is provided we exit with status 3."""
        with pytest.raises(SystemExit) as exc:
            cloud_walrestore.main(
                [
                    "s3://test-bucket/testfolder/",
                    "test-server",
                    "not_a_valid_wal_name",
                    "/tmp/000000080000ABFF000000C1",
                ]
            )
        assert exc.value.code == 3
        assert "not_a_valid_wal_name is an invalid name for a WAL file\n" in caplog.text

    @mock.patch("barman.clients.cloud_walrestore.get_cloud_interface")
    def test_fails_on_download_exception(self, get_cloud_interface_mock, caplog):
        """Test that any cloud_interface.download exceptions cause exit status 4."""
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.path = "testfolder/"
        cloud_interface_mock.list_bucket.return_value = [
            "testfolder/test-server/wals/000000080000ABFF/000000080000ABFF000000C1"
        ]
        cloud_interface_mock.download_file.side_effect = Exception(
            "something went wrong"
        )
        # The exception is not a connectivity error, so it must be reported
        # as a generic error (exit code 4).
        cloud_interface_mock.is_connectivity_error.return_value = False
        with pytest.raises(SystemExit) as exc:
            cloud_walrestore.main(
                [
                    "s3://test-bucket/testfolder/",
                    "test-server",
                    "000000080000ABFF000000C1",
                    "/tmp/000000080000ABFF000000C1",
                ]
            )
        assert exc.value.code == 4
        assert (
            "Barman cloud WAL restore exception: something went wrong\n" in caplog.text
        )

    @mock.patch("barman.clients.cloud_walrestore.get_cloud_interface")
    def test_fails_on_network_error_during_download(
        self, get_cloud_interface_mock, caplog
    ):
        """If a download fails and connectivity is lost we exit with status 2."""
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.path = "testfolder/"
        cloud_interface_mock.list_bucket.return_value = [
            "testfolder/test-server/wals/000000080000ABFF/000000080000ABFF000000C1"
        ]
        cloud_interface_mock.download_file.side_effect = Exception(
            "connection reset by peer"
        )
        # The exception is a connectivity error, so the failure must be
        # reported as a network error (exit code 2).
        cloud_interface_mock.is_connectivity_error.return_value = True
        with pytest.raises(SystemExit) as exc:
            cloud_walrestore.main(
                [
                    "s3://test-bucket/testfolder/",
                    "test-server",
                    "000000080000ABFF000000C1",
                    "/tmp/000000080000ABFF000000C1",
                ]
            )
        assert exc.value.code == 2
        assert (
            "Barman cloud WAL restore exception: connection reset by peer\n"
            in caplog.text
        )


class TestCloudWalRestoreAwsEncryptionArgs(object):
    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface")
    def test_sse_customer_key_not_set(self, cloud_interface_mock):
        """Verify that sse_customer_key is None when --sse-customer-key is not provided."""
        # GIVEN a cloud interface mock that simulates a WAL not being found
        cloud_interface_mock.return_value.download_file.return_value = False

        # WHEN barman-cloud-wal-restore is called without --sse-customer-key
        with pytest.raises(SystemExit):
            cloud_walrestore.main(
                [
                    "--cloud-provider",
                    "aws-s3",
                    "s3://test-bucket",
                    "test-server",
                    "000000010000000000000001",
                    "/tmp/000000010000000000000001",
                ]
            )

        # THEN S3CloudInterface was created with sse_customer_key=None
        cloud_interface_mock.assert_called_once()
        _, kwargs = cloud_interface_mock.call_args
        assert kwargs.get("sse_customer_key") is None

    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface")
    def test_sse_customer_key_passed_to_cloud_interface(
        self, cloud_interface_mock, tmp_path
    ):
        """Verify that --sse-customer-key file:// URI is passed to the S3 cloud interface."""
        # GIVEN a key file containing a valid base64-encoded key
        key_file = tmp_path / "key.b64"
        key_file.write_text("WkMb3SePiDn8cCIt/Knb0O+B0yYZavrwqsOQdjNe57g=")
        key_uri = "file://" + str(key_file)
        # AND a cloud interface mock that simulates a WAL not being found
        cloud_interface_mock.return_value.download_file.return_value = False

        # WHEN barman-cloud-wal-restore is called with --sse-customer-key
        with pytest.raises(SystemExit):
            cloud_walrestore.main(
                [
                    "--cloud-provider",
                    "aws-s3",
                    "s3://test-bucket",
                    "test-server",
                    "000000010000000000000001",
                    "/tmp/000000010000000000000001",
                    "--sse-customer-key",
                    key_uri,
                ]
            )

        # THEN S3CloudInterface was created with the file:// URI as sse_customer_key
        cloud_interface_mock.assert_called_once()
        _, kwargs = cloud_interface_mock.call_args
        assert kwargs.get("sse_customer_key") == key_uri
