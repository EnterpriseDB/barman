# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2025
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
    def test_fails_on_connectivity_test_failure(self, get_cloud_interface_mock):
        """If connectivity test fails we exit with status 2."""
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.test_connectivity.return_value = False
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
        cloud_interface_mock.test_connectivity.assert_called_once()

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
            "WAL file 000000080000ABFF000000C1 for server test-server does not exists\n"
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
            "WAL file 000000080000ABFF000000C0 for server test-server does not exists\n"
            in caplog.text
        )

    @mock.patch("barman.clients.cloud_walrestore.get_cloud_interface")
    def test_fails_if_bucket_not_found(self, get_cloud_interface_mock, caplog):
        """If the bucket does not exist we exit with status 1."""
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.bucket_name = "no_bucket_here"
        cloud_interface_mock.bucket_exists = False
        with pytest.raises(SystemExit) as exc:
            cloud_walrestore.main(
                [
                    "s3://test-bucket/testfolder/",
                    "test-server",
                    "000000080000ABFF000000C1",
                    "/tmp/000000080000ABFF000000C1",
                ]
            )
        assert exc.value.code == 1
        assert "Bucket no_bucket_here does not exist" in caplog.text

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
