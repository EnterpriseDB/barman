# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2022
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

import mock
import pytest

from barman.clients import cloud_check_wal_archive
from barman.exceptions import WalArchiveContentError


class TestCloudCheckWalArchive(object):
    @pytest.fixture
    def cloud_backup_catalog(self):
        """Create a mock CloudBackupCatalog with a WAL"""
        cloud_backup_catalog = mock.Mock()
        cloud_backup_catalog.get_wal_paths.return_value = {
            "000000010000000000000001": "path/to/wals/000000010000000000000001.gz",
        }
        return cloud_backup_catalog

    @mock.patch("barman.clients.cloud_check_wal_archive.get_cloud_interface")
    def test_exits_on_connectivity_test(self, get_cloud_interface_mock):
        """If the -t option is used we check connectivity and exit."""
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.test_connectivity.return_value = True
        with pytest.raises(SystemExit) as exc:
            cloud_check_wal_archive.main(["cloud_storage_url", "test_server", "-t"])
        assert exc.value.code == 0
        cloud_interface_mock.test_connectivity.assert_called_once()

    @mock.patch("barman.clients.cloud_check_wal_archive.check_archive_usable")
    @mock.patch("barman.clients.cloud_check_wal_archive.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_check_wal_archive.get_cloud_interface")
    def test_check_wal_archive_no_args(
        self,
        mock_cloud_interface,
        mock_cloud_backup_catalog,
        mock_check_archive_usable,
        cloud_backup_catalog,
    ):
        """Verify xlog.check_archive_usable is called with no additional args."""
        mock_cloud_interface.return_value.bucket_exists = True
        mock_cloud_backup_catalog.return_value = cloud_backup_catalog
        cloud_check_wal_archive.main(["cloud_storage_url", "test_server"])
        mock_check_archive_usable.assert_called_once_with(
            ["000000010000000000000001"],
            timeline=None,
        )

    @mock.patch("barman.clients.cloud_check_wal_archive.check_archive_usable")
    @mock.patch("barman.clients.cloud_check_wal_archive.get_cloud_interface")
    def test_check_wal_archive_missing_bucket(
        self,
        mock_cloud_interface,
        mock_cloud_backup_catalog,
    ):
        """Verify a missing bucket passes the check"""
        mock_cloud_interface.return_value.bucket_exists = False
        cloud_check_wal_archive.main(["cloud_storage_url", "test_server"])
        mock_cloud_backup_catalog.assert_not_called()

    @mock.patch("barman.clients.cloud_check_wal_archive.check_archive_usable")
    @mock.patch("barman.clients.cloud_check_wal_archive.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_check_wal_archive.get_cloud_interface")
    def test_check_wal_archive_args(
        self,
        mock_cloud_interface,
        mock_cloud_backup_catalog,
        mock_check_archive_usable,
        cloud_backup_catalog,
    ):
        """Verify xlog.check_archive_usable is called with no additional args."""
        mock_cloud_interface.return_value.bucket_exists = True
        mock_cloud_backup_catalog.return_value = cloud_backup_catalog
        cloud_check_wal_archive.main(
            [
                "cloud_storage_url",
                "test_server",
                "--timeline",
                "2",
            ]
        )
        mock_check_archive_usable.assert_called_once_with(
            ["000000010000000000000001"],
            timeline=2,
        )

    @mock.patch("barman.clients.cloud_check_wal_archive.check_archive_usable")
    @mock.patch("barman.clients.cloud_check_wal_archive.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_check_wal_archive.get_cloud_interface")
    def test_check_wal_archive_content_error(
        self,
        mock_cloud_interface,
        mock_cloud_backup_catalog,
        mock_check_archive_usable,
        cloud_backup_catalog,
        caplog,
    ):
        """Verify log output when wal archive check fails"""
        mock_cloud_interface.return_value.bucket_exists = True
        mock_cloud_backup_catalog.return_value = cloud_backup_catalog
        mock_check_archive_usable.side_effect = WalArchiveContentError("oh dear")
        with pytest.raises(SystemExit) as exc:
            cloud_check_wal_archive.main(["cloud_storage_url", "test_server"])
        assert 1 == exc.value.code
        assert "WAL archive check failed for server test_server: oh dear" in caplog.text

    @mock.patch("barman.clients.cloud_check_wal_archive.check_archive_usable")
    @mock.patch("barman.clients.cloud_check_wal_archive.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_check_wal_archive.get_cloud_interface")
    def test_check_wal_archive_exception(
        self,
        mock_cloud_interface,
        mock_cloud_backup_catalog,
        mock_check_archive_usable,
        cloud_backup_catalog,
        caplog,
    ):
        """Verify log output when there is an error checking the wal archive"""
        mock_cloud_interface.return_value.bucket_exists = True
        mock_cloud_backup_catalog.return_value = cloud_backup_catalog
        mock_check_archive_usable.side_effect = Exception("oh dear")
        with pytest.raises(SystemExit) as exc:
            cloud_check_wal_archive.main(["cloud_storage_url", "test_server"])
        assert 4 == exc.value.code
        assert "Barman cloud WAL archive check exception: oh dear" in caplog.text

    @mock.patch("barman.clients.cloud_check_wal_archive.get_cloud_interface")
    def test_check_wal_archive_failed_connectivity(self, mock_cloud_interface, caplog):
        """Verify the check errors if we cannot connect to the cloud provider"""
        mock_cloud_interface.return_value.test_connectivity.return_value = False
        with pytest.raises(SystemExit) as exc:
            cloud_check_wal_archive.main(["cloud_storage_url", "test_server"])
        assert 2 == exc.value.code
        mock_cloud_interface.return_value.test_connectivity.assert_called_once_with()
