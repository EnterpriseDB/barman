# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2021
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


class TestCloudCheckWalArchive(object):
    @pytest.fixture
    def cloud_backup_catalog(self):
        """Create a mock CloudBackupCatalog with a WAL"""
        cloud_backup_catalog = mock.Mock()
        cloud_backup_catalog.get_wal_paths.return_value = {
            "000000010000000000000001": "path/to/wals/000000010000000000000001.gz",
        }
        return cloud_backup_catalog

    @mock.patch("barman.clients.cloud_check_wal_archive.check_archive_usable")
    @mock.patch("barman.clients.cloud_check_wal_archive.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_check_wal_archive.get_cloud_interface")
    def test_check_wal_archive_no_args(
        self,
        _mock_cloud_interface,
        mock_cloud_backup_catalog,
        mock_check_archive_usable,
        cloud_backup_catalog,
    ):
        """Verify xlog.check_archive_usable is called with no additional args."""
        mock_cloud_backup_catalog.return_value = cloud_backup_catalog
        cloud_check_wal_archive.main(["cloud_storage_url", "test_server"])
        mock_check_archive_usable.assert_called_once_with(
            ["000000010000000000000001"],
            current_wal_segment=None,
            current_timeline=None,
        )

    @mock.patch("barman.clients.cloud_check_wal_archive.check_archive_usable")
    @mock.patch("barman.clients.cloud_check_wal_archive.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_check_wal_archive.get_cloud_interface")
    def test_check_wal_archive_args(
        self,
        _mock_cloud_interface,
        mock_cloud_backup_catalog,
        mock_check_archive_usable,
        cloud_backup_catalog,
    ):
        """Verify xlog.check_archive_usable is called with no additional args."""
        mock_cloud_backup_catalog.return_value = cloud_backup_catalog
        cloud_check_wal_archive.main(
            [
                "cloud_storage_url",
                "test_server",
                "--current-wal-segment",
                "0000000100000001",
                "--current-timeline",
                "2",
            ]
        )
        mock_check_archive_usable.assert_called_once_with(
            ["000000010000000000000001"],
            current_wal_segment="0000000100000001",
            current_timeline=2,
        )
