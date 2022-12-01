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

from barman.clients import cloud_restore


class TestCloudRestore(object):
    @pytest.mark.parametrize(
        ("backup_id_arg", "expected_backup_id"),
        (("20201110T120000", "20201110T120000"), ("backup name", "20201110T120000")),
    )
    @mock.patch("barman.clients.cloud_restore.CloudBackupDownloaderObjectStore")
    @mock.patch("barman.clients.cloud_restore.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_restore.get_cloud_interface")
    def test_restore_calls_backup_downloader_with_parsed_id(
        self,
        _mock_cloud_interface_factory,
        mock_catalog,
        mock_downloader,
        backup_id_arg,
        expected_backup_id,
    ):
        """Verify that we download the backup_id returned by parse_backup_id."""
        # GIVEN a mock backup catalog where parse_backup_id will always resolve
        # to the expected backup ID
        mock_catalog.return_value.parse_backup_id.return_value = expected_backup_id
        # AND the catalog returns a mock backup_info with the expected backup ID
        mock_backup_info = mock.Mock(backup_id=expected_backup_id)
        mock_catalog.return_value.get_backup_info.return_value = mock_backup_info

        # WHEN barman-backup-restore is called with the backup_id_arg
        recovery_dir = "/some/recovery/dir"
        cloud_restore.main(
            ["cloud_storage_url", "test_server", backup_id_arg, recovery_dir]
        )

        # THEN the backup downloader is called with the expected mock backup_info
        mock_downloader.return_value.download_backup.assert_called_once_with(
            mock_backup_info, recovery_dir, {}
        )
