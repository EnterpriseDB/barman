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

import datetime
import mock

from barman.annotations import KeepManager
from barman.clients import cloud_backup_list
from testing_helpers import build_test_backup_info


class TestCloudBackupList(object):
    @mock.patch("barman.clients.cloud_backup_list.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_list.get_cloud_interface")
    def test_cloud_backup_list(
        self, mock_get_cloud_interface, mock_cloud_backup_catalog, capsys
    ):
        """
        Verify output of barman-cloud-backup-list for a set of backups
        where two are archival and one is not.
        """
        cloud_backup_catalog = mock_cloud_backup_catalog.return_value
        cloud_backup_catalog.get_backup_list.return_value = {
            "backup_id_1": build_test_backup_info(
                backup_id="backup_id_1",
                end_time=datetime.datetime(2016, 3, 29, 17, 5, 20),
                begin_wal="000000010000000000000002",
            ),
            "backup_id_2": build_test_backup_info(
                backup_id="backup_id_2",
                end_time=datetime.datetime(2016, 3, 30, 17, 5, 20),
                begin_wal="000000010000000000000005",
            ),
            "backup_id_3": build_test_backup_info(
                backup_id="backup_id_3",
                end_time=datetime.datetime(2016, 3, 31, 17, 5, 20),
                begin_wal="000000010000000000000008",
            ),
        }
        cloud_backup_catalog.get_keep_target.side_effect = (
            lambda backup_id: backup_id == "backup_id_3"
            and KeepManager.TARGET_FULL
            or backup_id == "backup_id_1"
            and KeepManager.TARGET_STANDALONE
            or None
        )
        cloud_backup_list.main(
            [
                "cloud_storage_url",
                "test_server",
            ]
        )
        out, _err = capsys.readouterr()
        assert out == (
            "Backup ID           End Time                 Begin Wal                     Archival Status \n"
            "backup_id_1         2016-03-29 17:05:20      000000010000000000000002      KEEP:STANDALONE \n"
            "backup_id_2         2016-03-30 17:05:20      000000010000000000000005                      \n"
            "backup_id_3         2016-03-31 17:05:20      000000010000000000000008      KEEP:FULL       \n"
        )
