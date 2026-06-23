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

import datetime
import json

import mock
from testing_helpers import build_test_backup_info

from barman.annotations import KeepManager
from barman.clients import cloud_backup_list


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
                backup_name="named backup 1",
                end_time=datetime.datetime(2016, 3, 31, 5, 5, 20),
                begin_wal="000000010000000000000007",
            ),
            "backup_id_4": build_test_backup_info(
                backup_id="backup_id_4",
                backup_name="named backup 2",
                end_time=datetime.datetime(2016, 3, 31, 17, 5, 20),
                begin_wal="000000010000000000000008",
            ),
        }
        cloud_backup_catalog.get_keep_target.side_effect = lambda backup_id: (
            backup_id == "backup_id_4"
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
            "Backup ID           End Time                 Begin Wal                     Archival Status  Name                \n"
            "backup_id_1         2016-03-29 17:05:20      000000010000000000000002      KEEP:STANDALONE                      \n"
            "backup_id_2         2016-03-30 17:05:20      000000010000000000000005                                           \n"
            "backup_id_3         2016-03-31 05:05:20      000000010000000000000007                       named backup 1      \n"
            "backup_id_4         2016-03-31 17:05:20      000000010000000000000008      KEEP:FULL        named backup 2      \n"
        )

    @mock.patch("barman.clients.cloud_backup_list.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_list.get_cloud_interface")
    def test_started_backup_visible_in_listing(
        self, mock_get_cloud_interface, mock_cloud_backup_catalog, capsys
    ):
        """
        Verify that a STARTED backup is shown in the console listing with its
        status surfaced in the Archival Status column and an empty End Time,
        rather than being silently suppressed.
        """
        # GIVEN a catalog containing one DONE backup and one STARTED backup
        cloud_backup_catalog = mock_cloud_backup_catalog.return_value
        cloud_backup_catalog.get_backup_list.return_value = {
            "backup_id_1": build_test_backup_info(
                backup_id="backup_id_1",
                end_time=datetime.datetime(2016, 3, 29, 17, 5, 20),
                begin_wal="000000010000000000000002",
                status="DONE",
            ),
            "backup_id_2": build_test_backup_info(
                backup_id="backup_id_2",
                status="STARTED",
                end_time=None,
                end_xlog=None,
                begin_wal="000000010000000000000005",
            ),
        }
        cloud_backup_catalog.get_keep_target.return_value = None

        # WHEN barman-cloud-backup-list is called
        cloud_backup_list.main(["cloud_storage_url", "test_server"])
        out, _err = capsys.readouterr()

        # THEN the STARTED backup appears in the output
        assert "backup_id_2" in out
        # AND its status is shown in the Archival Status column
        assert "STARTED" in out
        # AND its End Time column is empty (no end_time available)
        lines = [line for line in out.splitlines() if "backup_id_2" in line]
        assert len(lines) == 1
        assert lines[0].startswith("backup_id_2")
        # AND the DONE backup is still listed normally
        assert "backup_id_1" in out
        assert "2016-03-29 17:05:20" in out

    @mock.patch("barman.clients.cloud_backup_list.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_list.get_cloud_interface")
    def test_done_backup_not_affected_by_started_backup(
        self, mock_get_cloud_interface, mock_cloud_backup_catalog, capsys
    ):
        """
        Verify that the presence of a STARTED backup does not affect the display
        of DONE backups, including their keep/archival status.
        """
        # GIVEN a catalog with a DONE archival backup and a STARTED backup
        cloud_backup_catalog = mock_cloud_backup_catalog.return_value
        cloud_backup_catalog.get_backup_list.return_value = {
            "backup_id_1": build_test_backup_info(
                backup_id="backup_id_1",
                end_time=datetime.datetime(2016, 3, 29, 17, 5, 20),
                begin_wal="000000010000000000000002",
                status="DONE",
            ),
            "backup_id_2": build_test_backup_info(
                backup_id="backup_id_2",
                status="STARTED",
                end_time=None,
                end_xlog=None,
                begin_wal="000000010000000000000005",
            ),
        }
        cloud_backup_catalog.get_keep_target.side_effect = lambda backup_id: (
            KeepManager.TARGET_FULL if backup_id == "backup_id_1" else None
        )

        # WHEN barman-cloud-backup-list is called
        cloud_backup_list.main(["cloud_storage_url", "test_server"])
        out, _err = capsys.readouterr()

        # THEN the DONE backup still shows its keep status correctly
        assert "KEEP:FULL" in out
        assert "2016-03-29 17:05:20" in out

    @mock.patch("barman.clients.cloud_backup_list.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_list.get_cloud_interface")
    def test_started_backup_appears_in_json_output(
        self, mock_get_cloud_interface, mock_cloud_backup_catalog, capsys
    ):
        """
        Verify that a STARTED backup is included in the JSON output and that
        its status field is set to STARTED.
        """

        def make_backup_item(backup_id, status):
            item = mock.MagicMock()
            item.backup_id = backup_id
            item.status = status
            item.to_json.return_value = {"backup_id": backup_id, "status": status}
            return item

        # GIVEN a catalog containing one DONE backup and one STARTED backup
        cloud_backup_catalog = mock_cloud_backup_catalog.return_value
        cloud_backup_catalog.get_backup_list.return_value = {
            "backup_id_1": make_backup_item("backup_id_1", "DONE"),
            "backup_id_2": make_backup_item("backup_id_2", "STARTED"),
        }

        # WHEN barman-cloud-backup-list is called with JSON format
        cloud_backup_list.main(["cloud_storage_url", "test_server", "--format", "json"])
        out, _err = capsys.readouterr()

        # THEN the output is valid JSON
        data = json.loads(out)
        backups = {b["backup_id"]: b for b in data["backups_list"]}

        # AND the STARTED backup is present with status=STARTED
        assert "backup_id_2" in backups
        assert backups["backup_id_2"]["status"] == "STARTED"

        # AND the DONE backup is present with status=DONE
        assert "backup_id_1" in backups
        assert backups["backup_id_1"]["status"] == "DONE"
