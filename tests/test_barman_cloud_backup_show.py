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

import datetime
import json
import mock
import pytest

from barman.clients import cloud_backup_show
from barman.clients.cloud_cli import OperationErrorExit
from testing_helpers import build_test_backup_info


class TestCloudBackupShow(object):
    @pytest.fixture
    def backup_id(self):
        yield "20380119T031408"

    @pytest.fixture
    def cloud_backup_catalog(self, backup_id):
        backup_info = build_test_backup_info(
            backup_id="backup_id_1",
            begin_time=datetime.datetime(2038, 1, 19, 3, 14, 8),
            begin_wal="000000010000000000000002",
            end_time=datetime.datetime(2038, 1, 19, 4, 14, 8),
            end_wal="000000010000000000000004",
            size=None,
            snapshots_info={
                "gcp_project": "test_project",
                "snapshots": {
                    "snapshot0": {
                        "block_size": 4096,
                        "device": "/dev/dev0",
                        "mount_options": "rw,noatime",
                        "mount_point": "/opt/disk0",
                        "size": 1,
                    },
                    "snapshot1": {
                        "block_size": 2048,
                        "device": "/dev/dev1",
                        "mount_options": "rw",
                        "mount_point": "/opt/disk1",
                        "size": 10,
                    },
                },
            },
            version=150000,
        )
        backup_info.mode = "concurrent"
        cloud_backup_catalog = mock.Mock()
        cloud_backup_catalog.get_backup_list.return_value = {backup_id: backup_info}
        cloud_backup_catalog.get_backup_info.return_value = backup_info
        yield cloud_backup_catalog

    @mock.patch("barman.clients.cloud_backup_show.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_show.get_cloud_interface")
    def test_cloud_backup_show(
        self,
        _mock_get_cloud_interface,
        mock_cloud_backup_catalog,
        cloud_backup_catalog,
        backup_id,
        capsys,
    ):
        """
        Verify plain output of barman-cloud-backup-show for a backup.
        """
        # GIVEN a backup catalog with a single backup
        mock_cloud_backup_catalog.return_value = cloud_backup_catalog
        # WHEN barman_cloud_backup_show is called for that backup
        cloud_backup_show.main(["cloud_storage_url", "test_server", backup_id])
        # THEN the expected output is printed
        out, _err = capsys.readouterr()
        assert out == (
            "Backup backup_id_1:\n"
            "  Server Name            : main\n"
            "  Status                 : DONE\n"
            "  PostgreSQL Version     : 150000\n"
            "  PGDATA directory       : /pgdata/location\n"
            "\n"
            "  Snapshot information:\n"
            "    gcp_project          : test_project\n"
            "\n"
            "    Snapshot name        : snapshot0\n"
            "    Disk size (GiB)      : 1\n"
            "    Device               : /dev/dev0\n"
            "    Block size           : 4096\n"
            "    Mount point          : /opt/disk0\n"
            "    Mount options        : rw,noatime\n"
            "\n"
            "    Snapshot name        : snapshot1\n"
            "    Disk size (GiB)      : 10\n"
            "    Device               : /dev/dev1\n"
            "    Block size           : 2048\n"
            "    Mount point          : /opt/disk1\n"
            "    Mount options        : rw\n"
            "\n"
            "  Tablespaces:\n"
            "    tbs1                 : /fake/location (oid: 16387)\n"
            "    tbs2                 : /another/location (oid: 16405)\n"
            "\n"
            "  Base backup information:\n"
            "    Timeline             : 1\n"
            "    Begin WAL            : 000000010000000000000002\n"
            "    End WAL              : 000000010000000000000004\n"
            "    Begin time           : 2038-01-19 03:14:08\n"
            "    End time             : 2038-01-19 04:14:08\n"
            "    Begin Offset         : 40\n"
            "    End Offset           : 184\n"
            "    Begin LSN            : 0/2000028\n"
            "    End LSN              : 0/20000B8\n"
            "\n"
        )

    @mock.patch("barman.clients.cloud_backup_show.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_show.get_cloud_interface")
    def test_cloud_backup_show_json(
        self,
        _mock_get_cloud_interface,
        mock_cloud_backup_catalog,
        cloud_backup_catalog,
        backup_id,
        capsys,
    ):
        """
        Verify json output of barman-cloud-backup-show for a backup.
        """
        # GIVEN a backup catalog with a single backup
        mock_cloud_backup_catalog.return_value = cloud_backup_catalog
        # WHEN barman_cloud_backup_show is called for that backup
        cloud_backup_show.main(
            ["cloud_storage_url", "test_server", backup_id, "--format", "json"]
        )
        # THEN the expected output is printed
        out, _err = capsys.readouterr()
        assert sorted(json.loads(out)) == sorted(
            {
                "main": {
                    "backup_label": None,
                    "begin_offset": 40,
                    "begin_time": "Tue Jan 19 03:14:08 2038",
                    "begin_wal": "000000010000000000000002",
                    "begin_xlog": "0/2000028",
                    "compression": None,
                    "config_file": "/pgdata/location/postgresql.conf",
                    "copy_stats": None,
                    "deduplicated_size": None,
                    "end_offset": 184,
                    "end_time": "Tue Jan 19 04:14:08 2038",
                    "end_wal": "000000010000000000000004",
                    "end_xlog": "0/20000B8",
                    "error": None,
                    "hba_file": "/pgdata/location/pg_hba.conf",
                    "ident_file": "/pgdata/location/pg_ident.conf",
                    "included_files": None,
                    "mode": "concurrent",
                    "pgdata": "/pgdata/location",
                    "server_name": "main",
                    "size": None,
                    "snapshots_info": {
                        "gcp_project": "test_project",
                        "snapshots": {
                            "snapshot0": {
                                "block_size": 4096,
                                "device": "/dev/dev0",
                                "mount_options": "rw,noatime",
                                "mount_point": "/opt/disk0",
                                "size": 1,
                            },
                            "snapshot1": {
                                "block_size": 2048,
                                "device": "/dev/dev1",
                                "mount_options": "rw",
                                "mount_point": "/opt/disk1",
                                "size": 10,
                            },
                        },
                    },
                    "status": "DONE",
                    "systemid": None,
                    "tablespaces": [
                        ["tbs1", 16387, "/fake/location"],
                        ["tbs2", 16405, "/another/location"],
                    ],
                    "timeline": 1,
                    "version": 150000,
                    "xlog_segment_size": 16777216,
                    "backup_id": "backup_id_1",
                }
            }
        )

    @mock.patch("barman.clients.cloud_backup_show.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_show.get_cloud_interface")
    def test_cloud_backup_show_missing_backup(
        self,
        _mock_get_cloud_interface,
        mock_cloud_backup_catalog,
        backup_id,
        caplog,
    ):
        """
        Verify plain output of barman-cloud-backup-show for a backup.
        """
        # GIVEN a backup catalog with a single backup
        cloud_backup_catalog = mock_cloud_backup_catalog.return_value
        cloud_backup_catalog.get_backup_list.return_value = {}
        cloud_backup_catalog.get_backup_info.return_value = None
        cloud_backup_catalog.parse_backup_id.return_value = backup_id
        # WHEN barman_cloud_backup_show is called for that backup
        # THEN an OperationErrorExit is raised
        with pytest.raises(OperationErrorExit) as exc:
            cloud_backup_show.main(["cloud_storage_url", "test_server", backup_id])
        # AND an error message was logged
        assert (
            "Backup {} for server test_server does not exist".format(backup_id)
            in caplog.text
        )
