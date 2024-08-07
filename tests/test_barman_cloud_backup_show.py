# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2023
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
from barman.cloud_providers.google_cloud_storage import (
    GcpSnapshotMetadata,
    GcpSnapshotsInfo,
)
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
            size=2048,
            data_checksums="on",
            summarize_wal="on",
            snapshots_info=GcpSnapshotsInfo(
                project="test_project",
                snapshots=[
                    GcpSnapshotMetadata(
                        mount_point="/opt/disk0",
                        mount_options="rw,noatime",
                        device_name="dev0",
                        snapshot_name="snapshot0",
                        snapshot_project="test_project",
                    ),
                    GcpSnapshotMetadata(
                        mount_point="/opt/disk1",
                        mount_options="rw",
                        device_name="dev1",
                        snapshot_name="snapshot1",
                        snapshot_project="test_project",
                    ),
                ],
            ),
            version=150000,
            cluster_size=2048,
            deduplicated_size=1024,
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
            "  Estimated Cluster Size : 2.0 KiB\n"
            "\n"
            "  Server information:\n"
            "    Checksums            : on\n"
            "    WAL summarizer       : on\n"
            "\n"
            "  Snapshot information:\n"
            "    provider             : gcp\n"
            "    project              : test_project\n"
            "\n"
            "    device_name          : dev0\n"
            "    snapshot_name        : snapshot0\n"
            "    snapshot_project     : test_project\n"
            "    Mount point          : /opt/disk0\n"
            "    Mount options        : rw,noatime\n"
            "\n"
            "    device_name          : dev1\n"
            "    snapshot_name        : snapshot1\n"
            "    snapshot_project     : test_project\n"
            "    Mount point          : /opt/disk1\n"
            "    Mount options        : rw\n"
            "\n"
            "  Tablespaces:\n"
            "    tbs1                 : /fake/location (oid: 16387)\n"
            "    tbs2                 : /another/location (oid: 16405)\n"
            "\n"
            "  Base backup information:\n"
            "    Backup Method        : concurrent\n"
            "    Backup Size          : 1.0 KiB\n"
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
        assert json.loads(out)["main"] == {
            "backup_label": None,
            "begin_offset": 40,
            "begin_time": "Tue Jan 19 03:14:08 2038",
            "begin_wal": "000000010000000000000002",
            "begin_xlog": "0/2000028",
            "children_backup_ids": None,
            "cluster_size": 2048,
            "compression": None,
            "config_file": "/pgdata/location/postgresql.conf",
            "copy_stats": None,
            "data_checksums": "on",
            "deduplicated_size": 1024,
            "end_offset": 184,
            "end_time": "Tue Jan 19 04:14:08 2038",
            "end_wal": "000000010000000000000004",
            "end_xlog": "0/20000B8",
            "error": None,
            "hba_file": "/pgdata/location/pg_hba.conf",
            "ident_file": "/pgdata/location/pg_ident.conf",
            "included_files": None,
            "mode": "concurrent",
            "parent_backup_id": None,
            "pgdata": "/pgdata/location",
            "server_name": "main",
            "size": 2048,
            "snapshots_info": {
                "provider": "gcp",
                "provider_info": {
                    "project": "test_project",
                },
                "snapshots": [
                    {
                        "mount": {
                            "mount_options": "rw,noatime",
                            "mount_point": "/opt/disk0",
                        },
                        "provider": {
                            "device_name": "dev0",
                            "snapshot_name": "snapshot0",
                            "snapshot_project": "test_project",
                        },
                    },
                    {
                        "mount": {
                            "mount_options": "rw",
                            "mount_point": "/opt/disk1",
                        },
                        "provider": {
                            "device_name": "dev1",
                            "snapshot_name": "snapshot1",
                            "snapshot_project": "test_project",
                        },
                    },
                ],
            },
            "status": "DONE",
            "summarize_wal": "on",
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

    @pytest.mark.parametrize("extra_args", [[], ["--format", "json"]])
    @mock.patch("barman.clients.cloud_backup_show.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_show.get_cloud_interface")
    def test_cloud_backup_show_from_hook_script(
        self,
        _mock_get_cloud_interface,
        mock_cloud_backup_catalog,
        cloud_backup_catalog,
        backup_id,
        extra_args,
    ):
        """
        Verify barman-cloud-backup-show for a backup with size information does not
        fail.
        """
        # GIVEN a backup catalog with a single backup
        mock_cloud_backup_catalog.return_value = cloud_backup_catalog
        # AND the single backup has a non-null size value due to being uploaded via
        # a hook script
        cloud_backup_catalog.get_backup_info.return_value.size = 42
        # WHEN barman_cloud_backup_show is called for that backup
        # THEN no errors are raised
        cloud_backup_show.main(
            ["cloud_storage_url", "test_server", backup_id, *extra_args]
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
        Verify failure of barman-cloud-backup-show for a missing backup.
        """
        # GIVEN a backup catalog with a single backup
        cloud_backup_catalog = mock_cloud_backup_catalog.return_value
        cloud_backup_catalog.get_backup_list.return_value = {}
        cloud_backup_catalog.get_backup_info.return_value = None
        cloud_backup_catalog.parse_backup_id.return_value = backup_id
        # WHEN barman_cloud_backup_show is called for that backup
        # THEN an OperationErrorExit is raised
        with pytest.raises(OperationErrorExit):
            cloud_backup_show.main(["cloud_storage_url", "test_server", backup_id])
        # AND an error message was logged
        assert (
            "Backup {} for server test_server does not exist".format(backup_id)
            in caplog.text
        )

    @pytest.mark.parametrize(
        ("connectivity_test_result", "expected_exit_code"), ([False, 2], [True, 0])
    )
    @mock.patch("barman.clients.cloud_backup_show.get_cloud_interface")
    def test_exits_on_connectivity_test(
        self, get_cloud_interface_mock, connectivity_test_result, expected_exit_code
    ):
        """If the -t option is used we check connectivity and exit."""
        # GIVEN a mock cloud interface
        cloud_interface_mock = get_cloud_interface_mock.return_value
        # AND the connectivity test returns the specified result
        cloud_interface_mock.test_connectivity.return_value = connectivity_test_result

        # WHEN cloud_backup_show is called with the `-t` option
        with pytest.raises(SystemExit) as exc:
            cloud_backup_show.main(
                ["cloud_storage_url", "test_server", "backup_id", "-t"]
            )

        # THEN the expected error code is returned
        assert exc.value.code == expected_exit_code
        # AND the connectivity test was called
        cloud_interface_mock.test_connectivity.assert_called_once()

    @mock.patch("barman.clients.cloud_backup_show.get_cloud_interface")
    def test_fails_if_bucket_not_found(self, get_cloud_interface_mock, caplog):
        """If the bucket does not exist we exit with status 1."""
        # GIVEN a mock cloud interface
        cloud_interface_mock = get_cloud_interface_mock.return_value
        # AND a bucket which does not exist
        bucket_name = "missing_bucket"
        cloud_interface_mock.bucket_name = bucket_name
        cloud_interface_mock.bucket_exists = False

        # WHEN cloud_backup_show is called against the missing bucket
        with pytest.raises(SystemExit) as exc:
            cloud_backup_show.main([bucket_name, "test_server", "backup_id"])

        # THEN an exit code of 1 is returned
        assert exc.value.code == 1
        # AND the expected message is logged
        assert "Bucket {} does not exist".format(bucket_name) in caplog.text

    @mock.patch("barman.clients.cloud_backup_show.get_cloud_interface")
    def test_fails_on_any_exception(self, get_cloud_interface_mock, caplog):
        """If any non-specific exception occurs then we exit with status 4."""
        # GIVEN a general exception when getting the cloud interface
        get_cloud_interface_mock.side_effect = Exception("an error happened")

        # WHEN cloud_backup_show is called with any valid args
        with pytest.raises(SystemExit) as exc:
            cloud_backup_show.main(["bucket", "server", "backup_id"])

        # THEN an exit code of 4 is returned
        assert exc.value.code == 4
        # AND the exception message is logged
        assert "an error happened" in caplog.text
