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

from barman.annotations import KeepManager
from barman.clients import cloud_backup_keep
from barman.infofile import BackupInfo


class TestCloudBackupKeepArguments(object):
    """Test handling of command line arguments."""

    def test_fails_if_no_backup_id_provided(self, capsys):
        """If no backup ID is provided then exit"""
        with pytest.raises(SystemExit):
            cloud_backup_keep.main(["cloud_storage_url", "test_server"])

        _, err = capsys.readouterr()
        assert (
            "error: the following arguments are required: backup_id"
            # argparse produces a different error in Python 2
            or "error: too few arguments" in err
        )

    @mock.patch("barman.clients.cloud_backup_keep.get_cloud_interface")
    def test_add_keep_fails_if_no_target_release_or_status_is_provided(
        self, _mock_cloud_interface, capsys
    ):
        """If none of --target, --release or --status is provided then exit"""
        with pytest.raises(SystemExit):
            cloud_backup_keep.main(
                ["cloud_storage_url", "test_server", "test_backup_id"]
            )

        _, err = capsys.readouterr()
        assert (
            "one of the arguments -r/--release -s/--status --target is required" in err
        )

    @mock.patch("barman.clients.cloud_backup_keep.get_cloud_interface")
    def test_exits_on_connectivity_test(self, get_cloud_interface_mock):
        """If the -t option is used we check connectivity and exit."""
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.test_connectivity.return_value = True
        with pytest.raises(SystemExit) as exc:
            cloud_backup_keep.main(
                [
                    "cloud_storage_url",
                    "test_server",
                    "test_backup_id",
                    "--target",
                    "standalone",
                    "-t",
                ]
            )
        assert exc.value.code == 0
        cloud_interface_mock.test_connectivity.assert_called_once()

    @mock.patch("barman.clients.cloud_backup_keep.get_cloud_interface")
    def test_exits_on_unsupported_target(self, get_cloud_interface_mock, capsys):
        """If an unsupported target is specified then exit"""
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.test_connectivity.return_value = True
        with pytest.raises(SystemExit):
            cloud_backup_keep.main(
                [
                    "cloud_storage_url",
                    "test_server",
                    "test_backup_id",
                    "--target",
                    "unsupported_target",
                ]
            )

        _, err = capsys.readouterr()
        assert (
            "error: argument --target: invalid choice: 'unsupported_target' (choose from 'full', 'standalone')"
            in err
        )


class TestCloudBackupKeep(object):
    """Test the interaction of barman-cloud-backup-delete with the cloud provider."""

    @mock.patch("barman.clients.cloud_backup_keep.get_cloud_interface")
    def test_fails_on_connectivity_test_failure(self, get_cloud_interface_mock):
        """If connectivity test fails we exit."""
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.test_connectivity.return_value = False
        with pytest.raises(SystemExit) as exc:
            cloud_backup_keep.main(
                [
                    "cloud_storage_url",
                    "test_server",
                    "test_backup_id",
                    "--target",
                    "standalone",
                ]
            )
        assert exc.value.code == 2
        cloud_interface_mock.test_connectivity.assert_called_once()

    @mock.patch("barman.clients.cloud_backup_keep.get_cloud_interface")
    def test_fails_if_bucket_not_found(self, get_cloud_interface_mock, caplog):
        """If the bucket does not exist we exit with status 1."""
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.bucket_name = "no_bucket_here"
        cloud_interface_mock.bucket_exists = False
        with pytest.raises(SystemExit) as exc:
            cloud_backup_keep.main(
                [
                    "s3://cloud_storage_url/no_bucket_here",
                    "test_server",
                    "test_backup_id",
                    "--target",
                    "standalone",
                ]
            )
        assert exc.value.code == 1
        assert "Bucket no_bucket_here does not exist" in caplog.text

    @pytest.fixture
    def cloud_backup_catalog(self):
        """Create a mock CloudBackupCatalog with a single BackupInfo"""
        cloud_backup_catalog = mock.Mock()
        mock_backup_info = mock.Mock()
        mock_backup_info.backup_id = "test_backup_id"
        mock_backup_info.status = BackupInfo.DONE
        cloud_backup_catalog.get_backup_info.return_value = mock_backup_info
        cloud_backup_catalog.parse_backup_id.return_value = mock_backup_info.backup_id
        return cloud_backup_catalog

    @mock.patch("barman.clients.cloud_backup_keep.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_keep.get_cloud_interface")
    def test_barman_keep_target(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, cloud_backup_catalog
    ):
        """Verify keep command with --target calls keep_backup"""
        cloud_backup_catalog_mock.return_value = cloud_backup_catalog
        cloud_backup_keep.main(
            [
                "cloud_storage_url",
                "test_server",
                "test_backup_id",
                "--target",
                "standalone",
            ]
        )
        cloud_backup_catalog.keep_backup.assert_called_once_with(
            "test_backup_id", "standalone"
        )

    @mock.patch("barman.clients.cloud_backup_keep.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_keep.get_cloud_interface")
    def test_barman_keep_fails_if_backup_status_not_done(
        self,
        get_cloud_interface_mock,
        cloud_backup_catalog_mock,
        cloud_backup_catalog,
        caplog,
    ):
        cloud_backup_catalog.get_backup_info.return_value.status = (
            BackupInfo.WAITING_FOR_WALS
        )
        cloud_backup_catalog_mock.return_value = cloud_backup_catalog
        with pytest.raises(SystemExit) as exc:
            cloud_backup_keep.main(
                [
                    "cloud_storage_url",
                    "test_server",
                    "test_backup_id",
                    "--target",
                    "standalone",
                ]
            )
        assert exc.value.code == 1
        assert (
            "Cannot add keep to backup test_backup_id because it has status "
            "WAITING_FOR_WALS. Only backups with status DONE can be kept."
        ) in caplog.text
        cloud_backup_catalog_mock.keep_backup.assert_not_called()

    @mock.patch("barman.clients.cloud_backup_keep.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_keep.get_cloud_interface")
    def test_barman_keep_release(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """Verify keep command with --release calls remove_backup"""
        cloud_backup_catalog = cloud_backup_catalog_mock.return_value
        cloud_backup_catalog.parse_backup_id.return_value = "test_backup_id"
        cloud_backup_keep.main(
            [
                "cloud_storage_url",
                "test_server",
                "test_backup_id",
                "--release",
            ]
        )
        cloud_backup_catalog.release_keep.assert_called_once_with("test_backup_id")

    @mock.patch("barman.clients.cloud_backup_keep.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_keep.get_cloud_interface")
    def test_barman_keep_status(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, capsys
    ):
        """Verify keep --status prints get_keep_target_output"""
        cloud_backup_catalog = cloud_backup_catalog_mock.return_value
        cloud_backup_catalog.parse_backup_id.return_value = "test_backup_id"
        cloud_backup_catalog.get_keep_target.return_value = (
            KeepManager.TARGET_STANDALONE
        )
        cloud_backup_keep.main(
            [
                "cloud_storage_url",
                "test_server",
                "test_backup_id",
                "--status",
            ]
        )
        cloud_backup_catalog.get_keep_target.assert_called_once_with("test_backup_id")
        out, _err = capsys.readouterr()
        assert KeepManager.TARGET_STANDALONE in out

    @mock.patch("barman.clients.cloud_backup_keep.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_keep.get_cloud_interface")
    def test_barman_keep_status_nokeep(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, capsys
    ):
        """Verify keep --status prints get_keep_target_output"""
        cloud_backup_catalog = cloud_backup_catalog_mock.return_value
        cloud_backup_catalog.parse_backup_id.return_value = "test_backup_id"
        cloud_backup_catalog.get_keep_target.return_value = None
        cloud_backup_keep.main(
            [
                "cloud_storage_url",
                "test_server",
                "test_backup_id",
                "--status",
            ]
        )
        cloud_backup_catalog.get_keep_target.assert_called_once_with("test_backup_id")
        out, _err = capsys.readouterr()
        assert "nokeep" in out
