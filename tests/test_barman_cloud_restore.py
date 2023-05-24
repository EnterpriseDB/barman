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

import mock
import pytest

from barman.clients import cloud_restore
from barman.clients.cloud_cli import OperationErrorExit
from barman.clients.cloud_restore import (
    CloudBackupDownloaderObjectStore,
    CloudBackupDownloaderSnapshot,
)
from barman.cloud import BackupFileInfo
from barman.exceptions import RecoveryPreconditionException


class TestCloudRestore(object):
    @mock.patch("barman.clients.cloud_restore.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_restore.get_cloud_interface")
    def test_cloud_backup_restore_missing_backup(
        self,
        _mock_get_cloud_interface,
        mock_cloud_backup_catalog,
        caplog,
    ):
        """
        Verify plain output of barman-cloud-restore for a backup.
        """
        # GIVEN a backup catalog with a single backup
        backup_id = "20201110T120000"
        cloud_backup_catalog = mock_cloud_backup_catalog.return_value
        cloud_backup_catalog.get_backup_list.return_value = {}
        cloud_backup_catalog.get_backup_info.return_value = None
        cloud_backup_catalog.parse_backup_id.return_value = backup_id
        # WHEN barman_cloud_restore is called for that backup
        # THEN an OperationErrorExit is raised
        with pytest.raises(OperationErrorExit):
            cloud_restore.main(
                ["cloud_storage_url", "test_server", backup_id, "/path/to/dir"]
            )
        # AND an error message was logged
        assert (
            "Backup {} for server test_server does not exist".format(backup_id)
            in caplog.text
        )

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
        mock_backup_info = mock.Mock(backup_id=expected_backup_id, snapshots_info=None)
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

    @pytest.mark.parametrize(
        ("provider", "snapshot_args", "expected_error"),
        (
            # Backups with provider gcp require gcp_zone and snapshot_recovery_instance
            [
                "gcp",
                [],
                (
                    "Incomplete options for snapshot restore - missing: "
                    "snapshot_recovery_instance, gcp_zone"
                ),
            ],
            [
                "gcp",
                [
                    "--snapshot-recovery-instance",
                    "test_instance",
                ],
                "Incomplete options for snapshot restore - missing: gcp_zone",
            ],
            [
                "gcp",
                [
                    "--gcp-zone",
                    "test_zone",
                ],
                (
                    "Incomplete options for snapshot restore - missing: "
                    "snapshot_recovery_instance"
                ),
            ],
            # Backups with provider azure require azure_resource_group and
            # snapshot_recovery_instance
            [
                "azure",
                [],
                (
                    "Incomplete options for snapshot restore - missing: "
                    "snapshot_recovery_instance, azure_resource_group"
                ),
            ],
            [
                "azure",
                [
                    "--snapshot-recovery-instance",
                    "test_instance",
                ],
                (
                    "Incomplete options for snapshot restore - missing: "
                    "azure_resource_group"
                ),
            ],
            [
                "azure",
                [
                    "--azure-resource-group",
                    "test_resource_group",
                ],
                (
                    "Incomplete options for snapshot restore - missing: "
                    "snapshot_recovery_instance"
                ),
            ],
            # Snapshot backups cannot be recovered with tablespace relocation rules
            [
                "gcp",
                [
                    "--snapshot-recovery-instance",
                    "test_instance",
                    "--gcp-zone",
                    "test_zone",
                    "--tablespace",
                    "tbs1:/path/to/tbs1",
                ],
                (
                    "Backup {backup_id} is a snapshot backup therefore tablespace "
                    "relocation rules cannot be used."
                ),
            ],
            [
                "azure",
                [
                    "--snapshot-recovery-instance",
                    "test_instance",
                    "--azure-resource-group",
                    "test_resource_group",
                    "--tablespace",
                    "tbs1:/path/to/tbs1",
                ],
                (
                    "Backup {backup_id} is a snapshot backup therefore tablespace "
                    "relocation rules cannot be used."
                ),
            ],
        ),
    )
    @mock.patch("barman.clients.cloud_restore.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_restore.get_cloud_interface")
    @mock.patch("barman.cloud_providers.azure_blob_storage.import_azure_mgmt_compute")
    @mock.patch(
        "barman.cloud_providers.google_cloud_storage.import_google_cloud_compute"
    )
    def test_unsupported_snapshot_args(
        self,
        _mock_google_cloud_compute,
        _mock_azure_mgmt_compute,
        _mock_cloud_interface_factory,
        mock_catalog,
        provider,
        snapshot_args,
        expected_error,
        caplog,
    ):
        """
        Verify that an error is raised if an unsupported set of snapshot arguments is
        used.
        """
        # GIVEN a mock backup catalog where parse_backup_id will always resolve
        # the backup ID
        backup_id = "20380119T031408"
        mock_catalog.return_value.parse_backup_id.return_value = backup_id
        # AND the catalog returns a mock backup_info with a snapshots_info field
        mock_backup_info = mock.Mock(
            backup_id=backup_id, snapshots_info=mock.Mock(provider=provider)
        )
        mock_catalog.return_value.get_backup_info.return_value = mock_backup_info

        # WHEN barman-cloud-restore is run with a subset of snapshot arguments
        # THEN a SystemExit occurs
        recovery_dir = "/path/to/restore_dir"
        with pytest.raises(SystemExit):
            cloud_restore.main(
                ["cloud_storage_url", "test_server", backup_id, recovery_dir]
                + snapshot_args
            )
        # AND the expected error message occurs
        assert expected_error.format(**{"backup_id": backup_id}) in caplog.text

    @pytest.mark.parametrize(
        ("provider", "provider_args"),
        (
            (
                "gcp",
                ["--gcp-zone", "test_zone"],
            ),
            (
                "azure",
                ["--azure-resource-group", "test_resource_group"],
            ),
        ),
    )
    @mock.patch("barman.clients.cloud_restore.get_snapshot_interface_from_backup_info")
    @mock.patch("barman.clients.cloud_restore.CloudBackupDownloaderSnapshot")
    @mock.patch("barman.clients.cloud_restore.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_restore.get_cloud_interface")
    def test_restore_snapshots_backup(
        self,
        mock_cloud_interface_factory,
        mock_catalog,
        mock_backup_downloader_snapshot,
        mock_get_snapshot_interface,
        provider,
        provider_args,
    ):
        """
        Verify that restoring a snapshots backup uses CloudBackupDownloaderSnapshot
        to restore the backup.
        """
        # GIVEN a mock backup catalog where parse_backup_id will always resolve
        # the backup ID
        backup_id = "20380119T031408"
        mock_catalog.return_value.parse_backup_id.return_value = backup_id
        # AND the catalog returns a mock backup_info with a snapshots_info field
        mock_backup_info = mock.Mock(
            backup_id=backup_id, snapshots_info=mock.Mock(provider=provider)
        )
        mock_catalog.return_value.get_backup_info.return_value = mock_backup_info

        # WHEN barman-cloud-restore is run with the required arguments for a snapshots
        # backup
        recovery_dir = "/path/to/restore_dir"
        recovery_instance = "test_instance"
        cloud_restore.main(
            [
                "cloud_storage_url",
                "test_server",
                backup_id,
                recovery_dir,
                "--snapshot-recovery-instance",
                recovery_instance,
            ]
            + provider_args
        )

        # THEN a CloudBackupDownloaderSnapshot is created
        mock_backup_downloader_snapshot.assert_called_once_with(
            mock_cloud_interface_factory.return_value,
            mock_catalog.return_value,
            mock_get_snapshot_interface.return_value,
        )
        # AND download_backup is called with the expected arguments
        backup_downloader = mock_backup_downloader_snapshot.return_value
        backup_downloader.download_backup.assert_called_once_with(
            mock_backup_info,
            recovery_dir,
            recovery_instance,
        )


class TestCloudBackupDownloader(object):
    """Superclass containing common fixtures for CloudBackupDownloader tests."""

    backup_id = "20380119T031408"

    @pytest.fixture
    def backup_info(self):
        yield mock.Mock(backup_id=self.backup_id)

    @pytest.fixture
    def mock_cloud_interface(self):
        yield mock.Mock(path="")

    @pytest.fixture
    def mock_catalog(self, backup_info):
        catalog = mock.Mock(prefix="test_server/base", server_name="test_server")
        catalog.return_value.parse_backup_id.return_value = self.backup_id
        catalog.return_value.get_backup_info.return_value = backup_info
        yield catalog


class TestCloudBackupDownloaderObjectStore(TestCloudBackupDownloader):
    """Verify the cloud backup downloader for object store backups."""

    @pytest.fixture
    def backup_info(self):
        backup_info = mock.Mock(backup_id=self.backup_id, snapshots_info=None)
        backup_info.wal_directory.return_value = "/path/to/wals"
        yield backup_info

    @mock.patch("barman.clients.cloud_restore.os.path.exists")
    def test_download_backup(
        self,
        mock_os_path_exists,
        backup_info,
        mock_cloud_interface,
        mock_catalog,
    ):
        """Verify that the tar files and backup label are downloaded."""
        # GIVEN a backup catalog with a single backup with a data.tar file
        backup_file_path = "mock_catalog.prefix/{}/data".format(self.backup_id)
        mock_catalog.get_backup_files.return_value = {
            None: BackupFileInfo(oid=None, path=backup_file_path)
        }
        # AND a CloudBackupObjectStoreDownloader
        downloader = CloudBackupDownloaderObjectStore(
            mock_cloud_interface, mock_catalog
        )
        # AND the following recovery args
        recovery_dir = "/path/to/restore_dir"
        # AND the recovery_dir does not exist
        mock_os_path_exists.side_effect = lambda x: x != recovery_dir

        # WHEN download_backup is called
        downloader.download_backup(backup_info, recovery_dir, None)

        # THEN the data.tar file is extracted into the recovery dir
        mock_cloud_interface.extract_tar.assert_called_once_with(
            backup_file_path, recovery_dir
        )

    @mock.patch("barman.clients.cloud_restore.os.listdir")
    @mock.patch("barman.clients.cloud_restore.os.path.exists")
    def test_download_backup_recovery_dir_exists(
        self,
        mock_os_path_exists,
        mock_os_listdir,
        backup_info,
        mock_cloud_interface,
        mock_catalog,
        caplog,
    ):
        """Verify that backup download fails when preconditions are not met."""
        # GIVEN a CloudBackupObjectStoreDownloader
        downloader = CloudBackupDownloaderObjectStore(
            mock_cloud_interface, mock_catalog
        )
        # AND a recovery_dir which exists
        recovery_dir = "/path/to/restore_dir"
        mock_os_path_exists.return_value = True
        mock_os_listdir.return_value = ["some_file"]

        # WHEN download_backup is called
        # THEN an OperationErrorExit is raised
        with pytest.raises(OperationErrorExit):
            downloader.download_backup(backup_info, recovery_dir, None)
        # AND an error message is logged
        assert (
            "Destination {} already exists and it is not empty".format(recovery_dir)
            in caplog.text
        )


class TestCloudBackupDownloaderSnapshot(TestCloudBackupDownloader):
    """Verify the cloud backup downloader for snapshot backups."""

    snapshot_name = "snapshot0"
    disk_name = "disk0"
    mount_point = "/opt/disk0"

    @pytest.fixture
    def snapshots_info(self):
        yield mock.Mock(
            snapshots=[
                mock.Mock(
                    identifier="snapshot0",
                    device="/dev/dev0",
                    mount_point="/opt/disk0",
                    mount_options="rw,noatime",
                ),
            ]
        )

    @pytest.fixture
    def backup_info(self, snapshots_info):
        yield mock.Mock(backup_id=self.backup_id, snapshots_info=snapshots_info)

    @mock.patch("barman.clients.cloud_restore.UnixLocalCommand")
    def test_download_backup(
        self,
        mock_cmd,
        backup_info,
        mock_cloud_interface,
        mock_catalog,
    ):
        """Verify that the backup label is downloaded if all preconditions are met."""
        # GIVEN a snapshot interface which returns volume metadata for the specified
        # snapshots
        mock_volume_metadata = mock.Mock(source_snapshot="snapshot0")

        def mock_resolve_mounted_volume(_self):
            mock_volume_metadata.mount_point = "/opt/disk0"
            mock_volume_metadata.mount_options = "rw,noatime"

        mock_volume_metadata.resolve_mounted_volume.side_effect = (
            mock_resolve_mounted_volume
        )
        mock_snapshots_interface = mock.Mock()
        mock_snapshots_interface.get_attached_volumes.return_value = {
            "disk0": mock_volume_metadata
        }
        # AND a CloudBackupDownloaderSnapshot
        downloader = CloudBackupDownloaderSnapshot(
            mock_cloud_interface, mock_catalog, mock_snapshots_interface
        )
        # AND the following recovery args
        recovery_dir = "/path/to/restore_dir"
        recovery_instance = "test_instance"
        # AND a mock findmnt command which returns a successful response
        mock_cmd.return_value.findmnt.return_value = ["/opt/disk0", "rw,noatime"]
        # AND a mock check_directory_exists command which returns True
        mock_cmd.return_value.check_directory_exists.return_value = True

        # WHEN download_backup is called
        downloader.download_backup(backup_info, recovery_dir, recovery_instance)
        # THEN the backup label is downloaded to the recovery destination
        mock_cloud_interface.download_file.assert_called_once_with(
            "{}/base/{}/backup_label".format(
                mock_catalog.server_name,
                self.backup_id,
            ),
            "{}/backup_label".format(recovery_dir),
            decompress=None,
        )

    @pytest.mark.parametrize(
        (
            "snapshot_name",
            "mount_point",
            "mount_options",
            "check_directory_exists_output",
            "expected_error_msg",
        ),
        (
            # No disk cloned from snapshot attached
            [
                None,
                None,
                None,
                None,
                (
                    "The following snapshots are not attached to recovery instance "
                    "{recovery_instance}: {snapshot_name}"
                ),
            ],
            # Correct disk attached but not mounted in the right place
            [
                "snapshot0",
                "/opt/disk1",
                "rw,noatime",
                None,
                (
                    "Error checking mount points: Disk {disk_name} cloned from "
                    "snapshot {snapshot_name} is mounted at /opt/disk1 but "
                    "{mount_point} was expected."
                ),
            ],
            # Recovery directory not present
            [
                "snapshot0",
                "/opt/disk0",
                "rw,noatime",
                False,
                (
                    "Recovery directory '{recovery_dir}' does not exist on the "
                    "recovery instance. Check all required disks have been created, "
                    "attached and mounted."
                ),
            ],
        ),
    )
    @mock.patch("barman.clients.cloud_restore.UnixLocalCommand")
    def test_download_backup_preconditions_failed(
        self,
        mock_cmd,
        backup_info,
        mock_cloud_interface,
        mock_catalog,
        snapshot_name,
        mount_point,
        mount_options,
        check_directory_exists_output,
        expected_error_msg,
    ):
        """Verify that backup download fails when preconditions are not met."""
        # GIVEN a snapshot interface which returns volume metadata for the specified
        # snapshots
        mock_volume_metadata = mock.Mock(source_snapshot=snapshot_name)

        def mock_resolve_mounted_volume(_self):
            mock_volume_metadata.mount_point = mount_point
            mock_volume_metadata.mount_options = mount_options

        mock_volume_metadata.resolve_mounted_volume.side_effect = (
            mock_resolve_mounted_volume
        )
        mock_snapshots_interface = mock.Mock()
        mock_snapshots_interface.get_attached_volumes.return_value = {
            self.disk_name: mock_volume_metadata
        }
        # AND a CloudBackupDownloaderSnapshot
        downloader = CloudBackupDownloaderSnapshot(
            mock_cloud_interface, mock_catalog, mock_snapshots_interface
        )
        # AND the following recovery args
        recovery_dir = "/path/to/restore_dir"
        recovery_instance = "test_instance"
        # AND a mock check_directory_exists command which returns the specified respone
        mock_cmd.return_value.check_directory_exists.return_value = (
            check_directory_exists_output
        )

        # WHEN download_backup is called
        # THEN a RecoveryPreconditionException is raised
        with pytest.raises(RecoveryPreconditionException) as exc:
            downloader.download_backup(backup_info, recovery_dir, recovery_instance)
        # AND the exception has the expected message
        assert str(exc.value) == expected_error_msg.format(
            disk_name=self.disk_name,
            mount_point=self.mount_point,
            recovery_dir=recovery_dir,
            recovery_instance=recovery_instance,
            snapshot_name=self.snapshot_name,
        )
