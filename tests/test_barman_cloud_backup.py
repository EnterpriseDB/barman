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
import os
import pytest

from barman.clients import cloud_backup

EXAMPLE_BACKUP_DIR = "/path/to/backup"
EXAMPLE_BACKUP_ID = "20210707T132804"


@mock.patch("barman.clients.cloud_backup.tempfile")
@mock.patch("barman.clients.cloud_backup.rmtree")
class TestCloudBackup(object):
    """
    Test that we get the intended behaviour when called directly
    """

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.clients.cloud_backup.PostgreSQLConnection")
    @mock.patch("barman.clients.cloud_backup.get_cloud_interface")
    @mock.patch("barman.clients.cloud_backup.CloudBackupUploader")
    def test_uses_postgres_backup_uploader(
        self,
        uploader_mock,
        cloud_interface_mock,
        postgres_connection,
        _rmtree_mock,
        _tempfile_mock,
    ):
        uploader = uploader_mock.return_value
        cloud_backup.main(["cloud_storage_url", "test_server"])
        postgres_connection.assert_called_once()
        cloud_interface_mock.assert_called_once()
        uploader_mock.assert_called_once_with(
            server_name="test_server",
            compression=None,
            backup_name=None,
            postgres=postgres_connection.return_value,
            max_archive_size=107374182400,
            cloud_interface=cloud_interface_mock.return_value,
        )
        uploader.backup.assert_called_once()

    @mock.patch("barman.clients.cloud_backup.PostgreSQLConnection")
    @mock.patch("barman.clients.cloud_backup.get_cloud_interface")
    @mock.patch("barman.clients.cloud_backup.CloudBackupUploader")
    def test_name_option_success(
        self,
        uploader_mock,
        _cloud_interface_mock,
        _postgres_connection,
        _rmtree_mock,
        _tmpfile_mock,
        caplog,
    ):
        # WHEN barman-cloud-backup is run with the --name option
        cloud_backup.main(
            ["cloud_storage_url", "test_server", "--name", "backup name", "-vv"]
        )

        # THEN no messages or exceptions occur
        assert caplog.messages == []

    @pytest.mark.parametrize(
        ("backup_name", "expected_error"),
        (
            ("latest", "reserved word"),
            ("last", "reserved word"),
            ("oldest", "reserved word"),
            ("first", "reserved word"),
            ("last-failed", "reserved word"),
            ("20201110T120000", "backup ID"),
        ),
    )
    @mock.patch("barman.clients.cloud_backup.PostgreSQLConnection")
    @mock.patch("barman.clients.cloud_backup.get_cloud_interface")
    @mock.patch("barman.clients.cloud_backup.CloudBackupUploader")
    def test_name_option_validation_failure(
        self,
        uploader_mock,
        _cloud_interface_mock,
        _postgres_connection,
        _rmtree_mock,
        _tmpfile_mock,
        backup_name,
        expected_error,
        capsys,
    ):
        # WHEN barman-cloud-backup is run with the --name option
        # THEN a CLIErrorExit occurs
        with pytest.raises(SystemExit):
            cloud_backup.main(
                ["cloud_storage_url", "test_server", "--name", backup_name]
            )

        # AND the expected error message occurs
        _out, err = capsys.readouterr()
        expected_message = "Backup name '%s' is not allowed: %s" % (
            backup_name,
            expected_error,
        )
        assert expected_message in err

    @pytest.mark.parametrize(
        ("snapshot_args", "expected_error"),
        (
            [
                ["--snapshot-disk", "disk0", "--snapshot-instance", "test_instance"],
                "Incomplete options for snapshot backup - missing: snapshot_zone",
            ],
            [
                [
                    "--snapshot-zone",
                    "test_zone",
                    "--snapshot-instance",
                    "test_instance",
                ],
                "Incomplete options for snapshot backup - missing: snapshot_disks",
            ],
            [
                ["--snapshot-disk", "disk0", "--snapshot-zone", "test_zone"],
                "Incomplete options for snapshot backup - missing: snapshot_instance",
            ],
            [
                [
                    "--snapshot-disk",
                    "disk0",
                    "--snapshot-instance",
                    "test_instance",
                    "--snapshot-zone",
                    "test_zone",
                    "--snappy",
                ],
                "Compression options cannot be used with snapshot backups",
            ],
            [
                [
                    "--snapshot-disk",
                    "disk0",
                    "--snapshot-instance",
                    "test_instance",
                    "--snapshot-zone",
                    "test_zone",
                    "--cloud-provider",
                    "google-cloud-storage",
                ],
                (
                    "--snapshot-gcp-project option must be set for snapshot backups "
                    "when cloud provider is google-cloud-storage"
                ),
            ],
            [
                [
                    "--snapshot-disk",
                    "disk0",
                    "--snapshot-instance",
                    "test_instance",
                    "--snapshot-zone",
                    "test_zone",
                ],
                "No snapshot provider for cloud provider: aws-s3",
            ],
            [
                [
                    "--snapshot-disk",
                    "disk0",
                    "--snapshot-instance",
                    "test_instance",
                    "--snapshot-zone",
                    "test_zone",
                    "--cloud-provider",
                    "azure-blob-storage",
                ],
                "No snapshot provider for cloud provider: azure-blob-storage",
            ],
        ),
    )
    @mock.patch("barman.clients.cloud_backup.PostgreSQLConnection")
    @mock.patch("barman.clients.cloud_backup.get_cloud_interface")
    @mock.patch("barman.clients.cloud_backup.CloudBackupUploader")
    def test_unsupported_snapshot_args(
        self,
        uploader_mock,
        _cloud_interface_mock,
        _postgres_connection,
        _rmtree_mock,
        _tmpfile_mock,
        snapshot_args,
        expected_error,
        caplog,
    ):
        """
        Verify that an error is raised if an unsupported set of snapshot arguments is
        used.
        """
        # WHEN barman-cloud-backup is run with a subset of snapshot arguments
        # THEN a SystemExit occurs
        with pytest.raises(SystemExit):
            cloud_backup.main(["cloud_storage_url", "test_server"] + snapshot_args)

        # AND the expected error message occurs
        assert expected_error in caplog.text

    @pytest.mark.parametrize(
        "cloud_provider_args",
        (
            [
                "--cloud-provider",
                "google-cloud-storage",
                "--snapshot-gcp-project",
                "gcp_project",
            ],
        ),
    )
    @mock.patch("barman.clients.cloud_backup.PostgreSQLConnection")
    @mock.patch("barman.clients.cloud_backup.get_snapshot_interface")
    @mock.patch("barman.clients.cloud_backup.get_cloud_interface")
    @mock.patch("barman.clients.cloud_backup.CloudBackupSnapshot")
    def test_uses_snapshot_backup_uploader(
        self,
        mock_cloud_backup_snapshot,
        mock_get_cloud_interface,
        mock_get_snapshot_interface,
        postgres_connection,
        _rmtree_mock,
        _tempfile_mock,
        cloud_provider_args,
    ):
        mock_snapshot_backup = mock_cloud_backup_snapshot.return_value
        cloud_backup.main(
            [
                "cloud_storage_url",
                "test_server",
                "--snapshot-disk",
                "disk0",
                "--snapshot-instance",
                "test_instance",
                "--snapshot-zone",
                "test_zone",
            ]
            + cloud_provider_args
        )
        mock_cloud_backup_snapshot.assert_called_once_with(
            "test_server",
            mock_get_cloud_interface.return_value,
            mock_get_snapshot_interface.return_value,
            postgres_connection.return_value,
            "test_instance",
            "test_zone",
            [
                "disk0",
            ],
            None,
        )
        mock_snapshot_backup.backup.assert_called_once()


@mock.patch("barman.clients.cloud_backup.tempfile")
@mock.patch("barman.clients.cloud_backup.rmtree")
class TestCloudBackupHookScript(object):
    """
    Test that we get the intended behaviour when called as a hook script
    """

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "connection_string",
            "BARMAN_HOOK": "backup_script",
            "BARMAN_PHASE": "post",
            "BARMAN_BACKUP_DIR": EXAMPLE_BACKUP_DIR,
            "BARMAN_BACKUP_ID": EXAMPLE_BACKUP_ID,
            "BARMAN_STATUS": "DONE",
        },
    )
    @mock.patch("barman.clients.cloud_backup.get_cloud_interface")
    @mock.patch("barman.clients.cloud_backup.CloudBackupUploaderBarman")
    def test_uses_barman_backup_uploader_when_running_as_hook(
        self,
        uploader_mock,
        cloud_interface_mock,
        rmtree_mock,
        tempfile_mock,
    ):
        uploader = uploader_mock.return_value
        cloud_backup.main(["cloud_storage_url", "test_server"])
        cloud_interface_mock.assert_called_once()
        uploader_mock.assert_called_once_with(
            server_name="test_server",
            compression=None,
            max_archive_size=107374182400,
            cloud_interface=cloud_interface_mock.return_value,
            backup_dir=EXAMPLE_BACKUP_DIR,
            backup_id=EXAMPLE_BACKUP_ID,
        )
        uploader.backup.assert_called_once()

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "connection_string",
            "BARMAN_HOOK": "backup_retry_script",
            "BARMAN_PHASE": "post",
            "BARMAN_BACKUP_DIR": EXAMPLE_BACKUP_DIR,
            "BARMAN_BACKUP_ID": EXAMPLE_BACKUP_ID,
            "BARMAN_STATUS": "DONE",
        },
    )
    @mock.patch("barman.clients.cloud_backup.get_cloud_interface")
    @mock.patch("barman.clients.cloud_backup.CloudBackupUploaderBarman")
    def test_uses_barman_backup_uploader_when_running_as_retry_hook(
        self,
        uploader_mock,
        cloud_interface_mock,
        rmtree_mock,
        tempfile_mock,
    ):
        uploader = uploader_mock.return_value
        cloud_backup.main(["cloud_storage_url", "test_server"])
        cloud_interface_mock.assert_called_once()
        uploader_mock.assert_called_once_with(
            server_name="test_server",
            compression=None,
            max_archive_size=107374182400,
            cloud_interface=cloud_interface_mock.return_value,
            backup_dir=EXAMPLE_BACKUP_DIR,
            backup_id=EXAMPLE_BACKUP_ID,
        )
        uploader.backup.assert_called_once()

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "connection_string",
            "BARMAN_HOOK": "backup_script",
            "BARMAN_PHASE": "post",
            "BARMAN_BACKUP_ID": EXAMPLE_BACKUP_ID,
            "BARMAN_STATUS": "DONE",
        },
    )
    @mock.patch("barman.clients.cloud_backup.get_cloud_interface")
    @mock.patch("barman.clients.cloud_backup.CloudBackupUploaderBarman")
    def test_error_if_backup_dir_not_provided(
        self,
        uploader_mock,
        cloud_interface_mock,
        rmtree_mock,
        tempfile_mock,
        caplog,
    ):
        with pytest.raises(SystemExit):
            cloud_backup.main(["cloud_storage_url", "test_server"])

        assert "BARMAN_BACKUP_DIR environment variable not set" in caplog.messages[0]
        cloud_interface_mock.assert_called_once()
        uploader_mock.assert_not_called()

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "connection_string",
            "BARMAN_HOOK": "backup_script",
            "BARMAN_PHASE": "post",
            "BARMAN_BACKUP_DIR": EXAMPLE_BACKUP_DIR,
            "BARMAN_STATUS": "DONE",
        },
    )
    @mock.patch("barman.clients.cloud_backup.get_cloud_interface")
    @mock.patch("barman.clients.cloud_backup.CloudBackupUploaderBarman")
    def test_error_if_backup_id_not_provided(
        self,
        uploader_mock,
        cloud_interface_mock,
        rmtree_mock,
        tempfile_mock,
        caplog,
    ):
        with pytest.raises(SystemExit):
            cloud_backup.main(["cloud_storage_url", "test_server"])

        assert "BARMAN_BACKUP_ID environment variable not set" in caplog.messages[0]
        cloud_interface_mock.assert_called_once()
        uploader_mock.assert_not_called()

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "connection_string",
            "BARMAN_HOOK": "backup_script",
            "BARMAN_PHASE": "pre",
            "BARMAN_BACKUP_DIR": EXAMPLE_BACKUP_DIR,
            "BARMAN_BACKUP_ID": EXAMPLE_BACKUP_ID,
            "BARMAN_STATUS": "DONE",
        },
    )
    @mock.patch("barman.clients.cloud_backup.get_cloud_interface")
    @mock.patch("barman.clients.cloud_backup.CloudBackupUploaderBarman")
    def test_error_if_running_as_unsupported_phase(
        self,
        uploader_mock,
        cloud_interface_mock,
        rmtree_mock,
        tempfile_mock,
        caplog,
    ):
        with pytest.raises(SystemExit):
            cloud_backup.main(["cloud_storage_url", "test_server"])

        assert (
            "barman-cloud-backup called as unsupported hook script"
            in caplog.messages[0]
        )
        cloud_interface_mock.assert_called_once()
        uploader_mock.assert_not_called()

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "connection_string",
            "BARMAN_HOOK": "archive_script",
            "BARMAN_PHASE": "post",
            "BARMAN_BACKUP_DIR": EXAMPLE_BACKUP_DIR,
            "BARMAN_BACKUP_ID": EXAMPLE_BACKUP_ID,
            "BARMAN_STATUS": "DONE",
        },
    )
    @mock.patch("barman.clients.cloud_backup.get_cloud_interface")
    @mock.patch("barman.clients.cloud_backup.CloudBackupUploaderBarman")
    def test_error_if_running_as_unsupported_hook(
        self,
        uploader_mock,
        cloud_interface_mock,
        rmtree_mock,
        tempfile_mock,
        caplog,
    ):
        with pytest.raises(SystemExit):
            cloud_backup.main(["cloud_storage_url", "test_server"])

        assert (
            "barman-cloud-backup called as unsupported hook script"
            in caplog.messages[0]
        )
        cloud_interface_mock.assert_called_once()
        uploader_mock.assert_not_called()

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "connection_string",
            "BARMAN_HOOK": "backup_script",
            "BARMAN_PHASE": "post",
            "BARMAN_BACKUP_DIR": EXAMPLE_BACKUP_DIR,
            "BARMAN_BACKUP_ID": EXAMPLE_BACKUP_ID,
            "BARMAN_STATUS": "WAITING_FOR_WALS",
        },
    )
    @mock.patch("barman.clients.cloud_backup.get_cloud_interface")
    @mock.patch("barman.clients.cloud_backup.CloudBackupUploaderBarman")
    def test_error_if_backup_status_is_not_DONE(
        self,
        uploader_mock,
        cloud_interface_mock,
        rmtree_mock,
        tempfile_mock,
        caplog,
    ):
        with pytest.raises(SystemExit) as exc:
            cloud_backup.main(["cloud_storage_url", "test_server"])

        # Barman hook scripts should exit with status 63 if the failure is not
        # recoverable and barman should not continue.
        assert exc.value.code == 63
        expected_error = "backup in '%s' has status '%s' (status should be: DONE)" % (
            EXAMPLE_BACKUP_DIR,
            "WAITING_FOR_WALS",
        )
        assert expected_error in caplog.messages[0]
        cloud_interface_mock.assert_called_once()
        uploader_mock.assert_not_called()

    @mock.patch.dict(
        os.environ,
        {
            "BARMAN_HOOK": "backup_retry_script",
            "BARMAN_PHASE": "post",
            "BARMAN_BACKUP_DIR": EXAMPLE_BACKUP_DIR,
            "BARMAN_BACKUP_ID": EXAMPLE_BACKUP_ID,
            "BARMAN_STATUS": "DONE",
        },
    )
    @mock.patch("barman.clients.cloud_backup.get_cloud_interface")
    def test_error_if_backup_name_set_when_hook_script(
        self,
        _cloud_interface_mock,
        _rmtree_mock,
        _tmpfile_mock,
        caplog,
    ):
        # WHEN barman-cloud-backup is run as a hook script with the --name option
        # THEN a SystemExit occurs
        with pytest.raises(SystemExit):
            cloud_backup.main(
                ["cloud_storage_url", "test_server", "--name", "backup name"]
            )

        # AND the expected error message occurs
        assert (
            "Barman cloud backup exception: "
            "Cannot set backup name when running as a hook script" in caplog.messages
        )
