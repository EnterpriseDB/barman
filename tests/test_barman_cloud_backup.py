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
                [
                    "--cloud-provider",
                    "google-cloud-storage",
                    "--gcp-project",
                    "test-project",
                    "--snapshot-disk",
                    "disk0",
                    "--snapshot-instance",
                    "test_instance",
                ],
                "Incomplete options for snapshot backup - missing: gcp_zone",
            ],
            [
                [
                    "--cloud-provider",
                    "google-cloud-storage",
                    "--gcp-project",
                    "test-project",
                    "--gcp-zone",
                    "test_zone",
                    "--snapshot-instance",
                    "test_instance",
                ],
                "Incomplete options for snapshot backup - missing: snapshot_disks",
            ],
            [
                [
                    "--cloud-provider",
                    "google-cloud-storage",
                    "--gcp-project",
                    "gcp_project",
                    "--snapshot-disk",
                    "disk0",
                    "--gcp-zone",
                    "test_zone",
                ],
                "Incomplete options for snapshot backup - missing: snapshot_instance",
            ],
            [
                [
                    "--cloud-provider",
                    "google-cloud-storage",
                    "--gcp-project",
                    "gcp_project",
                    "--snapshot-disk",
                    "disk0",
                    "--snapshot-instance",
                    "test_instance",
                    "--gcp-zone",
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
                    "--gcp-zone",
                    "test_zone",
                    "--cloud-provider",
                    "google-cloud-storage",
                ],
                (
                    "--gcp-project option must be set for snapshot backups "
                    "when cloud provider is google-cloud-storage"
                ),
            ],
            [
                [
                    "--snapshot-disk",
                    "disk0",
                    "--snapshot-instance",
                    "test_instance",
                    "--gcp-zone",
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
                    "--gcp-zone",
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
    @mock.patch(
        "barman.cloud_providers.google_cloud_storage.import_google_cloud_compute"
    )
    def test_unsupported_snapshot_args(
        self,
        _mock_google_cloud_compute,
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
    ):
        # GIVEN a mock CloudBackupSnapshot instance
        mock_snapshot_backup = mock_cloud_backup_snapshot.return_value

        # WHEN barman-cloud-backup is called with arguments which cause a snapshot
        # backup to happen
        cloud_backup.main(
            [
                "cloud_storage_url",
                "test_server",
                "--snapshot-disk",
                "disk0",
                "--snapshot-instance",
                "test_instance",
            ]
        )

        # THEN the mock CloudBackupSnapshot instance was called with the expected
        # arguments
        mock_cloud_backup_snapshot.assert_called_once_with(
            "test_server",
            mock_get_cloud_interface.return_value,
            mock_get_snapshot_interface.return_value,
            postgres_connection.return_value,
            "test_instance",
            [
                "disk0",
            ],
            None,
        )
        # AND its backup function was called exactly once
        mock_snapshot_backup.backup.assert_called_once()

    @pytest.mark.parametrize(
        ("aws_cli_args", "expected_cloud_interface_kwargs"),
        [
            # Defaults should result in None values being passed
            (
                [],
                {
                    "encryption": None,
                    "sse_kms_key_id": None,
                },
            ),
            # If values are provided then they should be passed to the cloud interface
            (
                ["--encryption", "aws:kms", "--sse-kms-key-id", "somekeyid"],
                {
                    "encryption": "aws:kms",
                    "sse_kms_key_id": "somekeyid",
                },
            ),
        ],
    )
    @mock.patch("barman.clients.cloud_backup.PostgreSQLConnection")
    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface")
    @mock.patch("barman.clients.cloud_backup.CloudBackupUploader")
    def test_aws_encryption_args(
        self,
        _uploader_mock,
        cloud_interface_mock,
        _mock_postgres_conn,
        _rmtree_mock,
        _tempfile_mock,
        aws_cli_args,
        expected_cloud_interface_kwargs,
    ):
        """Verify that AWS encryption arguments are passed to the cloud interface."""
        # WHEN barman-cloud-backup is run with the provided arguments
        cloud_backup.main(["cloud_storage_url", "test_server"] + aws_cli_args)

        # THEN they are passed to the cloud interface
        cloud_interface_mock.assert_called_once_with(
            url="cloud_storage_url",
            jobs=2,
            tags=None,
            profile_name=None,
            endpoint_url=None,
            read_timeout=None,
            **expected_cloud_interface_kwargs
        )

    @pytest.mark.parametrize(
        ("gcp_cli_args", "expected_cloud_interface_kwargs"),
        [
            # Defaults should result in None values being passed
            (
                [],
                {
                    "kms_key_name": None,
                },
            ),
            # If values are provided then they should be passed to the cloud interface
            (
                ["--kms-key-name", "somekeyname"],
                {
                    "kms_key_name": "somekeyname",
                },
            ),
        ],
    )
    @mock.patch("barman.clients.cloud_backup.PostgreSQLConnection")
    @mock.patch("barman.cloud_providers.google_cloud_storage.GoogleCloudInterface")
    @mock.patch("barman.clients.cloud_backup.CloudBackupUploader")
    def test_gcp_encryption_args(
        self,
        _uploader_mock,
        cloud_interface_mock,
        _mock_postgres_conn,
        _rmtree_mock,
        _tempfile_mock,
        gcp_cli_args,
        expected_cloud_interface_kwargs,
    ):
        """Verify that GCP encryption arguments are passed to the cloud interface."""
        # WHEN barman-cloud-backup is run with the provided arguments
        cloud_backup.main(
            [
                "cloud_storage_url",
                "test_server",
                "--cloud-provider",
                "google-cloud-storage",
            ]
            + gcp_cli_args
        )

        # THEN they are passed to the cloud interface
        cloud_interface_mock.assert_called_once_with(
            url="cloud_storage_url",
            jobs=1,
            tags=None,
            **expected_cloud_interface_kwargs
        )


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
