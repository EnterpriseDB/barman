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
import os
import pytest

from barman.clients import cloud_backup

EXAMPLE_BACKUP_DIR = "/path/to/backup"
EXAMPLE_BACKUP_ID = "20210707T132804"


@mock.patch("barman.clients.cloud_backup.tempfile")
@mock.patch("barman.clients.cloud_backup.rmtree")
class TestCloudBackupHookScript(object):
    """
    Test that we get the intended behaviour when called as a hook script
    """

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.clients.cloud_backup.PostgreSQLConnection")
    @mock.patch("barman.clients.cloud_backup.get_cloud_interface")
    @mock.patch("barman.clients.cloud_backup.CloudBackupUploaderPostgres")
    def test_uses_postgres_backup_uploader_when_not_running_as_hook(
        self,
        uploader_mock,
        cloud_interface_mock,
        postgres_connection,
        rmtree_mock,
        tempfile_mock,
    ):
        uploader = uploader_mock.return_value
        cloud_backup.main(["cloud_storage_url", "test_server"])
        postgres_connection.assert_called_once()
        cloud_interface_mock.assert_called_once()
        uploader_mock.assert_called_once_with(
            server_name="test_server",
            compression=None,
            postgres=postgres_connection.return_value,
            max_archive_size=107374182400,
            cloud_interface=cloud_interface_mock.return_value,
        )
        uploader.backup.assert_called_once()

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
