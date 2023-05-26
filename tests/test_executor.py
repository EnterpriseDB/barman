# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2023
#
# This file is part of Barman.
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
from functools import partial
import logging
import os

import mock
import pytest
from dateutil import tz
from mock import Mock, PropertyMock, patch

from barman.backup_executor import (
    ExclusiveBackupStrategy,
    PostgresBackupExecutor,
    RsyncBackupExecutor,
    SnapshotBackupExecutor,
)
from barman.config import BackupOptions
from barman.exceptions import (
    BackupException,
    CommandFailedException,
    DataTransferFailure,
    FsOperationFailed,
    PostgresConnectionError,
    SnapshotBackupException,
    SshCommandException,
)
from barman.infofile import BackupInfo, LocalBackupInfo, Tablespace
from barman.postgres_plumbing import EXCLUDE_LIST, PGDATA_EXCLUDE_LIST
from barman.server import CheckOutputStrategy, CheckStrategy
from testing_helpers import (
    build_backup_manager,
    build_mocked_server,
    build_test_backup_info,
)


# noinspection PyMethodMayBeStatic
class TestRsyncBackupExecutor(object):
    """
    this class tests the methods of the executor object hierarchy
    """

    def test_rsync_backup_executor_init(self):
        """
        Test the construction of a RsyncBackupExecutor
        """

        # Test
        server = build_mocked_server()
        backup_manager = Mock(server=server, config=server.config)
        assert RsyncBackupExecutor(backup_manager)

        # Test exception for the missing ssh_command
        with pytest.raises(SshCommandException):
            server.config.ssh_command = None
            RsyncBackupExecutor(server)

        # Test exception with local backup and not empty ssh_command
        with pytest.raises(SshCommandException):
            server.config.ssh_command = "Fake ssh command"
            RsyncBackupExecutor(server, local_mode=True)

    def test_reuse_path(self):
        """
        Simple test for the reuse_dir method

        The method is necessary for the execution of incremental backups,
        we need to test that the method build correctly the path
        that will be the base for an incremental backup
        """
        # Build a backup info and configure the mocks
        backup_manager = build_backup_manager()
        backup_info = build_test_backup_info()

        # No path if the backup is not incremental
        assert backup_manager.executor._reuse_path(backup_info) is None

        # check for the expected path with copy
        backup_manager.executor.config.reuse_backup = "copy"
        assert (
            backup_manager.executor._reuse_path(backup_info)
            == "/some/barman/home/main/base/1234567890/data"
        )

        # check for the expected path with link
        backup_manager.executor.config.reuse_backup = "link"
        assert (
            backup_manager.executor._reuse_path(backup_info)
            == "/some/barman/home/main/base/1234567890/data"
        )

    @patch("barman.backup_executor.UnixRemoteCommand")
    def test_check(self, command_mock, capsys):
        """
        Check the ssh connection to a remote server
        """
        backup_manager = build_backup_manager(
            global_conf={
                # Silence the warning for default backup strategy
                "backup_options": "exclusive_backup",
            }
        )
        # Set server_version on the mock postgres connection because the
        # strategy check needs access to it
        backup_manager.executor.strategy.postgres.server_version = 140000

        # Test 1: ssh ok
        check_strategy = CheckOutputStrategy()
        command_mock.return_value.get_last_output.return_value = ("", "")
        backup_manager.executor.check(check_strategy)
        out, err = capsys.readouterr()
        assert err == ""
        assert "ssh: OK" in out

        # Test 2: ssh success, with unclean output (out)
        command_mock.reset_mock()
        command_mock.return_value.get_last_output.return_value = ("This is unclean", "")
        backup_manager.executor.check(check_strategy)
        out, err = capsys.readouterr()
        assert err == ""
        assert "ssh output clean: FAILED" in out

        # Test 2bis: ssh success, with unclean output (err)
        command_mock.reset_mock()
        command_mock.return_value.get_last_output.return_value = ("", "This is unclean")
        backup_manager.executor.check(check_strategy)
        out, err = capsys.readouterr()
        assert err == ""
        assert "ssh output clean: FAILED" in out

        # Test 3: ssh ok and PostgreSQL is not responding
        command_mock.reset_mock()
        command_mock.return_value.get_last_output.return_value = ("", "")
        check_strategy = CheckOutputStrategy()
        backup_manager.server.get_remote_status.return_value = {
            "server_txt_version": None
        }
        backup_manager.server.get_backup.return_value.pgdata = "test/"
        backup_manager.executor.check(check_strategy)
        out, err = capsys.readouterr()
        assert err == ""
        assert "ssh: OK" in out
        assert (
            "Check that the PostgreSQL server is up and no "
            "'backup_label' file is in PGDATA." in out
        )

        # Test 3-err: ssh ok and PostgreSQL is not configured
        command_mock.reset_mock()
        command_mock.return_value.get_last_output.return_value = ("", "")
        check_strategy = CheckOutputStrategy()
        # No postgres instance, so no remote status keys available
        backup_manager.server.get_remote_status.return_value = {}
        backup_manager.server.get_backup.return_value.pgdata = "test/"
        # No exception must raise
        backup_manager.executor.check(check_strategy)
        out, err = capsys.readouterr()
        assert err == ""
        assert "ssh: OK" in out

        # Test 4: ssh failed
        command_mock.reset_mock()
        command_mock.side_effect = FsOperationFailed
        backup_manager.executor.check(check_strategy)
        out, err = capsys.readouterr()
        assert err == ""
        assert "ssh: FAILED" in out

    @patch("barman.backup.RsyncBackupExecutor.backup_copy")
    @patch("barman.backup.BackupManager.get_previous_backup")
    @patch("barman.backup.BackupManager.remove_wal_before_backup")
    def test_backup(self, rwbb_mock, gpb_mock, backup_copy_mock, capsys, tmpdir):
        """
        Test the execution of a backup

        :param rwbb_mock: mock for the remove_wal_before_backup method
        :param gpb_mock: mock for the get_previous_backup method
        :param backup_copy_mock: mock for the executor's backup_copy method
        :param capsys: stdout capture module
        :param tmpdir: pytest temp directory
        """
        backup_manager = build_backup_manager(
            global_conf={
                "barman_home": tmpdir.mkdir("home").strpath,
                # Silence the warning for default backup strategy
                "backup_options": "exclusive_backup",
            }
        )
        backup_info = LocalBackupInfo(backup_manager.server, backup_id="fake_backup_id")
        backup_info.begin_xlog = "0/2000028"
        backup_info.begin_wal = "000000010000000000000002"
        backup_info.begin_offset = 40
        backup_info.status = BackupInfo.EMPTY
        backup_info.copy_stats = dict(copy_time=100)

        gpb_mock.return_value = None

        rwbb_mock.return_value = ["000000010000000000000001"]

        # Test 1: exclusive backup
        backup_manager.executor.strategy = Mock()
        backup_manager.executor.backup(backup_info)
        out, err = capsys.readouterr()
        assert err == ""
        assert (
            "Backup start at LSN: "
            "0/2000028 (000000010000000000000002, 00000028)\n"
            "This is the first backup for server main\n"
            "WAL segments preceding the current backup have been found:\n"
            "\t000000010000000000000001 from server main has been removed\n"
            "Starting backup copy via rsync/SSH for fake_backup_id\n"
            "Copy done (time: 1 minute, 40 seconds)"
        ) in out

        gpb_mock.assert_called_with(backup_info.backup_id)
        rwbb_mock.assert_called_with(backup_info)
        backup_manager.executor.strategy.start_backup.assert_called_once_with(
            backup_info
        )
        backup_copy_mock.assert_called_once_with(backup_info)
        backup_manager.executor.strategy.stop_backup.assert_called_once_with(
            backup_info
        )

        # Test 2: concurrent backup
        # change the configuration to concurrent backup
        backup_manager.executor.config.backup_options = [
            BackupOptions.CONCURRENT_BACKUP
        ]

        # reset mocks
        gpb_mock.reset_mock()
        rwbb_mock.reset_mock()
        backup_manager.executor.strategy.reset_mock()
        backup_copy_mock.reset_mock()

        # prepare data directory for backup_label generation
        backup_info.backup_label = "test\nlabel\n"

        backup_manager.executor.backup(backup_info)
        out, err = capsys.readouterr()
        assert err == ""
        assert (
            "Backup start at LSN: "
            "0/2000028 (000000010000000000000002, 00000028)\n"
            "This is the first backup for server main\n"
            "WAL segments preceding the current backup have been found:\n"
            "\t000000010000000000000001 from server main has been removed\n"
            "Starting backup copy via rsync/SSH for fake_backup_id\n"
            "Copy done (time: 1 minute, 40 seconds)"
        ) in out

        gpb_mock.assert_called_with(backup_info.backup_id)
        rwbb_mock.assert_called_with(backup_info)
        backup_manager.executor.strategy.start_backup.assert_called_once_with(
            backup_info
        )
        backup_copy_mock.assert_called_once_with(backup_info)
        backup_manager.executor.strategy.start_backup.assert_called_once_with(
            backup_info
        )

    @patch("barman.backup_executor.RsyncCopyController")
    def test_backup_copy(self, rsync_mock, tmpdir):
        """
        Test the execution of a rsync copy

        :param rsync_mock: mock for the RsyncCopyController object
        :param tmpdir: temporary dir
        """
        backup_manager = build_backup_manager(
            global_conf={"barman_home": tmpdir.mkdir("home").strpath}
        )
        backup_manager.server.path = None
        backup_manager.server.postgres.server_major_version = "9.6"
        backup_info = build_test_backup_info(
            server=backup_manager.server,
            pgdata="/pg/data",
            config_file="/etc/postgresql.conf",
            hba_file="/pg/data/pg_hba.conf",
            ident_file="/pg/data/pg_ident.conf",
            begin_xlog="0/2000028",
            begin_wal="000000010000000000000002",
            begin_offset=28,
        )
        backup_info.save()
        # This is to check that all the preparation is done correctly
        assert os.path.exists(backup_info.filename)

        backup_manager.executor.backup_copy(backup_info)

        assert rsync_mock.mock_calls == [
            mock.call(
                reuse_backup=None,
                safe_horizon=None,
                network_compression=False,
                ssh_command="ssh",
                path=None,
                ssh_options=[
                    "-c",
                    '"arcfour"',
                    "-p",
                    "22",
                    "postgres@pg01.nowhere",
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    "StrictHostKeyChecking=no",
                ],
                retry_sleep=30,
                retry_times=0,
                workers=1,
                workers_start_batch_period=1,
                workers_start_batch_size=10,
            ),
            mock.call().add_directory(
                label="tbs1",
                src=":/fake/location/",
                dst=backup_info.get_data_directory(16387),
                reuse=None,
                bwlimit=None,
                item_class=rsync_mock.return_value.TABLESPACE_CLASS,
                exclude=["/*"] + EXCLUDE_LIST,
                include=["/PG_9.6_*"],
            ),
            mock.call().add_directory(
                label="tbs2",
                src=":/another/location/",
                dst=backup_info.get_data_directory(16405),
                reuse=None,
                bwlimit=None,
                item_class=rsync_mock.return_value.TABLESPACE_CLASS,
                exclude=["/*"] + EXCLUDE_LIST,
                include=["/PG_9.6_*"],
            ),
            mock.call().add_directory(
                label="pgdata",
                src=":/pg/data/",
                dst=backup_info.get_data_directory(),
                reuse=None,
                bwlimit=None,
                item_class=rsync_mock.return_value.PGDATA_CLASS,
                exclude=(PGDATA_EXCLUDE_LIST + EXCLUDE_LIST),
                exclude_and_protect=["/pg_tblspc/16387", "/pg_tblspc/16405"],
            ),
            mock.call().add_file(
                label="pg_control",
                src=":/pg/data/global/pg_control",
                dst="%s/global/pg_control" % backup_info.get_data_directory(),
                item_class=rsync_mock.return_value.PGCONTROL_CLASS,
            ),
            mock.call().add_file(
                label="config_file",
                src=":/etc/postgresql.conf",
                dst=backup_info.get_data_directory(),
                item_class=rsync_mock.return_value.CONFIG_CLASS,
                optional=False,
            ),
            mock.call().copy(),
            mock.call().statistics(),
        ]

    @patch("barman.backup_executor.RsyncCopyController")
    def test_backup_copy_tablespaces_in_datadir(self, rsync_mock, tmpdir):
        """
        Test the execution of a rsync copy with tablespaces in data directory

        :param rsync_mock: mock for the RsyncCopyController object
        :param tmpdir: temporary dir
        """
        backup_manager = build_backup_manager(
            global_conf={"barman_home": tmpdir.mkdir("home").strpath}
        )
        backup_manager.server.path = None
        backup_manager.server.postgres.server_major_version = "9.6"
        backup_info = build_test_backup_info(
            server=backup_manager.server,
            pgdata="/pg/data",
            config_file="/etc/postgresql.conf",
            hba_file="/pg/data/pg_hba.conf",
            ident_file="/pg/data/pg_ident.conf",
            begin_xlog="0/2000028",
            begin_wal="000000010000000000000002",
            begin_offset=28,
            tablespaces=(
                ("tbs1", 16387, "/pg/data/tbs1"),
                ("tbs2", 16405, "/pg/data/pg_tblspc/tbs2"),
                ("tbs3", 123456, "/pg/data3"),
            ),
        )
        backup_info.save()
        # This is to check that all the preparation is done correctly
        assert os.path.exists(backup_info.filename)

        backup_manager.executor.backup_copy(backup_info)

        assert rsync_mock.mock_calls == [
            mock.call(
                reuse_backup=None,
                safe_horizon=None,
                network_compression=False,
                ssh_command="ssh",
                path=None,
                ssh_options=[
                    "-c",
                    '"arcfour"',
                    "-p",
                    "22",
                    "postgres@pg01.nowhere",
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    "StrictHostKeyChecking=no",
                ],
                retry_sleep=30,
                retry_times=0,
                workers=1,
                workers_start_batch_period=1,
                workers_start_batch_size=10,
            ),
            mock.call().add_directory(
                label="tbs1",
                src=":/pg/data/tbs1/",
                dst=backup_info.get_data_directory(16387),
                reuse=None,
                bwlimit=None,
                item_class=rsync_mock.return_value.TABLESPACE_CLASS,
                exclude=["/*"] + EXCLUDE_LIST,
                include=["/PG_9.6_*"],
            ),
            mock.call().add_directory(
                label="tbs2",
                src=":/pg/data/pg_tblspc/tbs2/",
                dst=backup_info.get_data_directory(16405),
                reuse=None,
                bwlimit=None,
                item_class=rsync_mock.return_value.TABLESPACE_CLASS,
                exclude=["/*"] + EXCLUDE_LIST,
                include=["/PG_9.6_*"],
            ),
            mock.call().add_directory(
                label="tbs3",
                src=":/pg/data3/",
                dst=backup_info.get_data_directory(123456),
                reuse=None,
                bwlimit=None,
                item_class=rsync_mock.return_value.TABLESPACE_CLASS,
                exclude=["/*"] + EXCLUDE_LIST,
                include=["/PG_9.6_*"],
            ),
            mock.call().add_directory(
                label="pgdata",
                src=":/pg/data/",
                dst=backup_info.get_data_directory(),
                reuse=None,
                bwlimit=None,
                item_class=rsync_mock.return_value.PGDATA_CLASS,
                exclude=(PGDATA_EXCLUDE_LIST + EXCLUDE_LIST),
                exclude_and_protect=[
                    "/tbs1",
                    "/pg_tblspc/16387",
                    "/pg_tblspc/tbs2",
                    "/pg_tblspc/16405",
                    "/pg_tblspc/123456",
                ],
            ),
            mock.call().add_file(
                label="pg_control",
                src=":/pg/data/global/pg_control",
                dst="%s/global/pg_control" % backup_info.get_data_directory(),
                item_class=rsync_mock.return_value.PGCONTROL_CLASS,
            ),
            mock.call().add_file(
                label="config_file",
                src=":/etc/postgresql.conf",
                dst=backup_info.get_data_directory(),
                item_class=rsync_mock.return_value.CONFIG_CLASS,
                optional=False,
            ),
            mock.call().copy(),
            mock.call().statistics(),
        ]

    @patch("barman.backup_executor.RsyncCopyController")
    def test_backup_copy_with_included_files(self, rsync_moc, tmpdir, capsys):
        backup_manager = build_backup_manager(
            global_conf={"barman_home": tmpdir.mkdir("home").strpath}
        )
        # Create a backup info with additional configuration files
        backup_info = build_test_backup_info(
            server=backup_manager.server,
            pgdata="/pg/data",
            config_file="/etc/postgresql.conf",
            hba_file="/pg/data/pg_hba.conf",
            ident_file="/pg/data/pg_ident.conf",
            begin_xlog="0/2000028",
            begin_wal="000000010000000000000002",
            included_files=["/tmp/config/file.conf"],
            begin_offset=28,
        )
        backup_info.save()
        # This is to check that all the preparation is done correctly
        assert os.path.exists(backup_info.filename)
        # Execute a backup
        backup_manager.executor.backup_copy(backup_info)
        out, err = capsys.readouterr()
        # check for the presence of the warning in the stderr
        assert ("WARNING: The usage of include directives is not supported") in err
        # check that the additional configuration file is present in the output
        assert backup_info.included_files[0] in err

    @patch("barman.backup_executor.RsyncCopyController")
    def test_backup_copy_with_included_files_nowarning(self, rsync_moc, tmpdir, capsys):
        backup_manager = build_backup_manager(
            global_conf={
                "barman_home": tmpdir.mkdir("home").strpath,
            },
            main_conf={
                "backup_options": "exclusive_backup, external_configuration",
            },
        )
        # Create a backup info with additional configuration files
        backup_info = build_test_backup_info(
            server=backup_manager.server,
            pgdata="/pg/data",
            config_file="/etc/postgresql.conf",
            hba_file="/pg/data/pg_hba.conf",
            ident_file="/pg/data/pg_ident.conf",
            begin_xlog="0/2000028",
            begin_wal="000000010000000000000002",
            included_files=["/tmp/config/file.conf"],
            begin_offset=28,
        )
        backup_info.save()
        # This is to check that all the preparation is done correctly
        assert os.path.exists(backup_info.filename)
        # Execute a backup
        backup_manager.executor.backup_copy(backup_info)
        out, err = capsys.readouterr()
        # check for the presence of the warning in the stderr
        assert ("WARNING: The usage of include directives is not supported") not in err

    def test_validate_config_compression(self):
        # GIVEN a server with backup_method = rysnc and backup_method = gzip
        server = build_mocked_server(
            global_conf={"backup_method": "rsync", "backup_compression": "gzip"}
        )

        # WHEN an RsyncBackupExecutor is created
        RsyncBackupExecutor(server.backup_manager)

        # THEN the server is disabled
        assert server.config.disabled
        # AND the server config has a single error message
        assert len(server.config.msg_list) == 1
        assert (
            "backup_compression option is not supported by rsync backup_method"
            in server.config.msg_list[0]
        )


# noinspection PyMethodMayBeStatic
class TestStrategy(object):
    """
    Testing class for backup strategies
    """

    def test_exclusive_start_backup(self):
        """
        Basic test for the exclusive start_backup method
        """
        # Build a backup_manager using a mocked server
        server = build_mocked_server(
            main_conf={"backup_options": BackupOptions.EXCLUSIVE_BACKUP}
        )
        backup_manager = build_backup_manager(server=server)

        # Mock server.get_pg_setting('data_directory') call
        backup_manager.server.postgres.get_setting.return_value = "/pg/data"
        # Mock server.get_pg_configuration_files() call
        server.postgres.get_configuration_files.return_value = dict(
            config_file="/etc/postgresql.conf",
            hba_file="/pg/pg_hba.conf",
            ident_file="/pg/pg_ident.conf",
        )
        # Mock server.get_pg_tablespaces() call
        tablespaces = [Tablespace._make(("test_tbs", 1234, "/tbs/test"))]
        server.postgres.get_tablespaces.return_value = tablespaces

        # Test 1: start exclusive backup
        # Mock server.start_exclusive_backup(label) call
        start_time = datetime.datetime.now()
        server.postgres.start_exclusive_backup.return_value = {
            "location": "A257/44B4C0D8",
            "file_name": "000000060000A25700000044",
            "file_offset": 11845848,
            "timestamp": start_time,
        }

        # Build a test empty backup info
        backup_info = LocalBackupInfo(server=backup_manager.server, backup_id="fake_id")

        backup_manager.executor.strategy.start_backup(backup_info)

        # Check that all the values are correctly saved inside the BackupInfo
        assert backup_info.pgdata == "/pg/data"
        assert backup_info.config_file == "/etc/postgresql.conf"
        assert backup_info.hba_file == "/pg/pg_hba.conf"
        assert backup_info.ident_file == "/pg/pg_ident.conf"
        assert backup_info.tablespaces == tablespaces
        assert backup_info.status == "STARTED"
        assert backup_info.timeline == 6
        assert backup_info.begin_xlog == "A257/44B4C0D8"
        assert backup_info.begin_wal == "000000060000A25700000044"
        assert backup_info.begin_offset == 11845848
        assert backup_info.begin_time == start_time
        # Check that the correct call to start_exclusive_backup has been made
        server.postgres.start_exclusive_backup.assert_called_with(
            "Barman backup main fake_id"
        )

    def test_start_backup_for_old_pg(self):
        """
        Test concurrent start backup when postgres version older then 9.6
        """
        # Test: start concurrent backup
        # Build a backup_manager using a mocked server
        server = build_mocked_server(
            main_conf={"backup_options": BackupOptions.CONCURRENT_BACKUP}
        )
        backup_manager = build_backup_manager(server=server)
        # Simulate old Postgres version
        backup_manager.server.postgres.is_minimal_postgres_version.return_value = False
        # Build a test empty backup info
        backup_info = LocalBackupInfo(
            server=backup_manager.server, backup_id="fake_id2"
        )

        with pytest.raises(BackupException):
            backup_manager.executor.strategy.start_backup(backup_info)

    def test_concurrent_start_backup(self):
        """
        Test concurrent backup using 9.6 api
        """
        # Test: start concurrent backup
        # Build a backup_manager using a mocked server
        server = build_mocked_server(
            main_conf={"backup_options": BackupOptions.CONCURRENT_BACKUP}
        )
        backup_manager = build_backup_manager(server=server)
        # Mock server.get_pg_setting('data_directory') call
        backup_manager.server.postgres.get_setting.return_value = "/pg/data"
        # Mock server.get_pg_configuration_files() call
        server.postgres.get_configuration_files.return_value = dict(
            config_file="/etc/postgresql.conf",
            hba_file="/pg/pg_hba.conf",
            ident_file="/pg/pg_ident.conf",
        )
        # Mock server.get_pg_tablespaces() call
        tablespaces = [Tablespace._make(("test_tbs", 1234, "/tbs/test"))]
        server.postgres.get_tablespaces.return_value = tablespaces
        # this is a postgres 9.6
        server.postgres.server_version = 90600

        # Mock call to new api method
        start_time = datetime.datetime.now()
        server.postgres.start_concurrent_backup.return_value = {
            "location": "A257/44B4C0D8",
            "timeline": 6,
            "timestamp": start_time,
        }
        # Build a test empty backup info
        backup_info = LocalBackupInfo(
            server=backup_manager.server, backup_id="fake_id2"
        )

        backup_manager.executor.strategy.start_backup(backup_info)

        # Check that all the values are correctly saved inside the BackupInfo
        assert backup_info.pgdata == "/pg/data"
        assert backup_info.config_file == "/etc/postgresql.conf"
        assert backup_info.hba_file == "/pg/pg_hba.conf"
        assert backup_info.ident_file == "/pg/pg_ident.conf"
        assert backup_info.tablespaces == tablespaces
        assert backup_info.status == "STARTED"
        assert backup_info.timeline == 6
        assert backup_info.begin_xlog == "A257/44B4C0D8"
        assert backup_info.begin_wal == "000000060000A25700000044"
        assert backup_info.begin_offset == 11845848
        assert backup_info.begin_time == start_time

    def test_exclusive_stop_backup(self):
        """
        Basic test for the stop_backup method
        """
        # Build a backup info and configure the mocks
        server = build_mocked_server(
            main_conf={"backup_options": BackupOptions.EXCLUSIVE_BACKUP}
        )
        backup_manager = build_backup_manager(server=server)
        # Mock postgres.stop_exclusive_backup() call
        stop_time = datetime.datetime.now()
        server.postgres.stop_exclusive_backup.return_value = {
            "location": "266/4A9C1EF8",
            "file_name": "00000010000002660000004A",
            "file_offset": 10231544,
            "timestamp": stop_time,
        }

        backup_info = build_test_backup_info(server=server)
        backup_manager.executor.strategy.stop_backup(backup_info)

        # check that the submitted values are stored inside the BackupInfo obj
        assert backup_info.end_xlog == "266/4A9C1EF8"
        assert backup_info.end_wal == "00000010000002660000004A"
        assert backup_info.end_offset == 10231544
        assert backup_info.end_time == stop_time

    def test_stop_backup_for_old_pg(self):
        """
        Test concurrent stop backup when postgres version older then 9.6
        """
        # Build a backup info and configure the mocks
        server = build_mocked_server(
            main_conf={"backup_options": BackupOptions.CONCURRENT_BACKUP}
        )
        backup_manager = build_backup_manager(server=server)

        # Simulate old postgres version
        backup_manager.server.postgres.is_minimal_postgres_version.return_value = False

        backup_info = build_test_backup_info(timeline=6)
        with pytest.raises(BackupException):
            backup_manager.executor.strategy.stop_backup(backup_info)

    @patch("barman.backup_executor.LocalConcurrentBackupStrategy._write_backup_label")
    def test_concurrent_stop_backup(self, tbs_map_mock):
        """
        Basic test for the stop_backup method for 9.6 concurrent api

        :param label_mock: mimic the response of _write_backup_label
        """
        # Build a backup info and configure the mocks
        server = build_mocked_server(
            main_conf={"backup_options": BackupOptions.CONCURRENT_BACKUP}
        )
        backup_manager = build_backup_manager(server=server)

        stop_time = datetime.datetime.now()
        # This is a pg 9.6
        server.postgres.server_version = 90600
        # Mock stop backup call for the new api method
        start_time = datetime.datetime.now(tz.tzlocal()).replace(microsecond=0)
        server.postgres.stop_concurrent_backup.return_value = {
            "location": "A266/4A9C1EF8",
            "timeline": 6,
            "timestamp": stop_time,
            "backup_label": "START WAL LOCATION: A257/44B4C0D8 "
            # Timeline 0 simulates a bug in PostgreSQL 9.6 beta2
            "(file 000000000000A25700000044)\n"
            "START TIME: %s\n" % start_time.strftime("%Y-%m-%d %H:%M:%S %Z"),
        }

        backup_info = build_test_backup_info()
        backup_manager.executor.strategy.stop_backup(backup_info)

        assert backup_info.end_xlog == "A266/4A9C1EF8"
        assert backup_info.end_wal == "000000060000A2660000004A"
        assert backup_info.end_offset == 0x9C1EF8
        assert backup_info.end_time == stop_time
        assert backup_info.backup_label == (
            "START WAL LOCATION: A257/44B4C0D8 "
            "(file 000000000000A25700000044)\n"
            "START TIME: %s\n" % start_time.strftime("%Y-%m-%d %H:%M:%S %Z")
        )

    @pytest.mark.parametrize(
        ("server_version", "expected_message"),
        [(140000, ""), (150000, "exclusive backups not supported on PostgreSQL 15")],
    )
    def test_exclusive_check(self, server_version, expected_message, capsys):
        # GIVEN a PostgreSQL connection of the specified version
        mock_postgres = mock.Mock()
        mock_postgres.server_version = server_version
        mock_postgres.server_major_version = str(server_version)[:2]
        # AND the PostgreSQL server is not in recovery
        mock_postgres.is_in_recovery = False

        # AND a ConcurrentBackupStrategy for that server
        strategy = ExclusiveBackupStrategy(mock_postgres, "test server")

        # AND a CheckOutputStrategy
        check_strategy = CheckOutputStrategy()

        # WHEN the check function is called
        strategy.check(check_strategy)

        # THEN if an error is expected, the "exclusive backup supported"
        # check has status False
        check_result = [
            r
            for r in check_strategy.check_result
            if r.check == "exclusive backup supported"
        ][0]
        if len(expected_message) > 0:
            assert check_result.status is False
            # AND the output contains the expected message
            out, _err = capsys.readouterr()
            assert expected_message in out
        # OR if no errors are expected, the "exclusive backup supported"
        # check has status True
        else:
            assert check_result.status is True


class TestPostgresBackupExecutor(object):
    """
    This class tests the methods of the executor object hierarchy
    """

    def test_postgres_backup_executor_init(self):
        """
        Test the construction of a PostgresBackupExecutor
        """
        server = build_mocked_server(global_conf={"backup_method": "postgres"})
        executor = PostgresBackupExecutor(server.backup_manager)
        assert executor
        assert executor.strategy

        # Expect an error if the tablespace_bandwidth_limit option
        # is set for this server.
        server = build_mocked_server(
            global_conf={"backup_method": "postgres", "tablespace_bandwidth_limit": 1}
        )
        executor = PostgresBackupExecutor(server.backup_manager)
        assert executor
        assert executor.strategy
        assert server.config.disabled

    @patch("barman.backup_executor.PostgresBackupExecutor.backup_copy")
    @patch("barman.backup.BackupManager.get_previous_backup")
    def test_backup(self, gpb_mock, pbc_mock, capsys, tmpdir):
        """
        Test backup

        :param gpb_mock: mock for the get_previous_backup method
        :param pbc_mock: mock for the backup_copy method
        :param capsys: stdout capture module
        :param tmpdir: pytest temp directory
        """
        tmp_home = tmpdir.mkdir("home")
        backup_manager = build_backup_manager(
            global_conf={"barman_home": tmp_home.strpath, "backup_method": "postgres"}
        )
        backup_info = build_test_backup_info(
            backup_id="fake_backup_id",
            server=backup_manager.server,
            pgdata="/pg/data",
            config_file="/pg/data/postgresql.conf",
            hba_file="/pg/data/pg_hba.conf",
            ident_file="/pg/pg_ident.conf",
            begin_offset=28,
            copy_stats=dict(copy_time=100, total_time=105),
        )
        current_xlog_timestamp = datetime.datetime(2015, 10, 26, 14, 38)
        backup_manager.server.postgres.current_xlog_info = dict(
            location="0/12000090",
            file_name="000000010000000000000012",
            file_offset=144,
            timestamp=current_xlog_timestamp,
        )
        backup_manager.server.postgres.get_setting.return_value = "/pg/data"
        tmp_backup_label = (
            tmp_home.mkdir("main")
            .mkdir("base")
            .mkdir("fake_backup_id")
            .mkdir("data")
            .join("backup_label")
        )
        start_time = datetime.datetime.now(tz.tzlocal()).replace(microsecond=0)
        tmp_backup_label.write(
            "START WAL LOCATION: 0/40000028 (file 000000010000000000000040)\n"
            "CHECKPOINT LOCATION: 0/40000028\n"
            "BACKUP METHOD: streamed\n"
            "BACKUP FROM: master\n"
            "START TIME: %s\n"
            "LABEL: pg_basebackup base backup"
            % start_time.strftime("%Y-%m-%d %H:%M:%S %Z")
        )
        backup_manager.executor.backup(backup_info)
        out, err = capsys.readouterr()
        gpb_mock.assert_called_once_with(backup_info.backup_id)
        assert err == ""
        assert "Starting backup copy via pg_basebackup" in out
        assert "Copy done" in out
        assert "Finalising the backup." in out
        assert backup_info.end_xlog == "0/12000090"
        assert backup_info.end_offset == 144
        assert backup_info.begin_time == current_xlog_timestamp
        assert backup_info.begin_wal == "000000010000000000000040"

        # Check the CommandFailedException re raising
        with pytest.raises(CommandFailedException):
            pbc_mock.side_effect = CommandFailedException("test")
            backup_manager.executor.backup(backup_info)

    @patch("barman.backup_executor.PostgresBackupExecutor.get_remote_status")
    def test_check(self, remote_status_mock):
        """
        Very simple and basic test for the check method
        :param remote_status_mock: mock for the get_remote_status method
        """
        remote_status_mock.return_value = {
            "pg_basebackup_compatible": True,
            "pg_basebackup_installed": True,
            "pg_basebackup_path": "/fake/path",
            "pg_basebackup_bwlimit": True,
            "pg_basebackup_version": "9.5",
            "pg_basebackup_tbls_mapping": True,
        }
        check_strat = CheckStrategy()
        backup_manager = build_backup_manager(global_conf={"backup_method": "postgres"})
        backup_manager.server.postgres.server_txt_version = "9.5"
        backup_manager.executor.check(check_strategy=check_strat)
        # No errors detected
        assert check_strat.has_error is not True

        remote_status_mock.reset_mock()
        remote_status_mock.return_value = {
            "pg_basebackup_compatible": False,
            "pg_basebackup_installed": True,
            "pg_basebackup_path": True,
            "pg_basebackup_bwlimit": True,
            "pg_basebackup_version": "9.5",
            "pg_basebackup_tbls_mapping": True,
        }
        check_strat = CheckStrategy()
        backup_manager.executor.check(check_strategy=check_strat)
        # Error present because of the 'pg_basebackup_compatible': False
        assert check_strat.has_error is True

        # Even if pg_backup has no tbls_mapping option the check
        # succeeds if the server doesn't have any tablespaces
        remote_status_mock.reset_mock()
        remote_status_mock.return_value = {
            "pg_basebackup_compatible": True,
            "pg_basebackup_installed": True,
            "pg_basebackup_path": True,
            "pg_basebackup_bwlimit": True,
            "pg_basebackup_version": "9.3",
            "pg_basebackup_tbls_mapping": False,
        }
        check_strat = CheckStrategy()
        backup_manager.server.postgres.get_tablespaces.return_value = []
        backup_manager.executor.check(check_strategy=check_strat)
        assert check_strat.has_error is False

        # This check fails because the server contains tablespaces and
        # pg_basebackup doesn't support the tbls_mapping option
        remote_status_mock.reset_mock()
        remote_status_mock.return_value = {
            "pg_basebackup_compatible": True,
            "pg_basebackup_installed": True,
            "pg_basebackup_path": True,
            "pg_basebackup_bwlimit": True,
            "pg_basebackup_version": "9.3",
            "pg_basebackup_tbls_mapping": False,
        }
        check_strat = CheckStrategy()
        backup_manager.server.postgres.get_tablespaces.return_value = [True]
        backup_manager.executor.check(check_strategy=check_strat)
        assert check_strat.has_error is True

    @patch("barman.command_wrappers.PostgreSQLClient.find_command")
    def test_fetch_remote_status(self, find_command):
        """
        Test the fetch_remote_status method
        :param cmd_mock: mock the Command class
        """
        backup_manager = build_backup_manager(global_conf={"backup_method": "postgres"})
        # Simulate the absence of pg_basebackup
        find_command.side_effect = CommandFailedException
        backup_manager.server.streaming.server_major_version = "9.5"
        remote = backup_manager.executor.fetch_remote_status()
        assert remote["pg_basebackup_installed"] is False
        assert remote["pg_basebackup_path"] is None

        # Simulate the presence of pg_basebackup 9.5.1 and pg 95
        find_command.side_effect = None
        find_command.return_value.cmd = "/fake/path"
        find_command.return_value.out = "pg_basebackup 9.5.1"
        backup_manager.server.streaming.server_major_version = "9.5"
        backup_manager.server.path = "fake/path2"
        remote = backup_manager.executor.fetch_remote_status()
        assert remote["pg_basebackup_installed"] is True
        assert remote["pg_basebackup_path"] == "/fake/path"
        assert remote["pg_basebackup_version"] == "9.5.1"
        assert remote["pg_basebackup_compatible"] is True
        assert remote["pg_basebackup_tbls_mapping"] is True

        # Simulate the presence of pg_basebackup 9.5.1 and no Pg
        backup_manager.server.streaming.server_major_version = None
        find_command.reset_mock()
        find_command.return_value.out = "pg_basebackup 9.5.1"
        remote = backup_manager.executor.fetch_remote_status()
        assert remote["pg_basebackup_installed"] is True
        assert remote["pg_basebackup_path"] == "/fake/path"
        assert remote["pg_basebackup_version"] == "9.5.1"
        assert remote["pg_basebackup_compatible"] is None
        assert remote["pg_basebackup_tbls_mapping"] is True

        # Simulate the presence of pg_basebackup 9.3.3 and Pg 9.5
        backup_manager.server.streaming.server_major_version = "9.5"
        find_command.reset_mock()
        find_command.return_value.out = "pg_basebackup 9.3.3"
        remote = backup_manager.executor.fetch_remote_status()
        assert remote["pg_basebackup_installed"] is True
        assert remote["pg_basebackup_path"] == "/fake/path"
        assert remote["pg_basebackup_version"] == "9.3.3"
        assert remote["pg_basebackup_compatible"] is False
        assert remote["pg_basebackup_tbls_mapping"] is False

    @patch("barman.backup_executor.PgBaseBackup")
    @patch("barman.backup_executor.PostgresBackupExecutor.fetch_remote_status")
    def test_backup_copy(self, remote_mock, pg_basebackup_mock, tmpdir, capsys):
        """
        Test backup folder structure

        :param remote_mock: mock for the fetch_remote_status method
        :param pg_basebackup_mock: mock for the PgBaseBackup object
        :param tmpdir: pytest temp directory
        """
        backup_manager = build_backup_manager(
            global_conf={
                "barman_home": tmpdir.mkdir("home").strpath,
                "backup_method": "postgres",
            }
        )
        # simulate a old version of pg_basebackup
        # not supporting bandwidth_limit
        remote_mock.return_value = {
            "pg_basebackup_version": "9.2",
            "pg_basebackup_path": "/fake/path",
            "pg_basebackup_bwlimit": False,
        }
        server_mock = backup_manager.server
        streaming_mock = server_mock.streaming
        server_mock.config.bandwidth_limit = 1
        streaming_mock.get_connection_string.return_value = "fake=connstring"
        streaming_mock.conn_parameters = {
            "host": "fakeHost",
            "port": "fakePort",
            "user": "fakeUser",
        }
        backup_info = build_test_backup_info(
            server=backup_manager.server, backup_id="fake_backup_id"
        )
        backup_manager.executor.backup_copy(backup_info)
        out, err = capsys.readouterr()
        assert out == ""
        assert err == ""
        # check that the bwlimit option have been ignored
        assert pg_basebackup_mock.mock_calls == [
            mock.call.make_logging_handler(logging.INFO),
            mock.call(
                connection=mock.ANY,
                version="9.2",
                app_name="barman_streaming_backup",
                destination=mock.ANY,
                command="/fake/path",
                tbs_mapping=mock.ANY,
                bwlimit=None,
                immediate=False,
                retry_times=0,
                retry_sleep=30,
                retry_handler=mock.ANY,
                path=mock.ANY,
                compression=None,
                err_handler=mock.ANY,
                out_handler=mock.ANY,
            ),
            mock.call()(),
        ]

        # Check with newer version
        remote_mock.reset_mock()
        pg_basebackup_mock.reset_mock()
        backup_manager.executor._remote_status = None
        remote_mock.return_value = {
            "pg_basebackup_version": "9.5",
            "pg_basebackup_path": "/fake/path",
            "pg_basebackup_bwlimit": True,
        }
        backup_manager.executor.config.immediate_checkpoint = True
        backup_manager.executor.config.streaming_conninfo = "fake=connstring"
        backup_manager.executor.backup_copy(backup_info)
        out, err = capsys.readouterr()
        assert out == ""
        assert err == ""
        # check that the bwlimit option have been passed to the test call
        assert pg_basebackup_mock.mock_calls == [
            mock.call.make_logging_handler(logging.INFO),
            mock.call(
                connection=mock.ANY,
                version="9.5",
                app_name="barman_streaming_backup",
                destination=mock.ANY,
                command="/fake/path",
                tbs_mapping=mock.ANY,
                bwlimit=1,
                immediate=True,
                retry_times=0,
                retry_sleep=30,
                retry_handler=mock.ANY,
                path=mock.ANY,
                compression=None,
                err_handler=mock.ANY,
                out_handler=mock.ANY,
            ),
            mock.call()(),
        ]

        # Check with a config file outside the data directory
        remote_mock.reset_mock()
        pg_basebackup_mock.reset_mock()
        backup_info.ident_file = "/pg/pg_ident.conf"
        backup_manager.executor.backup_copy(backup_info)
        out, err = capsys.readouterr()
        assert out == ""
        assert (
            err.strip() == "WARNING: pg_basebackup does not copy "
            "the PostgreSQL configuration files that "
            "reside outside PGDATA. "
            "Please manually backup the following files:"
            "\n\t/pg/pg_ident.conf"
        )
        # check that the bwlimit option have been passed to the test call
        assert pg_basebackup_mock.mock_calls == [
            mock.call.make_logging_handler(logging.INFO),
            mock.call(
                connection=mock.ANY,
                version="9.5",
                app_name="barman_streaming_backup",
                destination=mock.ANY,
                command="/fake/path",
                tbs_mapping=mock.ANY,
                bwlimit=1,
                immediate=True,
                retry_times=0,
                retry_sleep=30,
                retry_handler=mock.ANY,
                path=mock.ANY,
                compression=None,
                err_handler=mock.ANY,
                out_handler=mock.ANY,
            ),
            mock.call()(),
        ]

        # Check with a config file outside the data directory and
        # external_configurations backup option
        remote_mock.reset_mock()
        pg_basebackup_mock.reset_mock()
        backup_manager.config.backup_options.add(BackupOptions.EXTERNAL_CONFIGURATION)
        backup_manager.executor.backup_copy(backup_info)
        out, err = capsys.readouterr()
        assert out == ""
        assert err == ""
        # check that the bwlimit option have been passed to the test call
        assert pg_basebackup_mock.mock_calls == [
            mock.call.make_logging_handler(logging.INFO),
            mock.call(
                connection=mock.ANY,
                version="9.5",
                app_name="barman_streaming_backup",
                destination=mock.ANY,
                command="/fake/path",
                tbs_mapping=mock.ANY,
                bwlimit=1,
                immediate=True,
                retry_times=0,
                retry_sleep=30,
                retry_handler=mock.ANY,
                path=mock.ANY,
                compression=None,
                err_handler=mock.ANY,
                out_handler=mock.ANY,
            ),
            mock.call()(),
        ]

        # Raise a test CommandFailedException and expect it to be wrapped
        # inside a DataTransferFailure exception
        remote_mock.reset_mock()
        pg_basebackup_mock.reset_mock()
        pg_basebackup_mock.return_value.side_effect = CommandFailedException(
            dict(ret="ret", out="out", err="err")
        )
        with pytest.raises(DataTransferFailure):
            backup_manager.executor.backup_copy(backup_info)

    def test_postgres_start_backup(self):
        """
        Test concurrent backup using pg_basebackup
        """
        # Test: start concurrent backup
        backup_manager = build_backup_manager(global_conf={"backup_method": "postgres"})
        # Mock server.get_pg_setting('data_directory') call
        postgres_mock = backup_manager.server.postgres
        postgres_mock.get_setting.side_effect = [
            "/test/fake_data_dir",
        ]
        # Mock server.get_pg_configuration_files() call
        postgres_mock.get_configuration_files.return_value = dict(
            config_file="/etc/postgresql.conf",
            hba_file="/pg/pg_hba.conf",
            ident_file="/pg/pg_ident.conf",
        )
        # Mock server.get_pg_tablespaces() call
        tablespaces = [Tablespace._make(("test_tbs", 1234, "/tbs/test"))]
        postgres_mock.get_tablespaces.return_value = tablespaces
        # this is a postgres 9.5
        postgres_mock.server_version = 90500

        # Mock call to new api method
        start_time = datetime.datetime.now()
        postgres_mock.current_xlog_info = {
            "location": "A257/44B4C0D8",
            "timestamp": start_time,
        }
        # Build a test empty backup info
        backup_info = LocalBackupInfo(
            server=backup_manager.server, backup_id="fake_id2"
        )

        backup_manager.executor.strategy.start_backup(backup_info)

        # Check that all the values are correctly saved inside the BackupInfo
        assert backup_info.pgdata == "/test/fake_data_dir"
        assert backup_info.config_file == "/etc/postgresql.conf"
        assert backup_info.hba_file == "/pg/pg_hba.conf"
        assert backup_info.ident_file == "/pg/pg_ident.conf"
        assert backup_info.tablespaces == tablespaces
        assert backup_info.status == "STARTED"
        assert backup_info.timeline is None
        assert backup_info.begin_xlog == "A257/44B4C0D8"
        assert backup_info.begin_wal is None
        assert backup_info.begin_offset is None
        assert backup_info.begin_time == start_time

    def test_backup_compression_gzip(self):
        """
        Checks that a backup_compression object is created if the backup_compression
        option is set.
        """
        # GIVEN a server with backup_method postgres and backup_compression gzip
        server = build_mocked_server(
            global_conf={"backup_method": "postgres", "backup_compression": "gzip"}
        )
        # WHEN a PostgresBackupExecutor is created
        executor = PostgresBackupExecutor(server.backup_manager)
        # THEN a PgBaseBackupCompression is created with type == "gzip"
        assert executor.backup_compression.config.type == "gzip"

    def test_no_backup_compression(self):
        """
        Checks that backup_compression is None if the backup_compression option is
        not set.
        """
        # GIVEN a server with backup_method postgres and no backup_compression
        server = build_mocked_server(global_conf={"backup_method": "postgres"})
        # WHEN a PostgresBackupExecutor is created
        executor = PostgresBackupExecutor(server.backup_manager)
        # THEN the backup_compression attribute of the executor is None
        assert executor.backup_compression is None

    def test_validate_config_bandwidth_limit_closes_server_conn(self):
        """
        Checks that the server connection required to verify bwlimit support is
        not left open after creating the PostgresBackupExecutor.
        """
        # GIVEN a server with backup_method postgres and bandwidth_limit
        server = build_mocked_server(
            global_conf={"backup_method": "postgres", "bandwidth_limit": "1000"}
        )
        # WHEN a PostgresBackupExecutor is created
        PostgresBackupExecutor(server.backup_manager)
        # THEN the server's close method was called
        server.close.assert_called_once()

    @patch("barman.compression.PgBaseBackupCompression")
    def test_validate_config_compression(self, mock_pgbb_compression):
        """
        Checks that the validate_config method validates compression options.
        We do not care about the details of the validation here, we only care
        that it is called.
        """
        # GIVEN a server with backup_method postgres and backup_compression gzip
        server = build_mocked_server(
            global_conf={"backup_method": "postgres", "backup_compression": "gzip"}
        )
        # WITH a valid compression configuration
        mock_pgbb_compression.return_value.validate.return_value = []
        # AND the server_version property is mocked
        mock_server_version = PropertyMock()
        type(server.postgres).server_version = mock_server_version
        # AND a mock object which is used to validate call ordering
        call_validation_mock = Mock()
        call_validation_mock.attach_mock(server.close, "mock_close")
        call_validation_mock.mock_server_version = mock_server_version

        # WHEN a PostgresBackupExecutor is created
        PostgresBackupExecutor(server.backup_manager)

        # THEN the validate method of the executor's PgBaseBackupCompression object
        # is called
        mock_pgbb_compression.return_value.validate.assert_called_once()
        # AND the server config message list has no errors
        assert len(server.config.msg_list) == 0
        # AND the server's close method was called after the call to retreive the
        # server version
        server.close.assert_called_once()
        mock_server_version.assert_called_once()
        call_validation_mock.assert_has_calls(
            [mock.call.mock_server_version, mock.call.mock_close]
        )

    def test_postgres_connection_error_validating_compression(self, caplog):
        """
        Checks that a PostgresConnectionError raised during compression
        validation does not cause a server to be disabled.
        """
        # GIVEN a server with backup_method postgres and backup_compression gzip
        server = build_mocked_server(
            global_conf={"backup_method": "postgres", "backup_compression": "gzip"}
        )
        # WHEN a PostgresConnectionError is thrown when determining the server version
        # during the creation of a PostgresBackupExecutor
        type(server.postgres).server_version = PropertyMock(
            side_effect=PostgresConnectionError
        )
        PostgresBackupExecutor(server.backup_manager)
        # THEN the server config message list has no errors
        assert len(server.config.msg_list) == 0
        # AND the expected message is logged
        assert (
            "Could not validate compression due to a problem with the PostgreSQL "
            "connection"
        ) in caplog.text

    @pytest.mark.parametrize(
        ("primary_conninfo", "err_line", "expected_wal_switch"),
        (
            # No primary_conninfo so we do not expect a WAL switch
            (None, "regular stderr log", False),
            (None, "waiting for required WAL segments to be archived", False),
            # primary_conninfo is set but the log line should not trigger a WAL switch
            ("db=primary", "regular stderr log", False),
            # primary_conninfo is set and the log line tells us a WAL switch is
            # required
            ("db=primary", "waiting for required WAL segments to be archived", True),
        ),
    )
    def test_err_handler(self, primary_conninfo, err_line, expected_wal_switch, caplog):
        """Verify behaviour of err_handler."""
        # GIVEN a server with backup_method postgres
        # AND the specified primary_conninfo
        server = build_mocked_server(
            global_conf={"backup_method": "postgres"},
            main_conf={"primary_conninfo": primary_conninfo},
        )
        # AND a PostgresBackupExecutor
        executor = PostgresBackupExecutor(server.backup_manager)
        # AND the err handler for the PgBaseBackup command
        err_handler = executor._err_handler
        # AND a log level of INFO
        caplog.set_level(logging.INFO)

        # WHEN the handler is called with the specified error line
        err_handler(err_line)

        # THEN the error line is logged at INFO level
        assert err_line in caplog.text

        # AND if we expected switch_wal to have been called it is called on the primary
        if expected_wal_switch:
            server.postgres.switch_wal.assert_called_once()


class TestSnapshotBackupExecutor(object):
    """
    Verifies behaviour of the SnapshotBackupExecutor class.
    """

    @pytest.fixture
    def core_snapshot_options(self):
        return {
            "backup_method": "snapshot",
            "snapshot_disks": "disk_name",
            "snapshot_instance": "instance_name",
            "snapshot_provider": "gcp",
            "snapshot_zone": "zone_name",
        }

    @pytest.mark.parametrize("additional_options", ({}, {"reuse_backup": "off"}))
    @patch("barman.backup_executor.get_snapshot_interface_from_server_config")
    def test_snapshot_backup_executor_init(
        self, _mock_get_snapshot_interface, core_snapshot_options, additional_options
    ):
        """
        Verify the executor can be initialised given the correct options.
        """
        # GIVEN a server with the supplied config options and backup_method "snapshot"
        core_snapshot_options.update(additional_options)
        server = build_mocked_server(main_conf=core_snapshot_options)

        # WHEN the backup executor is initialised
        SnapshotBackupExecutor(server.backup_manager)

        # THEN the message list is empty
        assert server.config.msg_list == []

        # AND the server is not disabled
        assert not server.config.disabled

    @patch("barman.backup_executor.get_snapshot_interface_from_server_config")
    def test_snapshot_backup_executor_init_bad_snapshot_interface(
        self, mock_get_snapshot_interface, core_snapshot_options
    ):
        """
        Verify the constructor fails if the snapshot interface cannot be created.
        """
        # GIVEN a server with backup_method "snapshot"
        server = build_mocked_server(main_conf=core_snapshot_options)

        # WHEN the snapshot interface raises an exception when building the executor
        mock_get_snapshot_interface.side_effect = Exception("nope")
        SnapshotBackupExecutor(server.backup_manager)

        # THEN an error is added to the server's message list
        assert (
            "Error initialising snapshot provider gcp: nope" in server.config.msg_list
        )
        # AND the server is disabled
        assert server.config.disabled

    @pytest.mark.parametrize(
        "additional_options",
        (
            {"reuse_backup": "copy"},
            {"reuse_backup": "link"},
            {
                "backup_compression": "gzip",
                "bandwidth_limit": 56,
                "network_compression": True,
                "tablespace_bandwidth_limit": 56,
            },
        ),
    )
    @patch("barman.backup_executor.get_snapshot_interface_from_server_config")
    def test_snapshot_backup_executor_init_unexpected_options(
        self,
        _mock_get_snapshot_interface,
        core_snapshot_options,
        additional_options,
    ):
        """
        Verify the constructor fails if disallowed combinations of options are provided.
        """
        # GIVEN a server with the supplied config options and backup_method "snapshot"
        core_snapshot_options.update(additional_options)
        server = build_mocked_server(main_conf=core_snapshot_options)

        # WHEN the backup executor is initialised
        SnapshotBackupExecutor(server.backup_manager)

        # THEN the expected errors are present in the server's message list
        for option in additional_options:
            assert (
                "{} option is not supported by snapshot backup_method".format(option)
                in server.config.msg_list
            )

        # AND the number of messages matches the number of expected errors
        assert len(server.config.msg_list) == len(additional_options)

        # AND the server is disabled
        assert server.config.disabled

    @pytest.mark.parametrize(
        "missing_option",
        ("snapshot_disks", "snapshot_instance", "snapshot_provider"),
    )
    @patch("barman.backup_executor.get_snapshot_interface_from_server_config")
    def test_snapshot_backup_executor_init_missing_options(
        self,
        _mock_get_snapshot_interface,
        core_snapshot_options,
        missing_option,
    ):
        """
        Verify the constructor fails if disallowed combinations of options are provided.
        """
        # GIVEN a server with a missing snapshot option and backup_method "snapshot"
        del core_snapshot_options[missing_option]
        server = build_mocked_server(main_conf=core_snapshot_options)

        # WHEN the backup executor is initialised
        SnapshotBackupExecutor(server.backup_manager)

        # THEN the expected error is present in the server's message list
        assert (
            "{} option is required by snapshot backup_method".format(missing_option)
            in server.config.msg_list
        )

        # AND it is the only error present
        assert len(server.config.msg_list) == 1

        # AND the server is disabled
        assert server.config.disabled

    def test_add_mount_data_to_volume_metadata(self):
        """Verify that mount data is added to volume metadata when it is returned."""

        # GIVEN volumes which are mounted
        def mock_resolve_mounted_volume(mock_volume, mount_point, mount_options, _cmd):
            mock_volume.mount_point = mount_point
            mock_volume.mount_options = mount_options

        mock_volumes = {
            "disk1": mock.Mock(),
            "disk2": mock.Mock(),
        }
        mock_volumes["disk1"].resolve_mounted_volume.side_effect = partial(
            mock_resolve_mounted_volume,
            mock_volumes["disk1"],
            "/opt/mount1",
            "rw,noatime",
        )
        mock_volumes["disk2"].resolve_mounted_volume.side_effect = partial(
            mock_resolve_mounted_volume,
            mock_volumes["disk2"],
            "/opt/mount2",
            "ro",
        )

        mock_cmd = mock.Mock()

        # WHEN add_mount_data_to_volume_metadata is called
        SnapshotBackupExecutor.add_mount_data_to_volume_metadata(mock_volumes, mock_cmd)

        # THEN the backup_info is enhanced with the mount point and mount options
        # for each device
        assert mock_volumes["disk1"].mount_point == "/opt/mount1"
        assert mock_volumes["disk1"].mount_options == "rw,noatime"
        assert mock_volumes["disk2"].mount_point == "/opt/mount2"
        assert mock_volumes["disk2"].mount_options == "ro"

    @patch("barman.backup_executor.get_snapshot_interface_from_server_config")
    @patch(
        "barman.backup_executor.SnapshotBackupExecutor.add_mount_data_to_volume_metadata"
    )
    @patch("barman.backup_executor.UnixRemoteCommand")
    @patch("barman.backup_executor.UnixLocalCommand")
    def test_backup_copy(
        self,
        mock_unix_local_command,
        mock_unix_remote_command,
        mock_add_mount_data_to_volume_metadata,
        mock_get_snapshot_interface,
        core_snapshot_options,
    ):
        """
        Verifies backup_copy function creates a backup via the snapshot interface and
        retrieves mount point information.
        """
        # GIVEN a backup executor for a snapshot backup
        server = build_mocked_server(main_conf=core_snapshot_options)
        executor = SnapshotBackupExecutor(server.backup_manager)
        # AND backup_info for a new backup
        backup_info = build_test_backup_info()
        # AND a snapshot interface which returns mock volume metadata
        mock_snapshot_interface = mock_get_snapshot_interface.return_value
        mock_volume_metadata = mock_snapshot_interface.get_attached_volumes.return_value

        # WHEN backup_copy is called
        executor.backup_copy(backup_info)

        # THEN the data directory was created
        mock_unix_local_command.return_value.create_dir_if_not_exists.assert_called_once_with(
            backup_info.get_data_directory()
        )
        # AND the snapshot interface was asked for volume metadata from the
        # expected disks
        mock_snapshot_interface.get_attached_volumes.assert_called_once_with(
            core_snapshot_options["snapshot_instance"],
            [core_snapshot_options["snapshot_disks"]],
        )
        # AND the snapshot interface is used to take a snapshot backup with the
        # expected args
        mock_get_snapshot_interface.return_value.take_snapshot_backup.assert_called_once_with(
            backup_info,
            core_snapshot_options["snapshot_instance"],
            mock_volume_metadata,
        )
        # AND add_mount_data_to_volume_metadata was called
        mock_add_mount_data_to_volume_metadata.assert_called_once_with(
            mock_volume_metadata, mock_unix_remote_command.return_value
        )

    @patch("barman.backup_executor.get_snapshot_interface_from_server_config")
    @patch(
        "barman.backup_executor.SnapshotBackupExecutor.add_mount_data_to_volume_metadata"
    )
    @patch("barman.backup_executor.UnixRemoteCommand")
    @patch("barman.backup_executor.UnixLocalCommand")
    def test_backup_copy_records_copy_stats(
        self,
        _mock_unix_local_command,
        _mock_unix_remote_command,
        _mock_add_mount_data_to_volume_metadata,
        _mock_get_snapshot_interface,
        core_snapshot_options,
    ):
        """Verifies backup_copy function adds copy stats to backup_info."""
        # GIVEN a backup executor for a snapshot backup
        server = build_mocked_server(main_conf=core_snapshot_options)
        executor = SnapshotBackupExecutor(server.backup_manager)
        # AND backup_info for a new backup
        backup_info = build_test_backup_info()

        # WHEN backup_copy is called
        executor.backup_copy(backup_info)

        # THEN the copy stats are added to the backup_info
        assert backup_info.copy_stats
        assert backup_info.copy_stats["copy_time"]
        assert backup_info.copy_stats["total_time"]

    @pytest.mark.parametrize(
        (
            "expected_missing_disks",
            "expected_unmounted_disks",
            "expected_mounted_disks",
        ),
        [
            # Cases where all disks are attached and mounted
            ([], [], ["disk1"]),
            ([], [], ["disk1", "disk2"]),
            # Cases where a disk is attached but not mounted
            ([], ["disk1"], []),
            ([], ["disk1"], ["disk2"]),
            # Cases with one or more missing disks
            (["disk1"], [], []),
            (["disk1"], [], ["disk2"]),
            (["disk1"], ["disk2", "disk3"], ["disk4", "disk5", "disk6"]),
            (["disk1", "disk2"], ["disk3", "disk4"], ["disk5", "disk6"]),
        ],
    )
    @patch("barman.backup_executor.get_snapshot_interface_from_server_config")
    def test_find_missing_and_unmounted_disks(
        self,
        mock_get_snapshot_interface,
        expected_missing_disks,
        expected_unmounted_disks,
        expected_mounted_disks,
    ):
        """
        Verifies missing and unmounted disks are correctly determined from the attached
        and mounted devices.
        """
        # GIVEN the specified attached and mounted disks are all returned by the
        # get_attached_volumes function
        mock_get_attached_volumes = (
            mock_get_snapshot_interface.return_value.get_attached_volumes
        )
        mock_get_attached_volumes.return_value = dict(
            (disk, mock.Mock(mount_point=None, mount_options=None))
            for disk in expected_unmounted_disks + expected_mounted_disks
        )
        # AND the specified mounted disks are returned by resolve_mounted_volume
        # while the attached(but not mounted) disks are not found
        cmd = mock.Mock()

        def mock_resolve_mounted_volume(mock_volume, disk_name, _cmd):
            mock_volume.mount_point = "/opt/" + disk_name
            mock_volume.mount_options = "rw"

        for disk in expected_mounted_disks:
            mock_volume = mock_get_attached_volumes.return_value[disk]
            mock_volume.resolve_mounted_volume.side_effect = partial(
                mock_resolve_mounted_volume, mock_volume, disk
            )

        # WHEN find_missing_and_unmounted_disks is called for all expected disksts
        (
            missing_disks,
            unmounted_disks,
        ) = SnapshotBackupExecutor.find_missing_and_unmounted_disks(
            cmd,
            mock_get_snapshot_interface.return_value,
            "instance1",
            expected_missing_disks + expected_unmounted_disks + expected_mounted_disks,
        )

        # THEN the returned list of missing disks matches those not found amongst the
        # attached devices
        assert missing_disks == expected_missing_disks
        # AND the returned list of unmounted disks matches those not found by findmnt
        assert unmounted_disks == expected_unmounted_disks
        # AND none of the expected mounted disks are present in missing or unmounted
        # disks
        assert not any(disk in missing_disks for disk in expected_mounted_disks)
        assert not any(disk in unmounted_disks for disk in expected_mounted_disks)

    @patch("barman.backup_executor.get_snapshot_interface_from_server_config")
    def test_find_missing_and_unmounted_disks_resolve_exception(
        self, mock_get_snapshot_interface, caplog
    ):
        """
        Verify that, when a SnapshotBackupException is raised during resolution of
        mounted volumes, the disk is considered unmounted.
        """
        # GIVEN the specified attached and mounted disks are all returned by the
        # get_attached_volumes function
        mock_get_attached_volumes = (
            mock_get_snapshot_interface.return_value.get_attached_volumes
        )
        mock_get_attached_volumes.return_value = {
            "disk0": mock.Mock(mount_point=None, mount_options=None)
        }
        # AND resolve_mounted_volume raises a SnapshotBackupException
        cmd = mock.Mock()
        mock_volume = mock_get_attached_volumes.return_value["disk0"]
        mock_volume.resolve_mounted_volume.side_effect = SnapshotBackupException(
            "test-message"
        )

        # WHEN find_missing_and_unmounted_disks is called
        (
            missing_disks,
            unmounted_disks,
        ) = SnapshotBackupExecutor.find_missing_and_unmounted_disks(
            cmd, mock_get_snapshot_interface.return_value, "instance0", ["disk0"]
        )

        # THEN the disk is not present in missing_disks
        assert len(missing_disks) == 0
        # AND the disk is present in unmounted_disks
        assert "disk0" in unmounted_disks
        # AND the exception message was logged
        assert "test-message" in caplog.text

    @patch("barman.backup_executor.ExternalBackupExecutor.check")
    def test_check_skipped_if_server_disabled(
        self, mock_parent_check_fun, core_snapshot_options
    ):
        """
        Verifies the snapshot-specific checks are not started if the server is disabled.
        """
        # GIVEN a backup executor for a snapshot backup
        server = build_mocked_server(main_conf=core_snapshot_options)
        executor = SnapshotBackupExecutor(server.backup_manager)
        # AND the server is disabled
        server.config.disabled = True

        # WHEN check is called on the backup executor
        mock_check_strategy = mock.Mock()
        executor.check(mock_check_strategy)

        # THEN the check function of the parent class is called
        mock_parent_check_fun.assert_called_once_with(mock_check_strategy)
        # AND no additional checks are initiated
        mock_check_strategy.init_check.assert_not_called()

    @patch("barman.backup_executor.unix_command_factory")
    @patch(
        "barman.backup_executor.SnapshotBackupExecutor.find_missing_and_unmounted_disks"
    )
    @patch("barman.backup_executor.get_snapshot_interface_from_server_config")
    @patch("barman.backup_executor.ExternalBackupExecutor.check")
    def test_check_success(
        self,
        mock_parent_check_fun,
        mock_get_snapshot_interface,
        mock_find_missing_and_unmounted_disks,
        _mock_unix_command_factory,
        core_snapshot_options,
    ):
        """
        Verifies all snapshot-specific checks pass when the instance exists and the
        disks are attached and mounted.
        """
        # GIVEN a backup executor for a snapshot backup
        server = build_mocked_server(main_conf=core_snapshot_options)
        executor = SnapshotBackupExecutor(server.backup_manager)
        # AND an instance to snapshot which exists
        mock_get_snapshot_interface.return_value.instance_exists.return_value = True
        # AND all disks are attached and mounted
        mock_find_missing_and_unmounted_disks.return_value = [], []

        # WHEN check is called on the backup executor
        check_strategy = CheckOutputStrategy()
        executor.check(check_strategy)

        # THEN the check function of the parent class is called
        mock_parent_check_fun.assert_called_once_with(check_strategy)
        # AND three checks run in total
        assert len(check_strategy.check_result) == 3
        # AND each snapshot-specific check passes
        for check in (
            "snapshot instance exists",
            "snapshot disks attached to instance",
            "snapshot disks mounted on instance",
        ):
            result = [
                result
                for result in check_strategy.check_result
                if result.check == check
            ]
            assert len(result) == 1
            assert result[0].status

    @pytest.mark.parametrize(
        (
            "check_msg",
            "instance_exists",
            "missing_disks",
            "unmounted_disks",
            "expected_error_msg",
        ),
        [
            (
                "snapshot instance exists",
                False,
                [],
                [],
                "cannot find compute instance {snapshot_instance}",
            ),
            (
                "snapshot disks attached to instance",
                True,
                ["disk1", "disk2"],
                [],
                "cannot find snapshot disks attached to instance {snapshot_instance}: disk1, disk2",
            ),
            (
                "snapshot disks mounted on instance",
                True,
                [],
                ["disk1", "disk2"],
                "cannot find snapshot disks mounted on instance {snapshot_instance}: disk1, disk2",
            ),
        ],
    )
    @patch("barman.backup_executor.unix_command_factory")
    @patch(
        "barman.backup_executor.SnapshotBackupExecutor.find_missing_and_unmounted_disks"
    )
    @patch("barman.backup_executor.get_snapshot_interface_from_server_config")
    def test_check_failure(
        self,
        mock_get_snapshot_interface,
        mock_find_missing_and_unmounted_disks,
        _mock_unix_command_factory,
        check_msg,
        instance_exists,
        missing_disks,
        unmounted_disks,
        expected_error_msg,
        core_snapshot_options,
        capsys,
    ):
        """
        Verifies the scenarios which can cause the snapshot-specific checks to fail.
        """
        # GIVEN a backup executor for a snapshot backup
        server = build_mocked_server(main_conf=core_snapshot_options)
        executor = SnapshotBackupExecutor(server.backup_manager)
        # AND the specified instance existence state
        mock_get_snapshot_interface.return_value.instance_exists.return_value = (
            instance_exists
        )
        # AND the specified attached / mounted disk state
        mock_find_missing_and_unmounted_disks.return_value = (
            missing_disks,
            unmounted_disks,
        )

        # WHEN check is called on the backup executor
        check_strategy = CheckOutputStrategy()
        executor.check(check_strategy)

        # THEN the expected check runs
        check_result = [r for r in check_strategy.check_result if r.check == check_msg][
            0
        ]
        # AND the check result was a failure
        assert not check_result.status
        # AND the exepcted hint was written to the output
        out, _err = capsys.readouterr()
        assert expected_error_msg.format(**core_snapshot_options) in out
