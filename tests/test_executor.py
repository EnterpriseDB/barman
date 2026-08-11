# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2026
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
import logging
import os
import queue
from functools import partial

import mock
import pytest
from dateutil import tz
from mock import Mock, PropertyMock, call, patch
from testing_helpers import (
    build_backup_manager,
    build_mocked_server,
    build_test_backup_info,
)

from barman.backup_executor import (
    CloudBackupExecutor,
    CloudPostgresBackupExecutor,
    ExclusiveBackupStrategy,
    PostgresBackupExecutor,
    PostgresBackupStrategy,
    RsyncBackupExecutor,
    SnapshotBackupExecutor,
    _get_bandwidth_limit_in_bytes,
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
from barman.utils import total_seconds


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
        # mocks the keep-alive query
        backup_manager.server.postgres.send_heartbeat_query.return_value = True, None

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
            main_conf={"backup_options": BackupOptions.EXCLUSIVE_BACKUP},
            pg_version=170000,
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
            main_conf={"backup_options": BackupOptions.CONCURRENT_BACKUP},
            pg_version=170000,
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
            main_conf={"backup_options": BackupOptions.CONCURRENT_BACKUP},
            pg_version=90600,  # this is a postgres 9.6
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

    def test_backup_info_from_stop_location_uses_stop_info_timeline(self):
        """
        When stop_info already carries a timeline (e.g. as returned by the
        concurrent backup API), it must be used as-is and PostgreSQL must
        not be queried again for the current timeline.
        """
        # GIVEN a backup_info whose timeline was set from the backup_label
        # written at the start of the backup, and a PostgreSQL connection
        # that fails the test if queried for the current timeline
        mock_postgres = mock.Mock()
        type(mock_postgres).current_timeline = mock.PropertyMock(
            side_effect=AssertionError("current_timeline should not be queried")
        )
        strategy = PostgresBackupStrategy(mock_postgres, "test server")
        backup_info = build_test_backup_info(timeline=1)

        # WHEN the backup stops on a later timeline, as reported live by
        # PostgreSQL, and file_name/file_offset are not directly available
        stop_info = {
            "location": "0/12000090",
            "file_name": None,
            "file_offset": None,
            "timestamp": datetime.datetime(2015, 10, 26, 14, 38),
            "timeline": 2,
        }
        strategy._backup_info_from_stop_location(backup_info, stop_info)

        # THEN end_wal is computed using the timeline carried by stop_info,
        # not the stale one recorded in backup_info.timeline
        assert backup_info.end_wal == "000000020000000000000012"

    def test_backup_info_from_stop_location_fetches_current_timeline(self):
        """
        A backup taken from a standby can be stopped after the standby has
        followed the primary through a failover, landing on a newer timeline
        than the one recorded in the backup_label at the start of the
        backup. When stop_info carries no timeline (e.g. current_xlog_info
        on a standby), the live timeline must be fetched from PostgreSQL
        rather than falling back to the stale backup_info.timeline.
        """
        # GIVEN a backup_info whose timeline was set from the backup_label
        # written at the start of the backup
        mock_postgres = mock.Mock()
        mock_postgres.current_timeline = 2
        strategy = PostgresBackupStrategy(mock_postgres, "test server")
        backup_info = build_test_backup_info(timeline=1)

        # WHEN the backup stops without stop_info carrying a timeline
        stop_info = {
            "location": "0/12000090",
            "file_name": None,
            "file_offset": None,
            "timestamp": datetime.datetime(2015, 10, 26, 14, 38),
        }
        strategy._backup_info_from_stop_location(backup_info, stop_info)

        # THEN end_wal is computed using the live timeline fetched from
        # PostgreSQL, not the stale one recorded in backup_info.timeline
        assert backup_info.end_wal == "000000020000000000000012"

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
            main_conf={"backup_options": BackupOptions.CONCURRENT_BACKUP},
            pg_version=90600,  # This is a postgres 9.6
        )
        backup_manager = build_backup_manager(server=server)

        stop_time = datetime.datetime.now()
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

    @pytest.mark.parametrize(
        ("server_version", "expected_value"),
        [(160000, None), (170000, "on")],
    )
    def test__pg_get_metadata(self, server_version, expected_value):
        # Given a PostgreSQL connection of the specified version
        mock_postgres = mock.Mock()
        mock_postgres.server_version = server_version
        # Mock the get_setting("data_directory"), get_setting("data_checksums") and
        # get_setting("summarize_wal") calls, respectively
        mock_postgres.get_setting.side_effect = [
            "data_directory",
            "off",
            expected_value,
        ]

        # Mock postgres server.get_configuration_files() call
        mock_postgres.get_configuration_files.return_value = dict(
            config_file="/etc/postgresql.conf",
            hba_file="/pg/pg_hba.conf",
            ident_file="/pg/pg_ident.conf",
        )
        # Mock postgres server.get_tablespaces() call
        tablespaces = [Tablespace._make(("test_tbs", 1234, "/tbs/test"))]
        mock_postgres.get_tablespaces.return_value = tablespaces

        mock_postgres.current_size = 2048
        mock_postgres.xlog_segment_size = 16
        strategy = PostgresBackupStrategy(mock_postgres, "test server")
        backup_info = build_test_backup_info()

        # default values from build_test_backup_info() to verify that the values
        # set in this method were changed inplace after calling the method
        assert backup_info.pgdata == "/pgdata/location"
        assert backup_info.version == 90302
        assert backup_info.xlog_segment_size == 16777216
        assert backup_info.tablespaces == [
            Tablespace(name="tbs1", oid=16387, location="/fake/location"),
            Tablespace(name="tbs2", oid=16405, location="/another/location"),
        ]
        assert backup_info.summarize_wal is None
        assert backup_info.cluster_size == 2048

        strategy._pg_get_metadata(backup_info)

        mock_postgres.get_tablespaces.assert_called_once()
        mock_postgres.get_configuration_files.assert_called_once()
        if mock_postgres.server_version < 170000:
            calls = [call("data_directory"), call("data_checksums")]
            mock_postgres.get_setting.assert_has_calls(calls, any_order=False)
            assert mock_postgres.get_setting.call_count == 2
            assert backup_info.summarize_wal is None
            assert backup_info.version == 160000
        else:
            calls = [
                call("data_directory"),
                call("data_checksums"),
                call("summarize_wal"),
            ]
            assert mock_postgres.get_setting.call_count == 3
            mock_postgres.get_setting.assert_has_calls(calls, any_order=False)
            assert backup_info.summarize_wal == "on"
            assert backup_info.version == 170000

        assert backup_info.pgdata == "data_directory"
        assert backup_info.xlog_segment_size == 16
        assert backup_info.tablespaces == tablespaces
        assert backup_info.cluster_size == 2048


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
        backup_manager.server.postgres.server_version = backup_info.version
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
                parent_backup_manifest_path=None,
                warehousepg_dbid=None,
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
                parent_backup_manifest_path=None,
                warehousepg_dbid=None,
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
                parent_backup_manifest_path=None,
                warehousepg_dbid=None,
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
                parent_backup_manifest_path=None,
                warehousepg_dbid=None,
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

        # Check incremental backups with Postgres 17 onward
        remote_mock.reset_mock()
        pg_basebackup_mock.reset_mock()
        pg_basebackup_mock.return_value.side_effect = None
        backup_manager.executor._remote_status = None
        remote_mock.return_value = {
            "pg_basebackup_version": "17",
            "pg_basebackup_path": "/fake/path",
            "pg_basebackup_bwlimit": True,
        }
        backup_manager.executor.config.immediate_checkpoint = True
        backup_manager.executor.config.streaming_conninfo = "fake=connstring"
        mock_parent_backup_info = Mock()
        mock_parent_backup_info.get_backup_manifest_path.return_value = "/SOME/MANIFEST"
        with patch("barman.infofile.LocalBackupInfo.get_parent_backup_info") as mock_gp:
            mock_gp.return_value = mock_parent_backup_info
            backup_manager.executor.backup_copy(backup_info)
        out, err = capsys.readouterr()
        assert out == ""
        assert err == ""
        # Check that expected parameter was passed to pg_basebackup to identify
        # the parent backup
        assert pg_basebackup_mock.mock_calls == [
            mock.call.make_logging_handler(logging.INFO),
            mock.call(
                connection=mock.ANY,
                version="17",
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
                parent_backup_manifest_path="/SOME/MANIFEST",
                warehousepg_dbid=None,
            ),
            mock.call()(),
        ]

    @patch("barman.backup_executor.PgBaseBackup")
    @patch("barman.backup_executor.PostgresBackupExecutor.fetch_remote_status")
    def test_backup_copy_with_warehousepg_dbid(
        self, remote_mock, pg_basebackup_mock, tmpdir, capsys
    ):
        """
        Test that warehousepg_dbid is correctly passed to PgBaseBackup.

        :param remote_mock: mock for the fetch_remote_status method
        :param pg_basebackup_mock: mock for the PgBaseBackup object
        :param tmpdir: pytest temp directory
        :param capsys: pytest capture output
        """
        backup_manager = build_backup_manager(
            global_conf={
                "barman_home": tmpdir.mkdir("home").strpath,
                "backup_method": "postgres",
                "warehousepg_dbid": "42",
            }
        )
        remote_mock.return_value = {
            "pg_basebackup_version": "14",
            "pg_basebackup_path": "/fake/path",
            "pg_basebackup_bwlimit": True,
        }
        server_mock = backup_manager.server
        streaming_mock = server_mock.streaming
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

        # Verify that warehousepg_dbid was passed to PgBaseBackup
        assert pg_basebackup_mock.mock_calls == [
            mock.call.make_logging_handler(logging.INFO),
            mock.call(
                connection=mock.ANY,
                version="14",
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
                parent_backup_manifest_path=None,
                warehousepg_dbid=42,
            ),
            mock.call()(),
        ]

    @patch("barman.backup_executor.PgBaseBackup")
    @patch("barman.backup_executor.PostgresBackupExecutor.fetch_remote_status")
    def test_backup_copy_without_warehousepg_dbid(
        self, remote_mock, pg_basebackup_mock, tmpdir, capsys
    ):
        """
        Test that warehousepg_dbid is None when not configured.

        :param remote_mock: mock for the fetch_remote_status method
        :param pg_basebackup_mock: mock for the PgBaseBackup object
        :param tmpdir: pytest temp directory
        :param capsys: pytest capture output
        """
        backup_manager = build_backup_manager(
            global_conf={
                "barman_home": tmpdir.mkdir("home").strpath,
                "backup_method": "postgres",
            }
        )
        remote_mock.return_value = {
            "pg_basebackup_version": "14",
            "pg_basebackup_path": "/fake/path",
            "pg_basebackup_bwlimit": True,
        }
        server_mock = backup_manager.server
        streaming_mock = server_mock.streaming
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

        # Verify that warehousepg_dbid was passed as None to PgBaseBackup
        assert pg_basebackup_mock.mock_calls == [
            mock.call.make_logging_handler(logging.INFO),
            mock.call(
                connection=mock.ANY,
                version="14",
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
                parent_backup_manifest_path=None,
                warehousepg_dbid=None,
            ),
            mock.call()(),
        ]

    def test_postgres_start_backup(self):
        """
        Test concurrent backup using pg_basebackup
        """
        # Test: start concurrent backup
        backup_manager = build_backup_manager(global_conf={"backup_method": "postgres"})
        # Mock the get_setting("data_directory") and get_setting("data_checksums") calls
        postgres_mock = backup_manager.server.postgres
        postgres_mock.get_setting.side_effect = [
            "/test/fake_data_dir",
            "off",
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

    @pytest.mark.parametrize("bandwidth_limit_supported", [True, False])
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_get_bandwidth_limit(self, _, bandwidth_limit_supported):
        """
        Test that the bandwidth limit is correctly retrieved based on
        whether the remote server supports it.
        """
        # GIVEN a CloudPostgresBackupExecutor with all relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor.config = mock.Mock(bandwidth_limit="100")
        # WHEN _get_bandwidth_limit is called with the relevant remote status
        remote_status = {"pg_basebackup_bwlimit": bandwidth_limit_supported}
        result = executor._get_bandwidth_limit(remote_status)
        # THEN it returns the expected value
        assert result == ("100" if bandwidth_limit_supported else None)


class TestCloudPostgresBackupExecutor(object):
    """
    Tests for the CloudPostgresBackupExecutor class.
    """

    @patch("barman.backup_executor.CloudPostgresBackupStrategy")
    @patch("barman.backup_executor.os.getpid", return_value=123)
    def test__init__(self, _, mock_strategy):
        """
        Test the construction of a CloudPostgresBackupExecutor
        """
        # GIVEN a server with the following configs
        server = build_mocked_server(
            main_conf={
                "backup_compression": "none",
                "cloud_staging_directory": "/tmp/barman",
            }
        )
        mock_cloud_interface = mock.Mock()
        server.get_backup_cloud_interface.return_value = mock_cloud_interface
        # WHEN a CloudPostgresBackupExecutor is created
        executor = CloudPostgresBackupExecutor(server.backup_manager)
        # THEN a CloudPostgresBackupStrategy is created with the expected parameters
        mock_strategy.assert_called_once_with(
            executor.server.postgres, executor.config.name, executor.backup_compression
        )
        assert executor.strategy == mock_strategy.return_value
        # AND the executor attributes are correctly set
        assert executor._cloud_staging_dir == "/tmp/barman/123"
        assert executor._pgdata_dest == "/tmp/barman/123/plain/data"
        assert executor._tarball_dest == "/tmp/barman/123/tarballs"
        assert executor._plain_dest == "/tmp/barman/123/plain"
        assert executor._cloud_interface == mock_cloud_interface
        assert executor._upload_controller is None
        assert isinstance(executor._ready_queue, queue.Queue)

    @patch("barman.backup_executor.os.kill")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_thread_wrapper(self, _, mock_kill):
        """
        Assert that ``_thread_wrapper`` correctly calls the target function with the
        provided arguments and that if an exception is raised, the exception is stored
        in ``_thread_exec`` and the event ``_thread_exec_event`` is set.
        """
        # GIVEN a CloudPostgresBackupExecutor instance
        executor = CloudPostgresBackupExecutor(None)
        executor._thread_exc = None
        executor._thread_exc_event = mock.Mock(is_set=lambda: False)
        executor._thread_exc_lock = mock.Mock(
            __enter__=mock.Mock(), __exit__=mock.Mock()
        )

        # Case 1: WHEN _thread_wrapper is called with a target that executes successfully
        target, args, kwargs = mock.Mock(), ("arg1", "arg2"), {"kwarg1": "value1"}
        executor._thread_wrapper(target, *args, **kwargs)
        # THEN the target function is called with the provided arguments
        target.assert_called_once_with("arg1", "arg2", kwarg1="value1")

        # Case 2: WHEN _thread_wrapper is called with a target that raises an exception
        exception = Exception("Thread error")
        target = mock.Mock(side_effect=exception)
        executor._thread_wrapper(target)
        # THEN the exception object is set in the executor using the lock
        executor._thread_exc_lock.__enter__.assert_called_once()
        executor._thread_exc = exception
        executor._thread_exc_lock.__exit__.assert_called_once()
        # AND the _thread_exc_event is set
        executor._thread_exc_event.set.assert_called_once()

    @patch("barman.backup_executor.threading.Thread")
    @patch("barman.backup_executor.shutil")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor._prepare_backup_destination"
    )
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._run_pg_basebackup")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._init_upload_controller")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._upload_ready_files")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._read_backup_label")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._save_backup_manifest")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._upload_backup_info")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_backup_copy(
        self,
        _,
        mock_upload_backup_info,
        mock_save_backup_manifest,
        mock_read_backup_label,
        mock_upload_ready_files,
        mock_init_upload_controller,
        mock_run_pg_basebackup,
        mock_prepare_backup_destination,
        mock_shutil,
        mock_thread,
    ):
        """
        Test that the ``backup_copy`` method performs all the expected steps
        in the correct order to properly manage the backup copy process.
        """
        # GIVEN a CloudPostgresBackupExecutor with relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor._cloud_staging_dir = "/tmp/barman"
        executor._plain_dest = "/tmp/barman/plain"
        executor._tarball_dest = "/tmp/barman/tarballs"
        executor._upload_controller = mock.Mock(statistics=lambda: {}, size=12345)
        executor._cloud_interface = mock.Mock()
        executor._thread_exc_event = mock.Mock(is_set=lambda: False)
        # Mock the threads
        mock_monitoring_thread, mock_fetching_thread = mock.Mock(), mock.Mock()
        mock_thread.side_effect = [mock_monitoring_thread, mock_fetching_thread]
        # WHEN backup_copy is called with a BackupInfo
        backup_info = mock.Mock()
        executor.backup_copy(backup_info)
        # THEN the plain and tarballs destinations are prepared correctly
        mock_prepare_backup_destination.assert_called_once_with(
            ["/tmp/barman/plain", "/tmp/barman/tarballs"]
        )
        # AND pg_basebackup is started
        mock_run_pg_basebackup.assert_called_once_with(backup_info)
        pg_basebackup = mock_run_pg_basebackup.return_value
        # AND monitoring of the staging area and fetching of ready files
        # are started in their dedicated threads
        mock_thread.assert_has_calls(
            [
                mock.call(
                    target=executor._thread_wrapper,
                    args=(
                        executor._monitor_staging_area,
                        pg_basebackup,
                    ),
                ),
                mock.call(
                    target=executor._thread_wrapper,
                    args=(
                        executor._fetch_ready_files,
                        pg_basebackup,
                    ),
                ),
            ]
        )
        mock_monitoring_thread.start.assert_called_once()
        mock_fetching_thread.start.assert_called_once()
        # AND the upload controller is initialized
        mock_init_upload_controller.assert_called_once_with(backup_info)
        # AND ready files upload is started
        mock_upload_ready_files.assert_called_once()
        # AND pg_basebackup termination is waited and threads joined
        mock_run_pg_basebackup.return_value.wait_exit.assert_called_once()
        mock_run_pg_basebackup.return_value.terminate.assert_not_called()
        mock_monitoring_thread.join.assert_called_once()
        mock_fetching_thread.join.assert_called_once()
        # AND the cloud_interface and upload_controller are closed
        executor._cloud_interface.close.assert_called_once()
        executor._upload_controller.close.assert_called_once()
        # AND the backup label is read and the backup info is uploaded
        mock_read_backup_label.assert_called_once_with(backup_info)
        mock_upload_backup_info.assert_called_once_with(backup_info)
        # AND the cloud staging directory is removed
        mock_shutil.rmtree.assert_called_once_with(
            executor._cloud_staging_dir, ignore_errors=True
        )
        # AND time and size stats are saved
        assert backup_info.copy_stats is not None
        assert backup_info.copy_stats["total_time"] == total_seconds(
            executor.copy_end_time - executor.copy_start_time
        )
        assert backup_info.set_attribute.call_args_list == [
            mock.call("size", executor._upload_controller.size),
            mock.call("deduplicated_size", executor._upload_controller.size),
        ]

    @patch("barman.backup_executor.threading.Thread")
    @patch("barman.backup_executor.shutil")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor._prepare_backup_destination"
    )
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._run_pg_basebackup")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._init_upload_controller")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._upload_ready_files")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._read_backup_label")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._save_backup_manifest")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._upload_backup_info")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_backup_copy_exception_is_present(
        self,
        _,
        mock_upload_backup_info,
        mock_save_backup_manifest,
        mock_read_backup_label,
        mock_upload_ready_files,
        mock_init_upload_controller,
        mock_run_pg_basebackup,
        mock_prepare_backup_destination,
        mock_shutil,
        mock_thread,
    ):
        """
        Test that if an exception is set in the executor during the backup copy
        process, it is re-raised by the method.
        """
        # GIVEN a CloudPostgresBackupExecutor with relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor._cloud_staging_dir = "/tmp/barman"
        executor._plain_dest = "/tmp/barman/plain"
        executor._tarball_dest = "/tmp/barman/tarballs"
        executor._upload_controller = mock.Mock(statistics=lambda: {}, size=12345)
        executor._cloud_interface = mock.Mock()
        executor._thread_exc_event = mock.Mock(is_set=lambda: True)
        executor._thread_exc = ValueError("Test exception")
        # Mock the threads
        mock_monitoring_thread, mock_fetching_thread = mock.Mock(), mock.Mock()
        mock_thread.side_effect = [mock_monitoring_thread, mock_fetching_thread]
        # WHEN backup_copy is called with a BackupInfo
        # THEN the exception set in the executor is raised
        backup_info = mock.Mock()
        with pytest.raises(ValueError):
            executor.backup_copy(backup_info)
        # AND pg_basebackup termination is waited and threads joined
        mock_run_pg_basebackup.return_value.terminate.assert_called_once()
        mock_monitoring_thread.join.assert_called_once()
        mock_fetching_thread.join.assert_called_once()

    @patch("barman.cloud.CloudUploadController")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_init_upload_controller(self, _, mock_upload_controller):
        """
        Test that the upload controller is correctly initialized with
        the expected parameters.
        """
        # GIVEN a CloudPostgresBackupExecutor with relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor._tarball_dest = "/tmp/barman/tarballs"
        executor.config = mock.Mock(
            bandwidth_limit=None,
            cloud_upload_max_archive_size=100 * 1024 * 1024 * 1024,  # 100GiB
            cloud_upload_min_chunk_size=5 * 1024 * 1024,  # 5MiB
        )
        executor.config.name = "test_server"
        executor._cloud_interface = mock.Mock(path="/my-bucket/backups")
        # WHEN _init_upload_controller is called with a BackupInfo
        key_prefix = "/my-bucket/backups/test_server/base/fake_backup_id"
        backup_info = mock.Mock(
            backup_id="fake_backup_id",
            get_basebackup_directory=lambda: key_prefix,
        )
        executor._init_upload_controller(backup_info)
        # THEN a CloudUploadController is created with the expected parameters
        mock_upload_controller.assert_called_once_with(
            cloud_interface=executor._cloud_interface,
            max_archive_size=100 * 1024 * 1024 * 1024,  # 100GiB
            key_prefix=key_prefix,
            compression=None,
            min_chunk_size=5 * 1024 * 1024,  # 5MiB
            max_bandwidth=None,
            staging_dir=executor._tarball_dest,
        )
        # AND the executor's _upload_controller attribute is set
        assert executor._upload_controller == mock_upload_controller.return_value

    @patch("barman.cloud.CloudUploadController")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_init_upload_controller_with_bandwidth_limit(
        self, _, mock_upload_controller
    ):
        """
        Test that the upload controller correctly converts and applies bandwidth_limit.
        """
        # GIVEN a CloudPostgresBackupExecutor with bandwidth_limit set
        executor = CloudPostgresBackupExecutor(None)
        executor._tarball_dest = "/tmp/barman/tarballs"
        executor.config = mock.Mock(
            bandwidth_limit=512,  # 512 kB/s
            cloud_upload_max_archive_size=100 * 1024 * 1024 * 1024,  # 100GiB
            cloud_upload_min_chunk_size=5 * 1024 * 1024,  # 5MiB
        )
        executor.config.name = "test_server"
        executor._cloud_interface = mock.Mock(path="/my-bucket/backups")
        # WHEN _init_upload_controller is called with a BackupInfo
        key_prefix = "/my-bucket/backups/test_server/base/fake_backup_id"
        backup_info = mock.Mock(
            backup_id="fake_backup_id", get_basebackup_directory=lambda: key_prefix
        )
        executor._init_upload_controller(backup_info)
        # THEN a CloudUploadController is created with converted bandwidth_limit (512 kB/s → 512000 B/s)
        mock_upload_controller.assert_called_once_with(
            cloud_interface=executor._cloud_interface,
            key_prefix=key_prefix,
            max_archive_size=100 * 1024 * 1024 * 1024,  # 100GiB
            compression=None,
            min_chunk_size=5 * 1024 * 1024,  # 5MiB
            max_bandwidth=512000,  # 512 kB/s converted to 512000 B/s
            staging_dir=executor._tarball_dest,
        )
        # AND the executor's _upload_controller attribute is set
        assert executor._upload_controller == mock_upload_controller.return_value

    @patch("barman.backup_executor.open")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_read_backup_label(self, _, mock_open):
        """
        Test that the backup label is read from the expected path and
        its content set in the BackupInfo.
        """
        # GIVEN a CloudPostgresBackupExecutor with relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor._pgdata_dest = "/tmp/barman/plain/data"
        # WHEN _read_backup_label is called
        backup_info = mock.Mock()
        executor._read_backup_label(backup_info)
        # THEN the backup_label file is opened in the expected path
        mock_open.assert_called_once_with("/tmp/barman/plain/data/backup_label", "r")
        # AND the contents are read and set in the BackupInfo
        backup_info.set_attribute.assert_called_once_with(
            "backup_label",
            mock_open.return_value.__enter__.return_value.read.return_value,
        )

    @patch("barman.backup_executor.shutil.copy2")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_save_backup_manifest(self, _, mock_copy2):
        """
        Test that the backup manifest is saved to the expected path.
        """
        # GIVEN a CloudPostgresBackupExecutor with relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor.server = mock.Mock(meta_directory="/path/to/meta")
        executor._pgdata_dest = "/tmp/barman/plain/data"
        # WHEN _save_backup_manifest is called with a BackupInfo
        backup_info = mock.Mock(backup_id="fake_backup_id")
        executor._save_backup_manifest(backup_info)
        # THEN the backup manifest is saved to the expected path
        mock_copy2.assert_called_once_with(
            "/tmp/barman/plain/data/backup_manifest",
            "/path/to/meta/fake_backup_id-backup_manifest",
        )

    @patch("barman.backup_executor.BytesIO")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_upload_backup_info(self, _, mock_bytes_io):
        """
        Test that the backup info is uploaded to the expected cloud path.
        """
        # GIVEN a CloudPostgresBackupExecutor with relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor._cloud_interface = mock.Mock()
        executor._upload_controller = mock.Mock(
            key_prefix="my-bucket/backups/test_server/base/fake_backup_id"
        )
        # WHEN _upload_backup_info is called with a BackupInfo
        backup_info = mock.Mock()
        executor._upload_backup_info(backup_info)
        # THEN the backup info is saved to a BytesIO object
        backup_info.save(file_obj=mock_bytes_io.return_value.__enter__.return_value)
        # AND the BytesIO object is uploaded to the expected cloud path
        mock_bytes_io.return_value.__enter__.return_value.seek.assert_called_once_with(
            0
        )
        executor._cloud_interface.upload_fileobj.assert_called_once_with(
            mock_bytes_io.return_value.__enter__.return_value,
            "my-bucket/backups/test_server/base/fake_backup_id/backup.info",
        )

    @patch("barman.backup_executor.CloudPostgresBackupExecutor._wait_copy_start")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.get_remote_status",
        return_value={
            "pg_basebackup_path": "/usr/pgsql-17/bin/pg_basebackup",
            "pg_basebackup_version": "17",
        },
    )
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._get_bandwidth_limit")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._get_tablespace_mapping")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor._get_parent_backup_manifest_path"
    )
    @patch("barman.backup_executor.PgBaseBackup")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_run_pg_basebackup(
        self,
        mock_init,
        mock_pg_basebackup,
        mock_get_parent_manifest,
        mock_get_tablespace_mapping,
        mock_get_bandwidth_limit,
        mock_get_remote_status,
        mock_wait_copy_start,
    ):
        """
        Test that :class:`PgBaseBackup` is correctly initialized and run.
        """
        # GIVEN a CloudPostgresBackupExecutor with all relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        pgdata_dest = "/tmp/barman/plain/data"
        executor._pgdata_dest = pgdata_dest
        server_path, streaming_conn = "/fake/server/path", mock.Mock()
        executor.server = mock.Mock(path=server_path, streaming=streaming_conn)
        executor.config = mock.Mock(
            backup_compression=None,
            cloud_staging_directory="/tmp/barman",
            streaming_backup_name="barman_cloud_postgres_backup",
            immediate_checkpoint=True,
        )
        # WHEN _run_pg_basebackup is called with a test BackupInfo
        backup_info = build_test_backup_info()
        result = executor._run_pg_basebackup(backup_info)
        # THEN it returns a PgBaseBackup instance
        assert result == mock_pg_basebackup.return_value
        # AND PgBaseBackup was initialized with the expected parameters
        mock_pg_basebackup.assert_called_once_with(
            wait=False,
            connection=streaming_conn,
            destination=pgdata_dest,
            command="/usr/pgsql-17/bin/pg_basebackup",
            version="17",
            app_name="barman_cloud_postgres_backup",
            no_sync=True,
            tbs_mapping=mock_get_tablespace_mapping.return_value,
            bwlimit=mock_get_bandwidth_limit.return_value,
            immediate=True,
            path=server_path,
            parent_backup_manifest_path=mock_get_parent_manifest.return_value,
        )
        # AND the instance of PgBaseBackup was actually run
        mock_pg_basebackup.return_value.assert_called_once()
        # AND the copy start was waited for before returning
        mock_wait_copy_start.assert_called_once_with(mock_pg_basebackup.return_value)

    @patch("barman.backup_executor.CloudPostgresBackupExecutor.get_remote_status")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._get_bandwidth_limit")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._get_tablespace_mapping")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor._get_parent_backup_manifest_path"
    )
    @patch("barman.backup_executor.DataTransferFailure", wraps=DataTransferFailure)
    @patch("barman.backup_executor.PgBaseBackup")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_run_pg_basebackup_fails(
        self,
        mock_init,
        mock_pg_basebackup,
        mock_data_transfer_failure,
        mock_get_parent_manifest,
        mock_get_tablespace_mapping,
        mock_get_bandwidth_limit,
        mock_get_remote_status,
    ):
        """
        Test that failures in :class:`PgBaseBackup` are correctly handled and wrapped
        into a :exec:`DataTransferFailure`.
        """
        # GIVEN a CloudPostgresBackupExecutor
        executor = CloudPostgresBackupExecutor(None)
        # Prepare the relevant executor attributes. Since we are testing failure
        # handling, the actual values do not matter here as the call is not checked
        executor._pgdata_dest = "/tmp/barman/plain/data"
        executor.config, executor.server = mock.Mock(), mock.Mock()
        # Mock PgBaseBackup to raise a CommandFailedException when called
        exception = CommandFailedException("Failure!")
        mock_pg_basebackup.return_value.side_effect = exception
        # WHEN _run_pg_basebackup is called
        # THEN it raises a DataTransferFailure exception
        with pytest.raises(DataTransferFailure):
            backup_info = build_test_backup_info()
            executor._run_pg_basebackup(backup_info)
        # AND PgBaseBackup was initialized (again the actual parameters do not matter here)
        mock_pg_basebackup.assert_called_once()
        # AND the DataTransferFailure was raised by calling from_command_error with the
        # expected parameters
        mock_data_transfer_failure.from_command_error.assert_called_once_with(
            "pg_basebackup",
            exception,
            ("data transfer failure on directory '%s'" % executor._pgdata_dest),
        )

    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_get_tablespace_mapping(self, _):
        """
        Test that tablespaces are correctly mapped inside the staging directory.
        """
        # GIVEN a CloudPostgresBackupExecutor with relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor._cloud_staging_dir = "/tmp/barman"
        executor.server = mock.Mock()
        # AND a BackupInfo with tablespaces
        backup_info = build_test_backup_info(
            tablespaces=(
                ("tbs1", 16387, "/fake/location"),
                ("tbs2", 16405, "/another/location"),
            )
        )
        # WHEN _get_tablespace_mapping is called
        result = executor._get_tablespace_mapping(backup_info)
        # THEN tablespaces are mapped to the staging dir inside the plain subdir
        assert result == {
            "/fake/location": "/tmp/barman/plain/16387",
            "/another/location": "/tmp/barman/plain/16405",
        }

    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_get_parent_backup_manifest_path_with_parent(self, _):
        """
        Test that the parent backup's manifest path is correctly retrieved
        when a parent backup exists.
        """
        # GIVEN a CloudPostgresBackupExecutor with relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor.server = mock.Mock(meta_directory="/path/to/meta")
        # AND a backup_info that HAS a parent
        mock_parent_info = mock.Mock(backup_id="20260112T114307")
        backup_info = mock.Mock(
            get_parent_backup_info=mock.Mock(return_value=mock_parent_info)
        )
        # WHEN _get_parent_backup_manifest_path is called
        result = executor._get_parent_backup_manifest_path(backup_info)
        # THEN it returns the expected manifest path
        assert result == "/path/to/meta/20260112T114307-backup_manifest"
        # AND the correct calls were made
        backup_info.get_parent_backup_info.assert_called_once()

    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_get_parent_backup_manifest_path_without_parent(self, _):
        """
        Test that ``None`` is returned when there is no parent backup.
        """
        # GIVEN a CloudPostgresBackupExecutor
        executor = CloudPostgresBackupExecutor(None)
        # AND a backup_info that HAS NO parent
        backup_info = mock.Mock(get_parent_backup_info=mock.Mock(return_value=None))
        # WHEN _get_parent_backup_manifest_path is called
        result = executor._get_parent_backup_manifest_path(backup_info)
        # THEN it returns the expected manifest path
        assert result is None
        # Assert the correct calsl were made
        backup_info.get_parent_backup_info.assert_called_once()

    @patch("barman.backup_executor.os.walk")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_wait_copy_start_raises_exception(self, _, mock_walk):
        """
        Test that ``_wait_copy_start`` correctly raises an exception when
        ``pg_basebackup`` finishes without copying any files.
        """
        # Mock pg_basebackup object to simulate it errored during spawning
        pg_basebackup = mock.Mock(
            get_returncode=lambda: 1,
            get_stderr=lambda: "a super bad thing just happened!",
        )
        # Simulate the plain staging directory created but no files present yet
        plain_dest = "/tmp/barman/cloud-staging/34234/plain"
        mock_walk.return_value = [(plain_dest, [], [])]
        # GIVEN a CloudPostgresBackupExecutor
        executor = CloudPostgresBackupExecutor(None)
        executor._plain_dest = plain_dest
        # WHEN _wait_copy_start is called with the mocked pg_basebackup
        # THEN it raises DataTransferFailure as pg_basebackup finished without any copy
        with pytest.raises(DataTransferFailure) as exc_info:
            executor._wait_copy_start(pg_basebackup)
        assert str(exc_info.value) == (
            "pg_basebackup process failed to start (return code 1): "
            "a super bad thing just happened!"
        )

    @patch("barman.backup_executor.os.walk")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_wait_copy_start_no_exception_raised(self, _, mock_walk):
        """
        Test that ``_wait_copy_start`` correctly returns when ``pg_basebackup`` started
        copying files to the staging directory.
        """
        # Mock pg_basebackup object to simulate it still running
        pg_basebackup = mock.Mock(get_returncode=lambda: None, get_stderr=lambda: None)
        # Simulate the plain staging directory created and files already present
        plain_dest = "/tmp/barman/cloud-staging/34234/plain"
        mock_walk.return_value = [(plain_dest, [], ["data/file1", "data/file2"])]
        # GIVEN a CloudPostgresBackupExecutor
        executor = CloudPostgresBackupExecutor(None)
        executor._plain_dest = plain_dest
        # WHEN _wait_copy_start is called with the mocked pg_basebackup
        # THEN it returns without raising an exception
        executor._wait_copy_start(pg_basebackup)

    @patch("barman.backup_executor.get_directory_size")
    @patch("barman.backup_executor.os.path.isdir", return_value=True)
    @patch("barman.backup_executor.time.sleep")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_monitor_staging_area(self, _, mock_sleep, mock_isdir, mock_get_dir_size):
        """
        Test that ``_monitor_staging_area`` correctly pauses and resumes the backup
        according the threshold configured and the current directory size.
        """
        # Mock get_directory_size to simulate directory size increasing and decreasing
        mock_get_dir_size.side_effect = [
            35 * 1024 * 1024 * 1024,  # 35 GB
            40 * 1024 * 1024 * 1024,  # 40 GB
            25 * 1024 * 1024 * 1024,  # 25 GB
        ]
        # Mock the backup process to simulate it running then ending so that
        # the monitoring loop does not run indefinitely
        pg_basebackup = mock.Mock()
        pg_basebackup.is_running.side_effect = [True, True, True, False]

        # GIVEN a CloudPostgresBackupExecutor with all relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor.config = mock.Mock(
            cloud_staging_max_size=30 * 1024 * 1024 * 1024  # 30 GB as threashold
        )
        executor._cloud_staging_dir = "/tmp/barman"
        executor._plain_dest = "/tmp/barman/plain"
        executor._thread_exc_event = mock.Mock(is_set=lambda: False)
        # WHEN _monitor_staging_area is called
        executor._monitor_staging_area(pg_basebackup)
        # THEN the process is paused twice (as the first two sizes are above threshold)
        # and resumed once (as the third size is below threshold)
        pg_basebackup.pause.call_count == 2
        pg_basebackup.resume.call_count == 1
        # AND os.path.isdir, get_directory_size and time.sleep were called as expected
        mock_isdir.assert_has_calls([mock.call("/tmp/barman/plain")] * 3)
        mock_get_dir_size.assert_has_calls([mock.call("/tmp/barman")] * 3)
        mock_sleep.assert_has_calls([mock.call(1)] * 3)

    @patch("barman.backup_executor.os.path.isdir", return_value=False)
    @patch("barman.backup_executor.time.sleep")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_monitor_staging_area_not_created_yet(self, _, mock_sleep, mock_isdir):
        """
        Test that ``_monitor_staging_area`` correctly handles the case where
        the staging directory does not yet exist. It should wait until the
        directory is created, sleeping between checks.
        """
        # GIVEN a CloudPostgresBackupExecutor with all relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor.config = mock.Mock(
            cloud_staging_max_size=30 * 1024 * 1024 * 1024  # 30 GB
        )
        executor._cloud_staging_dir = "/tmp/barman"
        executor._plain_dest = "/tmp/barman/plain"
        executor._thread_exc_event = mock.Mock(is_set=lambda: False)
        # Mock the backup process to simulate it running then ending so that
        # the monitoring loop does not run indefinitely
        pg_basebackup = mock.Mock()
        pg_basebackup.is_running.side_effect = [True, True, False]
        # WHEN _monitor_staging_area is called
        executor._monitor_staging_area(pg_basebackup)
        # THEN os.path.isdir AND time.sleep are called as expected, meaning that
        # the method kept checking for the directory to be created until the
        # backup process ended
        mock_isdir.assert_has_calls([mock.call("/tmp/barman/plain")] * 2)
        mock_sleep.assert_has_calls([mock.call(1)] * 2)

    @patch("barman.backup_executor._logger")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_monitor_staging_area_exception_is_present(self, _, mock_logger):
        """
        Test that ``_monitor_staging_area`` correctly exits when an exception is set
        in the executor, meaning that the backup copy process will be stopped.
        """
        # GIVEN a CloudPostgresBackupExecutor with all relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor._thread_exc_event = mock.Mock(is_set=lambda: True)
        # WHEN _monitor_staging_area is called
        pg_basebackup = mock.Mock()
        executor._monitor_staging_area(pg_basebackup)
        # THEN it returns without doing anything
        # By asserting that the logger was called only once with the thread error
        # message we ensure that the method exited right after checking the event
        mock_logger.debug.assert_called_once_with(
            "An error occurred in another thread, exiting monitoring thread"
        )

    @patch("barman.backup_executor.time.sleep")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._get_files_from_dir")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._get_open_files")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._is_last_file")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._ignore_file")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_fetch_ready_files(
        self,
        _,
        mock_ignore_file,
        mock_is_last_file,
        mock_get_open_files,
        mock_get_files,
        mock_sleep,
    ):
        """
        Test that ``_fetch_ready_files`` correctly populates the ready queue
        with the correct files and in the correct order.
        """
        # Mock the following files being in the staging directory
        mock_get_files.return_value = [
            "/tmp/barman/plain/data/base/4/2658",
            "/tmp/barman/plain/data/base/4/2672",
            "/tmp/barman/plain/data/log/postgresql-Sat.log",
            "/tmp/barman/plain/39484/PG_17_2024/5/271",
            "/tmp/barman/plain/data/postgresql.conf",
        ]
        # Mock that only the following file is still open by pg_basebackup
        mock_get_open_files.return_value = ["/tmp/barman/plain/data/base/4/2672"]
        # Mock that postgresql.conf should be fetched lastly
        # and postgresql-Sat.log should be ignored
        mock_is_last_file.side_effect = lambda f: f.endswith("postgresql.conf")
        mock_ignore_file.side_effect = lambda f: f.endswith("postgresql-Sat.log")
        # Mock the backup process to simulate it running then ending so that
        # the fetch loop does not run indefinitely
        pg_basebackup = mock.Mock()
        pg_basebackup.is_running.side_effect = [True, True, False]
        pg_basebackup.pid = 12345

        # GIVEN a CloudPostgresBackupExecutor with all relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor._ready_queue = queue.SimpleQueue()
        executor._plain_dest = "/tmp/barman/plain"
        executor._thread_exc_event = mock.Mock(is_set=lambda: False)
        # WHEN _fetch_ready_files is called
        executor._fetch_ready_files(pg_basebackup)
        # THEN the ready queue contains the expected files in the expected order
        # First the files that are neither open nor last files
        assert executor._ready_queue.get() == "/tmp/barman/plain/data/base/4/2658"
        assert executor._ready_queue.get() == "/tmp/barman/plain/39484/PG_17_2024/5/271"
        # The below file was open during the fetching, so it appears later in the queue
        assert executor._ready_queue.get() == "/tmp/barman/plain/data/base/4/2672"
        # The below file was marked as last file so it appears at the end of the queue
        assert executor._ready_queue.get() == "/tmp/barman/plain/data/postgresql.conf"
        assert executor._ready_queue.get() is None  # End marker
        assert executor._ready_queue.empty()
        # AND also assert that the relevant methods were called as expected
        mock_get_files.assert_has_calls([mock.call("/tmp/barman/plain")] * 3)
        mock_get_open_files.assert_has_calls([mock.call(pg_basebackup)] * 3)
        mock_ignore_file.assert_called()
        mock_is_last_file.assert_called()
        mock_sleep.assert_has_calls([mock.call(1)] * 2)

    @patch("barman.backup_executor._logger")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_fetch_ready_files_exception_is_present(self, _, mock_logger):
        """
        Test that ``_fetch_ready_files`` correctly exits when an exception is set
        in the executor, meaning that the backup copy process will be stopped.
        """
        # GIVEN a CloudPostgresBackupExecutor with all relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor._plain_dest = "/tmp/barman/plain"
        executor._thread_exc_event = mock.Mock(is_set=lambda: True)
        # WHEN _fetch_ready_files is called
        pg_basebackup = mock.Mock()
        executor._fetch_ready_files(pg_basebackup)
        # THEN it returns without doing anything
        # By asserting the logger only has no calls after the thread error message
        # we ensure that the method exited right after checking the event
        mock_logger.debug.assert_has_calls(
            [
                mock.call("Starting to fetch ready files from /tmp/barman/plain"),
                mock.call(
                    "An error occurred in another thread, exiting fetching thread"
                ),
            ]
        )

    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_get_files_from_dir(self, _, tmpdir):
        """
        Test that ``_get_files_from_dir`` correctly retrieves all files
        from the given directory and its subdirectories.
        """
        # Create a test directory structure with the following files:
        # /data/base/4/2658
        # /data/base/4/2672
        # /data/log/postgresql-Sat.log
        # /39484/PG_17_2024/5/271
        data_dir = tmpdir.mkdir("data")
        base_dir = data_dir.mkdir("base").mkdir("4")
        base_dir.join("2658").write("")
        base_dir.join("2672").write("")
        data_dir.mkdir("log").join("postgresql-Sat.log").write("")
        tmpdir.mkdir("39484").mkdir("PG_17_2024").mkdir("5").join("271").write("")

        # GIVEN a CloudPostgresBackupExecutor
        executor = CloudPostgresBackupExecutor(None)
        result = executor._get_files_from_dir(str(tmpdir))
        # THEN it returns the expected list of files
        assert sorted(result) == sorted(
            [
                str(tmpdir.join("data/base/4/2658")),
                str(tmpdir.join("data/base/4/2672")),
                str(tmpdir.join("data/log/postgresql-Sat.log")),
                str(tmpdir.join("39484/PG_17_2024/5/271")),
            ]
        )

    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_get_open_files(self, _, tmpdir):
        """
        Test that ``_get_open_files`` correctly retrieves the list of open files
        for the given ``pg_basebackup`` process, filtering only those that are inside
        the plain directory and ignoring the rest.
        """
        # GIVEN a CloudPostgresBackupExecutor with relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        staging_dir = tmpdir.mkdir("staging")
        plain_dir = staging_dir.mkdir("plain")
        executor._plain_dest = str(plain_dir)
        # Override the _FILE_DESCRIPTORS_DIRECTORY so we don't write to the actual /proc
        executor._FILE_DESCRIPTORS_DIRECTORY = str(tmpdir) + "/{pid}/fd"

        # Simulate a file descriptor directory with the following structure:
        # /123/fd/1 (stdout)
        # /123/fd/2 (stderr)
        # /123/fd/40 -> symlink to /staging/plain/data/base/4/2672 (open file inside plain_dir)
        # /123/fd/45 -> symlink to /other/file (open file outside plain_dir)
        fd_dir = tmpdir.mkdir("123").mkdir("fd")
        fd_dir.join("1").write("")  # stdout
        fd_dir.join("2").write("")  # stderr

        # File INSIFE plain_dir: /staging/plain/data/base/4/2672
        segment_file = plain_dir.join("data/base/4/2672")
        segment_file.ensure()  # Create the actual file/path
        fd_40 = fd_dir.join("40")
        fd_40.mksymlinkto(str(segment_file))  # Symlink from fd/40 to the target file

        # File OUTSIDE plain_dir
        other_file = tmpdir.join("other/file")
        other_file.ensure()
        fd_45 = fd_dir.join("45")
        fd_45.mksymlinkto(str(other_file))  # Symlink from fd/45 to the other/ile

        # WHEN _get_open_files is called with a mock pg_basebackup process
        pg_basebackup = mock.Mock(pid=123, is_running=lambda: True)
        result = executor._get_open_files(pg_basebackup)

        # THEN it returns only the file inside plain_dir and ignores the one outside
        assert result == [str(segment_file)]

    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_get_open_files_process_exited_midway(self, _, tmp_path):
        """
        Test that ``_get_open_files`` correctly handles the case where the
        ``pg_basebackup`` process exits midway through the retrieval of open files,
        which leads to it returning an empty list.
        """
        # GIVEN a CloudPostgresBackupExecutor instance with relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor._FILE_DESCRIPTORS_DIRECTORY = str(tmp_path) + "/{pid}/fd"
        pg_basebackup = mock.Mock(pid=999, is_running=lambda: True)  # Non-existent PID

        # WHEN _get_open_files is called
        result = executor._get_open_files(pg_basebackup)
        # THEN it returns an empty list and does not raise an exception
        assert result == []

    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_get_open_files_process_not_running(self, _):
        """
        Test that ``_get_open_files`` correctly handles the case where the
        ``pg_basebackup`` process is not running, which leads to it returning
        an empty list.
        """
        # GIVEN a CloudPostgresBackupExecutor instance with relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        pg_basebackup = mock.Mock(
            pid=123, is_running=lambda: False
        )  # Process not running

        # WHEN _get_open_files is called
        result = executor._get_open_files(pg_basebackup)
        # THEN it returns an empty list without attempting to read file descriptors
        assert result == []

    @patch("barman.backup_executor.path_allowed", return_value=False)
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_ignore_file(self, _, mock_path_allowed):
        """
        Test that ``_ignore_file`` correctly determines whether a file
        should be ignored based on the exclude lists.
        """
        # GIVEN a CloudPostgresBackupExecutor with all relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor._pgdata_dest = "/tmp/barman/plain/data"
        # WHEN _ignore_file is called with a test file path
        result = executor._ignore_file("/tmp/barman/plain/data/log/postgresql-Sat.log")
        # THEN path_allowed is called with the correct parameters
        mock_path_allowed.assert_called_once_with(
            EXCLUDE_LIST + PGDATA_EXCLUDE_LIST, None, "log/postgresql-Sat.log", False
        )
        # AND the result is the opposite boolean of whatever path_allowed returned
        assert result == (not mock_path_allowed.return_value)

    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_is_last_file(self, _):
        """
        Test that ``_is_last_file`` correctly identifies files that should be
        uploaded lastly, such files are defined in
        ``CloudPostgresBackupExecutor.LAST_FILE_LIST``.
        """
        # GIVEN a CloudPostgresBackupExecutor with all relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor._pgdata_dest = "/tmp/barman/plain/data"
        # WHEN _is_last_file is called with a path to postgresql.conf
        result = executor._is_last_file("/tmp/barman/plain/data/postgresql.conf")
        # THEN the result is True as .conf files are in the last file list
        assert result is True
        # AND WHEN _is_last_file is called with a path to a regular data file
        result = executor._is_last_file("/tmp/barman/plain/data/base/4/2658")
        # THEN the result is False
        assert result is False

    @patch("barman.backup_executor.os.listdir")
    @patch("barman.backup_executor.os.unlink")
    @patch("barman.backup_executor.CloudPostgresBackupExecutor._get_tarball_name")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_upload_ready_files(
        self, _, mock_get_tarball_name, mock_unlink, mock_listdir
    ):
        """
        Test that ``_upload_ready_files`` correctly uploads files from the ready queue
        using the upload controller, and unlinks them afterwards (except for backup_label).
        Also assert that directories are uploaded to handle empty directories,
        and that pg_control is uploaded at the end.
        """
        # GIVEN a CloudPostgresBackupExecutor with all relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor._plain_dest = "/tmp/barman/plain"
        executor._pgdata_dest = "/tmp/barman/plain/data"
        executor._upload_controller = mock.Mock()
        executor._ready_queue = queue.SimpleQueue()
        executor._thread_exc_event = mock.Mock(is_set=lambda: False)
        # Mock the ready queue to contain the following files
        executor._ready_queue.put("/tmp/barman/plain/data/base/4/2658")
        executor._ready_queue.put("/tmp/barman/plain/data/base/4/2672")
        executor._ready_queue.put(
            "/tmp/barman/plain/data/backup_label"
        )  # should not be removed
        executor._ready_queue.put("/tmp/barman/plain/2024/PG_17_2024/5/271")
        executor._ready_queue.put(None)  # End marker
        # Mock listdir to return the data and 2024 subdirs when called on plain dest
        mock_listdir.return_value = ["data", "2024"]
        # Mock get_tarball_name to return the tarball name respective to the above files
        mock_get_tarball_name.side_effect = [
            "data",  # when called for /tmp/barman/plain/data/base/4/2658
            "data",  # when called for /tmp/barman/plain/data/base/4/2672
            "data",  # when called for /tmp/barman/plain/data/backup_label
            "2024",  # when called for /tmp/barman/plain/2024/PG_17_2024/5/271
            "data",  # when called for the subdir /tmp/barman/plain/data
            "2024",  # when called for the subdir /tmp/barman/plain/2024
        ]
        # WHEN _upload_ready_files is called
        executor._upload_ready_files()
        # THEN each file is uploaded correctly by using the upload controller add_file
        executor._upload_controller.add_file.assert_has_calls(
            [
                mock.call(
                    label="2658",
                    src="/tmp/barman/plain/data/base/4/2658",
                    dst="data",
                    path="base/4/2658",
                ),
                mock.call(
                    label="2672",
                    src="/tmp/barman/plain/data/base/4/2672",
                    dst="data",
                    path="base/4/2672",
                ),
                mock.call(
                    label="backup_label",
                    src="/tmp/barman/plain/data/backup_label",
                    dst="data",
                    path="backup_label",
                ),
                mock.call(
                    label="271",
                    src="/tmp/barman/plain/2024/PG_17_2024/5/271",
                    dst="2024",
                    path="PG_17_2024/5/271",
                ),
            ]
        )
        # AND all uploaded files are unlinked, except the backup_label
        mock_unlink.assert_has_calls(
            [
                mock.call("/tmp/barman/plain/data/base/4/2658"),
                mock.call("/tmp/barman/plain/data/base/4/2672"),
                mock.call("/tmp/barman/plain/2024/PG_17_2024/5/271"),
            ]
        )
        # AND the data and tablespaces directories are uploaded so that empty dirs are handled
        executor._upload_controller.upload_directory.assert_has_calls(
            [
                mock.call(
                    label="data",
                    src="/tmp/barman/plain/data",
                    dst="data",
                    exclude=EXCLUDE_LIST + PGDATA_EXCLUDE_LIST,
                ),
                mock.call(
                    label="2024",
                    src="/tmp/barman/plain/2024",
                    dst="2024",
                    exclude=EXCLUDE_LIST + PGDATA_EXCLUDE_LIST,
                ),
            ]
        )
        # AND lastly, pg_control is uploaded
        executor._upload_controller.add_file.assert_called_with(
            label="pg_control",
            src="/tmp/barman/plain/data/global/pg_control",
            dst="data",
            path="global/pg_control",
        )

    @patch("barman.backup_executor._logger")
    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_upload_ready_files_exception_is_present(self, _, mock_logger):
        """
        Test that ``_upload_ready_files`` correctly exits when an exception is set
        in the executor, meaning that the backup copy process will be stopped.
        """
        # GIVEN a CloudPostgresBackupExecutor with all relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor._thread_exc_event = mock.Mock(is_set=lambda: True)
        # WHEN _upload_ready_files is called
        executor._upload_ready_files()
        # THEN it returns without doing anything
        # By asserting that the logger was called only once with the expected message,
        # we ensure that the method exited right after checking the event
        mock_logger.debug.assert_called_once_with(
            "An error occurred in a thread, stopping upload of ready files"
        )

    @patch(
        "barman.backup_executor.CloudPostgresBackupExecutor.__init__", return_value=None
    )
    def test_get_tarball_name(self, _):
        """
        Test that ``_get_tarball_name`` correctly determines the tarball name
        for data files and tablespace files. It should return "data" for files
        within the pgdata directory, and the tablespace OID for files within
        tablespace directories.
        """
        # GIVEN a CloudPostgresBackupExecutor with all relevant attributes set
        executor = CloudPostgresBackupExecutor(None)
        executor._plain_dest = "/tmp/barman/plain"
        executor._pgdata_dest = "/tmp/barman/plain/data"
        # WHEN _get_tarball_name is called with a data file path
        data_file = "/tmp/barman/plain/data/base/4/2658"
        # THEN it returns "data"
        assert executor._get_tarball_name(data_file) == "data"
        # WHEN _get_tarball_name is called with a tablespace file path
        tablespace_file = "/tmp/barman/plain/39484/PG_17_2024/5/271"
        # THEN it returns the tablespace its OID
        assert executor._get_tarball_name(tablespace_file) == "39484"


class TestGetBandwidthLimitInBytes(object):
    """
    Tests for the _get_bandwidth_limit_in_bytes helper function.
    """

    @pytest.mark.parametrize(
        "input_kb, expected_bytes",
        [
            (None, None),  # Returns None when input is None
            (100, 100000),  # Converts 100 kB/s to 100000 B/s
            (1024, 1024000),  # Converts 1024 kB/s to 1024000 B/s
            (1000000000, 1000000000000),  # Handles large values correctly
        ],
    )
    def test_bandwidth_limit_conversion(self, input_kb, expected_bytes):
        """
        Test that _get_bandwidth_limit_in_bytes correctly converts kB/s to B/s
        """
        # WHEN _get_bandwidth_limit_in_bytes is called
        result = _get_bandwidth_limit_in_bytes(input_kb)

        # THEN it returns the expected result
        assert result == expected_bytes


class TestCloudBackupExecutor(object):
    """
    Tests for the CloudBackupExecutor class.
    """

    @patch("barman.backup_executor.ConcurrentBackupStrategy")
    def test__init__(self, mock_strategy):
        """
        Test the construction of a CloudBackupExecutor
        """
        # GIVEN a server with cloud configuration
        server = build_mocked_server(
            main_conf={
                "backup_method": "local-to-cloud",
                "basebackups_directory": "s3://bucket/path",
            }
        )
        # Set the strategy mode to None for testing
        mock_strategy.return_value.mode = None
        # WHEN a CloudBackupExecutor is created
        executor = CloudBackupExecutor(server.backup_manager)
        # THEN a ConcurrentBackupStrategy is created
        mock_strategy.assert_called_once_with(
            executor.server.postgres, executor.config.name
        )
        assert executor.strategy == mock_strategy.return_value
        # AND the executor mode is set to "local-to-cloud"
        assert executor.mode == "local-to-cloud"

    @patch("barman.cloud.CloudBackupUploader")
    @patch("barman.backup_executor.CloudBackupExecutor.__init__", return_value=None)
    def test_backup(self, _, mock_uploader_class):
        """
        Test that the backup method properly delegates to CloudBackupUploader
        """
        # GIVEN a CloudBackupExecutor with necessary attributes
        executor = CloudBackupExecutor(None)
        executor.server = mock.Mock()
        mock_config = mock.Mock()
        mock_config.name = "test_server"
        mock_config.cloud_upload_max_archive_size = 100 * 1024 * 1024 * 1024  # 100 GiB
        mock_config.cloud_upload_min_chunk_size = 5 * 1024 * 1024  # 5 MiB
        mock_config.bandwidth_limit = None
        mock_config.keepalive_interval = 60
        executor.config = mock_config

        # Mock the postgres connection and cloud interface
        mock_postgres = mock.Mock()
        mock_cloud_interface = mock.Mock()
        executor.server.postgres = mock_postgres
        executor.server.get_backup_cloud_interface.return_value = mock_cloud_interface

        # Mock the uploader instance
        mock_uploader = mock.Mock()
        mock_uploader.copy_start_time = "2026-02-09 12:00:00"
        mock_uploader.copy_end_time = "2026-02-09 13:00:00"
        mock_uploader_class.return_value = mock_uploader

        # Mock the upload controller
        mock_controller = mock.Mock()
        mock_controller.size = 1024000
        mock_uploader.create_upload_controller.return_value = mock_controller

        # Create a mock backup_info
        backup_info = mock.Mock()
        backup_info.backup_name = "test_backup"
        backup_info.backup_id = "20260209T120000"

        # Mock _purge_unused_wal_files method
        executor._purge_unused_wal_files = mock.Mock()

        # WHEN backup is called
        executor.backup(backup_info)

        # THEN CloudBackupUploader is instantiated with correct parameters
        mock_uploader_class.assert_called_once_with(
            server_name="test_server",
            cloud_interface=mock_cloud_interface,
            max_archive_size=100 * 1024 * 1024 * 1024,  # 100 GiB
            postgres=mock_postgres,
            backup_name="test_backup",
            min_chunk_size=5 * 1024 * 1024,  # 5 MiB
            max_bandwidth=None,
            keepalive_interval=60,
        )

        # AND the backup_info is set on the uploader
        assert mock_uploader.backup_info == backup_info

        # AND the upload controller is created
        mock_uploader.create_upload_controller.assert_called_once_with(
            "20260209T120000"
        )
        assert mock_uploader.controller == mock_controller

        # AND the backup is coordinated
        mock_uploader.coordinate_backup.assert_called_once()

        # AND timing info is copied back
        assert executor.copy_start_time == "2026-02-09 12:00:00"
        assert executor.copy_end_time == "2026-02-09 13:00:00"

        # AND size information is set on backup_info
        backup_info.set_attribute.assert_any_call("size", 1024000)
        backup_info.set_attribute.assert_any_call("deduplicated_size", 1024000)

        # AND cloud interface and postgres are closed
        mock_cloud_interface.close.assert_called_once()
        mock_postgres.close.assert_called_once()

        # AND unused WAL files are purged
        executor._purge_unused_wal_files.assert_called_once_with(backup_info)

    @patch("barman.cloud.CloudBackupUploader")
    @patch("barman.backup_executor.CloudBackupExecutor.__init__", return_value=None)
    def test_backup_handles_exception(self, _, mock_uploader_class):
        """
        Test that the backup method properly handles SystemExit exceptions
        """
        # GIVEN a CloudBackupExecutor
        executor = CloudBackupExecutor(None)
        executor.server = mock.Mock()
        mock_config = mock.Mock()
        mock_config.name = "test_server"
        mock_config.cloud_upload_max_archive_size = 100 * 1024 * 1024 * 1024  # 100 GiB
        mock_config.cloud_upload_min_chunk_size = 5 * 1024 * 1024  # 5 MiB
        mock_config.bandwidth_limit = None
        executor.config = mock_config

        # Mock the postgres connection and cloud interface
        mock_postgres = mock.Mock()
        mock_cloud_interface = mock.Mock()
        executor.server.postgres = mock_postgres
        executor.server.get_backup_cloud_interface.return_value = mock_cloud_interface

        # Mock the uploader to raise SystemExit
        mock_uploader = mock.Mock()
        mock_uploader.coordinate_backup.side_effect = SystemExit("Test error")
        mock_uploader_class.return_value = mock_uploader
        mock_uploader.create_upload_controller.return_value = mock.Mock()

        backup_info = mock.Mock(backup_name="test_backup", backup_id="20260209T120000")

        # WHEN backup is called and an exception occurs
        # THEN a BackupException is raised
        with pytest.raises(BackupException) as exc_info:
            executor.backup(backup_info)

        assert "Cloud backup failed with error" in str(exc_info.value)

    @patch("barman.cloud.CloudBackupUploader")
    @patch("barman.backup_executor.CloudBackupExecutor.__init__", return_value=None)
    def test_backup_with_bandwidth_limit(self, _, mock_uploader_class):
        """
        Test that bandwidth_limit is correctly converted and passed to CloudBackupUploader
        """
        # GIVEN a CloudBackupExecutor with bandwidth_limit set
        executor = CloudBackupExecutor(None)
        executor.server = mock.Mock()
        mock_config = mock.Mock()
        mock_config.name = "test_server"
        mock_config.cloud_upload_max_archive_size = 100 * 1024 * 1024 * 1024  # 100 GiB
        mock_config.cloud_upload_min_chunk_size = 5 * 1024 * 1024  # 5 MiB
        mock_config.bandwidth_limit = 100  # 100 kB/s
        mock_config.keepalive_interval = 60
        executor.config = mock_config

        # Mock the postgres connection and cloud interface
        mock_postgres = mock.Mock()
        mock_cloud_interface = mock.Mock()
        executor.server.postgres = mock_postgres
        executor.server.get_backup_cloud_interface.return_value = mock_cloud_interface

        # Mock the uploader instance
        mock_uploader = mock.Mock()
        mock_uploader.copy_start_time = "2026-02-09 12:00:00"
        mock_uploader.copy_end_time = "2026-02-09 13:00:00"
        mock_uploader_class.return_value = mock_uploader

        # Mock the upload controller
        mock_controller = mock.Mock()
        mock_controller.size = 1024000
        mock_uploader.create_upload_controller.return_value = mock_controller

        # Create a mock backup_info
        backup_info = mock.Mock()
        backup_info.backup_name = "test_backup"
        backup_info.backup_id = "20260209T120000"

        # Mock _purge_unused_wal_files method
        executor._purge_unused_wal_files = mock.Mock()

        # WHEN backup is called
        executor.backup(backup_info)

        # THEN CloudBackupUploader is instantiated with converted bandwidth_limit (100 kB/s → 100000 B/s)
        mock_uploader_class.assert_called_once_with(
            server_name="test_server",
            cloud_interface=mock_cloud_interface,
            max_archive_size=100 * 1024 * 1024 * 1024,  # 100 GiB
            postgres=mock_postgres,
            backup_name="test_backup",
            min_chunk_size=5 * 1024 * 1024,  # 5 MiB
            max_bandwidth=100000,  # 100 kB/s converted to 100000 B/s
            keepalive_interval=60,
        )


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

        # WHEN find_missing_and_unmounted_disks is called for all expected disks
        instance_name = "instance1"
        snapshot_disks = (
            expected_missing_disks + expected_unmounted_disks + expected_mounted_disks
        )
        (
            missing_disks,
            unmounted_disks,
        ) = SnapshotBackupExecutor.find_missing_and_unmounted_disks(
            cmd,
            mock_get_snapshot_interface.return_value,
            instance_name,
            snapshot_disks,
        )

        # THEN get_attached_volumes was called with the expected arguments
        mock_get_attached_volumes.assert_called_once_with(
            instance_name, snapshot_disks, fail_on_missing=False
        )
        # AND the returned list of missing disks matches those not found amongst the
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

    @patch("barman.backup_executor.PostgresKeepAlive")
    @patch("barman.backup_executor.get_snapshot_interface_from_server_config")
    @patch("barman.backup_executor.ExternalBackupExecutor.backup")
    def test_backup_wraps_with_keepalive(
        self,
        mock_super_backup,
        _mock_get_snapshot_interface,
        mock_keepalive,
        core_snapshot_options,
    ):
        """
        Verify that SnapshotBackupExecutor.backup() wraps the parent backup call
        inside a PostgresKeepAlive context using the configured interval.
        """
        # GIVEN a SnapshotBackupExecutor with keepalive_interval configured
        server = build_mocked_server(
            main_conf={**core_snapshot_options, "keepalive_interval": 30}
        )
        executor = SnapshotBackupExecutor(server.backup_manager)
        backup_info = mock.Mock()

        # WHEN backup is called
        executor.backup(backup_info)

        # THEN PostgresKeepAlive is started with the server's postgres connection
        # and the configured interval
        mock_keepalive.assert_called_once_with(
            server.postgres, server.config.keepalive_interval, True
        )
        # AND the parent backup method ran inside the keepalive context
        mock_keepalive.return_value.__enter__.assert_called_once()
        mock_keepalive.return_value.__exit__.assert_called_once()
        mock_super_backup.assert_called_once_with(backup_info)


class TestBackupExecutor(object):
    """
    This class tests the methods of the executor base object
    """

    def test__purge_unused_wal_files_begin_wal_skip(self):
        """
        Test skip purging wals when :attr:`begin_wal` is not defined yet.
        """
        backup_manager = build_backup_manager(main_conf={"backup_method": "postgres"})

        backup_info = build_test_backup_info(
            server=backup_manager.server,
            backup_name="test_backup",
            server_name="main",
            begin_wal=None,
        )

        result = backup_manager.executor._purge_unused_wal_files(backup_info)
        assert result is None

    def test__purge_unused_wal_files_worm_mode_skip(self, capsys):
        """
        Test skip purging wals when :attr:`worm_mode` is enabled.
        """
        server = build_mocked_server(
            main_conf={
                "backup_options": BackupOptions.EXCLUSIVE_BACKUP,
                "worm_mode": "on",
            }
        )
        backup_manager = server.backup_manager
        backup_info = build_test_backup_info(
            server=server,
            backup_name="test_backup",
            server_name="main",
            begin_wal="0/00000F0",
        )
        executor = RsyncBackupExecutor(backup_manager)
        backup_manager.get_previous_backup.return_value = None
        result = executor._purge_unused_wal_files(backup_info)
        assert result is None
        out, _ = capsys.readouterr()
        assert "This is the first backup for server main" in out
        assert "'worm_mode' is enabled, skip purging of unused WAL files." in out

    def test__purge_unused_wal_files(self, capsys):
        """
        Test :meth:`_purge_unused_wal_files`.
        """
        server = build_mocked_server(
            main_conf={
                "backup_options": BackupOptions.EXCLUSIVE_BACKUP,
                "worm_mode": "off",
            }
        )
        backup_manager = server.backup_manager
        backup_info = build_test_backup_info(
            server=server,
            backup_name="test_backup",
            server_name="main",
            begin_wal="0/00000F0",
        )
        # We have to pick a subclass executor of the BackupExecutor because the base
        # class is abstract and we cannot create an instance of an abstract class.
        executor = RsyncBackupExecutor(backup_manager)
        backup_manager.get_previous_backup.return_value = None
        backup_manager.remove_wal_before_backup.return_value = [
            "0/0000040",
            "0/00000B0",
        ]
        executor._purge_unused_wal_files(backup_info)
        backup_manager.get_previous_backup.assert_called_with(backup_info.backup_id)
        backup_manager.remove_wal_before_backup.assert_called_with(backup_info)
        out, _ = capsys.readouterr()
        assert "This is the first backup for server main" in out
        assert "WAL segments preceding the current backup have been found:" in out
        assert "0/0000040 from server main has been removed" in out
        assert "0/00000B0 from server main has been removed" in out
