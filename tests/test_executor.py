# Copyright (C) 2013-2016 2ndQuadrant Italia Srl
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
import os

import mock
import pytest
from dateutil import tz
from mock import Mock, patch

from barman.backup_executor import PostgresBackupExecutor, RsyncBackupExecutor
from barman.config import BackupOptions
from barman.exceptions import (CommandFailedException, DataTransferFailure,
                               FsOperationFailed, SshCommandException)
from barman.infofile import BackupInfo, Tablespace
from barman.server import CheckOutputStrategy, CheckStrategy
from testing_helpers import (build_backup_manager, build_mocked_server,
                             build_test_backup_info)


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
        backup_manager.executor.config.reuse_backup = 'copy'
        assert backup_manager.executor._reuse_path(backup_info) == \
            '/some/barman/home/main/base/1234567890/data'

        # check for the expected path with link
        backup_manager.executor.config.reuse_backup = 'link'
        assert backup_manager.executor._reuse_path(backup_info) == \
            '/some/barman/home/main/base/1234567890/data'

    @patch('barman.backup_executor.UnixRemoteCommand')
    def test_check(self, command_mock, capsys):
        """
        Check the ssh connection to a remote server
        """
        backup_manager = build_backup_manager()

        # Test 1: ssh ok
        check_strategy = CheckOutputStrategy()
        command_mock.return_value.get_last_output.return_value = ('', '')
        backup_manager.executor.check(check_strategy)
        out, err = capsys.readouterr()
        assert err == ''
        assert 'ssh: OK' in out

        # Test 2: ssh success, with unclean output (out)
        command_mock.reset_mock()
        command_mock.return_value.get_last_output.return_value = (
            'This is unclean', '')
        backup_manager.executor.check(check_strategy)
        out, err = capsys.readouterr()
        assert err == ''
        assert 'ssh output clean: FAILED' in out

        # Test 2bis: ssh success, with unclean output (err)
        command_mock.reset_mock()
        command_mock.return_value.get_last_output.return_value = (
            '', 'This is unclean')
        backup_manager.executor.check(check_strategy)
        out, err = capsys.readouterr()
        assert err == ''
        assert 'ssh output clean: FAILED' in out

        # Test 3: ssh ok and PostgreSQL is not responding
        command_mock.reset_mock()
        command_mock.return_value.get_last_output.return_value = ('', '')
        check_strategy = CheckOutputStrategy()
        backup_manager.server.get_remote_status.return_value = {
            'server_txt_version': None
        }
        backup_manager.server.get_backup.return_value.pgdata = 'test/'
        backup_manager.executor.check(check_strategy)
        out, err = capsys.readouterr()
        assert err == ''
        assert 'ssh: OK' in out
        assert "Check that the PostgreSQL server is up and no " \
               "'backup_label' file is in PGDATA."

        # Test 4: ssh failed
        command_mock.reset_mock()
        command_mock.side_effect = FsOperationFailed
        backup_manager.executor.check(check_strategy)
        out, err = capsys.readouterr()
        assert err == ''
        assert 'ssh: FAILED' in out

    @patch("barman.backup.RsyncBackupExecutor.backup_copy")
    @patch("barman.backup.BackupManager.get_previous_backup")
    @patch("barman.backup.BackupManager.remove_wal_before_backup")
    def test_backup(self, rwbb_mock, gpb_mock, backup_copy_mock,
                    capsys, tmpdir):
        """
        Test the execution of a backup

        :param rwbb_mock: mock for the remove_wal_before_backup method
        :param gpb_mock: mock for the get_previous_backup method
        :param backup_copy_mock: mock for the executor's backup_copy method
        :param capsys: stdout capture module
        :param tmpdir: pytest temp directory
        """
        backup_manager = build_backup_manager(global_conf={
            'barman_home': tmpdir.mkdir('home').strpath
        })
        backup_info = BackupInfo(backup_manager.server,
                                 backup_id='fake_backup_id')
        backup_info.begin_xlog = "0/2000028"
        backup_info.begin_wal = "000000010000000000000002"
        backup_info.begin_offset = 40
        backup_info.status = BackupInfo.EMPTY

        gpb_mock.return_value = None

        rwbb_mock.return_value = ['000000010000000000000001']

        # Test 1: exclusive backup
        backup_manager.executor.strategy = Mock()
        backup_manager.executor.backup(backup_info)
        out, err = capsys.readouterr()
        assert err == ''
        assert (
            "Backup start at xlog location: "
            "0/2000028 (000000010000000000000002, 00000028)\n"
            "This is the first backup for server main\n"
            "WAL segments preceding the current backup have been found:\n"
            "\t000000010000000000000001 from server main has been removed\n"
            "Copying files.\n"
            "Copy done.") in out

        gpb_mock.assert_called_with(backup_info.backup_id)
        rwbb_mock.assert_called_with(backup_info)
        backup_manager.executor.strategy.start_backup.assert_called_once_with(
            backup_info)
        backup_copy_mock.assert_called_once_with(backup_info)
        backup_manager.executor.strategy.stop_backup.assert_called_once_with(
            backup_info)

        # Test 2: concurrent backup
        # change the configuration to concurrent backup
        backup_manager.executor.config.backup_options = [
            BackupOptions.CONCURRENT_BACKUP]

        # reset mocks
        gpb_mock.reset_mock()
        rwbb_mock.reset_mock()
        backup_manager.executor.strategy.reset_mock()
        backup_copy_mock.reset_mock()

        # prepare data directory for backup_label generation
        backup_info.backup_label = 'test\nlabel\n'

        backup_manager.executor.backup(backup_info)
        out, err = capsys.readouterr()
        assert err == ''
        assert (
            "Backup start at xlog location: "
            "0/2000028 (000000010000000000000002, 00000028)\n"
            "This is the first backup for server main\n"
            "WAL segments preceding the current backup have been found:\n"
            "\t000000010000000000000001 from server main has been removed\n"
            "Copying files.\n"
            "Copy done.") in out

        gpb_mock.assert_called_with(backup_info.backup_id)
        rwbb_mock.assert_called_with(backup_info)
        backup_manager.executor.strategy.start_backup.assert_called_once_with(
            backup_info)
        backup_copy_mock.assert_called_once_with(backup_info)
        backup_manager.executor.strategy.start_backup.assert_called_once_with(
            backup_info)

    @patch('barman.backup_executor.RsyncCopyController')
    def test_backup_copy(self, rsync_mock, tmpdir):
        """
        Test the execution of a rsync copy

        :param rsync_mock: mock for the RsyncCopyController object
        :param tmpdir: temporary dir
        """
        backup_manager = build_backup_manager(global_conf={
            'barman_home': tmpdir.mkdir('home').strpath
        })
        backup_manager.server.path = None
        backup_manager.server.postgres.server_major_version = '9.6'
        backup_info = build_test_backup_info(
            server=backup_manager.server,
            pgdata="/pg/data",
            config_file="/etc/postgresql.conf",
            hba_file="/pg/data/pg_hba.conf",
            ident_file="/pg/data/pg_ident.conf",
            begin_xlog="0/2000028",
            begin_wal="000000010000000000000002",
            begin_offset=28)
        backup_info.save()
        # This is to check that all the preparation is done correctly
        assert os.path.exists(backup_info.filename)

        backup_manager.executor.backup_copy(backup_info)

        assert rsync_mock.mock_calls == [
            mock.call(reuse_backup=None, safe_horizon=None,
                      network_compression=False,
                      ssh_command='ssh', path=None,
                      ssh_options=['-c', '"arcfour"', '-p', '22',
                                   'postgres@pg01.nowhere', '-o',
                                   'BatchMode=yes', '-o',
                                   'StrictHostKeyChecking=no'],
                      retry_sleep=30, retry_times=0, workers=1),
            mock.call().add_directory(
                label='tbs1',
                src=':/fake/location/',
                dst=backup_info.get_data_directory(16387),
                reuse=None,
                bwlimit=None,
                item_class=rsync_mock.return_value.TABLESPACE_CLASS,
                exclude=["/*"] + RsyncBackupExecutor.EXCLUDE_LIST,
                include=["/PG_9.6_*"]),
            mock.call().add_directory(
                label='tbs2',
                src=':/another/location/',
                dst=backup_info.get_data_directory(16405),
                reuse=None,
                bwlimit=None,
                item_class=rsync_mock.return_value.TABLESPACE_CLASS,
                exclude=["/*"] + RsyncBackupExecutor.EXCLUDE_LIST,
                include=["/PG_9.6_*"]),
            mock.call().add_directory(
                label='pgdata',
                src=':/pg/data/',
                dst=backup_info.get_data_directory(),
                reuse=None,
                bwlimit=None,
                item_class=rsync_mock.return_value.PGDATA_CLASS,
                exclude=RsyncBackupExecutor.PGDATA_EXCLUDE_LIST +
                    RsyncBackupExecutor.EXCLUDE_LIST,
                exclude_and_protect=['pg_tblspc/16387', 'pg_tblspc/16405']),
            mock.call().add_file(
                label='pg_control',
                src=':/pg/data/global/pg_control',
                dst='%s/global/pg_control' % backup_info.get_data_directory(),
                item_class=rsync_mock.return_value.PGCONTROL_CLASS),
            mock.call().add_file(
                label='config_file',
                src=':/etc/postgresql.conf',
                dst=backup_info.get_data_directory(),
                item_class=rsync_mock.return_value.CONFIG_CLASS,
                optional=False),
            mock.call().copy(),
        ]

    @patch('barman.backup_executor.RsyncCopyController')
    def test_backup_copy_with_included_files(self, rsync_moc, tmpdir, capsys):
        backup_manager = build_backup_manager(global_conf={
            'barman_home': tmpdir.mkdir('home').strpath
        })
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
            begin_offset=28)
        backup_info.save()
        # This is to check that all the preparation is done correctly
        assert os.path.exists(backup_info.filename)
        # Execute a backup
        backup_manager.executor.backup_copy(backup_info)
        out, err = capsys.readouterr()
        # check for the presence of the warning in the stderr
        assert ("WARNING: The usage of include directives "
                "is not supported") in err
        # check that the additional configuration file is present in the output
        assert backup_info.included_files[0] in err

    @patch('barman.backup_executor.RsyncCopyController')
    def test_backup_copy_with_included_files_nowarning(self, rsync_moc,
                                                       tmpdir, capsys):
        backup_manager = build_backup_manager(
            global_conf={
                'barman_home': tmpdir.mkdir('home').strpath,
            },
            main_conf={
                'backup_options': 'exclusive_backup, external_configuration',
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
            begin_offset=28)
        backup_info.save()
        # This is to check that all the preparation is done correctly
        assert os.path.exists(backup_info.filename)
        # Execute a backup
        backup_manager.executor.backup_copy(backup_info)
        out, err = capsys.readouterr()
        # check for the presence of the warning in the stderr
        assert ("WARNING: The usage of include directives "
                "is not supported") not in err


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
        server = build_mocked_server(main_conf={
            'backup_options':
            BackupOptions.EXCLUSIVE_BACKUP
        })
        backup_manager = build_backup_manager(server=server)

        # Mock server.get_pg_setting('data_directory') call
        backup_manager.server.postgres.get_setting.return_value = '/pg/data'
        # Mock server.get_pg_configuration_files() call
        server.postgres.get_configuration_files.return_value = dict(
            config_file="/etc/postgresql.conf",
            hba_file="/pg/pg_hba.conf",
            ident_file="/pg/pg_ident.conf",
        )
        # Mock server.get_pg_tablespaces() call
        tablespaces = [Tablespace._make(('test_tbs', 1234, '/tbs/test'))]
        server.postgres.get_tablespaces.return_value = tablespaces

        # Test 1: start exclusive backup
        # Mock server.start_exclusive_backup(label) call
        start_time = datetime.datetime.now()
        server.postgres.start_exclusive_backup.return_value = {
            'location': "A257/44B4C0D8",
            'file_name': "000000060000A25700000044",
            'file_offset': 11845848,
            'timestamp': start_time}

        # Build a test empty backup info
        backup_info = BackupInfo(server=backup_manager.server,
                                 backup_id='fake_id')

        backup_manager.executor.strategy.start_backup(backup_info)

        # Check that all the values are correctly saved inside the BackupInfo
        assert backup_info.pgdata == '/pg/data'
        assert backup_info.config_file == "/etc/postgresql.conf"
        assert backup_info.hba_file == "/pg/pg_hba.conf"
        assert backup_info.ident_file == "/pg/pg_ident.conf"
        assert backup_info.tablespaces == tablespaces
        assert backup_info.status == 'STARTED'
        assert backup_info.timeline == 6
        assert backup_info.begin_xlog == 'A257/44B4C0D8'
        assert backup_info.begin_wal == '000000060000A25700000044'
        assert backup_info.begin_offset == 11845848
        assert backup_info.begin_time == start_time
        # Check that the correct call to start_exclusive_backup has been made
        server.postgres.start_exclusive_backup.assert_called_with(
            'Barman backup main fake_id')

    def test_pgespresso_start_backup(self):
        """
        Test concurrent backup using pgespresso
        """
        # Test: start concurrent backup
        # Build a backup_manager using a mocked server
        server = build_mocked_server(main_conf={
            'backup_options':
            BackupOptions.CONCURRENT_BACKUP
        })
        backup_manager = build_backup_manager(server=server)
        # Mock server.get_pg_setting('data_directory') call
        backup_manager.server.postgres.get_setting.return_value = '/pg/data'
        # Mock server.get_pg_configuration_files() call
        server.postgres.get_configuration_files.return_value = dict(
            config_file="/etc/postgresql.conf",
            hba_file="/pg/pg_hba.conf",
            ident_file="/pg/pg_ident.conf",
        )
        # Mock server.get_pg_tablespaces() call
        tablespaces = [Tablespace._make(('test_tbs', 1234, '/tbs/test'))]
        server.postgres.get_tablespaces.return_value = tablespaces
        server.postgres.server_version = 90500

        # Mock executor._pgespresso_start_backup(label) call
        start_time = datetime.datetime.now(tz.tzlocal()).replace(microsecond=0)
        server.postgres.pgespresso_start_backup.return_value = {
            'backup_label':
                "START WAL LOCATION: 266/4A9C1EF8 "
                "(file 00000010000002660000004A)\n"
                "START TIME: %s" % start_time.strftime('%Y-%m-%d %H:%M:%S %Z'),
        }
        # Build a test empty backup info
        backup_info = BackupInfo(server=backup_manager.server,
                                 backup_id='fake_id2')

        backup_manager.executor.strategy.start_backup(backup_info)

        # Check that all the values are correctly saved inside the BackupInfo
        assert backup_info.pgdata == '/pg/data'
        assert backup_info.config_file == "/etc/postgresql.conf"
        assert backup_info.hba_file == "/pg/pg_hba.conf"
        assert backup_info.ident_file == "/pg/pg_ident.conf"
        assert backup_info.tablespaces == tablespaces
        assert backup_info.status == 'STARTED'
        assert backup_info.timeline == 16
        assert backup_info.begin_xlog == '266/4A9C1EF8'
        assert backup_info.begin_wal == '00000010000002660000004A'
        assert backup_info.begin_offset == 10231544
        assert backup_info.begin_time == start_time
        # Check that the correct call to pg_start_backup has been made
        server.postgres.pgespresso_start_backup.assert_called_with(
            'Barman backup main fake_id2')

    def test_concurrent_start_backup(self):
        """
        Test concurrent backup using 9.6 api
        """
        # Test: start concurrent backup
        # Build a backup_manager using a mocked server
        server = build_mocked_server(main_conf={
            'backup_options':
            BackupOptions.CONCURRENT_BACKUP
        })
        backup_manager = build_backup_manager(server=server)
        # Mock server.get_pg_setting('data_directory') call
        backup_manager.server.postgres.get_setting.return_value = '/pg/data'
        # Mock server.get_pg_configuration_files() call
        server.postgres.get_configuration_files.return_value = dict(
            config_file="/etc/postgresql.conf",
            hba_file="/pg/pg_hba.conf",
            ident_file="/pg/pg_ident.conf",
        )
        # Mock server.get_pg_tablespaces() call
        tablespaces = [Tablespace._make(('test_tbs', 1234, '/tbs/test'))]
        server.postgres.get_tablespaces.return_value = tablespaces
        # this is a postgres 9.6
        server.postgres.server_version = 90600

        # Mock call to new api method
        start_time = datetime.datetime.now()
        server.postgres.start_concurrent_backup.return_value = {
            'location': "A257/44B4C0D8",
            'timeline': 6,
            'timestamp': start_time,
        }
        # Build a test empty backup info
        backup_info = BackupInfo(server=backup_manager.server,
                                 backup_id='fake_id2')

        backup_manager.executor.strategy.start_backup(backup_info)

        # Check that all the values are correctly saved inside the BackupInfo
        assert backup_info.pgdata == '/pg/data'
        assert backup_info.config_file == "/etc/postgresql.conf"
        assert backup_info.hba_file == "/pg/pg_hba.conf"
        assert backup_info.ident_file == "/pg/pg_ident.conf"
        assert backup_info.tablespaces == tablespaces
        assert backup_info.status == 'STARTED'
        assert backup_info.timeline == 6
        assert backup_info.begin_xlog == 'A257/44B4C0D8'
        assert backup_info.begin_wal == '000000060000A25700000044'
        assert backup_info.begin_offset == 11845848
        assert backup_info.begin_time == start_time

    def test_exclusive_stop_backup(self):
        """
        Basic test for the stop_backup method

        :param stop_mock: mimic the response od _pg_stop_backup
        """
        # Build a backup info and configure the mocks
        server = build_mocked_server(main_conf={
            'backup_options':
            BackupOptions.EXCLUSIVE_BACKUP
        })
        backup_manager = build_backup_manager(server=server)
        # Mock executor._pg_stop_backup(backup_info) call
        stop_time = datetime.datetime.now()
        server.postgres.stop_exclusive_backup.return_value = {
            'location': "266/4A9C1EF8",
            'file_name': "00000010000002660000004A",
            'file_offset': 10231544,
            'timestamp': stop_time
        }

        backup_info = build_test_backup_info()
        backup_manager.executor.strategy.stop_backup(backup_info)

        # check that the submitted values are stored inside the BackupInfo obj
        assert backup_info.end_xlog == '266/4A9C1EF8'
        assert backup_info.end_wal == '00000010000002660000004A'
        assert backup_info.end_offset == 10231544
        assert backup_info.end_time == stop_time

    @patch('barman.backup_executor.ConcurrentBackupStrategy.'
           '_write_backup_label')
    @patch('barman.backup_executor.ConcurrentBackupStrategy.'
           '_write_tablespace_map')
    def test_pgespresso_stop_backup(self, tbs_map_mock, label_mock):
        """
        Basic test for the pgespresso_stop_backup method
        """
        # Build a backup info and configure the mocks
        server = build_mocked_server(main_conf={
            'backup_options':
            BackupOptions.CONCURRENT_BACKUP
        })
        backup_manager = build_backup_manager(server=server)

        # Mock executor._pgespresso_stop_backup(backup_info) call
        stop_time = datetime.datetime.now()
        server.postgres.server_version = 90500
        server.postgres.pgespresso_stop_backup.return_value = {
            'end_wal': "000000060000A25700000044",
            'timestamp': stop_time
        }

        backup_info = build_test_backup_info(timeline=6)
        backup_manager.executor.strategy.stop_backup(backup_info)

        assert backup_info.end_xlog == 'A257/44FFFFFF'
        assert backup_info.end_wal == '000000060000A25700000044'
        assert backup_info.end_offset == 0xFFFFFF
        assert backup_info.end_time == stop_time

    @patch('barman.backup_executor.ConcurrentBackupStrategy.'
           '_write_backup_label')
    @patch('barman.backup_executor.ConcurrentBackupStrategy.'
           '_write_tablespace_map')
    def test_concurrent_stop_backup(self, tbs_map_mock, label_mock,):
        """
        Basic test for the stop_backup method for 9.6 concurrent api

        :param label_mock: mimic the response of _write_backup_label
        """
        # Build a backup info and configure the mocks
        server = build_mocked_server(main_conf={
            'backup_options':
            BackupOptions.CONCURRENT_BACKUP
        })
        backup_manager = build_backup_manager(server=server)

        stop_time = datetime.datetime.now()
        # This is a pg 9.6
        server.postgres.server_version = 90600
        # Mock stop backup call for the new api method
        start_time = datetime.datetime.now(tz.tzlocal()).replace(microsecond=0)
        server.postgres.stop_concurrent_backup.return_value = {
            'location': "A266/4A9C1EF8",
            'timeline': 6,
            'timestamp': stop_time,
            'backup_label':
                'START WAL LOCATION: A257/44B4C0D8 '
                # Timeline 0 simulates a bug in PostgreSQL 9.6 beta2
                '(file 000000000000A25700000044)\n'
                'START TIME: %s\n' %
                start_time.strftime('%Y-%m-%d %H:%M:%S %Z')
        }

        backup_info = build_test_backup_info()
        backup_manager.executor.strategy.stop_backup(backup_info)

        assert backup_info.end_xlog == 'A266/4A9C1EF8'
        assert backup_info.end_wal == '000000060000A2660000004A'
        assert backup_info.end_offset == 0x9C1EF8
        assert backup_info.end_time == stop_time
        assert backup_info.backup_label == (
            'START WAL LOCATION: A257/44B4C0D8 '
            '(file 000000000000A25700000044)\n'
            'START TIME: %s\n' %
            start_time.strftime('%Y-%m-%d %H:%M:%S %Z')
        )


class TestPostgresBackupExecutor(object):
    """
    This class tests the methods of the executor object hierarchy
    """

    def test_postgres_backup_executor_init(self):
        """
        Test the construction of a PostgresBackupExecutor
        """
        server = build_mocked_server(global_conf={'backup_method': 'postgres'})
        executor = PostgresBackupExecutor(server.backup_manager)
        assert executor
        assert executor.strategy

        # Expect an error if the tablespace_bandwidth_limit option
        # is set for this server.
        server = build_mocked_server(
            global_conf={'backup_method': 'postgres',
                         'tablespace_bandwidth_limit': 1})
        executor = PostgresBackupExecutor(server.backup_manager)
        assert executor
        assert executor.strategy
        assert server.config.disabled

    @patch(
        "barman.backup_executor.PostgresBackupExecutor.backup_copy")
    @patch("barman.backup.BackupManager.get_previous_backup")
    def test_backup(self, gpb_mock, pbc_mock, capsys, tmpdir):
        """
        Test backup

        :param gpb_mock: mock for the get_previous_backup method
        :param pbc_mock: mock for the backup_copy method
        :param capsys: stdout capture module
        :param tmpdir: pytest temp directory
        """
        tmp_home = tmpdir.mkdir('home')
        backup_manager = build_backup_manager(global_conf={
            'barman_home': tmp_home.strpath,
            'backup_method': 'postgres'
        })
        backup_info = build_test_backup_info(
            backup_id='fake_backup_id',
            server=backup_manager.server,
            pgdata="/pg/data",
            config_file="/pg/data/postgresql.conf",
            hba_file="/pg/data/pg_hba.conf",
            ident_file="/pg/pg_ident.conf",
            begin_offset=28)
        timestamp = datetime.datetime(2015, 10, 26, 14, 38)
        backup_manager.server.postgres.current_xlog_info = dict(
            location='0/12000090',
            file_name='000000010000000000000012',
            file_offset=144,
            timestamp=timestamp,
        )
        backup_manager.server.postgres.get_setting.return_value = '/pg/data'
        tmp_backup_label = tmp_home.mkdir('main')\
            .mkdir('base').mkdir('fake_backup_id')\
            .mkdir('data').join('backup_label')
        start_time = datetime.datetime.now(tz.tzlocal()).replace(microsecond=0)
        tmp_backup_label.write(
            'START WAL LOCATION: 0/40000028 (file 000000010000000000000040)\n'
            'CHECKPOINT LOCATION: 0/40000028\n'
            'BACKUP METHOD: streamed\n'
            'BACKUP FROM: master\n'
            'START TIME: %s\n'
            'LABEL: pg_basebackup base backup' %
            start_time.strftime('%Y-%m-%d %H:%M:%S %Z')
        )
        backup_manager.executor.backup(backup_info)
        out, err = capsys.readouterr()
        gpb_mock.assert_called_once_with(backup_info.backup_id)
        assert err == ''
        assert 'Copying files.' in out
        assert 'Copy done.' in out
        assert 'Finalising the backup.' in out
        assert backup_info.end_xlog == '0/12000090'
        assert backup_info.end_offset == 144
        assert backup_info.begin_time == start_time
        assert backup_info.begin_wal == '000000010000000000000040'

        # Check the CommandFailedException re raising
        with pytest.raises(CommandFailedException):
            pbc_mock.side_effect = CommandFailedException('test')
            backup_manager.executor.backup(backup_info)

    @patch("barman.backup_executor.PostgresBackupExecutor.get_remote_status")
    def test_check(self, remote_status_mock):
        """
        Very simple and basic test for the check method
        :param remote_status_mock: mock for the get_remote_status method
        """
        remote_status_mock.return_value = {
            'pg_basebackup_compatible': True,
            'pg_basebackup_installed': True,
            'pg_basebackup_path': '/fake/path',
            'pg_basebackup_bwlimit': True,
            'pg_basebackup_version': '9.5',
            'pg_basebackup_tbls_mapping': True,
        }
        check_strat = CheckStrategy()
        backup_manager = build_backup_manager(global_conf={
            'backup_method': 'postgres'
        })
        backup_manager.server.postgres.server_txt_version = '9.5'
        backup_manager.executor.check(check_strategy=check_strat)
        # No errors detected
        assert check_strat.has_error is not True

        remote_status_mock.reset_mock()
        remote_status_mock.return_value = {
            'pg_basebackup_compatible': False,
            'pg_basebackup_installed': True,
            'pg_basebackup_path': True,
            'pg_basebackup_bwlimit': True,
            'pg_basebackup_version': '9.5',
            'pg_basebackup_tbls_mapping': True,
        }
        check_strat = CheckStrategy()
        backup_manager.executor.check(check_strategy=check_strat)
        # Error present because of the 'pg_basebackup_compatible': False
        assert check_strat.has_error is True

        # Even if pg_backup has no tbls_mapping option the check
        # succeeds if the server doesn't have any tablespaces
        remote_status_mock.reset_mock()
        remote_status_mock.return_value = {
            'pg_basebackup_compatible': True,
            'pg_basebackup_installed': True,
            'pg_basebackup_path': True,
            'pg_basebackup_bwlimit': True,
            'pg_basebackup_version': '9.3',
            'pg_basebackup_tbls_mapping': False,
        }
        check_strat = CheckStrategy()
        backup_manager.server.postgres.get_tablespaces.return_value = []
        backup_manager.executor.check(check_strategy=check_strat)
        assert check_strat.has_error is False

        # This check fails because the server contains tablespaces and
        # pg_basebackup doesn't support the tbls_mapping option
        remote_status_mock.reset_mock()
        remote_status_mock.return_value = {
            'pg_basebackup_compatible': True,
            'pg_basebackup_installed': True,
            'pg_basebackup_path': True,
            'pg_basebackup_bwlimit': True,
            'pg_basebackup_version': '9.3',
            'pg_basebackup_tbls_mapping': False,
        }
        check_strat = CheckStrategy()
        backup_manager.server.postgres.get_tablespaces.return_value = [True]
        backup_manager.executor.check(check_strategy=check_strat)
        assert check_strat.has_error is True

    @mock.patch("barman.command_wrappers.Command")
    def test_fetch_remote_status(self, cmd_mock):
        """
        Test the fetch_remote_status method
        :param cmd_mock: mock the Command class
        """
        backup_manager = build_backup_manager(global_conf={
            'backup_method': 'postgres'
        })
        # Simulate the absence of pg_basebackup
        cmd_mock.side_effect = CommandFailedException
        backup_manager.server.streaming.server_major_version = '9.5'
        remote = backup_manager.executor.fetch_remote_status()
        assert remote['pg_basebackup_installed'] is False
        assert remote['pg_basebackup_path'] is None

        # Simulate the presence of pg_basebackup 9.5.1 and pg 95
        cmd_mock.side_effect = None
        cmd_mock.return_value.cmd = '/fake/path'
        backup_manager.server.streaming.server_major_version = '9.5'
        backup_manager.server.path = 'fake/path2'
        cmd_mock.return_value.out = '9.5.1'
        remote = backup_manager.executor.fetch_remote_status()
        assert remote['pg_basebackup_installed'] is True
        assert remote['pg_basebackup_path'] == '/fake/path'
        assert remote['pg_basebackup_version'] == '9.5.1'
        assert remote['pg_basebackup_compatible'] is True
        assert remote['pg_basebackup_tbls_mapping'] is True

        # Simulate the presence of pg_basebackup 9.5.1 and no Pg
        backup_manager.server.streaming.server_major_version = None
        cmd_mock.reset_mock()
        cmd_mock.return_value.out = '9.5.1'
        remote = backup_manager.executor.fetch_remote_status()
        assert remote['pg_basebackup_installed'] is True
        assert remote['pg_basebackup_path'] == '/fake/path'
        assert remote['pg_basebackup_version'] == '9.5.1'
        assert remote['pg_basebackup_compatible'] is None
        assert remote['pg_basebackup_tbls_mapping'] is True

        # Simulate the presence of pg_basebackup 9.3.3 and Pg 9.5
        backup_manager.server.streaming.server_major_version = '9.5'
        cmd_mock.reset_mock()
        cmd_mock.return_value.out = '9.3.3'
        remote = backup_manager.executor.fetch_remote_status()
        assert remote['pg_basebackup_installed'] is True
        assert remote['pg_basebackup_path'] == '/fake/path'
        assert remote['pg_basebackup_version'] == '9.3.3'
        assert remote['pg_basebackup_compatible'] is False
        assert remote['pg_basebackup_tbls_mapping'] is False

    @patch("barman.backup_executor.PgBaseBackup")
    @patch("barman.backup_executor.PostgresBackupExecutor.fetch_remote_status")
    def test_backup_copy(self, remote_mock, pg_basebackup_mock,
                         tmpdir, capsys):
        """
        Test backup folder structure

        :param remote_mock: mock for the fetch_remote_status method
        :param pg_basebackup_mock: mock for the PgBaseBackup object
        :param tmpdir: pytest temp directory
        """
        backup_manager = build_backup_manager(global_conf={
            'barman_home': tmpdir.mkdir('home').strpath,
            'backup_method': 'postgres'
        })
        # simulate a old version of pg_basebackup
        # not supporting bandwidth_limit
        remote_mock.return_value = {
            'pg_basebackup_version': '9.2',
            'pg_basebackup_path': '/fake/path',
            'pg_basebackup_bwlimit': False,
        }
        server_mock = backup_manager.server
        streaming_mock = server_mock.streaming
        server_mock.config.bandwidth_limit = 1
        streaming_mock.get_connection_string.return_value = 'fake=connstring'
        streaming_mock.conn_parameters = {
            'host': 'fakeHost',
            'port': 'fakePort',
            'user': 'fakeUser'
        }
        backup_info = build_test_backup_info(server=backup_manager.server,
                                             backup_id='fake_backup_id')
        backup_manager.executor.backup_copy(backup_info)
        out, err = capsys.readouterr()
        assert out == ''
        assert err == ''
        # check that the bwlimit option have been ignored
        assert pg_basebackup_mock.mock_calls == [
            mock.call(
                connection=mock.ANY,
                version='9.2',
                app_name='barman_streaming_backup',
                destination=mock.ANY,
                command='/fake/path',
                tbs_mapping=mock.ANY,
                bwlimit=None,
                immediate=False,
                retry_times=0,
                retry_sleep=30,
                retry_handler=mock.ANY,
                path=mock.ANY),
            mock.call()(),
        ]

        # Check with newer version
        remote_mock.reset_mock()
        pg_basebackup_mock.reset_mock()
        backup_manager.executor._remote_status = None
        remote_mock.return_value = {
            'pg_basebackup_version': '9.5',
            'pg_basebackup_path': '/fake/path',
            'pg_basebackup_bwlimit': True,
        }
        backup_manager.executor.config.immediate_checkpoint = True
        backup_manager.executor.config.streaming_conninfo = 'fake=connstring'
        backup_manager.executor.backup_copy(backup_info)
        out, err = capsys.readouterr()
        assert out == ''
        assert err == ''
        # check that the bwlimit option have been passed to the test call
        assert pg_basebackup_mock.mock_calls == [
            mock.call(
                connection=mock.ANY,
                version='9.5',
                app_name='barman_streaming_backup',
                destination=mock.ANY,
                command='/fake/path',
                tbs_mapping=mock.ANY,
                bwlimit=1,
                immediate=True,
                retry_times=0,
                retry_sleep=30,
                retry_handler=mock.ANY,
                path=mock.ANY),
            mock.call()(),
        ]

        # Check with a config file outside the data directory
        remote_mock.reset_mock()
        pg_basebackup_mock.reset_mock()
        backup_info.ident_file = '/pg/pg_ident.conf'
        backup_manager.executor.backup_copy(backup_info)
        out, err = capsys.readouterr()
        assert out == ''
        assert err.strip() == 'WARNING: pg_basebackup does not copy ' \
                              'the PostgreSQL configuration files that '\
                              'reside outside PGDATA. ' \
                              'Please manually backup the following files:' \
                              '\n\t/pg/pg_ident.conf'
        # check that the bwlimit option have been passed to the test call
        assert pg_basebackup_mock.mock_calls == [
            mock.call(
                connection=mock.ANY,
                version='9.5',
                app_name='barman_streaming_backup',
                destination=mock.ANY,
                command='/fake/path',
                tbs_mapping=mock.ANY,
                bwlimit=1,
                immediate=True,
                retry_times=0,
                retry_sleep=30,
                retry_handler=mock.ANY,
                path=mock.ANY),
            mock.call()(),
        ]

        # Check with a config file outside the data directory and
        # external_configurations backup option
        remote_mock.reset_mock()
        pg_basebackup_mock.reset_mock()
        backup_manager.config.backup_options.add(
            BackupOptions.EXTERNAL_CONFIGURATION)
        backup_manager.executor.backup_copy(backup_info)
        out, err = capsys.readouterr()
        assert out == ''
        assert err == ''
        # check that the bwlimit option have been passed to the test call
        assert pg_basebackup_mock.mock_calls == [
            mock.call(
                connection=mock.ANY,
                version='9.5',
                app_name='barman_streaming_backup',
                destination=mock.ANY,
                command='/fake/path',
                tbs_mapping=mock.ANY,
                bwlimit=1,
                immediate=True,
                retry_times=0,
                retry_sleep=30,
                retry_handler=mock.ANY,
                path=mock.ANY),
            mock.call()(),
        ]

        # Raise a test CommandFailedException and expect it to be wrapped
        # inside a DataTransferFailure exception
        remote_mock.reset_mock()
        pg_basebackup_mock.reset_mock()
        pg_basebackup_mock.return_value.side_effect = \
            CommandFailedException(dict(ret='ret', out='out', err='err'))
        with pytest.raises(DataTransferFailure):
            backup_manager.executor.backup_copy(backup_info)

    def test_postgres_start_backup(self):
        """
        Test concurrent backup using pg_basebackup
        """
        # Test: start concurrent backup
        backup_manager = build_backup_manager(global_conf={
            'backup_method': 'postgres'
        })
        # Mock server.get_pg_setting('data_directory') call
        postgres_mock = backup_manager.server.postgres
        postgres_mock.get_setting.side_effect = [
            '/test/fake_data_dir',
        ]
        # Mock server.get_pg_configuration_files() call
        postgres_mock.get_configuration_files.return_value = dict(
            config_file="/etc/postgresql.conf",
            hba_file="/pg/pg_hba.conf",
            ident_file="/pg/pg_ident.conf",
        )
        # Mock server.get_pg_tablespaces() call
        tablespaces = [Tablespace._make(('test_tbs', 1234, '/tbs/test'))]
        postgres_mock.get_tablespaces.return_value = tablespaces
        # this is a postgres 9.5
        postgres_mock.server_version = 90500

        # Mock call to new api method
        start_time = datetime.datetime.now()
        postgres_mock.current_xlog_info = {
            'location': "A257/44B4C0D8",
            'timestamp': start_time,
        }
        # Build a test empty backup info
        backup_info = BackupInfo(server=backup_manager.server,
                                 backup_id='fake_id2')

        backup_manager.executor.strategy.start_backup(backup_info)

        # Check that all the values are correctly saved inside the BackupInfo
        assert backup_info.pgdata == '/test/fake_data_dir'
        assert backup_info.config_file == "/etc/postgresql.conf"
        assert backup_info.hba_file == "/pg/pg_hba.conf"
        assert backup_info.ident_file == "/pg/pg_ident.conf"
        assert backup_info.tablespaces == tablespaces
        assert backup_info.status == 'STARTED'
        assert backup_info.timeline is None
        assert backup_info.begin_xlog == 'A257/44B4C0D8'
        assert backup_info.begin_wal is None
        assert backup_info.begin_offset is None
        assert backup_info.begin_time == start_time
