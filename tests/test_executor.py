# Copyright (C) 2013-2015 2ndQuadrant Italia (Devise.IT S.r.L.)
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

from mock import patch, Mock, call, ANY
import psycopg2
import pytest

from barman.backup_executor import RsyncBackupExecutor, SshCommandException
from barman.config import BackupOptions
from barman.infofile import BackupInfo, Tablespace
from testing_helpers import build_backup_manager, build_mocked_server, \
    build_test_backup_info


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

    def test_pg_start_backup(self):
        """
        Simple test for pg_start_backup method of the RsyncBackupExecutor class
        """
        # Build and configure a server using a mock
        backup_manager = build_backup_manager()
        backup_label = 'test label'

        # Expect an exception for a non correctly configured server
        with pytest.raises(Exception):
            backup_manager.executor.pg_start_backup(backup_label)

        # Expect no error and the correct call sequence
        backup_manager.server.reset_mock()
        backup_manager.server.pg_is_in_recovery.return_value = False
        backup_manager.server.server_version = 90300
        backup_manager.executor.pg_start_backup(backup_label)

        pg_connect = call.pg_connect()
        with_pg_connect = pg_connect.__enter__()
        cursor = with_pg_connect.cursor()
        assert backup_manager.server.mock_calls == [
            pg_connect,
            with_pg_connect,
            call.pg_is_in_recovery(),
            cursor,
            cursor.execute(ANY, ANY),
            cursor.fetchone(),
            pg_connect.__exit__(None, None, None)]

        # Change server version and expect no error and the correct call
        # sequence
        backup_manager.server.reset_mock()
        backup_manager.server.server_version = 80300
        backup_manager.executor.pg_start_backup(backup_label)

        assert backup_manager.server.mock_calls == [
            pg_connect,
            with_pg_connect,
            call.pg_is_in_recovery(),
            cursor,
            cursor.execute(ANY, ANY),
            cursor.fetchone(),
            pg_connect.__exit__(None, None, None)]

    def test_pgespresso_start_backup(self):
        """
        Simple test for pgespresso_start_backup method
        of the RsyncBackupExecutor class
        """
        # Build and configure a server using a mock
        backup_manager = build_backup_manager()
        backup_manager.config.backup_options = [BackupOptions.CONCURRENT_BACKUP]
        backup_label = 'test label'

        # Expect an exception because pg_espresso is not installed
        backup_manager.server.pg_espresso_installed.return_value = False
        with pytest.raises(Exception):
            backup_manager.executor.pgespresso_start_backup(backup_label)

        # Report pg_expresso installed. Expect no error and the correct call
        # sequence
        backup_manager.server.reset_mock()
        backup_manager.executor.server.pg_espresso_installed.return_value = True
        backup_manager.executor.pgespresso_start_backup(backup_label)

        pg_connect = call.pg_connect()
        with_pg_connect = pg_connect.__enter__()
        cursor = with_pg_connect.cursor()
        assert backup_manager.server.mock_calls == [
            pg_connect,
            with_pg_connect,
            call.pg_espresso_installed(),
            cursor,
            cursor.execute(ANY, ANY),
            cursor.fetchone(),
            pg_connect.__exit__(None, None, None)]

    @patch('barman.backup_executor.RsyncBackupExecutor.pg_start_backup')
    @patch('barman.backup_executor.RsyncBackupExecutor.pgespresso_start_backup')
    def test_start_backup(self, espresso_start_mock, start_mock):
        """
        Basic test for the start_backup method

        :param espresso_start_mock: mock for the pgespresso_start_backup
        :param start_mock: mock for the pg_start_backup
        """
        # Build a backup_manager using a mocked server
        backup_manager = build_backup_manager(name='test-server')

        # Mock server.get_pg_setting('data_directory') call
        backup_manager.server.get_pg_setting.return_value = '/pg/data'
        # Mock server.get_pg_configuration_files() call
        backup_manager.server.get_pg_configuration_files.return_value = dict(
            config_file="/etc/postgresql.conf",
            hba_file="/pg/pg_hba.conf",
            ident_file="/pg/pg_ident.conf",
        )
        # Mock server.get_pg_tablespaces() call
        tablespaces = [Tablespace._make(('test_tbs', 1234, '/tbs/test'))]
        backup_manager.server.get_pg_tablespaces.return_value = tablespaces

        # Test 1: start exclusive backup
        # Mock executor.pg_start_backup(label) call
        start_time = datetime.datetime.now()
        start_mock.return_value = ("A257/44B4C0D8", "000000060000A25700000044",
                                   11845848, start_time)

        # Build a test empty backup info
        backup_info = BackupInfo(server=backup_manager.server,
                                 backup_id='fake_id')

        backup_manager.executor.start_backup(backup_info)

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
        # Check that the correct call to pg_start_backup has been made
        start_mock.assert_called_with('Barman backup test-server fake_id')
        assert not espresso_start_mock.called

        # Test 2: start concurrent backup
        # change the configuration to concurrent backup
        backup_manager.executor.config.backup_options = [
            BackupOptions.CONCURRENT_BACKUP]

        # Reset mock executor.pg_start_backup(label) call
        start_mock.reset_mock()

        # Mock executor.pgespresso_start_backup(label) call
        start_time = datetime.datetime.now()
        espresso_start_mock.return_value = ("START WAL LOCATION: 266/4A9C1EF8 "
                                            "(file 00000010000002660000004A)",
                                            start_time)
        # Build a test empty backup info
        backup_info = BackupInfo(server=backup_manager.server,
                                 backup_id='fake_id2')

        backup_manager.executor.start_backup(backup_info)

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
        espresso_start_mock.assert_called_with(
            'Barman backup test-server fake_id2')
        assert not start_mock.called

    def test_pg_stop_backup(self):
        """
        Basic test for the pg_stop_backup method
        """
        # Build a backup info and configure the mocks
        backup_manager = build_backup_manager()

        # Test 1: Expect no error and the correct call sequence
        backup_manager.executor.pg_stop_backup()

        pg_connect = call.pg_connect()
        with_pg_connect = pg_connect.__enter__()
        cursor = with_pg_connect.cursor()
        assert backup_manager.server.mock_calls == [
            pg_connect,
            with_pg_connect,
            cursor,
            cursor.execute(ANY),
            cursor.fetchone(),
            pg_connect.__exit__(None, None, None)]

        # Test 2: Setup the mock to trigger an exception
        backup_manager.executor.server.reset_mock()
        backup_manager.executor.server.pg_connect.return_value. \
            __enter__.return_value. \
            cursor.return_value.\
            execute.side_effect = psycopg2.Error

        # Check that the method returns None as result
        assert backup_manager.executor.pg_stop_backup() is None
        assert backup_manager.server.mock_calls == [
            pg_connect,
            with_pg_connect,
            cursor,
            cursor.execute(ANY),
            pg_connect.__exit__(None, None, None)]

    def test_pgespresso_stop_backup(self):
        """
        Basic test for pgespresso_stop_backup
        """
        # Build a backup info and configure the mocks
        backup_manager = build_backup_manager()

        # Test 1: Expect no error and the correct call sequence
        backup_manager.executor.pgespresso_stop_backup('test_label')

        pg_connect = call.pg_connect()
        with_pg_connect = pg_connect.__enter__()
        cursor = with_pg_connect.cursor()
        assert backup_manager.server.mock_calls == [
            pg_connect,
            with_pg_connect,
            cursor,
            cursor.execute(ANY, ('test_label',)),
            cursor.fetchone(),
            pg_connect.__exit__(None, None, None)]

        # Test 2: Setup the mock to trigger an exception
        backup_manager.executor.server.reset_mock()
        backup_manager.executor.server.pg_connect.return_value. \
            __enter__.return_value. \
            cursor.return_value.\
            execute.side_effect = psycopg2.Error

        assert backup_manager.executor.pgespresso_stop_backup(
            'test_label1') is None
        assert backup_manager.server.mock_calls == [
            pg_connect,
            with_pg_connect,
            cursor,
            cursor.execute(ANY, ('test_label1',)),
            pg_connect.__exit__(None, None, None)]

    @patch('barman.backup_executor.RsyncBackupExecutor.pg_stop_backup')
    @patch('barman.backup_executor.RsyncBackupExecutor.pgespresso_stop_backup')
    def test_stop_backup(self, espressostop_mock, stop_mock):
        """
        Basic test for the start_backup method

        :param espressostop_mock: mimic the response of pg_espresso_stop_backup
        :param stop_mock: mimic the response od pg_stop_backup
        """
        # Build a backup info and configure the mocks
        backup_manager = build_backup_manager()

        # Test 1: stop exclusive backup
        # Mock executor.pg_stop_backup(backup_info) call
        stop_time = datetime.datetime.now()
        stop_mock.return_value = ("266/4A9C1EF8",
                                  "00000010000002660000004A",
                                  10231544,
                                  stop_time)

        backup_info = build_test_backup_info()
        backup_manager.executor.stop_backup(backup_info)

        # check that the submitted values are stored inside the BackupInfo obj
        assert backup_info.end_xlog == '266/4A9C1EF8'
        assert backup_info.end_wal == '00000010000002660000004A'
        assert backup_info.end_offset == 10231544
        assert backup_info.end_time == stop_time

        # Test 2: stop concurrent backup
        # change the configuration to concurrent backup
        backup_manager.executor.config.backup_options = [
            BackupOptions.CONCURRENT_BACKUP]

        # Mock executor.pgespresso_stop_backup(backup_info) call
        stop_time = datetime.datetime.now()
        espressostop_mock.return_value = ("000000060000A25700000044", stop_time)

        backup_info = build_test_backup_info()
        backup_manager.executor.stop_backup(backup_info)

        assert backup_info.end_xlog == 'A257/45000000'
        assert backup_info.end_wal == '000000060000A25700000044'
        assert backup_info.end_offset == 0
        assert backup_info.end_time == stop_time

    def test_reuse_dir(self):
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
        assert backup_manager.executor.reuse_dir(backup_info) is None

        # check for the expected path with copy
        backup_manager.executor.config.reuse_backup = 'copy'
        assert backup_manager.executor.reuse_dir(backup_info) == \
            '/some/barman/home/main/base/1234567890/data'

        # check for the expected path with link
        backup_manager.executor.config.reuse_backup = 'link'
        assert backup_manager.executor.reuse_dir(backup_info) == \
            '/some/barman/home/main/base/1234567890/data'

    def test_reuse_args(self):
        """
        Simple test for the reuse_args method

        The method is necessary for the execution of incremental backups,
        we need to test that the method build correctly the rsync option that
        enables the incremental backup
        """
        backup_manager = build_backup_manager()
        reuse_dir = "some/dir"

        # Test for disabled incremental
        assert backup_manager.executor.reuse_args(reuse_dir) == []

        # Test for link incremental
        backup_manager.executor.config.reuse_backup = 'link'
        assert backup_manager.executor.reuse_args(reuse_dir) == \
            ['--link-dest=some/dir']

        # Test for copy incremental
        backup_manager.executor.config.reuse_backup = 'copy'
        assert backup_manager.executor.reuse_args(reuse_dir) == \
            ['--copy-dest=some/dir']

    @patch('barman.backup_executor.Command')
    def test_check(self, command_mock, capsys):
        """
        Check the ssh connection to a remote server
        """
        backup_manager = build_backup_manager()

        # Test 1: ssh ok
        command_mock.return_value.return_value = 0
        backup_manager.executor.check()
        out, err = capsys.readouterr()
        assert err == ''
        assert 'ssh: OK' in out

        # Test 2: ssh ok
        command_mock.return_value.return_value = 1
        backup_manager.executor.check()
        out, err = capsys.readouterr()
        assert err == ''
        assert 'ssh: FAILED' in out

    @patch("barman.backup_executor.RsyncBackupExecutor.start_backup")
    @patch("barman.backup_executor.RsyncBackupExecutor.stop_backup")
    @patch("barman.backup.BackupManager.retry_backup_copy")
    @patch("barman.backup.BackupManager.get_previous_backup")
    @patch("barman.backup.BackupManager.remove_wal_before_backup")
    def test_backup(self, rwbb_mock, gpb_mock, retry_mock,
                    stop_mock, start_mock, capsys, tmpdir):
        """
        Test the execution of a backup

        :param rwbb_mock: mock for the remove_wal_before_backup method
        :param gpb_mock: mock for the get_previous_backup method
        :param retry_mock: mock for the retry_backup_copy method
        :param stop_mock: mock for stop_backup method
        :param start_mock: mock for start_backup method
        :param capsys: stdout capture module
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

        # Test 1: exclusive backup
        backup_manager.executor.backup(backup_info)
        out, err = capsys.readouterr()
        assert err == ''
        assert "Backup start at xlog location: " \
               "0/2000028 (000000010000000000000002, 00000028)\n" \
               "Copying files.\n" \
               "Copy done." in out

        gpb_mock.assert_called_once_with(backup_info.backup_id)
        rwbb_mock.assert_called_once_with(backup_info)
        start_mock.assert_called_once_with(backup_info)
        retry_mock.assert_called_once_with(
            backup_manager.executor.backup_copy, backup_info)
        stop_mock.assert_called_once_with(backup_info)

        # Test 2: concurrent backup
        # change the configuration to concurrent backup
        backup_manager.executor.config.backup_options = [
            BackupOptions.CONCURRENT_BACKUP]

        # reset mocks
        gpb_mock.reset_mock()
        rwbb_mock.reset_mock()
        start_mock.reset_mock()
        retry_mock.reset_mock()
        stop_mock.reset_mock()

        # prepare data directory for backup_label generation
        backup_info.backup_label = 'test\nlabel\n'
        pg_data = tmpdir.join(backup_info.get_data_directory(), abs=1).mkdir()

        backup_manager.executor.backup(backup_info)
        out, err = capsys.readouterr()
        assert err == ''
        assert "Backup start at xlog location: " \
               "0/2000028 (000000010000000000000002, 00000028)\n" \
               "Copying files.\n" \
               "Copy done." in out

        gpb_mock.assert_called_once_with(backup_info.backup_id)
        rwbb_mock.assert_called_once_with(backup_info)
        start_mock.assert_called_once_with(backup_info)
        retry_mock.assert_called_once_with(
            backup_manager.executor.backup_copy, backup_info)
        stop_mock.assert_called_once_with(backup_info)

        # check the backup label content
        assert pg_data.join('backup_label').read() == 'test\nlabel\n'

    @patch('barman.backup_executor.RsyncPgData')
    def test_backup_copy(self, rsync_mock, tmpdir):
        """
        Test the execution of a rsync copy

        :param rsync_mock: mock for the rsync command
        :param tmpdir: temporary dir
        """
        backup_manager = build_backup_manager(global_conf={
            'barman_home': tmpdir.mkdir('home').strpath
        })
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
            call(check=True, network_compression=False, args=[], bwlimit=None,
                 ssh='ssh', ssh_options=['-c', '"arcfour"', '-p', '22',
                                         'postgres@pg01.nowhere', '-o',
                                         'BatchMode=yes', '-o',
                                         'StrictHostKeyChecking=no']),
            call().smart_copy(':/fake/location/',
                              backup_info.get_data_directory(16387),
                              None, None),
            call(check=True, network_compression=False, args=[], bwlimit=None,
                 ssh='ssh', ssh_options=['-c', '"arcfour"', '-p', '22',
                                         'postgres@pg01.nowhere', '-o',
                                         'BatchMode=yes', '-o',
                                         'StrictHostKeyChecking=no']),
            call().smart_copy(':/another/location/',
                              backup_info.get_data_directory(16405),
                              None, None),
            call(network_compression=False,
                 exclude_and_protect=['/pg_tblspc/16387', '/pg_tblspc/16405'],
                 args=[], bwlimit=None, ssh='ssh',
                 ssh_options=['-c', '"arcfour"', '-p', '22',
                              'postgres@pg01.nowhere', '-o', 'BatchMode=yes',
                              '-o', 'StrictHostKeyChecking=no']),
            call().smart_copy(':/pg/data/',
                              backup_info.get_data_directory(),
                              None, None),
            call()(':/pg/data/global/pg_control',
                   '%s/global/pg_control' % backup_info.get_data_directory()),
            call()(':/etc/postgresql.conf', backup_info.get_data_directory())]
