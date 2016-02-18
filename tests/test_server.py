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
from collections import defaultdict

import pytest
from mock import MagicMock, patch

from barman.infofile import BackupInfo, WalFileInfo
from barman.lockfile import (LockFileBusy, LockFilePermissionDenied,
                             ServerBackupLock, ServerCronLock,
                             ServerWalArchiveLock, ServerWalReceiveLock)
from barman.process import ProcessInfo
from barman.server import CheckOutputStrategy, CheckStrategy, Server
from testing_helpers import (build_config_from_dicts, build_real_server,
                             build_test_backup_info)


class ExceptionTest(Exception):
    """
    Exception for test purposes
    """
    pass


# noinspection PyMethodMayBeStatic
class TestServer(object):

    def test_init(self):
        """
        Basic initialization test with minimal parameters
        """
        server = Server(build_config_from_dicts().get_server('main'))
        assert not server.config.disabled

    def test_bad_init(self):
        """
        Check the server is buildable with an empty configuration
        """
        server = Server(build_config_from_dicts(
            main_conf={
                'conninfo': '',
                'ssh_command': '',
            }
        ).get_server('main'))
        assert server.config.disabled

    def test_check_config_missing(self):
        """
        Verify the check method can be called on an empty configuration
        """
        server = Server(build_config_from_dicts(
            main_conf={
                'conninfo': '',
                'ssh_command': '',
            }
        ).get_server('main'))
        check_strategy = CheckOutputStrategy()
        server.check(check_strategy)
        assert check_strategy.has_error

    @patch('barman.server.os')
    def test_xlogdb_with_exception(self, os_mock, tmpdir):
        """
        Testing the execution of xlog-db operations with an Exception

        :param os_mock: mock for os module
        :param tmpdir: temporary directory unique to the test invocation
        """
        # unpatch os.path
        os_mock.path = os.path
        # Setup temp dir and server
        server = build_real_server(
            global_conf={
                "barman_lock_directory": tmpdir.mkdir('lock').strpath
            },
            main_conf={
                "wals_directory": tmpdir.mkdir('wals').strpath
            })
        # Test the execution of the fsync on xlogdb file forcing an exception
        with pytest.raises(ExceptionTest):
            with server.xlogdb('w') as fxlogdb:
                fxlogdb.write("00000000000000000000")
                raise ExceptionTest()
        # Check call on fsync method. If the call have been issued,
        # the "exit" section of the contextmanager have been executed
        assert os_mock.fsync.called

    @patch('barman.server.os')
    @patch('barman.server.ServerXLOGDBLock')
    def test_xlogdb(self, lock_file_mock, os_mock, tmpdir):
        """
        Testing the normal execution of xlog-db operations.

        :param lock_file_mock: mock for LockFile object
        :param os_mock: mock for os module
        :param tmpdir: temporary directory unique to the test invocation
        """
        # unpatch os.path
        os_mock.path = os.path
        # Setup temp dir and server
        server = build_real_server(
            global_conf={
                "barman_lock_directory": tmpdir.mkdir('lock').strpath
            },
            main_conf={
                "wals_directory": tmpdir.mkdir('wals').strpath
            })
        # Test the execution of the fsync on xlogdb file
        with server.xlogdb('w') as fxlogdb:
            fxlogdb.write("00000000000000000000")
        # Check for calls on fsync method. If the call have been issued
        # the "exit" method of the contextmanager have been executed
        assert os_mock.fsync.called
        # Check for enter and exit calls on mocked LockFile
        lock_file_mock.return_value.__enter__.assert_called_once_with()
        lock_file_mock.return_value.__exit__.assert_called_once_with(
            None, None, None)

        os_mock.fsync.reset_mock()
        with server.xlogdb():
            # nothing to do here.
            pass
        # Check for calls on fsync method.
        # If the file is readonly exit method of the context manager must
        # skip calls on fsync method
        assert not os_mock.fsync.called

    def test_get_wal_full_path(self, tmpdir):
        """
        Testing Server.get_wal_full_path() method
        """
        wal_name = '0000000B00000A36000000FF'
        wal_hash = wal_name[:16]
        server = build_real_server(
            global_conf={
                "barman_lock_directory": tmpdir.mkdir('lock').strpath
            },
            main_conf={
                "wals_directory": tmpdir.mkdir('wals').strpath
            })
        full_path = server.get_wal_full_path(wal_name)
        assert full_path == \
            str(tmpdir.join('wals').join(wal_hash).join(wal_name))

    @patch("barman.server.Server.get_next_backup")
    def test_get_wal_until_next_backup(self, get_backup_mock, tmpdir):
        """
        Simple test for the management of .history files
        """
        # build a WalFileInfo object
        wfile_info = WalFileInfo()
        wfile_info.name = '000000010000000000000003'
        wfile_info.size = 42
        wfile_info.time = 43
        wfile_info.compression = None

        # build a WalFileInfo history object
        history_info = WalFileInfo()
        history_info.name = '00000001.history'
        history_info.size = 42
        history_info.time = 43
        history_info.compression = None

        # create a xlog.db and add the 2 entries
        wals_dir = tmpdir.mkdir("wals")
        xlog = wals_dir.join("xlog.db")
        xlog.write(wfile_info.to_xlogdb_line() + history_info.to_xlogdb_line())
        # facke backup
        backup = build_test_backup_info(
            begin_wal='000000010000000000000001',
            end_wal='000000010000000000000004')

        # mock a server object and mock a return call to get_next_backup method
        server = build_real_server(
            global_conf={
                "barman_lock_directory": tmpdir.mkdir('lock').strpath
            },
            main_conf={
                "wals_directory": wals_dir.strpath
            })
        get_backup_mock.return_value = build_test_backup_info(
            backup_id="1234567899",
            begin_wal='000000010000000000000005',
            end_wal='000000010000000000000009')

        wals = []
        for wal_file in server.get_wal_until_next_backup(backup,
                                                         include_history=True):
            # get the result of the xlogdb read
            wals.append(wal_file.name)
        # check for the presence of the .history file
        assert history_info.name in wals

    @patch('barman.server.Server.get_remote_status')
    def test_pg_stat_archiver_output(self, remote_mock, capsys):
        """
        Test management of pg_stat_archiver view output

        :param MagicMock connect_mock: mock the database connection
        :param capsys: retrieve output from consolle

        """
        stats = {
            "failed_count": "2",
            "last_archived_wal": "000000010000000000000006",
            "last_archived_time": datetime.datetime.now(),
            "last_failed_wal": "000000010000000000000005",
            "last_failed_time": datetime.datetime.now(),
            "current_archived_wals_per_second": 1.0002,
        }
        remote_mock.return_value = dict(stats)

        server = build_real_server()
        server.server_version = 90400
        server.config.description = None
        server.config.KEYS = []
        server.config.last_backup_maximum_age = datetime.timedelta(days=1)
        # Mock the BackupExecutor.get_remote_status() method
        server.backup_manager.executor.get_remote_status = MagicMock(
            return_value={})

        # testing for show-server command.
        # Expecting in the output the same values present into the stats dict
        server.show()
        (out, err) = capsys.readouterr()
        assert err == ''
        result = dict(item.strip('\t\n\r').split(": ")
                      for item in out.split("\n") if item != '')
        assert result['failed_count'] == stats['failed_count']
        assert result['last_archived_wal'] == stats['last_archived_wal']
        assert result['last_archived_time'] == str(stats['last_archived_time'])
        assert result['last_failed_wal'] == stats['last_failed_wal']
        assert result['last_failed_time'] == str(stats['last_failed_time'])
        assert result['current_archived_wals_per_second'] == \
            str(stats['current_archived_wals_per_second'])

        # test output for status
        # Expecting:
        # Last archived WAL:
        #   <last_archived_wal>, at <last_archived_time>
        # Failures of WAL archiver:
        #   <failed_count> (<last_failed wal>, at <last_failed_time>)
        remote_mock.return_value = defaultdict(lambda: None,
                                               server_txt_version=1,
                                               **stats)
        server.status()
        (out, err) = capsys.readouterr()
        # clean the output
        result = dict(item.strip('\t\n\r').split(": ")
                      for item in out.split("\n") if item != '')
        assert err == ''
        # check the result
        assert result['Last archived WAL'] == '%s, at %s' % (
            stats['last_archived_wal'], stats['last_archived_time'].ctime()
        )
        assert result['Failures of WAL archiver'] == '%s (%s at %s)' % (
            stats['failed_count'],
            stats['last_failed_wal'],
            stats['last_failed_time'].ctime()
        )

    @patch('barman.server.Server.get_remote_status')
    def test_check_postgres(self, postgres_mock, capsys):
        """
        Test management of check_postgres view output

        :param postgres_mock: mock get_remote_status function
        :param capsys: retrieve output from consolle
        """
        postgres_mock.return_value = {'server_txt_version': None}
        # Create server
        server = build_real_server()
        # Case: no reply by PostgreSQL
        # Expect out: PostgreSQL: FAILED
        strategy = CheckOutputStrategy()
        server.check_postgres(strategy)
        (out, err) = capsys.readouterr()
        assert out == '	PostgreSQL: FAILED\n'
        # Case: correct configuration
        postgres_mock.return_value = {'current_xlog': None,
                                      'archive_command': 'wal to archive',
                                      'pgespresso_installed': None,
                                      'server_txt_version': 'PostgresSQL 9_4',
                                      'data_directory': '/usr/local/postgres',
                                      'archive_mode': 'on',
                                      'wal_level': 'archive'}

        # Expect out: all parameters: OK

        # Postgres version >= 9.0 - check wal_level
        server = build_real_server()
        server.check_postgres(strategy)
        (out, err) = capsys.readouterr()
        assert out == "\tPostgreSQL: OK\n" \
                      "\twal_level: OK\n"

        # Postgres version < 9.0 - avoid wal_level check
        del postgres_mock.return_value['wal_level']

        server = build_real_server()
        server.check_postgres(strategy)
        (out, err) = capsys.readouterr()
        assert out == "\tPostgreSQL: OK\n"

        # Case: wal_level and archive_command values are not acceptable
        postgres_mock.return_value = {'current_xlog': None,
                                      'archive_command': None,
                                      'pgespresso_installed': None,
                                      'server_txt_version': 'PostgresSQL 9_4',
                                      'data_directory': '/usr/local/postgres',
                                      'archive_mode': 'on',
                                      'wal_level': 'minimal'}
        # Expect out: some parameters: FAILED
        strategy = CheckOutputStrategy()
        server.check_postgres(strategy)
        (out, err) = capsys.readouterr()
        assert out == "\tPostgreSQL: OK\n" \
                      "\twal_level: FAILED (please set it to a higher level " \
                      "than 'minimal')\n"

    @patch('barman.server.Server.get_wal_until_next_backup')
    def test_get_wal_info(self, get_wal_mock, tmpdir):
        """
        Basic test for get_wal_info method
        Test the wals per second and total time in seconds values.
        :return:
        """
        # Build a test server with a test path
        server = build_real_server(global_conf={
            'barman_home': tmpdir.strpath
        })
        # Mock method get_wal_until_next_backup for returning a list of
        # 3 fake WAL. the first one is the start and stop WAL of the backup
        wal_list = [
            WalFileInfo.from_xlogdb_line(
                "000000010000000000000002\t16777216\t1434450086.53\tNone\n"),
            WalFileInfo.from_xlogdb_line(
                "000000010000000000000003\t16777216\t1434450087.54\tNone\n"),
            WalFileInfo.from_xlogdb_line(
                "000000010000000000000004\t16777216\t1434450088.55\tNone\n")]
        get_wal_mock.return_value = wal_list
        backup_info = build_test_backup_info(
            server=server,
            begin_wal=wal_list[0].name,
            end_wal=wal_list[0].name)
        backup_info.save()
        # Evaluate total time in seconds:
        # last_wal_timestamp - first_wal_timestamp
        wal_total_seconds = wal_list[-1].time - wal_list[0].time
        # Evaluate the wals_per_second value:
        # wals_in_backup + wals_until_next_backup / total_time_in_seconds
        wals_per_second = len(wal_list) / wal_total_seconds
        wal_info = server.get_wal_info(backup_info)
        assert wal_info
        assert wal_info['wal_total_seconds'] == wal_total_seconds
        assert wal_info['wals_per_second'] == wals_per_second

    @patch('barman.server.Server.check')
    @patch('barman.server.Server._make_directories')
    @patch('barman.backup.BackupManager.backup')
    @patch('barman.server.Server.archive_wal')
    @patch('barman.server.ServerBackupLock')
    def test_backup(self, backup_lock_mock, archive_wal_mock,
                    backup_manager_mock, dir_mock, check_mock, capsys):
        """

        :param backup_lock_mock: mock ServerBackupLock
        :param archive_wal_mock: mock archive_wal server method
        :param backup_manager_mock: mock BackupManager.backup
        :param dir_mock: mock _make_directories
        :param check_mock: mock check
        """

        # Create server
        server = build_real_server()
        dir_mock.side_effect = OSError()
        server.backup()
        out, err = capsys.readouterr()
        assert 'failed to create' in err

        dir_mock.side_effect = None
        server.backup()
        backup_manager_mock.assert_called_once_with()
        archive_wal_mock.assert_called_once_with(verbose=False)

        backup_manager_mock.side_effect = LockFileBusy()
        server.backup()
        out, err = capsys.readouterr()
        assert 'Another backup process is running' in err

        backup_manager_mock.side_effect = LockFilePermissionDenied()
        server.backup()
        out, err = capsys.readouterr()
        assert 'Permission denied, unable to access' in err

    @patch('barman.server.Server.get_first_backup_id')
    @patch('barman.server.BackupManager.delete_backup')
    def test_delete_running_backup(self, delete_mock, get_first_backup_mock,
                                   tmpdir, capsys):
        """
        Simple test for the deletion of a running backup.
        We want to test the behaviour of the server.delete_backup method
        when invoked on a running backup
        """
        # Test the removal of a running backup. status STARTED
        server = build_real_server({'barman_home': tmpdir.strpath})
        backup_info_started = build_test_backup_info(
            status=BackupInfo.STARTED,
            server_name=server.config.name)
        get_first_backup_mock.return_value = backup_info_started.backup_id
        with ServerBackupLock(tmpdir.strpath, server.config.name):
            server.delete_backup(backup_info_started)
            out, err = capsys.readouterr()
            assert "Cannot delete a running backup (%s %s)" % (
                server.config.name,
                backup_info_started.backup_id) in err

        # Test the removal of a running backup. status EMPTY
        backup_info_empty = build_test_backup_info(
            status=BackupInfo.EMPTY,
            server_name=server.config.name)
        get_first_backup_mock.return_value = backup_info_empty.backup_id
        with ServerBackupLock(tmpdir.strpath, server.config.name):
            server.delete_backup(backup_info_empty)
            out, err = capsys.readouterr()
            assert "Cannot delete a running backup (%s %s)" % (
                server.config.name,
                backup_info_started.backup_id) in err

        # Test the removal of a running backup. status DONE
        backup_info_done = build_test_backup_info(
            status=BackupInfo.DONE,
            server_name=server.config.name)
        with ServerBackupLock(tmpdir.strpath, server.config.name):
            server.delete_backup(backup_info_done)
            delete_mock.assert_called_with(backup_info_done)

        # Test the removal of a backup not running. status STARTED
        server.delete_backup(backup_info_started)
        delete_mock.assert_called_with(backup_info_started)

    @patch("subprocess.Popen")
    def test_archive_wal_lock_acquisition(self, subprocess_mock,
                                          tmpdir, capsys):
        """
        Basic test for archive-wal lock acquisition
        """
        server = build_real_server({'barman_home': tmpdir.strpath})

        with ServerWalArchiveLock(tmpdir.strpath, server.config.name):
            server.archive_wal()
            out, err = capsys.readouterr()
            assert ("Another archive-wal process is already running "
                    "on server %s. Skipping to the next server"
                    % server.config.name) in out

    @patch("subprocess.Popen")
    def test_cron_lock_acquisition(self, subprocess_mock,
                                   tmpdir, capsys, caplog):
        """
        Basic test for cron process lock acquisition
        """
        server = build_real_server({'barman_home': tmpdir.strpath})

        # Basic cron lock acquisition
        with ServerCronLock(tmpdir.strpath, server.config.name):
            server.cron(wals=True, retention_policies=False)
            out, err = capsys.readouterr()
            assert ("Another cron process is already running on server %s. "
                    "Skipping to the next server\n" %
                    server.config.name) in out

        # Lock acquisition for archive-wal
        with ServerWalArchiveLock(tmpdir.strpath, server.config.name):
            server.cron(wals=True, retention_policies=False)
            out, err = capsys.readouterr()
            assert ("Another archive-wal process is already running "
                    "on server %s. Skipping to the next server"
                    % server.config.name) in out
        # Lock acquisition for receive-wal
        with ServerWalArchiveLock(tmpdir.strpath, server.config.name):
            with ServerWalReceiveLock(tmpdir.strpath, server.config.name):
                # force the streaming_archiver to True for this test
                server.config.streaming_archiver = True
                server.cron(wals=True, retention_policies=False)
                assert ("Another STREAMING ARCHIVER process is running for "
                        "server %s" % server.config.name) in caplog.text

    @patch('barman.server.ProcessManager')
    def test_kill(self, pm_mock, capsys):

        server = build_real_server()

        # Empty process list, the process is not running
        task_name = 'test_task'
        process_list = []
        pm_mock.return_value.list.return_value = process_list
        pm_mock.return_value.kill.return_value = True
        server.kill(task_name)
        out, err = capsys.readouterr()
        assert ('Termination of %s failed: no such process for server %s' % (
            task_name,
            server.config.name)) in err

        # Successful kill
        pid = 1234
        process_list.append(ProcessInfo(pid, server.config.name, task_name))
        pm_mock.return_value.list.return_value = process_list
        pm_mock.return_value.kill.return_value = True
        server.kill('test_task')
        out, err = capsys.readouterr()
        assert ('Stopped process %s(%s)' %
                (task_name, pid)) in out

        # The process don't terminate
        pm_mock.return_value.kill.return_value = False
        server.kill('test_task')
        out, err = capsys.readouterr()
        assert ('ERROR: Cannot terminate process %s(%s)' %
                (task_name, pid)) in err

    @patch('os.listdir')
    @patch('os.path.isdir')
    def test_check_archiver_errors(self, isdir_mock, listdir_mock):
        server = build_real_server()
        check_strategy = MagicMock()

        # There is no error file
        check_strategy.reset_mock()
        listdir_mock.return_value = []
        server.check_archiver_errors(check_strategy)
        check_strategy.result.assert_called_with(
            'main',
            'archiver errors',
            True,
            None,
        )

        # There is one duplicate file
        check_strategy.reset_mock()
        listdir_mock.return_value = ['testing.duplicate']
        server.check_archiver_errors(check_strategy)
        check_strategy.result.assert_called_with(
            'main',
            'archiver errors',
            False,
            'duplicates: 1',
        )

        # There is one unknown file
        check_strategy.reset_mock()
        listdir_mock.return_value = ['testing.unknown']
        server.check_archiver_errors(check_strategy)
        check_strategy.result.assert_called_with(
            'main',
            'archiver errors',
            False,
            'unknown: 1',
        )

        # There is one not relevant file
        check_strategy.reset_mock()
        listdir_mock.return_value = ['testing.error']
        server.check_archiver_errors(check_strategy)
        check_strategy.result.assert_called_with(
            'main',
            'archiver errors',
            False,
            'not relevant: 1',
        )

        # There is one extraneous file
        check_strategy.reset_mock()
        listdir_mock.return_value = ['testing.wrongextension']
        server.check_archiver_errors(check_strategy)
        check_strategy.result.assert_called_with(
            'main',
            'archiver errors',
            False,
            'unknown failure: 1'
        )


class TestCheckStrategy(object):
    """
    Test the different strategies for the results of the check command
    """

    def test_check_output_strategy(self, capsys):
        """
        Test correct output result
        """
        strategy = CheckOutputStrategy()
        # Expected result OK
        strategy.result('test_server_one', 'wal_level', True)
        out, err = capsys.readouterr()
        assert out == '	wal_level: OK\n'
        # Expected result FAILED
        strategy.result('test_server_one', 'wal_level', False)
        out, err = capsys.readouterr()
        assert out == '	wal_level: FAILED\n'

    def test_check_output_strategy_log(self, caplog):
        """
        Test correct output log

        :type caplog: pytest_capturelog.CaptureLogFuncArg
        """
        strategy = CheckOutputStrategy()
        # Expected result OK
        strategy.result('test_server_one', 'wal_level', True)
        records = list(caplog.records)
        assert len(records) == 1
        record = records.pop()
        assert record.msg == \
            "Check 'wal_level' succeeded for server 'test_server_one'"
        assert record.levelname == 'DEBUG'
        # Expected result FAILED
        strategy = CheckOutputStrategy()
        strategy.result('test_server_one', 'wal_level', False)
        strategy.result('test_server_one', 'backup maximum age', False)
        records = list(caplog.records)
        assert len(records) == 3
        record = records.pop()
        assert record.levelname == 'ERROR'
        assert record.msg == \
            "Check 'backup maximum age' failed for server 'test_server_one'"
        record = records.pop()
        assert record.levelname == 'ERROR'
        assert record.msg == \
            "Check 'wal_level' failed for server 'test_server_one'"

    def test_check_strategy(self, capsys):
        """
        Test correct values result

        :type capsys: pytest
        """
        strategy = CheckStrategy()
        # Expected no errors
        strategy.result('test_server_one', 'wal_level', True)
        strategy.result('test_server_one', 'archive mode', True)
        assert ('', '') == capsys.readouterr()
        assert strategy.has_error is False
        assert strategy.check_result
        assert len(strategy.check_result) == 2
        # Expected two errors
        strategy = CheckStrategy()
        strategy.result('test_server_one', 'wal_level', False)
        strategy.result('test_server_one', 'archive mode', False)
        assert ('', '') == capsys.readouterr()
        assert strategy.has_error is True
        assert strategy.check_result
        assert len(strategy.check_result) == 2
        assert len([result
                    for result in strategy.check_result
                    if not result.status]) == 2
        # Test Non blocking error behaviour (one non blocking error)
        strategy = CheckStrategy()
        strategy.result('test_server_one', 'backup maximum age', False)
        strategy.result('test_server_one', 'archive mode', True)
        assert ('', '') == capsys.readouterr()
        assert strategy.has_error is False
        assert strategy.check_result
        assert len(strategy.check_result) == 2
        assert len([result
                    for result in strategy.check_result
                    if not result.status]) == 1

        # Test Non blocking error behaviour (2 errors one is non blocking)
        strategy = CheckStrategy()
        strategy.result('test_server_one', 'backup maximum age', False)
        strategy.result('test_server_one', 'archive mode', False)
        assert ('', '') == capsys.readouterr()
        assert strategy.has_error is True
        assert strategy.check_result
        assert len(strategy.check_result) == 2
        assert len([result
                    for result in strategy.check_result
                    if not result.status]) == 2

    def test_check_strategy_log(self, caplog):
        """
        Test correct log

        :type caplog: pytest_capturelog.CaptureLogFuncArg
        """
        strategy = CheckStrategy()
        # Expected result OK
        strategy.result('test_server_one', 'wal_level', True)
        records = list(caplog.records)
        assert len(records) == 1
        record = records.pop()
        assert record.msg == \
            "Check 'wal_level' succeeded for server 'test_server_one'"
        assert record.levelname == 'DEBUG'
        # Expected result FAILED
        strategy.result('test_server_one', 'wal_level', False)
        records = list(caplog.records)
        assert len(records) == 2
        record = records.pop()
        assert record.levelname == 'ERROR'
        assert record.msg == \
            "Check 'wal_level' failed for server 'test_server_one'"
