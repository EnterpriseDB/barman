# Copyright (C) 2013-2017 2ndQuadrant Limited
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
from collections import namedtuple

import pytest
from mock import MagicMock, mock, patch
from psycopg2.tz import FixedOffsetTimezone

from barman.exceptions import (LockFileBusy, LockFilePermissionDenied,
                               PostgresDuplicateReplicationSlot,
                               PostgresInvalidReplicationSlot,
                               PostgresReplicationSlotsFull,
                               PostgresSuperuserRequired,
                               PostgresUnsupportedFeature)
from barman.infofile import BackupInfo, WalFileInfo
from barman.lockfile import (ServerBackupLock, ServerCronLock,
                             ServerWalArchiveLock, ServerWalReceiveLock)
from barman.postgres import PostgreSQLConnection
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
        server = Server(build_config_from_dicts(
            global_conf={
                'archiver': 'on'
            }).get_server('main'))
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
        # ARCHIVER_OFF_BACKCOMPATIBILITY - START OF CODE
        # # Check that either archiver or streaming_archiver are set
        # server = Server(build_config_from_dicts(
        #     main_conf={
        #         'archiver': 'off',
        #         'streaming_archiver': 'off'
        #     }
        # ).get_server('main'))
        # assert server.config.disabled
        # assert "No archiver enabled for server 'main'. " \
        #        "Please turn on 'archiver', 'streaming_archiver' or " \
        #        "both" in server.config.msg_list
        # ARCHIVER_OFF_BACKCOMPATIBILITY - START OF CODE
        server = Server(build_config_from_dicts(
            main_conf={
                'archiver': 'off',
                'streaming_archiver': 'on',
                'slot_name': ''
            }
        ).get_server('main'))
        assert server.config.disabled
        assert "Streaming-only archiver requires 'streaming_conninfo' and " \
               "'slot_name' options to be properly configured" \
               in server.config.msg_list

    def test_check_config_missing(self, tmpdir):
        """
        Verify the check method can be called on an empty configuration
        """
        server = Server(build_config_from_dicts(
            global_conf={
                # Required by server.check_archive method
                "barman_lock_directory": tmpdir.mkdir('lock').strpath
            },
            main_conf={
                'conninfo': '',
                'ssh_command': '',
                # Required by server.check_archive method
                'wals_directory': tmpdir.mkdir('wals').strpath,
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
        # fake backup
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
    def test_pg_stat_archiver_show(self, remote_mock, capsys):
        """
        Test management of pg_stat_archiver view output in show command

        :param MagicMock remote_mock: mock the Server.get_remote_status method
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

        server = build_real_server(
            global_conf={
                'archiver': 'on',
                'last_backup_maximum_age': '1 day',
            }
        )

        # Testing for show-server command.
        # Expecting in the output the same values present into the stats dict
        server.show()

        # Parse the output
        (out, err) = capsys.readouterr()
        result = dict(item.strip('\t\n\r').split(": ")
                      for item in out.split("\n") if item != '')
        assert err == ''

        assert result['failed_count'] == stats['failed_count']
        assert result['last_archived_wal'] == stats['last_archived_wal']
        assert result['last_archived_time'] == str(stats['last_archived_time'])
        assert result['last_failed_wal'] == stats['last_failed_wal']
        assert result['last_failed_time'] == str(stats['last_failed_time'])
        assert result['current_archived_wals_per_second'] == \
            str(stats['current_archived_wals_per_second'])

    @patch('barman.server.Server.status_postgres')
    @patch('barman.wal_archiver.FileWalArchiver.get_remote_status')
    def test_pg_stat_archiver_status(self, remote_mock, status_postgres_mock,
                                     capsys):
        """
        Test management of pg_stat_archiver view output in status command

        :param MagicMock remote_mock: mock the
            FileWalArchiver.get_remote_status method
        :param capsys: retrieve output from consolle
        """

        archiver_remote_status = {
            "archive_mode": "on",
            "archive_command": "send_to_barman.sh %p %f",
            "failed_count": "2",
            "last_archived_wal": "000000010000000000000006",
            "last_archived_time": datetime.datetime.now(),
            "last_failed_wal": "000000010000000000000005",
            "last_failed_time": datetime.datetime.now(),
            "current_archived_wals_per_second": 1.0002,
        }
        remote_mock.return_value = dict(archiver_remote_status)

        status_postgres_mock.return_value = dict()

        server = build_real_server(
            global_conf={
                'archiver': 'on',
            }
        )

        # Test output for status invocation
        # Expecting:
        # Last archived WAL:
        #   <last_archived_wal>, at <last_archived_time>
        # Failures of WAL archiver:
        #   <failed_count> (<last_failed wal>, at <last_failed_time>)
        server.status()
        (out, err) = capsys.readouterr()

        # Parse the output
        result = dict(item.strip('\t\n\r').split(": ")
                      for item in out.split("\n") if item != '')
        assert err == ''

        # Check the result
        assert result['Last archived WAL'] == '%s, at %s' % (
            archiver_remote_status['last_archived_wal'],
            archiver_remote_status['last_archived_time'].ctime()
        )
        assert result['Failures of WAL archiver'] == '%s (%s at %s)' % (
            archiver_remote_status['failed_count'],
            archiver_remote_status['last_failed_wal'],
            archiver_remote_status['last_failed_time'].ctime()
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
                                      'wal_level': 'replica'}

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

    @patch('barman.server.Server.get_remote_status')
    def test_check_replication_slot(self, postgres_mock, capsys):
        """
        Extension of the check_postgres test.
        Tests the replication_slot check

        :param postgres_mock: mock get_remote_status function
        :param capsys: retrieve output from console
        """
        postgres_mock.return_value = {
            'current_xlog': None,
            'archive_command': 'wal to archive',
            'pgespresso_installed': None,
            'server_txt_version': '9.3.1',
            'data_directory': '/usr/local/postgres',
            'archive_mode': 'on',
            'wal_level': 'replica',
            'replication_slot_support': False,
            'replication_slot': None,
        }

        # Create server
        server = build_real_server()

        # Case: Postgres version < 9.4
        strategy = CheckOutputStrategy()
        server.check_postgres(strategy)
        (out, err) = capsys.readouterr()
        assert '\treplication slot:' not in out

        # Case: correct configuration
        # use a mock as a quick disposable obj
        rep_slot = mock.Mock()
        rep_slot.slot_name = 'test'
        rep_slot.active = True
        rep_slot.restart_lsn = 'aaaBB'
        postgres_mock.return_value = {
            'server_txt_version': '9.4.1',
            'replication_slot_support': True,
            'replication_slot': rep_slot,
        }
        server = build_real_server()
        server.config.streaming_archiver = True
        server.config.slot_name = 'test'
        server.check_postgres(strategy)
        (out, err) = capsys.readouterr()

        # Everything is ok
        assert '\treplication slot: OK\n' in out

        rep_slot.active = False
        rep_slot.restart_lsn = None
        postgres_mock.return_value = {
            'server_txt_version': '9.4.1',
            'replication_slot_support': True,
            'replication_slot': rep_slot,
        }

        # Replication slot not initialised.
        server = build_real_server()
        server.config.slot_name = 'test'
        server.config.streaming_archiver = True
        server.check_postgres(strategy)
        (out, err) = capsys.readouterr()
        # Everything is ok
        assert "\treplication slot: FAILED (slot '%s' not initialised: " \
               "is 'receive-wal' running?)\n" \
               % server.config.slot_name in out

        rep_slot.reset_mock()
        rep_slot.active = False
        rep_slot.restart_lsn = 'Test'
        postgres_mock.return_value = {
            'server_txt_version': '9.4.1',
            'replication_slot_support': True,
            'replication_slot': rep_slot
        }

        # Replication slot not active.
        server = build_real_server()
        server.config.slot_name = 'test'
        server.config.streaming_archiver = True
        server.check_postgres(strategy)
        (out, err) = capsys.readouterr()
        # Everything is ok
        assert "\treplication slot: FAILED (slot '%s' not active: " \
               "is 'receive-wal' running?)\n" % server.config.slot_name in out

        rep_slot.reset_mock()
        rep_slot.active = False
        rep_slot.restart_lsn = 'Test'
        postgres_mock.return_value = {
            'server_txt_version': 'PostgreSQL 9.4.1',
            'replication_slot_support': True,
            'replication_slot': rep_slot
        }

        # Replication slot not active with streaming_archiver off.
        server = build_real_server()
        server.config.slot_name = 'test'
        server.config.streaming_archiver = False
        server.check_postgres(strategy)
        (out, err) = capsys.readouterr()
        # Everything is ok
        assert "\treplication slot: OK (WARNING: slot '%s' is initialised " \
               "but not required by the current config)\n" \
               % server.config.slot_name in out

        rep_slot.reset_mock()
        rep_slot.active = True
        rep_slot.restart_lsn = 'Test'
        postgres_mock.return_value = {
            'server_txt_version': 'PostgreSQL 9.4.1',
            'replication_slot_support': True,
            'replication_slot': rep_slot,
        }

        # Replication slot not active with streaming_archiver off.
        server = build_real_server()
        server.config.slot_name = 'test'
        server.config.streaming_archiver = False
        server.check_postgres(strategy)
        (out, err) = capsys.readouterr()
        # Everything is ok
        assert "\treplication slot: OK (WARNING: slot '%s' is active " \
               "but not required by the current config)\n" \
               % server.config.slot_name in out

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
            True,
            hint=None
        )

        # There is one duplicate file
        check_strategy.reset_mock()
        listdir_mock.return_value = ['testing.duplicate']
        server.check_archiver_errors(check_strategy)
        check_strategy.result.assert_called_with(
            'main',
            False,
            hint='duplicates: 1',
        )

        # There is one unknown file
        check_strategy.reset_mock()
        listdir_mock.return_value = ['testing.unknown']
        server.check_archiver_errors(check_strategy)
        check_strategy.result.assert_called_with(
            'main',
            False,
            hint='unknown: 1',
        )

        # There is one not relevant file
        check_strategy.reset_mock()
        listdir_mock.return_value = ['testing.error']
        server.check_archiver_errors(check_strategy)
        check_strategy.result.assert_called_with(
            'main',
            False,
            hint='not relevant: 1',
        )

        # There is one extraneous file
        check_strategy.reset_mock()
        listdir_mock.return_value = ['testing.wrongextension']
        server.check_archiver_errors(check_strategy)
        check_strategy.result.assert_called_with(
            'main',
            False,
            hint='unknown failure: 1'
        )

    def test_switch_xlog(self, capsys):
        server = build_real_server()

        server.postgres = MagicMock()
        server.postgres.switch_xlog.return_value = '000000010000000000000001'
        server.switch_xlog(force=False)
        out, err = capsys.readouterr()
        assert "The xlog file 000000010000000000000001 has been closed " \
               "on server 'main'" in out
        assert server.postgres.checkpoint.called is False

        server.postgres.reset_mock()
        server.postgres.switch_xlog.return_value = '000000010000000000000001'
        server.switch_xlog(force=True)

        out, err = capsys.readouterr()
        assert "The xlog file 000000010000000000000001 has been closed " \
               "on server 'main'" in out
        assert server.postgres.checkpoint.called is True
        server.postgres.reset_mock()
        server.postgres.switch_xlog.return_value = ''

        server.switch_xlog(force=False)

        out, err = capsys.readouterr()
        assert "No switch required for server 'main'" in out
        assert server.postgres.checkpoint.called is False

    def test_check_archive(self, tmpdir):
        """
        Test the check_archive method
        """
        # Setup temp dir and server
        server = build_real_server(
            global_conf={
                "barman_lock_directory": tmpdir.mkdir('lock').strpath
            },
            main_conf={
                "wals_directory": tmpdir.mkdir('wals').strpath,
                "incoming_wals_directory": tmpdir.mkdir('incoming').strpath,
                "streaming_wals_directory": tmpdir.mkdir('streaming').strpath
            })
        strategy = CheckStrategy()

        # Call the server on an unexistent xlog file. expect it to fail
        server.check_archive(strategy)
        assert strategy.has_error is True
        assert strategy.check_result[0].check == 'WAL archive'
        assert strategy.check_result[0].status is False

        # Call the check on an empty xlog file. expect it to contain errors.
        with open(server.xlogdb_file_name, "a"):
            # the open call forces the file creation
            pass

        server.check_archive(strategy)
        assert strategy.has_error is True
        assert strategy.check_result[0].check == 'WAL archive'
        assert strategy.check_result[0].status is False

        # Write something in the xlog db file and check for the results
        with server.xlogdb('w') as fxlogdb:
            fxlogdb.write("00000000000000000000")
        # The check strategy should contain no errors.
        strategy = CheckStrategy()
        server.check_archive(strategy)
        assert strategy.has_error is False
        assert len(strategy.check_result) == 0

        # Call the server on with archive = off and
        # the incoming directory not empty
        with open("%s/00000000000000000000" %
                  server.config.incoming_wals_directory, 'w') as f:
            f.write('fake WAL')
        server.config.archiver = False
        server.check_archive(strategy)
        assert strategy.has_error is False
        assert strategy.check_result[0].check == 'empty incoming directory'
        assert strategy.check_result[0].status is False

        # Check that .tmp files are ignored
        # Create a nonempty tmp file
        with open(os.path.join(server.config.incoming_wals_directory,
                  "00000000000000000000.tmp"), 'w') as wal:
            wal.write('a')
        # The check strategy should contain no errors.
        strategy = CheckStrategy()
        server.config.archiver = True
        server.check_archive(strategy)
        # Check that is ignored
        assert strategy.has_error is False
        assert len(strategy.check_result) == 0

    @pytest.mark.parametrize('icoming_name, archiver_name',
                             [
                                 ['incoming', 'archiver'],
                                 ['streaming', 'streaming_archiver'],
                             ])
    def test_incoming_thresholds(self, icoming_name, archiver_name, tmpdir):
        """
        Test the check_archive method thresholds
        """
        # Setup temp dir and server
        server = build_real_server(
            global_conf={
                "barman_lock_directory": tmpdir.mkdir('lock').strpath
            },
            main_conf={
                "wals_directory": tmpdir.mkdir('wals').strpath,
                "%s_wals_directory" % icoming_name:
                    tmpdir.mkdir(icoming_name).strpath,
            }
        )

        # Make sure the test has configured correctly
        incoming_dir_setting = '%s_wals_directory' % icoming_name
        incoming_dir = getattr(server.config, incoming_dir_setting)
        assert incoming_dir

        # Create some content in the fake xlog.db to avoid triggering
        # empty xlogdb errors
        with open(server.xlogdb_file_name, "a") as fxlogdb:
            # write something
            fxlogdb.write("00000000000000000000")

        # Utility function to generare fake WALs
        def write_wal(target_dir, wal_number, partial=False):
            wal_name = "%s/0000000000000000%08d" % (target_dir, wal_number)
            if partial:
                wal_name += '.partial'
            with open(wal_name, 'w') as wal_file:
                wal_file.write('fake WAL %s' % wal_number)

        # Case one, queue below the threshold

        # Enable the archiver we are checking and put max_incoming_wals_queue
        # files inside the directory
        setattr(server.config, archiver_name, True)
        server.config.max_incoming_wals_queue = 2
        # Fill the incoming dir to the threshold limit, we leave out the wal 0
        # to add it in a further test
        for x in range(1, server.config.max_incoming_wals_queue + 1):
            write_wal(incoming_dir, x)
        # If streaming, add a fake .partial file
        if icoming_name == 'streaming':
            write_wal(incoming_dir,
                      server.config.max_incoming_wals_queue + 1,
                      partial=True)

        # Expect this to succeed
        strategy = CheckStrategy()
        server.check_archive(strategy)
        assert not strategy.has_error
        assert len(strategy.check_result) == 0

        # Case two, queue over the threshold

        # Add one more file to go over the threshold
        write_wal(incoming_dir, 0)
        # Expect this to fail, but with not critical errors
        strategy = CheckStrategy()
        server.check_archive(strategy)
        # Errors are not critical
        assert strategy.has_error is False
        assert len(strategy.check_result) == 1
        assert strategy.check_result[0].check == (
            '%s WALs directory' % icoming_name)
        assert strategy.check_result[0].status is False

        # Case three, disable the archiver and clean the incoming

        # Disable the archiver and clean the incoming dir
        setattr(server.config, archiver_name, False)
        for wal_file in os.listdir(incoming_dir):
            os.remove(os.path.join(incoming_dir, wal_file))

        # If streaming, add a fake .partial file
        if icoming_name == 'streaming':
            write_wal(incoming_dir, 1, partial=True)

        # Expect this to succeed
        strategy = CheckStrategy()
        server.check_archive(strategy)
        assert not strategy.has_error
        assert len(strategy.check_result) == 0

        # Case four, disable the archiver an add something inside the
        # incoming directory. expect the check to fail

        # Disable the streaming archiver and add something inside the dir
        setattr(server.config, archiver_name, False)
        write_wal(incoming_dir, 0)
        # Expect this to fail, but with not critical errors
        strategy = CheckStrategy()
        server.check_archive(strategy)
        # Errors are not critical
        assert not strategy.has_error
        assert len(strategy.check_result) == 1
        assert strategy.check_result[0].check == (
            'empty %s directory' % icoming_name)
        assert strategy.check_result[0].status is False

    def test_replication_status(self, capsys):
        """
        Test management of pg_stat_archiver view output

        :param MagicMock connect_mock: mock the database connection
        :param capsys: retrieve output from consolle

        """

        # Build a fake get_replication_stats record
        replication_stats_data = dict(
            pid=93275,
            usesysid=10,
            usename='postgres',
            application_name='replica',
            client_addr=None,
            client_hostname=None,
            client_port=-1,
            slot_name=None,
            backend_start=datetime.datetime(
                2016, 5, 6, 9, 29, 20, 98534,
                tzinfo=FixedOffsetTimezone(offset=120)),
            backend_xmin='940',
            state='streaming',
            sent_location='0/3005FF0',
            write_location='0/3005FF0',
            flush_location='0/3005FF0',
            replay_location='0/3005FF0',
            current_location='0/3005FF0',
            sync_priority=0,
            sync_state='async'
        )
        replication_stats_class = namedtuple("Record",
                                             replication_stats_data.keys())
        replication_stats_record = replication_stats_class(
            **replication_stats_data)

        # Prepare the server
        server = build_real_server(main_conf={'archiver': 'on'})
        server.postgres = MagicMock()
        server.postgres.get_replication_stats.return_value = [
            replication_stats_record]
        server.postgres.current_xlog_location = "AB/CDEF1234"

        # Execute the test (ALL)
        server.postgres.reset_mock()
        server.replication_status('all')
        (out, err) = capsys.readouterr()
        assert err == ''
        server.postgres.get_replication_stats.assert_called_once_with(
            PostgreSQLConnection.ANY_STREAMING_CLIENT)

        # Execute the test (WALSTREAMER)
        server.postgres.reset_mock()
        server.replication_status('wal-streamer')
        (out, err) = capsys.readouterr()
        assert err == ''
        server.postgres.get_replication_stats.assert_called_once_with(
            PostgreSQLConnection.WALSTREAMER)

        # Execute the test (failure: PostgreSQL too old)
        server.postgres.reset_mock()
        server.postgres.get_replication_stats.side_effect = \
            PostgresUnsupportedFeature('9.1')
        server.replication_status('all')
        (out, err) = capsys.readouterr()
        assert 'Requires PostgreSQL 9.1 or higher' in out
        assert err == ''
        server.postgres.get_replication_stats.assert_called_once_with(
            PostgreSQLConnection.ANY_STREAMING_CLIENT)

        # Execute the test (failure: superuser required)
        server.postgres.reset_mock()
        server.postgres.get_replication_stats.side_effect = \
            PostgresSuperuserRequired
        server.replication_status('all')
        (out, err) = capsys.readouterr()
        assert 'Requires superuser rights' in out
        assert err == ''
        server.postgres.get_replication_stats.assert_called_once_with(
            PostgreSQLConnection.ANY_STREAMING_CLIENT)

        # Test output reaction to missing attributes
        del replication_stats_data['slot_name']
        server.postgres.reset_mock()
        server.replication_status('all')
        (out, err) = capsys.readouterr()
        assert 'Replication slot' not in out

    def test_timeline_has_children(self, tmpdir):
        """
        Test for the timeline_has_children
        """
        server = build_real_server({'barman_home': tmpdir.strpath})
        tmpdir.join('main/wals').ensure(dir=True)

        # Write two history files
        history_2 = server.get_wal_full_path('00000002.history')
        with open(history_2, "w") as fp:
            fp.write('1\t2/83000168\tat restore point "myrp"\n')

        history_3 = server.get_wal_full_path('00000003.history')
        with open(history_3, "w") as fp:
            fp.write('1\t2/83000168\tat restore point "myrp"\n')

        history_4 = server.get_wal_full_path('00000004.history')
        with open(history_4, "w") as fp:
            fp.write('1\t2/83000168\tat restore point "myrp"\n')
            fp.write('2\t2/84000268\tunknown\n')

        # Check that the first timeline has children but the
        # others have not
        assert len(server.get_children_timelines(1)) == 3
        assert len(server.get_children_timelines(2)) == 1
        assert len(server.get_children_timelines(3)) == 0
        assert len(server.get_children_timelines(4)) == 0

    def test_xlogdb_file_name(self):
        """
        Test the xlogdb_file_name server property
        """
        server = build_real_server()
        server.config.wals_directory = 'mock_wals_directory'

        result = os.path.join(
            server.config.wals_directory,
            server.XLOG_DB
        )

        assert server.xlogdb_file_name == result

    def test_create_physical_repslot(self, capsys):
        """
        Test the 'create_physical_repslot' method of the Postgres
        class
        """

        # No operation if there is no streaming connection
        server = build_real_server()
        server.streaming = None
        assert server.create_physical_repslot() is None

        # No operation if the slot name is empty
        server.streaming = MagicMock()
        server.config.slot_name = None
        server.streaming.server_version = 90400
        assert server.create_physical_repslot() is None

        # If there is a streaming connection and the replication
        # slot is defined, then the replication slot should be
        # created
        server.config.slot_name = 'test_repslot'
        server.streaming.server_version = 90400
        server.create_physical_repslot()
        create_physical_repslot = server.streaming.create_physical_repslot
        create_physical_repslot.assert_called_with('test_repslot')

        # If the replication slot was already created
        # check that underlying the exception is correctly managed
        create_physical_repslot.side_effect = PostgresDuplicateReplicationSlot
        server.create_physical_repslot()
        create_physical_repslot.assert_called_with('test_repslot')
        out, err = capsys.readouterr()
        assert "Replication slot 'test_repslot' already exists" in err

        # Test the method failure if the replication slots
        # on the server are all taken
        create_physical_repslot.side_effect = PostgresReplicationSlotsFull
        server.create_physical_repslot()
        create_physical_repslot.assert_called_with('test_repslot')
        out, err = capsys.readouterr()
        assert "All replication slots for server 'main' are in use\n" in err

    def test_drop_repslot(self, capsys):
        """
        Test the 'drop_repslot' method of the Postgres
        class
        """

        # No operation if there is no streaming connection
        server = build_real_server()
        server.streaming = None
        assert server.drop_repslot() is None

        # No operation if the slot name is empty
        server.streaming = MagicMock()
        server.config.slot_name = None
        server.streaming.server_version = 90400
        assert server.drop_repslot() is None

        # If there is a streaming connection and the replication
        # slot is defined, then the replication slot should be
        # created
        server.config.slot_name = 'test_repslot'
        server.streaming.server_version = 90400
        server.drop_repslot()
        drop_repslot = server.streaming.drop_repslot
        drop_repslot.assert_called_with('test_repslot')

        # If the replication slot doesn't exist
        # check that the underlying exception is correctly managed
        drop_repslot.side_effect = PostgresInvalidReplicationSlot
        server.drop_repslot()
        drop_repslot.assert_called_with('test_repslot')
        out, err = capsys.readouterr()
        assert "Replication slot 'test_repslot' does not exist" in err


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
        strategy.result('test_server_one', True, check='wal_level')
        out, err = capsys.readouterr()
        assert out == '	wal_level: OK\n'
        # Expected result FAILED
        strategy.result('test_server_one', False, check='wal_level')
        out, err = capsys.readouterr()
        assert out == '	wal_level: FAILED\n'

    def test_check_output_strategy_log(self, caplog):
        """
        Test correct output log

        :type caplog: pytest_capturelog.CaptureLogFuncArg
        """
        strategy = CheckOutputStrategy()
        # Expected result OK
        strategy.result('test_server_one', True, check='wal_level')
        records = list(caplog.records)
        assert len(records) == 1
        record = records.pop()
        assert record.msg == \
            "Check 'wal_level' succeeded for server 'test_server_one'"
        assert record.levelname == 'DEBUG'
        # Expected result FAILED
        strategy = CheckOutputStrategy()
        strategy.result('test_server_one', False, check='wal_level')
        strategy.result('test_server_one', False, check='backup maximum age')
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
        strategy.result('test_server_one', True, check='wal_level')
        strategy.result('test_server_one', True, check='archive_mode')
        assert ('', '') == capsys.readouterr()
        assert strategy.has_error is False
        assert strategy.check_result
        assert len(strategy.check_result) == 2
        # Expected two errors
        strategy = CheckStrategy()
        strategy.result('test_server_one', False, check='wal_level')
        strategy.result('test_server_one', False, check='archive_mode')
        assert ('', '') == capsys.readouterr()
        assert strategy.has_error is True
        assert strategy.check_result
        assert len(strategy.check_result) == 2
        assert len([result
                    for result in strategy.check_result
                    if not result.status]) == 2
        # Test Non blocking error behaviour (one non blocking error)
        strategy = CheckStrategy()
        strategy.result('test_server_one', False, check='backup maximum age')
        strategy.result('test_server_one', True, check='archive mode')
        assert ('', '') == capsys.readouterr()
        assert strategy.has_error is False
        assert strategy.check_result
        assert len(strategy.check_result) == 2
        assert len([result
                    for result in strategy.check_result
                    if not result.status]) == 1

        # Test Non blocking error behaviour (2 errors one is non blocking)
        strategy = CheckStrategy()
        strategy.result('test_server_one', False, check='backup maximum age')
        strategy.result('test_server_one', False, check='archive mode')
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
        strategy.result('test_server_one', True, check='wal_level')
        records = list(caplog.records)
        assert len(records) == 1
        record = records.pop()
        assert record.msg == \
            "Check 'wal_level' succeeded for server 'test_server_one'"
        assert record.levelname == 'DEBUG'
        # Expected result FAILED
        strategy.result('test_server_one', False, check='wal_level')
        records = list(caplog.records)
        assert len(records) == 2
        record = records.pop()
        assert record.levelname == 'ERROR'
        assert record.msg == \
            "Check 'wal_level' failed for server 'test_server_one'"
