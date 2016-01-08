# Copyright (C) 2013-2015 2ndQuadrant Italia Srl
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

import pytest
from mock import ANY, patch

from barman.command_wrappers import CommandFailedException
from barman.server import CheckOutputStrategy
from barman.wal_archiver import (ArchiverFailure, FileWalArchiver,
                                 StreamingWalArchiver)
from testing_helpers import build_backup_manager


# noinspection PyMethodMayBeStatic
class TestFileWalArchiver(object):
    def test_init(self):
        """
        Basic init test for the FileWalArchiver class
        """
        backup_manager = build_backup_manager()
        FileWalArchiver(backup_manager)

    def test_get_remote_status(self):
        """
        Basic test for the check method of the FileWalArchiver class
        """
        # Create a backup_manager
        backup_manager = build_backup_manager()
        # Set up mock responses
        postgres = backup_manager.server.postgres
        postgres.get_setting.side_effect = ["value1", "value2"]
        postgres.get_archiver_stats.return_value = {
            'pg_stat_archiver': 'value3'
        }
        # Instantiate a FileWalArchiver obj
        archiver = FileWalArchiver(backup_manager)
        result = {
            'archive_mode': 'value1',
            'archive_command': 'value2',
            'pg_stat_archiver': 'value3'
        }
        # Compare results of the check method
        assert archiver.get_remote_status() == result

    @patch('barman.wal_archiver.FileWalArchiver.get_remote_status')
    def test_check(self, remote_mock, capsys):
        """
        Test management of check_postgres view output

        :param remote_mock: mock get_remote_status function
        :param capsys: retrieve output from consolle
        """
        # Create a backup_manager
        backup_manager = build_backup_manager()
        # Set up mock responses
        postgres = backup_manager.server.postgres
        postgres.server_version = 90501
        # Instantiate a FileWalArchiver obj
        archiver = FileWalArchiver(backup_manager)
        # Prepare the output check strategy
        strategy = CheckOutputStrategy()
        # Case: no reply by PostgreSQL
        remote_mock.return_value = {
            'archive_mode': None,
            'archive_command': None,
        }
        # Expect no output from check
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert out == ''
        # Case: correct configuration
        remote_mock.return_value = {
            'archive_mode': 'on',
            'archive_command': 'wal to archive',
            'is_archiving': True,
        }
        # Expect out: all parameters: OK
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert out == \
            "\tarchive_mode: OK\n" \
            "\tarchive_command: OK\n" \
            "\tcontinuous archiving: OK\n"

        # Case: archive_command value is not acceptable
        remote_mock.return_value = {
            'archive_command': None,
            'archive_mode': 'on',
            'is_archiving': False,
        }
        # Expect out: some parameters: FAILED
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert out == \
            "\tarchive_mode: OK\n" \
            "\tarchive_command: FAILED " \
            "(please set it accordingly to documentation)\n"
        # Case: all but is_archiving ok
        remote_mock.return_value = {
            'archive_mode': 'on',
            'archive_command': 'wal to archive',
            'is_archiving': False,
        }
        # Expect out: all parameters: OK
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert out == \
            "\tarchive_mode: OK\n" \
            "\tarchive_command: OK\n" \
            "\tcontinuous archiving: FAILED\n"


# noinspection PyMethodMayBeStatic
class TestStreamingWalArchiver(object):
    def test_init(self):
        """
        Basic init test for the StreamingWalArchiver class
        """
        backup_manager = build_backup_manager()
        StreamingWalArchiver(backup_manager)

    @patch("barman.utils.which")
    @patch("barman.wal_archiver.Command")
    def test_check_receivexlog_installed(self, command_mock, which_mock):
        """
        Test for the check method of the StreamingWalArchiver class
        """
        backup_manager = build_backup_manager()
        backup_manager.server.postgres.server_txt_version = "9.2"
        which_mock.return_value = None

        archiver = StreamingWalArchiver(backup_manager)
        result = archiver.get_remote_status()

        which_mock.assert_called_with('pg_receivexlog', ANY)
        assert result == {
            "pg_receivexlog_installed": False,
            "pg_receivexlog_path": None,
            "pg_receivexlog_compatible": None,
            "pg_receivexlog_version": None,
        }

        backup_manager.server.postgres.server_txt_version = "9.2"
        which_mock.return_value = '/some/path/to/pg_receivexlog'
        command_mock.return_value.side_effect = CommandFailedException
        archiver.reset_remote_status()
        result = archiver.get_remote_status()

        assert result == {
            "pg_receivexlog_installed": True,
            "pg_receivexlog_path": "/some/path/to/pg_receivexlog",
            "pg_receivexlog_compatible": None,
            "pg_receivexlog_version": None,
        }

    @patch("barman.utils.which")
    @patch("barman.wal_archiver.Command")
    def test_check_receivexlog_is_compatible(self, command_mock, which_mock):
        """
        Test for the compatibility checks between versions of pg_receivexlog
        and PostgreSQL
        """
        # pg_receivexlog 9.2 is compatible only with PostgreSQL 9.2
        backup_manager = build_backup_manager()
        backup_manager.server.streaming.server_txt_version = "9.2.0"
        archiver = StreamingWalArchiver(backup_manager)
        which_mock.return_value = '/some/path/to/pg_receivexlog'

        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.2"
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is True

        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.5"
        archiver.reset_remote_status()
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is False

        # Every pg_receivexlog is compatible with older PostgreSQL
        backup_manager.server.streaming.server_txt_version = "9.3.0"
        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.5"
        archiver.reset_remote_status()
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is True

        backup_manager.server.streaming.server_txt_version = "9.5.0"
        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.3"
        archiver.reset_remote_status()
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is False

        # Check for minor versions
        backup_manager.server.streaming.server_txt_version = "9.4.5"
        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.4.4"
        archiver.reset_remote_status()
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is True

    @patch("barman.wal_archiver.StreamingWalArchiver.get_remote_status")
    @patch("barman.wal_archiver.PgReceiveXlog")
    def test_receive_wal(self, receivexlog_mock, remote_mock):
        backup_manager = build_backup_manager()
        backup_manager.server.streaming.server_txt_version = "9.4.0"
        backup_manager.server.streaming.get_remote_status.return_value = {
            "streaming_supported": True
        }

        # Test: normal run
        archiver = StreamingWalArchiver(backup_manager)
        remote_mock.return_value = {
            'pg_receivexlog_installed': True,
            'pg_receivexlog_compatible': True,
            'pg_receivexlog_path': 'fake/path'
        }
        archiver.receive_wal()
        receivexlog_mock.assert_called_once_with(
            'fake/path',
            'host=pg01.nowhere user=postgres port=5432',
            '/some/barman/home/main/streaming'
        )
        receivexlog_mock.return_value.execute.assert_called_once_with()

        # Test: incompatible pg_receivexlog
        with pytest.raises(ArchiverFailure):
            remote_mock.return_value = {
                'pg_receivexlog_installed': True,
                'pg_receivexlog_compatible': False,
                'pg_receivexlog_path': 'fake/path'
            }
            archiver.receive_wal()

        # Test: missing pg_receivexlog
        with pytest.raises(ArchiverFailure):
            remote_mock.return_value = {
                'pg_receivexlog_installed': False,
                'pg_receivexlog_compatible': True,
                'pg_receivexlog_path': 'fake/path'
            }
            archiver.receive_wal()
        # Test: impossible to connect with streaming protocol
        with pytest.raises(ArchiverFailure):
            backup_manager.server.streaming.get_remote_status.return_value = {
                'streaming_supported': None
            }
            remote_mock.return_value = {
                'pg_receivexlog_installed': True,
                'pg_receivexlog_compatible': True,
                'pg_receivexlog_path': 'fake/path'
            }
            archiver.receive_wal()
        # Test: PostgreSQL too old
        with pytest.raises(ArchiverFailure):
            backup_manager.server.streaming.get_remote_status.return_value = {
                'streaming_supported': False
            }
            remote_mock.return_value = {
                'pg_receivexlog_installed': True,
                'pg_receivexlog_compatible': True,
                'pg_receivexlog_path': 'fake/path'
            }
            archiver.receive_wal()
        # Test: general failure executing pg_receivexlog
        with pytest.raises(ArchiverFailure):
            remote_mock.return_value = {
                'pg_receivexlog_installed': True,
                'pg_receivexlog_compatible': True,
                'pg_receivexlog_path': 'fake/path'
            }
            receivexlog_mock.return_value.execute.side_effect = \
                CommandFailedException
            archiver.receive_wal()

    @patch("barman.utils.which")
    @patch("barman.wal_archiver.Command")
    def test_when_streaming_connection_rejected(
            self, command_mock, which_mock):
        """
        Test the StreamingWalArchiver behaviour when the streaming
        connection is rejected by the PostgreSQL server and
        pg_receivexlog is installed.
        """

        # When the streaming connection is not available, the
        # server_txt_version property will have a None value.
        backup_manager = build_backup_manager()
        backup_manager.server.streaming.server_txt_version = None
        archiver = StreamingWalArchiver(backup_manager)
        which_mock.return_value = '/some/path/to/pg_receivexlog'
        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.2"

        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is None

    @patch('barman.wal_archiver.StreamingWalArchiver.get_remote_status')
    def test_check(self, remote_mock, capsys):
        """
        Test management of check_postgres view output

        :param remote_mock: mock get_remote_status function
        :param capsys: retrieve output from consolle
        """
        # Create a backup_manager
        backup_manager = build_backup_manager()
        # Set up mock responses
        streaming = backup_manager.server.streaming
        streaming.server_txt_version = '9.5'
        # Instantiate a StreamingWalArchiver obj
        archiver = StreamingWalArchiver(backup_manager)
        # Prepare the output check strategy
        strategy = CheckOutputStrategy()
        # Case: correct configuration
        remote_mock.return_value = {
            'pg_receivexlog_installed': True,
            'pg_receivexlog_compatible': True,
            'pg_receivexlog_path': 'fake/path',
        }
        # Expect out: all parameters: OK
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert out == \
            "\tpg_receivexlog: OK\n" \
            "\tpg_receivexlog compatible: OK\n"

        # Case: pg_receivexlog is not compatible
        remote_mock.return_value = {
            'pg_receivexlog_installed': True,
            'pg_receivexlog_compatible': False,
            'pg_receivexlog_path': 'fake/path',
            'pg_receivexlog_version': '9.2',
        }
        # Expect out: some parameters: FAILED
        strategy = CheckOutputStrategy()
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert out == \
            "\tpg_receivexlog: OK\n" \
            "\tpg_receivexlog compatible: FAILED " \
            "(PostgreSQL version: 9.5, pg_receivexlog version: 9.2)\n"
        # Case: pg_receivexlog returned error
        remote_mock.return_value = {
            'pg_receivexlog_installed': True,
            'pg_receivexlog_compatible': None,
            'pg_receivexlog_path': 'fake/path',
            'pg_receivexlog_version': None,
        }
        # Expect out: all parameters: OK
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert out == \
            "\tpg_receivexlog: OK\n" \
            "\tpg_receivexlog compatible: FAILED " \
            "(PostgreSQL version: 9.5, pg_receivexlog version: None)\n"
