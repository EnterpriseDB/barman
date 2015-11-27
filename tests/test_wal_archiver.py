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

from mock import ANY, patch

from barman.command_wrappers import CommandFailedException
from barman.wal_archiver import FileWalArchiver, StreamingWalArchiver
from testing_helpers import build_backup_manager


# noinspection PyMethodMayBeStatic
class TestWalArchiver(object):

    def test_filewalarchiver_init(self):
        """
        Basic init test for the FileWalArchiver class
        """
        backup_manager = build_backup_manager()
        FileWalArchiver(backup_manager)

    def test_filewalarchiver_get_remote_status(self):
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

    def test_streamingwalarchiver_init(self):
        """
        Basic init test for the StreamingWalArchiver class
        """
        backup_manager = build_backup_manager()
        StreamingWalArchiver(backup_manager)

    @patch("barman.utils.which")
    @patch("barman.wal_archiver.Command")
    def test_streamingwalarchiver_check_receivexlog_installed(
            self, command_mock, which_mock):
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
        result = archiver.get_remote_status()

        assert result == {
            "pg_receivexlog_installed": True,
            "pg_receivexlog_path": "/some/path/to/pg_receivexlog",
            "pg_receivexlog_compatible": None,
            "pg_receivexlog_version": None,
        }

    @patch("barman.utils.which")
    @patch("barman.wal_archiver.Command")
    def test_streamingwalarchiver_check_receivexlog_is_compatible(
            self, command_mock, which_mock):
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
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is False

        # Every pg_receivexlog is compatible with older PostgreSQL
        backup_manager.server.streaming.server_txt_version = "9.3.0"
        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.5"
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is True

        backup_manager.server.streaming.server_txt_version = "9.5.0"
        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.3"
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is False

        # Check for minor versions
        backup_manager.server.streaming.server_txt_version = "9.4.5"
        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.4.4"
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is True
