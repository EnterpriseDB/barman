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

from barman.wal_archiver import FileWalArchiver
from testing_helpers import build_backup_manager


# noinspection PyMethodMayBeStatic
class TestWalArchiver(object):

    def test_filewalarchiver_init(self):
        """
        Basic init test for the FileWalArchiver class
        """
        backup_manager = build_backup_manager()
        FileWalArchiver(backup_manager)

    def test_filewalarchiver_check(self):
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
