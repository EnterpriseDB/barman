# Copyright (C) 2013-2014 2ndQuadrant Italia (Devise.IT S.r.L.)
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

from mock import patch, Mock, call
import pytest
from datetime import timedelta, datetime
from barman.backup import BackupManager, DataTransferFailure
import barman.utils


class TestBackup(object):
    @staticmethod
    def build_backup_manager():
        # instantiate a BackupManager object using mocked parameters
        server = Mock(name='server')
        server.config = Mock(name='config')
        with patch("barman.backup.CompressionManager"):
            return BackupManager(server=server)

    @patch('time.sleep')
    def test_retry(self, sleep_moc):
        # BackupManager setup
        backup_manager = self.build_backup_manager()
        backup_manager.config.basebackup_retry_times = 5
        backup_manager.config.basebackup_retry_sleep = 10
        f = Mock()

        # check for correct return value
        r = backup_manager.retry_backup_copy(f, 'test string')
        f.assert_called_with('test string')
        assert f.return_value == r

        # check for correct number of calls
        expected = Mock()
        f = Mock(side_effect=[DataTransferFailure('testException'), expected])
        r = backup_manager.retry_backup_copy(f, 'test string')
        assert f.call_count == 2

        # check for correct number of tries and invocations of sleep method
        sleep_moc.reset_mock()
        e = DataTransferFailure('testException')
        f = Mock(side_effect=[e, e, e, e, e, e])
        with pytest.raises(DataTransferFailure):
            backup_manager.retry_backup_copy(f, 'test string')

        assert sleep_moc.call_count == 5
        assert f.call_count == 6

    @patch('barman.backup.datetime')
    @patch('barman.backup.BackupInfo')
    @patch('barman.backup.BackupManager.get_last_backup')
    def test_backup_maximum_age(self, backup_id_mock, infofile_mock,
                                datetime_mock):

        # BackupManager setup
        backup_manager = self.build_backup_manager()
        # setting basic configuration for this test
        backup_manager.config.last_backup_maximum_age = timedelta(days=7)
        # force the tests to use the same values for the now() method,
        # doing so the result is predictable
        now = datetime.now()

        # case 1: No available backups
        # set the mock to None, simulating a no backup situation
        backup_id_mock.return_value = None
        datetime_mock.datetime.now.return_value = now
        r = backup_manager.validate_last_backup_maximum_age(
            backup_manager.config.last_backup_maximum_age)

        assert r[0] is False, r[1] == "No available backups"

        # case 2: backup older than the 1 day limit
        # mocking the backup id to a custom value
        backup_id_mock.return_value = "Mock_backup"
        # simulate an existing backup using a mock obj
        instance = infofile_mock.return_value
        #force the backup end date over 1 day over the limit
        instance.end_time = now - timedelta(days=8)
        # build the expected message
        msg = barman.utils.human_readable_timedelta(now - instance.end_time)
        r = backup_manager.validate_last_backup_maximum_age(
            backup_manager.config.last_backup_maximum_age)
        assert (r[0], r[1]) == (False, msg)

        # case 3: backup inside the one day limit
        # mocking the backup id to a custom value
        backup_id_mock.return_value = "Mock_backup"
        # simulate an existing backup using a mock obj
        instance = infofile_mock.return_value
        # set the backup end date inside the limit
        instance.end_time = now - timedelta(days=2)
        # build the expected msg
        msg = barman.utils.human_readable_timedelta(now - instance.end_time)
        r = backup_manager.validate_last_backup_maximum_age(
            backup_manager.config.last_backup_maximum_age)
        assert (r[0], r[1]) == (True, msg)

    @patch('barman.backup.BackupManager.backup_start')
    @patch('barman.backup.BackupInfo')
    def test_keyboard_interrupt(self, mock_infofile, mock_start):
        """
        Integration test for a quick check on exception catching
        during backup operations

        Test case 1: raise a general exception, backup status in
        BackupInfo should be FAILED.

        Test case 2: raise a KeyboardInterrupt exception, simulating
        a user pressing CTRL + C while a backup is in progress,
        backup status in BackupInfo should be FAILED.
        """
        # BackupManager setup
        backup_manager = self.build_backup_manager()
        instance = mock_infofile.return_value
        # Instruct the patched method to raise a general exception
        mock_start.side_effect = Exception('abc')
        # invoke backup method
        backup_manager.backup()
        # verify that mock status is FAILED
        assert call.set_attribute('status', 'FAILED') in instance.mock_calls
        # Instruct the patched method to raise a KeyboardInterrupt
        mock_start.side_effect = KeyboardInterrupt()
        # invoke backup method
        backup_manager.backup()
        # verify that mock status is FAILED
        assert call.set_attribute('status', 'FAILED') in instance.mock_calls

