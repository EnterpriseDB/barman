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

from mock import MagicMock, patch, Mock
import pytest
from barman.backup import BackupManager, DataTransferFailure


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