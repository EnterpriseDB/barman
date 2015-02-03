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


from datetime import timedelta, datetime
import os

import pytest
import dateutil.parser
import dateutil.tz
from mock import patch, Mock, call

from barman.backup import BackupManager, DataTransferFailure
from barman.testing_helpers import build_test_backup_info
import barman.utils


class TestBackup(object):
    @staticmethod
    def build_backup_manager(server=None):
        # instantiate a BackupManager object using mocked parameters
        if server is None:
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
        # force the backup end date over 1 day over the limit
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
        Unit test for a quick check on exception catching
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

    def test_dateutil_parser(self, tmpdir, capsys):
        """
        Unit test for dateutil package during recovery.
        This test checks that a SystemExit error is raised when a wrong
        target_time parameter is passed in a recover invocation.

        This test doesn't cover all the recover code

        :param tmpdir: temporary folder
        """
        # test dir
        test_dir = tmpdir.mkdir("recover")
        # BackupInfo setup
        backup_info = build_test_backup_info(tablespaces=None)
        # BackupManager setup
        backup_manager = self.build_backup_manager(backup_info.server)

        # test 1
        # use dateutil to parse a date in our desired format
        assert dateutil.parser.parse("2015-02-13 11:44:22.123") == \
            datetime(year=2015, month=2, day=13,
                     hour=11, minute=44, second=22, microsecond=123000)

        # test 2: parse the ctime output
        test_date = datetime.now()
        # remove microseconds as ctime() doesn't output them
        test_date = test_date.replace(microsecond=0)
        assert dateutil.parser.parse(test_date.ctime()) == test_date

        # test 3: parse the str output on local timezone
        test_date = datetime.now(dateutil.tz.tzlocal())
        assert dateutil.parser.parse(str(test_date)) == test_date

        # test 4: check behaviour with a bad date
        # capture ValueError because target_time = 'foo bar'
        with pytest.raises(SystemExit):
            backup_manager.recover(backup_info,
                                   test_dir.strpath, None, None,
                                   'foo bar', None, "name", True, None)
        # checked that the raised error is the correct error
        (out, err) = capsys.readouterr()
        assert "unable to parse the target time parameter " in err

    @patch('barman.backup.BackupManager.get_available_backups')
    def test_delete_backup(self, mock_available_backups, tmpdir, caplog):
        """
        Simple test for the deletion of a backup.
        We want to test the behaviour of the delete_backup method
        """
        # Setup of the test backup_manager
        backup_manager = self.build_backup_manager()
        backup_manager.server.config.name = 'TestServer'
        backup_manager.server.config.basebackups_directory = tmpdir.strpath
        backup_manager.server.config.backup_options = []

        # Create a fake backup directory inside tmpdir (old format)
        backup_dir = tmpdir.mkdir('fake_backup_id')
        pg_data = backup_dir.mkdir('pgdata')
        pg_tblspc = pg_data.mkdir('pg_tblspc')
        # The following tablespaces are defined in the default backup info
        # generated by build_test_backup_info
        pg_tblspc.mkdir('16387')
        pg_tblspc.mkdir('16405')
        b_info = build_test_backup_info(
            backup_id='fake_backup_id',
            server=backup_manager.server,
        )
        # Make sure we are not trying to delete any WAL file,
        # just by having a previous backup
        mock_available_backups.return_value = {
            "fake_backup": build_test_backup_info(server=backup_manager.server),
            "fake_backup_id": b_info,
        }

        # Test 1: minimum redundancy not satisfied
        backup_manager.server.config.minimum_redundancy = 2
        del caplog.records()[:]  # remove previous messages from caplog
        backup_manager.delete_backup(b_info)
        assert 'WARNING  Skipping delete of backup ' in caplog.text()

        # Test 2: normal delete expecting no errors (old format)
        backup_manager.server.config.minimum_redundancy = 1
        backup_manager.delete_backup(b_info)
        # the backup must not exists on disk anymore
        assert not os.path.exists(pg_data.strpath)

        # Test 3: delete the backup again, expect a failure in log
        del caplog.records()[:]  # remove previous messages from caplog
        backup_manager.delete_backup(b_info)
        assert 'Failure deleting backup fake_backup_id' in caplog.text()

        # Create a fake backup directory inside tmpdir (new format)
        backup_dir = tmpdir.mkdir('fake_backup_id')
        backup_dir.mkdir('data')
        # The following tablespaces are defined in the default backup info
        # generated by build_test_backup_info
        backup_dir.mkdir('16387')
        backup_dir.mkdir('16405')
        b_info = build_test_backup_info(
            backup_id='fake_backup_id',
            server=backup_manager.server,
        )

        # Test 4: normal delete expecting no errors (new format)
        del caplog.records()[:]  # remove previous messages from caplog
        backup_manager.delete_backup(b_info)
        assert 'WARNING  Skipping delete of backup ' in caplog.text()

        # Test 5: simulate and error deleting the the backup.
        with patch('barman.backup.BackupManager.delete_backup_data')\
                as mock_delete_data:
            # We force delete_pgdata method to raise an exception.
            mock_delete_data.side_effect = OSError('TestError')
            del caplog.records()[:]  # remove previous messages from caplog
            backup_manager.delete_backup(b_info)
            assert 'TestError' in caplog.text()
