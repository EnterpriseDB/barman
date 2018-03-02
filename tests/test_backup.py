# Copyright (C) 2013-2018 2ndQuadrant Limited
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

import os
from datetime import datetime, timedelta

import dateutil.parser
import dateutil.tz
import mock
import pytest
from mock import Mock, patch

import barman.utils
from barman.exceptions import CompressionIncompatibility
from barman.infofile import BackupInfo
from testing_helpers import (build_backup_directories, build_backup_manager,
                             build_test_backup_info, caplog_reset)


# noinspection PyMethodMayBeStatic
class TestBackup(object):

    @patch('barman.backup.datetime')
    @patch('barman.backup.BackupInfo')
    @patch('barman.backup.BackupManager.get_last_backup_id')
    def test_backup_maximum_age(self, backup_id_mock, infofile_mock,
                                datetime_mock):
        # BackupManager setup
        backup_manager = build_backup_manager()
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

    @patch('barman.backup.BackupInfo')
    def test_keyboard_interrupt(self, mock_infofile):
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
        backup_manager = build_backup_manager()
        instance = mock_infofile.return_value
        # Instruct the patched method to raise a general exception
        backup_manager.executor.start_backup = Mock(
            side_effect=Exception('abc'))
        # invoke backup method
        backup_manager.backup()
        # verify that mock status is FAILED
        assert mock.call.set_attribute(
            'status', 'FAILED') in instance.mock_calls
        # Instruct the patched method to raise a KeyboardInterrupt
        backup_manager.executor.start_backup = Mock(
            side_effect=KeyboardInterrupt())
        # invoke backup method
        backup_manager.backup()
        # verify that mock status is FAILED
        assert mock.call.set_attribute(
            'status', 'FAILED') in instance.mock_calls

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
        backup_manager = build_backup_manager(backup_info.server)

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
        backup_manager = build_backup_manager()
        backup_manager.server.config.name = 'TestServer'
        backup_manager.server.config.barman_lock_directory = tmpdir.strpath
        backup_manager.server.config.backup_options = []

        # Create a fake backup directory inside tmpdir (old format)

        base_dir = tmpdir.mkdir('base')
        backup_dir = base_dir.mkdir('fake_backup_id')
        pg_data = backup_dir.mkdir('pgdata')
        pg_data_v2 = backup_dir.mkdir('data')
        wal_dir = tmpdir.mkdir('wals')
        wal_history_file02 = wal_dir.join('00000002.history')
        wal_history_file03 = wal_dir.join('00000003.history')
        wal_history_file04 = wal_dir.join('00000004.history')
        wal_history_file02.write('1\t0/2000028\tat restore point "myrp"\n')
        wal_history_file03.write('1\t0/2000028\tat restore point "myrp"\n')
        wal_history_file04.write('1\t0/2000028\tat restore point "myrp"\n')
        wal_history_file04.write('2\t0/3000028\tunknown\n')
        wal_file = wal_dir.join('0000000100000000/000000010000000000000001')
        wal_file.ensure()
        xlog_db = wal_dir.join('xlog.db')
        xlog_db.write(
            '000000010000000000000001\t42\t43\tNone\n'
            '00000002.history\t42\t43\tNone\n'
            '00000003.history\t42\t43\tNone\n'
            '00000004.history\t42\t43\tNone\n')
        backup_manager.server.xlogdb.return_value.__enter__.return_value = (
            xlog_db.open())
        backup_manager.server.config.basebackups_directory = base_dir.strpath
        backup_manager.server.config.wals_directory = wal_dir.strpath
        # The following tablespaces are defined in the default backup info
        # generated by build_test_backup_info
        b_info = build_test_backup_info(
            backup_id='fake_backup_id',
            server=backup_manager.server,
        )

        # Make sure we are not trying to delete any WAL file,
        # just by having a previous backup
        b_pre_info = build_test_backup_info(
            backup_id='fake_backup',
            server=backup_manager.server,
        )
        mock_available_backups.return_value = {
            "fake_backup": b_pre_info,
            "fake_backup_id": b_info,
        }

        # Test 1: minimum redundancy not satisfied
        caplog_reset(caplog)
        backup_manager.server.config.minimum_redundancy = 2
        b_info.set_attribute('backup_version', 1)
        build_backup_directories(b_info)
        backup_manager.delete_backup(b_info)
        assert 'WARNING  Skipping delete of backup ' in caplog.text
        assert 'ERROR' not in caplog.text
        assert os.path.exists(pg_data.strpath)
        assert not os.path.exists(pg_data_v2.strpath)
        assert os.path.exists(wal_file.strpath)
        assert os.path.exists(wal_history_file02.strpath)
        assert os.path.exists(wal_history_file03.strpath)
        assert os.path.exists(wal_history_file04.strpath)

        # Test 2: normal delete expecting no errors (old format)
        caplog_reset(caplog)
        backup_manager.server.config.minimum_redundancy = 1
        b_info.set_attribute('backup_version', 1)
        build_backup_directories(b_info)
        backup_manager.delete_backup(b_info)
        # the backup must not exists on disk anymore
        assert 'WARNING' not in caplog.text
        assert 'ERROR' not in caplog.text
        assert not os.path.exists(pg_data.strpath)
        assert not os.path.exists(pg_data_v2.strpath)
        assert os.path.exists(wal_file.strpath)
        assert os.path.exists(wal_history_file02.strpath)
        assert os.path.exists(wal_history_file03.strpath)
        assert os.path.exists(wal_history_file04.strpath)

        # Test 3: delete the backup again, expect a failure in log
        caplog_reset(caplog)
        backup_manager.delete_backup(b_info)
        assert 'ERROR    Failure deleting backup fake_backup_id' in caplog.text
        assert not os.path.exists(pg_data.strpath)
        assert not os.path.exists(pg_data_v2.strpath)
        assert os.path.exists(wal_file.strpath)
        assert os.path.exists(wal_history_file02.strpath)
        assert os.path.exists(wal_history_file03.strpath)
        assert os.path.exists(wal_history_file04.strpath)

        # Test 4: normal delete expecting no errors (new format)
        caplog_reset(caplog)
        b_info.set_attribute('backup_version', 2)
        build_backup_directories(b_info)
        backup_manager.delete_backup(b_info)
        assert 'WARNING' not in caplog.text
        assert 'ERROR' not in caplog.text
        assert not os.path.exists(pg_data.strpath)
        assert not os.path.exists(pg_data_v2.strpath)
        assert os.path.exists(wal_file.strpath)
        assert os.path.exists(wal_history_file02.strpath)
        assert os.path.exists(wal_history_file03.strpath)
        assert os.path.exists(wal_history_file04.strpath)

        # Test 5: normal delete of first backup no errors and no skip
        # removing one of the two backups present (new format)
        # and all the previous wal
        caplog_reset(caplog)
        b_pre_info.set_attribute('backup_version', 2)
        build_backup_directories(b_pre_info)
        backup_manager.delete_backup(b_pre_info)
        assert 'WARNING' not in caplog.text
        assert 'ERROR' not in caplog.text
        assert not os.path.exists(pg_data.strpath)
        assert not os.path.exists(pg_data_v2.strpath)
        assert not os.path.exists(wal_file.strpath)
        assert os.path.exists(wal_history_file02.strpath)
        assert os.path.exists(wal_history_file03.strpath)
        assert os.path.exists(wal_history_file04.strpath)

        # Test 6: normal delete of first backup no errors and no skip
        # removing one of the two backups present (new format)
        # the previous wal is retained as on a different timeline
        caplog_reset(caplog)
        wal_file.ensure()
        b_pre_info.set_attribute('timeline', 2)
        b_pre_info.set_attribute('backup_version', 2)
        build_backup_directories(b_pre_info)
        backup_manager.delete_backup(b_pre_info)
        assert 'WARNING' not in caplog.text
        assert 'ERROR' not in caplog.text
        assert not os.path.exists(pg_data.strpath)
        assert not os.path.exists(pg_data_v2.strpath)
        assert os.path.exists(wal_file.strpath)
        assert os.path.exists(wal_history_file02.strpath)
        assert os.path.exists(wal_history_file03.strpath)
        assert os.path.exists(wal_history_file04.strpath)

        # Test 7: simulate an error deleting the the backup.
        with patch('barman.backup.BackupManager.delete_backup_data')\
                as mock_delete_data:
            caplog_reset(caplog)
            # We force delete_pgdata method to raise an exception.
            mock_delete_data.side_effect = OSError('TestError')
            wal_file.ensure()
            b_pre_info.set_attribute('backup_version', 2)
            build_backup_directories(b_pre_info)
            backup_manager.delete_backup(b_info)
            assert 'TestError' in caplog.text
            assert os.path.exists(wal_file.strpath)
            assert os.path.exists(wal_history_file02.strpath)
            assert os.path.exists(wal_history_file03.strpath)
            assert os.path.exists(wal_history_file04.strpath)

    def test_available_backups(self, tmpdir):
        """
        Test the get_available_backups that retrieves all the
        backups from the backups_cache using a set of backup status as filter
        """
        # build a backup_manager and setup a basic configuration
        backup_manager = build_backup_manager(
            name='TestServer',
            global_conf={
                'barman_home': tmpdir.strpath
            })

        # BackupInfo object with status DONE
        b_info = build_test_backup_info(
            backup_id='fake_backup_id',
            server=backup_manager.server,
            status=BackupInfo.DONE
        )
        b_info.save()

        # Create a BackupInfo object with status FAILED
        failed_b_info = build_test_backup_info(
            backup_id='failed_backup_id',
            server=backup_manager.server,
            status=BackupInfo.FAILED
        )
        failed_b_info.save()

        assert backup_manager._backup_cache is None

        available_backups = backup_manager.get_available_backups(
            (BackupInfo.DONE,))

        assert available_backups[b_info.backup_id].to_dict() == (
            b_info.to_dict())
        # Check that the  failed backup have been filtered from the result
        assert failed_b_info.backup_id not in available_backups
        assert len(available_backups) == 1

    def test_load_backup_cache(self, tmpdir):
        """
        Check the loading of backups inside the backup_cache
        """
        # build a backup_manager and setup a basic configuration
        backup_manager = build_backup_manager(
            name='TestServer',
            global_conf={
                'barman_home': tmpdir.strpath
            })

        # Make sure the cache is uninitialized
        assert backup_manager._backup_cache is None

        # Create a BackupInfo object with status DONE
        b_info = build_test_backup_info(
            backup_id='fake_backup_id',
            server=backup_manager.server,
        )
        b_info.save()

        # Load backups inside the cache
        backup_manager._load_backup_cache()

        # Check that the test backup is inside the backups_cache
        assert backup_manager._backup_cache[b_info.backup_id].to_dict() == \
            b_info.to_dict()

    def test_backup_cache_add(self, tmpdir):
        """
        Check the method responsible for the registration of a BackupInfo obj
        into the backups cache
        """
        # build a backup_manager and setup a basic configuration
        backup_manager = build_backup_manager(
            name='TestServer',
            global_conf={
                'barman_home': tmpdir.strpath
            })

        # Create a BackupInfo object with status DONE
        b_info = build_test_backup_info(
            backup_id='fake_backup_id',
            server=backup_manager.server,
        )
        b_info.save()

        assert backup_manager._backup_cache is None

        # Register the object to cache. The cache is not initialized, so it
        # must load the cache from disk.
        backup_manager.backup_cache_add(b_info)
        # Check that the test backup is in the cache
        assert backup_manager.get_backup(b_info.backup_id) is b_info

        # Initialize an empty cache
        backup_manager._backup_cache = {}
        # Add the backup again
        backup_manager.backup_cache_add(b_info)
        assert backup_manager.get_backup(b_info.backup_id) is b_info

    def test_backup_cache_remove(self, tmpdir):
        """
        Check the method responsible for the removal of a BackupInfo object
        from the backups cache
        """
        # build a backup_manager and setup a basic configuration
        backup_manager = build_backup_manager(
            name='TestServer',
            global_conf={
                'barman_home': tmpdir.strpath
            })

        assert backup_manager._backup_cache is None

        # Create a BackupInfo object with status DONE
        b_info = build_test_backup_info(
            backup_id='fake_backup_id',
            server=backup_manager.server,
        )

        # Remove the backup from the uninitialized cache
        backup_manager.backup_cache_remove(b_info)
        # Check that the test backup is still not initialized
        assert backup_manager._backup_cache is None

        # Initialize the cache
        backup_manager._backup_cache = {b_info.backup_id: b_info}
        # Remove the backup from the cache
        backup_manager.backup_cache_remove(b_info)
        assert b_info.backup_id not in backup_manager._backup_cache

    def test_get_backup(self, tmpdir):
        """
        Check the get_backup method that uses the backups cache to retrieve
        a backup using the id
        """
        # Setup temp dir and server
        # build a backup_manager and setup a basic configuration
        backup_manager = build_backup_manager(
            name='TestServer',
            global_conf={
                'barman_home': tmpdir.strpath
            })

        # Create a BackupInfo object with status DONE
        b_info = build_test_backup_info(
            backup_id='fake_backup_id',
            server=backup_manager.server,
        )
        b_info.save()

        assert backup_manager._backup_cache is None

        # Check that the backup returned is the same
        assert backup_manager.get_backup(b_info.backup_id).to_dict() == \
            b_info.to_dict()

        # Empty the backup manager cache
        backup_manager._backup_cache = {}

        # Check that the backup returned is None
        assert backup_manager.get_backup(b_info.backup_id) is None

    def test_check_redundancy(self, tmpdir):
        """
        Test the check method
        """
        # Setup temp dir and server
        # build a backup_manager and setup a basic configuration
        backup_manager = build_backup_manager(
            name='TestServer',
            global_conf={
                'barman_home': tmpdir.strpath,
                'minimum_redundancy': "1"
            })
        backup_manager.executor = mock.MagicMock()

        # Test the unsatisfied minimum_redundancy option
        strategy_mock = mock.MagicMock()
        backup_manager.check(strategy_mock)
        # Expect a failure from the method
        strategy_mock.result.assert_called_with(
            'TestServer',
            False,
            hint='have 0 backups, expected at least 1'
        )
        # Test the satisfied minimum_redundancy option
        b_info = build_test_backup_info(
            backup_id='fake_backup_id',
            server=backup_manager.server,
        )
        b_info.save()

        strategy_mock.reset_mock()
        backup_manager._load_backup_cache()
        backup_manager.check(strategy_mock)
        # Expect a success from the method
        strategy_mock.result.assert_called_with(
            'TestServer',
            True,
            hint='have 1 backups, expected at least 1'
        )

        # Test for no failed backups
        strategy_mock.reset_mock()
        backup_manager._load_backup_cache()
        backup_manager.check(strategy_mock)
        # Expect a failure from the method
        strategy_mock.result.assert_any_call(
            'TestServer',
            True,
            hint='there are 0 failed backups'
        )

        # Test for failed backups in catalog
        b_info = build_test_backup_info(
            backup_id='failed_backup_id',
            server=backup_manager.server,
            status=BackupInfo.FAILED,
        )
        b_info.save()
        strategy_mock.reset_mock()
        backup_manager._load_backup_cache()
        backup_manager.check(strategy_mock)
        # Expect a failure from the method
        strategy_mock.result.assert_any_call(
            'TestServer',
            False,
            hint='there are 1 failed backups'
        )

        # Test unknown compression
        backup_manager.config.compression = 'test_compression'
        backup_manager.compression_manager.check.return_value = False
        strategy_mock.reset_mock()
        backup_manager.check(strategy_mock)
        # Expect a failure from the method
        strategy_mock.result.assert_any_call(
            'TestServer',
            False
        )

        # Test valid compression
        backup_manager.config.compression = 'test_compression'
        backup_manager.compression_manager.check.return_value = True
        strategy_mock.reset_mock()
        backup_manager.check(strategy_mock)
        # Expect a success from the method
        strategy_mock.result.assert_any_call(
            'TestServer',
            True
        )
        # Test failure retrieving a compressor
        backup_manager.config.compression = 'test_compression'
        backup_manager.compression_manager.check.return_value = True
        backup_manager.compression_manager.get_compressor.side_effect = \
            CompressionIncompatibility()
        strategy_mock.reset_mock()
        backup_manager.check(strategy_mock)
        # Expect a failure from the method
        strategy_mock.result.assert_any_call(
            'TestServer',
            False
        )

    def test_get_latest_archived_wals_info(self, tmpdir):
        """
        Test the get_latest_archived_wals_info method
        """
        # build a backup_manager and setup a basic configuration
        backup_manager = build_backup_manager(
            main_conf={
                'backup_directory': tmpdir.strpath,
            })

        # Test: insistent wals directory
        assert backup_manager.get_latest_archived_wals_info() is None

        # Test: empty wals directory
        wals = tmpdir.join('wals').ensure(dir=True)
        assert backup_manager.get_latest_archived_wals_info() is None

        # Test: ignore WAL-like files in the root
        wals.join('000000010000000000000003').ensure()
        assert backup_manager.get_latest_archived_wals_info() is None

        # Test: find the fist WAL
        wals.join('0000000100000000').join(
            '000000010000000000000001').ensure()
        latest = backup_manager.get_latest_archived_wals_info()
        assert latest
        assert len(latest) == 1
        assert latest['00000001'].name == '000000010000000000000001'

        # Test: find the 2nd WAL in the same dir
        wals.join('0000000100000000').join(
            '000000010000000000000002').ensure()
        latest = backup_manager.get_latest_archived_wals_info()
        assert latest
        assert len(latest) == 1
        assert latest['00000001'].name == '000000010000000000000002'

        # Test: the newer dir is empty
        wals.join('0000000100000001').ensure(dir=True)
        latest = backup_manager.get_latest_archived_wals_info()
        assert latest
        assert len(latest) == 1
        assert latest['00000001'].name == '000000010000000000000002'

        # Test: the newer contains a newer file
        wals.join('0000000100000001').join(
            '000000010000000100000001').ensure()
        latest = backup_manager.get_latest_archived_wals_info()
        assert latest
        assert len(latest) == 1
        assert latest['00000001'].name == '000000010000000100000001'

        # Test: ignore out of order files
        wals.join('0000000100000000').join('000000010000000100000005').ensure()
        latest = backup_manager.get_latest_archived_wals_info()
        assert latest
        assert len(latest) == 1
        assert latest['00000001'].name == '000000010000000100000001'

        # Test: find the 2nd timeline
        wals.join('0000000200000000').join(
            '000000020000000000000003').ensure()
        latest = backup_manager.get_latest_archived_wals_info()
        assert latest
        assert len(latest) == 2
        assert latest['00000001'].name == '000000010000000100000001'
        assert latest['00000002'].name == '000000020000000000000003'
