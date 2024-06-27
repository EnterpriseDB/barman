# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2023
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

import errno
import os
import re
from datetime import datetime, timedelta

import dateutil.parser
import dateutil.tz
import mock
import pytest
from mock import Mock, patch
from barman.backup import BackupManager
from barman.lockfile import ServerBackupIdLock

import barman.utils
from barman.annotations import KeepManager
from barman.config import BackupOptions
from barman.exceptions import (
    BackupException,
    CompressionIncompatibility,
    RecoveryInvalidTargetException,
    CommandFailedException,
)
from barman.infofile import BackupInfo
from barman.retention_policies import RetentionPolicyFactory
from testing_helpers import (
    build_backup_directories,
    build_backup_manager,
    build_mocked_server,
    build_test_backup_info,
    caplog_reset,
    interpolate_wals,
)


# noinspection PyMethodMayBeStatic
class TestBackup(object):
    @patch("barman.backup.datetime")
    @patch("barman.backup.LocalBackupInfo")
    @patch("barman.backup.BackupManager.get_last_backup_id")
    def test_backup_maximum_age(self, backup_id_mock, infofile_mock, datetime_mock):
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
            backup_manager.config.last_backup_maximum_age
        )

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
            backup_manager.config.last_backup_maximum_age
        )
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
            backup_manager.config.last_backup_maximum_age
        )
        assert (r[0], r[1]) == (True, msg)

    @patch("barman.backup.LocalBackupInfo")
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
        backup_manager.executor.start_backup = Mock(side_effect=Exception("abc"))
        # invoke backup method
        result = backup_manager.backup()
        # verify that mock status is FAILED
        assert mock.call.set_attribute("status", "FAILED") in instance.mock_calls
        # verify that a backup info has been returned
        assert result is not None
        # Instruct the patched method to raise a KeyboardInterrupt
        backup_manager.executor.start_backup = Mock(side_effect=KeyboardInterrupt())
        # invoke backup method
        result = backup_manager.backup()
        # verify that a backup info has been returned
        assert result is not None
        # verify that mock status is FAILED
        assert mock.call.set_attribute("status", "FAILED") in instance.mock_calls

    def test_dateutil_parser(self, tmpdir):
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
        assert dateutil.parser.parse("2015-02-13 11:44:22.123") == datetime(
            year=2015,
            month=2,
            day=13,
            hour=11,
            minute=44,
            second=22,
            microsecond=123000,
        )

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
        with pytest.raises(RecoveryInvalidTargetException) as exc:
            backup_manager.recover(
                backup_info,
                test_dir.strpath,
                None,
                None,
                target_time="foo bar",
                target_name="name",
                target_immediate=True,
            )
        # checked that the raised error is the correct error
        assert "Unable to parse the target time parameter " in str(exc.value)

    @patch("barman.backup.BackupManager.get_available_backups")
    def test_delete_backup(self, mock_available_backups, tmpdir, caplog):
        """
        Simple test for the deletion of a backup.
        We want to test the behaviour of the delete_backup method
        """
        # Setup of the test backup_manager
        backup_manager = build_backup_manager()
        backup_manager.server.config.name = "TestServer"
        backup_manager.server.config.barman_lock_directory = tmpdir.strpath
        backup_manager.server.config.backup_options = []

        # Create a fake backup directory inside tmpdir (old format)

        base_dir = tmpdir.mkdir("base")
        backup_dir = base_dir.mkdir("fake_backup_id")
        pg_data = backup_dir.mkdir("pgdata")
        pg_data_v2 = backup_dir.mkdir("data")
        wal_dir = tmpdir.mkdir("wals")
        wal_history_file02 = wal_dir.join("00000002.history")
        wal_history_file03 = wal_dir.join("00000003.history")
        wal_history_file04 = wal_dir.join("00000004.history")
        wal_history_file02.write('1\t0/2000028\tat restore point "myrp"\n')
        wal_history_file03.write('1\t0/2000028\tat restore point "myrp"\n')
        wal_history_file04.write('1\t0/2000028\tat restore point "myrp"\n')
        wal_history_file04.write("2\t0/3000028\tunknown\n")
        wal_file = wal_dir.join("0000000100000000/000000010000000000000001")
        wal_file.ensure()
        xlog_db = wal_dir.join("xlog.db")
        xlog_db.write(
            "000000010000000000000001\t42\t43\tNone\n"
            "00000002.history\t42\t43\tNone\n"
            "00000003.history\t42\t43\tNone\n"
            "00000004.history\t42\t43\tNone\n"
        )
        backup_manager.server.xlogdb.return_value.__enter__.return_value = xlog_db.open(
            mode="r+"
        )
        backup_manager.server.config.basebackups_directory = base_dir.strpath
        backup_manager.server.config.wals_directory = wal_dir.strpath
        # The following tablespaces are defined in the default backup info
        # generated by build_test_backup_info
        b_info = build_test_backup_info(
            backup_id="fake_backup_id",
            server=backup_manager.server,
        )

        # Make sure we are not trying to delete any WAL file,
        # just by having a previous backup
        b_pre_info = build_test_backup_info(
            backup_id="fake_backup",
            server=backup_manager.server,
        )
        mock_available_backups.return_value = {
            "fake_backup": b_pre_info,
            "fake_backup_id": b_info,
        }

        # Test 1: normal delete expecting no errors (old format)
        caplog_reset(caplog)
        backup_manager.server.config.minimum_redundancy = 1
        b_info.set_attribute("backup_version", 1)
        build_backup_directories(b_info)
        backup_manager.delete_backup(b_info)
        # the backup must not exists on disk anymore
        assert "WARNING" not in caplog.text
        assert "ERROR" not in caplog.text
        assert not os.path.exists(pg_data.strpath)
        assert not os.path.exists(pg_data_v2.strpath)
        assert os.path.exists(wal_file.strpath)
        assert os.path.exists(wal_history_file02.strpath)
        assert os.path.exists(wal_history_file03.strpath)
        assert os.path.exists(wal_history_file04.strpath)

        # Test 2: delete the backup again, expect a failure in log
        caplog_reset(caplog)
        backup_manager.delete_backup(b_info)
        assert re.search("ERROR .* Failure deleting backup fake_backup_id", caplog.text)
        assert not os.path.exists(pg_data.strpath)
        assert not os.path.exists(pg_data_v2.strpath)
        assert os.path.exists(wal_file.strpath)
        assert os.path.exists(wal_history_file02.strpath)
        assert os.path.exists(wal_history_file03.strpath)
        assert os.path.exists(wal_history_file04.strpath)

        # Test 3: normal delete expecting no errors (new format)
        caplog_reset(caplog)
        b_info.set_attribute("backup_version", 2)
        build_backup_directories(b_info)
        backup_manager.delete_backup(b_info)
        assert "WARNING" not in caplog.text
        assert "ERROR" not in caplog.text
        assert not os.path.exists(pg_data.strpath)
        assert not os.path.exists(pg_data_v2.strpath)
        assert os.path.exists(wal_file.strpath)
        assert os.path.exists(wal_history_file02.strpath)
        assert os.path.exists(wal_history_file03.strpath)
        assert os.path.exists(wal_history_file04.strpath)

        # Test 4: normal delete of first backup no errors and no skip
        # removing one of the two backups present (new format)
        # and all the previous wal
        caplog_reset(caplog)
        b_pre_info.set_attribute("backup_version", 2)
        build_backup_directories(b_pre_info)
        backup_manager.delete_backup(b_pre_info)
        assert "WARNING" not in caplog.text
        assert "ERROR" not in caplog.text
        assert not os.path.exists(pg_data.strpath)
        assert not os.path.exists(pg_data_v2.strpath)
        assert not os.path.exists(wal_file.strpath)
        assert os.path.exists(wal_history_file02.strpath)
        assert os.path.exists(wal_history_file03.strpath)
        assert os.path.exists(wal_history_file04.strpath)

        # Test 5: normal delete of first backup no errors and no skip
        # removing one of the two backups present (new format)
        # the previous wal is retained as on a different timeline
        caplog_reset(caplog)
        wal_file.ensure()
        b_pre_info.set_attribute("timeline", 2)
        b_pre_info.set_attribute("backup_version", 2)
        build_backup_directories(b_pre_info)
        backup_manager.delete_backup(b_pre_info)
        assert "WARNING" not in caplog.text
        assert "ERROR" not in caplog.text
        assert not os.path.exists(pg_data.strpath)
        assert not os.path.exists(pg_data_v2.strpath)
        assert os.path.exists(wal_file.strpath)
        assert os.path.exists(wal_history_file02.strpath)
        assert os.path.exists(wal_history_file03.strpath)
        assert os.path.exists(wal_history_file04.strpath)

        # Test 6: simulate an error deleting the backup.
        with patch(
            "barman.backup.BackupManager.delete_backup_data"
        ) as mock_delete_data:
            caplog_reset(caplog)
            # We force delete_pgdata method to raise an exception.
            mock_delete_data.side_effect = OSError("TestError")
            wal_file.ensure()
            b_pre_info.set_attribute("backup_version", 2)
            build_backup_directories(b_pre_info)
            backup_manager.delete_backup(b_info)
            assert "TestError" in caplog.text
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
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )
        backup_manager.server.systemid = "123"

        # BackupInfo object with status DONE
        b_info = build_test_backup_info(
            backup_id="fake_backup_id",
            server=backup_manager.server,
            status=BackupInfo.DONE,
        )
        b_info.save()

        # Create a BackupInfo object with status FAILED
        failed_b_info = build_test_backup_info(
            backup_id="failed_backup_id",
            server=backup_manager.server,
            status=BackupInfo.FAILED,
        )
        failed_b_info.save()

        assert backup_manager._backup_cache is None

        available_backups = backup_manager.get_available_backups((BackupInfo.DONE,))

        assert available_backups[b_info.backup_id].to_dict() == (b_info.to_dict())
        # Check that the  failed backup have been filtered from the result
        assert failed_b_info.backup_id not in available_backups
        assert len(available_backups) == 1

    def test_load_backup_cache(self, tmpdir):
        """
        Check the loading of backups inside the backup_cache
        """
        # build a backup_manager and setup a basic configuration
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )

        # Make sure the cache is uninitialized
        assert backup_manager._backup_cache is None

        # Create a BackupInfo object with status DONE
        b_info = build_test_backup_info(
            backup_id="fake_backup_id",
            server=backup_manager.server,
        )
        b_info.save()

        # Load backups inside the cache
        backup_manager._load_backup_cache()

        # Check that the test backup is inside the backups_cache
        assert (
            backup_manager._backup_cache[b_info.backup_id].to_dict() == b_info.to_dict()
        )

    def test_backup_cache_add(self, tmpdir):
        """
        Check the method responsible for the registration of a BackupInfo obj
        into the backups cache
        """
        # build a backup_manager and setup a basic configuration
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )

        # Create a BackupInfo object with status DONE
        b_info = build_test_backup_info(
            backup_id="fake_backup_id",
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
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )

        assert backup_manager._backup_cache is None

        # Create a BackupInfo object with status DONE
        b_info = build_test_backup_info(
            backup_id="fake_backup_id",
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
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )

        # Create a BackupInfo object with status DONE
        b_info = build_test_backup_info(
            backup_id="fake_backup_id",
            server=backup_manager.server,
        )
        b_info.save()

        assert backup_manager._backup_cache is None

        # Check that the backup returned is the same
        assert backup_manager.get_backup(b_info.backup_id).to_dict() == b_info.to_dict()

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
            name="TestServer",
            global_conf={"barman_home": tmpdir.strpath, "minimum_redundancy": "1"},
        )
        backup_manager.executor = mock.MagicMock()

        # Test the unsatisfied minimum_redundancy option
        strategy_mock = mock.MagicMock()
        backup_manager.check(strategy_mock)
        # Expect a failure from the method
        strategy_mock.result.assert_called_with(
            "TestServer", False, hint="have 0 backups, expected at least 1"
        )
        # Test the satisfied minimum_redundancy option
        b_info = build_test_backup_info(
            backup_id="fake_backup_id",
            server=backup_manager.server,
        )
        b_info.save()

        strategy_mock.reset_mock()
        backup_manager._load_backup_cache()
        backup_manager.check(strategy_mock)
        # Expect a success from the method
        strategy_mock.result.assert_called_with(
            "TestServer", True, hint="have 1 backups, expected at least 1"
        )

        # Test for no failed backups
        strategy_mock.reset_mock()
        backup_manager._load_backup_cache()
        backup_manager.check(strategy_mock)
        # Expect a failure from the method
        strategy_mock.result.assert_any_call(
            "TestServer", True, hint="there are 0 failed backups"
        )

        # Test for failed backups in catalog
        b_info = build_test_backup_info(
            backup_id="failed_backup_id",
            server=backup_manager.server,
            status=BackupInfo.FAILED,
        )
        b_info.save()
        strategy_mock.reset_mock()
        backup_manager._load_backup_cache()
        backup_manager.check(strategy_mock)
        # Expect a failure from the method
        strategy_mock.result.assert_any_call(
            "TestServer", False, hint="there are 1 failed backups"
        )

        # Test unknown compression
        backup_manager.config.compression = "test_compression"
        backup_manager.compression_manager.check.return_value = False
        strategy_mock.reset_mock()
        backup_manager.check(strategy_mock)
        # Expect a failure from the method
        strategy_mock.result.assert_any_call("TestServer", False)

        # Test valid compression
        backup_manager.config.compression = "test_compression"
        backup_manager.compression_manager.check.return_value = True
        strategy_mock.reset_mock()
        backup_manager.check(strategy_mock)
        # Expect a success from the method
        strategy_mock.result.assert_any_call("TestServer", True)
        # Test failure retrieving a compressor
        backup_manager.config.compression = "test_compression"
        backup_manager.compression_manager.check.return_value = True
        backup_manager.compression_manager.get_default_compressor.side_effect = (
            CompressionIncompatibility()
        )
        strategy_mock.reset_mock()
        backup_manager.check(strategy_mock)
        # Expect a failure from the method
        strategy_mock.result.assert_any_call("TestServer", False)

    def test_get_latest_archived_wals_info(self, tmpdir):
        """
        Test the get_latest_archived_wals_info method
        """
        # build a backup_manager and setup a basic configuration
        backup_manager = build_backup_manager(
            main_conf={
                "backup_directory": tmpdir.strpath,
            }
        )

        # Test: insistent wals directory
        assert backup_manager.get_latest_archived_wals_info() == dict()

        # Test: empty wals directory
        wals = tmpdir.join("wals").ensure(dir=True)
        assert backup_manager.get_latest_archived_wals_info() == dict()

        # Test: ignore WAL-like files in the root
        wals.join("000000010000000000000003").ensure()
        assert backup_manager.get_latest_archived_wals_info() == dict()

        # Test: find the fist WAL
        wals.join("0000000100000000").join("000000010000000000000001").ensure()
        latest = backup_manager.get_latest_archived_wals_info()
        assert latest
        assert len(latest) == 1
        assert latest["00000001"].name == "000000010000000000000001"

        # Test: find the 2nd WAL in the same dir
        wals.join("0000000100000000").join("000000010000000000000002").ensure()
        latest = backup_manager.get_latest_archived_wals_info()
        assert latest
        assert len(latest) == 1
        assert latest["00000001"].name == "000000010000000000000002"

        # Test: the newer dir is empty
        wals.join("0000000100000001").ensure(dir=True)
        latest = backup_manager.get_latest_archived_wals_info()
        assert latest
        assert len(latest) == 1
        assert latest["00000001"].name == "000000010000000000000002"

        # Test: the newer contains a newer file
        wals.join("0000000100000001").join("000000010000000100000001").ensure()
        latest = backup_manager.get_latest_archived_wals_info()
        assert latest
        assert len(latest) == 1
        assert latest["00000001"].name == "000000010000000100000001"

        # Test: ignore out of order files
        wals.join("0000000100000000").join("000000010000000100000005").ensure()
        latest = backup_manager.get_latest_archived_wals_info()
        assert latest
        assert len(latest) == 1
        assert latest["00000001"].name == "000000010000000100000001"

        # Test: find the 2nd timeline
        wals.join("0000000200000000").join("000000020000000000000003").ensure()
        latest = backup_manager.get_latest_archived_wals_info()
        assert latest
        assert len(latest) == 2
        assert latest["00000001"].name == "000000010000000100000001"
        assert latest["00000002"].name == "000000020000000000000003"

    def test_backup_manager_has_keep_manager_capability(self, tmpdir):
        """
        Verifies that KeepManagerMixin methods are available in BackupManager
        and that they work as expected.

        We deliberately do not test the functionality at a more granular level as
        KeepManagerMixin has its own tests and BackupManager adds no extra
        functionality.
        """
        test_backup_id = "20210723T095432"
        backup_manager = build_backup_manager(
            name="test_server", global_conf={"barman_home": tmpdir.strpath}
        )
        # Initially a backup has no annotations and therefore shouldn't be kept
        assert backup_manager.should_keep_backup(test_backup_id) is False
        # The target is None because there is no keep annotation
        assert backup_manager.get_keep_target(test_backup_id) is None
        # Releasing the keep is a no-op because there is no keep
        backup_manager.release_keep(test_backup_id)
        # We can add a new keep
        backup_manager.keep_backup(test_backup_id, KeepManager.TARGET_STANDALONE)
        # Now we have added a keep, the backup manager knows the backup should be kept
        assert backup_manager.should_keep_backup(test_backup_id) is True
        # We can also see the recovery target
        assert (
            backup_manager.get_keep_target(test_backup_id)
            == KeepManager.TARGET_STANDALONE
        )
        # We can release the keep
        backup_manager.release_keep(test_backup_id)
        # Having released the keep, the backup manager tells us it shouldn't be kept
        assert backup_manager.should_keep_backup(test_backup_id) is False
        # And the recovery target is None again
        assert backup_manager.get_keep_target(test_backup_id) is None

    @patch("barman.backup.BackupManager.delete_backup")
    @patch("barman.backup.BackupManager.get_available_backups")
    def test_cron_retention_only_deletes_OBSOLETE_backups(
        self, get_available_backups, delete_backup, tmpdir
    ):
        """
        Verify only backups with retention status OBSOLETE are deleted by
        retention policy.
        """
        backup_manager = build_backup_manager()
        backup_manager.server.config.name = "TestServer"
        backup_manager.server.config.barman_lock_directory = tmpdir.strpath
        backup_manager.server.config.backup_options = []
        backup_manager.server.config.retention_policy = Mock()
        backup_manager.config.retention_policy.report.return_value = {
            "keep_full_backup": BackupInfo.KEEP_FULL,
            "keep_standalone_backup": BackupInfo.KEEP_STANDALONE,
            "valid_backup": BackupInfo.VALID,
            "none_backup": BackupInfo.NONE,
            "obsolete_backup": BackupInfo.OBSOLETE,
            "potentially_obsolete_backup": BackupInfo.POTENTIALLY_OBSOLETE,
        }
        available_backups = dict(
            (k, build_test_backup_info(server=backup_manager.server, backup_id=k))
            for k in backup_manager.config.retention_policy.report.return_value
        )
        get_available_backups.return_value = available_backups
        backup_manager.cron_retention_policy()
        delete_backup.assert_called_once_with(
            available_backups["obsolete_backup"], skip_wal_cleanup_if_standalone=False
        )

    @patch("barman.backup.BackupManager.delete_backup")
    @patch("barman.backup.BackupManager.get_available_backups")
    def test_cron_retention_skip_OBSOLETE_backups_if_lock(
        self, get_available_backups, delete_backup, tmpdir, capsys
    ):
        """
        Verify only backups with retention status OBSOLETE is not deleted if
        the ServerBackupIdLock is in place.
        """
        backup_manager = build_backup_manager()
        backup_manager.server.config.name = "TestServer"
        backup_manager.server.config.barman_lock_directory = tmpdir.strpath
        backup_manager.server.config.backup_options = []
        backup_manager.server.config.retention_policy = Mock()
        backup_manager.config.retention_policy.report.return_value = {
            "keep_full_backup": BackupInfo.KEEP_FULL,
            "keep_standalone_backup": BackupInfo.KEEP_STANDALONE,
            "valid_backup": BackupInfo.VALID,
            "none_backup": BackupInfo.NONE,
            "obsolete_backup": BackupInfo.OBSOLETE,
            "potentially_obsolete_backup": BackupInfo.POTENTIALLY_OBSOLETE,
        }
        available_backups = dict(
            (k, build_test_backup_info(server=backup_manager.server, backup_id=k))
            for k in backup_manager.config.retention_policy.report.return_value
        )
        get_available_backups.return_value = available_backups
        lock = ServerBackupIdLock(
            backup_manager.config.barman_lock_directory,
            backup_manager.config.name,
            "obsolete_backup",
        )
        lock.acquire()
        backup_manager.cron_retention_policy()
        lock.release()
        out, err = capsys.readouterr()
        assert not delete_backup.called
        assert "skipping retention policy application" in err

    @pytest.mark.parametrize("should_fail", (True, False))
    @patch("barman.backup.LocalBackupInfo.save")
    @patch("barman.backup.output")
    def test_backup_with_name(self, _mock_output, _mock_backup_info_save, should_fail):
        """Verify that backup name is written to backup info during the backup."""
        # GIVEN a backup manager
        backup_manager = build_backup_manager()
        backup_manager.executor.backup = Mock()
        backup_manager.executor.copy_start_time = datetime.now()

        # AND a backup executor which will either succeed or fail
        if should_fail:
            backup_manager.executor.backup.side_effect = Exception("failed!")

        # WHEN a backup is taken with a given name
        backup_name = "arire tbaan tvir lbh hc"
        backup_info = backup_manager.backup(name=backup_name)

        # THEN the backup name is set on the backup_info
        assert backup_info.backup_name == backup_name

    @pytest.mark.parametrize("should_fail", (True, False))
    @patch("barman.backup.LocalBackupInfo.save")
    @patch("barman.backup.output")
    def test_backup_without_name(
        self, _mock_output, _mock_backup_info_save, should_fail
    ):
        """Verify that backup name is not written to backup info if name not used."""
        # GIVEN a backup manager
        backup_manager = build_backup_manager()
        backup_manager.executor.backup = Mock()
        backup_manager.executor.copy_start_time = datetime.now()

        # AND a backup executor which will either succeed or fail
        if should_fail:
            backup_manager.executor.backup.side_effect = Exception("failed!")

        # WHEN a backup is taken with no name
        backup_info = backup_manager.backup()

        # THEN backup name is None in the backup_info
        assert backup_info.backup_name is None

    @patch("barman.backup.LocalBackupInfo.save")
    @patch("barman.backup.output")
    def test_backup_without_parent_backup_id(
        self,
        _mock_output,
        _mock_backup_info_save,
    ):
        """
        Verify that information about parent and children are not updated when no parent
        backup ID is specified.
        """
        # GIVEN a backup manager
        backup_manager = build_backup_manager()
        backup_manager.executor.backup = Mock()
        backup_manager.executor.copy_start_time = datetime.now()

        # WHEN a backup is taken with no parent backup ID
        backup_info = backup_manager.backup()

        # THEN parent backup ID is None in the backup_info
        assert backup_info.parent_backup_id is None

    @patch("barman.backup.LocalBackupInfo.save")
    @patch("barman.backup.output")
    def test_backup_with_parent_backup_id(
        self,
        _mock_output,
        _mock_backup_info_save,
    ):
        """
        Verify that information about parent and children are updated when a parent
        backup ID is specified.
        """
        # GIVEN a backup manager
        backup_manager = build_backup_manager()
        backup_manager.executor.backup = Mock()
        backup_manager.executor.copy_start_time = datetime.now()

        # WHEN a backup is taken with a parent backup ID which contains no children
        with patch("barman.infofile.LocalBackupInfo.get_parent_backup_info") as mock:
            mock.return_value.children_backup_ids = None
            backup_info = backup_manager.backup(
                parent_backup_id="SOME_PARENT_BACKUP_ID",
            )

        # THEN parent backup ID is filled in the backup_info
        assert backup_info.parent_backup_id == "SOME_PARENT_BACKUP_ID"

        # AND children backup IDs is set in the parent backup_info
        assert mock.return_value.children_backup_ids == [backup_info.backup_id]

        # WHEN a backup is taken with a parent backup ID which contains a child
        with patch("barman.infofile.LocalBackupInfo.get_parent_backup_info") as mock:
            mock.return_value.children_backup_ids = ["SOME_CHILD_BACKUP_ID"]
            backup_info = backup_manager.backup(
                parent_backup_id="SOME_PARENT_BACKUP_ID",
            )

        # THEN parent backup ID is filled in the backup_info
        assert backup_info.parent_backup_id == "SOME_PARENT_BACKUP_ID"

        # AND children backup IDs is set in the parent backup_info
        assert mock.return_value.children_backup_ids == [
            "SOME_CHILD_BACKUP_ID",
            backup_info.backup_id,
        ]

    @patch("barman.backup.BackupManager._validate_incremental_backup_configs")
    def test_validate_backup_args(self, mock_validate_incremental):
        """
        Test the validate_backup_args method, ensuring that validations are passed
        correctly to all responsible methods according to the parameters received.
        """
        backup_manager = build_backup_manager(global_conf={"backup_method": "postgres"})

        # incremental backup validation is skipped when no parent backup is present
        incremental_kwargs = {}
        backup_manager.validate_backup_args(**incremental_kwargs)
        mock_validate_incremental.assert_not_called()

        # incremental backup validation is called when a parent backup is present
        mock_validate_incremental.reset_mock()
        incremental_kwargs = {"parent_backup_id": Mock()}
        backup_manager.validate_backup_args(**incremental_kwargs)
        mock_validate_incremental.assert_called_once()

    def test_validate_incremental_backup_configs_pg_version(self):
        """
        Test Postgres incremental backup validations for Postgres
        server version
        """
        backup_manager = build_backup_manager(global_conf={"backup_method": "postgres"})

        # mock the postgres object to set server version
        mock_postgres = Mock()
        backup_manager.executor.server.postgres = mock_postgres

        # mock enabled summarize_wal option
        backup_manager.executor.server.postgres.get_setting.side_effect = ["on"]

        # ensure no exception is raised when Postgres version >= 17
        mock_postgres.configure_mock(server_version=180500)
        backup_manager._validate_incremental_backup_configs()

        # ensure BackupException is raised when Postgres version is < 17
        mock_postgres.configure_mock(server_version=160000)
        with pytest.raises(BackupException):
            backup_manager._validate_incremental_backup_configs()

    def test_validate_incremental_backup_configs_summarize_wal(self):
        """
        Test that summarize_wal is enabled on Postgres incremental backup
        """
        backup_manager = build_backup_manager(global_conf={"backup_method": "postgres"})

        # mock the postgres object to set server version
        mock_postgres = Mock()
        backup_manager.executor.server.postgres = mock_postgres
        mock_postgres.configure_mock(server_version=170000)

        # change the behavior of get_setting("summarize_wal") function call
        backup_manager.executor.server.postgres.get_setting.side_effect = [
            None,
            "off",
            "on",
        ]

        err_msg = "'summarize_wal' option has to be enabled in the Postgres server "
        "to perform an incremental backup using the Postgres backup method"

        # ensure incremental backup with summarize_wal disabled raises exception
        with pytest.raises(BackupException, match=err_msg):
            backup_manager._validate_incremental_backup_configs()
        with pytest.raises(BackupException, match=err_msg):
            backup_manager._validate_incremental_backup_configs()
        # no exception is raised when summarize_wal is on
        backup_manager._validate_incremental_backup_configs()

    @patch("barman.backup.BackupManager.get_available_backups")
    def test_get_last_full_backup_id(self, get_available_backups):
        """
        Test that the function returns the correct last full backup
        """
        backup_manager = build_backup_manager(global_conf={"backup_method": "postgres"})

        available_backups = {
            "20241010T120000": "20241009T131000",
            "20241010T131000": None,
            "20241010T140202": "20241010T131000",
            "20241010T150000": "20241010T140202",
            "20241010T160000": None,
            "20241010T180000": "20241010T160000",
            "20241011T180000": None,
            "20241012T180000": "20241011T180000",
            "20241013T180000": "20241012T180000",
        }

        backups = dict(
            (
                bkp_id,
                build_test_backup_info(
                    server=backup_manager.server,
                    backup_id=bkp_id,
                    parent_backup_id=par_bkp_id,
                    summarize_wal="on",
                ),
            )
            for bkp_id, par_bkp_id in available_backups.items()
        )
        get_available_backups.return_value = backups

        last_full_backup = backup_manager.get_last_full_backup_id()
        get_available_backups.assert_called_once()
        assert last_full_backup == "20241011T180000"


class TestWalCleanup(object):
    """Test cleanup of WALs by BackupManager"""

    @pytest.fixture
    def backup_manager(self, tmpdir):
        """
        Creates a BackupManager backed by the filesystem with empty base backup
        and WAL directories and an empty xlog.db.
        """
        backup_manager = build_backup_manager(
            global_conf={"barman_home": tmpdir.strpath}
        )
        backup_manager.server.config.name = "TestServer"
        backup_manager.server.config.barman_lock_directory = tmpdir.strpath
        backup_manager.server.config.backup_options = [BackupOptions.CONCURRENT_BACKUP]
        base_dir = tmpdir.mkdir("base")
        wal_dir = tmpdir.mkdir("wals")
        backup_manager.server.config.basebackups_directory = base_dir.strpath
        backup_manager.server.config.wals_directory = wal_dir.strpath
        backup_manager.server.config.minimum_redundancy = 1
        self.xlog_db = wal_dir.join("xlog.db")
        self.xlog_db.write("")

        def open_xlog_db():
            return open(self.xlog_db.strpath, "r+")

        # This must be a side-effect so we open xlog_db each time it is called
        backup_manager.server.xlogdb.return_value.__enter__.side_effect = open_xlog_db

        # Wire get_available_backups in our mock server to call
        # backup_manager.get_available_backups, just like a non-mock server
        backup_manager.server.get_available_backups = (
            backup_manager.get_available_backups
        )
        yield backup_manager

    def _assert_wals_exist(self, wals_directory, begin_wal, end_wal):
        """
        Assert all WALs between begin_wal and end_wal (inclusive) exist in
        wals_directory.
        """
        for wal in interpolate_wals(begin_wal, end_wal):
            assert os.path.isfile("%s/%s/%s" % (wals_directory, wal[:16], wal))

    def _assert_wals_missing(self, wals_directory, begin_wal, end_wal):
        """
        Assert all WALs between begin_wal and end_wal (inclusive) do not
        exist in wals_directory.
        """
        for wal in interpolate_wals(begin_wal, end_wal):
            assert not os.path.isfile("%s/%s/%s" % (wals_directory, wal[:16], wal))

    def _create_wal_on_filesystem(self, wals_directory, wal):
        """
        Helper which creates the specified WAL on the filesystem and adds it to
        xlogdb.
        """
        wal_path = "%s/%s" % (wals_directory, wal[:16])
        try:
            os.mkdir(wal_path)
        except EnvironmentError as e:
            # For Python 2 compatibility we must check the error code directly
            # If the directory already exists then it is not an error condition
            if e.errno != errno.EEXIST:
                raise
        with open("%s/%s" % (wal_path, wal), "a"):
            # An empty file is fine for the purposes of these tests
            pass
        self.xlog_db.write("%s\t42\t43\tNone\n" % wal, mode="a")

    def _create_wals_on_filesystem(self, wals_directory, begin_wal, end_wal):
        """
        Helper which creates all WALs between begin_wal and end_wal (inclusive)
        on the filesystem.
        """
        for wal in interpolate_wals(begin_wal, end_wal):
            self._create_wal_on_filesystem(wals_directory, wal)

    def _create_backup_on_filesystem(self, backup_info):
        """Helper which creates the backup on the filesystem"""
        backup_path = "%s/%s" % (
            backup_info.server.config.basebackups_directory,
            backup_info.backup_id,
        )
        os.mkdir(backup_path)
        backup_info.save("%s/backup.info" % backup_path)

    def test_delete_no_wal_cleanup_if_not_oldest_backup(self, backup_manager):
        """Verify no WALs are removed when the deleted backup is not the oldest"""
        # GIVEN two backups
        oldest_backup = build_test_backup_info(
            backup_id="20210722T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000073",
            end_wal="000000010000000000000076",
        )
        backup = build_test_backup_info(
            backup_id="20210723T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000078",
            end_wal="00000001000000000000007A",
        )
        for backup_info in [oldest_backup, backup]:
            self._create_backup_on_filesystem(backup_info)

        # AND WALs which range from just before the oldest backup to the end_wal
        # of the newest backup
        wals_directory = backup_manager.server.config.wals_directory
        self._create_wals_on_filesystem(
            wals_directory, "00000001000000000000006C", "00000001000000000000007A"
        )

        # WHEN the newest backup is deleted
        backup_manager.delete_backup(backup)

        # THEN no WALs were deleted
        self._assert_wals_exist(
            wals_directory, "00000001000000000000006C", "00000001000000000000007A"
        )

    def test_delete_wal_cleanup(self, backup_manager):
        """Verify correct WALs are removed when the oldest backup is deleted"""
        # GIVEN two backups
        oldest_backup = build_test_backup_info(
            backup_id="20210722T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000073",
            end_wal="000000010000000000000076",
        )
        backup = build_test_backup_info(
            backup_id="20210723T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000078",
            end_wal="00000001000000000000007A",
        )
        for backup_info in [oldest_backup, backup]:
            self._create_backup_on_filesystem(backup_info)

        # AND WALs which range from just before the oldest backup to the end_wal
        # of the newest backup
        wals_directory = backup_manager.server.config.wals_directory
        self._create_wals_on_filesystem(
            wals_directory, "00000001000000000000006C", "00000001000000000000007A"
        )

        # WHEN the newest backup is deleted
        backup_manager.delete_backup(oldest_backup)

        # THEN all WALs up to begin_wal of the remaining backup were deleted
        self._assert_wals_missing(
            wals_directory, "00000001000000000000006C", "000000010000000000000077"
        )

        # AND all subsequent WALs still exist
        self._assert_wals_exist(
            wals_directory, "000000010000000000000078", "00000001000000000000007A"
        )

    def test_delete_wal_cleanup_last_backup(self, backup_manager):
        """
        Verify correct WALs are removed when the last backup is deleted.
        Because backup_manager is configured with the CONCURRENT_BACKUP BackupOption
        only WALs up to begin_wal of the last backup should be removed.
        """
        # GIVEN a single backup
        backup = build_test_backup_info(
            backup_id="20210723T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000078",
            end_wal="00000001000000000000007A",
        )
        self._create_backup_on_filesystem(backup)

        # AND WALs which range from before the backup to the end_wal of the backup
        wals_directory = backup_manager.server.config.wals_directory
        self._create_wals_on_filesystem(
            wals_directory, "00000001000000000000006C", "00000001000000000000007A"
        )

        # AND minimum_redundancy=0 so that the last backup can be removed
        backup_manager.server.config.minimum_redundancy = 0

        # WHEN the backup is deleted
        backup_manager.delete_backup(backup)

        # THEN all WALs up to the begin_wal of the deleted backup were deleted
        self._assert_wals_missing(
            wals_directory, "00000001000000000000006C", "000000010000000000000077"
        )

        # AND all subsequent WALs still exist
        self._assert_wals_exist(
            wals_directory, "000000010000000000000078", "00000001000000000000007A"
        )

    def test_delete_wal_cleanup_preserves_history_files(self, backup_manager):
        """ "Verify history files are preserved when WALs are removed"""
        # GIVEN two backups
        oldest_backup = build_test_backup_info(
            backup_id="20210722T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000073",
            end_wal="000000010000000000000076",
        )
        backup = build_test_backup_info(
            backup_id="20210723T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000078",
            end_wal="00000001000000000000007A",
        )
        for backup_info in [oldest_backup, backup]:
            self._create_backup_on_filesystem(backup_info)

        # AND a WAL history file
        wals_directory = backup_manager.server.config.wals_directory
        # Create a history file
        with open("%s/%s" % (wals_directory, "00000001.history"), "a"):
            # An empty file is fine for the purposes of these tests
            pass
        self.xlog_db.write("%s\t42\t43\tNone\n" % "00000001.history", mode="a")

        # AND WALs which range from just before the oldest backup to the end_wal
        # of the newest backup
        self._create_wals_on_filesystem(
            wals_directory, "00000001000000000000006C", "00000001000000000000007A"
        )

        # WHEN the oldest backup is deleted
        backup_manager.delete_backup(oldest_backup)

        # THEN all WALs up to begin_wal of remaining backup are gone
        self._assert_wals_missing(
            wals_directory, "00000001000000000000006C", "000000010000000000000077"
        )

        # AND all subsequent WALs still exist
        self._assert_wals_exist(
            wals_directory, "000000010000000000000078", "00000001000000000000007A"
        )

        # AND the history file still exists
        assert os.path.isfile("%s/%s" % (wals_directory, "00000001.history"))

    def test_delete_no_wal_cleanup_if_oldest_is_keep_full(self, backup_manager):
        """Verify no WALs are cleaned up if the oldest backup is keep:full"""
        # GIVEN three backups
        oldest_backup = build_test_backup_info(
            backup_id="20210722T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000073",
            end_wal="000000010000000000000076",
        )
        target_backup = build_test_backup_info(
            backup_id="20210723T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000078",
            end_wal="00000001000000000000007A",
        )
        backup = build_test_backup_info(
            backup_id="20210724T095432",
            server=backup_manager.server,
            begin_wal="00000001000000000000007C",
            end_wal="00000001000000000000007E",
        )
        for backup_info in [oldest_backup, target_backup, backup]:
            self._create_backup_on_filesystem(backup_info)

        # AND WALs which range from just before the oldest backup to the end_wal
        # of the newest backup
        wals_directory = backup_manager.server.config.wals_directory
        self._create_wals_on_filesystem(
            wals_directory, "00000001000000000000006C", "00000001000000000000007E"
        )

        # AND the oldest backup is a full archival backup (i.e. it has a
        # keep:full annotation)
        def get_keep_target(backup_id):
            return (
                backup_id == oldest_backup.backup_id and KeepManager.TARGET_FULL or None
            )

        backup_manager.get_keep_target = get_keep_target

        # WHEN the second oldest backup is deleted
        backup_manager.delete_backup(target_backup)

        # THEN no WALs were deleted at all
        self._assert_wals_exist(
            wals_directory, "00000001000000000000006C", "00000001000000000000007A"
        )

    def test_delete_no_wal_cleanup_if_oldest_remaining_is_keep_standalone(
        self, backup_manager
    ):
        """
        Verify no WAL cleanup if oldest remaining backup is keep:standalone and we are
        deleting by backup_id.
        """
        # GIVEN three backups
        oldest_backup = build_test_backup_info(
            backup_id="20210722T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000073",
            end_wal="000000010000000000000076",
        )
        target_backup = build_test_backup_info(
            backup_id="20210723T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000078",
            end_wal="00000001000000000000007A",
        )
        backup = build_test_backup_info(
            backup_id="20210724T095432",
            server=backup_manager.server,
            begin_wal="00000001000000000000007C",
            end_wal="00000001000000000000007E",
        )
        for backup_info in [oldest_backup, target_backup, backup]:
            self._create_backup_on_filesystem(backup_info)

        # AND WALs which range from just before the oldest backup to the end_wal
        # of the newest backup
        wals_directory = backup_manager.server.config.wals_directory
        self._create_wals_on_filesystem(
            wals_directory, "00000001000000000000006C", "00000001000000000000007E"
        )

        # AND the oldest backup is a standalone archival backup (i.e. it has a
        # keep:standalone annotation)
        def get_keep_target(backup_id):
            return (
                backup_id == oldest_backup.backup_id
                and KeepManager.TARGET_STANDALONE
                or None
            )

        backup_manager.get_keep_target = get_keep_target

        # WHEN the second oldest backup is deleted
        backup_manager.delete_backup(target_backup)

        # THEN no WALs were deleted at all
        self._assert_wals_exist(
            wals_directory, "00000001000000000000006C", "00000001000000000000007E"
        )

    def test_delete_by_retention_wal_cleanup_if_oldest_is_keep_standalone(
        self, backup_manager
    ):
        """
        Verify >=oldest.begin_wal and <=oldest.end_wal are preserved when the
        oldest backup is archival with keep:standalone and we are deleting by
        retention policy.
        """
        # GIVEN a server with a retention policy of REDUNDANCY 1
        backup_manager.server.config.retention_policy = RetentionPolicyFactory.create(
            "retention_policy",
            "REDUNDANCY 1",
            server=backup_manager.server,
        )

        # AND three backups
        oldest_backup = build_test_backup_info(
            backup_id="20210722T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000073",
            end_wal="000000010000000000000076",
        )
        target_backup = build_test_backup_info(
            backup_id="20210723T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000078",
            end_wal="00000001000000000000007A",
        )
        backup = build_test_backup_info(
            backup_id="20210724T095432",
            server=backup_manager.server,
            begin_wal="00000001000000000000007C",
            end_wal="00000001000000000000007E",
        )
        for backup_info in [oldest_backup, target_backup, backup]:
            self._create_backup_on_filesystem(backup_info)

        # AND WALs which range from just before the oldest backup to the end_wal
        # of the newest backup
        wals_directory = backup_manager.server.config.wals_directory
        self._create_wals_on_filesystem(
            wals_directory, "00000001000000000000006C", "00000001000000000000007E"
        )

        # AND the oldest backup is a standalone archival backup (i.e. it has a
        # keep:standalone annotation)
        def get_keep_target(backup_id):
            return (
                backup_id == oldest_backup.backup_id
                and KeepManager.TARGET_STANDALONE
                or None
            )

        backup_manager.get_keep_target = get_keep_target

        # WHEN the retention policy is enforced
        backup_manager.cron_retention_policy()

        # THEN all WALs before the oldest backup were deleted
        self._assert_wals_missing(
            wals_directory, "00000001000000000000006C", "000000010000000000000072"
        )
        # AND all WALs from begin_wal to end_wal (inclusive) of the oldest backup
        # still exist
        self._assert_wals_exist(
            wals_directory, "000000010000000000000073", "000000010000000000000076"
        )
        # AND all WALs after end_wal of the oldest backup to before begin_wal of the
        # newest backup were deleted
        self._assert_wals_missing(
            wals_directory, "000000010000000000000077", "00000001000000000000007B"
        )
        # AND all subsequent WALs still exist
        self._assert_wals_exist(
            wals_directory, "00000001000000000000007C", "00000001000000000000007E"
        )

    def test_delete_by_retention_wal_cleanup_if_all_oldest_are_keep_standalone(
        self, backup_manager
    ):
        """
        Verify all >=begin_wal and <= end_wal are preserved for all standalone
        backups when all backups up to oldest are standalone and we are deleting
        by retention policy.
        """
        # GIVEN a server with a retention policy of REDUNDANCY 1
        backup_manager.server.config.retention_policy = RetentionPolicyFactory.create(
            "retention_policy",
            "REDUNDANCY 1",
            server=backup_manager.server,
        )
        # AND four backups
        oldest_backup = build_test_backup_info(
            backup_id="20210721T095432",
            server=backup_manager.server,
            begin_wal="00000001000000000000006E",
            end_wal="000000010000000000000071",
        )
        second_oldest_backup = build_test_backup_info(
            backup_id="20210722T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000073",
            end_wal="000000010000000000000076",
        )
        target_backup = build_test_backup_info(
            backup_id="20210723T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000078",
            end_wal="00000001000000000000007A",
        )
        backup = build_test_backup_info(
            backup_id="20210724T095432",
            server=backup_manager.server,
            begin_wal="00000001000000000000007C",
            end_wal="00000001000000000000007E",
        )
        for backup_info in [oldest_backup, second_oldest_backup, target_backup, backup]:
            self._create_backup_on_filesystem(backup_info)

        # AND WALs which range from just before the oldest backup to the end_wal
        # of the newest backup
        wals_directory = backup_manager.server.config.wals_directory
        self._create_wals_on_filesystem(
            wals_directory, "00000001000000000000006C", "00000001000000000000007E"
        )

        # AND the oldest two backups are standalone archival backups (i.e. they have
        # keep:standalone annotations)
        def get_keep_target(backup_id):
            return (
                (
                    backup_id == oldest_backup.backup_id
                    or backup_id == second_oldest_backup.backup_id
                )
                and KeepManager.TARGET_STANDALONE
                or None
            )

        backup_manager.get_keep_target = get_keep_target

        # WHEN the retention policy is enforced
        backup_manager.cron_retention_policy()

        # THEN all WALs before the oldest backup were deleted
        self._assert_wals_missing(
            wals_directory, "00000001000000000000006C", "00000001000000000000006D"
        )
        # AND all WALs from begin_wal to end_wal (inclusive) of the oldest backup
        # still exist
        self._assert_wals_exist(
            wals_directory, "00000001000000000000006E", "000000010000000000000071"
        )
        # AND all WALs from after end_wal of the oldest backup to before begin_wal of
        # the second oldest backup were deleted
        self._assert_wals_missing(
            wals_directory, "000000010000000000000072", "000000010000000000000072"
        )
        # AND all WALs from begin_wal to end_wal (inclusive) of the second oldest
        # backup still exist
        self._assert_wals_exist(
            wals_directory, "000000010000000000000073", "000000010000000000000076"
        )
        # AND all WALs from after end_wal of the second oldest backup to before
        # begin_wal of the newest backup were deleted
        self._assert_wals_missing(
            wals_directory, "000000010000000000000077", "00000001000000000000007B"
        )
        # AND all subsequent WALs still exist
        self._assert_wals_exist(
            wals_directory, "00000001000000000000007C", "00000001000000000000007E"
        )

    def test_delete_wal_cleanup_if_oldest_two_nokeep_and_standalone(
        self, backup_manager
    ):
        """
        Verify WALs are cleaned up if the oldest backup has no keep and the
        second oldest is keep:standalone.
        """
        # GIVEN a server with a retention policy of REDUNDANCY 1
        backup_manager.server.config.retention_policy = RetentionPolicyFactory.create(
            "retention_policy",
            "REDUNDANCY 1",
            server=backup_manager.server,
        )
        # AND four backups
        oldest_backup = build_test_backup_info(
            backup_id="20210721T095432",
            server=backup_manager.server,
            begin_wal="00000001000000000000006E",
            end_wal="000000010000000000000071",
        )
        second_oldest_backup = build_test_backup_info(
            backup_id="20210722T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000073",
            end_wal="000000010000000000000076",
        )
        target_backup = build_test_backup_info(
            backup_id="20210723T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000078",
            end_wal="00000001000000000000007A",
        )
        backup = build_test_backup_info(
            backup_id="20210724T095432",
            server=backup_manager.server,
            begin_wal="00000001000000000000007C",
            end_wal="00000001000000000000007E",
        )
        for backup_info in [oldest_backup, second_oldest_backup, target_backup, backup]:
            self._create_backup_on_filesystem(backup_info)

        # AND WALs which range from just before the oldest backup to the end_wal
        # of the newest backup
        wals_directory = backup_manager.server.config.wals_directory
        self._create_wals_on_filesystem(
            wals_directory, "00000001000000000000006C", "00000001000000000000007E"
        )

        # AND the second oldest backup is a standalone archive backup (i.e. it has
        # a the keep:standalone annotation)
        def get_keep_target(backup_id):
            return (
                backup_id == second_oldest_backup.backup_id
                and KeepManager.TARGET_STANDALONE
                or None
            )

        backup_manager.get_keep_target = get_keep_target

        # WHEN the retention policy is enforced
        backup_manager.cron_retention_policy()

        # THEN all WALs before the standalone backup were deleted
        self._assert_wals_missing(
            wals_directory, "00000001000000000000006C", "000000010000000000000072"
        )
        # AND all WALs from begin_wal to end_wal (inclusive) of the standalone backup
        # still exist
        self._assert_wals_exist(
            wals_directory, "000000010000000000000073", "000000010000000000000076"
        )
        # AND all WALs from after end_wal of the standalone backup to before
        # begin_wal of the newest backup were deleted
        self._assert_wals_missing(
            wals_directory, "000000010000000000000077", "00000001000000000000007B"
        )
        # AND all subsequent WALs still exist
        self._assert_wals_exist(
            wals_directory, "00000001000000000000007C", "00000001000000000000007E"
        )

    def test_delete_no_wal_cleanup_if_oldest_two_full_and_standalone(
        self, backup_manager
    ):
        """
        Verify no WALs are cleaned up if the oldest backup has keep:full and the
        second oldest is keep:standalone.
        """
        # GIVEN a server with a retention policy of REDUNDANCY 1
        backup_manager.server.config.retention_policy = RetentionPolicyFactory.create(
            "retention_policy",
            "REDUNDANCY 1",
            server=backup_manager.server,
        )
        # AND four backups
        oldest_backup = build_test_backup_info(
            backup_id="20210721T095432",
            server=backup_manager.server,
            begin_wal="00000001000000000000006E",
            end_wal="000000010000000000000071",
        )
        second_oldest_backup = build_test_backup_info(
            backup_id="20210722T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000073",
            end_wal="000000010000000000000076",
        )
        target_backup = build_test_backup_info(
            backup_id="20210723T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000078",
            end_wal="00000001000000000000007A",
        )
        backup = build_test_backup_info(
            backup_id="20210724T095432",
            server=backup_manager.server,
            begin_wal="00000001000000000000007C",
            end_wal="00000001000000000000007E",
        )
        for backup_info in [oldest_backup, second_oldest_backup, target_backup, backup]:
            self._create_backup_on_filesystem(backup_info)

        # AND WALs which range from just before the oldest backup to the end_wal
        # of the newest backup
        wals_directory = backup_manager.server.config.wals_directory
        self._create_wals_on_filesystem(
            wals_directory, "00000001000000000000006C", "00000001000000000000007E"
        )

        # AND the oldest backup is a full archival backup (has a keep:full
        # annotation) and the second oldest backup is a standalone archive
        # backup (i.e. it has a keep:standalone annotation)
        def get_keep_target(backup_id):
            return (
                backup_id == oldest_backup.backup_id
                and KeepManager.TARGET_FULL
                or backup_id == second_oldest_backup.backup_id
                and KeepManager.TARGET_STANDALONE
                or None
            )

        backup_manager.get_keep_target = get_keep_target

        # WHEN the retention policy is enforced
        backup_manager.cron_retention_policy()

        # THEN no WALs were deleted at all
        self._assert_wals_exist(
            wals_directory, "00000001000000000000006C", "00000001000000000000007A"
        )

    def test_delete_by_retention_wal_cleanup_preserves_backup_wal(self, backup_manager):
        """
        Verify .backup WALs are preserved for standalone archival backups.
        """
        # GIVEN a server with a retention policy of REDUNDANCY 1
        backup_manager.server.config.retention_policy = RetentionPolicyFactory.create(
            "retention_policy",
            "REDUNDANCY 1",
            server=backup_manager.server,
        )

        # AND three backups
        oldest_backup = build_test_backup_info(
            backup_id="20210722T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000073",
            end_wal="000000010000000000000076",
        )
        target_backup = build_test_backup_info(
            backup_id="20210723T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000078",
            end_wal="00000001000000000000007A",
        )
        backup = build_test_backup_info(
            backup_id="20210724T095432",
            server=backup_manager.server,
            begin_wal="00000001000000000000007C",
            end_wal="00000001000000000000007E",
        )
        for backup_info in [oldest_backup, target_backup, backup]:
            self._create_backup_on_filesystem(backup_info)

        # AND WALs which range from just before the oldest backup to the end_wal
        # of the newest backup
        wals_directory = backup_manager.server.config.wals_directory
        self._create_wals_on_filesystem(
            wals_directory, "00000001000000000000006C", "000000010000000000000076"
        )
        # AND the oldest backup has a .backup WAL
        backup_wal = "000000010000000000000076.00000028.backup"
        self._create_wal_on_filesystem(wals_directory, backup_wal)
        self._create_wals_on_filesystem(
            wals_directory, "000000010000000000000077", "00000001000000000000007E"
        )

        # AND the oldest backup is a standalone archival backup (i.e. it has a
        # keep:standalone annotation)
        def get_keep_target(backup_id):
            return (
                backup_id == oldest_backup.backup_id
                and KeepManager.TARGET_STANDALONE
                or None
            )

        backup_manager.get_keep_target = get_keep_target

        # WHEN the retention policy is enforced
        backup_manager.cron_retention_policy()

        # THEN all WALs before the oldest backup were deleted
        self._assert_wals_missing(
            wals_directory, "00000001000000000000006C", "000000010000000000000072"
        )
        # AND all WALs from begin_wal to end_wal (inclusive) of the oldest backup
        # still exist
        self._assert_wals_exist(
            wals_directory, "000000010000000000000073", "000000010000000000000076"
        )
        # AND the .backup WAL still exists
        assert os.path.isfile(
            "%s/%s/%s" % (wals_directory, backup_wal[:16], backup_wal)
        )
        # AND all WALs after end_wal of the oldest backup to before begin_wal of the
        # newest backup were deleted
        self._assert_wals_missing(
            wals_directory, "000000010000000000000077", "00000001000000000000007B"
        )
        # AND all subsequent WALs still exist
        self._assert_wals_exist(
            wals_directory, "00000001000000000000007C", "00000001000000000000007E"
        )


class TestVerifyBackup:
    """Test backupManager verify_backup function"""

    @patch("barman.backup.PgVerifyBackup")
    def test_verify_backup_nominal(self, mock_pg_verify_backup):
        backup_path = "/fake/path"
        pg_verify_backup_path = "/path/to/pg_verifybackup"
        backup_manager = build_backup_manager()
        mock_backup_info = Mock()
        mock_backup_info.get_data_directory.return_value = backup_path

        mock_pg_verify_backup.get_version_info.return_value = {
            "full_path": pg_verify_backup_path,
            "full_version": "13.2",
        }

        backup_manager.verify_backup(mock_backup_info)

        mock_backup_info.get_data_directory.assert_called_once()
        mock_pg_verify_backup_instance = mock_pg_verify_backup.return_value
        mock_pg_verify_backup.assert_called_once_with(
            data_path=backup_path, command=pg_verify_backup_path, version="13.2"
        )
        mock_pg_verify_backup.return_value.assert_called_once()
        mock_pg_verify_backup_instance.get_output.assert_called_once()

    @patch("barman.backup.PgVerifyBackup")
    def test_verify_backup_exec_not_found(self, mock_pg_verify_backup):
        backup_manager = build_backup_manager()
        mock_backup_info = Mock()
        mock_backup_info.get_data_directory.return_value = "/fake/path2"
        mock_pg_verify_backup.get_version_info.return_value = dict.fromkeys(
            ("full_path", "full_version", "major_version"), None
        )

        backup_manager.verify_backup(mock_backup_info)

        mock_backup_info.get_data_directory.assert_not_called()
        mock_pg_verify_backup.assert_not_called()

    @patch("barman.backup.PgVerifyBackup")
    def test_verify_backup_failed_cmd(self, mock_pg_verify_backup):
        backup_manager = build_backup_manager()
        mock_backup_info = Mock()
        mock_backup_info.get_data_directory.return_value = "/fake/path3"
        mock_pg_verify_backup.get_version_info.return_value = {
            "full_path": "/path/to/pg_verifybackup",
            "full_version": "13.2",
        }
        mock_pg_verify_backup_instance = mock_pg_verify_backup.return_value
        mock_pg_verify_backup_instance.side_effect = CommandFailedException(
            {"err": "Failed"}
        )

        backup_manager.verify_backup(mock_backup_info)

        mock_pg_verify_backup_instance.get_output.assert_not_called()


class TestSnapshotBackup(object):
    """Test handling of snapshot backups by BackupManager."""

    @patch("barman.backup.SnapshotBackupExecutor")
    def test_snapshot_backup_method(self, mock_snapshot_executor):
        """
        Verify that a SnapshotBackupExecutor is created for backup_method "snapshot".
        """
        # GIVEN a server with backup_method = "snapshot"
        server = build_mocked_server(
            "test_server", main_conf={"backup_method": "snapshot"}
        )
        # WHEN a BackupManager is created for that server
        manager = BackupManager(server=server)
        # THEN its executor is a SnapshotBackupExecutor
        assert manager.executor == mock_snapshot_executor.return_value

    @patch("barman.backup.os")
    @patch("barman.backup.shutil")
    @patch("barman.backup.get_snapshot_interface_from_backup_info")
    @patch("barman.backup.BackupManager.remove_wal_before_backup")
    @patch("barman.backup.BackupManager.get_available_backups")
    def test_snapshot_delete(
        self,
        mock_get_available_backups,
        mock_remove_wal_before_backup,
        mock_get_snapshot_interface,
        mock_shutil,
        mock_os,
        caplog,
    ):
        """
        Verify that the snapshots are deleted via the snapshot interface.
        """
        # GIVEN a backup manager
        backup_manager = build_backup_manager()
        backup_manager.server.config.name = "test_server"
        backup_manager.server.config.minimum_redundancy = 0
        # WITH a single snapshot backup
        backup_info = build_test_backup_info(
            backup_id="test_backup_id",
            server=backup_manager.server,
            snapshots_info=mock.Mock(snapshots=[mock.Mock(identifier="test_snapshot")]),
            tablespaces=[("tbs1", 16385, "/tbs1")],
        )
        mock_get_available_backups.return_value = {backup_info.backup_id: backup_info}

        # WHEN the backup is deleted
        delete_result = backup_manager.delete_backup(backup_info)

        # THEN the deletion is successful
        assert delete_result is True
        # AND the snapshots were deleted via the snapshot interface
        mock_get_snapshot_interface.assert_called_once_with(
            backup_info, backup_manager.server.config
        )
        mock_snapshot_interface = mock_get_snapshot_interface.return_value
        mock_snapshot_interface.delete_snapshot_backup.assert_called_once_with(
            backup_info
        )
        # AND rmtree was called twice in total
        assert mock_shutil.rmtree.call_count == 2
        # AND rmtree was called on the data directory
        assert (
            mock_shutil.rmtree.call_args_list[0][0][0]
            == backup_info.get_data_directory()
        )
        # AND rmtree was called on the base directory
        assert (
            mock_shutil.rmtree.call_args_list[1][0][0]
            == backup_info.get_basebackup_directory()
        )
