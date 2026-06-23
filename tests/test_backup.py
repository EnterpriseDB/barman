# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2026
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
import io
import json
import os
import shutil
import tarfile
from contextlib import contextmanager
from datetime import datetime, timedelta

import dateutil.parser
import dateutil.tz
import mock
import pytest
from mock import Mock, call, patch
from testing_helpers import (
    build_backup_directories,
    build_backup_manager,
    build_mocked_server,
    build_test_backup_info,
    caplog_reset,
    interpolate_wals,
)

import barman.utils
from barman.annotations import KeepManager
from barman.backup import BackupManager
from barman.config import BackupOptions
from barman.exceptions import (
    BackupException,
    CommandFailedException,
    CompressionIncompatibility,
    ExportBackupException,
    ImportBackupException,
    RecoveryInvalidTargetException,
)
from barman.infofile import BackupInfo, LocalBackupInfo, Tablespace, load_datetime_tz
from barman.lockfile import ServerBackupIdLock
from barman.retention_policies import RetentionPolicyFactory
from barman.wal_archiver import CloudWalStorageStrategy, LocalWalStorageStrategy


# noinspection PyMethodMayBeStatic
class TestBackup(object):
    @patch("barman.backup.datetime")
    @patch("barman.backup.BackupInfoFactory.build_backup_info")
    @patch("barman.backup.BackupManager.get_last_backup_id")
    def test_backup_maximum_age(
        self, backup_id_mock, build_infofile_mock, datetime_mock
    ):
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
        instance = build_infofile_mock.return_value
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
        instance = build_infofile_mock.return_value
        # set the backup end date inside the limit
        instance.end_time = now - timedelta(days=2)
        # build the expected msg
        msg = barman.utils.human_readable_timedelta(now - instance.end_time)
        r = backup_manager.validate_last_backup_maximum_age(
            backup_manager.config.last_backup_maximum_age
        )
        assert (r[0], r[1]) == (True, msg)

    @patch("barman.backup.BackupManager.get_backup")
    @patch("barman.backup.BackupManager.get_last_backup_id")
    def test_validate_last_backup_min_size(self, backup_id_mock, get_backup_mock):
        # GIVEN a backup manager with a configured minimum size requirement
        backup_manager = build_backup_manager()
        # Set minimum size to 1 MiB (1048576 bytes)
        backup_manager.config.last_backup_minimum_size = 1048576

        # WHEN no backups are available
        backup_id_mock.return_value = None
        # THEN validate_last_backup_min_size returns False with size 0
        r = backup_manager.validate_last_backup_min_size(
            backup_manager.config.last_backup_minimum_size
        )
        assert r == (False, 0)

        # WHEN a backup exists but is smaller than the minimum size
        backup_id_mock.return_value = "Mock_backup"
        mock_backup = Mock()
        mock_backup.size = 524288  # 512 KiB (smaller than 1 MiB)
        get_backup_mock.return_value = mock_backup
        # THEN validate_last_backup_min_size returns False with the actual size
        r = backup_manager.validate_last_backup_min_size(
            backup_manager.config.last_backup_minimum_size
        )
        assert r == (False, 524288)

        # WHEN a backup exists and meets the minimum size requirement
        backup_id_mock.return_value = "Mock_backup"
        mock_backup.size = 2097152  # 2 MiB (larger than 1 MiB)
        get_backup_mock.return_value = mock_backup
        # THEN validate_last_backup_min_size returns True with the actual size
        r = backup_manager.validate_last_backup_min_size(
            backup_manager.config.last_backup_minimum_size
        )
        assert r == (True, 2097152)

        # WHEN a backup exists and exactly matches the minimum size requirement
        backup_id_mock.return_value = "Mock_backup"
        mock_backup.size = 1048576  # Exactly 1 MiB
        get_backup_mock.return_value = mock_backup
        # THEN validate_last_backup_min_size returns True with the actual size
        r = backup_manager.validate_last_backup_min_size(
            backup_manager.config.last_backup_minimum_size
        )
        assert r == (True, 1048576)

    @patch("barman.backup.BackupInfoFactory.build_backup_info")
    def test_keyboard_interrupt(self, mock_build_infofile):
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
        instance = mock_build_infofile.return_value
        # mocks the keep-alive query
        backup_manager.server.postgres.send_heartbeat_query.return_value = True, None
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

    @patch("barman.backup.BackupManager.release_delete_annotation")
    @patch("barman.backup.BackupManager.put_delete_annotation")
    @patch("barman.backup.BackupManager.get_available_backups")
    def test_delete_backup(
        self,
        mock_available_backups,
        mock_put_annotation,
        mock_delete_annotation,
        tmpdir,
        caplog,
    ):
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
        backup_manager.server.meta_directory = "%s/meta" % backup_dir
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

        # Mock the put_annotation method to simulate successful annotation
        mock_put_annotation.return_value = None

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

        # Test 7: ensure a child backup has its referenced removed from
        # the parent when removed successfully
        parent_backup = build_test_backup_info(
            backup_id="parent_backup_id",
            server=backup_manager.server,
        )
        build_backup_directories(parent_backup)
        child_backup = build_test_backup_info(
            backup_id="child_backup_id",
            server=backup_manager.server,
            parent_backup_id=parent_backup.backup_id,
        )
        build_backup_directories(child_backup)
        parent_backup.set_attribute(
            "children_backup_ids", [child_backup.backup_id, "another_backup_id"]
        )
        mock_available_backups.return_value = {
            parent_backup.backup_id: parent_backup,
            child_backup.backup_id: child_backup,
        }
        with patch("barman.infofile.LocalBackupInfo.get_parent_backup_info") as mock:
            mock.return_value = parent_backup
            deleted = backup_manager.delete_backup(child_backup)

        assert deleted is True
        assert child_backup.backup_id not in parent_backup.children_backup_ids

        # Test 8: Update next rsync backup information
        given_backup = build_test_backup_info(
            backup_id="rsync_backup_id",
            server=backup_manager.server,
        )
        build_backup_directories(given_backup)
        next_backup = build_test_backup_info(
            backup_id="next_rsync_backup_id",
            server=backup_manager.server,
        )
        build_backup_directories(next_backup)
        mock_available_backups.return_value = {
            given_backup.backup_id: given_backup,
            next_backup.backup_id: next_backup,
        }
        with patch("barman.backup.BackupManager.get_next_backup") as get_next_backup:
            with patch(
                "barman.backup.BackupManager._set_backup_sizes"
            ) as set_backup_sizes:
                get_next_backup.return_value = next_backup
                deleted = backup_manager.delete_backup(given_backup)
                assert deleted is True
                set_backup_sizes.assert_called_once_with(next_backup)

        # Test 9: ensure the delete annotation is created and removed during the deletion
        caplog_reset(caplog)
        mock_put_annotation.reset_mock()
        mock_delete_annotation.reset_mock()
        build_backup_directories(b_info)
        mock_available_backups.return_value = {
            "fake_backup_id": b_info,
        }
        backup_manager.delete_backup(b_info)
        # Ensure the annotation was created
        mock_put_annotation.assert_called_once_with(b_info.backup_id)
        # Ensure the annotation was deleted
        mock_delete_annotation.assert_called_once_with(b_info.backup_id)

    @patch("barman.backup.os.unlink")
    @patch("barman.backup.os.path.exists", return_value=True)
    def test_delete_cloud_backup_data(self, _, mock_unlink):
        """
        Test deletion of backup data from cloud storage.
        """
        # GIVEN a BackupManager with a mocked backup info and cloud interface
        backup_manager = build_backup_manager(name="my-server")
        manifest_path = os.path.join(
            backup_manager.server.meta_directory, "test_backup_id-backup_manifest"
        )
        backup_info = Mock(
            backup_id="test_backup_id",
            get_backup_manifest_path=lambda: manifest_path,
            get_basebackup_directory=lambda: "my-backups/my-server/base/test_backup_id",
        )
        cloud_interface_mock = Mock(
            path="my-backups",
            list_bucket=Mock(
                return_value=[
                    "my-backups/my-server/base/%s/data.tar" % backup_info.backup_id,
                    "my-backups/my-server/base/%s/tbs1.tar" % backup_info.backup_id,
                    "my-backups/my-server/base/%s/backup.info" % backup_info.backup_id,
                ]
            ),
        )
        backup_manager.server.get_backup_cloud_interface = lambda: cloud_interface_mock
        # WHEN _delete_cloud_backup_data is called
        backup_manager._delete_cloud_backup_data(backup_info)
        # THEN the listed objects from the bucket are deleted from cloud storage
        cloud_interface_mock.list_bucket.assert_called_once_with(
            "my-backups/my-server/base/%s/" % backup_info.backup_id
        )
        cloud_interface_mock.delete_objects.assert_called_once_with(
            [
                "my-backups/my-server/base/%s/data.tar" % backup_info.backup_id,
                "my-backups/my-server/base/%s/tbs1.tar" % backup_info.backup_id,
                "my-backups/my-server/base/%s/backup.info" % backup_info.backup_id,
            ],
            check_locks=False,
        )
        # AND the backup manifest file is deleted from the local meta directory
        mock_unlink.assert_called_once_with(manifest_path)

    @pytest.mark.parametrize("aws_check_object_lock", [True, False])
    @patch("barman.backup.os.unlink")
    @patch("barman.backup.os.path.exists", return_value=True)
    def test_delete_cloud_backup_data_check_object_lock(
        self, _, mock_unlink, aws_check_object_lock
    ):
        """
        Test that aws_check_object_lock is passed correctly to delete_objects.

        References: BAR-1113.
        """
        # GIVEN a BackupManager with aws_check_object_lock configured
        backup_manager = build_backup_manager(name="my-server")
        backup_manager.config.aws_check_object_lock = aws_check_object_lock
        manifest_path = os.path.join(
            backup_manager.server.meta_directory, "test_backup_id-backup_manifest"
        )
        backup_info = Mock(
            backup_id="test_backup_id",
            get_backup_manifest_path=lambda: manifest_path,
            get_basebackup_directory=lambda: "my-backups/my-server/base/test_backup_id",
        )
        cloud_interface_mock = Mock(
            path="my-backups",
            list_bucket=Mock(
                return_value=["my-backups/my-server/base/test_backup_id/data.tar"]
            ),
        )
        backup_manager.server.get_backup_cloud_interface = lambda: cloud_interface_mock
        # WHEN _delete_cloud_backup_data is called
        backup_manager._delete_cloud_backup_data(backup_info)
        # THEN delete_objects is called with the correct check_locks value
        cloud_interface_mock.delete_objects.assert_called_once_with(
            ["my-backups/my-server/base/test_backup_id/data.tar"],
            check_locks=aws_check_object_lock,
        )

    @patch("barman.backup.os.unlink")
    @patch("barman.backup.os.path.exists", return_value=False)
    def test_delete_cloud_backup_data_check_object_lock_non_s3_warning(
        self, _, mock_unlink
    ):
        """
        Test that a warning is logged when aws_check_object_lock is enabled
        but the cloud provider is not S3.

        References: BAR-1113.
        """
        # GIVEN a BackupManager with aws_check_object_lock=True
        backup_manager = build_backup_manager(name="my-server")
        backup_manager.config.aws_check_object_lock = True
        backup_info = Mock(
            backup_id="test_backup_id",
            get_backup_manifest_path=lambda: "/some/path",
            get_basebackup_directory=lambda: "my-backups/my-server/base/test_backup_id",
        )
        # AND a non-S3 cloud interface (e.g. Azure)
        cloud_interface_mock = Mock(spec=["list_bucket", "delete_objects"])
        cloud_interface_mock.list_bucket = Mock(return_value=[])
        backup_manager.server.get_backup_cloud_interface = lambda: cloud_interface_mock

        # WHEN _delete_cloud_backup_data is called
        with patch("barman.backup._logger") as mock_logger:
            backup_manager._delete_cloud_backup_data(backup_info)
            # THEN a warning is logged about non-S3 provider
            mock_logger.warning.assert_called_once_with(
                "aws_check_object_lock is only supported for S3 storage. "
                "Object lock checks will not be performed for server '%s'.",
                "my-server",
            )

    @patch("barman.backup.output")
    @patch("barman.backup.os.unlink", side_effect=OSError("Some error"))
    @patch("barman.backup.os.path.exists", return_value=True)
    def test_delete_cloud_backup_data_deleting_manifest_fails(
        self, _, mock_unlink, mock_output
    ):
        """
        Test that if an error occurs while deleting the backup manifest file during
        cloud backup deletion, the error is logged.
        """
        # GIVEN a BackupManager with a mocked backup info and cloud interface
        backup_manager = build_backup_manager(name="my-server")
        manifest_path = os.path.join(
            backup_manager.server.meta_directory, "test_backup_id-backup_manifest"
        )
        backup_info = Mock(
            backup_id="test_backup_id",
            get_backup_manifest_path=lambda: manifest_path,
            get_basebackup_directory=lambda: "my-backups/my-server/base/test_backup_id",
        )
        cloud_interface_mock = Mock(path="my-backups", list_bucket=lambda x: [])
        backup_manager.server.get_backup_cloud_interface = lambda: cloud_interface_mock
        # WHEN _delete_cloud_backup_data is called
        backup_manager._delete_cloud_backup_data(backup_info)
        # THEN when deleting the manifest file an error occurs and is logged
        mock_output.warning.assert_called_once_with(
            "Failed to delete backup manifest file: %s. Please manually delete "
            "this file if it still exists.",
            manifest_path,
        )

    @pytest.mark.parametrize("use_backup_cloud_storage", [True, False])
    @patch("barman.backup.KeepManagerMixin.keep_backup")
    @patch("barman.backup.KeepManagerMixinCloud")
    @patch("barman.backup.BackupManager.__init__", return_value=None)
    def test_keep_backup(
        self,
        _,
        mock_cloud_keep_mixin,
        mock_parent_keep_backup,
        use_backup_cloud_storage,
    ):
        """
        Assert that ``keep_backup`` stores annotation locally and/or in the cloud
        according to the server's configuration.
        """
        # Initialize a simpel backup manager
        backup_manager = BackupManager(None)
        # Simulate a server with or without cloud storage based on use_backup_cloud_storage
        backup_cloud_interface = mock.Mock()
        server = mock.Mock(
            use_backup_cloud_storage=use_backup_cloud_storage,
            get_backup_cloud_interface=lambda: backup_cloud_interface,
        )
        backup_manager.server = server
        backup_manager.config = mock.Mock()
        backup_manager.config.name = "test-server"

        # WHEN keep_backup is called
        backup_manager.keep_backup("fake_backup_id", "standalone")
        # THEN if using a cloud storage it should first store the annotation there
        if use_backup_cloud_storage:
            mock_cloud_keep_mixin.assert_called_once_with(
                cloud_interface=backup_cloud_interface,
                server_name="test-server",
            )
            mock_cloud_keep_mixin.return_value.keep_backup.assert_called_once_with(
                "fake_backup_id", "standalone"
            )
        else:
            mock_cloud_keep_mixin.assert_not_called()
        # AND always save the annotation locally
        mock_parent_keep_backup.assert_called_once_with("fake_backup_id", "standalone")

    @pytest.mark.parametrize("use_backup_cloud_storage", [True, False])
    @patch("barman.backup.KeepManagerMixin.release_keep")
    @patch("barman.backup.KeepManagerMixinCloud")
    @patch("barman.backup.BackupManager.__init__", return_value=None)
    def test_release_keep(
        self,
        _,
        mock_cloud_keep_mixin,
        mock_parent_release_keep,
        use_backup_cloud_storage,
    ):
        """
        Assert that ``release_keep`` removes the annotation locally and/or from the
        cloud storage, if configured.
        """
        # Initialize a simpel backup manager
        backup_manager = BackupManager(None)
        # Simulate a server with or without cloud storage based on use_backup_cloud_storage
        backup_cloud_interface = mock.Mock()
        server = mock.Mock(
            use_backup_cloud_storage=use_backup_cloud_storage,
            get_backup_cloud_interface=lambda: backup_cloud_interface,
        )
        backup_manager.server = server
        backup_manager.config = mock.Mock()
        backup_manager.config.name = "test-server"

        # WHEN keep_backup is called
        backup_manager.release_keep("fake_backup_id")
        # THEN if using a cloud storage it should first delete the annotation there
        if use_backup_cloud_storage:
            mock_cloud_keep_mixin.assert_called_once_with(
                cloud_interface=backup_cloud_interface,
                server_name="test-server",
            )
            mock_cloud_keep_mixin.return_value.release_keep.assert_called_once_with(
                "fake_backup_id"
            )
        else:
            mock_cloud_keep_mixin.assert_not_called()
        # AND always delete the annotation locally
        mock_parent_release_keep.assert_called_once_with("fake_backup_id")

    @patch("os.stat")
    @patch("barman.backup.fsync_file")
    @patch("barman.backup.fsync_dir")
    @patch("os.walk")
    @pytest.mark.parametrize("fsync", [True, False])
    def test_set_backup_sizes(
        self,
        mock_walk,
        mock_fsync_dir,
        mock_fsync_file,
        mock_stat,
        fsync,
    ):
        """
        Test that the _set_backup_sizes method correctly sets the backup sizes
        and optionally performs fsync.
        """
        # Set up the mocks
        backup_manager = build_backup_manager()
        mock_stat.reset_mock()
        mock_backup_info = Mock()

        # Mock os.walk to return a predefined directory structure
        mock_walk.return_value = [
            ("/root", ["dir1", "dir2"], ["file1.txt"]),
            ("/root/dir1", [], ["file2.txt"]),
            ("/root/dir2", ["subdir"], []),
            ("/root/dir2/subdir", [], ["file3.txt"]),
        ]

        # Define the mock return values for os.stat
        def mock_stat_return_value(backup):
            return_values = {
                "/root/file1.txt": {
                    "size": 1024,
                    "nlink": 3,
                },
                "/root/dir1/file2.txt": {
                    "size": 2048,
                    "nlink": 2,
                },
                "/root/dir2/subdir/file3.txt": {
                    "size": 4096,
                    "nlink": 1,
                },
            }
            return Mock(
                st_size=return_values[backup]["size"],
                st_nlink=return_values[backup]["nlink"],
            )

        mock_stat.side_effect = mock_stat_return_value

        # Define the mock return values for fsync_file
        def mock_fsync_file_return_value(file_path):
            return mock_stat_return_value(file_path)

        mock_fsync_file.side_effect = mock_fsync_file_return_value

        # Call the method under test
        backup_manager._set_backup_sizes(mock_backup_info, fsync)

        # Assertions for both with and without fsync cases
        mock_walk.assert_called_once_with(
            mock_backup_info.get_basebackup_directory.return_value,
        )
        assert mock_backup_info.set_attribute.call_count == 2
        mock_backup_info.set_attribute.assert_has_calls(
            [
                call("size", 7168),
                call("deduplicated_size", 4096),
            ]
        )
        mock_backup_info.save.assert_called_once()

        # Assertions when called with fsync
        if fsync:
            mock_stat.assert_not_called()
            assert mock_fsync_dir.call_count == 4
            mock_fsync_dir.assert_has_calls(
                [
                    call("/root"),
                    call("/root/dir1"),
                    call("/root/dir2"),
                    call("/root/dir2/subdir"),
                ]
            )
            assert mock_fsync_file.call_count == 3
            mock_fsync_file.assert_has_calls(
                [
                    call("/root/file1.txt"),
                    call("/root/dir1/file2.txt"),
                    call("/root/dir2/subdir/file3.txt"),
                ]
            )

        # Assertions without fsync (standard case)
        else:
            mock_fsync_dir.assert_not_called()
            mock_fsync_file.assert_not_called()
            assert mock_stat.call_count == 3
            mock_stat.assert_has_calls(
                [
                    call("/root/file1.txt"),
                    call("/root/dir1/file2.txt"),
                    call("/root/dir2/subdir/file3.txt"),
                ]
            )

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
            children_backup_ids=["child_backup_id"],
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

        # Create an incremental BackupInfo object with status DONE
        incremental_b_info = build_test_backup_info(
            backup_id="child_backup_id",
            server=backup_manager.server,
            status=BackupInfo.DONE,
            parent_backup_id="fake_backup_id",
        )
        incremental_b_info.save()

        available_backups = backup_manager.get_available_backups(
            status_filter=(BackupInfo.DONE,),
            backup_type_filter=(BackupInfo.NOT_INCREMENTAL),
        )

        assert available_backups[b_info.backup_id].to_dict() == (b_info.to_dict())
        # Check that the incremental backup have been filtered from the result
        assert incremental_b_info.backup_id not in available_backups
        assert len(available_backups) == 1

    @patch("barman.backup.BackupManager._load_backups_from_cloud")
    def test_load_backup_cache(self, mock_load_backups_from_cloud, tmpdir):
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

        # Should not check for cloud backups in this test
        mock_load_backups_from_cloud.assert_not_called()

    @patch("barman.backup.BackupManager._load_backups_from_cloud")
    def test_load_backup_cache_loads_from_cloud_when_server_is_disabled(
        self, mock_load_backups_from_cloud
    ):
        """
        Test that when loading the backup cache, if the server is inactive and has
        cloud storage enabled, it loads backups from the cloud.
        """
        # GIVEN a BackupManager with a server with cloud storage and inactive
        server = mock.Mock(use_backup_cloud_storage=True)
        server.config.active = False
        backup_manager = build_backup_manager(server)

        # WHEN _load_backup_cache is called
        backup_manager._load_backup_cache()

        # THEN it should load backups from the cloud
        mock_load_backups_from_cloud.assert_called_once()

    @patch("barman.backup.CloudLocalBackupInfo")
    @patch("barman.backup.CloudBackupCatalog")
    def test_load_backups_from_cloud(
        self, mock_cloud_catalog_cls, mock_cloud_local_backup_info_cls
    ):
        """
        Test that _load_backups_from_cloud fetches backup.info files from the
        cloud storage and populates the cache with CloudLocalBackupInfo objects.
        """
        # Prepare mocks
        # GIVEN a backup manager with a mocked cloud interface
        mock_cloud_interface = mock.Mock()
        server = mock.Mock(get_backup_cloud_interface=lambda: mock_cloud_interface)
        backup_manager = build_backup_manager(server=server)
        backup_manager._backup_cache = {}

        # Simulate CloudBackupCatalog returning a list of backup info objects from the cloud
        mock_backup_info_1 = mock.Mock(backup_id="backup_1")
        mock_backup_info_2 = mock.Mock(backup_id="backup_2")
        mock_cloud_catalog_cls.return_value.get_backup_list.return_value = {
            "backup_1": mock_backup_info_1,
            "backup_2": mock_backup_info_2,
        }

        # Mock CloudLocalBackupInfo to return mocks as to avoid actual instantiation
        mock_cloud_backup_1 = mock.Mock()
        mock_cloud_backup_2 = mock.Mock()
        mock_cloud_local_backup_info_cls.side_effect = [
            mock_cloud_backup_1,
            mock_cloud_backup_2,
        ]

        # WHEN _load_backups_from_cloud is called
        backup_manager._load_backups_from_cloud()

        # THEN CloudBackupCatalog is instantiated with the cloud interface and server name
        mock_cloud_catalog_cls.assert_called_once_with(
            cloud_interface=mock_cloud_interface,
            server_name=backup_manager.config.name,
        )

        # AND a CloudLocalBackupInfo is created for each backup returned by CloudBackupCatalog
        assert mock_cloud_local_backup_info_cls.call_count == 2

        # Creation of backup_1 (save, instantiate CloudLocalBackupInfo, load)
        mock_backup_info_1.save.assert_called_once()
        mock_cloud_local_backup_info_cls.assert_any_call(
            server=backup_manager.server, backup_id="backup_1"
        )
        mock_cloud_backup_1.load.assert_called_once()

        # Creation of backup_2 (save, instantiate CloudLocalBackupInfo, load)
        mock_backup_info_2.save.assert_called_once()
        mock_cloud_local_backup_info_cls.assert_any_call(
            server=backup_manager.server, backup_id="backup_2"
        )
        mock_cloud_backup_2.load.assert_called_once()

        # AND the CloudLocalBackupInfo objects are stored in the backup cache
        assert backup_manager._backup_cache["backup_1"] is mock_cloud_backup_1
        assert backup_manager._backup_cache["backup_2"] is mock_cloud_backup_2

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
            "TestServer",
            False,
            hint="have 0 non-incremental backups, expected at least 1",
        )
        # Test the satisfied minimum_redundancy option
        # Add parent backup
        b_info = build_test_backup_info(
            backup_id="fake_backup_id",
            server=backup_manager.server,
            children_backup_ids=["child_backup_id1"],
        )
        b_info.save()

        # Add 2 incremental backups - chained from `fake_backup_id`
        b_info_ch1 = build_test_backup_info(
            backup_id="child_backup_id1",
            server=backup_manager.server,
            parent_backup_id="fake_backup_id",
            children_backup_ids=["child_backup_id2"],
        )
        b_info_ch1.save()

        b_info_ch2 = build_test_backup_info(
            backup_id="child_backup_id2",
            server=backup_manager.server,
            parent_backup_id="child_backup_id1",
        )
        b_info_ch2.save()

        strategy_mock.reset_mock()
        backup_manager._load_backup_cache()
        backup_manager.check(strategy_mock)
        # Expect a success from the method
        strategy_mock.result.assert_called_with(
            "TestServer",
            True,
            hint="have 1 non-incremental backups, expected at least 1",
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

    @patch("barman.backup.BackupManager.delete_backup")
    @patch("barman.backup.BackupManager.get_available_backups")
    @patch("barman.backup.BackupManager.check_delete_annotation")
    @patch("barman.backup.BackupManager.release_delete_annotation")
    def test_cron_retention_obsoletes_backups_with_delete_annotation(
        self,
        release_delete_annotation,
        check_delete_annotation,
        get_available_backups,
        delete_backup,
        tmpdir,
    ):
        """
        Verify that a backup with the delete annotation is marked as obsolete and then deleted.
        """
        backup_manager = build_backup_manager()
        backup_manager.server.config.name = "TestServer"
        backup_manager.server.config.barman_lock_directory = tmpdir.strpath
        backup_manager.server.config.backup_options = []
        backup_manager.server.config.retention_policy = Mock()
        backup_manager.config.retention_policy.report.return_value = {
            "test_backup": BackupInfo.VALID,
        }
        available_backups = {
            "test_backup": build_test_backup_info(
                server=backup_manager.server, backup_id="test_backup"
            )
        }
        get_available_backups.return_value = available_backups
        check_delete_annotation.return_value = True

        backup_manager.cron_retention_policy()

        # Ensure the backup was marked as obsolete
        assert (
            backup_manager.config.retention_policy.report.return_value["test_backup"]
            == BackupInfo.OBSOLETE
        )
        # Ensure the delete annotation was released
        release_delete_annotation.assert_called_once_with("test_backup")
        # Ensure the backup was deleted
        delete_backup.assert_called_once_with(
            available_backups["test_backup"], skip_wal_cleanup_if_standalone=False
        )

    @patch("barman.backup.BackupManager.delete_backup")
    @patch("barman.backup.BackupManager.get_available_backups")
    @patch("barman.backup.BackupManager.check_delete_annotation")
    @patch("barman.backup.BackupManager.release_delete_annotation")
    def test_cron_retention_orphan_backup_skip_deletion_and_warn(
        self,
        release_delete_annotation,
        check_delete_annotation,
        get_available_backups,
        delete_backup,
        tmpdir,
        caplog,
    ):
        """
        Verify that if the backup is orphaned, cron will output a warning.
        """
        backup_manager = build_backup_manager(
            main_conf={"backup_options": "concurrent_backup"}
        )
        backup_manager.server.config.name = "TestServer"
        backup_manager.server.config.barman_lock_directory = tmpdir.strpath
        backup_manager.server.config.backup_options = []
        backup_manager.server.config.retention_policy = Mock()
        backup_manager.config.retention_policy.report.return_value = {
            "test_backup": BackupInfo.VALID,
        }
        available_backups = {
            "test_backup": build_test_backup_info(
                server=backup_manager.server, backup_id="test_backup"
            )
        }
        get_available_backups.return_value = available_backups
        check_delete_annotation.return_value = False

        # Simulate orphan backup
        with patch("barman.infofile.LocalBackupInfo.is_orphan") as mock_is_orphan:
            mock_is_orphan.return_value = True
            backup_manager.cron_retention_policy()

        # Ensure the warning was logged
        expected_warning = (
            "Backup 'test_backup' is an orphan backup, this is possibly "
            "the result of an incomplete delete operation. Please manually "
            "delete the following files or directories if present:\n* %s \n* %s \n"
            % (
                available_backups["test_backup"].get_basebackup_directory(),
                available_backups["test_backup"].get_filename(),
            )
        )
        assert expected_warning in caplog.text

    @pytest.mark.parametrize("should_fail", (True, False))
    @patch("barman.infofile.LocalBackupInfo.save")
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
    @patch("barman.infofile.LocalBackupInfo.save")
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

    @patch("barman.infofile.LocalBackupInfo.save")
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

    @patch("barman.infofile.LocalBackupInfo.save")
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

    @pytest.mark.parametrize("summarize_wal", ["on", "off", None])
    @patch("barman.backup.BackupManager.get_backup")
    def test_validate_incremental_backup_parent_backup_info_summarize_wal(
        self, mock_get_backup, summarize_wal
    ):
        """
        Verify how ``_validate_incremental_backup_configs`` behave based on the parent
        backup info.
        """
        backup_manager = build_backup_manager(global_conf={"backup_method": "postgres"})

        # mock the postgres object to set server version
        mock_postgres = Mock()
        backup_manager.executor.server.postgres = mock_postgres
        mock_postgres.configure_mock(server_version=170000)

        # To get to the check of the parent_backup `summarize_wal` status
        # `summarize_wal` should be 'on' in the postgres node.
        backup_manager.executor.server.postgres.get_setting.return_value = "on"

        mock_get_backup.return_value = build_test_backup_info(
            server=backup_manager.server, backup_id="12345", summarize_wal=summarize_wal
        )
        incremental_kwargs = {"parent_backup_id": Mock()}

        err_msg = (
            "Backup ID 12345 is not eligible as a parent for an incremental "
            "backup because WAL summaries were not enabled when that backup was taken."
        )

        if summarize_wal != "on":
            with pytest.raises(BackupException, match=err_msg):
                backup_manager._validate_incremental_backup_configs(
                    **incremental_kwargs
                )
        else:
            backup_manager._validate_incremental_backup_configs(**incremental_kwargs)

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
                ),
            )
            for bkp_id, par_bkp_id in available_backups.items()
        )
        get_available_backups.return_value = backups

        last_full_backup = backup_manager.get_last_full_backup_id()
        get_available_backups.assert_called_once()
        assert last_full_backup == "20241011T180000"
        get_available_backups.reset_mock()
        # Add an rsync backup
        backup_manager = build_backup_manager(global_conf={"backup_method": "rsync"})
        rsync_backup = build_test_backup_info(
            server=backup_manager.server,
            backup_id="20241015T180000",
        )

        backups["20241015T180000"] = rsync_backup
        get_available_backups.return_value = backups

        last_full_backup = backup_manager.get_last_full_backup_id()
        get_available_backups.assert_called_once()
        assert last_full_backup == "20241015T180000"

    @patch("barman.backup._logger")
    @patch("barman.backup.output")
    @patch("barman.backup.BackupManager._set_backup_sizes")
    def test_backup_fsync_and_set_sizes(
        self,
        mock_set_backup_sizes,
        mock_output,
        mock_logger,
    ):
        """
        Test the function for correct backup size and deduplication ratio
        setting and logging.
        """
        backup_manager = build_backup_manager()
        backup_manager.executor.current_action = "calculating backup size"
        backup_info = Mock()
        backup_info.size = 0

        # Test case with no deduplication ratio output
        backup_manager.backup_fsync_and_set_sizes(backup_info)
        mock_logger.debug.assert_called_once_with("calculating backup size")
        mock_set_backup_sizes.assert_called_with(backup_info, fsync=True)
        mock_output.info.assert_called_with("Backup size: %s" % "0 B")

        # Reset mocks
        mock_logger.reset_mock()
        mock_set_backup_sizes.reset_mock()
        mock_output.reset_mock()

        # Test case when reuse_backup == "link"
        backup_manager.config.reuse_backup = "link"
        backup_info.size = 1000
        backup_info.deduplicated_size = 800
        backup_manager.backup_fsync_and_set_sizes(backup_info)
        mock_logger.debug.assert_called_once_with("calculating backup size")
        mock_set_backup_sizes.assert_called_with(backup_info, fsync=True)
        mock_output.info.assert_called_once_with(
            "Backup size: %s. Actual size on disk: %s (-%s deduplication ratio)."
            % ("1000 B", "800 B", "20.00%")
        )

    @pytest.mark.parametrize("backup_id", ["20250107T120000", None])
    @patch("barman.backup.get_backup_id_from_target_time")
    @patch("barman.backup.BackupManager.get_available_backups")
    def test_get_closest_backup_id_from_target_time(
        self, mock_get_available_backups, mock_get_backup_id_from_target_time, backup_id
    ):
        """
        Test the function get_closest_backup_id_from_target_time will return the correct
        backup_id from the catalog depending on the recovery target `target_time`.
        """
        backup_manager = build_backup_manager()

        available_backups = {
            "20250107T120000": {
                "backup_id": "20250107T120000",
                "end_time": load_datetime_tz("2025-01-07 12:00:00"),
                "end_xlog": "3/5E000000",
                "status": "DONE",
            },
        }

        backups = dict(
            (
                bkp_id,
                build_test_backup_info(server=backup_manager.server, **bkp_metadata),
            )
            for bkp_id, bkp_metadata in available_backups.items()
        )

        target_time = "2025-01-07 12:15:00"
        target_tli = None
        dict_values = mock_get_available_backups.return_value.values.return_value = (
            backups.values()
        )
        mock_get_backup_id_from_target_time.return_value = backup_id
        backup_id_found = backup_manager.get_closest_backup_id_from_target_time(
            target_time, target_tli
        )
        mock_get_available_backups.assert_called_once()
        mock_get_backup_id_from_target_time.assert_called_once_with(
            dict_values, target_time, target_tli
        )
        assert backup_id == backup_id_found

    @pytest.mark.parametrize("backup_id", ["20250107T120000", None])
    @patch("barman.backup.get_backup_id_from_target_lsn")
    @patch("barman.backup.BackupManager.get_available_backups")
    def test_get_closest_backup_id_from_target_lsn(
        self, mock_get_available_backups, mock_get_backup_id_from_target_lsn, backup_id
    ):
        """
        Test the function get_closest_backup_id_from_target_time will return the correct
        backup_id from the catalog depending on the recovery target `target_time`.
        """
        backup_manager = build_backup_manager()

        available_backups = {
            "20250107T120000": {
                "backup_id": "20250107T120000",
                "end_time": load_datetime_tz("2025-01-07 12:00:00"),
                "end_xlog": "3/5E000000",
                "status": "DONE",
            },
        }

        backups = dict(
            (
                bkp_id,
                build_test_backup_info(server=backup_manager.server, **bkp_metadata),
            )
            for bkp_id, bkp_metadata in available_backups.items()
        )

        target_lsn = "3/5F000000"
        target_tli = None
        dict_values = mock_get_available_backups.return_value.values.return_value = (
            backups.values()
        )
        mock_get_backup_id_from_target_lsn.return_value = backup_id
        backup_id_found = backup_manager.get_closest_backup_id_from_target_lsn(
            target_lsn, target_tli
        )
        mock_get_available_backups.assert_called_once()
        mock_get_backup_id_from_target_lsn.assert_called_once_with(
            dict_values, target_lsn, target_tli
        )
        assert backup_id == backup_id_found

    @pytest.mark.parametrize("backup_id", ["20250107T120000", None])
    @patch("barman.backup.get_backup_id_from_target_tli")
    @patch("barman.backup.BackupManager.get_available_backups")
    def test_get_last_backup_id_from_target_tli(
        self, mock_get_available_backups, mock_get_backup_id_from_target_tli, backup_id
    ):
        """
        Test the function get_closest_backup_id_from_target_time will return the correct
        backup_id from the catalog depending on the recovery target `target_time`.
        """
        backup_manager = build_backup_manager()

        available_backups = {
            "20250107T120000": {
                "backup_id": "20250107T120000",
                "end_time": load_datetime_tz("2025-01-07 12:00:00"),
                "end_xlog": "3/5E000000",
                "status": "DONE",
                "timeline": 1,
            },
        }

        backups = dict(
            (
                bkp_id,
                build_test_backup_info(server=backup_manager.server, **bkp_metadata),
            )
            for bkp_id, bkp_metadata in available_backups.items()
        )

        target_tli = 1
        dict_values = mock_get_available_backups.return_value.values.return_value = (
            backups.values()
        )
        mock_get_backup_id_from_target_tli.return_value = backup_id
        backup_id_found = backup_manager.get_last_backup_id_from_target_tli(target_tli)
        mock_get_available_backups.assert_called_once()
        mock_get_backup_id_from_target_tli.assert_called_once_with(
            dict_values, target_tli
        )
        assert backup_id == backup_id_found

    @patch("barman.backup.get_backup_info_from_name")
    @patch("barman.backup.BackupManager.get_available_backups")
    def test_get_backup_id_from_name(
        self, mock_get_available_backups, mock_get_backup_info_from_name
    ):
        """
        Test that the method `get_backup_id_from_name` will behave as expected
        throughout its code path, calling the correct mocked methods and returning the
        mocked result.
        """
        backup_manager = build_backup_manager()
        available_backups = {
            "20250107T120000": {
                "backup_name": "my_test_backup",
                "backup_id": "20250107T120000",
                "end_time": load_datetime_tz("2025-01-07 12:00:00"),
                "end_xlog": "3/5E000000",
                "status": "DONE",
                "timeline": 1,
            },
        }

        backups = dict(
            (
                bkp_id,
                build_test_backup_info(server=backup_manager.server, **bkp_metadata),
            )
            for bkp_id, bkp_metadata in available_backups.items()
        )
        dict_values = mock_get_available_backups.return_value.values.return_value = (
            backups.values()
        )
        mock_get_backup_info_from_name.return_value = backups["20250107T120000"]
        backup_id_found = backup_manager.get_backup_id_from_name("my_test_backup")
        mock_get_available_backups.assert_called_once()
        mock_get_backup_info_from_name.assert_called_once_with(
            dict_values, "my_test_backup"
        )
        assert "20250107T120000" == backup_id_found

    @patch("barman.backup.EncryptionManager.get_encryption")
    @patch("barman.backup.EncryptionManager.validate_config")
    @patch("barman.backup.BackupManager._encrypt_tar_backup")
    def test_encrypt_backup(
        self, mock_encrypt_tar_backup, mock_validate_config, mock_get_encryption
    ):
        """Test that the `_encrypt_backup` works correctly"""

        # GIVEN a backup manager with some tar encryption enabled
        backup_manager = build_backup_manager()
        backup_manager.config.encryption = "gpg"
        backup_manager.config.backup_compression_format = "tar"

        # WHEN `_encrypt_backup` is called on a backup
        mock_backup_info = Mock(spec=build_test_backup_info(backup_manager.server))
        backup_manager._encrypt_backup(mock_backup_info)

        # THEN a valid encryptor is fetched using the encryption manager
        mock_validate_config.assert_called_once()
        mock_get_encryption.assert_called_once_with()
        mock_encryptor = mock_get_encryption.return_value

        # AND `_encrypt_tar_backup` is called with the correct arguments
        mock_encrypt_tar_backup.assert_called_once_with(
            mock_backup_info, mock_encryptor
        )

        # AND the encryption attribute is set in the backup info
        mock_backup_info.set_attribute.assert_called_once_with(
            "encryption", mock_encryptor.NAME
        )

    @patch("os.unlink")
    def test_encrypt_tar_backup(self, mock_os_unlink):
        """
        Test that `_encrypt_tar_backup` encrypts all `.tar` and `.tar.*`
        files in the backup directory.
        """
        # GIVEN a backup manager and a mock backup info
        backup_manager = build_backup_manager()
        mock_backup_info = Mock(spec=build_test_backup_info(backup_manager.server))

        # AND a backup directory with the following files
        mock_backup_info.get_directory_entries.return_value = [
            "path/to/backup/base.tar",
            "path/to/backup/25137.tar.gz",
            "path/to/backup/25138.tar.zstd",
            "path/to/backup/backup_manifest",
            "path/to/backup/annotations/keep",
            "path/to/backup/annotations/delete",
            "path/to/backup/text_file.txt",
            "path/to/backup/random_file",
        ]

        mock_encryptor = Mock()

        # WHEN `_encrypt_tar_backup` is called
        backup_manager._encrypt_tar_backup(mock_backup_info, mock_encryptor)

        # THEN `encrypt_file` is called only for `.tar` and `.tar.*` files
        dest_directory = "path/to/backup"
        mock_encryptor.encrypt.assert_has_calls(
            [
                call("path/to/backup/base.tar", dest_directory),
                call("path/to/backup/25137.tar.gz", dest_directory),
                call("path/to/backup/25138.tar.zstd", dest_directory),
            ],
            any_order=True,
        )
        assert mock_encryptor.encrypt.call_count == 3

        # AND the unencrypted files are deleted
        mock_os_unlink.assert_has_calls(
            [
                call("path/to/backup/base.tar"),
                call("path/to/backup/25137.tar.gz"),
                call("path/to/backup/25138.tar.zstd"),
            ],
            any_order=True,
        )

    @mock.patch("barman.backup.unix_command_factory")
    def test_recover_check_pgdata_directory_is_empty(
        self, remote_cmd_mock, tmpdir, caplog
    ):
        command = remote_cmd_mock.return_value
        backup_manager = build_backup_manager(
            main_conf={"backup_options": "concurrent_backup"}
        )
        destination = tmpdir.mkdir("data").strpath

        # destination for pgdata is non-empty.
        command.list_dir_content.side_effect = ["non-empty"]

        backup_info = build_test_backup_info()
        # No tablespaces
        with pytest.raises(SystemExit):
            backup_manager.recover(backup_info, destination)

        assert (
            "The restore operation cannot proceed because the destination folder"
            in caplog.text
        )

    @mock.patch("barman.backup.unix_command_factory")
    def test_recover_check_tablespace_directory_is_empty(
        self, remote_cmd_mock, tmpdir, caplog
    ):
        command = remote_cmd_mock.return_value
        backup_manager = build_backup_manager(
            main_conf={"backup_options": "concurrent_backup"}
        )
        destination = tmpdir.mkdir("data").strpath

        backup_info = build_test_backup_info(
            tablespaces=[
                ("ts_data", 1234567, "/tbs/destination/ts_data"),
                ("ts_data2", 1234567, "/tbs/destination/ts_data2"),
            ]
        )
        # destination pgdata is empty, relocated 'ts_data' is non-empty.
        command.list_dir_content.side_effect = [None, "non-empty"]
        with pytest.raises(SystemExit):
            backup_manager.recover(
                backup_info,
                destination,
                tablespaces={"ts_data": "/new_destination"},
            )
        assert (
            "The restore operation cannot proceed. The destination path "
            "'/new_destination' for the tablespace 'ts_data' is not empty"
            in caplog.text
        )

        # destination for pgdata is empty, relocated 'ts_data' is empty, non-relocated
        # 'ts_data2' is non-empty.
        command.list_dir_content.side_effect = [None, None, "non-empty"]
        with pytest.raises(SystemExit):
            backup_manager.recover(
                backup_info,
                destination,
                tablespaces={"ts_data": "/new_destination"},
            )
        assert (
            "The restore operation cannot proceed. The destination path "
            "'/tbs/destination/ts_data2' for the tablespace 'ts_data2' is not empty"
            in caplog.text
        )

    @mock.patch("barman.backup.recovery_executor_factory")
    @mock.patch("barman.backup.unix_command_factory")
    def test_recover_with_delta_restore_recovery_option(
        self, remote_cmd_mock, rec_exec_fac_mock, tmpdir
    ):
        """
        Test that delta_restore will call executor.recover and will not halt the
        execution because of non-empty files.
        """
        command = remote_cmd_mock.return_value
        backup_manager = build_backup_manager(
            main_conf={
                "backup_options": "concurrent_backup",
                "recovery_options": "delta-restore",
            }
        )
        destination = tmpdir.mkdir("data").strpath

        # destination for pgdata is non-empty.
        command.list_dir_content.side_effect = ["non-empty"]

        backup_info = build_test_backup_info()

        executor = mock.Mock()
        rec_exec_fac_mock.return_value = executor
        executor.recover.return_value = {
            "configuration_files": ["postgresql.conf", "postgresql.auto.conf"],
            "tempdir": tmpdir.strpath,
            "results": {
                "changes": [],
                "warnings": [],
                "missing_files": [],
                "get_wal": False,
                "recovery_start_time": datetime.now(dateutil.tz.tzlocal()),
            },
            "target_datetime": "2015-06-03 16:11:03.71038+02",
            "wal_dest": "/wherever",
        }
        # No tablespaces
        backup_manager.recover(backup_info, destination)

        executor.recover.assert_called()


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

    @pytest.fixture
    def mock_put_annotation(self):
        with patch("barman.backup.AnnotationManagerFile.put_annotation") as mock:
            mock.return_value = None
            yield mock

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
        # Make it a non-orphan backup
        os.mkdir("%s/%s" % (backup_path, "base"))

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

    @patch("barman.backup.shutil.rmtree", wraps=shutil.rmtree)
    def test_delete_wal_directory_when_feasible(self, mock_rmtree, backup_manager):
        """
        Test that entire WAL directories are removed with ``rmtree`` when all files
        in that directory are no longer needed by Barman.
        """
        # Case 1: All 256 files in a WAL directory are to be deleted
        # GIVEN a backup
        backup_info = build_test_backup_info(
            backup_id="20210723T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000101",
            end_wal="000000010000000000000105",
        )
        # AND 256 files previous to the begin_wal of that backup
        wals_directory = backup_manager.server.config.wals_directory
        self._create_wals_on_filesystem(
            wals_directory, "000000010000000000000001", "000000010000000000000100"
        )
        # WHEN the WALs before the backup are requested to be deleted
        backup_manager.remove_wal_before_backup(backup_info)
        # THEN rmtree can be used to delete the whole directory containing the WALs
        mock_rmtree.assert_called_once()
        assert mock_rmtree.call_args.args[0].endswith("wals/0000000100000000")

        mock_rmtree.reset_mock()

        # Case 2: A few files in the directory but all also to be deleted
        self._create_wals_on_filesystem(
            wals_directory, "000000010000000000000050", "000000010000000000000100"
        )
        backup_manager.remove_wal_before_backup(backup_info)
        mock_rmtree.assert_called_once()
        assert mock_rmtree.call_args.args[0].endswith("wals/0000000100000000")

        mock_rmtree.reset_mock()

        # Case 3: A few files have to be kept so the directory can not be deleted
        backup_info = build_test_backup_info(
            backup_id="20210723T095432",
            server=backup_manager.server,
            begin_wal="000000010000000000000101",
            end_wal="000000010000000000000105",
        )
        self._create_wals_on_filesystem(
            wals_directory, "000000010000000000000050", "000000010000000000000100"
        )
        backup_manager.remove_wal_before_backup(
            backup_info,
            wal_ranges_to_protect=[
                ("000000010000000000000070", "000000010000000000000080"),
            ],
        )
        mock_rmtree.assert_not_called()


class TestVerifyBackup:
    """Test backupManager verify_backup function"""

    @patch("barman.backup.PgVerifyBackup")
    def test_verify_backup_nominal(self, mock_pg_verify_backup):
        backup_path = "/fake/path"
        pg_verify_backup_path = "/path/to/pg_verifybackup"
        backup_manager = build_backup_manager()
        backup_manager.server.use_backup_cloud_storage = False
        backup_manager.server.use_wal_cloud_storage = False
        mock_backup_info = Mock()
        mock_backup_info.compression = None
        mock_backup_info.encryption = None
        mock_backup_info.get_data_directory.return_value = backup_path

        mock_pg_verify_backup.get_version_info.return_value = {
            "full_path": pg_verify_backup_path,
            "full_version": "13.2",
            "major_version": barman.utils.LooseVersion("13"),
        }

        backup_manager.verify_backup(mock_backup_info)

        mock_backup_info.get_data_directory.assert_called_once()
        mock_pg_verify_backup_instance = mock_pg_verify_backup.return_value
        mock_pg_verify_backup.assert_called_once_with(
            data_path=backup_path,
            command=pg_verify_backup_path,
            version="13.2",
            format=None,
        )
        mock_pg_verify_backup.return_value.assert_called_once()
        mock_pg_verify_backup_instance.get_output.assert_called_once()

    @patch("barman.backup.PgVerifyBackup")
    def test_verify_backup_exec_not_found(self, mock_pg_verify_backup):
        backup_manager = build_backup_manager()
        mock_backup_info = Mock()
        mock_backup_info.compression = None
        mock_backup_info.encryption = None
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
        mock_backup_info.compression = None
        mock_backup_info.encryption = None
        mock_backup_info.get_data_directory.return_value = "/fake/path3"
        mock_pg_verify_backup.get_version_info.return_value = {
            "full_path": "/path/to/pg_verifybackup",
            "full_version": "13.2",
            "major_version": barman.utils.LooseVersion("13"),
        }
        mock_pg_verify_backup_instance = mock_pg_verify_backup.return_value
        mock_pg_verify_backup_instance.side_effect = CommandFailedException(
            {"err": "Failed"}
        )

        backup_manager.verify_backup(mock_backup_info)

        mock_pg_verify_backup_instance.get_output.assert_not_called()

    @patch("barman.backup.output")
    def test_verify_backup_not_supported_with_cloud(self, mock_output):
        backup_manager = build_backup_manager()
        mock_backup_info = Mock()
        backup_manager.server.use_backup_cloud_storage = True
        backup_manager.server.use_wal_cloud_storage = True
        backup_manager.verify_backup(mock_backup_info)
        mock_output.error.assert_called_once_with(
            "Backup verification is not supported for servers using cloud storage"
        )

    @pytest.mark.parametrize("compression", ["none", "gzip", "lz4", "zstd"])
    @patch("barman.backup.PgVerifyBackup")
    def test_verify_backup_tar_format_pg18(self, mock_pg_verify_backup, compression):
        """Tar (and compressed tar) backups should be verified with -F tar when
        pg_verifybackup >= 18 is available."""
        # GIVEN a tar-format backup and pg_verifybackup 18
        backup_path = "/fake/path"
        pg_verify_backup_path = "/path/to/pg_verifybackup"
        backup_manager = build_backup_manager()
        backup_manager.server.use_backup_cloud_storage = False
        backup_manager.server.use_wal_cloud_storage = False
        mock_backup_info = Mock()
        mock_backup_info.compression = compression
        mock_backup_info.encryption = None
        mock_backup_info.get_data_directory.return_value = backup_path
        mock_pg_verify_backup.get_version_info.return_value = {
            "full_path": pg_verify_backup_path,
            "full_version": "18.0",
            "major_version": barman.utils.LooseVersion("18"),
        }

        # WHEN verify_backup is invoked
        backup_manager.verify_backup(mock_backup_info)

        # THEN PgVerifyBackup is called with format="tar"
        mock_pg_verify_backup.assert_called_once_with(
            data_path=backup_path,
            command=pg_verify_backup_path,
            version="18.0",
            format="tar",
        )
        mock_pg_verify_backup.return_value.assert_called_once()

    @pytest.mark.parametrize("compression", ["none", "gzip", "lz4", "zstd"])
    @patch("barman.backup.output")
    @patch("barman.backup.PgVerifyBackup")
    def test_verify_backup_tar_format_requires_pg18(
        self, mock_pg_verify_backup, mock_output, compression
    ):
        """Verifying tar/compressed backups with pg_verifybackup < 18 must
        fail with a clear error and never invoke the command."""
        # GIVEN a tar-format backup and a pre-PG18 pg_verifybackup
        backup_manager = build_backup_manager()
        backup_manager.server.use_backup_cloud_storage = False
        backup_manager.server.use_wal_cloud_storage = False
        mock_backup_info = Mock()
        mock_backup_info.compression = compression
        mock_backup_info.encryption = None
        mock_backup_info.get_data_directory.return_value = "/fake/path"
        mock_pg_verify_backup.get_version_info.return_value = {
            "full_path": "/path/to/pg_verifybackup",
            "full_version": "17.0",
            "major_version": barman.utils.LooseVersion("17"),
        }

        # WHEN verify_backup is invoked
        backup_manager.verify_backup(mock_backup_info)

        # THEN an error is emitted and pg_verifybackup is not instantiated
        mock_pg_verify_backup.assert_not_called()
        mock_output.error.assert_called_once()
        assert (
            "pg_verifybackup from Postgres 18 or newer"
            in mock_output.error.call_args[0][0]
        )

    @patch("barman.backup.output")
    @patch("barman.backup.PgVerifyBackup")
    def test_verify_backup_refuses_encrypted(self, mock_pg_verify_backup, mock_output):
        """Encrypted backups must be refused by verify_backup."""
        # GIVEN an encrypted backup
        backup_manager = build_backup_manager()
        backup_manager.server.use_backup_cloud_storage = False
        backup_manager.server.use_wal_cloud_storage = False
        mock_backup_info = Mock()
        mock_backup_info.compression = "gzip"
        mock_backup_info.encryption = "gpg"

        # WHEN verify_backup is invoked
        backup_manager.verify_backup(mock_backup_info)

        # THEN it is refused and pg_verifybackup is not invoked at all
        mock_pg_verify_backup.assert_not_called()
        mock_pg_verify_backup.get_version_info.assert_not_called()
        mock_output.error.assert_called_once_with(
            "Backup verification is not supported for encrypted backups"
        )


class TestCloudBackup(object):
    """Test handling of cloud backups by BackupManager."""

    @patch("barman.backup.CloudBackupExecutor")
    def test_cloud_backup_method(self, mock_cloud_executor):
        """
        Verify that a CloudBackupExecutor is created for backup_method "cloud".
        """
        # GIVEN a server with backup_method = "cloud"
        server = build_mocked_server(
            "test_server",
            main_conf={
                "backup_method": "local-to-cloud",
                "basebackups_directory": "s3://bucket/path",
            },
        )
        # WHEN a BackupManager is created for that server
        manager = BackupManager(server=server)
        # THEN its executor is a CloudBackupExecutor
        assert manager.executor == mock_cloud_executor.return_value
        # AND CloudBackupExecutor was called with the backup manager
        mock_cloud_executor.assert_called_once_with(manager)

    @patch("barman.backup.CloudWalArchiver")
    @patch("barman.backup.output")
    def test_cloud_wal_archive_success(self, mock_output, mock_cloud_wal_archiver):
        """
        Test cloud_wal_archive creates a CloudWalArchiver and delegates archival to it.
        """
        # GIVEN a backup manager for a server using cloud WAL storage
        backup_manager = build_backup_manager()
        backup_manager.server.wal_storage = Mock(spec=CloudWalStorageStrategy)

        # WHEN cloud_wal_archive is called with no explicit parallel setting
        wal_path = "/pg_wal/000000010000000000000001"
        backup_manager.cloud_wal_archive(wal_path)

        # THEN a CloudWalArchiver is created with the backup manager
        mock_cloud_wal_archiver.assert_called_once_with(backup_manager)

        # AND archive is called with the wal path and default options
        mock_cloud_wal_archiver.return_value.archive.assert_called_once_with(
            wal_path, 0
        )

        # AND no error is logged
        mock_output.error.assert_not_called()

    @patch("barman.backup.CloudWalArchiver")
    @patch("barman.backup.output")
    def test_cloud_wal_archive_with_parallel(
        self, mock_output, mock_cloud_wal_archiver
    ):
        """
        Test cloud_wal_archive passes the parallel value through to CloudWalArchiver.
        """
        # GIVEN a backup manager for a server using cloud WAL storage
        backup_manager = build_backup_manager()
        backup_manager.server.wal_storage = Mock(spec=CloudWalStorageStrategy)

        # WHEN cloud_wal_archive is called with an explicit parallel value
        wal_path = "/pg_wal/000000010000000000000001"
        backup_manager.cloud_wal_archive(wal_path, parallel=4)

        # THEN archive is called with the provided parallel value
        mock_cloud_wal_archiver.return_value.archive.assert_called_once_with(
            wal_path, 4
        )

    @patch("barman.backup.CloudWalArchiver")
    @patch("barman.backup.output")
    def test_cloud_wal_archive_invalid_wal_storage_strategy(
        self, mock_output, mock_cloud_wal_archiver
    ):
        """
        Test cloud_wal_archive logs an error if the server's wal_storage is not a
        CloudWalStorageStrategy, and does not create a CloudWalArchiver.
        """
        # GIVEN a backup manager whose wal_storage is not a CloudWalStorageStrategy
        backup_manager = build_backup_manager()
        backup_manager.server.wal_storage = Mock(spec=LocalWalStorageStrategy)

        # WHEN cloud_wal_archive is called
        wal_path = "/pg_wal/000000010000000000000001"
        backup_manager.cloud_wal_archive(wal_path)

        # THEN no CloudWalArchiver is created
        mock_cloud_wal_archiver.assert_not_called()

        # AND an error is logged
        mock_output.error.assert_called_once_with(
            "The 'cloud-wal-archive' command can only be used with cloud WAL "
            "storage strategies. Please check your server configuration and ensure "
            "that the `wals_directory` points to a cloud object storage."
        )

    @patch("barman.backup.CloudWalDownloader")
    def test_cloud_wal_restore(self, mock_wal_downloader):
        """
        Test cloud_wal_restore calls the wal_storage restore method for a server using
        cloud storage for WALs.
        """
        # Prepare the server and backup manager
        mock_cloud_interface = mock.Mock()
        server = mock.Mock(
            get_wal_cloud_interface=lambda: mock_cloud_interface,
        )
        backup_manager = build_backup_manager(server)
        backup_manager.config.name = "test-server"
        backup_manager.config.cloud_wal_restore_parallel = 2

        # WHEN cloud_wal_restore is called
        wal_name = "000000010000000000000001"
        wal_dest = "/var/lib/pgsql/17/data/pg_wal/000000010000000000000001"
        spool_dir = "/path/to/spool"
        backup_manager.cloud_wal_restore(wal_name, wal_dest, spool_dir)

        # THEN a CloudWalDownloader is created with the cloud interface and server name
        mock_wal_downloader.assert_called_once_with(
            mock_cloud_interface, "test-server", spool_dir
        )
        # AND the download_wal method is called correctly
        mock_wal_downloader.return_value.download_wal.assert_called_once_with(
            wal_name, wal_dest, False, 2
        )


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
    @patch("barman.backup.AnnotationManagerFile.put_annotation")
    def test_snapshot_delete(
        self,
        mock_put_annotation,
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


class TestExportBackup(object):
    """Test class for BackupManager.export_backup and related helper methods."""

    @pytest.fixture
    def wal_env(self, tmpdir):
        """
        Set up a backup manager with WAL files on disk and a matching xlog.db.

        Returns a dict with ``backup_manager``, ``wals_dir``, ``hash_dir``,
        and helpers to populate WAL files and build a mock backup_info.
        """
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )
        wals_dir = tmpdir.mkdir("wals")
        hash_dir = wals_dir.mkdir("0000000100000000")
        backup_manager.server.config.wals_directory = wals_dir.strpath

        def populate(wal_configs):
            """
            Create WAL files and xlog.db entries.

            :param list wal_configs: list of (wal_name, compression) tuples.
                Use ``None`` for no compression.
            """
            xlog_db_content = ""
            for wal_name, compression in wal_configs:
                hash_dir.join(wal_name).write_binary(os.urandom(16))
                comp_str = compression if compression else "None"
                xlog_db_content += f"{wal_name}\t16\t1712994000.0\t{comp_str}\tNone\n"
            wals_dir.join("xlog.db").write(xlog_db_content)

        def populate_xlogdb_only(wal_configs):
            """
            Create xlog.db entries without creating WAL files on disk.

            :param list wal_configs: list of (wal_name, compression) tuples.
            """
            xlog_db_content = ""
            for wal_name, compression in wal_configs:
                comp_str = compression if compression else "None"
                xlog_db_content += f"{wal_name}\t16\t1712994000.0\t{comp_str}\tNone\n"
            wals_dir.join("xlog.db").write(xlog_db_content)

        def make_backup_info(required_wals, backup_id="20260413T100000"):
            """Build a mock backup_info with the given required WAL segments."""
            backup_info = Mock()
            backup_info.backup_id = backup_id
            backup_info.get_required_wal_segments.return_value = iter(required_wals)
            return backup_info

        def setup_xlogdb_mock():
            """Wire up the xlogdb context manager mock."""
            backup_manager.server.xlogdb = Mock(
                side_effect=lambda mode: open(wals_dir.join("xlog.db").strpath, mode)
            )

        return {
            "backup_manager": backup_manager,
            "tmpdir": tmpdir,
            "wals_dir": wals_dir,
            "hash_dir": hash_dir,
            "populate": populate,
            "populate_xlogdb_only": populate_xlogdb_only,
            "make_backup_info": make_backup_info,
            "setup_xlogdb_mock": setup_xlogdb_mock,
        }

    @patch.object(BackupManager, "_add_wal_data_to_tar")
    def test_export_backup_success(self, mock_add_wal, tmpdir):
        """
        Test that export_backup creates a tarball with the expected contents.
        """
        # GIVEN a backup manager with a valid backup
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )
        backup_info = build_test_backup_info(
            backup_id="20240101T120000",
            server=backup_manager.server,
        )

        # AND the backup directory exists with some data
        build_backup_directories(backup_info)
        data_dir = backup_info.get_data_directory()
        test_file = os.path.join(data_dir, "test_file.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        backup_info.save()

        # AND identity and barman data are provided
        identity_data = {"systemid": "1234567890"}
        barman_data = {"barman_ver": "3.10.0", "timestamp": "2024-01-01T12:00:00"}

        # AND an output file path is defined
        output_filepath = os.path.join(tmpdir.strpath, "export.tar")

        # WHEN export_backup is called
        backup_manager.export_backup(
            backup_info, output_filepath, identity_data, barman_data
        )

        # THEN the tarball is created
        assert os.path.exists(output_filepath)

        # AND _add_wal_data_to_tar was called
        mock_add_wal.assert_called_once()

        # AND the tarball contains the expected files with correct content
        with tarfile.open(output_filepath, "r") as tar:
            tar_members = tar.getnames()

            # Should contain backup directory with its contents
            assert any(name.startswith("backup/") for name in tar_members)

            # Should contain metadata files
            assert "identity.json" in tar_members
            assert "backup.info" in tar_members
            assert "barman.json" in tar_members

            # Verify identity.json content
            identity_file = tar.extractfile("identity.json")
            identity_content = json.loads(identity_file.read().decode("utf-8"))
            assert identity_content == {"systemid": "1234567890"}

            # Verify barman.json content
            barman_file = tar.extractfile("barman.json")
            barman_content = json.loads(barman_file.read().decode("utf-8"))
            assert barman_content["barman_ver"] == "3.10.0"
            assert barman_content["timestamp"] == "2024-01-01T12:00:00"

    @pytest.mark.parametrize(
        "compression,read_mode",
        [
            (None, "r"),
            ("gzip", "r:gz"),
            ("bzip2", "r:bz2"),
            ("xz", "r:xz"),
        ],
    )
    @patch.object(BackupManager, "_add_wal_data_to_tar")
    def test_export_backup_compression(
        self, mock_add_wal, compression, read_mode, tmpdir
    ):
        """
        Test that export_backup creates a tarball compressed with the specified
        algorithm.
        """
        # GIVEN a backup manager with a valid backup
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )
        backup_info = build_test_backup_info(
            backup_id="20240101T120000",
            server=backup_manager.server,
        )

        # AND the backup directory exists with some data
        build_backup_directories(backup_info)
        data_dir = backup_info.get_data_directory()
        test_file = os.path.join(data_dir, "test_file.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        backup_info.save()

        # AND identity and barman data are provided
        identity_data = {"systemid": "1234567890"}
        barman_data = {"barman_ver": "3.10.0"}

        # AND an output file path is defined
        output_filepath = os.path.join(tmpdir.strpath, "export.tar")

        # WHEN export_backup is called with the specified compression
        backup_manager.export_backup(
            backup_info,
            output_filepath,
            identity_data,
            barman_data,
            compression=compression,
        )

        # THEN the tarball is created
        assert os.path.exists(output_filepath)

        # AND the tarball can be opened with the expected read mode
        with tarfile.open(output_filepath, read_mode) as tar:
            tar_members = tar.getnames()

            # AND contains the expected backup data and metadata
            assert any(name.startswith("backup/") for name in tar_members)
            assert "identity.json" in tar_members
            assert "backup.info" in tar_members
            assert "barman.json" in tar_members

    @pytest.mark.parametrize(
        "compression,read_mode",
        [
            ("gzip", "r:gz"),
            ("bzip2", "r:bz2"),
            ("xz", "r:xz"),
        ],
    )
    @patch.object(BackupManager, "_add_wal_data_to_tar")
    def test_export_backup_compression_level_round_trip(
        self, mock_add_wal, compression, read_mode, tmpdir
    ):
        """
        Test that export_backup produces a valid compressed tarball when a
        non-default compression level is set, by letting tarfile actually run
        and reopening the result. On interpreters that accept the level kwarg
        for the streaming mode, the level is forwarded; on older ones, the
        version gate drops the level and the algorithm default is used. In
        both cases the export must succeed and the tarball must be readable.
        """
        # GIVEN a backup manager with a valid backup
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )
        backup_info = build_test_backup_info(
            backup_id="20240101T120000",
            server=backup_manager.server,
        )

        # AND the backup directory exists with some data
        build_backup_directories(backup_info)
        data_dir = backup_info.get_data_directory()
        test_file = os.path.join(data_dir, "test_file.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        backup_info.save()

        identity_data = {"systemid": "1234567890"}
        barman_data = {"barman_ver": "3.10.0"}
        output_filepath = os.path.join(tmpdir.strpath, "export.tar")

        # WHEN export_backup is called with the chosen algorithm and an
        # explicit non-default compression level
        backup_manager.export_backup(
            backup_info,
            output_filepath,
            identity_data,
            barman_data,
            compression=compression,
            compression_level=1,
        )

        # THEN the tarball exists and can be opened with the matching read
        # mode, regardless of whether the running interpreter honored the
        # level kwarg or fell back to the algorithm's default via the gate
        assert os.path.exists(output_filepath)
        with tarfile.open(output_filepath, read_mode) as tar:
            tar_members = tar.getnames()
            assert any(name.startswith("backup/") for name in tar_members)
            assert "identity.json" in tar_members
            assert "backup.info" in tar_members
            assert "barman.json" in tar_members

    @pytest.mark.parametrize(
        "compression,expected_kwarg",
        [
            ("gzip", "compresslevel"),
            ("bzip2", "compresslevel"),
            ("xz", "preset"),
        ],
    )
    # Simulate an interpreter new enough that all three streaming modes accept
    # their compression-level kwarg (compresslevel for gz/bz2: 3.12+; preset
    # for xz: 3.14+). This keeps the test focused on the kwarg-routing logic
    # independent of the version-gating fallback exercised below.
    @patch("barman.backup.sys.version_info", (3, 99, 0))
    @patch.object(BackupManager, "_add_wal_data_to_tar")
    @patch.object(BackupManager, "_add_metadata_to_tar")
    @patch("barman.backup.tarfile.open")
    def test_export_backup_compression_level(
        self,
        mock_tar_open,
        mock_add_metadata,
        mock_add_wal,
        compression,
        expected_kwarg,
        tmpdir,
    ):
        """
        Test that export_backup forwards the compression level to tarfile.open
        using the correct keyword argument for each algorithm.
        """
        # GIVEN a backup manager
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )

        # AND tarfile.open returns a mock that supports context manager
        mock_tar = mock.MagicMock()
        mock_tar.name = os.path.join(tmpdir.strpath, "export.tar")
        mock_tar_open.return_value.__enter__ = Mock(return_value=mock_tar)
        mock_tar_open.return_value.__exit__ = Mock(return_value=False)

        backup_info = build_test_backup_info(
            backup_id="20240101T120000",
            server=backup_manager.server,
        )

        # AND identity and barman data are provided
        identity_data = {"systemid": "1234567890"}
        barman_data = {"barman_ver": "3.10.0"}

        output_filepath = os.path.join(tmpdir.strpath, "export.tar")

        # WHEN export_backup is called with compression and a compression level
        backup_manager.export_backup(
            backup_info,
            output_filepath,
            identity_data,
            barman_data,
            compression=compression,
            compression_level=5,
        )

        # THEN tarfile.open was called with the correct keyword argument
        call_kwargs = mock_tar_open.call_args[1]
        assert expected_kwarg in call_kwargs
        assert call_kwargs[expected_kwarg] == 5

    @pytest.mark.parametrize(
        "compression,kwarg_name,running_version,required_version",
        [
            # compresslevel for gz/bz2 streaming requires Python 3.12+
            ("gzip", "compresslevel", (3, 11, 0), (3, 12)),
            ("bzip2", "compresslevel", (3, 11, 0), (3, 12)),
            # preset for xz streaming requires Python 3.14+
            ("xz", "preset", (3, 13, 0), (3, 14)),
        ],
    )
    @patch.object(BackupManager, "_add_wal_data_to_tar")
    @patch.object(BackupManager, "_add_metadata_to_tar")
    @patch("barman.backup.tarfile.open")
    @patch("barman.backup.output.warning")
    def test_export_backup_compression_level_ignored_on_old_python(
        self,
        mock_warning,
        mock_tar_open,
        mock_add_metadata,
        mock_add_wal,
        compression,
        kwarg_name,
        running_version,
        required_version,
        tmpdir,
    ):
        """
        Test that on Python versions that do not yet accept the compression
        level kwarg for streaming tar mode, the level is silently dropped and
        a warning is emitted instead of letting tarfile.open raise.
        """
        # GIVEN a backup manager
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )

        # AND tarfile.open returns a mock that supports context manager
        mock_tar = mock.MagicMock()
        mock_tar.name = os.path.join(tmpdir.strpath, "export.tar")
        mock_tar_open.return_value.__enter__ = Mock(return_value=mock_tar)
        mock_tar_open.return_value.__exit__ = Mock(return_value=False)

        backup_info = build_test_backup_info(
            backup_id="20240101T120000",
            server=backup_manager.server,
        )

        identity_data = {"systemid": "1234567890"}
        barman_data = {"barman_ver": "3.10.0"}
        output_filepath = os.path.join(tmpdir.strpath, "export.tar")

        # WHEN export_backup is called on an interpreter older than the one
        # that added kwarg support for the chosen streaming mode
        with patch("barman.backup.sys.version_info", running_version):
            backup_manager.export_backup(
                backup_info,
                output_filepath,
                identity_data,
                barman_data,
                compression=compression,
                compression_level=5,
            )

        # THEN the level kwarg is NOT forwarded to tarfile.open
        call_kwargs = mock_tar_open.call_args[1]
        assert kwarg_name not in call_kwargs

        # AND exactly one warning is emitted naming the kwarg, the running
        # version, and the required version (other unrelated warnings from
        # build_backup_manager are ignored). output.warning uses lazy
        # %-formatting, so render the message before substring checks.
        matching_calls = [
            call for call in mock_warning.call_args_list if kwarg_name in call.args
        ]
        assert len(matching_calls) == 1
        warning_args = matching_calls[0].args
        rendered = warning_args[0] % warning_args[1:]
        assert (
            "Python %d.%d does not support" % (running_version[0], running_version[1])
            in rendered
        )
        assert (
            "Requires Python %d.%d or later"
            % (required_version[0], required_version[1])
            in rendered
        )
        assert "ignoring compression level" in rendered
        assert "using the algorithm's default level" in rendered

    def test_export_backup_integration(self, tmpdir):
        """
        Test end-to-end export_backup without mocking _add_wal_data_to_tar.
        """
        # GIVEN a backup manager with a valid backup
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )
        backup_info = build_test_backup_info(
            backup_id="20240101T120000",
            server=backup_manager.server,
        )

        # AND the backup directory exists with some data
        build_backup_directories(backup_info)
        data_dir = backup_info.get_data_directory()
        test_file = os.path.join(data_dir, "test_file.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        backup_info.save()

        # AND WAL files and xlog.db exist
        wals_dir = tmpdir.mkdir("wals")
        hash_dir = wals_dir.mkdir("0000000100000000")
        wal_files = [
            "000000010000000000000001",
            "000000010000000000000002",
        ]
        xlog_db_content = ""
        for wal in wal_files:
            hash_dir.join(wal).write_binary(os.urandom(16))
            xlog_db_content += f"{wal}\t16\t1712994000.0\tNone\tNone\n"
        wals_dir.join("xlog.db").write(xlog_db_content)

        backup_manager.server.config.wals_directory = wals_dir.strpath
        backup_manager.server.xlogdb = Mock(
            side_effect=lambda mode: open(wals_dir.join("xlog.db").strpath, mode)
        )

        # AND identity and barman data are provided
        identity_data = {"systemid": "1234567890"}
        barman_data = {"barman_ver": "3.10.0", "timestamp": "2024-01-01T12:00:00"}

        # AND an output file path is defined
        output_filepath = os.path.join(tmpdir.strpath, "export.tar")

        # WHEN export_backup is called with patched required WAL segments
        with patch.object(
            type(backup_info),
            "get_required_wal_segments",
            return_value=iter(wal_files),
        ):
            backup_manager.export_backup(
                backup_info, output_filepath, identity_data, barman_data
            )

        # THEN the tarball contains backup data, WAL files, xlog.db, and metadata
        with tarfile.open(output_filepath, "r") as tar:
            members = tar.getnames()

            assert any(name.startswith("backup/") for name in members)
            for wal in wal_files:
                assert f"wals/0000000100000000/{wal}" in members
            assert "xlog.db" in members
            assert "identity.json" in members
            assert "backup.info" in members
            assert "barman.json" in members

    def test_export_backup_metadata_first_ordering(self, tmpdir):
        """
        Test that metadata entries are written at the expected positions
        (identity.json, backup.info, barman.json) at the very start of the
        export tarball, ahead of any bulk backup data or WAL entries.
        """
        # GIVEN a backup manager with a valid backup
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )
        backup_info = build_test_backup_info(
            backup_id="20240101T120000",
            server=backup_manager.server,
        )

        # AND the backup directory exists with some data
        build_backup_directories(backup_info)
        data_dir = backup_info.get_data_directory()
        test_file = os.path.join(data_dir, "test_file.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        backup_info.save()

        # AND WAL files and xlog.db exist
        wals_dir = tmpdir.mkdir("wals")
        hash_dir = wals_dir.mkdir("0000000100000000")
        wal_files = [
            "000000010000000000000001",
            "000000010000000000000002",
        ]
        xlog_db_content = ""
        for wal in wal_files:
            hash_dir.join(wal).write_binary(os.urandom(16))
            xlog_db_content += f"{wal}\t16\t1712994000.0\tNone\tNone\n"
        wals_dir.join("xlog.db").write(xlog_db_content)

        backup_manager.server.config.wals_directory = wals_dir.strpath
        backup_manager.server.xlogdb = Mock(
            side_effect=lambda mode: open(wals_dir.join("xlog.db").strpath, mode)
        )

        # AND identity and barman data are provided
        identity_data = {"systemid": "1234567890"}
        barman_data = {"barman_ver": "3.10.0", "timestamp": "2024-01-01T12:00:00"}

        # AND an output file path is defined
        output_filepath = os.path.join(tmpdir.strpath, "export.tar")

        # WHEN export_backup is called with patched required WAL segments
        with patch.object(
            type(backup_info),
            "get_required_wal_segments",
            return_value=iter(wal_files),
        ):
            backup_manager.export_backup(
                backup_info, output_filepath, identity_data, barman_data
            )

        # THEN metadata entries occupy the first three positions, in order
        with tarfile.open(output_filepath, "r") as tar:
            members = tar.getnames()

            assert members[0] == "identity.json"
            assert members[1] == "backup.info"
            assert members[2] == "barman.json"

            # AND none of the metadata files appear again later in the tarball
            metadata_files = {"identity.json", "backup.info", "barman.json"}
            for name in members[3:]:
                assert name not in metadata_files, (
                    "metadata file '%s' must not appear after the metadata block" % name
                )

    def test_add_wal_data_to_tar(self, wal_env):
        """
        Test that _add_wal_data_to_tar adds WAL files and xlog.db to the tarball,
        preserving hash directory structure and xlog.db entry order.
        """
        # GIVEN WAL files on disk with matching xlog.db entries
        wal_files = [
            "000000010000000000000001",
            "000000010000000000000002",
            "000000010000000000000003",
        ]
        wal_env["populate"]([(w, None) for w in wal_files])
        wal_env["setup_xlogdb_mock"]()
        backup_info = wal_env["make_backup_info"](wal_files)

        # WHEN _add_wal_data_to_tar is called
        tar_path = wal_env["tmpdir"].join("test.tar").strpath
        with tarfile.open(tar_path, "w") as tar:
            wal_env["backup_manager"]._add_wal_data_to_tar(tar, backup_info)

        # THEN the tarball contains all WAL files under wals/<hash_dir>/
        with tarfile.open(tar_path, "r") as tar:
            members = tar.getnames()

            for wal in wal_files:
                expected_path = f"wals/0000000100000000/{wal}"
                assert expected_path in members

            # AND the tarball contains xlog.db
            assert "xlog.db" in members

            # AND xlog.db entries are in order and match the exported WALs
            xlog_db_content = tar.extractfile("xlog.db").read().decode("utf-8")
            lines = [line for line in xlog_db_content.strip().split("\n") if line]
            extracted_wals = [line.split("\t")[0] for line in lines]
            assert extracted_wals == wal_files

    def test_add_wal_data_to_tar_only_includes_required_wals(self, wal_env):
        """
        Test that only required WALs (and their xlog.db entries) are exported,
        even when xlog.db contains additional entries.
        """
        # GIVEN xlog.db and disk contain more WALs than are required
        all_wals = [
            "000000010000000000000001",
            "000000010000000000000002",
            "000000010000000000000003",
            "000000010000000000000004",
            "000000010000000000000005",
        ]
        wal_env["populate"]([(w, None) for w in all_wals])
        wal_env["setup_xlogdb_mock"]()

        # AND only a subset of WALs are required
        required_wals = ["000000010000000000000002", "000000010000000000000003"]
        backup_info = wal_env["make_backup_info"](required_wals)

        # WHEN _add_wal_data_to_tar is called
        tar_path = wal_env["tmpdir"].join("test.tar").strpath
        with tarfile.open(tar_path, "w") as tar:
            wal_env["backup_manager"]._add_wal_data_to_tar(tar, backup_info)

        # THEN xlog.db only contains required WAL entries
        with tarfile.open(tar_path, "r") as tar:
            xlog_db_content = tar.extractfile("xlog.db").read().decode("utf-8")
            lines = [line for line in xlog_db_content.strip().split("\n") if line]

            assert len(lines) == len(required_wals)
            for wal in required_wals:
                assert wal in xlog_db_content

            # AND unrequired WALs are not in xlog.db
            for wal in (
                "000000010000000000000001",
                "000000010000000000000004",
                "000000010000000000000005",
            ):
                assert wal not in xlog_db_content

    def test_add_wal_data_to_tar_preserves_compression_metadata(self, wal_env):
        """
        Test that xlog.db compression metadata is preserved in the export.

        Barman stores WAL files without compression extensions in the filename.
        The compression type is recorded in xlog.db metadata only.
        """
        # GIVEN a mix of compressed and uncompressed WAL files
        wal_configs = [
            ("000000010000000000000001", None),
            ("000000010000000000000002", "gzip"),
            ("000000010000000000000003", None),
        ]
        wal_env["populate"](wal_configs)
        wal_env["setup_xlogdb_mock"]()
        backup_info = wal_env["make_backup_info"]([w for w, _ in wal_configs])

        # WHEN _add_wal_data_to_tar is called
        tar_path = wal_env["tmpdir"].join("test.tar").strpath
        with tarfile.open(tar_path, "w") as tar:
            wal_env["backup_manager"]._add_wal_data_to_tar(tar, backup_info)

        # THEN all WAL files are in the tarball
        with tarfile.open(tar_path, "r") as tar:
            members = tar.getnames()
            for wal, _ in wal_configs:
                assert f"wals/0000000100000000/{wal}" in members

            # AND xlog.db preserves per-WAL compression metadata
            xlog_db_content = tar.extractfile("xlog.db").read().decode("utf-8")
            for line in xlog_db_content.strip().split("\n"):
                parts = line.split("\t")
                wal_name, compression = parts[0], parts[3]
                if wal_name == "000000010000000000000002":
                    assert compression == "gzip"
                else:
                    assert compression == "None"

    def test_add_wal_data_to_tar_missing_wal_file_raises(self, wal_env):
        """
        Test that a WAL file present in xlog.db but missing from disk raises
        ExportBackupException.
        """
        # GIVEN some WAL files exist on disk
        existing_wals = [
            ("000000010000000000000001", None),
            ("000000010000000000000002", None),
        ]
        wal_env["populate"](existing_wals)

        # AND xlog.db also has an entry for a WAL file that doesn't exist on disk
        all_wals = existing_wals + [("000000010000000000000003", None)]
        wal_env["populate_xlogdb_only"](all_wals)
        wal_env["setup_xlogdb_mock"]()

        backup_info = wal_env["make_backup_info"]([w for w, _ in all_wals])

        # WHEN _add_wal_data_to_tar is called
        # THEN an ExportBackupException is raised mentioning the missing WAL
        tar_path = wal_env["tmpdir"].join("test.tar").strpath
        with tarfile.open(tar_path, "w") as tar:
            with pytest.raises(ExportBackupException) as exc_info:
                wal_env["backup_manager"]._add_wal_data_to_tar(tar, backup_info)

            assert "not found for backup" in str(exc_info.value)
            assert "000000010000000000000003" in str(exc_info.value)

    def test_add_wal_data_to_tar_wal_not_in_xlogdb_raises(self, wal_env):
        """
        Test that a required WAL segment missing from xlog.db raises
        ExportBackupException.
        """
        # GIVEN WAL files exist on disk
        wal_files = ["000000010000000000000001", "000000010000000000000002"]
        wal_env["populate"]([(w, None) for w in wal_files])

        # AND xlog.db only contains the first WAL
        wal_env["populate_xlogdb_only"]([("000000010000000000000001", None)])
        wal_env["setup_xlogdb_mock"]()

        backup_info = wal_env["make_backup_info"](wal_files)

        # WHEN _add_wal_data_to_tar is called
        # THEN an ExportBackupException is raised mentioning the missing WAL
        tar_path = wal_env["tmpdir"].join("test.tar").strpath
        with tarfile.open(tar_path, "w") as tar:
            with pytest.raises(ExportBackupException) as exc_info:
                wal_env["backup_manager"]._add_wal_data_to_tar(tar, backup_info)

            assert "not found in xlog.db" in str(exc_info.value)
            assert "000000010000000000000002" in str(exc_info.value)

    def test_add_json_to_tar(self, tmpdir):
        """
        Test that _add_json_to_tar creates a valid JSON file in the tarball.
        """
        # GIVEN a backup manager
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )

        # AND a JSON string
        json_data = '{"key": "value", "number": 42}'

        # WHEN _add_json_to_tar is called
        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
            backup_manager._add_json_to_tar(tar, "data.json", json_data)

        # THEN the file is in the tarball with correct JSON content
        tar_buffer.seek(0)
        with tarfile.open(fileobj=tar_buffer, mode="r") as tar:
            assert "data.json" in tar.getnames()
            content = tar.extractfile("data.json").read().decode("utf-8")
            parsed = json.loads(content)
            assert parsed == {"key": "value", "number": 42}

    def test_add_metadata_to_tar(self, tmpdir):
        """
        Test that _add_metadata_to_tar adds all metadata files correctly.
        """
        # GIVEN a backup manager
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )
        backup_info = build_test_backup_info(
            backup_id="20240101T120000",
            server=backup_manager.server,
        )
        build_backup_directories(backup_info)
        backup_info.save()

        identity_data = {"systemid": "1234567890", "version": "15"}
        barman_data = {"barman_ver": "3.10.0"}

        # WHEN _add_metadata_to_tar is called
        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
            backup_manager._add_metadata_to_tar(
                tar, identity_data, backup_info, barman_data
            )

        # THEN the tarball contains all metadata files
        tar_buffer.seek(0)
        with tarfile.open(fileobj=tar_buffer, mode="r") as tar:
            members = tar.getnames()
            assert "identity.json" in members
            assert "backup.info" in members
            assert "barman.json" in members

            # AND identity.json has correct content
            identity_content = json.loads(
                tar.extractfile("identity.json").read().decode("utf-8")
            )
            assert identity_content["systemid"] == "1234567890"
            assert identity_content["version"] == "15"

            # AND barman.json has correct content
            barman_content = json.loads(
                tar.extractfile("barman.json").read().decode("utf-8")
            )
            assert barman_content["barman_ver"] == "3.10.0"


class TestImportBackup(object):
    """Test class for BackupManager.import_backup and related helper methods."""

    @pytest.fixture
    def import_env(self, tmpdir):
        """
        Set up a backup manager and helper to create valid export tarballs.

        Returns a dict with ``backup_manager``, ``tmpdir``, and a
        ``make_tarball`` helper.
        """
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )
        # Ensure basebackups_directory exists on disk
        os.makedirs(backup_manager.config.basebackups_directory, exist_ok=True)

        def make_tarball(
            identity=None,
            backup_info_content=None,
            include_backup_dir=True,
            include_wals=True,
            wal_configs=None,
        ):
            """
            Create a valid export tarball on disk.

            :param dict identity: identity.json content
            :param str backup_info_content: raw backup.info content
            :param bool include_backup_dir: whether to include backup/ directory
            :param bool include_wals: whether to include wals/ dir and xlog.db
            :param list|None wal_configs: list of (wal_name, compression) tuples.
                Defaults to WALs matching begin_wal..end_wal in backup_info_content.
            :return: path to the created tarball
            :rtype: str
            """
            if identity is None:
                identity = {"systemid": "1234567890", "version": "15"}
            if backup_info_content is None:
                # Match production: ``backup_id`` is not a ``Field`` on
                # ``BackupInfo``, so ``BackupInfo.save()`` does not write it
                # to the file. A real exported ``backup.info`` therefore has
                # no ``backup_id=`` line.
                backup_info_content = (
                    "server_name=TestServer\n"
                    "status=DONE\n"
                    "begin_wal=000000010000000000000001\n"
                    "end_wal=000000010000000000000002\n"
                )
            if wal_configs is None:
                wal_configs = [
                    ("000000010000000000000001", None),
                    ("000000010000000000000002", None),
                ]

            input_tarball = os.path.join(tmpdir.strpath, "export.tar")
            with tarfile.open(input_tarball, "w") as tar:
                # Add identity.json
                identity_bytes = json.dumps(identity).encode("utf-8")
                info = tarfile.TarInfo(name="identity.json")
                info.size = len(identity_bytes)
                tar.addfile(info, io.BytesIO(identity_bytes))

                # Add backup.info
                info_bytes = backup_info_content.encode("utf-8")
                info = tarfile.TarInfo(name="backup.info")
                info.size = len(info_bytes)
                tar.addfile(info, io.BytesIO(info_bytes))

                # Add backup/ directory with a test file
                if include_backup_dir:
                    data_content = b"test data"
                    info = tarfile.TarInfo(name="backup/data/test_file.txt")
                    info.size = len(data_content)
                    tar.addfile(info, io.BytesIO(data_content))

                # Add wals/ directory and xlog.db
                if include_wals:
                    xlogdb_content = ""
                    for wal_name, compression in wal_configs:
                        # Create WAL file entry in wals/<hash_dir>/<name>
                        from barman import xlog as xlog_mod

                        hash_subdir = xlog_mod.hash_dir(wal_name)
                        wal_data = os.urandom(16)
                        wal_path = "wals/%s/%s" % (hash_subdir, wal_name)
                        info = tarfile.TarInfo(name=wal_path)
                        info.size = len(wal_data)
                        tar.addfile(info, io.BytesIO(wal_data))
                        # Build xlog.db line
                        comp_str = compression if compression else "None"
                        xlogdb_content += "%s\t%d\t1712994000.0\t%s\tNone\n" % (
                            wal_name,
                            len(wal_data),
                            comp_str,
                        )

                    # Add xlog.db
                    xlogdb_bytes = xlogdb_content.encode("utf-8")
                    info = tarfile.TarInfo(name="xlog.db")
                    info.size = len(xlogdb_bytes)
                    tar.addfile(info, io.BytesIO(xlogdb_bytes))

            return input_tarball

        # Set up a real wals_directory and xlogdb for WAL import operations
        wals_dir = tmpdir.mkdir("wals")
        backup_manager.server.config.wals_directory = wals_dir.strpath
        xlogdb_file = wals_dir.join("xlog.db")
        xlogdb_file.write("")
        backup_manager.server.xlogdb_file_path = xlogdb_file.strpath

        @contextmanager
        def _xlogdb_ctx(mode="r"):
            with open(xlogdb_file.strpath, mode) as f:
                yield f

        backup_manager.server.xlogdb = _xlogdb_ctx
        backup_manager.server.rebuild_xlogdb = Mock()

        return {
            "backup_manager": backup_manager,
            "tmpdir": tmpdir,
            "make_tarball": make_tarball,
            "wals_dir": wals_dir,
            "xlogdb_file": xlogdb_file,
        }

    def test_import_backup_success(self, import_env):
        """
        Test that import_backup extracts tarball, validates identity, registers
        metadata, and moves backup data to the correct location.
        """
        # GIVEN a valid export tarball
        backup_manager = import_env["backup_manager"]
        input_tarball = import_env["make_tarball"]()
        local_identity = {"systemid": "1234567890", "version": "15"}

        # AND the meta directory exists
        os.makedirs(backup_manager.server.meta_directory, exist_ok=True)

        # WHEN import_backup is called
        backup_manager.import_backup(input_tarball, local_identity, "20240101T120000")

        # THEN the backup is registered in the cache
        backup_info = backup_manager.get_backup("20240101T120000")
        assert backup_info is not None
        assert backup_info.backup_id == "20240101T120000"

        # AND the backup data directory exists with the test file
        data_dir = backup_info.get_basebackup_directory()
        assert os.path.isdir(data_dir)
        assert os.path.exists(os.path.join(data_dir, "data", "test_file.txt"))

        # AND no staging directories are left behind
        base_dir = backup_manager.config.basebackups_directory
        staging_dirs = [d for d in os.listdir(base_dir) if d.startswith(".import-")]
        assert len(staging_dirs) == 0

        # AND the backup is marked as KEEP:STANDALONE
        assert backup_manager.should_keep_backup("20240101T120000") is True
        assert (
            backup_manager.get_keep_target("20240101T120000")
            == KeepManager.TARGET_STANDALONE
        )

    def test_import_backup_identity_mismatch(self, import_env):
        """
        Test that import raises ImportBackupException when systemid mismatches.
        """
        # GIVEN a tarball with systemid "9999999999"
        backup_manager = import_env["backup_manager"]
        input_tarball = import_env["make_tarball"](
            identity={"systemid": "9999999999", "version": "15"}
        )

        # AND a local identity with a different systemid
        local_identity = {"systemid": "1234567890", "version": "15"}

        # WHEN import_backup is called
        # THEN an ImportBackupException is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager.import_backup(
                input_tarball, local_identity, "20240101T120000"
            )

        assert "identity mismatch" in str(exc_info.value).lower()
        assert "9999999999" in str(exc_info.value)
        assert "1234567890" in str(exc_info.value)

        # AND no staging directories are left behind
        base_dir = backup_manager.config.basebackups_directory
        staging_dirs = [d for d in os.listdir(base_dir) if d.startswith(".import-")]
        assert len(staging_dirs) == 0

    def test_import_backup_version_mismatch_warns(self, import_env, capsys):
        """
        Test that a PostgreSQL version mismatch logs a warning but does not
        block the import.
        """
        # GIVEN a tarball with PG version "15" and local server with version "16"
        backup_manager = import_env["backup_manager"]
        input_tarball = import_env["make_tarball"](
            identity={"systemid": "1234567890", "version": "15"}
        )
        local_identity = {"systemid": "1234567890", "version": "16"}

        # AND the meta directory exists
        os.makedirs(backup_manager.server.meta_directory, exist_ok=True)

        # WHEN import_backup is called
        backup_manager.import_backup(input_tarball, local_identity, "20240101T120000")

        # THEN a warning about version mismatch is emitted
        out, err = capsys.readouterr()
        assert "version mismatch" in err.lower()
        assert "15" in err
        assert "16" in err

        # AND the import still succeeds
        backup_info = backup_manager.get_backup("20240101T120000")
        assert backup_info is not None

    def test_import_backup_missing_identity_json(self, import_env):
        """
        Test that import raises ImportBackupException when identity.json is
        missing from the tarball.
        """
        # GIVEN a tarball without identity.json
        tmpdir = import_env["tmpdir"]
        input_tarball = os.path.join(tmpdir.strpath, "no_identity.tar")
        with tarfile.open(input_tarball, "w") as tar:
            data = b"backup_id=20240101T120000\nserver_name=TestServer\n"
            info = tarfile.TarInfo(name="backup.info")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

        backup_manager = import_env["backup_manager"]
        local_identity = {"systemid": "1234567890"}

        # WHEN import_backup is called
        # THEN an ImportBackupException is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager.import_backup(
                input_tarball, local_identity, "20240101T120000"
            )

        assert "identity.json" in str(exc_info.value)

    def test_import_backup_missing_backup_info(self, import_env):
        """
        Test that import raises ImportBackupException when backup.info is
        missing from the tarball.
        """
        # GIVEN a tarball with identity.json and a backup/ directory but no
        # backup.info (so the backup.info check is the one that fires)
        tmpdir = import_env["tmpdir"]
        input_tarball = os.path.join(tmpdir.strpath, "no_backup_info.tar")
        with tarfile.open(input_tarball, "w") as tar:
            identity = json.dumps({"systemid": "1234567890", "version": "15"}).encode(
                "utf-8"
            )
            info = tarfile.TarInfo(name="identity.json")
            info.size = len(identity)
            tar.addfile(info, io.BytesIO(identity))

            data_content = b"test data"
            info = tarfile.TarInfo(name="backup/data/test_file.txt")
            info.size = len(data_content)
            tar.addfile(info, io.BytesIO(data_content))

        backup_manager = import_env["backup_manager"]
        local_identity = {"systemid": "1234567890", "version": "15"}

        # WHEN import_backup is called
        # THEN an ImportBackupException is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager.import_backup(
                input_tarball, local_identity, "20240101T120000"
            )

        assert "backup.info" in str(exc_info.value)

    def test_import_backup_missing_backup_directory(self, import_env):
        """
        Test that import raises ImportBackupException when the backup/
        directory is missing from the tarball.
        """
        # GIVEN a tarball without backup/ directory
        backup_manager = import_env["backup_manager"]
        input_tarball = import_env["make_tarball"](include_backup_dir=False)
        local_identity = {"systemid": "1234567890", "version": "15"}

        # AND the meta directory exists
        os.makedirs(backup_manager.server.meta_directory, exist_ok=True)

        # WHEN import_backup is called
        # THEN an ImportBackupException is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager.import_backup(
                input_tarball, local_identity, "20240101T120000"
            )

        assert "'backup/' directory" in str(exc_info.value)

        # AND the catalog is left clean: no meta file, no cache entry,
        # no target data directory, no staging directory
        meta_info_path = os.path.join(
            backup_manager.server.meta_directory, "20240101T120000-backup.info"
        )
        assert not os.path.exists(meta_info_path)
        assert backup_manager.get_backup("20240101T120000") is None

        target_dir = os.path.join(
            backup_manager.config.basebackups_directory, "20240101T120000"
        )
        assert not os.path.exists(target_dir)

        base_dir = backup_manager.config.basebackups_directory
        staging_dirs = [d for d in os.listdir(base_dir) if d.startswith(".import-")]
        assert len(staging_dirs) == 0

    def test_import_backup_duplicate_backup_id(self, import_env):
        """
        Test that import raises ImportBackupException when backup_id already
        exists in the catalog.
        """
        # GIVEN a valid export tarball
        backup_manager = import_env["backup_manager"]
        input_tarball = import_env["make_tarball"]()
        local_identity = {"systemid": "1234567890", "version": "15"}

        # AND the meta directory exists
        os.makedirs(backup_manager.server.meta_directory, exist_ok=True)

        # AND a backup with the same ID already exists in the catalog
        existing_backup = build_test_backup_info(
            backup_id="20240101T120000",
            server=backup_manager.server,
        )
        backup_manager.backup_cache_add(existing_backup)

        # WHEN import_backup is called
        # THEN an ImportBackupException is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager.import_backup(
                input_tarball, local_identity, "20240101T120000"
            )

        assert "already exists" in str(exc_info.value)
        assert "20240101T120000" in str(exc_info.value)

    def test_import_backup_incremental_refused(self, import_env):
        """
        Test that import_backup refuses block-level incremental backups
        (defense-in-depth) before any catalog state is mutated.
        """
        # GIVEN a tarball whose embedded backup.info advertises a parent
        # backup, i.e. it is a PostgreSQL block-level incremental backup
        backup_manager = import_env["backup_manager"]
        backup_info_content = (
            "server_name=TestServer\n"
            "status=DONE\n"
            "begin_wal=000000010000000000000001\n"
            "end_wal=000000010000000000000002\n"
            "parent_backup_id=20231231T120000\n"
        )
        input_tarball = import_env["make_tarball"](
            backup_info_content=backup_info_content
        )
        local_identity = {"systemid": "1234567890", "version": "15"}

        # AND the meta directory exists
        os.makedirs(backup_manager.server.meta_directory, exist_ok=True)

        # WHEN import_backup is called
        # THEN an ImportBackupException naming the operation and the
        # parent-chain reason is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager.import_backup(
                input_tarball, local_identity, "20240101T120000"
            )

        assert "incremental backup" in str(exc_info.value)
        assert "20240101T120000" in str(exc_info.value)
        assert "Only full backups are eligible for importing." in str(exc_info.value)

        # AND no catalog state has been mutated: the backup is not
        # registered, no basebackup directory exists for it, and the
        # staging directory has been cleaned up.
        assert backup_manager.get_backup("20240101T120000") is None
        target_dir = os.path.join(
            backup_manager.config.basebackups_directory, "20240101T120000"
        )
        assert not os.path.exists(target_dir)
        base_dir = backup_manager.config.basebackups_directory
        staging_dirs = [d for d in os.listdir(base_dir) if d.startswith(".import-")]
        assert len(staging_dirs) == 0

    def test_import_backup_cleanup_on_failure(self, import_env):
        """
        Test that staging directory is cleaned up on any failure that occurs
        after the staging directory has been created.
        """
        # GIVEN a tarball with valid identity but no backup/ directory
        # (fails after extraction, when staging dir already exists)
        backup_manager = import_env["backup_manager"]
        input_tarball = import_env["make_tarball"](include_backup_dir=False)
        local_identity = {"systemid": "1234567890", "version": "15"}

        # AND the meta directory exists
        os.makedirs(backup_manager.server.meta_directory, exist_ok=True)

        # WHEN import_backup is called and fails
        with pytest.raises(ImportBackupException):
            backup_manager.import_backup(
                input_tarball, local_identity, "20240101T120000"
            )

        # THEN no staging directories are left in basebackups_directory
        base_dir = backup_manager.config.basebackups_directory
        staging_dirs = [d for d in os.listdir(base_dir) if d.startswith(".import-")]
        assert len(staging_dirs) == 0

    def test_import_backup_path_traversal_rejected(self, import_env):
        """
        Test that a tarball with path traversal entries is rejected.
        """
        # GIVEN a tarball containing a valid identity.json but also a path
        # traversal entry — identity validation passes, but extraction
        # should be rejected.
        tmpdir = import_env["tmpdir"]
        input_tarball = os.path.join(tmpdir.strpath, "evil.tar")
        with tarfile.open(input_tarball, "w") as tar:
            # Add valid identity.json so we pass identity validation
            identity = json.dumps({"systemid": "1234567890", "version": "15"}).encode(
                "utf-8"
            )
            info = tarfile.TarInfo(name="identity.json")
            info.size = len(identity)
            tar.addfile(info, io.BytesIO(identity))

            # Add path traversal entry
            data = b"malicious content"
            info = tarfile.TarInfo(name="../../../etc/passwd")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

        backup_manager = import_env["backup_manager"]
        local_identity = {"systemid": "1234567890", "version": "15"}

        # WHEN import_backup is called
        # THEN an ImportBackupException is raised mentioning unsafe path
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager.import_backup(
                input_tarball, local_identity, "20240101T120000"
            )

        assert "unsafe path" in str(exc_info.value).lower()

    def test_extract_tarball_rejects_sibling_prefix_collision(self, import_env):
        """
        Test that the path-traversal check uses path-prefix (not string-prefix)
        semantics. A sibling directory whose name string-starts-with the
        staging dir's name (e.g. ``staging`` vs ``stagingX``) must not slip
        through the check.

        This case is exercised directly against ``_extract_tarball`` so the
        staging dir name is predictable (the random suffix from
        ``tempfile.mkdtemp`` would make it impossible to reliably construct
        a sibling whose name collides as a string prefix).
        """
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]

        # GIVEN a staging dir
        staging_dir = tmpdir.mkdir("staging").strpath

        # AND a tarball entry that resolves to a sibling whose name starts
        # with the staging dir's name as a string (../stagingX/file.txt).
        # The resolved path "<tmpdir>/stagingX/file.txt" string-starts-with
        # "<tmpdir>/staging", but is not under "<tmpdir>/staging/".
        input_tarball = os.path.join(tmpdir.strpath, "evil.tar")
        with tarfile.open(input_tarball, "w") as tar:
            data = b"malicious content"
            info = tarfile.TarInfo(name="../stagingX/file.txt")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

        # WHEN _extract_tarball is called
        # THEN it raises ImportBackupException mentioning the unsafe path
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager._extract_tarball(input_tarball, staging_dir)

        assert "unsafe path" in str(exc_info.value).lower()

    @pytest.mark.parametrize(
        "filename, mode",
        [
            ("export.tar", "w"),
            ("export.tar.gz", "w:gz"),
            ("export.tar.bz2", "w:bz2"),
            ("export.tar.xz", "w:xz"),
        ],
    )
    def test_extract_tarball_supports_compressed_formats(
        self, import_env, filename, mode
    ):
        """
        Test that ``_extract_tarball`` transparently handles the compressed
        tarball formats produced by ``export-backup`` (gzip, bzip2, xz) in
        addition to plain uncompressed tar.
        """
        # GIVEN a tarball written using the given compression mode, holding
        # a regular file and a directory entry
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        input_tarball = os.path.join(tmpdir.strpath, filename)
        with tarfile.open(input_tarball, mode) as tar:
            data = b"hello world"
            info = tarfile.TarInfo(name="dir/file.txt")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

        # AND a freshly created staging directory
        staging_dir = tmpdir.mkdir("staging-" + filename.replace(".", "_")).strpath

        # WHEN _extract_tarball is called
        backup_manager._extract_tarball(input_tarball, staging_dir)

        # THEN the file is extracted with its original contents
        extracted = os.path.join(staging_dir, "dir", "file.txt")
        assert os.path.isfile(extracted)
        with open(extracted, "rb") as fh:
            assert fh.read() == b"hello world"

    def test_import_backup_unsafe_member_type_rejected(self, import_env):
        """
        Test that a tarball containing a non-regular-file/non-directory entry
        (symlink, hardlink, device, fifo, etc.) is rejected, even if the path
        would be safe.
        """
        # GIVEN a tarball with valid identity.json followed by a symlink entry
        tmpdir = import_env["tmpdir"]
        input_tarball = os.path.join(tmpdir.strpath, "with_symlink.tar")
        with tarfile.open(input_tarball, "w") as tar:
            # Add valid identity.json so we pass identity validation
            identity = json.dumps({"systemid": "1234567890", "version": "15"}).encode(
                "utf-8"
            )
            info = tarfile.TarInfo(name="identity.json")
            info.size = len(identity)
            tar.addfile(info, io.BytesIO(identity))

            # Add a symlink entry pointing somewhere outside (or anywhere —
            # the type itself is the problem, not the target)
            symlink_info = tarfile.TarInfo(name="evil_link")
            symlink_info.type = tarfile.SYMTYPE
            symlink_info.linkname = "/etc/passwd"
            tar.addfile(symlink_info)

        backup_manager = import_env["backup_manager"]
        local_identity = {"systemid": "1234567890", "version": "15"}

        # WHEN import_backup is called
        # THEN an ImportBackupException is raised mentioning unsafe member type
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager.import_backup(
                input_tarball, local_identity, "20240101T120000"
            )

        assert "unsafe member type" in str(exc_info.value).lower()
        assert "evil_link" in str(exc_info.value)

    def test_extract_tarball_skips_pg_tblspc_symlinks(self, import_env):
        """
        Test that symlinks directly under ``backup/data/pg_tblspc/`` are
        skipped during extraction rather than being rejected.

        ``pg_basebackup`` emits tablespace entries as symlinks at exactly this
        path.  They are skipped here and recreated by
        ``_reconstruct_tablespace_symlinks`` with correct destination paths.
        """
        # GIVEN a staging directory
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        staging_dir = tmpdir.mkdir("staging_tblspc").strpath

        # AND a tarball containing a pg_tblspc symlink (OID 16384)
        input_tarball = os.path.join(tmpdir.strpath, "with_tblspc_symlink.tar")
        with tarfile.open(input_tarball, "w") as tar:
            data = b"test"
            info = tarfile.TarInfo(name="backup/data/test_file.txt")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

            symlink_info = tarfile.TarInfo(name="backup/data/pg_tblspc/16384")
            symlink_info.type = tarfile.SYMTYPE
            symlink_info.linkname = "/var/lib/barman/server/base/backup_id/16384"
            tar.addfile(symlink_info)

        # WHEN _extract_tarball is called
        # THEN it succeeds (no ImportBackupException)
        backup_manager._extract_tarball(input_tarball, staging_dir)

        # AND the regular file was extracted
        assert os.path.isfile(
            os.path.join(staging_dir, "backup", "data", "test_file.txt")
        )

        # AND the symlink was NOT extracted (will be reconstructed later)
        assert not os.path.lexists(
            os.path.join(staging_dir, "backup", "data", "pg_tblspc", "16384")
        )

    def test_extract_tarball_rejects_nested_pg_tblspc_symlink(self, import_env):
        """
        Test that a symlink nested deeper under ``pg_tblspc/`` (e.g.
        ``backup/data/pg_tblspc/16384/something``) is still rejected.

        Only the direct OID entry is legitimate; anything nested inside it
        could be a path-traversal attempt.
        """
        # GIVEN a staging directory
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        staging_dir = tmpdir.mkdir("staging_nested").strpath

        # AND a tarball with a symlink nested under pg_tblspc/<oid>/
        input_tarball = os.path.join(tmpdir.strpath, "nested_tblspc_symlink.tar")
        with tarfile.open(input_tarball, "w") as tar:
            symlink_info = tarfile.TarInfo(name="backup/data/pg_tblspc/16384/something")
            symlink_info.type = tarfile.SYMTYPE
            symlink_info.linkname = "/etc/passwd"
            tar.addfile(symlink_info)

        # WHEN _extract_tarball is called
        # THEN an ImportBackupException is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager._extract_tarball(input_tarball, staging_dir)

        assert "unsafe member type" in str(exc_info.value).lower()

    def test_reconstruct_tablespace_symlinks_creates_links(self, import_env):
        """
        Test that ``_reconstruct_tablespace_symlinks`` creates a symlink
        under ``staging/backup/data/pg_tblspc/<oid>`` pointing to
        ``backup_info.get_data_directory(oid)`` for each tablespace.
        """
        # GIVEN a staging directory with the expected backup/data/pg_tblspc/ tree
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        staging_dir = tmpdir.mkdir("staging_reconstruct").strpath
        pg_tblspc_dir = os.path.join(staging_dir, "backup", "data", "pg_tblspc")
        os.makedirs(pg_tblspc_dir)

        # AND a backup_info populated with one tablespace (OID 16384)

        backup_info = LocalBackupInfo(
            backup_manager.server, backup_id="20240101T120000"
        )
        backup_info.tablespaces = [Tablespace(oid=16384, name="mytbs", location="/tbs")]
        backup_info.backup_version = 2

        # WHEN _reconstruct_tablespace_symlinks is called
        backup_manager._reconstruct_tablespace_symlinks(staging_dir, backup_info)

        # THEN a symlink exists at staging/backup/data/pg_tblspc/16384
        link_path = os.path.join(pg_tblspc_dir, "16384")
        assert os.path.islink(link_path)

        # AND it points to backup_info.get_data_directory(16384)
        expected_target = backup_info.get_data_directory(16384)
        assert os.readlink(link_path) == expected_target

    def test_reconstruct_tablespace_symlinks_noop_when_no_tablespaces(self, import_env):
        """
        Test that ``_reconstruct_tablespace_symlinks`` is a no-op when
        ``backup_info.tablespaces`` is None or empty.
        """
        # GIVEN a staging directory without any pg_tblspc subtree
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        staging_dir = tmpdir.mkdir("staging_no_tbs").strpath

        # AND a backup_info with no tablespaces

        backup_info = LocalBackupInfo(
            backup_manager.server, backup_id="20240101T120000"
        )
        backup_info.tablespaces = None

        # WHEN _reconstruct_tablespace_symlinks is called
        # THEN it returns without error and creates no pg_tblspc directory
        backup_manager._reconstruct_tablespace_symlinks(staging_dir, backup_info)

        assert not os.path.exists(
            os.path.join(staging_dir, "backup", "data", "pg_tblspc")
        )

    def test_reconstruct_tablespace_symlinks_raises_on_existing_directory(
        self, import_env
    ):
        """
        Test that ``_reconstruct_tablespace_symlinks`` raises
        ``ImportBackupException`` when a real directory (not a symlink) already
        exists at the expected link path.
        """
        # GIVEN a staging directory with a real directory at the oid path
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        staging_dir = tmpdir.mkdir("staging_dir_conflict").strpath
        pg_tblspc_dir = os.path.join(staging_dir, "backup", "data", "pg_tblspc")
        os.makedirs(os.path.join(pg_tblspc_dir, "16384"))

        # AND a backup_info populated with that tablespace OID

        backup_info = LocalBackupInfo(
            backup_manager.server, backup_id="20240101T120000"
        )
        backup_info.tablespaces = [Tablespace(oid=16384, name="mytbs", location="/tbs")]
        backup_info.backup_version = 2

        # WHEN _reconstruct_tablespace_symlinks is called
        # THEN it raises ImportBackupException
        with pytest.raises(ImportBackupException, match="Cannot replace existing"):
            backup_manager._reconstruct_tablespace_symlinks(staging_dir, backup_info)

    def test_import_backup_with_tablespaces(self, import_env):
        """
        Test that ``import_backup`` succeeds end-to-end when the exported
        tarball contains a pg_tblspc symlink for a custom tablespace.

        Verifies that after import the catalog contains a symlink at
        ``<basebackup>/data/pg_tblspc/<oid>`` pointing to the correct
        destination-catalog tablespace directory.
        """
        # GIVEN a backup_info content that declares one tablespace (OID 16384).
        # Note: backup_version is a runtime attribute detected from the directory
        # structure, not a Field written to backup.info.  Omitting it here
        # ensures the backup_info loaded from staging correctly defaults to 2.
        backup_info_content = (
            "server_name=TestServer\n"
            "status=DONE\n"
            "begin_wal=000000010000000000000001\n"
            "end_wal=000000010000000000000002\n"
            "tablespaces=[('mytbs', 16384, '/original/tbs')]\n"
        )

        backup_manager = import_env["backup_manager"]
        tmpdir = import_env["tmpdir"]

        # AND a tarball that includes the pg_tblspc symlink
        input_tarball = os.path.join(tmpdir.strpath, "tblspc_export.tar")
        with tarfile.open(input_tarball, "w") as tar:
            # identity.json
            identity = json.dumps({"systemid": "1234567890", "version": "15"}).encode(
                "utf-8"
            )
            info = tarfile.TarInfo(name="identity.json")
            info.size = len(identity)
            tar.addfile(info, io.BytesIO(identity))

            # backup.info
            info_bytes = backup_info_content.encode("utf-8")
            info = tarfile.TarInfo(name="backup.info")
            info.size = len(info_bytes)
            tar.addfile(info, io.BytesIO(info_bytes))

            # backup/data/test_file.txt
            data = b"pgdata content"
            info = tarfile.TarInfo(name="backup/data/test_file.txt")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

            # backup/data/pg_tblspc/16384 (symlink — as emitted by pg_basebackup)
            sym = tarfile.TarInfo(name="backup/data/pg_tblspc/16384")
            sym.type = tarfile.SYMTYPE
            sym.linkname = "/var/lib/barman/source/base/20240101T120000/16384"
            tar.addfile(sym)

            # wals and xlog.db
            for wal in ["000000010000000000000001", "000000010000000000000002"]:
                from barman import xlog as xlog_mod

                wal_data = os.urandom(16)
                wal_path = "wals/%s/%s" % (xlog_mod.hash_dir(wal), wal)
                info = tarfile.TarInfo(name=wal_path)
                info.size = len(wal_data)
                tar.addfile(info, io.BytesIO(wal_data))
            xlogdb = (
                "000000010000000000000001\t16\t1712994000.0\tNone\tNone\n"
                "000000010000000000000002\t16\t1712994000.0\tNone\tNone\n"
            ).encode("utf-8")
            info = tarfile.TarInfo(name="xlog.db")
            info.size = len(xlogdb)
            tar.addfile(info, io.BytesIO(xlogdb))

        local_identity = {"systemid": "1234567890", "version": "15"}

        # WHEN import_backup is called
        backup_manager.import_backup(input_tarball, local_identity, "20240101T120000")

        # THEN a symlink exists at <basebackup>/data/pg_tblspc/16384.
        # With backup_version auto-detected as 2 from the imported directory
        # layout, PGDATA lives under "data/" and tablespace dirs under
        # "<oid>/".  Use explicit paths so the assertion is independent of the
        # backup_version auto-detection logic in LocalBackupInfo.__init__.
        base = os.path.join(
            backup_manager.config.basebackups_directory, "20240101T120000"
        )
        pg_tblspc_link = os.path.join(base, "data", "pg_tblspc", "16384")
        assert os.path.islink(pg_tblspc_link), "Expected symlink at %s" % pg_tblspc_link

        # AND the symlink points to the destination-catalog tablespace directory
        # (i.e. <basebackup>/<oid>/, where tablespace data lives for v2 backups)
        expected_target = os.path.join(base, "16384")
        assert os.readlink(pg_tblspc_link) == expected_target

    def test_import_backup_invalid_tarball(self, import_env):
        """
        Test that a non-tar file raises ImportBackupException.
        """
        # GIVEN a file that is not a valid tarball
        tmpdir = import_env["tmpdir"]
        not_a_tarball = os.path.join(tmpdir.strpath, "not_a_tarball.tar")
        with open(not_a_tarball, "w") as f:
            f.write("this is not a tarball")

        backup_manager = import_env["backup_manager"]
        local_identity = {"systemid": "1234567890"}

        # WHEN import_backup is called
        # THEN an ImportBackupException is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager.import_backup(
                not_a_tarball, local_identity, "20240101T120000"
            )

        assert "Failed to read identity.json from tarball" in str(exc_info.value)

    def test_import_backup_metadata_failure_rolls_back_data(self, import_env):
        """
        Test that when _import_backup_metadata fails (e.g. corrupt
        backup.info), the already-moved data directory is removed and
        no orphan state is left behind.
        """
        # GIVEN a tarball with valid identity and backup/ directory but
        # a backup.info that cannot be parsed by LocalBackupInfo
        backup_manager = import_env["backup_manager"]
        input_tarball = import_env["make_tarball"](
            backup_info_content="this is not valid backup info content\n"
        )
        local_identity = {"systemid": "1234567890", "version": "15"}

        # AND the meta directory exists
        os.makedirs(backup_manager.server.meta_directory, exist_ok=True)

        # WHEN import_backup is called
        # THEN an ImportBackupException is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager.import_backup(
                input_tarball, local_identity, "20240101T120000"
            )

        assert "Failed to load backup.info" in str(exc_info.value)

        # AND the data directory that was already moved is rolled back
        target_dir = os.path.join(
            backup_manager.config.basebackups_directory, "20240101T120000"
        )
        assert not os.path.exists(target_dir)

        # AND no staging directories are left behind
        base_dir = backup_manager.config.basebackups_directory
        staging_dirs = [d for d in os.listdir(base_dir) if d.startswith(".import-")]
        assert len(staging_dirs) == 0

        # AND no backup is registered in the cache
        assert backup_manager.get_backup("20240101T120000") is None

    def test_read_identity_from_tarball_invalid_json(self, import_env):
        """
        Test that _read_identity_from_tarball raises ImportBackupException
        when identity.json contains invalid JSON.
        """
        # GIVEN a tarball with an identity.json that is not valid JSON
        tmpdir = import_env["tmpdir"]
        input_tarball = os.path.join(tmpdir.strpath, "bad_json.tar")
        with tarfile.open(input_tarball, "w") as tar:
            bad_json = b"not valid json {{"
            info = tarfile.TarInfo(name="identity.json")
            info.size = len(bad_json)
            tar.addfile(info, io.BytesIO(bad_json))

        backup_manager = import_env["backup_manager"]

        # WHEN _read_identity_from_tarball is called
        # THEN an ImportBackupException is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager._read_identity_from_tarball(input_tarball)

        assert "Failed to parse identity.json" in str(exc_info.value)

    def test_import_backup_identity_missing_systemid(self, import_env):
        """
        Test that import raises KeyError when identity.json does not contain
        a systemid field, since these keys are expected to always be defined.
        """
        # GIVEN a tarball with an identity.json that has no systemid
        backup_manager = import_env["backup_manager"]
        input_tarball = import_env["make_tarball"](identity={"version": "15"})
        local_identity = {"systemid": "1234567890", "version": "15"}

        # WHEN import_backup is called
        # THEN a KeyError is raised because systemid should always be defined
        with pytest.raises(KeyError) as exc_info:
            backup_manager.import_backup(
                input_tarball, local_identity, "20240101T120000"
            )

        assert "systemid" in str(exc_info.value)

    def test_verify_staged_wals_success(self, import_env):
        """
        Test that _verify_staged_wals passes when all required WALs are
        listed in the staging xlog.db, present on disk, and do not collide
        with the server's existing WAL archive.
        """
        # GIVEN a staging directory with wals/ and xlog.db matching the
        # backup's required range, and an empty target server
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        xlogdb_file = import_env["xlogdb_file"]

        staging_dir = tmpdir.mkdir("staging_valid")
        wals_staging = staging_dir.mkdir("wals").mkdir("0000000100000000")
        wals_staging.join("000000010000000000000001").write_binary(b"wal1")
        wals_staging.join("000000010000000000000002").write_binary(b"wal2")

        staging_dir.join("xlog.db").write(
            "000000010000000000000001\t16\t1712994000.0\tNone\tNone\n"
            "000000010000000000000002\t16\t1712994000.0\tNone\tNone\n"
        )
        xlogdb_file.write("")

        backup_info = Mock()
        backup_info.get_required_wal_segments.return_value = iter(
            ["000000010000000000000001", "000000010000000000000002"]
        )

        # WHEN _verify_staged_wals is called
        # THEN no exception is raised
        backup_manager._verify_staged_wals(staging_dir.strpath, backup_info)

    def test_verify_staged_wals_missing_required_wal(self, import_env):
        """
        Test that _verify_staged_wals raises when a required WAL is not
        listed in the staging xlog.db.
        """
        # GIVEN a staging dir where xlog.db only lists one of two required WALs
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        xlogdb_file = import_env["xlogdb_file"]

        staging_dir = tmpdir.mkdir("staging_missing_xlogdb")
        wals_staging = staging_dir.mkdir("wals").mkdir("0000000100000000")
        wals_staging.join("000000010000000000000001").write_binary(b"wal1")

        staging_dir.join("xlog.db").write(
            "000000010000000000000001\t16\t1712994000.0\tNone\tNone\n"
        )
        xlogdb_file.write("")

        backup_info = Mock()
        backup_info.get_required_wal_segments.return_value = iter(
            ["000000010000000000000001", "000000010000000000000002"]
        )

        # WHEN _verify_staged_wals is called
        # THEN an ImportBackupException naming the missing WAL is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager._verify_staged_wals(staging_dir.strpath, backup_info)

        assert "000000010000000000000002" in str(exc_info.value)
        assert "not found in" in str(exc_info.value).lower()

    def test_verify_staged_wals_listed_wal_missing_from_disk(self, import_env):
        """
        Test that _verify_staged_wals raises when a WAL is listed in
        xlog.db but the physical file is not in staging.
        """
        # GIVEN a staging dir where xlog.db lists a WAL but the file is missing
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        xlogdb_file = import_env["xlogdb_file"]

        staging_dir = tmpdir.mkdir("staging_missing_disk")
        staging_dir.mkdir("wals").mkdir("0000000100000000")
        # Only create the first WAL file, skip the second
        staging_dir.join("wals", "0000000100000000", "000000010000000000000001").write(
            "wal1", ensure=True
        )

        staging_dir.join("xlog.db").write(
            "000000010000000000000001\t16\t1712994000.0\tNone\tNone\n"
            "000000010000000000000002\t16\t1712994000.0\tNone\tNone\n"
        )
        xlogdb_file.write("")

        backup_info = Mock()
        backup_info.get_required_wal_segments.return_value = iter(
            ["000000010000000000000001", "000000010000000000000002"]
        )

        # WHEN _verify_staged_wals is called
        # THEN an ImportBackupException about the missing physical file is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager._verify_staged_wals(staging_dir.strpath, backup_info)

        assert "000000010000000000000002" in str(exc_info.value)
        assert "not found under its wals/ directory" in str(exc_info.value).lower()

    def test_verify_staged_wals_malformed_xlogdb(self, import_env):
        """
        Test that _verify_staged_wals raises when staging xlog.db contains
        a malformed entry.
        """
        # GIVEN a staging dir with a malformed xlog.db line
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        xlogdb_file = import_env["xlogdb_file"]

        staging_dir = tmpdir.mkdir("staging_malformed")
        staging_dir.mkdir("wals")
        staging_dir.join("xlog.db").write("this is not a valid xlogdb line\n")
        xlogdb_file.write("")

        backup_info = Mock()
        backup_info.get_required_wal_segments.return_value = iter(
            ["000000010000000000000001"]
        )

        # WHEN _verify_staged_wals is called
        # THEN an ImportBackupException about the malformed entry is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager._verify_staged_wals(staging_dir.strpath, backup_info)

        assert "malformed" in str(exc_info.value).lower()

    def test_verify_staged_wals_conflict_in_server_xlogdb(self, import_env):
        """
        Test that _verify_staged_wals raises when an imported WAL is already
        listed in the server's xlog.db.
        """
        # GIVEN a staging dir with a WAL that the server already lists
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        xlogdb_file = import_env["xlogdb_file"]

        staging_dir = tmpdir.mkdir("staging_conflict_xlogdb")
        wals_staging = staging_dir.mkdir("wals").mkdir("0000000100000000")
        wals_staging.join("000000010000000000000001").write_binary(b"wal1")
        staging_dir.join("xlog.db").write(
            "000000010000000000000001\t16\t1712994000.0\tNone\tNone\n"
        )
        xlogdb_file.write("000000010000000000000001\t16\t1712994000.0\tNone\tNone\n")

        backup_info = Mock()
        backup_info.get_required_wal_segments.return_value = iter(
            ["000000010000000000000001"]
        )

        # WHEN _verify_staged_wals is called
        # THEN an ImportBackupException about the conflict is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager._verify_staged_wals(staging_dir.strpath, backup_info)

        assert "already exist" in str(exc_info.value).lower()
        assert "000000010000000000000001" in str(exc_info.value)

    def test_verify_staged_wals_conflict_on_disk(self, import_env):
        """
        Test that _verify_staged_wals raises when an imported WAL file is
        present on the server's disk even if absent from its xlog.db.
        """
        # GIVEN a staging dir with a WAL whose physical path exists on the server
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        xlogdb_file = import_env["xlogdb_file"]
        wals_dir = import_env["wals_dir"]

        staging_dir = tmpdir.mkdir("staging_conflict_disk")
        wals_staging = staging_dir.mkdir("wals").mkdir("0000000100000000")
        wals_staging.join("000000010000000000000001").write_binary(b"wal1")
        staging_dir.join("xlog.db").write(
            "000000010000000000000001\t16\t1712994000.0\tNone\tNone\n"
        )
        xlogdb_file.write("")
        wals_dir.mkdir("0000000100000000").join(
            "000000010000000000000001"
        ).write_binary(b"existing")

        backup_info = Mock()
        backup_info.get_required_wal_segments.return_value = iter(
            ["000000010000000000000001"]
        )

        # WHEN _verify_staged_wals is called
        # THEN an ImportBackupException about the conflict is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager._verify_staged_wals(staging_dir.strpath, backup_info)

        assert "already exist" in str(exc_info.value).lower()
        assert "000000010000000000000001" in str(exc_info.value)

    def test_verify_staged_wals_many_conflicts_truncates(self, import_env):
        """
        Test that _verify_staged_wals truncates the conflict list to at
        most 5 names in the error message and indicates how many more.
        """
        # GIVEN a staging dir with 7 WALs that all conflict with the server
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        xlogdb_file = import_env["xlogdb_file"]

        staging_dir = tmpdir.mkdir("staging_many_conflicts")
        wals_staging = staging_dir.mkdir("wals").mkdir("0000000100000000")
        wal_names = ["00000001000000000000000%d" % i for i in range(1, 8)]
        xlogdb_lines = ""
        for name in wal_names:
            wals_staging.join(name).write_binary(b"w")
            xlogdb_lines += "%s\t16\t1712994000.0\tNone\tNone\n" % name
        staging_dir.join("xlog.db").write(xlogdb_lines)
        xlogdb_file.write(xlogdb_lines)

        backup_info = Mock()
        backup_info.get_required_wal_segments.return_value = iter(wal_names)

        # WHEN _verify_staged_wals is called
        # THEN the error message shows at most 5 and "and X more"
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager._verify_staged_wals(staging_dir.strpath, backup_info)

        assert "and 2 more" in str(exc_info.value)

    def test_verify_staged_wals_idempotent_reimport(self, import_env):
        """
        Test that _verify_staged_wals treats a re-import of an identical
        WAL (same xlog.db line AND byte-equal file content) as a no-op,
        not a conflict.
        """
        # GIVEN a staging dir with a WAL and matching xlog.db line
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        xlogdb_file = import_env["xlogdb_file"]
        wals_dir = import_env["wals_dir"]

        wal_name = "000000010000000000000001"
        hash_subdir = "0000000100000000"
        wal_content = b"identical wal content"
        xlogdb_line = "%s\t%d\t1712994000.0\tNone\tNone\n" % (
            wal_name,
            len(wal_content),
        )

        staging_dir = tmpdir.mkdir("staging_idempotent")
        wals_staging = staging_dir.mkdir("wals").mkdir(hash_subdir)
        wals_staging.join(wal_name).write_binary(wal_content)
        staging_dir.join("xlog.db").write(xlogdb_line)

        # AND the target server has the same WAL with identical xlog.db
        # line and identical file content
        xlogdb_file.write(xlogdb_line)
        wals_dir.mkdir(hash_subdir).join(wal_name).write_binary(wal_content)

        backup_info = Mock()
        backup_info.get_required_wal_segments.return_value = iter([wal_name])

        # WHEN _verify_staged_wals is called
        # THEN no exception is raised — idempotent re-import is allowed
        backup_manager._verify_staged_wals(staging_dir.strpath, backup_info)

    def test_verify_staged_wals_conflict_when_content_differs(self, import_env):
        """
        Test that _verify_staged_wals still reports a conflict when the
        server has the same WAL name but the file contents differ — even
        if the xlog.db line is otherwise identical.
        """
        # GIVEN a staging dir with a WAL and matching xlog.db line
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        xlogdb_file = import_env["xlogdb_file"]
        wals_dir = import_env["wals_dir"]

        wal_name = "000000010000000000000001"
        hash_subdir = "0000000100000000"
        xlogdb_line = "%s\t9\t1712994000.0\tNone\tNone\n" % wal_name

        staging_dir = tmpdir.mkdir("staging_content_differs")
        wals_staging = staging_dir.mkdir("wals").mkdir(hash_subdir)
        wals_staging.join(wal_name).write_binary(b"version A")
        staging_dir.join("xlog.db").write(xlogdb_line)

        # AND the target server has the same WAL name with identical
        # xlog.db line but a different file body on disk
        xlogdb_file.write(xlogdb_line)
        wals_dir.mkdir(hash_subdir).join(wal_name).write_binary(b"version B")

        backup_info = Mock()
        backup_info.get_required_wal_segments.return_value = iter([wal_name])

        # WHEN _verify_staged_wals is called
        # THEN an ImportBackupException about the conflict is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager._verify_staged_wals(staging_dir.strpath, backup_info)

        assert "already exist" in str(exc_info.value).lower()
        assert wal_name in str(exc_info.value)

    def test_verify_staged_wals_conflict_when_xlogdb_line_differs(self, import_env):
        """
        Test that _verify_staged_wals reports a conflict when the server
        has the same WAL name and byte-equal file contents but the
        xlog.db line differs (e.g. recorded with different timestamp or
        compression metadata).
        """
        # GIVEN a staging dir with a WAL and xlog.db line
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        xlogdb_file = import_env["xlogdb_file"]
        wals_dir = import_env["wals_dir"]

        wal_name = "000000010000000000000001"
        hash_subdir = "0000000100000000"
        wal_content = b"identical body"

        staging_dir = tmpdir.mkdir("staging_xlogdb_differs")
        wals_staging = staging_dir.mkdir("wals").mkdir(hash_subdir)
        wals_staging.join(wal_name).write_binary(wal_content)
        staging_dir.join("xlog.db").write(
            "%s\t%d\t1712994000.0\tNone\tNone\n" % (wal_name, len(wal_content))
        )

        # AND the target server has the same WAL name and byte-equal
        # file body, but the xlog.db line differs (different timestamp)
        xlogdb_file.write(
            "%s\t%d\t9999999999.0\tNone\tNone\n" % (wal_name, len(wal_content))
        )
        wals_dir.mkdir(hash_subdir).join(wal_name).write_binary(wal_content)

        backup_info = Mock()
        backup_info.get_required_wal_segments.return_value = iter([wal_name])

        # WHEN _verify_staged_wals is called
        # THEN an ImportBackupException about the conflict is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager._verify_staged_wals(staging_dir.strpath, backup_info)

        assert "already exist" in str(exc_info.value).lower()
        assert wal_name in str(exc_info.value)

    def test_verify_staging_layout_complete(self, import_env):
        """
        All four expected entries (``backup/``, ``backup.info``,
        ``wals/``, ``xlog.db``) present — no exception raised.
        """
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]

        staging_dir = tmpdir.mkdir("staging_layout_complete")
        staging_dir.mkdir("backup")
        staging_dir.join("backup.info").write("")
        staging_dir.mkdir("wals")
        staging_dir.join("xlog.db").write("")

        backup_manager._verify_staging_layout(staging_dir.strpath)

    @pytest.mark.parametrize(
        "missing, expected_message_fragment",
        [
            ("backup", "'backup/' directory"),
            ("backup.info", "'backup.info' file"),
            ("wals", "'wals/' directory"),
            ("xlog.db", "'xlog.db' file"),
        ],
    )
    def test_verify_staging_layout_missing_entry(
        self, import_env, missing, expected_message_fragment
    ):
        """
        Each of the four expected entries, when missing, produces an
        ``ImportBackupException`` whose message names the entry.
        """
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]

        staging_dir = tmpdir.mkdir(
            "staging_layout_missing_%s" % missing.replace(".", "_").replace("/", "_")
        )
        # Create all four expected entries, then remove the one under test.
        staging_dir.mkdir("backup")
        staging_dir.join("backup.info").write("")
        staging_dir.mkdir("wals")
        staging_dir.join("xlog.db").write("")
        target = staging_dir.join(missing)
        if target.isdir():
            target.remove(rec=True)
        else:
            target.remove()

        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager._verify_staging_layout(staging_dir.strpath)

        assert expected_message_fragment in str(exc_info.value)

    def test_verify_staging_layout_file_in_place_of_directory(self, import_env):
        """
        A regular file at a path that should be a directory must be
        rejected with the "directory" message — that's the point of the
        ``os.path.isdir`` check vs. plain ``os.path.exists``.
        """
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]

        staging_dir = tmpdir.mkdir("staging_layout_file_for_dir")
        # 'backup' is supposed to be a directory; create a regular file
        # there instead.
        staging_dir.join("backup").write("not a directory")
        staging_dir.join("backup.info").write("")
        staging_dir.mkdir("wals")
        staging_dir.join("xlog.db").write("")

        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager._verify_staging_layout(staging_dir.strpath)

        assert "'backup/' directory" in str(exc_info.value)

    def test_verify_staging_layout_directory_in_place_of_file(self, import_env):
        """
        A directory at a path that should be a file must be rejected
        with the "file" message — symmetric to the previous test.
        """
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]

        staging_dir = tmpdir.mkdir("staging_layout_dir_for_file")
        staging_dir.mkdir("backup")
        # 'backup.info' should be a file; create a directory instead.
        staging_dir.mkdir("backup.info")
        staging_dir.mkdir("wals")
        staging_dir.join("xlog.db").write("")

        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager._verify_staging_layout(staging_dir.strpath)

        assert "'backup.info' file" in str(exc_info.value)

    def test_iter_tarball_xlogdb_normal(self, import_env):
        """
        Yields ``(stripped_line, wal_name)`` tuples for each non-blank
        entry, in file order.
        """
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]

        staging_dir = tmpdir.mkdir("staging_iter_normal")
        staging_dir.join("xlog.db").write(
            "000000010000000000000001\t4\t1712994000.0\tNone\tNone\n"
            "000000010000000000000002\t4\t1712994000.0\tNone\tNone\n"
        )

        entries = list(backup_manager._iter_tarball_xlogdb(staging_dir.strpath))

        assert len(entries) == 2
        # Each entry is (stripped_line, wal_name)
        assert entries[0][1] == "000000010000000000000001"
        assert entries[1][1] == "000000010000000000000002"
        # Lines are stripped (no trailing newline)
        for line, _ in entries:
            assert not line.endswith("\n")

    def test_iter_tarball_xlogdb_missing_file(self, import_env):
        """
        Raises ``ImportBackupException`` if the staging xlog.db file
        cannot be opened.
        """
        backup_manager = import_env["backup_manager"]
        tmpdir = import_env["tmpdir"]

        empty_staging = tmpdir.mkdir("staging_iter_missing")
        # no xlog.db inside

        with pytest.raises(ImportBackupException) as exc_info:
            list(backup_manager._iter_tarball_xlogdb(empty_staging.strpath))

        assert "Failed to read xlog.db" in str(exc_info.value)

    def test_iter_tarball_xlogdb_malformed_line(self, import_env):
        """
        Raises ``ImportBackupException`` with the offending line number
        when a line cannot be parsed.
        """
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]

        staging_dir = tmpdir.mkdir("staging_iter_malformed")
        staging_dir.join("xlog.db").write(
            "000000010000000000000001\t4\t1712994000.0\tNone\tNone\n"
            "this is not a valid xlogdb line\n"
        )

        with pytest.raises(ImportBackupException) as exc_info:
            list(backup_manager._iter_tarball_xlogdb(staging_dir.strpath))

        assert "Malformed" in str(exc_info.value)
        assert "line 2" in str(exc_info.value)

    def test_iter_tarball_xlogdb_skips_blank_lines(self, import_env):
        """
        Blank or whitespace-only lines are silently skipped.
        """
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]

        staging_dir = tmpdir.mkdir("staging_iter_blanks")
        staging_dir.join("xlog.db").write(
            "\n"
            "000000010000000000000001\t4\t1712994000.0\tNone\tNone\n"
            "   \n"
            "000000010000000000000002\t4\t1712994000.0\tNone\tNone\n"
        )

        entries = list(backup_manager._iter_tarball_xlogdb(staging_dir.strpath))

        assert [name for _, name in entries] == [
            "000000010000000000000001",
            "000000010000000000000002",
        ]

    def test_xlogdb_metadata_match_identical_lines(self, import_env):
        """Identical lines (every field equal) → match."""
        backup_manager = import_env["backup_manager"]
        line = "000000010000000000000001\t16\t1712994000.0\tgzip\tNone"
        assert backup_manager._xlogdb_metadata_match(line, line) is True

    def test_xlogdb_metadata_match_compression_only_diff(self, import_env):
        """
        Lines differ ONLY in the ``compression`` field → still a match.
        See the docstring of ``_wal_conflicts_with_server`` for why
        compression is excused (rebuild_xlogdb can't reconstruct it for
        WALs that are both compressed and encrypted).
        """
        backup_manager = import_env["backup_manager"]
        line_a = "000000010000000000000001\t16\t1712994000.0\tgzip\taes256"
        line_b = "000000010000000000000001\t16\t1712994000.0\tNone\taes256"
        assert backup_manager._xlogdb_metadata_match(line_a, line_b) is True

    @pytest.mark.parametrize(
        "line_a, line_b, diff_field",
        [
            (
                "000000010000000000000001\t16\t1712994000.0\tgzip\tNone",
                "000000010000000000000002\t16\t1712994000.0\tgzip\tNone",
                "name",
            ),
            (
                "000000010000000000000001\t16\t1712994000.0\tgzip\tNone",
                "000000010000000000000001\t32\t1712994000.0\tgzip\tNone",
                "size",
            ),
            (
                "000000010000000000000001\t16\t1712994000.0\tgzip\tNone",
                "000000010000000000000001\t16\t9999999999.0\tgzip\tNone",
                "time",
            ),
            (
                "000000010000000000000001\t16\t1712994000.0\tgzip\tNone",
                "000000010000000000000001\t16\t1712994000.0\tgzip\taes256",
                "encryption",
            ),
        ],
    )
    def test_xlogdb_metadata_match_non_compression_field_diff(
        self, import_env, line_a, line_b, diff_field
    ):
        """Any non-``compression`` field difference → no match."""
        backup_manager = import_env["backup_manager"]
        assert backup_manager._xlogdb_metadata_match(line_a, line_b) is False

    def test_wal_conflicts_with_server_clean_import(self, import_env):
        """
        Server has no xlog.db entry and no file for this WAL — the
        helper must report no conflict.
        """
        backup_manager = import_env["backup_manager"]
        tmpdir = import_env["tmpdir"]

        assert (
            backup_manager._wal_conflicts_with_server(
                wal_name="000000010000000000000001",
                tarball_line=("000000010000000000000001\t4\t1712994000.0\tNone\tNone"),
                tarball_wal_path=tmpdir.join("tarball_wal").strpath,
                server_line=None,
                server_wal_path=tmpdir.join("nonexistent_server_wal").strpath,
            )
            is False
        )

    def test_wal_conflicts_with_server_idempotent(self, import_env):
        """
        Server has matching xlog.db line and byte-equal file — the
        helper must report no conflict (idempotent re-import).
        """
        backup_manager = import_env["backup_manager"]
        tmpdir = import_env["tmpdir"]

        line = "000000010000000000000001\t4\t1712994000.0\tNone\tNone"
        content = b"identical"
        tarball_path = tmpdir.join("tarball_wal")
        tarball_path.write_binary(content)
        server_path = tmpdir.join("server_wal")
        server_path.write_binary(content)

        assert (
            backup_manager._wal_conflicts_with_server(
                wal_name="000000010000000000000001",
                tarball_line=line,
                tarball_wal_path=tarball_path.strpath,
                server_line=line,
                server_wal_path=server_path.strpath,
            )
            is False
        )

    def test_wal_conflicts_with_server_entry_only(self, import_env):
        """
        Server has the xlog.db entry but no file on disk — inconsistent
        state, treat as conflict.
        """
        backup_manager = import_env["backup_manager"]
        tmpdir = import_env["tmpdir"]

        line = "000000010000000000000001\t4\t1712994000.0\tNone\tNone"
        tarball_path = tmpdir.join("tarball_wal")
        tarball_path.write_binary(b"x")

        assert (
            backup_manager._wal_conflicts_with_server(
                wal_name="000000010000000000000001",
                tarball_line=line,
                tarball_wal_path=tarball_path.strpath,
                server_line=line,
                server_wal_path=tmpdir.join("nonexistent_server_wal").strpath,
            )
            is True
        )

    def test_wal_conflicts_with_server_file_only(self, import_env):
        """
        Server has the file but no matching xlog.db entry — inconsistent
        state, treat as conflict.
        """
        backup_manager = import_env["backup_manager"]
        tmpdir = import_env["tmpdir"]

        tarball_path = tmpdir.join("tarball_wal")
        tarball_path.write_binary(b"x")
        server_path = tmpdir.join("server_wal_only")
        server_path.write_binary(b"x")

        assert (
            backup_manager._wal_conflicts_with_server(
                wal_name="000000010000000000000001",
                tarball_line=("000000010000000000000001\t4\t1712994000.0\tNone\tNone"),
                tarball_wal_path=tarball_path.strpath,
                server_line=None,
                server_wal_path=server_path.strpath,
            )
            is True
        )

    def test_wal_conflicts_with_server_xlogdb_line_differs(self, import_env):
        """
        Server has both artifacts but the xlog.db lines differ — treat
        as conflict, even if the files happen to match.
        """
        backup_manager = import_env["backup_manager"]
        tmpdir = import_env["tmpdir"]

        content = b"identical"
        tarball_path = tmpdir.join("tarball_wal")
        tarball_path.write_binary(content)
        server_path = tmpdir.join("server_wal")
        server_path.write_binary(content)

        assert (
            backup_manager._wal_conflicts_with_server(
                wal_name="000000010000000000000001",
                tarball_line=("000000010000000000000001\t9\t1712994000.0\tNone\tNone"),
                tarball_wal_path=tarball_path.strpath,
                server_line=("000000010000000000000001\t9\t9999999999.0\tNone\tNone"),
                server_wal_path=server_path.strpath,
            )
            is True
        )

    def test_wal_conflicts_with_server_file_content_differs(self, import_env):
        """
        Server has both artifacts with matching xlog.db line but the
        file bodies differ — treat as conflict.
        """
        backup_manager = import_env["backup_manager"]
        tmpdir = import_env["tmpdir"]

        line = "000000010000000000000001\t9\t1712994000.0\tNone\tNone"
        tarball_path = tmpdir.join("tarball_wal")
        tarball_path.write_binary(b"version A")
        server_path = tmpdir.join("server_wal")
        server_path.write_binary(b"version B")

        assert (
            backup_manager._wal_conflicts_with_server(
                wal_name="000000010000000000000001",
                tarball_line=line,
                tarball_wal_path=tarball_path.strpath,
                server_line=line,
                server_wal_path=server_path.strpath,
            )
            is True
        )

    def test_wal_conflicts_with_server_compression_differs_is_idempotent(
        self, import_env
    ):
        """
        Server has the same WAL with byte-equal contents and matching
        metadata except for ``compression`` — treat as idempotent.

        This is the encrypted-and-compressed corner case: if the
        server's xlog.db has been rebuilt at some point between
        export and import, the rebuild only sees the outer encryption
        magic and clears the previously-correct compression field
        (e.g. ``gzip`` → ``None``). The file itself is unchanged. We
        recognize this as the same WAL.
        """
        backup_manager = import_env["backup_manager"]
        tmpdir = import_env["tmpdir"]

        content = b"identical"
        tarball_path = tmpdir.join("tarball_wal")
        tarball_path.write_binary(content)
        server_path = tmpdir.join("server_wal")
        server_path.write_binary(content)

        # Tarball remembers the original compression metadata
        tarball_line = "000000010000000000000001\t9\t1712994000.0\tgzip\taes256"
        # Server's compression has been cleared by a later rebuild
        server_line = "000000010000000000000001\t9\t1712994000.0\tNone\taes256"

        assert (
            backup_manager._wal_conflicts_with_server(
                wal_name="000000010000000000000001",
                tarball_line=tarball_line,
                tarball_wal_path=tarball_path.strpath,
                server_line=server_line,
                server_wal_path=server_path.strpath,
            )
            is False
        )

    def test_wal_conflicts_with_server_encryption_differs_is_conflict(self, import_env):
        """
        Server has the same WAL name and file contents but the
        ``encryption`` field differs — still a conflict. Only
        ``compression`` is excused (encryption can't be silently lost
        by a rebuild the way compression can).
        """
        backup_manager = import_env["backup_manager"]
        tmpdir = import_env["tmpdir"]

        content = b"identical"
        tarball_path = tmpdir.join("tarball_wal")
        tarball_path.write_binary(content)
        server_path = tmpdir.join("server_wal")
        server_path.write_binary(content)

        tarball_line = "000000010000000000000001\t9\t1712994000.0\tNone\taes256"
        server_line = "000000010000000000000001\t9\t1712994000.0\tNone\tNone"

        assert (
            backup_manager._wal_conflicts_with_server(
                wal_name="000000010000000000000001",
                tarball_line=tarball_line,
                tarball_wal_path=tarball_path.strpath,
                server_line=server_line,
                server_wal_path=server_path.strpath,
            )
            is True
        )

    def test_wal_conflicts_with_server_different_name_in_server_line(self, import_env):
        """
        ``server_line`` is from a later WAL name (the caller didn't
        advance past us; we're checking a tarball WAL that the server
        doesn't have yet). Treat as clean (no entry for THIS name).
        """
        backup_manager = import_env["backup_manager"]
        tmpdir = import_env["tmpdir"]

        tarball_path = tmpdir.join("tarball_wal")
        tarball_path.write_binary(b"x")

        # server_line is for a different (later) WAL name
        assert (
            backup_manager._wal_conflicts_with_server(
                wal_name="000000010000000000000001",
                tarball_line=("000000010000000000000001\t1\t1712994000.0\tNone\tNone"),
                tarball_wal_path=tarball_path.strpath,
                server_line=("000000010000000000000099\t1\t1712994000.0\tNone\tNone"),
                server_wal_path=tmpdir.join("nonexistent").strpath,
            )
            is False
        )

    def test_import_backup_wals_moves_files_and_merges_xlogdb(self, import_env):
        """
        Test that _import_backup_wals moves WAL files from staging to the
        target wals directory and merges entries into the server's xlog.db.
        """
        # GIVEN a staging directory with WAL files and xlog.db
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        wals_dir = import_env["wals_dir"]
        xlogdb_file = import_env["xlogdb_file"]

        staging_dir = tmpdir.mkdir("staging_wals")
        wals_staging = staging_dir.mkdir("wals").mkdir("0000000100000000")
        wals_staging.join("000000010000000000000003").write_binary(b"wal3")
        wals_staging.join("000000010000000000000004").write_binary(b"wal4")

        staging_dir.join("xlog.db").write(
            "000000010000000000000003\t4\t1712994000.0\tNone\tNone\n"
            "000000010000000000000004\t4\t1712994000.0\tNone\tNone\n"
        )

        # AND the server already has WALs 1 and 2 in its xlog.db
        xlogdb_file.write(
            "000000010000000000000001\t16\t1712994000.0\tNone\tNone\n"
            "000000010000000000000002\t16\t1712994000.0\tNone\tNone\n"
        )

        # WHEN _import_backup_wals is called
        rollback = backup_manager._import_backup_wals(staging_dir.strpath)

        # THEN WAL files are moved to the target directory
        hash_dir = os.path.join(wals_dir.strpath, "0000000100000000")
        assert os.path.exists(os.path.join(hash_dir, "000000010000000000000003"))
        assert os.path.exists(os.path.join(hash_dir, "000000010000000000000004"))

        # AND the files are removed from staging
        assert not os.path.exists(wals_staging.join("000000010000000000000003").strpath)
        assert not os.path.exists(wals_staging.join("000000010000000000000004").strpath)

        # AND the server's xlog.db contains all 4 entries in sorted order
        xlogdb_content = xlogdb_file.read()
        lines = [entry for entry in xlogdb_content.strip().split("\n") if entry]
        assert len(lines) == 4
        names = [entry.split("\t")[0] for entry in lines]
        assert names == sorted(names)
        assert "000000010000000000000003" in names
        assert "000000010000000000000004" in names

        # AND a rollback callable is returned
        assert callable(rollback)

    def test_import_backup_wals_merge_interleaves_correctly(self, import_env):
        """
        Test that the streaming merge correctly interleaves import entries
        between existing entries (not just appending).
        """
        # GIVEN a server xlog.db with WALs 1, 3, 5
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        xlogdb_file = import_env["xlogdb_file"]

        xlogdb_file.write(
            "000000010000000000000001\t16\t1712994000.0\tNone\tNone\n"
            "000000010000000000000003\t16\t1712994000.0\tNone\tNone\n"
            "000000010000000000000005\t16\t1712994000.0\tNone\tNone\n"
        )

        # AND a staging dir with WALs 2 and 4 (which should interleave)
        staging_dir = tmpdir.mkdir("staging_interleave")
        wals_staging = staging_dir.mkdir("wals").mkdir("0000000100000000")
        wals_staging.join("000000010000000000000002").write_binary(b"wal2")
        wals_staging.join("000000010000000000000004").write_binary(b"wal4")

        staging_dir.join("xlog.db").write(
            "000000010000000000000002\t4\t1712994000.0\tNone\tNone\n"
            "000000010000000000000004\t4\t1712994000.0\tNone\tNone\n"
        )

        # WHEN _import_backup_wals is called
        backup_manager._import_backup_wals(staging_dir.strpath)

        # THEN the xlog.db has all 5 entries in correct sorted order
        lines = [entry for entry in xlogdb_file.read().strip().split("\n") if entry]
        names = [entry.split("\t")[0] for entry in lines]
        assert names == [
            "000000010000000000000001",
            "000000010000000000000002",
            "000000010000000000000003",
            "000000010000000000000004",
            "000000010000000000000005",
        ]

    def test_import_backup_wals_skips_idempotent_entries(self, import_env):
        """
        Test that when the tarball contains a WAL the server already
        has (idempotent re-import, already verified by
        ``_verify_staged_wals``), ``_import_backup_wals`` does NOT:

        - overwrite the server's pre-existing WAL file (and consequently
          mark it for rollback), and
        - duplicate the WAL's xlog.db line in the merged xlog.db.

        Both bugs are easy to introduce with a naive two-stream merge
        that treats every tarball entry as a new import.
        """
        # GIVEN the server already has WAL 2 in both xlog.db and on disk
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        xlogdb_file = import_env["xlogdb_file"]
        wals_dir = import_env["wals_dir"]

        idempotent_name = "000000010000000000000002"
        new_name = "000000010000000000000003"
        hash_subdir = "0000000100000000"
        idempotent_line = "%s\t4\t1712994000.0\tNone\tNone\n" % idempotent_name
        new_line = "%s\t4\t1712994000.0\tNone\tNone\n" % new_name
        idempotent_content = b"wal2"
        new_content = b"wal3"

        xlogdb_file.write(idempotent_line)
        server_idempotent_path = wals_dir.mkdir(hash_subdir).join(idempotent_name)
        server_idempotent_path.write_binary(idempotent_content)

        # AND the staging dir contains the same idempotent WAL plus a
        # genuinely new WAL
        staging_dir = tmpdir.mkdir("staging_idempotent_import")
        wals_staging = staging_dir.mkdir("wals").mkdir(hash_subdir)
        wals_staging.join(idempotent_name).write_binary(idempotent_content)
        wals_staging.join(new_name).write_binary(new_content)
        staging_dir.join("xlog.db").write(idempotent_line + new_line)

        # WHEN _import_backup_wals is called
        rollback = backup_manager._import_backup_wals(staging_dir.strpath)

        # THEN the new WAL is in the server's wals_directory
        server_new_path = os.path.join(wals_dir.strpath, hash_subdir, new_name)
        assert os.path.exists(server_new_path)

        # AND the idempotent WAL is still there with original content
        assert os.path.exists(server_idempotent_path.strpath)
        assert server_idempotent_path.read_binary() == idempotent_content

        # AND the server's xlog.db has each entry exactly once
        merged_lines = [
            entry for entry in xlogdb_file.read().strip().split("\n") if entry
        ]
        merged_names = [entry.split("\t")[0] for entry in merged_lines]
        assert merged_names == [idempotent_name, new_name]

        # AND if rollback runs later, it must NOT delete the server's
        # pre-existing idempotent WAL — only the genuinely-moved one.
        rollback()
        assert os.path.exists(server_idempotent_path.strpath)
        assert server_idempotent_path.read_binary() == idempotent_content
        assert not os.path.exists(server_new_path)

    def test_import_backup_wals_into_empty_xlogdb(self, import_env):
        """
        Test that _import_backup_wals works when the server's xlog.db
        is initially empty.
        """
        # GIVEN an empty server xlog.db
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        xlogdb_file = import_env["xlogdb_file"]

        xlogdb_file.write("")

        # AND a staging dir with WAL files
        staging_dir = tmpdir.mkdir("staging_empty_xlogdb")
        wals_staging = staging_dir.mkdir("wals").mkdir("0000000100000000")
        wals_staging.join("000000010000000000000001").write_binary(b"wal1")

        staging_dir.join("xlog.db").write(
            "000000010000000000000001\t4\t1712994000.0\tNone\tNone\n"
        )

        # WHEN _import_backup_wals is called
        backup_manager._import_backup_wals(staging_dir.strpath)

        # THEN xlog.db contains the imported entry
        lines = [entry for entry in xlogdb_file.read().strip().split("\n") if entry]
        assert len(lines) == 1
        assert lines[0].startswith("000000010000000000000001")

    def test_import_backup_wals_creates_hash_dirs(self, import_env):
        """
        Test that _import_backup_wals creates hash subdirectories in the
        target wals directory when they don't already exist.
        """
        # GIVEN a server wals directory without the needed hash subdir
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        wals_dir = import_env["wals_dir"]
        xlogdb_file = import_env["xlogdb_file"]

        xlogdb_file.write("")

        hash_subdir = os.path.join(wals_dir.strpath, "0000000100000000")
        assert not os.path.exists(hash_subdir)

        # AND a staging dir with a WAL file
        staging_dir = tmpdir.mkdir("staging_create_hash")
        wals_staging = staging_dir.mkdir("wals").mkdir("0000000100000000")
        wals_staging.join("000000010000000000000001").write_binary(b"wal1")
        staging_dir.join("xlog.db").write(
            "000000010000000000000001\t4\t1712994000.0\tNone\tNone\n"
        )

        # WHEN _import_backup_wals is called
        backup_manager._import_backup_wals(staging_dir.strpath)

        # THEN the hash subdirectory was created
        assert os.path.isdir(hash_subdir)

        # AND the WAL file is in it
        assert os.path.exists(os.path.join(hash_subdir, "000000010000000000000001"))

    def test_import_backup_wals_rollback_on_failure(self, import_env):
        """
        Test that _import_backup_wals rolls back moved files and rebuilds
        xlog.db if an error occurs during the merge phase.
        """
        # GIVEN a staging dir with a WAL file
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        wals_dir = import_env["wals_dir"]
        xlogdb_file = import_env["xlogdb_file"]

        xlogdb_file.write("")

        staging_dir = tmpdir.mkdir("staging_rollback")
        wals_staging = staging_dir.mkdir("wals").mkdir("0000000100000000")
        wals_staging.join("000000010000000000000001").write_binary(b"wal1")
        staging_dir.join("xlog.db").write(
            "000000010000000000000001\t4\t1712994000.0\tNone\tNone\n"
        )

        # AND the xlogdb context manager is rigged to fail during merge

        @contextmanager
        def failing_xlogdb(mode="r"):
            if "+" in mode or "w" in mode:
                raise IOError("simulated disk failure")
            with open(xlogdb_file.strpath, mode) as f:
                yield f

        backup_manager.server.xlogdb = failing_xlogdb

        # WHEN _import_backup_wals is called
        # THEN an ImportBackupException is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager._import_backup_wals(staging_dir.strpath)

        assert "Failed to import WAL files" in str(exc_info.value)

        # AND the WAL file was cleaned up (rollback removed it)
        hash_dir = os.path.join(wals_dir.strpath, "0000000100000000")
        assert not os.path.exists(os.path.join(hash_dir, "000000010000000000000001"))

        # AND rebuild_xlogdb was called
        backup_manager.server.rebuild_xlogdb.assert_called_once_with(silent=True)

    def test_rollback_wal_import_removes_files_and_dirs(self, import_env):
        """
        Test that _rollback_wal_import removes moved WAL files, empty
        hash directories, and calls rebuild_xlogdb.
        """
        # GIVEN WAL files that were "imported" to target directory
        backup_manager = import_env["backup_manager"]
        wals_dir = import_env["wals_dir"]

        hash_dir = wals_dir.mkdir("0000000100000000")
        wal_path = hash_dir.join("000000010000000000000001")
        wal_path.write_binary(b"wal1")

        moved_files = [wal_path.strpath]
        created_dirs = [hash_dir.strpath]

        # WHEN _rollback_wal_import is called
        backup_manager._rollback_wal_import(moved_files, created_dirs)

        # THEN WAL file is removed
        assert not os.path.exists(wal_path.strpath)

        # AND the empty hash directory is removed
        assert not os.path.exists(hash_dir.strpath)

        # AND rebuild_xlogdb is called
        backup_manager.server.rebuild_xlogdb.assert_called_once_with(silent=True)

    def test_rollback_wal_import_preserves_non_empty_dirs(self, import_env):
        """
        Test that _rollback_wal_import does not remove hash directories
        that still contain other files.
        """
        # GIVEN a hash directory with both an imported WAL and a pre-existing one
        backup_manager = import_env["backup_manager"]
        wals_dir = import_env["wals_dir"]

        hash_dir = wals_dir.mkdir("0000000100000000")
        imported_wal = hash_dir.join("000000010000000000000002")
        imported_wal.write_binary(b"wal2")
        preexisting_wal = hash_dir.join("000000010000000000000001")
        preexisting_wal.write_binary(b"wal1")

        moved_files = [imported_wal.strpath]
        created_dirs = [hash_dir.strpath]

        # WHEN _rollback_wal_import is called
        backup_manager._rollback_wal_import(moved_files, created_dirs)

        # THEN the imported WAL is removed
        assert not os.path.exists(imported_wal.strpath)

        # AND the directory is NOT removed (still has the pre-existing WAL)
        assert os.path.isdir(hash_dir.strpath)
        assert os.path.exists(preexisting_wal.strpath)

    def test_rollback_wal_import_tolerates_already_removed_files(self, import_env):
        """
        Test that _rollback_wal_import does not fail if WAL files have
        already been removed (e.g. partial failure scenario).
        """
        # GIVEN file paths that don't exist on disk
        backup_manager = import_env["backup_manager"]
        wals_dir = import_env["wals_dir"]

        moved_files = [
            os.path.join(wals_dir.strpath, "0000000100000000", "nonexistent_wal")
        ]
        created_dirs = []

        # WHEN _rollback_wal_import is called
        # THEN no exception is raised
        backup_manager._rollback_wal_import(moved_files, created_dirs)

        # AND rebuild_xlogdb is still called
        backup_manager.server.rebuild_xlogdb.assert_called_once_with(silent=True)

    def test_import_backup_wals_rollback_closure(self, import_env):
        """
        Test that the rollback closure returned by _import_backup_wals
        removes imported WAL files and rebuilds xlog.db when called.
        """
        # GIVEN a successful WAL import
        tmpdir = import_env["tmpdir"]
        backup_manager = import_env["backup_manager"]
        wals_dir = import_env["wals_dir"]
        xlogdb_file = import_env["xlogdb_file"]

        xlogdb_file.write("")

        staging_dir = tmpdir.mkdir("staging_closure")
        wals_staging = staging_dir.mkdir("wals").mkdir("0000000100000000")
        wals_staging.join("000000010000000000000001").write_binary(b"wal1")
        staging_dir.join("xlog.db").write(
            "000000010000000000000001\t4\t1712994000.0\tNone\tNone\n"
        )

        rollback = backup_manager._import_backup_wals(staging_dir.strpath)

        # AND the WAL file was moved
        target_path = os.path.join(
            wals_dir.strpath, "0000000100000000", "000000010000000000000001"
        )
        assert os.path.exists(target_path)

        # WHEN the rollback closure is invoked
        rollback()

        # THEN the imported WAL file is removed
        assert not os.path.exists(target_path)

        # AND rebuild_xlogdb was called
        backup_manager.server.rebuild_xlogdb.assert_called_with(silent=True)

    def test_import_backup_full_success_with_wals(self, import_env):
        """
        Test the full import_backup flow including WAL import, verifying
        that WAL files are moved and xlog.db is updated.
        """
        # GIVEN a valid export tarball with WALs
        backup_manager = import_env["backup_manager"]
        wals_dir = import_env["wals_dir"]
        xlogdb_file = import_env["xlogdb_file"]
        input_tarball = import_env["make_tarball"]()
        local_identity = {"systemid": "1234567890", "version": "15"}

        # AND the meta directory exists
        os.makedirs(backup_manager.server.meta_directory, exist_ok=True)

        # AND the server's xlog.db is initially empty
        xlogdb_file.write("")

        # WHEN import_backup is called
        backup_manager.import_backup(input_tarball, local_identity, "20240101T120000")

        # THEN backup is registered
        assert backup_manager.get_backup("20240101T120000") is not None

        # AND WAL files are in the target directory
        hash_dir = os.path.join(wals_dir.strpath, "0000000100000000")
        assert os.path.exists(os.path.join(hash_dir, "000000010000000000000001"))
        assert os.path.exists(os.path.join(hash_dir, "000000010000000000000002"))

        # AND xlog.db contains the imported entries
        xlogdb_content = xlogdb_file.read()
        assert "000000010000000000000001" in xlogdb_content
        assert "000000010000000000000002" in xlogdb_content

        # AND the backup is marked as KEEP:STANDALONE
        assert backup_manager.should_keep_backup("20240101T120000") is True
        assert (
            backup_manager.get_keep_target("20240101T120000")
            == KeepManager.TARGET_STANDALONE
        )

    def test_import_backup_metadata_failure_rolls_back_wals(self, import_env):
        """
        Test that when _import_backup_metadata fails after WAL import, both
        WAL files and backup data are rolled back.
        """
        # GIVEN a tarball that will fail at the metadata registration stage
        # (backup.info loads fine but save() raises an error)
        backup_manager = import_env["backup_manager"]
        wals_dir = import_env["wals_dir"]
        xlogdb_file = import_env["xlogdb_file"]

        xlogdb_file.write("")

        input_tarball = import_env["make_tarball"]()
        local_identity = {"systemid": "1234567890", "version": "15"}

        # AND the meta directory exists
        os.makedirs(backup_manager.server.meta_directory, exist_ok=True)

        # AND _import_backup_metadata is patched to raise an error
        with patch.object(
            BackupManager,
            "_import_backup_metadata",
            side_effect=ImportBackupException("metadata registration failed"),
        ):
            # WHEN import_backup is called
            # THEN it raises the exception
            with pytest.raises(ImportBackupException) as exc_info:
                backup_manager.import_backup(
                    input_tarball, local_identity, "20240101T120000"
                )

            assert "metadata registration failed" in str(exc_info.value)

        # AND WAL files are rolled back (removed)
        hash_dir = os.path.join(wals_dir.strpath, "0000000100000000")
        assert not os.path.exists(os.path.join(hash_dir, "000000010000000000000001"))

        # AND rebuild_xlogdb was called to restore the catalog
        backup_manager.server.rebuild_xlogdb.assert_called_with(silent=True)

        # AND backup data directory is removed
        target_dir = os.path.join(
            backup_manager.config.basebackups_directory, "20240101T120000"
        )
        assert not os.path.exists(target_dir)

    def test_import_backup_wal_import_failure_rolls_back_data(self, import_env):
        """
        Test that when ``_import_backup_wals`` fails after the basebackup
        directory has been moved, the data directory is rolled back so
        the import leaves no partial state on disk.
        """
        # GIVEN a valid tarball
        backup_manager = import_env["backup_manager"]
        xlogdb_file = import_env["xlogdb_file"]
        xlogdb_file.write("")

        input_tarball = import_env["make_tarball"]()
        local_identity = {"systemid": "1234567890", "version": "15"}

        # AND the meta directory exists
        os.makedirs(backup_manager.server.meta_directory, exist_ok=True)

        # AND _import_backup_wals is patched to raise after the data
        # directory has already been moved by _import_backup_data
        with patch.object(
            BackupManager,
            "_import_backup_wals",
            side_effect=ImportBackupException("wal import failed"),
        ):
            # WHEN import_backup is called
            # THEN it raises the exception
            with pytest.raises(ImportBackupException) as exc_info:
                backup_manager.import_backup(
                    input_tarball, local_identity, "20240101T120000"
                )

            assert "wal import failed" in str(exc_info.value)

        # AND the basebackup data directory is rolled back (removed)
        target_dir = os.path.join(
            backup_manager.config.basebackups_directory, "20240101T120000"
        )
        assert not os.path.exists(target_dir)

        # AND no staging directory is left behind
        base_dir = backup_manager.config.basebackups_directory
        staging_dirs = [d for d in os.listdir(base_dir) if d.startswith(".import-")]
        assert len(staging_dirs) == 0

        # AND no backup is registered in the catalog
        assert backup_manager.get_backup("20240101T120000") is None

    def test_import_backup_missing_wals_directory_in_tarball(self, import_env):
        """
        Test that import raises ImportBackupException when the wals/
        directory is missing from the tarball.
        """
        # GIVEN a tarball without wals/ directory
        backup_manager = import_env["backup_manager"]
        input_tarball = import_env["make_tarball"](include_wals=False)
        local_identity = {"systemid": "1234567890", "version": "15"}

        # AND the meta directory exists
        os.makedirs(backup_manager.server.meta_directory, exist_ok=True)

        # WHEN import_backup is called
        # THEN an ImportBackupException is raised
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager.import_backup(
                input_tarball, local_identity, "20240101T120000"
            )

        assert "'wals/' directory" in str(exc_info.value)

    def test_import_backup_wal_conflict_blocks_import(self, import_env):
        """
        Test that import_backup fails when imported WALs conflict with
        existing WALs in the server, before any data is moved.
        """
        # GIVEN a tarball with WALs that already exist in the server
        backup_manager = import_env["backup_manager"]
        xlogdb_file = import_env["xlogdb_file"]
        input_tarball = import_env["make_tarball"]()
        local_identity = {"systemid": "1234567890", "version": "15"}

        # AND the meta directory exists
        os.makedirs(backup_manager.server.meta_directory, exist_ok=True)

        # AND the server's xlog.db already has the WALs
        xlogdb_file.write(
            "000000010000000000000001\t16\t1712994000.0\tNone\tNone\n"
            "000000010000000000000002\t16\t1712994000.0\tNone\tNone\n"
        )

        # WHEN import_backup is called
        # THEN it raises an ImportBackupException
        with pytest.raises(ImportBackupException) as exc_info:
            backup_manager.import_backup(
                input_tarball, local_identity, "20240101T120000"
            )

        assert "already exist" in str(exc_info.value).lower()

        # AND no backup data was moved (conflict detected before data move)
        target_dir = os.path.join(
            backup_manager.config.basebackups_directory, "20240101T120000"
        )
        assert not os.path.exists(target_dir)

    def test_import_backup_applies_keep_standalone_annotation(self, import_env):
        """
        Test that a successful import persists the KEEP:STANDALONE annotation
        file in the meta directory.
        """
        # GIVEN a valid export tarball
        backup_manager = import_env["backup_manager"]
        input_tarball = import_env["make_tarball"]()
        local_identity = {"systemid": "1234567890", "version": "15"}

        # AND the meta directory exists
        os.makedirs(backup_manager.server.meta_directory, exist_ok=True)

        # WHEN import_backup is called
        backup_manager.import_backup(input_tarball, local_identity, "20240101T120000")

        # THEN the annotation file exists in the meta directory
        keep_file = os.path.join(
            backup_manager.server.meta_directory, "20240101T120000-keep"
        )
        assert os.path.exists(keep_file)

        # AND its content is "standalone"
        with open(keep_file, "r") as f:
            assert f.read().strip() == KeepManager.TARGET_STANDALONE

    def test_import_backup_keep_failure_warns_but_succeeds(self, import_env, capsys):
        """
        Test that when keep_backup fails, the import still succeeds and a
        warning is emitted.
        """
        # GIVEN a valid export tarball
        backup_manager = import_env["backup_manager"]
        input_tarball = import_env["make_tarball"]()
        local_identity = {"systemid": "1234567890", "version": "15"}

        # AND the meta directory exists
        os.makedirs(backup_manager.server.meta_directory, exist_ok=True)

        # AND keep_backup is patched to raise an exception
        with patch.object(
            BackupManager,
            "keep_backup",
            side_effect=OSError("permission denied"),
        ):
            # WHEN import_backup is called
            backup_manager.import_backup(
                input_tarball, local_identity, "20240101T120000"
            )

        # THEN the import still succeeds (backup is registered)
        assert backup_manager.get_backup("20240101T120000") is not None

        # AND a warning about the annotation failure was emitted
        _, err = capsys.readouterr()
        assert "Failed to apply KEEP:STANDALONE" in err
        assert "permission denied" in err

        # AND the backup is NOT marked as kept (annotation was not applied)
        assert backup_manager.should_keep_backup("20240101T120000") is False
