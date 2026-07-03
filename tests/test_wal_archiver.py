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
import io
import logging
import os
import stat

import pytest
from mock import ANY, MagicMock, PropertyMock, call, patch
from testing_helpers import build_backup_manager, build_test_backup_info, caplog_reset

import barman.xlog
from barman.cloud_providers import ObjectKeyAlreadyExists
from barman.compression import InternalCompressor
from barman.exceptions import (
    AbortedRetryHookScript,
    ArchiverFailure,
    CommandFailedException,
    DuplicateWalFile,
    MatchingDuplicateWalFile,
)
from barman.infofile import WalFileInfo
from barman.process import ProcessInfo
from barman.server import CheckOutputStrategy
from barman.wal_archiver import (
    CloudWalArchiver,
    CloudWalStorageStrategy,
    FileWalArchiver,
    LocalWalStorageStrategy,
    ParallelWalArchiver,
    StreamingWalArchiver,
    WalArchiverQueue,
    WalPrefetchWorker,
    WalStorageStrategy,
)


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
        postgres.get_archiver_stats.return_value = {"pg_stat_archiver": "value3"}
        # Instantiate a FileWalArchiver obj
        archiver = FileWalArchiver(backup_manager)
        result = {
            "archive_mode": "value1",
            "archive_command": "value2",
            "pg_stat_archiver": "value3",
        }
        # Compare results of the check method
        assert archiver.get_remote_status() == result

    @patch("barman.wal_archiver.FileWalArchiver.get_remote_status")
    def test_check(self, remote_mock, capsys):
        """
        Test management of check_postgres view output

        :param remote_mock: mock get_remote_status function
        :param capsys: retrieve output from console
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
            "archive_mode": None,
            "archive_command": None,
        }
        # Expect no output from check
        archiver.check(strategy)
        out, err = capsys.readouterr()
        assert out == ""
        # Case: correct configuration
        remote_mock.return_value = {
            "archive_mode": "on",
            "archive_command": "wal to archive",
            "is_archiving": True,
            "incoming_wals_count": 0,
        }
        # Expect out: all parameters: OK
        archiver.check(strategy)
        out, err = capsys.readouterr()
        assert (
            out == "\tarchive_mode: OK\n"
            "\tarchive_command: OK\n"
            "\tcontinuous archiving: OK\n"
        )

        # Case: archive_command value is not acceptable
        remote_mock.return_value = {
            "archive_command": None,
            "archive_mode": "on",
            "is_archiving": False,
            "incoming_wals_count": 0,
        }
        # Expect out: some parameters: FAILED
        archiver.check(strategy)
        out, err = capsys.readouterr()
        assert (
            out == "\tarchive_mode: OK\n"
            "\tarchive_command: FAILED "
            "(please set it accordingly to documentation)\n"
        )
        # Case: all but is_archiving ok
        remote_mock.return_value = {
            "archive_mode": "on",
            "archive_command": "wal to archive",
            "is_archiving": False,
            "incoming_wals_count": 0,
        }
        # Expect out: all parameters: OK
        archiver.check(strategy)
        out, err = capsys.readouterr()
        assert (
            out == "\tarchive_mode: OK\n"
            "\tarchive_command: OK\n"
            "\tcontinuous archiving: FAILED\n"
        )
        # Case: too many wal files in the incoming queue
        archiver.config.max_incoming_wals_queue = 10
        remote_mock.return_value = {
            "archive_mode": "on",
            "archive_command": "wal to archive",
            "is_archiving": False,
            "incoming_wals_count": 20,
        }
        # Expect out: the wals incoming queue is too big
        archiver.check(strategy)
        out, err = capsys.readouterr()
        assert (
            out == "\tarchive_mode: OK\n"
            "\tarchive_command: OK\n"
            "\tcontinuous archiving: FAILED\n"
        )

    @patch("os.unlink")
    @patch("barman.wal_archiver.FileWalArchiver.get_next_batch")
    @patch("datetime.datetime")
    def test_archive(
        self,
        datetime_mock,
        get_next_batch_mock,
        unlink_mock,
        capsys,
        caplog,
    ):
        """
        Test FileWalArchiver.archive method
        """
        # See all logs
        caplog.set_level(0)

        fxlogdb_mock = MagicMock()
        backup_manager = MagicMock()
        archiver = FileWalArchiver(backup_manager)
        archiver.config.name = "test_server"
        archiver.config.errors_directory = "/server/errors"

        wal_info = WalFileInfo(name="test_wal_file")
        wal_info.orig_filename = "test_wal_file"

        batch = WalArchiverQueue([wal_info], total_size=1)
        assert batch.total_size == 1
        assert batch.run_size == 1
        get_next_batch_mock.return_value = batch
        archiver.wal_storage = MagicMock(save=MagicMock(side_effect=DuplicateWalFile))
        datetime_mock.utcnow.return_value.strftime.return_value = "test_time"

        archiver.archive(fxlogdb_mock)

        out, err = capsys.readouterr()
        assert (
            "\tError: %s is already present in server %s. "
            "File moved to errors directory." % (wal_info.name, archiver.config.name)
        ) in out

        assert (
            "\tError: %s is already present in server %s. "
            "File moved to errors directory." % (wal_info.name, archiver.config.name)
        ) in caplog.text

        archiver.wal_storage = MagicMock(
            save=MagicMock(side_effect=MatchingDuplicateWalFile)
        )
        archiver.archive(fxlogdb_mock)
        unlink_mock.assert_called_with(wal_info.orig_filename)

        # Test batch errors
        caplog_reset(caplog)
        batch.errors = ["testfile_1", "testfile_2"]
        archiver.wal_storage = MagicMock(save=MagicMock(side_effect=DuplicateWalFile))
        archiver.archive(fxlogdb_mock)
        out, err = capsys.readouterr()

        assert (
            "Some unknown objects have been found while "
            "processing xlog segments for %s. "
            "Objects moved to errors directory:" % archiver.config.name
        ) in out

        assert (
            "Archiver is about to move %s unexpected file(s) to errors "
            "directory for %s from %s"
            % (len(batch.errors), archiver.config.name, archiver.name)
        ) in caplog.text

        assert (
            "Moving unexpected file for %s from %s: %s"
            % (archiver.config.name, archiver.name, "testfile_1")
        ) in caplog.text

        assert (
            "Moving unexpected file for %s from %s: %s"
            % (archiver.config.name, archiver.name, "testfile_2")
        ) in caplog.text

        archiver.server.move_wal_file_to_errors_directory.assert_any_call(
            "testfile_1", "testfile_1", "unknown"
        )

        archiver.server.move_wal_file_to_errors_directory.assert_any_call(
            "testfile_2", "testfile_2", "unknown"
        )

    @patch("os.fsync")
    @patch("barman.wal_archiver.FileWalArchiver.get_next_batch")
    def test_archive_batch(self, get_next_batch_mock, fsync_mock, caplog):
        """
        Test archive using batch limit
        """
        # See all logs
        caplog.set_level(0)

        # Setup the test
        fxlogdb_mock = MagicMock()
        backup_manager = MagicMock()
        archiver = FileWalArchiver(backup_manager)
        archiver.wal_storage = MagicMock()
        archiver.config.name = "test_server"

        wal_info = WalFileInfo(name="test_wal_file")
        wal_info.orig_filename = "test_wal_file"
        wal_info2 = WalFileInfo(name="test_wal_file2")
        wal_info2.orig_filename = "test_wal_file2"

        # Test queue with batch of 2 and 4 in total
        batch = WalArchiverQueue([wal_info, wal_info2], total_size=4)
        assert batch.total_size == 4
        assert batch.run_size == 2

        get_next_batch_mock.return_value = batch
        archiver.archive(fxlogdb_mock)
        # check the log for messages
        assert (
            "Found %s xlog segments from %s for %s."
            " Archive a batch of %s segments in this run."
            % (batch.total_size, archiver.name, archiver.config.name, batch.run_size)
        ) in caplog.text
        assert (
            "Batch size reached (%s) - "
            "Exit %s process for %s"
            % (batch.batch_size, archiver.name, archiver.config.name)
        ) in caplog.text

    # TODO: The following test should be splitted in two
    # the BackupManager part and the FileWalArchiver part
    def test_base_archive_wal(self, tmpdir):
        """
        Basic archiving test

        Provide a WAL file and check for the correct location of the file at
        the end of the process
        """
        # Build a real backup manager
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )
        b_info = build_test_backup_info(
            backup_id="fake_backup_id",
            server=backup_manager.server,
            begin_wal="000000010000000000000001",
        )
        b_info.save()
        backup_manager.server.get_backup.return_value = b_info
        backup_manager.compression_manager.get_default_compressor.return_value = None
        backup_manager.compression_manager.get_compressor.return_value = None
        # Build the basic folder structure and files
        basedir = tmpdir.join("main")
        incoming_dir = basedir.join("incoming")
        archive_dir = basedir.join("wals")
        xlog_db = archive_dir.join("xlog.db")
        wal_name = "000000010000000000000001"
        wal_file = incoming_dir.join(wal_name)
        wal_file.ensure()
        archive_dir.ensure(dir=True)
        xlog_db.ensure()
        xlog_db_fileobj = xlog_db.open(mode="a")
        backup_manager.server.xlogdb.return_value.__enter__.return_value = (
            xlog_db_fileobj
        )
        backup_manager.server.use_wal_cloud_storage = False
        backup_manager.server.archivers = [FileWalArchiver(backup_manager)]

        backup_manager.archive_wal()
        wal_path = os.path.join(
            archive_dir.strpath, barman.xlog.hash_dir(wal_name), wal_name
        )
        # Check for the presence of the wal file in the wal catalog
        xlog_db_fileobj.flush()
        with xlog_db.open() as f:
            line = str(f.readline())
            assert wal_name in line
        # Check that the wal file have been moved from the incoming dir
        assert not os.path.exists(wal_file.strpath)
        # Check that the wal file have been archived to the expected location
        assert os.path.exists(wal_path)

    @pytest.fixture
    def mock_compression_registry(self):
        """
        Return a mock compression registry which omits the native gzip commands.
        This allows test_archive_wal to use a real compression manager without
        introducing a dependency on compression programs available in the shell.
        """
        registry = barman.compression.compression_registry.copy()
        registry.pop("gzip")
        registry.pop("pigz")
        return registry

    # TODO: The following test should be splitted in two
    # the BackupManager part and the FileWalArchiver part
    def test_archive_wal_no_backup(self, tmpdir, capsys):
        """
        Test archive-wal behaviour when there are no backups.

        Expect it to archive the files anyway
        """
        # Build a real backup manager
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )
        backup_manager.compression_manager.get_default_compressor.return_value = None
        backup_manager.compression_manager.get_compressor.return_value = None
        backup_manager.server.get_backup.return_value = None
        # Build the basic folder structure and files
        basedir = tmpdir.join("main")
        incoming_dir = basedir.join("incoming")
        archive_dir = basedir.join("wals")
        xlog_db = archive_dir.join("xlog.db")
        wal_name = "000000010000000000000001"
        wal_file = incoming_dir.join(wal_name)
        wal_file.ensure()
        archive_dir.ensure(dir=True)
        xlog_db.ensure()
        xlog_db_fileobj = xlog_db.open(mode="a")
        backup_manager.server.xlogdb.return_value.__enter__.return_value = (
            xlog_db_fileobj
        )
        backup_manager.server.use_wal_cloud_storage = False
        backup_manager.server.archivers = [FileWalArchiver(backup_manager)]

        backup_manager.archive_wal()

        # Check that the WAL file is present inside the wal catalog
        xlog_db_fileobj.flush()
        with xlog_db.open() as f:
            line = str(f.readline())
            assert wal_name in line
        wal_path = os.path.join(
            archive_dir.strpath, barman.xlog.hash_dir(wal_name), wal_name
        )
        # Check that the wal file have been archived
        assert os.path.exists(wal_path)
        out, err = capsys.readouterr()
        # Check the output for the archival of the wal file
        assert ("\t%s\n" % wal_name) in out

    # TODO: The following test should be splitted in two
    # the BackupManager part and the FileWalArchiver part
    def test_archive_wal_older_than_backup(self, tmpdir, capsys):
        """
        Test archive-wal command behaviour when the WAL files are older than
        the first backup of a server.

        Expect it to archive the files anyway
        """
        # Build a real backup manager and a fake backup
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )
        b_info = build_test_backup_info(
            backup_id="fake_backup_id",
            server=backup_manager.server,
            begin_wal="000000010000000000000002",
        )
        b_info.save()
        # Build the basic folder structure and files
        backup_manager.compression_manager.get_default_compressor.return_value = None
        backup_manager.compression_manager.get_compressor.return_value = None
        backup_manager.server.get_backup.return_value = b_info
        basedir = tmpdir.join("main")
        incoming_dir = basedir.join("incoming")
        basedir.mkdir("errors")
        archive_dir = basedir.join("wals")
        xlog_db = archive_dir.join("xlog.db")
        wal_name = "000000010000000000000001"
        wal_file = incoming_dir.join(wal_name)
        wal_file.ensure()
        archive_dir.ensure(dir=True)
        xlog_db.ensure()
        xlog_db_fileobj = xlog_db.open(mode="a")
        backup_manager.server.xlogdb.return_value.__enter__.return_value = (
            xlog_db_fileobj
        )
        backup_manager.server.use_wal_cloud_storage = False
        backup_manager.server.archivers = [FileWalArchiver(backup_manager)]

        backup_manager.archive_wal()

        # Check that the WAL file is not present inside the wal catalog
        xlog_db_fileobj.flush()
        with xlog_db.open() as f:
            line = str(f.readline())
            assert wal_name in line
        wal_path = os.path.join(
            archive_dir.strpath, barman.xlog.hash_dir(wal_name), wal_name
        )
        # Check that the wal file have been archived
        assert os.path.exists(wal_path)
        # Check the output for the archival of the wal file
        out, err = capsys.readouterr()
        assert ("\t%s\n" % wal_name) in out

    # TODO: The following test should be splitted in two
    # the BackupManager part and the FileWalArchiver part
    def test_archive_wal_timeline_lower_than_backup(self, tmpdir, capsys):
        """
        Test archive-wal command behaviour when the WAL files are older than
        the first backup of a server.

        Expect it to archive the files anyway
        """
        # Build a real backup manager and a fake backup
        backup_manager = build_backup_manager(
            name="TestServer", global_conf={"barman_home": tmpdir.strpath}
        )
        b_info = build_test_backup_info(
            backup_id="fake_backup_id",
            server=backup_manager.server,
            begin_wal="000000020000000000000002",
            timeline=2,
        )
        b_info.save()
        # Build the basic folder structure and files
        backup_manager.compression_manager.get_default_compressor.return_value = None
        backup_manager.compression_manager.get_compressor.return_value = None
        backup_manager.server.get_backup.return_value = b_info
        basedir = tmpdir.join("main")
        incoming_dir = basedir.join("incoming")
        basedir.mkdir("errors")
        archive_dir = basedir.join("wals")
        xlog_db = archive_dir.join("xlog.db")
        wal_name = "000000010000000000000001"
        wal_file = incoming_dir.join(wal_name)
        wal_file.ensure()
        archive_dir.ensure(dir=True)
        xlog_db.ensure()
        xlog_db_fileobj = xlog_db.open(mode="a")
        backup_manager.server.xlogdb.return_value.__enter__.return_value = (
            xlog_db_fileobj
        )
        backup_manager.server.use_wal_cloud_storage = False
        backup_manager.server.archivers = [FileWalArchiver(backup_manager)]

        backup_manager.archive_wal()

        # Check that the WAL file is present inside the wal catalog
        xlog_db_fileobj.flush()
        with xlog_db.open() as f:
            line = str(f.readline())
            assert wal_name in line
        wal_path = os.path.join(
            archive_dir.strpath, barman.xlog.hash_dir(wal_name), wal_name
        )
        # Check that the wal file have been archived
        assert os.path.exists(wal_path)
        # Check the output for the archival of the wal file
        out, err = capsys.readouterr()
        assert ("\t%s\n" % wal_name) in out

    @patch("barman.wal_archiver.glob")
    @patch("os.path.isfile")
    @patch("barman.wal_archiver.WalFileInfo.from_file")
    def test_get_next_batch(self, from_file_mock, isfile_mock, glob_mock):
        """
        Test the FileWalArchiver.get_next_batch method
        """

        # WAL batch no errors
        glob_mock.return_value = ["000000010000000000000001"]
        isfile_mock.return_value = True
        # This is an hack, instead of a WalFileInfo we use a simple string to
        # ease all the comparisons. The resulting string is the name enclosed
        # in colons. e.g. ":000000010000000000000001:"
        from_file_mock.side_effect = (
            lambda filename, compression_manager, unidentified_compression, encryption_manager, *args, **kwargs: (
                ":%s:" % filename
            )
        )

        backup_manager = build_backup_manager(name="TestServer")
        archiver = FileWalArchiver(backup_manager)
        backup_manager.server.archivers = [archiver]

        batch = archiver.get_next_batch()
        assert [":000000010000000000000001:"] == batch

        # WAL batch with errors
        wrong_file_name = "test_wrong_wal_file.2"
        glob_mock.return_value = ["test_wrong_wal_file.2"]
        batch = archiver.get_next_batch()
        assert [wrong_file_name] == batch.errors


# noinspection PyMethodMayBeStatic
class TestStreamingWalArchiver(object):
    def test_init(self):
        """
        Basic init test for the StreamingWalArchiver class
        """
        backup_manager = build_backup_manager()
        StreamingWalArchiver(backup_manager)

    @patch("barman.command_wrappers.PostgreSQLClient.find_command")
    def test_check_receivexlog_installed(self, find_command):
        """
        Test for the check method of the StreamingWalArchiver class
        """
        backup_manager = build_backup_manager()
        find_command.side_effect = CommandFailedException

        archiver = StreamingWalArchiver(backup_manager)
        result = archiver.get_remote_status()

        assert result == {
            "pg_receivexlog_installed": False,
            "pg_receivexlog_path": None,
            "pg_receivexlog_compatible": None,
            "pg_receivexlog_synchronous": None,
            "pg_receivexlog_version": None,
            "pg_receivexlog_supports_slots": None,
        }

        backup_manager.server.streaming.server_major_version = "9.2"
        find_command.side_effect = None
        find_command.return_value.cmd = "/some/path/to/pg_receivexlog"
        find_command.return_value.out = ""
        archiver.reset_remote_status()
        result = archiver.get_remote_status()

        assert result == {
            "pg_receivexlog_installed": True,
            "pg_receivexlog_path": "/some/path/to/pg_receivexlog",
            "pg_receivexlog_compatible": None,
            "pg_receivexlog_synchronous": None,
            "pg_receivexlog_version": None,
            "pg_receivexlog_supports_slots": None,
        }

    @patch("barman.utils.which")
    @patch("barman.command_wrappers.Command")
    def test_check_receivexlog_is_compatible(self, command_mock, which_mock):
        """
        Test for the compatibility checks between versions of pg_receivexlog
        and PostgreSQL
        """
        # pg_receivexlog 9.2 is compatible only with PostgreSQL 9.2
        backup_manager = build_backup_manager()
        backup_manager.server.streaming.server_major_version = "9.2"
        archiver = StreamingWalArchiver(backup_manager)
        which_mock.return_value = "/some/path/to/pg_receivexlog"

        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.2.1"
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is True

        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.5.3"
        archiver.reset_remote_status()
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is False

        # Every pg_receivexlog is compatible with older PostgreSQL
        backup_manager.server.streaming.server_major_version = "9.3"
        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.5.3"
        archiver.reset_remote_status()
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is True

        backup_manager.server.streaming.server_major_version = "9.5"
        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.3.0"
        archiver.reset_remote_status()
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is False

        # Check for minor versions
        backup_manager.server.streaming.server_major_version = "9.4"
        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.4.4"
        archiver.reset_remote_status()
        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is True
        assert result["pg_receivexlog_synchronous"] is False

    @patch("barman.wal_archiver.StreamingWalArchiver.get_remote_status")
    @patch("barman.wal_archiver.PgReceiveXlog")
    def test_receive_wal(self, receivexlog_mock, remote_mock, tmpdir):
        backup_manager = build_backup_manager(
            main_conf={"backup_directory": tmpdir},
        )
        streaming_mock = backup_manager.server.streaming
        streaming_mock.server_txt_version = "9.4.0"
        streaming_mock.get_connection_string.return_value = (
            "host=pg01.nowhere user=postgres port=5432 "
            "application_name=barman_receive_wal"
        )
        streaming_mock.get_remote_status.return_value = {
            "streaming_supported": True,
            "timeline": 1,
        }
        postgres_mock = backup_manager.server.postgres
        replication_slot_status = MagicMock(restart_lsn="F/A12D687", active=False)
        postgres_mock.get_remote_status.return_value = {
            "current_xlog": "000000010000000F0000000A",
            "current_lsn": "F/A12D687",
            "replication_slot": replication_slot_status,
            "xlog_segment_size": 16777216,
        }
        backup_manager.server.streaming.conn_parameters = {
            "host": "pg01.nowhere",
            "user": "postgres",
            "port": "5432",
        }
        streaming_dir = tmpdir.join("streaming")
        streaming_dir.ensure(dir=True)
        # Test: normal run
        archiver = StreamingWalArchiver(backup_manager)
        archiver.server.streaming.server_version = 90400
        remote_mock.return_value = {
            "pg_receivexlog_installed": True,
            "pg_receivexlog_compatible": True,
            "pg_receivexlog_synchronous": None,
            "pg_receivexlog_path": "fake/path",
            "pg_receivexlog_supports_slots": True,
            "pg_receivexlog_version": "9.4",
        }

        # Test: execute a reset request
        partial = streaming_dir.join("000000010000000100000001.partial")
        partial.ensure()
        archiver.receive_wal(reset=True)
        assert not partial.check()
        assert streaming_dir.join("000000010000000F0000000A.partial").check()

        archiver.receive_wal(reset=False)
        receivexlog_mock.assert_called_once_with(
            app_name="barman_receive_wal",
            synchronous=None,
            connection=ANY,
            destination=streaming_dir.strpath,
            err_handler=ANY,
            out_handler=ANY,
            path=ANY,
            slot_name=None,
            command="fake/path",
            version="9.4",
        )
        receivexlog_mock.return_value.execute.assert_called_once_with()

        # Test: pg_receivexlog from 9.2
        receivexlog_mock.reset_mock()
        remote_mock.return_value = {
            "pg_receivexlog_installed": True,
            "pg_receivexlog_compatible": True,
            "pg_receivexlog_synchronous": False,
            "pg_receivexlog_path": "fake/path",
            "pg_receivexlog_supports_slots": False,
            "pg_receivexlog_version": "9.2",
        }
        archiver.receive_wal(reset=False)
        receivexlog_mock.assert_called_once_with(
            app_name="barman_receive_wal",
            synchronous=False,
            connection=ANY,
            destination=streaming_dir.strpath,
            err_handler=ANY,
            out_handler=ANY,
            path=ANY,
            command="fake/path",
            slot_name=None,
            version="9.2",
        )
        receivexlog_mock.return_value.execute.assert_called_once_with()

        # Test: incompatible pg_receivexlog
        with pytest.raises(ArchiverFailure):
            remote_mock.return_value = {
                "pg_receivexlog_installed": True,
                "pg_receivexlog_compatible": False,
                "pg_receivexlog_supports_slots": False,
                "pg_receivexlog_synchronous": False,
                "pg_receivexlog_path": "fake/path",
            }
            archiver.receive_wal()

        # Test: missing pg_receivexlog
        with pytest.raises(ArchiverFailure):
            remote_mock.return_value = {
                "pg_receivexlog_installed": False,
                "pg_receivexlog_compatible": True,
                "pg_receivexlog_supports_slots": False,
                "pg_receivexlog_synchronous": False,
                "pg_receivexlog_path": "fake/path",
            }
            archiver.receive_wal()
        # Test: impossible to connect with streaming protocol
        with pytest.raises(ArchiverFailure):
            backup_manager.server.streaming.get_remote_status.return_value = {
                "streaming_supported": None
            }
            remote_mock.return_value = {
                "pg_receivexlog_installed": True,
                "pg_receivexlog_supports_slots": False,
                "pg_receivexlog_compatible": True,
                "pg_receivexlog_synchronous": False,
                "pg_receivexlog_path": "fake/path",
            }
            archiver.receive_wal()
        # Test: PostgreSQL too old
        with pytest.raises(ArchiverFailure):
            backup_manager.server.streaming.get_remote_status.return_value = {
                "streaming_supported": False
            }
            remote_mock.return_value = {
                "pg_receivexlog_installed": True,
                "pg_receivexlog_compatible": True,
                "pg_receivexlog_synchronous": False,
                "pg_receivexlog_path": "fake/path",
            }
            archiver.receive_wal()
        # Test: general failure executing pg_receivexlog
        with pytest.raises(ArchiverFailure):
            remote_mock.return_value = {
                "pg_receivexlog_installed": True,
                "pg_receivexlog_compatible": True,
                "pg_receivexlog_synchronous": False,
                "pg_receivexlog_path": "fake/path",
            }
            receivexlog_mock.return_value.execute.side_effect = CommandFailedException
            archiver.receive_wal()

    @patch("barman.utils.which")
    @patch("barman.command_wrappers.Command")
    def test_when_streaming_connection_rejected(self, command_mock, which_mock):
        """
        Test the StreamingWalArchiver behaviour when the streaming
        connection is rejected by the PostgreSQL server and
        pg_receivexlog is installed.
        """

        # When the streaming connection is not available, the
        # server_txt_version property will have a None value.
        backup_manager = build_backup_manager()
        backup_manager.server.streaming.server_major_version = None
        archiver = StreamingWalArchiver(backup_manager)
        which_mock.return_value = "/some/path/to/pg_receivexlog"
        command_mock.return_value.out = "pg_receivexlog (PostgreSQL) 9.2"

        result = archiver.get_remote_status()
        assert result["pg_receivexlog_compatible"] is None

    @patch("barman.wal_archiver.StreamingWalArchiver.get_remote_status")
    def test_check(self, remote_mock, capsys):
        """
        Test management of check_postgres view output

        :param remote_mock: mock get_remote_status function
        :param capsys: retrieve output from console
        """
        # Create a backup_manager
        backup_manager = build_backup_manager()
        # Set up mock responses
        streaming = backup_manager.server.streaming
        streaming.server_txt_version = "9.5"
        # Instantiate a StreamingWalArchiver obj
        archiver = StreamingWalArchiver(backup_manager)
        # Prepare the output check strategy
        strategy = CheckOutputStrategy()
        # Case: correct configuration
        remote_mock.return_value = {
            "pg_receivexlog_installed": True,
            "pg_receivexlog_compatible": True,
            "pg_receivexlog_path": "fake/path",
            "incoming_wals_count": 0,
        }
        # Expect out: all parameters: OK
        backup_manager.server.process_manager.list.return_value = []
        archiver.check(strategy)
        out, err = capsys.readouterr()
        assert (
            out == "\tpg_receivexlog: OK\n"
            "\tpg_receivexlog compatible: OK\n"
            "\treceive-wal running: FAILED "
            "(See the Barman log file for more details)\n"
        )

        # Case: pg_receivexlog is not compatible
        remote_mock.return_value = {
            "pg_receivexlog_installed": True,
            "pg_receivexlog_compatible": False,
            "pg_receivexlog_path": "fake/path",
            "pg_receivexlog_version": "9.2",
            "incoming_wals_count": 0,
        }
        # Expect out: some parameters: FAILED
        strategy = CheckOutputStrategy()
        archiver.check(strategy)
        out, err = capsys.readouterr()
        assert (
            out == "\tpg_receivexlog: OK\n"
            "\tpg_receivexlog compatible: FAILED "
            "(PostgreSQL version: 9.5, pg_receivexlog version: 9.2)\n"
            "\treceive-wal running: FAILED "
            "(See the Barman log file for more details)\n"
        )
        # Case: pg_receivexlog returned error
        remote_mock.return_value = {
            "pg_receivexlog_installed": True,
            "pg_receivexlog_compatible": None,
            "pg_receivexlog_path": "fake/path",
            "pg_receivexlog_version": None,
            "incoming_wals_count": 0,
        }
        # Expect out: all parameters: OK
        archiver.check(strategy)
        out, err = capsys.readouterr()
        assert (
            out == "\tpg_receivexlog: OK\n"
            "\tpg_receivexlog compatible: FAILED "
            "(PostgreSQL version: 9.5, pg_receivexlog version: None)\n"
            "\treceive-wal running: FAILED "
            "(See the Barman log file for more details)\n"
        )

        # Case: receive-wal running
        backup_manager.server.process_manager.list.return_value = [
            ProcessInfo(
                pid=1, server_name=backup_manager.config.name, task="receive-wal"
            )
        ]
        archiver.check(strategy)
        out, err = capsys.readouterr()
        assert (
            out == "\tpg_receivexlog: OK\n"
            "\tpg_receivexlog compatible: FAILED "
            "(PostgreSQL version: 9.5, pg_receivexlog version: None)\n"
            "\treceive-wal running: OK\n"
        )

        # Case: streaming connection not configured
        backup_manager.server.streaming = None
        archiver.check(strategy)
        out, err = capsys.readouterr()
        assert (
            out == "\tpg_receivexlog: OK\n"
            "\tpg_receivexlog compatible: FAILED "
            "(PostgreSQL version: Unknown, pg_receivexlog version: None)\n"
            "\treceive-wal running: OK\n"
        )
        # Case: too many wal files in the incoming queue
        archiver.config.max_incoming_wals_queue = 10
        remote_mock.return_value = {
            "pg_receivexlog_installed": True,
            "pg_receivexlog_compatible": None,
            "pg_receivexlog_path": "fake/path",
            "pg_receivexlog_version": None,
            "incoming_wals_count": 20,
        }
        # Expect out: the wals incoming queue is too big
        archiver.check(strategy)
        out, err = capsys.readouterr()
        assert (
            out == "\tpg_receivexlog: OK\n"
            "\tpg_receivexlog compatible: FAILED "
            "(PostgreSQL version: Unknown, pg_receivexlog version: None)\n"
            "\treceive-wal running: OK\n"
        )

    @patch("barman.wal_archiver.glob")
    @patch("os.path.exists")
    @patch("os.path.isfile")
    @patch("barman.wal_archiver.WalFileInfo.from_file")
    def test_get_next_batch(
        self, from_file_mock, isfile_mock, exists_mock, glob_mock, caplog
    ):
        """
        Test the FileWalArchiver.get_next_batch method
        """
        # See all logs
        caplog.set_level(0)

        # WAL batch, with 000000010000000000000001 that is currently being
        # written
        glob_mock.return_value = ["000000010000000000000001"]
        isfile_mock.return_value = True
        # This is an hack, instead of a WalFileInfo we use a simple string to
        # ease all the comparisons. The resulting string is the name enclosed
        # in colons. e.g. ":000000010000000000000001:"
        from_file_mock.side_effect = (
            lambda filename, compression_manager, unidentified_compression, encryption_manager, *args, **kwargs: (
                ":%s:" % filename
            )
        )

        backup_manager = build_backup_manager(name="TestServer")
        archiver = StreamingWalArchiver(backup_manager)
        backup_manager.server.archivers = [archiver]

        caplog_reset(caplog)
        batch = archiver.get_next_batch()
        assert ["000000010000000000000001"] == batch.skip
        assert "" == caplog.text

        # WAL batch, with 000000010000000000000002 that is currently being
        # written and 000000010000000000000001 can be archived
        caplog_reset(caplog)
        glob_mock.return_value = [
            "000000010000000000000001",
            "000000010000000000000002",
        ]
        batch = archiver.get_next_batch()
        assert [":000000010000000000000001:"] == batch
        assert ["000000010000000000000002"] == batch.skip
        assert "" == caplog.text

        # WAL batch, with two partial files.
        caplog_reset(caplog)
        glob_mock.return_value = [
            "000000010000000000000001.partial",
            "000000010000000000000002.partial",
        ]
        batch = archiver.get_next_batch()
        assert [":000000010000000000000001.partial:"] == batch
        assert ["000000010000000000000002.partial"] == batch.skip
        assert (
            "Archiving partial files for server %s: "
            "000000010000000000000001.partial" % archiver.config.name
        ) in caplog.text

        # WAL batch, with history files.
        caplog_reset(caplog)
        glob_mock.return_value = [
            "00000001.history",
            "000000010000000000000002.partial",
        ]
        batch = archiver.get_next_batch()
        assert [":00000001.history:"] == batch
        assert ["000000010000000000000002.partial"] == batch.skip
        assert "" == caplog.text

        # WAL batch with errors
        wrong_file_name = "test_wrong_wal_file.2"
        glob_mock.return_value = ["test_wrong_wal_file.2"]
        batch = archiver.get_next_batch()
        assert [wrong_file_name] == batch.errors

        # WAL batch, with two partial files, but one has been just renamed.
        caplog_reset(caplog)
        exists_mock.side_effect = [False, True]
        glob_mock.return_value = [
            "000000010000000000000001.partial",
            "000000010000000000000002.partial",
        ]
        batch = archiver.get_next_batch()
        assert len(batch) == 0
        assert ["000000010000000000000002.partial"] == batch.skip
        assert "" in caplog.text

    def test_is_synchronous(self):
        backup_manager = build_backup_manager(name="TestServer")
        archiver = StreamingWalArchiver(backup_manager)

        # 'barman_receive_wal' is not in the list of synchronous standby
        # names, so we expect is_synchronous to be false
        backup_manager.server.postgres.get_remote_status.return_value = {
            "synchronous_standby_names": ["a", "b", "c"]
        }
        assert not archiver._is_synchronous()

        # 'barman_receive_wal' is in the list of synchronous standby
        # names, so we expect is_synchronous to be true
        backup_manager.server.postgres.get_remote_status.return_value = {
            "synchronous_standby_names": ["a", "barman_receive_wal"]
        }
        assert archiver._is_synchronous()

        # '*' is in the list of synchronous standby names, so we expect
        # is_synchronous to be true even if 'barman_receive_wal' is not
        # explicitly referenced
        backup_manager.server.postgres.get_remote_status.return_value = {
            "synchronous_standby_names": ["a", "b", "*"]
        }
        assert archiver._is_synchronous()

        # There is only a '*' in the list of synchronous standby names,
        # so we expect every name to match
        backup_manager.server.postgres.get_remote_status.return_value = {
            "synchronous_standby_names": ["*"]
        }
        assert archiver._is_synchronous()


class TestWalStorageStrategy:
    """
    Tests for the :class:`WalStorageStrategy` abstract class.

    .. note::
        As :class:`WalStorageStrategy` is an abstract class, we need to
        patch the ``__abstractmethods__`` attribute in most tests to be able to
        instantiate it.
    """

    @patch("barman.wal_archiver.RetryHookScriptRunner")
    @patch("barman.wal_archiver.HookScriptRunner")
    @patch(
        "barman.wal_archiver.WalStorageStrategy.__abstractmethods__", new_callable=set
    )
    def test_run_pre_archive_scripts(self, _, mock_hook_script, mock_retry_hook_script):
        """Test that the pre-archive scripts are run correctly when present"""
        backup_manager = build_backup_manager(name="TestServer")
        wal_storage = WalStorageStrategy(backup_manager, backup_manager.server)

        # WHEN _run_pre_archive_scripts is called
        arg_wal_info, arg_src_path = MagicMock(), "/mock/src/path"
        wal_storage._run_pre_archive_scripts(arg_wal_info, arg_src_path)

        # THEN the pre-archive hook script is instantiated and run correctly
        mock_hook_script.assert_called_once_with(
            backup_manager, "archive_script", "pre"
        )
        mock_hook_script.return_value.env_from_wal_info.assert_called_once_with(
            arg_wal_info, arg_src_path
        )
        mock_hook_script.return_value.run.assert_called_once()

        # AND the pre-archive retry hook script is instantiated and run correctly
        mock_retry_hook_script.assert_called_once_with(
            backup_manager, "archive_retry_script", "pre"
        )
        mock_retry_hook_script.return_value.env_from_wal_info.assert_called_once_with(
            arg_wal_info, arg_src_path
        )
        mock_retry_hook_script.return_value.run.assert_called_once()

    @patch("barman.wal_archiver.RetryHookScriptRunner")
    @patch("barman.wal_archiver.HookScriptRunner")
    @patch(
        "barman.wal_archiver.WalStorageStrategy.__abstractmethods__", new_callable=set
    )
    def test_run_post_archive_scripts(
        self, _, mock_hook_script, mock_retry_hook_script
    ):
        """Test that the post-archive scripts are run correctly when present"""
        backup_manager = build_backup_manager(name="TestServer")
        wal_storage = WalStorageStrategy(backup_manager, backup_manager.server)

        # WHEN _run_post_archive_scripts is called
        arg_wal_info, arg_src_path, arg_error = MagicMock(), "/mock/src/path", None
        wal_storage._run_post_archive_scripts(arg_wal_info, arg_src_path, arg_error)

        # THEN the post-archive retry hook script is instantiated and run correctly
        mock_retry_hook_script.assert_called_once_with(
            backup_manager, "archive_retry_script", "post"
        )
        mock_retry_hook_script.env_from_wal_info(arg_wal_info, arg_src_path, arg_error)
        mock_retry_hook_script.run()

        # AND the post-archive hook script is instantiated and run correctly
        mock_hook_script.assert_called_once_with(
            backup_manager, "archive_script", "post", arg_error
        )
        mock_hook_script.env_from_wal_info(arg_wal_info, arg_src_path)
        mock_hook_script.run()

    @patch("barman.wal_archiver._logger")
    @patch("barman.wal_archiver.RetryHookScriptRunner")
    @patch("barman.wal_archiver.HookScriptRunner")
    @patch(
        "barman.wal_archiver.WalStorageStrategy.__abstractmethods__", new_callable=set
    )
    def test_run_post_archive_scripts_with_error(
        self, _, mock_hook_script, mock_retry_hook_script, mock_logger
    ):
        """
        Test that the post-archive scripts are run correctly when an error occurred.

        That hapens when the retry hook script raises an exec:`AbortedRetryHookScript`
        exception.
        """
        backup_manager = build_backup_manager(name="TestServer")
        wal_storage = WalStorageStrategy(backup_manager, backup_manager.server)

        # Mock the retry hook script to raise AbortedRetryHookScript when run
        mock_retry_hook_script.return_value.run.side_effect = AbortedRetryHookScript(
            hook=mock_retry_hook_script.return_value
        )

        # WHEN _run_post_archive_scripts is called
        arg_wal_info, arg_src_path, arg_error = MagicMock(), "/mock/src/path", None
        wal_storage._run_post_archive_scripts(arg_wal_info, arg_src_path, arg_error)

        # THEN the post-archive retry hook script is instantiated and run correctly
        # AND the AbortedRetryHookScript exception is catched and logged
        mock_retry_hook_script.assert_called_once_with(
            backup_manager, "archive_retry_script", "post"
        )
        mock_retry_hook_script.return_value.env_from_wal_info.assert_called_once_with(
            arg_wal_info, arg_src_path, arg_error
        )
        mock_retry_hook_script.return_value.run.assert_called_once()
        mock_logger.warning.assert_called_once_with(
            "Ignoring stop request after receiving "
            "abort (exit code %d) from post-archive "
            "retry hook script: %s",
            mock_retry_hook_script.return_value.exit_status,
            mock_retry_hook_script.return_value.script,
        )

        # AND the post-archive hook script is instantiated and run correctly
        # regardless of the error in the previous retry hook script
        mock_hook_script.assert_called_once_with(
            backup_manager, "archive_script", "post", arg_error
        )
        mock_hook_script.env_from_wal_info(arg_wal_info, arg_src_path)
        mock_hook_script.run()

    @patch("barman.wal_archiver.RetryHookScriptRunner")
    @patch("barman.wal_archiver.HookScriptRunner")
    @patch(
        "barman.wal_archiver.WalStorageStrategy.__abstractmethods__", new_callable=set
    )
    def test_run_pre_delete_wal_scripts(
        self, _, mock_hook_script, mock_retry_hook_script
    ):
        """Test that the pre-delete scripts are run correctly when present"""
        backup_manager = build_backup_manager(name="TestServer")
        wal_storage = WalStorageStrategy(backup_manager, backup_manager.server)

        # WHEN _run_pre_delete_wal_scripts is called
        arg_wal_info = MagicMock()
        wal_storage._run_pre_delete_wal_scripts(arg_wal_info)

        # THEN the pre-archive hook script is instantiated and run correctly
        mock_hook_script.assert_called_once_with(
            backup_manager, "wal_delete_script", "pre"
        )
        mock_hook_script.return_value.env_from_wal_info.assert_called_once_with(
            arg_wal_info
        )
        mock_hook_script.return_value.run.assert_called_once()

        # AND the pre-archive retry hook script is instantiated and run correctly
        mock_retry_hook_script.assert_called_once_with(
            backup_manager, "wal_delete_retry_script", "pre"
        )
        mock_retry_hook_script.return_value.env_from_wal_info.assert_called_once_with(
            arg_wal_info
        )
        mock_retry_hook_script.return_value.run.assert_called_once()

    @patch("barman.wal_archiver.RetryHookScriptRunner")
    @patch("barman.wal_archiver.HookScriptRunner")
    @patch(
        "barman.wal_archiver.WalStorageStrategy.__abstractmethods__", new_callable=set
    )
    def test_run_post_delete_wal_scripts(
        self, _, mock_hook_script, mock_retry_hook_script
    ):
        """Test that the post-delete scripts are run correctly when present"""
        backup_manager = build_backup_manager(name="TestServer")
        wal_storage = WalStorageStrategy(backup_manager, backup_manager.server)

        # WHEN _run_post_delete_wal_scripts is called
        arg_wal_info, arg_error = MagicMock(), None
        wal_storage._run_post_delete_wal_scripts(arg_wal_info, arg_error)

        # THEN the post-delete retry hook script is instantiated and run correctly
        mock_retry_hook_script.assert_called_once_with(
            backup_manager, "wal_delete_retry_script", "post"
        )
        mock_retry_hook_script.return_value.env_from_wal_info.assert_called_once_with(
            arg_wal_info, None, arg_error
        )
        mock_retry_hook_script.return_value.run.assert_called_once()

        # AND the post-delete hook script is instantiated and run correctly
        mock_hook_script.assert_called_once_with(
            backup_manager, "wal_delete_script", "post"
        )
        mock_hook_script.return_value.env_from_wal_info.assert_called_once_with(
            arg_wal_info, None, arg_error
        )
        mock_hook_script.return_value.run.assert_called_once()

    @patch("barman.wal_archiver._logger")
    @patch("barman.wal_archiver.RetryHookScriptRunner")
    @patch("barman.wal_archiver.HookScriptRunner")
    @patch(
        "barman.wal_archiver.WalStorageStrategy.__abstractmethods__", new_callable=set
    )
    def test_run_post_delete_wal_scripts_with_error(
        self, _, mock_hook_script, mock_retry_hook_script, mock_logger
    ):
        """
        Test that the post-delete scripts are run correctly when an error occurred.

        That happens when the retry hook script raises an exec:`AbortedRetryHookScript`
        exception.
        """
        backup_manager = build_backup_manager(name="TestServer")
        wal_storage = WalStorageStrategy(backup_manager, backup_manager.server)

        # Mock the retry hook script to raise AbortedRetryHookScript when run
        mock_retry_hook_script.return_value.run.side_effect = AbortedRetryHookScript(
            hook=mock_retry_hook_script.return_value
        )

        # WHEN _run_post_delete_wal_scripts is called
        arg_wal_info, arg_error = MagicMock(), None
        wal_storage._run_post_delete_wal_scripts(arg_wal_info, arg_error)

        # THEN the post-delete retry hook script is instantiated and run correctly
        # AND the AbortedRetryHookScript exception is caught and logged
        mock_retry_hook_script.assert_called_once_with(
            backup_manager, "wal_delete_retry_script", "post"
        )
        mock_retry_hook_script.return_value.env_from_wal_info.assert_called_once_with(
            arg_wal_info, None, arg_error
        )
        mock_retry_hook_script.return_value.run.assert_called_once()
        mock_logger.warning.assert_called_once_with(
            "Ignoring stop request after receiving "
            "abort (exit code %d) from post-wal-delete "
            "retry hook script: %s",
            mock_retry_hook_script.return_value.exit_status,
            mock_retry_hook_script.return_value.script,
        )

        # AND the post-delete hook script is instantiated and run correctly
        # regardless of the error in the previous retry hook script
        mock_hook_script.assert_called_once_with(
            backup_manager, "wal_delete_script", "post"
        )
        mock_hook_script.return_value.env_from_wal_info.assert_called_once_with(
            arg_wal_info, None, arg_error
        )
        mock_hook_script.return_value.run.assert_called_once()


class TestLocalWalStorageStrategy:
    """Tests for the :class:`LocalWalStorageStrategy` class"""

    @patch("barman.wal_archiver.os.path.exists", return_value=False)
    def test_check_duplicate_file_not_exists(self, _):
        """
        Test that no exception is raised when there is no duplicate file
        at the destination path.
        """
        backup_manager = build_backup_manager(name="TestServer")
        wal_storage = LocalWalStorageStrategy(backup_manager, backup_manager.server)
        # WHEN _check_duplicate is called for a file that does not exist
        arg_src, arg_dest, arg_wal_info = "/src/path", "/dest/path", MagicMock()
        # THEN no exception is raised
        wal_storage._check_duplicate(arg_src, arg_dest, arg_wal_info)

    @patch("barman.wal_archiver.filecmp.cmp", return_value=False)
    @patch("barman.wal_archiver.os.path.exists", return_value=True)
    def test_check_duplicate_file_exists_with_different_content(self, _, mock_cmp):
        """
        Test that a exec:`DuplicateWalFile` exception is raised when a file
        with different content exists at the destination path.
        """
        backup_manager = build_backup_manager(name="TestServer")
        wal_storage = LocalWalStorageStrategy(backup_manager, backup_manager.server)
        # Mock the get_wal_file_info to return a wal_info representing a destination
        # file without compression and encryption as we're not testing that here
        backup_manager.get_wal_file_info = MagicMock(
            return_value=MagicMock(compression=None, encryption=None)
        )
        # Prepare the arguments for _check_duplicate
        # Mock the src wal_info to represent a source file without compression and encryption
        arg_src, arg_dest, arg_wal_info = (
            "/src/path",
            "/dest/path",
            MagicMock(compression=None, encryption=None),
        )

        # WHEN _check_duplicate is called
        # THEN a DuplicateWalFile exception is raised, as the contents are different
        with pytest.raises(DuplicateWalFile):
            wal_storage._check_duplicate(arg_src, arg_dest, arg_wal_info)
            # Also assert that filecmp.cmp was called with the correct arguments
            mock_cmp.assert_called_once_with(arg_src, arg_dest)

    @patch("barman.wal_archiver.filecmp.cmp", return_value=True)
    @patch("barman.wal_archiver.os.path.exists", return_value=True)
    def test_check_duplicate_file_exists_with_same_content(self, _, mock_cmp):
        """
        Test that a exec:`MatchingDuplicateWalFile` exception is raised when a file
        with the same content exists at the destination path.
        """
        backup_manager = build_backup_manager(name="TestServer")
        wal_storage = LocalWalStorageStrategy(backup_manager, backup_manager.server)
        # Mock the get_wal_file_info to return a wal_info representing a destination
        # file without compression and encryption as we're not testing that here
        backup_manager.get_wal_file_info = MagicMock(
            return_value=MagicMock(compression=None, encryption=None)
        )
        # Prepare the arguments for _check_duplicate
        # Mock the src wal_info to represent a source file without compression and encryption
        arg_src, arg_dest, arg_wal_info = (
            "/src/path",
            "/dest/path",
            MagicMock(compression=None, encryption=None),
        )

        # WHEN _check_duplicate is called
        # THEN a MatchingDuplicateWalFile exception is raised, as the contents are the same
        with pytest.raises(MatchingDuplicateWalFile):
            wal_storage._check_duplicate(arg_src, arg_dest, arg_wal_info)
            # Also assert that filecmp.cmp was called with the correct arguments
            mock_cmp.assert_called_once_with(arg_src, arg_dest)

    @patch("barman.wal_archiver.os.path.exists", return_value=True)
    def test_check_duplicate_file_exists_with_encryption(self, _):
        """
        Test that a exec:`DuplicateWalFile` exception is raised when a file
        with encryption exists at the destination path.
        """
        backup_manager = build_backup_manager(name="TestServer")
        wal_storage = LocalWalStorageStrategy(backup_manager, backup_manager.server)
        # Mock the get_wal_file_info to return a wal_info representing a destination
        # file with encryption
        backup_manager.get_wal_file_info = MagicMock(
            return_value=MagicMock(compression=None, encryption="gpg")
        )
        # Mock the src wal_info to represent a source file with encryption
        arg_src, arg_dest, arg_wal_info = (
            "/src/path",
            "/dest/path",
            MagicMock(compression=None, encryption="gpg"),
        )

        # WHEN _check_duplicate is called
        # THEN a DuplicateWalFile exception is raised, as we cannot compare encrypted files
        with pytest.raises(DuplicateWalFile):
            wal_storage._check_duplicate(arg_src, arg_dest, arg_wal_info)

    @pytest.mark.parametrize("cmp_result", [True, False])
    @patch("barman.wal_archiver.os.unlink")
    @patch("barman.wal_archiver.filecmp.cmp")
    @patch("barman.wal_archiver.os.path.exists", return_value=True)
    def test_check_duplicate_file_exists_with_compression(
        self, _, mock_unlink, mock_cmp, cmp_result
    ):
        """
        Test that compressed files are decompressed correctly before comparison.

        Note that the exact exception raised is not the focus here.
        """
        mock_cmp.return_value = cmp_result
        backup_manager = build_backup_manager(name="TestServer")
        backup_manager.compression_manager = MagicMock()
        wal_storage = LocalWalStorageStrategy(backup_manager, backup_manager.server)
        # Mock the get_wal_file_info to return a wal_info representing a destination
        # file with compression
        backup_manager.get_wal_file_info = MagicMock(
            return_value=MagicMock(compression="gzip", encryption=None)
        )
        # Prepare the arguments for _check_duplicate
        # Mock the src wal_info to represent a source file with compression
        arg_src, arg_dest, arg_wal_info = (
            "/src/path",
            "/dest/path",
            MagicMock(compression="gzip", encryption=None),
        )
        # Prepare the expected temporary uncompressed file paths
        src_uncompressed, dest_uncompressed = "/src/uncompressed", "/dest/uncompressed"

        # WHEN _check_duplicate is called
        # THEN the appropriate exception is raised, according to the result of filecmp.cmp
        # Note: we don't care about the exact exception raised here, the important part
        # is ensuring that the decompression and comparison were done correctly
        ex = MatchingDuplicateWalFile if cmp_result else DuplicateWalFile
        with pytest.raises(ex):
            wal_storage._check_duplicate(arg_src, arg_dest, arg_wal_info)
            # AND the compressed src and dst files were decompressed correctly
            backup_manager.compression_manager.get_compressor.return_value.decompress.assert_called_once_with(
                arg_dest, dest_uncompressed
            )
            backup_manager.compression_manager.get_compressor.return_value.decompress.assert_called_once_with(
                arg_dest, src_uncompressed
            )
            # AND filecmp.cmp was called with the correct arguments
            mock_cmp.assert_called_once_with(src_uncompressed, dest_uncompressed)
            # AND the temporary uncompressed files were removed
            mock_unlink.assert_called_once_with(src_uncompressed)
            mock_unlink.assert_called_once_with(dest_uncompressed)

    def test_compress_file(self):
        """
        Test that :meth:`compress_file` correctly compresses and returns a file.
        """
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        mock_compressor = MagicMock(compression="gzip")
        mock_wal_info = MagicMock()

        # WHEN _compress_file is called
        result = wal_storage._compress_file(
            mock_compressor, "/src/000000010000000000000001", "/dest/dir", mock_wal_info
        )
        # THEN the compressor's compress method is called with correct arguments
        mock_compressor.compress.assert_called_once_with(
            "/src/000000010000000000000001",
            "/dest/dir/000000010000000000000001.compressed",
        )
        # AND the previous source file is appended to the removal list
        assert wal_storage.files_to_remove == ["/src/000000010000000000000001"]
        # AND the compression method is set in the wal_info object
        assert mock_wal_info.compression == "gzip"
        # AND the correct compressed file path is returned
        assert result == "/dest/dir/000000010000000000000001.compressed"

    def test_encrypt_file(self):
        """
        Test that :meth:`encrypt_file` correctly encrypts and returns a file.
        """
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        mock_encryption = MagicMock(NAME="gpg")
        mock_wal_info = MagicMock()

        # WHEN _encrypt_file is called
        result = wal_storage._encrypt_file(
            mock_encryption,
            "/src/000000010000000000000001",
            "/dest/dir",
            mock_wal_info,
        )
        # THEN the encryption's encrypt method is called with correct arguments
        mock_encryption.encrypt.assert_called_once_with(
            "/src/000000010000000000000001",
            "/dest/dir",
        )
        # AND the previous source file is appended to the removal list
        assert wal_storage.files_to_remove == ["/src/000000010000000000000001"]
        # AND the encryption method is set in the wal_info object
        assert mock_wal_info.encryption == "gpg"
        # AND the encrypted file path is returned
        assert result == mock_encryption.encrypt.return_value

    @patch("barman.wal_archiver.os.stat")
    @patch("barman.wal_archiver.shutil.copystat")
    def test_copy_stats(self, mock_copystat, mock_stat):
        """
        Test that :meth:`copy_stats` correctly copies file stats from the source
        file to the current file and updates the ``wal_info`` size stats.
        """
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        mock_wal_info = MagicMock()
        src_file, current_file = "/src/path/file1", "/dest/path/file2"

        # WHEN _copy_stats is called
        wal_storage._copy_stats(src_file, current_file, mock_wal_info)
        # THEN the file stats are copied from the source to the current file
        mock_copystat.assert_called_once_with(src_file, current_file)
        # AND the wal_info size is updated with the size of the current file
        mock_stat.assert_called_with(current_file)
        assert mock_wal_info.size == mock_stat.return_value.st_size

    @patch("barman.wal_archiver.fsync_dir")
    @patch("barman.wal_archiver.fsync_file")
    def test_fsync_contents(self, mock_fsync_file, mock_fsync_dir):
        """
        Test that :meth:`_fsync_contents` fsyncs the destination file
        and the source and destination directories.
        """
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        src_dir, dest_dir, dest_file = "/src/dir", "/dest/dir", "/dest/dir/walfile"

        # WHEN _fsync_contents is called
        wal_storage._fsync_contents(src_dir, dest_dir, dest_file)
        # THEN the destination file is fsynced
        mock_fsync_file.assert_called_once_with(dest_file)
        # AND the src and dest directories is fsynced
        mock_fsync_dir.assert_has_calls([call(src_dir), call(dest_dir)])

    @patch("barman.wal_archiver.os.rename")
    def test_rename_or_copy_file(self, mock_rename):
        """
        Test that :meth:`_rename_or_copy_file` correctly renames
        the source file to the destination file when they are on the same filesystem.
        """
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        file, dest_file = "/src/path/file1", "/dest/path/file2"
        # WHEN _rename_or_copy_file is called
        wal_storage._rename_or_copy_file(file, dest_file)
        # THEN os.rename is called to move the file
        mock_rename.assert_called_once_with(file, dest_file)

    @patch("barman.wal_archiver.shutil.copy2")
    @patch("barman.wal_archiver.os.rename", side_effect=OSError)
    def test_rename_or_copy_file_different_fs(self, _, mock_copy2):
        """
        Test that :meth:`_rename_or_copy_file` correctly copies the source file
        to the destination file when they are on different filesystems.
        """
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        file, dest_file = "/src/path/file1", "/dest/path/file2"
        # WHEN _rename_or_copy_file is called and os.rename raises OSError
        wal_storage._rename_or_copy_file(file, dest_file)
        # THEN shutil.copy2 is called to copy the file as a fallback
        mock_copy2.assert_called_once_with(file, dest_file)
        # AND the previous source file is appended to the removal list
        assert wal_storage.files_to_remove == [file]

    @patch("barman.wal_archiver.os.unlink")
    def test_remove_intermediary_files(self, mock_unlink):
        """
        Test that :meth:`_remove_intermediary_files` correctly removes
        all intermediary files listed in `files_to_remove`.
        """
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        wal_storage.files_to_remove = [
            "/path/to/tempfile1",
            "/path/to/tempfile2",
            "/path/to/tempfile3",
        ]
        # WHEN _remove_intermediary_files is called
        wal_storage._remove_intermediary_files()
        # THEN os.unlink is called for each file in the removal list
        mock_unlink.assert_has_calls(
            [
                call("/path/to/tempfile1"),
                call("/path/to/tempfile2"),
                call("/path/to/tempfile3"),
            ]
        )
        # AND the list is cleared at the end
        assert wal_storage.files_to_remove == []

    @patch("barman.wal_archiver.mkpath")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_pre_archive_scripts")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._check_duplicate")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._copy_stats")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._compress_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._encrypt_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._rename_or_copy_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._remove_intermediary_files")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._fsync_contents")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_post_archive_scripts")
    def test_save(
        self,
        mock_run_post_scripts,
        mock_fsync,
        mock_remove,
        mock_rename,
        mock_encrypt,
        mock_compress,
        mock_copy_stats,
        mock_check_duplicate,
        mock_run_pre_scripts,
        mock_mkpath,
    ):
        """
        Test that :meth:`save` correctly manages the saving of a WAL file
        (without compression or encryption).
        """
        # GIVEN a LocalWalStorageStrategy instance
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), MagicMock()
        )
        # AND no compression nor encryption is requested
        compressor, encryption = None, None
        # AND a mock WalFileInfo object
        mock_wal_info = MagicMock(
            orig_filename="/src/path/000000010000000000000001",
            fullpath=lambda x: "/server/wals/000000010000000000000001",
            compression=None,
            encryption=None,
        )
        # WHEN save is called
        wal_storage.save(compressor, encryption, mock_wal_info)
        # THEN ensure the destination directory is created
        mock_mkpath.assert_called_once_with("/server/wals")
        # AND the pre-archive scripts are run with correct arguments
        mock_run_pre_scripts.assert_called_once_with(
            mock_wal_info, "/src/path/000000010000000000000001"
        )
        # AND duplicate check is performed with correct arguments
        mock_check_duplicate.assert_called_once_with(
            "/src/path/000000010000000000000001",
            "/server/wals/000000010000000000000001",
            mock_wal_info,
        )
        # AND the file is renamed to the destination
        mock_rename.assert_called_once_with(
            "/src/path/000000010000000000000001",
            "/server/wals/000000010000000000000001",
        )
        # AND intermediary files are removed
        mock_remove.assert_called_once()
        # AND the contents are fsynced
        mock_fsync.assert_called_once_with(
            "/src/path", "/server/wals", "/server/wals/000000010000000000000001"
        )
        # AND xlogdb is NOT touched by save() — that responsibility belongs to the caller
        wal_storage.server.xlogdb.assert_not_called()
        # AND finally the post-archive scripts are run with correct arguments
        mock_run_post_scripts.assert_called_once_with(
            mock_wal_info, "/server/wals/000000010000000000000001", None
        )
        # Lasly, ensure that no compression nor encryption were performed and, as
        # a consequence, not stats were updated
        mock_compress.assert_not_called()
        mock_encrypt.assert_not_called()
        mock_copy_stats.assert_not_called()

    @patch("barman.wal_archiver.mkpath")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_pre_archive_scripts")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._check_duplicate")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._copy_stats")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._compress_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._encrypt_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._rename_or_copy_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._remove_intermediary_files")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._fsync_contents")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_post_archive_scripts")
    def test_save_with_compression(
        self,
        mock_run_post_scripts,
        mock_fsync,
        mock_remove,
        mock_rename,
        mock_encrypt,
        mock_compress,
        mock_copy_stats,
        mock_check_duplicate,
        mock_run_pre_scripts,
        mock_mkpath,
    ):
        """
        Test that :meth:`save` correctly manages the saving of a WAL file
        with compression but no encryption.
        """
        # GIVEN a LocalWalStorageStrategy instance
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), MagicMock()
        )
        # AND compression as gzip and no encryption is requested
        compressor, encryption = MagicMock(compression="gzip"), None
        # AND a mock WalFileInfo object
        mock_wal_info = MagicMock(
            orig_filename="/src/path/000000010000000000000001",
            fullpath=lambda x: "/server/wals/000000010000000000000001",
            compression=None,
            encryption=None,
        )
        # WHEN save is called
        wal_storage.save(compressor, encryption, mock_wal_info)
        # THEN ensure the destination directory is created
        mock_mkpath.assert_called_once_with("/server/wals")
        # AND the pre-archive scripts are run with correct arguments
        mock_run_pre_scripts.assert_called_once_with(
            mock_wal_info, "/src/path/000000010000000000000001"
        )
        # AND duplicate check is performed with correct arguments
        mock_check_duplicate.assert_called_once_with(
            "/src/path/000000010000000000000001",
            "/server/wals/000000010000000000000001",
            mock_wal_info,
        )
        # AND _compress_file is called to compress the file
        mock_compress.assert_called_once_with(
            compressor,
            "/src/path/000000010000000000000001",
            "/server/wals",
            mock_wal_info,
        )
        # AND the stats from the source file are updated to the compressed file
        mock_copy_stats.assert_called_once_with(
            "/src/path/000000010000000000000001",
            mock_compress.return_value,
            mock_wal_info,
        )
        # AND xlogdb is NOT touched by save() — that responsibility belongs to the caller
        wal_storage.server.xlogdb.assert_not_called()
        # AND the compressed file is renamed to the destination
        mock_rename.assert_called_once_with(
            mock_compress.return_value,
            "/server/wals/000000010000000000000001",
        )
        # AND intermediary files are removed
        mock_remove.assert_called_once()
        # AND the contents are fsynced
        mock_fsync.assert_called_once_with(
            "/src/path", "/server/wals", "/server/wals/000000010000000000000001"
        )
        # AND finally the post-archive scripts are run with correct arguments
        mock_run_post_scripts.assert_called_once_with(
            mock_wal_info, "/server/wals/000000010000000000000001", None
        )
        # Lasly, ensure that no encryption was performed
        mock_encrypt.assert_not_called()

    @patch("barman.wal_archiver.mkpath")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_pre_archive_scripts")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._check_duplicate")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._copy_stats")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._compress_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._encrypt_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._rename_or_copy_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._remove_intermediary_files")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._fsync_contents")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_post_archive_scripts")
    def test_save_with_encryption(
        self,
        mock_run_post_scripts,
        mock_fsync,
        mock_remove,
        mock_rename,
        mock_encrypt,
        mock_compress,
        mock_copy_stats,
        mock_check_duplicate,
        mock_run_pre_scripts,
        mock_mkpath,
    ):
        """
        Test that :meth:`save` correctly manages the saving of a WAL file
        with encryption but no compression.
        """
        # GIVEN a LocalWalStorageStrategy instance
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), MagicMock()
        )
        # AND no compression and encryption as gpg is requested
        compressor, encryption = None, MagicMock(NAME="gpg")
        # AND a mock WalFileInfo object
        mock_wal_info = MagicMock(
            orig_filename="/src/path/000000010000000000000001",
            fullpath=lambda x: "/server/wals/000000010000000000000001",
            compression=None,
            encryption=None,
        )
        # WHEN save is called
        wal_storage.save(compressor, encryption, mock_wal_info)
        # THEN ensure the destination directory is created
        mock_mkpath.assert_called_once_with("/server/wals")
        # AND the pre-archive scripts are run with correct arguments
        mock_run_pre_scripts.assert_called_once_with(
            mock_wal_info, "/src/path/000000010000000000000001"
        )
        # AND duplicate check is performed with correct arguments
        mock_check_duplicate.assert_called_once_with(
            "/src/path/000000010000000000000001",
            "/server/wals/000000010000000000000001",
            mock_wal_info,
        )
        # AND _encrypt_file is called to encrypt the file
        mock_encrypt.assert_called_once_with(
            encryption,
            "/src/path/000000010000000000000001",
            "/server/wals",
            mock_wal_info,
        )
        # AND the stats from the source file are updated to the encrypted file
        mock_copy_stats.assert_called_once_with(
            "/src/path/000000010000000000000001",
            mock_encrypt.return_value,
            mock_wal_info,
        )
        # AND xlogdb is NOT touched by save() — that responsibility belongs to the caller
        wal_storage.server.xlogdb.assert_not_called()
        # AND the encrypted file is renamed to the destination
        mock_rename.assert_called_once_with(
            mock_encrypt.return_value,
            "/server/wals/000000010000000000000001",
        )
        # AND intermediary files are removed
        mock_remove.assert_called_once()
        # AND the contents are fsynced
        mock_fsync.assert_called_once_with(
            "/src/path", "/server/wals", "/server/wals/000000010000000000000001"
        )
        # AND finally the post-archive scripts are run with correct arguments
        mock_run_post_scripts.assert_called_once_with(
            mock_wal_info, "/server/wals/000000010000000000000001", None
        )
        # Lasly, ensure that no compression was performed
        mock_compress.assert_not_called()

    @patch("barman.wal_archiver.mkpath")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_pre_archive_scripts")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._check_duplicate")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._copy_stats")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._compress_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._encrypt_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._rename_or_copy_file")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._remove_intermediary_files")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._fsync_contents")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_post_archive_scripts")
    def test_save_with_compression_and_encryption(
        self,
        mock_run_post_scripts,
        mock_fsync,
        mock_remove,
        mock_rename,
        mock_encrypt,
        mock_compress,
        mock_copy_stats,
        mock_check_duplicate,
        mock_run_pre_scripts,
        mock_mkpath,
    ):
        """
        Test that :meth:`save` correctly manages the saving of a WAL file
        with both compression and encryption.
        """
        # GIVEN a LocalWalStorageStrategy instance
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), MagicMock()
        )
        # AND compression as gzip and encryption as gpg is requested
        compressor, encryption = MagicMock(compression="gzip"), MagicMock(NAME="gpg")
        # AND a mock WalFileInfo object
        mock_wal_info = MagicMock(
            orig_filename="/src/path/000000010000000000000001",
            fullpath=lambda x: "/server/wals/000000010000000000000001",
            compression=None,
            encryption=None,
        )
        # WHEN save is called
        wal_storage.save(compressor, encryption, mock_wal_info)
        # THEN ensure the destination directory is created
        mock_mkpath.assert_called_once_with("/server/wals")
        # AND the pre-archive scripts are run with correct arguments
        mock_run_pre_scripts.assert_called_once_with(
            mock_wal_info, "/src/path/000000010000000000000001"
        )
        # AND duplicate check is performed with correct arguments
        mock_check_duplicate.assert_called_once_with(
            "/src/path/000000010000000000000001",
            "/server/wals/000000010000000000000001",
            mock_wal_info,
        )
        # AND _compress_file is called to compress the file
        mock_compress.assert_called_once_with(
            compressor,
            "/src/path/000000010000000000000001",
            "/server/wals",
            mock_wal_info,
        )
        # AND _encrypt_file is called to encrypt the compressed file
        mock_encrypt.assert_called_once_with(
            encryption, mock_compress.return_value, "/server/wals", mock_wal_info
        )
        # AND the stats from the source file are updated to the compressed-encrypted file
        mock_copy_stats.assert_called_once_with(
            "/src/path/000000010000000000000001",
            mock_encrypt.return_value,
            mock_wal_info,
        )
        # AND xlogdb is NOT touched by save() — that responsibility belongs to the caller
        wal_storage.server.xlogdb.assert_not_called()
        # AND the compressed-encrypted file is renamed to the destination
        mock_rename.assert_called_once_with(
            mock_encrypt.return_value,
            "/server/wals/000000010000000000000001",
        )
        # AND intermediary files are removed
        mock_remove.assert_called_once()
        # AND the contents are fsynced
        mock_fsync.assert_called_once_with(
            "/src/path", "/server/wals", "/server/wals/000000010000000000000001"
        )
        # AND finally the post-archive scripts are run with correct arguments
        mock_run_post_scripts.assert_called_once_with(
            mock_wal_info, "/server/wals/000000010000000000000001", None
        )

    @patch("barman.wal_archiver.LocalWalStorageStrategy._delete_wal_file")
    @patch("barman.wal_archiver.os.listdir")
    def test_delete_wal_files_individually(self, mock_listdir, mock_delete_file):
        """
        Test that :meth:`delete` correctly deletes specified WAL files individually
        when the whole WAL directory can not be deleted altogether.
        """
        # GIVEN a directory with three WAL files
        mock_listdir.return_value = [
            "000000010000000000000001",
            "000000010000000000000002",
            "000000010000000000000003",
        ]
        # AND two of them are requested to be deleted
        wal_info1, wal_info2 = MagicMock(), MagicMock()
        wal_info1.name = "000000010000000000000001"
        wal_info2.name = "000000010000000000000002"
        wals_to_delete = {"0000000100000001": [wal_info1, wal_info2]}
        # WHEN delete is called
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        wal_storage.delete(wals_to_delete)
        # THEN delete_wal_file is called for each requested WAL file
        mock_delete_file.assert_has_calls([call(wal_info1), call(wal_info2)])

    @patch("barman.wal_archiver.LocalWalStorageStrategy._delete_wal_directory")
    @patch("barman.wal_archiver.os.listdir")
    def test_delete_whole_directory(self, mock_listdir, mock_delete_directory):
        """
        Test that :meth:`delete` correctly deletes the whole wal directory
        when suitable.
        """
        # GIVEN a directory with two WAL files
        mock_listdir.return_value = [
            "000000010000000000000001",
            "000000010000000000000002",
        ]
        # AND all of them are requested to be deleted
        wal_info1, wal_info2 = MagicMock(), MagicMock()
        wal_info1.name = "000000010000000000000001"
        wal_info2.name = "000000010000000000000002"
        wals_to_delete = {"/server/wals/0000000100000001": [wal_info1, wal_info2]}
        # WHEN delete is called
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        wal_storage.delete(wals_to_delete)
        # THEN _delete_wal_directory is called on the WAL directory
        mock_delete_directory.assert_called_once_with(
            "/server/wals/0000000100000001", [wal_info1, wal_info2]
        )

    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_pre_delete_wal_scripts")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_post_delete_wal_scripts")
    @patch("barman.wal_archiver.shutil.rmtree")
    def test_delete_wal_directory(
        self, mock_rmtree, mock_post_scripts, mock_pre_scripts
    ):
        """
        Test that :meth:`_delete_wal_directory` correctly deletes a WAL directory
        and runs pre- and post-deletion scripts.
        """
        # GIVEN a LocalWalStorageStrategy instance
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        # WHEN _delete_wal_directory is called
        wal_dir = "/server/wals/0000000100000001"
        wal_info1, wal_info2 = MagicMock(), MagicMock()
        wal_info1.name = "000000010000000000000001"
        wal_info2.name = "000000010000000000000002"
        wal_storage._delete_wal_directory(wal_dir, [wal_info1, wal_info2])
        # THEN delete_wal_file is called for each requested WAL file
        mock_pre_scripts.assert_has_calls([call(wal_info1), call(wal_info2)])
        mock_rmtree.assert_called_once_with(wal_dir)
        mock_post_scripts.assert_has_calls([call(wal_info1), call(wal_info2)])

    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_pre_delete_wal_scripts")
    @patch("barman.wal_archiver.LocalWalStorageStrategy._run_post_delete_wal_scripts")
    @patch("barman.wal_archiver.os.unlink")
    def test_delete_wal_file(self, mock_unlink, mock_post_scripts, mock_pre_scripts):
        """
        Test that :meth:`_delete_wal_file` correctly deletes a WAL file
        and runs pre- and post-deletion scripts.
        """
        # GIVEN a LocalWalStorageStrategy instance
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        # WHEN _delete_wal_directory is called
        wal_info = MagicMock()
        wal_info.fullpath = lambda x: (
            "/server/wals/0000000100000001/000000010000000000000001"
        )
        wal_storage._delete_wal_file(wal_info)
        # THEN the file is unlinked and pre- and post-deletion scripts are run
        mock_pre_scripts.assert_called_once_with(wal_info)
        mock_unlink.assert_called_once_with(
            "/server/wals/0000000100000001/000000010000000000000001"
        )
        mock_post_scripts.assert_called_once_with(wal_info, None)

    def test_exists(self, tmpdir):
        """
        Test that :meth:`exists` correctly checks for the existence of a WAL file.
        """
        # GIVEN a LocalWalStorageStrategy instance
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(name="TestServer"), None
        )
        # AND a temporary file representing the WAL file
        wal_file_path = tmpdir.join("000000010000000000000001")
        wal_file_path.write("WAL file content")
        # WHEN exists is called on the existing file
        result_existing = wal_storage.exists(str(wal_file_path))
        # THEN it returns True
        assert result_existing is True

        # WHEN exists is called on a non-existing file
        result_non_existing = wal_storage.exists(
            str(tmpdir.join("000000010000000000000002"))
        )
        # THEN it returns False
        assert result_non_existing is False

    @patch("barman.wal_archiver.xlog.hash_dir", return_value="0000000100000001")
    def test_get_full_path(self, mock_hash_dir):
        """
        Test that :meth:`get_full_path` correctly constructs the full path
        for a given WAL file.
        """
        # GIVEN a LocalWalStorageStrategy instance
        wal_storage = LocalWalStorageStrategy(
            build_backup_manager(
                name="TestServer", main_conf={"wals_directory": "/barman/wals"}
            ),
            None,
        )
        # WHEN get_full_path is called with a WAL file name
        wal_file_name = "000000010000000000000001"
        result = wal_storage.get_full_path(wal_file_name)
        # THEN it returns the correct full path
        expected_path = "/barman/wals/0000000100000001/000000010000000000000001"
        assert result == expected_path


class TestCloudWalStorageStrategy:
    """Tests for the :class:`CloudWalStorageStrategy` class"""

    @pytest.mark.parametrize(
        "cmp_result,expected_exception",
        [
            (False, DuplicateWalFile),
            (True, MatchingDuplicateWalFile),
        ],
    )
    @patch("barman.wal_archiver.NamedTemporaryFile")
    @patch("barman.wal_archiver.filecmp.cmp")
    def test_check_duplicate(
        self, mock_cmp, mock_tempfile, cmp_result, expected_exception
    ):
        """
        Test that :meth:`_check_duplicate` correctly compares files and raises
        the appropriate exceptions when duplicates are found in cloud storage.
        """
        # GIVEN a CloudWalStorageStrategy instance
        wal_storage = CloudWalStorageStrategy(
            build_backup_manager(name="TestServer"), MagicMock()
        )
        wal_storage.cloud_interface = MagicMock()
        # AND a mocked filecmp.cmp returning the desired comparison result
        mock_cmp.return_value = cmp_result
        # AND a wal_info and object key for the test
        mock_wal_info = MagicMock(
            orig_filename="/src/path/000000010000000000000001", compression=None
        )
        obj_key = "backups/barman/TestServer/wals/000000010000000000000001"

        # WHEN _check_duplicate is called
        # THEN the appropriate exception is raised based on the comparison result
        with pytest.raises(expected_exception):
            wal_storage._check_duplicate(mock_wal_info, obj_key)

        # AND the cloud object is downloaded to a temporary file for comparison
        mock_tempfile.assert_called_once_with(delete=True)
        wal_storage.cloud_interface.download_file.assert_called_once_with(
            obj_key,
            mock_tempfile.return_value.__enter__.return_value.name,
            decompress=None,
        )
        # AND filecmp.cmp was called with the original file and the temporary file
        mock_cmp.assert_called_once_with(
            mock_wal_info.orig_filename,
            mock_tempfile.return_value.__enter__.return_value.name,
        )

    @pytest.mark.parametrize("skip_delete", [None, False, True])
    @patch("barman.wal_archiver.xlog.hash_dir", return_value="0000000100000001")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._run_pre_archive_scripts")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._run_post_archive_scripts")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._check_duplicate")
    @patch("barman.wal_archiver.os.unlink")
    @patch("barman.wal_archiver.open")
    def test_save(
        self,
        mock_open,
        mock_unlink,
        mock_check_duplicate,
        mock_run_post_scripts,
        mock_run_pre_scripts,
        mock_hash_dir,
        skip_delete,
    ):
        """
        Test that :meth:`save` correctly uploads WAL files to cloud storage.
        """
        # GIVEN a CloudWalStorageStrategy instance
        wal_storage = CloudWalStorageStrategy(
            build_backup_manager(name="TestServer"), MagicMock()
        )
        wal_storage.cloud_interface = MagicMock(path="backups/barman")
        # AND the following wal_info object
        mock_wal_info = MagicMock(
            orig_filename="/src/path/000000010000000000000001",
            compression=None,
        )
        mock_wal_info.name = "000000010000000000000001"

        # WHEN save is called
        compressor, encryption = None, None
        if skip_delete is None:
            wal_storage.save(compressor, encryption, mock_wal_info)
        else:
            wal_storage.save(
                compressor, encryption, mock_wal_info, skip_delete=skip_delete
            )

        # THEN the pre-archive scripts are run with correct arguments
        mock_run_pre_scripts.assert_called_once_with(
            mock_wal_info, "/src/path/000000010000000000000001"
        )
        # AND the source file is opened correctly
        mock_open.assert_called_once_with("/src/path/000000010000000000000001", "rb")
        # AND xlogdb is NOT touched by save() — that responsibility belongs to the caller
        wal_storage.server.xlogdb.assert_not_called()
        # AND the opened src file is uploaded to the cloud with the correct key
        wal_storage.cloud_interface.upload_fileobj.assert_called_once_with(
            fileobj=mock_open.return_value,
            key="backups/barman/TestServer/wals/0000000100000001/000000010000000000000001",
            fail_if_exists=True,
        )
        # AND the post-archive scripts are run with correct arguments
        mock_run_post_scripts.assert_called_once_with(
            mock_wal_info, "/src/path/000000010000000000000001", None
        )
        # AND the source file is unlinked after the upload, if not requested to skip deletion
        if skip_delete:
            mock_unlink.assert_not_called()
        else:
            mock_unlink.assert_called_once_with("/src/path/000000010000000000000001")
        # Lastly, ensure that no duplicate check was performed, as no exception was
        # raised by the upload_fileobj method
        mock_check_duplicate.assert_not_called()

    @patch("barman.wal_archiver.xlog.hash_dir", return_value="0000000100000001")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._run_pre_archive_scripts")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._run_post_archive_scripts")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._check_duplicate")
    @patch("barman.wal_archiver.os.unlink")
    @patch("barman.wal_archiver.open")
    def test_save_duplicate(
        self,
        mock_open,
        mock_unlink,
        mock_check_duplicate,
        mock_run_post_scripts,
        mock_run_pre_scripts,
        mock_hash_dir,
    ):
        """
        Test that :meth:`save` correctly handles duplicate WAL files
        when uploading to cloud storage.
        """
        # GIVEN a CloudWalStorageStrategy instance
        wal_storage = CloudWalStorageStrategy(
            build_backup_manager(name="TestServer"), MagicMock()
        )
        wal_storage.cloud_interface = MagicMock(path="backups/barman")
        # AND the following wal_info object
        mock_wal_info = MagicMock(
            orig_filename="/src/path/000000010000000000000001",
            compression=None,
        )
        mock_wal_info.name = "000000010000000000000001"
        # AND the cloud_interface upload_fileobj raises ObjectKeyAlreadyExists
        # AND the _check_duplicate raises DuplicateWalFile
        wal_storage.cloud_interface.upload_fileobj.side_effect = ObjectKeyAlreadyExists
        mock_check_duplicate.side_effect = DuplicateWalFile

        # WHEN save is called
        # THEN ObjectKeyAlreadyExists is catched and _check_duplicate is called
        # which raises DuplicateWalFile
        compressor, encryption = None, None
        with pytest.raises(DuplicateWalFile) as exc_info:
            wal_storage.save(compressor, encryption, mock_wal_info)
            mock_check_duplicate.assert_called_once_with(
                mock_wal_info,
                "backups/barman/TestServer/wals/0000000100000001/000000010000000000000001",
            )
            # AND the post-archive is run correctly, including the exception info
            mock_run_post_scripts.assert_called_once_with(
                mock_wal_info,
                "/src/path/000000010000000000000001",
                exc_info,
            )

    @patch("barman.wal_archiver.xlog.hash_dir", return_value="0000000100000001")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._run_pre_delete_wal_scripts")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._run_post_delete_wal_scripts")
    def test_delete(self, mock_post_scripts, mock_pre_scripts, mock_hash_dir):
        """
        Test that :meth:`delete` correctly deletes WAL files from cloud storage.
        """
        # GIVEN a CloudWalStorageStrategy instance
        wal_storage = CloudWalStorageStrategy(
            build_backup_manager(name="server"), MagicMock()
        )
        wal_storage.cloud_interface = MagicMock(path="my-bucket")

        # AND two wal_info objects to be deleted
        wal_info1, wal_info2 = MagicMock(compression=None), MagicMock(compression=None)
        wal_info1.name = "000000010000000000000001"
        wal_info2.name = "000000010000000000000002"
        wals_to_delete = {
            "my-bucket/server/wals/0000000100000001": [wal_info1, wal_info2]
        }

        # WHEN delete is called
        wal_storage.delete(wals_to_delete)

        # THEN the cloud_interface delete_object is called for each WAL file
        mock_pre_scripts.assert_has_calls([call(wal_info1), call(wal_info2)])
        wal_storage.cloud_interface.delete_objects.assert_called_once_with(
            [
                "my-bucket/server/wals/0000000100000001/000000010000000000000001",
                "my-bucket/server/wals/0000000100000001/000000010000000000000002",
            ]
        )
        mock_post_scripts.assert_has_calls([call(wal_info1), call(wal_info2)])

    def test_exists(self):
        """
        Test that :meth:`exists` correctly checks for the existence of a WAL file
        in cloud storage.
        """
        # GIVEN a CloudWalStorageStrategy instance
        wal_storage = CloudWalStorageStrategy(
            build_backup_manager(name="TestServer"), MagicMock()
        )
        wal_storage.cloud_interface = MagicMock()

        # WHEN exists is called
        full_path = "barman-bucket/wals/0000000100000001/000000010000000000000001"
        wal_storage.exists(full_path)

        # THEN cloud_interface.object_exists is called with the correct key
        wal_storage.cloud_interface.check_object_existence.assert_called_once_with(
            full_path
        )

    @patch("barman.wal_archiver.xlog.hash_dir", return_value="0000000100000001")
    def test_get_full_path(self, mock_hash_dir):
        """
        Test that :meth:`get_full_path` correctly constructs the full path
        for a given WAL file in cloud storage.
        """
        # GIVEN a CloudWalStorageStrategy instance
        wal_storage = CloudWalStorageStrategy(
            build_backup_manager(name="TestServer"), MagicMock()
        )
        wal_storage.cloud_interface = MagicMock(path="barman-bucket")

        # WHEN get_full_path is called with a WAL file name
        wal_file_name = "000000010000000000000001"
        result = wal_storage.get_full_path(wal_file_name)
        # THEN it returns the correct full path
        expected_path = (
            "barman-bucket/TestServer/wals/0000000100000001/000000010000000000000001"
        )
        assert result == expected_path

    @pytest.mark.parametrize(
        "compression,expected_ext",
        [
            ("gzip", ".gz"),
            ("bzip2", ".bz2"),
            ("xz", ".xz"),
            ("snappy", ".snappy"),
            ("zstd", ".zst"),
            ("lz4", ".lz4"),
        ],
    )
    @patch("barman.wal_archiver.xlog.hash_dir", return_value="0000000100000001")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._run_pre_archive_scripts")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._run_post_archive_scripts")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._check_duplicate")
    @patch("barman.wal_archiver.os.unlink")
    @patch("barman.wal_archiver.open")
    def test_save_with_compression(
        self,
        mock_open,
        mock_unlink,
        mock_check_duplicate,
        mock_run_post_scripts,
        mock_run_pre_scripts,
        mock_hash_dir,
        compression,
        expected_ext,
    ):
        """
        Test that :meth:`save` compresses WAL files before uploading to cloud
        storage when a compressor is provided.
        """
        # GIVEN a CloudWalStorageStrategy instance
        wal_storage = CloudWalStorageStrategy(
            build_backup_manager(name="TestServer"), MagicMock()
        )
        wal_storage.cloud_interface = MagicMock(path="backups/barman")
        # AND the following wal_info object
        mock_wal_info = MagicMock(
            orig_filename="/src/path/000000010000000000000001",
            compression=None,
        )
        mock_wal_info.name = "000000010000000000000001"
        # AND a mock InternalCompressor that returns a BytesIO-like object
        compressed_data = io.BytesIO(b"compressed-data")
        mock_compressor = MagicMock(spec=InternalCompressor)
        mock_compressor.compress_in_mem.return_value = compressed_data
        mock_compressor.compression = compression

        # WHEN save is called with the compressor
        wal_storage.save(mock_compressor, None, mock_wal_info)

        # THEN the source file is opened
        mock_open.assert_called_once_with("/src/path/000000010000000000000001", "rb")
        # AND compress_in_mem is called with the file object
        mock_compressor.compress_in_mem.assert_called_once_with(
            mock_open.return_value.__enter__.return_value
        )
        # AND the cloud key includes the compression extension
        expected_key = (
            "backups/barman/TestServer/wals/0000000100000001/"
            "000000010000000000000001" + expected_ext
        )
        wal_storage.cloud_interface.upload_fileobj.assert_called_once_with(
            fileobj=compressed_data, key=expected_key, fail_if_exists=True
        )
        # AND wal_info.compression is updated
        assert mock_wal_info.compression == compression
        # AND wal_info.size is updated to the compressed size
        assert mock_wal_info.size == len(b"compressed-data")
        # AND the source file is unlinked
        mock_unlink.assert_called_once_with("/src/path/000000010000000000000001")

    @pytest.mark.parametrize(
        "compression,expected_ext",
        [
            ("gzip", ".gz"),
            ("snappy", ".snappy"),
        ],
    )
    @patch("barman.wal_archiver.xlog.hash_dir", return_value="0000000100000001")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._run_pre_archive_scripts")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._run_post_archive_scripts")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._check_duplicate")
    @patch("barman.wal_archiver.os.unlink")
    @patch("barman.wal_archiver.open")
    def test_save_duplicate_with_compression(
        self,
        mock_open,
        mock_unlink,
        mock_check_duplicate,
        mock_run_post_scripts,
        mock_run_pre_scripts,
        mock_hash_dir,
        compression,
        expected_ext,
    ):
        """
        Test that :meth:`save` correctly handles duplicate compressed WAL files.
        """
        # GIVEN a CloudWalStorageStrategy instance
        wal_storage = CloudWalStorageStrategy(
            build_backup_manager(name="TestServer"), MagicMock()
        )
        wal_storage.cloud_interface = MagicMock(path="backups/barman")
        # AND the following wal_info object
        mock_wal_info = MagicMock(
            orig_filename="/src/path/000000010000000000000001",
            compression=None,
        )
        mock_wal_info.name = "000000010000000000000001"
        # AND a mock InternalCompressor that returns a BytesIO-like object
        mock_compressor = MagicMock(spec=InternalCompressor)
        mock_compressor.compress_in_mem.return_value = io.BytesIO(b"compressed-data")
        mock_compressor.compression = compression
        # AND the upload raises ObjectKeyAlreadyExists
        wal_storage.cloud_interface.upload_fileobj.side_effect = ObjectKeyAlreadyExists
        mock_check_duplicate.side_effect = DuplicateWalFile

        # WHEN save is called with the compressor
        # THEN DuplicateWalFile is raised
        expected_key = (
            "backups/barman/TestServer/wals/0000000100000001/"
            "000000010000000000000001" + expected_ext
        )
        with pytest.raises(DuplicateWalFile):
            wal_storage.save(mock_compressor, None, mock_wal_info)
        mock_check_duplicate.assert_called_once_with(mock_wal_info, expected_key)

    @pytest.mark.parametrize(
        "compression,expected_ext",
        [
            (None, ""),
            ("gzip", ".gz"),
            ("lz4", ".lz4"),
        ],
    )
    @patch("barman.wal_archiver.xlog.hash_dir", return_value="0000000100000001")
    def test_get_full_path_with_compression(
        self, mock_hash_dir, compression, expected_ext
    ):
        """
        Test that :meth:`get_full_path` appends the compression extension.
        """
        # GIVEN a CloudWalStorageStrategy instance with compression configured
        backup_manager = build_backup_manager(name="TestServer")
        backup_manager.config.compression = compression
        wal_storage = CloudWalStorageStrategy(backup_manager, MagicMock())
        wal_storage.cloud_interface = MagicMock(path="barman-bucket")

        # WHEN get_full_path is called
        result = wal_storage.get_full_path("000000010000000000000001")

        # THEN the path includes the compression extension
        expected_path = (
            "barman-bucket/TestServer/wals/0000000100000001/"
            "000000010000000000000001" + expected_ext
        )
        assert result == expected_path

    @patch("barman.wal_archiver.xlog.hash_dir", return_value="0000000100000001")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._run_pre_delete_wal_scripts")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._run_post_delete_wal_scripts")
    def test_delete_with_compression(
        self, mock_post_scripts, mock_pre_scripts, mock_hash_dir
    ):
        """
        Test that :meth:`delete` uses compression extensions from wal_info.compression
        to build the correct cloud keys.
        """
        # GIVEN a CloudWalStorageStrategy instance (no current compression config needed)
        wal_storage = CloudWalStorageStrategy(
            build_backup_manager(name="server"), MagicMock()
        )
        wal_storage.cloud_interface = MagicMock(path="my-bucket")

        # AND two wal_info objects with compression metadata from xlogdb
        wal_info1 = MagicMock(compression="gzip")
        wal_info1.name = "000000010000000000000001"
        wal_info2 = MagicMock(compression="lz4")
        wal_info2.name = "000000010000000000000002"
        wals_to_delete = {
            "my-bucket/server/wals/0000000100000001": [wal_info1, wal_info2]
        }

        # WHEN delete is called
        wal_storage.delete(wals_to_delete)

        # THEN the cloud keys include the correct compression extensions
        wal_storage.cloud_interface.delete_objects.assert_called_once_with(
            [
                "my-bucket/server/wals/0000000100000001/000000010000000000000001.gz",
                "my-bucket/server/wals/0000000100000001/000000010000000000000002.lz4",
            ],
        )

    @patch("barman.wal_archiver.xlog.hash_dir", return_value="0000000100000001")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._run_pre_delete_wal_scripts")
    @patch("barman.wal_archiver.CloudWalStorageStrategy._run_post_delete_wal_scripts")
    def test_delete_without_compression(
        self, mock_post_scripts, mock_pre_scripts, mock_hash_dir
    ):
        """
        Test that :meth:`delete` works correctly when wal_info has no compression.
        """
        # GIVEN a CloudWalStorageStrategy instance
        wal_storage = CloudWalStorageStrategy(
            build_backup_manager(name="server"), MagicMock()
        )
        wal_storage.cloud_interface = MagicMock(path="my-bucket")

        # AND wal_info objects with no compression
        wal_info1 = MagicMock(compression=None)
        wal_info1.name = "000000010000000000000001"
        wals_to_delete = {"my-bucket/server/wals/0000000100000001": [wal_info1]}

        # WHEN delete is called
        wal_storage.delete(wals_to_delete)

        # THEN the cloud keys do not include compression extensions
        wal_storage.cloud_interface.delete_objects.assert_called_once_with(
            [
                "my-bucket/server/wals/0000000100000001/000000010000000000000001",
            ],
        )

    @pytest.mark.parametrize(
        "cmp_result,expected_exception",
        [
            (False, DuplicateWalFile),
            (True, MatchingDuplicateWalFile),
        ],
    )
    @patch("barman.wal_archiver.NamedTemporaryFile")
    @patch("barman.wal_archiver.filecmp.cmp")
    def test_check_duplicate_with_compression(
        self,
        mock_cmp,
        mock_tempfile,
        cmp_result,
        expected_exception,
    ):
        """
        Test that :meth:`_check_duplicate` passes the compression to
        ``download_file`` for decompression when compression is configured.
        """
        # GIVEN a CloudWalStorageStrategy instance
        wal_storage = CloudWalStorageStrategy(
            build_backup_manager(name="TestServer"), MagicMock()
        )
        wal_storage.cloud_interface = MagicMock()
        # AND a mocked filecmp.cmp returning the desired result
        mock_cmp.return_value = cmp_result
        mock_wal_info = MagicMock(
            orig_filename="/src/path/000000010000000000000001", compression="gzip"
        )
        obj_key = "backups/barman/TestServer/wals/000000010000000000000001.gz"

        # WHEN _check_duplicate is called
        # THEN the appropriate exception is raised
        with pytest.raises(expected_exception):
            wal_storage._check_duplicate(mock_wal_info, obj_key)

        # AND download_file was called with decompress="gzip"
        mock_tempfile.assert_called_once_with(delete=True)
        wal_storage.cloud_interface.download_file.assert_called_once_with(
            obj_key,
            mock_tempfile.return_value.__enter__.return_value.name,
            decompress="gzip",
        )


class TestParallelWalArchiver:
    """Tests for :class:`ParallelWalArchiver` base class methods."""

    def _make_archiver(self, tmp_path):
        """Return a minimal :class:`ParallelWalArchiver` subclass for testing."""

        class TestableArchiver(ParallelWalArchiver):
            def _archive_single_wal(self, wal_path):
                pass

        return TestableArchiver("test-server", str(tmp_path))

    @patch("barman.wal_archiver.ParallelWalArchiver._read_last_wal_archived")
    def test_is_already_archived_returns_true_when_wal_is_cached(
        self, mock_last_wal_archived, tmp_path
    ):
        """
        _is_already_archived should return True when the WAL name is
        lexicographically <= the cached last-archived WAL.
        """
        # GIVEN a cached last-archived WAL of 000000010000000000000005
        mock_last_wal_archived.return_value = "000000010000000000000005"
        archiver = self._make_archiver(tmp_path)

        # WHEN checking a WAL that precedes the cached entry
        result = archiver._is_already_archived("/pg_wal/000000010000000000000003")

        # THEN it is considered already archived
        assert result is True

    @patch("barman.wal_archiver.ParallelWalArchiver._read_last_wal_archived")
    def test_is_already_archived_returns_false_when_wal_is_not_cached(
        self, mock_last_wal_archived, tmp_path
    ):
        """
        _is_already_archived should return False when the WAL name is
        greater than the cached last-archived WAL.
        """
        # GIVEN a cached last-archived WAL of 000000010000000000000005
        mock_last_wal_archived.return_value = "000000010000000000000005"
        archiver = self._make_archiver(tmp_path)

        # WHEN checking a WAL that comes after the cached entry
        result = archiver._is_already_archived("/pg_wal/000000010000000000000007")

        # THEN it is not considered already archived
        assert result is False

    @patch("barman.wal_archiver.ParallelWalArchiver._read_last_wal_archived")
    def test_is_already_archived_returns_false_when_cache_missing(
        self, mock_last_wal_archived, tmp_path
    ):
        """
        _is_already_archived should return False when no cache file exists.
        """
        # GIVEN last-archived WAL is None
        mock_last_wal_archived.return_value = None
        archiver = self._make_archiver(tmp_path)

        # WHEN the WAL is checked for archival status
        result = archiver._is_already_archived("/pg_wal/000000010000000000000001")

        # THEN the WAL is not considered already archived
        assert result is False

    @patch("barman.wal_archiver.ParallelWalArchiver._write_last_wal_archived")
    def test_update_metadata_updates_cache_with_last_successful_wal(
        self, mock_write_last_wal, tmp_path
    ):
        """
        Base _update_metadata should update the cache with the last consecutively
        successful WAL.
        """
        archiver = self._make_archiver(tmp_path)
        wal_path = "/pg_wal/000000010000000000000001"

        worker_ok = MagicMock(spec=WalPrefetchWorker)
        worker_ok.success = True
        worker_ok.wal_path = "/pg_wal/000000010000000000000002"

        worker_fail = MagicMock(spec=WalPrefetchWorker)
        worker_fail.success = False
        worker_fail.wal_path = "/pg_wal/000000010000000000000003"

        archiver._update_metadata(wal_path, [worker_ok, worker_fail])

        # THEN cache is updated with the last successful WAL (002)
        mock_write_last_wal.assert_called_once_with("000000010000000000000002")

    @pytest.mark.parametrize(
        "wal_file",
        ["/pg_wal/00000003.history", "000000010000000000000004.00000001.backup"],
    )
    @patch("barman.wal_archiver.ParallelWalArchiver._write_last_wal_archived")
    def test_update_metadata_does_not_cache_history_or_backup_file(
        self, mock_write_last_wal, wal_file, tmp_path
    ):
        """
        Base _update_metadata must not write a .history or .backup file name to the cache.
        """
        archiver = self._make_archiver(tmp_path)

        # GIVEN _update_metadata is called with a .history or .backup WAL file
        archiver._update_metadata(wal_file, [])

        # THEN the cache is NOT updated (history files should not be cached)
        mock_write_last_wal.assert_not_called()

    def test_get_wals_to_prefetch_returns_next_in_sequence(self, tmp_path):
        """
        _get_wals_to_prefetch should return the next WAL paths in strict sequence
        order, stopping once the requested limit is reached.
        """
        # GIVEN a pg_wal directory with archive_status containing .ready files
        pg_wal = tmp_path / "pg_wal"
        archive_status = pg_wal / "archive_status"
        archive_status.mkdir(parents=True)

        # Create WALs in pg_wal and their .ready markers in archive_status
        requested = "000000010000000000000003"
        for wal in [
            "000000010000000000000004",
            "000000010000000000000005",
            "000000010000000000000006",
        ]:
            (archive_status / f"{wal}.ready").touch()
            (pg_wal / wal).touch()

        archiver = self._make_archiver(tmp_path)
        requested_path = str(pg_wal / requested)

        # WHEN requesting 2 WALs to prefetch
        result = archiver._get_wals_to_prefetch(
            requested_path, 2, barman.xlog.DEFAULT_XLOG_SEG_SIZE
        )

        # THEN exactly the next 2 WALs in sequence are returned
        assert result == [
            str(pg_wal / "000000010000000000000004"),
            str(pg_wal / "000000010000000000000005"),
        ]

    def test_get_wals_to_prefetch_stops_at_first_gap(self, tmp_path):
        """
        _get_wals_to_prefetch should stop at the first WAL in the sequence that
        does not have a .ready marker, even if later ones do.
        """
        # GIVEN a pg_wal directory with archive_status containing .ready files
        pg_wal = tmp_path / "pg_wal"
        archive_status = pg_wal / "archive_status"
        archive_status.mkdir(parents=True)

        # Create WALs in pg_wal and their .ready markers in archive_status
        # Only WALs 002 and 004 are marked as ready; 003 is missing
        requested = "000000010000000000000001"
        (archive_status / "000000010000000000000002.ready").touch()
        (pg_wal / "000000010000000000000002").touch()
        (archive_status / "000000010000000000000004.ready").touch()
        (pg_wal / "000000010000000000000004").touch()

        # WHEN requesting 5 WALs to prefetch
        archiver = self._make_archiver(tmp_path)
        result = archiver._get_wals_to_prefetch(
            str(pg_wal / requested), 5, barman.xlog.DEFAULT_XLOG_SEG_SIZE
        )

        # THEN only WAL 002 is returned — discovery stops at the gap on 003
        assert result == [str(pg_wal / "000000010000000000000002")]

    def test_get_wals_to_prefetch_returns_empty_for_non_wal_requested_path(
        self, tmp_path
    ):
        """
        _get_wals_to_prefetch should return an empty list if the requested path is
        not a WAL file (e.g. a .history file).
        """
        pg_wal = tmp_path / "pg_wal"
        pg_wal.mkdir()
        archiver = self._make_archiver(tmp_path)

        result = archiver._get_wals_to_prefetch(
            str(pg_wal / "00000001.history"), 5, barman.xlog.DEFAULT_XLOG_SEG_SIZE
        )

        assert result == []

    @patch("barman.wal_archiver.os.path.getsize")
    def test_archive_calls_archive_single_wal(self, mock_getsize, tmp_path):
        """
        archive() should call _archive_single_wal with the WAL path.
        """
        mock_getsize.return_value = barman.xlog.DEFAULT_XLOG_SEG_SIZE
        archiver = self._make_archiver(tmp_path)
        wal_path = "/pg_wal/000000010000000000000001"

        with patch.object(archiver, "_is_already_archived", return_value=False):
            with patch.object(archiver, "_archive_single_wal") as mock_archive:
                with patch.object(archiver, "_get_wals_to_prefetch", return_value=[]):
                    with patch.object(archiver, "_update_metadata"):
                        archiver.archive(wal_path, parallel=0)

        mock_archive.assert_called_once_with(wal_path)

    def test_archive_skips_already_archived_wal(self, tmp_path):
        """
        archive() should return early without archiving when the WAL is already
        in the last-archived cache and parallel archival is in use.
        """
        archiver = self._make_archiver(tmp_path)

        with patch.object(archiver, "_is_already_archived", return_value=True):
            with patch.object(archiver, "_archive_single_wal") as mock_archive:
                archiver.archive("/pg_wal/000000010000000000000003", parallel=2)

        mock_archive.assert_not_called()

    @patch("barman.wal_archiver.os.path.getsize")
    def test_archive_does_not_check_cache_when_not_parallel(
        self, mock_getsize, tmp_path
    ):
        """
        archive() should not consult the last-archived cache when parallel <= 1;
        the cache is only meaningful across concurrent parallel invocations.
        """
        mock_getsize.return_value = barman.xlog.DEFAULT_XLOG_SEG_SIZE
        archiver = self._make_archiver(tmp_path)
        wal_path = "/pg_wal/000000010000000000000003"

        with patch.object(
            archiver, "_is_already_archived", return_value=True
        ) as mock_cached:
            with patch.object(archiver, "_archive_single_wal") as mock_archive:
                archiver.archive(wal_path, parallel=1)

        mock_cached.assert_not_called()
        mock_archive.assert_called_once_with(wal_path)

    @patch("barman.wal_archiver.os.path.getsize")
    @patch("barman.wal_archiver.WalPrefetchWorker")
    def test_archive_spawns_no_workers_on_main_wal_failure(
        self, mock_worker_cls, mock_getsize, tmp_path
    ):
        """
        When the main WAL archival fails, archive() must not spawn any prefetch workers.
        """
        archiver = self._make_archiver(tmp_path)
        wal_path = "/pg_wal/000000010000000000000001"

        with patch.object(archiver, "_is_already_archived", return_value=False):
            with patch.object(
                archiver, "_archive_single_wal", side_effect=Exception("boom")
            ):
                with patch.object(
                    archiver,
                    "_get_wals_to_prefetch",
                    return_value=["/pg_wal/000000010000000000000002"],
                ):
                    with pytest.raises(Exception, match="boom"):
                        archiver.archive(wal_path, parallel=2)

        mock_worker_cls.assert_not_called()

    @patch("barman.wal_archiver.os.path.getsize")
    def test_archive_skips_update_metadata_without_prefetch_workers(
        self, mock_getsize, tmp_path
    ):
        """
        archive() should not call _update_metadata when no prefetch workers ran,
        since the last-archived cache is only meaningful for parallel archival.
        """
        mock_getsize.return_value = barman.xlog.DEFAULT_XLOG_SEG_SIZE
        archiver = self._make_archiver(tmp_path)
        wal_path = "/pg_wal/000000010000000000000001"

        with patch.object(archiver, "_archive_single_wal"):
            with patch.object(archiver, "_get_wals_to_prefetch", return_value=[]):
                with patch.object(archiver, "_update_metadata") as mock_update:
                    with patch.object(archiver, "_post_archive_exec") as mock_post:
                        archiver.archive(wal_path, parallel=0)

        mock_update.assert_not_called()
        mock_post.assert_called_once_with(wal_path, [])

    @patch("barman.wal_archiver.os.path.getsize")
    @patch("barman.wal_archiver.WalPrefetchWorker")
    def test_archive_calls_update_metadata_with_prefetch_workers(
        self, mock_worker_cls, mock_getsize, tmp_path
    ):
        """
        archive() should call _update_metadata when prefetch workers ran.
        """
        mock_getsize.return_value = barman.xlog.DEFAULT_XLOG_SEG_SIZE
        archiver = self._make_archiver(tmp_path)
        wal_path = "/pg_wal/000000010000000000000001"
        prefetch_wal_path = "/pg_wal/000000010000000000000002"
        mock_worker_cls.return_value = MagicMock(spec=WalPrefetchWorker)

        with patch.object(archiver, "_archive_single_wal"):
            with patch.object(
                archiver, "_get_wals_to_prefetch", return_value=[prefetch_wal_path]
            ):
                with patch.object(archiver, "_update_metadata") as mock_update:
                    with patch.object(archiver, "_post_archive_exec") as mock_post:
                        archiver.archive(wal_path, parallel=2)

        mock_update.assert_called_once_with(wal_path, [mock_worker_cls.return_value])
        mock_post.assert_called_once_with(wal_path, [mock_worker_cls.return_value])

    def test_ensure_cache_dir_creates_directory_with_restrictive_mode(self, tmp_path):
        """
        _ensure_cache_dir should create the cache directory with mode 0o700.
        """
        cache_dir = tmp_path / "cache"
        archiver = self._make_archiver(cache_dir)

        archiver._ensure_cache_dir()

        assert cache_dir.is_dir()
        assert stat.S_IMODE(os.stat(str(cache_dir)).st_mode) == 0o700

    def test_ensure_cache_dir_swallows_oserror(self, tmp_path, caplog):
        """
        _ensure_cache_dir should not propagate errors when directory creation
        fails, only log a warning.
        """
        # A path nested under a plain file can never be created
        blocking_file = tmp_path / "not_a_dir"
        blocking_file.write_text("content")
        cache_dir = blocking_file / "cache"
        archiver = self._make_archiver(cache_dir)

        with caplog.at_level(logging.WARNING, logger="barman.wal_archiver"):
            # THEN no exception is raised
            archiver._ensure_cache_dir()

        assert "Could not create cache directory" in caplog.text

    def test_is_cache_dir_secure_true_for_owned_private_directory(self, tmp_path):
        """
        _is_cache_dir_secure should return True for a directory owned by the
        current user and not writable by group/other (creating it if needed).
        """
        archiver = self._make_archiver(tmp_path / "cache")

        assert archiver._is_cache_dir_secure is True

    def test_is_cache_dir_secure_false_when_lstat_fails(self, tmp_path):
        """
        _is_cache_dir_secure should return False when the cache directory cannot
        be created or stat-ed (e.g. a parent path component is not a directory).
        """
        blocking_file = tmp_path / "not_a_dir"
        blocking_file.write_text("content")
        cache_dir = blocking_file / "cache"
        archiver = self._make_archiver(cache_dir)

        assert archiver._is_cache_dir_secure is False

    def test_is_cache_dir_secure_false_for_symlinked_directory(self, tmp_path, caplog):
        """
        _is_cache_dir_secure should refuse a cache directory that is actually a
        symlink, even if it points at a directory owned by the current user,
        since it could have been planted by another user.
        """
        real_dir = tmp_path / "real_cache"
        real_dir.mkdir()
        cache_dir = tmp_path / "cache_link"
        os.symlink(str(real_dir), str(cache_dir))
        archiver = self._make_archiver(cache_dir)

        with caplog.at_level(logging.WARNING, logger="barman.wal_archiver"):
            result = archiver._is_cache_dir_secure

        assert result is False
        assert "not a regular directory" in caplog.text

    @patch("barman.wal_archiver.os.getuid")
    def test_is_cache_dir_secure_false_when_not_owned_by_current_user(
        self, mock_getuid, tmp_path, caplog
    ):
        """
        _is_cache_dir_secure should refuse a cache directory not owned by the
        current user.
        """
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        # Make getuid() disagree with the directory's real owner
        mock_getuid.return_value = os.stat(str(cache_dir)).st_uid + 1
        archiver = self._make_archiver(cache_dir)

        with caplog.at_level(logging.WARNING, logger="barman.wal_archiver"):
            result = archiver._is_cache_dir_secure

        assert result is False
        assert "not owned by the current user" in caplog.text

    def test_is_cache_dir_secure_false_when_group_or_other_writable(
        self, tmp_path, caplog
    ):
        """
        _is_cache_dir_secure should refuse a cache directory that is writable by
        group or others.
        """
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        os.chmod(str(cache_dir), 0o777)
        archiver = self._make_archiver(cache_dir)

        with caplog.at_level(logging.WARNING, logger="barman.wal_archiver"):
            result = archiver._is_cache_dir_secure

        assert result is False
        assert "writable by group or others" in caplog.text

    @patch("barman.wal_archiver.os.path.getsize")
    def test_archive_disables_parallel_when_cache_dir_insecure(
        self, mock_getsize, tmp_path, caplog
    ):
        """
        archive() should disable parallel archival (and not raise) when the
        cache directory is not secure, so concurrent workers never rely on an
        untrusted cache.
        """
        mock_getsize.return_value = barman.xlog.DEFAULT_XLOG_SEG_SIZE
        archiver = self._make_archiver(tmp_path)
        wal_path = "/pg_wal/000000010000000000000001"

        with patch.object(
            type(archiver), "_is_cache_dir_secure", new_callable=PropertyMock
        ) as mock_secure:
            mock_secure.return_value = False
            with patch.object(archiver, "_archive_single_wal") as mock_archive:
                with patch.object(
                    archiver, "_get_wals_to_prefetch"
                ) as mock_get_prefetch:
                    with caplog.at_level(logging.WARNING, logger="barman.wal_archiver"):
                        archiver.archive(wal_path, parallel=4)

        # THEN archival still proceeds, but without any prefetching
        mock_archive.assert_called_once_with(wal_path)
        mock_get_prefetch.assert_called_once_with(wal_path, 0, ANY)
        assert "disabling parallel" in caplog.text

    @patch("barman.wal_archiver.os.path.getsize")
    @patch("barman.wal_archiver.WalPrefetchWorker")
    def test_archive_keeps_parallel_when_cache_dir_secure(
        self, mock_worker_cls, mock_getsize, tmp_path
    ):
        """
        archive() should keep the requested parallelism when the cache
        directory is secure.
        """
        mock_getsize.return_value = barman.xlog.DEFAULT_XLOG_SEG_SIZE
        archiver = self._make_archiver(tmp_path)
        wal_path = "/pg_wal/000000010000000000000001"

        with patch.object(
            type(archiver), "_is_cache_dir_secure", new_callable=PropertyMock
        ) as mock_secure:
            mock_secure.return_value = True
            with patch.object(archiver, "_archive_single_wal"):
                with patch.object(
                    archiver, "_get_wals_to_prefetch"
                ) as mock_get_prefetch:
                    archiver.archive(wal_path, parallel=4)

        # THEN the full prefetch count (parallel - 1) is requested
        mock_get_prefetch.assert_called_once_with(wal_path, 3, ANY)

    @patch("barman.wal_archiver.os.path.getsize")
    def test_archive_does_not_check_cache_dir_security_when_not_parallel(
        self, mock_getsize, tmp_path
    ):
        """
        archive() should not even check the cache directory security when
        parallel <= 1, since no shared cache reliance is involved.
        """
        mock_getsize.return_value = barman.xlog.DEFAULT_XLOG_SEG_SIZE
        archiver = self._make_archiver(tmp_path)
        wal_path = "/pg_wal/000000010000000000000001"

        with patch.object(
            type(archiver), "_is_cache_dir_secure", new_callable=PropertyMock
        ) as mock_secure:
            with patch.object(archiver, "_archive_single_wal"):
                with patch.object(archiver, "_get_wals_to_prefetch", return_value=[]):
                    archiver.archive(wal_path, parallel=1)

        mock_secure.assert_not_called()

    def test_write_last_wal_archived_writes_atomically(self, tmp_path):
        """
        _write_last_wal_archived must write via a temporary file and then rename.
        """
        archiver = self._make_archiver(tmp_path)
        tmp_file_path = str(tmp_path / "tmpXXXXXX")

        mock_tmp = MagicMock()
        mock_tmp.__enter__ = MagicMock(return_value=mock_tmp)
        mock_tmp.__exit__ = MagicMock(return_value=False)
        mock_tmp.name = tmp_file_path

        with (
            patch(
                "barman.wal_archiver.NamedTemporaryFile", return_value=mock_tmp
            ) as mock_ntf,
            patch("barman.wal_archiver.os.rename") as mock_rename,
            patch("barman.wal_archiver.os.makedirs"),
        ):
            archiver._write_last_wal_archived("000000010000000000000042")

            mock_ntf.assert_called_once_with(mode="w", dir=str(tmp_path), delete=False)
            mock_tmp.write.assert_called_once_with("000000010000000000000042")
            mock_rename.assert_called_once_with(
                tmp_file_path, archiver.last_archived_cache_path
            )

    def test_write_last_wal_archived_logs_warning_on_io_error(self, tmp_path, caplog):
        """
        _write_last_wal_archived should log a warning on I/O error.
        """
        import logging

        archiver = self._make_archiver(tmp_path)

        with patch(
            "barman.wal_archiver.NamedTemporaryFile", side_effect=IOError("disk full")
        ):
            with patch("barman.wal_archiver.os.makedirs"):
                with caplog.at_level(logging.WARNING, logger="barman.wal_archiver"):
                    archiver._write_last_wal_archived("000000010000000000000042")

        assert (
            "Failed to write last archived WAL file name to cache file" in caplog.text
        )


class TestCloudWalArchiver:
    """Tests for :class:`CloudWalArchiver` cloud-specific behavior."""

    def _make_archiver(self):
        """Return a :class:`CloudWalArchiver` backed by a mocked backup manager."""
        backup_manager = build_backup_manager()
        backup_manager.server.wal_storage = MagicMock(spec=CloudWalStorageStrategy)
        backup_manager.server.xlogdb = MagicMock()
        backup_manager.server.xlogdb.return_value.__enter__ = MagicMock(
            return_value=MagicMock()
        )
        backup_manager.server.xlogdb.return_value.__exit__ = MagicMock(
            return_value=False
        )
        backup_manager.server.meta_directory = "/barman/meta"
        backup_manager.server.get_errors_dst = MagicMock(
            return_value="/barman/errors/some_wal.duplicate"
        )
        backup_manager.compression_manager.get_default_compressor = MagicMock(
            return_value=None
        )
        backup_manager.encryption_manager.get_encryption = MagicMock(return_value=None)
        return CloudWalArchiver(backup_manager)

    def test_archive_single_wal_handles_matching_duplicate(self):
        """
        _archive_single_wal should silently skip a WAL that is already in cloud
        storage with the same content (MatchingDuplicateWalFile).
        """
        archiver = self._make_archiver()
        archiver.wal_storage.save.side_effect = MatchingDuplicateWalFile("wal_name")

        wal_path = "/pg_wal/000000010000000000000001"

        # WHEN _archive_single_wal is called and a matching duplicate is detected
        # THEN no exception is raised
        with patch.object(archiver, "_build_wal_info") as mock_build:
            mock_build.return_value = MagicMock(name="000000010000000000000001")
            archiver._archive_single_wal(wal_path)

        # AND save was called once
        archiver.wal_storage.save.assert_called_once()

    @patch("barman.wal_archiver.shutil.copy")
    def test_archive_single_wal_copies_conflicting_duplicate_to_errors(
        self, mock_shutil_copy
    ):
        """
        _archive_single_wal should copy the WAL to the errors directory when it
        conflicts with a different WAL of the same name already in cloud storage
        (DuplicateWalFile).
        """
        archiver = self._make_archiver()
        archiver.wal_storage.save.side_effect = DuplicateWalFile("wal_name")

        wal_path = "/pg_wal/000000010000000000000001"
        mock_wal_info = MagicMock()
        mock_wal_info.name = "000000010000000000000001"
        mock_wal_info.orig_filename = wal_path

        # WHEN _archive_single_wal is called with a conflicting duplicate
        with patch.object(archiver, "_build_wal_info", return_value=mock_wal_info):
            archiver._archive_single_wal(wal_path)

        # THEN get_errors_dst was called to resolve the error destination
        archiver.server.get_errors_dst.assert_called_once_with(
            mock_wal_info.name, "duplicate"
        )

        # AND shutil.copy was used to copy (not move) the WAL to the errors directory
        mock_shutil_copy.assert_called_once_with(
            mock_wal_info.orig_filename,
            archiver.server.get_errors_dst.return_value,
        )

    def test_post_archive_exec_stops_writing_at_first_worker_failure(self):
        """
        _post_archive_exec should stop writing to xlogdb at the first failed
        worker, to avoid gaps in the archive.
        """
        # GIVEN a main WAL path and three workers: first succeeds, second fails, third succeeds
        main_wal_path = "/pg_wal/000000010000000000000001"

        main_wal_info = MagicMock()
        main_wal_info.name = "000000010000000000000001"
        main_wal_info.to_xlogdb_line.return_value = "line1\n"

        worker_ok_wal_info = MagicMock()
        worker_ok_wal_info.name = "000000010000000000000002"
        worker_ok_wal_info.to_xlogdb_line.return_value = "line2\n"

        worker_ok = MagicMock(spec=WalPrefetchWorker)
        worker_ok.success = True
        worker_ok.wal_path = "/pg_wal/000000010000000000000002"

        worker_fail = MagicMock(spec=WalPrefetchWorker)
        worker_fail.success = False
        worker_fail.wal_path = "/pg_wal/000000010000000000000003"

        worker_after_fail = MagicMock(spec=WalPrefetchWorker)
        worker_after_fail.success = True
        worker_after_fail.wal_path = "/pg_wal/000000010000000000000004"

        archiver = self._make_archiver()
        mock_fxlogdb = MagicMock()
        archiver.server.xlogdb.return_value.__enter__.return_value = mock_fxlogdb

        with patch.object(
            archiver,
            "_build_wal_info",
            side_effect=[main_wal_info, worker_ok_wal_info],
        ):
            archiver._post_archive_exec(
                main_wal_path, [worker_ok, worker_fail, worker_after_fail]
            )

        # THEN only main WAL and first successful worker's one are written to xlogdb
        # Neither the failed worker's WAL nor the one after it should be written
        assert mock_fxlogdb.write.call_count == 2
        mock_fxlogdb.write.assert_any_call("line1\n")
        mock_fxlogdb.write.assert_any_call("line2\n")

    def test_post_archive_exec_writes_history_file_to_xlogdb(self):
        """
        _post_archive_exec should write a .history file to xlogdb like any other
        archived file. xlogdb records everything that was archived; it is only
        the last-archived cache (see
        TestParallelWalArchiver.test_update_metadata_does_not_cache_history_or_backup_file)
        that deliberately excludes .history/.backup files, to avoid corrupting
        cross-timeline detection.
        """
        # GIVEN the main "WAL" being archived is actually a .history file
        history_path = "/pg_wal/00000003.history"
        history_info = MagicMock()
        history_info.name = "00000003.history"
        history_info.to_xlogdb_line.return_value = "history-line\n"

        archiver = self._make_archiver()
        mock_fxlogdb = MagicMock()
        archiver.server.xlogdb.return_value.__enter__.return_value = mock_fxlogdb

        # WHEN _post_archive_exec is called with no prefetch workers
        with patch.object(archiver, "_build_wal_info", return_value=history_info):
            archiver._post_archive_exec(history_path, [])

        # THEN the xlogdb entry is still written (history files are valid xlogdb entries)
        mock_fxlogdb.write.assert_called_once_with("history-line\n")
