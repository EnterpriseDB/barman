# Copyright (C) 2013-2016 2ndQuadrant Italia Srl
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

import pytest
from mock import ANY, MagicMock, patch

import barman.xlog
from barman.backup import DuplicateWalFile, MatchingDuplicateWalFile
from barman.command_wrappers import CommandFailedException
from barman.compression import PyGZipCompressor, identify_compression
from barman.infofile import WalFileInfo
from barman.process import ProcessInfo
from barman.server import CheckOutputStrategy
from barman.wal_archiver import (ArchiverFailure, FileWalArchiver,
                                 StreamingWalArchiver, WalArchiverBatch)
from testing_helpers import build_backup_manager, build_test_backup_info


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

    @patch('os.unlink')
    @patch('barman.wal_archiver.FileWalArchiver.get_next_batch')
    @patch('barman.wal_archiver.FileWalArchiver.archive_wal')
    @patch('shutil.move')
    @patch('datetime.datetime')
    def test_archive(self, datetime_mock, move_mock, archive_wal_mock,
                     get_next_batch_mock, unlink_mock, capsys):
        """
        Test FileWalArchiver.archive method
        """
        fxlogdb_mock = MagicMock()
        backup_manager = MagicMock()
        archiver = FileWalArchiver(backup_manager)
        archiver.config.name = "test_server"

        wal_info = WalFileInfo(name="test_wal_file")
        wal_info.orig_filename = "test_wal_file"

        batch = WalArchiverBatch([wal_info])
        get_next_batch_mock.return_value = batch
        archive_wal_mock.side_effect = DuplicateWalFile

        archiver.archive(fxlogdb_mock)

        out, err = capsys.readouterr()
        assert ("\tError: %s is already present in server %s. "
                "File moved to errors directory." %
                (wal_info.name, archiver.config.name)) in out

        archive_wal_mock.side_effect = MatchingDuplicateWalFile
        archiver.archive(fxlogdb_mock)
        unlink_mock.assert_called_with(wal_info.orig_filename)

        # Test batch errors
        datetime_mock.utcnow.strftime.return_value = 'test_time'
        batch.errors = ['testfile_1', 'testfile_2']
        archive_wal_mock.side_effect = DuplicateWalFile
        archiver.archive(fxlogdb_mock)
        out, err = capsys.readouterr()

        assert ("Some unknown objects have been found while "
                "processing xlog segments for %s. "
                "Objects moved to errors directory:" %
                archiver.config.name) in out

        move_mock.assert_any_call(
            'testfile_1',
            os.path.join(archiver.config.errors_directory,
                         "%s.%s.unknown" % ('testfile_1', 'test_time')))

        move_mock.assert_any_call(
            'testfile_2',
            os.path.join(archiver.config.errors_directory,
                         "%s.%s.unknown" % ('testfile_2', 'test_time')))

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
            name='TestServer',
            global_conf={
                'barman_home': tmpdir.strpath
            })
        b_info = build_test_backup_info(
            backup_id='fake_backup_id',
            server=backup_manager.server,
            begin_wal='000000010000000000000001'
        )
        b_info.save()
        backup_manager.server.get_backup.return_value = b_info
        backup_manager.compression_manager.get_compressor.return_value = None
        # Build the basic folder structure and files
        basedir = tmpdir.join('main')
        incoming_dir = basedir.join('incoming')
        archive_dir = basedir.join('wals')
        xlog_db = archive_dir.join('xlog.db')
        wal_name = '000000010000000000000001'
        wal_file = incoming_dir.join(wal_name)
        wal_file.ensure()
        archive_dir.ensure(dir=True)
        xlog_db.ensure()
        backup_manager.server.xlogdb.return_value.__enter__.return_value = \
            xlog_db.open(mode='a')
        backup_manager.server.archivers = [FileWalArchiver(backup_manager)]

        backup_manager.archive_wal()
        wal_path = os.path.join(archive_dir.strpath,
                                barman.xlog.hash_dir(wal_name),
                                wal_name)
        # Check for the presence of the wal file in the wal catalog
        with xlog_db.open() as f:
            line = str(f.readline())
            assert wal_name in line
        # Check that the wal file have been moved from the incoming dir
        assert not os.path.exists(wal_file.strpath)
        # Check that the wal file have been archived to the expected location
        assert os.path.exists(wal_path)

    def test_archive_wal(self, tmpdir, capsys):
        """
        Test WalArchiver.archive_wal behaviour when the WAL file already
        exists in the archive
        """

        # Setup the test environment
        backup_manager = build_backup_manager(
            name='TestServer',
            global_conf={
                'barman_home': tmpdir.strpath
            })
        backup_manager.compression_manager.get_compressor.return_value = None
        backup_manager.server.get_backup.return_value = None

        basedir = tmpdir.join('main')
        incoming_dir = basedir.join('incoming')
        archive_dir = basedir.join('wals')
        xlog_db = archive_dir.join('xlog.db')
        wal_name = '000000010000000000000001'
        wal_file = incoming_dir.join(wal_name)
        wal_file.ensure()
        archive_dir.ensure(dir=True)
        xlog_db.ensure()
        backup_manager.server.xlogdb.return_value.__enter__.return_value = (
            xlog_db.open(mode='a'))
        archiver = FileWalArchiver(backup_manager)
        backup_manager.server.archivers = [archiver]

        # Tests a basic archival process
        wal_info = WalFileInfo.from_file(wal_file.strpath)
        archiver.archive_wal(None, wal_info)

        assert not os.path.exists(wal_file.strpath)
        assert os.path.exists(wal_info.fullpath(backup_manager.server))

        # Tests the archiver behaviour for duplicate WAL files, as the
        # wal file named '000000010000000000000001' was already archived
        # in the previous test
        wal_file.ensure()
        wal_info = WalFileInfo.from_file(wal_file.strpath)

        with pytest.raises(MatchingDuplicateWalFile):
            archiver.archive_wal(None, wal_info)

        # Tests the archiver behaviour for duplicated WAL files with
        # different contents
        wal_file.write('test')
        wal_info = WalFileInfo.from_file(wal_file.strpath)

        with pytest.raises(DuplicateWalFile):
            archiver.archive_wal(None, wal_info)

        # Tests the archiver behaviour for duplicate WAL files, as the
        # wal file named '000000010000000000000001' was already archived
        # in the previous test and the input file uses compression
        compressor = PyGZipCompressor(backup_manager.config, 'pygzip')
        compressor.compress(wal_file.strpath, wal_file.strpath)
        wal_info = WalFileInfo.from_file(wal_file.strpath)
        assert os.path.exists(wal_file.strpath)
        backup_manager.compression_manager.get_compressor.return_value = (
            compressor)

        with pytest.raises(MatchingDuplicateWalFile):
            archiver.archive_wal(None, wal_info)

        # Test the archiver behaviour when the incoming file is compressed
        # and it has been already archived and compressed.
        compressor.compress(wal_info.fullpath(backup_manager.server),
                            wal_info.fullpath(backup_manager.server))

        wal_info = WalFileInfo.from_file(wal_file.strpath)
        with pytest.raises(MatchingDuplicateWalFile):
            archiver.archive_wal(None, wal_info)

        # Reset the status of the incoming and WALs directory
        # removing the files archived during the preceding tests.
        os.unlink(wal_info.fullpath(backup_manager.server))
        os.unlink(wal_file.strpath)

        # Test the archival of a WAL file using compression.
        wal_file.write('test')
        wal_info = WalFileInfo.from_file(wal_file.strpath)
        archiver.archive_wal(compressor, wal_info)
        assert os.path.exists(wal_info.fullpath(backup_manager.server))
        assert not os.path.exists(wal_file.strpath)
        assert 'gzip' == identify_compression(
            wal_info.fullpath(backup_manager.server)
        )

    # TODO: The following test should be splitted in two
    # the BackupManager part and the FileWalArchiver part
    def test_archive_wal_no_backup(self, tmpdir, capsys):
        """
        Test archive-wal behaviour when there are no backups.

        Expect it to archive the files anyway
        """
        # Build a real backup manager
        backup_manager = build_backup_manager(
            name='TestServer',
            global_conf={
                'barman_home': tmpdir.strpath
            })
        backup_manager.compression_manager.get_compressor.return_value = None
        backup_manager.server.get_backup.return_value = None
        # Build the basic folder structure and files
        basedir = tmpdir.join('main')
        incoming_dir = basedir.join('incoming')
        archive_dir = basedir.join('wals')
        xlog_db = archive_dir.join('xlog.db')
        wal_name = '000000010000000000000001'
        wal_file = incoming_dir.join(wal_name)
        wal_file.ensure()
        archive_dir.ensure(dir=True)
        xlog_db.ensure()
        backup_manager.server.xlogdb.return_value.__enter__.return_value = \
            xlog_db.open(mode='a')
        backup_manager.server.archivers = [FileWalArchiver(backup_manager)]

        backup_manager.archive_wal()

        # Check that the WAL file is present inside the wal catalog
        with xlog_db.open() as f:
            line = str(f.readline())
            assert wal_name in line
        wal_path = os.path.join(archive_dir.strpath,
                                barman.xlog.hash_dir(wal_name),
                                wal_name)
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
            name='TestServer',
            global_conf={
                'barman_home': tmpdir.strpath
            })
        b_info = build_test_backup_info(
            backup_id='fake_backup_id',
            server=backup_manager.server,
            begin_wal='000000010000000000000002'
        )
        b_info.save()
        # Build the basic folder structure and files
        backup_manager.compression_manager.get_compressor.return_value = None
        backup_manager.server.get_backup.return_value = b_info
        basedir = tmpdir.join('main')
        incoming_dir = basedir.join('incoming')
        basedir.mkdir('errors')
        archive_dir = basedir.join('wals')
        xlog_db = archive_dir.join('xlog.db')
        wal_name = '000000010000000000000001'
        wal_file = incoming_dir.join(wal_name)
        wal_file.ensure()
        archive_dir.ensure(dir=True)
        xlog_db.ensure()
        backup_manager.server.xlogdb.return_value.__enter__.return_value = \
            xlog_db.open(mode='a')
        backup_manager.server.archivers = [FileWalArchiver(backup_manager)]

        backup_manager.archive_wal()

        # Check that the WAL file is not present inside the wal catalog
        with xlog_db.open() as f:
            line = str(f.readline())
            assert wal_name in line
        wal_path = os.path.join(archive_dir.strpath,
                                barman.xlog.hash_dir(wal_name),
                                wal_name)
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
            name='TestServer',
            global_conf={
                'barman_home': tmpdir.strpath
            })
        b_info = build_test_backup_info(
            backup_id='fake_backup_id',
            server=backup_manager.server,
            begin_wal='000000020000000000000002',
            timeline=2
        )
        b_info.save()
        # Build the basic folder structure and files
        backup_manager.compression_manager.get_compressor.return_value = None
        backup_manager.server.get_backup.return_value = b_info
        basedir = tmpdir.join('main')
        incoming_dir = basedir.join('incoming')
        basedir.mkdir('errors')
        archive_dir = basedir.join('wals')
        xlog_db = archive_dir.join('xlog.db')
        wal_name = '000000010000000000000001'
        wal_file = incoming_dir.join(wal_name)
        wal_file.ensure()
        archive_dir.ensure(dir=True)
        xlog_db.ensure()
        backup_manager.server.xlogdb.return_value.__enter__.return_value = \
            xlog_db.open(mode='a')
        backup_manager.server.archivers = [FileWalArchiver(backup_manager)]

        backup_manager.archive_wal()

        # Check that the WAL file is present inside the wal catalog
        with xlog_db.open() as f:
            line = str(f.readline())
            assert wal_name in line
        wal_path = os.path.join(archive_dir.strpath,
                                barman.xlog.hash_dir(wal_name),
                                wal_name)
        # Check that the wal file have been archived
        assert os.path.exists(wal_path)
        # Check the output for the archival of the wal file
        out, err = capsys.readouterr()
        assert ("\t%s\n" % wal_name) in out

    @patch('barman.wal_archiver.glob')
    @patch('os.path.isfile')
    @patch('barman.wal_archiver.WalFileInfo.from_file')
    def test_get_next_batch(self, from_file_mock, isfile_mock, glob_mock):
        """
        Test the FileWalArchiver.get_next_batch method
        """

        # WAL batch no errors
        glob_mock.return_value = ['000000010000000000000001']
        isfile_mock.return_value = True
        # This is an hack, instead of a WalFileInfo we use a simple string to
        # ease all the comparisons. The resulting string is the name enclosed
        # in colons. e.g. ":000000010000000000000001:"
        from_file_mock.side_effect = lambda wal_name: ':%s:' % wal_name

        backup_manager = build_backup_manager(
            name='TestServer'
        )
        archiver = FileWalArchiver(backup_manager)
        backup_manager.server.archivers = [archiver]

        batch = archiver.get_next_batch()
        assert [':000000010000000000000001:'] == batch

        # WAL batch with errors
        wrong_file_name = 'test_wrong_wal_file.2'
        glob_mock.return_value = ['test_wrong_wal_file.2']
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
    def test_receive_wal(self, receivexlog_mock, remote_mock, tmpdir):
        backup_manager = build_backup_manager(
            main_conf={
                'backup_directory': tmpdir
            },
        )
        backup_manager.server.streaming.server_txt_version = "9.4.0"
        backup_manager.server.streaming.get_remote_status.return_value = {
            "streaming_supported": True
        }
        streaming_dir = tmpdir.join('streaming')
        streaming_dir.ensure(dir=True)
        # Test: normal run
        archiver = StreamingWalArchiver(backup_manager)
        remote_mock.return_value = {
            'pg_receivexlog_installed': True,
            'pg_receivexlog_compatible': True,
            'pg_receivexlog_path': 'fake/path'
        }

        # Test: execute a reset request
        partial = streaming_dir.join('test.partial')
        partial.ensure()
        archiver.receive_wal(reset=True)
        assert not partial.check()

        archiver.receive_wal(reset=False)
        receivexlog_mock.assert_called_once_with(
            'fake/path',
            'host=pg01.nowhere user=postgres port=5432',
            streaming_dir.strpath,
            out_handler=ANY,
            err_handler=ANY,
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
        backup_manager.server.process_manager.list.return_value = []
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert out == \
            "\tpg_receivexlog: OK\n" \
            "\tpg_receivexlog compatible: OK\n" \
            "\treceive-wal running: FAILED " \
            "(See the Barman log file for more details)\n"

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
            "(PostgreSQL version: 9.5, pg_receivexlog version: 9.2)\n" \
            "\treceive-wal running: FAILED " \
            "(See the Barman log file for more details)\n"
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
            "(PostgreSQL version: 9.5, pg_receivexlog version: None)\n" \
            "\treceive-wal running: FAILED " \
            "(See the Barman log file for more details)\n"

        # Case: receive-wal running
        backup_manager.server.process_manager.list.return_value = [
            ProcessInfo(pid=1,
                        server_name=backup_manager.config.name,
                        task="receive-wal")
        ]
        archiver.check(strategy)
        (out, err) = capsys.readouterr()
        assert out == \
            "\tpg_receivexlog: OK\n" \
            "\tpg_receivexlog compatible: FAILED " \
            "(PostgreSQL version: 9.5, pg_receivexlog version: None)\n" \
            "\treceive-wal running: OK\n"

    @patch('barman.wal_archiver.glob')
    @patch('os.path.isfile')
    @patch('barman.wal_archiver.WalFileInfo.from_file')
    def test_get_next_batch(self, from_file_mock, isfile_mock, glob_mock):
        """
        Test the FileWalArchiver.get_next_batch method
        """

        # WAL batch, with 000000010000000000000001 that is currently being
        # written
        glob_mock.return_value = ['000000010000000000000001']
        isfile_mock.return_value = True
        # This is an hack, instead of a WalFileInfo we use a simple string to
        # ease all the comparisons. The resulting string is the name enclosed
        # in colons. e.g. ":000000010000000000000001:"
        from_file_mock.side_effect = lambda wal_name, compression: (
            ':%s:' % wal_name)

        backup_manager = build_backup_manager(
            name='TestServer'
        )
        archiver = StreamingWalArchiver(backup_manager)
        backup_manager.server.archivers = [archiver]

        batch = archiver.get_next_batch()
        assert ['000000010000000000000001'] == batch.skip

        # WAL batch, with 000000010000000000000002 that is currently being
        # written and 000000010000000000000001 can be archived
        glob_mock.return_value = [
            '000000010000000000000001',
            '000000010000000000000002',
        ]
        batch = archiver.get_next_batch()
        assert [':000000010000000000000001:'] == batch
        assert ['000000010000000000000002'] == batch.skip

        # WAL batch, with two partial files.
        glob_mock.return_value = [
            '000000010000000000000001.partial',
            '000000010000000000000002.partial',
        ]
        batch = archiver.get_next_batch()
        assert ['000000010000000000000001.partial'] == batch.errors
        assert ['000000010000000000000002.partial'] == batch.skip

        # WAL batch with errors
        wrong_file_name = 'test_wrong_wal_file.2'
        glob_mock.return_value = ['test_wrong_wal_file.2']
        batch = archiver.get_next_batch()
        assert [wrong_file_name] == batch.errors
