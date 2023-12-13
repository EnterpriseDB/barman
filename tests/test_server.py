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

import datetime
import hashlib
import json
import os
import tarfile
import time
from collections import namedtuple
import dateutil.tz
from io import BytesIO

import pytest
from mock import MagicMock, Mock, PropertyMock, patch
from psycopg2.tz import FixedOffsetTimezone

from barman import output
from barman.exceptions import (
    CommandFailedException,
    LockFileBusy,
    LockFilePermissionDenied,
    PostgresDuplicateReplicationSlot,
    PostgresInvalidReplicationSlot,
    PostgresReplicationSlotsFull,
    PostgresSuperuserRequired,
    PostgresUnsupportedFeature,
)
from barman.infofile import BackupInfo, WalFileInfo
from barman.lockfile import (
    ServerBackupLock,
    ServerCronLock,
    ServerWalArchiveLock,
    ServerWalReceiveLock,
)
from barman.postgres import PostgreSQLConnection
from barman.process import ProcessInfo
from barman.server import CheckOutputStrategy, CheckStrategy, Server
from testing_helpers import (
    build_config_from_dicts,
    build_real_server,
    build_test_backup_info,
)


class ExceptionTest(Exception):
    """
    Exception for test purposes
    """

    pass


def create_fake_info_file(name, size, time, compression=None):
    info = WalFileInfo()
    info.name = name
    info.size = size
    info.time = time
    info.compression = compression
    return info


def get_wal_lines_from_wal_list(wal_list):
    """
    converts each wal_info to an xlogdb line and concats into one string
    """
    walstring = ""
    for wal_info in wal_list:
        walstring += wal_info.to_xlogdb_line()
    return walstring


def get_wal_names_from_indices_selection(wal_info_files, indices):
    # Prepare expected list
    expected_wals = []
    for index in indices:
        expected_wals.append(wal_info_files[index].name)
    return expected_wals


# noinspection PyMethodMayBeStatic
class TestServer(object):
    def test_init(self):
        """
        Basic initialization test with minimal parameters
        """
        server = Server(
            build_config_from_dicts(global_conf={"archiver": "on"}).get_server("main")
        )
        assert not server.config.disabled

    def test_rerun_init(self):
        """
        Check initializing with the same config again
        """
        cfg = build_config_from_dicts(global_conf={"archiver": "on"}).get_server("main")
        cfg.minimum_redundancy = "2"
        cfg.retention_policy = "RECOVERY WINDOW OF 4 WEEKS"

        Server(cfg)
        assert cfg.minimum_redundancy == 2
        assert cfg.retention_policy
        assert cfg.retention_policy.mode == "window"
        assert cfg.retention_policy.value == 4
        assert cfg.retention_policy.unit == "w"

        Server(cfg)
        assert cfg.minimum_redundancy == 2
        assert cfg.retention_policy
        assert cfg.retention_policy.mode == "window"
        assert cfg.retention_policy.value == 4
        assert cfg.retention_policy.unit == "w"

    def test_bad_init(self):
        """
        Check the server is buildable with an empty configuration
        """
        server = Server(
            build_config_from_dicts(
                main_conf={
                    "conninfo": "",
                    "ssh_command": "",
                }
            ).get_server("main")
        )
        assert server.config.disabled
        # ARCHIVER_OFF_BACKCOMPATIBILITY - START OF CODE
        # # Check that either archiver or streaming_archiver are set
        # server = Server(build_config_from_dicts(
        #     main_conf={
        #         'archiver': 'off',
        #         'streaming_archiver': 'off'
        #     }
        # ).get_server('main'))
        # assert server.config.disabled
        # assert "No archiver enabled for server 'main'. " \
        #        "Please turn on 'archiver', 'streaming_archiver' or " \
        #        "both" in server.config.msg_list
        # ARCHIVER_OFF_BACKCOMPATIBILITY - START OF CODE
        server = Server(
            build_config_from_dicts(
                main_conf={
                    "archiver": "off",
                    "streaming_archiver": "on",
                    "slot_name": "",
                }
            ).get_server("main")
        )
        assert server.config.disabled
        assert (
            "Streaming-only archiver requires 'streaming_conninfo' and "
            "'slot_name' options to be properly configured" in server.config.msg_list
        )

    def test_primary_init(self):
        """Verify standby properties do not exist when no primary_conninfo is set"""
        # GIVEN a server with a default config
        cfg = build_config_from_dicts().get_server("main")
        # WHEN the server is instantiated
        server = Server(cfg)
        # THEN the postgres connection has no primary connection
        assert not hasattr(server.postgres, "primary")

    def test_standby_init(self):
        """Verify standby properties exist when primary_conninfo is set"""
        # GIVEN a server with primary_conninfo set
        cfg = build_config_from_dicts(
            main_conf={"primary_conninfo": "db=primary"},
        ).get_server("main")
        # WHEN the server is instantiated
        server = Server(cfg)
        # THEN the postgres connection has a primary connection
        assert server.postgres.primary is not None

    def test_check_config_missing(self, tmpdir):
        """
        Verify the check method can be called on an empty configuration
        """
        server = Server(
            build_config_from_dicts(
                global_conf={
                    # Required by server.check_archive method
                    "barman_lock_directory": tmpdir.mkdir("lock").strpath
                },
                main_conf={
                    "conninfo": "",
                    "ssh_command": "",
                    # Required by server.check_archive method
                    "wals_directory": tmpdir.mkdir("wals").strpath,
                },
            ).get_server("main")
        )
        check_strategy = CheckOutputStrategy()
        server.check(check_strategy)
        assert check_strategy.has_error

    @patch("barman.server.os")
    def test_xlogdb_with_exception(self, os_mock, tmpdir):
        """
        Testing the execution of xlog-db operations with an Exception

        :param os_mock: mock for os module
        :param tmpdir: temporary directory unique to the test invocation
        """
        # unpatch os.path
        os_mock.path = os.path
        # Setup temp dir and server
        server = build_real_server(
            global_conf={"barman_lock_directory": tmpdir.mkdir("lock").strpath},
            main_conf={"wals_directory": tmpdir.mkdir("wals").strpath},
        )
        # Test the execution of the fsync on xlogdb file forcing an exception
        with pytest.raises(ExceptionTest):
            with server.xlogdb("w") as fxlogdb:
                fxlogdb.write("00000000000000000000")
                raise ExceptionTest()
        # Check call on fsync method. If the call have been issued,
        # the "exit" section of the contextmanager have been executed
        assert os_mock.fsync.called

    @patch("barman.server.os")
    @patch("barman.server.ServerXLOGDBLock")
    def test_xlogdb(self, lock_file_mock, os_mock, tmpdir):
        """
        Testing the normal execution of xlog-db operations.

        :param lock_file_mock: mock for LockFile object
        :param os_mock: mock for os module
        :param tmpdir: temporary directory unique to the test invocation
        """
        # unpatch os.path
        os_mock.path = os.path
        # Setup temp dir and server
        server = build_real_server(
            global_conf={"barman_lock_directory": tmpdir.mkdir("lock").strpath},
            main_conf={"wals_directory": tmpdir.mkdir("wals").strpath},
        )
        # Test the execution of the fsync on xlogdb file
        with server.xlogdb("w") as fxlogdb:
            fxlogdb.write("00000000000000000000")
        # Check for calls on fsync method. If the call have been issued
        # the "exit" method of the contextmanager have been executed
        assert os_mock.fsync.called
        # Check for enter and exit calls on mocked LockFile
        lock_file_mock.return_value.__enter__.assert_called_once_with()
        lock_file_mock.return_value.__exit__.assert_called_once_with(None, None, None)

        os_mock.fsync.reset_mock()
        with server.xlogdb():
            # nothing to do here.
            pass
        # Check for calls on fsync method.
        # If the file is readonly exit method of the context manager must
        # skip calls on fsync method
        assert not os_mock.fsync.called

    def test_get_wal_full_path(self, tmpdir):
        """
        Testing Server.get_wal_full_path() method
        """
        wal_name = "0000000B00000A36000000FF"
        wal_hash = wal_name[:16]
        server = build_real_server(
            global_conf={"barman_lock_directory": tmpdir.mkdir("lock").strpath},
            main_conf={"wals_directory": tmpdir.mkdir("wals").strpath},
        )
        full_path = server.get_wal_full_path(wal_name)
        assert full_path == str(tmpdir.join("wals").join(wal_hash).join(wal_name))

    @pytest.mark.parametrize(
        ["wal_info_files", "target_tlis", "target_time", "expected_indices"],
        [
            (
                # GIVEN The following WALs
                [
                    create_fake_info_file("000000010000000000000002", 42, 43),
                    create_fake_info_file("00000001.history", 42, 43),
                    create_fake_info_file("000000020000000000000003", 42, 43),
                    create_fake_info_file("00000002.history", 42, 43),
                    create_fake_info_file("0000000A0000000000000005", 42, 43),
                    create_fake_info_file("0000000A.history", 42, 43),
                ],
                # AND target_tli values None, 2 and current
                (None, 2, "current"),
                # AND no target_time
                None,
                # WHEN get_required_xlog_files runs for a backup on tli 2
                # the WAL on tli 2 is returned along with all history files
                [1, 2, 3, 5],
            ),
            (
                # GIVEN The following WALs
                [
                    create_fake_info_file("000000010000000000000002", 42, 43),
                    create_fake_info_file("00000001.history", 42, 43),
                    create_fake_info_file("000000020000000000000003", 42, 43),
                    create_fake_info_file("000000020000000000000010", 42, 43),
                    create_fake_info_file("00000002.history", 42, 43),
                    create_fake_info_file("0000000A0000000000000005", 42, 43),
                    create_fake_info_file("0000000A.history", 42, 43),
                ],
                # AND target_tli values None, 2 and current
                (None, 2, "current"),
                # AND no target_time
                None,
                # WHEN get_required_xlog_files runs for a backup on tli 2
                # all WALs on tli 2 are returned along with all history files
                [1, 2, 3, 4, 6],
            ),
            (
                # GIVEN The following WALs
                [
                    create_fake_info_file("000000010000000000000002", 42, 43),
                    create_fake_info_file("00000001.history", 42, 43),
                    create_fake_info_file("000000020000000000000003", 42, 44),
                    create_fake_info_file("000000020000000000000005", 42, 45),
                    create_fake_info_file("000000020000000000000010", 42, 46),
                    create_fake_info_file("00000002.history", 42, 44),
                    create_fake_info_file("0000000A0000000000000005", 42, 47),
                    create_fake_info_file("0000000A.history", 42, 47),
                ],
                # AND target_tli values None, 2 and current
                (None, 2, "current"),
                # AND a target_time of 44
                44,
                # WHEN get_required_xlog_files runs for a backup on tli 2
                # the first two WALs on tli 2 are returned along with all history
                # files. The WAL on tli 2 which starts after the target_time is
                # not returned.
                [1, 2, 3, 5, 7],
            ),
            (
                # Verify both WALs on timeline 2 are returned plus all history files
                # when we specify the "latest" timeline
                [
                    create_fake_info_file("000000010000000000000002", 42, 43),
                    create_fake_info_file("00000001.history", 42, 43),
                    create_fake_info_file("000000020000000000000003", 42, 43),
                    create_fake_info_file("000000020000000000000010", 42, 43),
                    create_fake_info_file("00000002.history", 42, 43),
                    create_fake_info_file("0000000A0000000000000005", 42, 43),
                    create_fake_info_file("0000000A.history", 42, 43),
                ],
                # AND target_tli values of 10 and latest
                (10, "latest"),
                # AND no target_time
                None,
                # WHEN get_required_xlog_files runs for a backup on tli 2
                # all WALs on timelines 2 and 10 are returned along with all history
                # files.
                [1, 2, 3, 4, 5, 6],
            ),
        ],
    )
    def test_get_required_xlog_files(
        self, wal_info_files, target_tlis, target_time, expected_indices, tmpdir
    ):
        """
        Tests get_required_xlog_files function.
        Validates that exact expected walfile list matches result file list
        :param wal_info_files: List of fake WalFileInfo
        :param expected_indices: expected WalFileInfo.name indices (values refers to wal_info_files)
        :param tmpdir: _pytest.tmpdir
        """
        # Prepare input string
        walstring = get_wal_lines_from_wal_list(wal_info_files)

        # Prepare expected list
        expected_wals = get_wal_names_from_indices_selection(
            wal_info_files, expected_indices
        )

        # create a xlog.db and add those entries
        wals_dir = tmpdir.mkdir("wals")
        xlog = wals_dir.join("xlog.db")
        xlog.write(walstring)

        # Populate wals_dir with fake WALs
        for wal in wal_info_files:
            if wal.name.endswith("history"):
                wals_dir.join(wal.name).ensure()
            else:
                subdir = wal.name[0:16]
                wals_dir.join(subdir).join(wal.name).ensure()

        # fake backup
        backup = build_test_backup_info(
            begin_wal="000000020000000000000001",
            end_wal="000000020000000000000004",
            timeline=2,
        )

        # mock a server object and mock a return call to get_next_backup method
        server = build_real_server(
            global_conf={"barman_lock_directory": tmpdir.mkdir("lock").strpath},
            main_conf={"wals_directory": wals_dir.strpath},
        )

        for target_tli in target_tlis:
            wals = []
            for wal_file in server.get_required_xlog_files(
                backup, target_tli, target_time
            ):
                # get the result of the xlogdb read
                wals.append(wal_file.name)
            # Check for the presence of expected files
            assert expected_wals == wals

    @pytest.mark.parametrize(
        "wal_info_files,expected_indices",
        [
            (
                [
                    create_fake_info_file("000000010000000000000003", 42, 43),
                    create_fake_info_file("00000001.history", 42, 43),
                    create_fake_info_file("000000020000000000000003", 42, 43),
                    create_fake_info_file("00000002.history", 42, 43),
                    create_fake_info_file("000000030000000000000005", 42, 43),
                    create_fake_info_file("00000003.history", 42, 43),
                ],
                [1, 2, 3, 5],
            ),
            (
                [
                    create_fake_info_file("000000010000000000000003", 42, 43),
                    create_fake_info_file("00000001.history", 42, 43),
                    create_fake_info_file("000000020000000000000003", 42, 43),
                    create_fake_info_file("000000020000000000000010", 42, 43),
                    create_fake_info_file("00000002.history", 42, 43),
                    create_fake_info_file("000000030000000000000005", 42, 43),
                    create_fake_info_file("00000003.history", 42, 43),
                ],
                [1, 2],
            ),
        ],
    )
    @patch("barman.server.Server.get_next_backup")
    def test_get_wal_until_next_backup(
        self, get_backup_mock, wal_info_files, expected_indices, tmpdir
    ):
        """
        Test for the management of .history files and wal files
        Validates that exact expected walfile list matches result file list
        :param get_backup_mock: Mocks Server.get_next_backup function
        :param wal_info_files: List of fake WalFileInfo
        :param expected_indices: expected WalFileInfo.name indices (values refers to wal_info_files)
        :param tmpdir: _pytest.tmpdir
        """

        walstring = get_wal_lines_from_wal_list(wal_info_files)

        # Prepare expected list
        expected_wals = get_wal_names_from_indices_selection(
            wal_info_files, expected_indices
        )
        # create a xlog.db and add those entries
        wals_dir = tmpdir.mkdir("wals")
        xlog = wals_dir.join("xlog.db")
        xlog.write(walstring)
        # fake backup
        backup = build_test_backup_info(
            begin_wal="000000020000000000000001", end_wal="000000020000000000000004"
        )

        # mock a server object and mock a return call to get_next_backup method
        server = build_real_server(
            global_conf={"barman_lock_directory": tmpdir.mkdir("lock").strpath},
            main_conf={"wals_directory": wals_dir.strpath},
        )
        get_backup_mock.return_value = build_test_backup_info(
            backup_id="1234567899",
            begin_wal="000000020000000000000006",
            end_wal="000000020000000000000009",
        )

        wals = []
        for wal_file in server.get_wal_until_next_backup(backup, include_history=True):
            # get the result of the xlogdb read
            wals.append(wal_file.name)
        # Check for the presence of expected files
        assert expected_wals == wals

    @patch("barman.server.Server.get_remote_status")
    def test_pg_stat_archiver_show(self, remote_mock, capsys):
        """
        Test management of pg_stat_archiver view output in show command

        :param MagicMock remote_mock: mock the Server.get_remote_status method
        :param capsys: retrieve output from console

        """
        stats = {
            "failed_count": "2",
            "last_archived_wal": "000000010000000000000006",
            "last_archived_time": datetime.datetime.now(),
            "last_failed_wal": "000000010000000000000005",
            "last_failed_time": datetime.datetime.now(),
            "current_archived_wals_per_second": 1.0002,
        }
        remote_mock.return_value = dict(stats)

        server = build_real_server(
            global_conf={
                "archiver": "on",
                "last_backup_maximum_age": "1 day",
                # Silence the warning for default backup strategy
                "backup_options": "exclusive_backup",
            }
        )

        # Testing for show-servers command.
        # Expecting in the output the same values present into the stats dict
        server.show()

        # Parse the output
        (out, err) = capsys.readouterr()
        result = dict(
            item.strip("\t\n\r").split(": ") for item in out.split("\n") if item != ""
        )
        assert err == ""

        assert result["failed_count"] == stats["failed_count"]
        assert result["last_archived_wal"] == stats["last_archived_wal"]
        assert result["last_archived_time"] == str(stats["last_archived_time"])
        assert result["last_failed_wal"] == stats["last_failed_wal"]
        assert result["last_failed_time"] == str(stats["last_failed_time"])
        assert result["current_archived_wals_per_second"] == str(
            stats["current_archived_wals_per_second"]
        )

    @patch("barman.server.Server.status_postgres")
    @patch("barman.wal_archiver.FileWalArchiver.get_remote_status")
    def test_pg_stat_archiver_status(self, remote_mock, status_postgres_mock, capsys):
        """
        Test management of pg_stat_archiver view output in status command

        :param MagicMock remote_mock: mock the
            FileWalArchiver.get_remote_status method
        :param capsys: retrieve output from console
        """

        archiver_remote_status = {
            "archive_mode": "on",
            "archive_command": "send_to_barman.sh %p %f",
            "failed_count": "2",
            "last_archived_wal": "000000010000000000000006",
            "last_archived_time": datetime.datetime.now(),
            "last_failed_wal": "000000010000000000000005",
            "last_failed_time": datetime.datetime.now(),
            "current_archived_wals_per_second": 1.0002,
        }
        remote_mock.return_value = dict(archiver_remote_status)

        status_postgres_mock.return_value = dict()

        server = build_real_server(
            global_conf={
                "archiver": "on",
                # Silence the warning for default backup strategy
                "backup_options": "exclusive_backup",
            }
        )

        # Test output for status invocation
        # Expecting:
        # Last archived WAL:
        #   <last_archived_wal>, at <last_archived_time>
        # Failures of WAL archiver:
        #   <failed_count> (<last_failed wal>, at <last_failed_time>)
        server.status()
        (out, err) = capsys.readouterr()

        # Parse the output
        result = dict(
            item.strip("\t\n\r").split(": ") for item in out.split("\n") if item != ""
        )
        assert err == ""

        # Check the result
        assert result["Last archived WAL"] == "%s, at %s" % (
            archiver_remote_status["last_archived_wal"],
            archiver_remote_status["last_archived_time"].ctime(),
        )
        assert result["Failures of WAL archiver"] == "%s (%s at %s)" % (
            archiver_remote_status["failed_count"],
            archiver_remote_status["last_failed_wal"],
            archiver_remote_status["last_failed_time"].ctime(),
        )

    @patch("barman.server.Server.get_remote_status")
    def test_check_postgres_too_old(self, postgres_mock, capsys):
        postgres_mock.return_value = {
            "server_txt_version": "x.y.z",
            "version_supported": False,
        }

        # Create server
        server = build_real_server()
        strategy = CheckOutputStrategy()
        server.check_postgres(strategy)
        (out, err) = capsys.readouterr()
        assert (
            out
            == "	PostgreSQL: FAILED (unsupported version: PostgreSQL server is too old (x.y.z < 9.6.0))\n"
        )

    @patch("barman.server.Server.get_remote_status")
    def test_check_postgres(self, postgres_mock, capsys):
        """
        Test management of check_postgres view output

        :param postgres_mock: mock get_remote_status function
        :param capsys: retrieve output from console
        """
        postgres_mock.return_value = {"server_txt_version": None}
        # Create server
        server = build_real_server()
        # Case: no reply by PostgreSQL
        # Expect out: PostgreSQL: FAILED
        strategy = CheckOutputStrategy()
        server.check_postgres(strategy)
        (out, err) = capsys.readouterr()
        assert out == "	PostgreSQL: FAILED\n"
        # Case: correct configuration
        postgres_mock.return_value = {
            "current_xlog": None,
            "archive_command": "wal to archive",
            "server_txt_version": "PostgresSQL 9_4",
            "data_directory": "/usr/local/postgres",
            "archive_mode": "on",
            "wal_level": "replica",
        }

        # Expect out: all parameters: OK

        # Postgres version >= 9.0 - check wal_level
        server = build_real_server()
        server.check_postgres(strategy)
        (out, err) = capsys.readouterr()
        assert out == "\tPostgreSQL: OK\n\twal_level: OK\n"

        # Postgres version < 9.0 - avoid wal_level check
        del postgres_mock.return_value["wal_level"]

        server = build_real_server()
        server.check_postgres(strategy)
        (out, err) = capsys.readouterr()
        assert out == "\tPostgreSQL: OK\n"

        # Case: wal_level and archive_command values are not acceptable
        postgres_mock.return_value = {
            "current_xlog": None,
            "archive_command": None,
            "server_txt_version": "PostgresSQL 9_4",
            "data_directory": "/usr/local/postgres",
            "archive_mode": "on",
            "wal_level": "minimal",
        }
        # Expect out: some parameters: FAILED
        strategy = CheckOutputStrategy()
        server.check_postgres(strategy)
        (out, err) = capsys.readouterr()
        assert (
            out == "\tPostgreSQL: OK\n"
            "\twal_level: FAILED (please set it to a higher level "
            "than 'minimal')\n"
        )

    @patch("barman.server.Server._check_replication_slot")
    @patch("barman.server.Server.get_remote_status")
    def test_check_wal_streaming(self, mock_remote_status, mock_get_replication_slot):
        """
        Verify that check_wal_streaming calls _check_replication_slot using the
        remote_status information for the server.
        """
        # GIVEN a server which is configured for WALs to be streamed using the same
        # connections as for backups
        server = build_real_server(
            main_conf={
                "streaming_archiver": "on",
                "conninfo": "backup_conninfo",
                "streaming_conninfo": "backup_streaming_conninfo",
            }
        )
        # AND a check output strategy
        strategy = CheckOutputStrategy()

        # WHEN check_wal_streaming is called
        server.check_wal_streaming(strategy)

        # THEN the replication slot check was carried out with the server remote_status
        mock_get_replication_slot.assert_called_once_with(
            strategy, mock_remote_status.return_value
        )

    @pytest.mark.parametrize(
        ("pg_remote_status", "streaming_remote_status", "expected_failure"),
        (
            # If all required status is present then there should be no failure
            ({}, {}, None),
            # Monitoring privileges failure
            (
                {"has_monitoring_privileges": False},
                {},
                (
                    "no access to monitoring functions: FAILED (privileges for "
                    "PostgreSQL monitoring functions are required (see documentation))"
                ),
            ),
            # Streaming supported failures
            (
                {},
                {"streaming": False},
                "PostgreSQL streaming (WAL streaming): FAILED",
            ),
            (
                {},
                {
                    "connection_error": "test error",
                    "streaming": False,
                    "streaming_supported": None,
                },
                "PostgreSQL streaming (WAL streaming): FAILED (test error)",
            ),
            # WAL level failure
            (
                {"wal_level": "minimal"},
                {},
                (
                    "wal_level (WAL streaming): FAILED (please set it to a higher level"
                    " than 'minimal')"
                ),
            ),
            # Identity failures
            (
                {"postgres_systemid": "12345678"},
                {},
                (
                    "systemid coherence (WAL streaming): FAILED (is the streaming DSN "
                    "targeting the same server of the PostgreSQL connection string?)"
                ),
            ),
            (
                {},
                {"streaming_systemid": "12345678"},
                (
                    "systemid coherence (WAL streaming): FAILED (is the streaming DSN "
                    "targeting the same server of the PostgreSQL connection string?)"
                ),
            ),
            # Replication slot failures
            (
                {"replication_slot": Mock(restart_lsn=None, active=True)},
                {},
                (
                    "replication slot (WAL streaming): FAILED (slot 'test_slot' not "
                    "initialised: is 'receive-wal' running?)"
                ),
            ),
            (
                {"replication_slot": Mock(restart_lsn="mock_lsn", active=False)},
                {},
                (
                    "replication slot (WAL streaming): FAILED (slot 'test_slot' not "
                    "active: is 'receive-wal' running?)"
                ),
            ),
        ),
    )
    @patch("barman.server.PostgreSQLConnection")
    @patch("barman.server.StreamingConnection")
    @patch("barman.server.Server.get_remote_status")
    def test_check_wal_streaming_with_different_connections(
        self,
        mock_get_remote_status,
        mock_conn,
        mock_streaming_conn,
        pg_remote_status,
        streaming_remote_status,
        expected_failure,
        capsys,
    ):
        # GIVEN a server with a remote status which does not meet any of the
        # requirements for passing WAL streaming checks
        server = build_real_server(
            main_conf={
                "slot_name": "test_slot",
                "streaming_archiver": "on",
                "conninfo": "backup_conninfo",
                "streaming_conninfo": "backup_streaming_conninfo",
                "wal_conninfo": "wal_conninfo",
                "wal_streaming_conninfo": "wal_streaming_conninfo",
            }
        )
        mock_get_remote_status.return_value = {}
        # AND a remote status for the WAL connections which does meet the requirements
        # for passing WAL streaming checks
        mock_conn.return_value.get_remote_status.return_value = {
            "has_monitoring_privileges": True,
            "postgres_systemid": "01234567",
            "replication_slot": Mock(restart_lsn="mock_lsn", active=True),
            "wal_level": "replica",
        }
        mock_streaming_conn.return_value.get_remote_status.return_value = {
            "streaming": True,
            "streaming_supported": True,
            "streaming_systemid": "01234567",
        }

        # AND the specified remote status fields are overridden
        mock_conn.return_value.get_remote_status.return_value.update(pg_remote_status)
        mock_streaming_conn.return_value.get_remote_status.return_value.update(
            streaming_remote_status
        )

        # WHEN check_wal_streaming is called
        server.check_wal_streaming(CheckOutputStrategy())

        # THEN only the expected failure occurs
        out, _err = capsys.readouterr()
        if expected_failure is not None:
            assert out.count("FAILED") == 1
            assert expected_failure in out
        else:
            # OR if no failure was expected, no failures occurred
            assert "FAILED" not in out

    @pytest.mark.parametrize(
        (
            "primary_conninfo",
            "primary_in_recovery",
            "standby_in_recovery",
            "primary_systemid",
            "standby_systemid",
            "expected_msg",
        ),
        (
            # No primary_conninfo so check should always pass
            (None, None, None, None, None, None),
            # primary_conninfo set, primary is primary, standby is standby and
            # both primary and standby have same systemid - should pass
            ("db=primary", False, True, "fake_id", "fake_id", None),
            # Primary is a standby so check should fail
            (
                "db=primary",
                True,
                True,
                "fake_id",
                "fake_id",
                "primary_conninfo should point to a primary server, not a standby",
            ),
            # Standby is a primary so check should fail
            (
                "db=primary",
                False,
                False,
                "fake_id",
                "fake_id",
                "conninfo should point to a standby server if primary_conninfo is set",
            ),
            # systemid values do not match so check should fail
            (
                "db=primary",
                False,
                True,
                "fake_id_primary",
                "fake_id_standby",
                (
                    "primary_conninfo and conninfo should point to primary and "
                    "standby servers which share the same system identifier"
                ),
            ),
        ),
    )
    @patch("barman.server.StandbyPostgreSQLConnection")
    @patch("barman.server.PostgreSQLConnection")
    @patch("barman.server.Server.get_remote_status")
    def test_check_standby(
        self,
        _mock_remote_status,
        _pgconn_mock,
        _standby_pgconn_mock,
        primary_conninfo,
        primary_in_recovery,
        standby_in_recovery,
        primary_systemid,
        standby_systemid,
        expected_msg,
        capsys,
    ):
        """Verify standby-specific postgres checks."""
        # GIVEN a PostgreSQL server with the specified primary_conninfo
        server = build_real_server(main_conf={"primary_conninfo": primary_conninfo})
        strategy = CheckOutputStrategy()
        # AND if there is a primary server it has the specified in_recovery state
        # and systemid
        if primary_conninfo is not None:
            server.postgres.primary.is_in_recovery = primary_in_recovery
            server.postgres.primary.get_systemid.return_value = primary_systemid
        # AND the standby server has the specified in_recovery state and systemid
        server.postgres.is_in_recovery = standby_in_recovery
        server.postgres.get_systemid.return_value = standby_systemid

        # WHEN check_postgres runs
        server.check_postgres(strategy)
        out, _err = capsys.readouterr()

        if expected_msg is not None:
            # THEN if we expected an error we see the expected error
            assert expected_msg in out
            # AND the has_error status of the strategy is True
            assert strategy.has_error is True
        else:
            # OR if we didn't expect an error the has_error status is False
            assert strategy.has_error is False

    def test_check_replication_slot(self, capsys):
        """
        Extension of the check_postgres test.
        Tests the replication_slot check

        :param postgres_mock: mock get_remote_status function
        :param capsys: retrieve output from console
        """
        mock_remote_status = {
            "current_xlog": None,
            "archive_command": "wal to archive",
            "server_txt_version": "9.3.1",
            "data_directory": "/usr/local/postgres",
            "archive_mode": "on",
            "wal_level": "replica",
            "replication_slot_support": False,
            "replication_slot": None,
        }

        # Create server
        server = build_real_server()

        # Case: Postgres version < 9.4
        strategy = CheckOutputStrategy()
        server._check_replication_slot(strategy, mock_remote_status)
        (out, err) = capsys.readouterr()
        assert "\treplication slot:" not in out

        # Case: correct configuration
        # use a mock as a quick disposable obj
        rep_slot = MagicMock()
        rep_slot.slot_name = "test"
        rep_slot.active = True
        rep_slot.restart_lsn = "aaaBB"
        mock_remote_status = {
            "server_txt_version": "9.4.1",
            "replication_slot_support": True,
            "replication_slot": rep_slot,
        }
        server = build_real_server()
        server.config.streaming_archiver = True
        server.config.slot_name = "test"
        server._check_replication_slot(strategy, mock_remote_status)
        (out, err) = capsys.readouterr()

        # Everything is ok
        assert "\treplication slot: OK\n" in out

        rep_slot.active = False
        rep_slot.restart_lsn = None
        mock_remote_status = {
            "server_txt_version": "9.4.1",
            "replication_slot_support": True,
            "replication_slot": rep_slot,
        }

        # Replication slot not initialised.
        server = build_real_server()
        server.config.slot_name = "test"
        server.config.streaming_archiver = True
        server._check_replication_slot(strategy, mock_remote_status)
        (out, err) = capsys.readouterr()
        # Everything is ok
        assert (
            "\treplication slot: FAILED (slot '%s' not initialised: "
            "is 'receive-wal' running?)\n" % server.config.slot_name in out
        )

        rep_slot.reset_mock()
        rep_slot.active = False
        rep_slot.restart_lsn = "Test"
        mock_remote_status = {
            "server_txt_version": "9.4.1",
            "replication_slot_support": True,
            "replication_slot": rep_slot,
        }

        # Replication slot not active.
        server = build_real_server()
        server.config.slot_name = "test"
        server.config.streaming_archiver = True
        server._check_replication_slot(strategy, mock_remote_status)
        (out, err) = capsys.readouterr()
        # Everything is ok
        assert (
            "\treplication slot: FAILED (slot '%s' not active: "
            "is 'receive-wal' running?)\n" % server.config.slot_name in out
        )

        rep_slot.reset_mock()
        rep_slot.active = False
        rep_slot.restart_lsn = "Test"
        mock_remote_status = {
            "server_txt_version": "PostgreSQL 9.4.1",
            "replication_slot_support": True,
            "replication_slot": rep_slot,
        }

        # Replication slot not active with streaming_archiver off.
        server = build_real_server()
        server.config.slot_name = "test"
        server.config.streaming_archiver = False
        server._check_replication_slot(strategy, mock_remote_status)
        (out, err) = capsys.readouterr()
        # Everything is ok
        assert (
            "\treplication slot: OK (WARNING: slot '%s' is initialised "
            "but not required by the current config)\n" % server.config.slot_name in out
        )

        rep_slot.reset_mock()
        rep_slot.active = True
        rep_slot.restart_lsn = "Test"
        mock_remote_status = {
            "server_txt_version": "PostgreSQL 9.4.1",
            "replication_slot_support": True,
            "replication_slot": rep_slot,
        }

        # Replication slot not active with streaming_archiver off.
        server = build_real_server()
        server.config.slot_name = "test"
        server.config.streaming_archiver = False
        server._check_replication_slot(strategy, mock_remote_status)
        (out, err) = capsys.readouterr()
        # Everything is ok
        assert (
            "\treplication slot: OK (WARNING: slot '%s' is active "
            "but not required by the current config)\n" % server.config.slot_name in out
        )

    @patch("barman.server.Server.get_wal_until_next_backup")
    def test_get_wal_info(self, get_wal_mock, tmpdir):
        """
        Basic test for get_wal_info method
        Test the wals per second and total time in seconds values.
        :return:
        """
        # Build a test server with a test path
        server = build_real_server(global_conf={"barman_home": tmpdir.strpath})
        # Mock method get_wal_until_next_backup for returning a list of
        # 3 fake WAL. the first one is the start and stop WAL of the backup
        wal_list = [
            WalFileInfo.from_xlogdb_line(
                "000000010000000000000002\t16777216\t1434450086.53\tNone\n"
            ),
            WalFileInfo.from_xlogdb_line(
                "000000010000000000000003\t16777216\t1434450087.54\tNone\n"
            ),
            WalFileInfo.from_xlogdb_line(
                "000000010000000000000004\t16777216\t1434450088.55\tNone\n"
            ),
        ]
        get_wal_mock.return_value = wal_list
        backup_info = build_test_backup_info(
            server=server, begin_wal=wal_list[0].name, end_wal=wal_list[0].name
        )
        backup_info.save()
        # Evaluate total time in seconds:
        # last_wal_timestamp - first_wal_timestamp
        wal_total_seconds = wal_list[-1].time - wal_list[0].time
        # Evaluate the wals_per_second value:
        # wals_in_backup + wals_until_next_backup / total_time_in_seconds
        wals_per_second = len(wal_list) / wal_total_seconds
        wal_info = server.get_wal_info(backup_info)
        assert wal_info
        assert wal_info["wal_total_seconds"] == wal_total_seconds
        assert wal_info["wals_per_second"] == wals_per_second

    @patch("barman.server.BackupManager.get_previous_backup")
    @patch("barman.server.Server.check")
    @patch("barman.server.Server._make_directories")
    @patch("barman.backup.BackupManager.backup")
    @patch("barman.server.Server.archive_wal")
    @patch("barman.server.ServerBackupLock")
    def test_backup(
        self,
        backup_lock_mock,
        archive_wal_mock,
        backup_mock,
        dir_mock,
        check_mock,
        gpm_mock,
        capsys,
    ):
        """

        :param backup_lock_mock: mock ServerBackupLock
        :param archive_wal_mock: mock archive_wal server method
        :param backup_mock: mock BackupManager.backup
        :param dir_mock: mock _make_directories
        :param check_mock: mock check
        """

        # This is not the first backup
        gpm_mock.return_value = build_test_backup_info()

        # Create server
        server = build_real_server()
        dir_mock.side_effect = OSError()
        server.backup()
        out, err = capsys.readouterr()
        assert "failed to create" in err

        dir_mock.side_effect = None
        server.backup()
        backup_mock.assert_called_once_with(wait=False, wait_timeout=None, name=None)
        archive_wal_mock.assert_called_once_with(verbose=False)

        backup_mock.side_effect = LockFileBusy()
        server.backup()
        out, err = capsys.readouterr()
        assert "Another backup process is running" in err

        backup_mock.side_effect = LockFilePermissionDenied()
        server.backup()
        out, err = capsys.readouterr()
        assert "Permission denied, unable to access" in err

    @patch("barman.server.Server.get_first_backup_id")
    @patch("barman.server.BackupManager.delete_backup")
    def test_delete_running_backup(
        self, delete_mock, get_first_backup_mock, tmpdir, capsys
    ):
        """
        Simple test for the deletion of a running backup.
        We want to test the behaviour of the server.delete_backup method
        when invoked on a running backup
        """
        # Test the removal of a running backup. status STARTED
        server = build_real_server({"barman_home": tmpdir.strpath})
        backup_info_started = build_test_backup_info(
            status=BackupInfo.STARTED, server_name=server.config.name
        )
        get_first_backup_mock.return_value = backup_info_started.backup_id
        with ServerBackupLock(tmpdir.strpath, server.config.name):
            server.delete_backup(backup_info_started)
            out, err = capsys.readouterr()
            assert (
                "Another action is in progress for the backup %s"
                " of server %s. Impossible to delete the backup."
                % (backup_info_started.backup_id, server.config.name)
                in err
            )

        # Test the removal of a running backup. status EMPTY
        backup_info_empty = build_test_backup_info(
            status=BackupInfo.EMPTY, server_name=server.config.name
        )
        get_first_backup_mock.return_value = backup_info_empty.backup_id
        with ServerBackupLock(tmpdir.strpath, server.config.name):
            server.delete_backup(backup_info_empty)
            out, err = capsys.readouterr()
            assert (
                "Another action is in progress for the backup %s"
                " of server %s. Impossible to delete the backup."
                % (backup_info_started.backup_id, server.config.name)
                in err
            )

        # Test the removal of a running backup. status DONE
        backup_info_done = build_test_backup_info(
            status=BackupInfo.DONE, server_name=server.config.name
        )
        with ServerBackupLock(tmpdir.strpath, server.config.name):
            server.delete_backup(backup_info_done)
            delete_mock.assert_called_with(backup_info_done)

        # Test the removal of a backup not running. status STARTED
        server.delete_backup(backup_info_started)
        delete_mock.assert_called_with(backup_info_started)

    @patch("subprocess.Popen")
    def test_archive_wal_lock_acquisition(self, subprocess_mock, tmpdir, capsys):
        """
        Basic test for archive-wal lock acquisition
        """
        server = build_real_server({"barman_home": tmpdir.strpath})

        with ServerWalArchiveLock(tmpdir.strpath, server.config.name):
            server.archive_wal()
            out, err = capsys.readouterr()
            assert (
                "Another archive-wal process is already running "
                "on server %s. Skipping to the next server" % server.config.name
            ) in out

    @patch("subprocess.Popen")
    def test_cron_lock_acquisition(self, subprocess_mock, tmpdir, capsys, caplog):
        """
        Basic test for cron process lock acquisition
        """
        # See all logs
        caplog.set_level(0)

        server = build_real_server({"barman_home": tmpdir.strpath})

        # Basic cron lock acquisition
        with ServerCronLock(tmpdir.strpath, server.config.name):
            server.cron(wals=True, retention_policies=False)
            out, err = capsys.readouterr()
            assert (
                "Another cron process is already running on server %s. "
                "Skipping to the next server\n" % server.config.name
            ) in out

        # Lock acquisition for archive-wal
        with ServerWalArchiveLock(tmpdir.strpath, server.config.name):
            server.cron(wals=True, retention_policies=False)
            out, err = capsys.readouterr()
            assert (
                "Another archive-wal process is already running "
                "on server %s. Skipping to the next server" % server.config.name
            ) in out
        # Lock acquisition for receive-wal
        with ServerWalArchiveLock(tmpdir.strpath, server.config.name):
            with ServerWalReceiveLock(tmpdir.strpath, server.config.name):
                # force the streaming_archiver to True for this test
                server.config.streaming_archiver = True
                server.cron(wals=True, retention_policies=False)
                assert (
                    "Another STREAMING ARCHIVER process is running for "
                    "server %s" % server.config.name
                ) in caplog.text

    @patch("barman.server.ProcessManager")
    def test_kill(self, pm_mock, capsys):
        server = build_real_server()

        # Empty process list, the process is not running
        task_name = "test_task"
        process_list = []
        pm_mock.return_value.list.return_value = process_list
        pm_mock.return_value.kill.return_value = True
        server.kill(task_name)
        out, err = capsys.readouterr()
        assert (
            "Termination of %s failed: no such process for server %s"
            % (task_name, server.config.name)
        ) in err

        # Successful kill
        pid = 1234
        process_list.append(ProcessInfo(pid, server.config.name, task_name))
        pm_mock.return_value.list.return_value = process_list
        pm_mock.return_value.kill.return_value = True
        server.kill("test_task")
        out, err = capsys.readouterr()
        assert ("Stopped process %s(%s)" % (task_name, pid)) in out

        # The process don't terminate
        pm_mock.return_value.kill.return_value = False
        server.kill("test_task")
        out, err = capsys.readouterr()
        assert ("ERROR: Cannot terminate process %s(%s)" % (task_name, pid)) in err

    @patch("os.listdir")
    @patch("os.path.isdir")
    def test_check_archiver_errors(self, isdir_mock, listdir_mock):
        server = build_real_server()
        check_strategy = MagicMock()

        # There is no error file
        check_strategy.reset_mock()
        listdir_mock.return_value = []
        server.check_archiver_errors(check_strategy)
        check_strategy.result.assert_called_with("main", True, hint=None)

        # There is one duplicate file
        check_strategy.reset_mock()
        listdir_mock.return_value = ["testing.duplicate"]
        server.check_archiver_errors(check_strategy)
        check_strategy.result.assert_called_with(
            "main",
            False,
            hint="duplicates: 1",
        )

        # There is one unknown file
        check_strategy.reset_mock()
        listdir_mock.return_value = ["testing.unknown"]
        server.check_archiver_errors(check_strategy)
        check_strategy.result.assert_called_with(
            "main",
            False,
            hint="unknown: 1",
        )

        # There is one not relevant file
        check_strategy.reset_mock()
        listdir_mock.return_value = ["testing.error"]
        server.check_archiver_errors(check_strategy)
        check_strategy.result.assert_called_with(
            "main",
            False,
            hint="not relevant: 1",
        )

        # There is one extraneous file
        check_strategy.reset_mock()
        listdir_mock.return_value = ["testing.wrongextension"]
        server.check_archiver_errors(check_strategy)
        check_strategy.result.assert_called_with(
            "main", False, hint="unknown failure: 1"
        )

    def test_switch_wal(self, capsys):
        server = build_real_server()

        server.postgres = MagicMock()
        server.postgres.switch_wal.return_value = "000000010000000000000001"
        server.switch_wal(force=False)
        out, err = capsys.readouterr()
        assert (
            "The WAL file 000000010000000000000001 has been closed "
            "on server 'main'" in out
        )
        assert server.postgres.checkpoint.called is False

        server.postgres.reset_mock()
        server.postgres.switch_wal.return_value = "000000010000000000000001"
        server.switch_wal(force=True)

        out, err = capsys.readouterr()
        assert (
            "The WAL file 000000010000000000000001 has been closed "
            "on server 'main'" in out
        )
        assert server.postgres.checkpoint.called is True
        server.postgres.reset_mock()
        server.postgres.switch_wal.return_value = ""

        server.switch_wal(force=False)

        out, err = capsys.readouterr()
        assert "No switch required for server 'main'" in out
        assert server.postgres.checkpoint.called is False

    def test_check_archive(self, tmpdir):
        """
        Test the check_archive method
        """
        # Setup temp dir and server
        server = build_real_server(
            global_conf={"barman_lock_directory": tmpdir.mkdir("lock").strpath},
            main_conf={
                "wals_directory": tmpdir.mkdir("wals").strpath,
                "incoming_wals_directory": tmpdir.mkdir("incoming").strpath,
                "streaming_wals_directory": tmpdir.mkdir("streaming").strpath,
            },
        )
        strategy = CheckStrategy()

        # Call the server on an unexistent xlog file. expect it to fail
        server.check_archive(strategy)
        assert strategy.has_error is True
        assert strategy.check_result[0].check == "WAL archive"
        assert strategy.check_result[0].status is False

        # Call the check on an empty xlog file. expect it to contain errors.
        with open(server.xlogdb_file_name, "a"):
            # the open call forces the file creation
            pass

        server.check_archive(strategy)
        assert strategy.has_error is True
        assert strategy.check_result[0].check == "WAL archive"
        assert strategy.check_result[0].status is False

        # Write something in the xlog db file and check for the results
        with server.xlogdb("w") as fxlogdb:
            fxlogdb.write("00000000000000000000")
        # The check strategy should contain no errors.
        strategy = CheckStrategy()
        server.check_archive(strategy)
        assert strategy.has_error is False
        assert len(strategy.check_result) == 0

        # Call the server on with archive = off and
        # the incoming directory not empty
        with open(
            "%s/00000000000000000000" % server.config.incoming_wals_directory, "w"
        ) as f:
            f.write("fake WAL")
        server.config.archiver = False
        server.check_archive(strategy)
        assert strategy.has_error is False
        assert strategy.check_result[0].check == "empty incoming directory"
        assert strategy.check_result[0].status is False

        # Check that .tmp files are ignored
        # Create a nonempty tmp file
        with open(
            os.path.join(
                server.config.incoming_wals_directory, "00000000000000000000.tmp"
            ),
            "w",
        ) as wal:
            wal.write("a")
        # The check strategy should contain no errors.
        strategy = CheckStrategy()
        server.config.archiver = True
        server.check_archive(strategy)
        # Check that is ignored
        assert strategy.has_error is False
        assert len(strategy.check_result) == 0

    @pytest.mark.parametrize(
        "icoming_name, archiver_name",
        [
            ["incoming", "archiver"],
            ["streaming", "streaming_archiver"],
        ],
    )
    def test_incoming_thresholds(self, icoming_name, archiver_name, tmpdir):
        """
        Test the check_archive method thresholds
        """
        # Setup temp dir and server
        server = build_real_server(
            global_conf={"barman_lock_directory": tmpdir.mkdir("lock").strpath},
            main_conf={
                "wals_directory": tmpdir.mkdir("wals").strpath,
                "%s_wals_directory" % icoming_name: tmpdir.mkdir(icoming_name).strpath,
            },
        )

        # Make sure the test has configured correctly
        incoming_dir_setting = "%s_wals_directory" % icoming_name
        incoming_dir = getattr(server.config, incoming_dir_setting)
        assert incoming_dir

        # Create some content in the fake xlog.db to avoid triggering
        # empty xlogdb errors
        with open(server.xlogdb_file_name, "a") as fxlogdb:
            # write something
            fxlogdb.write("00000000000000000000")

        # Utility function to generate fake WALs
        def write_wal(target_dir, wal_number, partial=False):
            wal_name = "%s/0000000000000000%08d" % (target_dir, wal_number)
            if partial:
                wal_name += ".partial"
            with open(wal_name, "w") as wal_file:
                wal_file.write("fake WAL %s" % wal_number)

        # Case one, queue below the threshold

        # Enable the archiver we are checking and put max_incoming_wals_queue
        # files inside the directory
        setattr(server.config, archiver_name, True)
        server.config.max_incoming_wals_queue = 2
        # Fill the incoming dir to the threshold limit, we leave out the wal 0
        # to add it in a further test
        for x in range(1, server.config.max_incoming_wals_queue + 1):
            write_wal(incoming_dir, x)
        # If streaming, add a fake .partial file
        if icoming_name == "streaming":
            write_wal(
                incoming_dir, server.config.max_incoming_wals_queue + 1, partial=True
            )

        # Expect this to succeed
        strategy = CheckStrategy()
        server.check_archive(strategy)
        assert not strategy.has_error
        assert len(strategy.check_result) == 0

        # Case two, queue over the threshold

        # Add one more file to go over the threshold
        write_wal(incoming_dir, 0)
        # Expect this to fail, but with not critical errors
        strategy = CheckStrategy()
        server.check_archive(strategy)
        # Errors are not critical
        assert strategy.has_error is False
        assert len(strategy.check_result) == 1
        assert strategy.check_result[0].check == ("%s WALs directory" % icoming_name)
        assert strategy.check_result[0].status is False

        # Case three, disable the archiver and clean the incoming

        # Disable the archiver and clean the incoming dir
        setattr(server.config, archiver_name, False)
        for wal_file in os.listdir(incoming_dir):
            os.remove(os.path.join(incoming_dir, wal_file))

        # If streaming, add a fake .partial file
        if icoming_name == "streaming":
            write_wal(incoming_dir, 1, partial=True)

        # Expect this to succeed
        strategy = CheckStrategy()
        server.check_archive(strategy)
        assert not strategy.has_error
        assert len(strategy.check_result) == 0

        # Case four, disable the archiver an add something inside the
        # incoming directory. expect the check to fail

        # Disable the streaming archiver and add something inside the dir
        setattr(server.config, archiver_name, False)
        write_wal(incoming_dir, 0)
        # Expect this to fail, but with not critical errors
        strategy = CheckStrategy()
        server.check_archive(strategy)
        # Errors are not critical
        assert not strategy.has_error
        assert len(strategy.check_result) == 1
        assert strategy.check_result[0].check == ("empty %s directory" % icoming_name)
        assert strategy.check_result[0].status is False

    def test_replication_status(self, capsys):
        """
        Test management of pg_stat_archiver view output

        :param MagicMock connect_mock: mock the database connection
        :param capsys: retrieve output from console

        """

        # Build a fake get_replication_stats record
        replication_stats_data = dict(
            pid=93275,
            usesysid=10,
            usename="postgres",
            application_name="replica",
            client_addr=None,
            client_hostname=None,
            client_port=-1,
            slot_name=None,
            backend_start=datetime.datetime(
                2016, 5, 6, 9, 29, 20, 98534, tzinfo=FixedOffsetTimezone(offset=120)
            ),
            backend_xmin="940",
            state="streaming",
            sent_lsn="0/3005FF0",
            write_lsn="0/3005FF0",
            flush_lsn="0/3005FF0",
            replay_lsn="0/3005FF0",
            current_lsn="0/3005FF0",
            sync_priority=0,
            sync_state="async",
        )
        replication_stats_class = namedtuple("Record", replication_stats_data.keys())
        replication_stats_record = replication_stats_class(**replication_stats_data)

        # Prepare the server
        server = build_real_server(
            main_conf={
                "archiver": "on",
                # Silence the warning for default backup strategy
                "backup_options": "exclusive_backup",
            }
        )
        server.postgres = MagicMock()
        server.postgres.get_replication_stats.return_value = [replication_stats_record]
        server.postgres.current_xlog_location = "AB/CDEF1234"

        # Execute the test (ALL)
        server.postgres.reset_mock()
        server.replication_status("all")
        (out, err) = capsys.readouterr()
        assert err == ""
        server.postgres.get_replication_stats.assert_called_once_with(
            PostgreSQLConnection.ANY_STREAMING_CLIENT
        )

        # Execute the test (WALSTREAMER)
        server.postgres.reset_mock()
        server.replication_status("wal-streamer")
        (out, err) = capsys.readouterr()
        assert err == ""
        server.postgres.get_replication_stats.assert_called_once_with(
            PostgreSQLConnection.WALSTREAMER
        )

        # Execute the test (failure: PostgreSQL too old)
        server.postgres.reset_mock()
        server.postgres.get_replication_stats.side_effect = PostgresUnsupportedFeature(
            "9.1"
        )
        server.replication_status("all")
        (out, err) = capsys.readouterr()
        assert "Requires PostgreSQL 9.1 or higher" in out
        assert err == ""
        server.postgres.get_replication_stats.assert_called_once_with(
            PostgreSQLConnection.ANY_STREAMING_CLIENT
        )

        # Execute the test (failure: superuser required)
        server.postgres.reset_mock()
        server.postgres.get_replication_stats.side_effect = PostgresSuperuserRequired
        server.replication_status("all")
        (out, err) = capsys.readouterr()
        assert "Requires superuser rights" in out
        assert err == ""
        server.postgres.get_replication_stats.assert_called_once_with(
            PostgreSQLConnection.ANY_STREAMING_CLIENT
        )

        # Test output reaction to missing attributes
        del replication_stats_data["slot_name"]
        server.postgres.reset_mock()
        server.replication_status("all")
        (out, err) = capsys.readouterr()
        assert "Replication slot" not in out

    def test_timeline_has_children(self, tmpdir):
        """
        Test for the timeline_has_children
        """
        server = build_real_server({"barman_home": tmpdir.strpath})
        tmpdir.join("main/wals").ensure(dir=True)

        # Write two history files
        history_2 = server.get_wal_full_path("00000002.history")
        with open(history_2, "w") as fp:
            fp.write('1\t2/83000168\tat restore point "myrp"\n')

        history_3 = server.get_wal_full_path("00000003.history")
        with open(history_3, "w") as fp:
            fp.write('1\t2/83000168\tat restore point "myrp"\n')

        history_4 = server.get_wal_full_path("00000004.history")
        with open(history_4, "w") as fp:
            fp.write('1\t2/83000168\tat restore point "myrp"\n')
            fp.write("2\t2/84000268\tunknown\n")

        # Check that the first timeline has children but the
        # others have not
        assert len(server.get_children_timelines(1)) == 3
        assert len(server.get_children_timelines(2)) == 1
        assert len(server.get_children_timelines(3)) == 0
        assert len(server.get_children_timelines(4)) == 0

    def test_xlogdb_file_name(self):
        """
        Test the xlogdb_file_name server property
        """
        server = build_real_server()
        server.config.wals_directory = "mock_wals_directory"

        result = os.path.join(server.config.wals_directory, server.XLOG_DB)

        assert server.xlogdb_file_name == result

    def test_create_physical_repslot(self, capsys):
        """
        Test the 'create_physical_repslot' method of the Postgres
        class
        """

        # No operation if there is no streaming connection
        server = build_real_server()
        server.streaming = None
        assert server.create_physical_repslot() is None

        # No operation if the slot name is empty
        server.streaming = MagicMock()
        server.config.slot_name = None
        server.streaming.server_version = 90400
        assert server.create_physical_repslot() is None

        # If there is a streaming connection and the replication
        # slot is defined, then the replication slot should be
        # created
        server.config.slot_name = "test_repslot"
        server.streaming.server_version = 90400
        server.create_physical_repslot()
        create_physical_repslot = server.streaming.create_physical_repslot
        create_physical_repslot.assert_called_with("test_repslot")

        # If the replication slot was already created
        # check that underlying the exception is correctly managed
        create_physical_repslot.side_effect = PostgresDuplicateReplicationSlot
        server.create_physical_repslot()
        create_physical_repslot.assert_called_with("test_repslot")
        out, err = capsys.readouterr()
        assert "Replication slot 'test_repslot' already exists" in err

        # Test the method failure if the replication slots
        # on the server are all taken
        create_physical_repslot.side_effect = PostgresReplicationSlotsFull
        server.create_physical_repslot()
        create_physical_repslot.assert_called_with("test_repslot")
        out, err = capsys.readouterr()
        assert "All replication slots for server 'main' are in use\n" in err

    def test_drop_repslot(self, capsys):
        """
        Test the 'drop_repslot' method of the Postgres
        class
        """

        # No operation if there is no streaming connection
        server = build_real_server()
        server.streaming = None
        assert server.drop_repslot() is None

        # No operation if the slot name is empty
        server.streaming = MagicMock()
        server.config.slot_name = None
        server.streaming.server_version = 90400
        assert server.drop_repslot() is None

        # If there is a streaming connection and the replication
        # slot is defined, then the replication slot should be
        # created
        server.config.slot_name = "test_repslot"
        server.streaming.server_version = 90400
        server.drop_repslot()
        drop_repslot = server.streaming.drop_repslot
        drop_repslot.assert_called_with("test_repslot")

        # If the replication slot doesn't exist
        # check that the underlying exception is correctly managed
        drop_repslot.side_effect = PostgresInvalidReplicationSlot
        server.drop_repslot()
        drop_repslot.assert_called_with("test_repslot")
        out, err = capsys.readouterr()
        assert "Replication slot 'test_repslot' does not exist" in err

    @pytest.mark.parametrize(
        ("remote_status", "expected_failure"),
        (
            # If all required status is present then there should be no failure
            ({}, None),
            # Monitoring privileges failure
            (
                {"has_monitoring_privileges": False},
                "Check 'no access to monitoring functions' failed for server 'main'",
            ),
            # Streaming supported failures
            (
                {"streaming": False},
                "Check 'PostgreSQL streaming (WAL streaming)' failed for server 'main'",
            ),
            (
                {
                    "connection_error": "test error",
                    "streaming": False,
                    "streaming_supported": None,
                },
                "Check 'PostgreSQL streaming (WAL streaming)' failed for server 'main'",
            ),
            # WAL level failure
            (
                {"wal_level": "minimal"},
                "Check 'wal_level (WAL streaming)' failed for server 'main'",
            ),
            # Identity failures
            (
                {"postgres_systemid": "12345678"},
                "Check 'systemid coherence (WAL streaming)' failed for server 'main'",
            ),
            (
                {"streaming_systemid": "12345678"},
                "Check 'systemid coherence (WAL streaming)' failed for server 'main'",
            ),
        ),
    )
    @patch("barman.wal_archiver.StreamingWalArchiver.receive_wal")
    @patch("barman.server.Server.get_remote_status")
    def test_receive_wal_checks(
        self,
        mock_remote_status,
        mock_receive_wal,
        remote_status,
        expected_failure,
        caplog,
        tmpdir,
    ):
        """
        Verify that receive-wal performs preflight checks.
        """
        # GIVEN a server configured for streaming archiving
        server = build_real_server(
            global_conf={"barman_lock_directory": tmpdir.mkdir("lock").strpath},
            main_conf={
                "streaming_archiver": "on",
            },
        )
        # AND a remote_status
        mock_remote_status.return_value = {
            "has_monitoring_privileges": True,
            "postgres_systemid": "01234567",
            "replication_slot": Mock(restart_lsn="mock_lsn", active=True),
            "streaming": True,
            "streaming_supported": True,
            "streaming_systemid": "01234567",
            "wal_level": "replica",
        }
        mock_remote_status.return_value.update(remote_status)

        # WHEN receive_wal is called
        server.receive_wal()

        # THEN if we expected the checks to pass, receive_wal was called on the archiver
        if expected_failure is None:
            mock_receive_wal.assert_called_once()
        else:
            # AND if we expected the checks to fail, the failing check is logged to file
            assert expected_failure in caplog.text
            # AND the impossible to start WAL streaming message is logged
            assert "Impossible to start WAL streaming" in caplog.text
            # AND receive_wal was not called on the archiver
            mock_receive_wal.assert_not_called()

    @patch("barman.infofile.BackupInfo.save")
    @patch("os.path.exists")
    def test_check_backup(
        self, mock_exists, backup_info_save, tmpdir, orig_exists=os.path.exists
    ):
        """
        Test the check_backup method
        """

        # Prepare a mock implementation of os.path.exists
        available_wals = []

        def mock_os_path_exist(file_name):
            return orig_exists(file_name) or file_name in available_wals

        mock_exists.side_effect = mock_os_path_exist

        timeline_info = {}
        server = build_real_server(
            global_conf={
                "barman_home": tmpdir.mkdir("home").strpath,
            },
        )
        server.backup_manager.get_latest_archived_wals_info = MagicMock()
        server.backup_manager.get_latest_archived_wals_info.return_value = timeline_info

        # Case 0: backup in progress
        backup_info = build_test_backup_info(
            server=server,
            begin_wal="000000010000000000000002",
            end_wal=None,
        )
        server.check_backup(backup_info)
        assert not backup_info_save.called

        # Case 1: timeline not present in the archived WALs
        # Nothing should happen
        backup_info = build_test_backup_info(
            server=server,
            begin_wal="000000010000000000000002",
            end_wal="000000010000000000000008",
        )
        server.check_backup(backup_info)
        assert backup_info_save.called
        assert backup_info.status == BackupInfo.WAITING_FOR_WALS

        # Case 2: the most recent WAL archived is older than the start of
        # the backup. Nothing should happen
        timeline_info["00000001"] = MagicMock()
        timeline_info["00000001"].name = "000000010000000000000001"
        server.check_backup(backup_info)
        assert backup_info_save.called
        assert backup_info.status == BackupInfo.WAITING_FOR_WALS

        # Case 3: the more recent WAL archived is more recent than the
        # start of the backup, but we still have not archived the
        # backup end.
        timeline_info["00000001"].name = "000000010000000000000004"

        # Case 3.1: we have all the files until this moment, nothing should
        # happen
        available_wals.append(server.get_wal_full_path("000000010000000000000002"))
        available_wals.append(server.get_wal_full_path("000000010000000000000003"))
        available_wals.append(server.get_wal_full_path("000000010000000000000004"))
        server.check_backup(backup_info)
        assert backup_info_save.called
        assert backup_info.status == BackupInfo.WAITING_FOR_WALS

        # Case 3.2: we miss two WAL files
        del available_wals[:]
        available_wals.append(server.get_wal_full_path("000000010000000000000002"))
        server.check_backup(backup_info)
        assert backup_info_save.called
        assert backup_info.status == BackupInfo.FAILED
        assert (
            backup_info.error == "At least one WAL file is missing. "
            "The first missing WAL file is "
            "000000010000000000000003"
        )
        backup_info_save.reset_mock()

        # Case 4: the more recent WAL archived is more recent than the end
        # of the backup, so we can be sure if the backup is failed or not
        timeline_info["00000001"].name = "000000010000000000000009"

        # Case 4.1: we have all the files, so the backup should be marked as
        # done
        del available_wals[:]
        available_wals.append(server.get_wal_full_path("000000010000000000000002"))
        available_wals.append(server.get_wal_full_path("000000010000000000000003"))
        available_wals.append(server.get_wal_full_path("000000010000000000000004"))
        available_wals.append(server.get_wal_full_path("000000010000000000000005"))
        available_wals.append(server.get_wal_full_path("000000010000000000000006"))
        available_wals.append(server.get_wal_full_path("000000010000000000000007"))
        available_wals.append(server.get_wal_full_path("000000010000000000000008"))
        backup_info.status = BackupInfo.WAITING_FOR_WALS
        server.check_backup(backup_info)
        assert backup_info_save.called
        assert backup_info.status == BackupInfo.DONE
        backup_info_save.reset_mock()

        # Case 4.2: a WAL file is missing
        del available_wals[:]
        available_wals.append(server.get_wal_full_path("000000010000000000000002"))
        available_wals.append(server.get_wal_full_path("000000010000000000000003"))
        available_wals.append(server.get_wal_full_path("000000010000000000000005"))
        available_wals.append(server.get_wal_full_path("000000010000000000000006"))
        available_wals.append(server.get_wal_full_path("000000010000000000000007"))
        available_wals.append(server.get_wal_full_path("000000010000000000000008"))
        backup_info.status = BackupInfo.WAITING_FOR_WALS
        server.check_backup(backup_info)
        assert backup_info_save.called
        assert backup_info.status == BackupInfo.FAILED
        assert (
            backup_info.error == "At least one WAL file is missing. "
            "The first missing WAL file is "
            "000000010000000000000004"
        )
        backup_info_save.reset_mock()

        # Case 4.3: we have all the files, but the backup is marked as
        # FAILED (i.e. the rsync copy failed). The backup should still be
        # kept as failed
        del available_wals[:]
        available_wals.append(server.get_wal_full_path("000000010000000000000002"))
        available_wals.append(server.get_wal_full_path("000000010000000000000003"))
        available_wals.append(server.get_wal_full_path("000000010000000000000004"))
        available_wals.append(server.get_wal_full_path("000000010000000000000005"))
        available_wals.append(server.get_wal_full_path("000000010000000000000006"))
        available_wals.append(server.get_wal_full_path("000000010000000000000007"))
        available_wals.append(server.get_wal_full_path("000000010000000000000008"))
        backup_info.status = BackupInfo.FAILED
        server.check_backup(backup_info)
        assert not backup_info_save.called
        assert backup_info.status == BackupInfo.FAILED
        backup_info_save.reset_mock()

    def test_wait_for_wal(self, tmpdir):
        # Waiting for a new WAL without archive_timeout without any WAL
        # file archived and no timeout should not raise an error.
        server = build_real_server(
            global_conf={
                "barman_home": tmpdir.mkdir("barman_home").strpath,
            },
        )
        server.wait_for_wal(archive_timeout=0.1)

        # Doing the same thing with a wal file should also not raise an
        # error
        server.wait_for_wal(wal_file="00000001000000EF000000AB", archive_timeout=0.1)

    @patch("barman.server.NamedTemporaryFile")
    @patch("barman.backup.CompressionManager")
    def test_get_wal_sendfile_uncompress_fail(
        self, mock_compression_manager, _mock_named_temporary_file, capsys
    ):
        """Verify CommandFailedException uncompressing WAL is handled"""
        # GIVEN a server
        server = build_real_server()
        # AND no existing errors in the output
        output.error_occurred = False
        # AND a mock compressor which raises CommandFailedException
        mock_compressor = Mock()
        mock_compressor.decompress.side_effect = CommandFailedException(
            "an error happened"
        )
        # Make sure the two compressors used by get_wal_sendfile are different mocks
        mock_compression_manager.return_value.get_compressor.side_effect = [
            mock_compressor,
            Mock(),
        ]
        mock_wal_info = Mock()
        mock_compression_manager.return_value.get_wal_file_info.return_value = (
            mock_wal_info
        )

        # WHEN get_wal_sendfile is called
        server.get_wal_sendfile("test_wal_file", "some compression", "/path/to/dest")

        # THEN output indicates an error
        assert output.error_occurred

        # AND the expected message is in the output
        _out, err = capsys.readouterr()
        assert "ERROR: Error decompressing WAL: an error happened" in err

    @pytest.mark.parametrize(
        "mode, success, error_msg",
        [
            ["plain", True, None],
            ["relative", True, None],
            ["bad_sum_line", True, "Bad checksum line"],
            ["bad_file_type", False, "Unsupported file type"],
            ["subdir", False, "Unsupported filename"],
        ],
    )
    def test_put_wal(
        self, mode, success, error_msg, tmpdir, capsys, caplog, monkeypatch
    ):
        # See all logs
        caplog.set_level(0)

        lab = tmpdir.mkdir("lab")
        incoming = tmpdir.mkdir("incoming")
        server = build_real_server(
            main_conf={
                "incoming_wals_directory": incoming.strpath,
                # Silence the warning for default backup strategy
                "backup_options": "exclusive_backup",
            }
        )
        output.error_occurred = False

        # Simulate a connection from a remote host
        monkeypatch.setenv("SSH_CONNECTION", "192.168.66.99")

        file_name = "00000001000000EF000000AB"
        if mode == "relative":
            file_name = "./" + file_name
        elif mode == "subdir":
            file_name = "test/" + file_name

        # Generate some test data in an in_memory tar
        tar_file = BytesIO()
        tar = tarfile.open(mode="w|", fileobj=tar_file, dereference=False)
        wal = lab.join(file_name)
        if mode == "bad_file_type":
            # Create a file with wrong file type
            wal.mksymlinkto("/nowhere")
            file_hash = hashlib.md5().hexdigest()
        else:
            wal.write("some random content", ensure=True)
            file_hash = wal.computehash("md5")
        tar.add(wal.strpath, file_name)
        md5 = lab.join("MD5SUMS")
        if mode == "bad_sum_line":
            md5.write("bad_line\n")
        md5.write("%s *%s\n" % (file_hash, file_name), mode="a")
        tar.add(md5.strpath, md5.basename)
        tar.close()

        # Feed the data to put-wal
        tar_file.seek(0)
        server.put_wal(tar_file)
        out, err = capsys.readouterr()

        # Output is always empty
        assert not out

        # Test error conditions
        if error_msg:
            assert error_msg in err
        else:
            assert not err
        assert success == (not output.error_occurred)

        # Verify the result if success
        if success:
            dest_file = incoming.join(wal.basename)
            assert dest_file.computehash() == wal.computehash()
            assert (
                "Received file '00000001000000EF000000AB' "
                "with checksum '34743e1e454e967eb76a16c66372b0ef' "
                "by put-wal for server 'main' "
                "(SSH host: 192.168.66.99)\n" in caplog.text
            )

    @pytest.mark.parametrize(
        "mode, error_msg",
        [
            ["file_absent", "Checksum without corresponding file"],
            ["sum_absent", "Missing checksum for file"],
            ["sum_mismatch", "Bad file checksum"],
            ["dest_exists", "Impossible to write already existing"],
        ],
    )
    def test_put_wal_fail(self, mode, error_msg, tmpdir, capsys, monkeypatch):
        lab = tmpdir.mkdir("lab")
        incoming = tmpdir.mkdir("incoming")
        server = build_real_server(
            main_conf={
                "incoming_wals_directory": incoming.strpath,
            }
        )
        output.error_occurred = False

        # Simulate a connection from a remote host
        monkeypatch.setenv("SSH_CONNECTION", "192.168.66.99")

        # Generate some test data in an in_memory tar
        tar_file = BytesIO()
        tar = tarfile.open(mode="w|", fileobj=tar_file)
        wal = lab.join("00000001000000EF000000AB")
        wal.write("some random content", ensure=True)
        if mode != "file_absent":
            tar.add(wal.strpath, wal.basename)
        md5 = lab.join("MD5SUMS")
        if mode != "sum_mismatch":
            md5.write("%s *%s\n" % (wal.computehash("md5"), wal.basename))
        else:
            # put an incorrect checksum in the file
            md5.write("%s *%s\n" % (hashlib.md5().hexdigest(), wal.basename))
        if mode != "sum_absent":
            tar.add(md5.strpath, md5.basename)
        tar.close()

        # If requested create a colliding file in the incoming directory
        if mode == "dest_exists":
            dest_file = incoming.join(wal.basename)
            dest_file.write("some random content", ensure=True)

        # Feed the data to put-wal
        tar_file.seek(0)
        server.put_wal(tar_file)
        out, err = capsys.readouterr()

        # Output is always empty
        assert not out

        assert error_msg in err
        assert (
            "file '00000001000000EF000000AB' in put-wal "
            "for server 'main' (SSH host: 192.168.66.99)\n" in err
        )
        assert output.error_occurred

    @patch("barman.server.fsync_file")
    @patch("barman.server.fsync_dir")
    def test_put_wal_fsync(self, fd_mock, ff_mock, tmpdir, capsys, caplog):
        # See all logs
        caplog.set_level(0)

        lab = tmpdir.mkdir("lab")
        incoming = tmpdir.mkdir("incoming")
        server = build_real_server(
            main_conf={
                "incoming_wals_directory": incoming.strpath,
                # Silence the warning for default backup strategy
                "backup_options": "exclusive_backup",
            }
        )
        output.error_occurred = False

        # Generate some test data in an in_memory tar
        tar_file = BytesIO()
        tar = tarfile.open(mode="w|", fileobj=tar_file, format=tarfile.PAX_FORMAT)
        wal = lab.join("00000001000000EF000000AB")
        wal.write("some random content", ensure=True)
        wal.setmtime(wal.mtime() - 100)  # Set mtime to 100 seconds ago
        tar.add(wal.strpath, wal.basename)
        md5 = lab.join("MD5SUMS")
        md5.write("%s *%s\n" % (wal.computehash("md5"), wal.basename))
        tar.add(md5.strpath, md5.basename)
        tar.close()

        # Feed the data to put-wal
        tar_file.seek(0)
        server.put_wal(tar_file)
        out, err = capsys.readouterr()

        # Output is always empty
        assert not out

        # Verify the result (this time without SSH_CONNECTION)
        assert not err
        assert not output.error_occurred
        dest_file = incoming.join(wal.basename)
        assert dest_file.computehash() == wal.computehash()
        assert (
            "Received file '00000001000000EF000000AB' "
            "with checksum '34743e1e454e967eb76a16c66372b0ef' "
            "by put-wal for server 'main'\n" in caplog.text
        )

        # Verify fsync calls
        ff_mock.assert_called_once_with(dest_file.strpath)
        fd_mock.assert_called_once_with(incoming.strpath)

        # Verify file mtime
        # Use a round(2) comparison because float is not precise in Python 2.x
        assert round(wal.mtime(), 2) == round(dest_file.mtime(), 2)

    def test_get_systemid_file_path(self):
        # Basic test for the get_systemid_file_path function
        server = build_real_server()
        file_path = "/some/barman/home/main/identity.json"
        assert server.get_identity_file_path() == file_path

    @patch("barman.postgres.PostgreSQL.server_major_version", new_callable=PropertyMock)
    @patch("barman.server.Server.get_remote_status")
    def test_write_systemid_file(self, get_remote_status, major_version, tmpdir):
        """
        Test the function to write the systemid file in the Barman home
        """
        server = build_real_server(
            global_conf={
                "barman_home": tmpdir.mkdir("barman_home").strpath,
            },
            main_conf={
                "backup_directory": tmpdir.mkdir("backup").strpath,
            },
        )
        major_version.return_value = "11"

        # First case: we have no systemid defined, the systemid file
        # cannot be written
        get_remote_status.return_value = {}
        server.write_identity_file()
        assert not os.path.exists(server.get_identity_file_path())

        # Second case: we have systemid defined from the PostgreSQL
        # connection
        get_remote_status.return_value = {"postgres_systemid": "1234567890"}
        server.write_identity_file()
        with open(server.get_identity_file_path(), "r") as fp:
            assert json.load(fp) == {
                "systemid": "1234567890",
                "version": "11",
            }

        # Third case: we have systemid defined from the PostgreSQL
        # streaming connection
        get_remote_status.return_value = {"streaming_systemid": "0987654321"}
        server.postgres._remote_status = None
        # Cleanup old file and write a new one
        os.unlink(server.get_identity_file_path())
        server.write_identity_file()
        with open(server.get_identity_file_path(), "r") as fp:
            assert json.load(fp) == {
                "systemid": "0987654321",
                "version": "11",
            }

    @patch("barman.server.Server.get_remote_status")
    def test_check_systemid(self, get_remote_status_mock, capsys, tmpdir):
        """
        Test the management of the check_systemid function
        """

        server = Server(
            build_config_from_dicts(
                global_conf={
                    "barman_home": tmpdir.mkdir("barman_home").strpath,
                },
                main_conf={
                    "backup_directory": tmpdir.mkdir("backup").strpath,
                },
            ).get_server("main")
        )

        # First case: we can't check anything since
        # we have no systemid from the PostgreSQL connection and no
        # systemid from the streaming connection
        get_remote_status_mock.return_value = {
            "streaming_systemid": None,
            "postgres_systemid": None,
        }
        strategy = CheckOutputStrategy()
        server.check_identity(strategy)
        (out, err) = capsys.readouterr()
        assert out == "\tsystemid coherence: OK (no system Id available)\n"
        assert not os.path.exists(server.get_identity_file_path())

        # Second case: we have the systemid from the PostgreSQL connection,
        # but we still haven't written the systemid file
        get_remote_status_mock.return_value = {
            "streaming_systemid": "1234567890",
            "postgres_systemid": None,
        }
        strategy = CheckOutputStrategy()
        server.check_identity(strategy)
        (out, err) = capsys.readouterr()
        assert out == "\tsystemid coherence: OK (no system Id stored on disk)\n"
        assert not os.path.exists(server.get_identity_file_path())

        # Third case: we don't have the systemid from the PostgreSQL
        # connection, but we have the one from the data connection.
        # The systemid file is still not written
        get_remote_status_mock.return_value = {
            "streaming_systemid": None,
            "postgres_systemid": "1234567890",
        }
        strategy = CheckOutputStrategy()
        server.check_identity(strategy)
        (out, err) = capsys.readouterr()
        assert out == "\tsystemid coherence: OK (no system Id stored on disk)\n"
        assert not os.path.exists(server.get_identity_file_path())

        # Forth case: we have the streaming and the normal connection, and
        # they are pointing to the same server
        get_remote_status_mock.return_value = {
            "streaming_systemid": "1234567890",
            "postgres_systemid": "1234567890",
        }
        strategy = CheckOutputStrategy()
        server.check_identity(strategy)
        (out, err) = capsys.readouterr()
        assert out == "\tsystemid coherence: OK (no system Id stored on disk)\n"
        assert not os.path.exists(server.get_identity_file_path())

        # Fifth case: the systemid from the streaming connection and the
        # one from the data connection are not the same
        get_remote_status_mock.return_value = {
            "streaming_systemid": "0987654321",
            "postgres_systemid": "1234567890",
        }
        strategy = CheckOutputStrategy()
        server.check_identity(strategy)
        (out, err) = capsys.readouterr()
        assert (
            out == "\tsystemid coherence: FAILED (is the streaming "
            "DSN targeting the same server of the PostgreSQL "
            "connection string?)\n"
        )
        assert not os.path.exists(server.get_identity_file_path())

        # Sixth case: the systemid loaded from the PostgreSQL connection
        # is not the same as the one written in the systemid file
        get_remote_status_mock.return_value = {
            "streaming_systemid": None,
            "postgres_systemid": "1234567890",
        }
        with open(server.get_identity_file_path(), "w") as fp:
            fp.write('{"systemid": "test"}')
        strategy = CheckOutputStrategy()
        server.check_identity(strategy)
        (out, err) = capsys.readouterr()
        assert (
            out == "\tsystemid coherence: FAILED "
            "(the system Id of the connected PostgreSQL "
            'server changed, stored in "%s")\n' % server.get_identity_file_path()
        )

        os.unlink(server.get_identity_file_path())

        # Seventh case: the systemid loaded from the PostgreSQL connection
        # is the same as the one written in the systemid file
        get_remote_status_mock.return_value = {
            "streaming_systemid": None,
            "postgres_systemid": "1234567890",
        }
        with open(server.get_identity_file_path(), "w") as fp:
            fp.write('{"systemid": "1234567890"}')
        strategy = CheckOutputStrategy()
        server.check_identity(strategy)
        (out, err) = capsys.readouterr()
        assert out == "\tsystemid coherence: OK\n"

    @pytest.fixture
    def server(self, tmpdir):
        """Returns a basic real server."""
        return Server(
            build_config_from_dicts(
                global_conf={
                    "barman_home": tmpdir.mkdir("barman_home").strpath,
                },
                main_conf={
                    "backup_directory": tmpdir.mkdir("backup").strpath,
                    "wals_directory": tmpdir.mkdir("wals").strpath,
                },
            ).get_server("main")
        )

    def test_check_wal_validity_no_wals(self, server, capsys):
        """Verify the check fails if there is no last WAL"""
        server.config.last_wal_maximum_age = datetime.timedelta(hours=1)
        strategy = CheckOutputStrategy()
        server.check_wal_validity(strategy)
        assert strategy.has_error
        out, _err = capsys.readouterr()
        assert (
            "\twal maximum age: FAILED (No WAL files archived for last backup)\n" in out
        )

    def _get_epoch_time_hours_ago(self, hours):
        """Helper function which returns unix timestamp exactly the specified hours old"""
        n_hours_ago = datetime.datetime.now(dateutil.tz.tzlocal()) - datetime.timedelta(
            hours=hours
        )
        return time.mktime(n_hours_ago.timetuple())

    def test_check_wal_validity_within_maximum_age(self, server, capsys):
        """Verify the check passes if the last WAL is newer than specified"""
        with server.xlogdb("w") as fxlogdb:
            fxlogdb.write(
                "000000020000000000000001 42 %s None"
                % self._get_epoch_time_hours_ago(1)
            )

        backup = build_test_backup_info(
            server=server,
            begin_wal="000000020000000000000001",
            end_wal="000000020000000000000001",
        )
        backup.save()

        server.config.last_wal_maximum_age = datetime.timedelta(hours=2)
        strategy = CheckOutputStrategy()
        server.check_wal_validity(strategy)
        assert not strategy.has_error
        out, _err = capsys.readouterr()
        assert (
            "\twal maximum age: OK (interval provided: 2 hours, latest wal age: 1 hour"
            in out
        )

    def test_check_wal_validity_exceeds_maximum_age(self, server, capsys):
        """Verify the check fails if the last WAL is older than specified"""
        with server.xlogdb("w") as fxlogdb:
            fxlogdb.write(
                "000000020000000000000001 42 %s None"
                % self._get_epoch_time_hours_ago(2)
            )

        backup = build_test_backup_info(
            server=server,
            begin_wal="000000020000000000000001",
            end_wal="000000020000000000000001",
        )
        backup.save()

        server.config.last_wal_maximum_age = datetime.timedelta(hours=1)
        strategy = CheckOutputStrategy()
        server.check_wal_validity(strategy)
        assert strategy.has_error
        out, _err = capsys.readouterr()
        assert (
            "\twal maximum age: FAILED (interval provided: 1 hour, latest wal age: 2 hours"
            in out
        )

    def test_check_wal_validity_no_maximum_age(self, server, capsys):
        """Verify the check passes when last_wal_maximum_age isn't set"""
        with server.xlogdb("w") as fxlogdb:
            fxlogdb.write("000000020000000000000001 42 0 None\n")

        backup = build_test_backup_info(
            server=server,
            begin_wal="000000020000000000000001",
            end_wal="000000020000000000000001",
        )
        backup.save()

        strategy = CheckOutputStrategy()
        server.check_wal_validity(strategy)
        assert not strategy.has_error
        out, _err = capsys.readouterr()
        assert "\twal maximum age: OK (no last_wal_maximum_age provided)\n" in out

    def test_check_wal_validity_size(self, server, capsys):
        """Verify that the correct WAL size since the last backup is returned"""
        with server.xlogdb("w") as fxlogdb:
            fxlogdb.write("000000020000000000000001 42 0 None\n")
            fxlogdb.write("000000020000000000000002 43 0 None\n")
            fxlogdb.write("000000020000000000000003 42 0 None\n")

        backup = build_test_backup_info(
            server=server,
            begin_wal="000000020000000000000001",
            end_wal="000000020000000000000001",
        )
        backup.save()

        strategy = CheckOutputStrategy()
        server.check_wal_validity(strategy)
        assert not strategy.has_error
        out, _err = capsys.readouterr()
        assert "\twal size: OK (85 B)\n" in out

    def test_check_backup_validity_no_minimum_age_or_size(self, server, capsys):
        backup = build_test_backup_info(
            server=server,
            begin_wal="000000020000000000000001",
            end_wal="000000020000000000000001",
        )
        backup.save()

        strategy = CheckOutputStrategy()
        server.check_backup_validity(strategy)

        assert not strategy.has_error
        out, _err = capsys.readouterr()
        assert "\tbackup maximum age: OK (no last_backup_maximum_age provided)\n" in out

    def test_check_backup_validity_within_minimum_age(self, server, capsys):
        backup = build_test_backup_info(
            server=server,
            begin_wal="000000020000000000000001",
            end_wal="000000020000000000000001",
            end_time=datetime.datetime.now(dateutil.tz.tzlocal())
            - datetime.timedelta(hours=1),
        )
        backup.save()

        server.config.last_backup_maximum_age = datetime.timedelta(hours=2)
        strategy = CheckOutputStrategy()
        server.check_backup_validity(strategy)

        assert not strategy.has_error
        out, _err = capsys.readouterr()
        assert (
            "\tbackup maximum age: OK (interval provided: 2 hours, latest backup age: 1 hour"
            in out
        )

    def test_check_backup_validity_exceeds_minimum_age(self, server, capsys):
        backup = build_test_backup_info(
            server=server,
            begin_wal="000000020000000000000001",
            end_wal="000000020000000000000001",
            end_time=datetime.datetime.now(dateutil.tz.tzlocal())
            - datetime.timedelta(hours=2),
        )
        backup.save()

        server.config.last_backup_maximum_age = datetime.timedelta(hours=1)
        strategy = CheckOutputStrategy()
        server.check_backup_validity(strategy)

        assert strategy.has_error
        out, _err = capsys.readouterr()
        assert (
            "\tbackup maximum age: FAILED (interval provided: 1 hour, latest backup age: 2 hours"
            in out
        )

    def test_check_backup_validity_exceeds_minimum_size(self, server, capsys):
        backup = build_test_backup_info(
            server=server,
            begin_wal="000000020000000000000001",
            end_wal="000000020000000000000001",
            size=43,
        )
        backup.save()

        server.config.last_backup_minimum_size = 42
        strategy = CheckOutputStrategy()
        server.check_backup_validity(strategy)

        assert not strategy.has_error
        out, _err = capsys.readouterr()
        assert (
            "\tbackup minimum size: OK (last backup size 43 B > 42 B minimum)\n" in out
        )

    def test_check_backup_validity_under_minimum_size(self, server, capsys):
        backup = build_test_backup_info(
            server=server,
            begin_wal="000000020000000000000001",
            end_wal="000000020000000000000001",
            size=41,
        )
        backup.save()

        server.config.last_backup_minimum_size = 42
        strategy = CheckOutputStrategy()
        server.check_backup_validity(strategy)

        assert strategy.has_error
        out, _err = capsys.readouterr()
        assert (
            "\tbackup minimum size: FAILED (last backup size 41 B < 42 B minimum)\n"
            in out
        )


class TestCheckStrategy(object):
    """
    Test the different strategies for the results of the check command
    """

    def test_check_output_strategy(self, capsys):
        """
        Test correct output result
        """
        strategy = CheckOutputStrategy()
        # Expected result OK
        strategy.result("test_server_one", True, check="wal_level")
        out, err = capsys.readouterr()
        assert out == "	wal_level: OK\n"
        # Expected result FAILED
        strategy.result("test_server_one", False, check="wal_level")
        out, err = capsys.readouterr()
        assert out == "	wal_level: FAILED\n"

    def test_check_output_strategy_log(self, caplog):
        """
        Test correct output log

        :type caplog: pytest_capturelog.CaptureLogFuncArg
        """
        # See all logs
        caplog.set_level(0)

        strategy = CheckOutputStrategy()
        # Expected result OK
        strategy.result("test_server_one", True, check="wal_level")
        records = list(caplog.records)
        assert len(records) == 1
        record = records.pop()
        assert record.msg == "Check 'wal_level' succeeded for server 'test_server_one'"
        assert record.levelname == "DEBUG"
        # Expected result FAILED
        strategy = CheckOutputStrategy()
        strategy.result("test_server_one", False, check="wal_level")
        strategy.result("test_server_one", False, check="backup maximum age")
        records = list(caplog.records)
        assert len(records) == 3
        record = records.pop()
        assert record.levelname == "ERROR"
        assert (
            record.msg
            == "Check 'backup maximum age' failed for server 'test_server_one'"
        )
        record = records.pop()
        assert record.levelname == "ERROR"
        assert record.msg == "Check 'wal_level' failed for server 'test_server_one'"

    def test_check_strategy(self, capsys):
        """
        Test correct values result

        :type capsys: pytest
        """
        strategy = CheckStrategy()
        # Expected no errors
        strategy.result("test_server_one", True, check="wal_level")
        strategy.result("test_server_one", True, check="archive_mode")
        assert ("", "") == capsys.readouterr()
        assert strategy.has_error is False
        assert strategy.check_result
        assert len(strategy.check_result) == 2
        # Expected two errors
        strategy = CheckStrategy()
        strategy.result("test_server_one", False, check="wal_level")
        strategy.result("test_server_one", False, check="archive_mode")
        assert ("", "") == capsys.readouterr()
        assert strategy.has_error is True
        assert strategy.check_result
        assert len(strategy.check_result) == 2
        assert (
            len([result for result in strategy.check_result if not result.status]) == 2
        )
        # Test Non blocking error behaviour (one non blocking error)
        strategy = CheckStrategy()
        strategy.result("test_server_one", False, check="backup maximum age")
        strategy.result("test_server_one", True, check="archive mode")
        assert ("", "") == capsys.readouterr()
        assert strategy.has_error is False
        assert strategy.check_result
        assert len(strategy.check_result) == 2
        assert (
            len([result for result in strategy.check_result if not result.status]) == 1
        )

        # Test Non blocking error behaviour (2 errors one is non blocking)
        strategy = CheckStrategy()
        strategy.result("test_server_one", False, check="backup maximum age")
        strategy.result("test_server_one", False, check="archive mode")
        assert ("", "") == capsys.readouterr()
        assert strategy.has_error is True
        assert strategy.check_result
        assert len(strategy.check_result) == 2
        assert (
            len([result for result in strategy.check_result if not result.status]) == 2
        )

    def test_check_strategy_log(self, caplog):
        """
        Test correct log

        :type caplog: pytest_capturelog.CaptureLogFuncArg
        """
        # See all logs
        caplog.set_level(0)

        strategy = CheckStrategy()
        # Expected result OK
        strategy.result("test_server_one", True, check="wal_level")
        records = list(caplog.records)
        assert len(records) == 1
        record = records.pop()
        assert record.msg == "Check 'wal_level' succeeded for server 'test_server_one'"
        assert record.levelname == "DEBUG"
        # Expected result FAILED
        strategy.result("test_server_one", False, check="wal_level")
        records = list(caplog.records)
        assert len(records) == 2
        record = records.pop()
        assert record.levelname == "ERROR"
        assert record.msg == "Check 'wal_level' failed for server 'test_server_one'"
