# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2025
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
import shutil
from contextlib import closing
from datetime import datetime
from functools import partial

import dateutil
import dateutil.tz
import mock
import pytest
import testing_helpers
from mock import MagicMock, Mock, call

from barman import output, xlog
from barman.command_wrappers import PgCombineBackup
from barman.exceptions import (
    CommandFailedException,
    CommandNotFoundException,
    DataTransferFailure,
    FsOperationFailed,
    RecoveryInvalidTargetException,
    RecoveryPreconditionException,
    RecoveryStandbyModeException,
    RecoveryTargetActionException,
    SnapshotBackupException,
    UnsupportedCompressionFormat,
)
from barman.infofile import BackupInfo, Tablespace, VolatileBackupInfo, WalFileInfo
from barman.recovery_executor import (
    Assertion,
    CombineOperation,
    ConfigurationFileMangeler,
    DecompressOperation,
    DecryptOperation,
    MainRecoveryExecutor,
    RecoveryExecutor,
    RecoveryOperation,
    RemoteConfigRecoveryExecutor,
    RsyncCopyOperation,
    SnapshotRecoveryExecutor,
    recovery_executor_factory,
)


# noinspection PyMethodMayBeStatic
class TestRecoveryExecutor(object):
    """
    this class tests the methods of the recovery_executor module
    """

    def test_rsync_backup_executor_init(self):
        """
        Test the construction of a RecoveryExecutor
        """

        # Test
        backup_manager = testing_helpers.build_backup_manager()
        assert RecoveryExecutor(backup_manager)

    @pytest.mark.parametrize(
        "recovery_configuration_file",
        ("postgresql.auto.conf", "custom.recovery.conf"),
    )
    def test_analyse_temporary_config_files(self, recovery_configuration_file, tmpdir):
        """
        Test the method that identifies dangerous options into
        the configuration files
        """
        # Build directory/files structure for testing
        tempdir = tmpdir.mkdir("tempdir")
        recovery_info = {
            "auto_conf_append_lines": ["standby_mode = 'on'"],
            "configuration_files": ["postgresql.conf", "postgresql.auto.conf"],
            "tempdir": tempdir.strpath,
            "temporary_configuration_files": [],
            "results": {
                "changes": [],
                "warnings": [],
                "recovery_configuration_file": recovery_configuration_file,
            },
        }
        postgresql_conf = tempdir.join("postgresql.conf")
        recovery_config_file = tempdir.join(recovery_configuration_file)
        postgresql_conf.write(
            "archive_command = something\n"
            "data_directory = something\n"
            "include = something\n"
            'include "without braces"'
        )
        recovery_config_file.write(
            "archive_command = something\n" "data_directory = something"
        )
        recovery_info["temporary_configuration_files"].append(postgresql_conf.strpath)
        recovery_info["temporary_configuration_files"].append(
            recovery_config_file.strpath
        )
        # Build a RecoveryExecutor object (using a mock as server and backup
        # manager.
        backup_manager = testing_helpers.build_backup_manager()
        executor = RecoveryExecutor(backup_manager)
        # Identify dangerous options into config files for remote recovery
        executor._analyse_temporary_config_files(recovery_info)
        assert len(recovery_info["results"]["changes"]) == 2
        assert len(recovery_info["results"]["warnings"]) == 4
        # Clean for a local recovery test
        recovery_info["results"]["changes"] = []
        recovery_info["results"]["warnings"] = []
        # Identify dangerous options for local recovery
        executor._analyse_temporary_config_files(recovery_info)
        assert len(recovery_info["results"]["changes"]) == 2
        assert len(recovery_info["results"]["warnings"]) == 4
        # Verify auto options were appended
        recovery_config_file_contents = recovery_config_file.read()
        assert all(
            append_line in recovery_config_file_contents
            for append_line in recovery_info["auto_conf_append_lines"]
        )

        # Test corner case with empty auto file
        recovery_info["results"]["changes"] = []
        recovery_info["results"]["warnings"] = []
        recovery_info["auto_conf_append_lines"] = ["l1", "l2"]
        recovery_config_file.write("")
        executor._analyse_temporary_config_files(recovery_info)
        assert len(recovery_info["results"]["changes"]) == 1
        assert len(recovery_info["results"]["warnings"]) == 3

    def test_map_temporary_config_files(self, tmpdir):
        """
        Test the method that prepares configuration files
        for the final steps of a recovery
        """
        # Build directory/files structure for testing
        tempdir = tmpdir.mkdir("tempdir")
        recovery_info = {
            "configuration_files": ["postgresql.conf", "postgresql.auto.conf"],
            "tempdir": tempdir.strpath,
            "temporary_configuration_files": [],
            "results": {"changes": [], "warnings": [], "missing_files": []},
        }

        backup_info = testing_helpers.build_test_backup_info()
        backup_info.config.basebackups_directory = tmpdir.strpath
        datadir = tmpdir.mkdir(backup_info.backup_id).mkdir("data")
        postgresql_conf_local = datadir.join("postgresql.conf")
        postgresql_auto_local = datadir.join("postgresql.auto.conf")
        postgresql_conf_local.write(
            "archive_command = something\n" "data_directory = something"
        )
        postgresql_auto_local.write(
            "archive_command = something\n" "data_directory = something"
        )
        # Build a RecoveryExecutor object (using a mock as server and backup
        # manager.
        backup_manager = testing_helpers.build_backup_manager()
        executor = RecoveryExecutor(backup_manager)
        executor._map_temporary_config_files(
            recovery_info, backup_info, "ssh@something"
        )
        # check that configuration files have been moved by the method
        assert tempdir.join("postgresql.conf").check()
        assert (
            tempdir.join("postgresql.conf").computehash()
            == postgresql_conf_local.computehash()
        )
        assert tempdir.join("postgresql.auto.conf").check()
        assert (
            tempdir.join("postgresql.auto.conf").computehash()
            == postgresql_auto_local.computehash()
        )
        assert len(recovery_info["results"]["missing_files"]) == 2
        assert (
            "pg_hba.conf" in recovery_info["results"]["missing_files"]
            and "pg_ident.conf" in recovery_info["results"]["missing_files"]
        )

    @pytest.mark.parametrize(
        (
            "remote_command",
            "recovery_dir_key",
            "recovery_configuration_file",
        ),
        [
            (None, "destination_path", "postgresql.auto.conf"),
            ("mock_remote_command", "tempdir", "postgresql.auto.conf"),
            (None, "destination_path", "custom.recovery.conf"),
            ("mock_remote_command", "tempdir", "custom.recovery.conf"),
        ],
    )
    @mock.patch("barman.recovery_executor.open")
    @mock.patch("barman.recovery_executor.RecoveryExecutor._copy_conf_files_to_tempdir")
    @mock.patch("barman.recovery_executor.RecoveryExecutor._conf_files_exist")
    def test_map_temporary_config_files_recovery_configuration_file(
        self,
        mock_conf_files_exist,
        mock_copy_conf_files_to_tempdir,
        mock_open,
        remote_command,
        recovery_dir_key,
        recovery_configuration_file,
        tmpdir,
    ):
        """
        Test the method that prepares configuration files for the final steps of a
        recovery handles the recovery_configuration_file correctly.
        """
        # GIVEN a backup from PostgreSQL 12 (or above)
        backup_info = testing_helpers.build_test_backup_info()
        backup_info.version = 120000
        # AND a recovery executor
        backup_manager = testing_helpers.build_backup_manager()
        executor = RecoveryExecutor(backup_manager)
        # AND recovery_info specifies a recovery_configuration_file
        destination_path = tmpdir.mkdir("destination_path")
        tempdir = tmpdir.mkdir("tempdir")
        recovery_info = {
            "configuration_files": ["postgresql.conf", "postgresql.auto.conf"],
            "destination_path": destination_path.strpath,
            "tempdir": tempdir.strpath,
            "temporary_configuration_files": [],
            "results": {
                "missing_files": [],
                "recovery_configuration_file": recovery_configuration_file,
            },
        }
        # AND postgresql.conf and postgresql.auto.conf exist
        mock_conf_files_exist.return_value = {
            "postgresql.conf": True,
            "postgresql.auto.conf": True,
        }
        mock_copy_conf_files_to_tempdir.return_value = [
            os.path.join(recovery_info[recovery_dir_key], filename)
            for filename in recovery_info["configuration_files"]
        ]

        # WHEN _map_temporary_config_files is called
        executor._map_temporary_config_files(recovery_info, backup_info, remote_command)

        # THEN the configuration files were added to expected_temporary_files
        assert recovery_info["temporary_configuration_files"][:2] == [
            os.path.join(recovery_info[recovery_dir_key], filename)
            for filename in recovery_info["configuration_files"]
        ]
        if recovery_configuration_file not in recovery_info["configuration_files"]:
            # THEN the file was created if it was not already in
            # configuration_files
            conf_file_path = os.path.join(
                recovery_info[recovery_dir_key], recovery_configuration_file
            )
            mock_open.assert_called_once_with(conf_file_path, "ab")
            # AND the path was appended to temporary_configuration_files
            assert recovery_info["temporary_configuration_files"][-1] == os.path.join(
                recovery_info[recovery_dir_key], recovery_configuration_file
            )
        else:
            # OR if the file was already in configuration_files
            # THEN it was not created
            mock_open.assert_not_called()
            # AND no additional temporary files were created
            assert len(recovery_info["temporary_configuration_files"]) == len(
                recovery_info["configuration_files"]
            )

    @mock.patch("barman.recovery_executor.RsyncPgData")
    def test_setup(self, rsync_mock):
        """
        Test the method that set up a recovery
        """
        backup_info = testing_helpers.build_test_backup_info()
        backup_manager = testing_helpers.build_backup_manager()
        executor = RecoveryExecutor(backup_manager)
        backup_info.version = 90300
        recovery_dir = "/path/to/recovery/dir"

        # setup should create a temporary directory
        # and teardown should delete it
        ret = executor._setup(backup_info, None, recovery_dir, None)
        assert os.path.exists(ret["tempdir"])
        executor.close()
        assert not os.path.exists(ret["tempdir"])
        assert ret["wal_dest"].endswith("/pg_xlog")

        # no postgresql.auto.conf on version 9.3
        ret = executor._setup(backup_info, None, recovery_dir, None)
        executor.close()
        assert "postgresql.auto.conf" not in ret["configuration_files"]

        # Check the present for postgresql.auto.conf on version 9.4
        backup_info.version = 90400
        ret = executor._setup(backup_info, None, recovery_dir, None)
        executor.close()
        assert "postgresql.auto.conf" in ret["configuration_files"]

        # Receive a error if the remote command is invalid
        with pytest.raises(SystemExit):
            executor.server.path = None
            executor._setup(backup_info, "invalid", recovery_dir, None)

        # Test for PostgreSQL 10
        backup_info.version = 100000
        ret = executor._setup(backup_info, None, recovery_dir, None)
        executor.close()
        assert ret["wal_dest"].endswith("/pg_wal")

    @pytest.mark.parametrize(
        (
            "postgres_version",
            "recovery_conf_filename",
            "expected_recovery_configuration_file",
            "expected_in_configuration_files",
        ),
        [
            (110000, None, "recovery.conf", False),
            (110000, "custom.recovery.conf", "custom.recovery.conf", False),
            (120000, None, "postgresql.auto.conf", True),
            (120000, "custom.recovery.conf", "custom.recovery.conf", False),
        ],
    )
    @mock.patch("barman.recovery_executor.RsyncPgData")
    def test_setup_recovery_configuration_file(
        self,
        _rsync_mock,
        postgres_version,
        recovery_conf_filename,
        expected_recovery_configuration_file,
        expected_in_configuration_files,
    ):
        """
        Test the handling of recovery configuration files during _setup.
        """
        # GIVEN a backup from a PostgreSQL server with the specified version
        backup_info = testing_helpers.build_test_backup_info()
        backup_info.version = postgres_version
        # AND a recovery executor
        backup_manager = testing_helpers.build_backup_manager()
        executor = RecoveryExecutor(backup_manager)

        # WHEN _setup is called on the recovery executor
        recovery_info = executor._setup(
            backup_info, None, "/path/to/recovery/dir", recovery_conf_filename
        )
        executor.close()

        # THEN the expected recovery configuration file is set
        assert (
            recovery_info["results"]["recovery_configuration_file"]
            == expected_recovery_configuration_file
        )
        # AND the presence of the recovery conf file in configuration_files matches
        # expectations
        assert (
            expected_recovery_configuration_file in recovery_info["configuration_files"]
        ) == expected_in_configuration_files
        # AND regardless of the recovery configuration file, postgresql.auto.conf is in
        # configuration_files
        assert "postgresql.auto.conf" in recovery_info["configuration_files"]

    def test_set_pitr_targets(self, tmpdir):
        """
        Evaluate targets for point in time recovery
        """
        # Build basic folder/files structure
        tempdir = tmpdir.mkdir("temp_dir")
        dest = tmpdir.mkdir("dest")
        wal_dest = tmpdir.mkdir("wal_dest")
        recovery_info = {
            "configuration_files": ["postgresql.conf", "postgresql.auto.conf"],
            "tempdir": tempdir.strpath,
            "results": {"changes": [], "warnings": []},
            "is_pitr": False,
            "wal_dest": wal_dest.strpath,
            "get_wal": False,
        }
        backup_info = testing_helpers.build_test_backup_info(
            end_time=dateutil.parser.parse("2015-06-03 16:11:01.71038+02")
        )
        backup_manager = testing_helpers.build_backup_manager()
        # Build a recovery executor
        executor = RecoveryExecutor(backup_manager)
        executor._set_pitr_targets(
            recovery_info,
            backup_info,
            dest.strpath,
            None,
            "",
            "",
            "",
            "",
            "",
            False,
            None,
        )
        # Test with empty values (no PITR)
        assert recovery_info["target_datetime"] is None
        assert recovery_info["wal_dest"] == wal_dest.strpath

        # Test for PITR targets
        executor._set_pitr_targets(
            recovery_info,
            backup_info,
            dest.strpath,
            None,
            "target_name",
            "2015-06-03 16:11:03.71038+02",
            "2",
            None,
            "",
            False,
            None,
        )
        target_datetime = dateutil.parser.parse("2015-06-03 16:11:03.710380+02:00")

        assert recovery_info["target_datetime"] == target_datetime
        assert recovery_info["wal_dest"] == dest.join("barman_wal").strpath

        # Test for PITR targets with implicit target time
        executor._set_pitr_targets(
            recovery_info,
            backup_info,
            dest.strpath,
            None,
            "target_name",
            "2015-06-03 16:11:03.71038",
            "2",
            None,
            "",
            False,
            None,
        )
        target_datetime = dateutil.parser.parse("2015-06-03 16:11:03.710380")
        target_datetime = target_datetime.replace(tzinfo=dateutil.tz.tzlocal())

        assert recovery_info["target_datetime"] == target_datetime
        assert recovery_info["wal_dest"] == dest.join("barman_wal").strpath

        # Test for too early PITR target
        with pytest.raises(RecoveryInvalidTargetException) as exc_info:
            executor._set_pitr_targets(
                recovery_info,
                backup_info,
                dest.strpath,
                None,
                None,
                "2015-06-03 16:11:00.71038+02",
                None,
                None,
                None,
                False,
                None,
            )
        assert (
            str(exc_info.value) == "The requested target time "
            "2015-06-03 16:11:00.710380+02:00 "
            "is before the backup end time "
            "2015-06-03 16:11:01.710380+02:00"
        )

        # Tests for PostgreSQL < 9.1
        backup_info.version = 90000
        with pytest.raises(RecoveryTargetActionException) as exc_info:
            executor._set_pitr_targets(
                recovery_info,
                backup_info,
                dest.strpath,
                None,
                "target_name",
                "2015-06-03 16:11:03.71038+02",
                "2",
                None,
                None,
                False,
                "pause",
            )
        assert (
            str(exc_info.value) == "Illegal target action 'pause' "
            "for this version of PostgreSQL"
        )

        # Tests for PostgreSQL between 9.1 and 9.4 included
        backup_info.version = 90100
        executor._set_pitr_targets(
            recovery_info,
            backup_info,
            dest.strpath,
            None,
            "target_name",
            "2015-06-03 16:11:03.71038+02",
            "2",
            None,
            None,
            False,
            None,
        )
        assert "pause_at_recovery_target" not in recovery_info

        executor._set_pitr_targets(
            recovery_info,
            backup_info,
            dest.strpath,
            None,
            "target_name",
            "2015-06-03 16:11:03.71038+02",
            "2",
            None,
            None,
            False,
            "pause",
        )
        assert recovery_info["pause_at_recovery_target"] == "on"
        del recovery_info["pause_at_recovery_target"]

        with pytest.raises(RecoveryTargetActionException) as exc_info:
            executor._set_pitr_targets(
                recovery_info,
                backup_info,
                dest.strpath,
                None,
                "target_name",
                "2015-06-03 16:11:03.71038+02",
                "2",
                None,
                None,
                False,
                "promote",
            )
        assert (
            str(exc_info.value) == "Illegal target action 'promote' "
            "for this version of PostgreSQL"
        )

        # Tests for PostgreSQL >= 9.5
        backup_info.version = 90500
        executor._set_pitr_targets(
            recovery_info,
            backup_info,
            dest.strpath,
            None,
            "target_name",
            "2015-06-03 16:11:03.71038+02",
            "2",
            None,
            None,
            False,
            "pause",
        )
        assert recovery_info["recovery_target_action"] == "pause"

        executor._set_pitr_targets(
            recovery_info,
            backup_info,
            dest.strpath,
            None,
            "target_name",
            "2015-06-03 16:11:03.71038+02",
            "2",
            None,
            None,
            False,
            "promote",
        )
        assert recovery_info["recovery_target_action"] == "promote"

        with pytest.raises(RecoveryTargetActionException) as exc_info:
            executor._set_pitr_targets(
                recovery_info,
                backup_info,
                dest.strpath,
                None,
                "target_name",
                "2015-06-03 16:11:03.71038+02",
                "2",
                None,
                None,
                False,
                "unavailable",
            )
        assert (
            str(exc_info.value) == "Illegal target action 'unavailable' "
            "for this version of PostgreSQL"
        )

        # Recovery target action should not be available is PITR is not
        # enabled
        backup_info.version = 90500
        with pytest.raises(RecoveryTargetActionException) as exc_info:
            executor._set_pitr_targets(
                recovery_info,
                backup_info,
                dest.strpath,
                None,
                None,
                None,
                None,
                None,
                None,
                False,
                "pause",
            )
        assert (
            str(exc_info.value) == "Can't enable recovery target action "
            "when PITR is not required"
        )

        # Test that we are not using target_lsn with a version < 10
        backup_info.version = 90500
        with pytest.raises(RecoveryInvalidTargetException) as exc_info:
            executor._set_pitr_targets(
                recovery_info,
                backup_info,
                dest.strpath,
                None,
                None,
                None,
                None,
                None,
                10000,
                False,
                "pause",
            )
        assert (
            str(exc_info.value) == "Illegal use of recovery_target_lsn "
            "'10000' for this version "
            "of PostgreSQL "
            "(version 10 minimum required)"
        )

        # Test that we are not using target_immediate with a version < 9.4
        backup_info.version = 90300
        with pytest.raises(RecoveryInvalidTargetException) as exc_info:
            executor._set_pitr_targets(
                recovery_info,
                backup_info,
                dest.strpath,
                None,
                None,
                None,
                None,
                None,
                None,
                True,
                "pause",
            )
        assert (
            str(exc_info.value) == "Illegal use of "
            "recovery_target_immediate "
            "for this version "
            "of PostgreSQL "
            "(version 9.4 minimum required)"
        )

    @pytest.mark.parametrize(
        ["target_tli", "expected_pitr", "expected_tli"],
        [
            # WHEN no target_tli we expect no target timeline in the output
            # AND we expect that `is_pitr` is not set
            (None, False, None),
            # WHEN target_tli is 2 we expect target timeline 2 in the output
            # AND we expect that `is_pitr` is set
            (2, True, 2),
            # WHEN target_tli is 3 we expect target timeline 3 in the output
            # AND we expect that `is_pitr` is set
            (3, True, 3),
            # WHEN target_tli is current we expect target timeline 2 in the output
            # AND we expect that `is_pitr` is set
            ("current", True, 2),
            # WHEN target_tli is latest we expect target timeline 10 in the output
            # AND we expect that `is_pitr` is set
            ("latest", True, 10),
        ],
    )
    @mock.patch("barman.backup.BackupManager.get_latest_archived_wals_info")
    def test_set_pitr_targets_with_target_tli(
        self,
        mock_get_latest_archived_wals_info,
        target_tli,
        expected_pitr,
        expected_tli,
        capsys,
    ):
        """Verify target_tli values result in correct PITR status and output."""
        # GIVEN A simple recovery_info object
        recovery_info = {
            "is_pitr": False,
            "results": {},
        }
        # AND a recent backup on timeline 2
        backup_info = testing_helpers.build_test_backup_info(
            end_time=dateutil.parser.parse("2022-03-02 10:41:00.00000+01"), timeline=2
        )
        # AND a BackupManager and RecoveryExecutor
        backup_manager = testing_helpers.build_backup_manager()
        executor = RecoveryExecutor(backup_manager)

        # AND WALs in the archive for timelines 2, 3 and 4
        mock_get_latest_archived_wals_info.return_value = {
            "00000001": WalFileInfo(),
            "00000002": WalFileInfo(),
            "00000003": WalFileInfo(),
            # We deliberately use a latest timeline represented by a hexadecimal
            # value in the WAL name to verify it is handled correctly
            "0000000A": WalFileInfo(),
        }

        # WHEN _set_pitr_targets is called with the provided target_tli
        executor._set_pitr_targets(
            recovery_info,
            backup_info,
            "/path/to/nowhere",
            None,
            "",
            "",
            target_tli,
            "",
            "",
            False,
            None,
        )

        if expected_pitr:
            # THEN if we expected to enable pitr, is_pitr is set
            assert recovery_info["is_pitr"]
            # AND the output shows the expected recovery target timeline
            out, _ = capsys.readouterr()
            assert "Recovery target timeline: '%s'" % expected_tli in out
        else:
            # AND if we did not expect to enable pitr, is_pitr is not set
            assert not recovery_info["is_pitr"]

    @mock.patch("barman.recovery_executor.socket.getfqdn")
    @mock.patch("barman.recovery_executor.RsyncPgData")
    def test_generate_recovery_conf_pre12(self, rsync_pg_mock, mock_get_fqdn, tmpdir):
        """
        Test the generation of recovery.conf file
        """
        mock_get_fqdn.return_value = "BARMAN_SERVER"
        # Build basic folder/files structure
        dest = tmpdir.mkdir("destination")
        wal_dest = os.path.join(dest, "barman_wal")
        recovery_info = {
            "configuration_files": ["postgresql.conf", "postgresql.auto.conf"],
            "tempdir": tmpdir.strpath,
            "results": {"changes": [], "warnings": []},
            "get_wal": False,
            "target_datetime": "2015-06-03 16:11:03.71038+02",
            "wal_dest": wal_dest,
        }
        backup_info = testing_helpers.build_test_backup_info()

        # Build a recovery executor using a real server
        server = testing_helpers.build_real_server()
        executor = RecoveryExecutor(server.backup_manager)
        executor._generate_recovery_conf(
            recovery_info,
            backup_info,
            dest.strpath,
            True,
            True,
            "remote@command",
            "target_name",
            "2015-06-03 16:11:03.71038",
            "2",
            "",
            "",
            None,
        )

        # Check that the recovery.conf file exists
        recovery_conf_file = tmpdir.join("recovery.conf")
        assert recovery_conf_file.check()
        # Parse the generated recovery.conf
        recovery_conf = testing_helpers.parse_recovery_conf(recovery_conf_file)
        # check for contents
        assert "recovery_end_command" in recovery_conf
        assert "recovery_target_time" in recovery_conf
        assert "recovery_target_timeline" in recovery_conf
        assert "recovery_target_xid" not in recovery_conf
        assert "recovery_target_lsn" not in recovery_conf
        assert "recovery_target_name" in recovery_conf
        assert "recovery_target" not in recovery_conf
        assert recovery_conf["recovery_end_command"] == f"'rm -fr {wal_dest}'"
        assert recovery_conf["restore_command"] == f"'cp {wal_dest}/%f %p'"
        # what matters is the 'target_datetime', which always contain the target time
        # with a time zone, even if the user specified no time zone through
        # '--target-time'.
        assert recovery_conf["recovery_target_time"] == "'2015-06-03 16:11:03.71038+02'"
        assert recovery_conf["recovery_target_timeline"] == "2"
        assert recovery_conf["recovery_target_name"] == "'target_name'"

        # Test 'pause_at_recovery_target' recovery_info entry
        recovery_info["pause_at_recovery_target"] = "on"
        executor._generate_recovery_conf(
            recovery_info,
            backup_info,
            dest.strpath,
            True,
            True,
            "remote@command",
            "target_name",
            "2015-06-03 16:11:03.71038+02",
            "2",
            "",
            "",
            None,
        )
        recovery_conf_file = tmpdir.join("recovery.conf")
        assert recovery_conf_file.check()
        recovery_conf = testing_helpers.parse_recovery_conf(recovery_conf_file)
        assert recovery_conf["pause_at_recovery_target"] == "'on'"

        # Test 'recovery_target_action'
        del recovery_info["pause_at_recovery_target"]
        recovery_info["recovery_target_action"] = "pause"
        executor._generate_recovery_conf(
            recovery_info,
            backup_info,
            dest.strpath,
            True,
            True,
            "remote@command",
            "target_name",
            "2015-06-03 16:11:03.71038+02",
            "2",
            "",
            "",
            None,
        )
        recovery_conf_file = tmpdir.join("recovery.conf")
        assert recovery_conf_file.check()
        recovery_conf = testing_helpers.parse_recovery_conf(recovery_conf_file)
        assert recovery_conf["recovery_target_action"] == "'pause'"

        # Test 'standby_mode'
        executor._generate_recovery_conf(
            recovery_info,
            backup_info,
            dest.strpath,
            True,
            True,
            "remote@command",
            "target_name",
            "2015-06-03 16:11:03.71038+02",
            "2",
            "",
            "",
            True,
        )
        recovery_conf_file = tmpdir.join("recovery.conf")
        assert recovery_conf_file.check()
        recovery_conf = testing_helpers.parse_recovery_conf(recovery_conf_file)
        assert recovery_conf["standby_mode"] == "'on'"

        executor._generate_recovery_conf(
            recovery_info,
            backup_info,
            dest.strpath,
            True,
            True,
            "remote@command",
            "target_name",
            "2015-06-03 16:11:03.71038+02",
            "2",
            "",
            "",
            False,
        )
        recovery_conf_file = tmpdir.join("recovery.conf")
        assert recovery_conf_file.check()
        recovery_conf = testing_helpers.parse_recovery_conf(recovery_conf_file)
        assert "standby_mode" not in recovery_conf

        executor._generate_recovery_conf(
            recovery_info,
            backup_info,
            dest.strpath,
            True,
            True,
            "remote@command",
            "target_name",
            "2015-06-03 16:11:03.71038+02",
            "2",
            "",
            "",
            None,
        )
        recovery_conf_file = tmpdir.join("recovery.conf")
        assert recovery_conf_file.check()
        recovery_conf = testing_helpers.parse_recovery_conf(recovery_conf_file)
        assert "standby_mode" not in recovery_conf

        recovery_info["get_wal"] = True

        # Test when `--get-wal` is ``True`` and `-p` is not used.
        executor._generate_recovery_conf(
            recovery_info,
            backup_info,
            dest.strpath,
            True,
            True,
            "remote@command",
            "",
            "",
            "",
            "",
            "",
            None,
        )
        recovery_conf_file = tmpdir.join("recovery.conf")
        assert recovery_conf_file.check()
        recovery_conf = testing_helpers.parse_recovery_conf(recovery_conf_file)

        assert (
            recovery_conf["restore_command"]
            == "'barman-wal-restore -P -U {USER} BARMAN_SERVER main %f %p'"
        )

        # Build a recovery executor using a real server
        server = testing_helpers.build_real_server(main_conf={"parallel_jobs": 2})
        executor = RecoveryExecutor(server.backup_manager)
        # Test when `--get-wal` is ``True`` and `-p` is ``2``.
        executor._generate_recovery_conf(
            recovery_info,
            backup_info,
            dest.strpath,
            True,
            True,
            "remote@command",
            "",
            "",
            "",
            "",
            "",
            None,
        )
        recovery_conf_file = tmpdir.join("recovery.conf")
        assert recovery_conf_file.check()
        recovery_conf = testing_helpers.parse_recovery_conf(recovery_conf_file)

        assert (
            recovery_conf["restore_command"]
            == "'barman-wal-restore -P -U {USER} BARMAN_SERVER main %f %p -p 2'"
        )

    @mock.patch("barman.recovery_executor.RsyncPgData")
    def test_generate_recovery_conf(self, rsync_pg_mock, tmpdir):
        """
        Test the generation of recovery configuration
        :type tmpdir: py.path.local
        """
        # Build basic folder/files structure
        dest = tmpdir.mkdir("destination")
        wal_dest = os.path.join(dest, "barman_wal")
        recovery_info = {
            "configuration_files": ["postgresql.conf", "postgresql.auto.conf"],
            "tempdir": tmpdir.strpath,
            "results": {"changes": [], "warnings": []},
            "get_wal": False,
            "target_datetime": "2015-06-03 16:11:03.71038+02",
            "wal_dest": wal_dest,
        }
        backup_info = testing_helpers.build_test_backup_info(
            version=120000,
        )

        # Build a recovery executor using a real server
        server = testing_helpers.build_real_server()
        executor = RecoveryExecutor(server.backup_manager)
        executor._generate_recovery_conf(
            recovery_info,
            backup_info,
            dest.strpath,
            True,
            True,
            "remote@command",
            "target_name",
            "2015-06-03 16:11:03.71038",
            "2",
            "",
            "",
            None,
        )

        # Check that the recovery.conf file doesn't exist
        recovery_conf_file = tmpdir.join("recovery.conf")
        assert not recovery_conf_file.check()
        # Check that the recovery.signal file exists
        signal_file = tmpdir.join("recovery.signal")
        assert signal_file.check()
        # Parse the generated recovery configuration
        pg_auto_conf = self.parse_auto_conf_lines(recovery_info)
        # check for contents
        assert "recovery_end_command" in pg_auto_conf
        assert "recovery_target_time" in pg_auto_conf
        assert "recovery_target_timeline" in pg_auto_conf
        assert "recovery_target_xid" not in pg_auto_conf
        assert "recovery_target_lsn" not in pg_auto_conf
        assert "recovery_target_name" in pg_auto_conf
        assert "recovery_target" in pg_auto_conf
        assert pg_auto_conf["restore_command"] == f"'cp {wal_dest}/%f %p'"
        assert pg_auto_conf["recovery_end_command"] == f"'rm -fr {wal_dest}'"
        # what matters is the 'target_datetime', which always contain the target time
        # with a time zone, even if the user specified no time zone through
        # '--target-time'.
        assert pg_auto_conf["recovery_target_time"] == "'2015-06-03 16:11:03.71038+02'"
        assert pg_auto_conf["recovery_target_timeline"] == "2"
        assert pg_auto_conf["recovery_target_name"] == "'target_name'"

        # Test 'pause_at_recovery_target' recovery_info entry
        signal_file.remove()
        recovery_info["pause_at_recovery_target"] = "on"
        executor._generate_recovery_conf(
            recovery_info,
            backup_info,
            dest.strpath,
            True,
            True,
            "remote@command",
            "target_name",
            "2015-06-03 16:11:03.71038+02",
            "2",
            "",
            "",
            None,
        )
        # Check that the recovery.conf file doesn't exist
        recovery_conf_file = tmpdir.join("recovery.conf")
        assert not recovery_conf_file.check()
        # Check that the recovery.signal file exists
        signal_file = tmpdir.join("recovery.signal")
        assert signal_file.check()
        # Parse the generated recovery configuration
        pg_auto_conf = self.parse_auto_conf_lines(recovery_info)
        # Finally check pause_at_recovery_target value
        assert pg_auto_conf["pause_at_recovery_target"] == "'on'"

        # Test 'recovery_target_action'
        signal_file.remove()
        del recovery_info["pause_at_recovery_target"]
        recovery_info["recovery_target_action"] = "pause"
        executor._generate_recovery_conf(
            recovery_info,
            backup_info,
            dest.strpath,
            True,
            True,
            "remote@command",
            "target_name",
            "2015-06-03 16:11:03.71038+02",
            "2",
            "",
            "",
            None,
        )
        # Check that the recovery.conf file doesn't exist
        recovery_conf_file = tmpdir.join("recovery.conf")
        assert not recovery_conf_file.check()
        # Check that the recovery.signal file exists
        signal_file = tmpdir.join("recovery.signal")
        assert signal_file.check()
        # Parse the generated recovery configuration
        pg_auto_conf = self.parse_auto_conf_lines(recovery_info)
        # Finally check recovery_target_action value
        assert pg_auto_conf["recovery_target_action"] == "'pause'"

        # Test 'standby_mode'
        signal_file.remove()
        executor._generate_recovery_conf(
            recovery_info,
            backup_info,
            dest.strpath,
            True,
            True,
            "remote@command",
            "target_name",
            "2015-06-03 16:11:03.71038+02",
            "2",
            "",
            "",
            True,
        )
        # Check that the recovery.conf file doesn't exist
        recovery_conf_file = tmpdir.join("recovery.conf")
        assert not recovery_conf_file.check()
        # Check that the recovery.signal file doesn't exist
        wrong_signal_file = tmpdir.join("recovery.signal")
        assert not wrong_signal_file.check()
        # Check that the standby.signal file exists
        signal_file = tmpdir.join("standby.signal")
        assert signal_file.check()
        # Parse the generated recovery configuration
        pg_auto_conf = self.parse_auto_conf_lines(recovery_info)
        # standby_mode is not a valid configuration in PostgreSQL 12
        assert "standby_mode" not in pg_auto_conf

        signal_file.remove()
        executor._generate_recovery_conf(
            recovery_info,
            backup_info,
            dest.strpath,
            True,
            True,
            "remote@command",
            "target_name",
            "2015-06-03 16:11:03.71038+02",
            "2",
            "",
            "",
            False,
        )
        # Check that the recovery.conf file doesn't exist
        recovery_conf_file = tmpdir.join("recovery.conf")
        assert not recovery_conf_file.check()
        # Check that the standby.signal file doesn't exist
        wrong_signal_file = tmpdir.join("standby.signal")
        assert not wrong_signal_file.check()
        # Check that the recovery.signal file exists
        signal_file = tmpdir.join("recovery.signal")
        assert signal_file.check()
        # Parse the generated recovery configuration
        pg_auto_conf = self.parse_auto_conf_lines(recovery_info)
        # standby_mode is not a valid configuration in PostgreSQL 12
        assert "standby_mode" not in pg_auto_conf

        signal_file.remove()
        executor._generate_recovery_conf(
            recovery_info,
            backup_info,
            dest.strpath,
            True,
            True,
            "remote@command",
            "target_name",
            "2015-06-03 16:11:03.71038+02",
            "2",
            "",
            "",
            None,
        )
        # Check that the recovery.conf file doesn't exist
        recovery_conf_file = tmpdir.join("recovery.conf")
        assert not recovery_conf_file.check()
        # Check that the standby.signal file doesn't exist
        wrong_signal_file = tmpdir.join("standby.signal")
        assert not wrong_signal_file.check()
        # Check that the recovery.signal file exists
        signal_file = tmpdir.join("recovery.signal")
        assert signal_file.check()
        # Parse the generated recovery configuration
        pg_auto_conf = self.parse_auto_conf_lines(recovery_info)
        # standby_mode is not a valid configuration in PostgreSQL 12
        assert "standby_mode" not in pg_auto_conf

    def parse_auto_conf_lines(self, recovery_info):
        assert "auto_conf_append_lines" in recovery_info
        pg_auto_conf = {}
        for line in recovery_info["auto_conf_append_lines"]:
            kv = line.split("=", 1)
            try:
                pg_auto_conf[kv[0].strip()] = kv[1].strip()
            except IndexError:
                pg_auto_conf[kv[0].strip()] = None
        return pg_auto_conf

    @mock.patch("barman.recovery_executor.RsyncCopyController")
    def test_recover_backup_copy(self, copy_controller_mock, tmpdir):
        """
        Test the copy of a content of a backup during a recovery
        """
        # Build basic folder/files structure
        dest = tmpdir.mkdir("destination")
        server = testing_helpers.build_real_server()
        backup_info = testing_helpers.build_test_backup_info(
            server=server, tablespaces=[("tbs1", 16387, "/fake/location")]
        )
        # Build a executor
        executor = RecoveryExecutor(server.backup_manager)
        executor.config.tablespace_bandwidth_limit = {"tbs1": ""}
        executor.config.bandwidth_limit = 10

        executor._backup_copy(backup_info, dest.strpath, tablespaces=None)

        # Check the calls
        assert copy_controller_mock.mock_calls == [
            mock.call(
                network_compression=False,
                path=None,
                safe_horizon=None,
                ssh_command=None,
                retry_sleep=30,
                retry_times=0,
                workers=1,
                workers_start_batch_period=1,
                workers_start_batch_size=10,
            ),
            mock.call().add_directory(
                bwlimit="",
                dst="/fake/location",
                item_class=copy_controller_mock.return_value.TABLESPACE_CLASS,
                label="tbs1",
                src=backup_info.get_data_directory(16387) + "/",
            ),
            mock.call().add_directory(
                bwlimit=10,
                dst=dest.strpath,
                exclude=[
                    "/pg_log/*",
                    "/log/*",
                    "/pg_xlog/*",
                    "/pg_wal/*",
                    "/postmaster.pid",
                    "/recovery.conf",
                    "/tablespace_map",
                ],
                exclude_and_protect=["/pg_tblspc/16387"],
                item_class=copy_controller_mock.return_value.PGDATA_CLASS,
                label="pgdata",
                src=backup_info.get_data_directory() + "/",
            ),
            mock.call().copy(),
        ]

    @mock.patch("barman.recovery_executor.get_passphrase_from_command")
    @mock.patch("shutil.rmtree")
    @mock.patch("os.unlink")
    @mock.patch("shutil.copy2")
    @mock.patch("tempfile.mkdtemp")
    @mock.patch("barman.backup.EncryptionManager")
    @mock.patch("barman.backup.CompressionManager")
    @mock.patch("barman.recovery_executor.RsyncPgData")
    def test_recover_xlog(
        self,
        rsync_pg_mock,
        cm_mock,
        encr_mock,
        mock_tmp_file,
        mock_copy,
        mock_unlink,
        mock_rmtree,
        mock_passphrase,
        tmpdir,
    ):
        """
        Test the recovery of the xlogs of a backup. This unit test has 4 WAL files,
        one is plain, one is compressed with gzip, one is compressed with bzip2 and the
        last one is encrypted with gpg (not compressed).
        :param rsync_pg_mock: Mock rsync object for the purpose if this test
        """
        # Build basic folders/files structure
        dest = tmpdir.mkdir("destination")
        wals = tmpdir.mkdir("wals")
        # Create 3 WAL files with different compressions and 1 with encryption
        xlog_dir = wals.mkdir(xlog.hash_dir("000000000000000000000002"))
        xlog_plain = xlog_dir.join("000000000000000000000001")
        xlog_gz = xlog_dir.join("000000000000000000000002")
        xlog_bz2 = xlog_dir.join("000000000000000000000003")
        xlog_gpg = xlog_dir.join("000000000000000000000004")
        xlog_plain.write("dummy content")
        xlog_gz.write("dummy content gz")
        xlog_bz2.write("dummy content bz2")
        xlog_gpg.write("dummy content gpg")
        server = testing_helpers.build_real_server(
            main_conf={
                "wals_directory": wals.strpath,
                "encryption_passphrase_command": "echo 'passphrase'",
            }
        )
        # Prepare compressors mock
        c = {
            "gzip": mock.Mock(name="gzip"),
            "bzip2": mock.Mock(name="bzip2"),
        }
        cm_mock.return_value.get_compressor = lambda compression=None: c[compression]
        # Encrypted WAL is not compressed
        cm_mock.return_value.identify_compression.return_value = None
        cm_mock.return_value.unidentified_compression = None

        # Prepare compressors mock
        e = {
            "gpg": mock.Mock(name="gpg"),
        }
        encr_mock.return_value.get_encryption = lambda encryption: e[encryption]
        mock_tmp_file.return_value = "/tmp/barman-wal-x"
        mock_copy.return_value = None
        # touch destination files to avoid errors on cleanup
        c["gzip"].decompress.side_effect = lambda src, dst: open(dst, "w")
        c["bzip2"].decompress.side_effect = lambda src, dst: open(dst, "w")
        # Build executor
        executor = RecoveryExecutor(server.backup_manager)

        # Test: local copy
        required_wals = (
            WalFileInfo.from_xlogdb_line(
                "000000000000000000000001\t42\t43\tNone\tNone\n"
            ),
            WalFileInfo.from_xlogdb_line(
                "000000000000000000000002\t42\t43\tgzip\tNone\n"
            ),
            WalFileInfo.from_xlogdb_line(
                "000000000000000000000003\t42\t43\tbzip2\tNone\n"
            ),
            WalFileInfo.from_xlogdb_line(
                "000000000000000000000004\t42\t43\tNone\tgpg\n"
            ),
        )
        mock_passphrase.return_value = b"passphrase"
        executor._xlog_copy(required_wals, dest.strpath, None)
        # Check for a correct invocation of rsync using local paths
        rsync_pg_mock.assert_called_once_with(
            network_compression=False, bwlimit=None, path=None, ssh=None
        )
        assert not rsync_pg_mock.return_value.from_file_list.called
        c["gzip"].decompress.assert_called_once_with(xlog_gz.strpath, mock.ANY)
        c["bzip2"].decompress.assert_called_once_with(xlog_bz2.strpath, mock.ANY)
        e["gpg"].decrypt.assert_called_once_with(
            file=xlog_gpg.strpath,
            dest=dest.strpath + "/",
            passphrase=b"passphrase",
        )
        cm_mock.return_value.identify_compression.assert_called_once_with(
            e["gpg"].decrypt()
        )
        mock_copy.assert_called_once_with(xlog_plain.strpath, mock.ANY)
        # Reset mock calls
        rsync_pg_mock.reset_mock()
        c["gzip"].reset_mock()
        c["bzip2"].reset_mock()
        e["gpg"].reset_mock()
        cm_mock.reset_mock()
        mock_copy.reset_mock()
        # Test: remote copy
        executor._xlog_copy(required_wals, dest.strpath, "remote_command")
        # Check for the invocation of rsync on a remote call
        rsync_pg_mock.assert_called_once_with(
            network_compression=False, bwlimit=None, path=mock.ANY, ssh="remote_command"
        )
        rsync_pg_mock.return_value.from_file_list.assert_called_once_with(
            [
                "000000000000000000000001",
                "000000000000000000000002",
                "000000000000000000000003",
                "000000000000000000000004",
            ],
            "/tmp/barman-wal-x",
            mock.ANY,
        )
        c["gzip"].decompress.assert_called_once_with(xlog_gz.strpath, mock.ANY)
        c["bzip2"].decompress.assert_called_once_with(xlog_bz2.strpath, mock.ANY)
        e["gpg"].decrypt.assert_called_once_with(
            file=xlog_gpg.strpath,
            dest="/tmp/barman-wal-x",
            passphrase=b"passphrase",
        )
        cm_mock.return_value.identify_compression.assert_called_once_with(
            e["gpg"].decrypt()
        )
        mock_copy.assert_called_once_with(xlog_plain.strpath, mock.ANY)

        mock_unlink.call_count = 4
        mock_unlink.assert_has_calls(
            [
                call("/tmp/barman-wal-x/000000000000000000000001"),
                call("/tmp/barman-wal-x/000000000000000000000002"),
                call("/tmp/barman-wal-x/000000000000000000000003"),
                call("/tmp/barman-wal-x/000000000000000000000004"),
            ]
        )

    @mock.patch("barman.recovery_executor.get_passphrase_from_command")
    @mock.patch("shutil.move")
    @mock.patch("tempfile.mkdtemp")
    @mock.patch("barman.backup.EncryptionManager")
    @mock.patch("barman.backup.CompressionManager")
    @mock.patch("barman.recovery_executor.RsyncPgData")
    def test_recover_xlog_compressed_encrypted(
        self,
        rsync_pg_mock,
        cm_mock,
        encr_mock,
        mock_tmp_file,
        mock_move,
        mock_passphrase,
        tmpdir,
    ):
        """
        Test the recovery of the xlogs of a backup. This unit test has 1 WAL file that
        is compressed with gzip and encrypted with gpg.
        :param rsync_pg_mock: Mock rsync object for the purpose if this test
        """
        # Build basic folders/files structure
        dest = tmpdir.mkdir("destination")
        wals = tmpdir.mkdir("wals")
        # Create 3 WAL files with different compressions and 1 with encryption
        xlog_dir = wals.mkdir(xlog.hash_dir("000000000000000000000002"))
        xlog_gpg = xlog_dir.join("000000000000000000000004")
        xlog_gpg.write("dummy content gpg")
        server = testing_helpers.build_real_server(
            main_conf={
                "wals_directory": wals.strpath,
                "encryption_passphrase_command": "echo 'passphrase'",
            }
        )
        # Prepare compressors mock
        c = {
            "gzip": mock.Mock(name="gzip"),
        }
        cm_mock.return_value.get_compressor = lambda compression=None: c[compression]
        # Encrypted WAL is not compressed
        cm_mock.return_value.identify_compression.return_value = "gzip"
        # Prepare compressors mock
        e = {
            "gpg": mock.Mock(name="gpg"),
        }
        encr_mock.return_value.get_encryption = lambda encryption: e[encryption]
        mock_tmp_file.return_value = "/tmp/barman-wal-x"
        mock_move.return_value = None
        # touch destination files to avoid errors on cleanup
        c["gzip"].decompress.side_effect = lambda src, dst: open(dst, "w")
        e["gpg"].decrypt.return_value = dest.strpath + "/000000000000000000000004"
        # Build executor
        executor = RecoveryExecutor(server.backup_manager)

        # Test: local copy
        required_wals = (
            WalFileInfo.from_xlogdb_line(
                "000000000000000000000004\t42\t43\tNone\tgpg\n"
            ),
        )
        mock_passphrase.return_value = b"passphrase"
        executor._xlog_copy(required_wals, dest.strpath, None)
        # Check for a correct invocation of rsync using local paths
        rsync_pg_mock.assert_called_once_with(
            network_compression=False, bwlimit=None, path=None, ssh=None
        )
        assert not rsync_pg_mock.return_value.from_file_list.called
        e["gpg"].decrypt.assert_called_once_with(
            file=xlog_gpg.strpath,
            dest=dest.strpath + "/",
            passphrase=b"passphrase",
        )
        c["gzip"].decompress.assert_called_once_with(
            dest.strpath + "/000000000000000000000004",
            dest.strpath + "/000000000000000000000004.decompressed",
        )
        mock_move.assert_called_once_with(
            dest.strpath + "/000000000000000000000004.decompressed",
            dest.strpath + "/000000000000000000000004",
        )
        # Reset mock calls
        rsync_pg_mock.reset_mock()
        c["gzip"].reset_mock()
        e["gpg"].reset_mock()
        mock_move.reset_mock()
        e["gpg"].decrypt.return_value = "/tmp/barman-wal-x/000000000000000000000004"
        # Test: remote copy
        executor._xlog_copy(required_wals, dest.strpath, "remote_command")
        # Check for the invocation of rsync on a remote call
        rsync_pg_mock.assert_called_once_with(
            network_compression=False, bwlimit=None, path=mock.ANY, ssh="remote_command"
        )
        rsync_pg_mock.return_value.from_file_list.assert_called_once_with(
            [
                "000000000000000000000004",
            ],
            "/tmp/barman-wal-x",
            mock.ANY,
        )
        e["gpg"].decrypt.assert_called_once_with(
            file=xlog_gpg.strpath,
            dest="/tmp/barman-wal-x",
            passphrase=b"passphrase",
        )
        c["gzip"].decompress.assert_called_once_with(
            "/tmp/barman-wal-x/000000000000000000000004",
            "/tmp/barman-wal-x/000000000000000000000004.decompressed",
        )

        mock_move.assert_called_once_with(
            "/tmp/barman-wal-x/000000000000000000000004.decompressed",
            "/tmp/barman-wal-x/000000000000000000000004",
        )

    @mock.patch("barman.recovery_executor.get_passphrase_from_command")
    @mock.patch("barman.backup.EncryptionManager")
    @mock.patch("barman.backup.CompressionManager")
    @mock.patch("barman.recovery_executor.RsyncPgData")
    def test_recover_xlog_encrypted_wals_with_no_passphrase_raises_exception(
        self, rsync_pg_mock, cm_mock, encr_mock, mock_passphrase, tmpdir, caplog
    ):
        """
        Test that when there is an encrypted WAL and there is no passphrase, the restore
        process will raise an exception.
        """
        # Build basic folders/files structure
        dest = tmpdir.mkdir("destination")
        wals = tmpdir.mkdir("wals")
        # Create 1 WAL encrypted file
        xlog_dir = wals.mkdir(xlog.hash_dir("000000000000000000000002"))
        xlog_gpg = xlog_dir.join("000000000000000000000001")
        xlog_gpg.write("dummy content gpg")
        server = testing_helpers.build_real_server(
            main_conf={
                "wals_directory": wals.strpath,
                "encryption_passphrase_command": None,
            }
        )
        # Prepare compressors mock
        encr_mock.return_value.get_encryption.return_value = {
            "gpg": mock.Mock(name="gpg"),
        }
        # Build executor
        executor = RecoveryExecutor(server.backup_manager)

        required_wals = (
            WalFileInfo.from_xlogdb_line(
                "000000000000000000000001\t42\t43\tNone\tgpg\n"
            ),
        )
        mock_passphrase.return_value = None
        with pytest.raises(SystemExit):
            executor._xlog_copy(required_wals, dest.strpath, None)

        assert (
            "Encrypted WALs were found for server 'main', but "
            "'encryption_passphrase_command' is not configured correctly."
            in caplog.text
        )

    @mock.patch("barman.recovery_executor.RsyncCopyController")
    @mock.patch("barman.recovery_executor.RsyncPgData")
    @mock.patch("barman.recovery_executor.fs.unix_command_factory")
    def test_recovery(
        self, remote_cmd_mock, rsync_pg_mock, copy_controller_mock, tmpdir
    ):
        """
        Test the execution of a recovery
        """
        # Prepare basic directory/files structure
        dest = tmpdir.mkdir("destination")
        base = tmpdir.mkdir("base")
        wals = tmpdir.mkdir("wals")
        backup_info = testing_helpers.build_test_backup_info(tablespaces=[])
        backup_info.config.basebackups_directory = base.strpath
        backup_info.config.wals_directory = wals.strpath
        backup_info.version = 90400
        datadir = base.mkdir(backup_info.backup_id).mkdir("data")
        backup_info.pgdata = datadir.strpath
        postgresql_conf_local = datadir.join("postgresql.conf")
        postgresql_auto_local = datadir.join("postgresql.auto.conf")
        postgresql_conf_local.write(
            "archive_command = something\n" "data_directory = something"
        )
        postgresql_auto_local.write(
            "archive_command = something\n" "data_directory = something"
        )
        shutil.copy2(postgresql_conf_local.strpath, dest.strpath)
        shutil.copy2(postgresql_auto_local.strpath, dest.strpath)
        # Avoid triggering warning for missing config files
        datadir.ensure("pg_hba.conf")
        datadir.ensure("pg_ident.conf")
        # Build an executor
        server = testing_helpers.build_real_server(
            global_conf={"barman_lock_directory": tmpdir.mkdir("lock").strpath},
            main_conf={"wals_directory": wals.strpath},
        )
        executor = RecoveryExecutor(server.backup_manager)
        # test local recovery
        with closing(executor):
            rec_info = executor.recover(backup_info, dest.strpath, exclusive=True)
        # remove not useful keys from the result
        del rec_info["cmd"]
        sys_tempdir = rec_info["tempdir"]
        assert rec_info == {
            "rsync": None,
            "tempdir": sys_tempdir,
            "wal_dest": dest.join("pg_xlog").strpath,
            "recovery_dest": "local",
            "destination_path": dest.strpath,
            "temporary_configuration_files": [
                dest.join("postgresql.conf").strpath,
                dest.join("postgresql.auto.conf").strpath,
            ],
            "results": {
                "recovery_start_time": rec_info["results"]["recovery_start_time"],
                "get_wal": False,
                "changes": [
                    Assertion._make(["postgresql.conf", 0, "archive_command", "false"]),
                    Assertion._make(
                        ["postgresql.auto.conf", 0, "archive_command", "false"]
                    ),
                ],
                "missing_files": [],
                "recovery_configuration_file": "recovery.conf",
                "warnings": [
                    Assertion._make(
                        ["postgresql.conf", 2, "data_directory", "something"]
                    ),
                    Assertion._make(
                        ["postgresql.auto.conf", 2, "data_directory", "something"]
                    ),
                ],
            },
            "configuration_files": ["postgresql.conf", "postgresql.auto.conf"],
            "target_datetime": None,
            "safe_horizon": None,
            "is_pitr": False,
            "get_wal": False,
        }
        # test remote recovery
        with closing(executor):
            rec_info = executor.recover(
                backup_info,
                dest.strpath,
                remote_command="remote@command",
                exclusive=True,
            )
        # remove not useful keys from the result
        del rec_info["cmd"]
        del rec_info["rsync"]
        sys_tempdir = rec_info["tempdir"]
        assert rec_info == {
            "tempdir": sys_tempdir,
            "wal_dest": dest.join("pg_xlog").strpath,
            "recovery_dest": "remote",
            "destination_path": dest.strpath,
            "temporary_configuration_files": [
                os.path.join(sys_tempdir, "postgresql.conf"),
                os.path.join(sys_tempdir, "postgresql.auto.conf"),
            ],
            "results": {
                "get_wal": False,
                "recovery_start_time": rec_info["results"]["recovery_start_time"],
                "changes": [
                    Assertion._make(["postgresql.conf", 0, "archive_command", "false"]),
                    Assertion._make(
                        ["postgresql.auto.conf", 0, "archive_command", "false"]
                    ),
                ],
                "missing_files": [],
                "recovery_configuration_file": "recovery.conf",
                "warnings": [
                    Assertion._make(
                        ["postgresql.conf", 2, "data_directory", "something"]
                    ),
                    Assertion._make(
                        ["postgresql.auto.conf", 2, "data_directory", "something"]
                    ),
                ],
            },
            "configuration_files": ["postgresql.conf", "postgresql.auto.conf"],
            "target_datetime": None,
            "safe_horizon": None,
            "is_pitr": False,
            "get_wal": False,
        }
        # test failed rsync
        rsync_pg_mock.side_effect = CommandFailedException()
        with pytest.raises(CommandFailedException):
            with closing(executor):
                executor.recover(
                    backup_info,
                    dest.strpath,
                    exclusive=True,
                    remote_command="remote@command",
                )

    def test_recover_standby_mode(self, tmpdir):
        backup_info = testing_helpers.build_test_backup_info()
        backup_manager = testing_helpers.build_backup_manager()
        executor = RecoveryExecutor(backup_manager)
        backup_info.version = 90300
        destination = tmpdir.mkdir("destination").strpath

        # If standby mode is not enabled, recovery.conf is not generated
        executor._prepare_tablespaces = MagicMock()
        executor._backup_copy = MagicMock()
        executor._xlog_copy = MagicMock()
        executor._generate_recovery_conf = MagicMock()
        with closing(executor):
            executor.recover(backup_info, destination, standby_mode=None)
        executor._generate_recovery_conf.assert_not_called()

        # If standby mode is enabled, recovery.conf is generated
        executor._prepare_tablespaces.reset_mock()
        executor._backup_copy.reset_mock()
        executor._xlog_copy.reset_mock()
        executor._generate_recovery_conf.reset_mock()
        with closing(executor):
            executor.recover(backup_info, destination, standby_mode=True)
        executor._generate_recovery_conf.assert_called()

        # If standby mode is passed but PostgreSQL is older than 9.0,
        # we must raise an exception
        backup_info.version = 80000
        with pytest.raises(RecoveryStandbyModeException):
            with closing(executor):
                executor.recover(backup_info, destination, standby_mode=True)

    @pytest.mark.parametrize("manifest_exists", (False, True))
    @mock.patch("barman.recovery_executor.fs.unix_command_factory")
    def test_recover_rename_manifest(
        self, command_factory_mock, manifest_exists, tmpdir
    ):
        # GIVEN a backup_manifest file which exists according to manifest_exists
        command = command_factory_mock.return_value

        def mock_exists(filename):
            if filename.endswith("backup_manifest"):
                return manifest_exists
            else:
                return MagicMock()

        command.exists.side_effect = mock_exists

        # AND a mock recovery environment
        backup_info = testing_helpers.build_test_backup_info()
        backup_manager = testing_helpers.build_backup_manager()
        executor = RecoveryExecutor(backup_manager)
        backup_info.version = 90300
        destination = tmpdir.mkdir("destination").strpath

        executor._prepare_tablespaces = MagicMock()
        executor._backup_copy = MagicMock()
        executor._xlog_copy = MagicMock()
        executor._generate_recovery_conf = MagicMock()

        # WHEN recover is called
        with closing(executor):
            executor.recover(backup_info, destination, standby_mode=None)

        if manifest_exists:
            # THEN if the manifest exists it is renamed
            assert (
                (
                    "%s/backup_manifest" % destination,
                    "%s/backup_manifest.%s" % (destination, backup_info.backup_id),
                ),
                {},
            ) in command.move.call_args_list
        else:
            # OR if it does not exist, no attempt is made to rename it
            command.move.assert_not_called()

    @mock.patch("barman.recovery_executor.fs.unix_command_factory")
    @mock.patch("barman.recovery_executor.RsyncPgData")
    @mock.patch("barman.recovery_executor.output")
    @mock.patch("barman.recovery_executor.RsyncCopyController")
    @mock.patch("barman.recovery_executor.LocalBackupInfo")
    def test_recover_waiting_for_wals(
        self,
        backup_info_mock,
        rsync_copy_controller_mock,
        output_mock,
        rsync_pgdata_mock,
        unix_command_factory,
        tmpdir,
    ):
        # This backup is waiting for WALs and it remains in that status
        # even after having copied the data files
        backup_info_mock.WAITING_FOR_WALS = "WAITING_FOR_WALS"
        backup_info_mock.return_value.status = BackupInfo.WAITING_FOR_WALS
        backup_info = testing_helpers.build_test_backup_info()
        backup_manager = testing_helpers.build_backup_manager()
        executor = RecoveryExecutor(backup_manager)
        backup_info.status = BackupInfo.WAITING_FOR_WALS
        destination = tmpdir.mkdir("destination").strpath
        with closing(executor):
            executor.recover(backup_info, destination, standby_mode=None)

        # The backup info has been read again
        backup_info_mock.assert_called()

        # The following two warning messages have been emitted
        output_mock.warning.assert_has_calls(
            [
                mock.call(
                    "IMPORTANT: You have requested a recovery operation for "
                    "a backup that does not have yet all the WAL files that "
                    "are required for consistency."
                ),
                mock.call(
                    "IMPORTANT: The backup we have restored IS NOT "
                    "VALID. Required WAL files for consistency are "
                    "missing. Please verify that WAL archiving is "
                    "working correctly or evaluate using the 'get-wal' "
                    "option for recovery"
                ),
            ]
        )

        # In the following test case, the backup will be validated during
        # the copy of the data files, so there is no need for the warning
        # message at the end of the recovery process to be emitted again
        output_mock.warning.reset_mock()
        backup_info_mock.return_value.status = BackupInfo.DONE
        with closing(executor):
            executor.recover(backup_info, destination, standby_mode=None)

        # The backup info has been read again
        backup_info_mock.assert_called()

        # The following two warning messages have been emitted
        output_mock.warning.assert_has_calls(
            [
                mock.call(
                    "IMPORTANT: You have requested a recovery operation for "
                    "a backup that does not have yet all the WAL files that "
                    "are required for consistency."
                )
            ]
        )

    @pytest.mark.parametrize(
        ("conf_files", "temp_conf_files", "expected_file_list"),
        (
            # If configuration_files and temporary_configuration_files are the same
            # then we expect the configuration_files to be copied.
            [
                ["postgresql.auto.conf", "postgresql.conf"],
                [
                    "/path/to/tmp/postgresql.auto.conf",
                    "/path/to/tmp/barman/postgresql.conf",
                ],
                [
                    "postgresql.auto.conf",
                    "postgresql.auto.conf.origin",
                    "postgresql.conf",
                    "postgresql.conf.origin",
                ],
            ],
            # If temporary_configuration_files contains extra files then they should
            # also be copied
            [
                ["postgresql.auto.conf", "postgresql.conf"],
                [
                    "/path/to/tmp/postgresql.auto.conf",
                    "/path/to/tmp/barman/postgresql.conf",
                    "/path/to/tmp/barman/postgresql.new.conf",
                ],
                [
                    "postgresql.auto.conf",
                    "postgresql.auto.conf.origin",
                    "postgresql.conf",
                    "postgresql.conf.origin",
                    "postgresql.new.conf",
                    "postgresql.new.conf.origin",
                ],
            ],
        ),
    )
    def test_copy_temporary_config_files(
        self, conf_files, temp_conf_files, expected_file_list
    ):
        """Verify that modified config files are copied to their final destination."""
        # GIVEN a RecoveryExecutor
        mock_backup_manager = mock.Mock()
        executor = RecoveryExecutor(mock_backup_manager)
        # AND a recovery_info with a mock rsync object
        recovery_dir = "/path/to/recovery"
        temp_dir = "/path/to/tmp"
        recovery_info = {
            "configuration_files": conf_files,
            "temporary_configuration_files": temp_conf_files,
            "destination_path": recovery_dir,
            "rsync": mock.Mock(),
            "tempdir": temp_dir,
        }

        # WHEN _copy_temporary_config_files is called
        executor._copy_temporary_config_files(
            recovery_dir, "ssh db@remote", recovery_info
        )

        # THEN the expected configuration files are copied to the destination
        recovery_info["rsync"].from_file_list.assert_called_once_with(
            expected_file_list, temp_dir, ":" + recovery_dir
        )


class TestRemoteConfigRecoveryExecutor(object):
    """Test functions for managing remote configuration files during recovery."""

    def test_conf_files_exist(self):
        """Verify _conf_files_exist returns the expected map of file status."""
        # GIVEN a RemoteConfigRecoveryExecutor
        mock_backup_manager = mock.Mock()
        executor = RemoteConfigRecoveryExecutor(mock_backup_manager)
        # AND a mock recovery command for a specified list of files
        expected_conf_files = {"file0": True, "file1": False}
        conf_files = expected_conf_files.keys()
        exists_status = {"/path/to/dest/file0": True, "/path/to/dest/file1": False}

        def mock_exists(filename):
            return exists_status[filename]

        mock_cmd = mock.Mock(exists=mock_exists)
        recovery_info = {"cmd": mock_cmd, "destination_path": "/path/to/dest"}

        # WHEN _conf_files_exist is called
        response = executor._conf_files_exist(conf_files, None, recovery_info)
        # THEN the expected map of files is returned
        for filename in conf_files:
            assert response[filename] == expected_conf_files[filename]

    @mock.patch("barman.recovery_executor.RsyncPgData")
    def test_copy_conf_files_to_tempdir(self, mock_rsync_pg_data):
        """Verify that remote config files are copied correctly."""
        # GIVEN a RemoteConfigRecoveryExecutor
        mock_backup_manager = mock.Mock()
        executor = RemoteConfigRecoveryExecutor(mock_backup_manager)
        # AND a recovery_info
        conf_files = ("file0", "file1")
        recovery_dir = "/path/to/recovery"
        temp_dir = "/path/to/tmp"
        recovery_info = {
            "configuration_files": conf_files,
            "destination_path": recovery_dir,
            "tempdir": temp_dir,
        }

        # WHEN _copy_conf_files_to_tempdir is called
        response = executor._copy_conf_files_to_tempdir(None, recovery_info, None)

        # THEN the configuration files are copied to the destination
        mock_rsync_pg_data.return_value.from_file_list.assert_called_once_with(
            conf_files, ":" + recovery_dir, temp_dir
        )
        # AND a list of configuration paths is returned
        for filename in conf_files:
            assert os.path.join(temp_dir, filename) in response


class TestSnapshotRecoveryExecutor(object):
    @mock.patch("barman.recovery_executor.RecoveryExecutor.recover")
    @mock.patch("barman.recovery_executor.fs")
    @mock.patch("barman.recovery_executor.get_snapshot_interface_from_backup_info")
    def test_recover_success(
        self,
        mock_get_snapshot_interface,
        _mock_fs,
        mock_superclass_recover,
    ):
        """Verify that the recover method starts a recovery when all checks pass."""
        # GIVEN a SnapshotRecoveryExecutor
        mock_backup_manager = mock.Mock()
        executor = SnapshotRecoveryExecutor(mock_backup_manager)
        # AND a mock backup_info with snapshots
        backup_info = mock.Mock(
            snapshots_info=mock.Mock(
                snapshots=[
                    mock.Mock(
                        identifier="snapshot0",
                        device="/dev/dev0",
                        mount_point="/opt/disk0",
                        mount_options="rw,noatime",
                    ),
                ]
            )
        )
        # AND a given recovery destination and instance
        recovery_dest = "/path/to/dest"
        recovery_instance = "test_instance"
        # AND the correct volume is attached
        attached_volumes = {"disk0": mock.Mock(source_snapshot="snapshot0")}

        def mock_resolve_mounted_volume(_cmd):
            attached_volumes["disk0"].mount_point = "/opt/disk0"
            attached_volumes["disk0"].mount_options = "rw,noatime"

        attached_volumes["disk0"].resolve_mounted_volume.side_effect = (
            mock_resolve_mounted_volume
        )
        mock_get_snapshot_interface.return_value.get_attached_volumes.return_value = (
            attached_volumes
        )
        cmd = mock.Mock()
        _mock_fs.unix_command_factory.return_value = cmd

        # WHEN recover is called
        # THEN there are no errors
        executor.recover(
            backup_info,
            recovery_dest,
            recovery_instance=recovery_instance,
        )
        cmd.create_dir_if_not_exists.assert_called_once_with(recovery_dest, mode="700")

        # AND the superclass recovery method was called with the expected args
        mock_superclass_recover.assert_called_once_with(
            backup_info,
            recovery_dest,
            wal_dest=None,
            tablespaces=None,
            remote_command=None,
            target_tli=None,
            target_time=None,
            target_xid=None,
            target_lsn=None,
            target_name=None,
            target_immediate=False,
            exclusive=False,
            target_action=None,
            standby_mode=None,
            recovery_conf_filename=None,
        )

    @pytest.mark.parametrize(
        (
            "attached_volumes",
            "resolved_mount_info",
            "check_directory_exists_output",
            "should_fail",
        ),
        (
            # No disk cloned from snapshot attached
            [{}, None, None, True],
            # Correct disk attached but not mounted in the right place
            [
                {"disk0": mock.Mock(source_snapshot="snapshot0")},
                ("opt/disk1", "rw,noatime"),
                None,
                True,
            ],
            # Recovery directory not present
            [
                {"disk0": mock.Mock(source_snapshot="snapshot0")},
                ("/opt/disk0", "rw,noatime"),
                False,
                True,
            ],
            # All checks passing
            [
                {"disk0": mock.Mock(source_snapshot="snapshot0")},
                ("/opt/disk0", "rw,noatime"),
                True,
                False,
            ],
        ),
    )
    @mock.patch("barman.recovery_executor.RecoveryExecutor.recover")
    @mock.patch("barman.recovery_executor.fs")
    @mock.patch("barman.recovery_executor.get_snapshot_interface_from_backup_info")
    def test_recover_failure(
        self,
        mock_get_snapshot_interface,
        mock_fs,
        _mock_superclass_recover,
        attached_volumes,
        resolved_mount_info,
        check_directory_exists_output,
        should_fail,
    ):
        """Verify that the recover method fails when checks fail."""
        # GIVEN a SnapshotRecoveryExecutor
        mock_backup_manager = mock.Mock()
        executor = SnapshotRecoveryExecutor(mock_backup_manager)
        # AND the specified volumes are returned by the snapshot interface
        mock_get_snapshot_interface.return_value.get_attached_volumes.return_value = (
            attached_volumes
        )
        # AND a mock backup_info with snapshots
        backup_info = mock.Mock(
            snapshots_info=mock.Mock(
                snapshots=[
                    mock.Mock(
                        identifier="snapshot0",
                        device="/dev/dev0",
                        mount_point="/opt/disk0",
                        mount_options="rw,noatime",
                    ),
                ]
            )
        )
        # AND a given recovery destination and instance
        recovery_dest = "/path/to/dest"
        recovery_instance = "test_instance"

        # AND the mounted volumes resolve to the specified mount point and options
        def mock_resolve_mounted_volume(_cmd):
            volume.mount_point = resolved_mount_info[0]
            volume.mount_options = resolved_mount_info[1]

        for volume in attached_volumes.values():
            volume.resolve_mounted_volume.side_effect = mock_resolve_mounted_volume
        # AND a mock check_directory_exists command which returns the specified respone
        mock_cmd = mock_fs.unix_command_factory.return_value
        mock_cmd.check_directory_exists.return_value = check_directory_exists_output

        # WHEN recover is called AND an error is expected
        if should_fail:
            # THEN a RecoveryPreconditionException is raised and we intentionally
            # avoid checking the content because this is verified in tests for the
            # specific checks.
            with pytest.raises(RecoveryPreconditionException):
                executor.recover(
                    backup_info,
                    recovery_dest,
                    recovery_instance=recovery_instance,
                )
        else:
            # WHEN recover is called AND no error is expected then there is no error
            executor.recover(
                backup_info,
                recovery_dest,
                recovery_instance=recovery_instance,
            )

    @mock.patch("barman.recovery_executor.fs.unix_command_factory")
    @mock.patch("barman.recovery_executor.RsyncCopyController")
    def test_backup_copy(self, copy_controller_mock, command_factory_mock, tmpdir):
        """Verify that _backup_copy copies the backup_label into the destination."""
        # GIVEN a basic folder/files structure
        dest = tmpdir.mkdir("destination")
        barman_home = "/some/barman/home"
        server = testing_helpers.build_real_server(
            main_conf={"recovery_staging_path": "/wherever"}
        )
        backup_id = "111111"
        backup_info = testing_helpers.build_test_backup_info(
            backup_id=backup_id,
            server=server,
            snapshots_info=mock.Mock(),
        )
        # AND a SnapshotRecoveryExecutor
        executor = SnapshotRecoveryExecutor(server.backup_manager)
        # AND a mock command which always completes successfully
        command = command_factory_mock.return_value
        recovery_info = {"cmd": command}

        # WHEN _backup_copy is called
        executor._backup_copy(
            backup_info,
            dest.strpath,
            recovery_info=recovery_info,
        )

        # THEN the expected calls were made to the copy controller
        assert copy_controller_mock.mock_calls == [
            mock.call(
                network_compression=False,
                path=None,
                ssh_command=None,
                retry_sleep=30,
                retry_times=0,
                workers=1,
                workers_start_batch_period=1,
                workers_start_batch_size=10,
            ),
            mock.call().add_file(
                bwlimit=None,
                src="%s/main/base/%s/data/backup_label" % (barman_home, backup_id),
                dst="%s/backup_label" % dest.strpath,
                item_class=copy_controller_mock.return_value.PGDATA_CLASS,
                label="pgdata",
            ),
            mock.call().copy(),
        ]

    @mock.patch("barman.recovery_executor.fs.unix_command_factory")
    @mock.patch("barman.recovery_executor.RsyncCopyController")
    def test_backup_copy_command_failure(
        self, copy_controller_mock, command_factory_mock, tmpdir
    ):
        """Verify that _backup_copy fails when RsyncCopyController.copy fails."""
        # GIVEN a basic folder/files structure
        dest = tmpdir.mkdir("destination")
        server = testing_helpers.build_real_server(
            main_conf={"recovery_staging_path": "/wherever"}
        )
        backup_info = testing_helpers.build_test_backup_info(
            backup_id="backup_id",
            server=server,
            snapshots_info=mock.Mock(),
        )
        # AND a SnapshotRecoveryExecutor
        executor = SnapshotRecoveryExecutor(server.backup_manager)
        # AND a mock command which always completes successfully
        command = command_factory_mock.return_value
        recovery_info = {"cmd": command}
        # AND the copy controller fails with a CommandFailedException
        copy_controller_mock.return_value.copy.side_effect = CommandFailedException(
            "error message"
        )

        # WHEN _backup_copy is called
        # THEN a DataTransferFailure exception is raised
        with pytest.raises(DataTransferFailure) as exc:
            executor._backup_copy(
                backup_info, dest.strpath, recovery_info=recovery_info
            )
        # AND it has the expected error message
        assert str(exc.value) == "('error message',)"

    def test_check_recovery_dir_exists(self):
        """Verify check_recovery_dir_exists passes if the directory exists."""
        # GIVEN a mock check_directory_exists command which returns True
        cmd = mock.Mock()
        cmd.check_directory_exists.return_value = True

        # WHEN check_recovery_dir_exists is called, no exceptions are raised
        SnapshotRecoveryExecutor.check_recovery_dir_exists("/path/to/recovery_dir", cmd)

    def test_check_recovery_dir_exists_faiure(self):
        """Verify check_recovery_dir_exists raises exception if no directory exists."""
        # GIVEN a mock check_directory_exists command which returns True
        cmd = mock.Mock()
        cmd.check_directory_exists.return_value = False

        # WHEN check_recovery_dir_exists is called
        # THEN a RecoveryPreconditionException is raised
        with pytest.raises(RecoveryPreconditionException) as exc:
            SnapshotRecoveryExecutor.check_recovery_dir_exists(
                "/path/to/recovery_dir", cmd
            )

        # AND the exception has the expected message
        expected_message = (
            "Recovery directory '{}' does not exist on the recovery instance. "
            "Check all required disks have been created, attached and mounted."
        ).format("/path/to/recovery_dir")
        assert str(exc.value) == expected_message

    @pytest.mark.parametrize(
        ("attached_volumes", "snapshots_info", "expected_missing"),
        (
            # If all snapshots are present we expect success
            [
                {
                    "disk0": mock.Mock(source_snapshot="snapshot0"),
                    "disk1": mock.Mock(source_snapshot="snapshot1"),
                },
                mock.Mock(snapshots=[mock.Mock(identifier="snapshot0")]),
                [],
            ],
            [
                {
                    "disk0": mock.Mock(source_snapshot="snapshot0"),
                    "disk1": mock.Mock(source_snapshot="snapshot1"),
                },
                mock.Mock(
                    snapshots=[
                        mock.Mock(identifier="snapshot0", device="/dev/dev0"),
                        mock.Mock(identifier="snapshot1", device="/dev/dev1"),
                    ]
                ),
                [],
            ],
            # One or more snapshots are not attached so we expected failure
            [
                {
                    "disk0": mock.Mock(source_snapshot="snapshot0"),
                },
                mock.Mock(
                    snapshots=[
                        mock.Mock(identifier="snapshot0", device="/dev/dev0"),
                        mock.Mock(identifier="snapshot1", device="/dev/dev1"),
                    ]
                ),
                ["snapshot1"],
            ],
            [
                {},
                mock.Mock(
                    snapshots=[
                        mock.Mock(identifier="snapshot0", device="/dev/dev0"),
                        mock.Mock(identifier="snapshot1", device="/dev/dev1"),
                    ]
                ),
                ["snapshot0", "snapshot1"],
            ],
        ),
    )
    def test_get_attached_volumes_for_backup(
        self, attached_volumes, snapshots_info, expected_missing
    ):
        """Verify that the attached snapshots for the backup are returned."""
        # GIVEN a mock CloudSnapshotInterface which returns the specified attached
        # snapshots
        mock_snapshot_interface = mock.Mock()
        mock_snapshot_interface.get_attached_volumes.return_value = attached_volumes
        # AND a mock backup_info which contains the specified snapshots
        mock_backup_info = mock.Mock(snapshots_info=snapshots_info)
        # AND a given instance
        instance = "gcp_instance_name"

        # WHEN get_attached_volumes_for_backup is called
        # THEN if we expect missing snapshots, a RecoveryPreconditionException is
        # raised
        if expected_missing:
            with pytest.raises(RecoveryPreconditionException) as exc:
                SnapshotRecoveryExecutor.get_attached_volumes_for_backup(
                    mock_snapshot_interface, mock_backup_info, instance
                )
            # AND the exception has the expected message
            message_part_1, message_part_2 = str(exc.value).split(": ")
            expected_message = "The following snapshots are not attached to recovery instance {}".format(
                instance
            )
            assert message_part_1 == expected_message
            assert set(message_part_2.split(", ")) == set(expected_missing)
        else:
            # AND if we do not expect missing snapshots, no exception is raised and
            # the expected snapshots are returned
            attached_volumes_for_backup = (
                SnapshotRecoveryExecutor.get_attached_volumes_for_backup(
                    mock_snapshot_interface, mock_backup_info, instance
                )
            )
            for snapshot_metadata in snapshots_info.snapshots:
                assert (
                    len(
                        [
                            v
                            for v in attached_volumes_for_backup.values()
                            if v.source_snapshot == snapshot_metadata.identifier
                        ]
                    )
                    > 0
                )

    def test_get_attached_volumes_for_backup_no_snapshots_info(
        self,
    ):
        """
        Verify that an empty dict is returned for backups which have no snapshots_info.
        """
        # GIVEN a backup_info with no snapshots_info
        mock_backup_info = mock.Mock(snapshots_info=None)
        # WHEN get_attached_volumes_for_backup is called
        volumes = SnapshotRecoveryExecutor.get_attached_volumes_for_backup(
            mock.Mock(), mock_backup_info, "instance"
        )
        # THEN we expect an empty list to be returned
        assert volumes == {}

    @pytest.mark.parametrize(
        ("resolved_mount_info", "expected_error"),
        (
            # If the mount_point and mount_options resolved by resolve_mounted_volume
            # match those in backup_info.snapshots_info then we expect success.
            [
                (("/opt/disk0", "rw,noatime"), ("/opt/disk1", "rw")),
                None,
            ],
            # If resolving the mount point raises an exception we expect an error
            # finding the mount point
            [
                SnapshotBackupException("ssh error"),
                (
                    "Error checking mount points: Error finding mount point for disk "
                    "disk0: ssh error, Error finding mount point for disk disk1: ssh "
                    "error"
                ),
            ],
            # If a mount point cannot be found we expect an error message reporting
            # it could not be found
            [
                ([None, None], [None, None]),
                (
                    "Error checking mount points: Could not find disk disk0 "
                    "at any mount point, Could not find disk disk1 at any mount "
                    "point"
                ),
            ],
            # If a snapshot is mounted at an unexpected location then we expect an
            # error message reporting that this is the case
            [
                (("/opt/disk2", "rw,noatime"), ("/opt/disk3", "rw")),
                (
                    "Error checking mount points: Disk disk0 cloned from "
                    "snapshot snapshot0 is mounted at /opt/disk2 but /opt/disk0 was "
                    "expected., Disk disk1 cloned from snapshot snapshot1 is "
                    "mounted at /opt/disk3 but /opt/disk1 was expected."
                ),
            ],
            # If a snapshot is mounted with unexpected options then we expect an
            # error message reporting that this is the case
            [
                (("/opt/disk0", "rw"), ("/opt/disk1", "rw,noatime")),
                (
                    "Error checking mount options: Disk disk0 cloned from "
                    "snapshot snapshot0 is mounted with rw but rw,noatime was "
                    "expected., Disk disk1 cloned from snapshot snapshot1 is "
                    "mounted with rw,noatime but rw was expected."
                ),
            ],
        ),
    )
    def test_check_mount_points(self, resolved_mount_info, expected_error):
        """Verify check_mount_points fails when expected."""
        # GIVEN a mock VolumeMetadata which resolves the specified mount point and
        # options
        attached_volumes = {
            "disk0": mock.Mock(source_snapshot="snapshot0"),
            "disk1": mock.Mock(source_snapshot="snapshot1"),
        }

        def mock_resolve_mounted_volume(volume, mount_info, _cmd):
            volume.mount_point = mount_info[0]
            volume.mount_options = mount_info[1]

        for i, disk in enumerate(attached_volumes):
            # If resolved_mount_info should raise an exception then just set it as the
            # side effect
            if isinstance(resolved_mount_info, Exception):
                attached_volumes[disk].resolve_mounted_volume.side_effect = (
                    resolved_mount_info
                )
            # Otherwise, create a partial which sets the mount point and options to the
            # values at the current index
            else:
                attached_volumes[disk].resolve_mounted_volume.side_effect = partial(
                    mock_resolve_mounted_volume,
                    attached_volumes[disk],
                    resolved_mount_info[i],
                )

        # AND a backup_info which contains the specified snapshots_info
        snapshots_info = mock.Mock(
            snapshots=[
                mock.Mock(
                    identifier="snapshot0",
                    mount_point="/opt/disk0",
                    mount_options="rw,noatime",
                ),
                mock.Mock(
                    identifier="snapshot1",
                    mount_point="/opt/disk1",
                    mount_options="rw",
                ),
            ]
        )
        backup_info = mock.Mock(snapshots_info=snapshots_info)

        # WHEN check_mount_points is called and no error is expected
        # THEN no exception is raised
        mock_cmd = mock.Mock()
        if not expected_error:
            SnapshotRecoveryExecutor.check_mount_points(
                backup_info, attached_volumes, mock_cmd
            )
        # WHEN errors are expected
        else:
            # THEN a RecoveryPreconditionException is raised
            with pytest.raises(RecoveryPreconditionException) as exc:
                SnapshotRecoveryExecutor.check_mount_points(
                    backup_info, attached_volumes, mock_cmd
                )
            # AND the message matches the expected error message
            assert str(exc.value) == expected_error


class TestConfigurationFileMangeler:
    def test_simple_file_mangeling(self, tmpdir):
        a_file = tmpdir.join("some_file")
        file_content = "this is \n a very useful\t content.\nrecovery_target=something"
        a_file.write(file_content, ensure=True)
        cfm = ConfigurationFileMangeler()
        mangeled = cfm.mangle_options(a_file.strpath)
        content = a_file.read()
        assert len(mangeled) == 1
        assert "#BARMAN#recovery_target=something" in content


class TestRecoveryOperation(object):
    """
    Tests for the :class:`RecoveryOperation` class.
    """

    def get_recovery_operation(self, config=None, server=None, backup_manager=None):
        """
        Helper method to create an instance of :class:`RecoveryOperation` for testing.
        """
        config = config or mock.Mock()
        server = server or mock.Mock()
        backup_manager = backup_manager or mock.Mock()

        # Define a subclass of RecoveryOperation to be able to instantiate it
        class ImplementedOperation(RecoveryOperation):
            NAME = "dummy-op"

            def _execute(self, *args, **kwargs):
                pass

            def _should_execute(self, backup_info):
                return True

        return ImplementedOperation(config, server, backup_manager)

    def test_execute(self):
        """
        Test that :meth:`execute` calls the underlying :meth:`_execute` method correctly.
        """
        # GIVEN a RecoveryOperation instance
        operation = self.get_recovery_operation()
        operation._execute = mock.Mock()
        operation._get_command_interface = mock.Mock()

        # Mock the arguments to be passed to execute
        backup_info = testing_helpers.build_test_backup_info(
            server=testing_helpers.build_real_server()
        )
        args = (
            backup_info,
            "destination",
            "tablespaces",
            "remote_command",
            "recovery_info",
            "safe_horizon",
            "is_last_operation",
        )

        operation.execute(*args)
        # Assert that the command interface has been set
        operation._get_command_interface.assert_called_once_with("remote_command")
        assert operation.cmd == operation._get_command_interface.return_value
        # AND _execute is called with the arguments
        operation._execute.assert_called_once_with(*args)

    def test_execute_on_chain_on_non_incremental_backup(self):
        """
        Test that :meth:`_execute_on_chain` is called with the correct parameters
        when executing on a non-incremental backup.
        """
        # GIVEN a RecoveryOperation instance
        operation = self.get_recovery_operation()

        # AND a mock backup_info and method
        backup_info = testing_helpers.build_test_backup_info()
        mock_method = mock.Mock()

        # WHEN _execute_on_chain is called
        ret = operation._execute_on_chain(
            backup_info, "/destination", mock_method, "arg1", key="value", key2="value2"
        )

        # THEN the method is called with the correct parameters
        mock_method.assert_called_once_with(
            backup_info, "/destination", "arg1", key="value", key2="value2"
        )

        # AND the equivalent volatile backup info of the backup info passed is returned
        assert ret == mock_method.return_value

    @mock.patch("barman.infofile.LocalBackupInfo.walk_to_root")
    def test_execute_on_chain_on_incremental_backup(self, mock_walk_to_root):
        """
        Test that :meth:`_execute_on_chain` is called with the correct parameters
        when executing on an incremental backup.
        """
        # GIVEN a RecoveryOperation instance
        operation = self.get_recovery_operation()

        # AND a backup info chain
        parent_backup_info = testing_helpers.build_test_backup_info(
            backup_id="parent_backup",
        )
        backup_info = testing_helpers.build_test_backup_info(
            backup_id="incremental_backup",
            parent_backup_id=parent_backup_info.backup_id,
        )
        mock_walk_to_root.return_value = [backup_info, parent_backup_info]

        # Mock the return values of mock_method as if it were a volatile backup info
        # objects of the backups in the chain
        mock_vol_parent_backup_info = mock.Mock(backup_id=parent_backup_info.backup_id)
        mock_vol_backup_info = mock.Mock(
            backup_id=backup_info.backup_id,
            parent_backup_id=parent_backup_info.backup_id,
        )
        mock_method = mock.Mock()
        mock_method.side_effect = [mock_vol_backup_info, mock_vol_parent_backup_info]

        # WHEN _execute_on_chain is called
        ret = operation._execute_on_chain(
            backup_info, "/destination", mock_method, "arg1", key="value", key2="value2"
        )

        # THEN the method is called with the correct parameters
        mock_method.assert_has_calls(
            [
                mock.call(
                    backup_info, "/destination", "arg1", key="value", key2="value2"
                ),
                mock.call(
                    parent_backup_info,
                    "/destination",
                    "arg1",
                    key="value",
                    key2="value2",
                ),
            ]
        )

        # AND the chain is correctly remounted on the volatile backup info objects
        assert mock_vol_backup_info.parent_instance is mock_vol_parent_backup_info

        # AND the equivalent volatile backup info of the backup info passed is returned
        assert ret == mock_vol_backup_info

    def test_create_volatile_backup_info(self):
        """
        Test that :meth:`_create_volatile_backup_info` creates the respective
        :class:`VolatileBackupInfo` object.
        """
        # GIVEN a RecoveryOperation instance
        operation = self.get_recovery_operation(
            server=testing_helpers.build_mocked_server()
        )

        # AND a mock backup_info and base_directory
        backup_info = testing_helpers.build_test_backup_info()

        base_directory = "/fake/base/directory"

        # WHEN _create_volatile_backup_info is called
        vol_backup_info = operation._create_volatile_backup_info(
            backup_info, base_directory
        )

        # THEN the result is a VolatileBackupInfo object with the correct attributes
        assert isinstance(vol_backup_info, VolatileBackupInfo)
        assert vol_backup_info.server == operation.server
        assert vol_backup_info.base_directory == base_directory
        assert vol_backup_info.backup_id == backup_info.backup_id
        # Assert some random attributes just to be sure they were loaded to the
        # volatile backup info object
        assert vol_backup_info.begin_wal == backup_info.begin_wal
        assert vol_backup_info.end_wal == backup_info.end_wal

    def test_prepare_directory(self):
        """
        Test that :meth:`_prepare_directory` prepares the directory correctly.
        """
        # GIVEN a RecoveryOperation instance
        operation = self.get_recovery_operation()
        operation.cmd = mock.Mock()

        # Mock directory and command
        dest_dir = "/fake/directory"

        # WHEN _prepare_directory is called
        operation._prepare_directory(dest_dir)

        # THEN the correct calls are made to the command object
        operation.cmd.delete_if_exists.assert_called_once_with(dest_dir)
        operation.cmd.create_dir_if_not_exists.assert_called_once_with(
            dest_dir, mode="700"
        )
        operation.cmd.check_write_permission.assert_called_once_with(dest_dir)

    def test_prepare_directory_skip_delete_if_exists(self):
        """
        Test that :meth:`_prepare_directory` prepares the directory correctly when
        ``delete_if_exists``is ``True``.
        """
        # GIVEN a RecoveryOperation instance
        operation = self.get_recovery_operation()
        operation.cmd = mock.Mock()

        # Mock directory and command
        dest_dir = "/fake/directory"

        # WHEN _prepare_directory is called
        operation._prepare_directory(dest_dir, delete_if_exists=False)

        # THEN the correct calls are made to the command object
        operation.cmd.delete_if_exists.assert_not_called()
        operation.cmd.create_dir_if_not_exists.assert_called_once_with(
            dest_dir, mode="700"
        )
        operation.cmd.check_write_permission.assert_called_once_with(dest_dir)

    @pytest.mark.parametrize(
        "staging_location, remote_command, expected_call",
        [
            # Case 1: When staging location is local and it is a remote recovery
            # (remote_command is present), it requests a local command
            (
                "local",
                "ssh postgres@pg",
                mock.call(None, "/fake/server/path"),
            ),
            # Case 2: When staging location is local and it is a local recovery
            # (remote_command not present), it requests a local command interface
            (
                "local",
                None,
                mock.call(None, "/fake/server/path"),
            ),
            # Case 3: When staging location is remote and it is a remote recovery
            # (remote_command is present), it requests a remote command interface
            # This is the only possible case where it requests a remote command interface
            (
                "remote",
                "ssh postgres@pg",
                mock.call("ssh postgres@pg", "/fake/server/path"),
            ),
            # Case 4: When staging location is remote and it is a local recovery
            # (remote_command not present), it requests a local command interface
            # This case is not even valid in practice, but we test it just for
            # completeness
            (
                "remote",
                None,
                mock.call(None, "/fake/server/path"),
            ),
        ],
    )
    @mock.patch("barman.fs.unix_command_factory")
    def test_get_command_interface(
        self, mock_unix_command_factory, staging_location, remote_command, expected_call
    ):
        """
        Test that :meth:`_get_command_interface` returns the correct command interface.
        """
        # GIVEN a RecoveryOperation instance
        operation = self.get_recovery_operation()
        operation.server.path = "/fake/server/path"
        operation.config.staging_location = staging_location

        # WHEN _get_command_interface is called with the parametrized values
        cmd_interface = operation._get_command_interface(remote_command)

        # THEN the command interface is requested appropriately to the factory
        mock_unix_command_factory.assert_has_calls([expected_call])

        # AND the command interface is returned
        assert cmd_interface is mock_unix_command_factory.return_value

    def test_post_recovery_cleanup(self):
        """
        Test that :meth:`_post_recovery_cleanup` cleans up wanted files.
        """
        # GIVEN a RecoveryOperation instance
        operation = self.get_recovery_operation()
        operation.cmd = mock.Mock()

        # Mock directory and command
        dest_dir = "/fake/destination/directory"

        # WHEN _post_recovery_cleanup is called
        operation._post_recovery_cleanup(dest_dir)

        # THEN the correct calls are made to the command object to delete files
        operation.cmd.delete_if_exists.assert_has_calls(
            [
                mock.call("/fake/destination/directory/pg_log/*"),
                mock.call("/fake/destination/directory/log/*"),
                mock.call("/fake/destination/directory/pg_xlog/*"),
                mock.call("/fake/destination/directory/pg_wal/*"),
                mock.call("/fake/destination/directory/postmaster.pid"),
                mock.call("/fake/destination/directory/recovery.conf"),
                mock.call("/fake/destination/directory/tablespace_map"),
            ]
        )

    @mock.patch("barman.output.warning")
    def test_post_recovery_cleanup_failed(self, mock_warning):
        """
        Test that :meth:`_post_recovery_cleanup` warns about errors during cleanup.
        """
        # GIVEN a RecoveryOperation instance
        operation = self.get_recovery_operation()
        operation.cmd = mock.Mock()

        # Mock directory and command
        dest_dir = "/fake/destination/directory"

        # Simulate an error in the command
        operation.cmd.delete_if_exists.side_effect = CommandFailedException

        # WHEN _post_recovery_cleanup is called
        operation._post_recovery_cleanup(dest_dir)

        # THEN a warning is logged (in this case it will fail for all items)
        items = [
            os.path.join(dest_dir, "pg_log/*"),
            os.path.join(dest_dir, "log/*"),
            os.path.join(dest_dir, "pg_xlog/*"),
            os.path.join(dest_dir, "pg_wal/*"),
            os.path.join(dest_dir, "postmaster.pid"),
            os.path.join(dest_dir, "recovery.conf"),
            os.path.join(dest_dir, "tablespace_map"),
        ]
        for item in items:
            mock_warning.assert_any_call(
                "Cleanup operation failed to delete %s after backup copy: %s\n"
                "If this file or directory is irrelevant for the recovery, please "
                "remove it manually.",
                item,
                mock.ANY,  # The exception object will vary
            )

    @pytest.mark.parametrize(
        "is_last_operation, tablespace_mapping, tbspc_destinations",
        [
            # Case 1: When it is the last operation and relocation is provided, the
            # link is from <relocation> to pg_tblspc/<oid>
            (
                True,
                {"tbs1": "/path/relocation/tbs1", "tbs2": "/path/relocation/tbs2"},
                ["/path/relocation/tbs1", "/path/relocation/tbs2"],
            ),
            # Case 2: When it is the last operation and no tablespace mapping is provided,
            # the link is from <original_tbspc_location> to pg_tblspc/<oid>
            (
                True,
                None,
                ["/path/to/original/tbs1", "/path/to/original/tbs2"],
            ),
            # Case 3: When it is not the last operation the link is always from
            # <volatile_backup_info_location>/<oid> to pg_tblspc/<oid>
            (
                False,
                {"tbs1": "/relocation/tbs1", "tbs2": "/relocation/tbs2"},
                ["/path/to/backup_id/data/123", "/path/to/backup_id/data/321"],
            ),
            # Case 4: When it is not the last operation the link is always from
            # <volatile_backup_info_location>/<oid> to pg_tblspc/<oid>
            (
                False,
                None,
                ["/path/to/backup_id/data/123", "/path/to/backup_id/data/321"],
            ),
        ],
    )
    @mock.patch("barman.recovery_executor.output")
    def test_link_tablespaces(
        self, mock_output, is_last_operation, tablespace_mapping, tbspc_destinations
    ):
        """
        Test that :meth:`_link_tablespaces` links the tablespaces in ``pg_tblspc``.
        """
        # GIVEN a RecoveryOperation instance
        operation = self.get_recovery_operation()
        operation.cmd = mock.Mock()

        # Mock a backup_info with tablespaces
        vol_backup_info = mock.Mock(
            tablespaces=[
                mock.Mock(oid=123, location="/path/to/original/tbs1"),
                mock.Mock(oid=321, location="/path/to/original/tbs2"),
            ],
            get_data_directory=lambda oid: f"/path/to/backup_id/data/{oid}",
        )
        # Mock the tablespaces names as we can't do that when instantiate the
        # mock, e.g. mock.Mock(name="tbs1") does not work
        vol_backup_info.tablespaces[0].name = "tbs1"
        vol_backup_info.tablespaces[1].name = "tbs2"

        # Mock the rest of the method parameters
        pgdata_dir = "/destination/pgdata"

        # WHEN _link_tablespaces is called
        operation._link_tablespaces(
            vol_backup_info, pgdata_dir, tablespace_mapping, is_last_operation
        )

        # THEN the correct call is made to create the pg_tblspc directory
        pg_tblspc_dir = os.path.join(pgdata_dir, "pg_tblspc")
        operation.cmd.create_dir_if_not_exists.assert_called_once_with(pg_tblspc_dir)

        # AND the symlinks destination are cleaned up before being created
        tbs1_in_pgdata = os.path.join(pg_tblspc_dir, "123")
        tbs2_in_pgdata = os.path.join(pg_tblspc_dir, "321")
        operation.cmd.delete_if_exists.assert_has_calls(
            [mock.call(tbs1_in_pgdata), mock.call(tbs2_in_pgdata)]
        )
        # AND the write permission on the tablespace locations are checked
        operation.cmd.check_write_permission.assert_has_calls(
            [mock.call(tbspc_destinations[0]), mock.call(tbspc_destinations[1])]
        )
        # AND finally the symlinks are created correctly, linking the tablespace
        # destinations to their respective OID directories in pg_tblspc
        operation.cmd.create_symbolic_link.assert_has_calls(
            [
                mock.call(tbspc_destinations[0], os.path.join(pg_tblspc_dir, "123")),
                mock.call(tbspc_destinations[1], os.path.join(pg_tblspc_dir, "321")),
            ]
        )
        # AND the info messages are logged correctly in case it is the last operation
        if is_last_operation:
            mock_output.info.assert_has_calls(
                [
                    mock.call("\t%s, %s, %s", 123, "tbs1", tbspc_destinations[0]),
                    mock.call("\t%s, %s, %s", 321, "tbs2", tbspc_destinations[1]),
                ]
            )

    @mock.patch("barman.recovery_executor.output", wraps=output)
    def test_link_tablespaces_fails(self, mock_output):
        """
        Test that :meth:`_link_tablespaces` handle errors correctly when
        the preparation or creation of tablespaces symlinks fails.
        """
        # GIVEN a RecoveryOperation instance
        operation = self.get_recovery_operation()
        operation.cmd = mock.Mock()

        # Mock a backup_info with tablespaces
        vol_backup_info = mock.Mock(
            tablespaces=[
                mock.Mock(oid=123, location="/path/to/original/tbs1"),
                mock.Mock(oid=321, location="/path/to/original/tbs2"),
            ],
            get_data_directory=lambda oid: f"/path/to/backup_id/data/{oid}",
        )
        # Mock the tablespaces names as we can't do that when instantiate the
        # mock, e.g. mock.Mock(name="tbs1") does not work
        vol_backup_info.tablespaces[0].name = "tbs1"
        vol_backup_info.tablespaces[1].name = "tbs2"

        # Mock the rest of the method parameters
        pgdata_dir = "/destination/pgdata"

        tblspc_dir = os.path.join(pgdata_dir, "pg_tblspc")

        # Case 1: WHEN we can not create the pg_tblspc directory
        operation.cmd.create_dir_if_not_exists.side_effect = FsOperationFailed
        with pytest.raises(SystemExit):
            operation._link_tablespaces(vol_backup_info, pgdata_dir, None, True)
        # THEN it logs an error and exits
        mock_output.error.assert_called_once_with(
            "unable to initialize tablespace directory '%s': %s", tblspc_dir, mock.ANY
        )
        mock_output.close_and_exit.assert_called_once_with()

        # Reset mocks
        operation.cmd.create_dir_if_not_exists.reset_mock()
        operation.cmd.create_dir_if_not_exists.side_effect = None
        mock_output.reset_mock()

        # Case 2: WHEN we are unable to prepare or create the symlinks
        operation.cmd.create_symbolic_link.side_effect = FsOperationFailed
        with pytest.raises(SystemExit):
            operation._link_tablespaces(vol_backup_info, pgdata_dir, None, True)
        # THEN it logs an error and exits
        mock_output.error.assert_called_once_with(
            "unable to prepare '%s' tablespace (destination '%s'): %s",
            "tbs1",
            "/path/to/original/tbs1",
            mock.ANY,
        )
        mock_output.close_and_exit.assert_called_once_with()

    @mock.patch("barman.recovery_executor.output", wraps=output)
    def test_link_tablespaces_returns_with_no_tablespaces(self, mock_output):
        """
        Test that :meth:`_link_tablespaces` has no effect when there are no tablespaces
        to be linked and outputs a debug message.
        """
        operation = self.get_recovery_operation()
        operation.cmd = mock.Mock()
        vol_backup_info = mock.Mock(
            tablespaces=None,
            get_data_directory=lambda oid: f"/path/to/backup_id/data/{oid}",
        )
        pgdata_dir = "/destination/pgdata"
        pg_tblspc_dir = os.path.join(pgdata_dir, "pg_tblspc")
        operation._link_tablespaces(vol_backup_info, pgdata_dir, None, True)
        # call is made to create the pg_tblspc directory
        operation.cmd.create_dir_if_not_exists.assert_called_once_with(pg_tblspc_dir)
        # THEN it logs a debug message
        mock_output.debug.assert_called_once_with(
            "There are no tablespaces to be linked. Skipping this step."
        )

    @mock.patch("os.getpid")
    def test_staging_path_property(self, mock_pid):
        """
        Test that the :attr:`RecoveryOperation.staging_path`` property returns the
        correct path.

        This test verifies that the ``staging_path`` is constructed by joining the
        configured staging path with the operation name and the current process ID.

        Steps:
            - Mocks a configuration object with a specified staging path.
            - Builds a mocked server with the same staging path in its mai
              configuration.
            - Creates an `ImplementedOperation` instance using the mocked server, which
              is a dummy operator.
            - Asserts that the :attr:`RecoveryOperation.staging_path` property matches
              the expected value.

        :raises AssertionError: If the :attr:`RecoveryOperation.staging_path` property
            does not match the expected path.
        """
        config = Mock()
        config.staging_path = "/tmp/staging"
        server = testing_helpers.build_mocked_server(
            main_conf={"staging_path": "/tmp/staging"}
        )
        operation = self.get_recovery_operation(
            config=server.config, server=server, backup_manager=server.backup_manager
        )
        mock_pid.return_value = "123456"
        expected = os.path.join(config.staging_path, operation.NAME + "123456")
        assert operation.staging_path == expected

    @mock.patch("barman.recovery_executor.output")
    def test_cleanup_staging_dir_success(self, mock_output):
        """
        Test that the :meth:`RecoveryOperation.cleanup_staging_dir` method successfully
        deletes the staging directory without raising warnings.

        This test mocks the output and verifies that:
        - The :meth:`~UnixLocalCommand.delete_if_exists` method is called once with the
          correct staging path.
        - No warning is issued during the cleanup process.

        :param _patch.patcher mock_output: Mocked output object to capture warnings.
        """
        config = Mock()
        config.staging_path = "/tmp/staging"
        server = testing_helpers.build_mocked_server(
            main_conf={"staging_path": "/tmp/staging"}
        )
        operation = self.get_recovery_operation(
            config=server.config, server=server, backup_manager=server.backup_manager
        )
        operation.cmd = mock.Mock()
        operation.cleanup_staging_dir()
        operation.cmd.delete_if_exists.assert_called_once_with(operation.staging_path)
        mock_output.warning.assert_not_called()

    @mock.patch("barman.recovery_executor.output")
    def test_cleanup_staging_dir_failure(self, mock_output):
        """
        Test that :meth:`RecoveryOperation.cleanup_staging_dir` logs a warning when
        deletion of the staging directory fails.

        This test mocks the :meth:`~UnixLocalCommand.delete_if_exists` method to raise a
            :exc:`CommandFailedException`, simulating a failure during the cleanup
        operation. It verifies that a warning is logged and that the warning message
        contains the expected text and staging path.

        :param _patch.patcher mock_output: Mocked output module to capture warning logs.
        :raises AssertionError: If the warning is not logged or does not contain the
            expected message.
        """
        config = Mock()
        config.staging_path = "/tmp/staging"
        server = testing_helpers.build_mocked_server(
            main_conf={"staging_path": "/tmp/staging"}
        )
        operation = self.get_recovery_operation(
            config=server.config, server=server, backup_manager=server.backup_manager
        )
        operation.cmd = mock.Mock()
        operation.cmd.delete_if_exists.side_effect = CommandFailedException("fail")
        operation.cleanup_staging_dir()
        mock_output.warning.assert_called_once()
        args, kwargs = mock_output.warning.call_args
        assert "Staging path cleanup operation failed to delete" in args[0]
        assert operation.staging_path in args


class TestRsyncCopyOperation(object):
    """
    Tests for the :class:`RsyncCopyOperation` class.
    """

    @mock.patch("barman.recovery_executor.RsyncCopyOperation._get_command_interface")
    def test_execute(self, _mock_cmd):
        """
        Test that :meth:`_execute` calls :meth:`_execute_on_chain` correctly.
        """
        # GIVEN a RsyncCopyOperation instance
        operation = RsyncCopyOperation(
            config=mock.Mock(),
            server=mock.Mock(),
            backup_manager=mock.Mock(),
        )
        operation._execute_on_chain = mock.Mock()

        # Mock the arguments to be passed to execute
        backup_info = testing_helpers.build_test_backup_info(
            server=testing_helpers.build_real_server()
        )
        args = [
            backup_info,
            "destination",
            "tablespaces",
            "remote_command",
            "recovery_info",
            "safe_horizon",
            "is_last_operation",
        ]

        # WHEN execute is called
        operation.execute(*args)

        # THEN _rsync_backup_copy should be called with the correct arguments
        operation._execute_on_chain.assert_called_once_with(
            backup_info,
            "destination",
            operation._rsync_backup_copy,
            "tablespaces",
            "remote_command",
            # "recovery_info", # not used in _rsync_backup_copy
            "safe_horizon",
            "is_last_operation",
        )

    @pytest.mark.parametrize("is_last_operation", [True, False])
    @pytest.mark.parametrize("remote_command", ["ssh postgres@pg", None])
    @mock.patch("barman.recovery_executor.RsyncCopyOperation._copy_backup_dir")
    @mock.patch(
        "barman.recovery_executor.RsyncCopyOperation._copy_pgdata_and_tablespaces"
    )
    @mock.patch(
        "barman.recovery_executor.RsyncCopyOperation._create_volatile_backup_info",
    )
    @mock.patch("barman.recovery_executor.RsyncCopyController")
    @mock.patch("barman.recovery_executor.RsyncCopyOperation._link_tablespaces")
    def test_rsync_backup_copy(
        self,
        mock_link_tablespaces,
        mock_copy_controller,
        mock_create_vol_backup,
        mock_copy_pgdata_and_tablespaces,
        mock_copy_backup_dir,
        is_last_operation,
        remote_command,
    ):
        """
        Test that :meth:`_rsync_backup_copy` works as expected.

        It should create a volatile backup info, instantiate an
        :class:`RsyncCopyController` and call the responsible copy method based on
        whether it is the last operation or not.
        """
        # GIVEN a RsyncCopyOperation instance
        server = testing_helpers.build_mocked_server(
            main_conf={"path_prefix": "/path/to/binaries"}
        )
        operation = RsyncCopyOperation(
            config=server.config,
            server=server,
            backup_manager=server.backup_manager,
        )

        # Mock a backup_info with a tablespace
        backup_info = testing_helpers.build_test_backup_info(
            server=server,
            tablespaces=[("tbs1", 16409, "/var/lib/pgsql/17/tablespaces1")],
        )

        # Mock some parameters for _rsync_backup_copy. is_last_operation and
        # remote_command are in pytest.mark.parametrize so they are not mocked here
        destination = "/path/to/destination"
        tablespaces = {"tbs1": "/path/to/relocation"}
        safe_horizon = datetime.now()

        # WHEN _rsync_backup_copy is called
        ret = operation._rsync_backup_copy(
            backup_info,
            destination,
            tablespaces,
            remote_command,
            safe_horizon,
            is_last_operation,
        )

        # THEN the volatile backup info is created
        mock_create_vol_backup.assert_called_once_with(backup_info, destination)
        vol_backup_info = mock_create_vol_backup.return_value

        # AND an RsyncCopyController is intantiated with the correct parameters
        mock_copy_controller.assert_called_once_with(
            path=server.path,
            ssh_command=remote_command,
            network_compression=server.config.network_compression,
            safe_horizon=safe_horizon,
            retry_times=server.config.basebackup_retry_times,
            retry_sleep=server.config.basebackup_retry_sleep,
            workers=server.config.parallel_jobs,
            workers_start_batch_period=server.config.parallel_jobs_start_batch_period,
            workers_start_batch_size=server.config.parallel_jobs_start_batch_size,
        )

        # AND the responsible copy method is called based on is_last_operation
        dest_prefix = "" if not remote_command else ":"
        if is_last_operation:
            mock_copy_pgdata_and_tablespaces.assert_called_once_with(
                backup_info,
                mock_copy_controller.return_value,
                dest_prefix,
                destination,
                tablespaces,
            )
            mock_copy_backup_dir.assert_not_called()
        else:
            mock_copy_backup_dir.assert_called_once_with(
                backup_info,
                mock_copy_controller.return_value,
                dest_prefix,
                vol_backup_info,
            )
            mock_copy_pgdata_and_tablespaces.assert_not_called()

        # AND the tablespaces symlinks are handled
        pgdata_dir = (
            destination if is_last_operation else vol_backup_info.get_data_directory()
        )
        mock_link_tablespaces.assert_called_once_with(
            vol_backup_info,
            pgdata_dir,
            tablespaces,
            is_last_operation,
        )

        # THEN the volatile backup info is returned
        assert ret is vol_backup_info

    @mock.patch("barman.recovery_executor.RsyncCopyOperation._prepare_directory")
    @mock.patch(
        "barman.infofile.LocalBackupInfo.get_basebackup_directory",
        return_value="/path/to/basebackup/directory/backup_id",
    )
    def test_copy_backup_dir(self, mock_get_backup_dir, mock_prepare_directory):
        """
        Test that :meth:`_copy_backup_dir` copies the backup directory correctly.
        """
        # GIVEN a RsyncCopyOperation instance
        server = testing_helpers.build_mocked_server(
            main_conf={"path_prefix": "/path/to/binaries"}
        )
        operation = RsyncCopyOperation(
            config=server.config,
            server=server,
            backup_manager=server.backup_manager,
        )

        # Mock some parameters for _copy_backup_dir
        backup_info = testing_helpers.build_test_backup_info(server=server)
        mock_copy_controller = mock.Mock()
        dest_prefix = ":"
        vol_backup_info = mock.Mock(
            get_basebackup_directory=lambda: "/path/to/staging/backup_id"
        )

        # WHEN _copy_backup_dir is called
        operation._copy_backup_dir(
            backup_info, mock_copy_controller, dest_prefix, vol_backup_info
        )

        # THEN the backup directory is added to the controller correctly
        mock_copy_controller.add_directory.assert_called_once_with(
            label="backup",
            src="/path/to/basebackup/directory/backup_id/",
            dst=":/path/to/staging/backup_id",
            bwlimit=server.config.get_bwlimit(),
            item_class=mock_copy_controller.VOLATILE_BACKUP_CLASS,
        )

        # AND the staging directory destination is prepared correctly
        mock_prepare_directory.assert_called_once_with("/path/to/staging/backup_id")

        # AND the copy is executed
        mock_copy_controller.copy.assert_called_once()

    @mock.patch("barman.recovery_executor.RsyncCopyOperation._prepare_directory")
    @mock.patch(
        "barman.infofile.LocalBackupInfo.get_data_directory",
        return_value="/path/to/basebackup/directory/backup_id/data",
    )
    def test_copy_pgdata_and_tablespaces_with_no_tablespaces(
        self, mock_get_data_dir, mock_prepare_directory
    ):
        """
        Test that :meth:`_copy_pgdata_and_tablespaces` copies the pgdata directory
        correctly when there are no tablespaces.
        """
        # GIVEN a RsyncCopyOperation instance
        server = testing_helpers.build_mocked_server(
            main_conf={"path_prefix": "/path/to/binaries"}
        )
        operation = RsyncCopyOperation(
            config=server.config,
            server=server,
            backup_manager=server.backup_manager,
        )

        # Mock some parameters for _copy_backup_dir
        backup_info = testing_helpers.build_test_backup_info(
            server=server, tablespaces=[]
        )
        mock_copy_controller = mock.Mock()
        dest_prefix = ":"
        destination = "/path/to/destination"
        tablespaces = None

        # WHEN _copy_pgdata_and_tablespaces is called
        operation._copy_pgdata_and_tablespaces(
            backup_info, mock_copy_controller, dest_prefix, destination, tablespaces
        )

        # THEN the pgdata directory is added to the controller correctly
        mock_copy_controller.add_directory.assert_called_once_with(
            label="pgdata",
            src="/path/to/basebackup/directory/backup_id/data/",
            dst=dest_prefix + destination,
            bwlimit=server.config.get_bwlimit(),
            exclude=[
                "/pg_log/*",
                "/log/*",
                "/pg_xlog/*",
                "/pg_wal/*",
                "/postmaster.pid",
                "/recovery.conf",
                "/tablespace_map",
            ],
            exclude_and_protect=[],
            item_class=mock_copy_controller.PGDATA_CLASS,
        )

        # AND the destination directory is prepared correctly
        mock_prepare_directory.assert_called_once_with(destination)

        # AND the copy is executed
        mock_copy_controller.copy.assert_called_once()

    @pytest.mark.parametrize(
        "tablespace_relocation",
        [
            None,
            {"tbs1": "/path/to/relocation/tbs1", "tbs2": "/path/to/relocation/tbs2"},
        ],
    )
    @mock.patch("barman.recovery_executor.RsyncCopyOperation._prepare_directory")
    @mock.patch(
        "barman.infofile.LocalBackupInfo.get_data_directory",
        lambda self, oid=None: (
            "/path/to/basebackup/directory/backup_id/data"
            if oid is None
            else f"/path/to/basebackup/directory/backup_id/{oid}"
        ),
    )
    def test_copy_pgdata_and_tablespaces_with_tablespaces(
        self, mock_prepare_directory, tablespace_relocation
    ):
        """
        Test that :meth:`_copy_pgdata_and_tablespaces` copies the pgdata and
        tablespaces directories correctly when there are tablespaces, honoring
        the relocation if provided.
        """
        # GIVEN a RsyncCopyOperation instance
        server = testing_helpers.build_mocked_server(
            main_conf={"path_prefix": "/path/to/binaries"}
        )
        operation = RsyncCopyOperation(
            config=server.config,
            server=server,
            backup_manager=server.backup_manager,
        )

        # Mock a backup_info with tablespaces
        backup_info = testing_helpers.build_test_backup_info(
            server=server,
            tablespaces=[
                ("tbs1", 16409, "/var/lib/pgsql/17/tablespaces1"),
                ("tbs2", 16419, "/var/lib/pgsql/17/tablespaces2"),
            ],
        )

        # Mock some parameters for _copy_backup_dir
        mock_copy_controller = mock.Mock()
        dest_prefix = ":"
        destination = "/path/to/destination"
        tablespaces = tablespace_relocation

        # WHEN _copy_pgdata_and_tablespaces is called
        operation._copy_pgdata_and_tablespaces(
            backup_info, mock_copy_controller, dest_prefix, destination, tablespaces
        )

        # The tablespace destination honor the relocation if provided,
        # otherwise it is its original location
        if tablespace_relocation is not None:
            tbs1_dest = tablespace_relocation.get("tbs1")
            tbs2_dest = tablespace_relocation.get("tbs2")
        else:
            tbs1_dest = "/var/lib/pgsql/17/tablespaces1"
            tbs2_dest = "/var/lib/pgsql/17/tablespaces2"

        # THEN the tablespaces and pgdata are added to the controller correctly
        mock_copy_controller.add_directory.assert_has_calls(
            [
                mock.call(
                    label="tbs1",
                    src="/path/to/basebackup/directory/backup_id/16409/",
                    dst=dest_prefix + tbs1_dest,
                    bwlimit=server.config.get_bwlimit(16409),
                    item_class=mock_copy_controller.TABLESPACE_CLASS,
                ),
                mock.call(
                    label="tbs2",
                    src="/path/to/basebackup/directory/backup_id/16419/",
                    dst=dest_prefix + tbs2_dest,
                    bwlimit=server.config.get_bwlimit(16419),
                    item_class=mock_copy_controller.TABLESPACE_CLASS,
                ),
                mock.call(
                    label="pgdata",
                    src="/path/to/basebackup/directory/backup_id/data/",
                    dst=dest_prefix + destination,
                    bwlimit=server.config.get_bwlimit(),
                    exclude=[
                        "/pg_log/*",
                        "/log/*",
                        "/pg_xlog/*",
                        "/pg_wal/*",
                        "/postmaster.pid",
                        "/recovery.conf",
                        "/tablespace_map",
                    ],
                    exclude_and_protect=["/pg_tblspc/16409", "/pg_tblspc/16419"],
                    item_class=mock_copy_controller.PGDATA_CLASS,
                ),
            ]
        )

        # AND the destination directory is prepared correctly
        mock_prepare_directory.assert_has_calls(
            [
                mock.call("/path/to/destination"),
                mock.call(tbs1_dest),
                mock.call(tbs2_dest),
            ]
        )

        # # AND the copy is executed
        mock_copy_controller.copy.assert_called_once()

    @pytest.mark.parametrize("remote_command", ["ssh user@host", None])
    @mock.patch("barman.recovery_executor.fs.unix_command_factory")
    @mock.patch("barman.recovery_executor.output")
    def test__get_command_interface(
        self, mock_output, mock_unix_command_factory, remote_command
    ):
        backup_manager = testing_helpers.build_backup_manager(
            main_conf={
                "backup_options": "concurrent_backup",
            }
        )
        op = RsyncCopyOperation(
            backup_manager.config, backup_manager.server, backup_manager
        )
        result = op._get_command_interface(remote_command)
        mock_unix_command_factory.assert_called_once_with(
            remote_command, backup_manager.server.path
        )
        assert result == mock_unix_command_factory.return_value


class TestCombineOperation(object):
    """Tests for the :class:`CombineOperation` class"""

    def test_execute(self):
        """
        Test that :meth:`_execute` calls :meth:`_combine_backups` correctly.
        """
        # GIVEN a CombineOperation instance
        operation = CombineOperation(
            config=mock.Mock(),
            server=mock.Mock(),
            backup_manager=mock.Mock(),
        )
        operation._combine_backups = mock.Mock()

        # Mock the arguments to be passed to execute
        backup_info = mock.Mock()
        args = [
            backup_info,
            "destination",
            "tablespaces",
            "remote_command",
            "recovery_info",
            "safe_horizon",
            "is_last_operation",
        ]

        # WHEN _execute is called
        operation._execute(*args)

        # THEN _combine_backups should be called with the correct arguments
        operation._combine_backups.assert_called_once_with(
            backup_info,
            "destination",
            "tablespaces",
            "remote_command",
            "is_last_operation",
        )

    def test_should_execute(self):
        """
        Test that :meth:`_should_execute` works correctly.
        """
        # GIVEN a CombineOperation instance
        operation = CombineOperation(
            config=mock.Mock(),
            server=mock.Mock(),
            backup_manager=mock.Mock(),
        )
        # Case 1: When the backup is incremental it returns True
        backup_info = mock.Mock(is_incremental=True)
        with pytest.raises(NotImplementedError) as e:
            operation._should_execute(backup_info)

        assert (
            "CombineOperation is executed only on the leaf backup of the chain"
            in str(e)
        )

    @pytest.mark.parametrize("is_last_operation", [True, False])
    @pytest.mark.parametrize("tablespaces", [None, {"tbs1": "/path/to/relocation"}])
    @pytest.mark.parametrize("is_checksum_consistent", [True, False])
    @mock.patch(
        "barman.recovery_executor.CombineOperation._create_volatile_backup_info",
        return_value=mock.Mock(
            get_data_directory=lambda *args: "/path/to/backup_id/data"
        ),
    )
    @mock.patch("barman.recovery_executor.CombineOperation._get_tablespace_mapping")
    @mock.patch("barman.recovery_executor.CombineOperation._prepare_directory")
    @mock.patch("barman.recovery_executor.CombineOperation._post_recovery_cleanup")
    @mock.patch("barman.recovery_executor.CombineOperation._run_pg_combinebackup")
    @mock.patch("barman.recovery_executor.output")
    def test_combine_backups(
        self,
        mock_output,
        mock_run_pg_combinebackup,
        mock_post_recovery_cleanup,
        mock_prepare_directory,
        mock_get_tablespace_mapping,
        mock_create_volatile_backup_info,
        tablespaces,
        is_last_operation,
        is_checksum_consistent,
    ):
        """
        Test that :meth:`_combine_backups` works correctly.
        It should create a volatile backup info, do the tablespace mapping, and call
        :meth:`_run_pg_combinebackup` with the correct parameters.
        """
        # Finish mocking get_tablespace_mapping as it depends on the tablespaces value
        mock_get_tablespace_mapping.return_value = {}
        if tablespaces is not None:
            mock_get_tablespace_mapping.return_value = {
                "/path/tp/source/tbs1": "/path/to/final/destination/tbs1"
            }

        # Also finish mocking the volatile backup info, as its object retunred should
        # have the is_checksum_consistent method mocked to return the parametrized
        # value of is_checksum_consistent
        mock_create_volatile_backup_info.return_value.is_checksum_consistent = (
            lambda: is_checksum_consistent
        )

        # GIVEN a CombineOperation instance
        operation = CombineOperation(
            config=mock.Mock(),
            server=mock.Mock(),
            backup_manager=mock.Mock(),
        )

        # Mock the rest of the method arguments
        backup_info = mock.Mock()
        destination = "/path/to/destination"
        remote_command = "ssh postgres@pg"

        # WHEN _combine_backups is called
        ret = operation._combine_backups(
            backup_info, destination, tablespaces, remote_command, is_last_operation
        )

        # THEN _create_volatile_backup_info is called with the correct parameters
        mock_create_volatile_backup_info.assert_called_once_with(
            backup_info, destination
        )
        vol_backup_info = mock_create_volatile_backup_info.return_value

        # AND _get_tablespace_mapping is called with the correct parameters
        mock_get_tablespace_mapping.assert_called_once_with(
            backup_info, vol_backup_info, tablespaces, is_last_operation
        )

        # Note: The actual desination output of the operation depend
        # on whether it is the last operation or not
        output_dest = (
            destination if is_last_operation else vol_backup_info.get_data_directory()
        )

        # AND all the destinations are prepared
        dests = [output_dest] + list(mock_get_tablespace_mapping.return_value.values())
        mock_prepare_directory.assert_has_calls(
            [mock.call(dest) for dest in dests],
        )

        # AND _run_pg_combinebackup is called with the correct parameters
        mock_run_pg_combinebackup.assert_called_once_with(
            backup_info,
            output_dest,
            mock_get_tablespace_mapping.return_value,
            remote_command,
        )

        # AND if the checksum of the restored backup is inconsitent raises a warning
        if not is_checksum_consistent:
            mock_output.warning.assert_called_once_with(
                "You are restoring from an incremental backup where checksums were enabled on "
                "that backup, but not all backups in the chain. It is advised to disable, and "
                "optionally re-enable, checksums on the destination directory to avoid failures."
            )

        # AND if this is the last operation _post_recovery_cleanup is called correctly
        if is_last_operation:
            mock_post_recovery_cleanup.assert_called_once_with(output_dest)

        # AND the volatile backup info is returned
        assert ret == vol_backup_info

    @pytest.mark.parametrize("staging_location", ["local", "remote"])
    @mock.patch(
        "barman.recovery_executor.CombineOperation._fetch_remote_status",
        return_value={
            "pg_combinebackup_path": "/path/to/pg_combinebackup",
            "pg_combinebackup_version": "17.0.0",
        },
    )
    @mock.patch(
        "barman.recovery_executor.CombineOperation._get_backup_chain_paths",
        return_value=["/path/to/backup_id/data", "/path/to/backup_id/parent/data"],
    )
    @mock.patch("barman.recovery_executor.PgCombineBackup")
    @mock.patch("barman.recovery_executor.Command")
    @mock.patch("barman.recovery_executor.full_command_quote")
    def test_run_pg_combinebackup(
        self,
        mock_full_command_quote,
        mock_command,
        mock_pg_combine_backup,
        mock_get_backup_chain_paths,
        mock_fetch_remote_status,
        staging_location,
    ):
        """
        Test that :meth:`_run_pg_combinebackup` builds :class:`PgCombineBackup`
        with the correct parameters and execute it, locally or remotely
        depending on the staging location.
        """
        # GIVEN a CombineOperation instance
        operation = CombineOperation(
            config=mock.Mock(staging_location=staging_location),
            server=mock.Mock(),
            backup_manager=mock.Mock(),
        )

        # Build the method parameters
        backup_info = mock.Mock()
        backup_info.pg_major_version.return_value = "17"
        destination = "/path/to/destination"
        tablespace_mapping = {
            "/path/to/backup_id/tbs1": "/path/to/relocation/tbs1",
            "/path/to/backup_id/tbs2": "/path/to/relocation/tbs2",
        }
        # remote_command is only relevant when the staging location is remote,
        # otherwise it is not used, so we can leave it as None
        remote_command = "ssh postgres@pg" if staging_location == "remote" else None

        # WHEN _run_pg_combinebackup is called
        operation._run_pg_combinebackup(
            backup_info, destination, tablespace_mapping, remote_command
        )

        # THEN _fetch_remote_status is called correctly
        mock_fetch_remote_status.assert_called_once()
        remote_status = mock_fetch_remote_status.return_value

        # AND _get_backup_chain_paths is called correctly
        mock_get_backup_chain_paths.assert_called_once_with(backup_info)
        backups_chain = mock_get_backup_chain_paths.return_value

        # AND PgCombineBackup is instantiated with the correct parameters
        mock_pg_combine_backup.assert_called_once_with(
            destination=destination,
            command=remote_status["pg_combinebackup_path"],
            version=remote_status["pg_combinebackup_version"],
            app_name=None,
            tbs_mapping=tablespace_mapping,
            out_handler=mock.ANY,
            args=backups_chain,
        )

        # AND if the staging location is local, just executes the prepared command
        if staging_location == "local":
            mock_pg_combine_backup.return_value.assert_called_once()
        # otherwise, builds an alternative remote command based on the
        # prepared command and execute it remotely
        else:
            mock_command.assert_called_once_with(
                remote_command,
                shell=True,
                check=True,
                path=operation.server.path,
                out_handler=mock.ANY,
            )
            # The full command should be quoted correctly
            mock_full_command_quote.assert_called_once_with(
                mock_pg_combine_backup.return_value.cmd,
                mock_pg_combine_backup.return_value.args,
            )
            mock_command.return_value.assert_called_once_with(
                mock_full_command_quote.return_value
            )

    @pytest.mark.parametrize("staging_location", ["local", "remote"])
    @mock.patch(
        "barman.recovery_executor.CombineOperation._fetch_remote_status",
        return_value={
            "pg_combinebackup_path": "/path/to/pg_combinebackup",
            "pg_combinebackup_version": "17.0.0",
        },
    )
    @mock.patch(
        "barman.recovery_executor.CombineOperation._get_backup_chain_paths",
        return_value=["/path/to/backup_id/data", "/path/to/backup_id/parent/data"],
    )
    @mock.patch("barman.recovery_executor.PgCombineBackup")
    @mock.patch("barman.recovery_executor.Command")
    @mock.patch(
        "barman.recovery_executor.DataTransferFailure", wraps=DataTransferFailure
    )
    def test_run_pg_combinebackup_failed(
        self,
        mock_data_transfer_failure,
        mock_command,
        mock_pg_combine_backup,
        mock_get_backup_chain_paths,
        mock_fetch_remote_status,
        staging_location,
    ):
        """
        Test that :meth:`_run_pg_combinebackup` handles failures correctly.
        """
        # GIVEN a CombineOperation instance
        operation = CombineOperation(
            config=mock.Mock(staging_location=staging_location),
            server=mock.Mock(),
            backup_manager=mock.Mock(),
        )

        # Build the method parameters
        backup_info = mock.Mock()
        backup_info.pg_major_version.return_value = "17"
        destination = "/path/to/destination"
        tablespace_mapping = {
            "/path/to/backup_id/tbs1": "/path/to/relocation/tbs1",
            "/path/to/backup_id/tbs2": "/path/to/relocation/tbs2",
        }
        remote_command = "ssh postgres@pg"

        # If staging is local then it is the PgCombineBackup instance that will
        # raise the exception, otherwise it is the Command instance that will raise it
        if staging_location == "local":
            mock_pg_combine_backup.return_value.side_effect = CommandFailedException(
                "pg_combinebackup failed"
            )
        else:
            mock_command.return_value.side_effect = CommandFailedException(
                "pg_combinebackup failed"
            )

        # WHEN _run_pg_combinebackup is called THEN a DataTransferFailure is raised
        with pytest.raises(DataTransferFailure):
            operation._run_pg_combinebackup(
                backup_info, destination, tablespace_mapping, remote_command
            )

        # AND the exception was raised by calling from_command_error correctly
        msg = "Combine action failure on directory '%s'" % destination
        mock_data_transfer_failure.from_command_error.assert_called_once_with(
            "pg_combinebackup",
            mock.ANY,  # The exception object will vary
            msg,
        )

    @mock.patch(
        "barman.recovery_executor.CombineOperation._fetch_remote_status",
        return_value={
            "pg_combinebackup_path": "/path/to/pg_combinebackup",
            "pg_combinebackup_version": "17.0.0",
        },
    )
    @mock.patch(
        "barman.recovery_executor.CombineOperation._get_backup_chain_paths",
        return_value=["/path/to/backup_id/data", "/path/to/backup_id/parent/data"],
    )
    @mock.patch("barman.recovery_executor.PgCombineBackup")
    @mock.patch("barman.recovery_executor.Command")
    @mock.patch(
        "barman.recovery_executor.DataTransferFailure", wraps=DataTransferFailure
    )
    def test_run_pg_combinebackup_error_out_when_client_differs_backup_pg_version(
        self,
        mock_data_transfer_failure,
        mock_command,
        mock_pg_combine_backup,
        mock_get_backup_chain_paths,
        mock_fetch_remote_status,
        caplog,
    ):
        """
        Test that :meth:`_run_pg_combinebackup` handles failures correctly.
        """
        # GIVEN a CombineOperation instance
        operation = CombineOperation(
            config=mock.Mock(staging_location="remote"),
            server=mock.Mock(),
            backup_manager=mock.Mock(),
        )

        # Build the method parameters
        backup_info = testing_helpers.build_test_backup_info(version=180000)
        destination = "/path/to/destination"
        remote_command = "ssh postgres@pg"

        # WHEN _run_pg_combinebackup is called THEN a DataTransferFailure is raised
        with pytest.raises(SystemExit):
            operation._run_pg_combinebackup(
                backup_info, destination, None, remote_command
            )
        assert (
            "Postgres version mismatch: The backup '1234567890' was taken from "
            "Postgres version '18', but pg_combinebackup version is '17'. To restore "
            "successfully is necessary to use pg_combinebackup with the same version "
            "used when the backup was taken (18)." in caplog.text
        )

    @pytest.mark.parametrize(
        "is_last_operation, tablespaces_relocation, expected_result",
        [
            # Case 1: Last operation, no relocation
            # Tablespaces go from the source backup to their original location on the server
            (
                True,
                None,
                {
                    "/path/to/backup_id/123": "/path/to/server/tbs1",
                    "/path/to/backup_id/321": "/path/to/server/tbs2",
                },
            ),
            # Case 2: Not last operation, no relocation
            # Tablespaces go from the source backup to the volatile backup respective directory
            (
                False,
                None,
                {
                    "/path/to/backup_id/123": "/path/to/vol_backup_id/123",
                    "/path/to/backup_id/321": "/path/to/vol_backup_id/321",
                },
            ),
            # Case 3: Last operation, with relocation
            # Tablespaces go from the source backup to the relocation path
            (
                True,
                {"tbs1": "/path/to/relocation", "tbs2": "/path/to/relocation2"},
                {
                    "/path/to/backup_id/123": "/path/to/relocation",
                    "/path/to/backup_id/321": "/path/to/relocation2",
                },
            ),
            # Case 4: Not last operation, with relocation
            # Tablespaces go from the source backup to the volatile backup respective directory
            (
                False,
                {"tbs1": "/path/to/relocation", "tbs2": "/path/to/relocation2"},
                {
                    "/path/to/backup_id/123": "/path/to/vol_backup_id/123",
                    "/path/to/backup_id/321": "/path/to/vol_backup_id/321",
                },
            ),
        ],
    )
    def test_get_tablespace_mapping(
        self,
        is_last_operation,
        tablespaces_relocation,
        expected_result,
    ):
        """
        Test that :meth:`_get_tablespace_mapping` returns the correct mapping
        for tablespaces. This test covers all cases where the restoring backup
        actually has tablespaces to be mapped.
        """
        # GIVEN a CombineOperation instance
        operation = CombineOperation(
            config=mock.Mock(),
            server=mock.Mock(),
            backup_manager=mock.Mock(),
        )

        # AND a backup_info object with two tablespaces
        backup_info = mock.Mock(
            get_data_directory=lambda oid: f"/path/to/backup_id/{oid}",
            tablespaces=[
                mock.Mock(oid=123, location="/path/to/server/tbs1"),
                mock.Mock(oid=321, location="/path/to/server/tbs2"),
            ],
        )
        # Set names for the tablespaces as we can't pass them as parameters
        # to the mock, e.g. mock.Mock(name="tbs1") does not work
        backup_info.tablespaces[0].name = "tbs1"
        backup_info.tablespaces[1].name = "tbs2"

        # AND the operation's respective volatile backup info object
        vol_backup_info = mock.Mock(
            get_data_directory=lambda oid: f"/path/to/vol_backup_id/{oid}",
        )

        # WHEN _get_tablespace_mapping is called
        ret = operation._get_tablespace_mapping(
            backup_info, vol_backup_info, tablespaces_relocation, is_last_operation
        )

        # THEN the mapping is correct
        assert ret == expected_result

    def test_get_tablespace_mapping_no_tablespaces(self):
        """
        Test that :meth:`_get_tablespace_mapping` returns an empty mapping
        when the restoring backup does not have any tablespaces.
        """
        # GIVEN a CombineOperation instance
        operation = CombineOperation(
            config=mock.Mock(),
            server=mock.Mock(),
            backup_manager=mock.Mock(),
        )

        # AND a backup_info object without tablespaces
        backup_info = mock.Mock(tablespaces=[])

        # AND the operation's respective volatile backup info object
        vol_backup_info = mock.Mock()

        # WHEN _get_tablespace_mapping is called
        ret = operation._get_tablespace_mapping(
            backup_info, vol_backup_info, None, True
        )

        # THEN the mapping is empty
        assert ret == {}

    def test_get_backup_chain_paths(self):
        """
        Test that :meth:`_get_backup_chain_paths` returns the correct paths
        for the backup chain.
        """
        # GIVEN a CombineOperation instance
        operation = CombineOperation(
            config=mock.Mock(),
            server=mock.Mock(),
            backup_manager=mock.Mock(),
        )

        # AND a child backup info with a parent backup
        # We mock the walk_to_root method to return the correct chain of backups
        backup_info = mock.Mock(
            backup_id="child_backup",
            walk_to_root=lambda: [
                mock.Mock(
                    backup_id="child_backup",
                    get_data_directory=lambda: "/path/to/child/data",
                ),
                mock.Mock(
                    backup_id="parent_backup",
                    get_data_directory=lambda: "/path/to/parent/data",
                ),
            ],
        )

        # WHEN _get_backup_chain_paths is called
        ret = operation._get_backup_chain_paths(backup_info)

        # THEN the chain path is correctly returned
        assert list(ret) == ["/path/to/parent/data", "/path/to/child/data"]

    @mock.patch(
        "barman.recovery_executor.PgCombineBackup.get_version_info",
        return_value={
            "full_path": "/usr/bin/pg_combinebackup",
            "full_version": "17.0.0",
        },
    )
    def test_fetch_remote_status_staging_local(self, mock_get_version_info):
        """
        Assert that :meth:`_fetch_remote_status` returns the correct values
        when the staging location is local.
        """
        # GIVEN a CombineOperation instance
        operation = CombineOperation(
            config=mock.Mock(staging_location="local"),
            server=mock.Mock(),
            backup_manager=mock.Mock(),
        )

        # WHEN _fetch_remote_status is called
        ret = operation._fetch_remote_status()

        # THEN it calls PgCombineBackup.get_version_info and returns its result in
        # a slightly different format
        mock_get_version_info.assert_called_once()
        assert ret == {
            "pg_combinebackup_path": "/usr/bin/pg_combinebackup",
            "pg_combinebackup_version": "17.0.0",
        }

    def test_fetch_remote_status_staging_remote(self):
        """
        Assert that :meth:`_fetch_remote_status` returns the correct values
        when the staging location is remote.
        """
        # GIVEN a CombineOperation instance
        operation = CombineOperation(
            config=mock.Mock(staging_location="remote"),
            server=mock.Mock(),
            backup_manager=mock.Mock(),
        )
        # Mock the command to return a valid pg_combinebackup path and version
        operation.cmd = mock.Mock()
        operation.cmd.find_command.return_value = "/usr/bin/pg_combinebackup"
        operation.cmd.get_command_version.return_value = "17.0.0"

        # WHEN _fetch_remote_status is called
        ret = operation._fetch_remote_status()

        # THEN find_command and get_command_version are called correctly
        operation.cmd.find_command.assert_called_once_with(
            PgCombineBackup.COMMAND_ALTERNATIVES
        )
        operation.cmd.get_command_version.assert_called_once_with(
            operation.cmd.find_command.return_value
        )

        # AND it returns the correct status
        assert ret == {
            "pg_combinebackup_path": "/usr/bin/pg_combinebackup",
            "pg_combinebackup_version": "17.0.0",
        }

    @pytest.mark.parametrize("staging_location", ["local", "remote"])
    @mock.patch("barman.recovery_executor.PgCombineBackup")
    def test_fetch_remote_status_fail(self, mock_pg_combinebackup, staging_location):
        """
        Test that :meth:`_fetch_remote_status` raises a :exc:`CommandNotFoundException`
        when ``pg_combinebackup`` is not found, either locally or remotely.
        """
        # GIVEN a CombineOperation instance
        operation = CombineOperation(
            config=mock.Mock(staging_location=staging_location),
            server=mock.Mock(),
            backup_manager=mock.Mock(),
        )
        operation.cmd = mock.Mock()

        # If staging_location is "local", PgCombineBackup.get_version_info
        # is used otherwise cmd.find_command is used
        if staging_location == "local":
            mock_pg_combinebackup.get_version_info.return_value = {
                "full_path": None,
                "full_version": None,
            }
            with pytest.raises(CommandNotFoundException):
                operation._fetch_remote_status()
        else:
            operation.cmd.find_command.return_value = None
            with pytest.raises(CommandNotFoundException):
                operation._fetch_remote_status()


class TestDecryptOperation(object):
    """
    Test suite for the `DecryptOperation` class in the recovery executor.

    This suite verifies the functionality of decrypting an encrypted backup as
    part of a recovery process. It includes tests for:
    - Aborting the operation when a required passphrase is not provided.
    - Successfully executing the decryption process when a passphrase is
      available, ensuring the correct methods are called.
    - The internal `_decrypt_backup` logic, including manifest copying and
      invoking the correct decryption method for each encrypted file.
    - The `_should_execute` logic, which determines if the operation is
      necessary based on the backup's encryption status.
    """

    @mock.patch("barman.recovery_executor.get_passphrase_from_command")
    @mock.patch("barman.recovery_executor.DecryptOperation._prepare_directory")
    def test__execute_returns_volatile(self, prep_dir_mock, mock_passphrase):
        """
        Test that `_execute` returns a `VolatileBackupInfo` object when a
        passphrase is provided.

        This test verifies that the `_execute` method of `DecryptOperation`:
          - Calls the appropriate internal methods (`_prepare_directory`)
          - Logs debug and info messages.
          - Returns an instance of `VolatileBackupInfo` with the correct base directory.

        Mocks are used to isolate the method from its dependencies and to assert that
        the correct calls are made.

        :param _patcher.patch prep_dir_mock: Mock for the `_prepare_directory` method.
        :param _patcher.patch mock_passphrase: Mock for the
            `get_passphrase_from_command` method.
        :raises AssertionError: If the expected methods are not called or the result is
        not as expected.
        """
        backup_info = testing_helpers.build_test_backup_info(
            backup_id="backup-id", server_name="main", encryption="gpg"
        )
        backup_manager = testing_helpers.build_backup_manager(
            main_conf={"backup_options": "concurrent_backup"}
        )
        decrypt_operation = DecryptOperation(
            backup_manager.config, backup_manager.server, backup_manager
        )

        with mock.patch("barman.recovery_executor.output") as output_mock:
            result = decrypt_operation._execute(
                backup_info=backup_info,
                destination="/tmp/dest",
                tablespaces=None,
                remote_command=None,
                recovery_info=None,
                safe_horizon=None,
                is_last_operation=False,
            )
            output_mock.info.assert_called_once()
        prep_dir_mock.assert_has_calls(
            [
                call("/tmp/dest/backup-id/data"),
                call("/tmp/dest/backup-id/16387"),
                call("/tmp/dest/backup-id/16405"),
            ]
        )
        assert isinstance(result, VolatileBackupInfo)
        assert result.get_base_directory() == "/tmp/dest"

    @mock.patch("barman.recovery_executor.DecryptOperation._execute_on_chain")
    def test__execute_calls__decrypt_backup(self, mock_ex_on_chain):
        """
        Test that `DecryptOperation._execute` calls `_decrypt_backup` when a passphrase
        is provided.

        This test verifies that when the `passphrase` argument is supplied to the
        `_execute` method of `DecryptOperation`, the internal `_execute_on_chain` method
        is called with the `_decrypt_backup` method, and the result is returned as
        expected.

        Steps:
            1. Create a test backup info and backup manager.
            2. Instantiate a DecryptOperation.
            3. Patch the `_execute_on_chain` method to track calls.
            4. Call `_execute` with a passphrase.
            5. Assert that `_decrypt_backup` was passed to `_execute_on_chain`.
            6. Assert that the result is as expected.

        :param _patcher.patch mock_ex_on_chain: Mock for the `_execute_on_chain` method
        """
        backup_info = testing_helpers.build_test_backup_info(
            backup_id="backup-id", server_name="main"
        )
        backup_manager = testing_helpers.build_backup_manager(
            main_conf={"backup_options": "concurrent_backup"}
        )
        decrypt_operation = DecryptOperation(
            backup_manager.config, backup_manager.server, backup_manager
        )

        mock_ex_on_chain.return_value = "VOLATILE_BACKUP"

        result = decrypt_operation._execute(
            backup_info=backup_info,
            destination="/tmp/dest",
            tablespaces=None,
            remote_command=None,
            recovery_info=None,
            safe_horizon=None,
            is_last_operation=False,
        )
        # Assert
        mock_ex_on_chain.assert_called_once_with(
            backup_info, "/tmp/dest", decrypt_operation._decrypt_backup
        )
        assert result == "VOLATILE_BACKUP"

    @mock.patch("barman.infofile.LocalBackupInfo.get_data_directory")
    @mock.patch("shutil.copy2")
    @mock.patch("barman.recovery_executor.get_passphrase_from_command")
    @mock.patch("barman.recovery_executor.DecryptOperation._prepare_directory")
    def test__decrypt_backup(
        self,
        mock_prep_dir,
        mock_passphrase,
        mock_cp,
        mock_get_data_dir,
        tmpdir,
    ):
        """
        This test ensures that:
            - The appropriate encryption manager is selected based on the backup's
              encryption type.
            - The backup manifest file is copied to the correct destination directory.
            - The decryption method is invoked for each encrypted file in the backup
              directory, using the correct passphrase.

        Mocks are used to isolate file operations and encryption handling, allowing the
        test to focus on the logic of the decryption process.

        :param _patcher.patch mock_prep_dir: Mock for the directory preparation method.
        :param _patcher.patch mock_passphrase: Mock for retrieving the decryption
            passphrase.
        :param _patcher.patch mock_cp: Mocked `shutil.copy2` function.
        :param py.path.local tmpdir: Temporary directory fixture provided by pytest.
        """
        server = testing_helpers.build_real_server(
            main_conf={
                "local_staging_path": "/tmp",
                "encryption_key_id": "key_id",
                "backup_compression_format": "tar",
                "backup_compression": "none",
                "encryption_passphrase_command": "echo 'test-passphrase'",
            }
        )
        backup_manager = testing_helpers.build_backup_manager(server=server)
        operation = DecryptOperation(
            backup_manager.config, backup_manager.server, backup_manager
        )

        file = tmpdir.join("test_file")
        file.write("")
        mock_backup_info = Mock(
            server=server,
            backup_id="backup_id",
            filename=file,
            encryption="gpg",
            tablespaces=list(
                Tablespace._make(item) for item in (("tbs1", 11892, "/fake/location"),)
            ),
        )

        destination = "/tmp/barman-decryption-random"
        mock_backup_info.get_data_directory.return_value = "default/backup_id/data"
        mock_backup_info.get_directory_entries.return_value = [
            "default/backup_id/data/data.tar.gpg",
            "default/backup_id/data/11892.tar.gpg",
            "default/backup_id/data/backup_manifest",
        ]
        backup_manager.encryption_manager.get_encryption = Mock()

        decrypter = backup_manager.encryption_manager.get_encryption.return_value
        mock_passphrase.return_value = bytearray(b"test-passphrase")
        # Call the method
        operation._decrypt_backup(mock_backup_info, destination)

        backup_manager.encryption_manager.get_encryption.assert_called_once_with(
            mock_backup_info.encryption
        )
        mock_prep_dir.assert_has_calls(
            [call(mock_get_data_dir.return_value), call(mock_get_data_dir.return_value)]
        )
        mock_cp.assert_called_once_with(
            "default/backup_id/data/backup_manifest",
            mock_get_data_dir.return_value,
        )

        decrypter.decrypt.call_count == 2
        decrypter.decrypt.assert_any_call(
            file="default/backup_id/data/data.tar.gpg",
            dest=mock_get_data_dir.return_value,
            passphrase=mock_passphrase.return_value,
        )
        decrypter.decrypt.assert_any_call(
            file="default/backup_id/data/11892.tar.gpg",
            dest=mock_get_data_dir.return_value,
            passphrase=mock_passphrase.return_value,
        )

    @pytest.mark.parametrize(
        "encryption, should_execute", [("gpg", True), (None, False)]
    )
    def test_should_execute_returns_when_encryption_present(
        self, encryption, should_execute
    ):
        """
        Test that `_should_execute` returns the correct value based
        on encryption presence.

        This test verifies that the `_should_execute` method of `DecryptOperation`
        returns True when the `backup_info` object has encryption set to ``gpg``, and
        ``False`` when encryption is ``None``.

        :param str encryption: The encryption type to set on the backup_info mock object.
        :param bool should_execute: The expected boolean result from `_should_execute`.
        """
        backup_info = Mock()
        backup_info.encryption = encryption
        operation = DecryptOperation(Mock(), Mock(), Mock())
        assert operation._should_execute(backup_info) is should_execute

    @pytest.mark.parametrize(
        "staging_location, expect_warning", [("remote", True), ("local", False)]
    )
    @mock.patch("barman.recovery_executor.fs.unix_command_factory")
    @mock.patch("barman.recovery_executor.output")
    def test__get_command_interface(
        self, mock_output, mock_unix_command_factory, staging_location, expect_warning
    ):
        backup_manager = testing_helpers.build_backup_manager(
            main_conf={
                "backup_options": "concurrent_backup",
                "staging_location": staging_location,
            }
        )
        op = DecryptOperation(
            backup_manager.config, backup_manager.server, backup_manager
        )
        remote_command = "ssh user@host"
        result = op._get_command_interface(remote_command)
        mock_unix_command_factory.assert_called_once_with(
            None, backup_manager.server.path
        )
        assert result == mock_unix_command_factory.return_value
        if expect_warning:
            mock_output.warning.assert_called_once_with(
                "'staging_location' is set to 'remote', but decryption requires GPG,"
                "which is configured on the Barman host. For this reason, "
                "decryption will be performed locally, as if 'staging_location' were "
                "set to 'local'. This applies only to decryption, other steps will "
                "still honor the configured 'staging_location'."
            )
        else:
            mock_output.warning.assert_not_called()


class TestMainRecoveryExecutor(object):
    """
    Tests for the :class:`MainRecoveryExecutor` class.
    """

    @pytest.mark.parametrize(
        # Note: we don't test cases where `is_remote_recovery` is False and
        # staging_location is "remote" because this not a valid combination
        # and it's blocked directly in the CLI, so such cases will never
        # arrive to this method. Besides that, every combination is tested
        "is_remote_recovery, staging_location, is_incremental, any_compressed, any_encrypted, expected_operations",
        [
            (
                False,
                "local",
                False,
                False,
                False,
                [RsyncCopyOperation],
            ),
            (
                True,
                "local",
                False,
                False,
                False,
                [RsyncCopyOperation],
            ),
            (
                False,
                "local",
                False,
                False,
                True,
                [DecryptOperation],
            ),
            (
                True,
                "local",
                False,
                False,
                True,
                [DecryptOperation, RsyncCopyOperation],
            ),
            (
                False,
                "local",
                True,
                False,
                False,
                [CombineOperation],
            ),
            (
                True,
                "local",
                True,
                False,
                False,
                [CombineOperation, RsyncCopyOperation],
            ),
            (
                False,
                "local",
                True,
                False,
                True,
                [DecryptOperation, CombineOperation],
            ),
            (
                True,
                "local",
                True,
                False,
                True,
                [DecryptOperation, CombineOperation, RsyncCopyOperation],
            ),
            (
                False,
                "local",
                True,
                True,
                False,
                [DecompressOperation, CombineOperation],
            ),
            (
                True,
                "local",
                True,
                True,
                False,
                [DecompressOperation, CombineOperation, RsyncCopyOperation],
            ),
            (
                False,
                "local",
                True,
                True,
                True,
                [DecryptOperation, DecompressOperation, CombineOperation],
            ),
            (
                True,
                "local",
                True,
                True,
                True,
                [
                    DecryptOperation,
                    DecompressOperation,
                    CombineOperation,
                    RsyncCopyOperation,
                ],
            ),
            (
                False,
                "local",
                False,
                True,
                False,
                [DecompressOperation],
            ),
            (
                True,
                "local",
                False,
                True,
                False,
                [DecompressOperation, RsyncCopyOperation],
            ),
            (
                False,
                "local",
                False,
                True,
                True,
                [DecryptOperation, DecompressOperation],
            ),
            (
                True,
                "local",
                False,
                True,
                True,
                [DecryptOperation, DecompressOperation, RsyncCopyOperation],
            ),
            (
                True,
                "remote",
                False,
                False,
                False,
                [RsyncCopyOperation],
            ),
            (
                True,
                "remote",
                False,
                False,
                True,
                [DecryptOperation, RsyncCopyOperation],
            ),
            (
                True,
                "remote",
                True,
                False,
                False,
                [RsyncCopyOperation, CombineOperation],
            ),
            (
                True,
                "remote",
                True,
                False,
                True,
                [DecryptOperation, RsyncCopyOperation, CombineOperation],
            ),
            (
                True,
                "remote",
                True,
                True,
                False,
                [RsyncCopyOperation, DecompressOperation, CombineOperation],
            ),
            (
                True,
                "remote",
                True,
                True,
                True,
                [
                    DecryptOperation,
                    RsyncCopyOperation,
                    DecompressOperation,
                    CombineOperation,
                ],
            ),
            (
                True,
                "remote",
                False,
                True,
                False,
                [RsyncCopyOperation, DecompressOperation],
            ),
            (
                True,
                "remote",
                False,
                True,
                True,
                [DecryptOperation, RsyncCopyOperation, DecompressOperation],
            ),
        ],
    )
    def test_build_operations_pipeline(
        self,
        is_remote_recovery,
        staging_location,
        is_incremental,
        any_compressed,
        any_encrypted,
        expected_operations,
    ):
        """
        Test that :meth:`_build_operations_pipeline` creates the required operations
        and in the correct order.
        """
        # GIVEN a MainRecoveryExecutor
        mock_backup_manager = testing_helpers.build_backup_manager(
            main_conf={"staging_location": staging_location}
        )

        # AND a backup_info object (it has a parent if it is incremental)
        parent = None
        if is_incremental:
            parent = testing_helpers.build_test_backup_info(
                backup_id="test_backup_id",
                server=mock_backup_manager.server,
                parent_backup_id="parent_backup_id",
            )
        backup_info = testing_helpers.build_test_backup_info(
            backup_id="test_backup_id",
            server=mock_backup_manager.server,
            parent_backup_id=parent.backup_id if parent else None,
            compression="compression_method" if any_compressed else None,
            encryption="encryption_method" if any_encrypted else None,
        )

        # AND a remote command if it is a remote recovery
        remote_command = "ssh postgres@pg" if is_remote_recovery else None

        # WHEN _build_operations_pipeline is called
        executor = MainRecoveryExecutor(mock_backup_manager)
        operations = executor._build_operations_pipeline(backup_info, remote_command)

        # THEN the operations pipeline is built correctly
        assert len(operations) == len(expected_operations)
        for actual_op, expected_op in zip(operations, expected_operations):
            assert isinstance(actual_op, expected_op)

    @pytest.mark.parametrize(
        "staging_path, staging_location, recovery_staging_path, local_staging_path,"
        "operation_cls, remote_command, expected_path, expected_location",
        [
            # Case 1: staging_path and staging_location were set -> the method does nothing
            (
                "/some/staging/path",
                "remote",
                None,
                None,
                CombineOperation,
                "ssh postgres@pg",
                "/some/staging/path",
                "remote",
            ),
            # Case 2: staging_path and staging_location unset and operation is decompress -> maps
            # recovery_staging_path to staging_path and staging_location to "local"
            # (becausse remote_command is unset)
            (
                None,
                None,
                "/some/recovery/staging/path",
                None,
                DecompressOperation,
                None,
                "/some/recovery/staging/path",
                "local",
            ),
            # Case 3: staging_path and staging_location unset and operation is decompress -> maps
            # recovery_staging_path to staging_path and staging_location is always "local"
            (
                None,
                None,
                "/some/recovery/staging/path",
                None,
                DecompressOperation,
                "ssh postgres@pg",
                "/some/recovery/staging/path",
                "local",
            ),
            # Case 4: staging_path and staging_location unset and operation is any other than decompress -> maps
            # local_staging_path to staging_path and staging_location is always "local"
            (
                None,
                None,
                None,
                "/some/local/staging/path",
                CombineOperation,
                None,
                "/some/local/staging/path",
                "local",
            ),
            # Case 5: staging_path and staging_location unset and operation is any other than decompress -> maps
            # local_staging_path to staging_path and staging_location is always "local"
            # (this test is essentially the same as the previous one, but with
            # remote_command set to assert the result is the same)
            (
                None,
                None,
                None,
                "/some/local/staging/path",
                CombineOperation,
                "ssh postgres@pg",
                "/some/local/staging/path",
                "local",
            ),
        ],
    )
    def test_handle_deprecated_staging_options(
        self,
        staging_path,
        staging_location,
        recovery_staging_path,
        local_staging_path,
        operation_cls,
        remote_command,
        expected_path,
        expected_location,
    ):
        """
        Test that :meth:`_handle_deprecated_staging_options` correctly maps deprecated
        staging options to the new ones, during the context of the context manager.
        """
        # GIVEN a MainRecoveryExecutor
        backup_manager = mock.Mock()
        executor = MainRecoveryExecutor(backup_manager)
        executor.config = mock.Mock(
            staging_path=staging_path,
            staging_location=staging_location,
            recovery_staging_path=recovery_staging_path,
            local_staging_path=local_staging_path,
        )

        # Instantiate the operation
        operation = operation_cls(executor.config, None, None)

        # Save original values for later comparison
        orig_staging_path = operation.config.staging_path
        orig_staging_location = operation.config.staging_location

        # WHEN _handle_deprecated_staging_options is called
        with executor._handle_deprecated_staging_options(operation, remote_command):
            # THEN the operation's staging_path and staging_location are changed
            # to the expected values during the context
            assert executor.config.staging_path == expected_path
            assert executor.config.staging_location == expected_location

        # AND after context, values are restored
        assert executor.config.staging_path == orig_staging_path
        assert executor.config.staging_location == orig_staging_location

    @mock.patch(
        "barman.recovery_executor.MainRecoveryExecutor._build_operations_pipeline"
    )
    def test_backup_copy(self, mock_build_pipeline):
        """
        Test that :meth:`_backup_copy` executes the pipeline operations correctly.
        """
        # GIVEN a MainRecoveryExecutor
        backup_manager = testing_helpers.build_backup_manager(
            main_conf={
                "staging_path": "/fake/staging/path",
                "staging_location": "local",
            }
        )
        executor = MainRecoveryExecutor(backup_manager)
        # AND a mock _build_operations_pipeline that returns two mock operations
        op1 = mock.Mock(NAME="operation-1", config=backup_manager.config)
        op2 = mock.Mock(NAME="operation-2", config=backup_manager.config)
        mock_build_pipeline.return_value = [op1, op2]

        # Prepare all the parameters for the _backup_copy method
        backup_info = testing_helpers.build_test_backup_info()
        recovery_destination = "/fake/destination/path"
        tablespaces = mock.Mock()
        remote_command = "ssh postgres@pg"
        safe_horizon = datetime.now()
        recovery_info = {"random": "data"}

        # WHEN _backup_copy is called
        executor._backup_copy(
            backup_info=backup_info,
            dest=recovery_destination,
            tablespaces=tablespaces,
            remote_command=remote_command,
            safe_horizon=safe_horizon,
            recovery_info=recovery_info,
        )

        # THEN the pipeline operations are executed correctly
        # The first operation is executed with the staging directory as its destination
        op1.execute.assert_called_once_with(
            backup_info=backup_info,
            destination=op1.staging_path,
            tablespaces=tablespaces,
            remote_command=remote_command,
            safe_horizon=safe_horizon,
            recovery_info=recovery_info,
            is_last_operation=False,
        )
        # The last operation is executed with the recovery destination as destination
        # and the result of the previous operation as its input for backup_info
        op2.execute.assert_called_once_with(
            backup_info=op1.execute.return_value,
            destination=recovery_destination,
            tablespaces=tablespaces,
            remote_command=remote_command,
            recovery_info=recovery_info,
            safe_horizon=safe_horizon,
            is_last_operation=True,
        )

        op1.cleanup_staging_dir.assert_called_once_with()
        op2.cleanup_staging_dir.assert_not_called()


class TestDecompressOperation(object):
    """
    Test suite for the DecompressOperation class, which handles the decompression
    of backup files during the recovery process in Barman.

    This class contains tests for:
    - Decompression with and without tablespaces.
    - Handling of unexpected compression formats.
    - The logic determining whether decompression should be executed.
    - The integration of decompression within the operation execution chain.
    """

    name = "dummy-decompress"
    file_extension = "tar.dummy"

    @pytest.mark.parametrize(
        "is_last_op, tablespaces",
        [
            (
                True,
                {"tbs1": "/relocate_tbs1", "tbs2": "/relocate_tbs2"},
            ),
            (True, None),
            (
                False,
                {"tbs1": "/relocate_tbs1", "tbs2": "/relocate_tbs2"},
            ),
            (False, None),
        ],
    )
    @mock.patch("barman.recovery_executor.DecompressOperation._link_tablespaces")
    @mock.patch("barman.recovery_executor.DecompressOperation._prepare_directory")
    @mock.patch("barman.recovery_executor.GZipCompression")
    @mock.patch("barman.recovery_executor.output")
    def test_decompress_backup(
        self,
        mock_output,
        mock_gzip,
        mock_prep_dir,
        mock_link_tbs,
        is_last_op,
        tablespaces,
    ):
        """
        Test that the decompression operation correctly handles backups with
        tablespaces and relocation.
        Ensures that the decompress method is called for each tablespace and the base
        tarball, and that appropriate debug messages are logged.
        """
        compressor = mock.Mock()
        compressor.file_extension = "tar.gz"
        compressor.decompress.side_effect = (
            lambda src, dst, exclude=None: f"decompressed {src} to {dst}"
        )
        mock_gzip.return_value = compressor
        mock_gzip.name = "gzip"
        mock_backup_manager = testing_helpers.build_backup_manager()
        config = mock_backup_manager.config
        server = mock_backup_manager.server
        # Arrange
        op = DecompressOperation(config, server, mock_backup_manager)
        op.cmd = mock.Mock()
        backup_info = testing_helpers.build_test_backup_info(compression="gzip")

        # Test with no tbs relocation
        result = op._decompress_backup(
            backup_info=backup_info,
            destination="/dest",
            tablespaces=tablespaces,
            is_last_operation=is_last_op,
        )
        if tablespaces:
            tbs1 = tablespaces["tbs1"]
            tbs2 = tablespaces["tbs2"]
        else:
            tbs1 = "/fake/location"
            tbs2 = "/another/location"
        # Should create all destinations before decompressing
        if is_last_op:
            prep_calls = [
                call("/dest"),
                call(tbs1),
                call(tbs2),
            ]
            decompress_calls = [
                call(
                    "/some/barman/home/main/base/1234567890/data/16387.tar.gz",
                    tbs1,
                ),
                call(
                    "/some/barman/home/main/base/1234567890/data/16405.tar.gz",
                    tbs2,
                ),
                call(
                    "/some/barman/home/main/base/1234567890/data/base.tar.gz",
                    "/dest",
                    exclude=["recovery.conf", "tablespace_map"],
                ),
            ]
            dest = "/dest"
        else:
            prep_calls = [
                call("/dest/1234567890/data"),
                call("/dest/1234567890/16387"),
                call("/dest/1234567890/16405"),
            ]
            decompress_calls = [
                call(
                    "/some/barman/home/main/base/1234567890/data/16387.tar.gz",
                    "/dest/1234567890/16387",
                ),
                call(
                    "/some/barman/home/main/base/1234567890/data/16405.tar.gz",
                    "/dest/1234567890/16405",
                ),
                call(
                    "/some/barman/home/main/base/1234567890/data/base.tar.gz",
                    "/dest/1234567890/data",
                    exclude=["recovery.conf", "tablespace_map"],
                ),
            ]
            dest = result.get_data_directory()
        mock_prep_dir.call_count == 3
        mock_prep_dir.assert_has_calls(prep_calls)
        # Should copy the backup manifest if not the last operation
        if not is_last_op:
            op.cmd.copy.assert_called_once_with(
                backup_info.get_backup_manifest_path(),
                result.get_backup_manifest_path(),
            )
        else:
            op.cmd.copy.assert_not_called()
        # Should link the tablespaces
        mock_link_tbs.assert_called_once_with(result, dest, tablespaces, is_last_op)
        # Should call decompress for each tablespace and for base tarball
        assert compressor.decompress.call_count == 3
        compressor.decompress.assert_has_calls(decompress_calls)
        # Assert
        assert isinstance(result, VolatileBackupInfo)
        # Should log debug messages
        assert mock_output.debug.call_count == 6

    def test_should_execute_true(self):
        """
        Test that _should_execute returns ``True`` when the ``backup_info`` indicates a
        compression format.
        """
        backup_info = mock.Mock()
        backup_info.compression = "gzip"
        op = DecompressOperation(mock.Mock(), mock.Mock(), mock.Mock())
        assert op._should_execute(backup_info) is True

    def test_should_execute_false(self):
        """
        Test that _should_execute returns ``False`` when the ``backup_info`` does not
        indicate a compression format.
        """
        backup_info = mock.Mock()
        backup_info.compression = None
        op = DecompressOperation(mock.Mock(), mock.Mock(), mock.Mock())
        assert op._should_execute(backup_info) is False

    @mock.patch(
        "barman.recovery_executor.DecompressOperation._create_volatile_backup_info"
    )
    def test_decompress_backup_unexpected_compression(self, mock_create_v_bi):
        """
        Test that the decompression operation raises an `AttributeError` when an unexpected
        compression format is encountered.
        """
        backup_info = mock.Mock()
        backup_info.compression = "snappy"
        backup_info.tablespaces = []
        backup_info.get_data_directory.return_value = "/data/dir"
        op = DecompressOperation(mock.Mock(), mock.Mock(), mock.Mock())
        with pytest.raises(
            UnsupportedCompressionFormat, match="Unexpected compression format: snappy"
        ):
            op._decompress_backup(
                backup_info=backup_info,
                destination="/dest",
                tablespaces=None,
                is_last_operation=True,
            )

    @mock.patch("barman.recovery_executor.DecompressOperation._execute_on_chain")
    @mock.patch("barman.recovery_executor.DecompressOperation._prepare_directory")
    def test_decompress_operation__execute_on_chain_calls_decompress_backup(
        self, mock_prep_dir, mock_ex_on_chain
    ):
        """
        Test that the `_execute` method of `DecompressOperation` correctly delegates to
        the `_decompress_backup` method via the operation chain mechanism.
        """
        mock_backup_manager = testing_helpers.build_backup_manager()
        config = mock_backup_manager.config
        server = mock_backup_manager.server
        # Arrange
        op = DecompressOperation(config, server, mock_backup_manager)
        backup_info = Mock()
        backup_info.compression = "gzip"
        backup_info.tablespaces = []
        backup_info.get_data_directory.return_value = "/data/dir"
        destination = "/dest"
        tablespaces = None
        remote_command = None
        recovery_info = None
        safe_horizon = None
        is_last_operation = True

        mock_ex_on_chain.return_value = "VOLATILE_BACKUP"
        # Act
        result = op._execute(
            backup_info,
            destination,
            tablespaces,
            remote_command,
            recovery_info,
            safe_horizon,
            is_last_operation,
        )
        # Assert
        mock_ex_on_chain.assert_called_once_with(
            backup_info, "/dest", op._decompress_backup, None, True
        )
        assert result == "VOLATILE_BACKUP"

    @pytest.mark.parametrize(
        "base_directory",
        ["/some/barman/home/main/base", "/custom/barman/home/main/base"],
    )
    @mock.patch("barman.recovery_executor.DecompressOperation._link_tablespaces")
    @mock.patch("barman.recovery_executor.DecompressOperation._prepare_directory")
    @mock.patch("barman.recovery_executor.output")
    @mock.patch(
        "barman.recovery_executor.DecompressOperation._create_volatile_backup_info"
    )
    @mock.patch("barman.infofile.LocalBackupInfo.walk_to_root")
    def test_execute_on_chain_on_mixed_backups_compressed_and_uncompressed(
        self,
        mock_walk_to_root,
        mock_create_vol_b_info,
        mock_out,
        mock_prep_dir,
        mock_link_tbs,
        base_directory,
        monkeypatch,
    ):
        """
        Test that :meth:`_execute_on_chain` is called with the correct parameters
        when executing on an incremental backup chain with child compressed and parent
        uncompressed.
        """
        mock_backup_manager = testing_helpers.build_backup_manager(
            main_conf={"basebackups_directory": base_directory}
        )
        config = mock_backup_manager.config
        server = mock_backup_manager.server
        # GIVEN a DecompressOperation instance
        operation = DecompressOperation(config, server, mock_backup_manager)
        monkeypatch.setattr(operation, "cmd", mock.Mock())
        # AND a backup info chain
        # Parent uncompressed
        parent_backup_info = testing_helpers.build_test_backup_info(
            backup_id="parent_backup",
        )
        # Child compressed
        backup_info = testing_helpers.build_test_backup_info(
            backup_id="incremental_backup",
            parent_backup_id=parent_backup_info.backup_id,
            compression="gzip",
        )
        mock_walk_to_root.return_value = [backup_info, parent_backup_info]

        # Mock the return values of mock_method as if it were a volatile backup info
        # objects of the backups in the chain
        mock_vol_parent_backup_info = mock.Mock(backup_id=parent_backup_info.backup_id)
        mock_vol_backup_info = mock.Mock(
            backup_id=backup_info.backup_id,
            parent_backup_id=parent_backup_info.backup_id,
        )
        mock_method = mock.Mock()
        mock_method.side_effect = [mock_vol_backup_info, mock_vol_parent_backup_info]
        mock_create_vol_b_info.return_value = mock_vol_parent_backup_info
        destination = "/destination"

        # WHEN _execute_on_chain is called
        ret = operation._execute_on_chain(
            backup_info, destination, mock_method, "arg1", key="value", key2="value2"
        )

        # THEN the method is called only for the child with the correct parameters
        mock_method.assert_called_once_with(
            backup_info, destination, "arg1", key="value", key2="value2"
        )
        mock_prep_dir.assert_called_once_with(destination, delete_if_exists=False)

        if base_directory == parent_backup_info.get_base_directory():
            operation.cmd.copy.assert_called_once_with(
                parent_backup_info.get_basebackup_directory(), destination
            )
        else:
            operation.cmd.move.assert_called_once_with(
                parent_backup_info.get_basebackup_directory(), destination
            )
        # THEN the method _create_volatile_backup_info is called only for the parent
        # with the correct parameters
        mock_create_vol_b_info.assert_called_once_with(parent_backup_info, destination)
        mock_link_tbs.assert_called_once_with(
            mock_create_vol_b_info.return_value,
            mock_create_vol_b_info.return_value.get_data_directory(),
            tablespaces=None,
            is_last_operation=False,
        )
        mock_out.debug.call_count == 2
        mock_out.debug.assert_has_calls(
            [
                call(
                    "Executing %s operation for backup %s.",
                    "barman-decompress",
                    "incremental_backup",
                ),
                call(
                    "Skipping %s operation for backup %s as it's not required",
                    "barman-decompress",
                    "parent_backup",
                ),
            ]
        )
        # AND the chain is correctly remounted on the volatile backup info objects
        assert mock_vol_backup_info.parent_instance is mock_vol_parent_backup_info
        # AND the equivalent volatile backup info of the backup info passed is returned
        assert ret == mock_vol_backup_info

    @pytest.mark.parametrize(
        "failed_cmd, base_directory",
        [
            ["move", "/custom/barman/home/main/base"],
            ["copy", "/some/barman/home/main/base"],
            ["link", "/some/barman/home/main/base"],
            ["prepare_dir", "/some/barman/home/main/base"],
        ],
    )
    @mock.patch("barman.recovery_executor.DecompressOperation._link_tablespaces")
    @mock.patch("barman.recovery_executor.DecompressOperation._prepare_directory")
    @mock.patch("barman.recovery_executor.output", wraps=output)
    @mock.patch(
        "barman.recovery_executor.DecompressOperation._create_volatile_backup_info"
    )
    @mock.patch("barman.infofile.LocalBackupInfo.walk_to_root")
    def test_execute_on_chain_on_mixed_backups_compressed_and_uncompressed_fs_exceptions(
        self,
        mock_walk_to_root,
        mock_create_vol_b_info,
        mock_out,
        mock_prep_dir,
        mock_link_tbs,
        failed_cmd,
        base_directory,
        monkeypatch,
    ):
        """
        Test that :meth:`_execute_on_chain` is called with the correct parameters
        when executing on an incremental backup chain with child compressed and parent
        uncompressed.
        """
        mock_backup_manager = testing_helpers.build_backup_manager(
            main_conf={"basebackups_directory": base_directory}
        )
        config = mock_backup_manager.config
        server = mock_backup_manager.server
        # GIVEN a DecompressOperation instance
        operation = DecompressOperation(config, server, mock_backup_manager)
        monkeypatch.setattr(operation, "cmd", mock.Mock())
        # AND a backup info chain
        # Parent uncompressed
        parent_backup_info = testing_helpers.build_test_backup_info(
            backup_id="parent_backup",
        )
        # Child compressed
        backup_info = testing_helpers.build_test_backup_info(
            backup_id="incremental_backup",
            parent_backup_id=parent_backup_info.backup_id,
            compression="gzip",
        )
        mock_walk_to_root.return_value = [backup_info, parent_backup_info]

        # Mock the return values of mock_method as if it were a volatile backup info
        # objects of the backups in the chain
        mock_vol_parent_backup_info = mock.Mock(backup_id=parent_backup_info.backup_id)
        mock_vol_backup_info = mock.Mock(
            backup_id=backup_info.backup_id,
            parent_backup_id=parent_backup_info.backup_id,
        )
        mock_method = mock.Mock()
        mock_method.side_effect = [mock_vol_backup_info, mock_vol_parent_backup_info]
        mock_create_vol_b_info.return_valuwe = mock_vol_parent_backup_info
        destination = "/destination"

        msg = ""
        if failed_cmd == "prepare_dir":
            msg = "rm execution failed"
            mock_prep_dir.side_effect = FsOperationFailed(msg)
        if failed_cmd == "copy":
            msg = "cp execution failed"
            operation.cmd.copy.side_effect = FsOperationFailed(msg)
        if failed_cmd == "move":
            msg = "mv execution failed"
            operation.cmd.move.side_effect = FsOperationFailed(msg)
        if failed_cmd == "link":
            msg = "unable to initialize tablespace directory"
            mock_link_tbs.side_effect = FsOperationFailed(msg)
        with pytest.raises(SystemExit):
            _ = operation._execute_on_chain(
                backup_info,
                destination,
                mock_method,
                "arg1",
                key="value",
                key2="value2",
            )

        mock_out.error.assert_called_once_with("File system error: %s", msg)


def test_recovery_executor_factory():
    """
    Test that the :func:`recovery_executor_factory` returns the correct executor
    based on the backup type.
    """
    backup_manager = Mock()

    # Case 1: WHEN backup_info has snapshots_info
    backup_info = Mock(snapshots_info=True)
    # THEN it should return a SnapshotRecoveryExecutor
    executor = recovery_executor_factory(backup_manager, backup_info)
    assert isinstance(executor, SnapshotRecoveryExecutor)
    assert executor.backup_manager == backup_manager

    # Case 2: WHEN backup_info does not have snapshots_info
    backup_info.snapshots_info = None
    # THEN it should return a MainRecoveryExecutor
    executor = recovery_executor_factory(backup_manager, backup_info)
    assert isinstance(executor, MainRecoveryExecutor)
    assert executor.backup_manager == backup_manager
