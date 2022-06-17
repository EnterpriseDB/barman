# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2022
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
import time
from contextlib import closing

import dateutil
import mock
import pytest
from mock import MagicMock

import testing_helpers
from barman import xlog
from barman.exceptions import (
    CommandFailedException,
    RecoveryInvalidTargetException,
    RecoveryStandbyModeException,
    RecoveryTargetActionException,
)
from barman.infofile import BackupInfo, WalFileInfo
from barman.recovery_executor import (
    Assertion,
    RecoveryExecutor,
    TarballRecoveryExecutor,
    ConfigurationFileMangeler,
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

    def test_analyse_temporary_config_files(self, tmpdir):
        """
        Test the method that identifies dangerous options into
        the configuration files
        """
        # Build directory/files structure for testing
        tempdir = tmpdir.mkdir("tempdir")
        recovery_info = {
            "configuration_files": ["postgresql.conf", "postgresql.auto.conf"],
            "tempdir": tempdir.strpath,
            "temporary_configuration_files": [],
            "results": {"changes": [], "warnings": []},
        }
        postgresql_conf = tempdir.join("postgresql.conf")
        postgresql_auto = tempdir.join("postgresql.auto.conf")
        postgresql_conf.write(
            "archive_command = something\n"
            "data_directory = something\n"
            "include = something\n"
            'include "without braces"'
        )
        postgresql_auto.write(
            "archive_command = something\n" "data_directory = something"
        )
        recovery_info["temporary_configuration_files"].append(postgresql_conf.strpath)
        recovery_info["temporary_configuration_files"].append(postgresql_auto.strpath)
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

        # Test corner case with empty auto file
        recovery_info["results"]["changes"] = []
        recovery_info["results"]["warnings"] = []
        recovery_info["auto_conf_append_lines"] = ["l1", "l2"]
        postgresql_auto.write("")
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

    @mock.patch("barman.recovery_executor.RsyncPgData")
    def test_setup(self, rsync_mock):
        """
        Test the method that set up a recovery
        """
        backup_info = testing_helpers.build_test_backup_info()
        backup_manager = testing_helpers.build_backup_manager()
        executor = RecoveryExecutor(backup_manager)
        backup_info.version = 90300

        # setup should create a temporary directory
        # and teardown should delete it
        ret = executor._setup(backup_info, None, "/tmp")
        assert os.path.exists(ret["tempdir"])
        executor.close()
        assert not os.path.exists(ret["tempdir"])
        assert ret["wal_dest"].endswith("/pg_xlog")

        # no postgresql.auto.conf on version 9.3
        ret = executor._setup(backup_info, None, "/tmp")
        executor.close()
        assert "postgresql.auto.conf" not in ret["configuration_files"]

        # Check the present for postgresql.auto.conf on version 9.4
        backup_info.version = 90400
        ret = executor._setup(backup_info, None, "/tmp")
        executor.close()
        assert "postgresql.auto.conf" in ret["configuration_files"]

        # Receive a error if the remote command is invalid
        with pytest.raises(SystemExit):
            executor.server.path = None
            executor._setup(backup_info, "invalid", "/tmp")

        # Test for PostgreSQL 10
        backup_info.version = 100000
        ret = executor._setup(backup_info, None, "/tmp")
        executor.close()
        assert ret["wal_dest"].endswith("/pg_wal")

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
            recovery_info, backup_info, dest.strpath, "", "", "", "", "", False, None
        )
        # Test with empty values (no PITR)
        assert recovery_info["target_epoch"] is None
        assert recovery_info["target_datetime"] is None
        assert recovery_info["wal_dest"] == wal_dest.strpath

        # Test for PITR targets
        executor._set_pitr_targets(
            recovery_info,
            backup_info,
            dest.strpath,
            "target_name",
            "2015-06-03 16:11:03.71038+02",
            "2",
            None,
            "",
            False,
            None,
        )
        target_datetime = dateutil.parser.parse("2015-06-03 16:11:03.710380+02:00")
        target_epoch = time.mktime(target_datetime.timetuple()) + (
            target_datetime.microsecond / 1000000.0
        )

        assert recovery_info["target_datetime"] == target_datetime
        assert recovery_info["target_epoch"] == target_epoch
        assert recovery_info["wal_dest"] == dest.join("barman_wal").strpath

        # Test for too early PITR target
        with pytest.raises(RecoveryInvalidTargetException) as exc_info:
            executor._set_pitr_targets(
                recovery_info,
                backup_info,
                dest.strpath,
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
            # WHEN target_tli is 2 we expect no target timeline in the output
            # AND we expect that `is_pitr` is not set
            (2, False, None),
            # WHEN target_tli is 3 we expect target timeline 3 in the output
            # AND we expect that `is_pitr` is set
            (3, True, 3),
            # WHEN target_tli is current we expect no target timeline in the output
            # AND we expect that `is_pitr` is not set
            ("current", False, None),
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

    @mock.patch("barman.recovery_executor.RsyncPgData")
    def test_generate_recovery_conf_pre12(self, rsync_pg_mock, tmpdir):
        """
        Test the generation of recovery.conf file
        """
        # Build basic folder/files structure
        recovery_info = {
            "configuration_files": ["postgresql.conf", "postgresql.auto.conf"],
            "tempdir": tmpdir.strpath,
            "results": {"changes": [], "warnings": []},
            "get_wal": False,
        }
        backup_info = testing_helpers.build_test_backup_info()
        dest = tmpdir.mkdir("destination")

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
            "2015-06-03 16:11:03.71038+02",
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
        assert recovery_conf["recovery_end_command"] == "'rm -fr barman_wal'"
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

    @mock.patch("barman.recovery_executor.RsyncPgData")
    def test_generate_recovery_conf(self, rsync_pg_mock, tmpdir):
        """
        Test the generation of recovery configuration
        :type tmpdir: py.path.local
        """
        # Build basic folder/files structure
        recovery_info = {
            "configuration_files": ["postgresql.conf", "postgresql.auto.conf"],
            "tempdir": tmpdir.strpath,
            "results": {"changes": [], "warnings": []},
            "get_wal": False,
        }
        backup_info = testing_helpers.build_test_backup_info(
            version=120000,
        )
        dest = tmpdir.mkdir("destination")

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
        # check for contents
        assert "recovery_end_command" in pg_auto_conf
        assert "recovery_target_time" in pg_auto_conf
        assert "recovery_target_timeline" in pg_auto_conf
        assert "recovery_target_xid" not in pg_auto_conf
        assert "recovery_target_lsn" not in pg_auto_conf
        assert "recovery_target_name" in pg_auto_conf
        assert "recovery_target" in pg_auto_conf
        assert pg_auto_conf["recovery_end_command"] == "'rm -fr barman_wal'"
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

    @mock.patch("barman.backup.CompressionManager")
    @mock.patch("barman.recovery_executor.RsyncPgData")
    def test_recover_xlog(self, rsync_pg_mock, cm_mock, tmpdir):
        """
        Test the recovery of the xlogs of a backup
        :param rsync_pg_mock: Mock rsync object for the purpose if this test
        """
        # Build basic folders/files structure
        dest = tmpdir.mkdir("destination")
        wals = tmpdir.mkdir("wals")
        # Create 3 WAL files with different compressions
        xlog_dir = wals.mkdir(xlog.hash_dir("000000000000000000000002"))
        xlog_plain = xlog_dir.join("000000000000000000000001")
        xlog_gz = xlog_dir.join("000000000000000000000002")
        xlog_bz2 = xlog_dir.join("000000000000000000000003")
        xlog_plain.write("dummy content")
        xlog_gz.write("dummy content gz")
        xlog_bz2.write("dummy content bz2")
        server = testing_helpers.build_real_server(
            main_conf={"wals_directory": wals.strpath}
        )
        # Prepare compressors mock
        c = {
            "gzip": mock.Mock(name="gzip"),
            "bzip2": mock.Mock(name="bzip2"),
        }
        cm_mock.return_value.get_compressor = lambda compression=None: c[compression]
        # touch destination files to avoid errors on cleanup
        c["gzip"].decompress.side_effect = lambda src, dst: open(dst, "w")
        c["bzip2"].decompress.side_effect = lambda src, dst: open(dst, "w")
        # Build executor
        executor = RecoveryExecutor(server.backup_manager)

        # Test: local copy
        required_wals = (
            WalFileInfo.from_xlogdb_line("000000000000000000000001\t42\t43\tNone\n"),
            WalFileInfo.from_xlogdb_line("000000000000000000000002\t42\t43\tgzip\n"),
            WalFileInfo.from_xlogdb_line("000000000000000000000003\t42\t43\tbzip2\n"),
        )
        executor._xlog_copy(required_wals, dest.strpath, None)
        # Check for a correct invocation of rsync using local paths
        rsync_pg_mock.assert_called_once_with(
            network_compression=False, bwlimit=None, path=None, ssh=None
        )
        assert not rsync_pg_mock.return_value.from_file_list.called
        c["gzip"].decompress.assert_called_once_with(xlog_gz.strpath, mock.ANY)
        c["bzip2"].decompress.assert_called_once_with(xlog_bz2.strpath, mock.ANY)

        # Reset mock calls
        rsync_pg_mock.reset_mock()
        c["gzip"].reset_mock()
        c["bzip2"].reset_mock()

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
            ],
            mock.ANY,
            mock.ANY,
        )
        c["gzip"].decompress.assert_called_once_with(xlog_gz.strpath, mock.ANY)
        c["bzip2"].decompress.assert_called_once_with(xlog_bz2.strpath, mock.ANY)

    def test_prepare_tablespaces(self, tmpdir):
        """
        Test tablespaces preparation for recovery
        """
        # Prepare basic directory/files structure
        dest = tmpdir.mkdir("destination")
        wals = tmpdir.mkdir("wals")
        backup_info = testing_helpers.build_test_backup_info(
            tablespaces=[("tbs1", 16387, "/fake/location")]
        )
        # build an executor
        server = testing_helpers.build_real_server(
            main_conf={"wals_directory": wals.strpath}
        )
        executor = RecoveryExecutor(server.backup_manager)
        # use a mock as cmd obj
        cmd_mock = mock.Mock()
        executor._prepare_tablespaces(backup_info, cmd_mock, dest.strpath, {})
        cmd_mock.create_dir_if_not_exists.assert_any_call(
            dest.join("pg_tblspc").strpath
        )
        cmd_mock.create_dir_if_not_exists.assert_any_call("/fake/location")
        cmd_mock.delete_if_exists.assert_called_once_with(
            dest.join("pg_tblspc").join("16387").strpath
        )
        cmd_mock.create_symbolic_link.assert_called_once_with(
            "/fake/location", dest.join("pg_tblspc").join("16387").strpath
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
                "delete_barman_wal": False,
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
            "target_epoch": None,
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
                "delete_barman_wal": False,
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
            "target_epoch": None,
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
                    "IMPORTANT: The backup we have recovered IS NOT "
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


class TestRecoveryExecutorFactory(object):
    @pytest.mark.parametrize(
        ("compression", "expected_executor", "should_error"),
        [
            # No compression should return RecoveryExecutor
            (None, RecoveryExecutor, False),
            # Supported compression should return TarballRecoveryExecutor
            ("gzip", TarballRecoveryExecutor, False),
            # Unrecognised compression should cause an error
            ("snappy", None, True),
        ],
    )
    def test_recovery_executor_factory(
        self, compression, expected_executor, should_error
    ):
        mock_backup_manager = mock.Mock()
        mock_command = mock.Mock()

        # WHEN recovery_executor_factory is called with the specified compression
        # THEN if an error is expected we see an error
        if should_error:
            with pytest.raises(AttributeError):
                recovery_executor_factory(
                    mock_backup_manager, mock_command, compression
                )
        # OR the expected type of recovery executor is returned
        else:
            executor = recovery_executor_factory(
                mock_backup_manager, mock_command, compression
            )
            assert type(executor) is expected_executor


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
