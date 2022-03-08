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

import json
from datetime import datetime, timedelta

import dateutil
import mock
import pytest
from dateutil import tz

import barman.server
from barman.exceptions import (
    CommandFailedException,
    SyncError,
    SyncNothingToDo,
    SyncToBeDeleted,
)
from barman.infofile import BackupInfo, LocalBackupInfo
from barman.lockfile import LockFileBusy
from testing_helpers import (
    build_config_from_dicts,
    build_real_server,
    build_test_backup_info,
)

# expected result of the sync --status command
EXPECTED_MINIMAL = {
    "backups": {
        "1234567890": {
            "end_wal": "000000010000000000000002",
            "size": 12345,
            "server_name": "main",
            "begin_xlog": "0/2000028",
            "deduplicated_size": None,
            "version": 90302,
            "ident_file": "/pgdata/location/pg_ident.conf",
            "end_time": "Wed Jul 23 12:00:43 2014",
            "status": "DONE",
            "backup_id": "1234567890",
            "config_file": "/pgdata/location/postgresql.conf",
            "timeline": 1,
            "end_xlog": "0/20000B8",
            "pgdata": "/pgdata/location",
            "begin_time": "Wed Jul 23 11:00:43 2014",
            "hba_file": "/pgdata/location/pg_hba.conf",
            "end_offset": 184,
            "tablespaces": [
                ["tbs1", 16387, "/fake/location"],
                ["tbs2", 16405, "/another/location"],
            ],
            "begin_wal": "000000010000000000000002",
            "mode": "rsync-exclusive",
            "error": None,
            "begin_offset": 40,
            "backup_label": None,
            "included_files": None,
            "copy_stats": None,
            "xlog_segment_size": 16777216,
            "systemid": None,
        }
    },
    "config": {},
    "last_name": "000000010000000000000005",
    "last_position": 209,
    "wals": [
        {
            "time": 1406019026.0,
            "size": 16777216,
            "compression": None,
            "name": "000000010000000000000002",
        },
        {
            "time": 1406019026.0,
            "size": 16777216,
            "compression": None,
            "name": "000000010000000000000003",
        },
        {
            "time": 1406019329.9300001,
            "size": 16777216,
            "compression": None,
            "name": "000000010000000000000004",
        },
        {
            "time": 1406019330.84,
            "size": 16777216,
            "compression": None,
            "name": "000000010000000000000005",
        },
    ],
    "version": barman.__version__,
}


# noinspection PyMethodMayBeStatic
class TestSync(object):
    """
    Test class for sync module
    """

    def test_set_starting_point(self, tmpdir):
        """
        Test for the set_starting_point method.

        Test the different results of the method:
         * No last_position parameter, only last_wal.
         * last_position parameter and the correct last_wal
         * No last_position and no last_wal
         * Wrong combination of last_position and last_wal

        :param path tmpdir: py.test temporary directory unique to the test
        """
        # build a test xlog.db
        tmp_path = tmpdir.join("xlog.db")
        tmp_path.write(
            "000000010000000000000002\t16777216\t1406019026.0\tNone\n"
            "000000010000000000000003\t16777216\t1406019026.0\tNone\n"
            "000000010000000000000004\t16777216\t1406019329.93\tNone\n"
        )
        tmp_file = tmp_path.open()

        tmp_file.seek(0)
        server = build_real_server()
        # No last_position parameter, only last_wal.
        # Expect the method to set the read point to 0 (beginning of the file)
        result = server.set_sync_starting_point(
            tmp_file, "000000010000000000000002", None
        )
        assert result == 0
        assert tmp_file.tell() == 0

        # last_position parameter and the correct last_wal
        # Expect the method to set the read point to the given last_position
        result = server.set_sync_starting_point(
            tmp_file, "000000010000000000000003", 52
        )
        assert result == 52
        assert tmp_file.tell() == 52

        # No last_position and no last_wal.
        # Expect the method to set the read point to 0
        result = server.set_sync_starting_point(tmp_file, None, None)
        assert result == 0
        assert tmp_file.tell() == 0

        # Wrong combination of last_position and last_wal.
        # Expect the method to set the read point to 0
        result = server.set_sync_starting_point(
            tmp_file, "000000010000000000000004", 52
        )
        assert result == 0
        assert tmp_file.tell() == 0

    def test_status(self, capsys, tmpdir):
        """
        Test the status method.

        Given a test xlog.db expect the method to produce a json output.
        Compare the produced json with the EXPECTED_MINIMAL map

        :param path tmpdir: py.test temporary directory unique to the test
        :param capsys: fixture that allow to access stdout/stderr output
        """
        # Create a test xlog.db
        tmp_path = tmpdir.join("xlog.db")
        tmp_path.write(
            "000000010000000000000001\t16777216\t1406019022.4\tNone\n"
            "000000010000000000000002\t16777216\t1406019026.0\tNone\n"
            "000000010000000000000003\t16777216\t1406019026.0\tNone\n"
            "000000010000000000000004\t16777216\t1406019329.93\tNone\n"
            "000000010000000000000005\t16777216\t1406019330.84\tNone\n"
        )

        # Build a server, replacing some function to use the the tmpdir objects
        server = build_real_server()
        server.xlogdb = lambda: tmp_path.open()
        server.get_available_backups = lambda: {
            "1234567890": build_test_backup_info(
                server=server,
                begin_time=dateutil.parser.parse("Wed Jul 23 11:00:43 2014"),
                end_time=dateutil.parser.parse("Wed Jul 23 12:00:43 2014"),
            )
        }

        # Call the status method capturing the output using capsys
        server.sync_status(None, None)
        (out, err) = capsys.readouterr()
        # prepare the expected results
        # (complex values have to be converted to json)
        expected = dict(EXPECTED_MINIMAL)
        expected["config"] = dict(
            [
                (k, v.to_json() if hasattr(v, "to_json") else v)
                for k, v in server.config.to_json().items()
            ]
        )
        assert json.loads(out) == expected

        # Test that status method raises a SyncError
        # if last_wal is older than the first entry of the xlog.db
        with pytest.raises(SyncError):
            server.sync_status("000000010000000000000000")

        # Test that status method raises a SyncError
        # if last_wal is newer than the last entry of the xlog.db
        with pytest.raises(SyncError):
            server.sync_status("000000010000000000000007")

        # test with an empty file
        tmp_path.write("")
        server.sync_status("000000010000000000000001")
        (out, err) = capsys.readouterr()
        result = json.loads(out)
        assert result["last_position"] == 0
        assert result["last_name"] == ""

    def test_check_sync_required(self):
        """
        Test the behaviour of the check_sync_required method,
        testing all the possible error conditions.
        """
        backup_name = "test_backup_name"
        backups = {"backups": {"test_backup_name": {}}}
        server = build_real_server()
        # Test 1 pass no exception
        server.check_sync_required(backup_name, backups, None)

        # Test 2 backup_name not in backups and no local backup. SyncError
        backup_name = "wrong_test_backup_name"
        with pytest.raises(SyncError):
            server.check_sync_required(backup_name, backups, None)

        # Test 3 backup_name not in backups, and incomplete local
        # copy. Remove partial sync and raise SyncError
        backup_name = "wrong_test_backup_name"
        local_backup_info_mock = build_test_backup_info(
            server=server, status=BackupInfo.FAILED
        )
        with pytest.raises(SyncToBeDeleted):
            server.check_sync_required(backup_name, backups, local_backup_info_mock)

        # Test 4 Local only copy, nothing to do.
        backup_name = "wrong_test_backup_name"
        local_backup_info_mock = build_test_backup_info(
            server=server, status=BackupInfo.DONE
        )
        with pytest.raises(SyncNothingToDo):
            server.check_sync_required(backup_name, backups, local_backup_info_mock)

        # Test 5 already synced backup. Nothing to do.
        backup_name = "test_backup_name"
        local_backup_info_mock = build_test_backup_info(
            server=server, status=BackupInfo.DONE
        )
        with pytest.raises(SyncNothingToDo):
            server.check_sync_required(backup_name, backups, local_backup_info_mock)
        # Test 6 check backup with local retention policies.
        # Case one: Redundancy retention 1
        # Expect "nothing to do"
        backup_name = "test_backup6"
        # build a new server with new configuration that uses retention
        # policies
        server = build_real_server(
            global_conf={
                "retention_policy": "redundancy 1",
                "wal_retention_policy": "main",
            }
        )
        backups = {
            "backups": {
                "test_backup6": build_test_backup_info(
                    server=server, backup_id="test_backup6"
                ).to_json()
            },
            "config": {"name": "test_server"},
        }
        with mock.patch("barman.server.Server.get_available_backups") as bk:
            local_backup_info_mock = None
            bk.return_value = {
                "test_backup5": build_test_backup_info(
                    server=server, backup_id="test_backup5"
                ),
                "test_backup7": build_test_backup_info(
                    server=server, backup_id="test_backup7"
                ),
            }
            with pytest.raises(SyncNothingToDo):
                server.check_sync_required(backup_name, backups, local_backup_info_mock)

        # Test 7 check backup with local retention policies.
        # Case two: Recovery window of 1 day
        # Expect "nothing to do"
        backup_name = "test_backup6"
        # build a new server with new configuration that uses retention
        # policies
        server = build_real_server(
            global_conf={
                "retention_policy": "RECOVERY WINDOW OF 1 day",
                "wal_retention_policy": "main",
            }
        )
        backups = {
            "backups": {
                "test_backup6": build_test_backup_info(
                    server=server,
                    backup_id="test_backup6",
                    begin_time=(datetime.now(tz.tzlocal()) + timedelta(days=4)),
                    end_time=(datetime.now(tz.tzlocal()) - timedelta(days=3)),
                ).to_json()
            },
            "config": {"name": "test_server"},
        }
        with mock.patch("barman.server.Server.get_available_backups") as bk:
            local_backup_info_mock = None
            bk.return_value = {
                "test_backup7": build_test_backup_info(
                    server=server,
                    backup_id="test_backup7",
                    begin_time=(datetime.now(tz.tzlocal()) + timedelta(days=4)),
                    end_time=(datetime.now(tz.tzlocal()) - timedelta(days=3)),
                )
            }
            with pytest.raises(SyncNothingToDo):
                server.check_sync_required(backup_name, backups, local_backup_info_mock)

    def _create_primary_info_file(self, base_tmpdir, backup_dir, tablespaces=True):
        """
        Helper for sync_backup tests which creates a primary info file on disk.
        """
        primary_info_file = backup_dir.join(barman.server.PRIMARY_INFO_FILE)
        remote_basebackup_dir = base_tmpdir.mkdir("primary")
        primary_info_content = dict(EXPECTED_MINIMAL)
        if not tablespaces:
            primary_info_content["backups"]["1234567890"]["tablespaces"] = None
        primary_info_content["config"].update(
            basebackups_directory=str(remote_basebackup_dir)
        )
        primary_info_file.write(json.dumps(primary_info_content))
        return primary_info_content

    @mock.patch("barman.server.RsyncCopyController")
    @mock.patch("barman.server._logger")
    def test_sync_backup(self, logger_mock, rsync_mock, tmpdir, capsys):
        """
        Test the synchronisation method, testing all
        the possible error conditions.

        :param MagicMock logger_mock: MagicMock obj mimicking the logger
        :param MagicMock rsync_mock: MagicMock replacing Rsync class
        :param py.local.path tmpdir: py.test temporary directory
        :param capsys: fixture that allow to access stdout/stderr output
        """
        backup_name = "1234567890"
        server_name = "main"

        # Prepare paths
        backup_dir = tmpdir.mkdir(server_name)
        basebackup_dir = backup_dir.mkdir("base")
        full_backup_path = basebackup_dir.mkdir(backup_name)

        self._create_primary_info_file(tmpdir, backup_dir)

        # Test 1: Not a passive node.
        # Expect SyncError
        server = build_real_server(
            global_conf={"barman_lock_directory": tmpdir.strpath},
            main_conf={"backup_directory": backup_dir.strpath},
        )
        with pytest.raises(SyncError):
            server.sync_backup(backup_name)

        # Test 2: normal sync execution, no error expected.
        # test for all the step on the logger
        logger_mock.reset_mock()
        server = build_real_server(
            global_conf={"barman_lock_directory": tmpdir.strpath},
            main_conf={
                "backup_directory": backup_dir.strpath,
                "primary_ssh_command": "ssh fakeuser@fakehost",
            },
        )
        server.sync_backup(backup_name)
        logger_mock.info.assert_any_call(
            "Synchronising with server %s backup %s: step 1/3: "
            "parse server information",
            server_name,
            backup_name,
        )
        logger_mock.info.assert_any_call(
            "Synchronising with server %s backup %s: step 2/3: file copy",
            server_name,
            backup_name,
        )
        logger_mock.info.assert_any_call(
            "Synchronising with server %s backup %s: step 3/3: finalise sync",
            server_name,
            backup_name,
        )

        # Test 3: test Rsync Failure
        # Expect a BackupInfo object with status "FAILED"
        # and a error message on the "error" field of the obj
        rsync_mock.reset_mock()
        server.backup_manager._backup_cache = {}
        rsync_mock.side_effect = CommandFailedException("TestFailure")
        full_backup_path.remove(rec=1)
        server.sync_backup(backup_name)
        backup_info = server.get_backup(backup_name)
        assert backup_info.status == BackupInfo.FAILED
        assert (
            backup_info.error == "failure syncing server main "
            "backup 1234567890: TestFailure"
        )

        # Test 4: test KeyboardInterrupt management
        # Check the error message for the KeyboardInterrupt event
        rsync_mock.reset_mock()
        rsync_mock.side_effect = CommandFailedException("TestFailure")
        full_backup_path.remove(rec=1)
        rsync_mock.side_effect = KeyboardInterrupt()
        server.sync_backup(backup_name)
        backup_info = server.get_backup(backup_name)
        assert backup_info.status == BackupInfo.FAILED
        assert (
            backup_info.error == "failure syncing server main "
            "backup 1234567890: KeyboardInterrupt"
        )

        # Test 5: test backup name not present on Master server
        # Expect a error message on stderr
        rsync_mock.reset_mock()
        rsync_mock.side_effect = CommandFailedException("TestFailure")
        full_backup_path.remove(rec=1)
        server.sync_backup("wrong_backup_name")

        (out, err) = capsys.readouterr()
        # Check the stderr using capsys. we need only the first line
        # from stderr
        e = err.split("\n")
        assert "ERROR: failure syncing server main backup 1234567890: TestFailure" in e

        # Test 5: Backup already synced
        # Check for the warning message on the stout using capsys
        rsync_mock.reset_mock()
        rsync_mock.side_effect = None
        # do it the first time and check it succeeded
        server.sync_backup(backup_name)
        backup_info = server.get_backup(backup_name)
        assert backup_info.status == BackupInfo.DONE
        # do it again ant test it does not call rsync
        rsync_mock.reset_mock()
        server.sync_backup(backup_name)
        assert not rsync_mock.called
        (out, err) = capsys.readouterr()
        assert out.strip() == "Backup 1234567890 is already synced with main server"

    @mock.patch("barman.server.RsyncCopyController")
    def test_sync_backup_tablespaces(self, rsync_mock, tmpdir):
        """
        Verify that the top level tablespaces are synced but symlinks in pg_tblspc are
        not.
        """
        # GIVEN a backup for server 'main'
        backup_name = "1234567890"
        server_name = "main"

        # WITH a minimal set of files on disk
        backup_dir = tmpdir.mkdir(server_name)
        primary_info_content = self._create_primary_info_file(tmpdir, backup_dir)

        # AND a primary server
        server = build_real_server(
            global_conf={"barman_lock_directory": tmpdir.strpath},
            main_conf={
                "backup_directory": backup_dir.strpath,
                "primary_ssh_command": "ssh fakeuser@fakehost",
            },
        )

        # WHEN sync_backup is called for the backup
        server.sync_backup(backup_name)

        # THEN the data directory of that backup is added to the copy controller
        copy_controller = rsync_mock.return_value
        copy_controller.add_directory.assert_called_once()
        assert "basebackup" == copy_controller.add_directory.call_args_list[0][0][0]
        assert (
            ":%s/%s/"
            % (primary_info_content["config"]["basebackups_directory"], backup_name)
            == copy_controller.add_directory.call_args_list[0][0][1]
        )

        # AND the tablespace symlinks in /data/pg_tblspc are excluded along with
        # the /backup.info and /.backup.lock files AND no other files or directories
        # are excluded
        expected_excludes = [
            "/data/pg_tblspc/16387",
            "/data/pg_tblspc/16405",
            "/backup.info",
            "/.backup.lock",
        ]
        assert set(expected_excludes) == set(
            copy_controller.add_directory.call_args_list[0][1]["exclude_and_protect"]
        )

    @mock.patch("barman.server.RsyncCopyController")
    def test_sync_backup_no_tablespaces(self, rsync_mock, tmpdir):
        """
        Verify that sync_backup works if no tablespaces are present.
        """
        # GIVEN a backup for server 'main'
        backup_name = "1234567890"
        server_name = "main"

        # WITH a minimal set of files on disk
        backup_dir = tmpdir.mkdir(server_name)
        primary_info_content = self._create_primary_info_file(
            tmpdir, backup_dir, tablespaces=False
        )

        # AND a primary server
        server = build_real_server(
            global_conf={"barman_lock_directory": tmpdir.strpath},
            main_conf={
                "backup_directory": backup_dir.strpath,
                "primary_ssh_command": "ssh fakeuser@fakehost",
            },
        )

        # WHEN sync_backup is called for the backup
        server.sync_backup(backup_name)

        # THEN the data directory of that backup is added to the copy controller
        copy_controller = rsync_mock.return_value
        copy_controller.add_directory.assert_called_once()
        assert "basebackup" == copy_controller.add_directory.call_args_list[0][0][0]
        assert (
            ":%s/%s/"
            % (primary_info_content["config"]["basebackups_directory"], backup_name)
            == copy_controller.add_directory.call_args_list[0][0][1]
        )

    @mock.patch("barman.server.Rsync")
    def test_sync_wals(self, rsync_mock, tmpdir, capsys):
        """
        Test the WAL synchronisation method, testing all
        the possible error conditions.

        :param MagicMock rsync_mock: MagicMock replacing Rsync class
        :param py.local.path tmpdir: py.test temporary directory
        :param capsys: fixture that allow to access stdout/stderr output
        """
        server_name = "main"

        # Prepare paths
        barman_home = tmpdir.mkdir("barman_home")
        backup_dir = barman_home.mkdir(server_name)
        wals_dir = backup_dir.mkdir("wals")
        primary_info_file = backup_dir.join(barman.server.PRIMARY_INFO_FILE)

        # prepare the primary_info file
        remote_basebackup_dir = tmpdir.mkdir("primary")
        primary_info_content = dict(EXPECTED_MINIMAL)
        primary_info_content["config"].update(
            compression=None,
            basebackups_directory=str(remote_basebackup_dir),
            wals_directory=str(wals_dir),
        )
        primary_info_file.write(json.dumps(primary_info_content))

        # Test 1: Not a passive node.
        # Expect SyncError
        server = build_real_server(global_conf=dict(barman_home=str(barman_home)))
        with pytest.raises(SyncError):
            server.sync_wals()

        # Test 2: different compression between Master and Passive node.
        # Expect a SyncError
        server = build_real_server(
            global_conf=dict(barman_home=str(barman_home)),
            main_conf=dict(
                compression="gzip", primary_ssh_command="ssh fakeuser@fakehost"
            ),
        )

        server.sync_wals()
        (out, err) = capsys.readouterr()
        assert "Compression method on server %s " % server_name in err

        # Test 3: No base backup for server, exit with warning
        server = build_real_server(
            global_conf=dict(barman_home=str(barman_home)),
            main_conf=dict(
                compression=None,
                wals_directory=str(wals_dir),
                primary_ssh_command="ssh fakeuser@fakehost",
            ),
        )

        server.sync_wals()
        (out, err) = capsys.readouterr()

        assert "WARNING: No base backup for server %s" % server.config.name in err

        # Test 4: No wal synchronisation required, expect a warning

        # set return for get_first_backup and get_backup methods
        server.get_first_backup_id = lambda: "too_new"
        server.get_backup = lambda x: build_test_backup_info(
            server=server,
            begin_wal="000000010000000000000005",
            begin_time=dateutil.parser.parse("Wed Jul 23 11:00:43 2014"),
            end_time=dateutil.parser.parse("Wed Jul 23 12:00:43 2014"),
        )
        server.sync_wals()
        (out, err) = capsys.readouterr()

        assert (
            "WARNING: Skipping WAL synchronisation for "
            "server %s: no available local backup for %s"
            % (server.config.name, primary_info_content["wals"][0]["name"])
            in err
        )

        # Test 6: simulate rsync failure.
        # Expect a custom error message

        server.get_backup = lambda x: build_test_backup_info(
            server=server,
            begin_wal="000000010000000000000002",
            begin_time=dateutil.parser.parse("Wed Jul 23 11:00:43 2014"),
            end_time=dateutil.parser.parse("Wed Jul 23 12:00:43 2014"),
        )
        rsync_mock.side_effect = CommandFailedException("TestFailure")
        server.sync_wals()

        (out, err) = capsys.readouterr()
        # check stdout for the Custom error message
        assert "TestFailure" in err

        # Test 7: simulate keyboard interruption
        rsync_mock.side_effect = KeyboardInterrupt()
        server.sync_wals()
        # control the error message for KeyboardInterrupt
        (out, err) = capsys.readouterr()
        assert "KeyboardInterrupt" in err

        # Test 8: normal execution, expect no output. xlog.db
        # must contain information about the primary info wals

        # reset the rsync_moc, and remove the side_effect
        rsync_mock.reset_mock()
        rsync_mock.side_effect = mock.Mock(name="rsync")

        server.sync_wals()
        # check for no output on stdout and sterr
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""
        # check the xlog content for primary.info wals
        exp_xlog = [
            "000000010000000000000002\t16777216\t1406019026.0\tNone\n",
            "000000010000000000000003\t16777216\t1406019026.0\tNone\n",
            "000000010000000000000004\t16777216\t1406019329.93\tNone\n",
            "000000010000000000000005\t16777216\t1406019330.84\tNone\n",
        ]
        with server.xlogdb() as fxlogdb:
            xlog = fxlogdb.readlines()
            assert xlog == exp_xlog

    def _create_mock_config(self, tmpdir):
        """Helper for passive node tests which returns a mock config object"""
        barman_home = tmpdir.mkdir("barman_home")
        backup_dir = barman_home.mkdir("main")
        wals_dir = backup_dir.mkdir("wals")
        # Build the configuration for the server using
        # a fake configuration object filled with test values
        return build_config_from_dicts(
            global_conf=dict(barman_home=str(barman_home)),
            main_conf=dict(
                compression=None,
                wals_directory=str(wals_dir),
                primary_ssh_command="ssh fakeuser@fakehost",
            ),
        )

    @mock.patch("barman.server.Command")
    @mock.patch("barman.server.BarmanSubProcess")
    def test_passive_node_cron(
        self, subprocess_mock, command_mock, monkeypatch, tmpdir, capsys
    ):
        """
        check the passive node version of cron command

        :param MagicMock subprocess_mock: Mock of
            barman.command_wrappers.BarmanSubProcess
        :param MagicMock command_mock: Mock of
            barman.command_wrappers.Command
        :param monkeypatch monkeypatch: pytest patcher
        :param py.local.path tmpdir: pytest temporary directory
        :param capsys: fixture for reading sysout
        """
        config = self._create_mock_config(tmpdir)
        # We need to setup a server object
        server = barman.server.Server(config.get_server("main"))
        # Make the configuration available through the global namespace
        # (required to invoke a subprocess to retrieve the config file name)
        monkeypatch.setattr(barman, "__config__", config)
        # We need to build a test response from the remote server.
        # We use the out property of the command_mock for
        # returning the test response
        command_mock.return_value.out = json.dumps(EXPECTED_MINIMAL)
        server.cron()
        (out, err) = capsys.readouterr()
        # Assertion block 1: the execution of the cron command for passive
        # node should be successful
        assert "Starting copy of backup" in out
        assert "Started copy of WAL files for server" in out

        # Modify the response of the fake remote call
        primary_info = dict(EXPECTED_MINIMAL)
        primary_info["backups"] = []
        primary_info["wals"] = []
        command_mock.return_value.out = json.dumps(primary_info)
        server.cron()
        (out, err) = capsys.readouterr()
        # Assertion block 2: No backup or wal synchronisation required
        assert "No backup synchronisation required" in out
        assert "No WAL synchronisation required for server" in out

        # Add a backup to the remote response
        primary_info = dict(EXPECTED_MINIMAL)
        backup_info_dict = LocalBackupInfo(server, backup_id="1234567891").to_json()
        primary_info["backups"]["1234567891"] = backup_info_dict
        command_mock.return_value.out = json.dumps(primary_info)
        server.cron()
        (out, err) = capsys.readouterr()
        # Assertion block 3: start the copy the first backup
        # of the list (1234567890),
        # and not the one second one (1234567891)
        assert "Starting copy of backup 1234567890" in out
        assert "Started copy of WAL files for server main" in out
        assert "1234567891" not in out

        # Patch on the fly the Lockfile object, testing the locking
        # management of the method.
        with mock.patch.multiple(
            "barman.server",
            ServerBackupSyncLock=mock.DEFAULT,
            ServerWalSyncLock=mock.DEFAULT,
        ) as lock_mocks:
            for item in lock_mocks:
                lock_mocks[item].side_effect = LockFileBusy()
            primary_info = dict(EXPECTED_MINIMAL)
            primary_info["backups"]["1234567891"] = backup_info_dict
            command_mock.return_value.out = json.dumps(primary_info)
            server.sync_cron(keep_descriptors=False)
            (out, err) = capsys.readouterr()
            assert "A synchronisation process for backup 1234567890" in out
            assert "WAL synchronisation already running" in out

    @mock.patch("barman.server.Command")
    @mock.patch("barman.server.BarmanSubProcess")
    def test_passive_node_forward_config_path(
        self, subprocess_mock, command_mock, monkeypatch, tmpdir
    ):
        """
        Tests that the config file path is used in the primary node command only if
        forward_config_path is set.

        :param MagicMock subprocess_mock: Mock of
            barman.command_wrappers.BarmanSubProcess
        :param MagicMock command_mock: Mock of
            barman.command_wrappers.Command
        :param monkeypatch monkeypatch: pytest patcher
        :param py.local.path tmpdir: pytest temporary directory
        """
        # GIVEN a simple passive node configuration with the default value of
        # forward_config_path
        config = self._create_mock_config(tmpdir)

        # AND barman is invoked with the -c option, simulated here by directly
        # storing the path to the config file in the config_file attribute
        config.config_file = "/path/to/barman.conf"

        # AND a mock barman server which provides successful responses
        server = barman.server.Server(config.get_server("main"))
        monkeypatch.setattr(barman, "__config__", config)
        command = command_mock.return_value
        command.out = json.dumps(EXPECTED_MINIMAL)

        # WHEN cron is executed for the passive server
        server.cron()
        # THEN barman is invoked on the primary node with no -c option
        assert command.call_args_list[0][0][0] == "barman sync-info main"
        # WHEN forward_config_path is set to true for the server and cron is executed
        config.get_server("main").forward_config_path = True
        server.cron()
        # THEN barman is invoked on the primary node with a -c option which provides
        # the path to the barman config file
        assert (
            command.call_args_list[1][0][0]
            == "barman -c /path/to/barman.conf sync-info main"
        )
