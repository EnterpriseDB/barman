# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2014-2022
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

from argparse import ArgumentTypeError

import json
import os
import pytest
import sys
from mock import MagicMock, Mock, patch

import barman.config
from barman import output
from barman.cli import (
    ArgumentParser,
    argument,
    backup,
    check_target_action,
    check_wal_archive,
    command,
    get_server,
    get_server_list,
    manage_server_command,
    OrderedHelpFormatter,
    recover,
    keep,
    show_servers,
)
from barman.exceptions import WalArchiveContentError
from barman.infofile import BackupInfo
from barman.server import Server
from testing_helpers import build_config_dictionary, build_config_from_dicts


# noinspection PyMethodMayBeStatic
class TestCli(object):
    def test_get_server(self, monkeypatch):
        """
        Test the get_server method, providing a basic configuration

        :param monkeypatch monkeypatch: pytest patcher
        """
        # Mock the args from argparse
        args = Mock()
        args.server_name = "main"
        monkeypatch.setattr(
            barman,
            "__config__",
            build_config_from_dicts(
                main_conf={
                    "archiver": "on",
                }
            ),
        )
        server_main = get_server(args)
        # Expect the server to exists
        assert server_main
        # Expect the name to be the right one
        assert server_main.config.name == "main"

    def test_get_server_with_conflicts(self, monkeypatch, capsys):
        """
        Test get_server method using a configuration containing errors

        :param monkeypatch monkeypatch: pytest patcher
        """
        # Mock the args from argparse
        args = Mock()
        # conflicting directories
        monkeypatch.setattr(
            barman,
            "__config__",
            build_config_from_dicts(
                main_conf={
                    "wals_directory": "/some/barman/home/main/wals",
                    "basebackups_directory": "/some/barman/home/main/wals",
                    "archiver": "on",
                }
            ),
        )
        args.server_name = "main"
        with pytest.raises(SystemExit):
            get_server(args, True)
        out, err = capsys.readouterr()
        assert err
        assert "ERROR: Conflicting path:" in err

        # conflicting directories with on_error_stop=False
        monkeypatch.setattr(
            barman,
            "__config__",
            build_config_from_dicts(
                main_conf={
                    "wals_directory": "/some/barman/home/main/wals",
                    "basebackups_directory": "/some/barman/home/main/wals",
                    "archiver": "on",
                }
            ),
        )
        args.server_name = "main"
        get_server(args, on_error_stop=False)
        # In this case the server is returned and a warning message is emitted
        out, err = capsys.readouterr()
        assert err
        assert "ERROR: Conflicting path:" in err

    def test_manage_server_command(self, monkeypatch, capsys):
        """
        Test manage_server_command method checking
        the various types of error output

        :param monkeypatch monkeypatch: pytest patcher
        """
        # Build a server with a config with path conflicts
        monkeypatch.setattr(
            barman,
            "__config__",
            build_config_from_dicts(
                main_conf=build_config_dictionary(
                    {
                        "wals_directory": "/some/barman/home/main/wals",
                        "basebackups_directory": "/some/barman/home/main/wals",
                        "archiver": "on",
                    }
                )
            ),
        )
        server = Server(barman.__config__.get_server("main"))
        # Test a not blocking WARNING message
        manage_server_command(server)
        out, err = capsys.readouterr()
        # Expect an ERROR message because of conflicting paths
        assert "ERROR: Conflicting path" in err

        # Build a server with a config without path conflicts
        monkeypatch.setattr(barman, "__config__", build_config_from_dicts())
        server = Server(barman.__config__.get_server("main"))
        # Set the server as not active
        server.config.active = False
        # Request to treat inactive as errors
        to_be_executed = manage_server_command(server, inactive_is_error=True)
        out, err = capsys.readouterr()
        # Expect a ERROR message because of a not active server
        assert "ERROR: Inactive server" in err
        assert not to_be_executed

        # Request to treat inactive as warning
        to_be_executed = manage_server_command(server, inactive_is_error=False)
        out, err = capsys.readouterr()
        # Expect no error whatsoever
        assert err == ""
        assert not to_be_executed

    def test_get_server_global_error_list(self, monkeypatch, capsys):
        """
        Test the management of multiple servers and the
        presence of global errors

        :param monkeypatch monkeypatch: pytest patcher
        """
        args = Mock()
        args.server_name = "main"
        # Build 2 servers with shared path.
        monkeypatch.setattr(
            barman,
            "__config__",
            build_config_from_dicts(
                global_conf=None,
                main_conf={
                    "basebackups_directory": "/some/barman/home/main/base",
                    "incoming_wals_directory": "/some/barman/home/main/incoming",
                    "wals_directory": "/some/barman/home/main/wals",
                    "backup_directory": "/some/barman/home/main",
                    "archiver": "on",
                },
                test_conf={
                    "basebackups_directory": "/some/barman/home/test/wals",
                    "incoming_wals_directory": "/some/barman/home/main/incoming",
                    "wals_directory": "/some/barman/home/main/wals",
                    "backup_directory": "/some/barman/home/main",
                    "archiver": "on",
                },
            ),
        )
        # Expect a conflict because of the shared paths
        with pytest.raises(SystemExit):
            get_server(args)
        out, err = capsys.readouterr()
        # Check for the presence of error messages
        assert err
        # Check paths in error messages
        assert (
            "Conflicting path: "
            "basebackups_directory=/some/barman/home/main/base" in err
        )
        assert (
            "Conflicting path: "
            "incoming_wals_directory=/some/barman/home/main/incoming" in err
        )
        assert "Conflicting path: wals_directory=/some/barman/home/main/wals" in err
        assert "Conflicting path: backup_directory=/some/barman/home/main" in err

    def test_get_server_list(self, monkeypatch, capsys):
        """
        Test the get_server_list method

        :param monkeypatch monkeypatch: pytest patcher
        """
        monkeypatch.setattr(barman, "__config__", build_config_from_dicts())
        server_dict = get_server_list()
        assert server_dict
        # Expect 2 test servers Main and Test
        assert len(server_dict) == 2
        # Test the method with global errors
        monkeypatch.setattr(
            barman,
            "__config__",
            build_config_from_dicts(
                global_conf=None,
                main_conf={
                    "basebackups_directory": "/some/barman/home/main/base",
                    "incoming_wals_directory": "/some/barman/home/main/incoming",
                    "wals_directory": "/some/barman/home/main/wals",
                    "backup_directory": "/some/barman/home/main",
                    "archiver": "on",
                },
                test_conf={
                    "basebackups_directory": "/some/barman/home/test/wals",
                    "incoming_wals_directory": "/some/barman/home/main/incoming",
                    "wals_directory": "/some/barman/home/main/wals",
                    "backup_directory": "/some/barman/home/main",
                    "archiver": "on",
                },
            ),
        )
        # Expect the method to fail and exit
        with pytest.raises(SystemExit):
            get_server_list()
        out, err = capsys.readouterr()
        # Check for the presence of error messages
        assert err
        # Check paths in error messages
        assert (
            "Conflicting path: "
            "basebackups_directory=/some/barman/home/main/base" in err
        )
        assert (
            "Conflicting path: "
            "incoming_wals_directory=/some/barman/home/main/incoming" in err
        )
        assert "Conflicting path: wals_directory=/some/barman/home/main/wals" in err
        assert "Conflicting path: backup_directory=/some/barman/home/main" in err

    def test_get_server_list_global_error_continue(self, monkeypatch):
        """
        Test the population of the list of global errors for diagnostic
        purposes (diagnose invocation)

        :param monkeypatch monkeypatch: pytest patcher
        """
        monkeypatch.setattr(
            barman,
            "__config__",
            build_config_from_dicts(
                global_conf=None,
                main_conf={
                    "backup_directory": "/some/barman/home/main",
                    "archiver": "on",
                },
                test_conf={
                    "backup_directory": "/some/barman/home/main",
                    "archiver": "on",
                },
            ),
        )
        server_dict = get_server_list(on_error_stop=False)
        global_error_list = barman.__config__.servers_msg_list
        # Check for the presence of servers
        assert server_dict
        # Check for the presence of global errors
        assert global_error_list
        assert len(global_error_list) == 6

    @pytest.fixture
    def mock_backup_info(self):
        backup_info = Mock()
        backup_info.status = BackupInfo.DONE
        backup_info.tablespaces = []
        return backup_info

    @pytest.fixture
    def mock_recover_args(self):
        args = Mock()
        args.backup_id = "20170823T104400"
        args.server_name = "main"
        args.destination_directory = "recovery_dir"
        args.tablespace = None
        args.target_name = None
        args.target_tli = None
        args.target_immediate = None
        args.target_time = None
        args.target_xid = None
        args.target_lsn = None
        return args

    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_recover_multiple_targets(
        self,
        get_server_mock,
        parse_backup_id_mock,
        mock_backup_info,
        mock_recover_args,
        monkeypatch,
        capsys,
    ):
        parse_backup_id_mock.return_value = mock_backup_info

        monkeypatch.setattr(
            barman,
            "__config__",
            build_config_from_dicts(
                main_conf={
                    "archiver": "on",
                }
            ),
        )

        # Testing mutual exclusiveness of target options
        args = mock_recover_args
        args.backup_id = "20170823T104400"
        args.server_name = "main"
        args.destination_directory = "recovery_dir"
        args.target_immediate = True
        args.target_time = "2021-01-001 00:00:00.000"

        with pytest.raises(SystemExit):
            recover(args)

        _, err = capsys.readouterr()
        assert (
            "ERROR: You cannot specify multiple targets for the recovery "
            "operation" in err
        )

    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_recover_one_target(
        self,
        get_server_mock,
        parse_backup_id_mock,
        mock_backup_info,
        mock_recover_args,
        monkeypatch,
        capsys,
    ):
        parse_backup_id_mock.return_value = mock_backup_info

        monkeypatch.setattr(
            barman,
            "__config__",
            build_config_from_dicts(
                main_conf={
                    "archiver": "on",
                }
            ),
        )

        # This parameters are fine
        args = mock_recover_args
        args.backup_id = "20170823T104400"
        args.server_name = "main"
        args.destination_directory = "recovery_dir"
        args.target_action = None

        with pytest.raises(SystemExit):
            recover(args)

        _, err = capsys.readouterr()
        assert "" == err

    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_recover_default_target(
        self,
        get_server_mock,
        parse_backup_id_mock,
        mock_backup_info,
        mock_recover_args,
        monkeypatch,
        capsys,
    ):
        parse_backup_id_mock.return_value = mock_backup_info

        monkeypatch.setattr(
            barman,
            "__config__",
            build_config_from_dicts(
                main_conf={
                    "archiver": "on",
                }
            ),
        )

        # This parameters are fine
        args = mock_recover_args
        args.backup_id = "20170823T104400"
        args.server_name = "main"
        args.destination_directory = "recovery_dir"
        args.target_action = None

        with pytest.raises(SystemExit):
            recover(args)

        _, err = capsys.readouterr()
        assert "" == err

    @pytest.mark.parametrize(
        (
            "recovery_options",
            "get_wal_arg",
            "no_get_wal_arg",
            "expect_get_wal",
        ),
        [
            # WHEN there are no recovery options set
            # AND neither --get-wal nor --no-get-wal are used
            # THEN no get_wal option is expected
            ("", False, False, False),
            # OR --get-wal is not used and --no-get-wal is used
            # THEN no get_wal option is expected
            ("", False, True, False),
            # OR --get-wal is used and --no-get-wal is not used
            # THEN the get_wal option is expected
            ("", True, False, True),
            # WHEN get-wal is set in recovery options
            # AND neither --get-wal nor --no-get-wal are used
            # THEN the get_wal option is expected
            ("get-wal", False, False, True),
            # OR --get-wal is not used and --no-get-wal is used
            # THEN no get_wal option is expected
            ("get-wal", False, True, False),
            # OR --get-wal is used and --no-get-wal is not used
            # THEN the get_wal option is expected
            ("get-wal", True, False, True),
        ],
    )
    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_recover_get_wal(
        self,
        get_server_mock,
        parse_backup_id_mock,
        mock_backup_info,
        mock_recover_args,
        recovery_options,
        get_wal_arg,
        no_get_wal_arg,
        expect_get_wal,
        monkeypatch,
        capsys,
    ):
        # GIVEN a backup
        parse_backup_id_mock.return_value = mock_backup_info
        # AND a configuration with the specified recovery options
        config = build_config_from_dicts(
            global_conf={"recovery_options": recovery_options}
        )
        server = config.get_server("main")
        get_server_mock.return_value.config = server
        monkeypatch.setattr(
            barman,
            "__config__",
            (config,),
        )

        # WHEN the specified --get-wal / --no-get-wal combinations are used
        if get_wal_arg:
            mock_recover_args.get_wal = True
        elif no_get_wal_arg:
            mock_recover_args.get_wal = False
        else:
            del mock_recover_args.get_wal

        # WITH a barman recover command
        with pytest.raises(SystemExit):
            recover(mock_recover_args)

        # THEN then the presence of the get_wal recovery option matches expectations
        if expect_get_wal:
            assert barman.config.RecoveryOptions.GET_WAL in server.recovery_options
        else:
            assert barman.config.RecoveryOptions.GET_WAL not in server.recovery_options

        # AND there are no errors
        _out, err = capsys.readouterr()
        assert "" == err

    def test_check_target_action(self):
        # The following ones must work
        assert None is check_target_action(None)
        assert "pause" == check_target_action("pause")
        assert "promote" == check_target_action("promote")
        assert "shutdown" == check_target_action("shutdown")

        # Every other value is an error
        with pytest.raises(ArgumentTypeError):
            check_target_action("invalid_target_action")


class TestKeepCli(object):
    @pytest.fixture
    def mock_args(self):
        args = Mock()
        args.sever_name = "test_server"
        args.backup_id = "test_backup_id"
        args.release = None
        args.status = None
        args.target = None
        yield args

    @pytest.fixture
    def monkeypatch_config(self, monkeypatch):
        monkeypatch.setattr(
            barman,
            "__config__",
            build_config_from_dicts(),
        )

    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_barman_keep(
        self, mock_get_server, mock_parse_backup_id, mock_args, monkeypatch_config
    ):
        """Verify barman keep command calls keep_backup"""
        mock_args.target = "standalone"
        mock_parse_backup_id.return_value.backup_id = "test_backup_id"
        mock_parse_backup_id.return_value.status = BackupInfo.DONE
        keep(mock_args)
        mock_get_server.return_value.backup_manager.keep_backup.assert_called_once_with(
            "test_backup_id", "standalone"
        )

    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_barman_keep_fails_if_no_target_release_or_status_provided(
        self, mock_get_server, mock_parse_backup_id, mock_args, capsys
    ):
        """
        Verify barman keep command fails if none of --release, --status or --target
        are provided.
        """
        mock_parse_backup_id.return_value.backup_id = "test_backup_id"
        mock_parse_backup_id.return_value.status = BackupInfo.DONE
        with pytest.raises(SystemExit):
            keep(mock_args)
        _out, err = capsys.readouterr()
        assert (
            "one of the arguments -r/--release -s/--status --target is required" in err
        )
        mock_get_server.return_value.backup_manager.keep_backup.assert_not_called()

    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_barman_keep_backup_not_done(
        self,
        mock_get_server,
        mock_parse_backup_id,
        mock_args,
        capsys,
    ):
        """Verify barman keep command will not add keep if backup is not done"""
        mock_args.target = "standalone"
        mock_parse_backup_id.return_value.backup_id = "test_backup_id"
        mock_parse_backup_id.return_value.status = BackupInfo.WAITING_FOR_WALS
        with pytest.raises(SystemExit):
            keep(mock_args)
        _out, err = capsys.readouterr()
        assert (
            "Cannot add keep to backup test_backup_id because it has status "
            "WAITING_FOR_WALS. Only backups with status DONE can be kept."
        ) in err
        mock_get_server.return_value.backup_manager.keep_backup.assert_not_called()

    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_barman_keep_release(
        self, mock_get_server, mock_parse_backup_id, mock_args, monkeypatch_config
    ):
        """Verify `barman keep --release` command calls release_keep"""
        mock_parse_backup_id.return_value.backup_id = "test_backup_id"
        mock_args.release = True
        keep(mock_args)
        mock_get_server.return_value.backup_manager.release_keep.assert_called_once_with(
            "test_backup_id"
        )

    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_barman_keep_status(
        self,
        mock_get_server,
        mock_parse_backup_id,
        mock_args,
        monkeypatch_config,
        capsys,
    ):
        """Verify `barman keep --status` command prints get_keep_target output"""
        mock_parse_backup_id.return_value.backup_id = "test_backup_id"
        mock_get_server.return_value.backup_manager.get_keep_target.return_value = (
            "standalone"
        )
        mock_args.status = True
        keep(mock_args)
        mock_get_server.return_value.backup_manager.get_keep_target.assert_called_once_with(
            "test_backup_id"
        )
        out, _err = capsys.readouterr()
        assert "standalone" in out

    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_barman_keep_status_nokeep(
        self,
        mock_get_server,
        mock_parse_backup_id,
        mock_args,
        monkeypatch_config,
        capsys,
    ):
        """Verify `barman keep --status` command prints get_keep_target output"""
        mock_parse_backup_id.return_value.backup_id = "test_backup_id"
        mock_get_server.return_value.backup_manager.get_keep_target.return_value = None
        mock_args.status = True
        keep(mock_args)
        mock_get_server.return_value.backup_manager.get_keep_target.assert_called_once_with(
            "test_backup_id"
        )
        out, _err = capsys.readouterr()
        assert "nokeep" in out


class TestCliHelp(object):
    """
    Verify the help output of the ArgumentParser constructed by cli.py

    Checks that the cli ArgumentParser correctly expands the subcommand help and
    prints the subcommands and positional args in alphabetical order.

    This is achieved by creating a minimal argument parser and checking the
    print_help() output matches our expected output.
    """

    _expected_help_output = """usage: %s [-h] [-t] {another-test-command,test-command} ...

positional arguments:
  {another-test-command,test-command}
    another-test-command
                        Another test docstring also readable in expanded help
    test-command        Test docstring which should be readable in expanded
                        help

optional arguments:
  -h, --help            show this help message and exit
  -t, --test-arg        Test command arg

test epilog string
""" % os.path.basename(
        sys.argv[0]
    )

    @pytest.fixture
    def minimal_parser(self):
        parser = ArgumentParser(
            epilog="test epilog string", formatter_class=OrderedHelpFormatter
        )
        parser.add_argument(
            "-t", "--test-arg", help="Test command arg", action="store_true"
        )
        subparsers = parser.add_subparsers(dest="barman.cli.command")

        @command(
            [
                argument(
                    "--test-subcommand-arg", help="subcommand arg", action="store_true"
                )
            ],
            subparsers,
        )
        def test_command(args=None):
            """Test docstring which should be readable in expanded help"""
            pass

        @command([], subparsers)
        def another_test_command(args=None):
            """Another test docstring also readable in expanded help"""
            pass

        yield parser

    def test_help_output(self, minimal_parser, capsys):
        """Check the help output matches the expected help output"""
        minimal_parser.print_help()
        out, err = capsys.readouterr()
        assert "" == err
        assert self._expected_help_output == out


class TestCheckWalArchiveCli(object):
    @pytest.fixture
    def mock_args(self):
        args = Mock()
        args.sever_name = "test_server"
        args.timeline = None
        yield args

    @patch("barman.cli.check_archive_usable")
    @patch("barman.cli.get_server")
    def test_barman_check_wal_archive_no_args(
        self, mock_get_server, mock_check_archive_usable, mock_args
    ):
        """Verify barman check-wal-archive command calls xlog.check_archive_usable."""
        mock_get_server.return_value.xlogdb.return_value.__enter__.return_value = [
            "000000010000000000000001        0       0       gzip",
            "000000010000000000000002        0       0       gzip",
        ]
        check_wal_archive(mock_args)
        mock_check_archive_usable.assert_called_once_with(
            ["000000010000000000000001", "000000010000000000000002"],
            timeline=None,
        )

    @patch("barman.cli.check_archive_usable")
    @patch("barman.cli.get_server")
    def test_barman_check_wal_archive_args(
        self, mock_get_server, mock_check_archive_usable, mock_args
    ):
        """Verify args passed to xlog.check_archive_usable."""
        mock_get_server.return_value.xlogdb.return_value.__enter__.return_value = [
            "000000010000000000000001        0       0       gzip",
            "000000010000000000000002        0       0       gzip",
        ]
        mock_args.timeline = 2
        check_wal_archive(mock_args)
        mock_check_archive_usable.assert_called_once_with(
            ["000000010000000000000001", "000000010000000000000002"],
            timeline=2,
        )

    @patch("barman.cli.check_archive_usable")
    @patch("barman.cli.get_server")
    def test_barman_check_wal_archive_content_error(
        self, mock_get_server, mock_check_archive_usable, mock_args, caplog
    ):
        """Verify barman check-wal-archive command calls xlog.check_archive_usable."""
        mock_get_server.return_value.config.name = "test_server"
        mock_get_server.return_value.xlogdb.return_value.__enter__.return_value = []
        mock_check_archive_usable.side_effect = WalArchiveContentError("oh dear")
        with pytest.raises(SystemExit) as exc:
            check_wal_archive(mock_args)
        assert 1 == exc.value.code
        assert "WAL archive check failed for server test_server: oh dear" in caplog.text


class TestShowServersCli(object):
    """Verify output of show-servers command."""

    test_server_name = "test_server"

    @pytest.fixture
    def mock_args(self):
        args = Mock()
        args.server_name = self.test_server_name
        yield args

    @pytest.fixture
    def mock_config(self):
        mock_config = MagicMock()
        mock_config.name = self.test_server_name
        mock_config.retention_policy = None
        mock_config.last_backup_maximum_age = None
        yield mock_config

    @pytest.mark.parametrize(
        ("active", "disabled", "expected_description"),
        [
            # No description for active servers
            (True, False, ""),
            # Inactive servers are described as inactive
            (False, False, " (inactive)"),
            # Disabled servers are described as disabled
            (True, True, " (WARNING: disabled)"),
        ],
    )
    @patch("barman.server.ProcessManager")
    @patch("barman.cli.get_server_list")
    def test_show_servers_plain(
        self,
        mock_get_server_list,
        _mock_process_manager,
        mock_config,
        mock_args,
        active,
        disabled,
        expected_description,
        monkeypatch,
        capsys,
    ):
        # GIVEN a config with the specified active and disabled booleans
        mock_config.active = active
        mock_config.disabled = disabled
        # AND a server using that config
        server = Server(mock_config)
        mock_server_list = {self.test_server_name: server}
        mock_get_server_list.return_value = mock_server_list

        # WHEN the output format is console
        # monkeypatch(output._writer = output.AVAILABLE_WRITERS["console"]()
        monkeypatch.setattr(
            barman.output, "_writer", output.AVAILABLE_WRITERS["console"]()
        )
        with pytest.raises(SystemExit):
            # AND barman show-servers runs
            show_servers(mock_args)

        # THEN nothing is sent to stderr
        out, err = capsys.readouterr()
        assert "" == err

        # AND the command output includes the description and server name
        assert "%s%s:" % (self.test_server_name, expected_description) in out

    @pytest.mark.parametrize(
        ("active", "disabled", "expected_description"),
        [
            # No description for active servers
            (True, False, None),
            # Inactive servers are described as inactive
            (False, False, "(inactive)"),
            # Disabled servers are described as disabled
            (True, True, "(WARNING: disabled)"),
        ],
    )
    @patch("barman.server.ProcessManager")
    @patch("barman.cli.get_server_list")
    def test_show_servers_json(
        self,
        mock_get_server_list,
        _mock_process_manager,
        mock_config,
        mock_args,
        active,
        disabled,
        expected_description,
        monkeypatch,
        capsys,
    ):
        # GIVEN a config with the specified active and disabled booleans
        mock_config.active = active
        mock_config.disabled = disabled
        # AND a server using that config
        server = Server(mock_config)
        mock_server_list = {self.test_server_name: server}
        mock_get_server_list.return_value = mock_server_list

        # WHEN the output format is json
        # output._writer = output.AVAILABLE_WRITERS["json"]()
        monkeypatch.setattr(
            barman.output, "_writer", output.AVAILABLE_WRITERS["json"]()
        )
        with pytest.raises(SystemExit):
            # AND barman show-servers runs
            show_servers(mock_args)

        # THEN nothing is sent to stderr
        out, err = capsys.readouterr()
        assert "" == err

        # AND the description is available in the description field
        json_output = json.loads(out)
        assert [self.test_server_name] == list(json_output.keys())
        assert json_output[self.test_server_name]["description"] == expected_description


class TestBackupCli(object):
    """Verify argument handling of the backup command."""

    test_server_name = "test_server"

    @pytest.fixture
    def mock_args(self):
        args = Mock()
        args.server_name = self.test_server_name
        yield args

    @patch("barman.cli.manage_server_command")
    @patch("barman.cli.get_server_list")
    def test_compression_backup_method_postgres(
        self, mock_get_server_list, _mock_manage_server_command, mock_args
    ):
        """Verify compression argument is set on the server"""
        # GIVEN a server with backup_method = postgres
        mock_server = MagicMock()
        mock_server.config.backup_method = "postgres"
        # mock_server.config.backup_method = "postgres"
        mock_get_server_list.return_value = {mock_args.server_name: mock_server}

        # WHEN barman backup is called with a supported compression
        mock_args.compression_type = "gzip"
        with pytest.raises(SystemExit) as exc:
            backup(mock_args)

        # THEN the backup_compression server config property is set
        assert "gzip" == mock_server.config.backup_compression

        # AND the backup method of the server is called
        mock_get_server_list.return_value[
            self.test_server_name
        ].backup.assert_called_once()

    @patch("barman.cli.manage_server_command")
    @patch("barman.cli.get_server_list")
    def test_compression_backup_method_rsync(
        self, mock_get_server_list, _mock_manage_server_command, mock_args, capsys
    ):
        # GIVEN a server with backup_method = rsync
        mock_server = MagicMock()
        mock_server.config.backup_method = "rsync"
        mock_get_server_list.return_value = {mock_args.server_name: mock_server}

        # WHEN barman backup is called with a supported compression
        mock_args.compression_type = "gzip"
        with pytest.raises(SystemExit) as exc:
            backup(mock_args)

        # THEN an error message is printed
        _out, err = capsys.readouterr()
        assert (
            "The compression option is only supported with the following "
            "backup methods: postgres" in err
        )

        # AND the backup method of the server is not called
        mock_get_server_list.return_value[
            self.test_server_name
        ].backup.assert_not_called()

    @patch("barman.cli.manage_server_command")
    @patch("barman.cli.get_server_list")
    def test_compression_level(
        self, mock_get_server_list, _mock_manage_server_command, mock_args
    ):
        """Verify compression level is set on the server"""
        # GIVEN a server with backup_method = postgres
        mock_server = MagicMock()
        mock_server.config.backup_method = "postgres"
        # mock_server.config.backup_method = "postgres"
        mock_get_server_list.return_value = {mock_args.server_name: mock_server}

        # WHEN barman backup is called with a supported compression
        mock_args.compression_type = "gzip"
        # AND a compression level is specified
        mock_args.compression_level = 9
        with pytest.raises(SystemExit) as exc:
            backup(mock_args)

        # THEN the backup_compression_level server config property is set
        assert 9 == mock_server.config.backup_compression_level

        # AND the backup method of the server is called
        mock_get_server_list.return_value[
            self.test_server_name
        ].backup.assert_called_once()
