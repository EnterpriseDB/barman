# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2014-2023
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
import os
import sys
from argparse import ArgumentTypeError

import pytest
from mock import MagicMock, Mock, patch
from testing_helpers import (
    build_config_dictionary,
    build_config_from_dicts,
    build_mocked_server,
    build_real_server,
    build_test_backup_info,
)

import barman.config
from barman import output
from barman.cli import (
    ArgumentParser,
    OrderedHelpFormatter,
    argument,
    backup,
    check_target_action,
    check_wal_archive,
    command,
    config_switch,
    generate_manifest,
    get_model,
    get_models_list,
    get_server,
    get_server_list,
    keep,
    manage_model_command,
    manage_server_command,
    parse_backup_id,
    receive_wal,
    replication_status,
    restore,
    show_servers,
)
from barman.exceptions import WalArchiveContentError
from barman.infofile import BackupInfo
from barman.server import Server


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

    def test_get_server_inactive(self, monkeypatch):
        """
        Test that get_server correctly handles inactive servers.
        """
        # GIVEN an inactive server
        args = Mock()
        monkeypatch.setattr(
            barman, "__config__", build_config_from_dicts(main_conf={"active": "false"})
        )

        # WHEN get_server is called with skip_inactive=True
        # THEN a SystemExit is raised
        args.server_name = "main"
        with pytest.raises(SystemExit):
            get_server(args, skip_inactive=True)

        # AND WHEN get_server is called with skip_inactive=False
        # THEN a server is returned
        args.server_name = "main"
        assert get_server(args, skip_inactive=False) is not None

    @pytest.mark.parametrize(
        (
            "with_wal_streaming",
            "wal_streaming_conninfo",
            "wal_conninfo",
            "expected_streaming_conninfo",
            "expected_conninfo",
        ),
        (
            # If wal_streaming = False, regular conninfo and streaming_conninfo
            (False, "ws_conninfo", "w_conninfo", "s_conninfo", "conninfo"),
            # If wal_streaming_conninfo is not set then regular conninfo and
            # streaming_conninfo
            (True, None, None, "s_conninfo", "conninfo"),
            # If wal_streaming_conninfo is set then conninfo and streaming_conninfo
            # are overridden
            (True, "ws_conninfo", "w_conninfo", "ws_conninfo", "w_conninfo"),
            # If wal_streaming_conninfo is set and wal_conninfo is unset then
            # wal_streaming_conninfo is used for conninfo
            (True, "ws_conninfo", None, "ws_conninfo", "ws_conninfo"),
            # If wal_streaming_conninfo is not set then conninfo and streaming_conninfo
            # are not overridden even if wal_conninfo is set
            (True, None, "w_conninfo", "s_conninfo", "conninfo"),
        ),
    )
    @patch("barman.cli.manage_server_command")
    def test_get_server_wal_streaming(
        self,
        _manage_server_command,
        with_wal_streaming,
        wal_streaming_conninfo,
        wal_conninfo,
        expected_streaming_conninfo,
        expected_conninfo,
        monkeypatch,
    ):
        """
        Test that get_server will return servers configured for WAL streaming
        purposes, that is the streaming_conninfo and conninfo values are replaced
        with WAL-specifc versions.
        """
        # GIVEN a server with the specified conninfo strings
        monkeypatch.setattr(
            barman,
            "__config__",
            build_config_from_dicts(
                global_conf=None,
                main_conf={
                    "streaming_conninfo": "s_conninfo",
                    "conninfo": "conninfo",
                    "wal_streaming_conninfo": wal_streaming_conninfo,
                    "wal_conninfo": wal_conninfo,
                },
            ),
        )
        # WHEN we create the server via barman.cli.get_server
        server = get_server(Mock(server_name="main"), wal_streaming=with_wal_streaming)
        # THEN the configuration has the expected streaming_conninfo and conninfo values
        assert server.config.streaming_conninfo == expected_streaming_conninfo
        assert server.config.conninfo == expected_conninfo

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

    @pytest.mark.parametrize(
        (
            "with_wal_streaming",
            "wal_streaming_conninfo",
            "wal_conninfo",
            "expected_streaming_conninfo",
            "expected_conninfo",
        ),
        (
            # If wal_streaming = False, regular conninfo and streaming_conninfo
            (False, "ws_conninfo", "w_conninfo", "s_conninfo", "conninfo"),
            # If wal_streaming_conninfo is not set then regular conninfo and
            # streaming_conninfo
            (True, None, None, "s_conninfo", "conninfo"),
            # If wal_streaming_conninfo is set then conninfo and streaming_conninfo
            # are overridden
            (True, "ws_conninfo", "w_conninfo", "ws_conninfo", "w_conninfo"),
            # If wal_streaming_conninfo is set and wal_conninfo is unset then
            # wal_streaming_conninfo is used for conninfo
            (True, "ws_conninfo", None, "ws_conninfo", "ws_conninfo"),
            # If wal_streaming_conninfo is not set then conninfo and streaming_conninfo
            # are not overridden even if wal_conninfo is set
            (True, None, "w_conninfo", "s_conninfo", "conninfo"),
        ),
    )
    def test_get_server_list_wal_streaming(
        self,
        with_wal_streaming,
        wal_streaming_conninfo,
        wal_conninfo,
        expected_streaming_conninfo,
        expected_conninfo,
        monkeypatch,
    ):
        """
        Test that get_server_list will return servers configured for WAL streaming
        purposes, that is the streaming_conninfo and conninfo values are replaced
        with WAL-specifc versions.
        """
        # GIVEN a server with the specified conninfo strings
        monkeypatch.setattr(
            barman,
            "__config__",
            build_config_from_dicts(
                global_conf=None,
                main_conf={
                    "streaming_conninfo": "s_conninfo",
                    "conninfo": "conninfo",
                    "wal_streaming_conninfo": wal_streaming_conninfo,
                    "wal_conninfo": wal_conninfo,
                },
            ),
        )
        # WHEN we create the server via barman.cli.get_server_list
        server_dict = get_server_list(wal_streaming=with_wal_streaming)
        # THEN the configuration has the expected streaming_conninfo and conninfo values
        assert (
            server_dict["main"].config.streaming_conninfo == expected_streaming_conninfo
        )
        assert server_dict["main"].config.conninfo == expected_conninfo

    def test_get_model(self, monkeypatch):
        """
        Test the get_model method, providing a basic configuration

        :param monkeypatch monkeypatch: pytest patcher
        """
        # Mock the args from argparse
        args = Mock()
        args.model_name = "main:model"
        monkeypatch.setattr(
            barman,
            "__config__",
            build_config_from_dicts(
                with_model=True,
            ),
        )
        model_main = get_model(args)
        # Expect the model to exists
        assert model_main
        # Expect the name to be the right one
        assert model_main.name == "main:model"

    @pytest.mark.parametrize("model", [None, MagicMock()])
    @patch("barman.cli.output")
    def test_manage_model_command(self, mock_output, model):
        """Test :func:`manage_model_command`.

        Ensure it returns the expected result and log the expected message.
        """
        expected = model is not None

        assert manage_model_command(model, "SOME_MODEL") == expected

        if model is None:
            mock_output.error.assert_called_once_with(
                "Unknown model '%s'" % "SOME_MODEL"
            )

    def test_get_models_list_invalid_args(self):
        """Test :func:`get_models_list`.

        Ensure an :exc:`AssertionError` is thrown when calling with invalid args.
        """
        mock_args = Mock(model_name="SOME_MODEL")

        with pytest.raises(AssertionError):
            get_models_list(mock_args)

    def test_get_models_list_none_args(self, monkeypatch):
        """Test :func:`get_models_list`.

        Ensure the call brings all models when ``args`` is ``None``.
        """
        monkeypatch.setattr(
            barman, "__config__", build_config_from_dicts(with_model=True)
        )
        # we only have the ``main:model`` model by default
        model_list = get_models_list()
        assert len(model_list) == 1
        assert list(model_list.keys())[0] == "main:model"
        assert isinstance(model_list["main:model"], barman.config.ModelConfig)

    def test_get_models_list_valid_args(self, monkeypatch):
        """Test :func:`get_models_list`.

        Ensure it brings a list with the requested models if ``args`` is given.
        """
        monkeypatch.setattr(
            barman, "__config__", build_config_from_dicts(with_model=True)
        )

        mock_args = Mock(model_name=["main:model", "SOME_MODEL"])
        # we only have the ``main:model`` model by default, so ``SOME_MODEL``
        # should be ``None``
        model_list = get_models_list(mock_args)
        assert len(model_list) == 2
        assert sorted(list(model_list.keys())) == ["SOME_MODEL", "main:model"]
        assert isinstance(model_list["main:model"], barman.config.ModelConfig)
        assert model_list["SOME_MODEL"] is None

    @pytest.fixture
    def mock_backup_info(self):
        backup_info = Mock()
        backup_info.status = BackupInfo.DONE
        backup_info.tablespaces = []
        backup_info.compression = None
        backup_info.parent_backup_id = None
        return backup_info

    @pytest.fixture
    def mock_restore_args(self):
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
        args.recovery_staging_path = None
        args.local_staging_path = None
        return args

    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_restore_multiple_targets(
        self,
        get_server_mock,
        parse_backup_id_mock,
        mock_backup_info,
        mock_restore_args,
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
        args = mock_restore_args
        args.backup_id = "20170823T104400"
        args.server_name = "main"
        args.destination_directory = "recovery_dir"
        args.target_immediate = True
        args.target_time = "2021-01-001 00:00:00.000"

        with pytest.raises(SystemExit):
            restore(args)

        _, err = capsys.readouterr()
        assert (
            "ERROR: You cannot specify multiple targets for the recovery "
            "operation" in err
        )

    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_restore_one_target(
        self,
        get_server_mock,
        parse_backup_id_mock,
        mock_backup_info,
        mock_restore_args,
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
        args = mock_restore_args
        args.backup_id = "20170823T104400"
        args.server_name = "main"
        args.destination_directory = "recovery_dir"
        args.target_action = None

        with pytest.raises(SystemExit):
            restore(args)

        _, err = capsys.readouterr()
        assert "" == err

    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_restore_default_target(
        self,
        get_server_mock,
        parse_backup_id_mock,
        mock_backup_info,
        mock_restore_args,
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
        args = mock_restore_args
        args.backup_id = "20170823T104400"
        args.server_name = "main"
        args.destination_directory = "recovery_dir"
        args.target_action = None

        with pytest.raises(SystemExit):
            restore(args)

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
    def test_restore_get_wal(
        self,
        get_server_mock,
        parse_backup_id_mock,
        mock_backup_info,
        mock_restore_args,
        recovery_options,
        get_wal_arg,
        no_get_wal_arg,
        expect_get_wal,
        monkeypatch,
        capsys,
    ):
        # GIVEN a backup
        parse_backup_id_mock.return_value = mock_backup_info
        mock_backup_info.is_incremental = False
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
            mock_restore_args.get_wal = True
        elif no_get_wal_arg:
            mock_restore_args.get_wal = False
        else:
            del mock_restore_args.get_wal

        # WITH a barman recover command
        with pytest.raises(SystemExit):
            restore(mock_restore_args)

        # THEN then the presence of the get_wal recovery option matches expectations
        if expect_get_wal:
            assert barman.config.RecoveryOptions.GET_WAL in server.recovery_options
        else:
            assert barman.config.RecoveryOptions.GET_WAL not in server.recovery_options

        # AND there are no errors
        _out, err = capsys.readouterr()
        assert "" == err

    @pytest.mark.parametrize(
        (
            "backup_is_compressed",
            "recovery_staging_path_arg",
            "recovery_staging_path_config",
            "expected_recovery_staging_path",
            "should_error",
            "error_substring",
        ),
        [
            # If a backup is not compressed then recovery_staging_path is ignored
            (False, None, None, None, False, None),
            # If a backup is compressed and no recovery_staging_path is provided
            # we expect an error
            (
                True,
                None,
                None,
                None,
                True,
                "backup is compressed with gzip compression but no recovery staging "
                "path is provided.",
            ),
            # If a backup is compressed and an argument is provided then it should
            # be set in the config
            (True, "/from/arg", None, "/from/arg", False, None),
            # If a backup is compressed and a bad argument is provided then it should
            # error
            (
                True,
                "from/arg",
                None,
                None,
                True,
                "Cannot parse recovery staging path: Invalid value : 'from/arg' (must "
                "be an absolute path)",
            ),
            # If a backup is compressed and a config value is set then it should
            # be set in the config
            (True, None, "/from/conf", "/from/conf", False, None),
            # If a backup is compressed and both arg and config are set then arg
            # takes precedence
            (True, "/from/arg", "/from/conf", "/from/arg", False, None),
        ],
    )
    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_restore_recovery_staging_path(
        self,
        get_server_mock,
        parse_backup_id_mock,
        mock_backup_info,
        mock_restore_args,
        backup_is_compressed,
        recovery_staging_path_arg,
        recovery_staging_path_config,
        expected_recovery_staging_path,
        should_error,
        error_substring,
        monkeypatch,
        capsys,
    ):
        # GIVEN a backup
        parse_backup_id_mock.return_value = mock_backup_info
        # AND the backup is not incremental
        mock_backup_info.is_incremental = False
        # AND the backup has the specified compression
        mock_backup_info.compression = backup_is_compressed and "gzip" or None
        # AND a configuration with the specified recovery_staging_path
        config = build_config_from_dicts(
            global_conf={"recovery_staging_path": recovery_staging_path_config},
        )
        server = config.get_server("main")
        get_server_mock.return_value.config = server
        monkeypatch.setattr(
            barman,
            "__config__",
            (config,),
        )
        # WHEN recover is called with the specified --recovery-staging-path
        mock_restore_args.recovery_staging_path = recovery_staging_path_arg

        # WITH a barman recover command
        with pytest.raises(SystemExit):
            restore(mock_restore_args)

        # THEN if we expected an error the error was observed
        _, err = capsys.readouterr()
        errors = [msg for msg in err.split("\n") if msg.startswith("ERROR: ")]
        if should_error:
            assert len(err) > 0
            assert any([error_substring in msg for msg in errors])
        else:
            assert len(errors) == 0
            # AND if we expected success, the server config recovery staging
            # path matches expectations
            assert server.recovery_staging_path == expected_recovery_staging_path

    @pytest.mark.parametrize(
        (
            "backup_is_incremental",
            "local_staging_path_arg",
            "local_staging_path_config",
            "expected_local_staging_path",
            "should_error",
            "error_substring",
        ),
        [
            # If a backup is not incremental then local_staging_path is ignored
            (False, None, None, None, False, None),
            # If a backup is incremental and no local_staging_path is provided
            # we expect an error
            (
                True,
                None,
                None,
                None,
                True,
                "backup will be combined with pg_combinebackup in the barman host but "
                "no local staging path is provided.",
            ),
            # If a backup is incremental and an argument is provided then it should
            # be set in the config
            (True, "/from/arg", None, "/from/arg", False, None),
            # If a backup is incremental and a bad argument is provided then it should
            # error
            (
                True,
                "from/arg",
                None,
                None,
                True,
                "Cannot parse local staging path: Invalid value : 'from/arg' (must "
                "be an absolute path)",
            ),
            # If a backup is incremental and a config value is set then it should
            # be set in the config
            (True, None, "/from/conf", "/from/conf", False, None),
            # If a backup is incremental and both arg and config are set then arg
            # takes precedence
            (True, "/from/arg", "/from/conf", "/from/arg", False, None),
        ],
    )
    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_restore_local_staging_path(
        self,
        get_server_mock,
        parse_backup_id_mock,
        mock_backup_info,
        mock_restore_args,
        backup_is_incremental,
        local_staging_path_arg,
        local_staging_path_config,
        expected_local_staging_path,
        should_error,
        error_substring,
        monkeypatch,
        capsys,
    ):
        # GIVEN a backup
        parse_backup_id_mock.return_value = mock_backup_info
        # AND the backup is incremental
        mock_backup_info.is_incremental = backup_is_incremental
        # AND a configuration with the specified local_staging_path
        config = build_config_from_dicts(
            global_conf={"local_staging_path": local_staging_path_config},
        )
        server = config.get_server("main")
        get_server_mock.return_value.config = server
        monkeypatch.setattr(
            barman,
            "__config__",
            (config,),
        )
        # WHEN recover is called with the specified --local-staging-path
        mock_restore_args.local_staging_path = local_staging_path_arg

        # WITH a barman recover command
        with pytest.raises(SystemExit):
            restore(mock_restore_args)

        # THEN if we expected an error the error was observed
        _, err = capsys.readouterr()
        errors = [msg for msg in err.split("\n") if msg.startswith("ERROR: ")]
        if should_error:
            assert len(err) > 0
            assert any([error_substring in msg for msg in errors])
        else:
            assert len(errors) == 0
            # AND if we expected success, the server config recovery staging
            # path matches expectations
            assert server.local_staging_path == expected_local_staging_path

    @pytest.mark.parametrize(
        ("status", "should_error"),
        [
            (BackupInfo.DONE, False),
            (BackupInfo.WAITING_FOR_WALS, False),
            (BackupInfo.FAILED, True),
            (BackupInfo.EMPTY, True),
            (BackupInfo.SYNCING, True),
            (BackupInfo.STARTED, True),
        ],
    )
    @patch("barman.output.error")
    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_restore_backup_status(
        self,
        get_server_mock,
        parse_backup_id_mock,
        error_mock,
        status,
        should_error,
        mock_restore_args,
    ):

        server = build_mocked_server(name="test_server")

        get_server_mock.return_value = server

        backup_info = build_test_backup_info(
            server=server,
            backup_id="test_backup_id",
            status=status,
        )

        parse_backup_id_mock.return_value = backup_info
        mock_restore_args.backup_id = "test_backup_id"
        mock_restore_args.snapshot_recovery_instance = None

        with pytest.raises(
            SystemExit,
        ):
            restore(mock_restore_args)

        if should_error:
            error_mock.assert_called_once_with(
                "Cannot restore from backup '%s' of server "
                "'%s': backup status is not DONE",
                "test_backup_id",
                "test_server",
            )
        else:
            error_mock.assert_not_called()

    @pytest.mark.parametrize(
        (
            "snapshots_info",
            "snapshot_recovery_args",
            "extra_recovery_args",
            "error_message",
        ),
        (
            # If there is no snapshot_info but snapshot args are used then there should
            # be an error
            (
                None,
                {
                    "snapshot_recovery_instance": "test_instance",
                },
                {},
                (
                    "Backup backup_id is not a snapshot backup but the following "
                    "snapshot arguments have been used: --snapshot-recovery-instance"
                ),
            ),
            # If there is snapshot_info but no snapshot args then there should be an
            # error
            (
                Mock(snapshots=[]),
                {},
                {},
                (
                    "Backup backup_id is a snapshot backup and the following required "
                    "arguments have not been provided: --snapshot-recovery-instance"
                ),
            ),
            # If there is snapshot_info, snapshot args and also tablespace mappings
            # then there should be an error
            (
                Mock(snapshots=[]),
                {
                    "snapshot_recovery_instance": "test_instance",
                },
                {"tablespace": ("tbs1:/path/to/tbs1",)},
                (
                    "Backup backup_id is a snapshot backup therefore tablespace "
                    "relocation rules cannot be used"
                ),
            ),
            # If there is snapshot_info and snapshot args then there should not be an
            # error
            (
                Mock(snapshots=[]),
                {
                    "snapshot_recovery_instance": "test_instance",
                },
                {},
                None,
            ),
        ),
    )
    @patch("barman.cli.get_server")
    @patch("barman.cli.parse_backup_id")
    def test_restore_snapshots(
        self,
        parse_backup_id_mock,
        get_server_mock,
        mock_backup_info,
        mock_restore_args,
        snapshots_info,
        snapshot_recovery_args,
        extra_recovery_args,
        error_message,
        capsys,
    ):
        # GIVEN a backup with the specified snapshots_info
        mock_backup_info.snapshots_info = snapshots_info
        mock_backup_info.backup_id = "backup_id"
        mock_backup_info.tablespaces.append(Mock())
        mock_backup_info.tablespaces[-1].name = "tbs1"
        parse_backup_id_mock.return_value = mock_backup_info
        # AND the specified additional recovery args
        mock_restore_args.snapshot_recovery_instance = None
        extra_recovery_args.update(snapshot_recovery_args)
        for k, v in extra_recovery_args.items():
            setattr(mock_restore_args, k, v)

        # WHEN barman recover is called
        with pytest.raises(SystemExit):
            restore(mock_restore_args)

        # THEN if we expected an error the error was observed
        server = get_server_mock.return_value
        _, err = capsys.readouterr()
        if error_message:
            assert error_message in err
            # AND recover was not called
            server.recover.assert_not_called()
        else:
            # AND if we expected success, the server's recover method was called
            server.recover.assert_called_once()
            # AND the snapshot arguments were passed
            assert (
                server.recover.call_args_list[0][1]["recovery_instance"]
                == snapshot_recovery_args["snapshot_recovery_instance"]
            )

    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_restore_recovery_instance_kwarg_not_passed(
        self, get_server_mock, parse_backup_id_mock, mock_backup_info, mock_restore_args
    ):
        """
        Verifies that recovery_instance is not passed to server.recover for
        non-snapshot recoveries.
        """
        # GIVEN a regular non-snapshot basebackup
        mock_backup_info.snapshots_info = None
        parse_backup_id_mock.return_value = mock_backup_info
        # AND the args do not specify a recovery instance
        mock_restore_args.snapshot_recovery_instance = None
        # AND the args do not specify any other snapshot provider options
        mock_restore_args.azure_resource_group = None
        mock_restore_args.gcp_zone = None

        # WHEN barman recover is called
        with pytest.raises(SystemExit):
            restore(mock_restore_args)

        # THEN recover was called once
        get_server_mock.return_value.recover.assert_called_once()
        # AND recovery_instance was not a keyword argument
        assert (
            "recovery_instance"
            not in get_server_mock.return_value.recover.call_args_list[0][1]
        )

    @pytest.mark.parametrize(
        ("arg", "arg_alias"),
        (
            ("gcp_zone", "snapshot_recovery_zone"),
            ("azure_resource_group", None),
            ("aws_region", None),
        ),
    )
    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_restore_snapshot_provider_args(
        self,
        get_server_mock,
        parse_backup_id_mock,
        mock_backup_info,
        mock_restore_args,
        arg,
        arg_alias,
    ):
        """
        Verifies that snapshot provider args override the server config variables.
        """
        # GIVEN a server config
        config = get_server_mock.return_value.config
        # AND the specified arg is set to an initial value in the config
        initial_value = "initial"
        setattr(config, arg, initial_value)
        # AND the backup being recovered is a snapshot backup
        mock_backup_info.snapshots_info = Mock(snapshots=[])
        parse_backup_id_mock.return_value = mock_backup_info

        # WHEN recover is called without overriding the config
        setattr(mock_restore_args, arg, None)
        if arg_alias is not None:
            setattr(mock_restore_args, arg_alias, None)
        with pytest.raises(SystemExit):
            restore(mock_restore_args)
        # THEN the config value is unchanged
        assert getattr(config, arg) == initial_value

        # WHEN recover is called with the override argument
        updated_value = "updated"
        setattr(mock_restore_args, arg, updated_value)
        with pytest.raises(SystemExit):
            restore(mock_restore_args)
        # THEN the config value is updated
        assert getattr(config, arg) == updated_value

        # WHEN recover is called with the alias
        final_value = "final"
        if arg_alias is not None:
            setattr(mock_restore_args, arg_alias, final_value)
            setattr(mock_restore_args, arg, None)
            with pytest.raises(SystemExit):
                restore(mock_restore_args)
            # THEN the config value is updated
            assert getattr(config, arg) == final_value

    def test_check_target_action(self):
        # The following ones must work
        assert None is check_target_action(None)
        assert "pause" == check_target_action("pause")
        assert "promote" == check_target_action("promote")
        assert "shutdown" == check_target_action("shutdown")

        # Every other value is an error
        with pytest.raises(ArgumentTypeError):
            check_target_action("invalid_target_action")

    @pytest.mark.parametrize(
        ("config_value", "arg_value", "expected_value"),
        [
            # If args is not set then we expect the config value to be set
            (False, None, False),
            (True, None, True),
            # If args is False then it should override the config value
            (False, False, False),
            (True, False, False),
            # If args is True then it should override the config value
            (False, True, True),
            (True, True, True),
        ],
    )
    @patch("barman.server.Server.backup")
    @patch("barman.cli.get_server_list")
    def test_backup_immediate_checkpoint(
        self,
        mock_get_server_list,
        _mock_server_backup,
        config_value,
        arg_value,
        expected_value,
        capsys,
    ):
        """
        Verifies that the immediate_checkpoint flag is set on the postgres
        connection.
        """
        # GIVEN a server with immediate_checkpoint set in the config
        server_name = "test server"
        mock_config = MagicMock(
            name=server_name,
            immediate_checkpoint=config_value,
            retention_policy=None,
            primary_ssh_command=None,
            disabled=False,
            barman_lock_directory="/path/to/lockdir",
            backup_compression=None,
        )
        server = Server(mock_config)
        mock_server_list = {server_name: server}
        mock_get_server_list.return_value = mock_server_list

        # WHEN backup is called with the immediate_checkpoint arg
        mock_args = Mock(server_name=server_name, backup_id=None)
        if arg_value is not None:
            mock_args.immediate_checkpoint = arg_value
        else:
            # OR WHEN backup is called with no immediate_checkpoint arg
            del mock_args.immediate_checkpoint
        with pytest.raises(SystemExit):
            backup(mock_args)

        # THEN the config and the postgres connection have the expected
        # value for the config/arg combination
        assert server.config.immediate_checkpoint is expected_value
        assert server.postgres.immediate_checkpoint is expected_value

    @patch("barman.cli.BackupManifest")
    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_generate_manifest(
        self, _mock_get_server, _mock_parse_backup_id, _mock_backup_manifest, capsys
    ):
        """Verify expected log message is received on success."""
        # GIVEN a backup for a server
        args = Mock()
        args.server_name = "test_server"
        args.backup_id = "test_backup_id"

        # WHEN a backup manifest is successfully created
        with pytest.raises(SystemExit):
            generate_manifest(args)

        # THEN the expected message is in the logs
        out, _err = capsys.readouterr()
        assert (
            "Backup manifest for backup '%s' successfully generated for server %s"
            % (args.backup_id, args.server_name)
            in out
        )

    @pytest.mark.parametrize(
        ("option", "server_fun"),
        (
            # If no options are used then receive-wal should not run.
            (None, "receive_wal"),
            # If any option is set then receive-wal should run.
            ("create_slot", "create_physical_repslot"),
            ("drop_slot", "drop_repslot"),
            ("reset", "receive_wal"),
            ("stop", "kill"),
        ),
    )
    @patch("barman.cli.get_server_list")
    def test_receive_wal_inactive_server(
        self,
        mock_get_server_list,
        option,
        server_fun,
    ):
        """Verify appropriate options work with inactive servers."""
        # GIVEN an inactive server
        test_server_name = "an_arbitrary_server_name"
        config = MagicMock()
        config.active = False
        config.disabled = False
        config.retention_policy = None
        config.last_backup_maximum_age = None
        server = Mock(config=config)
        mock_server_list = {test_server_name: server}
        mock_get_server_list.return_value = mock_server_list

        # AND a set of args with the specified receive-wal option
        args = Mock(
            server_name=test_server_name,
            create_slot=None,
            drop_slot=None,
            reset=None,
            stop=None,
        )
        if option is not None:
            setattr(args, option, True)

        # WHEN receive_wal is called against the inactive server
        with pytest.raises(SystemExit):
            receive_wal(args)

        if option is not None:
            # THEN the expected server function was called
            getattr(server, server_fun).assert_called_once()
        else:
            # OR if there were no options, the expected function was not called
            getattr(server, server_fun).assert_not_called()

    @pytest.mark.parametrize(
        ("backup_id", "expected_backup_id"),
        (
            # `latest` and `last` should always return the most recent backup
            ("latest", "20221110T120000"),
            ("last", "20221110T120000"),
            # `oldest` and `first` should always return the earliest backup
            ("oldest", "20221106T120000"),
            ("first", "20221106T120000"),
            # `last-failed` should always return the last backup with FAILED status
            ("last-failed", "20221108T120000"),
            # Backup names should always return the backup with the corresponding ID
            ("named backup", "20221107T120000"),
            # The backup ID should return the backup with that ID
            ("20221109T120000", "20221109T120000"),
        ),
    )
    @patch("barman.backup.BackupManager._load_backup_cache")
    def test_parse_backup_id(
        self, _mock_load_backup_cache, backup_id, expected_backup_id
    ):
        # GIVEN a server with a list of backups
        server = build_real_server()
        backup_infos = {
            "20221110T120000": Mock(backup_id="20221110T120000", status="DONE"),
            "20221109T120000": Mock(backup_id="20221109T120000", status="DONE"),
            "20221108T120000": Mock(backup_id="20221108T120000", status="FAILED"),
            "20221107T120000": Mock(
                backup_id="20221107T120000",
                backup_name="named backup",
                status="DONE",
            ),
            "20221106T120000": Mock(backup_id="20221106T120000", status="DONE"),
        }
        server.backup_manager._backup_cache = backup_infos

        # WHEN parse_backup_id is called with a given backup ID
        args = Mock(backup_id=backup_id)
        backup_info = parse_backup_id(server, args)

        # THEN the expected backup_info is returned
        assert backup_info is backup_infos[expected_backup_id]

    @pytest.mark.parametrize(
        ("backup_infos", "backup_id"),
        (
            # Cases where backups exist but the requested backup can't be found
            # should raise an error
            (
                {
                    "20221110T120000": Mock(backup_id="20221110T120000", status="DONE"),
                },
                "20221109T120000",
            ),
            (
                {
                    "20221110T120000": Mock(backup_id="20221110T120000", status="DONE"),
                },
                "no-matching name",
            ),
            (
                {
                    "20221110T120000": Mock(backup_id="20221110T120000", status="DONE"),
                },
                "",
            ),
            # Cases where no backups exist so no requested backups can be found
            # should raise the usual "Unknown backup" error
            ({}, "20221109T120000"),
            ({}, "no-matching name"),
            ({}, "latest"),
            ({}, "last"),
            ({}, "oldest"),
            ({}, "first"),
            ({}, "last-failed"),
        ),
    )
    @patch("barman.backup.BackupManager._load_backup_cache")
    def test_parse_backup_id_no_match(
        self, _mock_load_backup_cache, backup_infos, backup_id, capsys
    ):
        # GIVEN a server with a list of backups
        server = build_real_server()
        server.backup_manager._backup_cache = backup_infos

        # WHEN parse_backup_id is called with a backup ID or name which does not exist
        args = Mock(backup_id=backup_id)

        # THEN an error is raised
        with pytest.raises(SystemExit):
            parse_backup_id(server, args)

        # AND the expected error is returned
        _out, err = capsys.readouterr()
        assert "Unknown backup '%s' for server 'main'" % backup_id in err

    @patch("barman.server.Server.replication_status")
    def test_replication_status(self, replication_status_mock, monkeypatch, capsys):
        """
        Test the test_replication_status method

        :param MagicMock replication_status_mock: Mock object for the replication_status method of the server
        :param monkeypatch monkeypatch: pytest patcher
        :param capsys: fixture that allow to access stdout/stderr output
        """
        # Simple test case, ensure that passive nodes are skipped
        # Monkeypatch the config and make `main` a passive node
        testing_conf = build_config_from_dicts(
            main_conf={
                "primary_ssh_command": "ssh fakeuser@fakehost",
            }
        )
        monkeypatch.setattr(barman, "__config__", testing_conf)
        # Mock object simulating the args
        args = MagicMock()
        args.server_name = ["all"]
        args.minimal = "minimal"
        # SystemExit exception will be issued even in case of success
        with pytest.raises(SystemExit):
            replication_status(args)
        out, err = capsys.readouterr()
        # Ensure there is an output and main is skipped
        assert out
        assert out.strip() == "Skipping passive server 'main'"

    @pytest.mark.parametrize(
        ("source", "wal_streaming_arg"),
        (
            # If the source is "backup-host" then we expect the server was fetched
            # without wal_streaming
            ("backup-host", False),
            # If the source is "wal-host" then we expect the server was fetched
            # with wal_streaming
            ("wal-host", True),
        ),
    )
    @patch("barman.cli.get_server_list")
    @patch("barman.server.Server.replication_status")
    def test_replication_status_source(
        self, _replication_status_mock, get_server_list_mock, source, wal_streaming_arg
    ):
        """
        Verify that the server is retrieved with either WAL conninfo strings or non-WAL
        conninfo strings depending on the value of the source argument.
        """
        # WHEN replication_status is called with the specified source arg
        args = MagicMock()
        args.server_name = ["main"]
        args.source = source
        with pytest.raises(SystemExit):
            replication_status(args)
        # THEN get_server_list was called with the expected wal_streaming argument
        get_server_list_mock.assert_called_once_with(
            args,
            skip_inactive=True,
            skip_passive=True,
            wal_streaming=wal_streaming_arg,
        )


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
        self,
        mock_get_server,
        mock_parse_backup_id,
        mock_args,
        monkeypatch_config,
    ):
        """Verify barman keep command calls keep_backup"""
        mock_args.target = "standalone"
        mock_parse_backup_id.return_value.backup_id = "test_backup_id"
        mock_parse_backup_id.return_value.status = BackupInfo.DONE
        mock_parse_backup_id.return_value.is_incremental = False
        keep(mock_args)
        mock_get_server.return_value.backup_manager.keep_backup.assert_called_once_with(
            "test_backup_id", "standalone"
        )

    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_barman_keep_fails_if_no_target_release_or_status_provided(
        self,
        mock_get_server,
        mock_parse_backup_id,
        mock_args,
        capsys,
    ):
        """
        Verify barman keep command fails if none of --release, --status or --target
        are provided.
        """
        mock_parse_backup_id.return_value.backup_id = "test_backup_id"
        mock_parse_backup_id.return_value.is_incremental = False
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
        mock_parse_backup_id.return_value.is_incremental = False
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
        self,
        mock_get_server,
        mock_parse_backup_id,
        mock_args,
        monkeypatch_config,
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
        mock_parse_backup_id.return_value.is_incremental = False
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
        mock_parse_backup_id.return_value.is_incremental = False
        mock_get_server.return_value.backup_manager.get_keep_target.return_value = None
        mock_args.status = True
        keep(mock_args)
        mock_get_server.return_value.backup_manager.get_keep_target.assert_called_once_with(
            "test_backup_id"
        )
        out, _err = capsys.readouterr()
        assert "nokeep" in out

    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_barman_keep_incremental_backup(
        self,
        mock_get_server,
        mock_parse_backup_id,
        mock_args,
        capsys,
    ):
        """Verify barman keep command will not add keep if backup is incremental"""
        mock_args.target = "standalone"
        mock_parse_backup_id.return_value.backup_id = "test_backup_id"
        mock_parse_backup_id.return_value.is_incremental = True
        mock_parse_backup_id.return_value.status = BackupInfo.DONE

        with pytest.raises(SystemExit):
            keep(mock_args)
        _out, err = capsys.readouterr()
        assert (
            "Unable to execute the keep command on backup test_backup_id: is an incremental backup.\n"
            "Only full backups are eligible for the use of the keep command."
        ) in err
        mock_get_server.return_value.backup_manager.keep_backup.assert_not_called()

    @patch("barman.cli.parse_backup_id")
    @patch("barman.cli.get_server")
    def test_barman_keep_full_backup(
        self, mock_get_server, mock_parse_backup_id, mock_args
    ):
        """Verify barman keep command will add keep if backup is not incremental"""
        mock_parse_backup_id.return_value.backup_id = "test_backup_id"
        mock_parse_backup_id.return_value.is_incremental = False
        mock_parse_backup_id.return_value.status = BackupInfo.DONE
        mock_args.release = True
        keep(mock_args)
        mock_get_server.return_value.backup_manager.release_keep.assert_called_once_with(
            "test_backup_id"
        )


class TestCliHelp(object):
    """
    Verify the help output of the ArgumentParser constructed by cli.py

    Checks that the cli ArgumentParser correctly expands the subcommand help and
    prints the subcommands and positional args in alphabetical order.

    This is achieved by creating a minimal argument parser and checking the
    print_help() output matches our expected output.
    """

    _expected_help_output = """usage: %s [-h] [-t] {{another-test-command,test-command}} ...

positional arguments:
  {{another-test-command,test-command}}
    another-test-command
                        Another test docstring also readable in expanded help
    test-command        Test docstring which should be readable in expanded
                        help

{options_label}:
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
        # GIVEN a minimal help parser
        # WHEN the help is printed
        minimal_parser.print_help()

        # THEN nothing is printed to stderr
        out, err = capsys.readouterr()
        assert "" == err

        # AND the expected help output is printed to stdout
        options_label = "options"
        # WITH the options being prefixed by 'optional arguments' for older versions of
        # python
        if sys.version_info < (3, 10):
            options_label = "optional arguments"
        expected_output = self._expected_help_output.format(options_label=options_label)
        assert expected_output == out


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


class TestConfigSwitchCli:
    """Test ``barman config-switch`` outcomes."""

    @pytest.fixture
    def mock_args(self):
        return MagicMock(
            server_name="SOME_SERVER", model_name="SOME_MODEL", reset=False
        )

    @patch("barman.cli.output")
    def test_config_switch_invalid_args(self, mock_output, mock_args):
        """Test :func:`config_switch`.

        It should error out if neither ``--reset`` nor ``model_name`` are given.
        """
        mock_args.model_name = None

        config_switch(mock_args)

        mock_output.error.assert_called_once_with(
            "Either a model name or '--reset' flag need to be given"
        )

    @patch("barman.cli.get_server")
    def test_config_switch_no_server(self, mock_get_server, mock_args):
        """Test :func:`config_switch`.

        It should do nothing if :func:`get_server` returns nothing.
        """
        mock_get_server.return_value = None

        config_switch(mock_args)

        mock_get_server.assert_called_once_with(mock_args, skip_inactive=False)

    @patch("barman.cli.get_model")
    @patch("barman.cli.get_server")
    def test_config_switch_model_apply_model_no_model(
        self, mock_get_server, mock_get_model, mock_args
    ):
        """Test :func:`config_switch`.

        It should call :meth:`barman.config.ServerConfig.apply_model` when
        a server and a model are given.
        """
        mock_apply_model = mock_get_server.return_value.config.apply_model
        mock_reset_model = mock_get_server.return_value.config.reset_model
        mock_get_model.return_value = None

        config_switch(mock_args)

        mock_get_server.assert_called_once_with(mock_args, skip_inactive=False)
        mock_apply_model.assert_not_called()
        mock_reset_model.assert_not_called()

    @patch("barman.cli.get_model")
    @patch("barman.cli.get_server")
    def test_config_switch_model_apply_model_ok(
        self, mock_get_server, mock_get_model, mock_args
    ):
        """Test :func:`config_switch`.

        It should call :meth:`barman.config.ServerConfig.apply_model` when
        a server and a model are given.
        """
        mock_apply_model = mock_get_server.return_value.config.apply_model
        mock_reset_model = mock_get_server.return_value.config.reset_model

        config_switch(mock_args)

        mock_get_server.assert_called_once_with(mock_args, skip_inactive=False)
        mock_apply_model.assert_called_once_with(mock_get_model.return_value, True)
        mock_reset_model.assert_not_called()

    @patch("barman.cli.get_server")
    def test_config_switch_model_reset_model(self, mock_get_server, mock_args):
        """Test :func:`config_switch`.

        It should call :meth:`barman.config.ServerConfig.reset_model` when
        a server and ``--reset`` flag are given.
        """
        mock_args.model_name = None
        mock_args.reset = True
        mock_apply_model = mock_get_server.return_value.config.apply_model
        mock_reset_model = mock_get_server.return_value.config.reset_model

        config_switch(mock_args)

        mock_get_server.assert_called_once_with(mock_args, skip_inactive=False)
        mock_apply_model.assert_not_called()
        mock_reset_model.assert_called_once_with()
