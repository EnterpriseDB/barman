# Copyright (C) 2014-2018 2ndQuadrant Limited
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

import pytest
from mock import Mock, patch

import barman.config
from barman.cli import (check_target_action, get_server, get_server_list,
                        manage_server_command, recover)
from barman.infofile import BackupInfo
from barman.i18n import ugettext as _
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
        args.server_name = 'main'
        monkeypatch.setattr(barman, '__config__', build_config_from_dicts(
            main_conf={
                'archiver': 'on',
            }))
        server_main = get_server(args)
        # Expect the server to exists
        assert server_main
        # Expect the name to be the right one
        assert server_main.config.name == 'main'

    def test_get_server_with_conflicts(self, monkeypatch, capsys):
        """
        Test get_server method using a configuration containing errors

        :param monkeypatch monkeypatch: pytest patcher
        """
        # Mock the args from argparse
        args = Mock()
        # conflicting directories
        monkeypatch.setattr(barman, '__config__', build_config_from_dicts(
            main_conf={
                'wals_directory': '/some/barman/home/main/wals',
                'basebackups_directory': '/some/barman/home/main/wals',
                'archiver': 'on',
            }))
        args.server_name = 'main'
        with pytest.raises(SystemExit):
            get_server(args, True)
        out, err = capsys.readouterr()
        assert err
        assert "ERROR: Conflicting path:" in err

        # conflicting directories with on_error_stop=False
        monkeypatch.setattr(barman, '__config__', build_config_from_dicts(
            main_conf={
                'wals_directory': '/some/barman/home/main/wals',
                'basebackups_directory': '/some/barman/home/main/wals',
                'archiver': 'on',
            }))
        args.server_name = 'main'
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
        monkeypatch.setattr(barman, '__config__', build_config_from_dicts(
            main_conf=build_config_dictionary({
                'wals_directory': '/some/barman/home/main/wals',
                'basebackups_directory': '/some/barman/home/main/wals',
                'archiver': 'on',
            })))
        server = Server(barman.__config__.get_server('main'))
        # Test a not blocking WARNING message
        manage_server_command(server)
        out, err = capsys.readouterr()
        # Expect an ERROR message because of conflicting paths
        assert 'ERROR: Conflicting path' in err

        # Build a server with a config without path conflicts
        monkeypatch.setattr(barman, '__config__', build_config_from_dicts())
        server = Server(barman.__config__.get_server('main'))
        # Set the server as not active
        server.config.active = False
        # Request to treat inactive as errors
        to_be_executed = manage_server_command(server, inactive_is_error=True)
        out, err = capsys.readouterr()
        # Expect a ERROR message because of a not active server
        assert 'ERROR: Inactive server' in err
        assert not to_be_executed

        # Request to treat inactive as warning
        to_be_executed = manage_server_command(server, inactive_is_error=False)
        out, err = capsys.readouterr()
        # Expect no error whatsoever
        assert err == ''
        assert not to_be_executed

    def test_get_server_global_error_list(self, monkeypatch, capsys):
        """
        Test the management of multiple servers and the
        presence of global errors

        :param monkeypatch monkeypatch: pytest patcher
        """
        args = Mock()
        args.server_name = 'main'
        # Build 2 servers with shared path.
        monkeypatch.setattr(barman, '__config__', build_config_from_dicts(
            global_conf=None,
            main_conf={
                'basebackups_directory': '/some/barman/home/main/base',
                'incoming_wals_directory': '/some/barman/home/main/incoming',
                'wals_directory': '/some/barman/home/main/wals',
                'backup_directory': '/some/barman/home/main',
                'archiver': 'on',
            },
            test_conf={
                'basebackups_directory': '/some/barman/home/test/wals',
                'incoming_wals_directory': '/some/barman/home/main/incoming',
                'wals_directory': '/some/barman/home/main/wals',
                'backup_directory': '/some/barman/home/main',
                'archiver': 'on',
            }))
        # Expect a conflict because of the shared paths
        with pytest.raises(SystemExit):
            get_server(args)
        out, err = capsys.readouterr()
        # Check for the presence of error messages
        assert err
        # Check paths in error messages
        assert 'Conflicting path: ' \
               'basebackups_directory=/some/barman/home/main/base' in err
        assert 'Conflicting path: ' \
               'incoming_wals_directory=/some/barman/home/main/incoming' in err
        assert 'Conflicting path: ' \
               'wals_directory=/some/barman/home/main/wals' in err
        assert 'Conflicting path: ' \
               'backup_directory=/some/barman/home/main' in err

    def test_get_server_list(self, monkeypatch, capsys):
        """
        Test the get_server_list method

        :param monkeypatch monkeypatch: pytest patcher
        """
        monkeypatch.setattr(barman, '__config__', build_config_from_dicts())
        server_dict = get_server_list()
        assert server_dict
        # Expect 2 test servers Main and Test
        assert len(server_dict) == 2
        # Test the method with global errors
        monkeypatch.setattr(barman, '__config__', build_config_from_dicts(
            global_conf=None,
            main_conf={
                'basebackups_directory': '/some/barman/home/main/base',
                'incoming_wals_directory': '/some/barman/home/main/incoming',
                'wals_directory': '/some/barman/home/main/wals',
                'backup_directory': '/some/barman/home/main',
                'archiver': 'on',
            },
            test_conf={
                'basebackups_directory': '/some/barman/home/test/wals',
                'incoming_wals_directory': '/some/barman/home/main/incoming',
                'wals_directory': '/some/barman/home/main/wals',
                'backup_directory': '/some/barman/home/main',
                'archiver': 'on',
            }))
        # Expect the method to fail and exit
        with pytest.raises(SystemExit):
            get_server_list()
        out, err = capsys.readouterr()
        # Check for the presence of error messages
        assert err
        # Check paths in error messages
        assert 'Conflicting path: ' \
               'basebackups_directory=/some/barman/home/main/base' in err
        assert 'Conflicting path: ' \
               'incoming_wals_directory=/some/barman/home/main/incoming' in err
        assert 'Conflicting path: ' \
               'wals_directory=/some/barman/home/main/wals' in err
        assert 'Conflicting path: ' \
               'backup_directory=/some/barman/home/main' in err

    def test_get_server_list_global_error_continue(self, monkeypatch):
        """
        Test the population of the list of global errors for diagnostic
        purposes (diagnose invocation)

        :param monkeypatch monkeypatch: pytest patcher
        """
        monkeypatch.setattr(barman, '__config__', build_config_from_dicts(
            global_conf=None,
            main_conf={
                'backup_directory': '/some/barman/home/main',
                'archiver': 'on',
            },
            test_conf={
                'backup_directory': '/some/barman/home/main',
                'archiver': 'on',
            }))
        server_dict = get_server_list(on_error_stop=False)
        global_error_list = barman.__config__.servers_msg_list
        # Check for the presence of servers
        assert server_dict
        # Check for the presence of global errors
        assert global_error_list
        assert len(global_error_list) == 6

    @patch('barman.cli.parse_backup_id')
    @patch('barman.cli.get_server')
    def test_recover_multiple_targets(
            self, get_server_mock,
            parse_backup_id_mock,
            monkeypatch, capsys):
        backup_info = Mock()
        backup_info.status = BackupInfo.DONE
        backup_info.tablespaces = []

        parse_backup_id_mock.return_value = backup_info

        monkeypatch.setattr(barman, '__config__', build_config_from_dicts(
            main_conf={
                'archiver': 'on',
            }))

        # Testing mutual exclusiveness of target options
        args = Mock()
        args.backup_id = '20170823T104400'
        args.server_name = 'main'
        args.destination_directory = 'recovery_dir'
        args.tablespace = None
        args.target_name = None
        args.target_tli = 3
        args.target_immediate = True
        args.target_time = None
        args.target_xid = None

        with pytest.raises(SystemExit):
            recover(args)

        _, err = capsys.readouterr()
        assert 'ERROR: You cannot specify multiple targets for the recovery ' \
               'operation' in err

    @patch('barman.cli.parse_backup_id')
    @patch('barman.cli.get_server')
    def test_recover_one_target(self, get_server_mock,
                                parse_backup_id_mock, monkeypatch,
                                capsys):
        backup_info = Mock()
        backup_info.status = BackupInfo.DONE
        backup_info.tablespaces = []

        parse_backup_id_mock.return_value = backup_info

        monkeypatch.setattr(barman, '__config__', build_config_from_dicts(
            main_conf={
                'archiver': 'on',
            }))

        # This parameters are fine
        args = Mock()
        args.backup_id = '20170823T104400'
        args.server_name = 'main'
        args.destination_directory = 'recovery_dir'
        args.tablespace = None
        args.target_name = None
        args.target_tli = None
        args.target_immediate = True
        args.target_time = None
        args.target_xid = None
        args.target_action = None

        _, err = capsys.readouterr()
        with pytest.raises(SystemExit):
            recover(args)
        assert "" == err

    @patch('barman.cli.parse_backup_id')
    @patch('barman.cli.get_server')
    def test_recover_default_target(self, get_server_mock,
                                    parse_backup_id_mock, monkeypatch,
                                    capsys):
        backup_info = Mock()
        backup_info.status = BackupInfo.DONE
        backup_info.tablespaces = []

        parse_backup_id_mock.return_value = backup_info

        monkeypatch.setattr(barman, '__config__', build_config_from_dicts(
            main_conf={
                'archiver': 'on',
            }))

        # This parameters are fine
        args = Mock()
        args.backup_id = '20170823T104400'
        args.server_name = 'main'
        args.destination_directory = 'recovery_dir'
        args.tablespace = None
        args.target_name = None
        args.target_tli = None
        args.target_immediate = None
        args.target_time = None
        args.target_xid = None
        args.target_action = None

        _, err = capsys.readouterr()
        with pytest.raises(SystemExit):
            recover(args)
        assert "" == err

    def test_check_target_action(self):
        # The following ones must work
        assert None is check_target_action(None)
        assert 'pause' is check_target_action('pause')
        assert 'promote' is check_target_action('promote')
        assert 'shutdown' is check_target_action('shutdown')

        # Every other value is an error
        with pytest.raises(ArgumentTypeError):
            check_target_action('invalid_target_action')
