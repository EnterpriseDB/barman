# Copyright (C) 2014-2016 2ndQuadrant Italia Srl
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

import pytest
from mock import Mock

import barman.config
from barman.cli import get_server, get_server_list, manage_server_command
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
        monkeypatch.setattr(barman, '__config__', build_config_from_dicts())
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
            },
            test_conf={
                'basebackups_directory': '/some/barman/home/test/wals',
                'incoming_wals_directory': '/some/barman/home/main/incoming',
                'wals_directory': '/some/barman/home/main/wals',
                'backup_directory': '/some/barman/home/main',
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
            },
            test_conf={
                'basebackups_directory': '/some/barman/home/test/wals',
                'incoming_wals_directory': '/some/barman/home/main/incoming',
                'wals_directory': '/some/barman/home/main/wals',
                'backup_directory': '/some/barman/home/main',
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
            },
            test_conf={
                'backup_directory': '/some/barman/home/main',
            }))
        server_dict = get_server_list(on_error_stop=False)
        global_error_list = barman.__config__.servers_msg_list
        # Check for the presence of servers
        assert server_dict
        # Check for the presence of global errors
        assert global_error_list
        assert len(global_error_list) == 6
