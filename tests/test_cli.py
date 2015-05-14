# Copyright (C) 2014-2015 2ndQuadrant Italia (Devise.IT S.r.L.)
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
from mock import Mock

from barman.server import Server
from testing_helpers import build_config_from_dicts


try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

import pytest

from barman.cli import get_server, get_server_list,\
    server_error_output
from test_config import MINIMAL_CONFIG_MAIN, MINIMAL_ERROR_CONFIG_MAIN
import barman.config


class TestCli(object):

    def test_get_server(self):
        """
        Test the get_server method, providing a basic configuration
        """
        #`Mock the args from argparse
        args = Mock()
        args.server_name = 'main'
        barman.__config__ = build_config_from_dicts(main_conf=MINIMAL_CONFIG_MAIN)
        server_main = get_server(args, dangerous=False)
        # Expect the server to exists
        assert server_main
        # Expect the name to be the right one
        assert server_main.config.name == 'main'

    def test_get_server_with_conflicts(self, capsys):
        """
        Test get_server method using a configuration containing errors
        """
        #`Mock the args from argparse
        args = Mock()
        args.server_name = 'main'
        # Build a configuration with error
        barman.__config__ = None
        barman.__config__ = build_config_from_dicts(main_conf=MINIMAL_ERROR_CONFIG_MAIN)
        with pytest.raises(SystemExit):
            get_server(args, True)
        out, err = capsys.readouterr()
        assert err
        assert "ERROR: Conflicting path:" in err
        barman.__config__ = build_config_from_dicts(main_conf=MINIMAL_ERROR_CONFIG_MAIN)
        server_main = get_server(args, False)
        # In this case the server is returned but with a warning message
        assert server_main
        out, err = capsys.readouterr()
        assert err
        assert "WARNING: Conflicting path:" in err

    def test_server_error_output(self, capsys):
        """
        Test server_error_output method checking
        the various types of error output
        """
        # Build a server with a config with path conflicts
        barman.__config__ = build_config_from_dicts(main_conf=MINIMAL_ERROR_CONFIG_MAIN)
        server = Server(barman.__config__.get_server('main'))
        # Test a blocking ERROR message
        with pytest.raises(SystemExit):
            server_error_output(server, is_error=True)
        out, err = capsys.readouterr()
        # Expect a ERROR message because of conflicting paths
        assert 'ERROR: Conflicting path' in err
        # Test a not blocking WARNING message
        server_error_output(server, is_error=False)
        out, err = capsys.readouterr()
        # Expect a WARNING message because of conflicting paths
        assert 'WARNING: Conflicting path' in err
        # Build a server with a config without path conflicts
        barman.__config__ = build_config_from_dicts(main_conf=MINIMAL_CONFIG_MAIN)
        server = Server(barman.__config__.get_server('main'))
        # Set the server as not active
        server.config.active = False
        # Test a blocking ERROR message
        with pytest.raises(SystemExit):
            server_error_output(server, is_error=True)
        out, err = capsys.readouterr()
        # Expect a ERROR message because of a not active server
        assert 'ERROR: Not active server' in err
        # Test a not blocking WARNING message
        server_error_output(server, is_error=False)
        # Expect a WARNING message because of a not active server
        out, err = capsys.readouterr()
        assert 'WARNING: Not active server' in err

    def test_get_server_global_error_list(self, capsys):
        """
        Test the management of multiple servers and the
        presence of global errors
        """
        args = Mock()
        args.server_name = 'main'
        # Build 2 servers with shared path.
        barman.__config__ = build_config_from_dicts(None,
                                                    MINIMAL_CONFIG_MAIN,
                                                    MINIMAL_CONFIG_MAIN)
        # Expect a conflict because of the shared paths
        with pytest.raises(SystemExit):
            get_server(args, dangerous=False)
        out, err = capsys.readouterr()
        # Check for the presence of error messages
        assert err
        # Check paths in error messages
        assert 'Conflicting path: basebackups_directory=/some/barman/home/main/base' in err
        assert 'Conflicting path: incoming_wals_directory=/some/barman/home/main/incoming' in err
        assert 'Conflicting path: wals_directory=/some/barman/home/main/wals' in err
        assert 'Conflicting path: backup_directory=/some/barman/home/main' in err

    def test_get_server_list(self, capsys):
        """
        Test the get_server_list method
        """
        barman.__config__ = build_config_from_dicts()
        server_dict = get_server_list()
        assert server_dict
        # Expect 2 test servers Main and Test
        assert len(server_dict) == 2
        # Test the method with global errors
        barman.__config__ = build_config_from_dicts(None,
                                    MINIMAL_CONFIG_MAIN,
                                    MINIMAL_CONFIG_MAIN)
        # Expect the method to fail and exit
        with pytest.raises(SystemExit):
            get_server_list()
        out, err = capsys.readouterr()
        # Check for the presence of error messages
        assert err
        # Check paths in error messages
        assert 'Conflicting path: basebackups_directory=/some/barman/home/main/base' in err
        assert 'Conflicting path: incoming_wals_directory=/some/barman/home/main/incoming' in err
        assert 'Conflicting path: wals_directory=/some/barman/home/main/wals' in err
        assert 'Conflicting path: backup_directory=/some/barman/home/main' in err

    def test_get_server_list_global_error_continue(self):
        """
        Test the population of the list of global errors for diagnostic purposes
        (diagnose invocation)
        """
        barman.__config__ = build_config_from_dicts(None,
                                    MINIMAL_CONFIG_MAIN,
                                    MINIMAL_CONFIG_MAIN)
        server_dict = get_server_list(on_error_stop=False)
        global_error_list = barman.__config__.servers_msg_list
        # Check for the presence of servers
        assert server_dict
        # Check for the presence of global errors
        assert global_error_list
        assert len(global_error_list) == 4
