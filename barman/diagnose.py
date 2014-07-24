# Copyright (C) 2011-2014 2ndQuadrant Italia (Devise.IT S.r.L.)
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

"""
This module represents the barman diagnostic tool.
"""

import logging
import json
import sys
from barman.backup import BackupInfo
import barman
from barman import output
from barman import fs
from barman.utils import BarmanEncoder


_logger = logging.getLogger(__name__)


def exec_diagnose(servers):
    """
    Diagnostic command: gathers information from backup server
    and from all the configured servers.

    Gathered information should be used for support and problems detection

    :param servers: list of configured servers
    """
    # global section. info about barman server
    diagnosis = {}
    diagnosis['global'] = {}
    diagnosis['servers'] = {}
    # barman global config
    diagnosis['global']['config'] = dict(barman.__config__._global_config)
    command = fs.UnixLocalCommand()
    # basic system info
    diagnosis['global']['system_info'] = command.get_system_info()
    diagnosis['global']['system_info']['barman_ver'] = barman.__version__
    # per server section
    for name in sorted(servers):
        server = servers[name]
        if server is None:
            output.error("Unknown server '%s'" % name)
            continue
        # server configuration
        diagnosis['servers'][name] = {}
        diagnosis['servers'][name]['config'] = vars(server.config)
        del diagnosis['servers'][name]['config']['config']
        # server system info
        command = fs.UnixRemoteCommand(ssh_command=server.config.ssh_command)
        diagnosis['servers'][name]['system_info'] = command.get_system_info()
        # barman statuts information for the server
        diagnosis['servers'][name]['status'] = server.get_remote_status()
        # backup list
        status_filter = BackupInfo.STATUS_NOT_EMPTY
        backups = server.get_available_backups(status_filter)
        diagnosis['servers'][name]['backups'] = dict([(k,v.to_dict())
                                           for (k,v) in backups.iteritems()])
    output.info(json.dumps(diagnosis, sys.stdout, cls=BarmanEncoder, indent=4,
                           sort_keys=True))


