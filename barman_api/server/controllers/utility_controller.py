# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2021
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

import connexion
import six
import json

import barman
from barman import diagnose, output
from barman.server import Server
from ..models.diagnose_output import DiagnoseOutput  # noqa: E501
from .. import util


def diagnose():  # noqa: E501
    """Return barman diagnose information

     # noqa: E501


    :rtype: DiagnoseOutput
    """
    # Get every server (both inactive and temporarily disabled)
    servers = barman.__config__.server_names()
    
    server_dict = {}
    for server in servers:
        conf = barman.__config__.get_server(server)
        if conf is None:
            # Unknown server
            server_dict[server] = None
        else:
            server_object = Server(conf)
            server_dict[server] = server_object

    # errors list with duplicate paths between servers
    errors_list = barman.__config__.servers_msg_list

    barman.diagnose.exec_diagnose(server_dict, errors_list)

    # FIXME not sure if the 0th thing is guaranteed
    stored_output = json.loads(output._writer.json_output['_INFO'][0])

    diag_output = DiagnoseOutput(
        _global=str(stored_output['global']), 
        servers=str(stored_output['servers'])
    )

    return diag_output.to_dict()

def status():  # noqa: E501
    """Check if Barman API App running

     # noqa: E501


    :rtype: None
    """
    return 'OK'  # If this app isn't running, we obviously won't return!