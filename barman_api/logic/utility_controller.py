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

import json

import barman
from barman import diagnose, output
from barman.server import Server
from barman_api.openapi_server.models.diagnose_output import DiagnoseOutput
from barman_api.openapi_server import util


class UtilityController:
    def diagnose(self):
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

        # new outputs are appended, so grab the last one
        stored_output = json.loads(output._writer.json_output["_INFO"][-1])

        diag_output = DiagnoseOutput(
            _global=str(stored_output["global"]), servers=str(stored_output["servers"])
        )

        return diag_output.to_dict()

    def status(self):
        return "OK"  # If this app isn't running, we obviously won't return!
