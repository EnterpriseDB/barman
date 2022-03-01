# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2011-2022
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

import datetime
from dateutil import tz
import json
import logging

import barman
from barman import fs, output
from barman.backup import BackupInfo
from barman.exceptions import CommandFailedException, FsOperationFailed
from barman.utils import BarmanEncoderV2

_logger = logging.getLogger(__name__)


def exec_diagnose(servers, errors_list):
    """
    Diagnostic command: gathers information from backup server
    and from all the configured servers.

    Gathered information should be used for support and problems detection

    :param dict(str,barman.server.Server) servers: list of configured servers
    :param list errors_list: list of global errors
    """
    # global section. info about barman server
    diagnosis = {"global": {}, "servers": {}}
    # barman global config
    diagnosis["global"]["config"] = dict(barman.__config__._global_config)
    diagnosis["global"]["config"]["errors_list"] = errors_list
    try:
        command = fs.UnixLocalCommand()
        # basic system info
        diagnosis["global"]["system_info"] = command.get_system_info()
    except CommandFailedException as e:
        diagnosis["global"]["system_info"] = {"error": repr(e)}
    diagnosis["global"]["system_info"]["barman_ver"] = barman.__version__
    diagnosis["global"]["system_info"]["timestamp"] = datetime.datetime.now(
        tz=tz.tzlocal()
    )
    # per server section
    for name in sorted(servers):
        server = servers[name]
        if server is None:
            output.error("Unknown server '%s'" % name)
            continue
        # server configuration
        diagnosis["servers"][name] = {}
        diagnosis["servers"][name]["config"] = vars(server.config)
        if "config" in diagnosis["servers"][name]["config"]:
            del diagnosis["servers"][name]["config"]["config"]
        # server system info
        if server.config.ssh_command:
            try:
                command = fs.UnixRemoteCommand(
                    ssh_command=server.config.ssh_command, path=server.path
                )
                diagnosis["servers"][name]["system_info"] = command.get_system_info()
            except FsOperationFailed:
                pass
        # barman status information for the server
        diagnosis["servers"][name]["status"] = server.get_remote_status()
        # backup list
        backups = server.get_available_backups(BackupInfo.STATUS_ALL)
        # update date format for each backup begin_time and end_time and ensure local timezone.
        # This code is a duplicate from BackupInfo.to_json()
        # This should be temporary to keep original behavior for other usage.
        for key in backups.keys():
            data = backups[key].to_dict()
            if data.get("tablespaces") is not None:
                data["tablespaces"] = [list(item) for item in data["tablespaces"]]
            if data.get("begin_time") is not None:
                data["begin_time"] = data["begin_time"].astimezone(tz=tz.tzlocal())
            if data.get("end_time") is not None:
                data["end_time"] = data["end_time"].astimezone(tz=tz.tzlocal())
            backups[key] = data

        diagnosis["servers"][name]["backups"] = backups
        # wal status
        diagnosis["servers"][name]["wals"] = {
            "last_archived_wal_per_timeline": server.backup_manager.get_latest_archived_wals_info(),
        }
        # Release any PostgreSQL resource
        server.close()
    output.info(
        json.dumps(diagnosis, cls=BarmanEncoderV2, indent=4, sort_keys=True), log=False
    )
