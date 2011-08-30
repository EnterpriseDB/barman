#
# barman - Backup and Recovery Manager for PostgreSQL
#
# Copyright (C) 2011  Devise.IT S.r.l. <info@2ndquadrant.it>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
from argh import ArghParser, alias, arg
import barman.config
from barman.server import Server

def list(args):
    "list available servers, with useful information"
    servers = barman.__config__.servers()
    for server in servers:
        yield server.name

def cron(args):
    "run maintenance tasks"
    yield "TODO" # TODO: implement this

SERVER_DESCRIPTION = """
all server commands require a server_name argument
"""

@alias('backup')
@arg('server_name', help='specifies the server name for the command')
def server_backup(args):
    'perform a full backup for the given server'
    yield "TODO" # TODO: implement this

@alias('list')
@arg('server_name', help='specifies the server name for the command')
def server_list(args):
    'list available backups for the given server'
    yield "TODO" # TODO: implement this

@alias('status')
@arg('server_name', help='specifies the server name for the command')
def server_status(args):
    'shows live information and status of the PostgreSQL server'
    yield "TODO" # TODO: implement this

@alias('delete_obsolete')
@arg('server_name', help='specifies the server name for the command')
def server_delete_obsolete(args):
    'delete obsolete backups and WAL (according to retention policy)'
    yield "TODO" # TODO: implement this

@alias('recover')
@arg('server_name', help='specifies the server name for the command')
@arg('--target-time', help='target time')
@arg('--target-xid', help='target xid')
@arg('--exclusive', help='set target xid to be non inclusive')
def server_recover(args):
    'recover a server at a given time or xid'
    yield "TODO" # TODO: implement this

@alias('check')
@arg('server_name', help='specifies the server name for the command (all to check all available servers)')
def server_check(args):
    'check if connection settings work properly for the specified server'
    config = barman.__config__.get_server(args.server_name)
    if not config:
        if args.server_name == 'all':
            servers = [Server(conf) for conf in barman.__config__.servers()]
        else:
            yield "Unknown server %r" % (args.server_name)
            return
    else:
        servers = [Server(config)]
    for server in servers:
        for line in server.check():
            yield line

BACKUP_DESCRIPTION = """
all backup commands accept require a backup_id argument
"""

@alias('show')
@arg('backup_id', help='specifies the backup ID')
def backup_show(args):
    'show a single backup information'
    yield "TODO" # TODO: implement this

@alias('terminate')
@arg('backup_id', help='specifies the backup ID')
def backup_terminate(backup):
    'terminate a running backup'
    yield "TODO" # TODO: implement this

@alias('delete')
@arg('backup_id', help='specifies the backup ID')
def backup_delete(backup):
    'delete a backup'
    yield "TODO" # TODO: implement this

@arg('backup_id', help='specifies the backup ID')
@alias('recover')
def backup_recover(backup):
    'recover a backup'
    yield "TODO" # TODO: implement this

def load_config(args):
    if hasattr(args, 'config'):
        barman.__config__ = barman.config.Config(args.config)
    else:
        try:
            file = os.environ['BARMAN_CONFIG_FILE']
        except KeyError:
            file = None
        barman.__config__ = barman.config.Config(file)

def main():
    p = ArghParser()
    p.add_argument('-v', '--version', action='version', version=barman.__version__)
    p.add_argument('-c', '--config', help='uses a configuration file (defaults: $HOME/.barman.conf, /etc/barman.conf)')
    p.add_commands([list, cron])
    p.add_commands(
        [
            server_backup,
            server_list,
            server_status,
            server_check,
            server_delete_obsolete,
            server_recover,
        ],
        namespace='server',
        title='commands acting on a server',
        description=SERVER_DESCRIPTION,
    )
    p.add_commands(
        [
            backup_show,
            backup_terminate,
            backup_delete,
            backup_recover
        ],
        namespace='backup',
        title='commands acting on a backup',
        description=BACKUP_DESCRIPTION,
    )
    p.dispatch(pre_call=load_config)

if __name__ == '__main__':
    main()
