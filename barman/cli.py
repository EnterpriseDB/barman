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

from argh import ArghParser, alias, arg

def list():
    "list available servers, with useful information"
    pass

def cron():
    "run maintenance tasks"
    pass

SERVER_DESCRIPTION = """
all server commands require a server-name argument
"""

@alias('backup')
@arg('-c', '--config', help='uses a configuration file (defaults: $HOME/.barman.conf, /etc/barman.conf)')
@arg('server-name', help='specifies the server name for the command')
def server_backup(args):
    'perform a full backup for the given server'
    pass

@alias('list')
@arg('-c', '--config', help='uses a configuration file (defaults: $HOME/.barman.conf, /etc/barman.conf)')
@arg('server-name', help='specifies the server name for the command')
def server_list(args):
    'list available backups for the given server'
    pass

@alias('status')
@arg('-c', '--config', help='uses a configuration file (defaults: $HOME/.barman.conf, /etc/barman.conf)')
@arg('server-name', help='specifies the server name for the command')
def server_status(args):
    'shows live information and status of the PostgreSQL server'
    pass

@alias('delete_obsolete')
@arg('-c', '--config', help='uses a configuration file (defaults: $HOME/.barman.conf, /etc/barman.conf)')
@arg('server-name', help='specifies the server name for the command')
def server_delete_obsolete(args):
    'delete obsolete backups and WAL (according to retention policy)'
    pass

@alias('recover')
@arg('-c', '--config', help='uses a configuration file (defaults: $HOME/.barman.conf, /etc/barman.conf)')
@arg('server-name', help='specifies the server name for the command')
@arg('--target-time', help='target time')
@arg('--target-xid', help='target xid')
@arg('--exclusive', help='set target xid to be non inclusive')
def server_recover(args):
    'recover a server at a given time or xid'
    pass

BACKUP_DESCRIPTION = """
all backup commands accept require a backup-id argument
"""

@alias('show')
@arg('-c', '--config', help='uses a configuration file (defaults: $HOME/.barman.conf, /etc/barman.conf)')
@arg('backup-id', help='specifies the backup ID')
def backup_show(args):
    'show a single backup information'
    pass

@alias('terminate')
@arg('-c', '--config', help='uses a configuration file (defaults: $HOME/.barman.conf, /etc/barman.conf)')
@arg('backup-id', help='specifies the backup ID')
def backup_terminate(backup):
    'terminate a running backup'
    pass

@alias('delete')
@arg('-c', '--config', help='uses a configuration file (defaults: $HOME/.barman.conf, /etc/barman.conf)')
@arg('backup-id', help='specifies the backup ID')
def backup_delete(backup):
    'delete a backup'
    pass

@arg('-c', '--config', help='uses a configuration file (defaults: $HOME/.barman.conf, /etc/barman.conf)')
@arg('backup-id', help='specifies the backup ID')
@alias('recover')
def backup_recover(backup):
    'recover a backup'
    pass

def main():
    p = ArghParser()
    p.add_commands([list, cron])
    p.add_commands(
        [
            server_backup,
            server_list,
            server_status,
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
    p.dispatch()

if __name__ == '__main__':
    main()
