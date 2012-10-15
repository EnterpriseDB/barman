# Copyright (C) 2011, 2012 2ndQuadrant Italia (Devise.IT S.r.L.)
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

''' This module implements the interface with the command line
and the logger.
'''

from argh import ArghParser, alias, arg
from barman.lockfile import lockfile
from barman.server import Server
import barman.config
import logging
import os
import sys
from barman.backup import BackupInfo

_logger = logging.getLogger(__name__)

@alias('list-server')
@arg('--minimal', help='machine readable output', action='store_true')
def list_server(args):
    """ List available servers, with useful information
    """
    servers = barman.__config__.servers()
    for server in servers:
        if server.description and not args.minimal:
            yield "%s - %s" % (server.name, server.description)
        else:
            yield server.name

def cron(args):
    """ Run maintenance tasks
    """
    filename = os.path.join(barman.__config__.barman_home, '.cron.lock')
    with lockfile(filename) as locked:
        if not locked:
            yield "ERROR: Another cron is running"
            raise SystemExit, 1
        else:
            servers = [ Server(conf) for conf in barman.__config__.servers()]
            for server in servers:
                for lines in server.cron(verbose=True):
                    yield lines

@arg('server_name', nargs='+', help="specifies the server names for the backup command ('all' will show all available servers)")
def backup(args):
    """ Perform a full backup for the given server
    """
    servers = get_server_list(args)
    for name, server in servers.items():
        if server == None:
            yield "Unknown server '%s'" % (name)
            continue
        for line in server.backup():
            yield line
        yield ''

@alias('list-backup')
@arg('server_name', help='specifies the server name for the command')
@arg('--minimal', help='machine readable output', action='store_true')
def list_backup(args):
    """ List available backups for the given server
    """
    server = get_server(args)
    if server == None:
        yield "Unknown server '%s'" % (args.server_name)
        return
    if not args.minimal:
        for line in server.list_backups():
            yield line
    else:
        for backup_id in sorted(server.get_available_backups().iterkeys(), reverse=True):
            yield backup_id

@arg('server_name', nargs='+', help='specifies the server name for the command')
def status(args):
    """ Shows live information and status of the PostgreSQL server
    """
    servers = get_server_list(args)

    for name, server in servers.items():
        if server == None:
            yield "Unknown server '%s'" % (name)
            continue
        for line in server.status():
            yield line
        yield ''


@arg('server_name', help='specifies the server name for the command')
@arg('--target-tli', help='target timeline', type=int)
@arg('--target-time',
     help='target time. You can use any valid unambiguous representation. e.g: "YYYY-MM-DD HH:MM:SS.mmm"')
@arg('--target-xid', help='target xid')
@arg('--exclusive',
     help='set target xid to be non inclusive', action="store_true")
@arg('--tablespace',
     help='tablespace relocation rule',
     metavar='NAME:LOCATION', action='append')
@arg('--remote-ssh-command',
     metavar='SSH_COMMAND',
     help='This options activates remote recovery, by specifying the secure shell command '
     'to be launched on a remote host. It is the equivalent of the "ssh_command" server'
     'option in the configuration file for remote recovery. Example: "ssh postgres@db2"')
@arg('backup_id',
     help='specifies the backup ID to recover')
@arg('destination_directory',
     help='the directory where the new server is created')
def recover(args):
    """ Recover a server at a given time or xid
    """
    server = get_server(args)
    if server == None:
        raise SystemExit("ERROR: unknown server '%s'" % (args.server_name))
    # Retrieves the backup info
    backup = server.get_backup(args.backup_id)
    if backup == None or backup.status != BackupInfo.DONE:
        raise SystemExit("ERROR: unknown backup '%s' for server '%s'" % (args.backup_id, args.server_name))
    # decode the tablespace relocation rules
    tablespaces = {}
    if args.tablespace:
        for rule in args.tablespace:
            try:
                tablespaces.update([rule.split(':', 1)])
            except:
                raise SystemExit("ERROR: invalid tablespace relocation rule '%s'\n"
                                 "HINT: the valid syntax for a relocation rule is NAME:LOCATION" % rule)
    # validate the rules against the tablespace list
    valid_tablespaces = [tablespace_data[0] for tablespace_data in backup.tablespaces] if backup.tablespaces else []
    for tablespace in tablespaces:
        if tablespace not in valid_tablespaces:
            raise SystemExit("ERROR: invalid tablespace name '%s'\n"
                             "HINT: please use any of the following tablespaces: %s" 
                             % (tablespace, ', '.join(valid_tablespaces)))
    # explicitly disallow the rsync remote syntax (common mistake)
    if ':' in args.destination_directory:
        raise SystemExit(
            "ERROR: the destination directory parameter cannot contain the ':' character\n"
            "HINT: if you want to do a remote recovery you have to use the --remote-ssh-command option")
    if args.remote_ssh_command and len(tablespaces) > 0:
        raise SystemExit("ERROR: Tablespace relocation is not supported with remote recovery")
    for line in server.recover(backup,
                               args.destination_directory,
                               tablespaces=tablespaces,
                               target_tli=args.target_tli,
                               target_time=args.target_time,
                               target_xid=args.target_xid,
                               exclusive=args.exclusive,
                               remote_command=args.remote_ssh_command
                               ):
        yield line

@alias('show-server')
@arg('server_name', nargs='+', help="specifies the server names to show ('all' will show all available servers)")
def show_server(args):
    """ Show all configuration parameters for the specified servers
    """
    servers = get_server_list(args)

    for name, server in servers.items():
        if server == None:
            yield "Unknown server '%s'" % (name)
            continue
        for line in server.show():
            yield line
        yield ''

@arg('server_name', nargs='+', help="specifies the server names to check ('all' will check all available servers)")
def check(args):
    """ Check if the server configuration is working.
    This function returns 0 if every checks pass, or 0 if any of these fails
    """
    servers = get_server_list(args)

    ok = True
    for name, server in servers.items():
        if server == None:
            yield "Unknown server '%s'" % (name)
            ok = False
            continue
        for line, status in server.check():
            ok &= status
            yield line
        yield ''
        if not ok:
            raise SystemExit(1)

@alias('show-backup')
@arg('server_name', help='specifies the server name for the command')
@arg('backup_id', help='specifies the backup ID')
def show_backup(args):
    """ This method Shows a single backup information
    """
    server = get_server(args)
    if server == None:
        yield "Unknown server '%s'" % (args.server_name)
        return
    # Retrieves the backup info
    backup = server.get_backup(args.backup_id)
    if backup == None:
        yield "Unknown backup '%s' for server '%s'" % (args.backup_id, args.server_name)
        return
    for line in backup.show():
        yield line

@alias('list-files')
@arg('server_name', help='specifies the server name for the command')
@arg('backup_id', help='specifies the backup ID')
@arg('--target', choices=('standalone', 'data', 'wal', 'full'), default='standalone',
     help='''
     Possible values are: data (just the data files), standalone (base backup files, including required WAL files),
     wal (just WAL files between the beginning of base backup and the following one (if any) or the end of the log) and
     full (same as data + wal). Defaults to %(default)s
     '''
     )
def list_files(args):
    """ List all the files for a single backup
    """
    server = get_server(args)
    if server == None:
        yield "Unknown server '%s'" % (args.server_name)
        return
    # Retrieves the backup info
    backup = server.get_backup(args.backup_id)
    if backup == None:
        yield "Unknown backup '%s' for server '%s'" % (args.backup_id, args.server_name)
        return
    for line in backup.get_list_of_files(args.target):
        yield line

@arg('server_name', help='specifies the server name for the command')
@arg('backup_id', help='specifies the backup ID')
def delete(args):
    """ Delete a backup
    """
    server = get_server(args)
    if server == None:
        yield "Unknown server '%s'" % (args.server_name)
        return
    # Retrieves the backup info
    backup = server.get_backup(args.backup_id)
    if backup == None:
        yield "Unknown backup '%s' for server '%s'" % (args.backup_id, args.server_name)
        return
    for line in server.delete_backup(backup):
        yield line


class stream_wrapper(object):
    """ This class represents a wrapper for a stream
    """
    def __init__(self, stream):
        self.stream = stream

    def set_stream(self, stream):
        ''' Set the stream as stream argument '''
        self.stream = stream

    def __getattr__(self, attr):
        return getattr(self.stream, attr)

_output_stream = stream_wrapper(sys.stdout)

def global_config(args):
    ''' Set the configuration file '''
    if hasattr(args, 'config'):
        filename = args.config
    else:
        try:
            filename = os.environ['BARMAN_CONFIG_FILE']
        except KeyError:
            filename = None
    barman.__config__ = barman.config.Config(filename)
    _logger.debug('Initialized Barman version %s (config: %s)',
                 barman.__version__, barman.__config__.config_file)
    if hasattr(args, 'quiet') and args.quiet:
        _logger.debug("Replacing output stream ")
        global _output_stream
        _output_stream.set_stream(open(os.devnull, 'w'))


def get_server(args):
    ''' Get the server from the configuration '''
    config = barman.__config__.get_server(args.server_name)
    if not config:
        return None
    return Server(config)

def get_server_list(args):
    ''' Get the server list from the configuration '''
    if args.server_name[0] == 'all':
        return dict((conf.name, Server(conf)) for conf in barman.__config__.servers())
    else:
        server_dict = {}
        for server in args.server_name:
            conf = barman.__config__.get_server(server)
            if conf == None:
                server_dict[server] = None
            else:
                server_dict[server] = Server(conf)
        return server_dict

def main():
    ''' The main method of Barman '''
    p = ArghParser()
    p.add_argument('-v', '--version', action='version', version=barman.__version__)
    p.add_argument('-c', '--config', help='uses a configuration file (defaults: $HOME/.barman.conf, /etc/barman.conf)')
    p.add_argument('-q', '--quiet', help='be quiet', action='store_true')
    p.add_commands(
        [
            cron,
            list_server,
            show_server,
            status,
            check,
            backup,
            list_backup,
            show_backup,
            list_files,
            recover,
            delete,
        ]
    )
    try:
        p.dispatch(pre_call=global_config, output_file=_output_stream)
    except Exception:
        msg = "ERROR: Unhandled exception. See log file for more details."
        logging.exception(msg)
        raise SystemExit(msg)

if __name__ == '__main__':
    main()
