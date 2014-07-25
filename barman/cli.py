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
This module implements the interface with the command line and the logger.
"""

from argh import ArghParser, named, arg, expects_obj
from argparse import SUPPRESS, ArgumentTypeError
from barman import output
from barman.infofile import BackupInfo
from barman import lockfile
from barman.server import Server
import barman.diagnose
import barman.config
import logging
import os
import sys
from barman.utils import drop_privileges, configure_logging, parse_log_level

_logger = logging.getLogger(__name__)


def check_positive(value):
    """
    Check for a positive integer option

    :param value: str containing the value to check
    """
    if value is None:
        return None
    try:
        int_value = int(value)
    except Exception:
        raise ArgumentTypeError("'%s' is not a valid positive integer" % value)
    if int_value < 0:
        raise ArgumentTypeError("'%s' is not a valid positive integer" % value)
    return int_value

@named('list-server')
@arg('--minimal', help='machine readable output')
def list_server(minimal=False):
    """
    List available servers, with useful information
    """
    servers = get_server_list()
    for name in sorted(servers):
        server = servers[name]
        output.init('list_server', name, minimal=minimal)
        output.result('list_server', name, server.config.description)
    output.close_and_exit()


def cron():
    """
    Run maintenance tasks
    """
    lockname = os.path.join(barman.__config__.barman_home, '.cron.lock')
    try:
        with lockfile.LockFile(lockname, raise_if_fail=True):
            servers = [Server(conf) for conf in barman.__config__.servers()]
            for server in servers:
                server.cron()
    except lockfile.LockFileBusy:
        output.info("Another cron is running")

    except lockfile.LockFilePermissionDenied:
        output.error("Permission denied, unable to access '%s'",
                     lockname)
    output.close_and_exit()


# noinspection PyUnusedLocal
def server_completer(prefix, parsed_args, **kwargs):
    global_config(parsed_args)
    for conf in barman.__config__.servers():
        if conf.name.startswith(prefix) \
                and conf.name not in parsed_args.server_name:
            yield conf.name


# noinspection PyUnusedLocal
def server_completer_all(prefix, parsed_args, **kwargs):
    global_config(parsed_args)
    for conf in barman.__config__.servers():
        if conf.name.startswith(prefix) \
                and conf.name not in parsed_args.server_name:
            yield conf.name
    if 'server_name' in parsed_args \
            and parsed_args.server_name is None \
            and 'all'.startswith(prefix):
        yield 'all'


# noinspection PyUnusedLocal
def backup_completer(prefix, parsed_args, **kwargs):
    global_config(parsed_args)
    server = get_server(parsed_args)
    if server and len(parsed_args.backup_id) == 0:
        for backup_id in sorted(server.get_available_backups().iterkeys(),
                                reverse=True):
            if backup_id.startswith(prefix):
                yield backup_id
        for special_id in ('latest', 'last', 'oldest', 'first'):
            if len(server.get_available_backups()) > 0 \
                    and special_id.startswidth(prefix):
                yield special_id
    else:
        return


@arg('server_name', nargs='+',
     completer=server_completer_all,
     help="specifies the server names for the backup command "
          "('all' will show all available servers)")
@arg('--immediate-checkpoint',
     help='forces the initial checkpoint to be done as quickly as possible',
     dest='immediate_checkpoint',
     action='store_true',
     default=SUPPRESS)
@arg('--no-immediate-checkpoint',
     help='forces the initial checkpoint to be spreaded',
     dest='immediate_checkpoint',
     action='store_false',
     default=SUPPRESS)
@arg('--retry-times',
     help='Number of retries after an error if base backup copy fails.',
     type=check_positive)
@arg('--retry-sleep',
     help='Wait time after a failed base backup copy, before retrying.',
     type=check_positive)
@arg('--no-retry', help='Disable base backup copy retry logic.',
     dest='retry_times', action='store_const', const=0)
@expects_obj
def backup(args):
    """
    Perform a full backup for the given server
    """
    servers = get_server_list(args)
    for name in sorted(servers):
        server = servers[name]
        if server is None:
            output.error("Unknown server '%s'" % name)
            continue
        if args.retry_sleep is not None:
            server.config.basebackup_retry_sleep = args.retry_sleep
        if args.retry_times is not None:
            server.config.basebackup_retry_times = args.retry_times
        if hasattr(args, 'immediate_checkpoint'):
            server.config.immediate_checkpoint = args.immediate_checkpoint
        server.backup()

    output.close_and_exit()


@named('list-backup')
@arg('server_name', nargs='+',
     completer=server_completer_all,
     help="specifies the server name for the command "
          "('all' will show all available servers)")
@arg('--minimal', help='machine readable output', action='store_true')
@expects_obj
def list_backup(args):
    """
    List available backups for the given server (supports 'all')
    """
    servers = get_server_list(args)
    for name in sorted(servers):
        server = servers[name]
        output.init('list_backup', name, minimal=args.minimal)
        if server is None:
            output.error("Unknown server '%s'" % name)
            continue
        server.list_backups()
    output.close_and_exit()


@arg('server_name', nargs='+',
     completer=server_completer_all,
     help='specifies the server name for the command')
@expects_obj
def status(args):
    """
    Shows live information and status of the PostgreSQL server
    """
    servers = get_server_list(args)
    for name in sorted(servers):
        server = servers[name]
        if server is None:
            output.error("Unknown server '%s'" % name)
            continue
        output.init('status', name)
        server.status()
    output.close_and_exit()


@arg('server_name', nargs='+',
     completer=server_completer_all,
     help='specifies the server name for the command')
@expects_obj
def rebuild_xlogdb(args):
    """
    Rebuild the WAL file database guessing it from the disk content.
    """
    servers = get_server_list(args)
    ok = True
    for name in sorted(servers):
        server = servers[name]
        if server is None:
            yield "Unknown server '%s'" % (name)
            ok = False
            continue
        for line in server.rebuild_xlogdb():
            yield line
        yield ''
    if not ok:
        raise SystemExit(1)

@arg('server_name',
     completer=server_completer,
     help='specifies the server name for the command')
@arg('--target-tli', help='target timeline', type=int)
@arg('--target-time',
     help='target time. You can use any valid unambiguous representation. e.g: "YYYY-MM-DD HH:MM:SS.mmm"')
@arg('--target-xid', help='target transaction ID')
@arg('--target-name',
     help='target name created previously with pg_create_restore_point() function call')
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
@arg('--retry-times',
     help='Number of retries after an error if base backup copy fails.',
     type=check_positive)
@arg('--retry-sleep',
     help='Wait time after a failed base backup copy, before retrying.',
     type=check_positive)
@arg('--no-retry', help='Disable base backup copy retry logic.',
     dest='retry_times', action='store_const', const=0)
@expects_obj
def recover(args):
    """
    Recover a server at a given time or xid
    """
    server = get_server(args)
    if server is None:
        raise SystemExit("ERROR: unknown server '%s'" % (args.server_name))
    # Retrieves the backup
    backup = parse_backup_id(server, args)
    if backup is None or backup.status != BackupInfo.DONE:
        raise SystemExit("ERROR: unknown backup '%s' for server '%s'" % (
            args.backup_id, args.server_name))

    # decode the tablespace relocation rules
    tablespaces = {}
    if args.tablespace:
        for rule in args.tablespace:
            try:
                tablespaces.update([rule.split(':', 1)])
            except ValueError:
                raise SystemExit(
                    "ERROR: invalid tablespace relocation rule '%s'\n"
                    "HINT: the valid syntax for a relocation rule is "
                    "NAME:LOCATION" % rule)

    # validate the rules against the tablespace list
    valid_tablespaces = [tablespace_data.name for tablespace_data in
                         backup.tablespaces] if backup.tablespaces else []
    for item in tablespaces:
        if item not in valid_tablespaces:
            raise SystemExit("ERROR: invalid tablespace name '%s'\n"
                             "HINT: please use any of the following "
                             "tablespaces: %s"
                             % (item, ', '.join(valid_tablespaces)))

    # explicitly disallow the rsync remote syntax (common mistake)
    if ':' in args.destination_directory:
        raise SystemExit(
            "ERROR: the destination directory parameter cannot contain the ':' character\n"
            "HINT: if you want to do a remote recovery you have to use the --remote-ssh-command option")
    if args.retry_sleep is not None:
        server.config.basebackup_retry_sleep = args.retry_sleep
    if args.retry_times is not None:
        server.config.basebackup_retry_times = args.retry_times
    for line in server.recover(backup,
                               args.destination_directory,
                               tablespaces=tablespaces,
                               target_tli=args.target_tli,
                               target_time=args.target_time,
                               target_xid=args.target_xid,
                               target_name=args.target_name,
                               exclusive=args.exclusive,
                               remote_command=args.remote_ssh_command):

        yield line


@named('show-server')
@arg('server_name', nargs='+',
     completer=server_completer_all,
     help="specifies the server names to show ('all' will show all available servers)")
@expects_obj
def show_server(args):
    """
    Show all configuration parameters for the specified servers
    """
    servers = get_server_list(args)
    for name in sorted(servers):
        server = servers[name]
        if server is None:
            output.error("Unknown server '%s'" % name)
            continue
        output.init('show_server', name)
        server.show()
    output.close_and_exit()


@arg('server_name', nargs='+',
     completer=server_completer_all,
     help="specifies the server names to check "
          "('all' will check all available servers)")
@arg('--nagios', help='Nagios plugin compatible output', action='store_true')
@expects_obj
def check(args):
    """
    Check if the server configuration is working.

    This command returns success if every checks pass,
    or failure if any of these fails
    """
    if args.nagios:
        output.set_output_writer(output.NagiosOutputWriter())
    servers = get_server_list(args)
    for name in sorted(servers):
        server = servers[name]
        if server is None:
            output.error("Unknown server '%s'" % name)
            continue
        output.init('check', name)
        server.check()
    output.close_and_exit()

@expects_obj
def diagnose(args):
    """
    Diagnostic command (for support and problems detection purpose)
    """
    servers = get_server_list(None)
    barman.diagnose.exec_diagnose(servers)
    output.close_and_exit()

@named('show-backup')
@arg('server_name',
     completer=server_completer,
     help='specifies the server name for the command')
@arg('backup_id',
     completer=backup_completer,
     help='specifies the backup ID')
@expects_obj
def show_backup(args):
    """
    This method shows a single backup information
    """
    server = get_server(args)
    if server is None:
        output.error("Unknown server '%s'" % args.server_name)
    else:
        # Retrieves the backup
        backup_info = parse_backup_id(server, args)
        if backup_info is None:
            output.error("Unknown backup '%s' for server '%s'" % (
                args.backup_id, args.server_name))
        else:
            server.show_backup(backup_info)
    output.close_and_exit()


@named('list-files')
@arg('server_name',
     completer=server_completer,
     help='specifies the server name for the command')
@arg('backup_id',
     completer=backup_completer,
     help='specifies the backup ID')
@arg('--target', choices=('standalone', 'data', 'wal', 'full'),
     default='standalone',
     help='''
     Possible values are: data (just the data files), standalone (base backup files, including required WAL files),
     wal (just WAL files between the beginning of base backup and the following one (if any) or the end of the log) and
     full (same as data + wal). Defaults to %(default)s
     '''
)
@expects_obj
def list_files(args):
    """ List all the files for a single backup
    """
    server = get_server(args)
    if server is None:
        yield "Unknown server '%s'" % (args.server_name)
        return
    # Retrieves the backup
    backup = parse_backup_id(server, args)
    if backup is None:
        yield "Unknown backup '%s' for server '%s'" % (
            args.backup_id, args.server_name)
        return
    for line in backup.get_list_of_files(args.target):
        yield line


@arg('server_name',
     completer=server_completer,
     help='specifies the server name for the command')
@arg('backup_id',
     completer=backup_completer,
     help='specifies the backup ID')
@expects_obj
def delete(args):
    """ Delete a backup
    """
    server = get_server(args)
    if server is None:
        yield "Unknown server '%s'" % (args.server_name)
        return
    # Retrieves the backup
    backup = parse_backup_id(server, args)
    if backup is None:
        yield "Unknown backup '%s' for server '%s'" % (
            args.backup_id, args.server_name)
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
    """ Set the configuration file """
    if hasattr(args, 'config'):
        filename = args.config
    else:
        try:
            filename = os.environ['BARMAN_CONFIG_FILE']
        except KeyError:
            filename = None
    config = barman.config.Config(filename)
    barman.__config__ = config

    # change user if needed
    try:
        drop_privileges(config.user)
    except OSError:
        msg = "ERROR: please run barman as %r user" % config.user
        raise SystemExit(msg)
    except KeyError:
        msg = "ERROR: the configured user %r does not exists" % config.user
        raise SystemExit(msg)

    # configure logging
    log_level = parse_log_level(config.log_level)
    configure_logging(config.log_file,
                      log_level or barman.config.DEFAULT_LOG_LEVEL,
                      config.log_format)
    if log_level is None:
        _logger.warn('unknown log_level in config file: %s', config.log_level)

    # configure output
    if args.format != output.DEFAULT_WRITER or args.quiet or args.debug:
        output.set_output_writer(args.format,
                                 quiet=args.quiet,
                                 debug=args.debug)

    # Load additional configuration files
    _logger.debug('Loading additional configuration files')
    config.load_configuration_files_directory()
    # We must validate the configuration here in order to have
    # both output and logging configured
    config.validate_global_config()

    _logger.debug('Initialized Barman version %s (config: %s)',
                  barman.__version__, config.config_file)
    if hasattr(args, 'quiet') and args.quiet:
        _logger.debug("Replacing output stream")
        global _output_stream
        _output_stream.set_stream(open(os.devnull, 'w'))


def get_server(args):
    """
    Get a single server from the configuration

    :param args: an argparse namespace containing a single server_name parameter
    """
    config = barman.__config__.get_server(args.server_name)
    if not config:
        return None
    return Server(config)


def get_server_list(args=None):
    """
    Get the server list from the configuration

    If args the parameter is None or arg.server_name[0] is 'all'
    returns all defined servers

    :param args: an argparse namespace containing a list server_name parameter
    """
    if args is None or args.server_name[0] == 'all':
        return dict(
            (conf.name, Server(conf)) for conf in barman.__config__.servers())
    else:
        server_dict = {}
        for server in args.server_name:
            conf = barman.__config__.get_server(server)
            if conf == None:
                server_dict[server] = None
            else:
                server_dict[server] = Server(conf)
        return server_dict


def parse_backup_id(server, args):
    """
    Parses backup IDs including special words such as latest, oldest, etc.

    :param Server server: server object to search for the required backup
    :param args: command lien arguments namespace
    :rtype BackupInfo,None: the decoded backup_info object
    """
    if args.backup_id in ('latest', 'last'):
        backup_id = server.get_last_backup()
    elif args.backup_id in ('oldest', 'first'):
        backup_id = server.get_first_backup()
    else:
        backup_id = args.backup_id
    backup_info = server.get_backup(backup_id) if backup_id else None
    return backup_info


def main():
    """
    The main method of Barman
    """
    p = ArghParser()
    p.add_argument('-v', '--version', action='version',
                   version=barman.__version__)
    p.add_argument('-c', '--config',
                   help='uses a configuration file '
                        '(defaults: %s)'
                        % ', '.join(barman.config.Config.CONFIG_FILES),
                   default=SUPPRESS)
    p.add_argument('-q', '--quiet', help='be quiet', action='store_true')
    p.add_argument('-d', '--debug', help='debug output', action='store_true')
    p.add_argument('-f', '--format', help='output format',
                   choices=output.AVAILABLE_WRITERS.keys(),
                   default=output.DEFAULT_WRITER)
    p.add_commands(
        [
            cron,
            list_server,
            show_server,
            status,
            check,
            diagnose,
            backup,
            list_backup,
            show_backup,
            list_files,
            recover,
            delete,
            rebuild_xlogdb,
        ]
    )
    # noinspection PyBroadException
    try:
        p.dispatch(pre_call=global_config, output_file=_output_stream)
    except KeyboardInterrupt:
        msg = "Process interrupted by user (KeyboardInterrupt)"
        output.exception(msg)
    except Exception, e:
        msg = "%s\nSee log file for more details." % e
        output.exception(msg)

    # cleanup output API and exit honoring output.error_occurred and
    # output.error_exit_code
    output.close_and_exit()


if __name__ == '__main__':
    # This code requires the mock module and allow us to test
    # bash completion inside the IDE debugger
    try:
        import mock
        sys.stdout = mock.Mock(wraps=sys.stdout)
        sys.stdout.isatty.return_value = True
        os.dup2(2, 8)
    except ImportError:
        pass
    main()
