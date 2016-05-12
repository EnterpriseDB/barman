# Copyright (C) 2011-2016 2ndQuadrant Italia Srl
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

import logging
import os
import sys
from argparse import SUPPRESS, ArgumentTypeError
from contextlib import closing

from argh import ArghParser, arg, expects_obj, named

import barman.config
import barman.diagnose
from barman import output
from barman.infofile import BackupInfo
from barman.server import Server
from barman.utils import configure_logging, drop_privileges, parse_log_level
from barman.xlog import BadXlogSegmentName

_logger = logging.getLogger(__name__)


def check_non_negative(value):
    """
    Check for a positive integer option

    :param value: str containing the value to check
    """
    if value is None:
        return None
    try:
        int_value = int(value)
    except Exception:
        raise ArgumentTypeError("'%s' is not a valid non negative integer" %
                                value)
    if int_value < 0:
        raise ArgumentTypeError("'%s' is not a valid non negative integer" %
                                value)
    return int_value


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
        raise ArgumentTypeError("'%s' is not a valid positive integer" %
                                value)
    if int_value < 1:
        raise ArgumentTypeError("'%s' is not a valid positive integer" %
                                value)
    return int_value


@named('list-server')
@arg('--minimal', help='machine readable output')
def list_server(minimal=False):
    """
    List available servers, with useful information
    """
    # Get every server, both inactive and temporarily disabled
    servers = get_server_list()
    for name in sorted(servers):
        server = servers[name]

        # Exception: manage_server_command is not invoked here
        # Normally you would call manage_server_command to check if the
        # server is None and to report inactive and disabled servers, but here
        # we want all servers and the server cannot be None

        output.init('list_server', name, minimal=minimal)
        description = server.config.description
        # If the server has been manually disabled
        if not server.config.active:
            description += " (inactive)"
        # If server has configuration errors
        elif server.config.disabled:
            description += " (WARNING: disabled)"
        output.result('list_server', name, description)
    output.close_and_exit()


def cron():
    """
    Run maintenance tasks (global command)
    """
    # Skip inactive and temporarily disabled servers
    servers = get_server_list(skip_inactive=True, skip_disabled=True)
    for name in sorted(servers):
        server = servers[name]

        # Exception: manage_server_command is not invoked here
        # Normally you would call manage_server_command to check if the
        # server is None and to report inactive and disabled servers,
        # but here we have only active and well configured servers.

        server.cron()

    output.close_and_exit()


# noinspection PyUnusedLocal
def server_completer(prefix, parsed_args, **kwargs):
    global_config(parsed_args)
    for conf in barman.__config__.servers():
        if conf.name.startswith(prefix):
            yield conf.name


# noinspection PyUnusedLocal
def server_completer_all(prefix, parsed_args, **kwargs):
    global_config(parsed_args)
    current_list = getattr(parsed_args, 'server_name', None) or ()
    for conf in barman.__config__.servers():
        if conf.name.startswith(prefix) and conf.name not in current_list:
            yield conf.name
    if len(current_list) == 0 and 'all'.startswith(prefix):
        yield 'all'


# noinspection PyUnusedLocal
def backup_completer(prefix, parsed_args, **kwargs):
    global_config(parsed_args)
    server = get_server(parsed_args)

    backups = server.get_available_backups()
    for backup_id in sorted(backups, reverse=True):
        if backup_id.startswith(prefix):
            yield backup_id
    for special_id in ('latest', 'last', 'oldest', 'first'):
        if len(backups) > 0 and special_id.startswith(prefix):
            yield special_id


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
@arg('--reuse-backup', nargs='?',
     choices=barman.config.REUSE_BACKUP_VALUES,
     default=None, const='link',
     help='use the previous backup to improve transfer-rate. '
          'If no argument is given "link" is assumed')
@arg('--retry-times',
     help='Number of retries after an error if base backup copy fails.',
     type=check_non_negative)
@arg('--retry-sleep',
     help='Wait time after a failed base backup copy, before retrying.',
     type=check_non_negative)
@arg('--no-retry', help='Disable base backup copy retry logic.',
     dest='retry_times', action='store_const', const=0)
@expects_obj
def backup(args):
    """
    Perform a full backup for the given server (supports 'all')
    """
    servers = get_server_list(args, skip_inactive=True)
    for name in sorted(servers):
        server = servers[name]

        # Skip the server (apply general rule)
        if not manage_server_command(server, name):
            continue

        if args.reuse_backup is not None:
            server.config.reuse_backup = args.reuse_backup
        if args.retry_sleep is not None:
            server.config.basebackup_retry_sleep = args.retry_sleep
        if args.retry_times is not None:
            server.config.basebackup_retry_times = args.retry_times
        if hasattr(args, 'immediate_checkpoint'):
            server.config.immediate_checkpoint = args.immediate_checkpoint
        with closing(server):
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
    servers = get_server_list(args, skip_inactive=True)
    for name in sorted(servers):
        server = servers[name]

        # Skip the server (apply general rule)
        if not manage_server_command(server, name):
            continue

        output.init('list_backup', name, minimal=args.minimal)
        with closing(server):
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
    servers = get_server_list(args, skip_inactive=True)
    for name in sorted(servers):
        server = servers[name]

        # Skip the server (apply general rule)
        if not manage_server_command(server, name):
            continue

        output.init('status', name)
        with closing(server):
            server.status()
    output.close_and_exit()


@named('replication-status')
@arg('server_name', nargs='+',
     completer=server_completer_all,
     help='specifies the server name for the command')
@arg('--minimal', help='machine readable output', action='store_true')
@arg('--target', choices=('all', 'hot-standby', 'wal-streamer'),
     default='all',
     help='''
         Possible values are: 'hot-standby' (only hot standby servers),
         'wal-streamer' (only WAL streaming clients, such as pg_receivexlog),
         'all' (any of them). Defaults to %(default)s''')
@expects_obj
def replication_status(args):
    """
    Shows live information and status of any streaming client
    """
    servers = get_server_list(args, skip_inactive=True)
    for name in sorted(servers):
        server = servers[name]

        # Skip the server (apply general rule)
        if not manage_server_command(server, name):
            continue

        with closing(server):
            output.init('replication_status',
                        name,
                        minimal=args.minimal)
            server.replication_status(args.target)
    output.close_and_exit()


@arg('server_name', nargs='+',
     completer=server_completer_all,
     help='specifies the server name for the command')
@expects_obj
def rebuild_xlogdb(args):
    """
    Rebuild the WAL file database guessing it from the disk content.
    """
    servers = get_server_list(args, skip_inactive=True)
    for name in sorted(servers):
        server = servers[name]

        # Skip the server (apply general rule)
        if not manage_server_command(server, name):
            continue

        with closing(server):
            server.rebuild_xlogdb()
    output.close_and_exit()


@arg('server_name',
     completer=server_completer,
     help='specifies the server name for the command')
@arg('--target-tli', help='target timeline', type=check_positive)
@arg('--target-time',
     help='target time. You can use any valid unambiguous representation. '
          'e.g: "YYYY-MM-DD HH:MM:SS.mmm"')
@arg('--target-xid', help='target transaction ID')
@arg('--target-name',
     help='target name created previously with '
          'pg_create_restore_point() function call')
@arg('--exclusive',
     help='set target xid to be non inclusive', action="store_true")
@arg('--tablespace',
     help='tablespace relocation rule',
     metavar='NAME:LOCATION', action='append')
@arg('--remote-ssh-command',
     metavar='SSH_COMMAND',
     help='This options activates remote recovery, by specifying the secure '
          'shell command to be launched on a remote host. It is '
          'the equivalent of the "ssh_command" server option in '
          'the configuration file for remote recovery. '
          'Example: "ssh postgres@db2"')
@arg('backup_id',
     completer=backup_completer,
     help='specifies the backup ID to recover')
@arg('destination_directory',
     help='the directory where the new server is created')
@arg('--retry-times',
     help='Number of retries after an error if base backup copy fails.',
     type=check_non_negative)
@arg('--retry-sleep',
     help='Wait time after a failed base backup copy, before retrying.',
     type=check_non_negative)
@arg('--no-retry', help='Disable base backup copy retry logic.',
     dest='retry_times', action='store_const', const=0)
@expects_obj
def recover(args):
    """
    Recover a server at a given time or xid
    """
    server = get_server(args)

    # Retrieves the backup
    backup_id = parse_backup_id(server, args)
    if backup_id.status != BackupInfo.DONE:
        output.error(
            "Cannot recover from backup '%s' of server '%s': "
            "backup status is not DONE",
            args.backup_id, server.config.name)
        output.close_and_exit()

    # decode the tablespace relocation rules
    tablespaces = {}
    if args.tablespace:
        for rule in args.tablespace:
            try:
                tablespaces.update([rule.split(':', 1)])
            except ValueError:
                output.error(
                    "Invalid tablespace relocation rule '%s'\n"
                    "HINT: The valid syntax for a relocation rule is "
                    "NAME:LOCATION", rule)
                output.close_and_exit()

    # validate the rules against the tablespace list
    valid_tablespaces = []
    if backup_id.tablespaces:
        valid_tablespaces = [tablespace_data.name for tablespace_data in
                             backup_id.tablespaces]
    for item in tablespaces:
        if item not in valid_tablespaces:
            output.error("Invalid tablespace name '%s'\n"
                         "HINT: Please use any of the following "
                         "tablespaces: %s",
                         item, ', '.join(valid_tablespaces))
            output.close_and_exit()

    # explicitly disallow the rsync remote syntax (common mistake)
    if ':' in args.destination_directory:
        output.error(
            "The destination directory parameter "
            "cannot contain the ':' character\n"
            "HINT: If you want to do a remote recovery you have to use "
            "the --remote-ssh-command option")
        output.close_and_exit()
    if args.retry_sleep is not None:
        server.config.basebackup_retry_sleep = args.retry_sleep
    if args.retry_times is not None:
        server.config.basebackup_retry_times = args.retry_times
    with closing(server):
        server.recover(backup_id,
                       args.destination_directory,
                       tablespaces=tablespaces,
                       target_tli=args.target_tli,
                       target_time=args.target_time,
                       target_xid=args.target_xid,
                       target_name=args.target_name,
                       exclusive=args.exclusive,
                       remote_command=args.remote_ssh_command)

    output.close_and_exit()


@named('show-server')
@arg('server_name', nargs='+',
     completer=server_completer_all,
     help="specifies the server names to show "
          "('all' will show all available servers)")
@expects_obj
def show_server(args):
    """
    Show all configuration parameters for the specified servers
    """
    servers = get_server_list(args, skip_inactive=True)
    for name in sorted(servers):
        server = servers[name]

        # Skip the server (apply general rule)
        if not manage_server_command(server, name):
            continue

        output.init('show_server', name)
        with closing(server):
            server.show()
    output.close_and_exit()


@named('switch-xlog')
@arg('server_name', nargs='+',
     completer=server_completer_all,
     help="specifies the server name target of the switch-xlog command")
@arg('--force',
     help='forces the switch of a xlog by executing a checkpoint before',
     dest='force',
     action='store_true',
     default=False)
@expects_obj
def switch_xlog(args):
    """
    Execute the switch-xlog command on the target server
    """
    servers = get_server_list(args, skip_inactive=True)
    for name in sorted(servers):
        server = servers[name]
        # Skip the server (apply general rule)
        if not manage_server_command(server, name):
            continue
        with closing(server):
            server.switch_xlog(args.force)
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

        # Validate the returned server
        if not manage_server_command(
                server, name, skip_inactive=False,
                skip_disabled=False, disabled_is_error=False):
            continue

        # If the server has been manually disabled
        if not server.config.active:
            name += " (inactive)"
        # If server has configuration errors
        elif server.config.disabled:
            name += " (WARNING: disabled)"
        output.init('check', name, server.config.active)
        with closing(server):
            server.check()
    output.close_and_exit()


def diagnose():
    """
    Diagnostic command (for support and problems detection purpose)
    """
    # Get every server (both inactive and temporarily disabled)
    servers = get_server_list(on_error_stop=False, suppress_error=True)
    # errors list with duplicate paths between servers
    errors_list = barman.__config__.servers_msg_list
    barman.diagnose.exec_diagnose(servers, errors_list)
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

    # Retrieves the backup
    backup_info = parse_backup_id(server, args)
    with closing(server):
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
         Possible values are: data (just the data files), standalone
         (base backup files, including required WAL files),
         wal (just WAL files between the beginning of base
         backup and the following one (if any) or the end of the log) and
         full (same as data + wal). Defaults to %(default)s''')
@expects_obj
def list_files(args):
    """
    List all the files for a single backup
    """
    server = get_server(args)

    # Retrieves the backup
    backup_info = parse_backup_id(server, args)
    try:
        for line in backup_info.get_list_of_files(args.target):
            output.info(line, log=False)
    except BadXlogSegmentName as e:
        output.error(
            "invalid xlog segment name %r\n"
            "HINT: Please run \"barman rebuild-xlogdb %s\" "
            "to solve this issue",
            str(e), server.config.name)
        output.close_and_exit()


@arg('server_name',
     completer=server_completer,
     help='specifies the server name for the command')
@arg('backup_id',
     completer=backup_completer,
     help='specifies the backup ID')
@expects_obj
def delete(args):
    """
    Delete a backup
    """
    server = get_server(args)

    # Retrieves the backup
    backup_id = parse_backup_id(server, args)
    with closing(server):
        server.delete_backup(backup_id)
    output.close_and_exit()


@named('get-wal')
@arg('server_name',
     completer=server_completer,
     help='specifies the server name for the command')
@arg('wal_name',
     help='the WAL file to get')
@arg('--output-directory', '-o',
     help='put the retrieved WAL file in this directory '
          'with the original name',
     default=SUPPRESS)
@arg('--gzip', '-z', '-x',
     help='compress the output with gzip',
     action='store_const', const='gzip', dest='compression', default=SUPPRESS)
@arg('--bzip2', '-j',
     help='compress the output with bzip2',
     action='store_const', const='bzip2', dest='compression', default=SUPPRESS)
@arg('--peek', '-p',
     help="peek from the WAL archive up to 'SIZE' WAL files, starting "
          "from the requested one. 'SIZE' must be an integer >= 1. "
          "When invoked with this option, get-wal returns a list of "
          "zero to 'SIZE' WAL segment names, one per row.",
     metavar='SIZE',
     type=check_positive,
     default=SUPPRESS)
@expects_obj
def get_wal(args):
    """
    Retrieve WAL_NAME file from SERVER_NAME archive.
    The content will be streamed on standard output unless
    the --output-directory option is specified.
    """
    server = get_server(args)

    # Retrieve optional arguments. If an argument is not specified,
    # the namespace doesn't contain it due to SUPPRESS default.
    # In that case we pick 'None' using getattr third argument.
    compression = getattr(args, 'compression', None)
    output_directory = getattr(args, 'output_directory', None)
    peek = getattr(args, 'peek', None)

    with closing(server):
        server.get_wal(args.wal_name,
                       compression=compression,
                       output_directory=output_directory,
                       peek=peek)
    output.close_and_exit()


@named('archive-wal')
@arg('server_name',
     completer=server_completer,
     help='specifies the server name for the command')
@expects_obj
def archive_wal(args):
    """
    Execute maintenance operations on WAL files for a given server.
    This command processes any incoming WAL files for the server
    and archives them along the catalogue.

    """
    server = get_server(args)
    with closing(server):
        server.archive_wal()
    output.close_and_exit()


@named('receive-wal')
@arg('--stop', help='stop the receive-wal subprocess for the server',
     action='store_true')
@arg('--reset', help='reset the status of receive-wal removing '
                     'any status files',
     action='store_true')
@arg('server_name',
     completer=server_completer,
     help='specifies the server name for the command')
@expects_obj
def receive_wal(args):
    """
    Start a receive-wal process.
    The process uses the streaming protocol to receive WAL files
    from the PostgreSQL server.
    """
    server = get_server(args)
    if args.stop and args.reset:
        output.error("--stop and --reset options are not compatible")
    # If the caller requested to shutdown the receive-wal process deliver the
    # termination signal, otherwise attempt to start it
    elif args.stop:
        server.kill('receive-wal')
    else:
        with closing(server):
            server.receive_wal(reset=args.reset)
    output.close_and_exit()


def pretty_args(args):
    """
    Prettify the given argh namespace to be human readable

    :type args: argh.dispatching.ArghNamespace
    :return: the human readable content of the namespace
    """
    values = dict(vars(args))
    # Retrieve the command name with recent argh versions
    if '_functions_stack' in values:
        values['command'] = values['_functions_stack'][0].__name__
        del values['_functions_stack']
    # Older argh versions only have the matching function in the namespace
    elif 'function' in values:
        values['command'] = values['function'].__name__
        del values['function']
    return "%r" % values


def global_config(args):
    """
    Set the configuration file
    """
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
    config.load_configuration_files_directory()
    # We must validate the configuration here in order to have
    # both output and logging configured
    config.validate_global_config()

    _logger.debug('Initialised Barman version %s (config: %s, args: %s)',
                  barman.__version__, config.config_file, pretty_args(args))


def get_server(args, skip_inactive=True, skip_disabled=False,
               on_error_stop=True, suppress_error=False):
    """
    Get a single server retrieving its configuration (wraps get_server_list())

    Returns a Server object or None if the required server is unknown and
    on_error_stop is False.

    WARNING: this function modifies the 'args' parameter

    :param args: an argparse namespace containing a single
        server_name parameter
        WARNING: the function modifies the content of this parameter
    :param bool skip_inactive: skip inactive servers when 'all' is required
    :param bool skip_disabled: skip disabled servers when 'all' is required
    :param bool on_error_stop: stop if an error is found
    :param bool suppress_error: suppress display of errors (e.g. diagnose)
    :rtype: barman.server.Server|None
    """
    # This function must to be called with in a single-server context
    name = args.server_name
    assert isinstance(name, str)

    # The 'all' special name is forbidden in this context
    if name == 'all':
        output.error("You cannot use 'all' in a single server context")
        output.close_and_exit()
        # The following return statement will never be reached
        # but it is here for clarity
        return None

    # Builds a list from a single given name
    args.server_name = [name]

    # Retrieve the requested server
    servers = get_server_list(args, skip_inactive, skip_disabled,
                              on_error_stop, suppress_error)

    # The requested server has been excluded from get_server_list result
    if len(servers) == 0:
        output.close_and_exit()
        # The following return statement will never be reached
        # but it is here for clarity
        return None

    # retrieve the server object
    server = servers[name]

    # Apply standard validation control and skips
    # the server if inactive or disabled, displaying standard
    # error messages. If on_error_stop (default) exits
    if not manage_server_command(server, name) and on_error_stop:
        output.close_and_exit()
        # The following return statement will never be reached
        # but it is here for clarity
        return None

    # Returns the filtered server
    return server


def get_server_list(args=None, skip_inactive=False, skip_disabled=False,
                    on_error_stop=True, suppress_error=False):
    """
    Get the server list from the configuration

    If args the parameter is None or arg.server_name is ['all']
    returns all defined servers

    :param args: an argparse namespace containing a list server_name parameter
    :param bool skip_inactive: skip inactive servers when 'all' is required
    :param bool skip_disabled: skip disabled servers when 'all' is required
    :param bool on_error_stop: stop if an error is found
    :param bool suppress_error: suppress display of errors (e.g. diagnose)
    :rtype: dict(str,barman.server.Server|None)
    """
    server_dict = {}

    # This function must to be called with in a multiple-server context
    assert not args or isinstance(args.server_name, list)

    # Generate the list of servers (required for global errors)
    available_servers = barman.__config__.server_names()

    # Get a list of configuration errors from all the servers
    global_error_list = barman.__config__.servers_msg_list

    # Global errors have higher priority
    if global_error_list:
        # Output the list of global errors
        if not suppress_error:
            for error in global_error_list:
                output.error(error)

        # If requested, exit on first error
        if on_error_stop:
            output.close_and_exit()
            # The following return statement will never be reached
            # but it is here for clarity
            return {}

    # Handle special 'all' server cases
    # - args is None
    # - 'all' special name
    if not args or 'all' in args.server_name:
        # When 'all' is used, it must be the only specified argument
        if args and len(args.server_name) != 1:
            output.error("You cannot use 'all' with other server names")
        servers = available_servers
    else:
        servers = args.server_name

    # Loop through all the requested servers
    for server in servers:
        conf = barman.__config__.get_server(server)
        if conf is None:
            # Unknown server
            server_dict[server] = None
        else:
            server_object = Server(conf)
            # Skip inactive servers, if requested
            if skip_inactive and not server_object.config.active:
                output.info("Skipping inactive server '%s'"
                            % conf.name)
                continue
            # Skip disabled servers, if requested
            if skip_disabled and server_object.config.disabled:
                output.info("Skipping temporarily disabled server '%s'"
                            % conf.name)
                continue
            server_dict[server] = server_object

    return server_dict


def manage_server_command(server,
                          name=None,
                          inactive_is_error=False,
                          disabled_is_error=True,
                          skip_inactive=True,
                          skip_disabled=True):
    """
    Standard and consistent method for managing server errors within
    a server command execution. By default, suggests to skip any inactive
    and disabled server; it also emits errors for disabled servers by
    default.

    Returns True if the command has to be executed for this server.

    :param barman.server.Server server: server to be checked for errors
    :param str name: name of the server, in a multi-server command
    :param bool inactive_is_error: treat inactive server as error
    :param bool disabled_is_error: treat disabled server as error
    :param bool skip_inactive: skip if inactive
    :param bool skip_disabled: skip if disabled
    :return: True if the command has to be executed on this server
    :rtype: boolean
    """

    # Unknown server (skip it)
    if not server:
        output.error("Unknown server '%s'" % name)
        return False

    if not server.config.active:
        # Report inactive server as error
        if inactive_is_error:
            output.error('Inactive server: %s' % server.config.name)
        if skip_inactive:
            return False

    # Report disabled server as error
    if server.config.disabled:
        # Output all the messages as errors, and exit terminating the run.
        if disabled_is_error:
            for message in server.config.msg_list:
                output.error(message)
        if skip_disabled:
            return False

    # All ok, execute the command
    return True


def parse_backup_id(server, args):
    """
    Parses backup IDs including special words such as latest, oldest, etc.

    Exit with error if the backup id doesn't exist.

    :param Server server: server object to search for the required backup
    :param args: command lien arguments namespace
    :rtype: BackupInfo
    """
    if args.backup_id in ('latest', 'last'):
        backup_id = server.get_last_backup_id()
    elif args.backup_id in ('oldest', 'first'):
        backup_id = server.get_first_backup_id()
    else:
        backup_id = args.backup_id
    backup_info = server.get_backup(backup_id)
    if backup_info is None:
        output.error(
            "Unknown backup '%s' for server '%s'",
            args.backup_id, server.config.name)
        output.close_and_exit()
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
            archive_wal,
            backup,
            check,
            cron,
            delete,
            diagnose,
            get_wal,
            list_backup,
            list_files,
            list_server,
            rebuild_xlogdb,
            receive_wal,
            recover,
            show_backup,
            show_server,
            replication_status,
            status,
            switch_xlog,
        ]
    )
    # noinspection PyBroadException
    try:
        p.dispatch(pre_call=global_config)
    except KeyboardInterrupt:
        msg = "Process interrupted by user (KeyboardInterrupt)"
        output.exception(msg)
    except Exception as e:
        msg = "%s\nSee log file for more details." % e
        output.exception(msg)

    # cleanup output API and exit honoring output.error_occurred and
    # output.error_exit_code
    output.close_and_exit()


if __name__ == '__main__':
    # This code requires the mock module and allow us to test
    # bash completion inside the IDE debugger
    try:
        # noinspection PyUnresolvedReferences
        import mock
        sys.stdout = mock.Mock(wraps=sys.stdout)
        sys.stdout.isatty.return_value = True
        os.dup2(2, 8)
    except ImportError:
        pass
    main()
