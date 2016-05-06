# Copyright (C) 2013-2016 2ndQuadrant Italia Srl
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
This module control how the output of Barman will be rendered
"""

from __future__ import print_function

import inspect
import logging
import sys

from barman.infofile import BackupInfo
from barman.utils import pretty_size

__all__ = [
    'error_occurred', 'debug', 'info', 'warning', 'error', 'exception',
    'result', 'close_and_exit', 'close', 'set_output_writer',
    'AVAILABLE_WRITERS', 'DEFAULT_WRITER', 'ConsoleOutputWriter',
    'NagiosOutputWriter',
]

#: True if error or exception methods have been called
error_occurred = False

#: Exit code if error occurred
error_exit_code = 1


def _format_message(message, args):
    """
    Format a message using the args list. The result will be equivalent to

        message % args

    If args list contains a dictionary as its only element the result will be

        message % args[0]

    :param str message: the template string to be formatted
    :param tuple args: a list of arguments
    :return: the formatted message
    :rtype: str
    """
    if len(args) == 1 and isinstance(args[0], dict):
        return message % args[0]
    elif len(args) > 0:
        return message % args
    else:
        return message


def _put(level, message, *args, **kwargs):
    """
    Send the message with all the remaining positional arguments to
    the configured output manager with the right output level. The message will
    be sent also to the logger unless  explicitly disabled with log=False

    No checks are performed on level parameter as this method is meant
    to be called only by this module.

    If level == 'exception' the stack trace will be also logged

    :param str level:
    :param str message: the template string to be formatted
    :param tuple args: all remaining arguments are passed to the log formatter
    :key bool log: whether to log the message
    :key bool is_error: treat this message as an error
    """
    # handle keyword-only parameters
    log = kwargs.pop('log', True)
    is_error = kwargs.pop('is_error', False)
    if len(kwargs):
        raise TypeError('%s() got an unexpected keyword argument %r'
                        % (inspect.stack()[1][3], kwargs.popitem()[0]))
    if is_error:
        global error_occurred
        error_occurred = True
        _writer.error_occurred()
    # dispatch the call to the output handler
    getattr(_writer, level)(message, *args)
    # log the message as originating from caller's caller module
    if log:
        exc_info = False
        if level == 'exception':
            level = 'error'
            exc_info = True
        frm = inspect.stack()[2]
        mod = inspect.getmodule(frm[0])
        logger = logging.getLogger(mod.__name__)
        log_level = logging.getLevelName(level.upper())
        logger.log(log_level, message, *args, **{'exc_info': exc_info})


def _dispatch(obj, prefix, name, *args, **kwargs):
    """
    Dispatch the call to the %(prefix)s_%(name) method of the obj object

    :param obj: the target object
    :param str prefix: prefix of the method to be called
    :param str name: name of the method to be called
    :param tuple args: all remaining positional arguments will be sent
        to target
    :param dict kwargs: all remaining keyword arguments will be sent to target
    :return: the result of the invoked method
    :raise ValueError: if the target method is not present
    """
    method_name = "%s_%s" % (prefix, name)
    handler = getattr(obj, method_name, None)
    if callable(handler):
        return handler(*args, **kwargs)
    else:
        raise ValueError("The object %r does not have the %r method" % (
            obj, method_name))


def is_quiet():
    """
    Calls the "is_quiet" method, accessing the protected parameter _quiet
    of the instanced OutputWriter
    :return bool: the _quiet parameter value
    """
    return _writer.is_quiet()


def is_debug():
    """
    Calls the "is_debug" method, accessing the protected parameter _debug
    of the instanced OutputWriter
    :return bool: the _debug parameter value
    """
    return _writer.is_debug()


def debug(message, *args, **kwargs):
    """
    Output a message with severity 'DEBUG'

    :key bool log: whether to log the message
    """
    _put('debug', message, *args, **kwargs)


def info(message, *args, **kwargs):
    """
    Output a message with severity 'INFO'

    :key bool log: whether to log the message
    """
    _put('info', message, *args, **kwargs)


def warning(message, *args, **kwargs):
    """
    Output a message with severity 'INFO'

    :key bool log: whether to log the message
    """
    _put('warning', message, *args, **kwargs)


def error(message, *args, **kwargs):
    """
    Output a message with severity 'ERROR'.
    Also records that an error has occurred unless the ignore parameter
    is True.

    :key bool ignore: avoid setting an error exit status (default False)
    :key bool log: whether to log the message
    """
    # ignore is a keyword-only parameter
    ignore = kwargs.pop('ignore', False)
    if not ignore:
        kwargs.setdefault('is_error', True)
    _put('error', message, *args, **kwargs)


def exception(message, *args, **kwargs):
    """
    Output a message with severity 'EXCEPTION'

    If raise_exception parameter doesn't evaluate to false raise and exception:
      - if raise_exception is callable raise the result of raise_exception()
      - if raise_exception is an exception raise it
      - else raise the last exception again

    :key bool ignore: avoid setting an error exit status
    :key raise_exception:
        raise an exception after the message has been processed
    :key bool log: whether to log the message
    """
    # ignore and raise_exception are keyword-only parameters
    ignore = kwargs.pop('ignore', False)
    # noinspection PyNoneFunctionAssignment
    raise_exception = kwargs.pop('raise_exception', None)
    if not ignore:
        kwargs.setdefault('is_error', True)
    _put('exception', message, *args, **kwargs)
    if raise_exception:
        if callable(raise_exception):
            # noinspection PyCallingNonCallable
            raise raise_exception(message)
        elif isinstance(raise_exception, BaseException):
            raise raise_exception
        else:
            raise


def init(command, *args, **kwargs):
    """
    Initialize the output writer for a given command.

    :param str command: name of the command are being executed
    :param tuple args: all remaining positional arguments will be sent
        to the output processor
    :param dict kwargs: all keyword arguments will be sent
        to the output processor
    """
    try:
        _dispatch(_writer, 'init', command, *args, **kwargs)
    except ValueError:
        exception('The %s writer does not support the "%s" command',
                  _writer.__class__.__name__, command)
        close_and_exit()


def result(command, *args, **kwargs):
    """
    Output the result of an operation.

    :param str command: name of the command are being executed
    :param tuple args: all remaining positional arguments will be sent
        to the output processor
    :param dict kwargs: all keyword arguments will be sent
        to the output processor
    """
    try:
        _dispatch(_writer, 'result', command, *args, **kwargs)
    except ValueError:
        exception('The %s writer does not support the "%s" command',
                  _writer.__class__.__name__, command)
        close_and_exit()


def close_and_exit():
    """
    Close the output writer and terminate the program.

    If an error has been emitted the program will report a non zero return
    value.
    """
    close()
    if error_occurred:
        sys.exit(error_exit_code)
    else:
        sys.exit(0)


def close():
    """
    Close the output writer.

    """
    _writer.close()


def set_output_writer(new_writer, *args, **kwargs):
    """
    Replace the current output writer with a new one.

    The new_writer parameter can be a symbolic name or an OutputWriter object

    :param new_writer: the OutputWriter name or the actual OutputWriter
    :type: string or an OutputWriter
    :param tuple args: all remaining positional arguments will be passed
        to the OutputWriter constructor
    :param dict kwargs: all remaining keyword arguments will be passed
        to the OutputWriter constructor
    """
    global _writer
    _writer.close()
    if new_writer in AVAILABLE_WRITERS:
        _writer = AVAILABLE_WRITERS[new_writer](*args, **kwargs)
    else:
        _writer = new_writer


class ConsoleOutputWriter(object):
    def __init__(self, debug=False, quiet=False):
        """
        Default output writer that output everything on console.

        :param bool debug: print debug messages on standard error
        :param bool quiet: don't print info messages
        """
        self._debug = debug
        self._quiet = quiet

        #: Used in check command to hold the check results
        self.result_check_list = []

        #: Used in status command to hold the status results
        self.result_status_list = []

        #: The minimal flag. If set the command must output a single list of
        #: values.
        self.minimal = False

        #: The server is active
        self.active = True

    def _out(self, message, args):
        """
        Print a message on standard output
        """
        print(_format_message(message, args), file=sys.stdout)

    def _err(self, message, args):
        """
        Print a message on standard error
        """
        print(_format_message(message, args), file=sys.stderr)

    def is_quiet(self):
        """
        Access the quiet property of the OutputWriter instance

        :return bool: if the writer is quiet or not
        """
        return self._quiet

    def is_debug(self):
        """
        Access the debug property of the OutputWriter instance

        :return bool: if the writer is in debug mode or not
        """
        return self._debug

    def debug(self, message, *args):
        """
        Emit debug.
        """
        if self._debug:
            self._err('DEBUG: %s' % message, args)

    def info(self, message, *args):
        """
        Normal messages are sent to standard output
        """
        if not self._quiet:
            self._out(message, args)

    def warning(self, message, *args):
        """
        Warning messages are sent to standard error
        """
        self._err('WARNING: %s' % message, args)

    def error(self, message, *args):
        """
        Error messages are sent to standard error
        """
        self._err('ERROR: %s' % message, args)

    def exception(self, message, *args):
        """
        Warning messages are sent to standard error
        """
        self._err('EXCEPTION: %s' % message, args)

    def error_occurred(self):
        """
        Called immediately before any message method when the originating
        call has is_error=True
        """

    def close(self):
        """
        Close the output channel.

        Nothing to do for console.
        """

    def result_backup(self, backup_info):
        """
        Render the result of a backup.

        Nothing to do for console.
        """
        # TODO: evaluate to display something useful here

    def result_recovery(self, results):
        """
        Render the result of a recovery.

        """
        if len(results['changes']) > 0:
            self.info("")
            self.info("IMPORTANT")
            self.info("These settings have been modified to prevent "
                      "data losses")
            self.info("")

            for assertion in results['changes']:
                self.info("%s line %s: %s = %s",
                          assertion.filename,
                          assertion.line,
                          assertion.key,
                          assertion.value)

        if len(results['warnings']) > 0:
            self.info("")
            self.info("WARNING")
            self.info("You are required to review the following options"
                      " as potentially dangerous")
            self.info("")

            for assertion in results['warnings']:
                self.info("%s line %s: %s = %s",
                          assertion.filename,
                          assertion.line,
                          assertion.key,
                          assertion.value)

        if results['delete_barman_xlog']:
            self.info("")
            self.info("After the recovery, please remember to remove the "
                      "\"barman_xlog\" directory")
            self.info("inside the PostgreSQL data directory.")

        if results['get_wal']:
            self.info("")
            self.info("WARNING: 'get-wal' is in the specified "
                      "'recovery_options'.")
            self.info("Before you start up the PostgreSQL server, please "
                      "review the recovery.conf file")
            self.info("inside the target directory. Make sure that "
                      "'restore_command' can be executed by "
                      "the PostgreSQL user.")
        self.info("")
        self.info("Your PostgreSQL server has been successfully "
                  "prepared for recovery!")

    def _record_check(self, server_name, check, status, hint):
        """
        Record the check line in result_check_map attribute

        This method is for subclass use

        :param str server_name: the server is being checked
        :param str check: the check name
        :param bool status: True if succeeded
        :param str,None hint: hint to print if not None
        """
        self.result_check_list.append(dict(
            server_name=server_name, check=check, status=status, hint=hint))
        if not status and self.active:
            global error_occurred
            error_occurred = True

    def init_check(self, server_name, active):
        """
        Init the check command

        :param str server_name: the server we are start listing
        :param boolean active: The server is active
        """
        self.info("Server %s:" % server_name)
        self.active = active

    def result_check(self, server_name, check, status, hint=None):
        """
        Record a server result of a server check

        and output it as INFO

        :param str server_name: the server is being checked
        :param str check: the check name
        :param bool status: True if succeeded
        :param str,None hint: hint to print if not None
        """
        self._record_check(server_name, check, status, hint)
        if hint:
            self.info("\t%s: %s (%s)" %
                      (check, 'OK' if status else 'FAILED', hint))
        else:
            self.info("\t%s: %s" %
                      (check, 'OK' if status else 'FAILED'))

    def init_list_backup(self, server_name, minimal=False):
        """
        Init the list-backup command

        :param str server_name: the server we are start listing
        :param bool minimal: if true output only a list of backup id
        """
        self.minimal = minimal

    def result_list_backup(self, backup_info,
                           backup_size, wal_size,
                           retention_status):
        """
        Output a single backup in the list-backup command

        :param BackupInfo backup_info: backup we are displaying
        :param backup_size: size of base backup (with the required WAL files)
        :param wal_size: size of WAL files belonging to this backup
            (without the required WAL files)
        :param retention_status: retention policy status
        """
        # If minimal is set only output the backup id
        if self.minimal:
            self.info(backup_info.backup_id)
            return

        out_list = [
            "%s %s - " % (backup_info.server_name, backup_info.backup_id)]
        if backup_info.status == BackupInfo.DONE:
            end_time = backup_info.end_time.ctime()
            out_list.append('%s - Size: %s - WAL Size: %s' %
                            (end_time,
                             pretty_size(backup_size),
                             pretty_size(wal_size)))
            if backup_info.tablespaces:
                tablespaces = [("%s:%s" % (tablespace.name,
                                           tablespace.location))
                               for tablespace in backup_info.tablespaces]
                out_list.append(' (tablespaces: %s)' %
                                ', '.join(tablespaces))
            if retention_status:
                out_list.append(' - %s' % retention_status)
        else:
            out_list.append(backup_info.status)
        self.info(''.join(out_list))

    def result_show_backup(self, backup_ext_info):
        """
        Output all available information about a backup in show-backup command

        The argument has to be the result
        of a Server.get_backup_ext_info() call

        :param dict backup_ext_info: a dictionary containing
            the info to display
        """
        data = dict(backup_ext_info)
        self.info("Backup %s:", data['backup_id'])
        self.info("  Server Name            : %s", data['server_name'])
        self.info("  Status                 : %s", data['status'])
        if data['status'] == BackupInfo.DONE:
            self.info("  PostgreSQL Version     : %s", data['version'])
            self.info("  PGDATA directory       : %s", data['pgdata'])
            if data['tablespaces']:
                self.info("  Tablespaces:")
                for item in data['tablespaces']:
                    self.info("    %s: %s (oid: %s)",
                              item.name, item.location, item.oid)
            self.info("")
            self.info("  Base backup information:")
            self.info("    Disk usage           : %s (%s with WALs)",
                      pretty_size(data['size']),
                      pretty_size(data['size'] + data[
                          'wal_size']))
            if data['deduplicated_size'] is not None and data['size'] > 0:
                deduplication_ratio = 1 - (float(data['deduplicated_size']) /
                                           data['size'])
                self.info("    Incremental size     : %s (-%s)",
                          pretty_size(data['deduplicated_size']),
                          '{percent:.2%}'.format(percent=deduplication_ratio)
                          )
            self.info("    Timeline             : %s", data['timeline'])
            self.info("    Begin WAL            : %s",
                      data['begin_wal'])
            self.info("    End WAL              : %s", data['end_wal'])
            self.info("    WAL number           : %s", data['wal_num'])
            # Output WAL compression ratio for basebackup WAL files
            if data['wal_compression_ratio'] > 0:
                self.info("    WAL compression ratio: %s",
                          '{percent:.2%}'.format(
                              percent=data['wal_compression_ratio']))
            self.info("    Begin time           : %s",
                      data['begin_time'])
            self.info("    End time             : %s", data['end_time'])
            self.info("    Begin Offset         : %s",
                      data['begin_offset'])
            self.info("    End Offset           : %s",
                      data['end_offset'])
            self.info("    Begin XLOG           : %s",
                      data['begin_xlog'])
            self.info("    End XLOG             : %s", data['end_xlog'])
            self.info("")
            self.info("  WAL information:")
            self.info("    No of files          : %s",
                      data['wal_until_next_num'])
            self.info("    Disk usage           : %s",
                      pretty_size(data['wal_until_next_size']))
            # Output WAL rate
            if data['wals_per_second'] > 0:
                self.info("    WAL rate             : %0.2f/hour",
                          data['wals_per_second'] * 3600)
            # Output WAL compression ratio for archived WAL files
            if data['wal_until_next_compression_ratio'] > 0:
                self.info(
                    "    Compression ratio    : %s",
                    '{percent:.2%}'.format(
                        percent=data['wal_until_next_compression_ratio']))
            self.info("    Last available       : %s", data['wal_last'])
            self.info("")
            self.info("  Catalog information:")
            self.info("    Retention Policy     : %s",
                      data['retention_policy_status'] or
                      'not enforced')
            self.info("    Previous Backup      : %s",
                      data.setdefault('previous_backup_id', 'not available') or
                      '- (this is the oldest base backup)')
            self.info("    Next Backup          : %s",
                      data.setdefault('next_backup_id', 'not available') or
                      '- (this is the latest base backup)')
        else:
            if data['error']:
                self.info("  Error:            : %s",
                          data['error'])

    def init_status(self, server_name):
        """
        Init the status command

        :param str server_name: the server we are start listing
        """
        self.info("Server %s:", server_name)

    def result_status(self, server_name, status, description, message):
        """
        Record a result line of a server status command

        and output it as INFO

        :param str server_name: the server is being checked
        :param str status: the returned status code
        :param str description: the returned status description
        :param str,object message: status message. It will be converted to str
        """
        message = str(message)
        self.result_status_list.append(dict(
            server_name=server_name, status=status,
            description=description, message=message))
        self.info("\t%s: %s", description, message)

    def init_replication_status(self, server_name, minimal=False):
        """
        Init the 'standby-status' command

        :param str server_name: the server we are start listing
        :param str minimal: minimal output
        """
        self.minimal = minimal

    def result_replication_status(self, server_name, target, xlog_location,
                                  standby_info):
        """
        Record a result line of a server status command

        and output it as INFO

        :param str server_name: the replication server
        :param str target: all|hot-standby|wal-streamer
        :param str xlog_location: server's xlog location
        :param StatReplication standby_info: status info of a standby
        """

        if target == 'hot-standby':
            title = 'hot standby servers'
        elif target == 'wal-streamer':
            title = 'WAL streamers'
        else:
            title = 'streaming clients'

        if self.minimal:
            # Minimal output
            if xlog_location:
                # xlog location from the master
                self.info("%s for master '%s' (xlog @ %s):",
                          title.capitalize(), server_name, xlog_location)
            else:
                # We are connected to a standby
                self.info("%s for slave '%s':",
                          title.capitalize(), server_name)
        else:
            # Full output
            self.info("Status of %s for server '%s':",
                      title, server_name)
            # xlog location from the master
            if xlog_location:
                self.info("  Current xlog location on master: %s",
                          xlog_location)

        if standby_info is not None and not len(standby_info):
            self.info("  No %s attached", title)
            return

        # Minimal output
        if self.minimal:
            n = 1
            for standby in standby_info:
                if not standby.replay_location:
                    # WAL streamer
                    self.info("  %s. W) %s@%s S:%s W:%s P:%s AN:%s",
                              n,
                              standby.usename,
                              standby.client_addr or 'socket',
                              standby.sent_location,
                              standby.write_location,
                              standby.sync_priority,
                              standby.application_name)
                else:
                    # Standby
                    self.info("  %s. %s) %s@%s S:%s F:%s R:%s P:%s AN:%s",
                              n,
                              standby.sync_state[0].upper(),
                              standby.usename,
                              standby.client_addr or 'socket',
                              standby.sent_location,
                              standby.flush_location,
                              standby.replay_location,
                              standby.sync_priority,
                              standby.application_name)
                n += 1
        else:
            n = 1
            self.info("  Number of %s: %s",
                      title, len(standby_info))
            for standby in standby_info:
                self.info("")

                # Determine the sync stage of the client
                sync_stage = None
                if not standby.replay_location:
                    client_type = 'WAL streamer'
                    max_level = 3
                else:
                    client_type = 'standby'
                    max_level = 5
                    # Only standby can replay WAL info
                    if standby.replay_diff == 0:
                        sync_stage = '5/5 Hot standby (max)'
                    elif standby.flush_diff == 0:
                        sync_stage = '4/5 2-safe'  # remote flush

                # If not yet done, set the sync stage
                if not sync_stage:
                    if standby.write_diff == 0:
                        sync_stage = '3/%s Remote write' % max_level
                    elif standby.sent_diff == 0:
                        sync_stage = '2/%s WAL Sent (min)' % max_level
                    else:
                        sync_stage = '1/%s 1-safe' % max_level

                # Synchronous standby
                if standby.sync_priority > 0:
                    self.info("  %s. #%s %s %s",
                              n,
                              standby.sync_priority,
                              standby.sync_state.capitalize(),
                              client_type)
                # Asynchronous standby
                else:
                    self.info("  %s. %s %s",
                              n,
                              standby.sync_state.capitalize(),
                              client_type)
                self.info("     Application name: %s",
                          standby.application_name)
                self.info("     Sync stage      : %s",
                          sync_stage)
                if standby.client_addr:
                    self.info("     Communication   : TCP/IP")
                    self.info("     IP Address      : %s "
                              "/ Port: %s / Host: %s",
                              standby.client_addr,
                              standby.client_port,
                              standby.client_hostname or '-')
                else:
                    self.info("     Communication   : Unix domain socket")
                self.info("     User name       : %s", standby.usename)
                self.info("     Current state   : %s (%s)",
                          standby.state,
                          standby.sync_state)
                self.info("     WAL sender PID  : %s", standby.pid)
                self.info("     Started at      : %s", standby.backend_start)
                if standby.backend_xmin:
                    self.info("     Standby's xmin  : %s",
                              standby.backend_xmin or '-')
                self.info("     Sent location   : %s (diff: %s)",
                          standby.sent_location,
                          pretty_size(standby.sent_diff))
                self.info("     Write location  : %s (diff: %s)",
                          standby.write_location,
                          pretty_size(standby.write_diff))
                if standby.flush_location:
                    self.info("     Flush location  : %s (diff: %s)",
                              standby.flush_location,
                              pretty_size(standby.flush_diff))
                if standby.replay_location:
                    self.info("     Replay location : %s (diff: %s)",
                              standby.replay_location,
                              pretty_size(standby.replay_diff))
                n += 1

    def init_list_server(self, server_name, minimal=False):
        """
        Init the list-server command

        :param str server_name: the server we are start listing
        """
        self.minimal = minimal

    def result_list_server(self, server_name, description=None):
        """
        Output a result line of a list-server command

        :param str server_name: the server is being checked
        :param str,None description: server description if applicable
        """
        if self.minimal or not description:
            self.info("%s", server_name)
        else:
            self.info("%s - %s", server_name, description)

    def init_show_server(self, server_name):
        """
        Init the show-server command output method

        :param str server_name: the server we are displaying
        """
        self.info("Server %s:" % server_name)

    def result_show_server(self, server_name, server_info):
        """
        Output the results of the show-server command

        :param str server_name: the server we are displaying
        :param dict server_info: a dictionary containing the info to display
        """
        for status, message in sorted(server_info.items()):
            self.info("\t%s: %s", status, message)


class NagiosOutputWriter(ConsoleOutputWriter):
    """
    Nagios output writer.

    This writer doesn't output anything to console.
    On close it writes a nagios-plugin compatible status
    """

    def _out(self, message, args):
        """
        Do not print anything on standard output
        """

    def _err(self, message, args):
        """
        Do not print anything on standard error
        """

    def close(self):
        """
        Display the result of a check run as expected by Nagios.

        Also set the exit code as 2 (CRITICAL) in case of errors
        """

        global error_occurred, error_exit_code

        # List of all servers that have been checked
        servers = []
        # List of servers reporting issues
        issues = []
        for item in self.result_check_list:
            # Keep track of all the checked servers
            if item['server_name'] not in servers:
                servers.append(item['server_name'])
            # Keep track of the servers with issues
            if not item['status'] and item['server_name'] not in issues:
                issues.append(item['server_name'])

        # Global error (detected at configuration level)
        if len(issues) == 0 and error_occurred:
            print("BARMAN CRITICAL - Global configuration errors")
            error_exit_code = 2
            return

        if len(issues) > 0:
            fail_summary = []
            details = []
            for server in issues:
                # Join all the issues for a server. Output format is in the
                # form:
                # "<server_name> FAILED: <failed_check1>, <failed_check2> ... "
                # All strings will be concatenated into the $SERVICEOUTPUT$
                # macro of the Nagios output
                server_fail = "%s FAILED: %s" % (
                    server,
                    ", ".join([
                        item['check']
                        for item in self.result_check_list
                        if item['server_name'] == server and not item['status']
                    ]))
                fail_summary.append(server_fail)
                # Prepare an array with the detailed output for
                # the $LONGSERVICEOUTPUT$ macro of the Nagios output
                # line format:
                # <servername>.<failed_check1>: FAILED
                # <servername>.<failed_check2>: FAILED (Hint if present)
                # <servername2.<failed_check1>: FAILED
                # .....
                for issue in self.result_check_list:
                    if issue['server_name'] == server and not issue['status']:
                        fail_detail = "%s.%s: FAILED" % (server,
                                                         issue['check'])
                        if issue['hint']:
                            fail_detail += " (%s)" % issue['hint']
                        details.append(fail_detail)
            # Append the summary of failures to the first line of the output
            # using * as delimiter
            if len(servers) == 1:
                print("BARMAN CRITICAL - server %s has issues * %s" %
                      (servers[0], " * ".join(fail_summary)))
            else:
                print("BARMAN CRITICAL - %d server out of %d have issues * "
                      "%s" % (len(issues), len(servers),
                              " * ".join(fail_summary)))

            # add the detailed list to the output
            for issue in details:
                print(issue)
            error_exit_code = 2
        else:
            # No issues, all good!
            # Display the output message for a single server check
            if len(servers) == 1:
                print("BARMAN OK - Ready to serve the Espresso backup "
                      "for %s" %
                      (servers[0]))
            else:
                # Display the output message for several servers, using
                # '*' as delimiter
                print("BARMAN OK - Ready to serve the Espresso backup "
                      "for %d server(s) * %s" % (
                          len(servers),
                          " * ".join([server for server in servers])))


#: This dictionary acts as a registry of available OutputWriters
AVAILABLE_WRITERS = {
    'console': ConsoleOutputWriter,
    # nagios is not registered as it isn't a general purpose output writer
    # 'nagios': NagiosOutputWriter,
}

#: The default OutputWriter
DEFAULT_WRITER = 'console'

#: the current active writer. Initialized according DEFAULT_WRITER on load
_writer = AVAILABLE_WRITERS[DEFAULT_WRITER]()
