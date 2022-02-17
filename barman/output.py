# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2022
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

import datetime
import inspect
import json
import logging
import sys

from barman.infofile import BackupInfo
from barman.utils import (
    BarmanEncoder,
    force_str,
    human_readable_timedelta,
    pretty_size,
    redact_passwords,
)
from barman.xlog import diff_lsn

__all__ = [
    "error_occurred",
    "debug",
    "info",
    "warning",
    "error",
    "exception",
    "result",
    "close_and_exit",
    "close",
    "set_output_writer",
    "AVAILABLE_WRITERS",
    "DEFAULT_WRITER",
    "ConsoleOutputWriter",
    "NagiosOutputWriter",
    "JsonOutputWriter",
]

#: True if error or exception methods have been called
error_occurred = False

#: Exit code if error occurred
error_exit_code = 1

#: Enable colors in the output
ansi_colors_enabled = False


def _ansi_color(command):
    """
    Return the ansi sequence for the provided color
    """
    return "\033[%sm" % command


def _colored(message, color):
    """
    Return a string formatted with the provided color.
    """
    if ansi_colors_enabled:
        return _ansi_color(color) + message + _ansi_color("0")
    else:
        return message


def _red(message):
    """
    Format a red string
    """
    return _colored(message, "31")


def _green(message):
    """
    Format a green string
    """
    return _colored(message, "32")


def _yellow(message):
    """
    Format a yellow string
    """
    return _colored(message, "33")


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
    log = kwargs.pop("log", True)
    is_error = kwargs.pop("is_error", False)
    global error_exit_code
    error_exit_code = kwargs.pop("exit_code", error_exit_code)
    if len(kwargs):
        raise TypeError(
            "%s() got an unexpected keyword argument %r"
            % (inspect.stack()[1][3], kwargs.popitem()[0])
        )
    if is_error:
        global error_occurred
        error_occurred = True
        _writer.error_occurred()
    # Make sure the message is an unicode string
    if message:
        message = force_str(message)
    # dispatch the call to the output handler
    getattr(_writer, level)(message, *args)
    # log the message as originating from caller's caller module
    if log:
        exc_info = False
        if level == "exception":
            level = "error"
            exc_info = True
        frm = inspect.stack()[2]
        mod = inspect.getmodule(frm[0])
        logger = logging.getLogger(mod.__name__)
        log_level = logging.getLevelName(level.upper())
        logger.log(log_level, message, *args, **{"exc_info": exc_info})


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
        raise ValueError(
            "The object %r does not have the %r method" % (obj, method_name)
        )


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
    _put("debug", message, *args, **kwargs)


def info(message, *args, **kwargs):
    """
    Output a message with severity 'INFO'

    :key bool log: whether to log the message
    """
    _put("info", message, *args, **kwargs)


def warning(message, *args, **kwargs):
    """
    Output a message with severity 'WARNING'

    :key bool log: whether to log the message
    """
    _put("warning", message, *args, **kwargs)


def error(message, *args, **kwargs):
    """
    Output a message with severity 'ERROR'.
    Also records that an error has occurred unless the ignore parameter
    is True.

    :key bool ignore: avoid setting an error exit status (default False)
    :key bool log: whether to log the message
    """
    # ignore is a keyword-only parameter
    ignore = kwargs.pop("ignore", False)
    if not ignore:
        kwargs.setdefault("is_error", True)
    _put("error", message, *args, **kwargs)


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
    ignore = kwargs.pop("ignore", False)
    # noinspection PyNoneFunctionAssignment
    raise_exception = kwargs.pop("raise_exception", None)
    if not ignore:
        kwargs.setdefault("is_error", True)
    _put("exception", message, *args, **kwargs)
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
        _dispatch(_writer, "init", command, *args, **kwargs)
    except ValueError:
        exception(
            'The %s writer does not support the "%s" command',
            _writer.__class__.__name__,
            command,
        )
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
        _dispatch(_writer, "result", command, *args, **kwargs)
    except ValueError:
        exception(
            'The %s writer does not support the "%s" command',
            _writer.__class__.__name__,
            command,
        )
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
    SERVER_OUTPUT_PREFIX = "Server %s:"

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

        #: The minimal flag. If set the command must output a single list of
        #: values.
        self.minimal = False

        #: The server is active
        self.active = True

    def _print(self, message, args, stream):
        """
        Print an encoded message on the given output stream
        """
        # Make sure to add a newline at the end of the message
        if message is None:
            message = "\n"
        else:
            message += "\n"
        # Format and encode the message, redacting eventual passwords
        encoded_msg = redact_passwords(_format_message(message, args)).encode("utf-8")
        try:
            # Python 3.x
            stream.buffer.write(encoded_msg)
        except AttributeError:
            # Python 2.x
            stream.write(encoded_msg)
        stream.flush()

    def _out(self, message, args):
        """
        Print a message on standard output
        """
        self._print(message, args, sys.stdout)

    def _err(self, message, args):
        """
        Print a message on standard error
        """
        self._print(message, args, sys.stderr)

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
            self._err("DEBUG: %s" % message, args)

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
        self._err(_yellow("WARNING: %s" % message), args)

    def error(self, message, *args):
        """
        Error messages are sent to standard error
        """
        self._err(_red("ERROR: %s" % message), args)

    def exception(self, message, *args):
        """
        Warning messages are sent to standard error
        """
        self._err(_red("EXCEPTION: %s" % message), args)

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
        if len(results["changes"]) > 0:
            self.info("")
            self.info("IMPORTANT")
            self.info("These settings have been modified to prevent data losses")
            self.info("")

            for assertion in results["changes"]:
                self.info(
                    "%s line %s: %s = %s",
                    assertion.filename,
                    assertion.line,
                    assertion.key,
                    assertion.value,
                )

        if len(results["warnings"]) > 0:
            self.info("")
            self.info("WARNING")
            self.info(
                "You are required to review the following options"
                " as potentially dangerous"
            )
            self.info("")

            for assertion in results["warnings"]:
                self.info(
                    "%s line %s: %s = %s",
                    assertion.filename,
                    assertion.line,
                    assertion.key,
                    assertion.value,
                )

        if results["missing_files"]:
            # At least one file is missing, warn the user
            self.info("")
            self.info("WARNING")
            self.info(
                "The following configuration files have not been "
                "saved during backup, hence they have not been "
                "restored."
            )
            self.info(
                "You need to manually restore them "
                "in order to start the recovered PostgreSQL instance:"
            )
            self.info("")
            for file_name in results["missing_files"]:
                self.info("    %s" % file_name)

        if results["delete_barman_wal"]:
            self.info("")
            self.info(
                "After the recovery, please remember to remove the "
                '"barman_wal" directory'
            )
            self.info("inside the PostgreSQL data directory.")

        if results["get_wal"]:
            self.info("")
            self.info("WARNING: 'get-wal' is in the specified 'recovery_options'.")
            self.info(
                "Before you start up the PostgreSQL server, please "
                "review the %s file",
                results["recovery_configuration_file"],
            )
            self.info(
                "inside the target directory. Make sure that "
                "'restore_command' can be executed by "
                "the PostgreSQL user."
            )
        self.info("")
        self.info(
            "Recovery completed (start time: %s, elapsed time: %s)",
            results["recovery_start_time"],
            human_readable_timedelta(
                datetime.datetime.now() - results["recovery_start_time"]
            ),
        )
        self.info("")
        self.info("Your PostgreSQL server has been successfully prepared for recovery!")

    def _record_check(self, server_name, check, status, hint, perfdata):
        """
        Record the check line in result_check_map attribute

        This method is for subclass use

        :param str server_name: the server is being checked
        :param str check: the check name
        :param bool status: True if succeeded
        :param str,None hint: hint to print if not None
        :param str,None perfdata: additional performance data to print if not None
        """
        self.result_check_list.append(
            dict(
                server_name=server_name,
                check=check,
                status=status,
                hint=hint,
                perfdata=perfdata,
            )
        )
        if not status and self.active:
            global error_occurred
            error_occurred = True

    def init_check(self, server_name, active, disabled):
        """
        Init the check command

        :param str server_name: the server we are start listing
        :param boolean active: The server is active
        :param boolean disabled: The server is disabled
        """
        display_name = server_name
        # If the server has been manually disabled
        if not active:
            display_name += " (inactive)"
        # If server has configuration errors
        elif disabled:
            display_name += " (WARNING: disabled)"
        self.info(self.SERVER_OUTPUT_PREFIX % display_name)
        self.active = active

    def result_check(self, server_name, check, status, hint=None, perfdata=None):
        """
        Record a server result of a server check
        and output it as INFO

        :param str server_name: the server is being checked
        :param str check: the check name
        :param bool status: True if succeeded
        :param str,None hint: hint to print if not None
        :param str,None perfdata: additional performance data to print if not None
        """
        self._record_check(server_name, check, status, hint, perfdata)

        if hint:
            self.info(
                "\t%s: %s (%s)"
                % (check, _green("OK") if status else _red("FAILED"), hint)
            )
        else:
            self.info("\t%s: %s" % (check, _green("OK") if status else _red("FAILED")))

    def init_list_backup(self, server_name, minimal=False):
        """
        Init the list-backups command

        :param str server_name: the server we are start listing
        :param bool minimal: if true output only a list of backup id
        """
        self.minimal = minimal

    def result_list_backup(self, backup_info, backup_size, wal_size, retention_status):
        """
        Output a single backup in the list-backups command

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

        out_list = ["%s %s - " % (backup_info.server_name, backup_info.backup_id)]
        if backup_info.status in BackupInfo.STATUS_COPY_DONE:
            end_time = backup_info.end_time.ctime()
            out_list.append(
                "%s - Size: %s - WAL Size: %s"
                % (end_time, pretty_size(backup_size), pretty_size(wal_size))
            )
            if backup_info.tablespaces:
                tablespaces = [
                    ("%s:%s" % (tablespace.name, tablespace.location))
                    for tablespace in backup_info.tablespaces
                ]
                out_list.append(" (tablespaces: %s)" % ", ".join(tablespaces))
            if backup_info.status == BackupInfo.WAITING_FOR_WALS:
                out_list.append(" - %s" % BackupInfo.WAITING_FOR_WALS)
            if retention_status and retention_status != BackupInfo.NONE:
                out_list.append(" - %s" % retention_status)
        else:
            out_list.append(backup_info.status)
        self.info("".join(out_list))

    def result_show_backup(self, backup_ext_info):
        """
        Output all available information about a backup in show-backup command

        The argument has to be the result
        of a Server.get_backup_ext_info() call

        :param dict backup_ext_info: a dictionary containing
            the info to display
        """
        data = dict(backup_ext_info)
        self.info("Backup %s:", data["backup_id"])
        self.info("  Server Name            : %s", data["server_name"])
        if data["systemid"]:
            self.info("  System Id              : %s", data["systemid"])
        self.info("  Status                 : %s", data["status"])
        if data["status"] in BackupInfo.STATUS_COPY_DONE:
            self.info("  PostgreSQL Version     : %s", data["version"])
            self.info("  PGDATA directory       : %s", data["pgdata"])
            if data["tablespaces"]:
                self.info("  Tablespaces:")
                for item in data["tablespaces"]:
                    self.info(
                        "    %s: %s (oid: %s)", item.name, item.location, item.oid
                    )
            self.info("")
            self.info("  Base backup information:")
            self.info(
                "    Disk usage           : %s (%s with WALs)",
                pretty_size(data["size"]),
                pretty_size(data["size"] + data["wal_size"]),
            )
            if data["deduplicated_size"] is not None and data["size"] > 0:
                deduplication_ratio = 1 - (
                    float(data["deduplicated_size"]) / data["size"]
                )
                self.info(
                    "    Incremental size     : %s (-%s)",
                    pretty_size(data["deduplicated_size"]),
                    "{percent:.2%}".format(percent=deduplication_ratio),
                )
            self.info("    Timeline             : %s", data["timeline"])
            self.info("    Begin WAL            : %s", data["begin_wal"])
            self.info("    End WAL              : %s", data["end_wal"])
            self.info("    WAL number           : %s", data["wal_num"])
            # Output WAL compression ratio for basebackup WAL files
            if data["wal_compression_ratio"] > 0:
                self.info(
                    "    WAL compression ratio: %s",
                    "{percent:.2%}".format(percent=data["wal_compression_ratio"]),
                )
            self.info("    Begin time           : %s", data["begin_time"])
            self.info("    End time             : %s", data["end_time"])
            # If copy statistics are available print a summary
            copy_stats = data.get("copy_stats")
            if copy_stats:
                copy_time = copy_stats.get("copy_time")
                if copy_time:
                    value = human_readable_timedelta(
                        datetime.timedelta(seconds=copy_time)
                    )
                    # Show analysis time if it is more than a second
                    analysis_time = copy_stats.get("analysis_time")
                    if analysis_time is not None and analysis_time >= 1:
                        value += " + %s startup" % (
                            human_readable_timedelta(
                                datetime.timedelta(seconds=analysis_time)
                            )
                        )
                    self.info("    Copy time            : %s", value)
                    size = data["deduplicated_size"] or data["size"]
                    value = "%s/s" % pretty_size(size / copy_time)
                    number_of_workers = copy_stats.get("number_of_workers", 1)
                    if number_of_workers > 1:
                        value += " (%s jobs)" % number_of_workers
                    self.info("    Estimated throughput : %s", value)
            self.info("    Begin Offset         : %s", data["begin_offset"])
            self.info("    End Offset           : %s", data["end_offset"])
            self.info("    Begin LSN           : %s", data["begin_xlog"])
            self.info("    End LSN             : %s", data["end_xlog"])
            self.info("")
            self.info("  WAL information:")
            self.info("    No of files          : %s", data["wal_until_next_num"])
            self.info(
                "    Disk usage           : %s",
                pretty_size(data["wal_until_next_size"]),
            )
            # Output WAL rate
            if data["wals_per_second"] > 0:
                self.info(
                    "    WAL rate             : %0.2f/hour",
                    data["wals_per_second"] * 3600,
                )
            # Output WAL compression ratio for archived WAL files
            if data["wal_until_next_compression_ratio"] > 0:
                self.info(
                    "    Compression ratio    : %s",
                    "{percent:.2%}".format(
                        percent=data["wal_until_next_compression_ratio"]
                    ),
                )
            self.info("    Last available       : %s", data["wal_last"])
            if data["children_timelines"]:
                timelines = data["children_timelines"]
                self.info(
                    "    Reachable timelines  : %s",
                    ", ".join([str(history.tli) for history in timelines]),
                )
            self.info("")
            self.info("  Catalog information:")
            self.info(
                "    Retention Policy     : %s",
                data["retention_policy_status"] or "not enforced",
            )
            previous_backup_id = data.setdefault("previous_backup_id", "not available")
            self.info(
                "    Previous Backup      : %s",
                previous_backup_id or "- (this is the oldest base backup)",
            )
            next_backup_id = data.setdefault("next_backup_id", "not available")
            self.info(
                "    Next Backup          : %s",
                next_backup_id or "- (this is the latest base backup)",
            )
            if data["children_timelines"]:
                self.info("")
                self.info(
                    "WARNING: WAL information is inaccurate due to "
                    "multiple timelines interacting with this backup"
                )
        else:
            if data["error"]:
                self.info("  Error:            : %s", data["error"])

    def init_status(self, server_name):
        """
        Init the status command

        :param str server_name: the server we are start listing
        """
        self.info(self.SERVER_OUTPUT_PREFIX, server_name)

    def result_status(self, server_name, status, description, message):
        """
        Record a result line of a server status command

        and output it as INFO

        :param str server_name: the server is being checked
        :param str status: the returned status code
        :param str description: the returned status description
        :param str,object message: status message. It will be converted to str
        """
        self.info("\t%s: %s", description, str(message))

    def init_replication_status(self, server_name, minimal=False):
        """
        Init the 'standby-status' command

        :param str server_name: the server we are start listing
        :param str minimal: minimal output
        """
        self.minimal = minimal

    def result_replication_status(self, server_name, target, server_lsn, standby_info):
        """
        Record a result line of a server status command

        and output it as INFO

        :param str server_name: the replication server
        :param str target: all|hot-standby|wal-streamer
        :param str server_lsn: server's current lsn
        :param StatReplication standby_info: status info of a standby
        """

        if target == "hot-standby":
            title = "hot standby servers"
        elif target == "wal-streamer":
            title = "WAL streamers"
        else:
            title = "streaming clients"

        if self.minimal:
            # Minimal output
            if server_lsn:
                # current lsn from the master
                self.info(
                    "%s for master '%s' (LSN @ %s):",
                    title.capitalize(),
                    server_name,
                    server_lsn,
                )
            else:
                # We are connected to a standby
                self.info("%s for slave '%s':", title.capitalize(), server_name)
        else:
            # Full output
            self.info("Status of %s for server '%s':", title, server_name)
            # current lsn from the master
            if server_lsn:
                self.info("  Current LSN on master: %s", server_lsn)

        if standby_info is not None and not len(standby_info):
            self.info("  No %s attached", title)
            return

        # Minimal output
        if self.minimal:
            n = 1
            for standby in standby_info:
                if not standby.replay_lsn:
                    # WAL streamer
                    self.info(
                        "  %s. W) %s@%s S:%s W:%s P:%s AN:%s",
                        n,
                        standby.usename,
                        standby.client_addr or "socket",
                        standby.sent_lsn,
                        standby.write_lsn,
                        standby.sync_priority,
                        standby.application_name,
                    )
                else:
                    # Standby
                    self.info(
                        "  %s. %s) %s@%s S:%s F:%s R:%s P:%s AN:%s",
                        n,
                        standby.sync_state[0].upper(),
                        standby.usename,
                        standby.client_addr or "socket",
                        standby.sent_lsn,
                        standby.flush_lsn,
                        standby.replay_lsn,
                        standby.sync_priority,
                        standby.application_name,
                    )
                n += 1
        else:
            n = 1
            self.info("  Number of %s: %s", title, len(standby_info))
            for standby in standby_info:
                self.info("")

                # Calculate differences in bytes
                sent_diff = diff_lsn(standby.sent_lsn, standby.current_lsn)
                write_diff = diff_lsn(standby.write_lsn, standby.current_lsn)
                flush_diff = diff_lsn(standby.flush_lsn, standby.current_lsn)
                replay_diff = diff_lsn(standby.replay_lsn, standby.current_lsn)

                # Determine the sync stage of the client
                sync_stage = None
                if not standby.replay_lsn:
                    client_type = "WAL streamer"
                    max_level = 3
                else:
                    client_type = "standby"
                    max_level = 5
                    # Only standby can replay WAL info
                    if replay_diff == 0:
                        sync_stage = "5/5 Hot standby (max)"
                    elif flush_diff == 0:
                        sync_stage = "4/5 2-safe"  # remote flush

                # If not yet done, set the sync stage
                if not sync_stage:
                    if write_diff == 0:
                        sync_stage = "3/%s Remote write" % max_level
                    elif sent_diff == 0:
                        sync_stage = "2/%s WAL Sent (min)" % max_level
                    else:
                        sync_stage = "1/%s 1-safe" % max_level

                # Synchronous standby
                if getattr(standby, "sync_priority", None) > 0:
                    self.info(
                        "  %s. #%s %s %s",
                        n,
                        standby.sync_priority,
                        standby.sync_state.capitalize(),
                        client_type,
                    )
                # Asynchronous standby
                else:
                    self.info(
                        "  %s. %s %s", n, standby.sync_state.capitalize(), client_type
                    )
                self.info("     Application name: %s", standby.application_name)
                self.info("     Sync stage      : %s", sync_stage)
                if getattr(standby, "client_addr", None):
                    self.info("     Communication   : TCP/IP")
                    self.info(
                        "     IP Address      : %s / Port: %s / Host: %s",
                        standby.client_addr,
                        standby.client_port,
                        standby.client_hostname or "-",
                    )
                else:
                    self.info("     Communication   : Unix domain socket")
                self.info("     User name       : %s", standby.usename)
                self.info(
                    "     Current state   : %s (%s)", standby.state, standby.sync_state
                )
                if getattr(standby, "slot_name", None):
                    self.info("     Replication slot: %s", standby.slot_name)
                self.info("     WAL sender PID  : %s", standby.pid)
                self.info("     Started at      : %s", standby.backend_start)
                if getattr(standby, "backend_xmin", None):
                    self.info("     Standby's xmin  : %s", standby.backend_xmin or "-")
                if getattr(standby, "sent_lsn", None):
                    self.info(
                        "     Sent LSN   : %s (diff: %s)",
                        standby.sent_lsn,
                        pretty_size(sent_diff),
                    )
                if getattr(standby, "write_lsn", None):
                    self.info(
                        "     Write LSN  : %s (diff: %s)",
                        standby.write_lsn,
                        pretty_size(write_diff),
                    )
                if getattr(standby, "flush_lsn", None):
                    self.info(
                        "     Flush LSN  : %s (diff: %s)",
                        standby.flush_lsn,
                        pretty_size(flush_diff),
                    )
                if getattr(standby, "replay_lsn", None):
                    self.info(
                        "     Replay LSN : %s (diff: %s)",
                        standby.replay_lsn,
                        pretty_size(replay_diff),
                    )
                n += 1

    def init_list_server(self, server_name, minimal=False):
        """
        Init the list-servers command

        :param str server_name: the server we are start listing
        """
        self.minimal = minimal

    def result_list_server(self, server_name, description=None):
        """
        Output a result line of a list-servers command

        :param str server_name: the server is being checked
        :param str,None description: server description if applicable
        """
        if self.minimal or not description:
            self.info("%s", server_name)
        else:
            self.info("%s - %s", server_name, description)

    def init_show_server(self, server_name, description=None):
        """
        Init the show-servers command output method

        :param str server_name: the server we are displaying
        :param str,None description: server description if applicable
        """
        if description:
            self.info(self.SERVER_OUTPUT_PREFIX % " ".join((server_name, description)))
        else:
            self.info(self.SERVER_OUTPUT_PREFIX % server_name)

    def result_show_server(self, server_name, server_info):
        """
        Output the results of the show-servers command

        :param str server_name: the server we are displaying
        :param dict server_info: a dictionary containing the info to display
        """
        for status, message in sorted(server_info.items()):
            self.info("\t%s: %s", status, message)

    def init_check_wal_archive(self, server_name):
        """
        Init the check-wal-archive command output method

        :param str server_name: the server we are displaying
        """
        self.info(self.SERVER_OUTPUT_PREFIX % server_name)

    def result_check_wal_archive(self, server_name):
        """
        Output the results of the check-wal-archive command

        :param str server_name: the server we are displaying
        """
        self.info(" - WAL archive check for server %s passed" % server_name)


class JsonOutputWriter(ConsoleOutputWriter):
    def __init__(self, *args, **kwargs):
        """
        Output writer that writes on standard output using JSON.

        When closed, it dumps all the collected results as a JSON object.
        """
        super(JsonOutputWriter, self).__init__(*args, **kwargs)

        #: Store JSON data
        self.json_output = {}

    def _mangle_key(self, value):
        """
        Mangle a generic description to be used as dict key

        :type value: str
        :rtype: str
        """
        return value.lower().replace(" ", "_").replace("-", "_").replace(".", "")

    def _out_to_field(self, field, message, *args):
        """
        Store a message in the required field
        """
        if field not in self.json_output:
            self.json_output[field] = []

        message = _format_message(message, args)
        self.json_output[field].append(message)

    def debug(self, message, *args):
        """
        Add debug messages in _DEBUG list
        """
        if not self._debug:
            return

        self._out_to_field("_DEBUG", message, *args)

    def info(self, message, *args):
        """
        Add normal messages in _INFO list
        """
        self._out_to_field("_INFO", message, *args)

    def warning(self, message, *args):
        """
        Add warning messages in _WARNING list
        """
        self._out_to_field("_WARNING", message, *args)

    def error(self, message, *args):
        """
        Add error messages in _ERROR list
        """
        self._out_to_field("_ERROR", message, *args)

    def exception(self, message, *args):
        """
        Add exception messages in _EXCEPTION list
        """
        self._out_to_field("_EXCEPTION", message, *args)

    def close(self):
        """
        Close the output channel.
        Print JSON output
        """
        if not self._quiet:
            json.dump(self.json_output, sys.stdout, sort_keys=True, cls=BarmanEncoder)
        self.json_output = {}

    def result_backup(self, backup_info):
        """
        Save the result of a backup.
        """
        self.json_output.update(backup_info.to_dict())

    def result_recovery(self, results):
        """
        Render the result of a recovery.
        """
        changes_count = len(results["changes"])
        self.json_output["changes_count"] = changes_count
        self.json_output["changes"] = results["changes"]

        if changes_count > 0:
            self.warning(
                "IMPORTANT! Some settings have been modified "
                "to prevent data losses. See 'changes' key."
            )

        warnings_count = len(results["warnings"])
        self.json_output["warnings_count"] = warnings_count
        self.json_output["warnings"] = results["warnings"]

        if warnings_count > 0:
            self.warning(
                "WARNING! You are required to review the options "
                "as potentially dangerous. See 'warnings' key."
            )

        missing_files_count = len(results["missing_files"])
        self.json_output["missing_files"] = results["missing_files"]

        if missing_files_count > 0:
            # At least one file is missing, warn the user
            self.warning(
                "WARNING! Some configuration files have not been "
                "saved during backup, hence they have not been "
                "restored. See 'missing_files' key."
            )

        if results["delete_barman_wal"]:
            self.warning(
                "After the recovery, please remember to remove the "
                "'barman_wal' directory inside the PostgreSQL "
                "data directory."
            )

        if results["get_wal"]:
            self.warning(
                "WARNING: 'get-wal' is in the specified "
                "'recovery_options'. Before you start up the "
                "PostgreSQL server, please review the recovery "
                "configuration inside the target directory. "
                "Make sure that 'restore_command' can be "
                "executed by the PostgreSQL user."
            )

        self.json_output.update(
            {
                "recovery_start_time": results["recovery_start_time"].isoformat(" "),
                "recovery_start_time_timestamp": results[
                    "recovery_start_time"
                ].strftime("%s"),
                "recovery_elapsed_time": human_readable_timedelta(
                    datetime.datetime.now() - results["recovery_start_time"]
                ),
                "recovery_elapsed_time_seconds": (
                    datetime.datetime.now() - results["recovery_start_time"]
                ).total_seconds(),
            }
        )

    def init_check(self, server_name, active, disabled):
        """
        Init the check command

        :param str server_name: the server we are start listing
        :param boolean active: The server is active
        :param boolean disabled: The server is disabled
        """
        self.json_output[server_name] = {}
        self.active = active

    def result_check(self, server_name, check, status, hint=None, perfdata=None):
        """
        Record a server result of a server check
        and output it as INFO

        :param str server_name: the server is being checked
        :param str check: the check name
        :param bool status: True if succeeded
        :param str,None hint: hint to print if not None
        :param str,None perfdata: additional performance data to print if not None
        """
        self._record_check(server_name, check, status, hint, perfdata)
        check_key = self._mangle_key(check)

        self.json_output[server_name][check_key] = dict(
            status="OK" if status else "FAILED", hint=hint or ""
        )

    def init_list_backup(self, server_name, minimal=False):
        """
        Init the list-backups command

        :param str server_name: the server we are listing
        :param bool minimal: if true output only a list of backup id
        """
        self.minimal = minimal
        self.json_output[server_name] = []

    def result_list_backup(self, backup_info, backup_size, wal_size, retention_status):
        """
        Output a single backup in the list-backups command

        :param BackupInfo backup_info: backup we are displaying
        :param backup_size: size of base backup (with the required WAL files)
        :param wal_size: size of WAL files belonging to this backup
            (without the required WAL files)
        :param retention_status: retention policy status
        """
        server_name = backup_info.server_name

        # If minimal is set only output the backup id
        if self.minimal:
            self.json_output[server_name].append(backup_info.backup_id)
            return

        output = dict(
            backup_id=backup_info.backup_id,
        )

        if backup_info.status in BackupInfo.STATUS_COPY_DONE:
            output.update(
                dict(
                    end_time_timestamp=backup_info.end_time.strftime("%s"),
                    end_time=backup_info.end_time.ctime(),
                    size_bytes=backup_size,
                    wal_size_bytes=wal_size,
                    size=pretty_size(backup_size),
                    wal_size=pretty_size(wal_size),
                    status=backup_info.status,
                    retention_status=retention_status or BackupInfo.NONE,
                )
            )
            output["tablespaces"] = []
            if backup_info.tablespaces:
                for tablespace in backup_info.tablespaces:
                    output["tablespaces"].append(
                        dict(name=tablespace.name, location=tablespace.location)
                    )
        else:
            output.update(dict(status=backup_info.status))

        self.json_output[server_name].append(output)

    def result_show_backup(self, backup_ext_info):
        """
        Output all available information about a backup in show-backup command

        The argument has to be the result
        of a Server.get_backup_ext_info() call

        :param dict backup_ext_info: a dictionary containing
            the info to display
        """
        data = dict(backup_ext_info)
        server_name = data["server_name"]

        output = self.json_output[server_name] = dict(
            backup_id=data["backup_id"], status=data["status"]
        )

        if data["status"] in BackupInfo.STATUS_COPY_DONE:
            output.update(
                dict(
                    postgresql_version=data["version"],
                    pgdata_directory=data["pgdata"],
                    tablespaces=[],
                )
            )
            if data["tablespaces"]:
                for item in data["tablespaces"]:
                    output["tablespaces"].append(
                        dict(name=item.name, location=item.location, oid=item.oid)
                    )

            output["base_backup_information"] = dict(
                disk_usage=pretty_size(data["size"]),
                disk_usage_bytes=data["size"],
                disk_usage_with_wals=pretty_size(data["size"] + data["wal_size"]),
                disk_usage_with_wals_bytes=data["size"] + data["wal_size"],
            )
            if data["deduplicated_size"] is not None and data["size"] > 0:
                deduplication_ratio = 1 - (
                    float(data["deduplicated_size"]) / data["size"]
                )
                output["base_backup_information"].update(
                    dict(
                        incremental_size=pretty_size(data["deduplicated_size"]),
                        incremental_size_bytes=data["deduplicated_size"],
                        incremental_size_ratio="-{percent:.2%}".format(
                            percent=deduplication_ratio
                        ),
                    )
                )
            output["base_backup_information"].update(
                dict(
                    timeline=data["timeline"],
                    begin_wal=data["begin_wal"],
                    end_wal=data["end_wal"],
                )
            )
            if data["wal_compression_ratio"] > 0:
                output["base_backup_information"].update(
                    dict(
                        wal_compression_ratio="{percent:.2%}".format(
                            percent=data["wal_compression_ratio"]
                        )
                    )
                )
            output["base_backup_information"].update(
                dict(
                    begin_time_timestamp=data["begin_time"].strftime("%s"),
                    begin_time=data["begin_time"].isoformat(sep=" "),
                    end_time_timestamp=data["end_time"].strftime("%s"),
                    end_time=data["end_time"].isoformat(sep=" "),
                )
            )
            copy_stats = data.get("copy_stats")
            if copy_stats:
                copy_time = copy_stats.get("copy_time")
                analysis_time = copy_stats.get("analysis_time", 0)
                if copy_time:
                    output["base_backup_information"].update(
                        dict(
                            copy_time=human_readable_timedelta(
                                datetime.timedelta(seconds=copy_time)
                            ),
                            copy_time_seconds=copy_time,
                            analysis_time=human_readable_timedelta(
                                datetime.timedelta(seconds=analysis_time)
                            ),
                            analysis_time_seconds=analysis_time,
                        )
                    )
                    size = data["deduplicated_size"] or data["size"]
                    output["base_backup_information"].update(
                        dict(
                            throughput="%s/s" % pretty_size(size / copy_time),
                            throughput_bytes=size / copy_time,
                            number_of_workers=copy_stats.get("number_of_workers", 1),
                        )
                    )

            output["base_backup_information"].update(
                dict(
                    begin_offset=data["begin_offset"],
                    end_offset=data["end_offset"],
                    begin_lsn=data["begin_xlog"],
                    end_lsn=data["end_xlog"],
                )
            )

            wal_output = output["wal_information"] = dict(
                no_of_files=data["wal_until_next_num"],
                disk_usage=pretty_size(data["wal_until_next_size"]),
                disk_usage_bytes=data["wal_until_next_size"],
                wal_rate=0,
                wal_rate_per_second=0,
                compression_ratio=0,
                last_available=data["wal_last"],
                timelines=[],
            )

            # TODO: move the following calculations in a separate function
            # or upstream (backup_ext_info?) so that they are shared with
            # console output.
            if data["wals_per_second"] > 0:
                wal_output["wal_rate"] = "%0.2f/hour" % (data["wals_per_second"] * 3600)
                wal_output["wal_rate_per_second"] = data["wals_per_second"]
            if data["wal_until_next_compression_ratio"] > 0:
                wal_output["compression_ratio"] = "{percent:.2%}".format(
                    percent=data["wal_until_next_compression_ratio"]
                )
            if data["children_timelines"]:
                wal_output[
                    "_WARNING"
                ] = "WAL information is inaccurate \
                    due to multiple timelines interacting with \
                    this backup"
                for history in data["children_timelines"]:
                    wal_output["timelines"].append(str(history.tli))

            previous_backup_id = data.setdefault("previous_backup_id", "not available")
            next_backup_id = data.setdefault("next_backup_id", "not available")

            output["catalog_information"] = {
                "retention_policy": data["retention_policy_status"] or "not enforced",
                "previous_backup": previous_backup_id
                or "- (this is the oldest base backup)",
                "next_backup": next_backup_id or "- (this is the latest base backup)",
            }

        else:
            if data["error"]:
                output["error"] = data["error"]

    def init_status(self, server_name):
        """
        Init the status command

        :param str server_name: the server we are start listing
        """
        if not hasattr(self, "json_output"):
            self.json_output = {}

        self.json_output[server_name] = {}

    def result_status(self, server_name, status, description, message):
        """
        Record a result line of a server status command

        and output it as INFO

        :param str server_name: the server is being checked
        :param str status: the returned status code
        :param str description: the returned status description
        :param str,object message: status message. It will be converted to str
        """
        self.json_output[server_name][status] = dict(
            description=description, message=str(message)
        )

    def init_replication_status(self, server_name, minimal=False):
        """
        Init the 'standby-status' command

        :param str server_name: the server we are start listing
        :param str minimal: minimal output
        """
        if not hasattr(self, "json_output"):
            self.json_output = {}

        self.json_output[server_name] = {}

        self.minimal = minimal

    def result_replication_status(self, server_name, target, server_lsn, standby_info):
        """
        Record a result line of a server status command

        and output it as INFO

        :param str server_name: the replication server
        :param str target: all|hot-standby|wal-streamer
        :param str server_lsn: server's current lsn
        :param StatReplication standby_info: status info of a standby
        """

        if target == "hot-standby":
            title = "hot standby servers"
        elif target == "wal-streamer":
            title = "WAL streamers"
        else:
            title = "streaming clients"

        title_key = self._mangle_key(title)
        if title_key not in self.json_output[server_name]:
            self.json_output[server_name][title_key] = []

        self.json_output[server_name]["server_lsn"] = server_lsn if server_lsn else None

        if standby_info is not None and not len(standby_info):
            self.json_output[server_name]["standby_info"] = "No %s attached" % title
            return

        self.json_output[server_name][title_key] = []

        # Minimal output
        if self.minimal:
            for idx, standby in enumerate(standby_info):
                if not standby.replay_lsn:
                    # WAL streamer
                    self.json_output[server_name][title_key].append(
                        dict(
                            user_name=standby.usename,
                            client_addr=standby.client_addr or "socket",
                            sent_lsn=standby.sent_lsn,
                            write_lsn=standby.write_lsn,
                            sync_priority=standby.sync_priority,
                            application_name=standby.application_name,
                        )
                    )
                else:
                    # Standby
                    self.json_output[server_name][title_key].append(
                        dict(
                            sync_state=standby.sync_state[0].upper(),
                            user_name=standby.usename,
                            client_addr=standby.client_addr or "socket",
                            sent_lsn=standby.sent_lsn,
                            flush_lsn=standby.flush_lsn,
                            replay_lsn=standby.replay_lsn,
                            sync_priority=standby.sync_priority,
                            application_name=standby.application_name,
                        )
                    )
        else:
            for idx, standby in enumerate(standby_info):
                self.json_output[server_name][title_key].append({})
                json_output = self.json_output[server_name][title_key][idx]

                # Calculate differences in bytes
                lsn_diff = dict(
                    sent=diff_lsn(standby.sent_lsn, standby.current_lsn),
                    write=diff_lsn(standby.write_lsn, standby.current_lsn),
                    flush=diff_lsn(standby.flush_lsn, standby.current_lsn),
                    replay=diff_lsn(standby.replay_lsn, standby.current_lsn),
                )

                # Determine the sync stage of the client
                sync_stage = None
                if not standby.replay_lsn:
                    client_type = "WAL streamer"
                    max_level = 3
                else:
                    client_type = "standby"
                    max_level = 5
                    # Only standby can replay WAL info
                    if lsn_diff["replay"] == 0:
                        sync_stage = "5/5 Hot standby (max)"
                    elif lsn_diff["flush"] == 0:
                        sync_stage = "4/5 2-safe"  # remote flush

                # If not yet done, set the sync stage
                if not sync_stage:
                    if lsn_diff["write"] == 0:
                        sync_stage = "3/%s Remote write" % max_level
                    elif lsn_diff["sent"] == 0:
                        sync_stage = "2/%s WAL Sent (min)" % max_level
                    else:
                        sync_stage = "1/%s 1-safe" % max_level

                # Synchronous standby
                if getattr(standby, "sync_priority", None) > 0:
                    json_output["name"] = "#%s %s %s" % (
                        standby.sync_priority,
                        standby.sync_state.capitalize(),
                        client_type,
                    )

                # Asynchronous standby
                else:
                    json_output["name"] = "%s %s" % (
                        standby.sync_state.capitalize(),
                        client_type,
                    )

                json_output["application_name"] = standby.application_name
                json_output["sync_stage"] = sync_stage

                if getattr(standby, "client_addr", None):
                    json_output.update(
                        dict(
                            communication="TCP/IP",
                            ip_address=standby.client_addr,
                            port=standby.client_port,
                            host=standby.client_hostname or None,
                        )
                    )
                else:
                    json_output["communication"] = "Unix domain socket"

                json_output.update(
                    dict(
                        user_name=standby.usename,
                        current_state=standby.state,
                        current_sync_state=standby.sync_state,
                    )
                )

                if getattr(standby, "slot_name", None):
                    json_output["replication_slot"] = standby.slot_name

                json_output.update(
                    dict(
                        wal_sender_pid=standby.pid,
                        started_at=standby.backend_start.isoformat(sep=" "),
                    )
                )
                if getattr(standby, "backend_xmin", None):
                    json_output["standbys_xmin"] = standby.backend_xmin or None

                for lsn in lsn_diff.keys():
                    standby_key = lsn + "_lsn"
                    if getattr(standby, standby_key, None):
                        json_output.update(
                            {
                                lsn + "_lsn": getattr(standby, standby_key),
                                lsn + "_lsn_diff": pretty_size(lsn_diff[lsn]),
                                lsn + "_lsn_diff_bytes": lsn_diff[lsn],
                            }
                        )

    def init_list_server(self, server_name, minimal=False):
        """
        Init the list-servers command

        :param str server_name: the server we are listing
        """
        self.json_output[server_name] = {}
        self.minimal = minimal

    def result_list_server(self, server_name, description=None):
        """
        Output a result line of a list-servers command

        :param str server_name: the server is being checked
        :param str,None description: server description if applicable
        """
        self.json_output[server_name] = dict(description=description)

    def init_show_server(self, server_name, description=None):
        """
        Init the show-servers command output method

        :param str server_name: the server we are displaying
        :param str,None description: server description if applicable
        """
        self.json_output[server_name] = dict(description=description)

    def result_show_server(self, server_name, server_info):
        """
        Output the results of the show-servers command

        :param str server_name: the server we are displaying
        :param dict server_info: a dictionary containing the info to display
        """
        for status, message in sorted(server_info.items()):
            if not isinstance(message, (int, str, bool, list, dict, type(None))):
                message = str(message)

            # Prevent null values overriding existing values
            if message is None and status in self.json_output[server_name]:
                continue
            self.json_output[server_name][status] = message

    def init_check_wal_archive(self, server_name):
        """
        Init the check-wal-archive command output method

        :param str server_name: the server we are displaying
        """
        self.json_output[server_name] = {}

    def result_check_wal_archive(self, server_name):
        """
        Output the results of the check-wal-archive command

        :param str server_name: the server we are displaying
        """
        self.json_output[server_name] = (
            "WAL archive check for server %s passed" % server_name
        )


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

    def _parse_check_results(self):
        """
        Parse the check results and return the servers checked and any issues.

        :return tuple: a tuple containing a list of checked servers, a list of all
            issues found and a list of additional performance detail.
        """
        # List of all servers that have been checked
        servers = []
        # List of servers reporting issues
        issues = []
        # Nagios performance data
        perf_detail = []
        for item in self.result_check_list:
            # Keep track of all the checked servers
            if item["server_name"] not in servers:
                servers.append(item["server_name"])
            # Keep track of the servers with issues
            if not item["status"] and item["server_name"] not in issues:
                issues.append(item["server_name"])
            # Build the performance data list
            if item["check"] == "backup minimum size":
                perf_detail.append(
                    "%s=%dB" % (item["server_name"], int(item["perfdata"]))
                )
            if item["check"] == "wal size":
                perf_detail.append(
                    "%s_wals=%dB" % (item["server_name"], int(item["perfdata"]))
                )
        return servers, issues, perf_detail

    def _summarise_server_issues(self, issues):
        """
        Converts the supplied list of issues into a printable summary.

        :return tuple: A tuple where the first element is a string summarising each
            server with issues and the second element is a string containing the
            details of all failures for each server.
        """
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
                ", ".join(
                    [
                        item["check"]
                        for item in self.result_check_list
                        if item["server_name"] == server and not item["status"]
                    ]
                ),
            )
            fail_summary.append(server_fail)
            # Prepare an array with the detailed output for
            # the $LONGSERVICEOUTPUT$ macro of the Nagios output
            # line format:
            # <servername>.<failed_check1>: FAILED
            # <servername>.<failed_check2>: FAILED (Hint if present)
            # <servername2.<failed_check1>: FAILED
            # .....
            for issue in self.result_check_list:
                if issue["server_name"] == server and not issue["status"]:
                    fail_detail = "%s.%s: FAILED" % (server, issue["check"])
                    if issue["hint"]:
                        fail_detail += " (%s)" % issue["hint"]
                    details.append(fail_detail)
        return fail_summary, details

    def _print_check_failure(self, servers, issues, perf_detail):
        """Prints the output for a failed check."""
        # Generate the performance data message - blank string if no perf detail
        perf_detail_message = perf_detail and "|%s" % " ".join(perf_detail) or ""

        fail_summary, details = self._summarise_server_issues(issues)
        # Append the summary of failures to the first line of the output
        # using * as delimiter
        if len(servers) == 1:
            print(
                "BARMAN CRITICAL - server %s has issues * %s%s"
                % (servers[0], " * ".join(fail_summary), perf_detail_message)
            )
        else:
            print(
                "BARMAN CRITICAL - %d server out of %d have issues * "
                "%s%s"
                % (
                    len(issues),
                    len(servers),
                    " * ".join(fail_summary),
                    perf_detail_message,
                )
            )

        # add the detailed list to the output
        for issue in details:
            print(issue)

    def _print_check_success(self, servers, issues=None, perf_detail=None):
        """Prints the output for a successful check."""
        if issues is None:
            issues = []

        # Generate the issues message - blank string if no issues
        issues_message = "".join([" * IGNORING: %s" % issue for issue in issues])
        # Generate the performance data message - blank string if no perf detail
        perf_detail_message = perf_detail and "|%s" % " ".join(perf_detail) or ""

        # Some issues, but only in skipped server
        good = [item for item in servers if item not in issues]
        # Display the output message for a single server check
        if len(good) == 0:
            print("BARMAN OK - No server configured%s" % issues_message)
        elif len(good) == 1:
            print(
                "BARMAN OK - Ready to serve the Espresso backup "
                "for %s%s%s" % (good[0], issues_message, perf_detail_message)
            )
        else:
            # Display the output message for several servers, using
            # '*' as delimiter
            print(
                "BARMAN OK - Ready to serve the Espresso backup "
                "for %d servers * %s%s%s"
                % (len(good), " * ".join(good), issues_message, perf_detail_message)
            )

    def close(self):
        """
        Display the result of a check run as expected by Nagios.

        Also set the exit code as 2 (CRITICAL) in case of errors
        """

        global error_occurred, error_exit_code

        servers, issues, perf_detail = self._parse_check_results()

        # Global error (detected at configuration level)
        if len(issues) == 0 and error_occurred:
            print("BARMAN CRITICAL - Global configuration errors")
            error_exit_code = 2
            return

        if len(issues) > 0 and error_occurred:
            self._print_check_failure(servers, issues, perf_detail)
            error_exit_code = 2
        else:
            self._print_check_success(servers, issues, perf_detail)


#: This dictionary acts as a registry of available OutputWriters
AVAILABLE_WRITERS = {
    "console": ConsoleOutputWriter,
    "json": JsonOutputWriter,
    # nagios is not registered as it isn't a general purpose output writer
    # 'nagios': NagiosOutputWriter,
}

#: The default OutputWriter
DEFAULT_WRITER = "console"

#: the current active writer. Initialized according DEFAULT_WRITER on load
_writer = AVAILABLE_WRITERS[DEFAULT_WRITER]()
