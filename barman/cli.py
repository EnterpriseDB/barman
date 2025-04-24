# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2011-2025
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

import argparse
import json
import logging
import os
import sys
from argparse import SUPPRESS, ArgumentParser, ArgumentTypeError, HelpFormatter
from collections import OrderedDict
from contextlib import closing

import barman.config
import barman.diagnose
import barman.utils
from barman import output
from barman.annotations import KeepManager
from barman.backup_manifest import BackupManifest
from barman.config import ConfigChangesProcessor, RecoveryOptions, parse_staging_path
from barman.exceptions import (
    BadXlogSegmentName,
    LockFileBusy,
    RecoveryException,
    SyncError,
    WalArchiveContentError,
)
from barman.infofile import BackupInfo, WalFileInfo
from barman.lockfile import ConfigUpdateLock
from barman.process import ProcessManager
from barman.server import Server
from barman.storage.local_file_manager import LocalFileManager
from barman.utils import (
    RESERVED_BACKUP_IDS,
    SHA256,
    BarmanEncoder,
    check_backup_name,
    check_non_negative,
    check_positive,
    check_tli,
    configure_logging,
    drop_privileges,
    force_str,
    get_backup_id_using_shortcut,
    get_log_levels,
    parse_log_level,
    parse_target_tli,
)
from barman.xlog import check_archive_usable

if sys.version_info.major < 3:
    from argparse import Action, _ActionsContainer, _SubParsersAction
try:
    import argcomplete
except ImportError:
    argcomplete = None


_logger = logging.getLogger(__name__)


# Support aliases for argparse in python2.
# Derived from https://gist.github.com/sampsyo/471779 and based on the
# initial patchset for CPython for supporting aliases in argparse.
# Licensed under CC0 1.0
if sys.version_info.major < 3:

    class AliasedSubParsersAction(_SubParsersAction):
        old_init = staticmethod(_ActionsContainer.__init__)

        @staticmethod
        def _containerInit(
            self, description, prefix_chars, argument_default, conflict_handler
        ):
            AliasedSubParsersAction.old_init(
                self, description, prefix_chars, argument_default, conflict_handler
            )
            self.register("action", "parsers", AliasedSubParsersAction)

        class _AliasedPseudoAction(Action):
            def __init__(self, name, aliases, help):
                dest = name
                if aliases:
                    dest += " (%s)" % ",".join(aliases)
                sup = super(AliasedSubParsersAction._AliasedPseudoAction, self)
                sup.__init__(option_strings=[], dest=dest, help=help)

        def add_parser(self, name, **kwargs):
            aliases = kwargs.pop("aliases", [])
            parser = super(AliasedSubParsersAction, self).add_parser(name, **kwargs)

            # Make the aliases work.
            for alias in aliases:
                self._name_parser_map[alias] = parser
            # Make the help text reflect them, first removing old help entry.
            if "help" in kwargs:
                help_text = kwargs.pop("help")
                self._choices_actions.pop()
                pseudo_action = self._AliasedPseudoAction(name, aliases, help_text)
                self._choices_actions.append(pseudo_action)

            return parser

    # override argparse to register new subparser action by default
    _ActionsContainer.__init__ = AliasedSubParsersAction._containerInit


class OrderedHelpFormatter(HelpFormatter):
    def _format_usage(self, usage, actions, groups, prefix):
        for action in actions:
            if not action.option_strings:
                action.choices = OrderedDict(sorted(action.choices.items()))
        return super(OrderedHelpFormatter, self)._format_usage(
            usage, actions, groups, prefix
        )


p = ArgumentParser(
    epilog="Barman by EnterpriseDB (www.enterprisedb.com)",
    formatter_class=OrderedHelpFormatter,
)
p.add_argument(
    "-v",
    "--version",
    action="version",
    version="%s\n\nBarman by EnterpriseDB (www.enterprisedb.com)" % barman.__version__,
)
p.add_argument(
    "-c",
    "--config",
    help="uses a configuration file "
    "(defaults: %s)" % ", ".join(barman.config.Config.CONFIG_FILES),
    default=SUPPRESS,
)
p.add_argument(
    "--color",
    "--colour",
    help="Whether to use colors in the output",
    choices=["never", "always", "auto"],
    default="auto",
)
p.add_argument(
    "--log-level",
    help="Override the default log level",
    choices=list(get_log_levels()),
    default=SUPPRESS,
)
p.add_argument("-q", "--quiet", help="be quiet", action="store_true")
p.add_argument("-d", "--debug", help="debug output", action="store_true")
p.add_argument(
    "-f",
    "--format",
    help="output format",
    choices=output.AVAILABLE_WRITERS.keys(),
    default=output.DEFAULT_WRITER,
)

subparsers = p.add_subparsers(dest="command")


def argument(*name_or_flags, **kwargs):
    """Convenience function to properly format arguments to pass to the
    command decorator.
    """

    # Remove the completer keyword argument from the dictionary
    completer = kwargs.pop("completer", None)
    return (list(name_or_flags), completer, kwargs)


def command(args=None, parent=subparsers, cmd_aliases=None):
    """Decorator to define a new subcommand in a sanity-preserving way.
    The function will be stored in the ``func`` variable when the parser
    parses arguments so that it can be called directly like so::
        args = cli.parse_args()
        args.func(args)
    Usage example::
        @command([argument("-d", help="Enable debug mode", action="store_true")])
        def command(args):
            print(args)
    Then on the command line::
        $ python cli.py command -d
    """

    if args is None:
        args = []
    if cmd_aliases is None:
        cmd_aliases = []

    def decorator(func):
        parser = parent.add_parser(
            func.__name__.replace("_", "-"),
            description=func.__doc__,
            help=func.__doc__,
            aliases=cmd_aliases,
        )
        parent._choices_actions = sorted(parent._choices_actions, key=lambda x: x.dest)
        for arg in args:
            if arg[1]:
                parser.add_argument(*arg[0], **arg[2]).completer = arg[1]
            else:
                parser.add_argument(*arg[0], **arg[2])
        parser.set_defaults(func=func)
        return func

    return decorator


@command()
def help(args=None):
    """
    show this help message and exit
    """
    p.print_help()


def check_target_action(value):
    """
    Check the target action option

    :param value: str containing the value to check
    """
    if value is None:
        return None

    if value in ("pause", "shutdown", "promote"):
        return value

    raise ArgumentTypeError("'%s' is not a valid recovery target action" % value)


@command(
    [argument("--minimal", help="machine readable output", action="store_true")],
    cmd_aliases=["list-server"],
)
def list_servers(args):
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

        output.init("list_server", name, minimal=args.minimal)
        description = server.config.description or ""
        # If the server has been manually disabled
        if not server.config.active:
            description += " (inactive)"
        # If server has configuration errors
        elif server.config.disabled:
            description += " (WARNING: disabled)"
        # If server is a passive node
        if server.passive_node:
            description += " (Passive)"
        output.result("list_server", name, description)
    output.close_and_exit()


@command(
    [argument("server_name", help="specifies the server name")],
    cmd_aliases=["list-process"],
)
def list_processes(args=None):
    """
    List all the active subprocesses started by the specified server.
    """
    server = get_server(args)
    proc_manager = ProcessManager(server.config)
    processes = proc_manager.list()
    output.result("list_processes", processes, server.config.name)
    output.close_and_exit()


@command(
    [
        argument("server_name", help="specifies the server name"),
        argument("task", help="the task name to terminate (e.g. backup, receive-wal)"),
    ],
)
def terminate_process(args):
    """
    Terminate a Barman server subprocess specified by task name.
    """
    server = get_server(args)
    server.kill(args.task)
    output.close_and_exit()


@command(
    [
        argument(
            "--keep-descriptors",
            help="Keep the stdout and the stderr streams attached to Barman subprocesses",
            action="store_true",
        )
    ]
)
def cron(args):
    """
    Run maintenance tasks (global command)
    """
    # Before doing anything, check if the configuration file has been updated
    try:
        with ConfigUpdateLock(barman.__config__.barman_lock_directory):
            procesor = ConfigChangesProcessor(barman.__config__)
            procesor.process_conf_changes_queue()

    except LockFileBusy:
        output.warning("another process is updating barman configuration files")

    # Skip inactive and temporarily disabled servers
    servers = get_server_list(
        skip_inactive=True, skip_disabled=True, wal_streaming=True
    )
    for name in sorted(servers):
        server = servers[name]

        # Exception: manage_server_command is not invoked here
        # Normally you would call manage_server_command to check if the
        # server is None and to report inactive and disabled servers,
        # but here we have only active and well configured servers.

        try:
            server.cron(keep_descriptors=args.keep_descriptors)
        except Exception:
            # A cron should never raise an exception, so this code
            # should never be executed. However, it is here to protect
            # unrelated servers in case of unexpected failures.
            output.exception(
                "Unable to run cron on server '%s', "
                "please look in the barman log file for more details.",
                name,
            )
    # Lockfile directory cleanup
    barman.utils.lock_files_cleanup(
        barman.__config__.barman_lock_directory,
        barman.__config__.lock_directory_cleanup,
    )
    output.close_and_exit()


@command(cmd_aliases=["lock-directory-cleanup"])
def lock_directory_cleanup(args=None):
    """
    Cleanup command for the lock directory, takes care of leftover lock files.
    """
    barman.utils.lock_files_cleanup(barman.__config__.barman_lock_directory, True)
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
    current_list = getattr(parsed_args, "server_name", None) or ()
    for conf in barman.__config__.servers():
        if conf.name.startswith(prefix) and conf.name not in current_list:
            yield conf.name
    if len(current_list) == 0 and "all".startswith(prefix):
        yield "all"


# noinspection PyUnusedLocal
def backup_completer(prefix, parsed_args, **kwargs):
    global_config(parsed_args)
    server = get_server(parsed_args)

    backups = server.get_available_backups()
    for backup_id in sorted(backups, reverse=True):
        if backup_id.startswith(prefix):
            yield backup_id
    for special_id in RESERVED_BACKUP_IDS:
        if len(backups) > 0 and special_id.startswith(prefix):
            yield special_id


@command(
    [
        argument(
            "server_name",
            completer=server_completer_all,
            nargs="+",
            help="specifies the server names for the backup command "
            "('all' will show all available servers)",
        ),
        argument(
            "--immediate-checkpoint",
            help="forces the initial checkpoint to be done as quickly as possible",
            dest="immediate_checkpoint",
            action="store_true",
            default=SUPPRESS,
        ),
        argument(
            "--no-immediate-checkpoint",
            help="forces the initial checkpoint to be spread",
            dest="immediate_checkpoint",
            action="store_false",
            default=SUPPRESS,
        ),
        argument(
            "--incremental",
            completer=backup_completer,
            dest="backup_id",
            help="performs an incremental backup. An ID of a previous backup must "
            "be provided ('latest' and 'latest-full' are also available options)",
        ),
        argument(
            "--reuse-backup",
            nargs="?",
            choices=barman.config.REUSE_BACKUP_VALUES,
            default=None,
            const="link",
            help="use the previous backup to improve transfer-rate. "
            'If no argument is given "link" is assumed',
        ),
        argument(
            "--retry-times",
            help="Number of retries after an error if base backup copy fails.",
            type=check_non_negative,
        ),
        argument(
            "--retry-sleep",
            help="Wait time after a failed base backup copy, before retrying.",
            type=check_non_negative,
        ),
        argument(
            "--no-retry",
            help="Disable base backup copy retry logic.",
            dest="retry_times",
            action="store_const",
            const=0,
        ),
        argument(
            "--jobs",
            "-j",
            help="Run the copy in parallel using NJOBS processes.",
            type=check_positive,
            metavar="NJOBS",
        ),
        argument(
            "--jobs-start-batch-period",
            help="The time period in seconds over which a single batch of jobs will "
            "be started.",
            type=check_positive,
        ),
        argument(
            "--jobs-start-batch-size",
            help="The maximum number of parallel Rsync jobs to start in a single "
            "batch.",
            type=check_positive,
        ),
        argument(
            "--bwlimit",
            help="maximum transfer rate in kilobytes per second. "
            "A value of 0 means no limit. Overrides 'bandwidth_limit' "
            "configuration option.",
            metavar="KBPS",
            type=check_non_negative,
            default=SUPPRESS,
        ),
        argument(
            "--wait",
            "-w",
            help="wait for all the required WAL files to be archived",
            dest="wait",
            action="store_true",
            default=False,
        ),
        argument(
            "--wait-timeout",
            help="the time, in seconds, spent waiting for the required "
            "WAL files to be archived before timing out",
            dest="wait_timeout",
            metavar="TIMEOUT",
            default=None,
            type=check_non_negative,
        ),
        argument(
            "--keepalive-interval",
            help="An interval, in seconds, at which a heartbeat query will be sent "
            "to the server to keep the libpq connection alive during an Rsync backup.",
            dest="keepalive_interval",
            type=check_non_negative,
        ),
        argument(
            "--name",
            help="a name which can be used to reference this backup in barman "
            "commands such as restore and delete",
            dest="backup_name",
            default=None,
            type=check_backup_name,
        ),
        argument(
            "--manifest",
            help="forces the creation of the backup manifest file for the "
            "rsync backup method",
            dest="automatic_manifest",
            action="store_true",
            default=SUPPRESS,
        ),
        argument(
            "--no-manifest",
            help="disables the creation of the backup manifest file for the "
            "rsync backup method",
            dest="automatic_manifest",
            action="store_false",
            default=SUPPRESS,
        ),
    ]
)
def backup(args):
    """
    Perform a full backup for the given server (supports 'all')
    """
    servers = get_server_list(args, skip_inactive=True, skip_passive=True)
    for name in sorted(servers):
        server = servers[name]

        # Skip the server (apply general rule)
        if not manage_server_command(server, name):
            continue

        incremental_kwargs = {}

        if args.backup_id is not None:
            parent_backup_info = parse_backup_id(server, args)
            if parent_backup_info:
                incremental_kwargs["parent_backup_id"] = parent_backup_info.backup_id
        if args.reuse_backup is not None:
            server.config.reuse_backup = args.reuse_backup
        if args.retry_sleep is not None:
            server.config.basebackup_retry_sleep = args.retry_sleep
        if args.retry_times is not None:
            server.config.basebackup_retry_times = args.retry_times
        if args.keepalive_interval is not None:
            server.config.keepalive_interval = args.keepalive_interval
        if hasattr(args, "immediate_checkpoint"):
            # As well as overriding the immediate_checkpoint value in the config
            # we must also update the immediate_checkpoint attribute on the
            # postgres connection because it has already been set from the config
            server.config.immediate_checkpoint = args.immediate_checkpoint
            server.postgres.immediate_checkpoint = args.immediate_checkpoint
        if hasattr(args, "automatic_manifest"):
            # Override the set value for the autogenerate_manifest config option.
            # The backup executor class will automatically ignore --manifest requests
            # for backup methods different from rsync.
            server.config.autogenerate_manifest = args.automatic_manifest
        if args.jobs is not None:
            server.config.parallel_jobs = args.jobs
        if args.jobs_start_batch_size is not None:
            server.config.parallel_jobs_start_batch_size = args.jobs_start_batch_size
        if args.jobs_start_batch_period is not None:
            server.config.parallel_jobs_start_batch_period = (
                args.jobs_start_batch_period
            )
        if hasattr(args, "bwlimit"):
            server.config.bandwidth_limit = args.bwlimit
        with closing(server):
            server.backup(
                wait=args.wait,
                wait_timeout=args.wait_timeout,
                backup_name=args.backup_name,
                **incremental_kwargs,
            )
    output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer_all,
            nargs="+",
            help="specifies the server name for the command "
            "('all' will show all available servers)",
        ),
        argument("--minimal", help="machine readable output", action="store_true"),
    ],
    cmd_aliases=["list-backup"],
)
def list_backups(args):
    """
    List available backups for the given server (supports 'all')
    """
    servers = get_server_list(args, skip_inactive=True)
    for name in sorted(servers):
        server = servers[name]

        # Skip the server (apply general rule)
        if not manage_server_command(server, name):
            continue

        output.init("list_backup", name, minimal=args.minimal)
        with closing(server):
            server.list_backups()
    output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer_all,
            nargs="+",
            help="specifies the server name for the command",
        )
    ]
)
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

        output.init("status", name)
        with closing(server):
            server.status()
    output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer_all,
            nargs="+",
            help="specifies the server name for the command "
            "('all' will show all available servers)",
        ),
        argument("--minimal", help="machine readable output", action="store_true"),
        argument(
            "--target",
            choices=("all", "hot-standby", "wal-streamer"),
            default="all",
            help="""
                        Possible values are: 'hot-standby' (only hot standby servers),
                        'wal-streamer' (only WAL streaming clients, such as pg_receivewal),
                        'all' (any of them). Defaults to %(default)s""",
        ),
        argument(
            "--source",
            choices=("backup-host", "wal-host"),
            default="backup-host",
            help="""
                        Possible values are: 'backup-host' (list clients using the
                        backup conninfo for a server) or `wal-host` (list clients using
                        the WAL streaming conninfo for a server). Defaults to
                        %(default)s""",
        ),
    ]
)
def replication_status(args):
    """
    Shows live information and status of any streaming client
    """
    wal_streaming = args.source == "wal-host"
    servers = get_server_list(
        args, skip_inactive=True, skip_passive=True, wal_streaming=wal_streaming
    )
    for name in sorted(servers):
        server = servers[name]

        # Skip the server (apply general rule)
        if not manage_server_command(server, name):
            continue

        with closing(server):
            output.init("replication_status", name, minimal=args.minimal)
            server.replication_status(args.target)
    output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer_all,
            nargs="+",
            help="specifies the server name for the command ",
        )
    ]
)
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


@command(
    [
        argument(
            "server_name",
            completer=server_completer,
            help="specifies the server name for the command ",
        ),
        argument("--target-tli", help="target timeline", type=check_tli),
        argument(
            "--target-time",
            help="target time. You can use any valid unambiguous representation. "
            'e.g: "YYYY-MM-DD HH:MM:SS.mmm"',
        ),
        argument("--target-xid", help="target transaction ID"),
        argument("--target-lsn", help="target LSN (Log Sequence Number)"),
        argument(
            "--target-name",
            help="target name created previously with "
            "pg_create_restore_point() function call",
        ),
        argument(
            "--target-immediate",
            help="end recovery as soon as a consistent state is reached",
            action="store_true",
            default=False,
        ),
        argument(
            "--exclusive", help="set target to be non inclusive", action="store_true"
        ),
        argument(
            "--tablespace",
            help="tablespace relocation rule",
            metavar="NAME:LOCATION",
            action="append",
        ),
        argument(
            "--remote-ssh-command",
            metavar="SSH_COMMAND",
            help="This options activates remote recovery, by specifying the secure "
            "shell command to be launched on a remote host. It is "
            'the equivalent of the "ssh_command" server option in '
            "the configuration file for remote recovery. "
            'Example: "ssh postgres@db2"',
        ),
        argument(
            "backup_id",
            completer=backup_completer,
            help="specifies the backup ID to restore",
        ),
        argument(
            "destination_directory",
            help="the directory where the new server is created",
        ),
        argument(
            "--staging-wal-directory",
            help="a staging directory in the target host for WAL files when performing "
            "PITR. If unspecified, it uses a `barman_wal` directory inside the "
            "destination directory.",
        ),
        argument(
            "--bwlimit",
            help="maximum transfer rate in kilobytes per second. "
            "A value of 0 means no limit. Overrides 'bandwidth_limit' "
            "configuration option.",
            metavar="KBPS",
            type=check_non_negative,
            default=SUPPRESS,
        ),
        argument(
            "--retry-times",
            help="Number of retries after an error if base backup copy fails.",
            type=check_non_negative,
        ),
        argument(
            "--retry-sleep",
            help="Wait time after a failed base backup copy, before retrying.",
            type=check_non_negative,
        ),
        argument(
            "--no-retry",
            help="Disable base backup copy retry logic.",
            dest="retry_times",
            action="store_const",
            const=0,
        ),
        argument(
            "--jobs",
            "-j",
            help="Run the copy in parallel using NJOBS processes.",
            type=check_positive,
            metavar="NJOBS",
        ),
        argument(
            "--jobs-start-batch-period",
            help="The time period in seconds over which a single batch of jobs will "
            "be started.",
            type=check_positive,
        ),
        argument(
            "--jobs-start-batch-size",
            help="The maximum number of Rsync jobs to start in a single batch.",
            type=check_positive,
        ),
        argument(
            "--get-wal",
            help="Enable the get-wal option during the recovery.",
            dest="get_wal",
            action="store_true",
            default=SUPPRESS,
        ),
        argument(
            "--no-get-wal",
            help="Disable the get-wal option during recovery.",
            dest="get_wal",
            action="store_false",
            default=SUPPRESS,
        ),
        argument(
            "--network-compression",
            help="Enable network compression during remote recovery.",
            dest="network_compression",
            action="store_true",
            default=SUPPRESS,
        ),
        argument(
            "--no-network-compression",
            help="Disable network compression during remote recovery.",
            dest="network_compression",
            action="store_false",
            default=SUPPRESS,
        ),
        argument(
            "--target-action",
            help="Specifies what action the server should take once the "
            "recovery target is reached. This option is not allowed for "
            "PostgreSQL < 9.1. If PostgreSQL is between 9.1 and 9.4 included "
            'the only allowed value is "pause". If PostgreSQL is 9.5 or newer '
            'the possible values are "shutdown", "pause", "promote".',
            dest="target_action",
            type=check_target_action,
            default=SUPPRESS,
        ),
        argument(
            "--standby-mode",
            dest="standby_mode",
            action="store_true",
            default=SUPPRESS,
            help="Enable standby mode when starting the restored PostgreSQL instance",
        ),
        argument(
            "--recovery-staging-path",
            dest="recovery_staging_path",
            help=(
                "A path to a location on the recovery host where compressed backup "
                "files will be staged during the recovery. This location must have "
                "enough available space to temporarily hold the full compressed "
                "backup. This option is *required* when restoring from a compressed "
                "backup."
            ),
        ),
        argument(
            "--local-staging-path",
            help=(
                "A path to a location on the local host where incremental backups "
                "will be combined during the recovery. This location must have "
                "enough available space to temporarily hold the new synthetic "
                "backup. This option is *required* when restoring from an "
                "incremental backup."
            ),
        ),
        argument(
            "--recovery-conf-filename",
            dest="recovery_conf_filename",
            help=(
                "Name of the file to which recovery configuration options will be "
                "added for PostgreSQL 12 and later (default: postgresql.auto.conf)."
            ),
        ),
        argument(
            "--snapshot-recovery-instance",
            help="Instance where the disks recovered from the snapshots are attached",
        ),
        argument(
            "--snapshot-recovery-zone",
            help=(
                "Zone containing the instance and disks for the snapshot recovery "
                "(deprecated: replaced by --gcp-zone)"
            ),
        ),
        argument(
            "--gcp-zone",
            help="Zone containing the instance and disks for the snapshot recovery",
        ),
        argument(
            "--azure-resource-group",
            help="Azure resource group containing the instance and disks for recovery "
            "of a snapshot backup",
        ),
        argument(
            "--aws-region",
            help="The name of the AWS region containing the EC2 VM and storage "
            "volumes for recovery of a snapshot backup",
        ),
    ],
    cmd_aliases=["recover"],
)
def restore(args):
    """
    Restore a server at a given time, name, LSN or xid
    """
    server = get_server(args)

    # PostgreSQL supports multiple parameters to specify when the recovery
    # process will end, and in that case the last entry in recovery
    # configuration files will be used. See [1]
    #
    # Since the meaning of the target options is not dependent on the order
    # of parameters, we decided to make the target options mutually exclusive.
    #
    # [1]: https://www.postgresql.org/docs/current/static/
    #   recovery-target-settings.html

    target_options = [
        "target_time",
        "target_xid",
        "target_lsn",
        "target_name",
        "target_immediate",
    ]

    specified_target_options = [
        option for option in target_options if getattr(args, option, None)
    ]
    if len(specified_target_options) > 1:
        output.error("You cannot specify multiple targets for the recovery operation")
        output.close_and_exit()

    target_option = (
        specified_target_options[0] if len(specified_target_options) == 1 else None
    )
    target_tli = None
    backup_info = None
    if args.backup_id != "auto":
        backup_info = parse_backup_id(server, args)
    else:
        target = getattr(args, target_option) if target_option else None
        # "Parse" the string value to integer for target_tli if passed as a string
        # ("current", "latest")
        target_tli = parse_target_tli(
            obj=server.backup_manager, target_tli=args.target_tli
        )
        #  Error out on recovery targets that are not allowed.
        if target_option in {"target_immediate", "target_xid", "target_name"}:
            output.error(
                "For PITR without a backup_id, the only possible recovery targets "
                "are target_time and target_lsn. '%s' recovery target is not "
                "allowed without a backup_id." % target_option
            )
            output.close_and_exit()
        # Search for a candidate backup based on recovery targets if "backup_id" is None
        elif target_option is None:
            if target_tli is not None:
                backup_id = server.get_last_backup_id_from_target_tli(target_tli)
            else:
                backup_id = server.get_last_backup_id()
        elif target_option == "target_time":
            backup_id = server.get_closest_backup_id_from_target_time(
                target, target_tli
            )
        elif target_option == "target_lsn":
            backup_id = server.get_closest_backup_id_from_target_lsn(target, target_tli)
        # If no candidate backup_id is found, error out.
        if backup_id is None:
            output.error("Cannot find any candidate backup for recovery.")
            output.close_and_exit()

        backup_info = server.get_backup(backup_id)

    if backup_info.status not in BackupInfo.STATUS_COPY_DONE:
        output.error(
            "Cannot restore from backup '%s' of server '%s': "
            "backup status is not DONE",
            args.backup_id,
            server.config.name,
        )
        output.close_and_exit()

    # If the backup to be recovered is compressed then there are additional
    # checks to be carried out
    if backup_info.compression is not None:
        # Set the recovery staging path from the cli if it is set
        if args.recovery_staging_path is not None:
            try:
                recovery_staging_path = parse_staging_path(args.recovery_staging_path)
            except ValueError as exc:
                output.error("Cannot parse recovery staging path: %s", str(exc))
                output.close_and_exit()
            server.config.recovery_staging_path = recovery_staging_path
        # If the backup is compressed but there is no recovery_staging_path
        # then this is an error - the user *must* tell barman where recovery
        # data can be staged.
        if server.config.recovery_staging_path is None:
            output.error(
                "Cannot restore from backup '%s' of server '%s': "
                "backup is compressed with %s compression but no recovery "
                "staging path is provided. Either set recovery_staging_path "
                "in the Barman config or use the --recovery-staging-path "
                "argument.",
                args.backup_id,
                server.config.name,
                backup_info.compression,
            )
            output.close_and_exit()

    # If the backup to be recovered is incremental or encrypted then there are
    # additional checks to be carried out. Note that currently Barman does not
    # support neither taking nor restoring backups that are both incremental
    # AND encrypted -- you can have only one or the other feature.
    if backup_info.is_incremental or backup_info.encryption:
        # Set the local staging path from the cli if it is set
        if args.local_staging_path is not None:
            try:
                local_staging_path = parse_staging_path(args.local_staging_path)
            except ValueError as exc:
                output.error("Cannot parse local staging path: %s", str(exc))
                output.close_and_exit()
            server.config.local_staging_path = local_staging_path
        # If the backup is incremental or encrypted, but no ``local_staging_path`` is
        # provided, this is considered an error — the user must specify a staging path
        # to combine or decrypt.
        if server.config.local_staging_path is None:
            if backup_info.is_incremental:
                output.error(
                    "Cannot restore from backup '%s' of server '%s': "
                    "backup will be combined with pg_combinebackup in the "
                    "barman host but no local staging path is provided. "
                    "Either set local_staging_path in the Barman config "
                    "or use the --local-staging-path argument.",
                    args.backup_id,
                    server.config.name,
                )
                output.close_and_exit()
            # If backup_info is not incremental, it is encrypted.
            else:
                output.error(
                    "Cannot restore from backup '%s' of server '%s': "
                    "backup is encrypted with '%s' and it will be decrypted in the "
                    "barman host but no local staging path is provided. "
                    "Either set local_staging_path in the Barman config "
                    "or use the --local-staging-path argument.",
                    args.backup_id,
                    server.config.name,
                    backup_info.encryption,
                )
                output.close_and_exit()
    # decode the tablespace relocation rules
    tablespaces = {}
    if args.tablespace:
        for rule in args.tablespace:
            try:
                tablespaces.update([rule.split(":", 1)])
            except ValueError:
                output.error(
                    "Invalid tablespace relocation rule '%s'\n"
                    "HINT: The valid syntax for a relocation rule is "
                    "NAME:LOCATION",
                    rule,
                )
                output.close_and_exit()

    # validate the rules against the tablespace list
    valid_tablespaces = []
    if backup_info.tablespaces:
        valid_tablespaces = [
            tablespace_data.name for tablespace_data in backup_info.tablespaces
        ]
    for item in tablespaces:
        if item not in valid_tablespaces:
            output.error(
                "Invalid tablespace name '%s'\n"
                "HINT: Please use any of the following "
                "tablespaces: %s",
                item,
                ", ".join(valid_tablespaces),
            )
            output.close_and_exit()

    # explicitly disallow the rsync remote syntax (common mistake)
    if ":" in args.destination_directory:
        output.error(
            "The destination directory parameter "
            "cannot contain the ':' character\n"
            "HINT: If you want to do a remote recovery you have to use "
            "the --remote-ssh-command option"
        )
        output.close_and_exit()
    if args.retry_sleep is not None:
        server.config.basebackup_retry_sleep = args.retry_sleep
    if args.retry_times is not None:
        server.config.basebackup_retry_times = args.retry_times
    if hasattr(args, "get_wal"):
        if args.get_wal:
            server.config.recovery_options.add(RecoveryOptions.GET_WAL)
        elif RecoveryOptions.GET_WAL in server.config.recovery_options:
            server.config.recovery_options.remove(RecoveryOptions.GET_WAL)
    if args.jobs is not None:
        server.config.parallel_jobs = args.jobs
    if args.jobs_start_batch_size is not None:
        server.config.parallel_jobs_start_batch_size = args.jobs_start_batch_size
    if args.jobs_start_batch_period is not None:
        server.config.parallel_jobs_start_batch_period = args.jobs_start_batch_period
    if hasattr(args, "bwlimit"):
        server.config.bandwidth_limit = args.bwlimit

    if hasattr(args, "network_compression"):
        if args.network_compression and args.remote_ssh_command is None:
            output.error(
                "Network compression can only be used with "
                "remote recovery.\n"
                "HINT: If you want to do a remote recovery "
                "you have to use the --remote-ssh-command option"
            )
            output.close_and_exit()
        server.config.network_compression = args.network_compression

    if backup_info.snapshots_info is not None:
        missing_args = []
        if not args.snapshot_recovery_instance:
            missing_args.append("--snapshot-recovery-instance")
        if len(missing_args) > 0:
            output.error(
                "Backup %s is a snapshot backup and the following required arguments "
                "have not been provided: %s",
                backup_info.backup_id,
                ", ".join(missing_args),
            )
            output.close_and_exit()
        if tablespaces != {}:
            output.error(
                "Backup %s is a snapshot backup therefore tablespace relocation rules "
                "cannot be used.",
                backup_info.backup_id,
            )
            output.close_and_exit()
        # Set the snapshot keyword arguments to be passed to the recovery executor
        snapshot_kwargs = {
            "recovery_instance": args.snapshot_recovery_instance,
        }
        # Special handling for deprecated snapshot_recovery_zone arg
        if args.gcp_zone is None and args.snapshot_recovery_zone is not None:
            args.gcp_zone = args.snapshot_recovery_zone
        # Override provider-specific options in the config
        for arg in (
            "aws_region",
            "azure_resource_group",
            "gcp_zone",
        ):
            value = getattr(args, arg)
            if value is not None:
                setattr(server.config, arg, value)
    else:
        unexpected_args = []
        if args.snapshot_recovery_instance:
            unexpected_args.append("--snapshot-recovery-instance")
        if len(unexpected_args) > 0:
            output.error(
                "Backup %s is not a snapshot backup but the following snapshot "
                "arguments have been used: %s",
                backup_info.backup_id,
                ", ".join(unexpected_args),
            )
            output.close_and_exit()
        # An empty dict is used so that snapshot-specific arguments are not passed to
        # non-snapshot recovery executors
        snapshot_kwargs = {}

    with closing(server):
        try:
            server.recover(
                backup_info,
                args.destination_directory,
                wal_dest=args.staging_wal_directory,
                tablespaces=tablespaces,
                target_tli=args.target_tli,
                target_time=args.target_time,
                target_xid=args.target_xid,
                target_lsn=args.target_lsn,
                target_name=args.target_name,
                target_immediate=args.target_immediate,
                exclusive=args.exclusive,
                remote_command=args.remote_ssh_command,
                target_action=getattr(args, "target_action", None),
                standby_mode=getattr(args, "standby_mode", None),
                recovery_conf_filename=args.recovery_conf_filename,
                **snapshot_kwargs,
            )
        except RecoveryException as exc:
            output.error(force_str(exc))

    output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer_all,
            nargs="+",
            help="specifies the server names to show "
            "('all' will show all available servers)",
        )
    ],
    cmd_aliases=["show-server"],
)
def show_servers(args):
    """
    Show all configuration parameters for the specified servers
    """
    servers = get_server_list(args)
    for name in sorted(servers):
        server = servers[name]

        # Skip the server (apply general rule)
        if not manage_server_command(
            server,
            name,
            skip_inactive=False,
            skip_disabled=False,
            disabled_is_error=False,
        ):
            continue

        # If the server has been manually disabled
        if not server.config.active:
            description = "(inactive)"
        # If server has configuration errors
        elif server.config.disabled:
            description = "(WARNING: disabled)"
        else:
            description = None
        output.init("show_server", name, description=description)
        with closing(server):
            server.show()
    output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer_all,
            nargs="+",
            help="specifies the server name target of the switch-wal command",
        ),
        argument(
            "--force",
            help="forces the switch of a WAL by executing a checkpoint before",
            dest="force",
            action="store_true",
            default=False,
        ),
        argument(
            "--archive",
            help="wait for one WAL file to be archived",
            dest="archive",
            action="store_true",
            default=False,
        ),
        argument(
            "--archive-timeout",
            help="the time, in seconds, the archiver will wait for a new WAL file "
            "to be archived before timing out",
            metavar="TIMEOUT",
            default="30",
            type=check_non_negative,
        ),
    ],
    cmd_aliases=["switch-xlog"],
)
def switch_wal(args):
    """
    Execute the switch-wal command on the target server
    """
    servers = get_server_list(args, skip_inactive=True)
    for name in sorted(servers):
        server = servers[name]
        # Skip the server (apply general rule)
        if not manage_server_command(server, name):
            continue
        with closing(server):
            server.switch_wal(args.force, args.archive, args.archive_timeout)
    output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer_all,
            nargs="+",
            help="specifies the server names to check "
            "('all' will check all available servers)",
        ),
        argument(
            "--nagios", help="Nagios plugin compatible output", action="store_true"
        ),
    ]
)
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
            server,
            name,
            skip_inactive=False,
            skip_disabled=False,
            disabled_is_error=False,
        ):
            continue

        output.init("check", name, server.config.active, server.config.disabled)
        with closing(server):
            server.check()
    output.close_and_exit()


@command(
    [
        argument(
            "--show-config-source",
            help="Include the source file which provides the effective value "
            "for each configuration option",
            action="store_true",
        )
    ],
)
def diagnose(args=None):
    """
    Diagnostic command (for support and problems detection purpose)
    """
    # Get every server (both inactive and temporarily disabled)
    servers = get_server_list(on_error_stop=False, suppress_error=True)
    models = get_models_list()
    # errors list with duplicate paths between servers
    errors_list = barman.__config__.servers_msg_list
    barman.diagnose.exec_diagnose(servers, models, errors_list, args.show_config_source)
    output.close_and_exit()


@command(
    [
        argument(
            "--primary",
            help="execute the sync-info on the primary node (if set)",
            action="store_true",
            default=SUPPRESS,
        ),
        argument(
            "server_name",
            completer=server_completer,
            help="specifies the server name for the command",
        ),
        argument(
            "last_wal", help="specifies the name of the latest WAL read", nargs="?"
        ),
        argument(
            "last_position",
            nargs="?",
            type=check_positive,
            help="the last position read from xlog database (in bytes)",
        ),
    ]
)
def sync_info(args):
    """
    Output the internal synchronisation status.
    Used to sync_backup with a passive node
    """
    server = get_server(args)
    try:
        # if called with --primary option
        if getattr(args, "primary", False):
            primary_info = server.primary_node_info(args.last_wal, args.last_position)
            output.info(
                json.dumps(primary_info, cls=BarmanEncoder, indent=4), log=False
            )
        else:
            server.sync_status(args.last_wal, args.last_position)
    except SyncError as e:
        # Catch SyncError exceptions and output only the error message,
        # preventing from logging the stack trace
        output.error(e)

    output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer,
            help="specifies the server name for the command",
        ),
        argument(
            "backup_id", help="specifies the backup ID to be copied on the passive node"
        ),
    ]
)
def sync_backup(args):
    """
    Command that synchronises a backup from a master to a passive node
    """
    server = get_server(args)
    try:
        server.sync_backup(args.backup_id)
    except SyncError as e:
        # Catch SyncError exceptions and output only the error message,
        # preventing from logging the stack trace
        output.error(e)
    output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer,
            help="specifies the server name for the command",
        )
    ]
)
def sync_wals(args):
    """
    Command that synchronises WAL files from a master to a passive node
    """
    server = get_server(args)
    try:
        server.sync_wals()
    except SyncError as e:
        # Catch SyncError exceptions and output only the error message,
        # preventing from logging the stack trace
        output.error(e)
    output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer,
            help="specifies the server name for the command",
        ),
        argument(
            "backup_id", completer=backup_completer, help="specifies the backup ID"
        ),
    ],
    cmd_aliases=["show-backups"],
)
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


@command(
    [
        argument(
            "server_name",
            completer=server_completer,
            help="specifies the server name for the command",
        ),
        argument(
            "backup_id", completer=backup_completer, help="specifies the backup ID"
        ),
        argument(
            "--target",
            choices=("standalone", "data", "wal", "full"),
            default="standalone",
            help="""
                       Possible values are: data (just the data files), standalone
                       (base backup files, including required WAL files),
                       wal (just WAL files between the beginning of base
                       backup and the following one (if any) or the end of the log) and
                       full (same as data + wal). Defaults to %(default)s""",
        ),
    ]
)
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
            'HINT: Please run "barman rebuild-xlogdb %s" '
            "to solve this issue",
            force_str(e),
            server.config.name,
        )
        output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer,
            help="specifies the server name for the command",
        ),
        argument(
            "backup_id", completer=backup_completer, help="specifies the backup ID"
        ),
    ]
)
def delete(args):
    """
    Delete a backup
    """
    server = get_server(args)

    # Retrieves the backup
    backup_id = parse_backup_id(server, args)
    with closing(server):
        if not server.delete_backup(backup_id):
            output.error(
                "Cannot delete backup (%s %s)" % (server.config.name, backup_id)
            )
    output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer,
            help="specifies the server name for the command",
        ),
        argument("wal_name", help="the WAL file to get"),
        argument(
            "--output-directory",
            "-o",
            help="put the retrieved WAL file in this directory with the original name",
            default=SUPPRESS,
        ),
        argument(
            "--partial",
            "-P",
            help="retrieve also partial WAL files (.partial)",
            action="store_true",
            dest="partial",
            default=False,
        ),
        argument(
            "--gzip",
            "-z",
            "-x",
            help="compress the output with gzip",
            action="store_const",
            const="gzip",
            dest="compression",
            default=SUPPRESS,
        ),
        argument(
            "--bzip2",
            "-j",
            help="compress the output with bzip2",
            action="store_const",
            const="bzip2",
            dest="compression",
            default=SUPPRESS,
        ),
        argument(
            "--keep-compression",
            help="do not decompress the output if compressed",
            action="store_true",
            dest="keep_compression",
        ),
        argument(
            "--peek",
            "-p",
            help="peek from the WAL archive up to 'SIZE' WAL files, starting "
            "from the requested one. 'SIZE' must be an integer >= 1. "
            "When invoked with this option, get-wal returns a list of "
            "zero to 'SIZE' WAL segment names, one per row.",
            metavar="SIZE",
            type=check_positive,
            default=SUPPRESS,
        ),
        argument(
            "--test",
            "-t",
            help="test both the connection and the configuration of the requested "
            "PostgreSQL server in Barman for WAL retrieval. With this option, "
            "the 'wal_name' mandatory argument is ignored.",
            action="store_true",
            default=SUPPRESS,
        ),
    ]
)
def get_wal(args):
    """
    Retrieve WAL_NAME file from SERVER_NAME archive.
    The content will be streamed on standard output unless
    the --output-directory option is specified.
    """
    server = get_server(args, inactive_is_error=True)

    if getattr(args, "test", None):
        output.info(
            "Ready to retrieve WAL files from the server %s", server.config.name
        )
        return

    # Retrieve optional arguments. If an argument is not specified,
    # the namespace doesn't contain it due to SUPPRESS default.
    # In that case we pick 'None' using getattr third argument.
    compression = getattr(args, "compression", None)
    keep_compression = getattr(args, "keep_compression", False)
    output_directory = getattr(args, "output_directory", None)
    peek = getattr(args, "peek", None)

    if compression and keep_compression:
        output.error(
            "argument `%s` not allowed with argument `keep-compression`" % compression
        )
        output.close_and_exit()

    with closing(server):
        server.get_wal(
            args.wal_name,
            compression=compression,
            keep_compression=keep_compression,
            output_directory=output_directory,
            peek=peek,
            partial=args.partial,
        )
    output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer,
            help="specifies the server name for the command",
        ),
        argument(
            "--test",
            "-t",
            help="test both the connection and the configuration of the requested "
            "PostgreSQL server in Barman to make sure it is ready to receive "
            "WAL files.",
            action="store_true",
            default=SUPPRESS,
        ),
    ]
)
def put_wal(args):
    """
    Receive a WAL file from SERVER_NAME and securely store it in the incoming
    directory. The file will be read from standard input in tar format.
    """
    server = get_server(args, inactive_is_error=True)

    if getattr(args, "test", None):
        output.info("Ready to accept WAL files for the server %s", server.config.name)
        return

    try:
        # Python 3.x
        stream = sys.stdin.buffer
    except AttributeError:
        # Python 2.x
        stream = sys.stdin
    with closing(server):
        server.put_wal(stream)
    output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer,
            help="specifies the server name for the command",
        )
    ]
)
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


@command(
    [
        argument(
            "--stop",
            help="stop the receive-wal subprocess for the server",
            action="store_true",
        ),
        argument(
            "--reset",
            help="reset the status of receive-wal removing any status files",
            action="store_true",
        ),
        argument(
            "--create-slot",
            help="create the replication slot, if it does not exist",
            action="store_true",
        ),
        argument(
            "--drop-slot",
            help="drop the replication slot, if it exists",
            action="store_true",
        ),
        argument(
            "server_name",
            completer=server_completer,
            help="specifies the server name for the command",
        ),
    ]
)
def receive_wal(args):
    """
    Start a receive-wal process.
    The process uses the streaming protocol to receive WAL files
    from the PostgreSQL server.
    """
    should_skip_inactive = not (
        args.create_slot or args.drop_slot or args.stop or args.reset
    )
    server = get_server(args, skip_inactive=should_skip_inactive, wal_streaming=True)

    if args.stop and args.reset:
        output.error("--stop and --reset options are not compatible")
    # If the caller requested to shutdown the receive-wal process deliver the
    # termination signal, otherwise attempt to start it
    elif args.stop:
        server.kill("receive-wal")
    elif args.create_slot:
        with closing(server):
            server.create_physical_repslot()
    elif args.drop_slot:
        with closing(server):
            server.drop_repslot()
    else:
        with closing(server):
            server.receive_wal(reset=args.reset)
    output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer,
            help="specifies the server name for the command",
        ),
        argument(
            "backup_id", completer=backup_completer, help="specifies the backup ID"
        ),
    ]
)
def check_backup(args):
    """
    Make sure that all the required WAL files to check
    the consistency of a physical backup (that is, from the
    beginning to the end of the full backup) are correctly
    archived. This command is automatically invoked by the
    cron command and at the end of every backup operation.
    """
    server = get_server(args)

    # Retrieves the backup
    backup_info = parse_backup_id(server, args)

    with closing(server):
        server.check_backup(backup_info)
    output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer,
            help="specifies the server name for the command ",
        ),
        argument(
            "backup_id", completer=backup_completer, help="specifies the backup ID"
        ),
    ],
    cmd_aliases=["verify"],
)
def verify_backup(args):
    """
    verify a backup for the given server and backup id
    """
    # get barman.server.Server
    server = get_server(args)
    # Raises an error if wrong backup
    backup_info = parse_backup_id(server, args)
    # get backup path
    output.info(
        "Verifying backup '%s' on server %s" % (args.backup_id, args.server_name)
    )

    server.backup_manager.verify_backup(backup_info)
    output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer,
            help="specifies the server name for the command ",
        ),
        argument(
            "backup_id", completer=backup_completer, help="specifies the backup ID"
        ),
    ],
)
def generate_manifest(args):
    """
    Generate a manifest-backup for the given server and backup id
    """
    server = get_server(args)
    # Raises an error if wrong backup
    backup_info = parse_backup_id(server, args)
    # know context (remote backup? local?)

    local_file_manager = LocalFileManager()
    backup_manifest = BackupManifest(
        backup_info.get_data_directory(), local_file_manager, SHA256()
    )
    backup_manifest.create_backup_manifest()

    output.info(
        "Backup manifest for backup '%s' successfully generated for server %s"
        % (args.backup_id, args.server_name)
    )
    output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer,
            help="specifies the server name for the command",
        ),
        argument(
            "backup_id", completer=backup_completer, help="specifies the backup ID"
        ),
        argument(
            "-r", "--release", help="remove the keep annotation", action="store_true"
        ),
        argument(
            "-s",
            "--status",
            help="return the keep status of the backup",
            action="store_true",
        ),
        argument(
            "--target",
            help="keep this backup with the specified recovery target",
            choices=[KeepManager.TARGET_FULL, KeepManager.TARGET_STANDALONE],
        ),
    ]
)
def keep(args):
    """
    Tag the specified backup so that it will never be deleted
    """
    if not any((args.release, args.status, args.target)):
        output.error(
            "one of the arguments -r/--release -s/--status --target is required"
        )
        output.close_and_exit()
    server = get_server(args)
    backup_info = parse_backup_id(server, args)
    backup_manager = server.backup_manager
    if args.status:
        output.init("status", server.config.name)
        target = backup_manager.get_keep_target(backup_info.backup_id)
        if target:
            output.result("status", server.config.name, "keep_status", "Keep", target)
        else:
            output.result("status", server.config.name, "keep_status", "Keep", "nokeep")
    elif args.release:
        backup_manager.release_keep(backup_info.backup_id)
    else:
        if backup_info.status != BackupInfo.DONE:
            msg = (
                "Cannot add keep to backup %s because it has status %s. "
                "Only backups with status DONE can be kept."
            ) % (backup_info.backup_id, backup_info.status)
            output.error(msg)
            output.close_and_exit()
        if backup_info.is_incremental:
            msg = (
                "Unable to execute the keep command on backup %s: is an incremental backup.\n"
                "Only full backups are eligible for the use of the keep command."
            ) % (backup_info.backup_id)
            output.error(msg)
            output.close_and_exit()
        backup_manager.keep_backup(backup_info.backup_id, args.target)


@command(
    [
        argument(
            "server_name",
            completer=server_completer,
            help="specifies the server name for the command",
        ),
        argument(
            "--timeline",
            help="the earliest timeline whose WALs should cause the check to fail",
            type=check_positive,
        ),
    ]
)
def check_wal_archive(args):
    """
    Check the WAL archive can be safely used for a new server.

    This will fail if there are any existing WALs in the archive.
    If the --timeline option is used then any WALs on earlier timelines
    than that specified will not cause the check to fail.
    """
    server = get_server(args)
    output.init("check_wal_archive", server.config.name)

    with server.xlogdb() as fxlogdb:
        wals = [WalFileInfo.from_xlogdb_line(w).name for w in fxlogdb]
        try:
            check_archive_usable(
                wals,
                timeline=args.timeline,
            )
            output.result("check_wal_archive", server.config.name)
        except WalArchiveContentError as err:
            msg = "WAL archive check failed for server %s: %s" % (
                server.config.name,
                force_str(err),
            )
            logging.error(msg)
            output.error(msg)
            output.close_and_exit()


@command(
    [
        argument(
            "server_name",
            completer=server_completer,
            help="specifies the name of the server which configuration should "
            "be overriden by the model",
        ),
        argument(
            "model_name",
            help="specifies the name of the model which configuration should "
            "override the server configuration. This is an optional argument "
            "and will not be used when called with the '--reset' flag.",
            nargs="?",
        ),
        argument(
            "--reset",
            help="indicates that we should unapply the currently active model "
            "for the server",
            action="store_true",
        ),
    ]
)
def config_switch(args):
    """
    Change the active configuration for a server by applying a named model on
    top of it, or by resetting the active model.
    """
    if args.model_name is None and not args.reset:
        output.error("Either a model name or '--reset' flag need to be given")
        return

    server = get_server(args, skip_inactive=False)

    if server is not None:
        if args.reset:
            server.config.reset_model()
        else:
            model = get_model(args)

            if model is not None:
                server.config.apply_model(model, True)
        server.restart_processes()


@command(
    [
        argument(
            "json_changes",
            help="specifies the configuration changes to apply, in json format ",
        ),
    ]
)
def config_update(args):
    """
    Receives a set of configuration changes in json format and applies them.
    """
    json_changes = json.loads(args.json_changes)
    # this prevents multiple concurrent executions of the config-update command
    with ConfigUpdateLock(barman.__config__.barman_lock_directory):
        processor = ConfigChangesProcessor(barman.__config__)
        processor.receive_config_changes(json_changes)
        processor.process_conf_changes_queue()
        for change in processor.applied_changes:
            server = get_server(
                argparse.Namespace(server_name=change.section),
                # skip_disabled=True,
                inactive_is_error=False,
                disabled_is_error=False,
                on_error_stop=False,
                suppress_error=True,
            )
            if server:
                server.restart_processes()


def pretty_args(args):
    """
    Prettify the given argparse namespace to be human readable

    :type args: argparse.Namespace
    :return: the human readable content of the namespace
    """
    values = dict(vars(args))
    # Retrieve the command name with recent argh versions
    if "_functions_stack" in values:
        values["command"] = values["_functions_stack"][0].__name__
        del values["_functions_stack"]
    # Older argh versions only have the matching function in the namespace
    elif "function" in values:
        values["command"] = values["function"].__name__
        del values["function"]
    return "%r" % values


def global_config(args):
    """
    Set the configuration file
    """
    if hasattr(args, "config"):
        filename = args.config
    else:
        try:
            filename = os.environ["BARMAN_CONFIG_FILE"]
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
    if hasattr(args, "log_level"):
        config.log_level = args.log_level
    log_level = parse_log_level(config.log_level)
    configure_logging(
        config.log_file, log_level or barman.config.DEFAULT_LOG_LEVEL, config.log_format
    )
    if log_level is None:
        _logger.warning("unknown log_level in config file: %s", config.log_level)

    # Configure output
    if args.format != output.DEFAULT_WRITER or args.quiet or args.debug:
        output.set_output_writer(args.format, quiet=args.quiet, debug=args.debug)

    # Configure color output
    if args.color == "auto":
        # Enable colored output if both stdout and stderr are TTYs
        output.ansi_colors_enabled = sys.stdout.isatty() and sys.stderr.isatty()
    else:
        output.ansi_colors_enabled = args.color == "always"

    # Load additional configuration files
    config.load_configuration_files_directory()
    # Handle the autoconf file, load it only if exists
    autoconf_path = "%s/.barman.auto.conf" % config.get("barman", "barman_home")
    if os.path.exists(autoconf_path):
        config.load_config_file(autoconf_path)
    # We must validate the configuration here in order to have
    # both output and logging configured
    config.validate_global_config()

    _logger.debug(
        "Initialised Barman version %s (config: %s, args: %s)",
        barman.__version__,
        config.config_file,
        pretty_args(args),
    )


def get_server(
    args,
    skip_inactive=True,
    skip_disabled=False,
    skip_passive=False,
    inactive_is_error=False,
    disabled_is_error=True,
    on_error_stop=True,
    suppress_error=False,
    wal_streaming=False,
):
    """
    Get a single server retrieving its configuration (wraps get_server_list())

    Returns a Server object or None if the required server is unknown and
    on_error_stop is False.

    WARNING: this function modifies the 'args' parameter

    :param args: an argparse namespace containing a single
        server_name parameter
        WARNING: the function modifies the content of this parameter
    :param bool skip_inactive: do nothing if the server is inactive
    :param bool skip_disabled: do nothing if the server is disabled
    :param bool skip_passive: do nothing if the server is passive
    :param bool inactive_is_error: treat inactive server as error
    :param bool on_error_stop: stop if an error is found
    :param bool suppress_error: suppress display of errors (e.g. diagnose)
    :param bool wal_streaming: create the :class:`barman.server.Server` using
        WAL streaming conninfo (if available in the configuration)
    :rtype: Server|None
    """
    # This function must to be called with in a single-server context
    name = args.server_name
    assert isinstance(name, str)

    # The 'all' special name is forbidden in this context
    if name == "all":
        output.error("You cannot use 'all' in a single server context")
        output.close_and_exit()
        # The following return statement will never be reached
        # but it is here for clarity
        return None

    # Builds a list from a single given name
    args.server_name = [name]

    # Skip_inactive is reset if inactive_is_error is set, because
    # it needs to retrieve the inactive server to emit the error.
    skip_inactive &= not inactive_is_error

    # Retrieve the requested server
    servers = get_server_list(
        args,
        skip_inactive,
        skip_disabled,
        skip_passive,
        on_error_stop,
        suppress_error,
        wal_streaming,
    )

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
    x = not manage_server_command(
        server,
        name,
        inactive_is_error,
        disabled_is_error,
        skip_inactive,
        skip_disabled,
        suppress_error,
    )
    if x and on_error_stop:
        output.close_and_exit()
        # The following return statement will never be reached
        # but it is here for clarity
        return None

    # Returns the filtered server
    return server


def get_server_list(
    args=None,
    skip_inactive=False,
    skip_disabled=False,
    skip_passive=False,
    on_error_stop=True,
    suppress_error=False,
    wal_streaming=False,
):
    """
    Get the server list from the configuration

    If args the parameter is None or arg.server_name is ['all']
    returns all defined servers

    :param args: an argparse namespace containing a list server_name parameter
    :param bool skip_inactive: skip inactive servers when 'all' is required
    :param bool skip_disabled: skip disabled servers when 'all' is required
    :param bool skip_passive: skip passive servers when 'all' is required
    :param bool on_error_stop: stop if an error is found
    :param bool suppress_error: suppress display of errors (e.g. diagnose)
    :param bool wal_streaming: create :class:`barman.server.Server` objects using
        WAL streaming conninfo (if available in the configuration)
    :rtype: dict[str,Server]
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
    if not args or "all" in args.server_name:
        # When 'all' is used, it must be the only specified argument
        if args and len(args.server_name) != 1:
            output.error("You cannot use 'all' with other server names")
        server_names = available_servers
    else:
        # Put servers in a set, so multiple occurrences are counted only once
        server_names = set(args.server_name)

    # Loop through all the requested servers
    for server_name in server_names:
        conf = barman.__config__.get_server(server_name)
        if conf is None:
            # Unknown server
            server_dict[server_name] = None
        else:
            if wal_streaming:
                conf.streaming_conninfo, conf.conninfo = conf.get_wal_conninfo()
            server_object = Server(conf)
            # Skip inactive servers, if requested
            if skip_inactive and not server_object.config.active:
                output.info("Skipping inactive server '%s'" % conf.name)
                continue
            # Skip disabled servers, if requested
            if skip_disabled and server_object.config.disabled:
                output.info("Skipping temporarily disabled server '%s'" % conf.name)
                continue
            # Skip passive nodes, if requested
            if skip_passive and server_object.passive_node:
                output.info("Skipping passive server '%s'", conf.name)
                continue
            server_dict[server_name] = server_object

    return server_dict


def manage_server_command(
    server,
    name=None,
    inactive_is_error=False,
    disabled_is_error=True,
    skip_inactive=True,
    skip_disabled=True,
    suppress_error=False,
):
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
        if not suppress_error:
            output.error("Unknown server '%s'" % name)
        return False

    if not server.config.active:
        # Report inactive server as error
        if inactive_is_error:
            output.error("Inactive server: %s" % server.config.name)
            return False
        if skip_inactive:
            return False

    # Report disabled server as error
    if server.config.disabled:
        # Output all the messages as errors, and exit terminating the run.
        if disabled_is_error:
            for message in server.config.msg_list:
                output.error(message)
                return False
        if skip_disabled:
            return False

    # All ok, execute the command
    return True


def get_models_list(args=None):
    """Get the model list from the configuration.

    If the *args* parameter is ``None`` returns all defined servers.

    :param args: an :class:`argparse.Namespace` containing a list
        ``model_name`` parameter.

    :return: a :class:`dict` -- each key is a model name, and its value the
        corresponding :class:`ModelConfig` instance.
    """
    model_dict = {}

    # This function must to be called with in a multiple-model context
    assert not args or isinstance(args.model_name, list)

    # Generate the list of models (required for global errors)
    available_models = barman.__config__.model_names()

    # Handle special *args* is ``None`` case
    if not args:
        model_names = available_models
    else:
        # Put models in a set, so multiple occurrences are counted only once
        model_names = set(args.model_name)

    # Loop through all the requested models
    for model_name in model_names:
        model = barman.__config__.get_model(model_name)
        if model is None:
            # Unknown model
            model_dict[model_name] = None
        else:
            model_dict[model_name] = model

    return model_dict


def manage_model_command(model, name=None):
    """
    Standard and consistent method for managing model errors within a model
    command execution.

    :param model: :class:`ModelConfig` to be checked for errors.
    :param name: name of the model.

    :return: ``True`` if the command has to be executed with this model.
    """

    # Unknown model (skip it)
    if not model:
        output.error("Unknown model '%s'" % name)
        return False

    # All ok, execute the command
    return True


def get_model(args, on_error_stop=True):
    """
    Get a single model retrieving its configuration (wraps :func:`get_models_list`).

    .. warning::
        This function modifies the *args* parameter.

    :param args: an :class:`argparse.Namespace` containing a single
        ``model_name`` parameter.
    :param on_error_stop: stop if an error is found.

    :return: a :class:`ModelConfig` or ``None`` if the required model is
        unknown and *on_error_stop* is ``False``.
    """
    # This function must to be called with in a single-model context
    name = args.model_name
    assert isinstance(name, str)

    # Builds a list from a single given name
    args.model_name = [name]

    # Retrieve the requested model
    models = get_models_list(args)

    # The requested model has been excluded from :func:`get_models_list`` result
    if len(models) == 0:
        output.close_and_exit()
        # The following return statement will never be reached
        # but it is here for clarity
        return None

    # retrieve the model object
    model = models[name]

    # Apply standard validation control and skips
    # the model if invalid, displaying standard
    # error messages. If on_error_stop (default) exits
    if not manage_model_command(model, name) and on_error_stop:
        output.close_and_exit()
        # The following return statement will never be reached
        # but it is here for clarity
        return None

    # Returns the filtered model
    return model


def parse_backup_id(server, args):
    """
    Parses backup IDs including special words such as latest, oldest, etc.

    Exit with error if the backup id doesn't exist.

    :param Server server: server object to search for the required backup
    :param args: command line arguments namespace
    :rtype: barman.infofile.LocalBackupInfo
    """
    backup_id = get_backup_id_using_shortcut(server, args.backup_id, BackupInfo)
    if backup_id is None:
        try:
            backup_id = server.get_backup_id_from_name(args.backup_id)
        except ValueError as exc:
            output.error(str(exc))
            output.close_and_exit()
    backup_info = server.get_backup(backup_id)
    if backup_info is None:
        output.error(
            "Unknown backup '%s' for server '%s'", args.backup_id, server.config.name
        )
        output.close_and_exit()
    return backup_info


def main():
    """
    The main method of Barman
    """
    # noinspection PyBroadException
    try:
        if argcomplete:
            argcomplete.autocomplete(p)
        args = p.parse_args()
        global_config(args)
        if args.command is None:
            p.print_help()
        else:
            args.func(args)
    except KeyboardInterrupt:
        msg = "Process interrupted by user (KeyboardInterrupt)"
        output.error(msg)
    except Exception as e:
        msg = "%s\nSee log file for more details." % e
        output.exception(msg)

    # cleanup output API and exit honoring output.error_occurred and
    # output.error_exit_code
    output.close_and_exit()


if __name__ == "__main__":
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
