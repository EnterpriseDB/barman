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
This module contains the methods necessary to perform a recovery
"""

from __future__ import print_function

import collections
import datetime
import io
import logging
import os
import re
import shutil
import socket
import tempfile
from abc import ABC, abstractmethod
from contextlib import contextmanager
from distutils.version import LooseVersion as Version
from io import BytesIO

import dateutil.parser
import dateutil.tz

import barman.fs as fs
from barman import output, xlog
from barman.cloud_providers import get_snapshot_interface_from_backup_info
from barman.command_wrappers import (
    Command,
    PgCombineBackup,
    RsyncPgData,
    full_command_quote,
)
from barman.compression import (
    GZipCompression,
    LZ4Compression,
    NoneCompression,
    ZSTDCompression,
)
from barman.config import RecoveryOptions
from barman.copy_controller import RsyncCopyController
from barman.encryption import get_passphrase_from_command
from barman.exceptions import (
    BadXlogSegmentName,
    CommandFailedException,
    CommandNotFoundException,
    DataTransferFailure,
    FsOperationFailed,
    RecoveryInvalidTargetException,
    RecoveryPreconditionException,
    RecoveryStandbyModeException,
    RecoveryTargetActionException,
    SnapshotBackupException,
    UnsupportedCompressionFormat,
)
from barman.infofile import BackupInfo, LocalBackupInfo, VolatileBackupInfo
from barman.utils import (
    force_str,
    get_major_version,
    is_subdirectory,
    mkpath,
    parse_target_tli,
    total_seconds,
)

# generic logger for this module
_logger = logging.getLogger(__name__)

# regexp matching a single value in Postgres configuration file
PG_CONF_SETTING_RE = re.compile(r"^\s*([^\s=]+)\s*=?\s*(.*)$")

# create a namedtuple object called Assertion
# with 'filename', 'line', 'key' and 'value' as properties
Assertion = collections.namedtuple("Assertion", "filename line key value")


# noinspection PyMethodMayBeStatic
class RecoveryExecutor(object):
    """
    Class responsible of recovery operations
    """

    def __init__(self, backup_manager):
        """
        Constructor

        :param barman.backup.BackupManager backup_manager: the BackupManager
            owner of the executor
        """
        self.backup_manager = backup_manager
        self.server = backup_manager.server
        self.config = backup_manager.config
        self.temp_dirs = []

    def recover(
        self,
        backup_info,
        dest,
        wal_dest=None,
        tablespaces=None,
        remote_command=None,
        target_tli=None,
        target_time=None,
        target_xid=None,
        target_lsn=None,
        target_name=None,
        target_immediate=False,
        exclusive=False,
        target_action=None,
        standby_mode=None,
        recovery_conf_filename=None,
        recovery_option_port=None,
    ):
        """
        Performs a recovery of a backup

        This method should be called in a closing context

        :param barman.infofile.BackupInfo backup_info: the backup to recover
        :param str dest: the destination directory
        :param str|None wal_dest: the destination directory for WALs when doing PITR.
            See :meth:`~barman.recovery_executor.RecoveryExecutor._set_pitr_targets`
            for more details.
        :param dict[str,str]|None tablespaces: a tablespace
            name -> location map (for relocation)
        :param str|None remote_command: The remote command to recover
                               the base backup, in case of remote backup.
        :param str|None target_tli: the target timeline
        :param str|None target_time: the target time
        :param str|None target_xid: the target xid
        :param str|None target_lsn: the target LSN
        :param str|None target_name: the target name created previously with
                            pg_create_restore_point() function call
        :param str|None target_immediate: end recovery as soon as consistency
            is reached
        :param bool exclusive: whether the recovery is exclusive or not
        :param str|None target_action: The recovery target action
        :param bool|None standby_mode: standby mode
        :param str|None recovery_conf_filename: filename for storing recovery
            configurations
        :kwparam str|None recovery_option_port: port to set in restore command
            when invoking ``barman-wal-restore``
        """

        # Run the cron to be sure the wal catalog is up to date
        # Prepare a map that contains all the objects required for a recovery
        recovery_info = self._setup(
            backup_info,
            remote_command,
            dest,
            recovery_conf_filename,
            recovery_option_port,
        )

        output.info(
            "Starting %s restore for server %s using backup %s",
            recovery_info["recovery_dest"],
            self.server.config.name,
            backup_info.backup_id,
        )
        output.info("Destination directory: %s", dest)
        if remote_command:
            output.info("Remote command: %s", remote_command)

        # If the backup we are recovering is still not validated and we
        # haven't requested the get-wal feature, display a warning message
        if not recovery_info["get_wal"]:
            if backup_info.status == BackupInfo.WAITING_FOR_WALS:
                output.warning(
                    "IMPORTANT: You have requested a recovery operation for "
                    "a backup that does not have yet all the WAL files that "
                    "are required for consistency."
                )

        # Set targets for PITR
        self._set_pitr_targets(
            recovery_info,
            backup_info,
            dest,
            wal_dest,
            target_name,
            target_time,
            target_tli,
            target_xid,
            target_lsn,
            target_immediate,
            target_action,
        )

        # Retrieve the safe_horizon for smart copy
        self._retrieve_safe_horizon(recovery_info, backup_info, dest)

        # Copy the base backup
        self._start_backup_copy_message()
        try:
            self._backup_copy(
                backup_info,
                dest,
                tablespaces=tablespaces,
                remote_command=remote_command,
                safe_horizon=recovery_info["safe_horizon"],
                recovery_info=recovery_info,
            )
        except DataTransferFailure as e:
            self._backup_copy_failure_message(e)
            output.close_and_exit()

        # Copy the backup.info file in the destination as
        # ".barman-recover.info"
        if remote_command:
            try:
                recovery_info["rsync"](
                    backup_info.filename, ":%s/.barman-recover.info" % dest
                )
            except CommandFailedException as e:
                output.error("copy of recovery metadata file failed: %s", e)
                output.close_and_exit()
        else:
            backup_info.save(os.path.join(dest, ".barman-recover.info"))

        # Rename the backup_manifest file by adding a backup ID suffix
        if recovery_info["cmd"].exists(os.path.join(dest, "backup_manifest")):
            recovery_info["cmd"].move(
                os.path.join(dest, "backup_manifest"),
                os.path.join(dest, "backup_manifest.%s" % backup_info.backup_id),
            )

        # Standby mode is not available for PostgreSQL older than 9.0
        if backup_info.version < 90000 and standby_mode:
            raise RecoveryStandbyModeException(
                "standby_mode is available only from PostgreSQL 9.0"
            )

        # Restore the WAL segments. If GET_WAL option is set, skip this phase
        # as they will be retrieved using the wal-get command.
        if not recovery_info["get_wal"]:
            # If the backup we restored is still waiting for WALS, read the
            # backup info again and check whether it has been validated.
            # Notify the user if it is still not DONE.
            if backup_info.status == BackupInfo.WAITING_FOR_WALS:
                data = LocalBackupInfo(self.server, backup_info.filename)
                if data.status == BackupInfo.WAITING_FOR_WALS:
                    output.warning(
                        "IMPORTANT: The backup we have restored IS NOT "
                        "VALID. Required WAL files for consistency are "
                        "missing. Please verify that WAL archiving is "
                        "working correctly or evaluate using the 'get-wal' "
                        "option for recovery"
                    )

            # check WALs destination directory. If doesn't exist create it
            # we use the value from recovery_info as it contains the final path
            try:
                recovery_info["cmd"].create_dir_if_not_exists(
                    recovery_info["wal_dest"], mode="700"
                )
            except FsOperationFailed as e:
                output.error(
                    "unable to initialise WAL destination directory '%s': %s",
                    wal_dest,
                    e,
                )
                output.close_and_exit()

            output.info("Copying required WAL segments.")

            required_xlog_files = ()  # Makes static analysers happy
            try:
                # TODO: Stop early if target-immediate
                # Retrieve a list of required log files
                required_xlog_files = tuple(
                    self.server.get_required_xlog_files(
                        backup_info,
                        target_tli,
                        None,
                        None,
                        target_lsn,
                        target_immediate,
                    )
                )

                # Restore WAL segments into the wal_dest directory
                self._xlog_copy(
                    required_xlog_files, recovery_info["wal_dest"], remote_command
                )
            except DataTransferFailure as e:
                output.error("Failure copying WAL files: %s", e)
                output.close_and_exit()
            except BadXlogSegmentName as e:
                output.error(
                    "invalid xlog segment name %r\n"
                    'HINT: Please run "barman rebuild-xlogdb %s" '
                    "to solve this issue",
                    force_str(e),
                    self.config.name,
                )
                output.close_and_exit()

            # If WAL files are put directly in the pg_xlog directory,
            # avoid shipping of just recovered files
            # by creating the corresponding archive status file
            if not recovery_info["is_pitr"]:
                output.info("Generating archive status files")
                self._generate_archive_status(
                    recovery_info, remote_command, required_xlog_files
                )

        # At this point, the encryption passphrase is not needed anymore, so
        # we clear the cache to avoid lingering.
        get_passphrase_from_command.cache_clear()

        # Generate recovery.conf file (only if needed by PITR or get_wal)
        is_pitr = recovery_info["is_pitr"]
        get_wal = recovery_info["get_wal"]
        if is_pitr or get_wal or standby_mode:
            output.info("Generating recovery configuration")
            self._generate_recovery_conf(
                recovery_info,
                backup_info,
                dest,
                target_immediate,
                exclusive,
                remote_command,
                target_name,
                target_time,
                target_tli,
                target_xid,
                target_lsn,
                standby_mode,
            )

        # Create archive_status directory if necessary
        archive_status_dir = os.path.join(recovery_info["wal_dest"], "archive_status")
        try:
            recovery_info["cmd"].create_dir_if_not_exists(archive_status_dir)
        except FsOperationFailed as e:
            output.error(
                "unable to create the archive_status directory '%s': %s",
                archive_status_dir,
                e,
            )
            output.close_and_exit()

        # As last step, analyse configuration files in order to spot
        # harmful options. Barman performs automatic conversion of
        # some options as well as notifying users of their existence.
        #
        # This operation is performed in three steps:
        # 1) mapping
        # 2) analysis
        # 3) copy
        output.info("Identify dangerous settings in destination directory.")

        self._map_temporary_config_files(recovery_info, backup_info, remote_command)
        self._analyse_temporary_config_files(recovery_info)
        self._copy_temporary_config_files(dest, remote_command, recovery_info)

        return recovery_info

    def _setup(
        self,
        backup_info,
        remote_command,
        dest,
        recovery_conf_filename,
        recovery_option_port,
    ):
        """
        Prepare the recovery_info dictionary for the recovery, as well
        as temporary working directory

        :param barman.infofile.LocalBackupInfo backup_info: representation of a
            backup
        :param str remote_command: ssh command for remote connection
        :param str|None recovery_conf_filename: filename for storing recovery configurations
        :kwparam str|None recovery_option_port: port to set in restore command
            when invoking ``barman-wal-restore``
        :return dict: recovery_info dictionary, holding the basic values for a
            recovery
        """
        # Calculate the name of the WAL directory
        if backup_info.version < 100000:
            wal_dest = os.path.join(dest, "pg_xlog")
        else:
            wal_dest = os.path.join(dest, "pg_wal")

        tempdir = tempfile.mkdtemp(prefix="barman_recovery-")
        self.temp_dirs.append(fs.LocalLibPathDeletionCommand(tempdir))

        recovery_info = {
            "cmd": fs.unix_command_factory(remote_command, self.server.path),
            "recovery_dest": "local",
            "rsync": None,
            "configuration_files": [],
            "destination_path": dest,
            "temporary_configuration_files": [],
            "tempdir": tempdir,
            "is_pitr": False,
            "wal_dest": wal_dest,
            "get_wal": RecoveryOptions.GET_WAL in self.config.recovery_options,
            "recovery_option_port": recovery_option_port,
        }
        # A map that will keep track of the results of the recovery.
        # Used for output generation
        results = {
            "changes": [],
            "warnings": [],
            "missing_files": [],
            "get_wal": False,
            "recovery_start_time": datetime.datetime.now(dateutil.tz.tzlocal()),
        }
        recovery_info["results"] = results
        # Set up a list of configuration files
        recovery_info["configuration_files"].append("postgresql.conf")
        # Always add postgresql.auto.conf to the list of configuration files even if
        # it is not the specified destination for recovery settings, because there may
        # be other configuration options which need to be checked by Barman.
        if backup_info.version >= 90400:
            recovery_info["configuration_files"].append("postgresql.auto.conf")

        # Determine the destination file for recovery options. This will normally be
        # postgresql.auto.conf (or recovery.conf for PostgreSQL versions earlier than
        # 12) however there are certain scenarios (such as postgresql.auto.conf being
        # deliberately symlinked to /dev/null) which mean a user might have specified
        # an alternative destination. If an alternative has been specified, via
        # recovery_conf_filename, then it should be set as the recovery configuration
        # file.
        if recovery_conf_filename:
            # There is no need to also add the file to recovery_info["configuration_files"]
            # because that is only required for files which may already exist and
            # therefore contain options which Barman should check for safety.
            results["recovery_configuration_file"] = recovery_conf_filename
        # Otherwise, set the recovery configuration file based on the PostgreSQL
        # version used to create the backup.
        else:
            results["recovery_configuration_file"] = "postgresql.auto.conf"
            if backup_info.version < 120000:
                # The recovery.conf file is created for the recovery and therefore
                # Barman does not need to check the content. The file therefore does
                # not need to be added to recovery_info["configuration_files"] and
                # just needs to be set as the recovery configuration file.
                results["recovery_configuration_file"] = "recovery.conf"

        # Handle remote recovery options
        if remote_command:
            recovery_info["recovery_dest"] = "remote"
            recovery_info["rsync"] = RsyncPgData(
                path=self.server.path,
                ssh=remote_command,
                bwlimit=self.config.bandwidth_limit,
                network_compression=self.config.network_compression,
            )

        return recovery_info

    def _set_pitr_targets(
        self,
        recovery_info,
        backup_info,
        dest,
        wal_dest,
        target_name,
        target_time,
        target_tli,
        target_xid,
        target_lsn,
        target_immediate,
        target_action,
    ):
        """
        Set PITR targets - as specified by the user

        :param dict recovery_info: Dictionary containing all the recovery
            parameters
        :param barman.infofile.LocalBackupInfo backup_info: representation of a
            backup
        :param str dest: destination directory of the recovery
        :param str|None wal_dest: the destination directory for WALs when doing PITR
        :param str|None target_name: recovery target name for PITR
        :param str|None target_time: recovery target time for PITR
        :param str|None target_tli: recovery target timeline for PITR
        :param str|None target_xid: recovery target transaction id for PITR
        :param str|None target_lsn: recovery target LSN for PITR
        :param bool|None target_immediate: end recovery as soon as consistency
            is reached
        :param str|None target_action: recovery target action for PITR
        """
        target_datetime = None

        # Calculate the integer value of TLI if a keyword is provided
        calculated_target_tli = parse_target_tli(
            self.backup_manager, target_tli, backup_info
        )

        d_immediate = backup_info.version >= 90400 and target_immediate
        d_lsn = backup_info.version >= 100000 and target_lsn

        # Detect PITR
        if any([target_time, target_xid, target_tli, target_name, d_immediate, d_lsn]):
            recovery_info["is_pitr"] = True
            targets = {}
            if target_time:
                try:
                    target_datetime = dateutil.parser.parse(target_time)
                except ValueError as e:
                    raise RecoveryInvalidTargetException(
                        "Unable to parse the target time parameter %r: %s"
                        % (target_time, e)
                    )
                except TypeError:
                    # this should not happen, but there is a known bug in
                    # dateutil.parser.parse() implementation
                    # ref: https://bugs.launchpad.net/dateutil/+bug/1247643
                    raise RecoveryInvalidTargetException(
                        "Unable to parse the target time parameter %r" % target_time
                    )

                # If the parsed timestamp is naive, forces it to local timezone
                if target_datetime.tzinfo is None:
                    target_datetime = target_datetime.replace(
                        tzinfo=dateutil.tz.tzlocal()
                    )

                    output.warning(
                        "No time zone has been specified through '--target-time' "
                        "command-line option. Barman assumed the same time zone from "
                        "the Barman host.",
                    )

                # Check if the target time is reachable from the
                # selected backup
                if backup_info.end_time > target_datetime:
                    raise RecoveryInvalidTargetException(
                        "The requested target time %s "
                        "is before the backup end time %s"
                        % (target_datetime, backup_info.end_time)
                    )

                targets["time"] = str(target_datetime)
            if target_xid:
                targets["xid"] = str(target_xid)
            if d_lsn:
                targets["lsn"] = str(d_lsn)
            if target_tli:
                targets["timeline"] = str(calculated_target_tli)
            if target_name:
                targets["name"] = str(target_name)
            if d_immediate:
                targets["immediate"] = d_immediate

            # Manage the target_action option
            if backup_info.version < 90100:
                if target_action:
                    raise RecoveryTargetActionException(
                        "Illegal target action '%s' "
                        "for this version of PostgreSQL" % target_action
                    )
            elif 90100 <= backup_info.version < 90500:
                if target_action == "pause":
                    recovery_info["pause_at_recovery_target"] = "on"
                elif target_action:
                    raise RecoveryTargetActionException(
                        "Illegal target action '%s' "
                        "for this version of PostgreSQL" % target_action
                    )
            else:
                if target_action in ("pause", "shutdown", "promote"):
                    recovery_info["recovery_target_action"] = target_action
                elif target_action:
                    raise RecoveryTargetActionException(
                        "Illegal target action '%s' "
                        "for this version of PostgreSQL" % target_action
                    )

            output.info(
                "Doing PITR. Recovery target %s",
                (", ".join(["%s: %r" % (k, v) for k, v in targets.items()])),
            )
            # If a custom WALs directory has been given, use it, otherwise defaults to
            # using a `barman_wal` directory inside the destination directory
            if wal_dest:
                recovery_info["wal_dest"] = wal_dest
            else:
                recovery_info["wal_dest"] = os.path.join(dest, "barman_wal")
        else:
            # Raise an error if target_lsn is used with a pgversion < 10
            if backup_info.version < 100000:
                if target_lsn:
                    raise RecoveryInvalidTargetException(
                        "Illegal use of recovery_target_lsn '%s' "
                        "for this version of PostgreSQL "
                        "(version 10 minimum required)" % target_lsn
                    )

                if target_immediate:
                    raise RecoveryInvalidTargetException(
                        "Illegal use of recovery_target_immediate "
                        "for this version of PostgreSQL "
                        "(version 9.4 minimum required)"
                    )

            if target_action:
                raise RecoveryTargetActionException(
                    "Can't enable recovery target action when PITR is not required"
                )

        recovery_info["target_datetime"] = target_datetime

    def _retrieve_safe_horizon(self, recovery_info, backup_info, dest):
        """
        Retrieve the safe_horizon for smart copy

        If the target directory contains a previous recovery, it is safe to
        pick the least of the two backup "begin times" (the one we are
        recovering now and the one previously recovered in the target
        directory). Set the value in the given recovery_info dictionary.

        :param dict recovery_info: Dictionary containing all the recovery
            parameters
        :param barman.infofile.LocalBackupInfo backup_info: a backup
            representation
        :param str dest: recovery destination directory
        """
        # noinspection PyBroadException
        try:
            backup_begin_time = backup_info.begin_time
            # Retrieve previously recovered backup metadata (if available)
            dest_info_txt = recovery_info["cmd"].get_file_content(
                os.path.join(dest, ".barman-recover.info")
            )
            dest_info = LocalBackupInfo(
                self.server, info_file=BytesIO(dest_info_txt.encode("utf-8"))
            )
            dest_begin_time = dest_info.begin_time
            # Pick the earlier begin time. Both are tz-aware timestamps because
            # BackupInfo class ensure it
            safe_horizon = min(backup_begin_time, dest_begin_time)
            output.info(
                "Using safe horizon time for smart rsync copy: %s", safe_horizon
            )
        except FsOperationFailed as e:
            # Setting safe_horizon to None will effectively disable
            # the time-based part of smart_copy method. However it is still
            # faster than running all the transfers with checksum enabled.
            #
            # FsOperationFailed means the .barman-recover.info is not available
            # on destination directory
            safe_horizon = None
            _logger.warning(
                "Unable to retrieve safe horizon time for smart rsync copy: %s", e
            )
        except Exception as e:
            # Same as above, but something failed decoding .barman-recover.info
            # or comparing times, so log the full traceback
            safe_horizon = None
            _logger.exception(
                "Error retrieving safe horizon time for smart rsync copy: %s", e
            )

        recovery_info["safe_horizon"] = safe_horizon

    def _start_backup_copy_message(self):
        """
        Write the start backup copy message to the output.
        """
        output.info("Copying the base backup.")

    def _backup_copy_failure_message(self, e):
        """
        Write the backup failure message to the output.
        """
        output.error("Failure copying base backup: %s", e)

    def _backup_copy(
        self,
        backup_info,
        dest,
        tablespaces=None,
        remote_command=None,
        safe_horizon=None,
        recovery_info=None,
    ):
        """
        Perform the actual copy of the base backup for recovery purposes

        First, it copies one tablespace at a time, then the PGDATA directory.

        Bandwidth limitation, according to configuration, is applied in
        the process.

        TODO: manage configuration files if outside PGDATA.

        :param barman.infofile.LocalBackupInfo backup_info: the backup
            to recover
        :param str dest: the destination directory
        :param dict[str,str]|None tablespaces: a tablespace
            name -> location map (for relocation)
        :param str|None remote_command: default None. The remote command to
            recover the base backup, in case of remote backup.
        :param datetime.datetime|None safe_horizon: anything after this time
            has to be checked with checksum
        """

        # Set a ':' prefix to remote destinations
        dest_prefix = ""
        if remote_command:
            dest_prefix = ":"

        # Create the copy controller object, specific for rsync,
        # which will drive all the copy operations. Items to be
        # copied are added before executing the copy() method
        controller = RsyncCopyController(
            path=self.server.path,
            ssh_command=remote_command,
            network_compression=self.config.network_compression,
            safe_horizon=safe_horizon,
            retry_times=self.config.basebackup_retry_times,
            retry_sleep=self.config.basebackup_retry_sleep,
            workers=self.config.parallel_jobs,
            workers_start_batch_period=self.config.parallel_jobs_start_batch_period,
            workers_start_batch_size=self.config.parallel_jobs_start_batch_size,
        )

        # Dictionary for paths to be excluded from rsync
        exclude_and_protect = []

        # Process every tablespace
        if backup_info.tablespaces:
            for tablespace in backup_info.tablespaces:
                # By default a tablespace goes in the same location where
                # it was on the source server when the backup was taken
                location = tablespace.location
                # If a relocation has been requested for this tablespace
                # use the user provided target directory
                if tablespaces and tablespace.name in tablespaces:
                    location = tablespaces[tablespace.name]

                # If the tablespace location is inside the data directory,
                # exclude and protect it from being deleted during
                # the data directory copy
                if is_subdirectory(dest, location):
                    exclude_and_protect += [location[len(dest) :]]

                # Exclude and protect the tablespace from being deleted during
                # the data directory copy
                exclude_and_protect.append("/pg_tblspc/%s" % tablespace.oid)

                # Add the tablespace directory to the list of objects
                # to be copied by the controller
                controller.add_directory(
                    label=tablespace.name,
                    src="%s/" % backup_info.get_data_directory(tablespace.oid),
                    dst=dest_prefix + location,
                    bwlimit=self.config.get_bwlimit(tablespace),
                    item_class=controller.TABLESPACE_CLASS,
                )

        # Add the PGDATA directory to the list of objects to be copied
        # by the controller
        controller.add_directory(
            label="pgdata",
            src="%s/" % backup_info.get_data_directory(),
            dst=dest_prefix + dest,
            bwlimit=self.config.get_bwlimit(),
            exclude=[
                "/pg_log/*",
                "/log/*",
                "/pg_xlog/*",
                "/pg_wal/*",
                "/postmaster.pid",
                "/recovery.conf",
                "/tablespace_map",
            ],
            exclude_and_protect=exclude_and_protect,
            item_class=controller.PGDATA_CLASS,
        )

        # TODO: Manage different location for configuration files
        # TODO: that were not within the data directory

        # Execute the copy
        try:
            controller.copy()
        # TODO: Improve the exception output
        except CommandFailedException as e:
            msg = "data transfer failure"
            raise DataTransferFailure.from_command_error("rsync", e, msg)

    def _xlog_copy(self, required_xlog_files, wal_dest, remote_command):
        """
        Restore WAL segments

        :param required_xlog_files: list of all required WAL files
        :param wal_dest: the destination directory for xlog recover
        :param remote_command: default None. The remote command to recover
               the xlog, in case of remote backup.
        """
        # List of required WAL files partitioned by containing directory
        xlogs = collections.defaultdict(list)
        # add '/' suffix to ensure it is a directory
        wal_dest = "%s/" % wal_dest
        # Map of every compressor used with any WAL file in the archive,
        # to be used during this recovery
        compressors = {}
        compression_manager = self.backup_manager.compression_manager
        # Map of every encryption used with any WAL file in the archive,
        # to be used during this recovery.
        encryptions = {}
        encryption_manager = self.backup_manager.encryption_manager
        # Fill xlogs and compressors and encryptions maps from
        # required_xlog_files
        for wal_info in required_xlog_files:
            hashdir = xlog.hash_dir(wal_info.name)
            xlogs[hashdir].append(wal_info)
            # If an encryption is required, make sure it exists in the cache
            if (
                wal_info.encryption is not None
                and wal_info.encryption not in encryptions
            ):
                # e.g. GPGEncryption
                encryptions[wal_info.encryption] = encryption_manager.get_encryption(
                    encryption=wal_info.encryption
                )
            # If a compressor is required, make sure it exists in the cache
            if (
                wal_info.compression is not None
                and wal_info.compression not in compressors
            ):
                compressors[wal_info.compression] = compression_manager.get_compressor(
                    compression=wal_info.compression
                )
        if encryptions:
            passphrase = None
            if self.config.encryption_passphrase_command:
                passphrase = get_passphrase_from_command(
                    self.config.encryption_passphrase_command
                )
            if not passphrase:
                output.error(
                    "Encrypted WALs were found for server '%s', but "
                    "'encryption_passphrase_command' is not configured correctly."
                    "Please configure it before attempting a restore.",
                    self.server.config.name,
                )
                output.close_and_exit()

        rsync = RsyncPgData(
            path=self.server.path,
            ssh=remote_command,
            bwlimit=self.config.bandwidth_limit,
            network_compression=self.config.network_compression,
        )
        # If encryption or compression is used during a remote recovery, we
        # need a temporary directory to spool the decrypted and/or decompressed
        # WAL files. Otherwise, we either decompress/decrypt directly in the
        # local destination or ship unprocessed files remotely.
        requires_decryption_or_decompression = bool(encryptions or compressors)
        if requires_decryption_or_decompression:
            if remote_command:
                # Decompress/decrypt to a temporary spool directory
                wal_staging_dest = tempfile.mkdtemp(prefix="barman_wal-")
            else:
                # Decompress/decrypt directly to the destination directory
                wal_staging_dest = wal_dest
            # Make sure wal_staging_dest exists
            mkpath(wal_staging_dest)
        else:
            # If no compression nor encryption
            wal_staging_dest = None
        if remote_command:
            # If remote recovery tell rsync to copy them remotely
            # add ':' prefix to mark it as remote
            wal_dest = ":%s" % wal_dest
        total_wals = sum(map(len, xlogs.values()))
        partial_count = 0
        for prefix in sorted(xlogs):
            batch_len = len(xlogs[prefix])
            partial_count += batch_len
            source_dir = os.path.join(self.config.wals_directory, prefix)
            _logger.info(
                "Starting copy of %s WAL files %s/%s from %s to %s",
                batch_len,
                partial_count,
                total_wals,
                xlogs[prefix][0],
                xlogs[prefix][-1],
            )
            # If WAL is encrypted and compressed: decrypt to 'wal_staging_dest',
            # then decompress the decrypted file to same location.
            #
            # If encrypted only: decrypt directly from source to 'wal_staging_dest'.
            #
            # If compressed only: decompress directly from source to 'wal_staging_dest'.
            #
            # If neither: simply copy from source to 'wal_staging_dest'.
            if requires_decryption_or_decompression:
                for segment in xlogs[prefix]:
                    segment_compression = segment.compression
                    src_file = os.path.join(source_dir, segment.name)
                    dst_file = os.path.join(wal_staging_dest, segment.name)
                    if segment.encryption is not None:
                        filename = encryptions[segment.encryption].decrypt(
                            file=src_file,
                            dest=wal_staging_dest,
                            passphrase=passphrase,
                        )
                        # If for some reason xlog.db had no informatiom about, then
                        # after decrypting, check if the file is compressed. This is a
                        # corner case which may occur if the user ran `rebuild-xlogdb`,
                        # for example, and the WALs were both encrypted and compressed.
                        # In that case, the rebuild would fill only the encryption info.
                        # Edge case consideration: If the compression is a custom
                        # implementation of a known algorithm (e.g., lz4), Barman may
                        # recognize it and default to its own decompression classes
                        # (which rely on external libraries), instead of using the
                        # custom decompression filter. If the compression is entirely
                        # custom and unidentifiable, we fallback to the 'custom'
                        # compression.
                        if segment_compression is None:
                            segment_compression = (
                                compression_manager.identify_compression(filename)
                                or compression_manager.unidentified_compression
                            )
                        if segment_compression is not None:
                            # If by chance the compressor is not available in the cache,
                            # then create an instance and add to the cache. Similar to
                            # the previous comment, this is only expected to occur when
                            # the user runs `rebuild-xlogdb` and the WALs were both
                            # encrypted and compressed, and the compression info is thus
                            # missing in xlog.db.
                            if segment_compression not in compressors:
                                compressor = compression_manager.get_compressor(
                                    segment_compression
                                )
                                compressors[segment_compression] = compressor

                            # At this point we are sure the cache contains the required
                            # compressor.
                            compressor = compressors.get(segment_compression)
                            # We have no control over the name of the file generated by
                            # the decrypt() method -- it writes a file with the name
                            # that we are expecting by the end of the process. So, we
                            # perform these steps:
                            # 1. Decrypt the file with the final file name.
                            # 2. Decompress the decrypted file as a temporary filel with
                            #    suffix ".decompressed".
                            # 3. Rename the decompressed file to the final file name,
                            #    effectively replacing the decrypted file with the
                            #    decompressed file.
                            decompressed_file = filename + ".decompressed"
                            compressor.decompress(filename, decompressed_file)

                            try:
                                shutil.move(decompressed_file, filename)
                            except OSError as e:
                                output.warning(
                                    "Error renaming decompressed file '%s' to '%s': %s (%s)",
                                    decompressed_file,
                                    filename,
                                    e,
                                    type(e).__name__,
                                )
                    elif segment_compression is not None:
                        compressors[segment_compression].decompress(src_file, dst_file)
                    else:
                        shutil.copy2(src_file, dst_file)

                if remote_command:
                    try:
                        # Transfer the WAL files
                        rsync.from_file_list(
                            list(segment.name for segment in xlogs[prefix]),
                            wal_staging_dest,
                            wal_dest,
                        )
                    except CommandFailedException as e:
                        msg = (
                            "data transfer failure while copying WAL files "
                            "to directory '%s'"
                        ) % (wal_dest[1:],)
                        raise DataTransferFailure.from_command_error("rsync", e, msg)

                    # Cleanup files after the transfer
                    for segment in xlogs[prefix]:
                        file_name = os.path.join(wal_staging_dest, segment.name)
                        try:
                            os.unlink(file_name)
                        except OSError as e:
                            output.warning(
                                "Error removing temporary file '%s': %s", file_name, e
                            )
            else:
                try:
                    rsync.from_file_list(
                        list(segment.name for segment in xlogs[prefix]),
                        "%s/" % os.path.join(self.config.wals_directory, prefix),
                        wal_dest,
                    )
                except CommandFailedException as e:
                    msg = (
                        "data transfer failure while copying WAL files "
                        "to directory '%s'" % (wal_dest[1:],)
                    )
                    raise DataTransferFailure.from_command_error("rsync", e, msg)

        _logger.info("Finished copying %s WAL files.", total_wals)

        # Remove local decompression target directory if different from the
        # destination directory (it happens when compression is in use during a
        # remote recovery
        if wal_staging_dest and wal_staging_dest != wal_dest:
            shutil.rmtree(wal_staging_dest)

    def _generate_archive_status(
        self, recovery_info, remote_command, required_xlog_files
    ):
        """
        Populate the archive_status directory

        :param dict recovery_info: Dictionary containing all the recovery
            parameters
        :param str remote_command: ssh command for remote connection
        :param tuple required_xlog_files: list of required WAL segments
        """
        if remote_command:
            status_dir = recovery_info["tempdir"]
        else:
            status_dir = os.path.join(recovery_info["wal_dest"], "archive_status")
            mkpath(status_dir)
        for wal_info in required_xlog_files:
            with open(os.path.join(status_dir, "%s.done" % wal_info.name), "a") as f:
                f.write("")
        if remote_command:
            try:
                recovery_info["rsync"](
                    "%s/" % status_dir,
                    ":%s" % os.path.join(recovery_info["wal_dest"], "archive_status"),
                )
            except CommandFailedException as e:
                output.error("unable to populate archive_status directory: %s", e)
                output.close_and_exit()

    def _generate_recovery_conf(
        self,
        recovery_info,
        backup_info,
        dest,
        immediate,
        exclusive,
        remote_command,
        target_name,
        target_time,
        target_tli,
        target_xid,
        target_lsn,
        standby_mode,
    ):
        """
        Generate recovery configuration for PITR

        :param dict recovery_info: Dictionary containing all the recovery
            parameters
        :param barman.infofile.LocalBackupInfo backup_info: representation
            of a backup
        :param str dest: destination directory of the recovery
        :param bool|None immediate: end recovery as soon as consistency
            is reached
        :param boolean exclusive: exclusive backup or concurrent
        :param str remote_command: ssh command for remote connection
        :param str target_name: recovery target name for PITR
        :param str target_time: recovery target time for PITR
        :param str target_tli: recovery target timeline for PITR
        :param str target_xid: recovery target transaction id for PITR
        :param str target_lsn: recovery target LSN for PITR
        :param bool|None standby_mode: standby mode
        """

        wal_dest = recovery_info["wal_dest"]
        recovery_conf_lines = []
        # If GET_WAL has been set, use the get-wal command to retrieve the
        # required wal files. Otherwise use the unix command "cp" to copy
        # them from the wal_dest directory
        if recovery_info["get_wal"]:
            port_option = ""
            if recovery_info["recovery_option_port"] is not None:
                port_option = "--port %s" % recovery_info["recovery_option_port"]
            partial_option = ""
            if not standby_mode:
                partial_option = "-P"

            # We need to create the right restore command.
            # If we are doing a remote recovery,
            # the barman-cli package is REQUIRED on the server that is hosting
            # the PostgreSQL server.
            # We use the machine FQDN and the barman_user
            # setting to call the barman-wal-restore correctly.
            # If local recovery, we use barman directly, assuming
            # the postgres process will be executed with the barman user.
            # It MUST to be reviewed by the user in any case.
            if remote_command:
                fqdn = socket.getfqdn()
                recovery_conf_lines.append(
                    "# The 'barman-wal-restore' command "
                    "is provided in the 'barman-cli' package"
                )
                restore_command = (
                    "restore_command = 'barman-wal-restore %s -U %s %s %s %s %%f %%p'"
                    % (
                        partial_option,
                        self.config.config.user,
                        port_option,
                        fqdn,
                        self.config.name,
                    )
                )
                if self.config.parallel_jobs > 1:
                    # Remove the last 'tick' if we are appending a `-p jobs'`.
                    restore_command = (
                        restore_command[:-1] + " -p %s'" % self.config.parallel_jobs
                    )
                # Normalize spaces
                restore_command = re.sub(r"\s+", " ", restore_command)
                recovery_conf_lines.append(restore_command)
            else:
                recovery_conf_lines.append(
                    "# The 'barman get-wal' command "
                    "must run as '%s' user" % self.config.config.user
                )
                recovery_conf_lines.append(
                    "restore_command = 'barman get-wal %s %s %%f > %%p'"
                    % (partial_option, self.config.name)
                )
            recovery_info["results"]["get_wal"] = True
        elif not standby_mode:
            # We copy all the needed WAL files to the wal_dest directory when get-wal
            # is not requested, except when we are in standby mode. In the case of
            # standby mode, the server will not exit recovery, so the
            # recovery_end_command would never be executed.
            # For this reason, with standby_mode, we need to copy the WAL files
            # directly in the pg_wal directory.
            recovery_conf_lines.append(f"restore_command = 'cp {wal_dest}/%f %p'")
            recovery_conf_lines.append(f"recovery_end_command = 'rm -fr {wal_dest}'")

        # Writes recovery target
        if target_time:
            # 'target_time' is the value as it came from '--target-time' command-line
            # option, which may be without a time zone. When writing the actual Postgres
            # configuration we should use a value with an explicit time zone set, so we
            # avoid hitting pitfalls. We use the 'target_datetime' which was prevously
            # added to 'recovery_info'. It already handles the cases where the user
            # specifies no time zone, and uses the Barman host time zone as a fallback.
            # In short: if 'target_time' is present it means the user asked for a
            # specific point in time, but we need a sanitized value to use in the
            # Postgres configuration, so we use 'target_datetime'.
            # See '_set_pitr_targets'.
            recovery_conf_lines.append(
                "recovery_target_time = '%s'" % recovery_info["target_datetime"],
            )
        if target_xid:
            recovery_conf_lines.append("recovery_target_xid = '%s'" % target_xid)
        if target_lsn:
            recovery_conf_lines.append("recovery_target_lsn = '%s'" % target_lsn)
        if target_name:
            recovery_conf_lines.append("recovery_target_name = '%s'" % target_name)
        # TODO: log a warning if PostgreSQL < 9.4 and --immediate
        if backup_info.version >= 90400 and immediate:
            recovery_conf_lines.append("recovery_target = 'immediate'")

        # Manage what happens after recovery target is reached
        if (target_xid or target_time or target_lsn) and exclusive:
            recovery_conf_lines.append(
                "recovery_target_inclusive = '%s'" % (not exclusive)
            )
        if target_tli:
            recovery_conf_lines.append("recovery_target_timeline = %s" % target_tli)

        # Write recovery target action
        if "pause_at_recovery_target" in recovery_info:
            recovery_conf_lines.append(
                "pause_at_recovery_target = '%s'"
                % recovery_info["pause_at_recovery_target"]
            )
        if "recovery_target_action" in recovery_info:
            recovery_conf_lines.append(
                "recovery_target_action = '%s'"
                % recovery_info["recovery_target_action"]
            )

        # Set the standby mode
        if backup_info.version >= 120000:
            signal_file = "recovery.signal"
            if standby_mode:
                signal_file = "standby.signal"

            if remote_command:
                recovery_file = os.path.join(recovery_info["tempdir"], signal_file)
            else:
                recovery_file = os.path.join(dest, signal_file)

            open(recovery_file, "ab").close()
            recovery_info["auto_conf_append_lines"] = recovery_conf_lines
        else:
            if standby_mode:
                recovery_conf_lines.append("standby_mode = 'on'")

            if remote_command:
                recovery_file = os.path.join(recovery_info["tempdir"], "recovery.conf")
            else:
                recovery_file = os.path.join(dest, "recovery.conf")

            with open(recovery_file, "wb") as recovery:
                recovery.write(("\n".join(recovery_conf_lines) + "\n").encode("utf-8"))

        if remote_command:
            plain_rsync = RsyncPgData(
                path=self.server.path,
                ssh=remote_command,
                bwlimit=self.config.bandwidth_limit,
                network_compression=self.config.network_compression,
            )
            try:
                plain_rsync.from_file_list(
                    [os.path.basename(recovery_file)],
                    recovery_info["tempdir"],
                    ":%s" % dest,
                )
            except CommandFailedException as e:
                output.error(
                    "remote copy of %s failed: %s", os.path.basename(recovery_file), e
                )
                output.close_and_exit()

    def _conf_files_exist(self, conf_files, backup_info, recovery_info):
        """
        Determine whether the conf files in the supplied list exist in the backup
        represented by backup_info.

        Returns a map of conf_file:exists.
        """
        exists = {}
        for conf_file in conf_files:
            source_path = os.path.join(backup_info.get_data_directory(), conf_file)
            exists[conf_file] = os.path.exists(source_path)
        return exists

    def _copy_conf_files_to_tempdir(
        self, backup_info, recovery_info, remote_command=None
    ):
        """
        Copy conf files from the backup location to a temporary directory so that
        they can be checked and mangled.

        Returns a list of the paths to the temporary conf files.
        """
        conf_file_paths = []
        for conf_file in recovery_info["configuration_files"]:
            conf_file_path = os.path.join(recovery_info["tempdir"], conf_file)
            shutil.copy2(
                os.path.join(backup_info.get_data_directory(), conf_file),
                conf_file_path,
            )
            conf_file_paths.append(conf_file_path)
        return conf_file_paths

    def _map_temporary_config_files(self, recovery_info, backup_info, remote_command):
        """
        Map configuration files, by filling the 'temporary_configuration_files'
        array, depending on remote or local recovery. This array will be used
        by the subsequent methods of the class.

        :param dict recovery_info: Dictionary containing all the recovery
            parameters
        :param barman.infofile.LocalBackupInfo backup_info: a backup
            representation
        :param str remote_command: ssh command for remote recovery
        """

        # Cycle over postgres configuration files which my be missing.
        # If a file is missing, we will be unable to restore it and
        # we will warn the user.
        # This can happen if we are using pg_basebackup and
        # a configuration file is located outside the data dir.
        # This is not an error condition, so we check also for
        # `pg_ident.conf` which is an optional file.
        hardcoded_files = ["pg_hba.conf", "pg_ident.conf"]
        conf_files = recovery_info["configuration_files"] + hardcoded_files
        conf_files_exist = self._conf_files_exist(
            conf_files, backup_info, recovery_info
        )

        for conf_file, exists in conf_files_exist.items():
            if not exists:
                recovery_info["results"]["missing_files"].append(conf_file)
                # Remove the file from the list of configuration files
                if conf_file in recovery_info["configuration_files"]:
                    recovery_info["configuration_files"].remove(conf_file)

        conf_file_paths = []
        if remote_command:
            # If the recovery is remote, copy the postgresql.conf
            # file in a temp dir
            conf_file_paths = self._copy_conf_files_to_tempdir(
                backup_info, recovery_info, remote_command
            )
        else:
            conf_file_paths = [
                os.path.join(recovery_info["destination_path"], conf_file)
                for conf_file in recovery_info["configuration_files"]
            ]
        recovery_info["temporary_configuration_files"].extend(conf_file_paths)

        if backup_info.version >= 120000:
            # Make sure the recovery configuration file ('postgresql.auto.conf', unless
            # a custom alternative was specified via recovery_conf_filename) exists in
            # recovery_info['temporary_configuration_files'] because the recovery
            # settings will end up there.
            conf_file = recovery_info["results"]["recovery_configuration_file"]
            # If the file did not exist it will have been removed from
            # recovery_info["configuration_files"] earlier in this method.
            if conf_file not in recovery_info["configuration_files"]:
                if remote_command:
                    conf_file_path = os.path.join(recovery_info["tempdir"], conf_file)
                else:
                    conf_file_path = os.path.join(
                        recovery_info["destination_path"], conf_file
                    )
                # Touch the file into existence
                open(conf_file_path, "ab").close()
                recovery_info["temporary_configuration_files"].append(conf_file_path)

    def _analyse_temporary_config_files(self, recovery_info):
        """
        Analyse temporary configuration files and identify dangerous options

        Mark all the dangerous options for the user to review. This procedure
        also changes harmful options such as 'archive_command'.

        :param dict recovery_info: dictionary holding all recovery parameters
        """
        results = recovery_info["results"]
        config_mangeler = ConfigurationFileMangeler()
        validator = ConfigIssueDetection()
        # Check for dangerous options inside every config file
        for conf_file in recovery_info["temporary_configuration_files"]:
            append_lines = None
            conf_file_suffix = results["recovery_configuration_file"]
            if conf_file.endswith(conf_file_suffix):
                append_lines = recovery_info.get("auto_conf_append_lines")

            # Identify and comment out dangerous options, replacing them with
            # the appropriate values
            results["changes"] += config_mangeler.mangle_options(
                conf_file, "%s.origin" % conf_file, append_lines
            )

            # Identify dangerous options and warn users about their presence
            results["warnings"] += validator.detect_issues(conf_file)

    def _copy_temporary_config_files(self, dest, remote_command, recovery_info):
        """
        Copy modified configuration files using rsync in case of
        remote recovery

        :param str dest: destination directory of the recovery
        :param str remote_command: ssh command for remote connection
        :param dict recovery_info: Dictionary containing all the recovery
            parameters
        """
        if remote_command:
            # If this is a remote recovery, rsync the modified files from the
            # temporary local directory to the remote destination directory.
            # The list of files is built from `temporary_configuration_files` instead
            # of `configuration_files` because `configuration_files` is not guaranteed
            # to include the recovery configuration file.
            file_list = []
            for conf_path in recovery_info["temporary_configuration_files"]:
                conf_file = os.path.basename(conf_path)
                file_list.append("%s" % conf_file)
                file_list.append("%s.origin" % conf_file)

            try:
                recovery_info["rsync"].from_file_list(
                    file_list, recovery_info["tempdir"], ":%s" % dest
                )
            except CommandFailedException as e:
                output.error("remote copy of configuration files failed: %s", e)
                output.close_and_exit()

    def close(self):
        """
        Cleanup operations for a recovery
        """
        # Remove the temporary directories
        for temp_dir in self.temp_dirs:
            temp_dir.delete()
        self.temp_dirs = []


class RemoteConfigRecoveryExecutor(RecoveryExecutor):
    """
    Recovery executor which retrieves config files from the recovery directory
    instead of the backup directory. Useful when the config files are not available
    in the backup directory (e.g. compressed backups).
    """

    def _conf_files_exist(self, conf_files, backup_info, recovery_info):
        """
        Determine whether the conf files in the supplied list exist in the backup
        represented by backup_info.

        :param list[str] conf_files: List of config files to be checked.
        :param BackupInfo backup_info: Backup information for the backup being
            recovered.
        :param dict recovery_info: Dictionary of recovery information.
        :rtype: dict[str,bool]
        :return: A dict representing a map of conf_file:exists.
        """
        exists = {}
        for conf_file in conf_files:
            source_path = os.path.join(recovery_info["destination_path"], conf_file)
            exists[conf_file] = recovery_info["cmd"].exists(source_path)
        return exists

    def _copy_conf_files_to_tempdir(
        self, backup_info, recovery_info, remote_command=None
    ):
        """
        Copy conf files from the backup location to a temporary directory so that
        they can be checked and mangled.

        :param BackupInfo backup_info: Backup information for the backup being
            recovered.
        :param dict recovery_info: Dictionary of recovery information.
        :param str remote_command: The ssh command to be used when copying the files.
        :rtype: list[str]
        :return: A list of paths to the destination conf files.
        """
        conf_file_paths = []

        rsync = RsyncPgData(
            path=self.server.path,
            ssh=remote_command,
            bwlimit=self.config.bandwidth_limit,
            network_compression=self.config.network_compression,
        )

        rsync.from_file_list(
            recovery_info["configuration_files"],
            ":" + recovery_info["destination_path"],
            recovery_info["tempdir"],
        )

        conf_file_paths.extend(
            [
                os.path.join(recovery_info["tempdir"], conf_file)
                for conf_file in recovery_info["configuration_files"]
            ]
        )
        return conf_file_paths


class SnapshotRecoveryExecutor(RemoteConfigRecoveryExecutor):
    """
    Recovery executor which performs barman recovery tasks for a backup taken with
    backup_method snapshot.

    It is responsible for:

        - Checking that disks cloned from the snapshots in the backup are attached to
          the recovery instance and that they are mounted at the correct location with
          the expected options.
        - Copying the backup_label into place.
        - Applying the requested recovery options to the PostgreSQL configuration.

    It does not handle the creation of the recovery instance, the creation of new disks
    from the snapshots or the attachment of the disks to the recovery instance. These
    are expected to have been performed before the `barman recover` runs.
    """

    def _prepare_tablespaces(self, backup_info, cmd, dest, tablespaces):
        """
        There is no need to prepare tablespace directories because they will already be
        present on the recovery instance through the cloning of disks from the backup
        snapshots.

        This function is therefore a no-op.
        """
        pass

    @staticmethod
    def check_recovery_dir_exists(recovery_dir, cmd):
        """
        Verify that the recovery directory already exists.

        :param str recovery_dir: Path to the recovery directory on the recovery instance
        :param UnixLocalCommand cmd: The command wrapper for running commands on the
            recovery instance.
        """
        if not cmd.check_directory_exists(recovery_dir):
            message = (
                "Recovery directory '{}' does not exist on the recovery instance. "
                "Check all required disks have been created, attached and mounted."
            ).format(recovery_dir)
            raise RecoveryPreconditionException(message)

    @staticmethod
    def get_attached_volumes_for_backup(snapshot_interface, backup_info, instance_name):
        """
        Verifies that disks cloned from the snapshots specified in the supplied
        backup_info are attached to the named instance and returns them as a dict
        where the keys are snapshot names and the values are the names of the
        attached devices.

        If any snapshot associated with this backup is not found as the source
        for any disk attached to the instance then a RecoveryPreconditionException
        is raised.

        :param CloudSnapshotInterface snapshot_interface: Interface for managing
            snapshots via a cloud provider API.
        :param BackupInfo backup_info: Backup information for the backup being
            recovered.
        :param str instance_name: The name of the VM instance to which the disks
            to be backed up are attached.
        :rtype: dict[str,str]
        :return: A dict where the key is the snapshot name and the value is the
            device path for the source disk for that snapshot on the specified
            instance.
        """
        if backup_info.snapshots_info is None:
            return {}
        attached_volumes = snapshot_interface.get_attached_volumes(instance_name)
        attached_volumes_for_backup = {}
        missing_snapshots = []
        for source_snapshot in backup_info.snapshots_info.snapshots:
            try:
                disk, attached_volume = [
                    (k, v)
                    for k, v in attached_volumes.items()
                    if v.source_snapshot == source_snapshot.identifier
                ][0]

                attached_volumes_for_backup[disk] = attached_volume
            except IndexError:
                missing_snapshots.append(source_snapshot.identifier)

        if len(missing_snapshots) > 0:
            raise RecoveryPreconditionException(
                "The following snapshots are not attached to recovery instance %s: %s"
                % (instance_name, ", ".join(missing_snapshots))
            )
        else:
            return attached_volumes_for_backup

    @staticmethod
    def check_mount_points(backup_info, attached_volumes, cmd):
        """
        Check that each disk cloned from a snapshot is mounted at the same mount point
        as the original disk and with the same mount options.

        Raises a RecoveryPreconditionException if any of the devices supplied in
        attached_snapshots are not mounted at the mount point or with the mount options
        specified in the snapshot metadata.

        :param BackupInfo backup_info: Backup information for the backup being
            recovered.
        :param dict[str,barman.cloud.VolumeMetadata] attached_volumes: Metadata for the
            volumes attached to the recovery instance.
        :param UnixLocalCommand cmd: The command wrapper for running commands on the
            recovery instance.
        """
        mount_point_errors = []
        mount_options_errors = []
        for disk, volume in sorted(attached_volumes.items()):
            try:
                volume.resolve_mounted_volume(cmd)
                mount_point = volume.mount_point
                mount_options = volume.mount_options
            except SnapshotBackupException as e:
                mount_point_errors.append(
                    "Error finding mount point for disk %s: %s" % (disk, e)
                )
                continue
            if mount_point is None:
                mount_point_errors.append(
                    "Could not find disk %s at any mount point" % disk
                )
                continue
            snapshot_metadata = next(
                metadata
                for metadata in backup_info.snapshots_info.snapshots
                if metadata.identifier == volume.source_snapshot
            )
            expected_mount_point = snapshot_metadata.mount_point
            expected_mount_options = snapshot_metadata.mount_options
            if mount_point != expected_mount_point:
                mount_point_errors.append(
                    "Disk %s cloned from snapshot %s is mounted at %s but %s was "
                    "expected."
                    % (disk, volume.source_snapshot, mount_point, expected_mount_point)
                )
            if mount_options != expected_mount_options:
                mount_options_errors.append(
                    "Disk %s cloned from snapshot %s is mounted with %s but %s was "
                    "expected."
                    % (
                        disk,
                        volume.source_snapshot,
                        mount_options,
                        expected_mount_options,
                    )
                )
        if len(mount_point_errors) > 0:
            raise RecoveryPreconditionException(
                "Error checking mount points: %s" % ", ".join(mount_point_errors)
            )
        if len(mount_options_errors) > 0:
            raise RecoveryPreconditionException(
                "Error checking mount options: %s" % ", ".join(mount_options_errors)
            )

    def recover(
        self,
        backup_info,
        dest,
        wal_dest=None,
        tablespaces=None,
        remote_command=None,
        target_tli=None,
        target_time=None,
        target_xid=None,
        target_lsn=None,
        target_name=None,
        target_immediate=False,
        exclusive=False,
        target_action=None,
        standby_mode=None,
        recovery_conf_filename=None,
        recovery_option_port=None,
        recovery_instance=None,
    ):
        """
        Performs a recovery of a snapshot backup.

        This method should be called in a closing context.

        :param barman.infofile.BackupInfo backup_info: the backup to recover
        :param str dest: the destination directory
        :param str|None wal_dest: the destination directory for WALs when doing PITR.
            See :meth:`~barman.recovery_executor.RecoveryExecutor._set_pitr_targets`
            for more details.
        :param dict[str,str]|None tablespaces: a tablespace
            name -> location map (for relocation)
        :param str|None remote_command: The remote command to recover
                               the base backup, in case of remote backup.
        :param str|None target_tli: the target timeline
        :param str|None target_time: the target time
        :param str|None target_xid: the target xid
        :param str|None target_lsn: the target LSN
        :param str|None target_name: the target name created previously with
                            pg_create_restore_point() function call
        :param str|None target_immediate: end recovery as soon as consistency
            is reached
        :param bool exclusive: whether the recovery is exclusive or not
        :param str|None target_action: The recovery target action
        :param bool|None standby_mode: standby mode
        :param str|None recovery_conf_filename: filename for storing recovery
            configurations
        :kwparam str|None recovery_option_port: port to set in restore command
            when invoking ``barman-wal-restore``
        :param str|None recovery_instance: The name of the recovery node as it
            is known by the cloud provider
        """
        snapshot_interface = get_snapshot_interface_from_backup_info(
            backup_info, self.server.config
        )
        attached_volumes = self.get_attached_volumes_for_backup(
            snapshot_interface, backup_info, recovery_instance
        )
        cmd = fs.unix_command_factory(remote_command, self.server.path)
        SnapshotRecoveryExecutor.check_mount_points(backup_info, attached_volumes, cmd)
        self.check_recovery_dir_exists(dest, cmd)

        try:
            cmd.create_dir_if_not_exists(dest, mode="700")
        except FsOperationFailed as e:
            output.error("unable to initialise destination directory '%s': %s", dest, e)
            output.close_and_exit()

        return super(SnapshotRecoveryExecutor, self).recover(
            backup_info,
            dest,
            wal_dest=wal_dest,
            tablespaces=None,
            remote_command=remote_command,
            target_tli=target_tli,
            target_time=target_time,
            target_xid=target_xid,
            target_lsn=target_lsn,
            target_name=target_name,
            target_immediate=target_immediate,
            exclusive=exclusive,
            target_action=target_action,
            standby_mode=standby_mode,
            recovery_conf_filename=recovery_conf_filename,
            recovery_option_port=recovery_option_port,
        )

    def _start_backup_copy_message(self):
        """
        Write the start backup copy message to the output.
        """
        output.info("Copying the backup label.")

    def _backup_copy_failure_message(self, e):
        """
        Write the backup failure message to the output.
        """
        output.error("Failure copying the backup label: %s", e)

    def _backup_copy(self, backup_info, dest, remote_command=None, **kwargs):
        """
        Copy any files from the backup directory which are required by the
        snapshot recovery (currently only the backup_label).

        :param barman.infofile.LocalBackupInfo backup_info: the backup
            to recover
        :param str dest: the destination directory
        """
        # Set a ':' prefix to remote destinations
        dest_prefix = ""
        if remote_command:
            dest_prefix = ":"

        # Create the copy controller object, specific for rsync,
        # which will drive all the copy operations. Items to be
        # copied are added before executing the copy() method
        controller = RsyncCopyController(
            path=self.server.path,
            ssh_command=remote_command,
            network_compression=self.config.network_compression,
            retry_times=self.config.basebackup_retry_times,
            retry_sleep=self.config.basebackup_retry_sleep,
            workers=self.config.parallel_jobs,
            workers_start_batch_period=self.config.parallel_jobs_start_batch_period,
            workers_start_batch_size=self.config.parallel_jobs_start_batch_size,
        )
        backup_label_file = "%s/%s" % (backup_info.get_data_directory(), "backup_label")
        controller.add_file(
            label="pgdata",
            src=backup_label_file,
            dst="%s/%s" % (dest_prefix + dest, "backup_label"),
            item_class=controller.PGDATA_CLASS,
            bwlimit=self.config.get_bwlimit(),
        )

        # Execute the copy
        try:
            controller.copy()
        except CommandFailedException as e:
            msg = "data transfer failure"
            raise DataTransferFailure.from_command_error("rsync", e, msg)


class RecoveryOperation(ABC):
    """
    A base class for recovery operations.

    This class defines the interface for recovery operations that can be executed on
    backups. Subclasses must implement the :meth:`_should_execute` and :meth:`_execute`
    methods, which checks if the operation should be executed and executes the actual
    operation, respectively.

    This class also provides utility methods for executing operations on backup trees,
    and creating volatile backup info objects.

    :cvar NAME: str: The name of the operation, used for identification.
    """

    NAME = None

    def __init__(self, config, server, backup_manager):
        """
        Constructor.

        :param barman.config.Config config: The Barman configuration
        :param barman.server.Server server: The Barman server instance
        :param barman.backup.BackupManager backup_manager: The BackupManager instance
        """
        self.config = config
        self.server = server
        self.backup_manager = backup_manager
        self.cmd = None  # Set once the operation is executed

    def execute(
        self,
        backup_info,
        destination,
        tablespaces=None,
        remote_command=None,
        recovery_info=None,
        safe_horizon=None,
        is_last_operation=False,
    ):
        """
        Execute the operation.

        .. note::
            This method is the entry point for executing operations. It calls the
            respective underlying :meth:`_execute` method of the class, which contains
            the actual implementation for dealing with the operation.

        :param barman.infofile.LocalBackupInfo backup_info: An object representing the
            backup
        :param str destination: The destination directory where the output of the
            operation will be stored
        :param dict[str,str]|None tablespaces: A dictionary mapping tablespace names to
            their target directories. This is the relocation chosen by the user when
            invoking the ``restore`` command. If ``None``, it means no relocation was
            chosen
        :param str|None remote_command: The SSH remote command to use for the recovery,
            in case of a remote recovery. If ``None``, it means the recovery is local
        :param dict[str,any] recovery_info: A dictionary that populated with metadata
            about the recovery process
        :param datetime.datetime|None safe_horizon: The safe horizon of the backup.
            Any file rsync-copied after this time has to be checked with checksum
        :param bool is_last_operation: Whether this is the last operation in the
            recovery chain
        :return barman.infofile.VolatileBackupInfo: The respective volatile backup info
            of *backup_info* which reflects all changes performed by the operation
        """
        # Set the appropriate command interface to run Unix commands
        self.cmd = self._get_command_interface(remote_command)

        # Then proceed with the operation execution
        return self._execute(
            backup_info,
            destination,
            tablespaces,
            remote_command,
            recovery_info,
            safe_horizon,
            is_last_operation,
        )

    @abstractmethod
    def _execute(
        self,
        backup_info,
        destination,
        tablespaces,
        remote_command,
        recovery_info,
        safe_horizon,
        is_last_operation,
    ):
        """
        Execute the operation for a given backup.

        :param barman.infofile.LocalBackupInfo backup_info: An object representing the
            backup
        :param str destination: The destination directory where the output of the
            operation will be stored
        :param dict[str,str]|None tablespaces: A dictionary mapping tablespace names to
            their target directories. This is the relocation chosen by the user when
            invoking the ``restore`` command. If ``None``, it means no relocation was
            chosen
        :param str|None remote_command: The SSH remote command to use for the recovery,
            in case of a remote recovery. If ``None``, it means the recovery is local
        :param dict[str,any] recovery_info: A dictionary that populated with metadata
            about the recovery process
        :param datetime.datetime|None safe_horizon: The safe horizon of the backup.
            Any file rsync-copied after this time has to be checked with checksum
        :param bool is_last_operation: Whether this is the last operation in the
            recovery chain
        :return barman.infofile.VolatileBackupInfo: The respective volatile backup info
            of *backup_info* which reflects all changes performed by the operation
        """

    @abstractmethod
    def _should_execute(self, backup_info):
        """
        Check if the operation should be executed on the given backup.

        This is checked before the operation is executed against each backup in the
        chain.

        This method must be overridden by subclasses to implement specific checks
        for whether the operation should be executed on the provided *backup_info*.

        :param barman.infofile.LocalBackupInfo backup_info: The backup info to check
        :return bool: ``True`` if the operation should be executed, ``False`` otherwise
        """

    def _execute_on_chain(self, backup_info, destination, method, *args, **kwargs):
        """
        Executes a given method iteratively on all backups in a chain. In case of
        a non-incremental backup, it executes the method only on the specified backup,
        as usual.

        This is a shorthand for operations such as decrypting or decompressing
        incremental backups, where the operation must be applied not only to the target
        backup but also to all its ascendants that are also encrypted/compressed.

        *method* must return a :class:`barman.infofile.VolatileBackupInfo` object.

        This method also ensures that all resulting volatile backups are properly
        re-linked in memory so that tree-based operations like
        :meth:`~LocalBackupInfo.walk_to_root` continue to work.

        .. note::
            This method executes the operation only in the backups that need it. If the
            chain contains backups that do not require the operation, it will skip the
            operation for those backups, and will simply move (or copy) the backup to
            the destination directory.

        :param barman.infofile.LocalBackupInfo backup_info: The backup in recovery.
            In case of an incremental backup, all its ascendants are also processed
        :param str destination: The destination directory where the output of the
            operation will be stored
        :param callable method: The method to execute on the backup. In case of an
            incremental backup, this method is also executed on all its ascendants
        :param args: Positional arguments to pass to the *method*
        :param kwargs: Keyword arguments to pass to the *method*
        :return barman.infofile.VolatileBackupInfo: The respective volatile backup info
            of *backup_info* which reflects all changes performed by the operation
        """
        # Main backup is the respective volatile backup of the backup_info received
        main_vol_backup = None

        backups = {}
        for backup in backup_info.walk_to_root():
            if self._should_execute(backup):
                output.debug(
                    "Executing %s operation for backup %s.",
                    self.NAME,
                    backup.backup_id,
                )
                volatile_backup = method(backup, destination, *args, **kwargs)
            else:
                # This block handles backups within an incremental chain that do not
                # require the current operation (e.g., an uncompressed backup during a
                # decompress operation, or an unencrypted backup during a decrypt
                # operation).
                #
                # Instead of processing these backups, we pass them through to the
                # destination directory, ensuring it contains the complete chain for the
                # next operation in the pipeline. This allows each operation to clean up
                # its source staging directory atomically, without having to care about
                # the staging directory of other previous operations.
                #
                # The logic is as follows:
                # - If the backup is from the main catalog (basebackups_directory),
                #   it is COPIED to preserve the original backup.
                # - If it's already in a staging path from a previous operation,
                #   it is MOVED for efficiency.
                #
                # Note: The "_prepare_directory" method is called to ensure the
                # destination directory exists and is ready to receive the backup. This
                # is done to prevent errors, just in case the first backup of the chain
                # is the one being skipped here. If it were the second onwards in the
                # chain, the directory would already have been created by a previous
                # call of *method*, and the "_prepare_directory" is a no-op in that
                # case.
                try:
                    self._prepare_directory(destination, delete_if_exists=False)
                    if backup.get_base_directory() == self.config.basebackups_directory:
                        self.cmd.copy(backup.get_basebackup_directory(), destination)
                    else:
                        self.cmd.move(backup.get_basebackup_directory(), destination)
                    volatile_backup = self._create_volatile_backup_info(
                        backup, destination
                    )
                    # The `_link_tablespace` method ensures that proper symbolic links
                    # are created for each tablespace that was moved or copied as part
                    # of skipping the operation. These links are essential for the
                    # restored cluster to correctly recognize and access the tablespaces
                    # during the final step of the backup combination process.
                    #
                    # We can safely hardcode `tablespaces` as `None` and
                    # `is_last_operation` as `False` because this "skip" logic can only
                    # be executed during intermediate stages of the recovery pipeline
                    # (e.g., decryption or decompression).
                    #
                    # The reasoning is as follows:
                    # 1. Entering this `else` block implies a mixed incremental chain
                    #    (e.g., some backups compressed, some not). Such a chain, by
                    #    definition, requires at least a subsequent `CombineOperation`,
                    #    and possibly yet a `RsyncOperation`, meaning this can't be the
                    #    final operation.
                    # 2. The `RsyncCopyOperation`, if required, is designed to always
                    #    execute and will never enter this 'skip' block. The
                    #    `CombineOperation`, on its turn, does not use this
                    #    `_execute_on_chain` method at all.
                    #
                    # Since tablespace relocation as requested by the user is a concern
                    # only for the final operation in the pipeline, i.e. intermediate
                    # relocations are performed inside the staging area, we do not need
                    # to care about such relocation here.
                    self._link_tablespaces(
                        volatile_backup,
                        volatile_backup.get_data_directory(),
                        tablespaces=None,
                        is_last_operation=False,
                    )
                    output.debug(
                        "Skipping %s operation for backup %s as it's not required",
                        self.NAME,
                        backup.backup_id,
                    )
                except FsOperationFailed as e:
                    output.error("File system error: %s", str(e))
                    output.close_and_exit()
            backups[volatile_backup.backup_id] = volatile_backup
            if main_vol_backup is None:
                main_vol_backup = volatile_backup

        # Rebuild the backup chain in memory if there are multiple backups. This ensures
        # that parent_instance links between backups are properly re-established
        if len(backups) > 1:
            for backup in backups.values():
                backup.parent_instance = backups.get(backup.parent_backup_id)

        return main_vol_backup

    def _create_volatile_backup_info(self, backup_info, base_directory):
        """
        Create a :class:`VolatileBackupInfo` instance as a copy of the given
        *backup_info* with *base_directory* as its location.

        :param barman.infofile.LocalBackupInfo backup_info: The original backup info
        :param str base_directory: The base directory where the new volatile backup
            info will be created
        :rtype: barman.infofile.VolatileBackupInfo
        :return: A new :class:`VolatileBackupInfo` instance that is a copy of the
            original *backup_info*, but with the specified base directory
        """
        # Serialize the original backup_info into memory
        buffer = io.BytesIO()
        backup_info.save(file_object=buffer)
        buffer.seek(0)

        # Instantiate a new VolatileBackupInfo and populate it using the serialized data
        volatile_backup_info = VolatileBackupInfo(
            server=self.server,
            base_directory=base_directory,
            backup_id=backup_info.backup_id,
        )
        volatile_backup_info.load(file_object=buffer)

        return volatile_backup_info

    def _prepare_directory(self, dest_dir, delete_if_exists=True):
        """
        Prepare a directory for receiving a backup operation's output.

        This method is responsible for removing a directory if it already
        exists, then (re)creating it and ensuring the correct permissions.

        :param str dest_dir: The destination directory
        :param bool delete_if_exists: If *dest_dir* should be removed before we attempt
            to create it.
        """
        if delete_if_exists:
            self.cmd.delete_if_exists(dest_dir)
        self.cmd.create_dir_if_not_exists(dest_dir, mode="700")
        self.cmd.check_write_permission(dest_dir)

    def _get_command_interface(self, remote_command):
        """
        Get the command interface for executing operations.

        The command interface can be a :class:`barman.fs.UnixLocalCommand` or a
        :class:`barman.fs.UnixRemoteCommand`, depending on where the operation
        is meant to be executed (if :attr:`config.staging_location` is ``remote`` or
        ``local``).

        :param str|None remote_command: The SSH remote command to use for the recovery,
            in case of a remote recovery. If ``None``, it means the recovery is local
        :return barman.fs.UnixLocalCommand: The command interface for executing
            operations
        """
        if remote_command and self.config.staging_location == "remote":
            return fs.unix_command_factory(remote_command, self.server.path)
        return fs.unix_command_factory(None, self.server.path)

    def _post_recovery_cleanup(self, destination):
        """
        Perform cleanup actions after the recovery operation is completed.

        This takes care of removing unnecessary files on the restored backup.
        This method is useful for certain operations that direct its output
        directly to the final destination, without the means to ignore certain files.

        .. note::
            Previously in Barman, rsync was the only way we copied files to the final
            destination. The :class:`RsyncCopyOperation` class has an ``exclude``
            parameter, which is used to ignore unwanted files, hence it was guaranteed
            such files would never appear in the destination directory.
            With the new structure provided by the :class:`MainRecoveryExecutor`, we
            have operations that might direct its output straight to the destination
            directory, e.g. :class:`CombineOperation` or :class:`DecryptOperation`,
            without providing any means to exclude specific content. For these cases,
            we need to ensure that unwanted files are removed after the operation
            is finished, which is the main purpose of this method.

        :param str destination: The destination directory where the recovery was performed
        """
        to_delete = [
            os.path.join(destination, "pg_log/*"),
            os.path.join(destination, "log/*"),
            os.path.join(destination, "pg_xlog/*"),
            os.path.join(destination, "pg_wal/*"),
            os.path.join(destination, "postmaster.pid"),
            os.path.join(destination, "recovery.conf"),
            os.path.join(destination, "tablespace_map"),
        ]
        for item in to_delete:
            try:
                self.cmd.delete_if_exists(item)
            except CommandFailedException as e:
                output.warning(
                    "Cleanup operation failed to delete %s after backup copy: %s\n"
                    "If this file or directory is irrelevant for the recovery, please remove it manually.",
                    item,
                    e,
                )

    def _link_tablespaces(
        self, backup_info, pgdata_dir, tablespaces, is_last_operation
    ):
        """
        Create the symlinks for the tablespaces in the destination directory.

        Each tablespace has a symlink created in the ``pg_tblspc`` directory
        pointing to the respective tablespace location after the recovery.

        Note:
            "pg_tblspc" directory is created even if there are no tablespaces to be
            linked, so we comply with the structure created by Postgres.

        :param barman.infofile.VolatileBackupInfo backup_info: The volatile backup info
            representing the backup state
        :param str pgdata_dir: The ``PGDATA`` directory of the restored backup
        :param dict[str,str]|None tablespaces: A dictionary mapping tablespace names to
            their target directories. This is the relocation chosen by the user when
            invoking the ``restore`` command. If ``None``, it means no relocation was
            chosen. Only used if it *is_last_operation*.
        :param bool is_last_operation: Whether this is the last operation in the
            recovery chain
        """
        tblspc_dir = os.path.join(pgdata_dir, "pg_tblspc")
        try:
            # create the pg_tblspc directory in the destination, if it does not exist
            self.cmd.create_dir_if_not_exists(tblspc_dir)
        except FsOperationFailed as e:
            output.error(
                "unable to initialize tablespace directory '%s': %s", tblspc_dir, e
            )
            output.close_and_exit()

        if not backup_info.tablespaces:
            output.debug("There are no tablespaces to be linked. Skipping this step.")
            return

        for tablespace in backup_info.tablespaces:
            # build the filename of the link under pg_tblspc directory
            pg_tblspc_file = os.path.join(tblspc_dir, str(tablespace.oid))
            # If this is not the last operation in the recovery chain,
            # the tablespace currently lives in the volatile backup directory
            if not is_last_operation:
                location = backup_info.get_data_directory(tablespace.oid)
            # Otherwise, it lives in its destination directory
            else:
                # by default a tablespace goes in the same location where
                # it was on the source server when the backup was taken
                location = tablespace.location
                # if a relocation has been requested for this tablespace,
                # use the target directory provided by the user
                if tablespaces and tablespace.name in tablespaces:
                    location = tablespaces[tablespace.name]
            try:
                # remove the current link in pg_tblspc, if it exists
                self.cmd.delete_if_exists(pg_tblspc_file)
                # check for write permissions on destination directory
                self.cmd.check_write_permission(location)
                # create symlink between tablespace and recovery folder
                self.cmd.create_symbolic_link(location, pg_tblspc_file)
            except FsOperationFailed as e:
                output.error(
                    "unable to prepare '%s' tablespace (destination '%s'): %s",
                    tablespace.name,
                    location,
                    e,
                )
                output.close_and_exit()

            # If this is the last operation in the recovery chain, we log the
            # tablespace information, as it might be relevant for the user to know
            # where tablespaces are being relocated to
            if is_last_operation:
                output.info("\t%s, %s, %s", tablespace.oid, tablespace.name, location)

    @property
    def staging_path(self):
        """
        Returns the staging path for the current process.

        The staging path is constructed by joining the base staging path from the
        configuration with the class name and the current process ID.

        :returns: The full staging path as a string.
        :rtype: str
        """
        return os.path.join(self.config.staging_path, self.NAME + str(os.getpid()))

    def cleanup_staging_dir(self):
        """
        Cleans up the staging directory if exists.

        Attempts to delete the staging directory specified by :attr:`self.staging_path`
        using the operation command interface. If the deletion fails,
        a warning is logged with the error details.

        :raises CommandFailedException: If the deletion operation fails.
        """
        try:
            self.cmd.delete_if_exists(self.staging_path)
        except CommandFailedException as e:
            output.warning(
                "Staging path cleanup operation failed to delete %s: %s\n",
                self.staging_path,
                e,
            )


class RsyncCopyOperation(RecoveryOperation):
    """
    Operation responsible for copying the backup data using ``rsync``.

    This operation copies PGDATA and respective tablespaces of a backup to a specified
    destination, making sure to exclude irrelevant files and directories.

    :cvar NAME: str: The name of the operation, used for identification
    """

    NAME = "barman-rsync-copy"

    def _get_command_interface(self, remote_command):
        """
        Get the command interface for executing operations.

        The command interface will be a :class:`barman.fs.UnixLocalCommand` or a
        :class:`barman.fs.UnixRemoteCommand`, depending on whether the
        *remote_command* is set or not.

        :param str|None remote_command: The SSH remote command to use for the recovery,
            in case of a remote recovery. If ``None``, it means the recovery is local
        :return barman.fs.UnixLocalCommand: The command interface for executing
            operations
        """
        return fs.unix_command_factory(remote_command, self.server.path)

    def _should_execute(self, backup_info):
        """
        Check if the rsync copy operation should be executed on the given backup.

        :param barman.infofile.LocalBackupInfo backup_info: The backup info to check
        :return bool: ``True`` if the operation should be executed, ``False`` otherwise
        """
        # This operation is always executed as there are no preconditions
        return True

    def _execute(
        self,
        backup_info,
        destination,
        tablespaces,
        remote_command,
        recovery_info,
        safe_horizon,
        is_last_operation,
    ):
        return self._execute_on_chain(
            backup_info,
            destination,
            self._rsync_backup_copy,
            tablespaces,
            remote_command,
            safe_horizon,
            is_last_operation,
        )

    def _rsync_backup_copy(
        self,
        backup_info,
        destination,
        tablespaces,
        remote_command,
        safe_horizon,
        is_last_operation,
    ):
        """
        Perform the rsync copy of the backup data to the specified destination.

        When this is the last operation in the recovery chain, it copies the PGDATA to
        the root of *destination* and tablespaces to their final destination, honoring
        relocation, if requested. Otherwise, it copies the whole backup directory
        (as in the barman catalog) to *destination* (a staging directory, in this case).

        :param barman.infofile.LocalBackupInfo backup_info: The backup info to copy
        :param str destination: The destination directory
        :param dict[str,str]|None tablespaces: A dictionary mapping tablespace names to
            their target directories. This is the relocation chosen by the user when
            invoking the ``restore`` command. If ``None``, it means no relocation
            chosen
        :param str|None remote_command: The SSH remote command to use for the recovery,
            in case of a remote recovery. If ``None``, it means the recovery is local
        :param datetime.datetime|None safe_horizon: The safe horizon of the backup.
            Any file rsync-copied after this time has to be checked with checksum
        :param bool is_last_operation: Whether this is the last operation in the
            recovery chain
        :return barman.infofile.VolatileBackupInfo: The respective volatile backup info
            of *backup_info* which reflects all changes performed by the operation
        """
        # Create a volatile backup info with the destination as its base directory
        vol_backup_info = self._create_volatile_backup_info(backup_info, destination)

        # Set a ':' prefix to remote destinations
        dest_prefix = ""
        if remote_command:
            dest_prefix = ":"

        # Create the copy controller object, specific for rsync, which will drive all
        # the copy operations. Items to be copied are added before calling copy()
        controller = RsyncCopyController(
            path=self.server.path,
            ssh_command=remote_command,
            network_compression=self.config.network_compression,
            safe_horizon=safe_horizon,
            retry_times=self.config.basebackup_retry_times,
            retry_sleep=self.config.basebackup_retry_sleep,
            workers=self.config.parallel_jobs,
            workers_start_batch_period=self.config.parallel_jobs_start_batch_period,
            workers_start_batch_size=self.config.parallel_jobs_start_batch_size,
        )

        if is_last_operation:
            # If this is the last operation then the root of *destination* is where
            # the backup is copied to. Tablespaces are copied directly to their
            # final destination
            self._copy_pgdata_and_tablespaces(
                backup_info, controller, dest_prefix, destination, tablespaces
            )
        else:
            # Otherwise, it means this is still an intermediate step, so we just copy
            # the whole backup directory (includes tablespaces) as in the Barman
            # catalog to the destination (which is a staging directory), as this backup
            # will still be referenced by following operations in the chain. This way
            # we maintain a consistent structure among volatile backups
            self._copy_backup_dir(backup_info, controller, dest_prefix, vol_backup_info)

        # Create the tablespaces symbolic links in the destination directory
        pgdata_dir = (
            destination if is_last_operation else vol_backup_info.get_data_directory()
        )
        self._link_tablespaces(
            vol_backup_info, pgdata_dir, tablespaces, is_last_operation
        )

        return vol_backup_info

    def _copy_backup_dir(self, backup_info, controller, dest_prefix, vol_backup_info):
        """
        Copy the backup directory to the destination.

        Copies the entire backup directory (as in the Barman catalog) to the
        specified destination, ensuring that the directory structure is preserved.

        :param barman.infofile.LocalBackupInfo backup_info: The backup info to copy
        :param barman.copy_controller.RsyncCopyController controller: The rsync controller object
        :param str dest_prefix: The prefix to add to the destination path
        :param barman.infofile.VolatileBackupInfo vol_backup_info: The volatile backup
            info that is the result of the whole operation
        :raises barman.exceptions.DataTransferFailure: If the copy operation fails
        """
        destination = vol_backup_info.get_basebackup_directory()
        controller.add_directory(
            label="backup",
            src="%s/" % backup_info.get_basebackup_directory(),
            dst=dest_prefix + destination,
            bwlimit=self.config.get_bwlimit(),
            item_class=controller.VOLATILE_BACKUP_CLASS,
        )

        # Prepare the destination directories for the backup copy
        self._prepare_directory(destination)

        # Execute the copy
        try:
            controller.copy()
        except CommandFailedException as e:
            msg = "data transfer failure"
            raise DataTransferFailure.from_command_error("rsync", e, msg)

    def _copy_pgdata_and_tablespaces(
        self, backup_info, controller, dest_prefix, destination, tablespaces
    ):
        """
        Copy the PGDATA and tablespaces to the specified destination.

        Copies the PGDATA to the root of *destination* and tablespaces to their
        final destination, honoring relocation if requested.

        :param barman.infofile.LocalBackupInfo backup_info: The backup info to copy
        :param barman.copy_controller.RsyncCopyController controller: The rsync controller object
        :param str dest_prefix: The prefix to add to the destination path
        :param str destination: The destination directory for the backup
        :param dict[str,str]|None tablespaces: A dictionary mapping tablespace names to
            their target directories. This is the relocation chosen by the user when
            invoking the ``restore`` command. If ``None``, it means no relocation was
            chosen
        :raises barman.exceptions.DataTransferFailure: If the copy operation fails
        """
        dest_dirs = [destination]
        exclude_and_protect = []
        if backup_info.tablespaces:
            for tablespace in backup_info.tablespaces:
                # By default a tablespace goes in the same location where
                # it was on the source server when the backup was taken
                location = tablespace.location
                # If a relocation has been requested for this tablespace
                # use the user provided target directory
                if tablespaces and tablespace.name in tablespaces:
                    location = tablespaces[tablespace.name]
                # If the tablespace location is inside the data directory,
                # exclude and protect it from being deleted during
                # the data directory copy
                if is_subdirectory(destination, location):
                    exclude_and_protect += [location[len(destination) :]]
                else:
                    # Else append it to the destination list so it is prepared
                    # before the copy operation
                    dest_dirs.append(location)
                # Exclude and protect the tablespace from being deleted during
                # the data directory copy
                exclude_and_protect.append("/pg_tblspc/%s" % tablespace.oid)
                # Add the tablespace directory to the list of objects
                # to be copied by the controller
                controller.add_directory(
                    label=tablespace.name,
                    src="%s/" % backup_info.get_data_directory(tablespace.oid),
                    dst=dest_prefix + location,
                    bwlimit=self.config.get_bwlimit(tablespace),
                    item_class=controller.TABLESPACE_CLASS,
                )
        # Add the PGDATA directory to the list of items to be copied by the controller
        controller.add_directory(
            label="pgdata",
            src="%s/" % backup_info.get_data_directory(),
            dst=dest_prefix + destination,
            bwlimit=self.config.get_bwlimit(),
            exclude=[
                "/pg_log/*",
                "/log/*",
                "/pg_xlog/*",
                "/pg_wal/*",
                "/postmaster.pid",
                "/recovery.conf",
                "/tablespace_map",
            ],
            exclude_and_protect=exclude_and_protect,
            item_class=controller.PGDATA_CLASS,
        )

        # Prepare the destination directories for the backup copy
        # No need to attempt deleting the directory as this is the last operation,
        # meaning that the destination is sure to be either empty or non-existing
        for _dir in dest_dirs:
            self._prepare_directory(_dir, delete_if_exists=False)

        # Execute the copy
        try:
            controller.copy()
        except CommandFailedException as e:
            msg = "data transfer failure"
            raise DataTransferFailure.from_command_error("rsync", e, msg)


class DecryptOperation(RecoveryOperation):
    """
    Operation responsible for decrypting backups.

    Decrypts the backup content using the respective encryption handler to the
    specified destination directory.

    :cvar NAME: str: The name of the operation, used for identification
    """

    NAME = "barman-decryption"

    def _execute(
        self,
        backup_info,
        destination,
        tablespaces,
        remote_command,
        recovery_info,
        safe_horizon,
        is_last_operation,
    ):
        return self._execute_on_chain(backup_info, destination, self._decrypt_backup)

    def _get_command_interface(self, remote_command):
        """
        Returns a command interface for executing commands locally.

        .. note::
            This method overrides the default behavior to ensure that decryption
            always occurs on the local machine, regardless of the configuration
            for staging location. It achieves this by always returning a local
           command interface.

        :param str|None remote_command: The SSH remote command to use for the recovery,
            in case of a remote recovery. If ``None``, it means the recovery is local
        :return barman.fs.UnixLocalCommand: The command interface for executing
            operations
        """
        if self.config.staging_location == "remote":
            output.warning(
                "'staging_location' is set to 'remote', but decryption requires GPG,"
                "which is configured on the Barman host. For this reason, "
                "decryption will be performed locally, as if 'staging_location' were "
                "set to 'local'. This applies only to decryption, other steps will "
                "still honor the configured 'staging_location'."
            )
        return fs.unix_command_factory(None, self.server.path)

    def _should_execute(self, backup_info):
        """
        Check if the decryption operation should be executed on the given backup.

        :param barman.infofile.LocalBackupInfo backup_info: The backup info to check
        :return bool: ``True`` if the operation should be executed, ``False`` otherwise
        """
        return backup_info.encryption is not None

    def _decrypt_backup(self, backup_info, destination):
        """
        Decrypt the given backup into the local staging path.

        .. note::
            We don't need to check whether this is the last operation because encrypted
            backups are always in 'tar' format. This format requires decryption to a
            staging path before decompression can occur elsewhere.

        :param barman.infofile.LocalBackupInfo backup_info: The backup to be decrypted.
        :param str destination: Path to the directory where the backup will be restored.
        """
        passphrase = None
        if self.config.encryption_passphrase_command:
            output.debug(
                "The 'encryption_passphrase_command' setting is present in the "
                "configuration. This implies that the catalog contains encrypted "
                "backup or WAL files. The private key will be retrieved to perform "
                "decryption as needed."
            )

            passphrase = get_passphrase_from_command(
                self.config.encryption_passphrase_command
            )
        if not passphrase:
            output.error(
                "Encrypted backup '%s' was found for server '%s', but "
                "'encryption_passphrase_command' is not configured correctly. "
                "Please fix it before attempting a restore.",
                backup_info.backup_id,
                self.server.config.name,
            )
            output.close_and_exit()

        volatile_backup_info = self._create_volatile_backup_info(
            backup_info, destination
        )
        destination = volatile_backup_info.get_data_directory()
        self._prepare_directory(destination)
        output.info(
            "Decrypting files from backup '%s' for server '%s'.",
            backup_info.backup_id,
            self.server.config.name,
        )
        encryption_manager = self.backup_manager.encryption_manager
        encryption_handler = encryption_manager.get_encryption(backup_info.encryption)
        for backup_file in backup_info.get_directory_entries("data"):
            # We "reconstruct" the "original backup" in the staging path. Encrypted
            # files are decrypted, while unencrypted files are copied as-is.
            if backup_file.endswith(".gpg"):
                output.debug("Decrypting file %s at %s" % (backup_file, destination))
                _ = encryption_handler.decrypt(
                    file=backup_file, dest=destination, passphrase=passphrase
                )
            else:
                shutil.copy2(backup_file, destination)

        # We create the tablespace directories to maintain the standard basebackup
        # directory structure. This is necessary to prevent failures in later
        # operations, such as RsyncCopyOperation, which rely on the presence of these
        # directories to create symlinks for tablespaces. During rsync, both PGDATA and
        # tablespace directories are copied — the tablespaces will be empty, while
        # PGDATA will contain tarballs for the base and tablespace OIDs.
        if backup_info.tablespaces:
            for tablespace in backup_info.tablespaces:
                tablespace_dst_path = volatile_backup_info.get_data_directory(
                    tablespace.oid
                )
                self._prepare_directory(tablespace_dst_path)
        return volatile_backup_info


class DecompressOperation(RecoveryOperation):
    """
    Operation responsible for decompressing backups.

    Decompresses the backup content using the respective compression handler to the
    specified destination directory.

    :cvar NAME: str: The name of the operation, used for identification
    :cvar BASE_TARBALL_NAME: str: The name of the backup tarball file
    """

    NAME = "barman-decompress"
    BASE_TARBALL_NAME = "base"

    def _execute(
        self,
        backup_info,
        destination,
        tablespaces,
        remote_command,
        recovery_info,
        safe_horizon,
        is_last_operation,
    ):
        return self._execute_on_chain(
            backup_info,
            destination,
            self._decompress_backup,
            tablespaces,
            is_last_operation,
        )

    def _should_execute(self, backup_info):
        """
        Check if the decompression operation should be executed on the given backup.

        :param barman.infofile.LocalBackupInfo backup_info: The backup info to check
        :return bool: ``True`` if the operation should be executed, ``False`` otherwise
        """
        return backup_info.compression is not None

    def _decompress_backup(
        self,
        backup_info,
        destination,
        tablespaces,
        is_last_operation,
    ):
        """
        Decompresses a backup and restores its contents to the specified *destination*.

        This method handles decompression of both the base backup tarball and any
        associated tablespaces, using the appropriate compression method as specified in
        the backup information. It supports relocation of tablespaces if requested and
        logs debug information about the decompression process.

        :param barman.infofile.LocalBackupInfo backup_info: Information about the backup
            to be decompressed.
        :param str destination: The target directory where the decompressed files will
            be placed.
        :param dict tablespaces: Optional mapping of tablespace names to their
            relocation paths.
        :param bool is_last_operation: Indicates if this is the final operation in the
        recovery process, affecting tablespace relocation.

        :return: Result of the recovery execution chain.
        :rtype: barman.infofile.VolatileBackupInfo
        :raises AttributeError: If the backup uses an unsupported or unknown compression
            format.
        """
        vol_backup_info = self._create_volatile_backup_info(backup_info, destination)
        if not is_last_operation:
            destination = vol_backup_info.get_data_directory()
        compressors = {
            GZipCompression.name: GZipCompression,
            LZ4Compression.name: LZ4Compression,
            ZSTDCompression.name: ZSTDCompression,
            NoneCompression.name: NoneCompression,
        }
        compression = backup_info.compression
        try:
            compressor = compressors[compression](self.cmd)
        except KeyError:
            raise UnsupportedCompressionFormat(
                f"Unexpected compression format: {compression}"
            )

        # Prepare the destination directory
        # No need to attempt deleting the directory if it's the last operation as it's
        # sure to be either empty or non-existing in that case
        self._prepare_directory(destination, delete_if_exists=(not is_last_operation))

        # Untar the results files to their intended location
        if backup_info.tablespaces:
            for tablespace in backup_info.tablespaces:
                tablespace_dst_path = vol_backup_info.get_data_directory(tablespace.oid)
                # Only relocate or send to final destination if decompression is the
                # last operation.
                if is_last_operation:
                    # By default a tablespace goes in the same location where
                    # it was on the source server when the backup was taken
                    tablespace_dst_path = tablespace.location
                    # If a relocation has been requested for this tablespace
                    # use the user provided target directory
                    if tablespaces and tablespace.name in tablespaces:
                        tablespace_dst_path = tablespaces[tablespace.name]
                tablespace_file = "%s.%s" % (
                    tablespace.oid,
                    compressor.file_extension,
                )
                tablespace_src_path = "%s/%s" % (
                    backup_info.get_data_directory(),
                    tablespace_file,
                )
                output.debug(
                    "Decompressing tablespace %s from %s to %s",
                    tablespace.name,
                    tablespace_src_path,
                    tablespace_dst_path,
                )
                self._prepare_directory(
                    tablespace_dst_path, delete_if_exists=(not is_last_operation)
                )
                cmd_output = compressor.decompress(
                    tablespace_src_path, tablespace_dst_path
                )
                output.debug(
                    "Decompression output for tablespace %s: %s",
                    tablespace.name,
                    cmd_output,
                )

        base_file = "%s.%s" % (self.BASE_TARBALL_NAME, compressor.file_extension)
        base_src_path = "%s/%s" % (backup_info.get_data_directory(), base_file)
        output.debug(
            "Decompressing base tarball from %s to %s.", base_src_path, destination
        )
        cmd_output = compressor.decompress(
            base_src_path, destination, exclude=["recovery.conf", "tablespace_map"]
        )
        output.debug("Decompression output for base tarball: %s", cmd_output)

        # If it's not the last operation we also copy the backup manifest to the next
        # staging area, as likely a combine operation is going to need it
        if not is_last_operation:
            try:
                self.cmd.copy(
                    backup_info.get_backup_manifest_path(),
                    vol_backup_info.get_backup_manifest_path(),
                )
            except FsOperationFailed as ex:
                output.error(
                    "Failed to copy backup manifest from %s to %s: %s",
                    backup_info.get_backup_manifest_path(),
                    vol_backup_info.get_backup_manifest_path(),
                    ex,
                )
                output.close_and_exit()

        # Create the tablespaces symbolic links in the destination directory
        self._link_tablespaces(
            vol_backup_info, destination, tablespaces, is_last_operation
        )

        return vol_backup_info


class CombineOperation(RecoveryOperation):
    """
    Operation responsible for combining a chain of backups (full + incrementals).

    :cvar NAME: the name of the operation
    """

    NAME = "barman-combine"

    def _execute(
        self,
        backup_info,
        destination,
        tablespaces,
        remote_command,
        recovery_info,
        safe_horizon,
        is_last_operation,
    ):
        return self._combine_backups(
            backup_info, destination, tablespaces, remote_command, is_last_operation
        )

    def _should_execute(self, backup_info):
        """
        Check if the combine operation should be executed on the given backup.

        .. note::
            This method is a no-op in the case of incremental backups as only the leaf
            backup in the chain will execute this operation (i.e., this class does not
            use :meth:`_execute_on_chain`). It's implemented only to satisfy the
            interface of the base class :class:`RecoveryOperation`.

        :param barman.infofile.LocalBackupInfo backup_info: The backup info to check
        :raises NotImplementedError: This method is not applicable for incremental
            backups.
        """
        raise NotImplementedError(
            "CombineOperation is executed only on the leaf backup of the chain"
        )

    def _combine_backups(
        self,
        backup_info,
        destination,
        tablespaces,
        remote_command,
        is_last_operation,
    ):
        """
        Perform the combination of incrementals + full backups to the destination.

        When this is the last operation in the recovery chain, the output of the
        combination will be placed in the root of *destination* and tablespaces mapped
        to their final destination, honoring relocation, if requested. Otherwise, it
        will be placed in the data directory of the respective volatile backup i.e.
        ``*destination*/<backup_id>/data``, ``*destination*/<backup_id>/<tbspc>``, etc.

        :param barman.infofile.LocalBackupInfo backup_info: The incremental backup
            to combine with its ascendants
        :param str destination: The directory where the combined backup will be placed
        :param dict[str,str]|None tablespaces: A dictionary mapping tablespace names to
            their target directories. This is the relocation chosen by the user when
            invoking the ``restore`` command. If ``None``, it means no relocation was
            chosen
        :param str|None remote_command: The SSH remote command to use for the recovery,
            in case of a remote recovery. If ``None``, it means the recovery is local
        :param bool is_last_operation: Whether this is the last operation in the
            recovery chain
        :return barman.infofile.VolatileBackupInfo: The respective volatile backup info
            of *backup_info* which reflects all changes performed by the operation
        """
        combine_start_time = datetime.datetime.now()

        # Create a volatile backup info with the destination as its base directory
        vol_backup_info = self._create_volatile_backup_info(backup_info, destination)

        # If this is the last operation then the root of *destination* is where
        # the output of pg_combinebackup will be placed.
        # Otherwise, it means this is still an intermediate step, so the output
        # must be in the data directory of the volatile backup info, as this backup
        # will still be referenced by following operations in the chain. This way
        # we maintain a consistent structure among volatile backups
        output_dest = vol_backup_info.get_data_directory()
        if is_last_operation:
            output_dest = destination

        # Get the tablespace mapping for the combine operation. If this is the last
        # operation, they are mapped to their final destination, otherwise they
        # are mapped to the volatile backup's directory
        tablespace_mapping = self._get_tablespace_mapping(
            backup_info, vol_backup_info, tablespaces, is_last_operation
        )

        # Prepare the destination directories (PGDATA and tablespaces)
        # Only include tablespace directories that are not subdirectories of output_dest,
        # to avoid preparing the same directory multiple times.
        # No need to attempt deleting the directory if it's the last operation as it's
        # sure to be either empty or non-existing in that case
        self._prepare_directory(output_dest, delete_if_exists=(not is_last_operation))
        for tbspc_dest in tablespace_mapping.values():
            if not is_subdirectory(output_dest, tbspc_dest):
                self._prepare_directory(
                    tbspc_dest, delete_if_exists=(not is_last_operation)
                )

        output.info(
            "Start combining backup via pg_combinebackup for backup %s on %s",
            backup_info.backup_id,
            output_dest,
        )

        # Do the actual combine
        self._run_pg_combinebackup(
            backup_info, output_dest, tablespace_mapping, remote_command
        )

        output.info(
            "End combining backup via pg_combinebackup for backup %s",
            backup_info.backup_id,
        )

        # Set copy stats
        # We set them on backup_info instead of vol_backup_info because
        # vol_backup_info is never saved to disk, backup_info is what will be saved
        # in the destination directory as .barman-recover.info
        combine_end_time = datetime.datetime.now()
        combine_time = total_seconds(combine_end_time - combine_start_time)
        backup_info.copy_stats = {"combine_time": combine_time}

        # If the checksum configuration is not consistent among all backups in the
        # chain, raise a warning at the end so the user can optionally take
        # action about it
        if not vol_backup_info.is_checksum_consistent():
            output.warning(
                "You are restoring from an incremental backup where checksums were "
                "enabled on that backup, but not all backups in the chain. It is "
                "advised to disable, and optionally re-enable, checksums on the "
                "destination directory to avoid failures."
            )

        # Remove unwanted files from the destination
        if is_last_operation:
            self._post_recovery_cleanup(output_dest)

        return vol_backup_info

    def _run_pg_combinebackup(
        self,
        backup_info,
        output_dest,
        tablespace_mapping,
        remote_command,
    ):
        """
        Run the ``pg_combinebackup`` utility to combine backup chain.

        If this operation is running on a remote server, it will execute it
        directly on the remote node. Otherwise, it is run locally.

        :param barman.infofile.LocalBackupInfo backup_info: The incremental backup
            to combine with its ascendants
        :param str output_dest: The destination directory where the combined backup
            will be placed
        :param dict[str,str] tablespace_mapping: A mapping of source tablespace
            directories to their destination directories
        :param str|None remote_command: The SSH remote command to use for the recovery,
            in case of a remote recovery. If ``None``, it means the recovery is local
        :raises barman.exceptions.DataTransferFailure: If the combine operation fails
        """
        # Retrieve pg_combinebackup information
        remote_status = self._fetch_remote_status()

        # Get the backup chain data paths to be passed to pg_combinebackup
        backups_chain = self._get_backup_chain_paths(backup_info)

        pg_combinebackup_major_version = get_major_version(
            str(remote_status["pg_combinebackup_version"])
        )
        backup_pg_major_version = backup_info.pg_major_version()

        if pg_combinebackup_major_version != backup_pg_major_version:
            output.error(
                "Postgres version mismatch: The backup '%s' was taken from Postgres "
                "version '%s', but pg_combinebackup version is '%s'. To restore "
                "successfully is necessary to use pg_combinebackup with the same "
                "version used when the backup was taken (%s).",
                backup_info.backup_id,
                backup_pg_major_version,
                pg_combinebackup_major_version,
                backup_pg_major_version,
            )
            output.close_and_exit()

        if self.config.combine_mode == "link":
            self.config.combine_mode = self._fallback_to_copy_if_link_is_not_supported(
                backup_info, output_dest, remote_command, pg_combinebackup_major_version
            )
        # Prepare the pg_combinebackup command
        # We skip checking paths as we already did it in _fetch_remote_status(). Also,
        # it can cause errors if staging_location = remote, as it only checks locally
        pg_combinebackup = PgCombineBackup(
            destination=output_dest,
            copy_mode=self.config.combine_mode,
            command=remote_status["pg_combinebackup_path"],
            version=remote_status["pg_combinebackup_version"],
            app_name=None,
            tbs_mapping=tablespace_mapping,
            out_handler=PgCombineBackup.make_logging_handler(logging.INFO),
            args=backups_chain,
            skip_path_check=True,
        )

        try:
            # If staging_location is remote, we build a remote command instance
            # with the same parameters as the PgCombineBackup instance and execute
            # the command using the same argument list
            # Note: We need to build a new Command here because PgCombineBackup (and
            # similar classes) are not designed for remote execution. As a workaround,
            # we essentially reconstruct the same command, but with the SSH command prepended.
            if self.config.staging_location == "remote":
                remote_cmd = Command(
                    remote_command,
                    shell=True,  # use the shell instead of an "execve" call
                    check=True,  # raise CommandFailedException on failure
                    path=self.server.path,
                    out_handler=Command.make_logging_handler(logging.INFO),
                )
                remote_cmd(
                    full_command_quote(pg_combinebackup.cmd, pg_combinebackup.args)
                )
            # Otherwise, if staging_location is local, just run the command as is
            else:
                pg_combinebackup()
        except CommandFailedException as e:
            msg = "Combine action failure on directory '%s'" % output_dest
            raise DataTransferFailure.from_command_error("pg_combinebackup", e, msg)

    def _get_tablespace_mapping(
        self, backup_info, vol_backup_info, tablespaces, is_last_operation
    ):
        """
        Get the mapping of tablespaces from the source to their destination directories.

        If this is the last operation, tablespaces are mapped to their final
        destination, honoring relocation, if requested. Otherwise, they are mapped to
        their directory in the volatile backup's directory (in the staging directory).

        :param barman.infofile.LocalBackupInfo backup_info: The backup info to process
        :param barman.infofile.VolatileBackupInfo vol_backup_info: The equivalent
            volatile backup info of the backup being processed by the operation
        :param dict[str,str]|None tablespaces: A dictionary mapping tablespace names to
            their target directories. This is the relocation chosen by the user when
            invoking the ``restore`` command. If ``None``, it means no relocation was
            chosen
        :param bool is_last_operation: Whether this is the last operation in the
            recovery chain
        :return dict[str,str]: A mapping of source tablespace directories to their
            destination directories
        """
        tbspc_mapping = {}
        if backup_info.tablespaces:
            for tablespace in backup_info.tablespaces:
                source = backup_info.get_data_directory(tablespace.oid)
                if is_last_operation:
                    if tablespaces and tablespace.name in tablespaces:
                        destination = tablespaces[tablespace.name]
                    else:
                        destination = tablespace.location
                else:
                    destination = vol_backup_info.get_data_directory(tablespace.oid)
                tbspc_mapping[source] = destination

        return tbspc_mapping

    def _get_backup_chain_paths(self, backup_info):
        """
        Get the path of each backup in the chain, from the full backup to
        the specified incremental backup.

        :param barman.infofile.LocalBackupInfo backup_info: The incremental backup
        :return Iterator[barman.infofile.LocalBackupInfo]: iterator of paths of
            the backups in the chain, going from the full to the incremental backup
            pointed by *backup_info*
        """
        return reversed(
            [backup.get_data_directory() for backup in backup_info.walk_to_root()]
        )

    def _fetch_remote_status(self):
        """
        Gather info from the remote server.

        Info includes the path to the ``pg_combinebackup`` client and its version.

        If the staging location is ``remote``, it attempts to find ``pg_combinebackup``
        in the remote server. If the staging location is ``local``, it attempts to find
        ``pg_combinebackup`` in the Barman host.

        :return dict[str, str|bool]: the pg_combinebackup client information
            of the remote server
        :raises barman.exceptions.CommandNotFoundException: if ``pg_combinebackup``
            it not found on the target server
        """
        remote_status = dict.fromkeys(
            (
                "pg_combinebackup_path",
                "pg_combinebackup_version",
            ),
            None,
        )

        full_path = None
        full_version = None
        if self.config.staging_location == "remote":
            full_path = self.cmd.find_command(PgCombineBackup.COMMAND_ALTERNATIVES)
            if full_path:
                full_version = self.cmd.get_command_version(full_path)
        else:
            version_info = PgCombineBackup.get_version_info(self.server.path)
            full_path = version_info["full_path"]
            full_version = version_info["full_version"]

        if not full_path:
            raise CommandNotFoundException(
                "pg_combinebackup could not be found on the target server"
            )

        remote_status["pg_combinebackup_path"] = full_path
        remote_status["pg_combinebackup_version"] = full_version
        return remote_status

    def _fallback_to_copy_if_link_is_not_supported(
        self, backup_info, output_dest, remote_command, pg_combinebackup_major_version
    ):
        """
        Fallback method to ``copy`` if link mode cannot be used for restoring a backup.

        This function will fall back to the `copy` mode if any of the following
        conditions are met:

        * The restore is performed locally (not a remote command), which can corrupt
          the backup(s) in the catalog.
        * The remote ``pg_combinebackup`` version is older than 18, where ``--link`` is
          not available.
        * The backup source and restore destination are on different filesystems, in
          which case hard-links cannot be created.

        .. note::
            A non-fatal warning is issued if the restore involves a local staging area
            that is linked to the backup catalog, as this still carries a risk of
            backup corruption if the staging area is not cleaned up properly. Barman
            does not modify those files, but unexpected situations can happen:

            * The user somehow modifies the files before Barman is able to copy them to
              the remote and delete from the staging area.
            * Barman faces an unexpected error and quits execution before cleaning up
              the staging area. The user would be able to modify the files in that case.

        :param barman.infofile.BackupInfo backup_info: The backup being checked.
        :param str output_dest: The destination path where the backup will be restored.
        :param str|None remote_command:  The remote command used for restore, or None if
            the restore is local.
        :param str pg_combinebackup_major_version: 'pg_combinebackup' client major version.
        :returns: ``link`` if link mode can be used, ``copy`` otherwise.
        :rtype: str
        """
        copy_mode = "link"
        if not remote_command:
            output.warning(
                "'link' mode is not supported for local restore. Falling back to "
                "'copy' mode to prevent modification of original backup files through "
                "hard-links."
            )
            copy_mode = "copy"
        elif Version(pg_combinebackup_major_version) < Version("18"):
            output.warning(
                "'link' mode is not supported on Postgres 17 or older. Falling back "
                "to 'copy' mode."
            )
            copy_mode = "copy"
        elif self.cmd.get_path_device_number(
            output_dest
        ) != self.cmd.get_path_device_number(backup_info.get_data_directory()):
            output.warning(
                "'link' mode is not supported with files across different file "
                "systems. Falling back to 'copy' mode."
            )
            copy_mode = "copy"
        elif self.config.staging_location == "local" and any(
            [
                b.get_base_directory() == self.config.basebackups_directory
                for b in backup_info.walk_to_root()
            ]
        ):
            output.warning(
                "CAUTION: Using 'link' mode with a local staging area. Files in the "
                "staging path may be hard-linked to files from your Barman backup "
                "catalog. Modifying these files, by any means, can lead to permanent "
                "backup(s) corruption."
            )
        return copy_mode


class MainRecoveryExecutor(RemoteConfigRecoveryExecutor):
    """
    Main recovery executor.

    This executor is used to deal with more complex recovery scenarios, which require
    one or more among these operations: backup decryption, backup decompression, or
    backup combination.
    """

    def _build_operations_pipeline(self, backup_info, remote_command=None):
        """
        Build a list of required operations to be executed on the target backup.

        This method ensures that all required operations are included and in their
        correct order.

        :param barman.infofile.LocalBackupInfo backup_info: The backup info to process
        :param str|None remote_command: The SSH remote command to use for the recovery,
            in case of a remote recovery. If ``None``, it means the recovery is local
        :return list[RecoveryOperation]: A list of operations to be executed in order
        """
        operations = []

        backup_chain = list(backup_info.walk_to_root())

        any_encrypted = any([b.encryption is not None for b in backup_chain])
        if any_encrypted:
            operations.append(
                DecryptOperation(self.config, self.server, self.backup_manager)
            )

        any_compressed = any([b.compression is not None for b in backup_chain])
        if any_compressed:
            operations.append(
                DecompressOperation(self.config, self.server, self.backup_manager)
            )

        if backup_info.is_incremental:
            operations.append(
                CombineOperation(self.config, self.server, self.backup_manager)
            )

        if remote_command:
            if self.config.staging_location == "local":
                operations.append(
                    RsyncCopyOperation(self.config, self.server, self.backup_manager)
                )
            elif self.config.staging_location == "remote":
                # If the staging location is remote, the copy operation must be the
                # first one to be executed, only after the decryption
                index = 1 if any_encrypted else 0
                operations.insert(
                    index,
                    RsyncCopyOperation(self.config, self.server, self.backup_manager),
                )

        if not operations:
            # If no operations were required, it means it is a local recovery of a plain
            # non-encrypted non-compressed backup. In such case, we still need to copy
            # the backup to the destination directory as no operation will do it
            operations.append(
                RsyncCopyOperation(self.config, self.server, self.backup_manager)
            )

        return operations

    # TODO: Remove this method once the deprecated options staging_path and
    # recovery_staging_path are removed
    @contextmanager
    def _handle_deprecated_staging_options(self, operation, remote_command):
        """
        Context manager to handle the deprecated staging options ``local_staging_path``
        and ``recovery_staging_path``.

        This context manager temporarily maps ``local_staging_path`` and
        ``recovery_staging_path`` to ``staging_path``. ``staging_location`` is always
        set to "local" when using the deprecated options to guarantee a consistent
        use of the staging area without adding extra complexity.

        This ensures that, even if using deprecated options, operations are still able
        to rely only on the new options, without having to handle multiple scenarios
        themselves.

        Once out of this context manager, the original values are restored in the
        configuration object.

        :param RecoveryOperation operation: The operation to be executed
        :param str|None remote_command: The SSH remote command to use for the recovery,
            in case of a remote recovery. If ``None``, it means the recovery is local
        """
        # If staging_path and staging_location are set, it means it is already using the
        # new options, so there's nothing to handle
        if operation.config.staging_path and operation.config.staging_location:
            yield
            return

        original_staging_path = operation.config.staging_path
        original_staging_location = operation.config.staging_location

        # When using the deprecated options, we set staging_path according to the
        # operation and staging_location to "local", as it is the safest way to guarantee
        # a consistent behavior accross all operations
        if isinstance(operation, DecompressOperation):
            operation.config.staging_path = operation.config.recovery_staging_path
        else:
            operation.config.staging_path = operation.config.local_staging_path

        operation.config.staging_location = "local"

        yield

        operation.config.staging_path = original_staging_path
        operation.config.staging_location = original_staging_location

    def _backup_copy(
        self,
        backup_info,
        dest,
        tablespaces=None,
        remote_command=None,
        safe_horizon=None,
        recovery_info=None,
    ):
        """
        Perform the backup copy operation.

        This method orchestrates the execution of all operations required to
        successfully copy the contents of a backup, including decryption, decompression,
        and combination of incremental backups.

        :param barman.infofile.LocalBackupInfo backup_info: The backup info to process
        :param str dest: The destination directory for the recovery
        :param dict[str,str]|None tablespaces: A dictionary mapping tablespace names to
            their target directories. This is the relocation chosen by the user when
            invoking the ``restore`` command. If ``None``, it means no relocation was
            chosen
        :param str|None remote_command: The SSH remote command to use for the recovery,
            in case of a remote recovery. If ``None``, it means the recovery is local
        :param dict[str,any] recovery_info: A dictionary that populated with metadata
            about the recovery process
        :param datetime.datetime|None safe_horizon: The safe horizon of the backup.
            Any file rsync-copied after this time has to be checked with checksum
        :return barman.infofile.VolatileBackupInfo: The volatile backup info of the
            final backup after all operations have been executed.
        """
        operations = self._build_operations_pipeline(backup_info, remote_command)
        for n, operation in enumerate(operations, start=1):
            with self._handle_deprecated_staging_options(operation, remote_command):
                destination = dest
                is_last_operation = n == len(operations)
                if not is_last_operation:
                    destination = operation.staging_path
                # Execute the operation on the current backup. Each operation returns a
                # VolatileBackupInfo that reflects all changes made by that operation.
                # The output of one operation is passed as input to the next.
                # Each operation is responsible for handling traversal and execution
                # on all parent backups (for incremental backups), so we don't need
                # to manually walk the backup chain here.
                output.debug("Executing operation %s: %s", n, operation.NAME)
                backup_info = operation.execute(
                    backup_info=backup_info,
                    destination=destination,
                    tablespaces=tablespaces,
                    recovery_info=recovery_info,
                    remote_command=remote_command,
                    safe_horizon=safe_horizon,
                    is_last_operation=is_last_operation,
                )

                # Cleanup staging directory of the previous operation, so skip the first
                # operation.
                if n > 1:
                    previous_op = operations[n - 2]  # n is 1-based, but list is 0-based
                    previous_op.cleanup_staging_dir()


def recovery_executor_factory(backup_manager, backup_info):
    """
    Helper function to create an appropriate recovery executor based on the backup type.

    :param barman.backup.BackupManager backup_manager: The backup manager instance
    :param barman.infofile.LocalBackupInfo backup_info: The backup info to restore
    :return barman.recovery_executor.RecoveryExecutor: An instance of the appropriate
        recovery executor
    """
    if backup_info.snapshots_info is not None:
        return SnapshotRecoveryExecutor(backup_manager)
    return MainRecoveryExecutor(backup_manager)


class ConfigurationFileMangeler:
    # List of options that, if present, need to be forced to a specific value
    # during recovery, to avoid data losses
    OPTIONS_TO_MANGLE = {
        # Dangerous options
        "archive_command": "false",
        # Recovery options that may interfere with recovery targets
        "recovery_target": None,
        "recovery_target_name": None,
        "recovery_target_time": None,
        "recovery_target_xid": None,
        "recovery_target_lsn": None,
        "recovery_target_inclusive": None,
        "recovery_target_timeline": None,
        "recovery_target_action": None,
    }

    def mangle_options(self, filename, backup_filename=None, append_lines=None):
        """
        This method modifies the given PostgreSQL configuration file,
        commenting out the given settings, and adding the ones generated by
        Barman.

        If backup_filename is passed, keep a backup copy.

        :param filename: the PostgreSQL configuration file
        :param backup_filename: config file backup copy. Default is None.
        :param append_lines: Additional lines to add to the config file
        :return: [Assertion]
        """
        # Read the full content of the file in memory
        with open(filename, "rb") as f:
            content = f.readlines()

        # Rename the original file to backup_filename or to a temporary name
        # if backup_filename is missing. We need to keep it to preserve
        # permissions.
        if backup_filename:
            orig_filename = backup_filename
        else:
            orig_filename = "%s.config_mangle.old" % filename
        shutil.move(filename, orig_filename)

        # Write the mangled content
        mangled = []
        with open(filename, "wb") as f:
            last_line = None
            for l_number, line in enumerate(content):
                rm = PG_CONF_SETTING_RE.match(line.decode("utf-8"))
                if rm:
                    key = rm.group(1)
                    if key in self.OPTIONS_TO_MANGLE:
                        value = self.OPTIONS_TO_MANGLE[key]
                        f.write("#BARMAN#".encode("utf-8") + line)
                        # If value is None, simply comment the old line
                        if value is not None:
                            changes = "%s = %s\n" % (key, value)
                            f.write(changes.encode("utf-8"))
                        mangled.append(
                            Assertion._make(
                                [os.path.basename(f.name), l_number, key, value]
                            )
                        )
                        continue
                last_line = line
                f.write(line)
            # Append content of append_lines array
            if append_lines:
                # Ensure we have end of line character at the end of the file before adding new lines
                if last_line and last_line[-1] != "\n".encode("utf-8"):
                    f.write("\n".encode("utf-8"))
                f.write(("\n".join(append_lines) + "\n").encode("utf-8"))

        # Restore original permissions
        shutil.copymode(orig_filename, filename)

        # If a backup copy of the file is not requested,
        # unlink the orig file
        if not backup_filename:
            os.unlink(orig_filename)

        return mangled


class ConfigIssueDetection:
    # Potentially dangerous options list, which need to be revised by the user
    # after a recovery
    DANGEROUS_OPTIONS = [
        "data_directory",
        "config_file",
        "hba_file",
        "ident_file",
        "external_pid_file",
        "ssl_cert_file",
        "ssl_key_file",
        "ssl_ca_file",
        "ssl_crl_file",
        "unix_socket_directory",
        "unix_socket_directories",
        "include",
        "include_dir",
        "include_if_exists",
    ]

    def detect_issues(self, filename):
        """
        This method looks for any possible issue with PostgreSQL
        location options such as data_directory, config_file, etc.
        It returns a dictionary with the dangerous options that
        have been found.

        :param filename str: the Postgres configuration file
        :return: clashes [Assertion]
        """

        clashes = []

        with open(filename) as f:
            content = f.readlines()

        # Read line by line and identify dangerous options
        for l_number, line in enumerate(content):
            rm = PG_CONF_SETTING_RE.match(line)
            if rm:
                key = rm.group(1)
                if key in self.DANGEROUS_OPTIONS:
                    clashes.append(
                        Assertion._make(
                            [os.path.basename(f.name), l_number, key, rm.group(2)]
                        )
                    )

        return clashes
