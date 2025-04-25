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
import logging
import os
import re
import shutil
import socket
import tempfile
from functools import partial
from io import BytesIO

import dateutil.parser
import dateutil.tz

import barman.fs as fs
from barman import output, xlog
from barman.cloud_providers import get_snapshot_interface_from_backup_info
from barman.command_wrappers import PgCombineBackup, RsyncPgData
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
    DataTransferFailure,
    FsOperationFailed,
    RecoveryInvalidTargetException,
    RecoveryPreconditionException,
    RecoveryStandbyModeException,
    RecoveryTargetActionException,
    SnapshotBackupException,
)
from barman.infofile import BackupInfo, LocalBackupInfo, SyntheticBackupInfo
from barman.utils import force_str, mkpath, parse_target_tli, total_seconds

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
        """

        # Run the cron to be sure the wal catalog is up to date
        # Prepare a map that contains all the objects required for a recovery
        recovery_info = self._setup(
            backup_info, remote_command, dest, recovery_conf_filename
        )

        passphrase = None
        if self.config.encryption_passphrase_command:
            output.info(
                "The 'encryption_passphrase_command' setting is present in your "
                "configuration. This implies that the catalog contains encrypted "
                "backup or WAL files. The private key will be retrieved to perform "
                "decryption as needed."
            )

            passphrase = get_passphrase_from_command(
                self.config.encryption_passphrase_command
            )

        # If the backup is encrypted, it consists of tarballs (Barman only supports
        # encryption of tarball based backups for now).
        # Decrypt as the first step to prepare the backup, then begin the recovery
        # process.
        if backup_info.encryption:
            if passphrase is None:
                output.error(
                    "Encrypted backup '%s' was found for server '%s', but "
                    "'encryption_passphrase_command' is not configured. Please "
                    "configure it before attempting a restore.",
                    backup_info.backup_id,
                    self.server.config.name,
                )
                output.close_and_exit()

            output.debug("Encrypted backup '%s' detected.", backup_info.backup_id)
            output.info(
                "Decrypting files from backup '%s' for server '%s'.",
                backup_info.backup_id,
                self.server.config.name,
            )

            # Create local staging path if not exist. Ignore if it does exist.
            os.makedirs(self.config.local_staging_path, mode=0o700, exist_ok=True)

            self._decrypt_backup(
                backup_info=backup_info,
                passphrase=passphrase,
                recovery_info=recovery_info,
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

        # check destination directory. If doesn't exist create it
        try:
            recovery_info["cmd"].create_dir_if_not_exists(dest, mode="700")
        except FsOperationFailed as e:
            output.error("unable to initialise destination directory '%s': %s", dest, e)
            output.close_and_exit()

        # Initialize tablespace directories
        if backup_info.tablespaces:
            self._prepare_tablespaces(
                backup_info, recovery_info["cmd"], dest, tablespaces
            )
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

        # We are not using the default interface for deletion of temporary
        # files (AKA self.tmp_dirs) because we want to perform an early
        # cleanup of the decryped backups, thus do not hold it using disk
        # space for longer than necessary.
        if recovery_info.get("decryption_dest") is not None:
            fs.LocalLibPathDeletionCommand(recovery_info["decryption_dest"]).delete()

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
                    required_xlog_files,
                    recovery_info["wal_dest"],
                    remote_command,
                    passphrase,
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

        # At this point, the encryption passphrase is not needed anymore, so we dispose
        # it from memory to avoid lingering. See the security note in the GPG command
        # class.
        if passphrase:
            passphrase[:] = b"\x00" * len(passphrase)

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

    def _setup(self, backup_info, remote_command, dest, recovery_conf_filename):
        """
        Prepare the recovery_info dictionary for the recovery, as well
        as temporary working directory

        :param barman.infofile.LocalBackupInfo backup_info: representation of a
            backup
        :param str remote_command: ssh command for remote connection
        :param str|None recovery_conf_filename: filename for storing recovery configurations
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
            "decryption_dest": None,
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

    def _prepare_tablespaces(self, backup_info, cmd, dest, tablespaces):
        """
        Prepare the directory structure for required tablespaces,
        taking care of tablespaces relocation, if requested.

        :param barman.infofile.LocalBackupInfo backup_info: backup
            representation
        :param barman.fs.UnixLocalCommand cmd: Object for
            filesystem interaction
        :param str dest: destination dir for the recovery
        :param dict tablespaces: dict of all the tablespaces and their location
        """
        tblspc_dir = os.path.join(dest, "pg_tblspc")
        try:
            # check for pg_tblspc dir into recovery destination folder.
            # if it does not exists, create it
            cmd.create_dir_if_not_exists(tblspc_dir)
        except FsOperationFailed as e:
            output.error(
                "unable to initialise tablespace directory '%s': %s", tblspc_dir, e
            )
            output.close_and_exit()
        for item in backup_info.tablespaces:
            # build the filename of the link under pg_tblspc directory
            pg_tblspc_file = os.path.join(tblspc_dir, str(item.oid))

            # by default a tablespace goes in the same location where
            # it was on the source server when the backup was taken
            location = item.location

            # if a relocation has been requested for this tablespace,
            # use the target directory provided by the user
            if tablespaces and item.name in tablespaces:
                location = tablespaces[item.name]

            try:
                # remove the current link in pg_tblspc, if it exists
                cmd.delete_if_exists(pg_tblspc_file)
                # create tablespace location, if does not exist
                # (raise an exception if it is not possible)
                cmd.create_dir_if_not_exists(location)
                # check for write permissions on destination directory
                cmd.check_write_permission(location)
                # create symlink between tablespace and recovery folder
                cmd.create_symbolic_link(location, pg_tblspc_file)
            except FsOperationFailed as e:
                output.error(
                    "unable to prepare '%s' tablespace (destination '%s'): %s",
                    item.name,
                    location,
                    e,
                )
                output.close_and_exit()
            output.info("\t%s, %s, %s", item.oid, item.name, location)

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
                if location.startswith(dest):
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

    def _xlog_copy(self, required_xlog_files, wal_dest, remote_command, passphrase):
        """
        Restore WAL segments

        :param required_xlog_files: list of all required WAL files
        :param wal_dest: the destination directory for xlog recover
        :param remote_command: default None. The remote command to recover
               the xlog, in case of remote backup.
        :param bytearray passphrase: UTF-8 encoded version of passphrase.
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
        if passphrase is None and encryptions:
            output.error(
                "Encrypted WALs were found for server '%s', but "
                "'encryption_passphrase_command' is not configured. Please configure "
                "it before attempting a restore.",
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
                recovery_conf_lines.append(
                    "restore_command = 'barman-wal-restore %s -U %s "
                    "%s %s %%f %%p'"
                    % (partial_option, self.config.config.user, fqdn, self.config.name)
                )
            else:
                recovery_conf_lines.append(
                    "# The 'barman get-wal' command "
                    "must run as '%s' user" % self.config.config.user
                )
                recovery_conf_lines.append(
                    "restore_command = 'sudo -u %s "
                    "barman get-wal %s %s %%f > %%p'"
                    % (self.config.config.user, partial_option, self.config.name)
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

    def _decrypt_backup(self, backup_info, passphrase, recovery_info):
        """
        Decrypt the given backup into the local staging path.

        :param barman.infofile.LocalBackupInfo backup_info: the backup to be decrypted.
        :param bytearray passphrase: the passphrase for decrypting the backup.
        :param dict recovery_info: Dictionary of recovery information.
        """
        tempdir = tempfile.mkdtemp(
            prefix="barman-decryption-", dir=self.config.local_staging_path
        )
        encryption_manager = self.backup_manager.encryption_manager
        encryption_handler = encryption_manager.get_encryption(backup_info.encryption)

        for backup_file in backup_info.get_list_of_files("data"):
            # We "reconstruct" the "original backup" in the staging path. Encrypted
            # files are decrypted, while unencrypted files are copied as-is.
            if backup_file.endswith(".gpg"):
                output.debug("Decrypting file %s at %s" % (backup_file, tempdir))
                _ = encryption_handler.decrypt(
                    file=backup_file, dest=tempdir, passphrase=passphrase
                )
            else:
                shutil.copy2(backup_file, tempdir)
        # Store `tempdir` in the recovery_info dict so that the `_backup_copy`
        # method knows the backup was encrypted and where to copy the decrypted backup
        # from.
        recovery_info["decryption_dest"] = tempdir


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


class TarballRecoveryExecutor(RemoteConfigRecoveryExecutor):
    """
    A specialised recovery method for compressed backups.
    Inheritence is not necessarily the best thing here since the two RecoveryExecutor
    classes only differ by this one method, and the same will be true for future
    RecoveryExecutors (i.e. ones which handle encryption).
    Nevertheless for a wip "make it work" effort this will do.
    """

    BASE_TARBALL_NAME = "base"

    def __init__(self, backup_manager, compression):
        """
        Constructor

        :param barman.backup.BackupManager backup_manager: the BackupManager
            owner of the executor
        :param compression Compression.
        """
        super(TarballRecoveryExecutor, self).__init__(backup_manager)
        self.compression = compression

    def _backup_copy(
        self,
        backup_info,
        dest,
        tablespaces=None,
        remote_command=None,
        safe_horizon=None,
        recovery_info=None,
    ):
        # Set a ':' prefix to remote destinations
        dest_prefix = ""
        if remote_command:
            dest_prefix = ":"

        # Instead of adding the `data` directory and `tablespaces` to a copy
        # controller we instead want to copy just the tarballs to a staging
        # location via the copy controller and then untar into place.

        # Create the staging area
        staging_dir = os.path.join(
            self.config.recovery_staging_path,
            "barman-staging-{}-{}".format(self.config.name, backup_info.backup_id),
        )
        output.info(
            "Staging compressed backup files on the recovery host in: %s", staging_dir
        )

        recovery_info["cmd"].create_dir_if_not_exists(staging_dir, mode="700")
        recovery_info["cmd"].validate_file_mode(staging_dir, mode="700")
        recovery_info["staging_dir"] = staging_dir
        self.temp_dirs.append(
            fs.UnixCommandPathDeletionCommand(staging_dir, recovery_info["cmd"])
        )

        # If the backup is encrypted in the Barman catalog, at this point it's already
        # decrypted in `decryption_dest` and we can use it as the source for the copy.
        # If the backup is not encrypted in the Barman catalog, we can simply use its
        # path in the catalog as the source.
        backup_data_dir = (
            recovery_info["decryption_dest"]
            if recovery_info.get("decryption_dest") is not None
            else backup_info.get_data_directory()
        )

        # Create the copy controller object, specific for rsync.
        # Network compression is always disabled because we are copying
        # data which has already been compressed.
        controller = RsyncCopyController(
            path=self.server.path,
            ssh_command=remote_command,
            network_compression=False,
            retry_times=self.config.basebackup_retry_times,
            retry_sleep=self.config.basebackup_retry_sleep,
            workers=self.config.parallel_jobs,
            workers_start_batch_period=self.config.parallel_jobs_start_batch_period,
            workers_start_batch_size=self.config.parallel_jobs_start_batch_size,
        )

        # Add the tarballs to the controller
        if backup_info.tablespaces:
            for tablespace in backup_info.tablespaces:
                tablespace_file = "%s.%s" % (
                    tablespace.oid,
                    self.compression.file_extension,
                )
                tablespace_path = "%s/%s" % (
                    backup_data_dir,
                    tablespace_file,
                )
                controller.add_file(
                    label=tablespace.name,
                    src=tablespace_path,
                    dst="%s/%s" % (dest_prefix + staging_dir, tablespace_file),
                    item_class=controller.TABLESPACE_CLASS,
                    bwlimit=self.config.get_bwlimit(tablespace),
                )
        base_file = "%s.%s" % (self.BASE_TARBALL_NAME, self.compression.file_extension)
        base_path = "%s/%s" % (
            backup_data_dir,
            base_file,
        )
        controller.add_file(
            label="pgdata",
            src=base_path,
            dst="%s/%s" % (dest_prefix + staging_dir, base_file),
            item_class=controller.PGDATA_CLASS,
            bwlimit=self.config.get_bwlimit(),
        )
        controller.add_file(
            label="pgdata",
            src=os.path.join(backup_data_dir, "backup_manifest"),
            dst=os.path.join(dest_prefix + dest, "backup_manifest"),
            item_class=controller.PGDATA_CLASS,
            bwlimit=self.config.get_bwlimit(),
        )

        # Execute the copy
        try:
            controller.copy()
        except CommandFailedException as e:
            msg = "data transfer failure"
            raise DataTransferFailure.from_command_error("rsync", e, msg)

        # Untar the results files to their intended location
        if backup_info.tablespaces:
            for tablespace in backup_info.tablespaces:
                # By default a tablespace goes in the same location where
                # it was on the source server when the backup was taken
                tablespace_dst_path = tablespace.location
                # If a relocation has been requested for this tablespace
                # use the user provided target directory
                if tablespaces and tablespace.name in tablespaces:
                    tablespace_dst_path = tablespaces[tablespace.name]
                tablespace_file = "%s.%s" % (
                    tablespace.oid,
                    self.compression.file_extension,
                )
                tablespace_src_path = "%s/%s" % (staging_dir, tablespace_file)
                _logger.debug(
                    "Uncompressing tablespace %s from %s to %s",
                    tablespace.name,
                    tablespace_src_path,
                    tablespace_dst_path,
                )
                cmd_output = self.compression.uncompress(
                    tablespace_src_path, tablespace_dst_path
                )
                _logger.debug(
                    "Uncompression output for tablespace %s: %s",
                    tablespace.name,
                    cmd_output,
                )
        base_src_path = "%s/%s" % (staging_dir, base_file)
        _logger.debug("Uncompressing base tarball from %s to %s.", base_src_path, dest)
        cmd_output = self.compression.uncompress(
            base_src_path, dest, exclude=["recovery.conf", "tablespace_map"]
        )
        _logger.debug("Uncompression output for base tarball: %s", cmd_output)


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


class IncrementalRecoveryExecutor(RemoteConfigRecoveryExecutor):
    """
    Recovery executor for recovery of Postgres incremental backups.

    This class implements the combine backup process as well as the
    recovery of the newly combined backup by reusing some of the logic
    from the :class:`RecoveryExecutor` class.
    """

    def __init__(self, backup_manager):
        """
        Constructor

        :param barman.backup.BackupManager backup_manager: the :class:`BackupManager`
            owner of the executor
        """
        super(IncrementalRecoveryExecutor, self).__init__(backup_manager)
        self.combine_start_time = None
        self.combine_end_time = None

    def recover(self, backup_info, dest, wal_dest=None, remote_command=None, **kwargs):
        """
        Performs the recovery of an incremental backup.

        It first combines all backups in the backup chain, full to incremental,
        then proceeds with the recovery of the generated synthetic backup.

        This method should be called in a :func:`contextlib.closing` context.

        :param barman.infofile.BackupInfo backup_info: the incremental
            backup to recover
        :param str dest: the destination directory
        :param str|None wal_dest: the destination directory for WALs when doing PITR.
            See :meth:`~barman.recovery_executor.RecoveryExecutor._set_pitr_targets`
            for more details.
        :param str|None remote_command: The remote command to recover
            the base backup, in case of remote backup.
        :return dict: ``recovery_info`` dictionary, holding the values related
            with the recovery process.
        """
        # First combine the backups, generating a new synthetic backup in the staging area
        combine_directory = self.config.local_staging_path
        synthetic_backup_info = self._combine_backups(backup_info, combine_directory)

        # Add the backup directory created in the staging area to be deleted after recovery
        synthetic_backup_dir = synthetic_backup_info.get_basebackup_directory()
        self.temp_dirs.append(fs.LocalLibPathDeletionCommand(synthetic_backup_dir))

        # Perform the standard recovery process passing the synthetic backup
        recovery_info = super(IncrementalRecoveryExecutor, self).recover(
            synthetic_backup_info,
            dest,
            wal_dest,
            remote_command=remote_command,
            **kwargs,
        )

        # If the checksum configuration is not consistent among all backups in the chain, we
        # raise a warning at the end so the user can optionally take action about it
        if not backup_info.is_checksum_consistent():
            output.warning(
                "You restored from an incremental backup where checksums were enabled on "
                "that backup, but not all backups in the chain. It is advised to disable, and "
                "optionally re-enable, checksums on the destination directory to avoid failures."
            )

        return recovery_info

    def _combine_backups(self, backup_info, dest):
        """
        Combines the backup chain into a single synthetic backup using the
        ``pg_combinebackup`` utility.

        :param barman.infofile.LocalBackupInfo backup_info: the incremental
            backup to be recovered
        :param str dest: the directory where the synthetic backup is going
            to be mounted on
        :return barman.infofile.SyntheticBackupInfo: the backup info file of the
            combined backup
        """
        self.combine_start_time = datetime.datetime.now()

        # Build the synthetic backup info from the incremental backup as it has
        # the most recent data relevant to the recovery. Also, the combine process
        # should be transparent to the end user so e.g. the .barman-recover.info file
        # that is created on destination and also the backup_id that is appended to the
        # manifest file in further steps of the recovery should be the same as the incremental
        synthetic_backup_info = SyntheticBackupInfo(
            self.server,
            base_directory=dest,
            backup_id=backup_info.backup_id,
        )
        synthetic_backup_info.load(filename=backup_info.filename)

        dest_dirs = [synthetic_backup_info.get_data_directory()]

        # Maps the tablespaces from the old backup directory to the new synthetic
        # backup directory. This mapping is passed to the pg_combinebackup as input
        tbs_map = {}
        if backup_info.tablespaces:
            for tablespace in backup_info.tablespaces:
                source = backup_info.get_data_directory(tablespace_oid=tablespace.oid)
                destination = synthetic_backup_info.get_data_directory(
                    tablespace_oid=tablespace.oid
                )
                tbs_map[source] = destination
                dest_dirs.append(destination)

        # Prepare the destination directories for pgdata and tablespaces
        for _dir in dest_dirs:
            self._prepare_destination(_dir)

        # Retrieve pg_combinebackup version information
        remote_status = self._fetch_remote_status()

        # Get the backup chain data paths to be passed to the pg_combinebackup
        backups_chain = self._get_backup_chain_paths(backup_info)

        self._start_message(synthetic_backup_info)

        pg_combinebackup = PgCombineBackup(
            destination=synthetic_backup_info.get_data_directory(),
            command=remote_status["pg_combinebackup_path"],
            version=remote_status["pg_combinebackup_version"],
            app_name=None,
            tbs_mapping=tbs_map,
            retry_times=self.config.basebackup_retry_times,
            retry_sleep=self.config.basebackup_retry_sleep,
            retry_handler=partial(self._retry_handler, dest_dirs),
            out_handler=PgCombineBackup.make_logging_handler(logging.INFO),
            args=backups_chain,
        )

        # Do the actual combine
        try:
            pg_combinebackup()
        except CommandFailedException as e:
            msg = "Combine action failure on directory '%s'" % dest
            raise DataTransferFailure.from_command_error("pg_combinebackup", e, msg)

        self._end_message(synthetic_backup_info)

        self.combine_end_time = datetime.datetime.now()
        combine_time = total_seconds(self.combine_end_time - self.combine_start_time)
        synthetic_backup_info.copy_stats = {
            "combine_time": combine_time,
        }

        return synthetic_backup_info

    def _backup_copy(
        self,
        backup_info,
        dest,
        tablespaces=None,
        remote_command=None,
        **kwargs,
    ):
        """
        Perform the actual copy/move of the synthetic backup to destination

        :param barman.infofile.SyntheticBackupInfo backup_info: the synthetic
            backup info file
        :param str dest: the destination directory
        :param dict[str,str]|None tablespaces: a tablespace
            name -> location map (for relocation)
        :param str|None remote_command: default ``None``. The remote command to
            recover the backup, in case of remote backup
        """
        # If it is a remote recovery we just follow the standard rsync copy process
        if remote_command:
            super(IncrementalRecoveryExecutor, self)._backup_copy(
                backup_info, dest, tablespaces, remote_command, **kwargs
            )
            return
        # If it is a local recovery we move the content from staging to destination
        # Starts with tablespaces
        if backup_info.tablespaces:
            for tablespace in backup_info.tablespaces:
                # By default a tablespace goes in the same location where
                # it was on the source server when the backup was taken
                destination = tablespace.location
                # If a relocation has been requested for this tablespace
                # use the user provided target directory
                if tablespaces and tablespace.name in tablespaces:
                    destination = tablespaces[tablespace.name]
                # Move the content of the tablespace directory to destination directory
                self._prepare_destination(destination)
                tbs_source = backup_info.get_data_directory(
                    tablespace_oid=tablespace.oid
                )
                self._move_to_destination(source=tbs_source, destination=destination)

        # Then procede to move the content of the data directory
        # We don't move the pg_tblspc as the _prepare_tablespaces method called earlier
        # in the process already created this directory and required symlinks in the destination
        # We also ignore any of the log directories and files not useful for the recovery
        data_source = backup_info.get_data_directory()
        self._move_to_destination(
            source=data_source,
            destination=dest,
            exclude_path_names={
                "pg_tblspc",
                "pg_log",
                "log",
                "pg_xlog",
                "pg_wal",
                "postmaster.pid",
                "recovery.conf",
                "tablespace_map",
            },
        )

    def _move_to_destination(self, source, destination, exclude_path_names=set()):
        """
        Move all files and directories contained within *source* to *destination*.

        :param str source: the source directory path from which underlying
            files and directories will be moved
        :param str destination: the destination directory path where to move the
            files and directories contained within *source*
        :param set[str] exclude_path_names: name of directories or files to be
            excluded from the moving action.
        """
        for file_or_dir in os.listdir(source):
            if file_or_dir not in exclude_path_names:
                file_or_dir_path = os.path.join(source, file_or_dir)
                try:
                    shutil.move(file_or_dir_path, destination)
                except shutil.Error:
                    output.error(
                        "Destination directory '%s' must be empty." % destination
                    )
                    output.close_and_exit()

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

    def _prepare_destination(self, dest_dir):
        """
        Prepare the destination directory or file before moving it.

        This method is responsible for removing a directory if it already
        exists, then (re)creating it and ensuring the correct permissions
        on the directory.

        :param str dest_dir: destination directory
        """
        # Remove a dir if exists. Ignore eventual errors
        shutil.rmtree(dest_dir, ignore_errors=True)
        # create the dir
        mkpath(dest_dir)
        # Ensure the right permissions for the destination directory
        # (0700 ocatl == 448 in decimal)
        os.chmod(dest_dir, 448)

    def _retry_handler(self, dest_dirs, attempt):
        """
        Handler invoked during a combine backup in case of retry.

        The method simply warn the user of the failure and
        remove the already existing directories of the backup.

        :param list[str] dest_dirs: destination directories
        :param int attempt: attempt number (starting from 0)
        """
        output.warning(
            "Failure combining backups using pg_combinebackup (attempt %s)", attempt
        )
        output.warning(
            "The files created so far will be removed and "
            "the combine process will restart in %s seconds",
            "30",
        )
        # Remove all the destination directories and reinit the backup
        for _dir in dest_dirs:
            self._prepare_destination(_dir)

    def _fetch_remote_status(self):
        """
        Gather info from the remote server.

        This method does not raise any exception in case of errors,
        but set the missing values to ``None`` in the resulting dictionary.

        :return dict[str, str|bool]: the pg_combinebackup client information
            of the remote server.
        """
        remote_status = dict.fromkeys(
            (
                "pg_combinebackup_installed",
                "pg_combinebackup_path",
                "pg_combinebackup_version",
            ),
            None,
        )

        # Test pg_combinebackup existence
        version_info = PgCombineBackup.get_version_info(self.server.path)

        if version_info["full_path"]:
            remote_status["pg_combinebackup_installed"] = True
            remote_status["pg_combinebackup_path"] = version_info["full_path"]
            remote_status["pg_combinebackup_version"] = version_info["full_version"]
        else:
            remote_status["pg_combinebackup_installed"] = False

        return remote_status

    def _start_message(self, backup_info):
        output.info(
            "Start combining backup via pg_combinebackup for backup %s on %s",
            backup_info.backup_id,
            backup_info.base_directory,
        )

    def _end_message(self, backup_info):
        output.info(
            "End combining backup via pg_combinebackup for backup %s",
            backup_info.backup_id,
        )


class MainRecoveryExecutor(RemoteConfigRecoveryExecutor):

    def _prepare_tablespaces(self, backup_info, cmd, dest, tablespaces):
        super()._prepare_tablespaces(backup_info, cmd, dest, tablespaces)

    def _backup_copy(
        self,
        backup_info,
        dest,
        tablespaces=None,
        remote_command=None,
        safe_horizon=None,
        recovery_info=None,
    ):
        is_incremental = backup_info.is_incremental
        any_compressed = any(
            [b.compression is not None for b in backup_info.walk_to_root()]
        )

        if is_incremental and any_compressed:
            self._handle_incremental_and_compressed_backup(
                backup_info,
                dest,
                tablespaces,
                remote_command,
            )
        elif is_incremental:
            self._handle_incremental_backup(
                backup_info,
                dest,
                tablespaces,
                remote_command,
            )
        elif any_compressed:
            self._handle_compressed_backup(
                backup_info,
                dest,
                tablespaces,
                remote_command,
            )

    def _handle_compressed_backup(
        self,
        backup_info,
        dest,
        tablespaces,
        remote_command,
    ):
        if remote_command:
            if self.config.staging_location == "remote":
                # copy the compressed backup to the remote staging path
                self._rsync_backup(backup_info, dest, tablespaces, remote_command)
                # decompress the backup to the remote destination
                self._decompress_backup(backup_info, dest, tablespaces, remote_command)
                # remove the compressed backup from the remote staging path
                # self.temp_dirs.append(...)
            elif self.config.staging_location == "local":
                # decompress the backup in the local staging path
                self._decompress_backup(backup_info, dest, tablespaces, remote_command)
                # copy the backup to the to the remote destination
                self._rsync_backup(backup_info, dest, tablespaces, remote_command)
                # remove the backup from the local staging path
                # self.temp_dirs.append(...)
        else:
            # decompress the backup in the local destination
            self._decompress_backup(backup_info, dest, tablespaces, remote_command)

    def _handle_incremental_backup(
        self,
        backup_info,
        dest,
        tablespaces,
        remote_command,
    ):
        if remote_command:
            if self.config.staging_location == "remote":
                # copy the backups to the remote staging path
                self._rsync_backup(backup_info, dest, tablespaces, remote_command)
                # combine the backups in the remote destination
                self._combine_backup(backup_info, dest, tablespaces, remote_command)
                # remove the backups from the remote staging path
                # self.temp_dirs.append(...)
            elif self.config.staging_location == "local":
                # combine the backups in the local staging path
                self._combine_backup(backup_info, dest, tablespaces, remote_command)
                # copy the backup to the to the remote destination
                self._rsync_backup(backup_info, dest, tablespaces, remote_command)
                # remove the backup from the local staging path
                # self.temp_dirs.append(...)
        else:
            # combine the backups in the local destination
            self._combine_backup(backup_info, dest, tablespaces, remote_command)

    def _handle_incremental_and_compressed_backup(
        self,
        backup_info,
        dest,
        tablespaces,
        remote_command,
    ):
        if remote_command:
            if self.config.staging_location == "remote":
                # copy the backups to the remote staging path
                self._rsync_backup(backup_info, dest, tablespaces, remote_command)
                # decompress the backups in the remote staging path
                self._decompress_backup(backup_info, dest, tablespaces, remote_command)
                # combine the backups in the remote destination
                self._decompress_backup(backup_info, dest, tablespaces, remote_command)
                # remove the backups from the remote staging path
                # self.temp_dirs.append(...)
            elif self.config.staging_location == "local":
                # decompress the backups in the local staging path
                self._decompress_backup(backup_info, dest, tablespaces, remote_command)
                # combine the backups in the local staging path
                self._combine_backup(backup_info, dest, tablespaces, remote_command)
                # copy the backup to the to the remote destination
                self._rsync_backup(backup_info, dest, tablespaces, remote_command)
                # remove the backup from the local staging path
                # self.temp_dirs.append(...)
        else:
            # decompress the backups in the local staging path
            self._decompress_backup(backup_info, dest, tablespaces, remote_command)
            # combine the backups in the local destination
            self._combine_backup(backup_info, dest, tablespaces, remote_command)
            # remote the backups from the local staging path
            # self.temp_dirs.append(...)

    def _rsync_backup(self, backup_info, dest, tablespaces, remote_command):
        pass

    def _decompress_backup(self, backup_info, dest, tablespaces, remote_command):
        pass

    def _combine_backup(self, backup_info, dest, tablespaces, remote_command):
        pass


def recovery_executor_factory(backup_manager, command, backup_info):
    """
    Method in charge of building adequate RecoveryExecutor depending on the context
    :param: backup_manager
    :param: command barman.fs.UnixLocalCommand
    :return: RecoveryExecutor instance
    """
    if backup_info.is_incremental:
        return IncrementalRecoveryExecutor(backup_manager)
    if backup_info.snapshots_info is not None:
        return SnapshotRecoveryExecutor(backup_manager)
    compression = backup_info.compression
    if compression is None:
        return RecoveryExecutor(backup_manager)
    if compression == GZipCompression.name:
        return TarballRecoveryExecutor(backup_manager, GZipCompression(command))
    if compression == LZ4Compression.name:
        return TarballRecoveryExecutor(backup_manager, LZ4Compression(command))
    if compression == ZSTDCompression.name:
        return TarballRecoveryExecutor(backup_manager, ZSTDCompression(command))
    if compression == NoneCompression.name:
        return TarballRecoveryExecutor(backup_manager, NoneCompression(command))
    raise AttributeError("Unexpected compression format: %s" % compression)


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
        :return [Assertion]
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
        :return clashes [Assertion]
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
