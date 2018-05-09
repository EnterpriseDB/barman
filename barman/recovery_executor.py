# Copyright (C) 2011-2018 2ndQuadrant Limited
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
import time
from io import StringIO

import dateutil.parser
import dateutil.tz

from barman import output, xlog
from barman.command_wrappers import RsyncPgData
from barman.config import RecoveryOptions
from barman.copy_controller import RsyncCopyController
from barman.exceptions import (BadXlogSegmentName, CommandFailedException,
                               DataTransferFailure, FsOperationFailed,
                               RecoveryInvalidTargetException,
                               RecoveryStandbyModeException,
                               RecoveryTargetActionException)
from barman.fs import UnixLocalCommand, UnixRemoteCommand
from barman.infofile import BackupInfo
from barman.utils import mkpath

# generic logger for this module
_logger = logging.getLogger(__name__)

# regexp matching a single value in Postgres configuration file
PG_CONF_SETTING_RE = re.compile(r"^\s*([^\s=]+)\s*=?\s*(.*)$")

# create a namedtuple object called Assertion
# with 'filename', 'line', 'key' and 'value' as properties
Assertion = collections.namedtuple('Assertion', 'filename line key value')


# noinspection PyMethodMayBeStatic
class RecoveryExecutor(object):
    """
    Class responsible of recovery operations
    """

    # Potentially dangerous options list, which need to be revised by the user
    # after a recovery
    DANGEROUS_OPTIONS = ['data_directory', 'config_file', 'hba_file',
                         'ident_file', 'external_pid_file', 'ssl_cert_file',
                         'ssl_key_file', 'ssl_ca_file', 'ssl_crl_file',
                         'unix_socket_directory', 'include', 'include_dir',
                         'include_if_exists']

    # List of options that, if present, need to be forced to a specific value
    # during recovery, to avoid data losses
    MANGLE_OPTIONS = {'archive_command': 'false'}

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

    def recover(self, backup_info, dest, tablespaces=None, remote_command=None,
                target_tli=None, target_time=None, target_xid=None,
                target_name=None, target_immediate=False, exclusive=False,
                target_action=None, standby_mode=None):
        """
        Performs a recovery of a backup

        This method should be called in a closing context

        :param barman.infofile.BackupInfo backup_info: the backup to recover
        :param str dest: the destination directory
        :param dict[str,str]|None tablespaces: a tablespace
            name -> location map (for relocation)
        :param str|None remote_command: The remote command to recover
                               the base backup, in case of remote backup.
        :param str|None target_tli: the target timeline
        :param str|None target_time: the target time
        :param str|None target_xid: the target xid
        :param str|None target_name: the target name created previously with
                            pg_create_restore_point() function call
        :param str|None target_immediate: end recovery as soon as consistency
            is reached
        :param bool exclusive: whether the recovery is exclusive or not
        :param str|None target_action: The recovery target action
        :param bool|None standby_mode: standby mode
        """

        # Run the cron to be sure the wal catalog is up to date
        # Prepare a map that contains all the objects required for a recovery
        recovery_info = self._setup(backup_info, remote_command, dest)
        output.info("Starting %s restore for server %s using backup %s",
                    recovery_info['recovery_dest'], self.server.config.name,
                    backup_info.backup_id)
        output.info("Destination directory: %s", dest)

        # Set targets for PITR
        self._set_pitr_targets(recovery_info,
                               backup_info, dest,
                               target_name,
                               target_time,
                               target_tli,
                               target_xid,
                               target_immediate,
                               target_action)

        # Retrieve the safe_horizon for smart copy
        self._retrieve_safe_horizon(recovery_info, backup_info, dest)

        # check destination directory. If doesn't exist create it
        try:
            recovery_info['cmd'].create_dir_if_not_exists(dest)
        except FsOperationFailed as e:
            output.error("unable to initialise destination directory "
                         "'%s': %s", dest, e)
            output.close_and_exit()

        # Initialize tablespace directories
        if backup_info.tablespaces:
            self._prepare_tablespaces(backup_info,
                                      recovery_info['cmd'],
                                      dest,
                                      tablespaces)
        # Copy the base backup
        output.info("Copying the base backup.")
        try:
            self._backup_copy(
                backup_info, dest,
                tablespaces, remote_command,
                recovery_info['safe_horizon'])
        except DataTransferFailure as e:
            output.error("Failure copying base backup: %s", e)
            output.close_and_exit()

        # Copy the backup.info file in the destination as
        # ".barman-recover.info"
        if remote_command:
            try:
                recovery_info['rsync'](backup_info.filename,
                                       ':%s/.barman-recover.info' % dest)
            except CommandFailedException as e:
                output.error(
                    'copy of recovery metadata file failed: %s', e)
                output.close_and_exit()
        else:
            backup_info.save(os.path.join(dest, '.barman-recover.info'))

        # Standby mode is not available for PostgreSQL older than 9.0
        if backup_info.version < 90000 and standby_mode:
            raise RecoveryStandbyModeException(
                'standby_mode is available only from PostgreSQL 9.0')

        # Restore the WAL segments. If GET_WAL option is set, skip this phase
        # as they will be retrieved using the wal-get command.
        if not recovery_info['get_wal']:
            output.info("Copying required WAL segments.")

            try:
                # Retrieve a list of required log files
                required_xlog_files = tuple(
                    self.server.get_required_xlog_files(
                        backup_info, target_tli,
                        recovery_info['target_epoch']))

                # Restore WAL segments into the wal_dest directory
                self._xlog_copy(required_xlog_files,
                                recovery_info['wal_dest'],
                                remote_command)
            except DataTransferFailure as e:
                output.error("Failure copying WAL files: %s", e)
                output.close_and_exit()
            except BadXlogSegmentName as e:
                output.error(
                    "invalid xlog segment name %r\n"
                    "HINT: Please run \"barman rebuild-xlogdb %s\" "
                    "to solve this issue",
                    str(e), self.config.name)
                output.close_and_exit()
            # If WAL files are put directly in the pg_xlog directory,
            # avoid shipping of just recovered files
            # by creating the corresponding archive status file
            if not recovery_info['is_pitr']:
                output.info("Generating archive status files")
                self._generate_archive_status(recovery_info,
                                              remote_command,
                                              required_xlog_files)

        # Generate recovery.conf file (only if needed by PITR or get_wal)
        is_pitr = recovery_info['is_pitr']
        get_wal = recovery_info['get_wal']
        if is_pitr or get_wal or standby_mode:
            output.info("Generating recovery.conf")
            self._generate_recovery_conf(recovery_info, backup_info, dest,
                                         target_immediate, exclusive,
                                         remote_command, target_name,
                                         target_time, target_tli, target_xid,
                                         standby_mode)

        # Create archive_status directory if necessary
        archive_status_dir = os.path.join(recovery_info['wal_dest'],
                                          'archive_status')
        try:
            recovery_info['cmd'].create_dir_if_not_exists(archive_status_dir)
        except FsOperationFailed as e:
            output.error("unable to create the archive_status directory "
                         "'%s': %s", archive_status_dir, e)
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

        self._map_temporary_config_files(recovery_info,
                                         backup_info,
                                         remote_command)
        self._analyse_temporary_config_files(recovery_info)
        self._copy_temporary_config_files(dest,
                                          remote_command,
                                          recovery_info)

        return recovery_info

    def _setup(self, backup_info, remote_command, dest):
        """
        Prepare the recovery_info dictionary for the recovery, as well
        as temporary working directory

        :param barman.infofile.BackupInfo backup_info: representation of a
            backup
        :param str remote_command: ssh command for remote connection
        :return dict: recovery_info dictionary, holding the basic values for a
            recovery
        """
        # Calculate the name of the WAL directory
        if backup_info.version < 100000:
            wal_dest = os.path.join(dest, 'pg_xlog')
        else:
            wal_dest = os.path.join(dest, 'pg_wal')

        tempdir = tempfile.mkdtemp(prefix='barman_recovery-')
        self.temp_dirs.append(tempdir)

        recovery_info = {
            'cmd': None,
            'recovery_dest': 'local',
            'rsync': None,
            'configuration_files': [],
            'destination_path': dest,
            'temporary_configuration_files': [],
            'tempdir': tempdir,
            'is_pitr': False,
            'wal_dest': wal_dest,
            'get_wal': RecoveryOptions.GET_WAL in self.config.recovery_options,
        }
        # A map that will keep track of the results of the recovery.
        # Used for output generation
        results = {
            'changes': [],
            'warnings': [],
            'delete_barman_xlog': False,
            'missing_files': [],
            'get_wal': False,
            'recovery_start_time': datetime.datetime.now()
        }
        recovery_info['results'] = results

        # Set up a list of configuration files
        recovery_info['configuration_files'].append('postgresql.conf')
        if backup_info.version >= 90400:
            recovery_info['configuration_files'].append('postgresql.auto.conf')

        # Handle remote recovery options
        if remote_command:
            recovery_info['recovery_dest'] = 'remote'
            recovery_info['rsync'] = RsyncPgData(
                path=self.server.path,
                ssh=remote_command,
                bwlimit=self.config.bandwidth_limit,
                network_compression=self.config.network_compression)

            try:
                # create a UnixRemoteCommand obj if is a remote recovery
                recovery_info['cmd'] = UnixRemoteCommand(remote_command,
                                                         path=self.server.path)
            except FsOperationFailed:
                output.error(
                    "Unable to connect to the target host using the command "
                    "'%s'", remote_command)
                output.close_and_exit()
        else:
            # if is a local recovery create a UnixLocalCommand
            recovery_info['cmd'] = UnixLocalCommand()

        return recovery_info

    def _set_pitr_targets(self, recovery_info, backup_info, dest, target_name,
                          target_time, target_tli, target_xid,
                          target_immediate, target_action):
        """
        Set PITR targets - as specified by the user

        :param dict recovery_info: Dictionary containing all the recovery
            parameters
        :param barman.infofile.BackupInfo backup_info: representation of a
            backup
        :param str dest: destination directory of the recovery
        :param str|None target_name: recovery target name for PITR
        :param str|None target_time: recovery target time for PITR
        :param str|None target_tli: recovery target timeline for PITR
        :param str|None target_xid: recovery target transaction id for PITR
        :param bool|None target_immediate: end recovery as soon as consistency
            is reached
        :param str|None target_action: recovery target action for PITR
        """
        target_epoch = None
        target_datetime = None
        # Detect PITR
        if (target_time or
                target_xid or
                (target_tli and target_tli != backup_info.timeline) or
                target_name or
                (backup_info.version >= 90400 and target_immediate)):
            recovery_info['is_pitr'] = True
            targets = {}
            if target_time:
                try:
                    target_datetime = dateutil.parser.parse(target_time)
                except ValueError as e:
                    raise RecoveryInvalidTargetException(
                        "Unable to parse the target time parameter %r: %s" % (
                            target_time, e))
                except TypeError:
                    # this should not happen, but there is a known bug in
                    # dateutil.parser.parse() implementation
                    # ref: https://bugs.launchpad.net/dateutil/+bug/1247643
                    raise RecoveryInvalidTargetException(
                        "Unable to parse the target time parameter %r" %
                        target_time)

                # If the parsed timestamp is naive, forces it to local timezone
                if target_datetime.tzinfo is None:
                    target_datetime = target_datetime.replace(
                        tzinfo=dateutil.tz.tzlocal())

                # Check if the target time is reachable from the
                # selected backup
                if backup_info.end_time > target_datetime:
                    raise RecoveryInvalidTargetException(
                        "The requested target time %s "
                        "is before the backup end time %s" %
                        (target_datetime, backup_info.end_time))

                target_epoch = (
                    time.mktime(target_datetime.timetuple()) +
                    (target_datetime.microsecond / 1000000.))
                targets['time'] = str(target_datetime)
            if target_xid:
                targets['xid'] = str(target_xid)
            if target_tli and target_tli != backup_info.timeline:
                targets['timeline'] = str(target_tli)
            if target_name:
                targets['name'] = str(target_name)
            if target_immediate:
                targets['immediate'] = target_immediate

            # Manage the target_action option
            if backup_info.version < 90100:
                if target_action:
                    raise RecoveryTargetActionException(
                        "Illegal target action '%s' "
                        "for this version of PostgreSQL" %
                        target_action)
            elif 90100 <= backup_info.version < 90500:
                if target_action == 'pause':
                    recovery_info['pause_at_recovery_target'] = "on"
                elif target_action:
                    raise RecoveryTargetActionException(
                        "Illegal target action '%s' "
                        "for this version of PostgreSQL" %
                        target_action)
            else:
                if target_action in ('pause', 'shutdown', 'promote'):
                    recovery_info['recovery_target_action'] = target_action
                elif target_action:
                    raise RecoveryTargetActionException(
                        "Illegal target action '%s' "
                        "for this version of PostgreSQL" %
                        target_action)

            output.info(
                "Doing PITR. Recovery target %s",
                (", ".join(["%s: %r" % (k, v) for k, v in targets.items()])))
            recovery_info['wal_dest'] = os.path.join(dest, 'barman_xlog')

            # With a PostgreSQL version older than 8.4, it is the user's
            # responsibility to delete the "barman_xlog" directory as the
            # restore_command option in recovery.conf is not supported
            if backup_info.version < 80400 and \
                    not recovery_info['get_wal']:
                recovery_info['results']['delete_barman_xlog'] = True
        else:
            if target_action:
                raise RecoveryTargetActionException(
                    "Can't enable recovery target action when PITR "
                    "is not required")

        recovery_info['target_epoch'] = target_epoch
        recovery_info['target_datetime'] = target_datetime

    def _retrieve_safe_horizon(self, recovery_info, backup_info, dest):
        """
        Retrieve the safe_horizon for smart copy

        If the target directory contains a previous recovery, it is safe to
        pick the least of the two backup "begin times" (the one we are
        recovering now and the one previously recovered in the target
        directory). Set the value in the given recovery_info dictionary.

        :param dict recovery_info: Dictionary containing all the recovery
            parameters
        :param barman.infofile.BackupInfo backup_info: a backup representation
        :param str dest: recovery destination directory
        """
        # noinspection PyBroadException
        try:
            backup_begin_time = backup_info.begin_time
            # Retrieve previously recovered backup metadata (if available)
            dest_info_txt = recovery_info['cmd'].get_file_content(
                os.path.join(dest, '.barman-recover.info'))
            dest_info = BackupInfo(
                self.server,
                info_file=StringIO(dest_info_txt))
            dest_begin_time = dest_info.begin_time
            # Pick the earlier begin time. Both are tz-aware timestamps because
            # BackupInfo class ensure it
            safe_horizon = min(backup_begin_time, dest_begin_time)
            output.info("Using safe horizon time for smart rsync copy: %s",
                        safe_horizon)
        except FsOperationFailed as e:
            # Setting safe_horizon to None will effectively disable
            # the time-based part of smart_copy method. However it is still
            # faster than running all the transfers with checksum enabled.
            #
            # FsOperationFailed means the .barman-recover.info is not available
            # on destination directory
            safe_horizon = None
            _logger.warning('Unable to retrieve safe horizon time '
                            'for smart rsync copy: %s', e)
        except Exception as e:
            # Same as above, but something failed decoding .barman-recover.info
            # or comparing times, so log the full traceback
            safe_horizon = None
            _logger.exception('Error retrieving safe horizon time '
                              'for smart rsync copy: %s', e)

        recovery_info['safe_horizon'] = safe_horizon

    def _prepare_tablespaces(self, backup_info, cmd, dest, tablespaces):
        """
        Prepare the directory structure for required tablespaces,
        taking care of tablespaces relocation, if requested.

        :param barman.infofile.BackupInfo backup_info: backup representation
        :param barman.fs.UnixLocalCommand cmd: Object for
            filesystem interaction
        :param str dest: destination dir for the recovery
        :param dict tablespaces: dict of all the tablespaces and their location
        """
        tblspc_dir = os.path.join(dest, 'pg_tblspc')
        try:
            # check for pg_tblspc dir into recovery destination folder.
            # if it does not exists, create it
            cmd.create_dir_if_not_exists(tblspc_dir)
        except FsOperationFailed as e:
            output.error("unable to initialise tablespace directory "
                         "'%s': %s", tblspc_dir, e)
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
                output.error("unable to prepare '%s' tablespace "
                             "(destination '%s'): %s",
                             item.name, location, e)
                output.close_and_exit()
            output.info("\t%s, %s, %s", item.oid, item.name, location)

    def _backup_copy(self, backup_info, dest, tablespaces=None,
                     remote_command=None, safe_horizon=None):
        """
        Perform the actual copy of the base backup for recovery purposes

        First, it copies one tablespace at a time, then the PGDATA directory.

        Bandwidth limitation, according to configuration, is applied in
        the process.

        TODO: manage configuration files if outside PGDATA.

        :param barman.infofile.BackupInfo backup_info: the backup to recover
        :param str dest: the destination directory
        :param dict[str,str]|None tablespaces: a tablespace
            name -> location map (for relocation)
        :param str|None remote_command: default None. The remote command to
            recover the base backup, in case of remote backup.
        :param datetime.datetime|None safe_horizon: anything after this time
            has to be checked with checksum
        """

        # Set a ':' prefix to remote destinations
        dest_prefix = ''
        if remote_command:
            dest_prefix = ':'

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
                    exclude_and_protect += [location[len(dest):]]

                # Exclude and protect the tablespace from being deleted during
                # the data directory copy
                exclude_and_protect.append("/pg_tblspc/%s" % tablespace.oid)

                # Add the tablespace directory to the list of objects
                # to be copied by the controller
                controller.add_directory(
                    label=tablespace.name,
                    src='%s/' % backup_info.get_data_directory(tablespace.oid),
                    dst=dest_prefix + location,
                    bwlimit=self.config.get_bwlimit(tablespace),
                    item_class=controller.TABLESPACE_CLASS
                )

        # Add the PGDATA directory to the list of objects to be copied
        # by the controller
        controller.add_directory(
            label='pgdata',
            src='%s/' % backup_info.get_data_directory(),
            dst=dest_prefix + dest,
            bwlimit=self.config.get_bwlimit(),
            exclude=[
                '/pg_log/*',
                '/pg_xlog/*',
                '/pg_wal/*',
                '/postmaster.pid',
                '/recovery.conf',
                '/tablespace_map',
            ],
            exclude_and_protect=exclude_and_protect,
            item_class=controller.PGDATA_CLASS
        )

        # TODO: Manage different location for configuration files
        # TODO: that were not within the data directory

        # Execute the copy
        try:
            controller.copy()
        # TODO: Improve the exception output
        except CommandFailedException as e:
            msg = "data transfer failure"
            raise DataTransferFailure.from_command_error(
                'rsync', e, msg)

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
        wal_dest = '%s/' % wal_dest
        # Map of every compressor used with any WAL file in the archive,
        # to be used during this recovery
        compressors = {}
        compression_manager = self.backup_manager.compression_manager
        # Fill xlogs and compressors maps from required_xlog_files
        for wal_info in required_xlog_files:
            hashdir = xlog.hash_dir(wal_info.name)
            xlogs[hashdir].append(wal_info)
            # If a compressor is required, make sure it exists in the cache
            if wal_info.compression is not None and \
                    wal_info.compression not in compressors:
                compressors[wal_info.compression] = \
                    compression_manager.get_compressor(
                        compression=wal_info.compression)

        rsync = RsyncPgData(
            path=self.server.path,
            ssh=remote_command,
            bwlimit=self.config.bandwidth_limit,
            network_compression=self.config.network_compression)
        # If compression is used and this is a remote recovery, we need a
        # temporary directory where to spool uncompressed files,
        # otherwise we either decompress every WAL file in the local
        # destination, or we ship the uncompressed file remotely
        if compressors:
            if remote_command:
                # Decompress to a temporary spool directory
                wal_decompression_dest = tempfile.mkdtemp(
                    prefix='barman_xlog-')
            else:
                # Decompress directly to the destination directory
                wal_decompression_dest = wal_dest
            # Make sure wal_decompression_dest exists
            mkpath(wal_decompression_dest)
        else:
            # If no compression
            wal_decompression_dest = None
        if remote_command:
            # If remote recovery tell rsync to copy them remotely
            # add ':' prefix to mark it as remote
            wal_dest = ':%s' % wal_dest
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
                xlogs[prefix][-1])
            # If at least one compressed file has been found, activate
            # compression check and decompression for each WAL files
            if compressors:
                for segment in xlogs[prefix]:
                    dst_file = os.path.join(wal_decompression_dest,
                                            segment.name)
                    if segment.compression is not None:
                        compressors[segment.compression].decompress(
                            os.path.join(source_dir, segment.name),
                            dst_file)
                    else:
                        shutil.copy2(os.path.join(source_dir, segment.name),
                                     dst_file)
                if remote_command:
                    try:
                        # Transfer the WAL files
                        rsync.from_file_list(
                            list(segment.name for segment in xlogs[prefix]),
                            wal_decompression_dest, wal_dest)
                    except CommandFailedException as e:
                        msg = ("data transfer failure while copying WAL files "
                               "to directory '%s'") % (wal_dest[1:],)
                        raise DataTransferFailure.from_command_error(
                            'rsync', e, msg)

                    # Cleanup files after the transfer
                    for segment in xlogs[prefix]:
                        file_name = os.path.join(wal_decompression_dest,
                                                 segment.name)
                        try:
                            os.unlink(file_name)
                        except OSError as e:
                            output.warning(
                                "Error removing temporary file '%s': %s",
                                file_name, e)
            else:
                try:
                    rsync.from_file_list(
                        list(segment.name for segment in xlogs[prefix]),
                        "%s/" % os.path.join(self.config.wals_directory,
                                             prefix),
                        wal_dest)
                except CommandFailedException as e:
                    msg = "data transfer failure while copying WAL files " \
                          "to directory '%s'" % (wal_dest[1:],)
                    raise DataTransferFailure.from_command_error(
                        'rsync', e, msg)

        _logger.info("Finished copying %s WAL files.", total_wals)

        # Remove local decompression target directory if different from the
        # destination directory (it happens when compression is in use during a
        # remote recovery
        if wal_decompression_dest and wal_decompression_dest != wal_dest:
            shutil.rmtree(wal_decompression_dest)

    def _generate_archive_status(self, recovery_info, remote_command,
                                 required_xlog_files):
        """
        Populate the archive_status directory

        :param dict recovery_info: Dictionary containing all the recovery
            parameters
        :param str remote_command: ssh command for remote connection
        :param tuple required_xlog_files: list of required WAL segments
        """
        if remote_command:
            status_dir = recovery_info['tempdir']
        else:
            status_dir = os.path.join(recovery_info['wal_dest'],
                                      'archive_status')
            mkpath(status_dir)
        for wal_info in required_xlog_files:
            with open(os.path.join(status_dir, "%s.done" % wal_info.name),
                      'a') as f:
                f.write('')
        if remote_command:
            try:
                recovery_info['rsync']('%s/' % status_dir,
                                       ':%s' % os.path.join(
                                           recovery_info['wal_dest'],
                                           'archive_status'))
            except CommandFailedException as e:
                output.error("unable to populate archive_status "
                             "directory: %s", e)
                output.close_and_exit()

    def _generate_recovery_conf(self, recovery_info, backup_info, dest,
                                immediate, exclusive, remote_command,
                                target_name, target_time, target_tli,
                                target_xid, standby_mode):
        """
        Generate a recovery.conf file for PITR containing
        all the required configurations

        :param dict recovery_info: Dictionary containing all the recovery
            parameters
        :param barman.infofile.BackupInfo backup_info: representation of a
            backup
        :param str dest: destination directory of the recovery
        :param str|None immediate: end recovery as soon as consistency
            is reached
        :param boolean exclusive: exclusive backup or concurrent
        :param str remote_command: ssh command for remote connection
        :param str target_name: recovery target name for PITR
        :param str target_time: recovery target time for PITR
        :param str target_tli: recovery target timeline for PITR
        :param str target_xid: recovery target transaction id for PITR
        :param bool|None standby_mode: standby mode
        """
        if remote_command:
            recovery = open(os.path.join(recovery_info['tempdir'],
                                         'recovery.conf'), 'w')
        else:
            recovery = open(os.path.join(dest, 'recovery.conf'), 'w')

        # If GET_WAL has been set, use the get-wal command to retrieve the
        # required wal files. Otherwise use the unix command "cp" to copy
        # them from the barman_xlog directory
        if recovery_info['get_wal']:
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
                print("# The 'barman-wal-restore' command "
                      "is provided in the 'barman-cli' package",
                      file=recovery)
                print("restore_command = 'barman-wal-restore -U %s "
                      "%s %s %%f %%p'" % (self.config.config.user,
                                          fqdn, self.config.name),
                      file=recovery)
            else:
                print("# The 'barman get-wal' command "
                      "must run as '%s' user" % self.config.config.user,
                      file=recovery)
                print("restore_command = 'sudo -u %s "
                      "barman get-wal %s %%f > %%p'" % (
                          self.config.config.user,
                          self.config.name),
                      file=recovery)
            recovery_info['results']['get_wal'] = True
        else:
            print("restore_command = 'cp barman_xlog/%f %p'", file=recovery)
        if backup_info.version >= 80400 and \
                not recovery_info['get_wal']:
            print("recovery_end_command = 'rm -fr barman_xlog'", file=recovery)

        # Writes recovery target
        if target_time:
            print("recovery_target_time = '%s'" % target_time, file=recovery)
        if target_xid:
            print("recovery_target_xid = '%s'" % target_xid, file=recovery)
        if target_name:
            print("recovery_target_name = '%s'" % target_name, file=recovery)
        # TODO: log a warning if PostgreSQL < 9.4 and --immediate
        if (backup_info.version >= 90400 and immediate):
            print("recovery_target = 'immediate'", file=recovery)

        # Manage what happens after recovery target is reached
        if (target_xid or target_time) and exclusive:
            print("recovery_target_inclusive = '%s'" % (
                not exclusive), file=recovery)
        if target_tli:
            print("recovery_target_timeline = %s" % target_tli, file=recovery)

        # Write recovery target action
        if 'pause_at_recovery_target' in recovery_info:
            print("pause_at_recovery_target = '%s'" %
                  recovery_info['pause_at_recovery_target'],
                  file=recovery)
        if 'recovery_target_action' in recovery_info:
            print("recovery_target_action = '%s'" %
                  recovery_info['recovery_target_action'],
                  file=recovery)

        # Writes the standby mode
        if standby_mode:
            print("standby_mode = 'on'", file=recovery)

        recovery.close()
        if remote_command:
            plain_rsync = RsyncPgData(
                path=self.server.path,
                ssh=remote_command,
                bwlimit=self.config.bandwidth_limit,
                network_compression=self.config.network_compression)
            try:
                plain_rsync.from_file_list(['recovery.conf'],
                                           recovery_info['tempdir'],
                                           ':%s' % dest)
            except CommandFailedException as e:
                output.error('remote copy of recovery.conf failed: %s', e)
                output.close_and_exit()

    def _map_temporary_config_files(self, recovery_info, backup_info,
                                    remote_command):
        """
        Map configuration files, by filling the 'temporary_configuration_files'
        array, depending on remote or local recovery. This array will be used
        by the subsequent methods of the class.

        :param dict recovery_info: Dictionary containing all the recovery
            parameters
        :param barman.infofile.BackupInfo backup_info: a backup representation
        :param str remote_command: ssh command for remote recovery
        """

        # Cycle over postgres configuration files which my be missing.
        # If a file is missing, we will be unable to restore it and
        # we will warn the user.
        # This can happen if we are using pg_basebackup and
        # a configuration file is located outside the data dir.
        # This is not an error condition, so we check also for
        # `pg_ident.conf` which is an optional file.
        for conf_file in (recovery_info['configuration_files'] +
                          ['pg_hba.conf', 'pg_ident.conf']):
            source_path = os.path.join(
                backup_info.get_data_directory(), conf_file)
            if not os.path.exists(source_path):
                recovery_info['results']['missing_files'].append(conf_file)
                # Remove the file from the list of configuration files
                if conf_file in recovery_info['configuration_files']:
                    recovery_info['configuration_files'].remove(conf_file)

        for conf_file in recovery_info['configuration_files']:
            if remote_command:
                # If the recovery is remote, copy the postgresql.conf
                # file in a temp dir
                # Otherwise we can modify the postgresql.conf file
                # in the destination directory.
                conf_file_path = os.path.join(
                    recovery_info['tempdir'], conf_file)
                shutil.copy2(
                    os.path.join(backup_info.get_data_directory(),
                                 conf_file), conf_file_path)
            else:
                # Otherwise use the local destination path.
                conf_file_path = os.path.join(
                    recovery_info['destination_path'], conf_file)

            recovery_info['temporary_configuration_files'].append(
                conf_file_path)

    def _analyse_temporary_config_files(self, recovery_info):
        """
        Analyse temporary configuration files and identify dangerous options

        Mark all the dangerous options for the user to review. This procedure
        also changes harmful options such as 'archive_command'.

        :param dict recovery_info: dictionary holding all recovery parameters
        """
        results = recovery_info['results']
        # Check for dangerous options inside every config file
        for conf_file in recovery_info['temporary_configuration_files']:

            # Identify and comment out dangerous options, replacing them with
            # the appropriate values
            results['changes'] += self._pg_config_mangle(
                conf_file,
                self.MANGLE_OPTIONS,
                "%s.origin" % conf_file)
            # Identify dangerous options and warn users about their presence
            results['warnings'] += self._pg_config_detect_possible_issues(
                conf_file)

    def _copy_temporary_config_files(self, dest,
                                     remote_command, recovery_info):
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
            file_list = []
            for conf_file in recovery_info['configuration_files']:
                file_list.append('%s' % conf_file)
                file_list.append('%s.origin' % conf_file)

            try:
                recovery_info['rsync'].from_file_list(file_list,
                                                      recovery_info['tempdir'],
                                                      ':%s' % dest)
            except CommandFailedException as e:
                output.error('remote copy of configuration files failed: %s',
                             e)
                output.close_and_exit()

    def close(self):
        """
        Cleanup operations for a recovery
        """
        # Remove the temporary directories
        for temp_dir in self.temp_dirs:
            shutil.rmtree(temp_dir, ignore_errors=True)
        self.temp_dirs = []

    def _pg_config_mangle(self, filename, settings, backup_filename=None):
        """
        This method modifies the given PostgreSQL configuration file,
        commenting out the given settings, and adding the ones generated by
        Barman.

        If backup_filename is passed, keep a backup copy.

        :param filename: the PostgreSQL configuration file
        :param settings: dictionary of settings to be mangled
        :param backup_filename: config file backup copy. Default is None.
        """
        # Read the full content of the file in memory
        with open(filename) as f:
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
        with open(filename, 'w') as f:
            for l_number, line in enumerate(content):
                rm = PG_CONF_SETTING_RE.match(line)
                if rm:
                    key = rm.group(1)
                    if key in settings:
                        f.write("#BARMAN# %s" % line)
                        # TODO is it useful to handle none values?
                        changes = "%s = %s\n" % (key, settings[key])
                        f.write(changes)
                        mangled.append(
                            Assertion._make([
                                os.path.basename(f.name),
                                l_number,
                                key,
                                settings[key]]))
                        continue
                f.write(line)

        # Restore original permissions
        shutil.copymode(orig_filename, filename)

        # If a backup copy of the file is not requested,
        # unlink the orig file
        if not backup_filename:
            os.unlink(orig_filename)

        return mangled

    def _pg_config_detect_possible_issues(self, filename):
        """
        This method looks for any possible issue with PostgreSQL
        location options such as data_directory, config_file, etc.
        It returns a dictionary with the dangerous options that
        have been found.

        :param filename: the Postgres configuration file
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
                        Assertion._make([
                            os.path.basename(f.name),
                            l_number,
                            key,
                            rm.group(2)]))

        return clashes
