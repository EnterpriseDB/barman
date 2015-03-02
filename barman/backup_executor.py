# Copyright (C) 2011-2015 2ndQuadrant Italia (Devise.IT S.r.L.)
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
Backup Executor module

A BackupExecutor is responsible to actually execute a backup using a specific
backup method.

A BackupExecutor is called during a backup by the BackupManager
"""
from abc import ABCMeta, abstractmethod
import logging
import os
import re

import psycopg2

from barman.command_wrappers import RsyncPgData, CommandFailedException, \
    Command, DataTransferFailure
from barman.utils import mkpath
from barman import output, xlog
from barman.config import BackupOptions


_logger = logging.getLogger(__name__)


class SshCommandException(Exception):
    """
    Error parsing ssh_command parameter
    """


class BackupExecutor(object):
    """
    Abstract base class for Backup Executors.
    """
    __metaclass__ = ABCMeta

    def __init__(self, backup_manager):
        """
        Constructor

        :param barman.backup.BackupManager backup_manager: the BackupManager
            owner of the executor
        """
        self.backup_manager = backup_manager
        self.server = backup_manager.server
        self.config = backup_manager.config

        # Holds the action being executed. Used for error messages.
        self.current_action = None

    def init(self):
        """
        Initialize backup executor internal state
        """
        self.current_action = "starting backup"

    @abstractmethod
    def backup(self, backup_info):
        """
        Performs a backup for the server

        :param barman.infofile.BackupInfo backup_info: the object representing
            the backup.
        """

    def check(self):
        """
        Do additional checks defined by the BackupExecutor.
        """

    def status(self):
        """
        Output additional status defined by the BackupExecutor.
        """

    def get_remote_status(self):
        """
        Build additional remote status lines defined by the BackupExecutor.

        :rtype: dict[str, str]
        """
        return {}


class RsyncBackupExecutor(BackupExecutor):
    """
    Execute a backup using Rsync to transfer the files.

    It invokes PostgreSQL commands to start and stop the backup.
    The data directory is copied using rsync.

    This backup executor requires the ssh_command option to be set, or it will
    raise a SshCommandException.
    """
    def __init__(self, backup_manager):
        # Retrieve the ssh command and the options necessary for the
        # execution of rsync.
        try:
            self.ssh_options = backup_manager.config.ssh_command.split()
        except AttributeError:
            raise SshCommandException(
                'Missing or invalid ssh_command in barman configuration '
                'for server %s' % backup_manager.config.name)
        self.ssh_command = self.ssh_options.pop(0)
        self.ssh_options.extend("-o BatchMode=yes "
                                "-o StrictHostKeyChecking=no".split())

        super(RsyncBackupExecutor, self).__init__(backup_manager)

    def start_backup(self, backup_info):
        """
        Start of the backup.

        The method performs all the preliminary operations required for a
        backup to start.

        :param barman.infofile.BackupInfo backup_info: the backup information
        """
        self.current_action = "connecting to database (%s)" % \
                              self.config.conninfo
        output.debug(self.current_action)
        with self.server.pg_connect():

            # Get the PostgreSQL data directory location
            self.current_action = 'detecting data directory'
            output.debug(self.current_action)
            data_directory = self.server.get_pg_setting('data_directory')
            backup_info.set_attribute('pgdata', data_directory)

            # Set server version
            backup_info.set_attribute('version', self.server.server_version)

            # Set configuration files location
            cf = self.server.get_pg_configuration_files()
            if cf:
                for key in sorted(cf.keys()):
                    backup_info.set_attribute(key, cf[key])

            # Get tablespaces information
            self.current_action = 'detecting tablespaces'
            output.debug(self.current_action)
            tablespaces = self.server.get_pg_tablespaces()
            if tablespaces and len(tablespaces) > 0:
                backup_info.set_attribute('tablespaces', tablespaces)
                for item in tablespaces:
                    msg = "\t%s, %s, %s" % (item.oid, item.name, item.location)
                    _logger.info(msg)

            # Issue pg_start_backup on the PostgreSQL server
            self.current_action = "issuing start backup command"
            _logger.debug(self.current_action)
            label = "Barman backup %s %s" % (
                backup_info.server_name, backup_info.backup_id)

            # Exclusive backup: issue a pg_start_Backup() command
            if BackupOptions.CONCURRENT_BACKUP not in \
                    self.config.backup_options:
                start_row = self.pg_start_backup(label)
                start_xlog, start_file_name, start_file_offset, start_time = \
                    start_row
                backup_info.set_attribute('status', "STARTED")
                backup_info.set_attribute('timeline',
                                          int(start_file_name[0:8], 16))
                backup_info.set_attribute('begin_xlog', start_xlog)
                backup_info.set_attribute('begin_wal', start_file_name)
                backup_info.set_attribute('begin_offset', start_file_offset)
                backup_info.set_attribute('begin_time', start_time)

            # Concurrent backup: use pgespresso extension to start a the backup
            else:
                start_row = self.pgespresso_start_backup(label)
                backup_data, start_time = start_row
                wal_re = re.compile(
                    '^START WAL LOCATION: (.*) \(file (.*)\)',
                    re.MULTILINE)
                wal_info = wal_re.search(backup_data)
                backup_info.set_attribute('status', "STARTED")
                backup_info.set_attribute('timeline',
                                          int(wal_info.group(2)[0:8], 16))
                backup_info.set_attribute('begin_xlog', wal_info.group(1))
                backup_info.set_attribute('begin_wal', wal_info.group(2))
                backup_info.set_attribute('begin_offset',
                                          xlog.get_offset_from_location(
                                              wal_info.group(1)))
                backup_info.set_attribute('backup_label', backup_data)
                backup_info.set_attribute('begin_time', start_time)

    def pg_start_backup(self, backup_label):
        """
        Execute a pg_start_backup

        :param str backup_label: label for the backup
        """
        with self.server.pg_connect() as conn:
            if (BackupOptions.CONCURRENT_BACKUP
                not in self.config.backup_options and
                    self.server.pg_is_in_recovery()):
                raise Exception(
                    'Unable to start a backup because of server recovery state')
            try:
                cur = conn.cursor()
                if self.server.server_version < 80400:
                    cur.execute(
                        "SELECT xlog_loc, "
                        "(pg_xlogfile_name_offset(xlog_loc)).*, "
                        "now() FROM pg_start_backup(%s) as xlog_loc",
                        (backup_label,))
                else:
                    cur.execute(
                        "SELECT xlog_loc, "
                        "(pg_xlogfile_name_offset(xlog_loc)).*, "
                        "now() FROM pg_start_backup(%s,%s) as xlog_loc",
                        (backup_label, self.config.immediate_checkpoint))
                return cur.fetchone()
            except psycopg2.Error, e:
                msg = "pg_start_backup(): %s" % e
                _logger.debug(msg)
                raise Exception(msg)

    def pgespresso_start_backup(self, backup_label):
        """
        Execute a pgespresso_start_backup

        :param str backup_label: label for the backup
        """
        with self.server.pg_connect() as conn:

            if (BackupOptions.CONCURRENT_BACKUP in
                    self.config.backup_options and not
                    self.server.pg_espresso_installed()):
                raise Exception(
                    'pgespresso extension required for concurrent_backup')
            try:
                cur = conn.cursor()
                cur.execute('SELECT pgespresso_start_backup(%s,%s), now()',
                            (backup_label, self.config.immediate_checkpoint))
                return cur.fetchone()
            except psycopg2.Error, e:
                msg = "pgespresso_start_backup(): %s" % e
                _logger.debug(msg)
                raise Exception(msg)

    def _write_backup_label(self, backup_info):
        """
        Write backup_label file inside pgdata folder

        :param backup_info: the backup information structure
        """
        label_file = os.path.join(backup_info.get_data_directory(),
                                  'backup_label')
        output.debug("Writing backup label: %s" % label_file)
        with open(label_file, 'w') as f:
            f.write(backup_info.backup_label)

    def backup(self, backup_info):
        """
        Implementation of the BackupExecutor.backup(backup_info) method.
        Execute the copy of a backup from a remote server using rsync

        :param barman.infofile.BackupInfo backup_info: the object representing
            the backup.
        :returns: the representation of a finalized backup.
        """

        # Start the backup, all the subsequent code must be wrapped in a
        # try except block which finally issue a backup_stop command
        self.start_backup(backup_info)
        try:
            # save any metadata changed by start_backup() call
            # This must be inside the try-except, because it could fail
            backup_info.save()

            # If we are the first backup, purge unused WAL files
            previous_backup = self.backup_manager.get_previous_backup(
                backup_info.backup_id)
            if not previous_backup:
                self.backup_manager.remove_wal_before_backup(backup_info)

            output.info("Backup start at xlog location: %s (%s, %08X)",
                        backup_info.begin_xlog,
                        backup_info.begin_wal,
                        backup_info.begin_offset)

            # Start the copy
            self.current_action = "copying files"
            output.info("Copying files.")
            # perform the backup copy, honouring the retry option if set
            self.backup_manager.retry_backup_copy(self.backup_copy, backup_info)

            output.info("Copy done.")
        except:
            # we do not need to do anything here besides re-raising the
            # exception. It will be handled in the external try block.
            raise
        else:
            self.current_action = "issuing stop of the backup"
            output.info("Asking PostgreSQL server to finalize the backup.")
        finally:
            self.stop_backup(backup_info)

        if BackupOptions.CONCURRENT_BACKUP in self.config.backup_options:
            self.current_action = "writing backup label"
            self._write_backup_label(backup_info)

    def backup_copy(self, backup_info):
        """
        Perform the copy of the backup using Rsync, copying tablespaces,
        and basebackup in two steps.

        This function returns the size of the backup (in bytes)

        :param barman.infofile.BackupInfo backup_info: the backup information
            structure
        :returns: the size of the backup (in bytes)
        """

        # paths to be ignored from rsync
        exclude_and_protect = []

        # Retrieve the previous backup metadata and set the safe_horizon
        # accordingly
        previous_backup = self.backup_manager.get_previous_backup(
            backup_info.backup_id)
        if previous_backup:
            # safe_horizon is a tz-aware timestamp because BackupInfo class
            # ensures it
            safe_horizon = previous_backup.begin_time
        else:
            # If no previous backup is present, the safe horizon is set to None
            safe_horizon = None

        # Copy tablespaces applying bwlimit when necessary
        if backup_info.tablespaces:
            tablespaces_bw_limit = self.config.tablespace_bandwidth_limit
            # Copy a tablespace at a time
            for tablespace in backup_info.tablespaces:
                self.current_action = "copying tablespace '%s'" % \
                                      tablespace.name
                # Apply bandwidth limit if requested
                bwlimit = self.config.bandwidth_limit
                if tablespaces_bw_limit and \
                        tablespace.name in tablespaces_bw_limit:
                    bwlimit = tablespaces_bw_limit[tablespace.name]
                if bwlimit:
                    self.current_action += (" with bwlimit '%d'" % bwlimit)
                _logger.debug(self.current_action)
                # If the tablespace location is inside the data directory,
                # exclude and protect it from being copied twice during
                # the data directory copy
                if tablespace.location.startswith(backup_info.pgdata):
                    exclude_and_protect.append(
                        tablespace.location[len(backup_info.pgdata):])
                # Make sure the destination directory exists in order for
                # smart copy to detect that no file is present there
                tablespace_dest = backup_info.get_data_directory(tablespace.oid)
                mkpath(tablespace_dest)
                # Exclude and protect the tablespace from being copied again
                # during the data directory copy
                exclude_and_protect.append("/pg_tblspc/%s" % tablespace.oid)
                # Copy the backup using smart_copy trying to reuse the
                # tablespace of the previous backup if incremental is active
                ref_dir = self.reuse_dir(previous_backup, tablespace.oid)
                tb_rsync = RsyncPgData(
                    ssh=self.ssh_command,
                    ssh_options=self.ssh_options,
                    args=self.reuse_args(ref_dir),
                    bwlimit=bwlimit,
                    network_compression=self.config.network_compression,
                    check=True)
                try:
                    tb_rsync.smart_copy(
                        ':%s/' % tablespace.location,
                        tablespace_dest,
                        safe_horizon,
                        ref_dir)
                except CommandFailedException, e:
                    msg = "data transfer failure on directory '%s'" % \
                          backup_info.get_data_directory(tablespace.oid)
                    raise DataTransferFailure.from_rsync_error(e, msg)

        # Make sure the destination directory exists in order for smart copy
        # to detect that no file is present there
        backup_dest = backup_info.get_data_directory()
        mkpath(backup_dest)

        # Copy the pgdata, trying to reuse the data dir
        # of the previous backup if incremental is active
        ref_dir = self.reuse_dir(previous_backup)
        rsync = RsyncPgData(
            ssh=self.ssh_command,
            ssh_options=self.ssh_options,
            args=self.reuse_args(ref_dir),
            bwlimit=self.config.bandwidth_limit,
            exclude_and_protect=exclude_and_protect,
            network_compression=self.config.network_compression)
        try:
            rsync.smart_copy(':%s/' % backup_info.pgdata, backup_dest,
                             safe_horizon,
                             ref_dir)
        except CommandFailedException, e:
            msg = "data transfer failure on directory '%s'" % \
                  backup_info.pgdata
            raise DataTransferFailure.from_rsync_error(e, msg)

        # at last copy pg_control
        try:
            rsync(':%s/global/pg_control' % (backup_info.pgdata,),
                  '%s/global/pg_control' % (backup_dest,))
        except CommandFailedException, e:
            msg = "data transfer failure on file '%s/global/pg_control'" % \
                  backup_info.pgdata
            raise DataTransferFailure.from_rsync_error(e, msg)

        # Copy configuration files (if not inside PGDATA)
        self.current_action = "copying configuration files"
        _logger.debug(self.current_action)
        for key in ('config_file', 'hba_file', 'ident_file'):
            cf = getattr(backup_info, key, None)
            # Consider only those that reside outside of the original PGDATA
            if cf:
                if cf.startswith(backup_info.pgdata):
                    self.current_action = \
                        "skipping %s as contained in %s directory" % (
                            key, backup_info.pgdata)
                    _logger.debug(self.current_action)
                    continue
                else:
                    self.current_action = \
                        "copying %s as outside %s directory" % (
                            key, backup_info.pgdata)
                    _logger.info(self.current_action)
                    try:
                        rsync(':%s' % cf, backup_dest)

                    except CommandFailedException, e:
                        ret_code = e.args[0]['ret']
                        msg = "data transfer failure on file '%s'" % cf
                        if 'ident_file' == key and ret_code == 23:
                            # if the ident file is not present
                            # it is not a blocking error, so,
                            # we need to track why the exception is raised.
                            # if ident file is missing, warn the user, log
                            # the data transfer but continue the backup
                            output.warning(msg, log=True)
                            continue
                        else:
                            raise DataTransferFailure.from_rsync_error(
                                e, msg)

    def reuse_dir(self, previous_backup_info, oid=None):
        """
        If reuse_backup is 'copy' or 'link', builds the path of the directory
        to reuse, otherwise always returns None.

        If oid is None, it returns the full path of pgdata directory of
        the previous_backup otherwise it returns the path to the specified
        tablespace using it's oid.

        :param barman.infofile.BackupInfo previous_backup_info: backup to be
            reused
        :param str oid: oid of the tablespace to be reused
        :returns: a string containing the local path with data to be reused
            or None
        """
        if self.config.reuse_backup in ('copy', 'link') and \
                previous_backup_info is not None:
            try:
                return previous_backup_info.get_data_directory(oid)
            except ValueError:
                return None

    def reuse_args(self, reuse_dir):
        """
        If reuse_backup is 'copy' or 'link', build the rsync option to enable
        the reuse, otherwise returns an empty list

        :param str reuse_dir: the local path with data to be reused or None
        :returns: list of argument for rsync call for incremental backup
            or empty list.
        """
        if self.config.reuse_backup in ('copy', 'link') and \
                reuse_dir is not None:
            return ['--%s-dest=%s' % (self.config.reuse_backup, reuse_dir)]
        else:
            return []

    def stop_backup(self, backup_info):
        """
        Stop backup wrapper

        :param barman.infofile.BackupInfo backup_info: backup_info object
        """
        if BackupOptions.CONCURRENT_BACKUP not in self.config.backup_options:
            stop_row = self.pg_stop_backup()
            if stop_row:
                stop_xlog, stop_file_name, stop_file_offset, stop_time = \
                    stop_row
                backup_info.set_attribute('end_time', stop_time)
                backup_info.set_attribute('end_xlog', stop_xlog)
                backup_info.set_attribute('end_wal', stop_file_name)
                backup_info.set_attribute('end_offset', stop_file_offset)
            else:
                raise Exception('Cannot terminate exclusive backup. You might '
                                'have to manually execute pg_stop_backup() on '
                                'your PostgreSQL server')
        else:
            stop_row = self.pgespresso_stop_backup(backup_info.backup_label)
            if stop_row:
                end_wal, stop_time = stop_row
                decoded_segment = xlog.decode_segment_name(end_wal)
                backup_info.set_attribute('end_time', stop_time)
                backup_info.set_attribute('end_xlog',
                                          "%X/%X" % (decoded_segment[1],
                                                     (decoded_segment[
                                                      2] + 1) << 24))
                backup_info.set_attribute('end_wal', end_wal)
                backup_info.set_attribute('end_offset', 0)
            else:
                raise Exception('Cannot terminate exclusive backup. You might '
                                'have to  manually execute '
                                'pg_espresso_abort_backup() on your PostgreSQL '
                                'server')

    def pg_stop_backup(self):
        """
        Execute a pg_stop_backup

        :returns: a string with the result of the pg_stop_backup call or None
        """
        with self.server.pg_connect() as conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    'SELECT xlog_loc, (pg_xlogfile_name_offset(xlog_loc)).*, '
                    'now() FROM pg_stop_backup() as xlog_loc')
                return cur.fetchone()
            except psycopg2.Error, e:
                _logger.debug('Error issuing pg_stop_backup() command: %s', e)
                return None

    def pgespresso_stop_backup(self, backup_label):
        """
        Execute a pgespresso_stop_backup

        :param str backup_label: label of the backup
        :returns: a string containing the result of the
            pg_espresso_stop_backup call or None
        """
        with self.server.pg_connect() as conn:
            try:
                cur = conn.cursor()
                cur.execute("SELECT pgespresso_stop_backup(%s), now()",
                            (backup_label,))
                return cur.fetchone()
            except psycopg2.Error, e:
                _logger.debug(
                    "Error issuing pgespresso_stop_backup() command: %s", e)
                return None

    def check(self):
        """
        Checks SSH connection trying to execute a 'true' command on the remote
        server.
        """
        cmd = Command(self.ssh_command, self.ssh_options)
        ret = cmd("true")
        if ret == 0:
            output.result('check', self.config.name, 'ssh', True)
        else:
            output.result('check', self.config.name, 'ssh', False,
                          'return code: %s' % ret)

    def status(self):
        # If the PostgreSQL version is < 9.4 pg_stat_archiver is not available.
        # Retrieve the last_archived_wal using the executor
        remote_status = self.get_remote_status()
        if 'last_archived_wal' in remote_status:
            output.result('status', self.config.name,
                          'last_archived_wal',
                          'Last archived WAL',
                          remote_status['last_archived_wal'] or
                          'No WAL segment shipped yet')

    def get_remote_status(self):
        """
        Retrieve the last archived WAL using a ssh connection on the remote
        server and executing an ls command.

        :rtype: dict
        """
        remote_status = {}
        with self.server.pg_connect():
            if self.server.server_version < 90400:
                remote_status['last_archived_wal'] = None
                if self.server.get_pg_setting('data_directory') and \
                        self.server.get_pg_setting('archive_command'):
                    # TODO: replace with RemoteUnixCommand
                    cmd = Command(self.ssh_command,
                                  self.ssh_options)
                    archive_dir = os.path.join(
                        self.server.get_pg_setting('data_directory'),
                        'pg_xlog', 'archive_status')
                    out = str(cmd.getoutput('ls', '-tr', archive_dir)[0])
                    for line in out.splitlines():
                        if line.endswith('.done'):
                            name = line[:-5]
                            if xlog.is_any_xlog_file(name):
                                remote_status['last_archived_wal'] = name
                                break
        return remote_status
