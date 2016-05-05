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
Backup Executor module

A Backup Executor is a class responsible for the execution
of a backup. Specific implementations of backups are defined by
classes that derive from BackupExecutor (e.g.: backup with rsync
through Ssh).

A BackupExecutor is invoked by the BackupManager for backup operations.
"""

import logging
import os
import re
from abc import ABCMeta, abstractmethod

from barman import output, xlog
from barman.command_wrappers import (Command, CommandFailedException,
                                     DataTransferFailure, RsyncPgData)
from barman.config import BackupOptions
from barman.infofile import BackupInfo
from barman.postgres import PostgresConnectionError
from barman.remote_status import RemoteStatusMixin
from barman.utils import mkpath, with_metaclass

_logger = logging.getLogger(__name__)


class SshCommandException(Exception):
    """
    Error parsing ssh_command parameter
    """


class BackupExecutor(with_metaclass(ABCMeta, RemoteStatusMixin)):
    """
    Abstract base class for any backup executors.
    """

    def __init__(self, backup_manager):
        """
        Base constructor

        :param barman.backup.BackupManager backup_manager: the BackupManager
            assigned to the executor
        """
        super(BackupExecutor, self).__init__()
        self.backup_manager = backup_manager
        self.server = backup_manager.server
        self.config = backup_manager.config

    def init(self):
        """
        Initialise the internal state of the backup executor
        """

    @abstractmethod
    def backup(self, backup_info):
        """
        Perform a backup for the server - invoked by BackupManager.backup()

        :param barman.infofile.BackupInfo backup_info: backup information
        """

    def check(self, check_strategy):
        """
        Perform additional checks - invoked by BackupManager.check()

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """

    def status(self):
        """
        Set additional status info - invoked by BackupManager.status()
        """

    def fetch_remote_status(self):
        """
        Get additional remote status info - invoked by
        BackupManager.get_remote_status()

        This method does not raise any exception in case of errors,
        but set the missing values to None in the resulting dictionary.

        :rtype: dict[str, None|str]
        """
        return {}


def _parse_ssh_command(ssh_command):
    """
    Parse a user provided ssh command to a single command and
    a list of arguments

    In case of error, the first member of the result (the command) will be None

    :param ssh_command: a ssh command provided by the user
    :return tuple[str,list[str]]: the command and a list of options
    """
    try:
        ssh_options = ssh_command.split()
    except AttributeError:
        return None, []
    ssh_command = ssh_options.pop(0)
    ssh_options.extend("-o BatchMode=yes -o StrictHostKeyChecking=no".split())
    return ssh_command, ssh_options


class SshBackupExecutor(with_metaclass(ABCMeta, BackupExecutor)):
    """
    Abstract base class for any backup executors based on Ssh
    remote connections. This class is also a factory for
    exclusive/concurrent backup strategy objects.

    Raises a SshCommandException if 'ssh_command' is not set.
    """

    def __init__(self, backup_manager):
        """
        Constructor of the abstract class for backups via Ssh

        :param barman.backup.BackupManager backup_manager: the BackupManager
            assigned to the executor
        """
        super(SshBackupExecutor, self).__init__(backup_manager)

        # Retrieve the ssh command and the options necessary for the
        # remote ssh access.
        self.ssh_command, self.ssh_options = _parse_ssh_command(
            backup_manager.config.ssh_command)

        # Requires ssh_command to be set
        if not self.ssh_command:
            raise SshCommandException(
                'Missing or invalid ssh_command in barman configuration '
                'for server %s' % backup_manager.config.name)

        # Holds the action being executed. Used for error messages.
        self.current_action = None
        # Depending on the backup options value, create the proper strategy
        if BackupOptions.CONCURRENT_BACKUP in self.config.backup_options:
            # Concurrent backup strategy
            self.strategy = ConcurrentBackupStrategy(self)
        else:
            # Exclusive backup strategy
            self.strategy = ExclusiveBackupStrategy(self)

    def _update_action_from_strategy(self):
        """
        Update the executor's current action with the one of the strategy.
        This is used during exception handling to let the caller know
        where the failure occurred.
        """

        action = getattr(self.strategy, 'current_action', None)
        if action:
            self.current_action = action

    @abstractmethod
    def backup_copy(self, backup_info):
        """
        Performs the actual copy of a backup for the server

        :param barman.infofile.BackupInfo backup_info: backup information
        """

    def init(self):
        """
        Initialise the internal state of the backup executor
        """
        self.current_action = "starting backup"

    def backup(self, backup_info):
        """
        Perform a backup for the server - invoked by BackupManager.backup()
        through the generic interface of a BackupExecutor. This implementation
        is responsible for performing a backup through a remote connection
        to the PostgreSQL server via Ssh. The specific set of instructions
        depends on both the specific class that derives from SshBackupExecutor
        and the selected strategy (e.g. exclusive backup through Rsync).

        :param barman.infofile.BackupInfo backup_info: backup information
        """

        previous_backup = self.backup_manager.get_previous_backup(
            backup_info.backup_id)

        # Start the backup, all the subsequent code must be wrapped in a
        # try except block which finally issues a stop_backup command
        try:
            self.strategy.start_backup(backup_info)
        except BaseException:
            self._update_action_from_strategy()
            raise

        try:
            # save any metadata changed by start_backup() call
            # This must be inside the try-except, because it could fail
            backup_info.save()

            output.info("Backup start at xlog location: %s (%s, %08X)",
                        backup_info.begin_xlog,
                        backup_info.begin_wal,
                        backup_info.begin_offset)

            # If this is the first backup, purge eventually unused WAL files
            if not previous_backup:
                output.info("This is the first backup for server %s",
                            self.config.name)
                removed = self.backup_manager.remove_wal_before_backup(
                    backup_info)
                if removed:
                    output.info("WAL segments preceding the current backup "
                                "have been found:", log=False)
                    for wal_name in removed:
                        output.info("\t%s from server %s "
                                    "has been removed",
                                    wal_name, self.config.name)

            # Start the copy
            self.current_action = "copying files"
            output.info("Copying files.")
            # perform the backup copy, honouring the retry option if set
            self.backup_manager.retry_backup_copy(self.backup_copy,
                                                  backup_info)

            output.info("Copy done.")
        except:
            # we do not need to do anything here besides re-raising the
            # exception. It will be handled in the external try block.
            raise
        else:
            self.current_action = "issuing stop of the backup"
            output.info("Asking PostgreSQL server to finalize the backup.")
        finally:
            try:
                self.strategy.stop_backup(backup_info)
            except BaseException:
                self._update_action_from_strategy()
                raise

    def check(self, check_strategy):
        """
        Perform additional checks for SshBackupExecutor, including
        Ssh connection (executing a 'true' command on the remote server)
        and specific checks for the given backup strategy.

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """

        # Execute a 'true' command on the remote server
        # TODO: replace with RemoteUnixCommand
        cmd = Command(self.ssh_command,
                      self.ssh_options,
                      path=self.server.path)

        hint = "PostgreSQL server"

        # Try executing 'true' through the ssh connection
        return_code = None
        try:
            return_code = cmd("true")
        except OSError as e:
            # The execution of ssh command has failed
            hint = "error executing '%s': %s" % (self.ssh_command, e.strerror)

        # The execution of ssh has succeeded but the return code is not 0
        if return_code is not None and return_code != 0:
            hint = '%s, return code: %s' % (hint, return_code)

        # Output the result
        check_strategy.result(self.config.name, 'ssh', return_code == 0, hint)

        # If SSH works but PostgreSQL is not responding
        if (return_code == 0 and
                self.server.get_remote_status()['server_txt_version']
                is None):
            # Check for 'backup_label' presence
            last_backup = self.server.get_backup(
                self.server.get_last_backup_id(BackupInfo.STATUS_NOT_EMPTY)
            )
            # Look for the latest backup in the catalogue
            if last_backup:
                # Get PGDATA and build path to 'backup_label'
                backup_label = os.path.join(last_backup.pgdata,
                                            'backup_label')
                # Verify that backup_label exists in the remote PGDATA.
                # If so, send an alert. Do not show anything if OK.
                # TODO: replace with RemoteUnixCommand
                exists = cmd("test -e %s" % backup_label)
                if exists == 0:
                    hint = 'Check that the PostgreSQL server is up ' \
                           "and no 'backup_label' file is in PGDATA."
                    check_strategy.result(self.config.name,
                                          'backup_label', False,
                                          hint)

        try:
            # Invoke specific checks for the backup strategy
            self.strategy.check(check_strategy)
        except BaseException:
            self._update_action_from_strategy()
            raise

    def status(self):
        """
        Set additional status info for SshBackupExecutor using remote
        commands via Ssh, as well as those defined by the given
        backup strategy.
        """
        try:
            # Invoke the status() method for the given strategy
            self.strategy.status()
        except BaseException:
            self._update_action_from_strategy()
            raise

    def fetch_remote_status(self):
        """
        Get remote information on PostgreSQL using Ssh, such as
        last archived WAL file

        This method does not raise any exception in case of errors,
        but set the missing values to None in the resulting dictionary.

        :rtype: dict[str, None|str]
        """
        remote_status = {}
        # Retrieve the last archived WAL using a Ssh connection on
        # the remote server and executing an 'ls' command. Only
        # for pre-9.4 versions of PostgreSQL.
        try:
            if self.server.postgres and \
                    self.server.postgres.server_version < 90400:
                remote_status['last_archived_wal'] = None
                if self.server.postgres.get_setting('data_directory') and \
                        self.server.postgres.get_setting('archive_command'):
                    # TODO: replace with RemoteUnixCommand
                    # The Command can raise OSError
                    # if self.ssh_command does not exist.
                    cmd = Command(self.ssh_command,
                                  self.ssh_options,
                                  path=self.server.path)
                    archive_dir = os.path.join(
                        self.server.postgres.get_setting('data_directory'),
                        'pg_xlog', 'archive_status')
                    out = str(cmd.getoutput('ls', '-t', archive_dir)[0])
                    for line in out.splitlines():
                        if line.endswith('.done'):
                            name = line[:-5]
                            if xlog.is_any_xlog_file(name):
                                remote_status['last_archived_wal'] = name
                                break
        except (PostgresConnectionError, OSError) as e:
            _logger.warn("Error retrieving PostgreSQL status: %s", e)
        return remote_status


class RsyncBackupExecutor(SshBackupExecutor):
    """
    Concrete class for backup via Rsync+Ssh.

    It invokes PostgreSQL commands to start and stop the backup, depending
    on the defined strategy. Data files are copied using Rsync via Ssh.
    It heavily relies on methods defined in the SshBackupExecutor class
    from which it derives.
    """

    def __init__(self, backup_manager):
        """
        Constructor

        :param barman.backup.BackupManager backup_manager: the BackupManager
            assigned to the strategy
        """
        super(RsyncBackupExecutor, self).__init__(backup_manager)

    def backup_copy(self, backup_info):
        """
        Perform the actual copy of the backup using Rsync.
        First, it copies one tablespace at a time, then the PGDATA directory,
        and finally configuration files (if outside PGDATA).
        Bandwidth limitation, according to configuration, is applied in
        the process.
        This method is the core of base backup copy using Rsync+Ssh.

        :param barman.infofile.BackupInfo backup_info: backup information
        """

        # List of paths to be ignored by Rsync
        exclude_and_protect = []

        # Retrieve the previous backup metadata, then set safe_horizon
        previous_backup = self.backup_manager.get_previous_backup(
            backup_info.backup_id)
        if previous_backup:
            # safe_horizon is a tz-aware timestamp because BackupInfo class
            # ensures it
            safe_horizon = previous_backup.begin_time
        else:
            # If no previous backup is present, safe_horizon is set to None
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
                tablespace_dest = backup_info.get_data_directory(
                    tablespace.oid)
                mkpath(tablespace_dest)
                # Exclude and protect the tablespace from being copied again
                # during the data directory copy
                exclude_and_protect.append("/pg_tblspc/%s" % tablespace.oid)
                # Copy the backup using smart_copy trying to reuse the
                # tablespace of the previous backup if incremental is active
                ref_dir = self._reuse_dir(previous_backup, tablespace.oid)
                tb_rsync = RsyncPgData(
                    path=self.server.path,
                    ssh=self.ssh_command,
                    ssh_options=self.ssh_options,
                    args=self._reuse_args(ref_dir),
                    bwlimit=bwlimit,
                    network_compression=self.config.network_compression,
                    check=True)
                try:
                    tb_rsync.smart_copy(
                        ':%s/' % tablespace.location,
                        tablespace_dest,
                        safe_horizon,
                        ref_dir)
                except CommandFailedException as e:
                    msg = "data transfer failure on directory '%s'" % \
                          backup_info.get_data_directory(tablespace.oid)
                    raise DataTransferFailure.from_rsync_error(e, msg)

        # Make sure the destination directory exists in order for smart copy
        # to detect that no file is present there
        backup_dest = backup_info.get_data_directory()
        mkpath(backup_dest)

        # Copy the PGDATA, trying to reuse the data dir
        # of the previous backup if incremental is active
        ref_dir = self._reuse_dir(previous_backup)
        rsync = RsyncPgData(
            path=self.server.path,
            ssh=self.ssh_command,
            ssh_options=self.ssh_options,
            args=self._reuse_args(ref_dir),
            bwlimit=self.config.bandwidth_limit,
            exclude_and_protect=exclude_and_protect,
            network_compression=self.config.network_compression)
        try:
            rsync.smart_copy(':%s/' % backup_info.pgdata, backup_dest,
                             safe_horizon,
                             ref_dir)
        except CommandFailedException as e:
            msg = "data transfer failure on directory '%s'" % \
                  backup_info.pgdata
            raise DataTransferFailure.from_rsync_error(e, msg)

        # At last copy pg_control
        try:
            rsync(':%s/global/pg_control' % (backup_info.pgdata,),
                  '%s/global/pg_control' % (backup_dest,))
        except CommandFailedException as e:
            msg = "data transfer failure on file '%s/global/pg_control'" % \
                  backup_info.pgdata
            raise DataTransferFailure.from_rsync_error(e, msg)

        # Copy configuration files (if not inside PGDATA)
        self.current_action = "copying configuration files"
        _logger.debug(self.current_action)
        for key in ('config_file', 'hba_file', 'ident_file'):
            cf = getattr(backup_info, key, None)
            if cf:
                assert isinstance(cf, str)
                # Consider only those that reside outside of the original
                # PGDATA directory
                if cf.startswith(backup_info.pgdata):
                    self.current_action = \
                        "skipping %s as contained in %s directory" % (
                            key, backup_info.pgdata)
                    _logger.debug(self.current_action)
                    continue
                self.current_action = "copying %s as outside %s directory" % (
                    key, backup_info.pgdata)
                _logger.info(self.current_action)
                try:
                    rsync(':%s' % cf, backup_dest)
                except CommandFailedException as e:
                    ret_code = e.args[0]['ret']
                    msg = "data transfer failure on file '%s'" % cf
                    if 'ident_file' == key and ret_code == 23:
                        # If the ident file is missing,
                        # it isn't an error condition for PostgreSQL.
                        # Barman is consistent with this behavior.
                        output.warning(msg, log=True)
                        continue
                    else:
                        raise DataTransferFailure.from_rsync_error(e, msg)
        # Check for any include directives in PostgreSQL configuration
        # Currently, include directives are not supported for files that
        # reside outside PGDATA. These files must be manually backed up.
        # Barman will emit a warning and list those files
        if backup_info.included_files:
            filtered_files = [
                included_file
                for included_file in backup_info.included_files
                if not included_file.startswith(backup_info.pgdata)
            ]
            if len(filtered_files) > 0:
                output.warning(
                    "The usage of include directives is not supported "
                    "for files that reside outside PGDATA.\n"
                    "Please manually backup the following files:\n"
                    "\t%s\n",
                    "\n\t".join(filtered_files)
                )

    def _reuse_dir(self, previous_backup_info, oid=None):
        """
        If reuse_backup is 'copy' or 'link', builds the path of the directory
        to reuse, otherwise always returns None.

        If oid is None, it returns the full path of PGDATA directory of
        the previous_backup otherwise it returns the path to the specified
        tablespace using it's oid.

        :param barman.infofile.BackupInfo previous_backup_info: backup to be
            reused
        :param str oid: oid of the tablespace to be reused
        :returns: a string containing the local path with data to be reused
            or None
        :rtype: str|None
        """
        if self.config.reuse_backup in ('copy', 'link') and \
                previous_backup_info is not None:
            try:
                return previous_backup_info.get_data_directory(oid)
            except ValueError:
                return None

    def _reuse_args(self, reuse_dir):
        """
        If reuse_backup is 'copy' or 'link', build the rsync option to enable
        the reuse, otherwise returns an empty list

        :param str reuse_dir: the local path with data to be reused or None
        :returns: list of argument for rsync call for incremental backup
            or empty list.
        :rtype: list(str)
        """
        if self.config.reuse_backup in ('copy', 'link') and \
                reuse_dir is not None:
            return ['--%s-dest=%s' % (self.config.reuse_backup, reuse_dir)]
        else:
            return []


class BackupStrategy(with_metaclass(ABCMeta, object)):
    """
    Abstract base class for a strategy to be used by a backup executor.
    """

    def __init__(self, executor):
        """
        Constructor

        :param BackupExecutor executor: the BackupExecutor assigned
            to the strategy
        """
        self.executor = executor

        # Holds the action being executed. Used for error messages.
        self.current_action = None

    @abstractmethod
    def start_backup(self, backup_info):
        """
        Issue a start of a backup - invoked by BackupExecutor.backup()

        :param barman.infofile.BackupInfo backup_info: backup information
        """

    @abstractmethod
    def stop_backup(self, backup_info):
        """
        Issue a stop of a backup - invoked by BackupExecutor.backup()

        :param barman.infofile.BackupInfo backup_info: backup information
        """

    @abstractmethod
    def check(self, check_strategy):
        """
        Perform additional checks - invoked by BackupExecutor.check()

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """

    # noinspection PyMethodMayBeStatic
    def status(self):
        """
        Set additional status info - invoked by BackupExecutor.status()
        """

    def _pg_get_metadata(self, backup_info):
        """
        Load PostgreSQL metadata into the backup_info parameter

        :param barman.infofile.BackupInfo backup_info: backup information
        """
        server = self.executor.server

        # Get the PostgreSQL data directory location
        self.current_action = 'detecting data directory'
        output.debug(self.current_action)
        data_directory = server.postgres.get_setting('data_directory')
        backup_info.set_attribute('pgdata', data_directory)

        # Set server version
        backup_info.set_attribute('version', server.postgres.server_version)

        # Set configuration files location
        cf = server.postgres.get_configuration_files()
        for key in cf:
            backup_info.set_attribute(key, cf[key])

        # Get tablespaces information
        self.current_action = 'detecting tablespaces'
        output.debug(self.current_action)
        tablespaces = server.postgres.get_tablespaces()
        if tablespaces and len(tablespaces) > 0:
            backup_info.set_attribute('tablespaces', tablespaces)
            for item in tablespaces:
                msg = "\t%s, %s, %s" % (item.oid, item.name, item.location)
                _logger.info(msg)


class ExclusiveBackupStrategy(BackupStrategy):
    """
    Concrete class for exclusive backup strategy.

    This strategy is for SshBackupExecutor only and is responsible for
    coordinating Barman with PostgreSQL on standard physical backup
    operations (known as 'exclusive' backup), such as invoking
    pg_start_backup() and pg_stop_backup() on the master server.
    """

    def __init__(self, executor):
        """
        Constructor

        :param BackupExecutor executor: the BackupExecutor assigned
            to the strategy
        """
        super(ExclusiveBackupStrategy, self).__init__(executor)
        # Make sure that executor is of type SshBackupExecutor
        assert isinstance(executor, SshBackupExecutor)
        # Make sure that backup_options does not contain 'concurrent'
        assert (BackupOptions.CONCURRENT_BACKUP not in
                self.executor.config.backup_options)

    def start_backup(self, backup_info):
        """
        Manage the start of an exclusive backup

        The method performs all the preliminary operations required for an
        exclusive physical backup to start, as well as preparing the
        information on the backup for Barman.

        :param barman.infofile.BackupInfo backup_info: backup information
        """
        self.current_action = "connecting to database (%s)" % \
                              self.executor.config.conninfo
        output.debug(self.current_action)
        # Retrieve PostgreSQL server metadata
        self._pg_get_metadata(backup_info)

        # Issue pg_start_backup on the PostgreSQL server
        self.current_action = "issuing start backup command"
        _logger.debug(self.current_action)
        label = "Barman backup %s %s" % (
            backup_info.server_name, backup_info.backup_id)

        # Exclusive backup: issue a pg_start_Backup() command
        start_row = self.executor.server.postgres.start_exclusive_backup(label)
        start_xlog, start_file_name, start_file_offset, start_time = \
            start_row
        backup_info.set_attribute('status', "STARTED")
        backup_info.set_attribute('timeline',
                                  int(start_file_name[0:8], 16))
        backup_info.set_attribute('begin_xlog', start_xlog)
        backup_info.set_attribute('begin_wal', start_file_name)
        backup_info.set_attribute('begin_offset', start_file_offset)
        backup_info.set_attribute('begin_time', start_time)

    def stop_backup(self, backup_info):
        """
        Manage the stop of an exclusive backup

        The method informs the PostgreSQL server that the physical
        exclusive backup is finished, as well as preparing the information
        returned by PostgreSQL for Barman.

        :param barman.infofile.BackupInfo backup_info: backup information
        """

        self.current_action = "issuing stop backup command"
        stop_row = self.executor.server.postgres.stop_exclusive_backup()
        if stop_row:
            stop_xlog, stop_file_name, stop_file_offset, stop_time = \
                stop_row
            backup_info.set_attribute('end_time', stop_time)
            backup_info.set_attribute('end_xlog', stop_xlog)
            backup_info.set_attribute('end_wal', stop_file_name)
            backup_info.set_attribute('end_offset', stop_file_offset)
        else:
            raise Exception('Cannot terminate exclusive backup. You might '
                            'have to  manually execute pg_stop_backup() '
                            'on your PostgreSQL server')

    def check(self, check_strategy):
        """
        Perform additional checks for ExclusiveBackupStrategy

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        # Make sure PostgreSQL is not in recovery (i.e. is a master)
        if self.executor.server.postgres:
            is_in_recovery = self.executor.server.postgres.is_in_recovery
            if not is_in_recovery:
                check_strategy.result(
                    self.executor.config.name, 'not in recovery', True)
            else:
                check_strategy.result(
                    self.executor.config.name, 'not in recovery', False,
                    'cannot perform exclusive backup on a standby')


class ConcurrentBackupStrategy(BackupStrategy):
    """
    Concrete class for concurrent backup strategy.

    This strategy is for SshBackupExecutor only and is responsible for
    coordinating Barman with PostgreSQL on concurrent physical backup
    operations through the pgespresso extension.
    """

    def __init__(self, executor):
        """
        Constructor

        :param BackupExecutor executor: the BackupExecutor assigned
            to the strategy
        """
        super(ConcurrentBackupStrategy, self).__init__(executor)
        # Make sure that executor is of type SshBackupExecutor
        assert isinstance(executor, SshBackupExecutor)
        # Make sure that backup_options contains 'concurrent'
        assert (BackupOptions.CONCURRENT_BACKUP in
                self.executor.config.backup_options)

    # noinspection PyMethodMayBeStatic
    def _write_backup_label(self, backup_info):
        """
        Write backup_label file inside PGDATA folder

        :param barman.infofile.BackupInfo  backup_info: tbackup information
        """
        label_file = os.path.join(backup_info.get_data_directory(),
                                  'backup_label')
        output.debug("Writing backup label: %s" % label_file)
        with open(label_file, 'w') as f:
            f.write(backup_info.backup_label)

    def start_backup(self, backup_info):
        """
        Start of the backup.

        The method performs all the preliminary operations required for a
        backup to start.

        :param barman.infofile.BackupInfo backup_info: backup information
        """
        self.current_action = "connecting to database (%s)" % \
                              self.executor.config.conninfo
        output.debug(self.current_action)
        # with self.executor.server.pg_connect():
        # Retrieve PostgreSQL server metadata
        self._pg_get_metadata(backup_info)

        # Issue _pg_start_backup on the PostgreSQL server
        self.current_action = "issuing start backup command"
        _logger.debug(self.current_action)
        label = "Barman backup %s %s" % (
            backup_info.server_name, backup_info.backup_id)

        # Concurrent backup: issue a pgespresso_start_Backup() command

        postgres = self.executor.server.postgres
        start_row = postgres.pgespresso_start_backup(label)
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

    def stop_backup(self, backup_info):
        """
        Stop backup wrapper

        :param barman.infofile.BackupInfo backup_info: backup information
        """
        postgres = self.executor.server.postgres
        stop_row = postgres.pgespresso_stop_backup(backup_info.backup_label)
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
                            'pgespresso_abort_backup() on your PostgreSQL '
                            'server')
        self.current_action = "writing backup label"
        self._write_backup_label(backup_info)

    def check(self, check_strategy):
        """
        Perform additional checks for ConcurrentBackupStrategy

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        if self.executor.server.postgres.has_pgespresso:
            check_strategy.result(self.executor.config.name,
                                  'pgespresso extension', True)
        else:
            check_strategy.result(self.executor.config.name,
                                  'pgespresso extension', False,
                                  'required for concurrent backups')
