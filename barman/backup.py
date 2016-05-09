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
This module represents a backup.
"""

import datetime
import logging
import os
import shutil
import time
from glob import glob

import dateutil.parser
import dateutil.tz

from barman import output, xlog
from barman.backup_executor import RsyncBackupExecutor, SshCommandException
from barman.command_wrappers import DataTransferFailure
from barman.compression import CompressionIncompatibility, CompressionManager
from barman.config import BackupOptions
from barman.hooks import (AbortedRetryHookScript, HookScriptRunner,
                          RetryHookScriptRunner)
from barman.infofile import BackupInfo, UnknownBackupIdException, WalFileInfo
from barman.recovery_executor import RecoveryExecutor
from barman.remote_status import RemoteStatusMixin
from barman.utils import fsync_dir, human_readable_timedelta, pretty_size

_logger = logging.getLogger(__name__)


class DuplicateWalFile(Exception):
    """
    A duplicate WAL file has been found
    """
    pass


class MatchingDuplicateWalFile(DuplicateWalFile):
    """
    A duplicate WAL file has been found, but it's identical to the one we
    already have.
    """
    pass


class BackupManager(RemoteStatusMixin):
    """Manager of the backup archive for a server"""

    DEFAULT_STATUS_FILTER = (BackupInfo.DONE,)

    def __init__(self, server):
        """
        Constructor
        """
        super(BackupManager, self).__init__()
        self.name = "default"
        self.server = server
        self.config = server.config
        self._backup_cache = None
        self.compression_manager = CompressionManager(self.config, server.path)
        self.executor = None
        try:
            self.executor = RsyncBackupExecutor(self)
        except SshCommandException as e:
            self.config.disabled = True
            self.config.msg_list.append(str(e).strip())

    def get_available_backups(self, status_filter=DEFAULT_STATUS_FILTER):
        """
        Get a list of available backups

        :param status_filter: default DEFAULT_STATUS_FILTER. The status of
            the backup list returned
        """
        # If the filter is not a tuple, create a tuple using the filter
        if not isinstance(status_filter, tuple):
            status_filter = tuple(status_filter,)
        # Load the cache if necessary
        if self._backup_cache is None:
            self._load_backup_cache()
        # Filter the cache using the status filter tuple
        backups = {}
        for key, value in self._backup_cache.items():
            if value.status in status_filter:
                backups[key] = value
        return backups

    def _load_backup_cache(self):
        """
        Populate the cache of the available backups, reading information
        from disk.
        """
        self._backup_cache = {}
        # Load all the backups from disk reading the backup.info files
        for filename in glob("%s/*/backup.info" %
                             self.config.basebackups_directory):
            backup = BackupInfo(self.server, filename)
            self._backup_cache[backup.backup_id] = backup

    def backup_cache_add(self, backup_info):
        """
        Register a BackupInfo object to the backup cache.

        NOTE: Initialise the cache - in case it has not been done yet

        :param barman.infofile.BackupInfo backup_info: the object we want to
            register in the cache
        """
        # Load the cache if needed
        if self._backup_cache is None:
            self._load_backup_cache()
        # Insert the BackupInfo object into the cache
        self._backup_cache[backup_info.backup_id] = backup_info

    def backup_cache_remove(self, backup_info):
        """
        Remove a BackupInfo object from the backup cache

        This method _must_ be called after removing the object from disk.

        :param barman.infofile.BackupInfo backup_info: the object we want to
            remove from the cache
        """
        # Nothing to do if the cache is not loaded
        if self._backup_cache is None:
            return
        # Remove the BackupInfo object from the backups cache
        del self._backup_cache[backup_info.backup_id]

    def get_backup(self, backup_id):
        """
        Return the backup information for the given backup id.

        If the backup_id is None or backup.info file doesn't exists,
        it returns None.

        :param str|None backup_id: the ID of the backup to return
        :rtype: BackupInfo|None
        """
        if backup_id is not None:
            # Get all the available backups from the cache
            available_backups = self.get_available_backups(
                BackupInfo.STATUS_ALL)
            # Return the BackupInfo if present, or None
            return available_backups.get(backup_id)
        return None

    def get_previous_backup(self, backup_id,
                            status_filter=DEFAULT_STATUS_FILTER):
        """
        Get the previous backup (if any) in the catalog

        :param status_filter: default DEFAULT_STATUS_FILTER. The status of
            the backup returned
        """
        if not isinstance(status_filter, tuple):
            status_filter = tuple(status_filter)
        backup = BackupInfo(self.server, backup_id=backup_id)
        available_backups = self.get_available_backups(status_filter +
                                                       (backup.status,))
        ids = sorted(available_backups.keys())
        try:
            current = ids.index(backup_id)
            while current > 0:
                res = available_backups[ids[current - 1]]
                if res.status in status_filter:
                    return res
                current -= 1
            return None
        except ValueError:
            raise UnknownBackupIdException('Could not find backup_id %s' %
                                           backup_id)

    def get_next_backup(self, backup_id, status_filter=DEFAULT_STATUS_FILTER):
        """
        Get the next backup (if any) in the catalog

        :param status_filter: default DEFAULT_STATUS_FILTER. The status of
            the backup returned
        """
        if not isinstance(status_filter, tuple):
            status_filter = tuple(status_filter)
        backup = BackupInfo(self.server, backup_id=backup_id)
        available_backups = self.get_available_backups(status_filter +
                                                       (backup.status,))
        ids = sorted(available_backups.keys())
        try:
            current = ids.index(backup_id)
            while current < (len(ids) - 1):
                res = available_backups[ids[current + 1]]
                if res.status in status_filter:
                    return res
                current += 1
            return None
        except ValueError:
            raise UnknownBackupIdException('Could not find backup_id %s' %
                                           backup_id)

    def get_last_backup_id(self, status_filter=DEFAULT_STATUS_FILTER):
        """
        Get the id of the latest/last backup in the catalog (if exists)

        :param status_filter: The status of the backup to return,
            default to DEFAULT_STATUS_FILTER.
        :return string|None: ID of the backup
        """
        available_backups = self.get_available_backups(status_filter)
        if len(available_backups) == 0:
            return None

        ids = sorted(available_backups.keys())
        return ids[-1]

    def get_first_backup_id(self, status_filter=DEFAULT_STATUS_FILTER):
        """
        Get the id of the oldest/first backup in the catalog (if exists)

        :param status_filter: The status of the backup to return,
            default to DEFAULT_STATUS_FILTER.
        :return string|None: ID of the backup
        """
        available_backups = self.get_available_backups(status_filter)
        if len(available_backups) == 0:
            return None

        ids = sorted(available_backups.keys())
        return ids[0]

    def delete_backup(self, backup):
        """
        Delete a backup

        :param backup: the backup to delete
        """
        available_backups = self.get_available_backups()
        minimum_redundancy = self.server.config.minimum_redundancy
        # Honour minimum required redundancy
        if backup.status == BackupInfo.DONE and \
                minimum_redundancy >= len(available_backups):
            output.warning("Skipping delete of backup %s for server %s "
                           "due to minimum redundancy requirements "
                           "(minimum redundancy = %s, "
                           "current redundancy = %s)",
                           backup.backup_id,
                           self.config.name,
                           len(available_backups),
                           minimum_redundancy)
            return

        output.info("Deleting backup %s for server %s",
                    backup.backup_id, self.config.name)
        previous_backup = self.get_previous_backup(backup.backup_id)
        next_backup = self.get_next_backup(backup.backup_id)
        # Delete all the data contained in the backup
        try:
            self.delete_backup_data(backup)
        except OSError as e:
            output.error("Failure deleting backup %s for server %s.\n%s",
                         backup.backup_id, self.config.name, e)
            return
        # Check if we are deleting the first available backup
        if not previous_backup:
            # In the case of exclusive backup (default), removes any WAL
            # files associated to the backup being deleted.
            # In the case of concurrent backup, removes only WAL files
            # prior to the start of the backup being deleted, as they
            # might be useful to any concurrent backup started immediately
            # after.
            remove_until = None  # means to remove all WAL files
            if next_backup:
                remove_until = next_backup
            elif BackupOptions.CONCURRENT_BACKUP in self.config.backup_options:
                remove_until = backup
            output.info("Delete associated WAL segments:")
            for name in self.remove_wal_before_backup(remove_until):
                output.info("\t%s", name)
        # As last action, remove the backup directory,
        # ending the delete operation
        try:
            self.delete_basebackup(backup)
        except OSError as e:
            output.error("Failure deleting backup %s for server %s.\n%s\n"
                         "Please manually remove the '%s' directory",
                         backup.backup_id, self.config.name, e,
                         backup.get_basebackup_directory())
            return
        self.backup_cache_remove(backup)
        output.info("Done")

    def retry_backup_copy(self, target_function, *args, **kwargs):
        """
        Execute the target backup copy function, retrying the configured
        number of times

        :param target_function: the base backup target function
        :param args: args for the target function
        :param kwargs: kwargs of the target function
        :return: the result of the target function
        """
        attempts = 0
        while True:
            try:
                # if is not the first attempt, output the retry number
                if attempts >= 1:
                    output.warning("Copy of base backup: retry #%s", attempts)
                # execute the target function for backup copy
                return target_function(*args, **kwargs)
            # catch rsync errors
            except DataTransferFailure as e:
                # exit condition: if retry number is lower than configured
                # retry limit, try again; otherwise exit.
                if attempts < self.config.basebackup_retry_times:
                    # Log the exception, for debugging purpose
                    _logger.exception("Failure in base backup copy: %s", e)
                    output.warning(
                        "Copy of base backup failed, waiting for next "
                        "attempt in %s seconds",
                        self.config.basebackup_retry_sleep)
                    # sleep for configured time. then try again
                    time.sleep(self.config.basebackup_retry_sleep)
                    attempts += 1
                else:
                    # if the max number of attempts is reached and
                    # there is still an error, exit re-raising the exception.
                    raise

    def backup(self):
        """
        Performs a backup for the server
        """
        _logger.debug("initialising backup information")
        self.executor.init()
        backup_info = None
        try:
            # Create the BackupInfo object representing the backup
            backup_info = BackupInfo(
                self.server,
                backup_id=datetime.datetime.now().strftime('%Y%m%dT%H%M%S'))
            backup_info.save()
            self.backup_cache_add(backup_info)
            output.info(
                "Starting backup for server %s in %s",
                self.config.name,
                backup_info.get_basebackup_directory())

            # Run the pre-backup-script if present.
            script = HookScriptRunner(self, 'backup_script', 'pre')
            script.env_from_backup_info(backup_info)
            script.run()

            # Run the pre-backup-retry-script if present.
            retry_script = RetryHookScriptRunner(
                self, 'backup_retry_script', 'pre')
            retry_script.env_from_backup_info(backup_info)
            retry_script.run()

            # Do the backup using the BackupExecutor
            self.executor.backup(backup_info)

            # Compute backup size and fsync it on disk
            self.backup_fsync_and_set_sizes(backup_info)

            # Mark the backup as DONE
            backup_info.set_attribute("status", "DONE")
        # Use BaseException instead of Exception to catch events like
        # KeyboardInterrupt (e.g.: CRTL-C)
        except BaseException as e:
            msg_lines = str(e).strip().splitlines()
            if backup_info:
                # Use only the first line of exception message
                # in backup_info error field
                backup_info.set_attribute("status", "FAILED")
                # If the exception has no attached message use the raw
                # type name
                if len(msg_lines) == 0:
                    msg_lines = [type(e).__name__]
                backup_info.set_attribute(
                    "error",
                    "failure %s (%s)" % (
                        self.executor.current_action, msg_lines[0]))

            output.error("Backup failed %s.\nDETAILS: %s\n%s",
                         self.executor.current_action, msg_lines[0],
                         '\n'.join(msg_lines[1:]))

        else:
            output.info("Backup end at xlog location: %s (%s, %08X)",
                        backup_info.end_xlog,
                        backup_info.end_wal,
                        backup_info.end_offset)
            output.info("Backup completed")
            # Create a restore point after a backup
            target_name = 'barman_%s' % backup_info.backup_id
            self.server.postgres.create_restore_point(target_name)
        finally:
            if backup_info:
                backup_info.save()

                # Make sure we are not holding any PostgreSQL connection
                # during the post-backup scripts
                self.server.close()

                # Run the post-backup-retry-script if present.
                try:
                    retry_script = RetryHookScriptRunner(
                        self, 'backup_retry_script', 'post')
                    retry_script.env_from_backup_info(backup_info)
                    retry_script.run()
                except AbortedRetryHookScript as e:
                    # Ignore the ABORT_STOP as it is a post-hook operation
                    _logger.warning("Ignoring stop request after receiving "
                                    "abort (exit code %d) from post-backup "
                                    "retry hook script: %s",
                                    e.hook.exit_status, e.hook.script)

                # Run the post-backup-script if present.
                script = HookScriptRunner(self, 'backup_script', 'post')
                script.env_from_backup_info(backup_info)
                script.run()

        output.result('backup', backup_info)

    def recover(self, backup_info, dest, tablespaces=None, target_tli=None,
                target_time=None, target_xid=None, target_name=None,
                exclusive=False, remote_command=None):
        """
        Performs a recovery of a backup

        :param barman.infofile.BackupInfo backup_info: the backup to recover
        :param str dest: the destination directory
        :param dict[str,str]|None tablespaces: a tablespace name -> location
            map (for relocation)
        :param str|None target_tli: the target timeline
        :param str|None target_time: the target time
        :param str|None target_xid: the target xid
        :param str|None target_name: the target name created previously with
            pg_create_restore_point() function call
        :param bool exclusive: whether the recovery is exclusive or not
        :param str|None remote_command: default None. The remote command
            to recover the base backup, in case of remote backup.
        """

        # Archive every WAL files in the incoming directory of the server
        self.server.archive_wal(verbose=False)
        # Delegate the recovery operation to a RecoveryExecutor object
        executor = RecoveryExecutor(self)
        recovery_info = executor.recover(backup_info,
                                         dest, tablespaces,
                                         target_tli, target_time,
                                         target_xid, target_name,
                                         exclusive, remote_command)

        # Output recovery results
        output.result('recovery', recovery_info['results'])

    def archive_wal(self, verbose=True):
        """
        Executes WAL maintenance operations, such as archiving and compression

        If verbose is set to False, outputs something only if there is
        at least one file

        :param bool verbose: report even if no actions
        """
        with self.server.xlogdb('a') as fxlogdb:
            for archiver in self.server.archivers:
                archiver.archive(fxlogdb, verbose)

    def cron_retention_policy(self):
        """
        Retention policy management
        """
        if (self.server.enforce_retention_policies and
                self.config.retention_policy_mode == 'auto'):
            available_backups = self.get_available_backups(
                BackupInfo.STATUS_ALL)
            retention_status = self.config.retention_policy.report()
            for bid in sorted(retention_status.keys()):
                if retention_status[bid] == BackupInfo.OBSOLETE:
                    output.info(
                        "Enforcing retention policy: removing backup %s for "
                        "server %s" % (bid, self.config.name))
                    self.delete_backup(available_backups[bid])

    def delete_basebackup(self, backup):
        """
        Delete the basebackup dir of a given backup.

        :param barman.infofile.BackupInfo backup: the backup to delete
        """
        backup_dir = backup.get_basebackup_directory()
        _logger.debug("Deleting base backup directory: %s" % backup_dir)
        shutil.rmtree(backup_dir)

    def delete_backup_data(self, backup):
        """
        Delete the data contained in a given backup.

        :param barman.infofile.BackupInfo backup: the backup to delete
        """
        if backup.tablespaces:
            if backup.backup_version == 2:
                tbs_dir = backup.get_basebackup_directory()
            else:
                tbs_dir = os.path.join(backup.get_data_directory(),
                                       'pg_tblspc')
            for tablespace in backup.tablespaces:
                rm_dir = os.path.join(tbs_dir, str(tablespace.oid))
                if os.path.exists(rm_dir):
                    _logger.debug("Deleting tablespace %s directory: %s" %
                                  (tablespace.name, rm_dir))
                    shutil.rmtree(rm_dir)

        pg_data = backup.get_data_directory()
        if os.path.exists(pg_data):
            _logger.debug("Deleting PGDATA directory: %s" % pg_data)
            shutil.rmtree(pg_data)

    def delete_wal(self, wal_info):
        """
        Delete a WAL segment, with the given WalFileInfo

        :param barman.infofile.WalFileInfo wal_info: the WAL to delete
        """

        try:
            os.unlink(wal_info.fullpath(self.server))
            try:
                os.removedirs(os.path.dirname(wal_info.fullpath(self.server)))
            except OSError:
                # This is not an error condition
                # We always try to remove the the trailing directories,
                # this means that hashdir is not empty.
                pass
        except OSError as e:
            output.warning('Ignoring deletion of WAL file %s '
                           'for server %s: %s',
                           wal_info.name, self.config.name, e)

    def check(self, check_strategy):
        """
        This function does some checks on the server.

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        # Check compression_setting parameter
        if self.config.compression and not self.compression_manager.check():
            check_strategy.result(self.config.name,
                                  'compression settings', False)
        else:
            status = True
            try:
                self.compression_manager.get_compressor()
            except CompressionIncompatibility as field:
                check_strategy.result(self.config.name,
                                      '%s setting' % field, False)
                status = False
            check_strategy.result(self.config.name,
                                  'compression settings', status)

        # Failed backups check
        failed_backups = self.get_available_backups((BackupInfo.FAILED,))
        status = len(failed_backups) == 0
        check_strategy.result(
            self.config.name,
            'failed backups',
            status,
            'there are %s failed backups' % (len(failed_backups,))
        )

        # Minimum redundancy checks
        no_backups = len(self.get_available_backups())
        # Check minimum_redundancy_requirements parameter
        if no_backups < int(self.config.minimum_redundancy):
            status = False
        else:
            status = True
        check_strategy.result(
            self.config.name,
            'minimum redundancy requirements', status,
            'have %s backups, expected at least %s' % (
                no_backups, self.config.minimum_redundancy))

        # TODO: Add a check for the existence of ssh and of rsync

        # Execute additional checks defined by the BackupExecutor
        if self.executor:
            self.executor.check(check_strategy)

    def status(self):
        """
        This function show the server status
        """
        # get number of backups
        no_backups = len(self.get_available_backups())
        output.result('status', self.config.name,
                      "backups_number",
                      "No. of available backups", no_backups)
        output.result('status', self.config.name,
                      "first_backup",
                      "First available backup",
                      self.get_first_backup_id())
        output.result('status', self.config.name,
                      "last_backup",
                      "Last available backup",
                      self.get_last_backup_id())
        # Minimum redundancy check. if number of backups minor than minimum
        # redundancy, fail.
        if no_backups < self.config.minimum_redundancy:
            output.result('status', self.config.name,
                          "minimum_redundancy",
                          "Minimum redundancy requirements",
                          "FAILED (%s/%s)" % (
                              no_backups,
                              self.config.minimum_redundancy))
        else:
            output.result('status', self.config.name,
                          "minimum_redundancy",
                          "Minimum redundancy requirements",
                          "satisfied (%s/%s)" % (
                              no_backups,
                              self.config.minimum_redundancy))

        # Output additional status defined by the BackupExecutor
        if self.executor:
            self.executor.status()

    def fetch_remote_status(self):
        """
        Build additional remote status lines defined by the BackupManager.

        This method does not raise any exception in case of errors,
        but set the missing values to None in the resulting dictionary.

        :rtype: dict[str, None|str]
        """
        if self.executor:
            return self.executor.get_remote_status()
        else:
            return {}

    def rebuild_xlogdb(self):
        """
        Rebuild the whole xlog database guessing it from the archive content.
        """
        from os.path import isdir, join

        output.info("Rebuilding xlogdb for server %s", self.config.name)
        root = self.config.wals_directory
        default_compression = self.config.compression
        wal_count = label_count = history_count = 0
        # lock the xlogdb as we are about replacing it completely
        with self.server.xlogdb('w') as fxlogdb:
            xlogdb_new = fxlogdb.name + ".new"
            with open(xlogdb_new, 'w') as fxlogdb_new:
                for name in sorted(os.listdir(root)):
                    # ignore the xlogdb and its lockfile
                    if name.startswith(self.server.XLOG_DB):
                        continue
                    fullname = join(root, name)
                    if isdir(fullname):
                        # all relevant files are in subdirectories
                        hash_dir = fullname
                        for wal_name in sorted(os.listdir(hash_dir)):
                            fullname = join(hash_dir, wal_name)
                            if isdir(fullname):
                                _logger.warning(
                                    'unexpected directory '
                                    'rebuilding the wal database: %s',
                                    fullname)
                            else:
                                if xlog.is_wal_file(fullname):
                                    wal_count += 1
                                elif xlog.is_backup_file(fullname):
                                    label_count += 1
                                else:
                                    _logger.warning(
                                        'unexpected file '
                                        'rebuilding the wal database: %s',
                                        fullname)
                                    continue
                                wal_info = WalFileInfo.from_file(
                                    fullname,
                                    default_compression=default_compression)
                                fxlogdb_new.write(wal_info.to_xlogdb_line())
                    else:
                        # only history files are here
                        if xlog.is_history_file(fullname):
                            history_count += 1
                            wal_info = WalFileInfo.from_file(
                                fullname,
                                default_compression=default_compression)
                            fxlogdb_new.write(wal_info.to_xlogdb_line())
                        else:
                            _logger.warning(
                                'unexpected file '
                                'rebuilding the wal database: %s',
                                fullname)
                os.fsync(fxlogdb_new.fileno())
            shutil.move(xlogdb_new, fxlogdb.name)
            fsync_dir(os.path.dirname(fxlogdb.name))
        output.info('Done rebuilding xlogdb for server %s '
                    '(history: %s, backup_labels: %s, wal_file: %s)',
                    self.config.name, history_count, label_count, wal_count)

    def remove_wal_before_backup(self, backup_info):
        """
        Remove WAL files which have been archived before the start of
        the provided backup.

        If no backup_info is provided delete all available WAL files

        :param BackupInfo|None backup_info: the backup information structure
        :return list: a list of removed WAL files
        """
        removed = []
        with self.server.xlogdb() as fxlogdb:
            xlogdb_new = fxlogdb.name + ".new"
            with open(xlogdb_new, 'w') as fxlogdb_new:
                for line in fxlogdb:
                    wal_info = WalFileInfo.from_xlogdb_line(line)
                    if not xlog.is_any_xlog_file(wal_info.name):
                        output.error(
                            "invalid xlog segment name %r\n"
                            "HINT: Please run \"barman rebuild-xlogdb %s\" "
                            "to solve this issue",
                            wal_info.name, self.config.name)
                        continue
                    # Keeps the WAL segment if it is a history file or later
                    # than the given backup (the first available)
                    if (xlog.is_history_file(wal_info.name) or
                            (backup_info and
                                wal_info.name >= backup_info.begin_wal)):
                        fxlogdb_new.write(wal_info.to_xlogdb_line())
                        continue
                    else:
                        self.delete_wal(wal_info)
                        removed.append(wal_info.name)
                fxlogdb_new.flush()
                os.fsync(fxlogdb_new.fileno())
            shutil.move(xlogdb_new, fxlogdb.name)
            fsync_dir(os.path.dirname(fxlogdb.name))
        return removed

    def validate_last_backup_maximum_age(self, last_backup_maximum_age):
        """
        Evaluate the age of the last available backup in a catalogue.
        If the last backup is older than the specified time interval (age),
        the function returns False. If within the requested age interval,
        the function returns True.

        :param timedate.timedelta last_backup_maximum_age: time interval
            representing the maximum allowed age for the last backup
            in a server catalogue
        :return tuple: a tuple containing the boolean result of the check and
            auxiliary information about the last backup current age
        """
        # Get the ID of the last available backup
        backup_id = self.get_last_backup_id()
        if backup_id:
            # Get the backup object
            backup = BackupInfo(self.server, backup_id=backup_id)
            now = datetime.datetime.now(dateutil.tz.tzlocal())
            # Evaluate the point of validity
            validity_time = now - last_backup_maximum_age
            # Pretty print of a time interval (age)
            msg = human_readable_timedelta(now - backup.end_time)
            # If the backup end time is older than the point of validity,
            # return False, otherwise return true
            if backup.end_time < validity_time:
                return False, msg
            else:
                return True, msg
        else:
            # If no backup is available return false
            return False, "No available backups"

    def backup_fsync_and_set_sizes(self, backup_info):
        """
        Fsync all files in a backup and set the actual size on disk
        of a backup.

        Also evaluate the deduplication ratio and the deduplicated size if
        applicable.

        :param barman.infofile.BackupInfo backup_info: the backup to update
        """
        # Calculate the base backup size
        self.executor.current_action = "calculating backup size"
        _logger.debug(self.executor.current_action)
        backup_size = 0
        deduplicated_size = 0
        backup_dest = backup_info.get_basebackup_directory()
        for dir_path, _, file_names in os.walk(backup_dest):
            # execute fsync() on the containing directory
            fsync_dir(dir_path)
            # execute fsync() on all the contained files
            for filename in file_names:
                file_path = os.path.join(dir_path, filename)
                file_fd = os.open(file_path, os.O_RDONLY)
                file_stat = os.fstat(file_fd)
                backup_size += file_stat.st_size
                # Excludes hard links from real backup size
                if file_stat.st_nlink == 1:
                    deduplicated_size += file_stat.st_size
                os.fsync(file_fd)
                os.close(file_fd)
        # Save size into BackupInfo object
        backup_info.set_attribute('size', backup_size)
        backup_info.set_attribute('deduplicated_size', deduplicated_size)
        if backup_info.size > 0:
            deduplication_ratio = 1 - (float(
                backup_info.deduplicated_size) / backup_info.size)
        else:
            deduplication_ratio = 0

        if self.config.reuse_backup == 'link':
            output.info(
                "Backup size: %s. Actual size on disk: %s"
                " (-%s deduplication ratio)." % (
                    pretty_size(backup_info.size),
                    pretty_size(backup_info.deduplicated_size),
                    '{percent:.2%}'.format(percent=deduplication_ratio)
                ))
        else:
            output.info("Backup size: %s" %
                        pretty_size(backup_info.size))
