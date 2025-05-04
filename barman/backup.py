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
This module represents a backup.
"""

import datetime
import logging
import os
import re
import shutil
import tempfile
from collections import defaultdict
from contextlib import closing
from glob import glob

import dateutil.parser
import dateutil.tz

from barman import output, xlog
from barman.annotations import AnnotationManagerFile, KeepManager, KeepManagerMixin
from barman.backup_executor import (
    PassiveBackupExecutor,
    PostgresBackupExecutor,
    RsyncBackupExecutor,
    SnapshotBackupExecutor,
)
from barman.backup_manifest import BackupManifest
from barman.cloud_providers import get_snapshot_interface_from_backup_info
from barman.command_wrappers import PgVerifyBackup
from barman.compression import CompressionManager
from barman.config import BackupOptions
from barman.encryption import EncryptionManager
from barman.exceptions import (
    AbortedRetryHookScript,
    BackupException,
    CommandFailedException,
    CompressionIncompatibility,
    LockFileBusy,
    SshCommandException,
    UnknownBackupIdException,
)
from barman.fs import unix_command_factory
from barman.hooks import HookScriptRunner, RetryHookScriptRunner
from barman.infofile import BackupInfo, LocalBackupInfo, WalFileInfo
from barman.lockfile import ServerBackupIdLock, ServerBackupSyncLock
from barman.recovery_executor import recovery_executor_factory
from barman.remote_status import RemoteStatusMixin
from barman.storage.local_file_manager import LocalFileManager
from barman.utils import (
    SHA256,
    force_str,
    fsync_dir,
    fsync_file,
    get_backup_id_from_target_lsn,
    get_backup_id_from_target_time,
    get_backup_id_from_target_tli,
    get_backup_info_from_name,
    get_last_backup_id,
    human_readable_timedelta,
    pretty_size,
)

_logger = logging.getLogger(__name__)


class BackupManager(RemoteStatusMixin, KeepManagerMixin):
    """Manager of the backup archive for a server"""

    DEFAULT_STATUS_FILTER = BackupInfo.STATUS_COPY_DONE
    DELETE_ANNOTATION = "delete_this"
    DEFAULT_BACKUP_TYPE_FILTER = BackupInfo.BACKUP_TYPE_ALL

    def __init__(self, server):
        """
        Constructor
        :param server: barman.server.Server
        """
        super(BackupManager, self).__init__(server=server)
        self.server = server
        self.config = server.config
        self._backup_cache = None
        self.compression_manager = CompressionManager(self.config, server.path)
        self.encryption_manager = EncryptionManager(self.config, server.path)
        self.annotation_manager = AnnotationManagerFile(
            self.server.meta_directory, self.server.config.basebackups_directory
        )
        self.executor = None
        try:
            if server.passive_node:
                self.executor = PassiveBackupExecutor(self)
            elif self.config.backup_method == "postgres":
                self.executor = PostgresBackupExecutor(self)
            elif self.config.backup_method == "local-rsync":
                self.executor = RsyncBackupExecutor(self, local_mode=True)
            elif self.config.backup_method == "snapshot":
                self.executor = SnapshotBackupExecutor(self)
            else:
                self.executor = RsyncBackupExecutor(self)
        except SshCommandException as e:
            self.config.update_msg_list_and_disable_server(force_str(e).strip())

    @property
    def mode(self):
        """
        Property defining the BackupInfo mode content
        """
        if self.executor:
            return self.executor.mode
        return None

    def get_available_backups(
        self,
        status_filter=DEFAULT_STATUS_FILTER,
        backup_type_filter=DEFAULT_BACKUP_TYPE_FILTER,
    ):
        """
        Get a list of available backups

        :param status_filter: default DEFAULT_STATUS_FILTER. The status of
            the backup list returned
        :param backup_type_filter: default DEFAULT_BACKUP_TYPE_FILTER. The type
            of the backup list returned
        """
        # If the status filter is not a tuple, create a tuple using the filter
        if not isinstance(status_filter, tuple):
            status_filter = tuple(
                status_filter,
            )
        # If the backup_type filter is not a tuple, create a tuple using the filter
        if not isinstance(backup_type_filter, tuple):
            backup_type_filter = tuple(
                backup_type_filter,
            )
        # Load the cache if necessary
        if self._backup_cache is None:
            self._load_backup_cache()
        # Filter the cache using the status filter tuple
        backups = {}
        for key, value in self._backup_cache.items():
            if (
                value.status in status_filter
                and value.backup_type in backup_type_filter
            ):
                backups[key] = value
        return backups

    def _load_backup_cache(self):
        """
        Populate the cache of the available backups, reading information
        from disk.
        """
        self._backup_cache = {}
        # Previous to version 3.13.2, Barman used to store the backup.info file
        # alongside with the base backup. While that, in general, is not a problem,
        # when dealing with WORM environments that could cause issues as the
        # base backups are expected to be stored in an immutable storage. This
        # code is only maintained as a fallback mechanism during a transient state
        # in the backup catalog, where we will find backup.info files in both
        # locations because of backups taken with < 3.13.2
        for filename in glob("%s/*/backup.info" % self.config.basebackups_directory):
            backup = LocalBackupInfo(self.server, filename)
            self._backup_cache[backup.backup_id] = backup
        # In version 3.13.2, Barman changed the location of backup.info files.
        # That was done so we have common location for the metadata, which
        # should always be in a mutable storage, independently if worm_mode
        # is enabled or not. So, this new approach takes precedence.
        for filename in glob("%s/*-backup.info" % self.server.meta_directory):
            backup = LocalBackupInfo(self.server, filename)
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
            available_backups = self.get_available_backups(BackupInfo.STATUS_ALL)
            # Return the BackupInfo if present, or None
            return available_backups.get(backup_id)
        return None

    @staticmethod
    def find_previous_backup_in(
        available_backups, backup_id, status_filter=DEFAULT_STATUS_FILTER
    ):
        """
        Find the next backup (if any) in the supplied dict of BackupInfo objects.
        """
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
            raise UnknownBackupIdException("Could not find backup_id %s" % backup_id)

    def get_previous_backup(self, backup_id, status_filter=DEFAULT_STATUS_FILTER):
        """
        Get the previous backup (if any) in the catalog

        :param status_filter: default DEFAULT_STATUS_FILTER. The status of
            the backup returned
        """
        if not isinstance(status_filter, tuple):
            status_filter = tuple(status_filter)
        backup = LocalBackupInfo(self.server, backup_id=backup_id)
        available_backups = self.get_available_backups(status_filter + (backup.status,))
        return self.find_previous_backup_in(available_backups, backup_id, status_filter)

    @staticmethod
    def should_remove_wals(
        backup,
        available_backups,
        keep_manager,
        skip_wal_cleanup_if_standalone,
        status_filter=DEFAULT_STATUS_FILTER,
    ):
        """
        Determine whether we should remove the WALs for the specified backup.

        Returns the following tuple:

           - `(bool should_remove_wals, list wal_ranges_to_protect)`

        Where `should_remove_wals` is a boolean which is True if the WALs associated
        with this backup should be removed and False otherwise.

        `wal_ranges_to_protect` is a list of `(begin_wal, end_wal)` tuples which define
        *inclusive* ranges where any matching WAL should not be deleted.

        The rules for determining whether we should remove WALs are as follows:

          1. If there is no previous backup then we can clean up the WALs.
          2. If there is a previous backup and it has no keep annotation then do
             not clean up the WALs. We need to allow PITR from that older backup
             to the current time.
          3. If there is a previous backup and it has a keep target of "full" then
             do nothing. We need to allow PITR from that keep:full backup to the
             current time.
          4. If there is a previous backup and it has a keep target of "standalone":
            a. If that previous backup is the oldest backup then delete WALs up to
               the begin_wal of the next backup except for WALs which are
               >= begin_wal and <= end_wal of the keep:standalone backup - we can
               therefore add `(begin_wal, end_wal)` to `wal_ranges_to_protect` and
               return True.
            b. If that previous backup is not the oldest backup then we add the
               `(begin_wal, end_wal)` to `wal_ranges_to_protect` and go to 2 above.
               We will either end up returning False, because we hit a backup with
               keep:full or no keep annotation, or all backups to the oldest backup
               will be keep:standalone in which case we will delete up to the
               begin_wal of the next backup, preserving the WALs needed by each
               keep:standalone backups by adding them to `wal_ranges_to_protect`.

        This is a static method so it can be re-used by barman-cloud which will
        pass in its own dict of available_backups.

        :param BackupInfo backup_info: The backup for which we are determining
          whether we can clean up WALs.
        :param dict[str,BackupInfo] available_backups: A dict of BackupInfo
          objects keyed by backup_id which represent all available backups for
          the current server.
        :param KeepManagerMixin keep_manager: An object implementing the
          KeepManagerMixin interface. This will be either a BackupManager (in
          barman) or a CloudBackupCatalog (in barman-cloud).
        :param bool skip_wal_cleanup_if_standalone: If set to True then we should
          skip removing WALs for cases where all previous backups are standalone
          archival backups (i.e. they have a keep annotation of "standalone").
          The default is True. It is only safe to set this to False if the backup
          is being deleted due to a retention policy rather than a `barman delete`
          command.
        :param status_filter: The status of the backups to check when determining
          if we should remove WALs. default to DEFAULT_STATUS_FILTER.
        """
        previous_backup = BackupManager.find_previous_backup_in(
            available_backups, backup.backup_id, status_filter=status_filter
        )
        wal_ranges_to_protect = []
        while True:
            if previous_backup is None:
                # No previous backup so we should remove WALs and return any WAL ranges
                # we have found so far
                return True, wal_ranges_to_protect
            elif (
                keep_manager.get_keep_target(previous_backup.backup_id)
                == KeepManager.TARGET_STANDALONE
            ):
                # A previous backup exists and it is a standalone backup - if we have
                # been asked to skip wal cleanup on standalone backups then we
                # should not remove wals
                if skip_wal_cleanup_if_standalone:
                    return False, []
                # Otherwise we add to the WAL ranges to protect
                wal_ranges_to_protect.append(
                    (previous_backup.begin_wal, previous_backup.end_wal)
                )
                # and continue iterating through previous backups until we find either
                # no previous backup or a non-standalone backup
                previous_backup = BackupManager.find_previous_backup_in(
                    available_backups,
                    previous_backup.backup_id,
                    status_filter=status_filter,
                )
                continue
            else:
                # A previous backup exists and it is not a standalone backup so we
                # must not remove any WALs and we can discard any wal_ranges_to_protect
                # since they are no longer relevant
                return False, []

    @staticmethod
    def find_next_backup_in(
        available_backups, backup_id, status_filter=DEFAULT_STATUS_FILTER
    ):
        """
        Find the next backup (if any) in the supplied dict of BackupInfo objects.
        """
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
            raise UnknownBackupIdException("Could not find backup_id %s" % backup_id)

    def get_next_backup(self, backup_id, status_filter=DEFAULT_STATUS_FILTER):
        """
        Get the next backup (if any) in the catalog

        :param status_filter: default DEFAULT_STATUS_FILTER. The status of
            the backup returned
        """
        if not isinstance(status_filter, tuple):
            status_filter = tuple(status_filter)
        backup = LocalBackupInfo(self.server, backup_id=backup_id)
        available_backups = self.get_available_backups(status_filter + (backup.status,))
        return self.find_next_backup_in(available_backups, backup_id, status_filter)

    def get_last_backup_id(self, status_filter=DEFAULT_STATUS_FILTER):
        """
        Get the id of the latest/last backup in the catalog (if exists)

        :param status_filter: The status of the backup to return,
            default to :attr:`DEFAULT_STATUS_FILTER`.
        :return str|None: ID of the backup
        """
        available_backups = self.get_available_backups(status_filter).values()
        backup_id = get_last_backup_id(available_backups)
        return backup_id

    def get_last_full_backup_id(self, status_filter=DEFAULT_STATUS_FILTER):
        """
        Get the id of the latest/last FULL backup in the catalog (if exists)

        :param status_filter: The status of the backup to return,
            default to :attr:`DEFAULT_STATUS_FILTER`.
        :return str|None: ID of the backup
        """
        available_full_backups = list(
            filter(
                lambda backup: backup.is_full,
                self.get_available_backups(status_filter).values(),
            )
        )

        if len(available_full_backups) == 0:
            return None

        backup_infos = sorted(
            available_full_backups, key=lambda backup_info: backup_info.backup_id
        )
        return backup_infos[-1].backup_id

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

    def get_backup_id_from_name(self, backup_name, status_filter=DEFAULT_STATUS_FILTER):
        """
        Get the id of the named backup, if it exists.

        :param string backup_name: The name of the backup for which an ID should be
            returned
        :param tuple status_filter: The status of the backup to return.
        :return string|None: ID of the backup
        """
        available_backups = self.get_available_backups(status_filter).values()
        backup_info = get_backup_info_from_name(available_backups, backup_name)
        if backup_info is not None:
            return backup_info.backup_id

    def get_closest_backup_id_from_target_time(
        self, target_time, target_tli, status_filter=DEFAULT_STATUS_FILTER
    ):
        """
        Get the id of a backup according to the time passed as the recovery target
        *target_time*, and in the given *target_tli*, if specified.

        :param str target_time: The target value with timestamp format
            ``%Y-%m-%d %H:%M:%S`` with or without timezone.
        :param int|None target_tli: The target timeline, if a specific one is required.
        :param tuple[str, ...] status_filter: The status of the backup to return.
        :return str|None: ID of the backup.
        """

        available_backups = self.get_available_backups(status_filter).values()
        backup_id = get_backup_id_from_target_time(
            available_backups, target_time, target_tli
        )
        return backup_id

    def get_closest_backup_id_from_target_lsn(
        self, target_lsn, target_tli, status_filter=DEFAULT_STATUS_FILTER
    ):
        """
        Get the id of a backup according to the lsn passed as the recovery target
        *target_lsn*, and in the given *target_tli*, if specified.

        :param str target_lsn: The target value with lsn format, e.g.,
            ``3/64000000``.
        :param int|None target_tli: The target timeline, if a specific one is required.
        :param tuple[str, ...] status_filter: The status of the backup to return.
        :return str|None: ID of the backup.
        """
        available_backups = self.get_available_backups(status_filter).values()
        backup_id = get_backup_id_from_target_lsn(
            available_backups, target_lsn, target_tli
        )
        return backup_id

    def get_last_backup_id_from_target_tli(
        self, target_tli, status_filter=DEFAULT_STATUS_FILTER
    ):
        """
        Get the id of a backup according to the timeline passed as the recovery target
        *target_tli*.

        :param int target_tli: The target timeline.
        :param tuple[str, ...] status_filter: The status of the backup to return.
        :return str|None: ID of the backup.
        """

        available_backups = self.get_available_backups(status_filter).values()
        backup_id = get_backup_id_from_target_tli(available_backups, target_tli)
        return backup_id

    def put_delete_annotation(self, backup_id):
        """
        Add a delete annotation to the specified backup.

        This method adds an annotation to the backup identified by *backup_id* to mark it
        for deletion. The annotation is stored using the annotation manager.

        :param str backup_id: The ID of the backup to annotate.
        """
        self.annotation_manager.put_annotation(
            backup_id, self.DELETE_ANNOTATION, "delete"
        )

    def check_delete_annotation(self, backup_id):
        """
        Check if a delete annotation exists for the specified backup.

        This method checks if the backup identified by *backup_id* has a delete annotation.
        It returns ``True`` if the annotation exists, otherwise ``False``.

        :param str backup_id: The ID of the backup to check.
        :return bool: ``True`` if the delete annotation exists, ``False`` otherwise.
        """
        return (
            self.annotation_manager.get_annotation(backup_id, self.DELETE_ANNOTATION)
            is not None
        )

    def release_delete_annotation(self, backup_id):
        """
        Remove the delete annotation from the backup identified by *backup_id*.

        :param str backup_id: The ID of the backup to remove the annotation from.
        """
        self.annotation_manager.delete_annotation(backup_id, self.DELETE_ANNOTATION)

    @staticmethod
    def get_timelines_to_protect(remove_until, deleted_backup, available_backups):
        """
        Returns all timelines in available_backups which are not associated with
        the backup at remove_until. This is so that we do not delete WALs on
        any other timelines.
        """
        timelines_to_protect = set()
        # If remove_until is not set there are no backup left
        if remove_until:
            # Retrieve the list of extra timelines that contains at least
            # a backup. On such timelines we don't want to delete any WAL
            for value in available_backups.values():
                # Ignore the backup that is being deleted
                if value == deleted_backup:
                    continue
                timelines_to_protect.add(value.timeline)
            # Remove the timeline of `remove_until` from the list.
            # We have enough information to safely delete unused WAL files
            # on it.
            timelines_to_protect -= set([remove_until.timeline])
        return timelines_to_protect

    def delete_backup(self, backup, skip_wal_cleanup_if_standalone=True):
        """
        Delete a backup

        :param backup: the backup to delete
        :param bool skip_wal_cleanup_if_standalone: By default we will skip removing
          WALs if the oldest backups are standalone archival backups (i.e. they have
          a keep annotation of "standalone"). If this function is being called in the
          context of a retention policy however, it is safe to set
          skip_wal_cleanup_if_standalone to False and clean up WALs associated with those
          backups.
        :return bool: True if deleted, False if could not delete the backup
        """
        # Set the delete annotation
        self.put_delete_annotation(backup.backup_id)

        # Keep track of when the delete operation started.
        delete_start_time = datetime.datetime.now()

        # Run the pre_delete_script if present.
        script = HookScriptRunner(self, "delete_script", "pre")
        script.env_from_backup_info(backup)
        script.run()

        # Run the pre_delete_retry_script if present.
        retry_script = RetryHookScriptRunner(self, "delete_retry_script", "pre")
        retry_script.env_from_backup_info(backup)
        retry_script.run()

        output.info(
            "Deleting backup %s for server %s", backup.backup_id, self.config.name
        )
        should_remove_wals, wal_ranges_to_protect = BackupManager.should_remove_wals(
            backup,
            self.get_available_backups(
                BackupManager.DEFAULT_STATUS_FILTER + (backup.status,)
            ),
            keep_manager=self,
            skip_wal_cleanup_if_standalone=skip_wal_cleanup_if_standalone,
        )

        next_backup = self.get_next_backup(backup.backup_id)
        # Delete all the data contained in the backup
        try:
            self.delete_backup_data(backup)
        except OSError as e:
            output.error(
                "Failure deleting backup %s for server %s.\n%s",
                backup.backup_id,
                self.config.name,
                e,
            )
            return False

        if should_remove_wals:
            # There is no previous backup or all previous backups are archival
            # standalone backups, so we can remove unused WALs (those WALs not
            # required by standalone archival backups).
            # If there is a next backup then all unused WALs up to the begin_wal
            # of the next backup can be removed.
            # If there is no next backup then there are no remaining backups so:
            #   - In the case of exclusive backup, remove all unused WAL files.
            #   - In the case of concurrent backup (the default), removes only
            #     unused WAL files prior to the start of the backup being deleted,
            #     as they might be useful to any concurrent backup started
            #     immediately after.
            remove_until = None  # means to remove all WAL files
            if next_backup:
                remove_until = next_backup
            elif BackupOptions.CONCURRENT_BACKUP in self.config.backup_options:
                remove_until = backup

            timelines_to_protect = self.get_timelines_to_protect(
                remove_until,
                backup,
                self.get_available_backups(BackupInfo.STATUS_ARCHIVING),
            )

            output.info("Delete associated WAL segments:")
            for name in self.remove_wal_before_backup(
                remove_until, timelines_to_protect, wal_ranges_to_protect
            ):
                output.info("\t%s", name)

        # Remove the delete annotation
        self.release_delete_annotation(backup.backup_id)

        # Remove the base backup directory,
        try:
            self.delete_basebackup(backup)
        except OSError as e:
            output.error(
                "Failure deleting backup %s for server %s.\n%s\n"
                "Please manually remove the '%s' directory",
                backup.backup_id,
                self.config.name,
                e,
                backup.get_basebackup_directory(),
            )
            return False

        # As a last action remove, remove the backup.info, ending the delete operation
        try:
            self.delete_backupinfo_file(backup)
        except OSError as e:
            output.error(
                "Failure deleting file %s for server %s.\n%s\n"
                "Please manually remove the file",
                backup.get_filename(),
                self.config.name,
                e,
            )
            return False

        # Save the time of the complete removal of the backup
        delete_end_time = datetime.datetime.now()
        output.info(
            "Deleted backup %s (start time: %s, elapsed time: %s)",
            backup.backup_id,
            delete_start_time.ctime(),
            human_readable_timedelta(delete_end_time - delete_start_time),
        )

        # remove its reference from its parent if it is an incremental backup
        parent_backup = backup.get_parent_backup_info()
        if parent_backup:
            parent_backup.children_backup_ids.remove(backup.backup_id)
            if not parent_backup.children_backup_ids:
                parent_backup.children_backup_ids = None
            parent_backup.save()

        # rsync backups can have deduplication at filesystem level by using
        # "reuse_backup = link". The deduplication size is calculated at the
        # time the backup is taken. If we remove a backup, it may be the case
        # that the next backup in the catalog is a rsync backup which was taken
        # with the "link" option. With that possibility in mind, we re-calculate the
        # deduplicated size of the next rsync backup because the removal of the
        # previous backup can impact on that number.
        # Note: we have no straight forward way of identifying if the next rsync
        # backup in the catalog was taken with "link" or not because
        # "reuse_backup" value is not stored in the "backup.info" file. In any
        # case, the "re-calculation" can still be performed even if "link" was not
        # used, and the only drawback is that we will waste some (small) amount
        # of CPU/disk usage.
        if next_backup and next_backup.backup_type == "rsync":
            self._set_backup_sizes(next_backup)

        # Remove the sync lockfile if exists
        sync_lock = ServerBackupSyncLock(
            self.config.barman_lock_directory, self.config.name, backup.backup_id
        )
        if os.path.exists(sync_lock.filename):
            _logger.debug("Deleting backup sync lockfile: %s" % sync_lock.filename)

            os.unlink(sync_lock.filename)

        # Run the post_delete_retry_script if present.
        try:
            retry_script = RetryHookScriptRunner(self, "delete_retry_script", "post")
            retry_script.env_from_backup_info(backup)
            retry_script.run()
        except AbortedRetryHookScript as e:
            # Ignore the ABORT_STOP as it is a post-hook operation
            _logger.warning(
                "Ignoring stop request after receiving "
                "abort (exit code %d) from post-delete "
                "retry hook script: %s",
                e.hook.exit_status,
                e.hook.script,
            )

        # Run the post_delete_script if present.
        script = HookScriptRunner(self, "delete_script", "post")
        script.env_from_backup_info(backup)
        script.run()

        self.backup_cache_remove(backup)

        return True

    def _set_backup_sizes(self, backup_info, fsync=False):
        """
        Set the actual size on disk of a backup.

        Optionally fsync all files in the backup.

        :param LocalBackupInfo backup_info: the backup to update
        :param bool fsync: whether to fsync files to disk
        """
        backup_size = 0
        deduplicated_size = 0
        backup_dest = backup_info.get_basebackup_directory()
        for dir_path, _, file_names in os.walk(backup_dest):
            if fsync:
                # If fsync, execute fsync() on the containing directory
                fsync_dir(dir_path)
            for filename in file_names:
                file_path = os.path.join(dir_path, filename)
                # If fsync, execute fsync() on all the contained files
                file_stat = fsync_file(file_path) if fsync else os.stat(file_path)
                backup_size += file_stat.st_size
                # Excludes hard links from real backup size and only counts
                # unique files for deduplicated size
                if file_stat.st_nlink == 1:
                    deduplicated_size += file_stat.st_size
        # Save size into BackupInfo object
        backup_info.set_attribute("size", backup_size)
        backup_info.set_attribute("deduplicated_size", deduplicated_size)
        backup_info.save()

    def validate_backup_args(self, **kwargs):
        """
        Validate backup arguments and Postgres configurations. Arguments
        might be syntactically correct but still be invalid if necessary
        Postgres configurations are not met.

        :kwparam str parent_backup_id: id of the parent backup when taking a
            Postgres incremental backup
        :raises BackupException: if a command argument is considered invalid
        """
        if "parent_backup_id" in kwargs:
            self._validate_incremental_backup_configs(**kwargs)

    def _validate_incremental_backup_configs(self, **kwargs):
        """
        Check required configurations for a Postgres incremental backup

        :raises BackupException: if a required configuration is missing
        """
        if self.server.postgres.server_version < 170000:
            raise BackupException(
                "Postgres version 17 or greater is required for incremental backups "
                "using the Postgres backup method"
            )

        if self.config.backup_method != "postgres":
            raise BackupException(
                "Backup using the `--incremental` flag is available only for "
                "'backup_method = postgres'. Check Barman's documentation for "
                "more help on this topic."
            )

        summarize_wal = self.server.postgres.get_setting("summarize_wal")
        if summarize_wal != "on":
            raise BackupException(
                "'summarize_wal' option has to be enabled in the Postgres server "
                "to perform an incremental backup using the Postgres backup method"
            )

        if self.config.backup_compression is not None:
            raise BackupException(
                "Incremental backups cannot be taken with "
                "'backup_compression' set in the configuration options."
            )

        parent_backup_id = kwargs.get("parent_backup_id")
        parent_backup_info = self.get_backup(parent_backup_id)

        if parent_backup_info:
            if parent_backup_info.summarize_wal != "on":
                raise BackupException(
                    "Backup ID %s is not eligible as a parent for an "
                    "incremental backup because WAL summaries were not enabled "
                    "when that backup was taken." % parent_backup_info.backup_id
                )
            if parent_backup_info.compression is not None:
                raise BackupException(
                    "The specified backup cannot be a parent for an "
                    "incremental backup. Reason: "
                    "Compressed backups are not eligible as parents of incremental backups."
                )

    def _encrypt_backup(self, backup_info):
        """
        Perform encryption of the base backup and tablespaces

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        :raises BackupException: If the encryption validation fails
        """
        try:
            self.encryption_manager.validate_config()
            encryption = self.encryption_manager.get_encryption()
        except ValueError as ex:
            raise BackupException(force_str(ex))

        output.info("Encrypting backup using %s encryption" % encryption.NAME)

        # At this point, all the encryption configuration has already been
        # validated. We only need to check the format of the backup, so
        # we know how to encrypt the underlying files.
        if self.config.backup_compression_format == "tar":
            self._encrypt_tar_backup(backup_info, encryption)

        backup_info.set_attribute("encryption", encryption.NAME)

    def _encrypt_tar_backup(self, backup_info, encryption):
        """
        Perform encryption of base backup and tablespaces in tar format.

        All ``.tar`` and ``.tar.*`` files under the backup data directory are encrypted.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information
        :param barman.encryption.Encryption encryption: The encryption handler class
        """
        for tar_file in backup_info.get_list_of_files("data"):
            filename = os.path.basename(tar_file)
            if re.search(r"\.tar(\.[^.]+)?$", filename):
                output.debug("Encrypting file %s" % tar_file)
                encryption.encrypt(tar_file, os.path.dirname(tar_file))
                output.debug("File encrypted. Deleting unencrypted file %s" % tar_file)
                os.unlink(tar_file)

    def backup(self, wait=False, wait_timeout=None, name=None, **kwargs):
        """
        Performs a backup for the server

        :param bool wait: wait for all the required WAL files to be archived
        :param int|None wait_timeout:
        :param str|None name: the friendly name to be saved with this backup
        :kwparam str parent_backup_id: id of the parent backup when taking a
            Postgres incremental backup
        :return BackupInfo: the generated BackupInfo
        """
        _logger.debug("initialising backup information")
        self.executor.init()
        backup_info = None
        try:
            # Create the BackupInfo object representing the backup
            backup_info = LocalBackupInfo(
                self.server,
                backup_id=datetime.datetime.now().strftime("%Y%m%dT%H%M%S"),
                backup_name=name,
            )
            backup_info.set_attribute("systemid", self.server.systemid)

            backup_info.set_attribute(
                "parent_backup_id",
                kwargs.get("parent_backup_id"),
            )

            backup_info.save()
            self.backup_cache_add(backup_info)
            output.info(
                "Starting backup using %s method for server %s in %s",
                self.mode,
                self.config.name,
                backup_info.get_basebackup_directory(),
            )

            # Run the pre-backup-script if present.
            script = HookScriptRunner(self, "backup_script", "pre")
            script.env_from_backup_info(backup_info)
            script.run()

            # Run the pre-backup-retry-script if present.
            retry_script = RetryHookScriptRunner(self, "backup_retry_script", "pre")
            retry_script.env_from_backup_info(backup_info)
            retry_script.run()

            # Do the backup using the BackupExecutor
            self.executor.backup(backup_info)

            # Create a restore point after a backup
            target_name = "barman_%s" % backup_info.backup_id
            self.server.postgres.create_restore_point(target_name)

            # Free the Postgres connection
            self.server.postgres.close()

            # Encrypt the backup if requested
            if self.config.encryption is not None:
                self._encrypt_backup(backup_info)

            # Compute backup size and fsync it on disk
            self.backup_fsync_and_set_sizes(backup_info)

            # Mark the backup as WAITING_FOR_WALS
            backup_info.set_attribute("status", BackupInfo.WAITING_FOR_WALS)
        # Use BaseException instead of Exception to catch events like
        # KeyboardInterrupt (e.g.: CTRL-C)
        except BaseException as e:
            msg_lines = force_str(e).strip().splitlines()
            # If the exception has no attached message use the raw
            # type name
            if len(msg_lines) == 0:
                msg_lines = [type(e).__name__]
            if backup_info:
                # Use only the first line of exception message
                # in backup_info error field
                backup_info.set_attribute("status", BackupInfo.FAILED)
                backup_info.set_attribute(
                    "error",
                    "failure %s (%s)" % (self.executor.current_action, msg_lines[0]),
                )

            output.error(
                "Backup failed %s.\nDETAILS: %s",
                self.executor.current_action,
                "\n".join(msg_lines),
            )

        else:
            output.info(
                "Backup end at LSN: %s (%s, %08X)",
                backup_info.end_xlog,
                backup_info.end_wal,
                backup_info.end_offset,
            )

            executor = self.executor
            output.info(
                "Backup completed (start time: %s, elapsed time: %s)",
                self.executor.copy_start_time,
                human_readable_timedelta(
                    datetime.datetime.now() - executor.copy_start_time
                ),
            )

            # If requested, wait for end_wal to be archived
            if wait:
                try:
                    self.server.wait_for_wal(backup_info.end_wal, wait_timeout)
                    self.check_backup(backup_info)
                except KeyboardInterrupt:
                    # Ignore CTRL-C pressed while waiting for WAL files
                    output.info(
                        "Got CTRL-C. Continuing without waiting for '%s' "
                        "to be archived",
                        backup_info.end_wal,
                    )

        finally:
            if backup_info:
                # IF is an incremental backup, we save here child backup info id
                # inside the parent list of children. no matter if the backup
                # is successful or not. This is needed to be able to retrieve
                # also failed incremental backups for removal or other operations
                # like show-backup.
                parent_backup_info = backup_info.get_parent_backup_info()

                if parent_backup_info:
                    if parent_backup_info.children_backup_ids:
                        parent_backup_info.children_backup_ids.append(  # type: ignore
                            backup_info.backup_id
                        )
                    else:
                        parent_backup_info.children_backup_ids = [backup_info.backup_id]
                    parent_backup_info.save()

                backup_info.save()

                # Make sure we are not holding any PostgreSQL connection
                # during the post-backup scripts
                self.server.close()

                # Run the post-backup-retry-script if present.
                try:
                    retry_script = RetryHookScriptRunner(
                        self, "backup_retry_script", "post"
                    )
                    retry_script.env_from_backup_info(backup_info)
                    retry_script.run()
                except AbortedRetryHookScript as e:
                    # Ignore the ABORT_STOP as it is a post-hook operation
                    _logger.warning(
                        "Ignoring stop request after receiving "
                        "abort (exit code %d) from post-backup "
                        "retry hook script: %s",
                        e.hook.exit_status,
                        e.hook.script,
                    )

                # Run the post-backup-script if present.
                script = HookScriptRunner(self, "backup_script", "post")
                script.env_from_backup_info(backup_info)
                script.run()

                # if the autogenerate_manifest functionality is active and the
                # backup files copy is successfully completed using the rsync method,
                # generate the backup manifest
                if (
                    isinstance(self.executor, RsyncBackupExecutor)
                    and self.config.autogenerate_manifest
                    and backup_info.status != BackupInfo.FAILED
                ):
                    local_file_manager = LocalFileManager()
                    backup_manifest = BackupManifest(
                        backup_info.get_data_directory(), local_file_manager, SHA256()
                    )
                    backup_manifest.create_backup_manifest()

                    output.info(
                        "Backup manifest for backup '%s' successfully "
                        "generated for server %s",
                        backup_info.backup_id,
                        self.config.name,
                    )

        output.result("backup", backup_info)
        return backup_info

    def recover(
        self,
        backup_info,
        dest,
        wal_dest=None,
        tablespaces=None,
        remote_command=None,
        **kwargs
    ):
        """
        Performs a recovery of a backup

        :param barman.infofile.LocalBackupInfo backup_info: the backup
            to recover
        :param str dest: the destination directory
        :param str|None wal_dest: the destination directory for WALs when doing PITR.
            See :meth:`~barman.recovery_executor.RecoveryExecutor._set_pitr_targets` for more details.
        :param dict[str,str]|None tablespaces: a tablespace name -> location
            map (for relocation)
        :param str|None remote_command: default None. The remote command
            to recover the base backup, in case of remote backup.
        :kwparam str|None target_tli: the target timeline
        :kwparam str|None target_time: the target time
        :kwparam str|None target_xid: the target xid
        :kwparam str|None target_lsn: the target LSN
        :kwparam str|None target_name: the target name created previously with
            pg_create_restore_point() function call
        :kwparam bool|None target_immediate: end recovery as soon as
            consistency is reached
        :kwparam bool exclusive: whether the recovery is exclusive or not
        :kwparam str|None target_action: default None. The recovery target
            action
        :kwparam bool|None standby_mode: the standby mode if needed
        :kwparam str|None recovery_conf_filename: filename for storing recovery
            configurations
        """

        # Archive every WAL files in the incoming directory of the server
        self.server.archive_wal(verbose=False)
        # Delegate the recovery operation to a RecoveryExecutor object

        command = unix_command_factory(remote_command, self.server.path)
        executor = recovery_executor_factory(self, command, backup_info)
        # Run the pre_recovery_script if present.
        script = HookScriptRunner(self, "recovery_script", "pre")
        script.env_from_recover(
            backup_info, dest, tablespaces, remote_command, **kwargs
        )
        script.run()

        # Run the pre_recovery_retry_script if present.
        retry_script = RetryHookScriptRunner(self, "recovery_retry_script", "pre")
        retry_script.env_from_recover(
            backup_info, dest, tablespaces, remote_command, **kwargs
        )
        retry_script.run()

        # Execute the recovery.
        # We use a closing context to automatically remove
        # any resource eventually allocated during recovery.
        with closing(executor):
            recovery_info = executor.recover(
                backup_info,
                dest,
                wal_dest=wal_dest,
                tablespaces=tablespaces,
                remote_command=remote_command,
                **kwargs
            )

        # Run the post_recovery_retry_script if present.
        try:
            retry_script = RetryHookScriptRunner(self, "recovery_retry_script", "post")
            retry_script.env_from_recover(
                backup_info, dest, tablespaces, remote_command, **kwargs
            )
            retry_script.run()
        except AbortedRetryHookScript as e:
            # Ignore the ABORT_STOP as it is a post-hook operation
            _logger.warning(
                "Ignoring stop request after receiving "
                "abort (exit code %d) from post-recovery "
                "retry hook script: %s",
                e.hook.exit_status,
                e.hook.script,
            )

        # Run the post-recovery-script if present.
        script = HookScriptRunner(self, "recovery_script", "post")
        script.env_from_recover(
            backup_info, dest, tablespaces, remote_command, **kwargs
        )
        script.run()

        # Output recovery results
        output.result("recovery", recovery_info["results"])

    def archive_wal(self, verbose=True):
        """
        Executes WAL maintenance operations, such as archiving and compression

        If verbose is set to False, outputs something only if there is
        at least one file

        :param bool verbose: report even if no actions
        """
        for archiver in self.server.archivers:
            archiver.archive(verbose)

    def cron_retention_policy(self):
        """
        Retention policy management
        """
        enforce_retention_policies = self.server.enforce_retention_policies
        retention_policy_mode = self.config.retention_policy_mode

        if enforce_retention_policies and retention_policy_mode == "auto":
            available_backups = self.get_available_backups(BackupInfo.STATUS_ALL)
            retention_status = self.config.retention_policy.report()

            # Find backups with the delete annotation and mark as obsolete
            for backup in available_backups.values():
                if self.check_delete_annotation(backup.backup_id):
                    retention_status[backup.backup_id] = BackupInfo.OBSOLETE
                    self.release_delete_annotation(backup.backup_id)

                # Check if the backup path still exists while the delete annotation
                # was already removed
                elif backup.is_orphan:
                    output.warning(
                        "WARNING: Backup directory %s contains only a non-empty "
                        "backup.info file which may indicate an incomplete delete operation. "
                        "Please manually delete the directory.",
                        backup.get_basebackup_directory(),
                    )

            for bid in sorted(retention_status.keys()):
                if retention_status[bid] == BackupInfo.OBSOLETE:
                    try:
                        # Lock acquisition: if you can acquire a ServerBackupLock
                        # it means that no other processes like another delete operation
                        # are running on that server for that backup id,
                        # and the retention policy can be applied.
                        with ServerBackupIdLock(
                            self.config.barman_lock_directory, self.config.name, bid
                        ):
                            output.info(
                                "Enforcing retention policy: removing backup %s for "
                                "server %s" % (bid, self.config.name)
                            )
                            self.delete_backup(
                                available_backups[bid],
                                skip_wal_cleanup_if_standalone=False,
                            )
                    except LockFileBusy:
                        # Another process is holding the backup lock, potentially
                        # is being removed manually. Skip it and output a message
                        output.warning(
                            "Another action is in progress for the backup %s "
                            "of server %s, skipping retention policy application"
                            % (bid, self.config.name)
                        )

    def delete_basebackup(self, backup):
        """
        Delete the basebackup dir of a given backup.

        :param barman.infofile.LocalBackupInfo backup: the backup to delete
        """
        backup_dir = backup.get_basebackup_directory()
        _logger.debug("Deleting base backup directory: %s" % backup_dir)

        shutil.rmtree(backup_dir)

    def delete_backupinfo_file(self, backup):
        """
        Delete the ``backup.info`` file of a given backup.

        :param barman.infofile.LocalBackupInfo backup: the backup to delete
        """
        backup_info_path = backup.get_filename()
        if os.path.exists(backup_info_path):
            _logger.debug("Deleting backup.info file: %s" % backup_info_path)
            os.unlink(backup_info_path)

    def delete_backup_data(self, backup):
        """
        Delete the data contained in a given backup.

        :param barman.infofile.LocalBackupInfo backup: the backup to delete
        """
        # If this backup has snapshots then they should be deleted first.
        if backup.snapshots_info:
            _logger.debug(
                "Deleting the following snapshots: %s"
                % ", ".join(
                    snapshot.identifier for snapshot in backup.snapshots_info.snapshots
                )
            )
            snapshot_interface = get_snapshot_interface_from_backup_info(
                backup, self.server.config
            )
            snapshot_interface.delete_snapshot_backup(backup)
        # If this backup does *not* have snapshots then tablespaces are stored on the
        # barman server so must be deleted.
        elif backup.tablespaces:
            if backup.backup_version == 2:
                tbs_dir = backup.get_basebackup_directory()
            else:
                tbs_dir = os.path.join(backup.get_data_directory(), "pg_tblspc")
            for tablespace in backup.tablespaces:
                rm_dir = os.path.join(tbs_dir, str(tablespace.oid))
                if os.path.exists(rm_dir):
                    _logger.debug(
                        "Deleting tablespace %s directory: %s"
                        % (tablespace.name, rm_dir)
                    )
                    shutil.rmtree(rm_dir)

        # Whether a backup has snapshots or not, the data directory will always be
        # present because this is where the backup_label is stored. It must therefore
        # be deleted here.
        pg_data = backup.get_data_directory()
        if os.path.exists(pg_data):
            _logger.debug("Deleting PGDATA directory: %s" % pg_data)
            shutil.rmtree(pg_data)

    def _run_pre_delete_wal_scripts(self, wal_info):
        """
        Run the pre-delete hook-scripts, if any, on the given WAL.

        :param barman.infofile.WalFileInfo wal_info: WAL to run the script on.
        """
        # Run the pre_wal_delete_script if present.
        script = HookScriptRunner(self, "wal_delete_script", "pre")
        script.env_from_wal_info(wal_info)
        script.run()
        # Run the pre_wal_delete_retry_script if present.
        retry_script = RetryHookScriptRunner(self, "wal_delete_retry_script", "pre")
        retry_script.env_from_wal_info(wal_info)
        retry_script.run()

    def _run_post_delete_wal_scripts(self, wal_info, error=None):
        """
        Run the post-delete hook-scripts, if any, on the given WAL.

        :param barman.infofile.WalFileInfo wal_info: WAL to run the script on.
        :param None|str error: error message in case a failure happened.
        """
        # Run the post_wal_delete_retry_script if present.
        try:
            retry_script = RetryHookScriptRunner(
                self, "wal_delete_retry_script", "post"
            )
            retry_script.env_from_wal_info(wal_info, None, error)
            retry_script.run()
        except AbortedRetryHookScript as e:
            # Ignore the ABORT_STOP as it is a post-hook operation
            _logger.warning(
                "Ignoring stop request after receiving "
                "abort (exit code %d) from post-wal-delete "
                "retry hook script: %s",
                e.hook.exit_status,
                e.hook.script,
            )
        # Run the post_wal_delete_script if present.
        script = HookScriptRunner(self, "wal_delete_script", "post")
        script.env_from_wal_info(wal_info, None, error)
        script.run()

    def delete_wal(self, wal_info):
        """
        Delete a WAL segment, with the given WalFileInfo

        :param barman.infofile.WalFileInfo wal_info: the WAL to delete
        """
        self._run_pre_delete_wal_scripts(wal_info)

        error = None
        try:
            os.unlink(wal_info.fullpath(self.server))
            try:
                os.removedirs(os.path.dirname(wal_info.fullpath(self.server)))
            except OSError:
                # This is not an error condition
                # We always try to remove the trailing directories,
                # this means that hashdir is not empty.
                pass
        except OSError as e:
            error = "Ignoring deletion of WAL file %s for server %s: %s" % (
                wal_info.name,
                self.config.name,
                e,
            )
            output.warning(error)

        self._run_post_delete_wal_scripts(wal_info, error)

    def check(self, check_strategy):
        """
        This function does some checks on the server.

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        check_strategy.init_check("compression settings")
        # Check compression_setting parameter
        if self.config.compression and not self.compression_manager.check():
            check_strategy.result(self.config.name, False)
        else:
            status = True
            try:
                self.compression_manager.get_default_compressor()
            except CompressionIncompatibility as field:
                check_strategy.result(self.config.name, "%s setting" % field, False)
                status = False
            check_strategy.result(self.config.name, status)

        # Failed backups check
        check_strategy.init_check("failed backups")
        failed_backups = self.get_available_backups((BackupInfo.FAILED,))
        status = len(failed_backups) == 0
        check_strategy.result(
            self.config.name,
            status,
            hint="there are %s failed backups"
            % (
                len(
                    failed_backups,
                )
            ),
        )
        check_strategy.init_check("minimum redundancy requirements")
        # Minimum redundancy checks will take into account only not-incremental backups
        no_backups = len(
            self.get_available_backups(
                status_filter=(BackupInfo.DONE,),
                backup_type_filter=(BackupInfo.NOT_INCREMENTAL),
            )
        )
        # Check minimum_redundancy_requirements parameter
        if no_backups < int(self.config.minimum_redundancy):
            status = False
        else:
            status = True
        check_strategy.result(
            self.config.name,
            status,
            hint="have %s non-incremental backups, expected at least %s"
            % (no_backups, self.config.minimum_redundancy),
        )

        # TODO: Add a check for the existence of ssh and of rsync

        # Execute additional checks defined by the BackupExecutor
        if self.executor:
            self.executor.check(check_strategy)

    def status(self):
        """
        This function show the server status
        """
        # get number of backups
        no_backups = len(self.get_available_backups(status_filter=(BackupInfo.DONE,)))
        output.result(
            "status",
            self.config.name,
            "backups_number",
            "No. of available backups",
            no_backups,
        )
        output.result(
            "status",
            self.config.name,
            "first_backup",
            "First available backup",
            self.get_first_backup_id(),
        )
        output.result(
            "status",
            self.config.name,
            "last_backup",
            "Last available backup",
            self.get_last_backup_id(),
        )

        no_backups_not_incremental = len(
            self.get_available_backups(
                status_filter=(BackupInfo.DONE,),
                backup_type_filter=(BackupInfo.NOT_INCREMENTAL),
            )
        )
        # Minimum redundancy check. if number of non-incremental backups minor than
        # minimum redundancy, fail.
        if no_backups_not_incremental < self.config.minimum_redundancy:
            output.result(
                "status",
                self.config.name,
                "minimum_redundancy",
                "Minimum redundancy requirements",
                "FAILED (%s/%s)"
                % (no_backups_not_incremental, self.config.minimum_redundancy),
            )
        else:
            output.result(
                "status",
                self.config.name,
                "minimum_redundancy",
                "Minimum redundancy requirements",
                "satisfied (%s/%s)"
                % (no_backups_not_incremental, self.config.minimum_redundancy),
            )

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

    def get_latest_archived_wals_info(self):
        """
        Return a dictionary of timelines associated with the
        WalFileInfo of the last WAL file in the archive,
        or None if the archive doesn't contain any WAL file.

        :rtype: dict[str, WalFileInfo]|None
        """
        from os.path import isdir, join

        root = self.config.wals_directory

        # If the WAL archive directory doesn't exists the archive is empty
        if not isdir(root):
            return dict()

        # Traverse all the directory in the archive in reverse order,
        # returning the first WAL file found
        timelines = {}
        for name in sorted(os.listdir(root), reverse=True):
            fullname = join(root, name)
            # All relevant files are in subdirectories, so
            # we skip any non-directory entry
            if isdir(fullname):
                # Extract the timeline. If it is not valid, skip this directory
                try:
                    timeline = name[0:8]
                    int(timeline, 16)
                except ValueError:
                    continue

                # If this timeline already has a file, skip this directory
                if timeline in timelines:
                    continue

                hash_dir = fullname
                # Inspect contained files in reverse order
                for wal_name in sorted(os.listdir(hash_dir), reverse=True):
                    fullname = join(hash_dir, wal_name)
                    # Return the first file that has the correct name
                    if not isdir(fullname) and xlog.is_wal_file(fullname):
                        timelines[timeline] = self.get_wal_file_info(
                            filename=fullname,
                        )
                        break

        # Return the timeline map
        return timelines

    def remove_wal_before_backup(
        self, backup_info, timelines_to_protect=None, wal_ranges_to_protect=[]
    ):
        """
        Remove WAL files which have been archived before the start of
        the provided backup.

        If no backup_info is provided delete all available WAL files

        If timelines_to_protect list is passed, never remove a wal in one of
        these timelines.

        :param BackupInfo|None backup_info: the backup information structure
        :param set timelines_to_protect: optional list of timelines
            to protect
        :param list wal_ranges_to_protect: optional list of `(begin_wal, end_wal)`
            tuples which define inclusive ranges of WALs which must not be deleted.
        :return list: a list of removed WAL files
        """
        # A dictionary where key is the WAL directory name and value is a list of
        # wal_info object representing the WALs to be deleted in that directory
        wals_to_remove = defaultdict(list)
        with self.server.xlogdb("r+") as fxlogdb:
            xlogdb_dir = os.path.dirname(fxlogdb.name)
            with tempfile.TemporaryFile(mode="w+", dir=xlogdb_dir) as fxlogdb_new:
                for line in fxlogdb:
                    wal_info = WalFileInfo.from_xlogdb_line(line)
                    if not xlog.is_any_xlog_file(wal_info.name):
                        output.error(
                            "invalid WAL segment name %r\n"
                            'HINT: Please run "barman rebuild-xlogdb %s" '
                            "to solve this issue",
                            wal_info.name,
                            self.config.name,
                        )
                        continue

                    # Keeps the WAL segment if it is a history file
                    keep = xlog.is_history_file(wal_info.name)

                    # Keeps the WAL segment if its timeline is in
                    # `timelines_to_protect`
                    if timelines_to_protect:
                        tli, _, _ = xlog.decode_segment_name(wal_info.name)
                        keep |= tli in timelines_to_protect

                    # Keeps the WAL segment if it is within a protected range
                    if xlog.is_backup_file(wal_info.name):
                        # If we have a .backup file then truncate the name for the
                        # range check
                        wal_name = wal_info.name[:24]
                    else:
                        wal_name = wal_info.name
                    for begin_wal, end_wal in wal_ranges_to_protect:
                        keep |= wal_name >= begin_wal and wal_name <= end_wal

                    # Keeps the WAL segment if it is a newer
                    # than the given backup (the first available)
                    if backup_info and backup_info.begin_wal is not None:
                        keep |= wal_info.name >= backup_info.begin_wal

                    # If the file has to be kept write it in the new xlogdb
                    # otherwise add it to the removal list
                    if keep:
                        fxlogdb_new.write(wal_info.to_xlogdb_line())
                    else:
                        wal_dir = os.path.dirname(wal_info.fullpath(self.server))
                        wals_to_remove[wal_dir].append(wal_info)

                wals_removed = self.delete_wals(wals_to_remove)

                fxlogdb_new.flush()
                fxlogdb_new.seek(0)
                fxlogdb.seek(0)
                shutil.copyfileobj(fxlogdb_new, fxlogdb)
                fxlogdb.truncate()

        return wals_removed

    def delete_wals(self, wals_to_delete):
        """
        Delete the given WAL files. The entire WAL directory is deleted when possible.

        :param dict[str, list[WalFileInfo]] wals_to_delete: A dictionary where key is the WAL directory name and
            value is a list of wal_info objects representing the WALs to be deleted in
            that directory.
        :return list[str]: a list of deleted WAL names.
        """
        wals_deleted = []
        for wal_dir, wal_list in wals_to_delete.items():
            delete_directory = False
            # Each directory can contain up to 256 WAL files. If the deletion list
            # contains 256 entries, the entire directory can be safely deleted
            # Otherwise, check if all WALs in the directory are in the deletion list
            if len(wal_list) >= 256:
                delete_directory = True
            else:
                wal_names_to_delete = {wal_info.name for wal_info in wal_list}
                wal_names_in_dir = os.listdir(wal_dir)
                if set(wal_names_in_dir).issubset(wal_names_to_delete):
                    delete_directory = True
            # If the directory can be deleted, run the hook-scripts on each WAL file
            # before and after the rmtree. Otherwise, delete each WAL individually
            if delete_directory:
                for wal_info in wal_list:
                    self._run_pre_delete_wal_scripts(wal_info)
                shutil.rmtree(wal_dir)
                for wal_info in wal_list:
                    self._run_post_delete_wal_scripts(wal_info)
                    wals_deleted.append(wal_info.name)
            else:
                for wal_info in wal_list:
                    self.delete_wal(wal_info)
                    wals_deleted.append(wal_info.name)
        return wals_deleted

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
            backup = LocalBackupInfo(self.server, backup_id=backup_id)
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

    def validate_last_backup_min_size(self, last_backup_minimum_size):
        """
        Evaluate the size of the last available backup in a catalogue.
        If the last backup is smaller than the specified size
        the function returns False.
        Otherwise, the function returns True.

        :param last_backup_minimum_size: size in bytes
            representing the maximum allowed age for the last backup
            in a server catalogue
        :return tuple: a tuple containing the boolean result of the check and
            auxiliary information about the last backup current age
        """
        # Get the ID of the last available backup
        backup_id = self.get_last_backup_id()
        if backup_id:
            # Get the backup object
            backup = LocalBackupInfo(self.server, backup_id=backup_id)
            if backup.size < last_backup_minimum_size:
                return False, backup.size
            else:
                return True, backup.size
        else:
            # If no backup is available return false
            return False, 0

    def backup_fsync_and_set_sizes(self, backup_info):
        """
        Fsync all files in a backup and set the actual size on disk
        of a backup.

        Also evaluate the deduplication ratio and the deduplicated size if
        applicable.

        :param LocalBackupInfo backup_info: the backup to update
        """
        # Calculate the base backup size
        self.executor.current_action = "calculating backup size"
        _logger.debug(self.executor.current_action)
        # Set backup sizes with fsync. We need to fsync files here to make sure
        # the backup files are persisted to disk, so we don't lose the backup in
        # the event of a system crash.
        self._set_backup_sizes(backup_info, fsync=True)
        if backup_info.size > 0:
            deduplication_ratio = 1 - (
                float(backup_info.deduplicated_size) / backup_info.size
            )
        else:
            deduplication_ratio = 0

        if self.config.reuse_backup == "link":
            output.info(
                "Backup size: %s. Actual size on disk: %s"
                " (-%s deduplication ratio)."
                % (
                    pretty_size(backup_info.size),
                    pretty_size(backup_info.deduplicated_size),
                    "{percent:.2%}".format(percent=deduplication_ratio),
                )
            )
        else:
            output.info("Backup size: %s" % pretty_size(backup_info.size))

    def check_backup(self, backup_info):
        """
        Make sure that all the required WAL files to check
        the consistency of a physical backup (that is, from the
        beginning to the end of the full backup) are correctly
        archived. This command is automatically invoked by the
        cron command and at the end of every backup operation.

        :param backup_info: the target backup
        """

        # Gather the list of the latest archived wals
        timelines = self.get_latest_archived_wals_info()

        # Get the basic info for the backup
        begin_wal = backup_info.begin_wal
        end_wal = backup_info.end_wal
        timeline = begin_wal[:8]

        # Case 0: there is nothing to check for this backup, as it is
        # currently in progress
        if not end_wal:
            return

        # Case 1: Barman still doesn't know about the timeline the backup
        # started with. We still haven't archived any WAL corresponding
        # to the backup, so we can't proceed with checking the existence
        # of the required WAL files
        if not timelines or timeline not in timelines:
            backup_info.status = BackupInfo.WAITING_FOR_WALS
            backup_info.save()
            return

        # Find the most recent archived WAL for this server in the timeline
        # where the backup was taken
        last_archived_wal = timelines[timeline].name

        # Case 2: the most recent WAL file archived is older than the
        # start of the backup. We must wait for the archiver to receive
        # and/or process the WAL files.
        if last_archived_wal < begin_wal:
            backup_info.status = BackupInfo.WAITING_FOR_WALS
            backup_info.save()
            return

        # Check the intersection between the required WALs and the archived
        # ones. They should all exist
        segments = backup_info.get_required_wal_segments()
        missing_wal = None
        for wal in segments:
            # Stop checking if we reach the last archived wal
            if wal > last_archived_wal:
                break
            wal_full_path = self.server.get_wal_full_path(wal)
            if not os.path.exists(wal_full_path):
                missing_wal = wal
                break

        if missing_wal:
            # Case 3: the most recent WAL file archived is more recent than
            # the one corresponding to the start of a backup. If WAL
            # file is missing, then we can't recover from the backup so we
            # must mark the backup as FAILED.
            # TODO: Verify if the error field is the right place
            # to store the error message
            backup_info.error = (
                "At least one WAL file is missing. "
                "The first missing WAL file is %s" % missing_wal
            )
            backup_info.status = BackupInfo.FAILED
            backup_info.save()
            output.error(
                "This backup has been marked as FAILED due to the "
                "following reason: %s" % backup_info.error
            )
            return

        if end_wal <= last_archived_wal:
            # Case 4: if the most recent WAL file archived is more recent or
            # equal than the one corresponding to the end of the backup and
            # every WAL that will be required by the recovery is available,
            # we can mark the backup as DONE.
            backup_info.status = BackupInfo.DONE
        else:
            # Case 5: if the most recent WAL file archived is older than
            # the one corresponding to the end of the backup but
            # all the WAL files until that point are present.
            backup_info.status = BackupInfo.WAITING_FOR_WALS
        backup_info.save()

    def verify_backup(self, backup_info):
        """
        This function should check if pg_verifybackup is installed and run it against backup path
        should test if pg_verifybackup is installed locally


        :param backup_info: barman.infofile.LocalBackupInfo instance
        """
        output.info("Calling pg_verifybackup")
        # Test pg_verifybackup existence
        version_info = PgVerifyBackup.get_version_info(self.server.path)
        if version_info.get("full_path", None) is None:
            output.error("pg_verifybackup not found")
            return

        pg_verifybackup = PgVerifyBackup(
            data_path=backup_info.get_data_directory(),
            command=version_info["full_path"],
            version=version_info["full_version"],
        )
        try:
            pg_verifybackup()
        except CommandFailedException as e:
            output.error(
                "verify backup failure on directory '%s'"
                % backup_info.get_data_directory()
            )
            output.error(e.args[0]["err"])
            return
        output.info(pg_verifybackup.get_output()[0].strip())

    def get_wal_file_info(self, filename):
        """
        Populate a WalFileInfo object taking into account the server
        configuration.

        Set compression to 'custom' if no compression is identified
        and Barman is configured to use custom compression.

        :param str filename: the path of the file to identify
        :rtype: barman.infofile.WalFileInfo
        """
        return WalFileInfo.from_file(
            filename,
            compression_manager=self.compression_manager,
            unidentified_compression=self.compression_manager.unidentified_compression,
            encryption_manager=self.encryption_manager,
        )
