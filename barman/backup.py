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
import filecmp
import io
import json
import logging
import os
import re
import shutil
import sys
import tarfile
import tempfile
from collections import defaultdict
from contextlib import closing
from glob import glob

import dateutil.tz

from barman import output, xlog
from barman.annotations import (
    AnnotationManagerFile,
    KeepManager,
    KeepManagerMixin,
    KeepManagerMixinCloud,
)
from barman.backup_executor import (
    CloudBackupExecutor,
    CloudPostgresBackupExecutor,
    PassiveBackupExecutor,
    PostgresBackupExecutor,
    RsyncBackupExecutor,
    SnapshotBackupExecutor,
)
from barman.backup_manifest import BackupManifest
from barman.cloud import CloudBackupCatalog, CloudWalDownloader
from barman.cloud_providers import (
    get_snapshot_interface_from_backup_info,
    recognize_cloud_provider,
)
from barman.command_wrappers import PgVerifyBackup
from barman.compression import CompressionManager
from barman.config import BackupOptions, RecoveryOptions
from barman.encryption import EncryptionManager
from barman.exceptions import (
    AbortedRetryHookScript,
    BackupException,
    CommandFailedException,
    CompressionIncompatibility,
    ExportBackupException,
    ImportBackupException,
    LockFileBusy,
    SshCommandException,
    UnknownBackupIdException,
)
from barman.fs import unix_command_factory
from barman.hooks import HookScriptRunner, RetryHookScriptRunner
from barman.infofile import (
    BackupInfo,
    BackupInfoFactory,
    CloudLocalBackupInfo,
    LocalBackupInfo,
    WalFileInfo,
)
from barman.lockfile import ServerBackupIdLock, ServerBackupSyncLock
from barman.recovery_executor import recovery_executor_factory
from barman.remote_status import RemoteStatusMixin
from barman.storage.local_file_manager import LocalFileManager
from barman.utils import (
    SHA256,
    BarmanEncoderV2,
)
from barman.utils import LooseVersion as Version
from barman.utils import (
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
from barman.wal_archiver import CloudWalArchiver, CloudWalStorageStrategy

_logger = logging.getLogger(__name__)

# Prefix for pg_tblspc symlinks in exported tarballs. Members matching
# this prefix exactly (no further '/') are skipped by _extract_tarball
# and reconstructed by _reconstruct_tablespace_symlinks.
_PG_TBLSPC_TARBALL_PREFIX = "backup/data/pg_tblspc/"


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
        if self.config.active:
            try:
                if server.passive_node:
                    self.executor = PassiveBackupExecutor(self)
                elif self.config.backup_method == "local-to-cloud":
                    self.executor = CloudBackupExecutor(self)
                elif self.config.backup_method == "postgres":
                    if recognize_cloud_provider(self.config.basebackups_directory):
                        self.executor = CloudPostgresBackupExecutor(self)
                    else:
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
            backup = BackupInfoFactory.build_backup_info(self.server, filename)
            self._backup_cache[backup.backup_id] = backup
        # In version 3.13.2, Barman changed the location of backup.info files.
        # That was done so we have common location for the metadata, which
        # should always be in a mutable storage, independently if worm_mode
        # is enabled or not. So, this new approach takes precedence.
        for filename in glob("%s/*-backup.info" % self.server.meta_directory):
            backup = BackupInfoFactory.build_backup_info(self.server, filename)
            self._backup_cache[backup.backup_id] = backup
        # If the server is disabled and configured to use cloud storage for backups, it
        # might be a new Barman instance on a target host configured specifically for
        # restoring a cloud backup taken by another Barman instance. In that case, we
        # should load the backup.info files from the cloud storage to populate the
        # backup cache so that list-backups, show-backup and restore operations work
        if not self.config.active and self.server.use_backup_cloud_storage:
            self._load_backups_from_cloud()

    def _load_backups_from_cloud(self):
        """
        Fetch ``backup.info`` files from the cloud storage and populate the cache.
        """
        cloud_catalog = CloudBackupCatalog(
            cloud_interface=self.server.get_backup_cloud_interface(),
            server_name=self.config.name,
        )
        backup_list = cloud_catalog.get_backup_list()
        for bk_name, bk_info in backup_list.items():
            # get_backup_list() returns a dict of BackupInfo objects. To be more
            # accurate with types, we convert them to CloudLocalBackupInfo objects
            buffer = io.BytesIO()
            bk_info.save(file_object=buffer)
            buffer.seek(0)
            cloud_backup_info = CloudLocalBackupInfo(
                server=self.server, backup_id=bk_info.backup_id
            )
            cloud_backup_info.load(file_object=buffer)
            self._backup_cache[bk_name] = cloud_backup_info

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
        backup = BackupInfoFactory.build_backup_info(self.server, backup_id=backup_id)
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
        backup = BackupInfoFactory.build_backup_info(self.server, backup_id=backup_id)
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

    def keep_backup(self, backup_id, target):
        """
        Add a keep annotation for backup with ID *backup_id* with the specified
        recovery *target*.

        :param str backup_id: The ID of the backup to keep
        :param str target: The desired target as defined in
            :class:`barman.annotations.KeepManager`
        """
        # If a cloud storage is configured, first send a copy of the annotation to the
        # cloud destination. This ensures compatibility with the cloud-* scripts
        if self.server.use_backup_cloud_storage:
            cloud_keep_manager = KeepManagerMixinCloud(
                cloud_interface=self.server.get_backup_cloud_interface(),
                server_name=self.config.name,
            )
            cloud_keep_manager.keep_backup(backup_id, target)

        # Then proceed with saving the annotation locally
        super(BackupManager, self).keep_backup(backup_id, target)

    def release_keep(self, backup_id):
        """
        Remove the keep annotation for backup with ID *backup_id*.

        :param str backup_id: The ID of the backup to release
        """
        # If a cloud storage is configured, a copy of the annotation is also present
        # in the cloud destination so we should remove it as well
        if self.server.use_backup_cloud_storage:
            cloud_keep_manager = KeepManagerMixinCloud(
                cloud_interface=self.server.get_backup_cloud_interface(),
                server_name=self.config.name,
            )
            cloud_keep_manager.release_keep(backup_id)

        # Then proceed with removing the local annotation
        super(BackupManager, self).release_keep(backup_id)

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

        parent_backup_id = kwargs.get("parent_backup_id")
        parent_backup_info = self.get_backup(parent_backup_id)
        if parent_backup_info:
            if parent_backup_info.summarize_wal != "on":
                raise BackupException(
                    "Backup ID %s is not eligible as a parent for an "
                    "incremental backup because WAL summaries were not enabled "
                    "when that backup was taken." % parent_backup_info.backup_id
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
        for tar_file in backup_info.get_directory_entries("data"):
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
            backup_info = BackupInfoFactory.build_backup_info(
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

            # Compute backup size and fsync it on disk, if not a cloud backup
            # Cloud backups size are calculated during the backup in the executor
            if not self.server.use_backup_cloud_storage:
                self.backup_fsync_and_set_sizes(backup_info)
            else:
                output.info("Backup size: %s" % pretty_size(backup_info.size))

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
        :kwparam str|None recovery_option_port: port to set in restore command
            when invoking ``barman-wal-restore``
        :kwparam str|None custom_restore_command: Custom restore command
            to override Barman's default (only used with get-wal mode)
        """

        # Archive every WAL files in the incoming directory of the server
        self.server.archive_wal(verbose=False)
        # Delegate the recovery operation to a RecoveryExecutor object

        command = unix_command_factory(remote_command, self.server.path)

        delta_restore = RecoveryOptions.DELTA_RESTORE in self.config.recovery_options

        # Avoid overwriting PGDATA files when restoring a backup without delta restore.
        if not delta_restore:
            # Ensure the PGDATA destination directory is empty.
            dest_dir = command.list_dir_content(dest)
            if dest_dir:
                output.error(
                    "The restore operation cannot proceed because the destination folder "
                    "'%s' is not empty. To prevent accidental data loss, the destination "
                    "must be empty. Please choose a different location or manually empty "
                    "the folder.",
                    dest,
                )
                output.close_and_exit()

        if backup_info.tablespaces:
            for tablespace in backup_info.tablespaces:
                location = tablespace.location
                if tablespaces and tablespace.name in tablespaces:
                    location = tablespaces[tablespace.name]
                # Avoid overwriting TABLESPACE files when restoring a backup without
                # delta restore.
                if not delta_restore:
                    tbs_dir = command.list_dir_content(location)
                    if tbs_dir:
                        output.error(
                            "The restore operation cannot proceed. The destination path "
                            "'%s' for the tablespace '%s' is not empty. To prevent "
                            "accidental data loss, the tablespace destination must be "
                            "empty. Please choose a different location or manually empty "
                            "the folder." % (location, tablespace.name)
                        )
                        output.close_and_exit()

        executor = recovery_executor_factory(self, backup_info)
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

    def cloud_wal_archive(self, wal_path, parallel=0):
        """
        Archive a WAL file to cloud storage.

        Intended to be used as ``archive_command`` in Postgres.

        Performs archiving similar to :class:`WalArchiver`, but with the specific
        purpose of archiving a WAL file to cloud storage directly from ``pg_wal``,
        meaning WAL files are not staged in ``incoming`` directory and do not go through
        the usual WAL archiving process. This is done that way because the
        ``barman cloud-wal-archive`` command is intended to replace the script
        ``barman-cloud-wal-archive`` when the Barman server runs in the same host as
        Postgres. Also, this avoids having to use `cp` as `archive_command`, which could
        send much more WALs to the ``incoming`` directory than what Barman is actually
        able to archive in a timely manner -- besides the fact that `barman wal-archive`
        is awaken only by `barman cron` every 1 minute, while `archive_command` is
        called by Postgres for every WAL file generated.

        .. note::
            This method assumes validation of the WAL file and of the server
            configuration has already been performed. At this point, we can simply
            archive the WAL to cloud object storage.

        .. important::
            This method cannot be used with :class:`LocalWalStorageStrategy` because
            that strategy modifies and/or removes the source WAL file as part of the
            ``save()`` call, which is not acceptable here given we are using the WAL
            from ``pg_wal``.

        :param str wal_path: the path of the WAL file to archive
        :param int parallel: number of WALs to archive in parallel (0=disabled)
        """
        # In the docstring we mention we don't perform validation here, but we still
        # want to make sure that the WAL storage strategy is compatible with this
        # method, otherwise we can end up in a situation where the WAL file is
        # removed/modified without being archived in cloud storage, which can cause data
        # loss.
        if not isinstance(self.server.wal_storage, CloudWalStorageStrategy):
            output.error(
                "The 'cloud-wal-archive' command can only be used with cloud WAL "
                "storage strategies. Please check your server configuration and ensure "
                "that the `wals_directory` points to a cloud object storage."
            )
            return

        cloud_archiver = CloudWalArchiver(self)
        cloud_archiver.archive(wal_path, parallel)

    def cloud_wal_restore(self, wal_name, wal_dest, spool_dir):
        """
        Restore a WAL file from a cloud object storage.

        :param str wal_name: the name of the WAL file to restore
        :param str wal_dest: the destination path where to restore the WAL file
        :param str spool_dir: the spool directory for extra WALs fetched in parallel
        """
        cloud_interface = self.server.get_wal_cloud_interface()
        parallel = self.config.cloud_wal_restore_parallel
        wal_downloader = CloudWalDownloader(
            cloud_interface, self.config.name, spool_dir
        )
        wal_downloader.download_wal(wal_name, wal_dest, False, parallel)

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
                        "Backup '%s' is an orphan backup, this is possibly "
                        "the result of an incomplete delete operation. Please manually "
                        "delete the following files or directories if present:\n"
                        "* %s \n"
                        "* %s \n",
                        backup.backup_id,
                        backup.get_basebackup_directory(),
                        backup.get_filename(),
                    )

            for bid in sorted(retention_status.keys()):
                if retention_status[bid] == BackupInfo.OBSOLETE:
                    backup = available_backups[bid]
                    try:
                        # Lock acquisition: if you can acquire a ServerBackupLock
                        # it means that no other processes like another delete operation
                        # are running on that server for that backup id,
                        # and the retention policy can be applied.
                        with ServerBackupIdLock(
                            self.config.barman_lock_directory, self.config.name, bid
                        ):
                            if backup.is_orphan:
                                output.debug(
                                    "Skipping deletion of orphan backup: '%s' at '%s'.",
                                    backup.backup_id,
                                    backup.get_basebackup_directory(),
                                )
                                continue
                            output.info(
                                "Enforcing retention policy: removing backup %s for "
                                "server %s" % (bid, self.config.name)
                            )
                            self.delete_backup(
                                backup,
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

        if os.path.exists(backup_dir):
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
        if self.server.use_backup_cloud_storage:
            self._delete_cloud_backup_data(backup)
            return
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

    def _delete_cloud_backup_data(self, backup):
        """
        Delete backup data from its cloud storage path.

        :param barman.infofile.LocalBackupInfo backup: the backup to delete
        """
        # get_basebackup_directory returns the cloud path e.g. if basebackups_directory
        # is s3://my-backups then it will be my-backups/<server_name>/base/<backup_id>
        backup_path = backup.get_basebackup_directory()
        # Get all the objects keys under the backup path and pass them to delete_objects
        # In the future we might replace this with just calling the cloud interface's
        # delete_under_prefix method instead, but currently that is only implemented for S3
        cloud_interface = self.server.get_backup_cloud_interface()
        if self.config.aws_check_object_lock:
            from barman.cloud_providers.aws_s3 import S3CloudInterface

            if not isinstance(cloud_interface, S3CloudInterface):
                _logger.warning(
                    "aws_check_object_lock is only supported for S3 storage. "
                    "Object lock checks will not be performed for server '%s'.",
                    self.config.name,
                )
        objects_keys = [k for k in cloud_interface.list_bucket(backup_path + "/")]
        _logger.debug("Deleting all backup data from cloud path: %s" % backup_path)
        cloud_interface.delete_objects(
            objects_keys,
            check_locks=self.config.aws_check_object_lock,
        )
        # Lastly delete the backup manifest from the local meta dir, if it exists
        manifest_path = backup.get_backup_manifest_path()
        if os.path.exists(manifest_path):
            _logger.debug("Deleting backup manifest file: %s" % manifest_path)
            try:
                os.unlink(manifest_path)
            except OSError:
                output.warning(
                    "Failed to delete backup manifest file: %s. Please manually delete "
                    "this file if it still exists.",
                    manifest_path,
                )

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
        if self.server.use_wal_cloud_storage:
            # Leverage the cloud catalog to get the latest archived WALs info
            cloud_catalog = CloudBackupCatalog(
                self.server.get_wal_cloud_interface(), self.config.name
            )
            return cloud_catalog.get_latest_archived_wals_info()

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

                wals_removed = self.server.wal_storage.delete(wals_to_remove)

                fxlogdb_new.flush()
                fxlogdb_new.seek(0)
                fxlogdb.seek(0)
                shutil.copyfileobj(fxlogdb_new, fxlogdb)
                fxlogdb.truncate()

        return wals_removed

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
            backup = BackupInfoFactory.build_backup_info(
                self.server, backup_id=backup_id
            )
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
            backup = self.get_backup(backup_id)
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
            wal_full_path = self.server.wal_storage.get_full_path(wal)
            if not self.server.wal_storage.exists(wal_full_path):
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
        if self.server.use_backup_cloud_storage or self.server.use_wal_cloud_storage:
            output.error(
                "Backup verification is not supported for servers using cloud storage"
            )
            return

        if backup_info.encryption is not None:
            output.error("Backup verification is not supported for encrypted backups")
            return

        output.info("Calling pg_verifybackup")
        # Test pg_verifybackup existence
        version_info = PgVerifyBackup.get_version_info(self.server.path)
        if version_info.get("full_path", None) is None:
            output.error("pg_verifybackup not found")
            return

        # Determine the backup format
        backup_format = "tar" if backup_info.compression is not None else None

        # Verifying tar-format backups requires pg_verifybackup from Postgres 18+
        if backup_format == "tar":
            major_version = version_info.get("major_version")
            if major_version is None or major_version < Version("18"):
                output.error(
                    "Verification of tar format backups requires pg_verifybackup from "
                    "Postgres 18 or newer (found %s)" % version_info.get("full_version")
                )
                return

        pg_verifybackup = PgVerifyBackup(
            data_path=backup_info.get_data_directory(),
            command=version_info["full_path"],
            version=version_info["full_version"],
            format=backup_format,
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

    def export_backup(
        self,
        backup_info,
        output_filepath,
        identity_data,
        barman_data,
        compression=None,
        compression_level=None,
    ):
        """
        Export a completed backup to a portable tarball format.

        This method creates a tarball containing:
        - ``identity.json`` from server
        - ``backup.info`` metadata
        - ``barman.json`` with system information
        - ``backup/`` directory with complete backup data
        - ``wals/`` directory with all required WAL files preserving hash structure
        - ``xlog.db`` metadata file for exported WAL files

        Metadata files are written first so that the importer can validate
        server identity in streaming mode (``r|*``) before reading bulk data.

        The caller (server.py) is responsible for:
        - Generating the export filename
        - Collecting server-level metadata
        - Handling checksum calculation and file renaming
        - Cleanup on failure

        :param BackupInfo backup_info: the backup to export
        :param str output_filepath: full path to the output tarball file
        :param dict identity_data: server identity information
        :param dict barman_data: system information for ``barman.json``
        :param str|None compression: compression algorithm (``gzip``, ``bzip2``,
            ``xz``) or ``None`` for no compression
        :param int|None compression_level: compression level to pass to the
            compressor, or ``None`` for the default level
        """
        output.debug("Starting export of backup '%s'" % backup_info.backup_id)

        # Get source backup directory
        backup_dir = backup_info.get_basebackup_directory()

        # Given the CLI module already handles the validation of compression and
        # compression_level parameters, this branch is not expected to be hit, but we
        # add a defensive check here just in case.
        if compression is None and compression_level is not None:
            output.warning(
                "Compression level specified without compression algorithm, ignoring "
                "compression level"
            )
            compression_level = None

        # Build tarfile mode string and keyword arguments for compression
        compression_modes = {"gzip": "gz", "bzip2": "bz2", "xz": "xz"}
        mode_suffix = compression_modes.get(compression, "")
        tar_mode = "w|%s" % mode_suffix if mode_suffix else "w|"

        # ``tarfile`` only accepts the compression-level keyword argument for
        # streaming modes on recent interpreters: ``compresslevel`` for
        # ``w|gz`` / ``w|bz2`` requires Python 3.12+, and ``preset`` for
        # ``w|xz`` requires Python 3.14+. On older interpreters we warn and
        # fall back to the algorithm's default level so the export still
        # succeeds.
        tar_kwargs = {}
        if compression_level is not None:
            if compression == "xz":
                kwarg_name = "preset"
                required_version = (3, 14)
            else:
                kwarg_name = "compresslevel"
                required_version = (3, 12)

            if sys.version_info < required_version:
                output.warning(
                    "Python %d.%d does not support '%s' for streaming tar mode '%s'; "
                    "ignoring compression level and using the algorithm's default "
                    "level. Requires Python %d.%d or later.",
                    sys.version_info[0],
                    sys.version_info[1],
                    kwarg_name,
                    tar_mode,
                    required_version[0],
                    required_version[1],
                )
            else:
                tar_kwargs[kwarg_name] = compression_level

        # Create the export tarball
        output.debug("Creating export tarball at '%s'" % output_filepath)

        with tarfile.open(output_filepath, tar_mode, **tar_kwargs) as tar:
            # Add metadata files first so that the importer can validate
            # identity before reading the bulk data in streaming mode (r|*)
            self._add_metadata_to_tar(tar, identity_data, backup_info, barman_data)

            # Add backup data with backup/ prefix
            output.debug("Adding backup data from '%s'" % backup_dir)
            tar.add(backup_dir, arcname="backup")

            # Add WAL files and xlog.db
            self._add_wal_data_to_tar(tar, backup_info)

    def _add_metadata_to_tar(self, tar, identity_data, backup_info, barman_data):
        """
        Add metadata files to the tarball root.

        :param TarFile tar: the tar file object
        :param dict identity_data: server identity information
        :param BackupInfo backup_info: backup information object
        :param dict barman_data: system information
        """
        output.debug("Adding metadata files")

        # Add identity.json
        # 'identity_data' is known to be non-empty at this point, otherwise
        # the Server.export_backup would have errored out before.
        identity_json = json.dumps(identity_data, indent=4, sort_keys=True)
        self._add_json_to_tar(tar, "identity.json", identity_json)

        # Add backup.info file directly (preserves original format)
        tar.add(backup_info.get_filename(), arcname="backup.info")

        # Add barman.json
        barman_json = json.dumps(
            barman_data, cls=BarmanEncoderV2, indent=4, sort_keys=True
        )
        self._add_json_to_tar(tar, "barman.json", barman_json)

    def _add_json_to_tar(self, tar, filename, json_content):
        """
        Add a JSON string as a file to the tarball.

        :param TarFile tar: the tar file object
        :param str filename: name of the file in the tarball
        :param str json_content: JSON string content
        """
        json_bytes = json_content.encode("utf-8")
        tarinfo = tarfile.TarInfo(name=filename)
        tarinfo.size = len(json_bytes)
        tarinfo.mode = 0o644

        tar.addfile(tarinfo, io.BytesIO(json_bytes))

    def _add_wal_data_to_tar(self, tar, backup_info):
        """
        Add WAL files and xlog.db metadata to the tarball.

        This method exports all WAL files required for backup consistency
        from begin_wal to end_wal (inclusive) and creates an xlog.db file
        containing metadata for the exported WAL files. Uses a single-pass
        merge-step algorithm, isolated in a helper method.

        Uses a temporary file to avoid memory overhead when exporting backups
        with many WAL files.

        :param TarFile tar: the tar file object
        :param BackupInfo backup_info: the backup information
        """
        output.debug("Adding WAL files to export")

        # Use export directory for temporary file to avoid memory overhead. As the
        # export directory is expected to be big enough to hold the tarball, it should
        # also be big enough to hold the temporary xlog.db file, which is very small
        # compared to the tarball.
        export_dir = os.path.dirname(tar.name)
        xlogdb_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", dir=export_dir, suffix=".xlogdb", delete=False
            ) as xlogdb_file:
                xlogdb_path = xlogdb_file.name
                wal_count = self._collect_wal_files_for_export(
                    tar, backup_info, xlogdb_file
                )

            # Add xlog.db to tar
            tar.add(xlogdb_path, arcname="xlog.db")

            output.debug("Successfully added %d WAL files to export" % wal_count)
        finally:
            # Clean up temp file
            if xlogdb_path and os.path.exists(xlogdb_path):
                os.unlink(xlogdb_path)

    def _collect_wal_files_for_export(self, tar, backup_info, xlogdb_file):
        """
        Merge-step algorithm for collecting WAL files and xlog.db metadata for export.

        Writes xlog.db lines directly to the provided file object to avoid
        memory overhead for backups with many WAL files.

        :param TarFile tar: the tar file object
        :param BackupInfo backup_info: the backup information
        :param file xlogdb_file: file object to write xlog.db metadata lines
        :return: count of WAL files added to the tarball
        :rtype: int
        :raises ExportBackupException: for WAL/xlogdb mismatches or missing files
        """
        required_wals = backup_info.get_required_wal_segments()
        # There is no need to add a fallback value for next() here because the CLI
        # filters for DONE backups, which are guaranteed to have at least one required
        # WAL segment, even if begin_wal is equal to end_wal.
        current_required = next(required_wals)
        wal_count = 0

        with self.server.xlogdb("r") as fxlogdb:
            for line in fxlogdb:
                if current_required is None:
                    # At this point we found all required WAL files, so we can stop
                    # iterating through xlog.db
                    break
                wal_info = WalFileInfo.from_xlogdb_line(line)
                if wal_info.name < current_required:
                    continue
                if wal_info.name > current_required:
                    # Stop iterating as we did not find the required WAL in xlog.db.
                    # That will cause execution to error out of the loop and report the
                    # missing WAL file.
                    break
                wal_path = wal_info.fullpath(self.server)
                if not os.path.exists(wal_path):
                    raise ExportBackupException(
                        "WAL file required for backup consistency not found for backup "
                        "'%s': %s" % (backup_info.backup_id, wal_info.name)
                    )
                tar_path = "wals/%s" % wal_info.relpath()
                tar.add(wal_path, arcname=tar_path)
                xlogdb_file.write(line)
                wal_count += 1
                current_required = next(required_wals, None)

        if current_required is not None:
            raise ExportBackupException(
                "WAL file required for backup consistency not found in xlog.db for "
                "backup '%s': %s" % (backup_info.backup_id, current_required)
            )
        return wal_count

    def import_backup(self, input_tarball, local_identity, backup_id):
        """
        Import a backup from an exported tarball into the Barman catalog.

        Extracts the tarball to a temporary staging directory, validates server
        identity, registers backup metadata, moves backup data to the target
        location, imports WAL files and merges xlog.db entries. Uses a staging
        directory to make the write to the catalog atomic — a failed import
        leaves no partial state.

        :param str input_tarball: path to the exported backup tarball
        :param dict local_identity: local server identity data for validation
        :param str backup_id: the backup ID extracted from the tarball filename
        :raises ImportBackupException: on identity mismatch or invalid tarball
        """
        output.debug("Starting import of backup from '%s'" % input_tarball)

        # Check that the backup_id doesn't already exist in the catalog
        existing = self.get_backup(backup_id)
        if existing is not None:
            raise ImportBackupException(
                "Backup '%s' already exists in the catalog for server '%s'"
                % (backup_id, self.config.name)
            )

        # Validate server identity before extracting
        export_identity = self._read_identity_from_tarball(input_tarball)
        self._validate_import_identity(export_identity, local_identity)

        # Construct the BackupInfo object up front so that knowledge about
        # where the catalog lives on disk (target data directory, canonical
        # backup.info path) stays inside LocalBackupInfo rather than being
        # rebuilt from config + backup_id at multiple call sites.
        backup_info = LocalBackupInfo(self.server, backup_id=backup_id)

        staging_dir = tempfile.mkdtemp(
            dir=self.config.basebackups_directory,
            prefix=".import-",
        )

        try:
            # Extract tarball to staging directory
            output.debug("Extracting tarball to staging directory '%s'" % staging_dir)
            self._extract_tarball(input_tarball, staging_dir)

            # Pre-flight: verify the staging contents are structurally
            # valid and that the staged WAL set is safe to publish, BEFORE
            # we touch the catalog or move any data. ``backup_info`` is
            # populated as a side effect (still not persisted to the
            # catalog; ``_import_backup_metadata`` re-loads idempotently
            # and saves later).
            self._verify_staging(staging_dir, backup_info)

            # Refuse incremental backups: a block-level incremental backup is
            # only meaningful as part of its parent chain. Importing one in
            # isolation would leave the catalog referencing a parent that
            # may not exist on the target server.
            # Given export-backup disallows incremental backups, this branch should
            # never be exercised, but we add a safeguard just in case.
            if backup_info.is_incremental:
                raise ImportBackupException(
                    "Cannot import backup %s into server %s: it is an "
                    "incremental backup.\nOnly full backups are eligible "
                    "for importing." % (backup_info.backup_id, self.config.name)
                )

            # Recreate pg_tblspc symlinks that were skipped during tar
            # extraction.  Must run after _verify_staging so that
            # backup_info.tablespaces is populated from backup.info.
            self._reconstruct_tablespace_symlinks(staging_dir, backup_info)

            # Move backup data to its target location
            self._import_backup_data(staging_dir, backup_info)

            # Import WAL files and merge xlog.db entries. Returns a rollback
            # callable that undoes WAL moves and restores the original xlog.db
            # if a later step fails. If WAL import itself fails, roll back
            # the moved basebackup directory so the import leaves no partial
            # state on disk.
            try:
                wal_rollback = self._import_backup_wals(staging_dir)
            except Exception:
                target_dir = backup_info.get_basebackup_directory()
                if os.path.exists(target_dir):
                    shutil.rmtree(target_dir, ignore_errors=True)
                raise

            # Register backup metadata. If this fails, roll back WAL import
            # and data move so we do not leave orphaned state on disk.
            try:
                self._import_backup_metadata(staging_dir, backup_info)
            except Exception:
                wal_rollback()
                target_dir = backup_info.get_basebackup_directory()
                if os.path.exists(target_dir):
                    shutil.rmtree(target_dir, ignore_errors=True)
                raise

            output.info(
                "Successfully imported backup '%s' into server '%s'"
                % (backup_info.backup_id, self.config.name)
            )

            # Apply KEEP:STANDALONE annotation to protect the imported backup
            # from automatic deletion by retention policies. Failures are
            # non-fatal: the import has already succeeded at this point.
            try:
                self.keep_backup(backup_info.backup_id, KeepManager.TARGET_STANDALONE)
                output.debug(
                    "Applied KEEP:STANDALONE annotation to backup '%s'"
                    % backup_info.backup_id
                )
            except Exception as e:
                output.warning(
                    "Failed to apply KEEP:STANDALONE annotation to backup "
                    "'%s' on server '%s': %s. The imported backup may be "
                    "subject to retention policy deletion."
                    % (backup_info.backup_id, self.config.name, e)
                )
        finally:
            # Clean up the staging directory, removing leftovers
            # From successful or failed operations
            if os.path.exists(staging_dir):
                shutil.rmtree(staging_dir)

    def _extract_tarball(self, input_tarball, staging_dir):
        """
        Extract the exported tarball to a staging directory.

        Uses streaming mode (``r|*``) to keep only one TarInfo object in
        memory at a time, avoiding excessive memory usage for tarballs with
        many files.

        :param str input_tarball: path to the tarball
        :param str staging_dir: target directory for extraction
        :raises ImportBackupException: if extraction fails or an unsafe member
            is encountered. Symlinks under ``backup/data/pg_tblspc/`` are
            skipped rather than rejected.
        """
        try:
            with tarfile.open(input_tarball, "r|*") as tar:
                real_staging_dir = os.path.realpath(staging_dir)
                for member in tar:
                    # Security: prevent path traversal attacks. Compare against
                    # ``staging_dir + os.sep`` so that a sibling directory whose
                    # name is a string-prefix of the staging dir cannot slip
                    # through (e.g. ``.import-abc`` vs ``.import-abcXYZ``).
                    member_path = os.path.join(staging_dir, member.name)
                    real_member_path = os.path.realpath(member_path)
                    if real_member_path != real_staging_dir and not (
                        real_member_path.startswith(real_staging_dir + os.sep)
                    ):
                        raise ImportBackupException(
                            "Tarball contains unsafe path: '%s'" % member.name
                        )
                    # Security: reject links and other special file types.
                    # Only regular files and directories are expected in an
                    # exported backup tarball, with the single exception of
                    # pg_tblspc symlinks (see below).
                    if member.issym():
                        # pg_basebackup emits tablespace entries as symlinks
                        # directly under ``data/pg_tblspc/``.  These are
                        # legitimate — skip them here and let
                        # ``_reconstruct_tablespace_symlinks`` recreate them
                        # with the correct destination-catalog paths after
                        # ``backup.info`` has been loaded.
                        # Any other symlink (including nested paths such as
                        # ``backup/data/pg_tblspc/oid/something``) is still
                        # rejected as a potential path-traversal vector.
                        if (
                            member.name.startswith(_PG_TBLSPC_TARBALL_PREFIX)
                            and "/" not in member.name[len(_PG_TBLSPC_TARBALL_PREFIX) :]
                        ):
                            continue
                        raise ImportBackupException(
                            "Tarball contains unsafe member type: '%s'" % member.name
                        )
                    if not (member.isdir() or member.isreg()):
                        raise ImportBackupException(
                            "Tarball contains unsafe member type: '%s'" % member.name
                        )
                    tar.extract(member, path=staging_dir)
        except (tarfile.TarError, OSError) as e:
            raise ImportBackupException(
                "Failed to extract tarball '%s': %s" % (input_tarball, e)
            )

    def _reconstruct_tablespace_symlinks(self, staging_dir, backup_info):
        """
        Recreate ``pg_tblspc`` symlinks in the staging directory after tarball
        extraction.

        ``pg_basebackup`` emits tablespace entries as symlinks directly under
        ``data/pg_tblspc/<oid>``.  :meth:`_extract_tarball` skips these
        members to avoid the broad symlink-rejection check.  This method
        recreates them pointing to the correct destination-catalog paths
        (``backup_info.get_data_directory(oid)``) before the staging
        ``backup/`` tree is moved to its final location by
        :meth:`_import_backup_data`.

        This is a no-op when *backup_info* carries no tablespaces.

        :param str staging_dir: path to the staging directory
        :param LocalBackupInfo backup_info: backup info whose
            ``tablespaces`` attribute provides the OID list
        :raises ImportBackupException: if a symlink cannot be created
        """
        if not backup_info.tablespaces:
            return

        pg_tblspc_dir = os.path.join(staging_dir, "backup", "data", "pg_tblspc")
        if not os.path.isdir(pg_tblspc_dir):
            try:
                os.makedirs(pg_tblspc_dir)
            except OSError as e:
                raise ImportBackupException(
                    "Failed to create pg_tblspc directory '%s': %s" % (pg_tblspc_dir, e)
                )

        for tablespace in backup_info.tablespaces:
            link_path = os.path.join(pg_tblspc_dir, str(tablespace.oid))
            target_path = backup_info.get_data_directory(tablespace.oid)

            # ``lexists`` (not ``exists``) so that a broken symlink — one
            # whose target no longer exists — still enters this branch and
            # is inspected. ``exists`` follows links and would skip such an
            # entry, leaving it in place to break the ``os.symlink`` call
            # below. Refuse to remove a real directory: ``os.path.isdir``
            # follows symlinks, so pairing it with ``not islink`` isolates
            # the real-directory case (suspicious, abort) from the symlink-
            # to-directory case (a stale link from a previous import, safe
            # to replace).
            if os.path.lexists(link_path):
                if os.path.isdir(link_path) and not os.path.islink(link_path):
                    raise ImportBackupException(
                        "Cannot replace existing directory at tablespace link "
                        "path '%s'" % link_path
                    )
                try:
                    os.unlink(link_path)
                except OSError as e:
                    raise ImportBackupException(
                        "Failed to remove existing tablespace link path "
                        "'%s': %s" % (link_path, e)
                    )
            try:
                os.symlink(target_path, link_path)
            except OSError as e:
                raise ImportBackupException(
                    "Failed to create tablespace symlink '%s' -> '%s': %s"
                    % (link_path, target_path, e)
                )

    def _read_identity_from_tarball(self, input_tarball):
        """
        Read identity.json directly from the tarball without extracting.

        Uses streaming mode (``r|*``) to avoid loading all TarInfo objects
        into memory. Since the export writes metadata first, identity.json
        is found immediately.

        :param str input_tarball: path to the tarball
        :return: parsed identity data
        :rtype: dict
        :raises ImportBackupException: if identity.json is missing or unreadable
        """
        try:
            with tarfile.open(input_tarball, "r|*") as tar:
                for member in tar:
                    if member.name == "identity.json":
                        f = tar.extractfile(member)
                        if f is None:
                            raise ImportBackupException(
                                "identity.json in tarball is not a regular file"
                            )
                        with closing(f):
                            return json.loads(f.read().decode("utf-8"))
                raise ImportBackupException(
                    "Exported tarball does not contain identity.json"
                )
        except tarfile.TarError as e:
            raise ImportBackupException(
                "Failed to read identity.json from tarball: %s" % e
            )
        except (ValueError, json.JSONDecodeError) as e:
            raise ImportBackupException(
                "Failed to parse identity.json from tarball: %s" % e
            )

    def _validate_import_identity(self, export_identity, local_identity):
        """
        Validate that the exported backup belongs to the target server by
        comparing identity data.

        :param dict export_identity: identity data from the exported tarball
        :param dict local_identity: local server identity data
        :raises ImportBackupException: if identity validation fails
        """
        # Validate systemid match
        # "systemid" is expected to always be present in the identity data
        export_systemid = export_identity["systemid"]
        local_systemid = local_identity["systemid"]

        if export_systemid != local_systemid:
            raise ImportBackupException(
                "Server identity mismatch: exported backup has systemid '%s' "
                "but target server has systemid '%s'"
                % (export_systemid, local_systemid)
            )

        # Warn on PostgreSQL version mismatch (non-blocking)
        # "version" is expected to always be present in the identity data
        export_version = export_identity["version"]
        local_version = local_identity["version"]
        if export_version != local_version:
            output.warning(
                "PostgreSQL version mismatch: exported backup has version '%s' "
                "but target server has version '%s'" % (export_version, local_version)
            )

    def _import_backup_metadata(self, staging_dir, backup_info):
        """
        Load backup.info from the staging directory into ``backup_info``,
        persist it to the catalog, and register it in the cache. Assumes
        the pre-flight in :meth:`import_backup` has already verified that
        ``backup.info`` exists in the staging dir.

        :param str staging_dir: path to the staging directory
        :param LocalBackupInfo backup_info: the backup info object to
            populate from the staging file and register in the catalog
        :raises ImportBackupException: if metadata loading or registration fails
        """
        backup_info_path = os.path.join(staging_dir, "backup.info")

        try:
            backup_info.load(filename=backup_info_path)
            backup_info.save()
        except Exception as e:
            raise ImportBackupException("Failed to load backup.info: %s" % e)

        # Register in cache
        self.backup_cache_add(backup_info)

    def _verify_staging(self, staging_dir, backup_info):
        """
        Pre-flight: confirm the extracted staging directory is structurally
        valid and that the staged WAL set will be safe to publish into the
        catalog. Loads ``backup.info`` from staging into *backup_info* as
        a side effect so callers don't have to do it separately
        (:meth:`_import_backup_metadata` will re-load idempotently and persist
        later).

        :param str staging_dir: path to the staging directory
        :param LocalBackupInfo backup_info: backup info object to populate
            from the staging ``backup.info`` and use for WAL verification
        :raises ImportBackupException: on any structural or WAL-set issue
        """
        self._verify_staging_layout(staging_dir)

        backup_info_path = os.path.join(staging_dir, "backup.info")
        try:
            backup_info.load(filename=backup_info_path)
        except Exception as e:
            raise ImportBackupException("Failed to load backup.info: %s" % e)

        self._verify_staged_wals(staging_dir, backup_info)

    def _verify_staging_layout(self, staging_dir):
        """
        Verify that the staging directory contains the entries an exported
        tarball is expected to provide: ``backup/``, ``backup.info``,
        ``wals/``, and ``xlog.db``. Raises ``ImportBackupException`` with a
        clear message naming the missing entry on the first failure.

        :param str staging_dir: path to the staging directory
        :raises ImportBackupException: if any expected entry is missing
        """
        # (path, is_dir) pairs in the order we want to report missing entries
        expected = [
            ("backup", True),
            ("backup.info", False),
            ("wals", True),
            ("xlog.db", False),
        ]
        for path, is_dir in expected:
            full_path = os.path.join(staging_dir, path)
            if is_dir:
                if not os.path.isdir(full_path):
                    raise ImportBackupException(
                        "Exported tarball does not contain a '%s/' directory" % path
                    )
            else:
                if not os.path.isfile(full_path):
                    raise ImportBackupException(
                        "Exported tarball does not contain a '%s' file" % path
                    )

    def _iter_tarball_xlogdb(self, staging_dir):
        """
        Stream the tarball's ``xlog.db`` line by line. Yields
        ``(stripped_line, wal_name)`` for each non-blank entry, in the
        order the entries appear in the file (which is sorted by WAL
        name — the export side writes via a merge-step over the server
        catalog, so this invariant is guaranteed for any tarball produced
        by ``barman export-backup``).

        :param str staging_dir: path to the staging directory
        :raises ImportBackupException: on I/O errors opening the file or
            on a malformed line (with a line number)
        """
        path = os.path.join(staging_dir, "xlog.db")
        try:
            fp = open(path, "r")
        except IOError as e:
            raise ImportBackupException("Failed to read xlog.db from tarball: %s" % e)
        with fp:
            for line_num, raw_line in enumerate(fp, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    name = WalFileInfo.from_xlogdb_line(line).name
                except ValueError as e:
                    raise ImportBackupException(
                        "Malformed tarball's xlog.db entry at line %d: %s"
                        % (line_num, e)
                    )
                yield line, name

    def _verify_staged_wals(self, staging_dir, backup_info):
        """
        Single-pass verification of the staged WAL set. Iterates the staging
        ``xlog.db`` once, advancing pointers in lock-step against:

        - the required WAL segments from ``backup_info`` (sorted by name)
        - the target server's existing ``xlog.db`` (sorted by name)

        For each entry in the staging ``xlog.db`` this verifies:

        - it covers the next required WAL (no gap in the required range)
        - the physical file exists in ``staging_dir/wals/<hash>/<name>``
        - it does not collide with the server's ``xlog.db``
        - the physical file does not collide with the server's
          ``wals_directory`` (catches the case where xlog.db is stale)

        Memory is O(1) on both ``xlog.db`` streams; only the truncated list
        of conflict names is accumulated for error reporting.

        :param str staging_dir: path to the staging directory
        :param LocalBackupInfo backup_info: the backup info with begin/end_wal
        :raises ImportBackupException: on missing required WALs, missing
            physical files, malformed xlog.db, or conflicts with the target
            server's WAL archive
        """
        tarball_wals_dir = os.path.join(staging_dir, "wals")
        server_wals_dir = self.server.config.wals_directory

        required_iter = iter(backup_info.get_required_wal_segments())
        # There is no need to add a fallback value for next() here because the CLI
        # filters for DONE backups, which are guaranteed to have at least one required
        # WAL segment, even if begin_wal is equal to end_wal.
        next_required = next(required_iter)
        conflicts = []

        def _next_stripped(fp):
            line = next(fp, None)
            return line.strip() if line is not None else None

        with self.server.xlogdb("r") as server_fp:
            server_line = _next_stripped(server_fp)
            for tarball_line, wal_name in self._iter_tarball_xlogdb(staging_dir):
                hash_subdir = xlog.hash_dir(wal_name)
                tarball_wal_path = os.path.join(tarball_wals_dir, hash_subdir, wal_name)
                server_wal_path = os.path.join(server_wals_dir, hash_subdir, wal_name)

                # Required-WAL coverage: any required name strictly less
                # than the current tarball name was skipped, so the
                # tarball is missing it.
                if next_required is not None and next_required < wal_name:
                    raise ImportBackupException(
                        "WAL file required for backup consistency not found in "
                        "tarball's xlog.db: %s" % next_required
                    )
                if next_required == wal_name:
                    if not os.path.exists(tarball_wal_path):
                        raise ImportBackupException(
                            "WAL file listed in tarball's xlog.db but not found "
                            "under its wals/ directory: %s" % wal_name
                        )
                    next_required = next(required_iter, None)

                # Advance the server xlog.db pointer past anything sorting
                # before this WAL name, so the conflict check below sees
                # either the matching entry or the next one beyond.
                while (
                    server_line is not None
                    and WalFileInfo.from_xlogdb_line(server_line).name < wal_name
                ):
                    server_line = _next_stripped(server_fp)

                if self._wal_conflicts_with_server(
                    wal_name=wal_name,
                    tarball_line=tarball_line,
                    tarball_wal_path=tarball_wal_path,
                    server_line=server_line,
                    server_wal_path=server_wal_path,
                ):
                    conflicts.append(wal_name)

        if next_required is not None:
            raise ImportBackupException(
                "WAL file required for backup consistency not found in tarball's "
                "xlog.db: %s" % next_required
            )
        if conflicts:
            shown = conflicts[:5]
            msg = (
                "WAL file(s) already exist in target server with conflicting "
                "content: %s" % ", ".join(shown)
            )
            if len(conflicts) > 5:
                msg += " (and %d more)" % (len(conflicts) - 5)
            raise ImportBackupException(msg)

    def _wal_conflicts_with_server(
        self,
        wal_name,
        tarball_line,
        tarball_wal_path,
        server_line,
        server_wal_path,
    ):
        """
        Classify a tarball WAL against the server's current state.

        Returns ``True`` if the server has this WAL in a form that
        disagrees with the tarball — i.e., a real conflict that must
        block the import.

        Returns ``False`` for both safe cases:

        - **clean import**: the server has neither the xlog.db entry nor
          the file on disk.
        - **idempotent re-import**: the server has both the xlog.db entry
          and the file, AND the xlog.db metadata agrees on every field
          except ``compression`` (see note below), AND the WAL file
          contents are byte-equal. This legitimately happens whenever
          the tarball contains WALs still alive in the target server's
          catalog — for example, importing a tarball whose required WAL
          range overlaps with WALs the server has continued to archive
          on its own.

          A WAL's on-disk form is fixed at archive time (its compression
          and encryption mode are recorded in xlog.db and not re-applied
          later), so a byte-equal file comparison is sufficient — no
          decoding or decompression is required.

        .. note::
            ``compression`` is intentionally excluded from the metadata
            comparison. ``Server.rebuild_xlogdb`` (which is also our
            rollback mechanism — see ``_rollback_wal_import``) detects
            file format by reading magic bytes, and for a WAL that is
            both compressed AND encrypted, the visible outer magic is
            the encryption magic. A rebuild therefore clears the
            ``compression`` field that the original xlog.db had
            recorded. Without this exception, a previously-correct
            tarball would become non-importable any time the server's
            xlog.db gets rebuilt between export and import. The file
            content itself is the ground truth for "same WAL", and
            ``filecmp.cmp`` below verifies it.

        :param str wal_name: the WAL name from the tarball
        :param str tarball_line: stripped xlog.db line from the tarball
        :param str tarball_wal_path: path to the WAL file in the staged
            tarball
        :param str|None server_line: the server's xlog.db line at or after
            ``wal_name`` (caller is responsible for advancing the pointer)
        :param str server_wal_path: path to where this WAL would live in
            the server's ``wals_directory``
        :rtype: bool
        """
        server_has_entry = (
            server_line is not None
            and WalFileInfo.from_xlogdb_line(server_line).name == wal_name
        )
        server_has_file = os.path.exists(server_wal_path)

        # Clean import: server has nothing for this WAL.
        if not server_has_entry and not server_has_file:
            return False

        # Idempotent re-import: server has both artifacts and they match
        # the tarball.
        if (
            server_has_entry
            and server_has_file
            and self._xlogdb_metadata_match(tarball_line, server_line)
            and filecmp.cmp(tarball_wal_path, server_wal_path, shallow=False)
        ):
            return False

        return True

    def _xlogdb_metadata_match(self, line_a, line_b):
        """
        Compare two xlog.db lines for equivalence, ignoring the
        ``compression`` field. See the ``.. note::`` in
        :meth:`_wal_conflicts_with_server` for why ``compression`` is
        excluded.

        :param str line_a: stripped xlog.db line
        :param str line_b: stripped xlog.db line
        :rtype: bool
        """
        a = WalFileInfo.from_xlogdb_line(line_a)
        b = WalFileInfo.from_xlogdb_line(line_b)
        return (a.name, a.size, a.time, a.encryption) == (
            b.name,
            b.size,
            b.time,
            b.encryption,
        )

    def _import_backup_wals(self, staging_dir):
        """
        Import WAL files from the staging directory into the server's WAL
        archive and merge xlog.db entries into the server's WAL catalog.

        Uses a streaming merge-step to combine import entries with the
        existing xlog.db without loading the entire file into memory.
        Operates under the server's xlog.db lock to prevent concurrent
        modifications. Returns a rollback callable that will undo all changes
        (remove imported WAL files and rebuild xlog.db to a consistent state
        derived from the on-disk WAL archive) if a later step in the import
        process fails.

        :param str staging_dir: path to the staging directory
        :return: a callable that rolls back all WAL import side effects
        :rtype: callable
        :raises ImportBackupException: if WAL import fails
        """
        tarball_wals_dir = os.path.join(staging_dir, "wals")
        server_wals_dir = self.server.config.wals_directory
        server_xlogdb_dir = os.path.dirname(self.server.xlogdb_file_path)

        moved_files = []
        created_dirs = []

        def _move_tarball_wal(wal_name):
            """Move a single tarball WAL into the server's wals_directory
            and record the move for the rollback closure."""
            hash_subdir = xlog.hash_dir(wal_name)
            server_hash_dir = os.path.join(server_wals_dir, hash_subdir)
            if not os.path.exists(server_hash_dir):
                os.makedirs(server_hash_dir)
                created_dirs.append(server_hash_dir)
            tarball_wal_path = os.path.join(tarball_wals_dir, hash_subdir, wal_name)
            server_wal_path = os.path.join(server_hash_dir, wal_name)
            shutil.move(tarball_wal_path, server_wal_path)
            moved_files.append(server_wal_path)

        def _next_tarball_entry(iter_):
            """Advance the tarball xlog.db iterator, returning
            ``(None, None)`` at exhaustion so the merge-step below can
            test ``tarball_name is None`` instead of catching
            ``StopIteration``."""
            return next(iter_, (None, None))

        try:
            # Single-pass merge of the tarball into the server catalog.
            # Both xlog.db streams are sorted by WAL name (fixed-width
            # hex), so the merge runs in O(1) extra memory. For each
            # tarball entry not present in the server we move its WAL
            # file into the server's wals_directory and write its
            # xlog.db line into a scratch tmp file. For each tarball
            # entry that DOES match a server entry, ``_verify_staged_wals``
            # has already guaranteed the entry is bit-identical to the
            # server's (idempotent re-import), so the move is skipped —
            # the file is already in place — and the line is emitted
            # from the server side, not duplicated from the tarball.
            # When the merged tmp file is complete it is copied back
            # into the live xlog.db.
            with self.server.xlogdb("r+") as server_fp:
                with tempfile.TemporaryFile(mode="w+", dir=server_xlogdb_dir) as tmp:
                    tarball_iter = self._iter_tarball_xlogdb(staging_dir)
                    tarball_line, tarball_name = _next_tarball_entry(tarball_iter)

                    for raw_server_line in server_fp:
                        server_line = raw_server_line.strip()
                        if not server_line:
                            continue
                        server_name = WalFileInfo.from_xlogdb_line(server_line).name

                        # Tarball entries strictly before this server
                        # entry: real new imports — move + emit.
                        while tarball_name is not None and tarball_name < server_name:
                            _move_tarball_wal(tarball_name)
                            tmp.write(tarball_line + "\n")
                            tarball_line, tarball_name = _next_tarball_entry(
                                tarball_iter
                            )

                        # Same name on both sides: idempotent re-import,
                        # advance the tarball iterator without moving or
                        # emitting (the server's line covers it).
                        if tarball_name == server_name:
                            tarball_line, tarball_name = _next_tarball_entry(
                                tarball_iter
                            )

                        tmp.write(server_line + "\n")

                    # Drain any tarball entries beyond the end of the
                    # server xlog.db: all real new imports.
                    while tarball_name is not None:
                        _move_tarball_wal(tarball_name)
                        tmp.write(tarball_line + "\n")
                        tarball_line, tarball_name = _next_tarball_entry(tarball_iter)

                    tmp.flush()
                    tmp.seek(0)
                    server_fp.seek(0)
                    shutil.copyfileobj(tmp, server_fp)
                    server_fp.truncate()

            output.debug("Successfully imported %d WAL files" % len(moved_files))

        except Exception as e:
            # Roll back: remove moved WAL files and rebuild xlog.db
            self._rollback_wal_import(moved_files, created_dirs)
            if isinstance(e, ImportBackupException):
                raise
            raise ImportBackupException("Failed to import WAL files: %s" % e)

        # Build and return a rollback closure for the caller to invoke
        # if a later step (e.g. metadata registration) fails
        def rollback():
            self._rollback_wal_import(moved_files, created_dirs)

        return rollback

    def _rollback_wal_import(self, moved_files, created_dirs):
        """
        Undo WAL import side effects: remove moved WAL files, remove
        newly-created empty hash directories, and rebuild xlog.db from
        the WAL archive on disk.

        :param list moved_files: list of WAL file paths that were moved
        :param list created_dirs: list of hash directories that were created
        """
        # Remove moved WAL files
        for filepath in moved_files:
            try:
                if os.path.exists(filepath):
                    os.unlink(filepath)
            except OSError:
                _logger.warning("Rollback: failed to remove WAL file '%s'" % filepath)

        # Remove newly-created empty hash directories
        for dirpath in created_dirs:
            try:
                if os.path.isdir(dirpath) and not os.listdir(dirpath):
                    os.rmdir(dirpath)
            except OSError:
                _logger.warning("Rollback: failed to remove directory '%s'" % dirpath)

        # Rebuild xlog.db from the WAL files actually on disk, which now
        # excludes the removed imports, restoring the pre-import state.
        self.server.rebuild_xlogdb(silent=True)

    def _import_backup_data(self, staging_dir, backup_info):
        """
        Move backup data from the staging directory to the target backup
        location. Assumes the pre-flight in :meth:`import_backup` has
        already verified that ``backup/`` exists in the staging dir.

        :param str staging_dir: path to the staging directory
        :param LocalBackupInfo backup_info: the backup info object whose
            ``get_basebackup_directory()`` provides the target path
        :raises ImportBackupException: if the data move fails
        """
        source_dir = os.path.join(staging_dir, "backup")
        target_dir = backup_info.get_basebackup_directory()

        try:
            os.rename(source_dir, target_dir)
        except OSError as e:
            raise ImportBackupException(
                "Failed to move backup data to '%s': %s" % (target_dir, e)
            )

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
