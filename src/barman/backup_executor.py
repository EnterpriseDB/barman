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
Backup Executor module

A Backup Executor is a class responsible for the execution
of a backup. Specific implementations of backups are defined by
classes that derive from BackupExecutor (e.g.: backup with rsync
through Ssh).

A BackupExecutor is invoked by the BackupManager for backup operations.
"""

import datetime
import logging
import os
import queue
import re
import shutil
import threading
import time
from abc import ABCMeta, abstractmethod
from contextlib import closing
from functools import partial
from io import BytesIO

import dateutil.parser

from barman import output, xlog
from barman.cloud_providers import get_snapshot_interface_from_server_config
from barman.command_wrappers import PgBaseBackup
from barman.compression import get_pg_basebackup_compression
from barman.config import BackupOptions
from barman.copy_controller import RsyncCopyController
from barman.exceptions import (
    BackupException,
    CommandFailedException,
    DataTransferFailure,
    FileNotFoundException,
    FsOperationFailed,
    PostgresConnectionError,
    PostgresConnectionLost,
    PostgresIsInRecovery,
    SnapshotBackupException,
    SshCommandException,
)
from barman.fs import (
    UnixLocalCommand,
    UnixRemoteCommand,
    path_allowed,
    unix_command_factory,
)
from barman.infofile import BackupInfo
from barman.postgres import PostgresKeepAlive
from barman.postgres_plumbing import EXCLUDE_LIST, PGDATA_EXCLUDE_LIST
from barman.remote_status import RemoteStatusMixin
from barman.utils import LooseVersion as Version
from barman.utils import (
    check_aws_expiration_date_format,
    check_aws_snapshot_lock_cool_off_period_range,
    check_aws_snapshot_lock_duration_range,
    check_aws_snapshot_lock_mode,
    force_str,
    get_directory_size,
    human_readable_timedelta,
    mkpath,
    total_seconds,
    with_metaclass,
)

_logger = logging.getLogger(__name__)


class BackupExecutor(with_metaclass(ABCMeta, RemoteStatusMixin)):
    """
    Abstract base class for any backup executors.
    """

    def __init__(self, backup_manager, mode=None):
        """
        Base constructor

        :param barman.backup.BackupManager backup_manager: the BackupManager
            assigned to the executor
        :param str mode: The mode used by the executor for the backup.
        """
        super(BackupExecutor, self).__init__()
        self.backup_manager = backup_manager
        self.server = backup_manager.server
        self.config = backup_manager.config
        self.strategy = None
        self._mode = mode
        self.copy_start_time = None
        self.copy_end_time = None

        # Holds the action being executed. Used for error messages.
        self.current_action = None

    def init(self):
        """
        Initialise the internal state of the backup executor
        """
        self.current_action = "starting backup"

    @property
    def mode(self):
        """
        Property that defines the mode used for the backup.

        If a strategy is present, the returned string is a combination
        of the mode of the executor and the mode of the strategy
        (eg: rsync-exclusive)

        :return str: a string describing the mode used for the backup
        """
        strategy_mode = self.strategy.mode
        if strategy_mode:
            return "%s-%s" % (self._mode, strategy_mode)
        else:
            return self._mode

    @abstractmethod
    def backup(self, backup_info):
        """
        Perform a backup for the server - invoked by BackupManager.backup()

        :param barman.infofile.LocalBackupInfo backup_info: backup information
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

    def _purge_unused_wal_files(self, backup_info):
        """
        If the provided backup is the first, purge unused WAL files before the backup
        start.

        .. note::
            If ``worm_mode`` is enabled, then we don't remove those WAL files
            because they are (should be) stored in an immutable storage, and at
            this point the grace period might already have been expired.

        :param barman.infofile.LocalBackupInfo backup_info: The backup to check.
        """
        if backup_info.begin_wal is None:
            return

        previous_backup = self.backup_manager.get_previous_backup(backup_info.backup_id)
        if not previous_backup:
            output.info("This is the first backup for server %s", self.config.name)
            if self.config.worm_mode is True:
                output.info("'worm_mode' is enabled, skip purging of unused WAL files.")
                return
            removed = self.backup_manager.remove_wal_before_backup(backup_info)
            if removed:
                # report the list of the removed WAL files
                output.info(
                    "WAL segments preceding the current backup have been found:",
                    log=False,
                )
                for wal_name in removed:
                    output.info(
                        "\t%s from server %s has been removed",
                        wal_name,
                        self.config.name,
                    )

    def _start_backup_copy_message(self, backup_info):
        """
        Output message for backup start

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """
        output.info("Copying files for %s", backup_info.backup_id)

    def _stop_backup_copy_message(self, backup_info):
        """
        Output message for backup end

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """
        output.info(
            "Copy done (time: %s)",
            human_readable_timedelta(
                datetime.timedelta(seconds=backup_info.copy_stats["copy_time"])
            ),
        )


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


def _get_bandwidth_limit_in_bytes(bandwidth_limit_kb):
    """
    Convert a bandwidth limit from kB/s to B/s.

    .. note::
        This is useful when reusing the configuration option ``bandwidth_limit``, which
        was originally designed for rsync and pg_basebackup backups (which take a value
        in kB/s), for cloud uploads (which expect the bandwidth limit in B/s).

    :param bandwidth_limit_kb: the bandwidth limit in kB/s
    :return int: the bandwidth limit in B/s
    """
    if bandwidth_limit_kb is not None:
        return bandwidth_limit_kb * 1000
    return None


class CloudBackupExecutor(BackupExecutor):
    """
    Backup executor for direct cloud backups (``backup_method = local-to-cloud``).

    This executor coordinates with PostgreSQL using the low-level backup API and uploads
    the backup directly to cloud object storage. It reads from PGDATA and uploads
    directly to the cloud, with no relaying through an intermediate storage. It
    delegates the complete backup process to
    :class:`~barman.cloud.CloudBackupUploader.coordinate_backup()`, which handles the
    full backup lifecycle including starting the backup, reading and uploading data,
    stopping the backup, uploading the backup_label, and finalizing the upload.

    .. note::
        This class inherits directly from :class:`BackupExecutor` rather than
        :class:`ExternalBackupExecutor` due to ``backup_label`` sequencing requirements.
        The :class:`ExternalBackupExecutor` flow (start_backup -> backup_copy ->
        stop_backup) would provide the ``backup_label`` only after ``backup_copy``
        completes. However, :class:`~barman.cloud.CloudBackupUploader` needs to upload
        the ``backup_label`` before finalizing the upload controller.

        By inheriting directly from :class:`BackupExecutor` and delegating to
        :meth:`~barman.cloud.CloudBackupUploader.coordinate_backup()`, we reuse
        existing cloud backup logic without duplication while respecting the required
        sequencing constraints.

    .. note::
        We could not simply reuse the entire :class:`CloudBackupUploader` because
        ``barman backup`` works on top of `BackupExecutor` which defines a specific
        interface and flow for backup execution, and the :class:`CloudBackupUploader`
        class is based on :class:`CloudBackup`, which has a some overlapping but not
        identical logic to the backup flow defined by `BackupExecutor`.
    """

    def __init__(self, backup_manager):
        """
        Constructor

        :param barman.backup.BackupManager backup_manager: the BackupManager
            assigned to the executor
        """
        super(CloudBackupExecutor, self).__init__(backup_manager, "local-to-cloud")
        # We set a strategy only because it's required by the BackupExecutor interface,
        # but the actual backup logic is implemented in the backup() method of this
        # class, which reuses part of the logic of CloudBackupUploader.
        self.strategy = ConcurrentBackupStrategy(self.server.postgres, self.config.name)

    def backup(self, backup_info):
        """
        Perform a backup for the server.

        This implementation uploads the backup directly to cloud storage using part of
        the logic from the :class:`CloudBackupUploader` class.

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """
        from barman.cloud import CloudBackupUploader

        _logger.info("Starting cloud backup for server %s", self.config.name)

        postgres = self.server.postgres
        cloud_interface = self.server.get_backup_cloud_interface()

        # Create the uploader with proper configuration
        with closing(cloud_interface):
            with closing(postgres):
                uploader = CloudBackupUploader(
                    server_name=self.config.name,
                    cloud_interface=cloud_interface,
                    max_archive_size=self.config.cloud_upload_max_archive_size,
                    postgres=postgres,
                    backup_name=getattr(backup_info, "backup_name", None),
                    min_chunk_size=self.config.cloud_upload_min_chunk_size,
                    max_bandwidth=_get_bandwidth_limit_in_bytes(
                        self.config.bandwidth_limit
                    ),
                )

                # Set the backup_info that was already started by BackupExecutor.
                # CloudBackupUploader normally creates its own, but we reuse the one
                # from BackupManager. We do that because the manager sets the fields the
                # way it expects to use them later, which diverges a bit from the way
                # CloudBackupUploader would set them. That way we avoid possible
                # inconsistencies later when running other barman commands through the
                # server.
                uploader.backup_info = backup_info

                # Create the upload controller. Normally, when using the method
                # 'uploader.backup', it would take care of that.
                uploader.controller = uploader.create_upload_controller(
                    backup_info.backup_id
                )

                # Coordinate the entire backup
                # This handles: start_backup -> upload data -> stop_backup -> upload
                #   label -> close controller
                try:
                    uploader.coordinate_backup()
                except SystemExit as exc:
                    raise BackupException(
                        "Cloud backup failed with error. Please check the logs for details."
                    ) from exc

                # Copy timing info back to executor
                self.copy_start_time = uploader.copy_start_time
                self.copy_end_time = uploader.copy_end_time

                # Fill size information in backup_info
                backup_info.set_attribute("size", uploader.controller.size)
                backup_info.set_attribute("deduplicated_size", uploader.controller.size)

        # If this is the first backup, purge eventually unused WAL files
        self._purge_unused_wal_files(backup_info)


class PostgresBackupExecutor(BackupExecutor):
    """
    Concrete class for backup via pg_basebackup (plain format).

    Relies on pg_basebackup command to copy data files from the PostgreSQL
    cluster using replication protocol.
    """

    def __init__(self, backup_manager):
        """
        Constructor

        :param barman.backup.BackupManager backup_manager: the BackupManager
            assigned to the executor
        """
        super(PostgresBackupExecutor, self).__init__(backup_manager, "postgres")
        self.backup_compression = get_pg_basebackup_compression(self.server)
        self.validate_configuration()
        self.strategy = PostgresBackupStrategy(
            self.server.postgres, self.config.name, self.backup_compression
        )

    def validate_configuration(self):
        """
        Validate the configuration for this backup executor.

        If the configuration is not compatible this method will disable the
        server.
        """

        # Check for the correct backup options
        if BackupOptions.EXCLUSIVE_BACKUP in self.config.backup_options:
            self.config.backup_options.remove(BackupOptions.EXCLUSIVE_BACKUP)
            output.warning(
                "'exclusive_backup' is not a valid backup_option "
                "using postgres backup_method. "
                "Overriding with 'concurrent_backup'."
            )

        # Apply the default backup strategy
        if BackupOptions.CONCURRENT_BACKUP not in self.config.backup_options:
            self.config.backup_options.add(BackupOptions.CONCURRENT_BACKUP)
            output.debug(
                "The default backup strategy for "
                "postgres backup_method is: concurrent_backup"
            )

        # Forbid tablespace_bandwidth_limit option.
        # It works only with rsync based backups.
        if self.config.tablespace_bandwidth_limit:
            # Report the error in the configuration errors message list
            self.server.config.update_msg_list_and_disable_server(
                "tablespace_bandwidth_limit option is not supported by "
                "postgres backup_method"
            )

        # Forbid reuse_backup option.
        # It works only with rsync based backups.
        if self.config.reuse_backup in ("copy", "link"):
            # Report the error in the configuration errors message list
            self.server.config.update_msg_list_and_disable_server(
                "reuse_backup option is not supported by postgres backup_method"
            )

        # Forbid network_compression option.
        # It works only with rsync based backups.
        if self.config.network_compression:
            # Report the error in the configuration errors message list
            self.server.config.update_msg_list_and_disable_server(
                "network_compression option is not supported by "
                "postgres backup_method"
            )

        # The following checks require interactions with the PostgreSQL server
        # therefore they are carried out within a `closing` context manager to
        # ensure the connection is not left dangling in cases where no further
        # server interaction is required.
        remote_status = None
        with closing(self.server):
            if self.server.config.bandwidth_limit or self.backup_compression:
                # This method is invoked too early to have a working streaming
                # connection. So we avoid caching the result by directly
                # invoking fetch_remote_status() instead of get_remote_status()
                remote_status = self.fetch_remote_status()

            # bandwidth_limit option is supported by pg_basebackup executable
            # starting from Postgres 9.4
            if (
                self.server.config.bandwidth_limit
                and remote_status["pg_basebackup_bwlimit"] is False
            ):
                # If pg_basebackup is present and it doesn't support bwlimit
                # disable the server.
                # Report the error in the configuration errors message list
                self.server.config.update_msg_list_and_disable_server(
                    "bandwidth_limit option is not supported by "
                    "pg_basebackup version (current: %s, required: 9.4)"
                    % remote_status["pg_basebackup_version"]
                )

            # validate compression options
            if self.backup_compression:
                self._validate_compression(remote_status)

    def _validate_compression(self, remote_status):
        """
        In charge of validating compression options.

        Note: Because this method requires a connection to the PostgreSQL server it
        should be called within the context of a closing context manager.

        :param remote_status:
        :return:
        """
        try:
            issues = self.backup_compression.validate(
                self.server.postgres.server_version, remote_status
            )
            if issues:
                self.server.config.update_msg_list_and_disable_server(issues)
        except PostgresConnectionError as exc:
            # If we can't validate the compression settings due to a connection error
            # it should not block whatever Barman is trying to do *unless* it is
            # doing a backup, in which case the pre-backup check will catch the
            # connection error and fail accordingly.
            # This is important because if the server is unavailable Barman
            # commands such as `recover` and `list-backups` must not break.
            _logger.warning(
                (
                    "Could not validate compression due to a problem "
                    "with the PostgreSQL connection: %s"
                ),
                exc,
            )

    def backup(self, backup_info):
        """
        Perform a backup for the server - invoked by BackupManager.backup()
        through the generic interface of a BackupExecutor.

        This implementation is responsible for performing a backup through the
        streaming protocol.

        The connection must be made with a superuser or a user having
        REPLICATION permissions (see PostgreSQL documentation, Section 20.2),
        and pg_hba.conf must explicitly permit the replication connection.
        The server must also be configured with enough max_wal_senders to leave
        at least one session available for the backup.

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """
        try:
            # Set data directory and server version
            self.strategy.start_backup(backup_info)
            backup_info.save()

            if backup_info.begin_wal is not None:
                output.info(
                    "Backup start at LSN: %s (%s, %08X)",
                    backup_info.begin_xlog,
                    backup_info.begin_wal,
                    backup_info.begin_offset,
                )
            else:
                output.info("Backup start at LSN: %s", backup_info.begin_xlog)

            # Start the copy
            self.current_action = "copying files"
            self._start_backup_copy_message(backup_info)
            self.backup_copy(backup_info)
            self._stop_backup_copy_message(backup_info)
            self.strategy.stop_backup(backup_info)

            # If this is the first backup, purge eventually unused WAL files
            self._purge_unused_wal_files(backup_info)
        except CommandFailedException as e:
            _logger.exception(e)
            raise

    def check(self, check_strategy):
        """
        Perform additional checks for PostgresBackupExecutor

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        check_strategy.init_check("pg_basebackup")
        remote_status = self.get_remote_status()

        # Check for the presence of pg_basebackup
        check_strategy.result(
            self.config.name, remote_status["pg_basebackup_installed"]
        )

        # remote_status['pg_basebackup_compatible'] is None if
        # pg_basebackup cannot be executed and False if it is
        # not compatible.
        hint = None
        check_strategy.init_check("pg_basebackup compatible")
        if not remote_status["pg_basebackup_compatible"]:
            pg_version = "Unknown"
            basebackup_version = "Unknown"
            if self.server.streaming is not None:
                pg_version = self.server.streaming.server_txt_version
            if remote_status["pg_basebackup_version"] is not None:
                basebackup_version = remote_status["pg_basebackup_version"]
            hint = "PostgreSQL version: %s, pg_basebackup version: %s" % (
                pg_version,
                basebackup_version,
            )
        check_strategy.result(
            self.config.name, remote_status["pg_basebackup_compatible"], hint=hint
        )

        # Skip further checks if the postgres connection doesn't work.
        # We assume that this error condition will be reported by
        # another check.
        postgres = self.server.postgres
        if postgres is None or postgres.server_txt_version is None:
            return

        check_strategy.init_check("pg_basebackup supports tablespaces mapping")
        # We can't backup a cluster with tablespaces if the tablespace
        # mapping option is not available in the installed version
        # of pg_basebackup.
        pg_version = Version(postgres.server_txt_version)
        tablespaces_list = postgres.get_tablespaces()

        # pg_basebackup supports the tablespace-mapping option,
        # so there are no problems in this case
        if remote_status["pg_basebackup_tbls_mapping"]:
            hint = None
            check_result = True

        # pg_basebackup doesn't support the tablespace-mapping option
        # and the data directory contains tablespaces, we can't correctly
        # backup it.
        elif tablespaces_list:
            check_result = False

            if pg_version < "9.3":
                hint = (
                    "pg_basebackup can't be used with tablespaces "
                    "and PostgreSQL older than 9.3"
                )
            else:
                hint = "pg_basebackup 9.4 or higher is required for tablespaces support"

        # Even if pg_basebackup doesn't support the tablespace-mapping
        # option, this location can be correctly backed up as doesn't
        # have any tablespaces
        else:
            check_result = True
            if pg_version < "9.3":
                hint = (
                    "pg_basebackup can be used as long as tablespaces "
                    "support is not required"
                )
            else:
                hint = "pg_basebackup 9.4 or higher is required for tablespaces support"

        check_strategy.result(self.config.name, check_result, hint=hint)

    def fetch_remote_status(self):
        """
        Gather info from the remote server.

        This method does not raise any exception in case of errors,
        but set the missing values to None in the resulting dictionary.
        """
        remote_status = dict.fromkeys(
            (
                "pg_basebackup_compatible",
                "pg_basebackup_installed",
                "pg_basebackup_tbls_mapping",
                "pg_basebackup_path",
                "pg_basebackup_bwlimit",
                "pg_basebackup_version",
            ),
            None,
        )

        # Test pg_basebackup existence
        version_info = PgBaseBackup.get_version_info(self.server.path)
        if version_info["full_path"]:
            remote_status["pg_basebackup_installed"] = True
            remote_status["pg_basebackup_path"] = version_info["full_path"]
            remote_status["pg_basebackup_version"] = version_info["full_version"]
            pgbasebackup_version = version_info["major_version"]
        else:
            remote_status["pg_basebackup_installed"] = False
            return remote_status

        # Is bandwidth limit supported?
        if (
            remote_status["pg_basebackup_version"] is not None
            and remote_status["pg_basebackup_version"] < "9.4"
        ):
            remote_status["pg_basebackup_bwlimit"] = False
        else:
            remote_status["pg_basebackup_bwlimit"] = True

        # Is the tablespace mapping option supported?
        if pgbasebackup_version >= "9.4":
            remote_status["pg_basebackup_tbls_mapping"] = True
        else:
            remote_status["pg_basebackup_tbls_mapping"] = False

        # Retrieve the PostgreSQL version
        pg_version = None
        if self.server.streaming is not None:
            pg_version = self.server.streaming.server_major_version

        # If any of the two versions is unknown, we can't compare them
        if pgbasebackup_version is None or pg_version is None:
            # Return here. We are unable to retrieve
            # pg_basebackup or PostgreSQL versions
            return remote_status

        # pg_version is not None so transform into a Version object
        # for easier comparison between versions
        pg_version = Version(pg_version)

        # pg_basebackup 9.2 is compatible only with PostgreSQL 9.2.
        if "9.2" == pg_version == pgbasebackup_version:
            remote_status["pg_basebackup_compatible"] = True

        # other versions are compatible with lesser versions of PostgreSQL
        # WARNING: The development versions of `pg_basebackup` are considered
        # higher than the stable versions here, but this is not an issue
        # because it accepts everything that is less than
        # the `pg_basebackup` version(e.g. '9.6' is less than '9.6devel')
        elif "9.2" < pg_version <= pgbasebackup_version:
            remote_status["pg_basebackup_compatible"] = True
        else:
            remote_status["pg_basebackup_compatible"] = False

        return remote_status

    def backup_copy(self, backup_info):
        """
        Perform the actual copy of the backup using pg_basebackup.
        First, manages tablespaces, then copies the base backup
        using the streaming protocol.

        In case of failure during the execution of the pg_basebackup command
        the method raises a DataTransferFailure, this trigger the retrying
        mechanism when necessary.

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """

        # Make sure the destination directory exists, ensure the
        # right permissions to the destination dir
        backup_dest = backup_info.get_data_directory()
        dest_dirs = [backup_dest]

        # Store the start time
        self.copy_start_time = datetime.datetime.now()

        # Manage tablespaces, we need to handle them now in order to
        # be able to relocate them inside the
        # destination directory of the basebackup
        tbs_map = {}
        if backup_info.tablespaces:
            for tablespace in backup_info.tablespaces:
                source = tablespace.location
                destination = backup_info.get_data_directory(tablespace.oid)
                tbs_map[source] = destination
                dest_dirs.append(destination)

        # Prepare the destination directories for pgdata and tablespaces
        self._prepare_backup_destination(dest_dirs)

        # Retrieve pg_basebackup version information
        remote_status = self.get_remote_status()

        # If pg_basebackup supports --max-rate set the bandwidth_limit
        bandwidth_limit = self._get_bandwidth_limit(remote_status)

        # Make sure we are not wasting precious PostgreSQL resources
        # for the whole duration of the copy
        self.server.close()

        # Find the backup_manifest file path of the parent backup in case
        # it is an incremental backup
        parent_backup_info = backup_info.get_parent_backup_info()
        parent_backup_manifest_path = None
        if parent_backup_info:
            parent_backup_manifest_path = parent_backup_info.get_backup_manifest_path()

        pg_basebackup = PgBaseBackup(
            connection=self.server.streaming,
            destination=backup_dest,
            command=remote_status["pg_basebackup_path"],
            version=remote_status["pg_basebackup_version"],
            app_name=self.config.streaming_backup_name,
            tbs_mapping=tbs_map,
            bwlimit=bandwidth_limit,
            immediate=self.config.immediate_checkpoint,
            path=self.server.path,
            retry_times=self.config.basebackup_retry_times,
            retry_sleep=self.config.basebackup_retry_sleep,
            retry_handler=partial(self._retry_handler, dest_dirs),
            compression=self.backup_compression,
            err_handler=self._err_handler,
            out_handler=PgBaseBackup.make_logging_handler(logging.INFO),
            parent_backup_manifest_path=parent_backup_manifest_path,
            warehousepg_dbid=self.config.warehousepg_dbid,
        )

        # Do the actual copy
        try:
            pg_basebackup()
        except CommandFailedException as e:
            msg = (
                "data transfer failure on directory '%s'"
                % backup_info.get_data_directory()
            )
            raise DataTransferFailure.from_command_error("pg_basebackup", e, msg)

        # Store the end time
        self.copy_end_time = datetime.datetime.now()

        # Store statistics about the copy
        copy_time = total_seconds(self.copy_end_time - self.copy_start_time)
        backup_info.copy_stats = {
            "copy_time": copy_time,
            "total_time": copy_time,
        }

        # Check for the presence of configuration files outside the PGDATA
        external_config = backup_info.get_external_config_files()
        if any(external_config):
            msg = (
                "pg_basebackup does not copy the PostgreSQL "
                "configuration files that reside outside PGDATA. "
                "Please manually backup the following files:\n"
                "\t%s\n" % "\n\t".join(ecf.path for ecf in external_config)
            )
            # Show the warning only if the EXTERNAL_CONFIGURATION option
            # is not specified in the backup_options.
            if BackupOptions.EXTERNAL_CONFIGURATION not in self.config.backup_options:
                output.warning(msg)
            else:
                _logger.debug(msg)

    def _get_bandwidth_limit(self, remote_status):
        """
        Get the bandwidth limit for ``pg_basebackup``, if supported.

        :param dict[str,Any] remote_status: remote status information
        :return int|None: the bandwidth limit configured, if any
        """
        if remote_status["pg_basebackup_bwlimit"]:
            return self.config.bandwidth_limit

    def _retry_handler(self, dest_dirs, command, args, kwargs, attempt, exc):
        """
        Handler invoked during a backup in case of retry.

        The method simply warn the user of the failure and
        remove the already existing directories of the backup.

        :param list[str] dest_dirs: destination directories
        :param RsyncPgData command: Command object being executed
        :param list args: command args
        :param dict kwargs: command kwargs
        :param int attempt: attempt number (starting from 0)
        :param CommandFailedException exc: the exception which caused the
            failure
        """
        output.warning(
            "Failure executing a backup using pg_basebackup (attempt %s)", attempt
        )
        output.warning(
            "The files copied so far will be removed and "
            "the backup process will restart in %s seconds",
            self.config.basebackup_retry_sleep,
        )
        # Remove all the destination directories and reinit the backup
        self._prepare_backup_destination(dest_dirs)

    def _err_handler(self, line):
        """
        Handler invoked during a backup when anything is sent to stderr.

        Used to perform a WAL switch on a primary server if pg_basebackup
        is running against a standby, otherwise just logs output at INFO
        level.

        :param str line: The error line to be handled.
        """

        # Always log the line, since this handler will have overridden the
        # default command err_handler.
        # Although this is used as a stderr handler, the pg_basebackup lines
        # logged here are more appropriate at INFO level since they are just
        # describing regular behaviour.
        _logger.log(logging.INFO, "%s", line)
        if (
            self.server.config.primary_conninfo is not None
            and "waiting for required WAL segments to be archived" in line
        ):
            # If pg_basebackup is waiting for WAL segments and primary_conninfo
            # is configured then we are backing up a standby and must manually
            # perform a WAL switch.
            self.server.postgres.switch_wal()

    def _prepare_backup_destination(self, dest_dirs):
        """
        Prepare the destination of the backup, including tablespaces.

        This method is also responsible for removing a directory if
        it already exists and for ensuring the correct permissions for
        the created directories

        :param list[str] dest_dirs: destination directories
        """
        for dest_dir in dest_dirs:
            # Remove a dir if exists. Ignore eventual errors
            shutil.rmtree(dest_dir, ignore_errors=True)
            # create the dir
            mkpath(dest_dir)
            # Ensure the right permissions to the destination directory
            # chmod 0700 octal
            os.chmod(dest_dir, 448)

    def _start_backup_copy_message(self, backup_info):
        output.info(
            "Starting backup copy via pg_basebackup for %s", backup_info.backup_id
        )


class CloudPostgresBackupExecutor(PostgresBackupExecutor):
    """
    Concrete class for backups via ``pg_basebackup`` with dynamic upload to cloud storage.

    In this method a backup is never fully stored locally. Instead, it is uploaded to
    the cloud storage as files are copied from the Postgres server by making use of
    a staging area on local disk.

    The complete process is as follows:

    1. ``pg_basebackup`` is started as a subprocess directing its output to a
        staging area (defined by :attr:`config.cloud_staging_directory`).
    2. A thread is started to monitor the staging area size so that it never goes
        over a limit (defined by :attr:`config.cloud_staging_max_size`). If that limit
        is ever reached, the backup subprocess is stopped and only resumed when enough
        space has been freed.
    3. A thread is started to fetch “ready files”. Such files are those which are
        already fully written and closed by ``pg_basebackup``. These files are then put
        in a “ready queue”.
    4. The main thread then starts to fetch ready files from the ready queue and
        appends them to tarballs (one for PGDATA and one for each tablespace) that are
        constantly being written. Each file is removed after appended to its tarball.
        Tarballs are written in chunks, called "part files". Part files are written to
        disk in a subdirectory of the staging area and live there until uploaded to the
        cloud on step 5.
    5. Multiple processes are started to upload part files to the cloud (the number of
        processes is defined by :attr:`config.parallel_jobs`). Each file is deleted
        after its upload is confirmed successful.

    .. note::
        The logic of steps 4 and 5 are not contained in this class, but in the
        :class:`barman.cloud.CloudUploadController` class utilized here.
    """

    # The below structure makes it easier to manage the staging area. Essentially:
    # 1. Tarball chunks are built reading from {staging_dir}/plain and
    #    written to {staging_dir}/tarballs
    # 2. Multipart upload is then performed reading chunks from {staging_dir}/tarballs
    _OUTPUT_PLAIN_DEST = "{staging_dir}/plain"
    _OUTPUT_PGDATA_DEST = "{staging_dir}/plain/data"
    _OUTPUT_TBLSPC_DEST = "{staging_dir}/plain/{oid}"
    _OUTPUT_TARBALL_DEST = "{staging_dir}/tarballs"

    # The files below have to be uploaded last, otherwise pg_basebackup might fail
    # at the end of its execution when trying to modify some of them. This was observed
    # during the initial development of this feature. There is also some hints of this
    # behavior in the pg_basebackup source code itself e.g.:
    # https://github.com/postgres/postgres/blob/REL_18_STABLE/src/bin/pg_basebackup/pg_basebackup.c#L2320
    # https://github.com/postgres/postgres/blob/REL_18_STABLE/src/bin/pg_basebackup/pg_basebackup.c#L2285
    _LAST_FILES = [
        r"^.*\.conf$",  # All .conf files
        r"^.*\.tmp$",  # All .tmp files (e.g. backup_manifest.tmp)
        r"^.*current_logfiles",
        r"^.*PG_VERSION",
    ]

    # Directory which contains file descriptors of a running process (Linux only)
    # This is used to in step 3 to keep track of files currently opened by pg_basebackup
    # i.e. those which are not ready yet to be uploaded to the cloud
    _FILE_DESCRIPTORS_DIRECTORY = "/proc/{pid}/fd"

    def __init__(self, backup_manager):
        """
        Constructor

        :param barman.backup.BackupManager backup_manager: the BackupManager
            assigned to the executor
        """
        super(CloudPostgresBackupExecutor, self).__init__(backup_manager)
        self.strategy = CloudPostgresBackupStrategy(
            self.server.postgres, self.config.name, self.backup_compression
        )
        self._cloud_staging_dir = os.path.join(
            self.config.cloud_staging_directory, str(os.getpid())
        )
        self._pgdata_dest = self._OUTPUT_PGDATA_DEST.format(
            staging_dir=self._cloud_staging_dir
        )
        self._tarball_dest = self._OUTPUT_TARBALL_DEST.format(
            staging_dir=self._cloud_staging_dir
        )
        self._plain_dest = self._OUTPUT_PLAIN_DEST.format(
            staging_dir=self._cloud_staging_dir
        )
        self._cloud_interface = self.server.get_backup_cloud_interface()
        self._upload_controller = None
        self._ready_queue = queue.Queue()

        self._thread_exc = None
        self._thread_exc_lock = threading.Lock()
        self._thread_exc_event = threading.Event()

    def _thread_wrapper(self, target, *args, **kwargs):
        """
        Wrapper for threads to catch exceptions and raise them in the main thread.

        :param callable target: the target function to be executed in the thread
        :param args: arguments for the target function
        :param kwargs: keyword arguments for the target function
        """
        try:
            target(*args, **kwargs)
        except Exception as exc:
            with self._thread_exc_lock:
                if self._thread_exc is None:
                    self._thread_exc = exc
            self._thread_exc_event.set()

    def backup_copy(self, backup_info):
        """
        Perform the actual copy of the backup using ``pg_basebackup``.

        For this executor, this means controlling the backup process, the building
        of the tarballs and their upload to the cloud storage.

        :param barman.infofile.LocalBackupInfo backup_info: backup information

        .. note::
            This method is reponsible for coordinating all the steps described in the
            class docstring.
        """
        self.copy_start_time = datetime.datetime.now()

        # Make sure the staging area directories exist and are writable
        self._prepare_backup_destination([self._plain_dest, self._tarball_dest])

        # Starts pg_basebackup directing its output to the staging area
        pg_basebackup = self._run_pg_basebackup(backup_info)

        # Start the monitoring thread to control the staging area size
        monitoring_thread = threading.Thread(
            target=self._thread_wrapper,
            args=(self._monitor_staging_area, pg_basebackup),
        )
        monitoring_thread.start()

        # Start the fetching thread to put ready files in the ready queue
        fetching_thread = threading.Thread(
            target=self._thread_wrapper,
            args=(self._fetch_ready_files, pg_basebackup),
        )
        fetching_thread.start()

        # Starts the actual upload process of files from the ready queue
        self._init_upload_controller(backup_info)
        with closing(self._cloud_interface), closing(self._upload_controller):
            self._upload_ready_files()
            try:
                # If any exception occurred, terminate pg_basebackup and reraise it
                if self._thread_exc_event.is_set():
                    pg_basebackup.terminate()
                    raise self._thread_exc
                # Otherwise wait for pg_basebackup to finish normally
                # It should be already finished at this point, as the upload is complete
                pg_basebackup.wait_exit()
            finally:
                # Regardless of failure or success, threads should be closed before main
                monitoring_thread.join()
                fetching_thread.join()

        # Store statistics about the upload time
        backup_info.copy_stats = self._upload_controller.statistics()
        # Store metadata from the backup label
        self._read_backup_label(backup_info)
        # Store the backup_manifest in the meta dir to allow future incremental backups
        self._save_backup_manifest(backup_info)
        # Once all metadata is collected, upload the backup.info file
        self._upload_backup_info(backup_info)
        # Cleanup the staging area
        shutil.rmtree(self._cloud_staging_dir, ignore_errors=True)
        # Override the actual total time
        self.copy_end_time = datetime.datetime.now()
        backup_info.copy_stats["total_time"] = total_seconds(
            self.copy_end_time - self.copy_start_time
        )
        backup_info.set_attribute("size", self._upload_controller.size)
        backup_info.set_attribute("deduplicated_size", self._upload_controller.size)

    def _init_upload_controller(self, backup_info):
        """
        Initialize the cloud upload controller.

        The cloud controller is an essential piece responsible for steps 4 and 5
        described in the class, namely building tarballs and uploading them to the
        cloud storage.

        :return barman.cloud.CloudUploadController: the upload controller
        """
        from barman.cloud import CloudUploadController

        # get_basebackup_directory returns the exact location in the bucket where
        # tarballs and backup.info will be stored e.g. if basebackups_directory is
        # s3://my-backups then its key prefix will be my-backups/<server_name>/base/<backup_id>
        # This follows the same structure used by the Barman cloud scripts
        key_prefix = backup_info.get_basebackup_directory()
        self._upload_controller = CloudUploadController(
            cloud_interface=self._cloud_interface,
            key_prefix=key_prefix,
            max_archive_size=self.config.cloud_upload_max_archive_size,
            compression=None,
            min_chunk_size=self.config.cloud_upload_min_chunk_size,
            max_bandwidth=_get_bandwidth_limit_in_bytes(self.config.bandwidth_limit),
            staging_dir=self._tarball_dest,
        )

    def _read_backup_label(self, backup_info):
        """
        Read and store metadata from the ``backup_label`` in the *backup_info* object.

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """
        label_path = os.path.join(self._pgdata_dest, "backup_label")
        with open(label_path, "r") as label_file:
            backup_info.set_attribute("backup_label", label_file.read())

    def _save_backup_manifest(self, backup_info):
        """
        Save the ``backup_manifest`` file in the server's meta directory.

        This is needed to allow future incremental backups based on this one.

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """
        filename = f"{backup_info.backup_id}-backup_manifest"
        manifest_dest_path = os.path.join(self.server.meta_directory, filename)
        manifest_src_path = os.path.join(self._pgdata_dest, "backup_manifest")
        shutil.copy2(manifest_src_path, manifest_dest_path)

    def _upload_backup_info(self, backup_info):
        """
        Upload the ``backup.info`` file to the cloud storage.

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """
        with BytesIO() as backup_info_fileobj:
            backup_info.save(file_object=backup_info_fileobj)
            backup_info_fileobj.seek(0)
            key = os.path.join(self._upload_controller.key_prefix, "backup.info")
            self._cloud_interface.upload_fileobj(backup_info_fileobj, key)

    def _run_pg_basebackup(self, backup_info):
        """
        Run ``pg_basebackup`` directing its output to the staging area.

        To be exact, the output is directed to ``{staging_dir}/plain/data`` for
        PGDATA and ``{staging_dir}/plain/{oid}`` for each tablespace.

        The execution is done asynchronously, meaning that this method returns
        immediately after successfully starting the backup subprocess. This allows
        threads such as :meth:`_monitor_staging_area` and :meth:`_fetch_ready_files`
        to have a reference to the running process as it executes.

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        :return barman.command_wrappers.PgBaseBackup: the running ``pg_basebackup``
            process

        .. note::
            This method performs step 1 described in the class docstring.
        """
        remote_status = self.get_remote_status()
        bandwidth_limit = self._get_bandwidth_limit(remote_status)
        tbspc_mapping = self._get_tablespace_mapping(backup_info)
        parent_backup_manifest = self._get_parent_backup_manifest_path(backup_info)
        pg_basebackup = PgBaseBackup(
            wait=False,
            connection=self.server.streaming,
            destination=self._pgdata_dest,
            command=remote_status["pg_basebackup_path"],
            version=remote_status["pg_basebackup_version"],
            app_name=self.config.streaming_backup_name,
            no_sync=True,
            tbs_mapping=tbspc_mapping,
            bwlimit=bandwidth_limit,
            immediate=self.config.immediate_checkpoint,
            path=self.server.path,
            parent_backup_manifest_path=parent_backup_manifest,
        )
        # Because we're using the wait=False here, this try-except block will only
        # catch basic errors such as command not found in PATH
        # Execution errors are caught in _wait_copy_start called below
        try:
            _logger.debug("Starting pg_basebackup to %s" % self._pgdata_dest)
            pg_basebackup()
        except CommandFailedException as e:
            msg = "data transfer failure on directory '%s'" % self._pgdata_dest
            raise DataTransferFailure.from_command_error("pg_basebackup", e, msg)

        # Wait for pg_basebackup to actually start copying. This ensures we catch
        # eventual init errors and fail early before spawning any other threads as
        # it gets harder to sync errors across them once they started
        self._wait_copy_start(pg_basebackup)

        return pg_basebackup

    def _get_tablespace_mapping(self, backup_info):
        """
        Get the tablespace mapping for ``pg_basebackup``.

        Each tablespace is mapped to "{staging_dir}/plain/{oid}".

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        :return dict[str,str]: mapping of source to destination tablespace locations
        """
        tbspc_mapping = {}
        tablespaces = backup_info.tablespaces or []
        for tbspc in tablespaces:
            source = tbspc.location
            destination = self._OUTPUT_TBLSPC_DEST.format(
                staging_dir=self._cloud_staging_dir,
                oid=tbspc.oid,
            )
            tbspc_mapping[source] = destination
        return tbspc_mapping

    def _get_parent_backup_manifest_path(self, backup_info):
        """
        Get the backup manifest path of the parent backup, if any.

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        :return str|None: the backup manifest path of the parent backup
        """
        parent_backup_info = backup_info.get_parent_backup_info()
        if parent_backup_info:
            filename = f"{parent_backup_info.backup_id}-backup_manifest"
            return os.path.join(self.server.meta_directory, filename)

    def _wait_copy_start(self, pg_basebackup):
        """
        Wait for ``pg_basebackup`` to successfully start copying files before returning.

        :param barman.command_wrappers.PgBaseBackup pg_basebackup: the running
            ``pg_basebackup`` process
        """
        # We can only reliably assert that pg_basebackup has started successfully when
        # we see files appearing in the staging area
        while not any(file for _, __, file in os.walk(self._plain_dest)):
            # Did not generate any files and already has an exit code? It sure failed.
            # Note: pg_basebackup is not expected to return '0' and write no files. With
            # that assumption, we do not care about checking if return code is '0' here.
            if pg_basebackup.get_returncode() is not None:
                raise DataTransferFailure(
                    "pg_basebackup process failed to start (return code %s): %s"
                    % (pg_basebackup.get_returncode(), pg_basebackup.get_stderr())
                )
            time.sleep(0.1)

    def _monitor_staging_area(self, pg_basebackup):
        """
        Monitor the staging area size, pausing/resuming the ``pg_basebackup`` process
        as needed to keep it under the configured limit in
        :attr:`config.cloud_staging_max_size`.

        This method is meant to run in a separate thread.

        :param barman.comamnd_wrappers.PgBaseBackup pg_basebackup: the running
            ``pg_basebackup`` process

        .. note::
            This method performs step 2 described in this class docstring.
        """
        is_paused = False
        while pg_basebackup.is_running():
            if self._thread_exc_event.is_set():
                _logger.debug(
                    "An error occurred in another thread, exiting monitoring thread"
                )
                return

            _logger.debug(
                "Monitoring the staging area '%s' size" % self._cloud_staging_dir
            )
            if not os.path.isdir(self._plain_dest):
                _logger.debug("No content in staging yet, sleeping for 1 second")
                time.sleep(1)
                continue

            staging_size = get_directory_size(self._cloud_staging_dir)
            if staging_size > self.config.cloud_staging_max_size and not is_paused:
                _logger.debug(
                    "Staging area size %s exceeds the max size %s, pausing pg_basebackup"
                    % (staging_size, self.config.cloud_staging_max_size)
                )
                pg_basebackup.pause()
                is_paused = True
            elif is_paused and staging_size < self.config.cloud_staging_max_size:
                _logger.debug(
                    "Staging area size %s is below the max size %s, resuming pg_basebackup"
                    % (staging_size, self.config.cloud_staging_max_size)
                )
                pg_basebackup.resume()
                is_paused = False

            # Check again every one second
            time.sleep(1)

    def _fetch_ready_files(self, pg_basebackup):
        """
        Grab ready files from the staging area and put them in the ready queue.

        Ready files are those which are fully written and closed by ``pg_basebackup``.
        Files present in :attr:`CloudPostgresBackupExecutor._LAST_FILES` are appended
        lastly, after the termination of ``pg_basebackup`` to guarantee its consistency.

        This method is meant to run in a separate thread.

        :param barman.command_wrappers.PgBaseBackup pg_basebackup: the running
            ``pg_basebackup`` process

        .. note::
            This method performs step 3 described in this class docstring.
        """
        _logger.debug("Starting to fetch ready files from %s" % self._plain_dest)
        processed = set()
        while True:
            if self._thread_exc_event.is_set():
                _logger.debug(
                    "An error occurred in another thread, exiting fetching thread"
                )
                return

            # Gather all files in the monitoring directory except: those still opened,
            # those already processed, those in LAST_FILES and those to be ignored
            all_files = self._get_files_from_dir(self._plain_dest)
            open_files = self._get_open_files(pg_basebackup)
            for filepath in all_files:
                if (
                    filepath not in processed
                    and filepath not in open_files
                    and not self._is_last_file(filepath)
                    and not self._ignore_file(filepath)
                ):
                    self._ready_queue.put(filepath)
                    processed.add(filepath)

            # When pg_basebackup ends, fetch all remaining files i.e. those present in
            # LAST_FILES and those that were opened during the last iteration
            # (they should be readily closed now)
            if not pg_basebackup.is_running():
                _logger.debug("pg_basebackup process ended, fetching remaining files")
                for filepath in self._get_files_from_dir(self._plain_dest):
                    if filepath not in processed and not self._ignore_file(filepath):
                        self._ready_queue.put(filepath)
                        processed.add(filepath)
                break

            _logger.debug(
                "Fetched round of ready files, current queue size is %s"
                % self._ready_queue.qsize()
            )
            time.sleep(1)

        _logger.debug("No more files to fetch, exiting ready files fetcher thread")
        self._ready_queue.put(None)  # Sentinel to signal no more files will be added

    def _get_files_from_dir(self, directory):
        """
        Get a list of all files in a given *directory* (recursively).

        :param str directory: the directory to scan
        :return generator[str]: generator of file paths
        """
        for dirpath, _, filenames in os.walk(directory):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                yield filepath

    def _get_open_files(self, pg_basebackup):
        """
        Get the list of files currently opened by the given process.

        :param barman.command_wrappers.PgBaseBackup pg_basebackup: the running
            ``pg_basebackup`` process
        :return list[str]: list of file paths
        """
        if not pg_basebackup.is_running():
            return []

        fd_dir = self._FILE_DESCRIPTORS_DIRECTORY.format(pid=pg_basebackup.pid)
        open_files = []
        try:
            for fd in os.listdir(fd_dir):
                try:
                    target = os.readlink(os.path.join(fd_dir, fd))
                    if target.startswith(self._plain_dest):
                        open_files.append(target)
                except OSError as ex:
                    # FD may have been closed between listdir and readlink
                    _logger.debug(
                        "Could not read file descriptor %s for pg_basebackup process: %s"
                        % (fd, ex)
                    )
                    continue
        except OSError as ex:
            # Process may have exited
            _logger.debug(
                "Could not access file descriptors for pg_basebackup process: %s" % ex
            )
            return []

        return open_files

    def _ignore_file(self, file):
        """
        Determine whether a file should be ignored.

        :param str file: path of the file or directory
        :return bool: ``True`` if the file should be ignored, ``False`` otherwise

        .. note::
            These are the same rules followed in :mod:`barman.cloud`.
        """
        ignore_list = EXCLUDE_LIST + PGDATA_EXCLUDE_LIST
        rel_src_path = os.path.relpath(file, self._pgdata_dest)
        if not path_allowed(ignore_list, None, rel_src_path, False):
            return True
        return False

    def _is_last_file(self, filepath):
        """
        Determine whether a file matches any pattern in
        :attr:`CloudPostgresBackupExecutor._LAST_FILES`.

        :param str filepath: path of the file
        :return bool: ``True`` if positive, ``False`` otherwise
        """
        filename = os.path.basename(filepath)
        for pattern in self._LAST_FILES:
            if re.match(pattern, filename):
                return True
        return False

    def _upload_ready_files(self):
        """
        Upload files from the ready queue to the cloud storage.

        Files are appended to tarballs (one for PGDATA and one for each tablespace).
        Each file is removed after appended to its tarball.

        .. note::
            The core logic of building tarballs and uploading them to the cloud storage
            is handled by the :attr:`_upload_controller` object used here, with its
            ``add_file`` and ``upload_directory`` methods.

            This method performs steps 4 and 5 described in this class docstring.
        """
        while True:
            if self._thread_exc_event.is_set():
                _logger.debug(
                    "An error occurred in a thread, stopping upload of ready files"
                )
                return

            try:
                filepath = self._ready_queue.get_nowait()
            except queue.Empty:
                _logger.debug("No ready files in the queue, sleeping for 1 second")
                time.sleep(1)
                continue

            if filepath is None:  # Sentinel found, no more files will come
                break

            _logger.debug("Uploading file %s" % filepath)
            # The file name
            filename = os.path.basename(filepath)
            # The tarball it belongs to
            tarball_name = self._get_tarball_name(filepath)
            # Its path inside the tarball i.e. its path when unarchived
            # E.g. plain/data/base/1/16384 -> base/1/16384; plain/<oid>/1/16384 -> 1/16384
            rel_path = os.path.relpath(filepath, self._plain_dest)
            path_inside_tarball = rel_path.split(os.sep, 1)[1]
            self._upload_controller.add_file(
                label=filename,
                src=filepath,
                dst=tarball_name,
                path=path_inside_tarball,
            )
            # When add_file returns the file has already been appended, so delete it
            # unless it's the backup_label or backup_manifest (both are needed later)
            if filename not in ("backup_label", "backup_manifest"):
                os.unlink(filepath)

        # Once all files in the queue have been appended then upload the remaining
        # content inside each subdirectory (PGDATA and tablespaces)
        # As most files were already uploadd and deleted, these will be mostly
        # empty directories (which are relevant when starting the cluster)
        sub_dirs = [item for item in os.listdir(self._plain_dest)]
        for dir in sub_dirs:
            full_path = os.path.join(self._plain_dest, dir)
            self._upload_controller.upload_directory(
                label=dir,
                src=full_path,
                dst=self._get_tarball_name(full_path),
                exclude=EXCLUDE_LIST + PGDATA_EXCLUDE_LIST,
            )

        # At last copy pg_control. As it contains info such as the latest checkpoint,
        # it must be copied after all other files to guarantee consistency
        self._upload_controller.add_file(
            label="pg_control",
            src="%s/global/pg_control" % self._pgdata_dest,
            dst="data",
            path="global/pg_control",
        )

    def _get_tarball_name(self, filepath):
        """
        Get the tarball name a given file belongs to.

        For PGDATA files, the tarball name is always "data". For tablespace files,
        the tarball name is the OID of the tablespace.

        .. note::
            It's sure that all tablespace files are under a dedicated directory with
            its OID as name because that's how :meth:`_run_pg_basebackup` mapped them.

        :param str filepath: path of the file
        :return str: the tarball destination name
        """
        if filepath.startswith(self._pgdata_dest):
            return "data"
        rel_path = os.path.relpath(filepath, self._plain_dest)
        oid = rel_path.split(os.sep)[0]
        return oid

    def _start_backup_copy_message(self, backup_info):
        output.info(
            "Starting backup copy via pg_basebackup for %s to the cloud storage %s: %s",
            backup_info.backup_id,
            self.server.backup_cloud_provider,
            self.config.basebackups_directory,
        )


class ExternalBackupExecutor(with_metaclass(ABCMeta, BackupExecutor)):
    """
    Abstract base class for non-postgres backup executors.

    An external backup executor is any backup executor which uses the
    PostgreSQL low-level backup API to coordinate the backup.

    Such executors can operate remotely via SSH or locally:

    - remote mode (default), operates via SSH
    - local mode, operates as the same user that Barman runs with

    It is also a factory for exclusive/concurrent backup strategy objects.

    Raises a SshCommandException if 'ssh_command' is not set and
    not operating in local mode.
    """

    def __init__(self, backup_manager, mode, local_mode=False):
        """
        Constructor of the abstract class for backups via Ssh

        :param barman.backup.BackupManager backup_manager: the BackupManager
            assigned to the executor
        :param str mode: The mode used by the executor for the backup.
        :param bool local_mode: if set to False (default), the class is able
            to operate on remote servers using SSH. Operates only locally
            if set to True.
        """
        super(ExternalBackupExecutor, self).__init__(backup_manager, mode)

        # Set local/remote mode for copy
        self.local_mode = local_mode

        # Retrieve the ssh command and the options necessary for the
        # remote ssh access.
        self.ssh_command, self.ssh_options = _parse_ssh_command(
            backup_manager.config.ssh_command
        )

        if not self.local_mode:
            # Remote copy requires ssh_command to be set
            if not self.ssh_command:
                raise SshCommandException(
                    "Missing or invalid ssh_command in barman configuration "
                    "for server %s" % backup_manager.config.name
                )
        else:
            # Local copy requires ssh_command not to be set
            if self.ssh_command:
                raise SshCommandException(
                    "Local copy requires ssh_command in barman configuration "
                    "to be empty for server %s" % backup_manager.config.name
                )

        # Apply the default backup strategy
        backup_options = self.config.backup_options
        concurrent_backup = BackupOptions.CONCURRENT_BACKUP in backup_options
        exclusive_backup = BackupOptions.EXCLUSIVE_BACKUP in backup_options
        if not concurrent_backup and not exclusive_backup:
            self.config.backup_options.add(BackupOptions.CONCURRENT_BACKUP)
            output.warning(
                "No backup strategy set for server '%s' "
                "(using default 'concurrent_backup').",
                self.config.name,
            )

        # Depending on the backup options value, create the proper strategy
        if BackupOptions.CONCURRENT_BACKUP in self.config.backup_options:
            # Concurrent backup strategy
            self.strategy = LocalConcurrentBackupStrategy(
                self.server.postgres, self.config.name
            )
        else:
            # Exclusive backup strategy
            self.strategy = ExclusiveBackupStrategy(
                self.server.postgres, self.config.name
            )

    def _update_action_from_strategy(self):
        """
        Update the executor's current action with the one of the strategy.
        This is used during exception handling to let the caller know
        where the failure occurred.
        """

        action = getattr(self.strategy, "current_action", None)
        if action:
            self.current_action = action

    @abstractmethod
    def backup_copy(self, backup_info):
        """
        Performs the actual copy of a backup for the server

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """

    def backup(self, backup_info):
        """
        Perform a backup for the server - invoked by BackupManager.backup()
        through the generic interface of a BackupExecutor. This implementation
        is responsible for performing a backup through a remote connection
        to the PostgreSQL server via Ssh. The specific set of instructions
        depends on both the specific class that derives from ExternalBackupExecutor
        and the selected strategy (e.g. exclusive backup through Rsync).

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """

        # Start the backup, all the subsequent code must be wrapped in a
        # try except block which finally issues a stop_backup command
        try:
            self.strategy.start_backup(backup_info)
        except BaseException:
            self._update_action_from_strategy()
            raise

        connection_error = False
        try:
            # save any metadata changed by start_backup() call
            # This must be inside the try-except, because it could fail
            backup_info.save()

            if backup_info.begin_wal is not None:
                output.info(
                    "Backup start at LSN: %s (%s, %08X)",
                    backup_info.begin_xlog,
                    backup_info.begin_wal,
                    backup_info.begin_offset,
                )
            else:
                output.info("Backup start at LSN: %s", backup_info.begin_xlog)

            # If this is the first backup, purge eventually unused WAL files
            self._purge_unused_wal_files(backup_info)

            # Start the copy
            self.current_action = "copying files"
            self._start_backup_copy_message(backup_info)
            self.backup_copy(backup_info)
            self._stop_backup_copy_message(backup_info)

            # Try again to purge eventually unused WAL files. At this point
            # the begin_wal value is surely known. Doing it twice is safe
            # because this function is useful only during the first backup.
            self._purge_unused_wal_files(backup_info)

        except PostgresConnectionLost:
            # This exception is most likely to be raised by the PostgresKeepAlive,
            # meaning that we lost the connection (and session) during the backup.
            connection_error = True
            raise
        except BaseException as ex:
            # we do not need to do anything here besides re-raising the
            # exception. It will be handled in the external try block.
            output.error("The backup has failed %s", self.current_action)
            # As we have found that in certain corner cases the exception
            # passing through this block is not logged or even hidden by
            # other exceptions happening in the finally block, we are adding a
            # debug log line to make sure that the exception is visible.
            _logger.debug("Backup failed: %s" % ex, exc_info=True)
            raise
        else:
            self.current_action = "issuing stop of the backup"
        finally:
            # If a connection error has been raised, it means we lost our session in the
            # Postgres server. In such cases, it's useless to try a backup stop command.
            if not connection_error:
                output.info("Asking PostgreSQL server to finalize the backup.")
                try:
                    self.strategy.stop_backup(backup_info)
                except BaseException:
                    self._update_action_from_strategy()
                    raise

    def _local_check(self, check_strategy):
        """
        Specific checks for local mode of ExternalBackupExecutor (same user)

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        cmd = UnixLocalCommand(path=self.server.path)
        pgdata = self.server.postgres.get_setting("data_directory")

        # Check that PGDATA is accessible
        check_strategy.init_check("local PGDATA")
        hint = "Access to local PGDATA"
        try:
            cmd.check_directory_exists(pgdata)
        except FsOperationFailed as e:
            hint = force_str(e).strip()

        # Output the result
        check_strategy.result(self.config.name, cmd is not None, hint=hint)

    def _remote_check(self, check_strategy):
        """
        Specific checks for remote mode of ExternalBackupExecutor, via SSH.

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """

        # Check the SSH connection
        check_strategy.init_check("ssh")
        hint = "PostgreSQL server"
        cmd = None
        minimal_ssh_output = None
        try:
            cmd = UnixRemoteCommand(
                self.ssh_command, self.ssh_options, path=self.server.path
            )
            minimal_ssh_output = "".join(cmd.get_last_output())
        except FsOperationFailed as e:
            hint = force_str(e).strip()

        # Output the result
        check_strategy.result(self.config.name, cmd is not None, hint=hint)

        # Check that the communication channel is "clean"
        if minimal_ssh_output:
            check_strategy.init_check("ssh output clean")
            check_strategy.result(
                self.config.name,
                False,
                hint="the configured ssh_command must not add anything to "
                "the remote command output",
            )

        # If SSH works but PostgreSQL is not responding
        server_txt_version = self.server.get_remote_status().get("server_txt_version")
        if cmd is not None and server_txt_version is None:
            # Check for 'backup_label' presence
            last_backup = self.server.get_backup(
                self.server.get_last_backup_id(BackupInfo.STATUS_NOT_EMPTY)
            )
            # Look for the latest backup in the catalogue
            if last_backup:
                check_strategy.init_check("backup_label")
                # Get PGDATA and build path to 'backup_label'
                backup_label = os.path.join(last_backup.pgdata, "backup_label")
                # Verify that backup_label exists in the remote PGDATA.
                # If so, send an alert. Do not show anything if OK.
                exists = cmd.exists(backup_label)
                if exists:
                    hint = (
                        "Check that the PostgreSQL server is up "
                        "and no 'backup_label' file is in PGDATA."
                    )
                    check_strategy.result(self.config.name, False, hint=hint)

    def check(self, check_strategy):
        """
        Perform additional checks for ExternalBackupExecutor, including
        Ssh connection (executing a 'true' command on the remote server)
        and specific checks for the given backup strategy.

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """

        if self.local_mode:
            # Perform checks for the local case
            self._local_check(check_strategy)
        else:
            # Perform checks for the remote case
            self._remote_check(check_strategy)

        try:
            # Invoke specific checks for the backup strategy
            self.strategy.check(check_strategy)
        except BaseException:
            self._update_action_from_strategy()
            raise

    def status(self):
        """
        Set additional status info for ExternalBackupExecutor using remote
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
            if self.server.postgres and self.server.postgres.server_version < 90400:
                remote_status["last_archived_wal"] = None
                if self.server.postgres.get_setting(
                    "data_directory"
                ) and self.server.postgres.get_setting("archive_command"):
                    if not self.local_mode:
                        cmd = UnixRemoteCommand(
                            self.ssh_command, self.ssh_options, path=self.server.path
                        )
                    else:
                        cmd = UnixLocalCommand(path=self.server.path)
                    # Here the name of the PostgreSQL WALs directory is
                    # hardcoded, but that doesn't represent a problem as
                    # this code runs only for PostgreSQL < 9.4
                    archive_dir = os.path.join(
                        self.server.postgres.get_setting("data_directory"),
                        "pg_xlog",
                        "archive_status",
                    )
                    out = str(cmd.list_dir_content(archive_dir, ["-t"]))
                    for line in out.splitlines():
                        if line.endswith(".done"):
                            name = line[:-5]
                            if xlog.is_any_xlog_file(name):
                                remote_status["last_archived_wal"] = name
                                break
        except (PostgresConnectionError, FsOperationFailed) as e:
            _logger.warning("Error retrieving PostgreSQL status: %s", e)
        return remote_status


class PassiveBackupExecutor(BackupExecutor):
    """
    Dummy backup executors for Passive servers.

    Raises a SshCommandException if 'primary_ssh_command' is not set.
    """

    def __init__(self, backup_manager):
        """
        Constructor of Dummy backup executors for Passive servers.

        :param barman.backup.BackupManager backup_manager: the BackupManager
            assigned to the executor
        """
        super(PassiveBackupExecutor, self).__init__(backup_manager)

        # Retrieve the ssh command and the options necessary for the
        # remote ssh access.
        self.ssh_command, self.ssh_options = _parse_ssh_command(
            backup_manager.config.primary_ssh_command
        )

        # Requires ssh_command to be set
        if not self.ssh_command:
            raise SshCommandException(
                "Invalid primary_ssh_command in barman configuration "
                "for server %s" % backup_manager.config.name
            )

    def backup(self, backup_info):
        """
        This method should never be called, because this is a passive server

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """
        # The 'backup' command is not available on a passive node.
        # If we get here, there is a programming error
        assert False

    def check(self, check_strategy):
        """
        Perform additional checks for PassiveBackupExecutor, including
        Ssh connection to the primary (executing a 'true' command on the
        remote server).

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        check_strategy.init_check("ssh")
        hint = "Barman primary node"
        cmd = None
        minimal_ssh_output = None
        try:
            cmd = UnixRemoteCommand(
                self.ssh_command, self.ssh_options, path=self.server.path
            )
            minimal_ssh_output = "".join(cmd.get_last_output())
        except FsOperationFailed as e:
            hint = force_str(e).strip()

        # Output the result
        check_strategy.result(self.config.name, cmd is not None, hint=hint)

        # Check if the communication channel is "clean"
        if minimal_ssh_output:
            check_strategy.init_check("ssh output clean")
            check_strategy.result(
                self.config.name,
                False,
                hint="the configured ssh_command must not add anything to "
                "the remote command output",
            )

    def status(self):
        """
        Set additional status info for PassiveBackupExecutor.
        """
        # On passive nodes show the primary_ssh_command
        output.result(
            "status",
            self.config.name,
            "primary_ssh_command",
            "SSH command to primary server",
            self.config.primary_ssh_command,
        )

    @property
    def mode(self):
        """
        Property that defines the mode used for the backup.
        :return str: a string describing the mode used for the backup
        """
        return "passive"


class RsyncBackupExecutor(ExternalBackupExecutor):
    """
    Concrete class for backup via Rsync+Ssh.

    It invokes PostgreSQL commands to start and stop the backup, depending
    on the defined strategy. Data files are copied using Rsync via Ssh.
    It heavily relies on methods defined in the ExternalBackupExecutor class
    from which it derives.
    """

    def __init__(self, backup_manager, local_mode=False):
        """
        Constructor

        :param barman.backup.BackupManager backup_manager: the BackupManager
            assigned to the strategy
        """
        super(RsyncBackupExecutor, self).__init__(backup_manager, "rsync", local_mode)
        self.validate_configuration()

    def validate_configuration(self):
        # Verify that backup_compression is not set
        if self.server.config.backup_compression:
            self.server.config.update_msg_list_and_disable_server(
                "backup_compression option is not supported by rsync backup_method"
            )

    def backup(self, *args, **kwargs):
        """
        Perform an Rsync backup.

        .. note::
            This method currently only calls the parent backup method but inside a keepalive
            context to ensure the connection does not become idle long enough to get dropped
            by a firewall, for instance. This is important to ensure that ``pg_backup_start()``
            and ``pg_backup_stop()`` are called within the same session.
        """
        try:
            with PostgresKeepAlive(
                self.server.postgres, self.config.keepalive_interval, True
            ):
                super(RsyncBackupExecutor, self).backup(*args, **kwargs)
        except PostgresConnectionLost:
            raise BackupException(
                "Connection to the Postgres server was lost during the backup."
            )

    def backup_copy(self, backup_info):
        """
        Perform the actual copy of the backup using Rsync.

        First, it copies one tablespace at a time, then the PGDATA directory,
        and finally configuration files (if outside PGDATA).
        Bandwidth limitation, according to configuration, is applied in
        the process.
        This method is the core of base backup copy using Rsync+Ssh.

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """

        # Retrieve the previous backup metadata, then calculate safe_horizon
        previous_backup = self.backup_manager.get_previous_backup(backup_info.backup_id)
        safe_horizon = None
        reuse_backup = None

        # Store the start time
        self.copy_start_time = datetime.datetime.now()

        if previous_backup:
            # safe_horizon is a tz-aware timestamp because BackupInfo class
            # ensures that property
            reuse_backup = self.config.reuse_backup
            safe_horizon = previous_backup.begin_time

        # Create the copy controller object, specific for rsync,
        # which will drive all the copy operations. Items to be
        # copied are added before executing the copy() method
        controller = RsyncCopyController(
            path=self.server.path,
            ssh_command=self.ssh_command,
            ssh_options=self.ssh_options,
            network_compression=self.config.network_compression,
            reuse_backup=reuse_backup,
            safe_horizon=safe_horizon,
            retry_times=self.config.basebackup_retry_times,
            retry_sleep=self.config.basebackup_retry_sleep,
            workers=self.config.parallel_jobs,
            workers_start_batch_period=self.config.parallel_jobs_start_batch_period,
            workers_start_batch_size=self.config.parallel_jobs_start_batch_size,
        )

        # List of paths to be excluded by the PGDATA copy
        exclude_and_protect = []

        # Process every tablespace
        if backup_info.tablespaces:
            for tablespace in backup_info.tablespaces:
                # If the tablespace location is inside the data directory,
                # exclude and protect it from being copied twice during
                # the data directory copy
                if tablespace.location.startswith(backup_info.pgdata + "/"):
                    exclude_and_protect += [
                        tablespace.location[len(backup_info.pgdata) :]
                    ]

                # Exclude and protect the tablespace from being copied again
                # during the data directory copy
                exclude_and_protect += ["/pg_tblspc/%s" % tablespace.oid]

                # Make sure the destination directory exists in order for
                # smart copy to detect that no file is present there
                tablespace_dest = backup_info.get_data_directory(tablespace.oid)
                mkpath(tablespace_dest)

                # Add the tablespace directory to the list of objects
                # to be copied by the controller.
                # NOTE: Barman should archive only the content of directory
                #    "PG_" + PG_MAJORVERSION + "_" + CATALOG_VERSION_NO
                # but CATALOG_VERSION_NO is not easy to retrieve, so we copy
                #    "PG_" + PG_MAJORVERSION + "_*"
                # It could select some spurious directory if a development or
                # a beta version have been used, but it's good enough for a
                # production system as it filters out other major versions.
                controller.add_directory(
                    label=tablespace.name,
                    src="%s/" % self._format_src(tablespace.location),
                    dst=tablespace_dest,
                    exclude=["/*"] + EXCLUDE_LIST,
                    include=["/PG_%s_*" % self.server.postgres.server_major_version],
                    bwlimit=self.config.get_bwlimit(tablespace),
                    reuse=self._reuse_path(previous_backup, tablespace),
                    item_class=controller.TABLESPACE_CLASS,
                )

        # Make sure the destination directory exists in order for smart copy
        # to detect that no file is present there
        backup_dest = backup_info.get_data_directory()
        mkpath(backup_dest)

        # Add the PGDATA directory to the list of objects to be copied
        # by the controller
        controller.add_directory(
            label="pgdata",
            src="%s/" % self._format_src(backup_info.pgdata),
            dst=backup_dest,
            exclude=PGDATA_EXCLUDE_LIST + EXCLUDE_LIST,
            exclude_and_protect=exclude_and_protect,
            bwlimit=self.config.get_bwlimit(),
            reuse=self._reuse_path(previous_backup),
            item_class=controller.PGDATA_CLASS,
        )

        # At last copy pg_control
        controller.add_file(
            label="pg_control",
            src="%s/global/pg_control" % self._format_src(backup_info.pgdata),
            dst="%s/global/pg_control" % (backup_dest,),
            item_class=controller.PGCONTROL_CLASS,
        )

        # Copy configuration files (if not inside PGDATA)
        external_config_files = backup_info.get_external_config_files()
        included_config_files = []
        for config_file in external_config_files:
            # Add included files to a list, they will be handled later
            if config_file.file_type == "include":
                included_config_files.append(config_file)
                continue

            # If the ident file is missing, it isn't an error condition
            # for PostgreSQL.
            # Barman is consistent with this behavior.
            optional = False
            if config_file.file_type == "ident_file":
                optional = True

            # Create the actual copy jobs in the controller
            controller.add_file(
                label=config_file.file_type,
                src=self._format_src(config_file.path),
                dst=backup_dest,
                optional=optional,
                item_class=controller.CONFIG_CLASS,
            )

        # Execute the copy
        try:
            controller.copy()
        # TODO: Improve the exception output
        except CommandFailedException as e:
            msg = "data transfer failure"
            raise DataTransferFailure.from_command_error("rsync", e, msg)

        # Store the end time
        self.copy_end_time = datetime.datetime.now()

        # Store statistics about the copy
        backup_info.copy_stats = controller.statistics()

        # Check for any include directives in PostgreSQL configuration
        # Currently, include directives are not supported for files that
        # reside outside PGDATA. These files must be manually backed up.
        # Barman will emit a warning and list those files
        if any(included_config_files):
            msg = (
                "The usage of include directives is not supported "
                "for files that reside outside PGDATA.\n"
                "Please manually backup the following files:\n"
                "\t%s\n" % "\n\t".join(icf.path for icf in included_config_files)
            )
            # Show the warning only if the EXTERNAL_CONFIGURATION option
            # is not specified in the backup_options.
            if BackupOptions.EXTERNAL_CONFIGURATION not in self.config.backup_options:
                output.warning(msg)
            else:
                _logger.debug(msg)

    def _reuse_path(self, previous_backup_info, tablespace=None):
        """
        If reuse_backup is 'copy' or 'link', builds the path of the directory
        to reuse, otherwise always returns None.

        If oid is None, it returns the full path of PGDATA directory of
        the previous_backup otherwise it returns the path to the specified
        tablespace using it's oid.

        :param barman.infofile.LocalBackupInfo previous_backup_info: backup
            to be reused
        :param barman.infofile.Tablespace tablespace: the tablespace to copy
        :returns: a string containing the local path with data to be reused
            or None
        :rtype: str|None
        """
        oid = None
        if tablespace:
            oid = tablespace.oid
        if (
            self.config.reuse_backup in ("copy", "link")
            and previous_backup_info is not None
        ):
            try:
                return previous_backup_info.get_data_directory(oid)
            except ValueError:
                return None

    def _format_src(self, path):
        """
        If the executor is operating in remote mode,
        add a `:` in front of the path for rsync to work via SSH.

        :param string path: the path to format
        :return str: the formatted path string
        """
        if not self.local_mode:
            return ":%s" % path
        return path

    def _start_backup_copy_message(self, backup_info):
        """
        Output message for backup start.

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """
        number_of_workers = self.config.parallel_jobs
        via = "rsync/SSH"
        if self.local_mode:
            via = "local rsync"
        message = "Starting backup copy via %s for %s" % (
            via,
            backup_info.backup_id,
        )
        if number_of_workers > 1:
            message += " (%s jobs)" % number_of_workers
        output.info(message)


class SnapshotBackupExecutor(ExternalBackupExecutor):
    """
    Concrete class which uses cloud provider disk snapshots to create backups.

    It invokes PostgreSQL commands to start and stop the backup, depending
    on the defined strategy.

    It heavily relies on methods defined in the ExternalBackupExecutor class
    from which it derives.

    No data files are copied and instead snapshots are created of the requested disks
    using the cloud provider API (abstracted through a CloudSnapshotInterface).

    As well as ensuring the backup happens via snapshot copy, this class also:

      - Checks that the specified disks are attached to the named instance.
      - Checks that the specified disks are mounted on the named instance.
      - Records the mount points and options of each disk in the backup info.

    Barman will still store the following files in its backup directory:

      - The backup_label (for concurrent backups) which is written by the
        LocalConcurrentBackupStrategy.
      - The backup.info which is written by the BackupManager responsible for
        instantiating this class.
    """

    def __init__(self, backup_manager):
        """
        Constructor for the SnapshotBackupExecutor

        :param barman.backup.BackupManager backup_manager: the BackupManager
            assigned to the strategy
        """
        super(SnapshotBackupExecutor, self).__init__(backup_manager, "snapshot")
        self.snapshot_instance = self.config.snapshot_instance
        self.snapshot_disks = self.config.snapshot_disks
        self.validate_configuration()
        try:
            self.snapshot_interface = get_snapshot_interface_from_server_config(
                self.config
            )
        except Exception as exc:
            self.server.config.update_msg_list_and_disable_server(
                "Error initialising snapshot provider %s: %s"
                % (self.config.snapshot_provider, exc)
            )

    def validate_configuration(self):
        """Verify configuration is valid for a snapshot backup."""
        excluded_config = (
            "backup_compression",
            "bandwidth_limit",
            "network_compression",
            "tablespace_bandwidth_limit",
        )
        for config_var in excluded_config:
            if getattr(self.server.config, config_var):
                self.server.config.update_msg_list_and_disable_server(
                    "%s option is not supported by snapshot backup_method" % config_var
                )

        if self.config.reuse_backup in ("copy", "link"):
            self.server.config.update_msg_list_and_disable_server(
                "reuse_backup option is not supported by snapshot backup_method"
            )

        required_config = (
            "snapshot_disks",
            "snapshot_instance",
            "snapshot_provider",
        )
        for config_var in required_config:
            if not getattr(self.server.config, config_var):
                self.server.config.update_msg_list_and_disable_server(
                    "%s option is required by snapshot backup_method" % config_var
                )

        # Check if aws_snapshot_lock_mode is set with snapshot_provider = aws.
        if (
            getattr(self.server.config, "aws_snapshot_lock_mode")
            and getattr(self.server.config, "snapshot_provider") == "aws"
        ):
            self._validate_aws_lock_configuration()

    def _validate_aws_lock_configuration(self):
        """Verify configuration is valid for locking a snapshot backup using AWS
        provider.
        """
        if not (
            getattr(self.server.config, "aws_snapshot_lock_duration")
            or getattr(self.server.config, "aws_snapshot_lock_expiration_date")
        ):
            self.server.config.update_msg_list_and_disable_server(
                "'aws_snapshot_lock_mode' is set, you must specify "
                "'aws_snapshot_lock_duration' or 'aws_snapshot_lock_expiration_date'."
            )

        if getattr(self.server.config, "aws_snapshot_lock_duration") and getattr(
            self.server.config, "aws_snapshot_lock_expiration_date"
        ):
            self.server.config.update_msg_list_and_disable_server(
                "You must specify either 'aws_snapshot_lock_duration' or "
                "'aws_snapshot_lock_expiration_date' in the configuration, but not both."
            )

        if getattr(
            self.server.config, "aws_snapshot_lock_mode"
        ) == "governance" and getattr(
            self.server.config, "aws_snapshot_lock_cool_off_period"
        ):
            self.server.config.update_msg_list_and_disable_server(
                "'aws_snapshot_lock_cool_off_period' cannot be used with "
                "'aws_snapshot_lock_mode' = 'governance'."
            )

        lock_args = {
            "aws_snapshot_lock_cool_off_period": check_aws_snapshot_lock_cool_off_period_range,
            "aws_snapshot_lock_duration": check_aws_snapshot_lock_duration_range,
            "aws_snapshot_lock_mode": check_aws_snapshot_lock_mode,
            "aws_snapshot_lock_expiration_date": check_aws_expiration_date_format,
        }
        for arg, parser in lock_args.items():
            lock_arg = getattr(self.server.config, arg)
            if lock_arg:
                try:
                    _ = parser(lock_arg)
                except Exception as e:
                    error_message = str(e)
                    self.server.config.update_msg_list_and_disable_server(error_message)

    @staticmethod
    def add_mount_data_to_volume_metadata(volumes, remote_cmd):
        """
        Adds the mount point and mount options for each supplied volume.

        Calls `resolve_mounted_volume` on each supplied volume so that the volume
        metadata (which originated from the cloud provider) can be resolved to the
        mount point and mount options of the volume as mounted on a compute instance.

        This will set the current mount point and mount options of the volume so that
        they can be stored in the snapshot metadata for the backup when the backup is
        taken.

        :param dict[str,barman.cloud.VolumeMetadata] volumes: Metadata for the volumes
            attached to a specific compute instance.
        :param UnixLocalCommand remote_cmd: Wrapper for executing local/remote commands
            on the compute instance to which the volumes are attached.
        """
        for volume in volumes.values():
            volume.resolve_mounted_volume(remote_cmd)

    def backup_copy(self, backup_info):
        """
        Perform the backup using cloud provider disk snapshots.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information.
        """
        # Create data dir so backup_label can be written
        cmd = UnixLocalCommand(path=self.server.path)
        cmd.create_dir_if_not_exists(backup_info.get_data_directory())

        # Start the snapshot
        self.copy_start_time = datetime.datetime.now()

        # Get volume metadata for the disks to be backed up
        volumes_to_snapshot = self.snapshot_interface.get_attached_volumes(
            self.snapshot_instance, self.snapshot_disks
        )

        # Resolve volume metadata to mount metadata using shell commands on the
        # compute instance to which the volumes are attached - this information
        # can then be added to the metadata for each snapshot when the backup is
        # taken.
        remote_cmd = UnixRemoteCommand(ssh_command=self.server.config.ssh_command)
        self.add_mount_data_to_volume_metadata(volumes_to_snapshot, remote_cmd)

        self.snapshot_interface.take_snapshot_backup(
            backup_info,
            self.snapshot_instance,
            volumes_to_snapshot,
        )

        self.copy_end_time = datetime.datetime.now()

        # Store statistics about the copy
        copy_time = total_seconds(self.copy_end_time - self.copy_start_time)
        backup_info.copy_stats = {
            "copy_time": copy_time,
            "total_time": copy_time,
        }

    @staticmethod
    def find_missing_and_unmounted_disks(
        cmd, snapshot_interface, snapshot_instance, snapshot_disks
    ):
        """
        Checks for any disks listed in snapshot_disks which are not correctly attached
        and mounted on the named instance and returns them as a tuple of two lists.

        This is used for checking that the disks which are to be used as sources for
        snapshots at backup time are attached and mounted on the instance to be backed
        up.

        :param UnixLocalCommand cmd: Wrapper for local/remote commands.
        :param barman.cloud.CloudSnapshotInterface snapshot_interface: Interface for
            taking snapshots and associated operations via cloud provider APIs.
        :param str snapshot_instance: The name of the VM instance to which the disks
            to be backed up are attached.
        :param list[str] snapshot_disks: A list containing the names of the disks for
            which snapshots should be taken at backup time.
        :rtype: tuple[list[str],list[str]]
        :return: A tuple where the first element is a list of all disks which are not
            attached to the VM instance and the second element is a list of all disks
            which are attached but not mounted.
        """
        attached_volumes = snapshot_interface.get_attached_volumes(
            snapshot_instance, snapshot_disks, fail_on_missing=False
        )
        missing_disks = []
        for disk in snapshot_disks:
            if disk not in attached_volumes.keys():
                missing_disks.append(disk)

        unmounted_disks = []
        for disk in snapshot_disks:
            try:
                attached_volumes[disk].resolve_mounted_volume(cmd)
                mount_point = attached_volumes[disk].mount_point
            except KeyError:
                # Ignore disks which were not attached
                continue
            except SnapshotBackupException as exc:
                _logger.warning("Error resolving mount point: {}".format(exc))
                mount_point = None
            if mount_point is None:
                unmounted_disks.append(disk)

        return missing_disks, unmounted_disks

    def check(self, check_strategy):
        """
        Perform additional checks for SnapshotBackupExecutor, specifically:

          - check that the VM instance for which snapshots should be taken exists
          - check that the expected disks are attached to that instance
          - check that the attached disks are mounted on the filesystem

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        super(SnapshotBackupExecutor, self).check(check_strategy)
        if self.server.config.disabled:
            # Skip checks if the server is not active
            return
        check_strategy.init_check("snapshot instance exists")
        if not self.snapshot_interface.instance_exists(self.snapshot_instance):
            check_strategy.result(
                self.config.name,
                False,
                hint="cannot find compute instance %s" % self.snapshot_instance,
            )
            return
        else:
            check_strategy.result(self.config.name, True)

        check_strategy.init_check("snapshot disks attached to instance")
        cmd = unix_command_factory(self.config.ssh_command, self.server.path)
        missing_disks, unmounted_disks = self.find_missing_and_unmounted_disks(
            cmd,
            self.snapshot_interface,
            self.snapshot_instance,
            self.snapshot_disks,
        )

        if len(missing_disks) > 0:
            check_strategy.result(
                self.config.name,
                False,
                hint="cannot find snapshot disks attached to instance %s: %s"
                % (self.snapshot_instance, ", ".join(missing_disks)),
            )
        else:
            check_strategy.result(self.config.name, True)

        check_strategy.init_check("snapshot disks mounted on instance")
        if len(unmounted_disks) > 0:
            check_strategy.result(
                self.config.name,
                False,
                hint="cannot find snapshot disks mounted on instance %s: %s"
                % (self.snapshot_instance, ", ".join(unmounted_disks)),
            )
        else:
            check_strategy.result(self.config.name, True)

    def _start_backup_copy_message(self, backup_info):
        """
        Output message for backup start.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information.
        """
        output.info("Starting backup with disk snapshots for %s", backup_info.backup_id)

    def _stop_backup_copy_message(self, backup_info):
        """
        Output message for backup end.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information.
        """
        output.info(
            "Snapshot backup done (time: %s)",
            human_readable_timedelta(
                datetime.timedelta(seconds=backup_info.copy_stats["copy_time"])
            ),
        )


class BackupStrategy(with_metaclass(ABCMeta, object)):
    """
    Abstract base class for a strategy to be used by a backup executor.
    """

    #: Regex for START WAL LOCATION info
    START_TIME_RE = re.compile(r"^START TIME: (.*)", re.MULTILINE)

    #: Regex for START TIME info
    WAL_RE = re.compile(r"^START WAL LOCATION: (.*) \(file (.*)\)", re.MULTILINE)

    def __init__(self, postgres, server_name, mode=None):
        """
        Constructor

        :param barman.postgres.PostgreSQLConnection postgres: the PostgreSQL
            connection
        :param str server_name: The name of the server
        """
        self.postgres = postgres
        self.server_name = server_name

        # Holds the action being executed. Used for error messages.
        self.current_action = None
        self.mode = mode

    def start_backup(self, backup_info):
        """
        Issue a start of a backup - invoked by BackupExecutor.backup()

        :param barman.infofile.BackupInfo backup_info: backup information
        """
        # Retrieve PostgreSQL server metadata
        self._pg_get_metadata(backup_info)

        # Record that we are about to start the backup
        self.current_action = "issuing start backup command"
        _logger.debug(self.current_action)

    @abstractmethod
    def stop_backup(self, backup_info):
        """
        Issue a stop of a backup - invoked by BackupExecutor.backup()

        :param barman.infofile.LocalBackupInfo backup_info: backup information
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
        # Get the PostgreSQL data directory location
        self.current_action = "detecting data directory"
        output.debug(self.current_action)
        data_directory = self.postgres.get_setting("data_directory")
        backup_info.set_attribute("pgdata", data_directory)

        # Set server version
        backup_info.set_attribute("version", self.postgres.server_version)

        # Set XLOG segment size
        backup_info.set_attribute("xlog_segment_size", self.postgres.xlog_segment_size)

        # Set configuration files location
        cf = self.postgres.get_configuration_files()
        for key in cf:
            backup_info.set_attribute(key, cf[key])

        # Get tablespaces information
        self.current_action = "detecting tablespaces"
        output.debug(self.current_action)
        tablespaces = self.postgres.get_tablespaces()
        if tablespaces and len(tablespaces) > 0:
            backup_info.set_attribute("tablespaces", tablespaces)
            for item in tablespaces:
                msg = "\t%s, %s, %s" % (item.oid, item.name, item.location)
                _logger.info(msg)

        # Set data_checksums state
        data_checksums = self.postgres.get_setting("data_checksums")
        backup_info.set_attribute("data_checksums", data_checksums)

        # Get summarize_wal information for incremental backups
        # Postgres major version should be >= 17
        backup_info.set_attribute("summarize_wal", None)
        if self.postgres.server_version >= 170000:
            summarize_wal = self.postgres.get_setting("summarize_wal")
            backup_info.set_attribute("summarize_wal", summarize_wal)

        # Set total size of the PostgreSQL server
        backup_info.set_attribute("cluster_size", self.postgres.current_size)

    @staticmethod
    def _backup_info_from_start_location(backup_info, start_info):
        """
        Fill a backup info with information from a start_backup

        :param barman.infofile.BackupInfo backup_info: object
            representing a backup
        :param DictCursor start_info: the result of the pg_backup_start
            command
        """
        backup_info.set_attribute("status", BackupInfo.STARTED)
        backup_info.set_attribute("begin_time", start_info["timestamp"])
        backup_info.set_attribute("begin_xlog", start_info["location"])

        # PostgreSQL 9.6+ directly provides the timeline
        if start_info.get("timeline") is not None:
            backup_info.set_attribute("timeline", start_info["timeline"])
            # Take a copy of stop_info because we are going to update it
            start_info = start_info.copy()
            start_info.update(
                xlog.location_to_xlogfile_name_offset(
                    start_info["location"],
                    start_info["timeline"],
                    backup_info.xlog_segment_size,
                )
            )

        # If file_name and file_offset are available, use them
        file_name = start_info.get("file_name")
        file_offset = start_info.get("file_offset")
        if file_name is not None and file_offset is not None:
            backup_info.set_attribute("begin_wal", start_info["file_name"])
            backup_info.set_attribute("begin_offset", start_info["file_offset"])

            # If the timeline is still missing, extract it from the file_name
            if backup_info.timeline is None:
                backup_info.set_attribute(
                    "timeline", int(start_info["file_name"][0:8], 16)
                )

    @staticmethod
    def _backup_info_from_stop_location(backup_info, stop_info):
        """
        Fill a backup info with information from a backup stop location

        :param barman.infofile.BackupInfo backup_info: object representing a
            backup
        :param DictCursor stop_info: location info of stop backup
        """

        # If file_name or file_offset are missing build them using the stop
        # location and the timeline.
        file_name = stop_info.get("file_name")
        file_offset = stop_info.get("file_offset")
        if file_name is None or file_offset is None:
            # Take a copy of stop_info because we are going to update it
            stop_info = stop_info.copy()
            # Get the timeline from the stop_info if available, otherwise
            # Use the one from the backup_label
            timeline = stop_info.get("timeline")
            if timeline is None:
                timeline = backup_info.timeline
            stop_info.update(
                xlog.location_to_xlogfile_name_offset(
                    stop_info["location"], timeline, backup_info.xlog_segment_size
                )
            )

        backup_info.set_attribute("end_time", stop_info["timestamp"])
        backup_info.set_attribute("end_xlog", stop_info["location"])
        backup_info.set_attribute("end_wal", stop_info["file_name"])
        backup_info.set_attribute("end_offset", stop_info["file_offset"])

    def _backup_info_from_backup_label(self, backup_info):
        """
        Fill a backup info with information from the backup_label file

        :param barman.infofile.BackupInfo backup_info: object
            representing a backup
        """
        # The backup_label must be already loaded
        assert backup_info.backup_label

        # Parse backup label
        wal_info = self.WAL_RE.search(backup_info.backup_label)
        start_time = self.START_TIME_RE.search(backup_info.backup_label)
        if wal_info is None or start_time is None:
            raise ValueError(
                "Failure parsing backup_label for backup %s" % backup_info.backup_id
            )

        # Set data in backup_info from backup_label
        backup_info.set_attribute("timeline", int(wal_info.group(2)[0:8], 16))
        backup_info.set_attribute("begin_xlog", wal_info.group(1))
        backup_info.set_attribute("begin_wal", wal_info.group(2))
        backup_info.set_attribute(
            "begin_offset",
            xlog.parse_lsn(wal_info.group(1)) % backup_info.xlog_segment_size,
        )

        # If we have already obtained a begin_time then it takes precedence over the
        # begin time in the backup label
        if not backup_info.begin_time:
            backup_info.set_attribute(
                "begin_time", dateutil.parser.parse(start_time.group(1))
            )

    def _read_backup_label(self, backup_info):
        """
        Read the backup_label file

        :param barman.infofile.LocalBackupInfo  backup_info: backup information
        """
        self.current_action = "reading the backup label"
        label_path = os.path.join(backup_info.get_data_directory(), "backup_label")
        output.debug("Reading backup label: %s" % label_path)
        with open(label_path, "r") as f:
            backup_label = f.read()
        backup_info.set_attribute("backup_label", backup_label)


class PostgresBackupStrategy(BackupStrategy):
    """
    Concrete class for postgres backup strategy.

    This strategy is for PostgresBackupExecutor only and is responsible for
    executing pre and post backup operations during a physical backup executed
    using pg_basebackup.
    """

    def __init__(self, postgres, server_name, backup_compression=None):
        """
        Constructor

        :param barman.postgres.PostgreSQLConnection postgres: the PostgreSQL
            connection
        :param str server_name: The name of the server
        :param barman.compression.PgBaseBackupCompression backup_compression:
            the pg_basebackup compression options used for this backup
        """
        super(PostgresBackupStrategy, self).__init__(postgres, server_name)
        self.backup_compression = backup_compression

    def check(self, check_strategy):
        """
        Perform additional checks for the Postgres backup strategy
        """

    def start_backup(self, backup_info):
        """
        Manage the start of an pg_basebackup backup

        The method performs all the preliminary operations required for a
        backup executed using pg_basebackup to start, gathering information
        from postgres and filling the backup_info.

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """
        self.current_action = "initialising postgres backup_method"
        super(PostgresBackupStrategy, self).start_backup(backup_info)
        current_xlog_info = self.postgres.current_xlog_info
        self._backup_info_from_start_location(backup_info, current_xlog_info)

    def stop_backup(self, backup_info):
        """
        Manage the stop of an pg_basebackup backup

        The method retrieves the information necessary for the
        backup.info file reading the backup_label file.

        Due of the nature of the pg_basebackup, information that are gathered
        during the start of a backup performed using rsync, are retrieved
        here

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """
        if self.backup_compression and self.backup_compression.config.format != "plain":
            backup_info.set_attribute(
                "compression", self.backup_compression.config.type
            )

        self._read_backup_label(backup_info)
        self._backup_info_from_backup_label(backup_info)

        # Set data in backup_info from current_xlog_info
        self.current_action = "stopping postgres backup_method"
        output.info("Finalising the backup.")

        # Get the current xlog position
        current_xlog_info = self.postgres.current_xlog_info
        if current_xlog_info:
            self._backup_info_from_stop_location(backup_info, current_xlog_info)

        # Ask PostgreSQL to switch to another WAL file. This is needed
        # to archive the transaction log file containing the backup
        # end position, which is required to recover from the backup.
        try:
            self.postgres.switch_wal()
        except PostgresIsInRecovery:
            # Skip switching XLOG if a standby server
            pass

    def _read_compressed_backup_label(self, backup_info):
        """
        Read the contents of a backup_label file from a compressed archive.

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """
        basename = os.path.join(backup_info.get_data_directory(), "base")
        try:
            return self.backup_compression.get_file_content("backup_label", basename)
        except FileNotFoundException:
            raise BackupException(
                "Could not find backup_label in %s"
                % self.backup_compression.with_suffix(basename)
            )

    def _read_backup_label(self, backup_info):
        """
        Read the backup_label file.

        Transparently handles the fact that the backup_label file may be in a
        compressed tarball.

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """
        self.current_action = "reading the backup label"
        if backup_info.compression is not None:
            backup_label = self._read_compressed_backup_label(backup_info)
            backup_info.set_attribute("backup_label", backup_label)
        else:
            super(PostgresBackupStrategy, self)._read_backup_label(backup_info)


class CloudPostgresBackupStrategy(PostgresBackupStrategy):
    """
    Concrete class for cloud postgres backup strategy.

    This strategy follows the same logic as :class:`PostgresBackupStrategy` except that
    it does not read the ``backup_label`` file. This is because in cloud backups the
    backup label is never stored in the backup's data directory, as this class' parent
    assumes. Instead, the file only exists temporarily in the staging area before being
    uploaded to cloud storage. Hence the need for this subclass with this specific
    override.

    The backup label is still read to fill the ``backup.info`` file in the
    :meth:`CloudPostgresBackupExecutor._read_backup_label` though, so the same
    behavior is still preserved.
    """

    def _read_backup_label(self, backup_info):
        return


class ExclusiveBackupStrategy(BackupStrategy):
    """
    Concrete class for exclusive backup strategy.

    This strategy is for ExternalBackupExecutor only and is responsible for
    coordinating Barman with PostgreSQL on standard physical backup
    operations (known as 'exclusive' backup), such as invoking
    pg_start_backup() and pg_stop_backup() on the master server.
    """

    def __init__(self, postgres, server_name):
        """
        Constructor

        :param barman.postgres.PostgreSQLConnection postgres: the PostgreSQL
            connection
        :param str server_name: The name of the server
        """
        super(ExclusiveBackupStrategy, self).__init__(
            postgres, server_name, "exclusive"
        )

    def start_backup(self, backup_info):
        """
        Manage the start of an exclusive backup

        The method performs all the preliminary operations required for an
        exclusive physical backup to start, as well as preparing the
        information on the backup for Barman.

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """
        super(ExclusiveBackupStrategy, self).start_backup(backup_info)
        label = "Barman backup %s %s" % (backup_info.server_name, backup_info.backup_id)

        # Issue an exclusive start backup command
        _logger.debug("Start of exclusive backup")
        start_info = self.postgres.start_exclusive_backup(label)
        self._backup_info_from_start_location(backup_info, start_info)

    def stop_backup(self, backup_info):
        """
        Manage the stop of an exclusive backup

        The method informs the PostgreSQL server that the physical
        exclusive backup is finished, as well as preparing the information
        returned by PostgreSQL for Barman.

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """

        self.current_action = "issuing stop backup command"
        _logger.debug("Stop of exclusive backup")
        stop_info = self.postgres.stop_exclusive_backup()
        self._backup_info_from_stop_location(backup_info, stop_info)

    def check(self, check_strategy):
        """
        Perform additional checks for ExclusiveBackupStrategy

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        # Make sure PostgreSQL is not in recovery (i.e. is a master)
        check_strategy.init_check("not in recovery")
        if self.postgres:
            is_in_recovery = self.postgres.is_in_recovery
            if not is_in_recovery:
                check_strategy.result(self.server_name, True)
            else:
                check_strategy.result(
                    self.server_name,
                    False,
                    hint="cannot perform exclusive backup on a standby",
                )
        check_strategy.init_check("exclusive backup supported")
        try:
            if self.postgres and self.postgres.server_version < 150000:
                check_strategy.result(self.server_name, True)
            else:
                check_strategy.result(
                    self.server_name,
                    False,
                    hint="exclusive backups not supported "
                    "on PostgreSQL %s" % self.postgres.server_major_version,
                )
        except PostgresConnectionError:
            check_strategy.result(
                self.server_name,
                False,
                hint="unable to determine postgres version",
            )


class ConcurrentBackupStrategy(BackupStrategy):
    """
    Concrete class for concurrent backup strategy.

    This strategy is responsible for coordinating Barman with PostgreSQL on
    concurrent physical backup operations through concurrent backup
    PostgreSQL api.
    """

    def __init__(self, postgres, server_name):
        """
        Constructor

        :param barman.postgres.PostgreSQLConnection postgres: the PostgreSQL
            connection
        :param str server_name: The name of the server
        """
        super(ConcurrentBackupStrategy, self).__init__(
            postgres, server_name, "concurrent"
        )

    def check(self, check_strategy):
        """
        Checks that Postgres is at least minimal version

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        check_strategy.init_check("postgres minimal version")
        try:
            # We execute this check only if the postgres connection is not None
            # to validate the server version matches at least minimal version
            if self.postgres and not self.postgres.is_minimal_postgres_version():
                check_strategy.result(
                    self.server_name,
                    False,
                    hint="unsupported PostgresSQL version %s. Expecting %s or above."
                    % (
                        self.postgres.server_major_version,
                        self.postgres.minimal_txt_version,
                    ),
                )

        except PostgresConnectionError:
            # Skip the check if the postgres connection doesn't work.
            # We assume that this error condition will be reported by
            # another check.
            pass

    def start_backup(self, backup_info):
        """
        Start of the backup.

        The method performs all the preliminary operations required for a
        backup to start.

        :param barman.infofile.BackupInfo backup_info: backup information
        """
        super(ConcurrentBackupStrategy, self).start_backup(backup_info)

        label = "Barman backup %s %s" % (backup_info.server_name, backup_info.backup_id)

        if not self.postgres.is_minimal_postgres_version():
            _logger.error("Postgres version not supported")
            raise BackupException("Postgres version not supported")
        # On 9.6+ execute native concurrent start backup
        _logger.debug("Start of native concurrent backup")
        self._concurrent_start_backup(backup_info, label)

    def stop_backup(self, backup_info):
        """
        Stop backup wrapper

        :param barman.infofile.BackupInfo backup_info: backup information
        """
        self.current_action = "issuing stop backup command (native concurrent)"
        if not self.postgres.is_minimal_postgres_version():
            _logger.error(
                "Postgres version not supported. Minimal version is %s"
                % self.postgres.minimal_txt_version
            )
            raise BackupException("Postgres version not supported")

        _logger.debug("Stop of native concurrent backup")
        self._concurrent_stop_backup(backup_info)

        # Update the current action in preparation for writing the backup label.
        # NOTE: The actual writing of the backup label happens either in the
        # specialization of this function in LocalConcurrentBackupStrategy
        # or out-of-band in a CloudBackupUploader (when ConcurrentBackupStrategy
        # is used directly when writing to an object store).
        self.current_action = "writing backup label"

        # Ask PostgreSQL to switch to another WAL file. This is needed
        # to archive the transaction log file containing the backup
        # end position, which is required to recover from the backup.
        try:
            self.postgres.switch_wal()
        except PostgresIsInRecovery:
            # Skip switching XLOG if a standby server
            pass

    def _concurrent_start_backup(self, backup_info, label):
        """
        Start a concurrent backup using the PostgreSQL 9.6
        concurrent backup api

        :param barman.infofile.BackupInfo backup_info: backup information
        :param str label: the backup label
        """
        start_info = self.postgres.start_concurrent_backup(label)
        self.postgres.allow_reconnect = False
        self._backup_info_from_start_location(backup_info, start_info)

    def _concurrent_stop_backup(self, backup_info):
        """
        Stop a concurrent backup using the PostgreSQL 9.6
        concurrent backup api

        :param barman.infofile.BackupInfo backup_info: backup information
        """
        stop_info = self.postgres.stop_concurrent_backup()
        self.postgres.allow_reconnect = True
        backup_info.set_attribute("backup_label", stop_info["backup_label"])
        self._backup_info_from_stop_location(backup_info, stop_info)


class LocalConcurrentBackupStrategy(ConcurrentBackupStrategy):
    """
    Concrete class for concurrent backup strategy writing data locally.

    This strategy is for ExternalBackupExecutor only and is responsible for
    coordinating Barman with PostgreSQL on concurrent physical backup
    operations through concurrent backup PostgreSQL api.
    """

    # noinspection PyMethodMayBeStatic
    def _write_backup_label(self, backup_info):
        """
        Write the backup_label file inside local data directory

        :param barman.infofile.LocalBackupInfo  backup_info: backup information
        """
        label_file = os.path.join(backup_info.get_data_directory(), "backup_label")
        output.debug("Writing backup label: %s" % label_file)
        with open(label_file, "w") as f:
            f.write(backup_info.backup_label)

    def stop_backup(self, backup_info):
        """
        Stop backup wrapper

        :param barman.infofile.LocalBackupInfo backup_info: backup information
        """
        super(LocalConcurrentBackupStrategy, self).stop_backup(backup_info)
        self._write_backup_label(backup_info)
