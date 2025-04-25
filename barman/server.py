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
This module represents a Server.
Barman is able to manage multiple servers.
"""
import datetime
import errno
import json
import logging
import os
import re
import shutil
import sys
import tarfile
import tempfile
import time
from collections import namedtuple
from contextlib import closing, contextmanager
from glob import glob
from tempfile import NamedTemporaryFile

import dateutil.tz

import barman
from barman import fs, output, xlog
from barman.backup import BackupManager
from barman.command_wrappers import BarmanSubProcess, Command, Rsync
from barman.compression import CustomCompressor
from barman.copy_controller import RsyncCopyController
from barman.encryption import get_passphrase_from_command
from barman.exceptions import (
    ArchiverFailure,
    BackupException,
    BadXlogSegmentName,
    CommandFailedException,
    ConninfoException,
    InvalidRetentionPolicy,
    LockFileBusy,
    LockFileException,
    LockFilePermissionDenied,
    PostgresCheckpointPrivilegesRequired,
    PostgresDuplicateReplicationSlot,
    PostgresException,
    PostgresInvalidReplicationSlot,
    PostgresIsInRecovery,
    PostgresObsoleteFeature,
    PostgresReplicationSlotInUse,
    PostgresReplicationSlotsFull,
    PostgresSuperuserRequired,
    PostgresUnsupportedFeature,
    SyncError,
    SyncNothingToDo,
    SyncToBeDeleted,
    TimeoutError,
    UnknownBackupIdException,
)
from barman.infofile import BackupInfo, LocalBackupInfo, WalFileInfo
from barman.lockfile import (
    ServerBackupIdLock,
    ServerBackupLock,
    ServerBackupSyncLock,
    ServerCronLock,
    ServerWalArchiveLock,
    ServerWalReceiveLock,
    ServerWalSyncLock,
    ServerXLOGDBLock,
)
from barman.postgres import (
    PostgreSQL,
    PostgreSQLConnection,
    StandbyPostgreSQLConnection,
    StreamingConnection,
)
from barman.process import ProcessManager
from barman.remote_status import RemoteStatusMixin
from barman.retention_policies import RetentionPolicy, RetentionPolicyFactory
from barman.utils import (
    BarmanEncoder,
    file_hash,
    force_str,
    fsync_dir,
    fsync_file,
    human_readable_timedelta,
    is_power_of_two,
    mkpath,
    parse_target_tli,
    pretty_size,
    timeout,
)
from barman.wal_archiver import FileWalArchiver, StreamingWalArchiver, WalArchiver

PARTIAL_EXTENSION = ".partial"
PRIMARY_INFO_FILE = "primary.info"
SYNC_WALS_INFO_FILE = "sync-wals.info"

_logger = logging.getLogger(__name__)

# NamedTuple for a better readability of SyncWalInfo
SyncWalInfo = namedtuple("SyncWalInfo", "last_wal last_position")


class CheckStrategy(object):
    """
    This strategy for the 'check' collects the results of
    every check and does not print any message.
    This basic class is also responsible for immediately
    logging any performed check with an error in case of
    check failure and a debug message in case of success.
    """

    # create a namedtuple object called CheckResult to manage check results
    CheckResult = namedtuple("CheckResult", "server_name check status")

    # Default list used as a filter to identify non-critical checks
    NON_CRITICAL_CHECKS = [
        "minimum redundancy requirements",
        "backup maximum age",
        "backup minimum size",
        "failed backups",
        "archiver errors",
        "empty incoming directory",
        "empty streaming directory",
        "incoming WALs directory",
        "streaming WALs directory",
        "wal maximum age",
        "PostgreSQL server is standby",
    ]

    def __init__(self, ignore_checks=NON_CRITICAL_CHECKS):
        """
        Silent Strategy constructor

        :param list ignore_checks: list of checks that can be ignored
        """
        self.ignore_list = ignore_checks
        self.check_result = []
        self.has_error = False
        self.running_check = None

    def init_check(self, check_name):
        """
        Mark in the debug log when barman starts the execution of a check

        :param str check_name: the name of the check that is starting
        """
        self.running_check = check_name
        _logger.debug("Starting check: '%s'" % check_name)

    def _check_name(self, check):
        if not check:
            check = self.running_check
        assert check
        return check

    def result(self, server_name, status, hint=None, check=None, perfdata=None):
        """
        Store the result of a check (with no output).
        Log any check result (error or debug level).

        :param str server_name: the server is being checked
        :param bool status: True if succeeded
        :param str,None hint: hint to print if not None:
        :param str,None check: the check name
        :param str,None perfdata: additional performance data to print if not None
        """
        check = self._check_name(check)
        if not status:
            # If the name of the check is not in the filter list,
            # treat it as a blocking error, then notify the error
            # and change the status of the strategy
            if check not in self.ignore_list:
                self.has_error = True
                _logger.error(
                    "Check '%s' failed for server '%s'" % (check, server_name)
                )
            else:
                # otherwise simply log the error (as info)
                _logger.info(
                    "Ignoring failed check '%s' for server '%s'" % (check, server_name)
                )
        else:
            _logger.debug("Check '%s' succeeded for server '%s'" % (check, server_name))

        # Store the result and does not output anything
        result = self.CheckResult(server_name, check, status)
        self.check_result.append(result)
        self.running_check = None


class CheckOutputStrategy(CheckStrategy):
    """
    This strategy for the 'check' command immediately sends
    the result of a check to the designated output channel.
    This class derives from the basic CheckStrategy, reuses
    the same logic and adds output messages.
    """

    def __init__(self):
        """
        Output Strategy constructor
        """
        super(CheckOutputStrategy, self).__init__(ignore_checks=())

    def result(self, server_name, status, hint=None, check=None, perfdata=None):
        """
        Store the result of a check.
        Log any check result (error or debug level).
        Output the result to the user

        :param str server_name: the server being checked
        :param str check: the check name
        :param bool status: True if succeeded
        :param str,None hint: hint to print if not None:
        :param str,None perfdata: additional performance data to print if not None
        """
        check = self._check_name(check)
        super(CheckOutputStrategy, self).result(
            server_name, status, hint, check, perfdata
        )
        # Send result to output
        output.result("check", server_name, check, status, hint, perfdata)


class Server(RemoteStatusMixin):
    """
    This class represents the PostgreSQL server to backup.
    """

    XLOGDB_NAME = "{server}-xlog.db"

    # the strategy for the management of the results of the various checks
    __default_check_strategy = CheckOutputStrategy()

    def __init__(self, config):
        """
        Server constructor.

        :param barman.config.ServerConfig config: the server configuration
        """
        super(Server, self).__init__()
        self.config = config
        self.path = self._build_path(self.config.path_prefix)
        self.process_manager = ProcessManager(self.config)

        # If 'primary_ssh_command' is specified, the source of the backup
        # for this server is a Barman installation (not a Postgres server)
        self.passive_node = config.primary_ssh_command is not None

        self.enforce_retention_policies = False
        self.postgres = None
        self.streaming = None
        self.archivers = []

        # Postgres configuration is available only if node is not passive
        if not self.passive_node:
            self._init_postgres(config)

        # Initialize the backup manager
        self.backup_manager = BackupManager(self)

        if not self.passive_node:
            self._init_archivers()

        # Set global and tablespace bandwidth limits
        self._init_bandwidth_limits()

        # Initialize minimum redundancy
        self._init_minimum_redundancy()

        # Initialise retention policies
        self._init_retention_policies()

    def _init_postgres(self, config):
        # Initialize the main PostgreSQL connection
        try:
            # Check that 'conninfo' option is properly set
            if config.conninfo is None:
                raise ConninfoException(
                    "Missing 'conninfo' parameter for server '%s'" % config.name
                )
            # If primary_conninfo is set then we're connecting to a standby
            if config.primary_conninfo is not None:
                self.postgres = StandbyPostgreSQLConnection(
                    config.conninfo,
                    config.primary_conninfo,
                    config.immediate_checkpoint,
                    config.slot_name,
                    config.primary_checkpoint_timeout,
                )
                # If primary_conninfo is set but conninfo does not point to a standby
                # it could be that a failover happend and the standby has been promoted.
                # In this case, don't set a standby connection and just warn the user.
                # A standard connection will be set further so that Barman keeps working.
                if self.postgres.is_in_recovery is False:
                    self.postgres.close()
                    self.postgres = None
                    output.warning(
                        "'primary_conninfo' is set but 'conninfo' does not point to a "
                        "standby server. Ignoring 'primary_conninfo'."
                    )
            if self.postgres is None:
                self.postgres = PostgreSQLConnection(
                    config.conninfo, config.immediate_checkpoint, config.slot_name
                )
        # If the PostgreSQLConnection creation fails, disable the Server
        except ConninfoException as e:
            self.config.update_msg_list_and_disable_server(
                "PostgreSQL connection: " + force_str(e).strip()
            )

        # Initialize the streaming PostgreSQL connection only when
        # backup_method is postgres or the streaming_archiver is in use
        if config.backup_method == "postgres" or config.streaming_archiver:
            try:
                if config.streaming_conninfo is None:
                    raise ConninfoException(
                        "Missing 'streaming_conninfo' parameter for "
                        "server '%s'" % config.name
                    )
                self.streaming = StreamingConnection(config.streaming_conninfo)
            # If the StreamingConnection creation fails, disable the server
            except ConninfoException as e:
                self.config.update_msg_list_and_disable_server(
                    "Streaming connection: " + force_str(e).strip()
                )

    def _init_archivers(self):
        # Initialize the StreamingWalArchiver
        # WARNING: Order of items in self.archivers list is important!
        # The files will be archived in that order.
        if self.config.streaming_archiver:
            try:
                self.archivers.append(StreamingWalArchiver(self.backup_manager))
            # If the StreamingWalArchiver creation fails,
            # disable the server
            except AttributeError as e:
                _logger.debug(e)
                self.config.update_msg_list_and_disable_server(
                    "Unable to initialise the streaming archiver"
                )

        # IMPORTANT: The following lines of code have been
        # temporarily commented in order to make the code
        # back-compatible after the introduction of 'archiver=off'
        # as default value in Barman 2.0.
        # When the back compatibility feature for archiver will be
        # removed, the following lines need to be decommented.
        # ARCHIVER_OFF_BACKCOMPATIBILITY - START OF CODE
        # # At least one of the available archive modes should be enabled
        # if len(self.archivers) < 1:
        #     self.config.update_msg_list_and_disable_server(
        #         "No archiver enabled for server '%s'. "
        #         "Please turn on 'archiver', 'streaming_archiver' or both"
        #         % config.name
        #     )
        # ARCHIVER_OFF_BACKCOMPATIBILITY - END OF CODE

        # Sanity check: if file based archiver is disabled, and only
        # WAL streaming is enabled, a replication slot name must be
        # configured.
        if (
            not self.config.archiver
            and self.config.streaming_archiver
            and self.config.slot_name is None
        ):
            self.config.update_msg_list_and_disable_server(
                "Streaming-only archiver requires 'streaming_conninfo' "
                "and 'slot_name' options to be properly configured"
            )

        # ARCHIVER_OFF_BACKCOMPATIBILITY - START OF CODE
        # IMPORTANT: This is a back-compatibility feature that has
        # been added in Barman 2.0. It highlights a deprecated
        # behaviour, and helps users during this transition phase.
        # It forces 'archiver=on' when both archiver and streaming_archiver
        # are set to 'off' (default values) and displays a warning,
        # requesting users to explicitly set the value in the
        # configuration.
        # When this back-compatibility feature will be removed from Barman
        # (in a couple of major releases), developers will need to remove
        # this block completely and reinstate the block of code you find
        # a few lines below (search for ARCHIVER_OFF_BACKCOMPATIBILITY
        # throughout the code).
        if self.config.archiver is False and self.config.streaming_archiver is False:
            output.warning(
                "No archiver enabled for server '%s'. "
                "Please turn on 'archiver', "
                "'streaming_archiver' or both",
                self.config.name,
            )
            output.warning("Forcing 'archiver = on'")
            self.config.archiver = True
        # ARCHIVER_OFF_BACKCOMPATIBILITY - END OF CODE

        # Initialize the FileWalArchiver
        # WARNING: Order of items in self.archivers list is important!
        # The files will be archived in that order.
        if self.config.archiver:
            try:
                self.archivers.append(FileWalArchiver(self.backup_manager))
            except AttributeError as e:
                _logger.debug(e)
                self.config.update_msg_list_and_disable_server(
                    "Unable to initialise the file based archiver"
                )

    def _init_bandwidth_limits(self):
        # Global bandwidth limits
        if self.config.bandwidth_limit:
            try:
                self.config.bandwidth_limit = int(self.config.bandwidth_limit)
            except ValueError:
                _logger.warning(
                    'Invalid bandwidth_limit "%s" for server "%s" '
                    '(fallback to "0")'
                    % (self.config.bandwidth_limit, self.config.name)
                )
                self.config.bandwidth_limit = None

        # Tablespace bandwidth limits
        if self.config.tablespace_bandwidth_limit:
            rules = {}
            for rule in self.config.tablespace_bandwidth_limit.split():
                try:
                    key, value = rule.split(":", 1)
                    value = int(value)
                    if value != self.config.bandwidth_limit:
                        rules[key] = value
                except ValueError:
                    _logger.warning(
                        "Invalid tablespace_bandwidth_limit rule '%s'" % rule
                    )
            if len(rules) > 0:
                self.config.tablespace_bandwidth_limit = rules
            else:
                self.config.tablespace_bandwidth_limit = None

    def _init_minimum_redundancy(self):
        # Set minimum redundancy (default 0)
        try:
            self.config.minimum_redundancy = int(self.config.minimum_redundancy)
            if self.config.minimum_redundancy < 0:
                _logger.warning(
                    'Negative value of minimum_redundancy "%s" '
                    'for server "%s" (fallback to "0")'
                    % (self.config.minimum_redundancy, self.config.name)
                )
                self.config.minimum_redundancy = 0
        except ValueError:
            _logger.warning(
                'Invalid minimum_redundancy "%s" for server "%s" '
                '(fallback to "0")' % (self.config.minimum_redundancy, self.config.name)
            )
            self.config.minimum_redundancy = 0

    def _init_retention_policies(self):
        # Set retention policy mode
        if self.config.retention_policy_mode != "auto":
            _logger.warning(
                'Unsupported retention_policy_mode "%s" for server "%s" '
                '(fallback to "auto")'
                % (self.config.retention_policy_mode, self.config.name)
            )
            self.config.retention_policy_mode = "auto"

        # If retention_policy is present, enforce them
        if self.config.retention_policy and not isinstance(
            self.config.retention_policy, RetentionPolicy
        ):
            # Check wal_retention_policy
            if self.config.wal_retention_policy != "main":
                _logger.warning(
                    'Unsupported wal_retention_policy value "%s" '
                    'for server "%s" (fallback to "main")'
                    % (self.config.wal_retention_policy, self.config.name)
                )
                self.config.wal_retention_policy = "main"
            # Create retention policy objects
            try:
                rp = RetentionPolicyFactory.create(
                    "retention_policy", self.config.retention_policy, server=self
                )
                # Reassign the configuration value (we keep it in one place)
                self.config.retention_policy = rp
                _logger.debug(
                    "Retention policy for server %s: %s"
                    % (self.config.name, self.config.retention_policy)
                )
                try:
                    rp = RetentionPolicyFactory.create(
                        "wal_retention_policy",
                        self.config.wal_retention_policy,
                        server=self,
                    )
                    # Reassign the configuration value
                    # (we keep it in one place)
                    self.config.wal_retention_policy = rp
                    _logger.debug(
                        "WAL retention policy for server %s: %s"
                        % (self.config.name, self.config.wal_retention_policy)
                    )
                except InvalidRetentionPolicy:
                    _logger.exception(
                        'Invalid wal_retention_policy setting "%s" '
                        'for server "%s" (fallback to "main")'
                        % (self.config.wal_retention_policy, self.config.name)
                    )
                    rp = RetentionPolicyFactory.create(
                        "wal_retention_policy", "main", server=self
                    )
                    self.config.wal_retention_policy = rp

                self.enforce_retention_policies = True

            except InvalidRetentionPolicy:
                _logger.exception(
                    'Invalid retention_policy setting "%s" for server "%s"'
                    % (self.config.retention_policy, self.config.name)
                )

    def get_identity_file_path(self):
        """
        Get the path of the file that should contain the identity
        of the cluster
        :rtype: str
        """
        return os.path.join(self.config.backup_directory, "identity.json")

    def write_identity_file(self):
        """
        Store the identity of the server if it doesn't already exist.
        """
        file_path = self.get_identity_file_path()

        # Do not write the identity if file already exists
        if os.path.exists(file_path):
            return

        systemid = self.systemid
        if systemid:
            try:
                with open(file_path, "w") as fp:
                    json.dump(
                        {
                            "systemid": systemid,
                            "version": self.postgres.server_major_version,
                        },
                        fp,
                        indent=4,
                        sort_keys=True,
                    )
                    fp.write("\n")
            except IOError:
                _logger.exception(
                    'Cannot write system Id file for server "%s"' % (self.config.name)
                )

    def read_identity_file(self):
        """
        Read the server identity
        :rtype: dict[str,str]
        """
        file_path = self.get_identity_file_path()
        try:
            with open(file_path, "r") as fp:
                return json.load(fp)
        except IOError:
            _logger.exception(
                'Cannot read system Id file for server "%s"' % (self.config.name)
            )
            return {}

    def close(self):
        """
        Close all the open connections to PostgreSQL
        """
        if self.postgres:
            self.postgres.close()
        if self.streaming:
            self.streaming.close()

    def check(self, check_strategy=__default_check_strategy):
        """
        Implements the 'server check' command and makes sure SSH and PostgreSQL
        connections work properly. It checks also that backup directories exist
        (and if not, it creates them).

        The check command will time out after a time interval defined by the
        check_timeout configuration value (default 30 seconds)

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        try:
            with timeout(self.config.check_timeout):
                # Check WAL archive
                self.check_archive(check_strategy)
                # Postgres configuration is not available on passive nodes
                if not self.passive_node:
                    self.check_postgres(check_strategy)
                    self.check_wal_streaming(check_strategy)
                # Check barman directories from barman configuration
                self.check_directories(check_strategy)
                # Check retention policies
                self.check_retention_policy_settings(check_strategy)
                # Check for backup validity
                self.check_backup_validity(check_strategy)
                # Check if encryption works
                self.check_encryption(check_strategy)
                # Check WAL archiving is happening
                self.check_wal_validity(check_strategy)
                # Executes the backup manager set of checks
                self.backup_manager.check(check_strategy)
                # Check if the msg_list of the server
                # contains messages and output eventual failures
                self.check_configuration(check_strategy)
                # Check the system Id coherence between
                # streaming and normal connections
                self.check_identity(check_strategy)
                # Executes check() for every archiver, passing
                # remote status information for efficiency
                for archiver in self.archivers:
                    archiver.check(check_strategy)

                # Check archiver errors
                self.check_archiver_errors(check_strategy)
        except TimeoutError:
            # The check timed out.
            # Add a failed entry to the check strategy for this.
            _logger.info(
                "Check command timed out executing '%s' check"
                % check_strategy.running_check
            )
            check_strategy.result(
                self.config.name,
                False,
                hint="barman check command timed out",
                check="check timeout",
            )

    def check_archive(self, check_strategy):
        """
        Checks WAL archive

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        check_strategy.init_check("WAL archive")
        # Make sure that WAL archiving has been setup
        # XLOG_DB needs to exist and its size must be > 0
        # NOTE: we do not need to acquire a lock in this phase
        xlogdb_empty = True
        if os.path.exists(self.xlogdb_file_path):
            with open(self.xlogdb_file_path, "rb") as fxlogdb:
                if os.fstat(fxlogdb.fileno()).st_size > 0:
                    xlogdb_empty = False

        # NOTE: This check needs to be only visible if it fails
        if xlogdb_empty:
            # Skip the error if we have a terminated backup
            # with status WAITING_FOR_WALS.
            # TODO: Improve this check
            backup_id = self.get_last_backup_id([BackupInfo.WAITING_FOR_WALS])
            if not backup_id:
                check_strategy.result(
                    self.config.name,
                    False,
                    hint="please make sure WAL shipping is setup",
                )

        # Check the number of wals in the incoming directory
        self._check_wal_queue(check_strategy, "incoming", "archiver")

        # Check the number of wals in the streaming directory
        self._check_wal_queue(check_strategy, "streaming", "streaming_archiver")

    def _check_wal_queue(self, check_strategy, dir_name, archiver_name):
        """
        Check if one of the wal queue directories beyond the
        max file threshold
        """
        # Read the wal queue location from the configuration
        config_name = "%s_wals_directory" % dir_name
        assert hasattr(self.config, config_name)
        incoming_dir = getattr(self.config, config_name)

        # Check if the archiver is enabled
        assert hasattr(self.config, archiver_name)
        enabled = getattr(self.config, archiver_name)

        # Inspect the wal queue directory
        file_count = 0
        for file_item in glob(os.path.join(incoming_dir, "*")):
            # Ignore temporary files
            if file_item.endswith(".tmp"):
                continue
            file_count += 1
        max_incoming_wal = self.config.max_incoming_wals_queue

        # Subtract one from the count because of .partial file inside the
        # streaming directory
        if dir_name == "streaming":
            file_count -= 1

        # If this archiver is disabled, check the number of files in the
        # corresponding directory.
        # If the directory is NOT empty, fail the check and warn the user.
        # NOTE: This check is visible only when it fails
        check_strategy.init_check("empty %s directory" % dir_name)
        if not enabled:
            if file_count > 0:
                check_strategy.result(
                    self.config.name,
                    False,
                    hint="'%s' must be empty when %s=off"
                    % (incoming_dir, archiver_name),
                )
            # No more checks are required if the archiver
            # is not enabled
            return

        # At this point if max_wals_count is none,
        # means that no limit is set so we just need to return
        if max_incoming_wal is None:
            return
        check_strategy.init_check("%s WALs directory" % dir_name)
        if file_count > max_incoming_wal:
            msg = "there are too many WALs in queue: %s, max %s" % (
                file_count,
                max_incoming_wal,
            )
            check_strategy.result(self.config.name, False, hint=msg)

    def check_postgres(self, check_strategy):
        """
        Checks PostgreSQL connection

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        check_strategy.init_check("PostgreSQL")
        # Take the status of the remote server
        remote_status = self.get_remote_status()
        if not remote_status.get("server_txt_version"):
            check_strategy.result(self.config.name, False)
            return
        # Now we know server version is accessible we can check if it is valid
        if remote_status.get("version_supported") is False:
            minimal_txt_version = PostgreSQL.int_version_to_string_version(
                PostgreSQL.MINIMAL_VERSION
            )
            check_strategy.result(
                self.config.name,
                False,
                hint="unsupported version: PostgreSQL server "
                "is too old (%s < %s)"
                % (remote_status["server_txt_version"], minimal_txt_version),
            )
            return
        else:
            check_strategy.result(self.config.name, True)
        # Check for superuser privileges or
        # privileges needed to perform backups
        if remote_status.get("has_backup_privileges") is not None:
            check_strategy.init_check(
                "superuser or standard user with backup privileges"
            )
            if remote_status.get("has_backup_privileges"):
                check_strategy.result(self.config.name, True)
            else:
                check_strategy.result(
                    self.config.name,
                    False,
                    hint="privileges for PostgreSQL backup functions are "
                    "required (see documentation)",
                    check="no access to backup functions",
                )

        self._check_streaming_supported(check_strategy, remote_status)

        self._check_wal_level(check_strategy, remote_status)

        if self.config.primary_conninfo is not None:
            self._check_standby(check_strategy)

    def _check_streaming_supported(self, check_strategy, remote_status, suffix=None):
        """
        Check whether the remote status indicates streaming is possible.

        :param CheckStrategy check_strategy: The strategy for the management
            of the result of this check
        :param dict[str, None|str] remote_status: Remote status information used
            by this check
        :param str|None suffix: A suffix to be appended to the check name
        """
        if "streaming_supported" in remote_status:
            check_name = "PostgreSQL streaming" + (
                "" if suffix is None else f" ({suffix})"
            )

            check_strategy.init_check(check_name)
            hint = None

            # If a streaming connection is available,
            # add its status to the output of the check
            if remote_status["streaming_supported"] is None:
                hint = remote_status["connection_error"]
            check_strategy.result(
                self.config.name, remote_status.get("streaming"), hint=hint
            )

    def _check_wal_level(self, check_strategy, remote_status, suffix=None):
        """
        Check whether the remote status indicates ``wal_level`` is correct.

        :param CheckStrategy check_strategy: The strategy for the management
            of the result of this check
        :param dict[str, None|str] remote_status: Remote status information used
            by this check
        :param str|None suffix: A suffix to be appended to the check name
        """
        # Check wal_level parameter: must be different from 'minimal'
        # the parameter has been introduced in postgres >= 9.0
        if "wal_level" in remote_status:
            check_name = "wal_level" + ("" if suffix is None else f" ({suffix})")
            check_strategy.init_check(check_name)
            if remote_status["wal_level"] != "minimal":
                check_strategy.result(self.config.name, True)
            else:
                check_strategy.result(
                    self.config.name,
                    False,
                    hint="please set it to a higher level than 'minimal'",
                )

    def _check_has_monitoring_privileges(
        self, check_strategy, remote_status, suffix=None
    ):
        """
        Check whether the remote status indicates monitoring information can be read.

        :param CheckStrategy check_strategy: The strategy for the management
            of the result of this check
        :param dict[str, None|str] remote_status: Remote status information used
            by this check
        :param str|None suffix: A suffix to be appended to the check name
        """
        check_name = "has monitoring privileges" + (
            "" if suffix is None else f" ({suffix})"
        )
        check_strategy.init_check(check_name)
        if remote_status.get("has_monitoring_privileges"):
            check_strategy.result(self.config.name, True)
        else:
            check_strategy.result(
                self.config.name,
                False,
                hint="privileges for PostgreSQL monitoring functions are "
                "required (see documentation)",
                check="no access to monitoring functions",
            )

    def check_wal_streaming(self, check_strategy):
        """
        Perform checks related to the streaming of WALs only (not backups).

        If no WAL-specific connection information is defined then checks already
        performed on the default connection information will have verified their
        suitability for WAL streaming so this check will only call
        :meth:`_check_replication_slot` for the existing streaming connection as
        this is the only additional check required.

        If WAL-specific connection information *is* defined then we must verify that
        streaming is possible using that connection information *as well as* check
        the replication slot. This check will therefore:
          1. Create these connections.
          2. Fetch the remote status of these connections.
          3. Pass the remote status information to :meth:`_check_wal_streaming_preflight`
             which will verify that the status information returned by these connections
             indicates they are suitable for WAL streaming.
          4. Pass the remote status information to :meth:`_check_replication_slot`
             so that the status of the replication slot can be verified.

        :param CheckStrategy check_strategy: The strategy for the management
            of the result of this check
        """
        # If we have wal-specific conninfo then we must use those to get
        # the remote status information for the check
        streaming_conninfo, conninfo = self.config.get_wal_conninfo()
        if conninfo != self.config.conninfo:
            with closing(StreamingConnection(streaming_conninfo)) as streaming, closing(
                PostgreSQLConnection(conninfo, slot_name=self.config.slot_name)
            ) as postgres:
                remote_status = postgres.get_remote_status()
                remote_status.update(streaming.get_remote_status())
                self._check_wal_streaming_preflight(check_strategy, remote_status)
                self._check_replication_slot(
                    check_strategy, remote_status, "WAL streaming"
                )
        else:
            # Use the status for the existing postgres connections
            remote_status = self.get_remote_status()
            self._check_replication_slot(check_strategy, remote_status)

    def _check_wal_streaming_preflight(self, check_strategy, remote_status):
        """
        Verify the supplied remote_status indicates WAL streaming is possible.

        Uses the remote status information to run the
        :meth:`_check_streaming_supported`, :meth:`_check_wal_level` and
        :meth:`check_identity` checks in order to verify that the connections
        can be used for WAL streaming. Also runs an additional
        :meth:`_has_monitoring_privileges` check, which validates the WAL-specific
        conninfo connects with a user than can read monitoring information.

        :param CheckStrategy check_strategy: The strategy for the management
            of the result of this check
        :param dict[str, None|str] remote_status: Remote status information used
            by this check
        """
        self._check_has_monitoring_privileges(
            check_strategy, remote_status, "WAL streaming"
        )
        self._check_streaming_supported(check_strategy, remote_status, "WAL streaming")
        self._check_wal_level(check_strategy, remote_status, "WAL streaming")
        self.check_identity(check_strategy, remote_status, "WAL streaming")

    def _check_replication_slot(self, check_strategy, remote_status, suffix=None):
        """
        Check the replication slot used for WAL streaming.

        If ``streaming_archiver`` is enabled, checks that the replication slot specified
        in the configuration exists, is initialised and is active.

        If ``streaming_archiver`` is disabled, checks that the replication slot does not
        exist.

        :param CheckStrategy check_strategy: The strategy for the management
            of the result of this check
        :param dict[str, None|str] remote_status: Remote status information used
            by this check
        :param str|None suffix: A suffix to be appended to the check name
        """
        # Check the presence and the status of the configured replication slot
        # This check will be skipped if `slot_name` is undefined
        if self.config.slot_name:
            check_name = "replication slot" + ("" if suffix is None else f" ({suffix})")
            check_strategy.init_check(check_name)
            slot = remote_status["replication_slot"]
            # The streaming_archiver is enabled
            if self.config.streaming_archiver is True:
                # Replication slots are supported
                # The slot is not present
                if slot is None:
                    check_strategy.result(
                        self.config.name,
                        False,
                        hint="replication slot '%s' doesn't exist. "
                        "Please execute 'barman receive-wal "
                        "--create-slot %s'" % (self.config.slot_name, self.config.name),
                    )
                else:
                    # The slot is present but not initialised
                    if slot.restart_lsn is None:
                        check_strategy.result(
                            self.config.name,
                            False,
                            hint="slot '%s' not initialised: is "
                            "'receive-wal' running?" % self.config.slot_name,
                        )
                    # The slot is present but not active
                    elif slot.active is False:
                        check_strategy.result(
                            self.config.name,
                            False,
                            hint="slot '%s' not active: is "
                            "'receive-wal' running?" % self.config.slot_name,
                        )
                    else:
                        check_strategy.result(self.config.name, True)
            else:
                # If the streaming_archiver is disabled and the slot_name
                # option is present in the configuration, we check that
                # a replication slot with the specified name is NOT present
                # and NOT active.
                # NOTE: This is not a failure, just a warning.
                if slot is not None:
                    if slot.restart_lsn is not None:
                        slot_status = "initialised"

                        # Check if the slot is also active
                        if slot.active:
                            slot_status = "active"

                        # Warn the user
                        check_strategy.result(
                            self.config.name,
                            True,
                            hint="WARNING: slot '%s' is %s but not required "
                            "by the current config"
                            % (self.config.slot_name, slot_status),
                        )

    def _check_standby(self, check_strategy):
        """
        Perform checks specific to a primary/standby configuration.

        :param CheckStrategy check_strategy: The strategy for the management
            of the results of the various checks.
        """
        is_standby_conn = isinstance(self.postgres, StandbyPostgreSQLConnection)

        # Check that standby is standby
        check_strategy.init_check("PostgreSQL server is standby")
        # The server only is in recovery if we have a standby connection and pg_is_in_recovery() is True
        is_in_recovery = is_standby_conn and self.postgres.is_in_recovery
        if is_in_recovery:
            check_strategy.result(self.config.name, True)
        else:
            check_strategy.result(
                self.config.name,
                False,
                hint=(
                    "conninfo should point to a standby server if "
                    "primary_conninfo is set"
                ),
            )

        # if we don't have a standby connection object then we can't perform
        # any of the further checks as they require a primary reference
        if not is_standby_conn:
            return

        # Check that primary is not standby
        check_strategy.init_check("Primary server is not a standby")
        primary_is_in_recovery = self.postgres.primary.is_in_recovery
        if not primary_is_in_recovery:
            check_strategy.result(self.config.name, True)
        else:
            check_strategy.result(
                self.config.name,
                False,
                hint=(
                    "primary_conninfo should point to a primary server, "
                    "not a standby"
                ),
            )

        # Check that system ID is the same for both
        check_strategy.init_check("Primary and standby have same system ID")
        standby_id = self.postgres.get_systemid()
        primary_id = self.postgres.primary.get_systemid()
        if standby_id == primary_id:
            check_strategy.result(self.config.name, True)
        else:
            check_strategy.result(
                self.config.name,
                False,
                hint=(
                    "primary_conninfo and conninfo should point to primary and "
                    "standby servers which share the same system identifier"
                ),
            )

    def _make_directories(self):
        """
        Make backup directories in case they do not exist
        """
        for key in self.config.KEYS:
            if key.endswith("_directory") and hasattr(self.config, key):
                val = getattr(self.config, key)
                if val is not None and not os.path.isdir(val):
                    # noinspection PyTypeChecker
                    os.makedirs(val)

    def check_directories(self, check_strategy):
        """
        Checks backup directories and creates them if they do not exist

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        check_strategy.init_check("directories")
        if not self.config.disabled:
            try:
                self._make_directories()
            except OSError as e:
                check_strategy.result(
                    self.config.name, False, "%s: %s" % (e.filename, e.strerror)
                )
            else:
                check_strategy.result(self.config.name, True)

    def check_configuration(self, check_strategy):
        """
        Check for error messages in the message list
        of the server and output eventual errors

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        check_strategy.init_check("configuration")
        if len(self.config.msg_list):
            check_strategy.result(self.config.name, False)
            for conflict_paths in self.config.msg_list:
                output.info("\t\t%s" % conflict_paths)

    def check_retention_policy_settings(self, check_strategy):
        """
        Checks retention policy setting

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        check_strategy.init_check("retention policy settings")
        config = self.config
        if config.retention_policy and not self.enforce_retention_policies:
            check_strategy.result(self.config.name, False, hint="see log")
        else:
            check_strategy.result(self.config.name, True)

    def check_backup_validity(self, check_strategy):
        """
        Check if backup validity requirements are satisfied

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        check_strategy.init_check("backup maximum age")
        # first check: check backup maximum age
        if self.config.last_backup_maximum_age is not None:
            # get maximum age information
            backup_age = self.backup_manager.validate_last_backup_maximum_age(
                self.config.last_backup_maximum_age
            )

            # format the output
            check_strategy.result(
                self.config.name,
                backup_age[0],
                hint="interval provided: %s, latest backup age: %s"
                % (
                    human_readable_timedelta(self.config.last_backup_maximum_age),
                    backup_age[1],
                ),
            )
        else:
            # last_backup_maximum_age provided by the user
            check_strategy.result(
                self.config.name, True, hint="no last_backup_maximum_age provided"
            )

        # second check: check backup minimum size
        check_strategy.init_check("backup minimum size")
        if self.config.last_backup_minimum_size is not None:
            backup_size = self.backup_manager.validate_last_backup_min_size(
                self.config.last_backup_minimum_size
            )
            gtlt = ">" if backup_size[0] else "<"
            check_strategy.result(
                self.config.name,
                backup_size[0],
                hint="last backup size %s %s %s minimum"
                % (
                    pretty_size(backup_size[1]),
                    gtlt,
                    pretty_size(self.config.last_backup_minimum_size),
                ),
                perfdata=backup_size[1],
            )
        else:
            # no last_backup_minimum_size provided by the user
            backup_size = self.backup_manager.validate_last_backup_min_size(0)
            check_strategy.result(
                self.config.name,
                True,
                hint=pretty_size(backup_size[1]),
                perfdata=backup_size[1],
            )

    def _check_wal_info(self, wal_info, last_wal_maximum_age):
        """
        Checks the supplied wal_info is within the last_wal_maximum_age.

        :param last_backup_minimum_age: timedelta representing the time from now
            during which a WAL is considered valid
        :return tuple: a tuple containing the boolean result of the check, a string
            with auxiliary information about the check, and an integer representing
            the size of the WAL in bytes
        """
        wal_last = datetime.datetime.fromtimestamp(
            wal_info["wal_last_timestamp"], dateutil.tz.tzlocal()
        )
        now = datetime.datetime.now(dateutil.tz.tzlocal())
        wal_age = now - wal_last
        if wal_age <= last_wal_maximum_age:
            wal_age_isok = True
        else:
            wal_age_isok = False
        wal_message = "interval provided: %s, latest wal age: %s" % (
            human_readable_timedelta(last_wal_maximum_age),
            human_readable_timedelta(wal_age),
        )
        if wal_info["wal_until_next_size"] is None:
            wal_size = 0
        else:
            wal_size = wal_info["wal_until_next_size"]
        return wal_age_isok, wal_message, wal_size

    def check_encryption(self, check_strategy):
        """
        Check if the configured encryption works.

        It attempts to encrypt a simple text file to assert that encryption works.

        :param CheckStrategy check_strategy: The strategy for the management
            of the results.
        """
        if not self.config.encryption:
            return

        check_strategy.init_check("encryption")
        try:
            self.backup_manager.encryption_manager.validate_config()
        except ValueError as ex:
            check_strategy.result(self.config.name, False, hint=force_str(ex))
            return

        encryption = self.backup_manager.encryption_manager.get_encryption()

        with tempfile.NamedTemporaryFile("w+", prefix="barman-encrypt-test-") as file:
            file.write("I am a secret message. Encrypt me!")
            try:
                dest_dir = os.path.dirname(file.name)
                encrypted_file = encryption.encrypt(file.name, dest_dir)
            except CommandFailedException as ex:
                output.debug("encryption test failed: %s" % force_str(ex))
                check_strategy.result(
                    self.config.name,
                    False,
                    hint="encryption test failed. Check the log file for more details",
                )
                return
            else:
                os.unlink(encrypted_file)

        check_strategy.result(self.config.name, True, hint="encryption test succeeded")

    def check_wal_validity(self, check_strategy):
        """
        Check if wal archiving requirements are satisfied
        """
        check_strategy.init_check("wal maximum age")
        backup_id = self.backup_manager.get_last_backup_id()
        backup_info = self.get_backup(backup_id)
        if backup_info is not None:
            wal_info = self.get_wal_info(backup_info)
        # first check: check wal maximum age
        if self.config.last_wal_maximum_age is not None:
            # get maximum age information
            if backup_info is None or wal_info["wal_last_timestamp"] is None:
                # No WAL files received
                # (we should have the .backup file, as a minimum)
                # This may also be an indication that 'barman cron' is not
                # running
                wal_age_isok = False
                wal_message = "No WAL files archived for last backup"
                wal_size = 0
            else:
                wal_age_isok, wal_message, wal_size = self._check_wal_info(
                    wal_info, self.config.last_wal_maximum_age
                )
            # format the output
            check_strategy.result(self.config.name, wal_age_isok, hint=wal_message)
        else:
            # no last_wal_maximum_age provided by the user
            if backup_info is None or wal_info["wal_until_next_size"] is None:
                wal_size = 0
            else:
                wal_size = wal_info["wal_until_next_size"]
            check_strategy.result(
                self.config.name, True, hint="no last_wal_maximum_age provided"
            )

        check_strategy.init_check("wal size")
        check_strategy.result(
            self.config.name, True, pretty_size(wal_size), perfdata=wal_size
        )

    def check_archiver_errors(self, check_strategy):
        """
        Checks the presence of archiving errors

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the check
        """
        check_strategy.init_check("archiver errors")
        if os.path.isdir(self.config.errors_directory):
            errors = os.listdir(self.config.errors_directory)
        else:
            errors = []

        check_strategy.result(
            self.config.name,
            len(errors) == 0,
            hint=WalArchiver.summarise_error_files(errors),
        )

    def check_identity(self, check_strategy, remote_status=None, suffix=None):
        """
        Check the systemid retrieved from the streaming connection
        is the same that is retrieved from the standard connection,
        and then verifies it matches the one stored on disk.

        :param CheckStrategy check_strategy: The strategy for the management
            of the result of this check
        :param dict[str, None|str] remote_status: Remote status information used
            by this check
        :param str|None suffix: A suffix to be appended to the check name
        """
        check_name = "systemid coherence" + ("" if suffix is None else f" ({suffix})")
        check_strategy.init_check(check_name)

        if remote_status is None:
            remote_status = self.get_remote_status()

        # Get system identifier from streaming and standard connections
        systemid_from_streaming = remote_status.get("streaming_systemid")
        systemid_from_postgres = remote_status.get("postgres_systemid")
        # If both available, makes sure they are coherent with each other
        if systemid_from_streaming and systemid_from_postgres:
            if systemid_from_streaming != systemid_from_postgres:
                check_strategy.result(
                    self.config.name,
                    systemid_from_streaming == systemid_from_postgres,
                    hint="is the streaming DSN targeting the same server "
                    "of the PostgreSQL connection string?",
                )
                return

        systemid_from_server = systemid_from_streaming or systemid_from_postgres
        if not systemid_from_server:
            # Can't check without system Id information
            check_strategy.result(self.config.name, True, hint="no system Id available")
            return

        # Retrieves the content on disk and matches it with the live ID
        file_path = self.get_identity_file_path()
        if not os.path.exists(file_path):
            # We still don't have the systemid cached on disk,
            # so let's wait until we store it
            check_strategy.result(
                self.config.name, True, hint="no system Id stored on disk"
            )
            return

        identity_from_file = self.read_identity_file()
        if systemid_from_server != identity_from_file.get("systemid"):
            check_strategy.result(
                self.config.name,
                False,
                hint="the system Id of the connected PostgreSQL server "
                'changed, stored in "%s"' % file_path,
            )
        else:
            check_strategy.result(self.config.name, True)

    def status_postgres(self):
        """
        Status of PostgreSQL server
        """
        remote_status = self.get_remote_status()
        if remote_status["server_txt_version"]:
            output.result(
                "status",
                self.config.name,
                "pg_version",
                "PostgreSQL version",
                remote_status["server_txt_version"],
            )
        else:
            output.result(
                "status",
                self.config.name,
                "pg_version",
                "PostgreSQL version",
                "FAILED trying to get PostgreSQL version",
            )
            return
        # Define the cluster state as pg_controldata do.
        if remote_status["is_in_recovery"]:
            output.result(
                "status",
                self.config.name,
                "is_in_recovery",
                "Cluster state",
                "in archive recovery",
            )
        else:
            output.result(
                "status",
                self.config.name,
                "is_in_recovery",
                "Cluster state",
                "in production",
            )
        if remote_status.get("current_size") is not None:
            output.result(
                "status",
                self.config.name,
                "current_size",
                "Current data size",
                pretty_size(remote_status["current_size"]),
            )
        if remote_status["data_directory"]:
            output.result(
                "status",
                self.config.name,
                "data_directory",
                "PostgreSQL Data directory",
                remote_status["data_directory"],
            )
        if remote_status["current_xlog"]:
            output.result(
                "status",
                self.config.name,
                "current_xlog",
                "Current WAL segment",
                remote_status["current_xlog"],
            )

    def status_wal_archiver(self):
        """
        Status of WAL archiver(s)
        """
        for archiver in self.archivers:
            archiver.status()

    def status_retention_policies(self):
        """
        Status of retention policies enforcement
        """
        if self.enforce_retention_policies:
            output.result(
                "status",
                self.config.name,
                "retention_policies",
                "Retention policies",
                "enforced "
                "(mode: %s, retention: %s, WAL retention: %s)"
                % (
                    self.config.retention_policy_mode,
                    self.config.retention_policy,
                    self.config.wal_retention_policy,
                ),
            )
        else:
            output.result(
                "status",
                self.config.name,
                "retention_policies",
                "Retention policies",
                "not enforced",
            )

    def status(self):
        """
        Implements the 'server-status' command.
        """
        if self.config.description:
            output.result(
                "status",
                self.config.name,
                "description",
                "Description",
                self.config.description,
            )
        output.result(
            "status", self.config.name, "active", "Active", self.config.active
        )
        output.result(
            "status", self.config.name, "disabled", "Disabled", self.config.disabled
        )

        # Show active configuration model information
        active_model = (
            self.config.active_model.name if self.config.active_model else None
        )

        output.result(
            "status",
            self.config.name,
            "active_model",
            "Active configuration model",
            active_model,
        )

        # Postgres status is available only if node is not passive
        if not self.passive_node:
            self.status_postgres()
            self.status_wal_archiver()

        output.result(
            "status",
            self.config.name,
            "passive_node",
            "Passive node",
            self.passive_node,
        )

        self.status_retention_policies()
        # Executes the backup manager status info method
        self.backup_manager.status()

    def fetch_remote_status(self):
        """
        Get the status of the remote server

        This method does not raise any exception in case of errors,
        but set the missing values to None in the resulting dictionary.

        :rtype: dict[str, None|str]
        """
        result = {}
        # Merge status for a postgres connection
        if self.postgres:
            result.update(self.postgres.get_remote_status())
        # Merge status for a streaming connection
        if self.streaming:
            result.update(self.streaming.get_remote_status())
        # Merge status for each archiver
        for archiver in self.archivers:
            result.update(archiver.get_remote_status())
        # Merge status defined by the BackupManager
        result.update(self.backup_manager.get_remote_status())
        return result

    def show(self):
        """
        Shows the server configuration
        """
        # Populate result map with all the required keys
        result = self.config.to_json()
        # Is the server a passive node?
        result["passive_node"] = self.passive_node
        # Skip remote status if the server is passive
        if not self.passive_node:
            remote_status = self.get_remote_status()
            result.update(remote_status)
        # Backup maximum age section
        if self.config.last_backup_maximum_age is not None:
            age = self.backup_manager.validate_last_backup_maximum_age(
                self.config.last_backup_maximum_age
            )
            # If latest backup is between the limits of the
            # last_backup_maximum_age configuration, display how old is
            # the latest backup.
            if age[0]:
                msg = "%s (latest backup: %s )" % (
                    human_readable_timedelta(self.config.last_backup_maximum_age),
                    age[1],
                )
            else:
                # If latest backup is outside the limits of the
                # last_backup_maximum_age configuration (or the configuration
                # value is none), warn the user.
                msg = "%s (WARNING! latest backup is %s old)" % (
                    human_readable_timedelta(self.config.last_backup_maximum_age),
                    age[1],
                )
            result["last_backup_maximum_age"] = msg
        else:
            result["last_backup_maximum_age"] = "None"
        # Add active model information
        result["active_model"] = (
            self.config.active_model.name if self.config.active_model else None
        )
        output.result("show_server", self.config.name, result)

    def delete_backup(self, backup):
        """
        Deletes a backup.

        Performs some checks to confirm that the backup can indeed be deleted
        and if so it is deleted along with all backups that depend on it, if any.

        :param barman.infofile.LocalBackupInfo backup: the backup to delete
        :return bool: True if deleted, False if could not delete the backup
        """
        if self.backup_manager.should_keep_backup(backup.backup_id):
            output.warning(
                "Skipping delete of backup %s for server %s "
                "as it has a current keep request. If you really "
                "want to delete this backup please remove the keep "
                "and try again.",
                backup.backup_id,
                self.config.name,
            )
            return False

        # Honour minimum required redundancy, considering backups that are not
        # incremental.
        available_backups = self.get_available_backups(
            status_filter=(BackupInfo.DONE,),
            backup_type_filter=(BackupInfo.NOT_INCREMENTAL),
        )
        minimum_redundancy = self.config.minimum_redundancy
        # If the backup is incremental, skip check for minimum redundancy and delete.
        if (
            backup.status == BackupInfo.DONE
            and not backup.is_incremental
            and minimum_redundancy >= len(available_backups)
        ):
            output.warning(
                "Skipping delete of backup %s for server %s "
                "due to minimum redundancy requirements "
                "(minimum redundancy = %s, "
                "current redundancy = %s)",
                backup.backup_id,
                self.config.name,
                minimum_redundancy,
                len(available_backups),
            )
            return False

        if backup.children_backup_ids:
            output.warning(
                "Backup %s has incremental backups which depend on it. "
                "Deleting all backups in the tree",
                backup.backup_id,
            )

        try:
            # Lock acquisition: if you can acquire a ServerBackupLock it means
            # that no other processes like a backup or another delete is running
            with ServerBackupLock(self.config.barman_lock_directory, self.config.name):
                # Delete the backup along with all its descendants in the
                # backup tree i.e. all its subsequent incremental backups.
                # If it has no descendants or it is an rsync backup then
                # only the current backup is deleted.
                deleted = False
                backups_to_delete = backup.walk_backups_tree()
                for del_backup in backups_to_delete:
                    deleted = self.perform_delete_backup(del_backup)
                    if not deleted and del_backup.backup_id != backup.backup_id:
                        output.error(
                            "Failed to delete one of its incremental backups. Make sure "
                            "all its dependent backups are deletable and try again."
                        )
                        break

                return deleted

        except LockFileBusy:
            # Otherwise if the lockfile is busy, a backup process is actually running
            output.error(
                "Another process in running on server %s. "
                "Impossible to delete the backup." % self.config.name
            )
            return False

        except LockFilePermissionDenied as e:
            # We cannot access the lockfile.
            # Exit without removing the backup.
            output.error("Permission denied, unable to access '%s'" % e)
            return False

    def perform_delete_backup(self, backup):
        """
        Performs the deletion of a backup.

        Deletes a single backup, ensuring that no other process can access
        the backup simultaneously during its deletion.

        :param barman.infofile.LocalBackupInfo backup: the backup to delete
        :return bool: True if deleted, False if could not delete the backup
        """
        try:
            # Take care of the backup lock.
            # Only one process can modify a backup at a time
            lock = ServerBackupIdLock(
                self.config.barman_lock_directory, self.config.name, backup.backup_id
            )
            with lock:
                deleted = self.backup_manager.delete_backup(backup)

            # At this point no-one should try locking a backup that
            # doesn't exists, so we can remove the lock
            # WARNING: the previous statement is true only as long as
            # no-one wait on this lock
            if deleted:
                os.remove(lock.filename)

            return deleted

        except LockFileBusy:
            # If another process is holding the backup lock,
            # warn the user and terminate
            output.error(
                "Another process is holding the lock for "
                "backup %s of server %s." % (backup.backup_id, self.config.name)
            )
            return False

        except LockFilePermissionDenied as e:
            # We cannot access the lockfile.
            # warn the user and terminate
            output.error("Permission denied, unable to access '%s'" % e)
            return False

    def backup(self, wait=False, wait_timeout=None, backup_name=None, **kwargs):
        """
        Performs a backup for the server
        :param bool wait: wait for all the required WAL files to be archived
        :param int|None wait_timeout: the time, in seconds, the backup
            will wait for the required WAL files to be archived
            before timing out
        :param str|None backup_name: a friendly name by which this backup can
            be referenced in the future
        :kwparam str parent_backup_id: id of the parent backup when taking a
            Postgres incremental backup
        """
        # The 'backup' command is not available on a passive node.
        # We assume that if we get here the node is not passive
        assert not self.passive_node

        try:
            # validate arguments, raise BackupException if any error is found
            self.backup_manager.validate_backup_args(**kwargs)
            # Default strategy for check in backup is CheckStrategy
            # This strategy does not print any output - it only logs checks
            strategy = CheckStrategy()
            self.check(strategy)
            if strategy.has_error:
                output.error(
                    "Impossible to start the backup. Check the log "
                    "for more details, or run 'barman check %s'" % self.config.name
                )
                return
            # check required backup directories exist
            self._make_directories()
        except BackupException as e:
            output.error("failed to start backup: %s", force_str(e))
            return
        except OSError as e:
            output.error("failed to create %s directory: %s", e.filename, e.strerror)
            return

        # Save the database identity
        self.write_identity_file()

        # Make sure we are not wasting an precious streaming PostgreSQL
        # connection that may have been opened by the self.check() call
        if self.streaming:
            self.streaming.close()

        try:
            # lock acquisition and backup execution
            with ServerBackupLock(self.config.barman_lock_directory, self.config.name):
                backup_info = self.backup_manager.backup(
                    wait=wait,
                    wait_timeout=wait_timeout,
                    name=backup_name,
                    **kwargs,
                )

            # Archive incoming WALs and update WAL catalogue
            self.archive_wal(verbose=False)

            # Invoke sanity check of the backup
            if backup_info.status == BackupInfo.WAITING_FOR_WALS:
                self.check_backup(backup_info)

            # At this point is safe to remove any remaining WAL file before the
            # first backup. The only exception is when worm_mode is enabled, in
            # which case the storage is expected to be immutable and out of the
            # grace period, so we skip that.
            previous_backup = self.get_previous_backup(backup_info.backup_id)
            if not previous_backup and self.config.worm_mode is False:
                self.backup_manager.remove_wal_before_backup(backup_info)

            # check if the backup chain (in case it is a Postgres incremental) is consistent
            # with their checksums configurations
            if not backup_info.is_checksum_consistent():
                output.warning(
                    "This is an incremental backup taken with `data_checksums = on` whereas "
                    "some previous backups in the chain were taken with `data_checksums = off`. "
                    "This can lead to potential recovery issues. Consider taking a new full backup "
                    "to avoid having inconsistent backup chains."
                )

            if backup_info.status == BackupInfo.WAITING_FOR_WALS:
                output.warning(
                    "IMPORTANT: this backup is classified as "
                    "WAITING_FOR_WALS, meaning that Barman has not received "
                    "yet all the required WAL files for the backup "
                    "consistency.\n"
                    "This is a common behaviour in concurrent backup "
                    "scenarios, and Barman automatically set the backup as "
                    "DONE once all the required WAL files have been "
                    "archived.\n"
                    "Hint: execute the backup command with '--wait'"
                )
        except LockFileBusy:
            output.error("Another backup process is running")

        except LockFilePermissionDenied as e:
            output.error("Permission denied, unable to access '%s'" % e)

    def get_available_backups(
        self,
        status_filter=BackupManager.DEFAULT_STATUS_FILTER,
        backup_type_filter=BackupManager.DEFAULT_BACKUP_TYPE_FILTER,
    ):
        """
        Get a list of available backups

        param: status_filter: the status of backups to return,
            default to BackupManager.DEFAULT_STATUS_FILTER
        """
        return self.backup_manager.get_available_backups(
            status_filter, backup_type_filter
        )

    def get_last_backup_id(self, status_filter=BackupManager.DEFAULT_STATUS_FILTER):
        """
        Get the id of the latest/last backup in the catalog (if exists)

        :param status_filter: The status of the backup to return,
            default to :attr:`BackupManager.DEFAULT_STATUS_FILTER`.
        :return str|None: ID of the backup
        """
        return self.backup_manager.get_last_backup_id(status_filter)

    def get_last_full_backup_id(
        self, status_filter=BackupManager.DEFAULT_STATUS_FILTER
    ):
        """
        Get the id of the latest/last FULL backup in the catalog (if exists)

        :param status_filter: The status of the backup to return,
            default to DEFAULT_STATUS_FILTER.
        :return string|None: ID of the backup
        """
        return self.backup_manager.get_last_full_backup_id(status_filter)

    def get_first_backup_id(self, status_filter=BackupManager.DEFAULT_STATUS_FILTER):
        """
        Get the id of the oldest/first backup in the catalog (if exists)

        :param status_filter: The status of the backup to return,
            default to DEFAULT_STATUS_FILTER.
        :return string|None: ID of the backup
        """
        return self.backup_manager.get_first_backup_id(status_filter)

    def get_backup_id_from_name(
        self, backup_name, status_filter=BackupManager.DEFAULT_STATUS_FILTER
    ):
        """
        Get the id of the named backup, if it exists.

        :param string backup_name: The name of the backup for which an ID should be
            returned
        :param tuple status_filter: The status of the backup to return.
        :return string|None: ID of the backup
        """
        # Iterate through backups and see if there is one which matches the name
        return self.backup_manager.get_backup_id_from_name(backup_name, status_filter)

    def get_closest_backup_id_from_target_lsn(
        self,
        target_lsn,
        target_tli,
        status_filter=BackupManager.DEFAULT_STATUS_FILTER,
    ):
        """
        Get the id of a backup according to the *target_lsn* and *target_tli*.

        :param str target_lsn: The target value with lsn format, e.g.,
            ``3/64000000``.
        :param int|None target_tli: The target timeline, if a specific one is required.
        :param tuple[str, ...] status_filter: The status of the backup to return.
        :return str|None: ID of the backup.
        """
        return self.backup_manager.get_closest_backup_id_from_target_lsn(
            target_lsn, target_tli, status_filter
        )

    def get_closest_backup_id_from_target_time(
        self,
        target_time,
        target_tli,
        status_filter=BackupManager.DEFAULT_STATUS_FILTER,
    ):
        """
        Get the id of a backup according to the *target_time* and *target_tli*, if
        it exists.

        :param str target_time: The target value with timestamp format
            ``%Y-%m-%d %H:%M:%S`` with or without timezone.
        :param int|None target_tli: The target timeline, if a specific one is required.
        :param tuple[str, ...] status_filter: The status of the backup to return.
        :return str|None: ID of the backup.
        """
        return self.backup_manager.get_closest_backup_id_from_target_time(
            target_time, target_tli, status_filter
        )

    def get_last_backup_id_from_target_tli(
        self,
        target_tli,
        status_filter=BackupManager.DEFAULT_STATUS_FILTER,
    ):
        """
        Get the id of a backup according to the *target_tli*.

        :param int target_tli: The recovery target timeline.
        :param tuple[str, ...] status_filter: The status of the backup to return.
        :return str|None: ID of the backup.
        """
        return self.backup_manager.get_last_backup_id_from_target_tli(
            target_tli, status_filter
        )

    def list_backups(self):
        """
        Lists all the available backups for the server
        """
        retention_status = self.report_backups()
        backups = self.get_available_backups(BackupInfo.STATUS_ALL)
        for key in sorted(backups.keys(), reverse=True):
            backup = backups[key]

            backup_size = backup.size or 0
            wal_size = 0
            rstatus = None
            if backup.status in BackupInfo.STATUS_COPY_DONE:
                try:
                    wal_info = self.get_wal_info(backup)
                    backup_size += wal_info["wal_size"]
                    wal_size = wal_info["wal_until_next_size"]
                except BadXlogSegmentName as e:
                    output.error(
                        "invalid WAL segment name %r\n"
                        'HINT: Please run "barman rebuild-xlogdb %s" '
                        "to solve this issue",
                        force_str(e),
                        self.config.name,
                    )
                if (
                    self.enforce_retention_policies
                    and retention_status[backup.backup_id] != BackupInfo.VALID
                ):
                    rstatus = retention_status[backup.backup_id]
            output.result("list_backup", backup, backup_size, wal_size, rstatus)

    def get_backup(self, backup_id):
        """
        Return the backup information for the given backup id.

        If the backup_id is None or backup.info file doesn't exists,
        it returns None.

        :param str|None backup_id: the ID of the backup to return
        :rtype: barman.infofile.LocalBackupInfo|None
        """
        return self.backup_manager.get_backup(backup_id)

    def get_previous_backup(self, backup_id):
        """
        Get the previous backup (if any) from the catalog

        :param backup_id: the backup id from which return the previous
        """
        return self.backup_manager.get_previous_backup(backup_id)

    def get_next_backup(self, backup_id):
        """
        Get the next backup (if any) from the catalog

        :param backup_id: the backup id from which return the next
        """
        return self.backup_manager.get_next_backup(backup_id)

    def get_required_xlog_files(
        self,
        backup,
        target_tli=None,
        target_time=None,
        target_xid=None,
        target_lsn=None,
        target_immediate=False,
    ):
        """
        Get the xlog files required for a recovery.

        .. note::
            *target_time* and *target_xid* are ignored by this method. As it can be very
            expensive to parse WAL dumps to identify which WAL files are required to
            honor the specific targets, we simply copy all WAL files up to the
            calculated target timeline, so we make sure recovery will be able to finish
            successfully (assuming the archived WALs honor the specified targets).

            On the other hand, *target_tli*, *target_lsn* and *target_immediate* are
            easier to handle, so we only copy the WALs required to reach the requested
            targets.

        :param BackupInfo backup: a backup object
        :param target_tli : target timeline, either a timeline ID or one of the keywords
            supported by Postgres
        :param target_time: target time, in epoch
        :param target_xid: target transaction ID
        :param target_lsn: target LSN
        :param target_immediate: target that ends recovery as soon as
            consistency is reached. Defaults to ``False``.
        """
        begin = backup.begin_wal
        end = backup.end_wal

        # Calculate the integer value of TLI if a keyword is provided
        calculated_target_tli = parse_target_tli(
            self.backup_manager, target_tli, backup
        )

        # If timeline isn't specified, assume it is the same timeline
        # of the backup
        if not target_tli:
            target_tli, _, _ = xlog.decode_segment_name(end)
            calculated_target_tli = target_tli

        # If a target LSN was specified, get the name of the last WAL file that is
        # required for the recovery process
        if target_lsn:
            target_wal = xlog.location_to_xlogfile_name_offset(
                target_lsn,
                calculated_target_tli,
                backup.xlog_segment_size,
            )["file_name"]

        with self.xlogdb() as fxlogdb:
            for line in fxlogdb:
                wal_info = WalFileInfo.from_xlogdb_line(line)
                # Handle .history files: add all of them to the output,
                # regardless of their age
                if xlog.is_history_file(wal_info.name):
                    yield wal_info
                    continue
                if wal_info.name < begin:
                    continue
                tli, _, _ = xlog.decode_segment_name(wal_info.name)
                if tli > calculated_target_tli:
                    continue
                if wal_info.name > end:
                    if target_immediate:
                        break
                    if target_lsn and wal_info.name > target_wal:
                        break
                    end = wal_info.name
                yield wal_info
            # return all the remaining history files
            for line in fxlogdb:
                wal_info = WalFileInfo.from_xlogdb_line(line)
                if xlog.is_history_file(wal_info.name):
                    yield wal_info

    # TODO: merge with the previous
    def get_wal_until_next_backup(self, backup, include_history=False):
        """
        Get the xlog files between backup and the next

        :param BackupInfo backup: a backup object, the starting point
            to retrieve WALs
        :param bool include_history: option for the inclusion of
            include_history files into the output
        """
        begin = backup.begin_wal
        next_end = None
        if self.get_next_backup(backup.backup_id):
            next_end = self.get_next_backup(backup.backup_id).end_wal
        backup_tli, _, _ = xlog.decode_segment_name(begin)

        with self.xlogdb() as fxlogdb:
            for line in fxlogdb:
                wal_info = WalFileInfo.from_xlogdb_line(line)
                # Handle .history files: add all of them to the output,
                # regardless of their age, if requested (the 'include_history'
                # parameter is True)
                if xlog.is_history_file(wal_info.name):
                    if include_history:
                        yield wal_info
                    continue
                if wal_info.name < begin:
                    continue
                tli, _, _ = xlog.decode_segment_name(wal_info.name)
                if tli > backup_tli:
                    continue
                if not xlog.is_wal_file(wal_info.name):
                    continue
                if next_end and wal_info.name > next_end:
                    break
                yield wal_info

    def get_wal_full_path(self, wal_name):
        """
        Build the full path of a WAL for a server given the name

        :param wal_name: WAL file name
        """
        # Build the path which contains the file
        hash_dir = os.path.join(self.config.wals_directory, xlog.hash_dir(wal_name))
        # Build the WAL file full path
        full_path = os.path.join(hash_dir, wal_name)
        return full_path

    def get_wal_possible_paths(self, wal_name, partial=False):
        """
        Build a list of possible positions of a WAL file

        :param str wal_name: WAL file name
        :param bool partial: add also the '.partial' paths
        """
        paths = list()

        # Path in the archive
        hash_dir = os.path.join(self.config.wals_directory, xlog.hash_dir(wal_name))
        full_path = os.path.join(hash_dir, wal_name)
        paths.append(full_path)

        # Path in incoming directory
        incoming_path = os.path.join(self.config.incoming_wals_directory, wal_name)
        paths.append(incoming_path)

        # Path in streaming directory
        streaming_path = os.path.join(self.config.streaming_wals_directory, wal_name)
        paths.append(streaming_path)

        # If partial files are required check also the '.partial' path
        if partial:
            paths.append(streaming_path + PARTIAL_EXTENSION)
            # Add the streaming_path again to handle races with pg_receivewal
            # completing the WAL file
            paths.append(streaming_path)
            # The following two path are only useful to retrieve the last
            # incomplete segment archived before a promotion.
            paths.append(full_path + PARTIAL_EXTENSION)
            paths.append(incoming_path + PARTIAL_EXTENSION)

        # Append the archive path again, to handle races with the archiver
        paths.append(full_path)

        return paths

    def get_wal_info(self, backup_info):
        """
        Returns information about WALs for the given backup

        :param barman.infofile.LocalBackupInfo backup_info: the target backup
        """
        begin = backup_info.begin_wal
        end = backup_info.end_wal

        # counters
        wal_info = dict.fromkeys(
            (
                "wal_num",
                "wal_size",
                "wal_until_next_num",
                "wal_until_next_size",
                "wal_until_next_compression_ratio",
                "wal_compression_ratio",
            ),
            0,
        )
        # First WAL (always equal to begin_wal) and Last WAL names and ts
        wal_info["wal_first"] = None
        wal_info["wal_first_timestamp"] = None
        wal_info["wal_last"] = None
        wal_info["wal_last_timestamp"] = None
        # WAL rate (default 0.0 per second)
        wal_info["wals_per_second"] = 0.0

        for item in self.get_wal_until_next_backup(backup_info):
            if item.name == begin:
                wal_info["wal_first"] = item.name
                wal_info["wal_first_timestamp"] = item.time
            if item.name <= end:
                wal_info["wal_num"] += 1
                wal_info["wal_size"] += item.size
            else:
                wal_info["wal_until_next_num"] += 1
                wal_info["wal_until_next_size"] += item.size
            wal_info["wal_last"] = item.name
            wal_info["wal_last_timestamp"] = item.time

        # Calculate statistics only for complete backups
        # If the cron is not running for any reason, the required
        # WAL files could be missing
        if wal_info["wal_first"] and wal_info["wal_last"]:
            # Estimate WAL ratio
            # Calculate the difference between the timestamps of
            # the first WAL (begin of backup) and the last WAL
            # associated to the current backup
            wal_last_timestamp = wal_info["wal_last_timestamp"]
            wal_first_timestamp = wal_info["wal_first_timestamp"]
            wal_info["wal_total_seconds"] = wal_last_timestamp - wal_first_timestamp
            if wal_info["wal_total_seconds"] > 0:
                wal_num = wal_info["wal_num"]
                wal_until_next_num = wal_info["wal_until_next_num"]
                wal_total_seconds = wal_info["wal_total_seconds"]
                wal_info["wals_per_second"] = (
                    float(wal_num + wal_until_next_num) / wal_total_seconds
                )

            # evaluation of compression ratio for basebackup WAL files
            wal_info["wal_theoretical_size"] = wal_info["wal_num"] * float(
                backup_info.xlog_segment_size
            )
            try:
                wal_size = wal_info["wal_size"]
                wal_info["wal_compression_ratio"] = 1 - (
                    wal_size / wal_info["wal_theoretical_size"]
                )
            except ZeroDivisionError:
                wal_info["wal_compression_ratio"] = 0.0

            # evaluation of compression ratio of WAL files
            wal_until_next_num = wal_info["wal_until_next_num"]
            wal_info["wal_until_next_theoretical_size"] = wal_until_next_num * float(
                backup_info.xlog_segment_size
            )
            try:
                wal_until_next_size = wal_info["wal_until_next_size"]
                until_next_theoretical_size = wal_info[
                    "wal_until_next_theoretical_size"
                ]
                wal_info["wal_until_next_compression_ratio"] = 1 - (
                    wal_until_next_size / until_next_theoretical_size
                )
            except ZeroDivisionError:
                wal_info["wal_until_next_compression_ratio"] = 0.0

        return wal_info

    def recover(
        self,
        backup_info,
        dest,
        wal_dest=None,
        tablespaces=None,
        remote_command=None,
        **kwargs,
    ):
        """
        Performs a recovery of a backup

        :param barman.infofile.LocalBackupInfo backup_info: the backup
            to recover
        :param str dest: the destination directory
        :param str|None wal_dest: the destination directory for WALs when doing PITR.
            See :meth:`~barman.recovery_executor.RecoveryExecutor._set_pitr_targets`
            for more details.
        :param dict[str,str]|None tablespaces: a tablespace
            name -> location map (for relocation)
        :param str|None remote_command: default None. The remote command to
            recover the base backup, in case of remote backup.
        :kwparam str|None target_tli: the target timeline
        :kwparam str|None target_time: the target time
        :kwparam str|None target_xid: the target xid
        :kwparam str|None target_lsn: the target LSN
        :kwparam str|None target_name: the target name created previously with
                            pg_create_restore_point() function call
        :kwparam bool|None target_immediate: end recovery as soon as
            consistency is reached
        :kwparam bool exclusive: whether the recovery is exclusive or not
        :kwparam str|None target_action: the recovery target action
        :kwparam bool|None standby_mode: the standby mode
        :kwparam str|None recovery_conf_filename: filename for storing recovery
            configurations
        """
        return self.backup_manager.recover(
            backup_info, dest, wal_dest, tablespaces, remote_command, **kwargs
        )

    def get_wal(
        self,
        wal_name,
        compression=None,
        keep_compression=False,
        output_directory=None,
        peek=None,
        partial=False,
    ):
        """
        Retrieve a WAL file from the archive

        :param str wal_name: id of the WAL file to find into the WAL archive
        :param str|None compression: compression format for the output
        :param bool keep_compression: if True, do not uncompress compressed WAL files
        :param str|None output_directory: directory where to deposit the
            WAL file
        :param int|None peek: if defined list the next N WAL file
        :param bool partial: retrieve also partial WAL files
        """

        # If used through SSH identify the client to add it to logs
        source_suffix = ""
        ssh_connection = os.environ.get("SSH_CONNECTION")
        if ssh_connection:
            # The client IP is the first value contained in `SSH_CONNECTION`
            # which contains four space-separated values: client IP address,
            # client port number, server IP address, and server port number.
            source_suffix = " (SSH host: %s)" % (ssh_connection.split()[0],)

        # Sanity check
        if not xlog.is_any_xlog_file(wal_name):
            output.error(
                "'%s' is not a valid wal file name%s",
                wal_name,
                source_suffix,
                exit_code=3,
            )
            return

        # If peek is requested we only output a list of files
        if peek:
            # Get the next ``peek`` files following the provided ``wal_name``.
            # If ``wal_name`` is not a simple wal file,
            # we cannot guess the names of the following WAL files.
            # So ``wal_name`` is the only possible result, if exists.
            if xlog.is_wal_file(wal_name):
                # We can't know what was the segment size of PostgreSQL WAL
                # files at backup time. Because of this, we generate all
                # the possible names for a WAL segment, and then we check
                # if the requested one is included.
                wal_peek_list = xlog.generate_segment_names(wal_name)
            else:
                wal_peek_list = iter([wal_name])

            # Output the content of wal_peek_list until we have displayed
            # enough files or find a missing file
            count = 0
            while count < peek:
                try:
                    wal_peek_name = next(wal_peek_list)
                except StopIteration:
                    # No more item in wal_peek_list
                    break

                # Get list of possible location. We do not prefetch
                # partial files
                wal_peek_paths = self.get_wal_possible_paths(
                    wal_peek_name, partial=False
                )

                # If the next WAL file is found, output the name
                # and continue to the next one
                if any(os.path.exists(path) for path in wal_peek_paths):
                    count += 1
                    output.info(wal_peek_name, log=False)
                    continue

                # If ``wal_peek_file`` doesn't exist, check if we need to
                # look in the following segment
                tli, log, seg = xlog.decode_segment_name(wal_peek_name)

                # If `seg` is not a power of two, it is not possible that we
                # are at the end of a WAL group, so we are done
                if not is_power_of_two(seg):
                    break

                # This is a possible WAL group boundary, let's try the
                # following group
                seg = 0
                log += 1

                # Install a new generator from the start of the next segment.
                # If the file doesn't exists we will terminate because
                # zero is not a power of two
                wal_peek_name = xlog.encode_segment_name(tli, log, seg)
                wal_peek_list = xlog.generate_segment_names(wal_peek_name)

            # Do not output anything else
            return

        # If an output directory was provided write the file inside it
        # otherwise we use standard output
        if output_directory is not None:
            destination_path = os.path.join(output_directory, wal_name)
            destination_description = "into '%s' file" % destination_path
            # Use the standard output for messages
            logger = output
            try:
                destination = open(destination_path, "wb")
            except IOError as e:
                output.error(
                    "Unable to open '%s' file%s: %s",
                    destination_path,
                    source_suffix,
                    e,
                    exit_code=3,
                )
                return
        else:
            destination_description = "to standard output"
            # Do not use the standard output for messages, otherwise we would
            # taint the output stream
            logger = _logger
            try:
                # Python 3.x
                destination = sys.stdout.buffer
            except AttributeError:
                # Python 2.x
                destination = sys.stdout

        # Get the list of WAL file possible paths
        wal_paths = self.get_wal_possible_paths(wal_name, partial)

        for wal_file in wal_paths:
            # Check for file existence
            if not os.path.exists(wal_file):
                continue

            logger.info(
                "Sending WAL '%s' for server '%s' %s%s",
                os.path.basename(wal_file),
                self.config.name,
                destination_description,
                source_suffix,
            )

            try:
                # Try returning the wal_file to the client
                self.get_wal_sendfile(
                    wal_file, compression, keep_compression, destination
                )
                # We are done, return to the caller
                return
            except CommandFailedException:
                # If an external command fails we cannot really know why,
                # but if the WAL file disappeared, we assume
                # it has been moved in the archive so we ignore the error.
                # This file will be retrieved later, as the last entry
                # returned by get_wal_possible_paths() is the archive position
                if not os.path.exists(wal_file):
                    pass
                else:
                    raise
            except OSError as exc:
                # If the WAL file disappeared just ignore the error
                # This file will be retrieved later, as the last entry
                # returned by get_wal_possible_paths() is the archive
                # position
                if exc.errno == errno.ENOENT and exc.filename == wal_file:
                    pass
                else:
                    raise

            logger.info("Skipping vanished WAL file '%s'%s", wal_file, source_suffix)

        output.error(
            "WAL file '%s' not found in server '%s'%s",
            wal_name,
            self.config.name,
            source_suffix,
        )

    def get_wal_sendfile(self, wal_file, compression, keep_compression, destination):
        """
        Send a WAL file to the destination file, using the required compression

        :param str wal_file: WAL file path
        :param str compression: required compression
        :param bool keep_compression: if True, do not uncompress compressed WAL files
        :param destination: file stream to use to write the data
        """
        backup_manager = self.backup_manager
        # Identify the wal file
        wal_info = backup_manager.get_wal_file_info(wal_file)

        # Initially our source is the stored WAL file and we do not have
        # any temporary file.
        source_file = wal_file
        uncompressed_file = None
        compressed_file = None
        tempdir = None

        # Check if it is not a partial file. In this case, the WAL file is still being
        # written by pg_receivewal, and surely has not yet been compressed nor encrypted
        # by the Barman archiver.
        if not xlog.is_partial_file(wal_info.fullpath(self)):
            wal_file_compression = None
            # Before any decompression operation, check for encryption.
            if wal_info.encryption:
                # We need to check if `encryption_passphrase_command` is set.
                if not self.config.encryption_passphrase_command:
                    output.error(
                        "Encrypted WAL file '%s' detected, but no "
                        "'encryption_passphrase_command' is configured. "
                        "Please set 'encryption_passphrase_command' in the configuration "
                        "so the correct private key can be identified for decryption.",
                        wal_info.name,
                    )
                    output.close_and_exit()

                passphrase = get_passphrase_from_command(
                    self.config.encryption_passphrase_command
                )

                encryption_handler = backup_manager.encryption_manager.get_encryption(
                    encryption=wal_info.encryption
                )

                tempdir = tempfile.mkdtemp(
                    dir=self.config.wals_directory,
                    prefix=".%s." % os.path.basename(wal_file),
                )
                # Decrypt wal to a tmp directory.
                decrypted_file = encryption_handler.decrypt(
                    file=source_file, dest=tempdir, passphrase=passphrase
                )
                # Now, check compression info.
                wal_file_compression = (
                    backup_manager.compression_manager.identify_compression(
                        decrypted_file
                    )
                )

                source_file = decrypted_file

            wal_info_compression = wal_info.compression or wal_file_compression
            # Get a decompressor for the file (None if not compressed)
            wal_compressor = backup_manager.compression_manager.get_compressor(
                wal_info_compression
            )

            # Get a compressor for the output (None if not compressed)
            out_compressor = backup_manager.compression_manager.get_compressor(
                compression
            )

            # Ignore compression/decompression when:
            # * It's a partial WAL file; and
            # * The user wants to decompress on the client side.
            if not keep_compression:
                # If the required compression is different from the source we
                # decompress/compress it into the required format (getattr is
                # used here to gracefully handle None objects)
                if getattr(wal_compressor, "compression", None) != getattr(
                    out_compressor, "compression", None
                ):
                    # If source is compressed, decompress it into a temporary file
                    if wal_compressor is not None:
                        uncompressed_file = NamedTemporaryFile(
                            dir=self.config.wals_directory,
                            prefix=".%s." % os.path.basename(wal_file),
                            suffix=".uncompressed",
                        )
                        # If a custom decompression filter is set, we prioritize using it
                        # instead of the compression guessed by Barman based on the magic
                        # number.
                        is_decompressed = False
                        if (
                            self.config.custom_decompression_filter is not None
                            and not isinstance(wal_compressor, CustomCompressor)
                        ):
                            try:
                                backup_manager.compression_manager.get_compressor(
                                    "custom"
                                ).decompress(source_file, uncompressed_file.name)
                            except CommandFailedException as exc:
                                output.debug("Error decompressing WAL: %s", str(exc))
                            else:
                                is_decompressed = True
                        # But if a custom decompression filter is not set, or if using the
                        # custom decompression filter was not successful, then try using
                        # the decompressor identified by the magic number
                        if not is_decompressed:
                            try:
                                wal_compressor.decompress(
                                    source_file, uncompressed_file.name
                                )
                            except CommandFailedException as exc:
                                output.error("Error decompressing WAL: %s", str(exc))
                                return

                        source_file = uncompressed_file.name

                    # If output compression is required compress the source
                    # into a temporary file
                    if out_compressor is not None:
                        compressed_file = NamedTemporaryFile(
                            dir=self.config.wals_directory,
                            prefix=".%s." % os.path.basename(wal_file),
                            suffix=".compressed",
                        )
                        out_compressor.compress(source_file, compressed_file.name)
                        source_file = compressed_file.name

        # Copy the prepared source file to destination
        with open(source_file, "rb") as input_file:
            shutil.copyfileobj(input_file, destination)

        # Remove file
        if tempdir is not None:
            fs.LocalLibPathDeletionCommand(tempdir).delete()
        # Remove temp files
        if uncompressed_file is not None:
            uncompressed_file.close()
        if compressed_file is not None:
            compressed_file.close()

    def put_wal(self, fileobj):
        """
        Receive a WAL file from SERVER_NAME and securely store it in the
        incoming directory.

        The file will be read from the fileobj passed as parameter.
        """

        # If used through SSH identify the client to add it to logs
        source_suffix = ""
        ssh_connection = os.environ.get("SSH_CONNECTION")
        if ssh_connection:
            # The client IP is the first value contained in `SSH_CONNECTION`
            # which contains four space-separated values: client IP address,
            # client port number, server IP address, and server port number.
            source_suffix = " (SSH host: %s)" % (ssh_connection.split()[0],)

        # Incoming directory is where the files will be extracted
        dest_dir = self.config.incoming_wals_directory

        # Ensure the presence of the destination directory
        mkpath(dest_dir)

        incoming_file = namedtuple(
            "incoming_file",
            [
                "name",
                "tmp_path",
                "path",
                "checksum",
            ],
        )

        # Stream read tar from stdin, store content in incoming directory
        # The closing wrapper is needed only for Python 2.6
        extracted_files = {}
        validated_files = {}
        hashsums = {}
        extracted_files_with_checksums = {}
        hash_algorithm = "sha256"
        try:
            with closing(tarfile.open(mode="r|", fileobj=fileobj)) as tar:
                for item in tar:
                    name = item.name
                    # Strip leading './' - tar has been manually created
                    if name.startswith("./"):
                        name = name[2:]
                    # Requires a regular file as tar item
                    if not item.isreg():
                        output.error(
                            "Unsupported file type '%s' for file '%s' "
                            "in put-wal for server '%s'%s",
                            item.type,
                            name,
                            self.config.name,
                            source_suffix,
                        )
                        return
                    # Subdirectories are not supported
                    if "/" in name:
                        output.error(
                            "Unsupported filename '%s' in put-wal for server '%s'%s",
                            name,
                            self.config.name,
                            source_suffix,
                        )
                        return
                    # Checksum file
                    if name in ("MD5SUMS", "SHA256SUMS"):
                        # Parse content and store it in md5sums dictionary
                        for line in tar.extractfile(item).readlines():
                            line = line.decode().rstrip()
                            try:
                                # Split checksums and path info
                                checksum, path = re.split(r" [* ]", line, 1)
                            except ValueError:
                                output.warning(
                                    "Bad checksum line '%s' found "
                                    "in put-wal for server '%s'%s",
                                    line,
                                    self.config.name,
                                    source_suffix,
                                )
                                continue
                            # Strip leading './' from path in the checksum file
                            if path.startswith("./"):
                                path = path[2:]
                            hashsums[path] = checksum
                        if name == "MD5SUMS":
                            hash_algorithm = "md5"
                    else:
                        # Extract using a temp name (with PID)
                        tmp_path = os.path.join(
                            dest_dir, ".%s-%s" % (os.getpid(), name)
                        )
                        path = os.path.join(dest_dir, name)
                        tar.makefile(item, tmp_path)
                        # Set the original timestamp
                        tar.utime(item, tmp_path)
                        # Add the tuple to the dictionary of extracted files
                        extracted_files[name] = dict(
                            name=name,
                            tmp_path=tmp_path,
                            path=path,
                        )
                        validated_files[name] = False

            for name, _dict in extracted_files.items():
                extracted_files_with_checksums[name] = incoming_file(
                    _dict["name"],
                    _dict["tmp_path"],
                    _dict["path"],
                    file_hash(_dict["tmp_path"], hash_algorithm=hash_algorithm),
                )

            # For each received checksum verify the corresponding file
            for name in hashsums:
                # Check that file is present in the tar archive
                if name not in extracted_files_with_checksums:
                    output.error(
                        "Checksum without corresponding file '%s' "
                        "in put-wal for server '%s'%s",
                        name,
                        self.config.name,
                        source_suffix,
                    )
                    return
                # Verify the checksum of the file
                if extracted_files_with_checksums[name].checksum != hashsums[name]:
                    output.error(
                        "Bad file checksum '%s' (should be %s) "
                        "for file '%s' "
                        "in put-wal for server '%s'%s",
                        extracted_files_with_checksums[name].checksum,
                        hashsums[name],
                        name,
                        self.config.name,
                        source_suffix,
                    )
                    return
                _logger.info(
                    "Received file '%s' with checksum '%s' "
                    "by put-wal for server '%s'%s",
                    name,
                    hashsums[name],
                    self.config.name,
                    source_suffix,
                )
                validated_files[name] = True

            # Put the files in the final place, atomically and fsync all
            for item in extracted_files_with_checksums.values():
                # Final verification of checksum presence for each file
                if not validated_files[item.name]:
                    output.error(
                        "Missing checksum for file '%s' "
                        "in put-wal for server '%s'%s",
                        item.name,
                        self.config.name,
                        source_suffix,
                    )
                    return
                # If a file with the same name exists, checksums are compared.
                # If checksums mismatch, an error message is generated, the incoming
                # file is moved to the errors directory.
                # If checksums are identical, a debug message is generated and the file
                # is skipped.
                # In both cases the archiving process will exit with 0, avoiding
                # that WALs pile up on Postgres.
                if os.path.exists(item.path):
                    incoming_dir_file_checksum = file_hash(
                        file_path=item.path, hash_algorithm=hash_algorithm
                    )
                    if item.checksum == incoming_dir_file_checksum:
                        output.debug(
                            "Duplicate Files with Identical Checksums. File %s already "
                            "exists on server %s, and the checksums are identical. "
                            "Skipping the file.",
                            item.name,
                            self.config.name,
                        )
                        continue
                    else:
                        self.move_wal_file_to_errors_directory(
                            item.tmp_path, item.name, "duplicate"
                        )
                        output.info(
                            "\tError: Duplicate Files Detected with Mismatched "
                            "Checksums. File %s already exists on server %s with "
                            "checksum %s, but the checksum of the incoming file is%s. "
                            "The file has been moved to the errors directory.",
                            item.name,
                            self.config.name,
                            incoming_dir_file_checksum,
                            item.checksum,
                        )
                        continue
                os.rename(item.tmp_path, item.path)
                fsync_file(item.path)
            fsync_dir(dest_dir)
        finally:
            # Cleanup of any remaining temp files (where applicable)
            for item in extracted_files_with_checksums.values():
                if os.path.exists(item.tmp_path):
                    os.unlink(item.tmp_path)

    def cron(self, wals=True, retention_policies=True, keep_descriptors=False):
        """
        Maintenance operations

        :param bool wals: WAL archive maintenance
        :param bool retention_policies: retention policy maintenance
        :param bool keep_descriptors: whether to keep subprocess descriptors,
            defaults to False
        """
        try:
            # Actually this is the highest level of locking in the cron,
            # this stops the execution of multiple cron on the same server
            with ServerCronLock(self.config.barman_lock_directory, self.config.name):
                # When passive call sync.cron() and never run
                # local WAL archival
                if self.passive_node:
                    self.sync_cron(keep_descriptors)
                # WAL management and maintenance
                elif wals:
                    # Execute the archive-wal sub-process
                    self.cron_archive_wal(keep_descriptors)
                    if self.config.streaming_archiver:
                        # Spawn the receive-wal sub-process
                        self.background_receive_wal(keep_descriptors)
                    else:
                        # Terminate the receive-wal sub-process if present
                        self.kill("receive-wal", fail_if_not_present=False)

                # Verify backup
                self.cron_check_backup(keep_descriptors)

                # Retention policies execution
                if retention_policies:
                    self.backup_manager.cron_retention_policy()
        except LockFileBusy:
            output.info(
                "Another cron process is already running on server %s. "
                "Skipping to the next server" % self.config.name
            )
        except LockFilePermissionDenied as e:
            output.error("Permission denied, unable to access '%s'" % e)
        except (OSError, IOError) as e:
            output.error("%s", e)

    def cron_archive_wal(self, keep_descriptors):
        """
        Method that handles the start of an 'archive-wal' sub-process.

        This method must be run protected by ServerCronLock
        :param bool keep_descriptors: whether to keep subprocess descriptors
            attached to this process.
        """
        try:
            # Try to acquire ServerWalArchiveLock, if the lock is available,
            # no other 'archive-wal' processes are running on this server.
            #
            # There is a very little race condition window here because
            # even if we are protected by ServerCronLock, the user could run
            # another 'archive-wal' command manually. However, it would result
            # in one of the two commands failing on lock acquisition,
            # with no other consequence.
            with ServerWalArchiveLock(
                self.config.barman_lock_directory, self.config.name
            ):
                # Output and release the lock immediately
                output.info(
                    "Starting WAL archiving for server %s", self.config.name, log=False
                )

            # Init a Barman sub-process object
            archive_process = BarmanSubProcess(
                subcommand="archive-wal",
                config=barman.__config__.config_file,
                args=[self.config.name],
                keep_descriptors=keep_descriptors,
            )
            # Launch the sub-process
            archive_process.execute()

        except LockFileBusy:
            # Another archive process is running for the server,
            # warn the user and skip to the next one.
            output.info(
                "Another archive-wal process is already running "
                "on server %s. Skipping to the next server" % self.config.name
            )

    def background_receive_wal(self, keep_descriptors):
        """
        Method that handles the start of a 'receive-wal' sub process, running in background.

        This method must be run protected by ServerCronLock
        :param bool keep_descriptors: whether to keep subprocess
            descriptors attached to this process.
        """
        try:
            # Try to acquire ServerWalReceiveLock, if the lock is available,
            # no other 'receive-wal' processes are running on this server.
            #
            # There is a very little race condition window here because
            # even if we are protected by ServerCronLock, the user could run
            # another 'receive-wal' command manually. However, it would result
            # in one of the two commands failing on lock acquisition,
            # with no other consequence.
            with ServerWalReceiveLock(
                self.config.barman_lock_directory, self.config.name
            ):
                # Output and release the lock immediately
                output.info(
                    "Starting streaming archiver for server %s",
                    self.config.name,
                    log=False,
                )

            # Start a new receive-wal process
            receive_process = BarmanSubProcess(
                subcommand="receive-wal",
                config=barman.__config__.config_file,
                args=[self.config.name],
                keep_descriptors=keep_descriptors,
            )
            # Launch the sub-process
            receive_process.execute()

        except LockFileBusy:
            # Another receive-wal process is running for the server
            # exit without message
            _logger.debug(
                "Another STREAMING ARCHIVER process is running for "
                "server %s" % self.config.name
            )

    def cron_check_backup(self, keep_descriptors):
        """
        Method that handles the start of a 'check-backup' sub process

        :param bool keep_descriptors: whether to keep subprocess
           descriptors attached to this process.
        """

        backup_id = self.get_first_backup_id([BackupInfo.WAITING_FOR_WALS])
        if not backup_id:
            # Nothing to be done for this server
            return

        try:
            # Try to acquire ServerBackupIdLock, if the lock is available,
            # no other 'check-backup' processes are running on this backup.
            #
            # There is a very little race condition window here because
            # even if we are protected by ServerCronLock, the user could run
            # another command that takes the lock. However, it would result
            # in one of the two commands failing on lock acquisition,
            # with no other consequence.
            with ServerBackupIdLock(
                self.config.barman_lock_directory, self.config.name, backup_id
            ):
                # Output and release the lock immediately
                output.info(
                    "Starting check-backup for backup %s of server %s",
                    backup_id,
                    self.config.name,
                    log=False,
                )

            # Start a check-backup process
            check_process = BarmanSubProcess(
                subcommand="check-backup",
                config=barman.__config__.config_file,
                args=[self.config.name, backup_id],
                keep_descriptors=keep_descriptors,
            )
            check_process.execute()

        except LockFileBusy:
            # Another process is holding the backup lock
            _logger.debug(
                "Another process is holding the backup lock for %s "
                "of server %s" % (backup_id, self.config.name)
            )

    def archive_wal(self, verbose=True):
        """
        Perform the WAL archiving operations.

        Usually run as subprocess of the barman cron command,
        but can be executed manually using the barman archive-wal command

        :param bool verbose: if false outputs something only if there is
            at least one file
        """
        output.debug("Starting archive-wal for server %s", self.config.name)
        try:
            # Take care of the archive lock.
            # Only one archive job per server is admitted
            with ServerWalArchiveLock(
                self.config.barman_lock_directory, self.config.name
            ):
                self.backup_manager.archive_wal(verbose)
        except LockFileBusy:
            # If another process is running for this server,
            # warn the user and skip to the next server
            output.info(
                "Another archive-wal process is already running "
                "on server %s. Skipping to the next server" % self.config.name
            )

    def create_physical_repslot(self):
        """
        Create a physical replication slot using the streaming connection
        """
        if not self.streaming:
            output.error(
                "Unable to create a physical replication slot: "
                "streaming connection not configured"
            )
            return

        # Replication slots are not supported by PostgreSQL < 9.4
        try:
            if self.streaming.server_version < 90400:
                output.error(
                    "Unable to create a physical replication slot: "
                    "not supported by '%s' "
                    "(9.4 or higher is required)" % self.streaming.server_major_version
                )
                return
        except PostgresException as exc:
            msg = "Cannot connect to server '%s'" % self.config.name
            output.error(msg, log=False)
            _logger.error("%s: %s", msg, force_str(exc).strip())
            return

        if not self.config.slot_name:
            output.error(
                "Unable to create a physical replication slot: "
                "slot_name configuration option required"
            )
            return

        output.info(
            "Creating physical replication slot '%s' on server '%s'",
            self.config.slot_name,
            self.config.name,
        )

        try:
            self.streaming.create_physical_repslot(self.config.slot_name)
            output.info("Replication slot '%s' created", self.config.slot_name)
        except PostgresDuplicateReplicationSlot:
            output.error("Replication slot '%s' already exists", self.config.slot_name)
        except PostgresReplicationSlotsFull:
            output.error(
                "All replication slots for server '%s' are in use\n"
                "Free one or increase the max_replication_slots "
                "value on your PostgreSQL server.",
                self.config.name,
            )
        except PostgresException as exc:
            output.error(
                "Cannot create replication slot '%s' on server '%s': %s",
                self.config.slot_name,
                self.config.name,
                force_str(exc).strip(),
            )

    def drop_repslot(self):
        """
        Drop a replication slot using the streaming connection
        """
        if not self.streaming:
            output.error(
                "Unable to drop a physical replication slot: "
                "streaming connection not configured"
            )
            return

        # Replication slots are not supported by PostgreSQL < 9.4
        try:
            if self.streaming.server_version < 90400:
                output.error(
                    "Unable to drop a physical replication slot: "
                    "not supported by '%s' (9.4 or higher is "
                    "required)" % self.streaming.server_major_version
                )
                return
        except PostgresException as exc:
            msg = "Cannot connect to server '%s'" % self.config.name
            output.error(msg, log=False)
            _logger.error("%s: %s", msg, force_str(exc).strip())
            return

        if not self.config.slot_name:
            output.error(
                "Unable to drop a physical replication slot: "
                "slot_name configuration option required"
            )
            return

        output.info(
            "Dropping physical replication slot '%s' on server '%s'",
            self.config.slot_name,
            self.config.name,
        )

        try:
            self.streaming.drop_repslot(self.config.slot_name)
            output.info("Replication slot '%s' dropped", self.config.slot_name)
        except PostgresInvalidReplicationSlot:
            output.error("Replication slot '%s' does not exist", self.config.slot_name)
        except PostgresReplicationSlotInUse:
            output.error(
                "Cannot drop replication slot '%s' on server '%s' "
                "because it is in use.",
                self.config.slot_name,
                self.config.name,
            )
        except PostgresException as exc:
            output.error(
                "Cannot drop replication slot '%s' on server '%s': %s",
                self.config.slot_name,
                self.config.name,
                force_str(exc).strip(),
            )

    def receive_wal(self, reset=False):
        """
        Enable the reception of WAL files using streaming protocol.

        Usually started by barman cron command.
        Executing this manually, the barman process will not terminate but
        will continuously receive WAL files from the PostgreSQL server.

        :param reset: When set, resets the status of receive-wal
        """
        # Execute the receive-wal command only if streaming_archiver
        # is enabled
        if not self.config.streaming_archiver:
            output.error(
                "Unable to start receive-wal process: "
                "streaming_archiver option set to 'off' in "
                "barman configuration file"
            )
            return

        # Use the default CheckStrategy to silently check WAL streaming
        # conditions are met and write errors to the log file.
        strategy = CheckStrategy()
        self._check_wal_streaming_preflight(strategy, self.get_remote_status())
        if strategy.has_error:
            output.error(
                "Impossible to start WAL streaming. Check the log "
                "for more details, or run 'barman check %s'" % self.config.name
            )
            return

        if not reset:
            output.info("Starting receive-wal for server %s", self.config.name)

        try:
            # Take care of the receive-wal lock.
            # Only one receiving process per server is permitted
            with ServerWalReceiveLock(
                self.config.barman_lock_directory, self.config.name
            ):
                try:
                    # Only the StreamingWalArchiver implementation
                    # does something.
                    # WARNING: This codes assumes that there is only one
                    # StreamingWalArchiver in the archivers list.
                    for archiver in self.archivers:
                        archiver.receive_wal(reset)
                except ArchiverFailure as e:
                    output.error(e)

        except LockFileBusy:
            # If another process is running for this server,
            if reset:
                output.error(
                    "Unable to reset the status of receive-wal "
                    "for server %s. Process is still running" % self.config.name
                )
            else:
                output.error(
                    "Another receive-wal process is already running "
                    "for server %s." % self.config.name
                )

    @property
    def meta_directory(self):
        """
        Directory used to store server metadata files.
        """
        return os.path.join(self.config.backup_directory, "meta")

    @property
    def systemid(self):
        """
        Get the system identifier, as returned by the PostgreSQL server
        :return str: the system identifier
        """
        status = self.get_remote_status()
        # Main PostgreSQL connection has higher priority
        if status.get("postgres_systemid"):
            return status.get("postgres_systemid")
        # Fallback: streaming connection
        return status.get("streaming_systemid")

    @property
    def xlogdb_directory(self):
        """
        The base directory where the xlogdb file lives

        :return str: the directory that contains the xlogdb file
        """
        return self.config.xlogdb_directory

    @property
    def xlogdb_file_name(self):
        """
        The name of the xlogdb file.

        :return str: the dynamic name for the xlogdb file
        """
        return self.XLOGDB_NAME.format(server=self.config.name)

    @property
    def xlogdb_file_path(self):
        """
        The path of the xlogdb file

        :return str: the full path of the xlogdb file
        """
        return os.path.join(self.xlogdb_directory, self.xlogdb_file_name)

    @contextmanager
    def xlogdb(self, mode="r"):
        """
        Context manager to access the xlogdb file.

        This method uses locking to make sure only one process is accessing
        the database at a time. The database file will be created
        if it not exists.

        Usage example:

            with server.xlogdb('w') as file:
                file.write(new_line)

        :param str mode: open the file with the required mode
            (default read-only)
        """
        xlogdb = self.xlogdb_file_path

        if not os.path.exists(xlogdb):
            self.rebuild_xlogdb(silent=True)

        with ServerXLOGDBLock(self.config.barman_lock_directory, self.config.name):
            with open(xlogdb, mode) as f:
                # execute the block nested in the with statement
                try:
                    yield f

                finally:
                    # we are exiting the context
                    # if file is writable (mode contains w, a or +)
                    # make sure the data is written to disk
                    # http://docs.python.org/2/library/os.html#os.fsync
                    if any((c in "wa+") for c in f.mode):
                        f.flush()
                        os.fsync(f.fileno())

    def report_backups(self):
        if not self.enforce_retention_policies:
            return dict()
        else:
            return self.config.retention_policy.report()

    def rebuild_xlogdb(self, silent=False):
        """
        Rebuild the whole xlog database guessing it from the archive content.

        :param bool silent: Supress output logs if ``True``.
        """
        from os.path import isdir, join

        if not silent:
            output.info("Rebuilding xlogdb for server %s", self.config.name)

        # create xlogdb directory and xlogdb file if they do not exist yet
        if not os.path.exists(self.xlogdb_file_path):
            if not os.path.exists(self.xlogdb_directory):
                os.makedirs(self.xlogdb_directory)
            open(self.xlogdb_file_path, mode="a").close()

            # the xlogdb file was renamed in Barman 3.13. In case of a recent
            # migration, also attempt to delete the old file to clean up leftovers
            try:
                os.unlink(os.path.join(self.config.wals_directory, "xlog.db"))
            except FileNotFoundError:
                pass

        root = self.config.wals_directory
        wal_count = label_count = history_count = 0
        # lock the xlogdb as we are about replacing it completely
        with self.xlogdb("w") as fxlogdb:
            xlogdb_dir = os.path.dirname(fxlogdb.name)
            with tempfile.TemporaryFile(mode="w+", dir=xlogdb_dir) as fxlogdb_new:
                for name in sorted(os.listdir(root)):
                    # ignore the xlogdb and its lockfile
                    if name.startswith(self.xlogdb_file_name):
                        continue
                    fullname = join(root, name)
                    if isdir(fullname):
                        # all relevant files are in subdirectories
                        hash_dir = fullname
                        for wal_name in sorted(os.listdir(hash_dir)):
                            fullname = join(hash_dir, wal_name)
                            if isdir(fullname):
                                _logger.warning(
                                    "unexpected directory "
                                    "rebuilding the wal database: %s",
                                    fullname,
                                )
                            else:
                                if xlog.is_wal_file(fullname):
                                    wal_count += 1
                                elif xlog.is_backup_file(fullname):
                                    label_count += 1
                                elif fullname.endswith(".tmp"):
                                    _logger.warning(
                                        "temporary file found "
                                        "rebuilding the wal database: %s",
                                        fullname,
                                    )
                                    continue
                                else:
                                    _logger.warning(
                                        "unexpected file "
                                        "rebuilding the wal database: %s",
                                        fullname,
                                    )
                                    continue
                                wal_info = self.backup_manager.get_wal_file_info(
                                    fullname
                                )
                                fxlogdb_new.write(wal_info.to_xlogdb_line())
                    else:
                        # only history files are here
                        if xlog.is_history_file(fullname):
                            history_count += 1
                            wal_info = self.backup_manager.get_wal_file_info(fullname)
                            fxlogdb_new.write(wal_info.to_xlogdb_line())
                        else:
                            _logger.warning(
                                "unexpected file rebuilding the wal database: %s",
                                fullname,
                            )
                fxlogdb_new.flush()
                fxlogdb_new.seek(0)
                fxlogdb.seek(0)
                shutil.copyfileobj(fxlogdb_new, fxlogdb)
                fxlogdb.truncate()

        if not silent:
            output.info(
                "Done rebuilding xlogdb for server %s "
                "(history: %s, backup_labels: %s, wal_file: %s)",
                self.config.name,
                history_count,
                label_count,
                wal_count,
            )

    def get_backup_ext_info(self, backup_info):
        """
        Return a dictionary containing all available information about a backup

        The result is equivalent to the sum of information from

         * BackupInfo object
         * the Server.get_wal_info() return value
         * the context in the catalog (if available)
         * the retention policy status
         * the copy statistics
         * the incremental backups information
         * extra backup.info properties

        :param backup_info: the target backup
        :rtype dict: all information about a backup
        """
        backup_ext_info = backup_info.to_dict()
        if backup_info.status in BackupInfo.STATUS_COPY_DONE:
            try:
                previous_backup = self.backup_manager.get_previous_backup(
                    backup_ext_info["backup_id"]
                )
                next_backup = self.backup_manager.get_next_backup(
                    backup_ext_info["backup_id"]
                )
                backup_ext_info["previous_backup_id"] = None
                backup_ext_info["next_backup_id"] = None
                if previous_backup:
                    backup_ext_info["previous_backup_id"] = previous_backup.backup_id
                if next_backup:
                    backup_ext_info["next_backup_id"] = next_backup.backup_id
            except UnknownBackupIdException:
                # no next_backup_id and previous_backup_id items
                # means "Not available"
                pass
            backup_ext_info.update(self.get_wal_info(backup_info))

            backup_ext_info["retention_policy_status"] = None
            if self.enforce_retention_policies:
                policy = self.config.retention_policy
                backup_ext_info["retention_policy_status"] = policy.backup_status(
                    backup_info.backup_id
                )
            # Check any child timeline exists
            children_timelines = self.get_children_timelines(
                backup_ext_info["timeline"], forked_after=backup_info.end_xlog
            )

            backup_ext_info["children_timelines"] = children_timelines

            # If copy statistics are available
            copy_stats = backup_ext_info.get("copy_stats")
            if copy_stats:
                analysis_time = copy_stats.get("analysis_time", 0)
                if analysis_time >= 1:
                    backup_ext_info["analysis_time"] = analysis_time
                copy_time = copy_stats.get("copy_time", 0)
                if copy_time > 0:
                    backup_ext_info["copy_time"] = copy_time
                    dedup_size = backup_ext_info.get("deduplicated_size", 0)
                    if dedup_size > 0:
                        estimated_throughput = dedup_size / copy_time
                        backup_ext_info["estimated_throughput"] = estimated_throughput
                        number_of_workers = copy_stats.get("number_of_workers", 1)
                        if number_of_workers > 1:
                            backup_ext_info["number_of_workers"] = number_of_workers

            backup_chain = [backup for backup in backup_info.walk_to_root()]
            chain_size = len(backup_chain)
            # last is root
            root_backup_info = backup_chain[-1]
            # "Incremental" backups
            backup_ext_info["root_backup_id"] = root_backup_info.backup_id
            backup_ext_info["chain_size"] = chain_size
            # Properties added to the result dictionary
            backup_ext_info["backup_type"] = backup_info.backup_type
            backup_ext_info["deduplication_ratio"] = backup_info.deduplication_ratio
            # A new field "cluster_size" was added to backup.info to be
            # able to calculate the resource saved by "incremental" backups
            # introduced in Postgres 17.
            # To keep backward compatibility between versions, barman relies
            # on two possible values to calculate "est_dedup_size",
            # "size" being used for older versions when "cluster_size"
            # is non existent (None).
            backup_ext_info["est_dedup_size"] = (
                backup_ext_info["cluster_size"] or backup_ext_info["size"]
            ) * backup_ext_info["deduplication_ratio"]
        return backup_ext_info

    def show_backup(self, backup_info):
        """
        Output all available information about a backup

        :param backup_info: the target backup
        """
        try:
            backup_ext_info = self.get_backup_ext_info(backup_info)
            output.result("show_backup", backup_ext_info)
        except BadXlogSegmentName as e:
            output.error(
                "invalid xlog segment name %r\n"
                'HINT: Please run "barman rebuild-xlogdb %s" '
                "to solve this issue",
                force_str(e),
                self.config.name,
            )
            output.close_and_exit()

    @staticmethod
    def _build_path(path_prefix=None):
        """
        If a path_prefix is provided build a string suitable to be used in
        PATH environment variable by joining the path_prefix with the
        current content of PATH environment variable.

        If the `path_prefix` is None returns None.

        :rtype: str|None
        """
        if not path_prefix:
            return None
        sys_path = os.environ.get("PATH")
        return "%s%s%s" % (path_prefix, os.pathsep, sys_path)

    def kill(self, task, fail_if_not_present=True):
        """
        Given the name of a barman sub-task type,
        attempts to stop all the processes

        :param string task: The task we want to stop
        :param bool fail_if_not_present: Display an error when the process
            is not present (default: True)
        """
        process_list = self.process_manager.list(task)
        for process in process_list:
            if self.process_manager.kill(process):
                output.info("Stopped process %s(%s)", process.task, process.pid)
                return
            else:
                output.error(
                    "Cannot terminate process %s(%s)", process.task, process.pid
                )
                return
        if fail_if_not_present:
            output.error(
                "Termination of %s failed: no such process for server %s",
                task,
                self.config.name,
            )

    def switch_wal(self, force=False, archive=None, archive_timeout=None):
        """
        Execute the switch-wal command on the target server
        """
        closed_wal = None
        try:
            if force:
                # If called with force, execute a checkpoint before the
                # switch_wal command
                _logger.info("Force a CHECKPOINT before pg_switch_wal()")
                self.postgres.checkpoint()

            # Perform the switch_wal. expect a WAL name only if the switch
            # has been successfully executed, False otherwise.
            closed_wal = self.postgres.switch_wal()
            if closed_wal is None:
                # Something went wrong during the execution of the
                # pg_switch_wal command
                output.error(
                    "Unable to perform pg_switch_wal "
                    "for server '%s'." % self.config.name
                )
                return

            if closed_wal:
                # The switch_wal command have been executed successfully
                output.info(
                    "The WAL file %s has been closed on server '%s'"
                    % (closed_wal, self.config.name)
                )
            else:
                # Is not necessary to perform a switch_wal
                output.info("No switch required for server '%s'" % self.config.name)
        except PostgresIsInRecovery:
            output.info(
                "No switch performed because server '%s' "
                "is a standby." % self.config.name
            )
        except PostgresCheckpointPrivilegesRequired:
            # Superuser rights are required to perform the switch_wal
            output.error(
                "Barman switch-wal --force requires superuser rights or "
                "the 'pg_checkpoint' role"
            )
            return

        # If the user has asked to wait for a WAL file to be archived,
        # wait until a new WAL file has been found
        # or the timeout has expired
        if archive:
            self.wait_for_wal(closed_wal, archive_timeout)

    def wait_for_wal(self, wal_file=None, archive_timeout=None):
        """
        Wait for a WAL file to be archived on the server

        :param str|None wal_file: Name of the WAL file, or None if we should
          just wait for a new WAL file to be archived
        :param int|None archive_timeout: Timeout in seconds
        """
        max_msg = ""
        if archive_timeout:
            max_msg = " (max: %s seconds)" % archive_timeout

        initial_wals = dict()
        if not wal_file:
            wals = self.backup_manager.get_latest_archived_wals_info()
            initial_wals = dict([(tli, wals[tli].name) for tli in wals])

        if wal_file:
            output.info(
                "Waiting for the WAL file %s from server '%s'%s",
                wal_file,
                self.config.name,
                max_msg,
            )
        else:
            output.info(
                "Waiting for a WAL file from server '%s' to be archived%s",
                self.config.name,
                max_msg,
            )

        # Wait for a new file until end_time or forever if no archive_timeout
        end_time = None
        if archive_timeout:
            end_time = time.time() + archive_timeout
        while not end_time or time.time() < end_time:
            self.archive_wal(verbose=False)

            # Finish if the closed wal file is in the archive.
            if wal_file:
                if os.path.exists(self.get_wal_full_path(wal_file)):
                    break
            else:
                # Check if any new file has been archived, on any timeline
                wals = self.backup_manager.get_latest_archived_wals_info()
                current_wals = dict([(tli, wals[tli].name) for tli in wals])

                if current_wals != initial_wals:
                    break

            # sleep a bit before retrying
            time.sleep(0.1)
        else:
            if wal_file:
                output.error(
                    "The WAL file %s has not been received in %s seconds",
                    wal_file,
                    archive_timeout,
                )
            else:
                output.info(
                    "A WAL file has not been received in %s seconds", archive_timeout
                )

    def replication_status(self, target="all"):
        """
        Implements the 'replication-status' command.
        """
        if target == "hot-standby":
            client_type = PostgreSQLConnection.STANDBY
        elif target == "wal-streamer":
            client_type = PostgreSQLConnection.WALSTREAMER
        else:
            client_type = PostgreSQLConnection.ANY_STREAMING_CLIENT
        try:
            standby_info = self.postgres.get_replication_stats(client_type)
            if standby_info is None:
                output.error("Unable to connect to server %s" % self.config.name)
            else:
                output.result(
                    "replication_status",
                    self.config.name,
                    target,
                    self.postgres.current_xlog_location,
                    standby_info,
                )
        except PostgresUnsupportedFeature as e:
            output.info("  Requires PostgreSQL %s or higher", e)
        except PostgresObsoleteFeature as e:
            output.info("  Requires PostgreSQL lower than %s", e)
        except PostgresSuperuserRequired:
            output.info("  Requires superuser rights")

    def get_children_timelines(self, tli, forked_after=None):
        """
        Get a list of the children of the passed timeline

        :param int tli: Id of the timeline to check
        :param str forked_after: XLog location after which the timeline
          must have been created
        :return List[xlog.HistoryFileData]: the list of timelines that
          have the timeline with id 'tli' as parent
        """

        if forked_after:
            forked_after = xlog.parse_lsn(forked_after)

        children = []
        # Search all the history files after the passed timeline
        children_tli = tli
        while True:
            children_tli += 1
            history_path = os.path.join(
                self.config.wals_directory, "%08X.history" % children_tli
            )
            # If the file doesn't exists, stop searching
            if not os.path.exists(history_path):
                break

            # Create the WalFileInfo object using the file
            wal_info = self.backup_manager.get_wal_file_info(history_path)
            # Get content of the file. We need to pass a compressor manager
            # here to handle an eventual compression of the history file
            history_info = xlog.decode_history_file(
                wal_info, self.backup_manager.compression_manager
            )

            # Save the history only if is reachable from this timeline.
            for tinfo in history_info:
                # The history file contains the full genealogy
                # but we keep only the line with `tli` timeline as parent.
                if tinfo.parent_tli != tli:
                    continue

                # We need to return this history info only if this timeline
                # has been forked after the passed LSN
                if forked_after and tinfo.switchpoint < forked_after:
                    continue

                children.append(tinfo)

        return children

    def check_backup(self, backup_info):
        """
        Make sure that we have all the WAL files required
        by a physical backup for consistency (from the
        first to the last WAL file)

        :param backup_info: the target backup
        """
        output.debug(
            "Checking backup %s of server %s", backup_info.backup_id, self.config.name
        )
        try:
            # No need to check a backup which is not waiting for WALs.
            # Doing that we could also mark as DONE backups which
            # were previously FAILED due to copy errors
            if backup_info.status == BackupInfo.FAILED:
                output.error("The validity of a failed backup cannot be checked")
                return

            # Take care of the backup lock.
            # Only one process can modify a backup a a time
            with ServerBackupIdLock(
                self.config.barman_lock_directory,
                self.config.name,
                backup_info.backup_id,
            ):
                orig_status = backup_info.status
                self.backup_manager.check_backup(backup_info)
                if orig_status == backup_info.status:
                    output.debug(
                        "Check finished: the status of backup %s of server %s "
                        "remains %s",
                        backup_info.backup_id,
                        self.config.name,
                        backup_info.status,
                    )
                else:
                    output.debug(
                        "Check finished: the status of backup %s of server %s "
                        "changed from %s to %s",
                        backup_info.backup_id,
                        self.config.name,
                        orig_status,
                        backup_info.status,
                    )
        except LockFileBusy:
            # If another process is holding the backup lock,
            # notify the user and terminate.
            # This is not an error condition because it happens when
            # another process is validating the backup.
            output.info(
                "Another process is holding the lock for "
                "backup %s of server %s." % (backup_info.backup_id, self.config.name)
            )
            return

        except LockFilePermissionDenied as e:
            # We cannot access the lockfile.
            # warn the user and terminate
            output.error("Permission denied, unable to access '%s'" % e)
            return

    def sync_status(self, last_wal=None, last_position=None):
        """
        Return server status for sync purposes.

        The method outputs JSON, containing:
         * list of backups (with DONE status)
         * server configuration
         * last read position (in xlog.db)
         * last read wal
         * list of archived wal files

        If last_wal is provided, the method will discard all the wall files
        older than last_wal.
        If last_position is provided the method will try to read
        the xlog.db file using last_position as starting point.
        If the wal file at last_position does not match last_wal, read from the
        start and use last_wal as limit

        :param str|None last_wal: last read wal
        :param int|None last_position: last read position (in xlog.db)
        """
        sync_status = {}
        wals = []
        # Get all the backups using default filter for
        # get_available_backups method
        # (BackupInfo.DONE)
        backups = self.get_available_backups()
        # Retrieve the first wal associated to a backup, it will be useful
        # to filter our eventual WAL too old to be useful
        first_useful_wal = None
        if backups:
            first_useful_wal = backups[sorted(backups.keys())[0]].begin_wal
        # Read xlogdb file.
        with self.xlogdb() as fxlogdb:
            starting_point = self.set_sync_starting_point(
                fxlogdb, last_wal, last_position
            )
            check_first_wal = starting_point == 0 and last_wal is not None
            # The wal_info and line variables are used after the loop.
            # We initialize them here to avoid errors with an empty xlogdb.
            line = None
            wal_info = None
            for line in fxlogdb:
                # Parse the line
                wal_info = WalFileInfo.from_xlogdb_line(line)
                # Check if user is requesting data that is not available.
                # TODO: probably the check should be something like
                # TODO: last_wal + 1 < wal_info.name
                if check_first_wal:
                    if last_wal < wal_info.name:
                        raise SyncError(
                            "last_wal '%s' is older than the first"
                            " available wal '%s'" % (last_wal, wal_info.name)
                        )
                    else:
                        check_first_wal = False
                # If last_wal is provided, discard any line older than last_wal
                if last_wal:
                    if wal_info.name <= last_wal:
                        continue
                # Else don't return any WAL older than first available backup
                elif first_useful_wal and wal_info.name < first_useful_wal:
                    continue
                wals.append(wal_info)
            if wal_info is not None:
                # Check if user is requesting data that is not available.
                if last_wal is not None and last_wal > wal_info.name:
                    raise SyncError(
                        "last_wal '%s' is newer than the last available wal "
                        " '%s'" % (last_wal, wal_info.name)
                    )
                # Set last_position with the current position - len(last_line)
                # (returning the beginning of the last line)
                sync_status["last_position"] = fxlogdb.tell() - len(line)
                # Set the name of the last wal of the file
                sync_status["last_name"] = wal_info.name
            else:
                # we started over
                sync_status["last_position"] = 0
                sync_status["last_name"] = ""
            sync_status["backups"] = backups
            sync_status["wals"] = wals
            sync_status["version"] = barman.__version__
            sync_status["config"] = self.config
        json.dump(sync_status, sys.stdout, cls=BarmanEncoder, indent=4)

    def sync_cron(self, keep_descriptors):
        """
        Manage synchronisation operations between passive node and
        master node.
        The method recover information from the remote master
        server, evaluate if synchronisation with the master is required
        and spawn barman sub processes, syncing backups and WAL files
        :param bool keep_descriptors: whether to keep subprocess descriptors
           attached to this process.
        """
        # Recover information from primary node
        sync_wal_info = self.load_sync_wals_info()
        # Use last_wal and last_position for the remote call to the
        # master server
        try:
            remote_info = self.primary_node_info(
                sync_wal_info.last_wal, sync_wal_info.last_position
            )
        except SyncError as exc:
            output.error(
                "Failed to retrieve the primary node status: %s" % force_str(exc)
            )
            return

        # Perform backup synchronisation
        if remote_info["backups"]:
            # Get the list of backups that need to be synced
            # with the local server
            local_backup_list = self.get_available_backups()
            # Subtract the list of the already
            # synchronised backups from the remote backup lists,
            # obtaining the list of backups still requiring synchronisation
            sync_backup_list = set(remote_info["backups"]) - set(local_backup_list)
        else:
            # No backup to synchronisation required
            output.info(
                "No backup synchronisation required for server %s",
                self.config.name,
                log=False,
            )
            sync_backup_list = []
        for backup_id in sorted(sync_backup_list):
            # Check if this backup_id needs to be synchronized by spawning a
            # sync-backup process.
            # The same set of checks will be executed by the spawned process.
            # This "double check" is necessary because we don't want the cron
            # to spawn unnecessary processes.
            try:
                local_backup_info = self.get_backup(backup_id)
                self.check_sync_required(backup_id, remote_info, local_backup_info)
            except SyncError as e:
                # It means that neither the local backup
                # nor the remote one exist.
                # This should not happen here.
                output.exception("Unexpected state: %s", e)
                break
            except SyncToBeDeleted:
                # The backup does not exist on primary server
                # and is FAILED here.
                # It must be removed by the sync-backup process.
                pass
            except SyncNothingToDo:
                # It could mean that the local backup is in DONE state or
                # that it is obsolete according to
                # the local retention policies.
                # In both cases, continue with the next backup.
                continue
            # Now that we are sure that a backup-sync subprocess is necessary,
            # we need to acquire the backup lock, to be sure that
            # there aren't other processes synchronising the backup.
            # If cannot acquire the lock, another synchronisation process
            # is running, so we give up.
            try:
                with ServerBackupSyncLock(
                    self.config.barman_lock_directory, self.config.name, backup_id
                ):
                    output.info(
                        "Starting copy of backup %s for server %s",
                        backup_id,
                        self.config.name,
                    )
            except LockFileBusy:
                output.info(
                    "A synchronisation process for backup %s"
                    " on server %s is already in progress",
                    backup_id,
                    self.config.name,
                    log=False,
                )
                # Stop processing this server
                break

            # Init a Barman sub-process object
            sub_process = BarmanSubProcess(
                subcommand="sync-backup",
                config=barman.__config__.config_file,
                args=[self.config.name, backup_id],
                keep_descriptors=keep_descriptors,
            )
            # Launch the sub-process
            sub_process.execute()

            # Stop processing this server
            break

        # Perform WAL synchronisation
        if remote_info["wals"]:
            # We need to acquire a sync-wal lock, to be sure that
            # there aren't other processes synchronising the WAL files.
            # If cannot acquire the lock, another synchronisation process
            # is running, so we give up.
            try:
                with ServerWalSyncLock(
                    self.config.barman_lock_directory,
                    self.config.name,
                ):
                    output.info(
                        "Started copy of WAL files for server %s", self.config.name
                    )
            except LockFileBusy:
                output.info(
                    "WAL synchronisation already running for server %s",
                    self.config.name,
                    log=False,
                )
                return

            # Init a Barman sub-process object
            sub_process = BarmanSubProcess(
                subcommand="sync-wals",
                config=barman.__config__.config_file,
                args=[self.config.name],
                keep_descriptors=keep_descriptors,
            )
            # Launch the sub-process
            sub_process.execute()
        else:
            # no WAL synchronisation is required
            output.info(
                "No WAL synchronisation required for server %s",
                self.config.name,
                log=False,
            )

    def check_sync_required(self, backup_name, primary_info, local_backup_info):
        """
        Check if it is necessary to sync a backup.

        If the backup is present on the Primary node:

        * if it does not exist locally: continue (synchronise it)
        * if it exists and is DONE locally: raise SyncNothingToDo
          (nothing to do)
        * if it exists and is FAILED locally: continue (try to recover it)

        If the backup is not present on the Primary node:

        * if it does not exist locally: raise SyncError (wrong call)
        * if it exists and is DONE locally: raise SyncNothingToDo
          (nothing to do)
        * if it exists and is FAILED locally: raise SyncToBeDeleted (remove it)

        If a backup needs to be synchronised but it is obsolete according
        to local retention policies, raise SyncNothingToDo,
        else return to the caller.

        :param str backup_name: str name of the backup to sync
        :param dict primary_info: dict containing the Primary node status
        :param barman.infofile.BackupInfo local_backup_info: BackupInfo object
                representing the current backup state

        :raise SyncError: There is an error in the user request
        :raise SyncNothingToDo: Nothing to do for this request
        :raise SyncToBeDeleted: Backup is not recoverable and must be deleted
        """
        backups = primary_info["backups"]
        # Backup not present on Primary node, and not present
        # locally. Raise exception.
        if backup_name not in backups and local_backup_info is None:
            raise SyncError(
                "Backup %s is absent on %s server" % (backup_name, self.config.name)
            )
        # Backup not present on Primary node, but is
        # present locally with status FAILED: backup incomplete.
        # Remove the backup and warn the user
        if (
            backup_name not in backups
            and local_backup_info is not None
            and local_backup_info.status == BackupInfo.FAILED
        ):
            raise SyncToBeDeleted(
                "Backup %s is absent on %s server and is incomplete locally"
                % (backup_name, self.config.name)
            )

        # Backup not present on Primary node, but is
        # present locally with status DONE. Sync complete, local only.
        if (
            backup_name not in backups
            and local_backup_info is not None
            and local_backup_info.status == BackupInfo.DONE
        ):
            raise SyncNothingToDo(
                "Backup %s is absent on %s server, but present locally "
                "(local copy only)" % (backup_name, self.config.name)
            )

        # Backup present on Primary node, and present locally
        # with status DONE. Sync complete.
        if (
            backup_name in backups
            and local_backup_info is not None
            and local_backup_info.status == BackupInfo.DONE
        ):
            raise SyncNothingToDo(
                "Backup %s is already synced with"
                " %s server" % (backup_name, self.config.name)
            )

        # Retention Policy: if the local server has a Retention policy,
        # check that the remote backup is not obsolete.
        enforce_retention_policies = self.enforce_retention_policies
        retention_policy_mode = self.config.retention_policy_mode
        if enforce_retention_policies and retention_policy_mode == "auto":
            # All the checks regarding retention policies are in
            # this boolean method.
            if self.is_backup_locally_obsolete(backup_name, backups):
                # The remote backup is obsolete according to
                # local retention policies.
                # Nothing to do.
                raise SyncNothingToDo(
                    "Remote backup %s/%s is obsolete for "
                    "local retention policies."
                    % (primary_info["config"]["name"], backup_name)
                )

    def load_sync_wals_info(self):
        """
        Load the content of SYNC_WALS_INFO_FILE for the given server

        :return collections.namedtuple: last read wal and position information
        """
        sync_wals_info_file = os.path.join(
            self.config.wals_directory, SYNC_WALS_INFO_FILE
        )
        if not os.path.exists(sync_wals_info_file):
            return SyncWalInfo(None, None)
        try:
            with open(sync_wals_info_file) as f:
                return SyncWalInfo._make(f.readline().split("\t"))
        except (OSError, IOError) as e:
            raise SyncError(
                "Cannot open %s file for server %s: %s"
                % (SYNC_WALS_INFO_FILE, self.config.name, e)
            )

    def primary_node_info(self, last_wal=None, last_position=None):
        """
        Invoke sync-info directly on the specified primary node

        The method issues a call to the sync-info method on the primary
        node through an SSH connection

        :param barman.server.Server self: the Server object
        :param str|None last_wal: last read wal
        :param int|None last_position: last read position (in xlog.db)
        :raise SyncError: if the ssh command fails
        """
        # First we need to check if the server is in passive mode
        _logger.debug(
            "primary sync-info(%s, %s, %s)", self.config.name, last_wal, last_position
        )
        if not self.passive_node:
            raise SyncError("server %s is not passive" % self.config.name)
        # Issue a call to 'barman sync-info' to the primary node,
        # using primary_ssh_command option to establish an
        # SSH connection.
        remote_command = Command(
            cmd=self.config.primary_ssh_command, shell=True, check=True, path=self.path
        )
        # We run it in a loop to retry when the master issues error.
        while True:
            try:
                # Include the config path as an option if configured for this server
                if self.config.forward_config_path:
                    base_cmd = "barman -c %s sync-info" % barman.__config__.config_file
                else:
                    base_cmd = "barman sync-info"
                # Build the command string
                cmd_str = "%s %s" % (base_cmd, self.config.name)
                # If necessary we add last_wal and last_position
                # to the command string
                if last_wal is not None:
                    cmd_str += " %s " % last_wal
                    if last_position is not None:
                        cmd_str += " %s " % last_position
                # Then issue the command
                remote_command(cmd_str)
                # All good, exit the retry loop with 'break'
                break
            except CommandFailedException as exc:
                # In case we requested synchronisation with a last WAL info,
                # we try again requesting the full current status, but only if
                # exit code is 1. A different exit code means that
                # the error is not from Barman (i.e. ssh failure)
                if exc.args[0]["ret"] == 1 and last_wal is not None:
                    last_wal = None
                    last_position = None
                    output.warning(
                        "sync-info is out of sync. "
                        "Self-recovery procedure started: "
                        "requesting full synchronisation from "
                        "primary server %s" % self.config.name
                    )
                    continue
                # Wrap the CommandFailed exception with a SyncError
                # for custom message and logging.
                raise SyncError(
                    "sync-info execution on remote "
                    "primary server %s failed: %s"
                    % (self.config.name, exc.args[0]["err"])
                )

        # Save the result on disk
        primary_info_file = os.path.join(
            self.config.backup_directory, PRIMARY_INFO_FILE
        )

        # parse the json output
        remote_info = json.loads(remote_command.out)

        try:
            # TODO: rename the method to make it public
            # noinspection PyProtectedMember
            self._make_directories()
            # Save remote info to disk
            # We do not use a LockFile here. Instead we write all data
            # in a new file (adding '.tmp' extension) then we rename it
            # replacing the old one.
            # It works while the renaming is an atomic operation
            # (this is a POSIX requirement)
            primary_info_file_tmp = primary_info_file + ".tmp"
            with open(primary_info_file_tmp, "w") as info_file:
                info_file.write(remote_command.out)
            os.rename(primary_info_file_tmp, primary_info_file)
        except (OSError, IOError) as e:
            # Wrap file access exceptions using SyncError
            raise SyncError(
                "Cannot open %s file for server %s: %s"
                % (PRIMARY_INFO_FILE, self.config.name, e)
            )

        return remote_info

    def is_backup_locally_obsolete(self, backup_name, remote_backups):
        """
        Check if a remote backup is obsolete according with the local
        retention policies.

        :param barman.server.Server self: Server object
        :param str backup_name: str name of the backup to sync
        :param dict remote_backups: dict containing the Primary node status

        :return bool: returns if the backup is obsolete or not
        """
        # Get the local backups and add the remote backup info. This will
        # simulate the situation after the copy of the remote backup.
        local_backups = self.get_available_backups(BackupInfo.STATUS_NOT_EMPTY)
        backup = remote_backups[backup_name]
        local_backups[backup_name] = LocalBackupInfo.from_json(self, backup)
        # Execute the local retention policy on the modified list of backups
        report = self.config.retention_policy.report(source=local_backups)
        # If the added backup is obsolete return true.
        return report[backup_name] == BackupInfo.OBSOLETE

    def sync_backup(self, backup_name):
        """
        Method for the synchronisation of a backup from a primary server.

        The Method checks that the server is passive, then if it is possible to
        sync with the Primary. Acquires a lock at backup level
        and copy the backup from the Primary node using rsync.

        During the sync process the backup on the Passive node
        is marked as SYNCING and if the sync fails
        (due to network failure, user interruption...) it is marked as FAILED.

        :param barman.server.Server self: the passive Server object to sync
        :param str backup_name: the name of the backup to sync.
        """

        _logger.debug("sync_backup(%s, %s)", self.config.name, backup_name)
        if not self.passive_node:
            raise SyncError("server %s is not passive" % self.config.name)

        local_backup_info = self.get_backup(backup_name)
        # Step 1. Parse data from Primary server.
        _logger.info(
            "Synchronising with server %s backup %s: step 1/3: "
            "parse server information",
            self.config.name,
            backup_name,
        )
        try:
            primary_info = self.load_primary_info()
            self.check_sync_required(backup_name, primary_info, local_backup_info)
        except SyncError as e:
            # Invocation error: exit with return code 1
            output.error("%s", e)
            return
        except SyncToBeDeleted as e:
            # The required backup does not exist on primary,
            # therefore it should be deleted also on passive node,
            # as it's not in DONE status.
            output.warning("%s, purging local backup", e)
            self.delete_backup(local_backup_info)
            return
        except SyncNothingToDo as e:
            # Nothing to do. Log as info level and exit
            output.info("%s", e)
            return
        # If the backup is present on Primary node, and is not present at all
        # locally or is present with FAILED status, execute sync.
        # Retrieve info about the backup from PRIMARY_INFO_FILE
        remote_backup_info = primary_info["backups"][backup_name]
        remote_backup_dir = primary_info["config"]["basebackups_directory"]

        # Try to acquire the backup lock, if the lock is not available abort
        # the copy.
        try:
            with ServerBackupSyncLock(
                self.config.barman_lock_directory, self.config.name, backup_name
            ):
                try:
                    backup_manager = self.backup_manager

                    # Build a BackupInfo object
                    local_backup_info = LocalBackupInfo.from_json(
                        self, remote_backup_info
                    )
                    local_backup_info.set_attribute("status", BackupInfo.SYNCING)
                    local_backup_info.save()
                    backup_manager.backup_cache_add(local_backup_info)

                    # Activate incremental copy if requested
                    # Calculate the safe_horizon as the start time of the older
                    # backup involved in the copy
                    # NOTE: safe_horizon is a tz-aware timestamp because
                    # BackupInfo class ensures that property
                    reuse_mode = self.config.reuse_backup
                    safe_horizon = None
                    reuse_dir = None
                    if reuse_mode:
                        prev_backup = backup_manager.get_previous_backup(backup_name)
                        next_backup = backup_manager.get_next_backup(backup_name)
                        # If a newer backup is present, using it is preferable
                        # because that backup will remain valid longer
                        if next_backup:
                            safe_horizon = local_backup_info.begin_time
                            reuse_dir = next_backup.get_basebackup_directory()
                        elif prev_backup:
                            safe_horizon = prev_backup.begin_time
                            reuse_dir = prev_backup.get_basebackup_directory()
                        else:
                            reuse_mode = None

                    # Try to copy from the Primary node the backup using
                    # the copy controller.
                    copy_controller = RsyncCopyController(
                        ssh_command=self.config.primary_ssh_command,
                        network_compression=self.config.network_compression,
                        path=self.path,
                        reuse_backup=reuse_mode,
                        safe_horizon=safe_horizon,
                        retry_times=self.config.basebackup_retry_times,
                        retry_sleep=self.config.basebackup_retry_sleep,
                        workers=self.config.parallel_jobs,
                        workers_start_batch_period=self.config.parallel_jobs_start_batch_period,
                        workers_start_batch_size=self.config.parallel_jobs_start_batch_size,
                    )
                    # Exclude primary Barman metadata and state
                    exclude_and_protect = ["/backup.info", "/.backup.lock"]
                    # Exclude any tablespace symlinks created by pg_basebackup
                    if local_backup_info.tablespaces is not None:
                        for tablespace in local_backup_info.tablespaces:
                            exclude_and_protect += [
                                "/data/pg_tblspc/%s" % tablespace.oid
                            ]
                    copy_controller.add_directory(
                        "basebackup",
                        ":%s/%s/" % (remote_backup_dir, backup_name),
                        local_backup_info.get_basebackup_directory(),
                        exclude_and_protect=exclude_and_protect,
                        bwlimit=self.config.bandwidth_limit,
                        reuse=reuse_dir,
                        item_class=RsyncCopyController.PGDATA_CLASS,
                    )
                    _logger.info(
                        "Synchronising with server %s backup %s: step 2/3: "
                        "file copy",
                        self.config.name,
                        backup_name,
                    )
                    copy_controller.copy()

                    # Save the backup state and exit
                    _logger.info(
                        "Synchronising with server %s backup %s: "
                        "step 3/3: finalise sync",
                        self.config.name,
                        backup_name,
                    )
                    local_backup_info.set_attribute("status", BackupInfo.DONE)
                    local_backup_info.save()
                except CommandFailedException as e:
                    # Report rsync errors
                    msg = "failure syncing server %s backup %s: %s" % (
                        self.config.name,
                        backup_name,
                        e,
                    )
                    output.error(msg)
                    # Set the BackupInfo status to FAILED
                    local_backup_info.set_attribute("status", BackupInfo.FAILED)
                    local_backup_info.set_attribute("error", msg)
                    local_backup_info.save()
                    return
                # Catch KeyboardInterrupt (Ctrl+c) and all the exceptions
                except BaseException as e:
                    msg_lines = force_str(e).strip().splitlines()
                    if local_backup_info:
                        # Use only the first line of exception message
                        # in local_backup_info error field
                        local_backup_info.set_attribute("status", BackupInfo.FAILED)
                        # If the exception has no attached message
                        # use the raw type name
                        if not msg_lines:
                            msg_lines = [type(e).__name__]
                        local_backup_info.set_attribute(
                            "error",
                            "failure syncing server %s backup %s: %s"
                            % (self.config.name, backup_name, msg_lines[0]),
                        )
                        local_backup_info.save()
                    output.error(
                        "Backup failed syncing with %s: %s\n%s",
                        self.config.name,
                        msg_lines[0],
                        "\n".join(msg_lines[1:]),
                    )
        except LockFileException:
            output.error(
                "Another synchronisation process for backup %s "
                "of server %s is already running.",
                backup_name,
                self.config.name,
            )

    def sync_wals(self):
        """
        Method for the synchronisation of WAL files on the passive node,
        by copying them from the primary server.

        The method checks if the server is passive, then tries to acquire
        a sync-wal lock.

        Recovers the id of the last locally archived WAL file from the
        status file ($wals_directory/sync-wals.info).

        Reads the primary.info file and parses it, then obtains the list of
        WAL files that have not yet been synchronised with the master.
        Rsync is used for file synchronisation with the primary server.

        Once the copy is finished, acquires a lock on xlog.db, updates it
        then releases the lock.

        Before exiting, the method updates the last_wal
        and last_position fields in the sync-wals.info file.

        :param barman.server.Server self: the Server object to synchronise
        """
        _logger.debug("sync_wals(%s)", self.config.name)
        if not self.passive_node:
            raise SyncError("server %s is not passive" % self.config.name)

        # Try to acquire the sync-wal lock if the lock is not available,
        # abort the sync-wal operation
        try:
            with ServerWalSyncLock(
                self.config.barman_lock_directory,
                self.config.name,
            ):
                try:
                    # Need to load data from status files: primary.info
                    # and sync-wals.info
                    sync_wals_info = self.load_sync_wals_info()
                    primary_info = self.load_primary_info()
                    # We want to exit if the compression on master is different
                    # from the one on the local server
                    if primary_info["config"]["compression"] != self.config.compression:
                        raise SyncError(
                            "Compression method on server %s "
                            "(%s) does not match local "
                            "compression method (%s) "
                            % (
                                self.config.name,
                                primary_info["config"]["compression"],
                                self.config.compression,
                            )
                        )
                    # If the first WAL that needs to be copied is older
                    # than the begin WAL of the first locally available backup,
                    # synchronisation is skipped. This means that we need
                    # to copy a WAL file which won't be associated to any local
                    # backup. Consider the following scenarios:
                    #
                    # bw: indicates the begin WAL of the first backup
                    # sw: the first WAL to be sync-ed
                    #
                    # The following examples use truncated names for WAL files
                    # (e.g. 1 instead of 000000010000000000000001)
                    #
                    # Case 1: bw = 10, sw = 9 - SKIP and wait for backup
                    # Case 2: bw = 10, sw = 10 - SYNC
                    # Case 3: bw = 10, sw = 15 - SYNC
                    #
                    # Search for the first WAL file (skip history,
                    # backup and partial files)
                    first_remote_wal = None
                    for wal in primary_info["wals"]:
                        if xlog.is_wal_file(wal["name"]):
                            first_remote_wal = wal["name"]
                            break

                    first_backup_id = self.get_first_backup_id()
                    first_backup = (
                        self.get_backup(first_backup_id) if first_backup_id else None
                    )
                    # Also if there are not any backups on the local server
                    # no wal synchronisation is required
                    if not first_backup:
                        output.warning(
                            "No base backup for server %s" % self.config.name
                        )
                        return

                    if first_backup.begin_wal > first_remote_wal:
                        output.warning(
                            "Skipping WAL synchronisation for "
                            "server %s: no available local backup "
                            "for %s" % (self.config.name, first_remote_wal)
                        )
                        return

                    local_wals = []
                    wal_file_paths = []
                    for wal in primary_info["wals"]:
                        # filter all the WALs that are smaller
                        # or equal to the name of the latest synchronised WAL
                        if (
                            sync_wals_info.last_wal
                            and wal["name"] <= sync_wals_info.last_wal
                        ):
                            continue
                        # Generate WalFileInfo Objects using remote WAL metas.
                        # This list will be used for the update of the xlog.db
                        wal_info_file = WalFileInfo(**wal)
                        local_wals.append(wal_info_file)
                        wal_file_paths.append(wal_info_file.relpath())

                    # Rsync Options:
                    # recursive: recursive copy of subdirectories
                    # perms: preserve permissions on synced files
                    # times: preserve modification timestamps during
                    #   synchronisation
                    # protect-args: force rsync to preserve the integrity of
                    #   rsync command arguments and filename.
                    # inplace: for inplace file substitution
                    #   and update of files
                    rsync = Rsync(
                        args=[
                            "--recursive",
                            "--perms",
                            "--times",
                            "--protect-args",
                            "--inplace",
                        ],
                        ssh=self.config.primary_ssh_command,
                        bwlimit=self.config.bandwidth_limit,
                        allowed_retval=(0,),
                        network_compression=self.config.network_compression,
                        path=self.path,
                    )
                    # Source and destination of the rsync operations
                    src = ":%s/" % primary_info["config"]["wals_directory"]
                    dest = "%s/" % self.config.wals_directory

                    # Perform the rsync copy using the list of relative paths
                    # obtained from the primary.info file
                    rsync.from_file_list(wal_file_paths, src, dest)

                    # If everything is synced without errors,
                    # update xlog.db using the list of WalFileInfo object
                    with self.xlogdb("a") as fxlogdb:
                        for wal_info in local_wals:
                            fxlogdb.write(wal_info.to_xlogdb_line())
                    # We need to update the sync-wals.info file with the latest
                    # synchronised WAL and the latest read position.
                    self.write_sync_wals_info_file(primary_info)

                except CommandFailedException as e:
                    msg = "WAL synchronisation for server %s failed: %s" % (
                        self.config.name,
                        e,
                    )
                    output.error(msg)
                    return
                except BaseException as e:
                    msg_lines = force_str(e).strip().splitlines()
                    # Use only the first line of exception message
                    # If the exception has no attached message
                    # use the raw type name
                    if not msg_lines:
                        msg_lines = [type(e).__name__]
                    output.error(
                        "WAL synchronisation for server %s failed with: %s\n%s",
                        self.config.name,
                        msg_lines[0],
                        "\n".join(msg_lines[1:]),
                    )
        except LockFileException:
            output.error(
                "Another sync-wal operation is running for server %s ",
                self.config.name,
            )

    @staticmethod
    def set_sync_starting_point(xlogdb_file, last_wal, last_position):
        """
        Check if the xlog.db file has changed between two requests
        from the client and set the start point for reading the file

        :param file xlogdb_file: an open and readable xlog.db file object
        :param str|None last_wal: last read name
        :param int|None last_position: last read position
        :return int: the position has been set
        """
        # If last_position is None start reading from the beginning of the file
        position = int(last_position) if last_position is not None else 0
        # Seek to required position
        xlogdb_file.seek(position)
        # Read 24 char (the size of a wal name)
        wal_name = xlogdb_file.read(24)
        # If the WAL name is the requested one start from last_position
        if wal_name == last_wal:
            # Return to the line start
            xlogdb_file.seek(position)
            return position
        # If the file has been truncated, start over
        xlogdb_file.seek(0)
        return 0

    def write_sync_wals_info_file(self, primary_info):
        """
        Write the content of SYNC_WALS_INFO_FILE on disk

        :param dict primary_info:
        """
        try:
            with open(
                os.path.join(self.config.wals_directory, SYNC_WALS_INFO_FILE), "w"
            ) as syncfile:
                syncfile.write(
                    "%s\t%s"
                    % (primary_info["last_name"], primary_info["last_position"])
                )
        except (OSError, IOError):
            # Wrap file access exceptions using SyncError
            raise SyncError(
                "Unable to write %s file for server %s"
                % (SYNC_WALS_INFO_FILE, self.config.name)
            )

    def load_primary_info(self):
        """
        Load the content of PRIMARY_INFO_FILE for the given server

        :return dict: primary server information
        """
        primary_info_file = os.path.join(
            self.config.backup_directory, PRIMARY_INFO_FILE
        )
        try:
            with open(primary_info_file) as f:
                return json.load(f)
        except (OSError, IOError) as e:
            # Wrap file access exceptions using SyncError
            raise SyncError(
                "Cannot open %s file for server %s: %s"
                % (PRIMARY_INFO_FILE, self.config.name, e)
            )

    def restart_processes(self):
        """
        Restart server subprocesses.
        """
        # Terminate the receive-wal sub-process if present
        self.kill("receive-wal", fail_if_not_present=False)
        if self.config.streaming_archiver:
            # Spawn the receive-wal sub-process
            self.background_receive_wal(keep_descriptors=False)

    def move_wal_file_to_errors_directory(self, src, file_name, suffix):
        """
        Move an unknown or (mismatching) duplicate WAL file to the ``errors`` directory.

        .. note:
            The issues can happen when:

            * Unknown WAL file:

                * The asynchronous WAL archiver detects a file in the ``incoming`` or
                  ``streaming`` directory which is not an WAL file.

            * Duplicate WAL file:

                * ``barman-wal-archive`` attempts to write a file to the ``incoming``
                  directory which already exists there, but with a different content.
                * The asynchronous WAL archiver detects a file in the ``incoming`` or
                  ``streaming`` which already exists in the ``wals`` directory, but with
                  a different content.

        :param str src: Incoming file to be moved to the ``errors`` directory.
        :param str file_name: Name of the incoming file.
        :param str suffix: String which identifies the kind of the issue.

            * ``duplicate``: if *src* is a (mismatching) duplicate WAL file.
            * ``unknown``: if *src* is not an WAL file.
        """
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        error_dst = os.path.join(
            self.config.errors_directory,
            "%s.%s.%s" % (file_name, stamp, suffix),
        )
        # TODO: cover corner case of duplication (unlikely,
        # but theoretically possible)
        try:
            shutil.move(src, error_dst)
        except IOError as e:
            if e.errno == errno.ENOENT:
                _logger.warning("%s not found" % src)
