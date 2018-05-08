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
This module represents a Server.
Barman is able to manage multiple servers.
"""

import logging
import os
import shutil
import sys
import time
from collections import namedtuple
from contextlib import contextmanager
from glob import glob
from tempfile import NamedTemporaryFile

import barman
from barman import output, xlog
from barman.backup import BackupManager
from barman.command_wrappers import BarmanSubProcess
from barman.compression import identify_compression
from barman.exceptions import (ArchiverFailure, BadXlogSegmentName,
                               ConninfoException, LockFileBusy,
                               LockFilePermissionDenied,
                               PostgresDuplicateReplicationSlot,
                               PostgresException,
                               PostgresInvalidReplicationSlot,
                               PostgresIsInRecovery,
                               PostgresReplicationSlotInUse,
                               PostgresReplicationSlotsFull,
                               PostgresSuperuserRequired,
                               PostgresUnsupportedFeature, TimeoutError,
                               UnknownBackupIdException)
from barman.infofile import BackupInfo, WalFileInfo
from barman.lockfile import (ServerBackupLock, ServerCronLock,
                             ServerWalArchiveLock, ServerWalReceiveLock,
                             ServerXLOGDBLock)
from barman.postgres import PostgreSQLConnection, StreamingConnection
from barman.process import ProcessManager
from barman.remote_status import RemoteStatusMixin
from barman.retention_policies import RetentionPolicyFactory
from barman.utils import (human_readable_timedelta, is_power_of_two,
                          pretty_size, timeout)
from barman.wal_archiver import (FileWalArchiver, StreamingWalArchiver,
                                 WalArchiver)

_logger = logging.getLogger(__name__)


class CheckStrategy(object):
    """
    This strategy for the 'check' collects the results of
    every check and does not print any message.
    This basic class is also responsible for immediately
    logging any performed check with an error in case of
    check failure and a debug message in case of success.
    """

    # create a namedtuple object called CheckResult to manage check results
    CheckResult = namedtuple('CheckResult', 'server_name check status')

    # Default list used as a filter to identify non-critical checks
    NON_CRITICAL_CHECKS = ['minimum redundancy requirements',
                           'backup maximum age',
                           'failed backups',
                           'archiver errors',
                           'empty incoming directory',
                           'empty streaming directory',
                           'incoming WALs directory',
                           'streaming WALs directory',
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
        _logger.debug("Starting check: %s" % check_name)

    def _check_name(self, check):
        if not check:
            check = self.running_check
        assert check
        return check

    def result(self, server_name, status, hint=None, check=None):
        """
        Store the result of a check (with no output).
        Log any check result (error or debug level).

        :param str server_name: the server is being checked
        :param bool status: True if succeeded
        :param str,None hint: hint to print if not None:
        :param str,None check: the check name
        """
        check = self._check_name(check)
        if not status:
            # If the name of the check is not in the filter list,
            # treat it as a blocking error, then notify the error
            # and change the status of the strategy
            if check not in self.ignore_list:
                self.has_error = True
                _logger.error(
                    "Check '%s' failed for server '%s'" %
                    (check, server_name))
            else:
                # otherwise simply log the error (as info)
                _logger.info(
                    "Ignoring failed check '%s' for server '%s'" %
                    (check, server_name))
        else:
            _logger.debug(
                "Check '%s' succeeded for server '%s'" %
                (check, server_name))

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

    def result(self, server_name, status, hint=None, check=None):
        """
        Store the result of a check.
        Log any check result (error or debug level).
        Output the result to the user

        :param str server_name: the server being checked
        :param str check: the check name
        :param bool status: True if succeeded
        :param str,None hint: hint to print if not None:
        """
        check = self._check_name(check)
        super(CheckOutputStrategy, self).result(
            server_name, status, hint, check)
        # Send result to output
        output.result('check', server_name, check, status, hint)


class Server(RemoteStatusMixin):
    """
    This class represents the PostgreSQL server to backup.
    """

    XLOG_DB = "xlog.db"

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

        self.enforce_retention_policies = False
        self.postgres = None
        self.streaming = None
        self.archivers = []

        # Initialize the backup manager
        self.backup_manager = BackupManager(self)

        # Initialize the main PostgreSQL connection
        try:
            self.postgres = PostgreSQLConnection(config)
        # If the PostgreSQLConnection creation fails, disable the Server
        except ConninfoException as e:
            self.config.disabled = True
            self.config.msg_list.append("PostgreSQL connection: " +
                                        str(e).strip())

        # ARCHIVER_OFF_BACKCOMPATIBILITY - START OF CODE
        # IMPORTANT: This is a back-compatibility feature that has
        # been added in Barman 2.0. It highlights a deprecated
        # behaviour, and helps users during this transition phase.
        # It forces 'archiver=on' when both archiver and streaming_archiver
        # are set to 'off' (default values) and displays a warning,
        # requesting users to explicitly set the value in the configuration.
        # When this back-compatibility feature will be removed from Barman
        # (in a couple of major releases), developers will need to remove
        # this block completely and reinstate the block of code you find
        # a few lines below (search for ARCHIVER_OFF_BACKCOMPATIBILITY
        # throughout the code).
        if self.config.archiver is False and \
                self.config.streaming_archiver is False:
            output.warning("No archiver enabled for server '%s'. "
                           "Please turn on 'archiver', 'streaming_archiver' "
                           "or both", self.config.name)
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
                self.config.disabled = True
                self.config.msg_list.append('Unable to initialise the '
                                            'file based archiver')

        # Initialize the streaming PostgreSQL connection only when
        # backup_method is postgres or the streaming_archiver is in use
        if (self.config.backup_method == 'postgres' or
                self.config.streaming_archiver):
            try:
                self.streaming = StreamingConnection(config)
            # If the StreamingConnection creation fails, disable the server
            except ConninfoException as e:
                self.config.disabled = True
                self.config.msg_list.append("Streaming connection: " +
                                            str(e).strip())

        # Initialize the StreamingWalArchiver
        # WARNING: Order of items in self.archivers list is important!
        # The files will be archived in that order.
        if self.config.streaming_archiver:
            try:
                self.archivers.append(StreamingWalArchiver(
                    self.backup_manager))
            # If the StreamingWalArchiver creation fails,
            # disable the server
            except AttributeError as e:
                _logger.debug(e)
                self.config.disabled = True
                self.config.msg_list.append('Unable to initialise the '
                                            'streaming archiver')

        # IMPORTANT: The following lines of code have been
        # temporarily commented in order to make the code
        # back-compatible after the introduction of 'archiver=off'
        # as default value in Barman 2.0.
        # When the back compatibility feature for archiver will be
        # removed, the following lines need to be decommented.
        # ARCHIVER_OFF_BACKCOMPATIBILITY - START OF CODE
        # # At least one of the available archive modes should be enabled
        # if len(self.archivers) < 1:
        #     self.config.disabled = True
        #     self.config.msg_list.append(
        #         "No archiver enabled for server '%s'. "
        #         "Please turn on 'archiver', 'streaming_archiver' or both"
        #         % config.name)
        # ARCHIVER_OFF_BACKCOMPATIBILITY - END OF CODE

        # Sanity check: if file based archiver is disabled, and only
        # WAL streaming is enabled, a replication slot name must be configured.
        if not self.config.archiver and self.config.streaming_archiver and \
                self.config.slot_name is None:
            self.config.disabled = True
            self.config.msg_list.append(
                "Streaming-only archiver requires 'streaming_conninfo' and "
                "'slot_name' options to be properly configured")

        # Set bandwidth_limit
        if self.config.bandwidth_limit:
            try:
                self.config.bandwidth_limit = int(self.config.bandwidth_limit)
            except ValueError:
                _logger.warning('Invalid bandwidth_limit "%s" for server "%s" '
                                '(fallback to "0")' % (
                                    self.config.bandwidth_limit,
                                    self.config.name))
                self.config.bandwidth_limit = None

        # set tablespace_bandwidth_limit
        if self.config.tablespace_bandwidth_limit:
            rules = {}
            for rule in self.config.tablespace_bandwidth_limit.split():
                try:
                    key, value = rule.split(':', 1)
                    value = int(value)
                    if value != self.config.bandwidth_limit:
                        rules[key] = value
                except ValueError:
                    _logger.warning(
                        "Invalid tablespace_bandwidth_limit rule '%s'" % rule)
            if len(rules) > 0:
                self.config.tablespace_bandwidth_limit = rules
            else:
                self.config.tablespace_bandwidth_limit = None

        # Set minimum redundancy (default 0)
        if self.config.minimum_redundancy.isdigit():
            self.config.minimum_redundancy = int(
                self.config.minimum_redundancy)
            if self.config.minimum_redundancy < 0:
                _logger.warning('Negative value of minimum_redundancy "%s" '
                                'for server "%s" (fallback to "0")' % (
                                    self.config.minimum_redundancy,
                                    self.config.name))
                self.config.minimum_redundancy = 0
        else:
            _logger.warning('Invalid minimum_redundancy "%s" for server "%s" '
                            '(fallback to "0")' % (
                                self.config.minimum_redundancy,
                                self.config.name))
            self.config.minimum_redundancy = 0

        # Initialise retention policies
        self._init_retention_policies()

    def _init_retention_policies(self):

        # Set retention policy mode
        if self.config.retention_policy_mode != 'auto':
            _logger.warning(
                'Unsupported retention_policy_mode "%s" for server "%s" '
                '(fallback to "auto")' % (
                    self.config.retention_policy_mode, self.config.name))
            self.config.retention_policy_mode = 'auto'

        # If retention_policy is present, enforce them
        if self.config.retention_policy:
            # Check wal_retention_policy
            if self.config.wal_retention_policy != 'main':
                _logger.warning(
                    'Unsupported wal_retention_policy value "%s" '
                    'for server "%s" (fallback to "main")' % (
                        self.config.wal_retention_policy, self.config.name))
                self.config.wal_retention_policy = 'main'
            # Create retention policy objects
            try:
                rp = RetentionPolicyFactory.create(
                    self, 'retention_policy', self.config.retention_policy)
                # Reassign the configuration value (we keep it in one place)
                self.config.retention_policy = rp
                _logger.debug('Retention policy for server %s: %s' % (
                    self.config.name, self.config.retention_policy))
                try:
                    rp = RetentionPolicyFactory.create(
                        self, 'wal_retention_policy',
                        self.config.wal_retention_policy)
                    # Reassign the configuration value
                    # (we keep it in one place)
                    self.config.wal_retention_policy = rp
                    _logger.debug(
                        'WAL retention policy for server %s: %s' % (
                            self.config.name,
                            self.config.wal_retention_policy))
                except ValueError:
                    _logger.exception(
                        'Invalid wal_retention_policy setting "%s" '
                        'for server "%s" (fallback to "main")' % (
                            self.config.wal_retention_policy,
                            self.config.name))
                    rp = RetentionPolicyFactory.create(
                        self, 'wal_retention_policy', 'main')
                    self.config.wal_retention_policy = rp

                self.enforce_retention_policies = True

            except ValueError:
                _logger.exception(
                    'Invalid retention_policy setting "%s" for server "%s"' % (
                        self.config.retention_policy, self.config.name))

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
                # Check postgres configuration
                self.check_postgres(check_strategy)
                # Check barman directories from barman configuration
                self.check_directories(check_strategy)
                # Check retention policies
                self.check_retention_policy_settings(check_strategy)
                # Check for backup validity
                self.check_backup_validity(check_strategy)
                # Executes the backup manager set of checks
                self.backup_manager.check(check_strategy)
                # Check if the msg_list of the server
                # contains messages and output eventual failures
                self.check_configuration(check_strategy)

                # Executes check() for every archiver, passing
                # remote status information for efficiency
                for archiver in self.archivers:
                    archiver.check(check_strategy)

                # Check archiver errors
                self.check_archiver_errors(check_strategy)
        except TimeoutError:
            # The check timed out.
            # Add a failed entry to the check strategy for this.
            _logger.debug("Check command timed out executing '%s' check"
                          % check_strategy.running_check)
            check_strategy.result(self.config.name, False,
                                  hint='barman check command timed out',
                                  check='check timeout')

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
        if os.path.exists(self.xlogdb_file_name):
            with open(self.xlogdb_file_name, "rb") as fxlogdb:
                if os.fstat(fxlogdb.fileno()).st_size > 0:
                    xlogdb_empty = False

        # NOTE: This check needs to be only visible if it fails
        if xlogdb_empty:
            check_strategy.result(
                self.config.name, False,
                hint='please make sure WAL shipping is setup')

        # Check the number of wals in the incoming directory
        self._check_wal_queue(check_strategy,
                              'incoming',
                              'archiver')

        # Check the number of wals in the streaming directory
        self._check_wal_queue(check_strategy,
                              'streaming',
                              'streaming_archiver')

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
        for file_item in glob(os.path.join(incoming_dir, '*')):
            # Ignore temporary files
            if file_item.endswith('.tmp'):
                continue
            file_count += 1
        max_incoming_wal = self.config.max_incoming_wals_queue

        # Subtract one from the count because of .partial file inside the
        # streaming directory
        if dir_name == 'streaming':
            file_count -= 1

        # If this archiver is disabled, check the number of files in the
        # corresponding directory.
        # If the directory is NOT empty, fail the check and warn the user.
        # NOTE: This check is visible only when it fails
        check_strategy.init_check("empty %s directory" % dir_name)
        if not enabled:
            if file_count > 0:
                check_strategy.result(
                    self.config.name, False,
                    hint="'%s' must be empty when %s=off"
                         % (incoming_dir, archiver_name))
            # No more checks are required if the archiver
            # is not enabled
            return

        # At this point if max_wals_count is none,
        # means that no limit is set so we just need to return
        if max_incoming_wal is None:
            return
        check_strategy.init_check("%s WALs directory" % dir_name)
        if file_count > max_incoming_wal:
            msg = 'there are too many WALs in queue: ' \
                  '%s, max %s' % (file_count, max_incoming_wal)
            check_strategy.result(self.config.name, False, hint=msg)

    def check_postgres(self, check_strategy):
        """
        Checks PostgreSQL connection

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        check_strategy.init_check('PostgreSQL')
        # Take the status of the remote server
        remote_status = self.get_remote_status()
        if remote_status.get('server_txt_version'):
            check_strategy.result(self.config.name, True)
        else:
            check_strategy.result(self.config.name, False)
            return
        # Check for superuser privileges in PostgreSQL
        if remote_status.get('is_superuser') is not None:
            check_strategy.init_check('is_superuser')
            if remote_status.get('is_superuser'):
                check_strategy.result(
                    self.config.name, True)
            else:
                check_strategy.result(
                    self.config.name, False,
                    hint='superuser privileges for PostgreSQL '
                         'connection required',
                    check='not superuser'
                )

        if 'streaming_supported' in remote_status:
            check_strategy.init_check("PostgreSQL streaming")
            hint = None

            # If a streaming connection is available,
            # add its status to the output of the check
            if remote_status['streaming_supported'] is None:
                hint = remote_status['connection_error']
            elif not remote_status['streaming_supported']:
                hint = ('Streaming connection not supported'
                        ' for PostgreSQL < 9.2')
            check_strategy.result(self.config.name,
                                  remote_status.get('streaming'), hint=hint)
        # Check wal_level parameter: must be different from 'minimal'
        # the parameter has been introduced in postgres >= 9.0
        if 'wal_level' in remote_status:
            check_strategy.init_check("wal_level")
            if remote_status['wal_level'] != 'minimal':
                check_strategy.result(
                    self.config.name, True)
            else:
                check_strategy.result(
                    self.config.name, False,
                    hint="please set it to a higher level than 'minimal'")

        # Check the presence and the status of the configured replication slot
        # This check will be skipped if `slot_name` is undefined
        if self.config.slot_name:
            check_strategy.init_check("replication slot")
            slot = remote_status['replication_slot']
            # The streaming_archiver is enabled
            if self.config.streaming_archiver is True:
                # Error if PostgreSQL is too old
                if not remote_status['replication_slot_support']:
                    check_strategy.result(
                        self.config.name,
                        False,
                        hint="slot_name parameter set but PostgreSQL server "
                             "is too old (%s < 9.4)" %
                             remote_status['server_txt_version'])
                # Replication slots are supported
                else:
                    # The slot is not present
                    if slot is None:
                        check_strategy.result(
                            self.config.name, False,
                            hint="replication slot '%s' doesn't exist. "
                                 "Please execute 'barman receive-wal "
                                 "--create-slot %s'" % (self.config.slot_name,
                                                        self.config.name))
                    else:
                        # The slot is present but not initialised
                        if slot.restart_lsn is None:
                            check_strategy.result(
                                self.config.name, False,
                                hint="slot '%s' not initialised: is "
                                     "'receive-wal' running?" %
                                     self.config.slot_name)
                        # The slot is present but not active
                        elif slot.active is False:
                            check_strategy.result(
                                self.config.name, False,
                                hint="slot '%s' not active: is "
                                     "'receive-wal' running?" %
                                     self.config.slot_name)
                        else:
                            check_strategy.result(self.config.name,
                                                  True)
            else:
                # If the streaming_archiver is disabled and the slot_name
                # option is present in the configuration, we check that
                # a replication slot with the specified name is NOT present
                # and NOT active.
                # NOTE: This is not a failure, just a warning.
                if slot is not None:
                    if slot.restart_lsn \
                            is not None:
                        slot_status = 'initialised'

                        # Check if the slot is also active
                        if slot.active:
                            slot_status = 'active'

                        # Warn the user
                        check_strategy.result(
                            self.config.name,
                            True,
                            hint="WARNING: slot '%s' is %s but not required "
                                 "by the current config" % (
                                     self.config.slot_name, slot_status))

    def _make_directories(self):
        """
        Make backup directories in case they do not exist
        """
        for key in self.config.KEYS:
            if key.endswith('_directory') and hasattr(self.config, key):
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
                check_strategy.result(self.config.name, False,
                                      "%s: %s" % (e.filename, e.strerror))
            else:
                check_strategy.result(self.config.name, True)

    def check_configuration(self, check_strategy):
        """
        Check for error messages in the message list
        of the server and output eventual errors

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        check_strategy.init_check('configuration')
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
        if (self.config.retention_policy and
                not self.enforce_retention_policies):
            check_strategy.result(self.config.name, False, hint='see log')
        else:
            check_strategy.result(self.config.name, True)

    def check_backup_validity(self, check_strategy):
        """
        Check if backup validity requirements are satisfied

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        check_strategy.init_check('backup maximum age')
        # first check: check backup maximum age
        if self.config.last_backup_maximum_age is not None:
            # get maximum age information
            backup_age = self.backup_manager.validate_last_backup_maximum_age(
                self.config.last_backup_maximum_age)

            # format the output
            check_strategy.result(
                self.config.name, backup_age[0],
                hint="interval provided: %s, latest backup age: %s" % (
                    human_readable_timedelta(
                        self.config.last_backup_maximum_age), backup_age[1]))
        else:
            # last_backup_maximum_age provided by the user
            check_strategy.result(
                self.config.name,
                True,
                hint="no last_backup_maximum_age provided")

    def check_archiver_errors(self, check_strategy):
        """
        Checks the presence of archiving errors

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the check
        """
        check_strategy.init_check('archiver errors')
        if os.path.isdir(self.config.errors_directory):
            errors = os.listdir(self.config.errors_directory)
        else:
            errors = []

        check_strategy.result(
            self.config.name,
            len(errors) == 0,
            hint=WalArchiver.summarise_error_files(errors)
        )

    def status_postgres(self):
        """
        Status of PostgreSQL server
        """
        remote_status = self.get_remote_status()
        if remote_status['server_txt_version']:
            output.result('status', self.config.name,
                          "pg_version",
                          "PostgreSQL version",
                          remote_status['server_txt_version'])
        else:
            output.result('status', self.config.name,
                          "pg_version",
                          "PostgreSQL version",
                          "FAILED trying to get PostgreSQL version")
            return
        # Define the cluster state as pg_controldata do.
        if remote_status['is_in_recovery']:
            output.result('status', self.config.name, 'is_in_recovery',
                          'Cluster state', "in archive recovery")
        else:
            output.result('status', self.config.name, 'is_in_recovery',
                          'Cluster state', "in production")
        if remote_status['pgespresso_installed']:
            output.result('status', self.config.name, 'pgespresso',
                          'pgespresso extension', "Available")
        else:
            output.result('status', self.config.name, 'pgespresso',
                          'pgespresso extension', "Not available")
        if remote_status.get('current_size') is not None:
            output.result('status', self.config.name,
                          'current_size',
                          'Current data size',
                          pretty_size(remote_status['current_size']))
        if remote_status['data_directory']:
            output.result('status', self.config.name,
                          "data_directory",
                          "PostgreSQL Data directory",
                          remote_status['data_directory'])
        if remote_status['current_xlog']:
            output.result('status', self.config.name,
                          "current_xlog",
                          "Current WAL segment",
                          remote_status['current_xlog'])

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
            output.result('status', self.config.name,
                          "retention_policies",
                          "Retention policies",
                          "enforced "
                          "(mode: %s, retention: %s, WAL retention: %s)" % (
                              self.config.retention_policy_mode,
                              self.config.retention_policy,
                              self.config.wal_retention_policy))
        else:
            output.result('status', self.config.name,
                          "retention_policies",
                          "Retention policies",
                          "not enforced")

    def status(self):
        """
        Implements the 'server-status' command.
        """
        if self.config.description:
            output.result('status', self.config.name,
                          "description",
                          "Description", self.config.description)
        output.result('status', self.config.name,
                      "active",
                      "Active", self.config.active)
        output.result('status', self.config.name,
                      "disabled",
                      "Disabled", self.config.disabled)
        self.status_postgres()
        self.status_wal_archiver()
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
        remote_status = self.get_remote_status()
        result.update(remote_status)
        # Backup maximum age section
        if self.config.last_backup_maximum_age is not None:
            age = self.backup_manager.validate_last_backup_maximum_age(
                self.config.last_backup_maximum_age)
            # If latest backup is between the limits of the
            # last_backup_maximum_age configuration, display how old is
            # the latest backup.
            if age[0]:
                msg = "%s (latest backup: %s )" % \
                    (human_readable_timedelta(
                        self.config.last_backup_maximum_age),
                     age[1])
            else:
                # If latest backup is outside the limits of the
                # last_backup_maximum_age configuration (or the configuration
                # value is none), warn the user.
                msg = "%s (WARNING! latest backup is %s old)" % \
                    (human_readable_timedelta(
                        self.config.last_backup_maximum_age),
                     age[1])
            result['last_backup_maximum_age'] = msg
        else:
            result['last_backup_maximum_age'] = "None"
        output.result('show_server', self.config.name, result)

    def delete_backup(self, backup):
        """Deletes a backup

        :param backup: the backup to delete
        """
        try:
            # Lock acquisition: if you can acquire a ServerBackupLock
            # it means that no backup process is running on that server,
            # so there is no need to check the backup status.
            # Simply proceed with the normal delete process.
            server_backup_lock = ServerBackupLock(
                self.config.barman_lock_directory,
                self.config.name)
            server_backup_lock.acquire(server_backup_lock.raise_if_fail,
                                       server_backup_lock.wait)
            server_backup_lock.release()
            return self.backup_manager.delete_backup(backup)

        except LockFileBusy:
            # Otherwise if the lockfile is busy, a backup process is actually
            # running on that server. To be sure that it's safe
            # to delete the backup, we must check its status and its position
            # in the catalogue.
            # If it is the first and it is STARTED or EMPTY, we are trying to
            # remove a running backup. This operation must be forbidden.
            # Otherwise, normally delete the backup.
            first_backup_id = self.get_first_backup_id(BackupInfo.STATUS_ALL)
            if backup.backup_id == first_backup_id \
                    and backup.status in (BackupInfo.STARTED,
                                          BackupInfo.EMPTY):
                output.error("Cannot delete a running backup (%s %s)"
                             % (self.config.name, backup.backup_id))
            else:
                return self.backup_manager.delete_backup(backup)

        except LockFilePermissionDenied as e:
            # We cannot access the lockfile.
            # Exit without removing the backup.
            output.error("Permission denied, unable to access '%s'" % e)

    def backup(self):
        """
        Performs a backup for the server
        """
        try:
            # Default strategy for check in backup is CheckStrategy
            # This strategy does not print any output - it only logs checks
            strategy = CheckStrategy()
            self.check(strategy)
            if strategy.has_error:
                output.error("Impossible to start the backup. Check the log "
                             "for more details, or run 'barman check %s'"
                             % self.config.name)
                return
            # check required backup directories exist
            self._make_directories()
        except OSError as e:
            output.error('failed to create %s directory: %s',
                         e.filename, e.strerror)
            return

        # Make sure we are not wasting an precious streaming PostgreSQL
        # connection that may have been opened by the self.check() call
        if self.streaming:
            self.streaming.close()

        try:
            # lock acquisition and backup execution
            with ServerBackupLock(self.config.barman_lock_directory,
                                  self.config.name):
                self.backup_manager.backup()
            # Archive incoming WALs and update WAL catalogue
            self.archive_wal(verbose=False)

        except LockFileBusy:
            output.error("Another backup process is running")

        except LockFilePermissionDenied as e:
            output.error("Permission denied, unable to access '%s'" % e)

    def get_available_backups(
            self, status_filter=BackupManager.DEFAULT_STATUS_FILTER):
        """
        Get a list of available backups

        param: status_filter: the status of backups to return,
            default to BackupManager.DEFAULT_STATUS_FILTER
        """
        return self.backup_manager.get_available_backups(status_filter)

    def get_last_backup_id(
            self, status_filter=BackupManager.DEFAULT_STATUS_FILTER):
        """
        Get the id of the latest/last backup in the catalog (if exists)

        :param status_filter: The status of the backup to return,
            default to DEFAULT_STATUS_FILTER.
        :return string|None: ID of the backup
        """
        return self.backup_manager.get_last_backup_id(status_filter)

    def get_first_backup_id(
            self, status_filter=BackupManager.DEFAULT_STATUS_FILTER):
        """
        Get the id of the oldest/first backup in the catalog (if exists)

        :param status_filter: The status of the backup to return,
            default to DEFAULT_STATUS_FILTER.
        :return string|None: ID of the backup
        """
        return self.backup_manager.get_first_backup_id(status_filter)

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
            if backup.status == BackupInfo.DONE:
                try:
                    wal_info = self.get_wal_info(backup)
                    backup_size += wal_info['wal_size']
                    wal_size = wal_info['wal_until_next_size']
                except BadXlogSegmentName as e:
                    output.error(
                        "invalid WAL segment name %r\n"
                        "HINT: Please run \"barman rebuild-xlogdb %s\" "
                        "to solve this issue",
                        str(e), self.config.name)
                if self.enforce_retention_policies and \
                        retention_status[backup.backup_id] != BackupInfo.VALID:
                    rstatus = retention_status[backup.backup_id]
            output.result('list_backup', backup, backup_size, wal_size,
                          rstatus)

    def get_backup(self, backup_id):
        """
        Return the backup information for the given backup id.

        If the backup_id is None or backup.info file doesn't exists,
        it returns None.

        :param str|None backup_id: the ID of the backup to return
        :rtype: BackupInfo|None
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

    def get_required_xlog_files(self, backup, target_tli=None,
                                target_time=None, target_xid=None):
        """
        Get the xlog files required for a recovery
        """
        begin = backup.begin_wal
        end = backup.end_wal
        # If timeline isn't specified, assume it is the same timeline
        # of the backup
        if not target_tli:
            target_tli, _, _ = xlog.decode_segment_name(end)
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
                if tli > target_tli:
                    continue
                yield wal_info
                if wal_info.name > end:
                    end = wal_info.name
                    if target_time and target_time < wal_info.time:
                        break
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
        hash_dir = os.path.join(self.config.wals_directory,
                                xlog.hash_dir(wal_name))
        # Build the WAL file full path
        full_path = os.path.join(hash_dir, wal_name)
        return full_path

    def get_wal_info(self, backup_info):
        """
        Returns information about WALs for the given backup

        :param BackupInfo backup_info: the target backup
        """
        begin = backup_info.begin_wal
        end = backup_info.end_wal

        # counters
        wal_info = dict.fromkeys(
            ('wal_num', 'wal_size',
             'wal_until_next_num', 'wal_until_next_size',
             'wal_until_next_compression_ratio',
             'wal_compression_ratio'), 0)
        # First WAL (always equal to begin_wal) and Last WAL names and ts
        wal_info['wal_first'] = None
        wal_info['wal_first_timestamp'] = None
        wal_info['wal_last'] = None
        wal_info['wal_last_timestamp'] = None
        # WAL rate (default 0.0 per second)
        wal_info['wals_per_second'] = 0.0

        for item in self.get_wal_until_next_backup(backup_info):
            if item.name == begin:
                wal_info['wal_first'] = item.name
                wal_info['wal_first_timestamp'] = item.time
            if item.name <= end:
                wal_info['wal_num'] += 1
                wal_info['wal_size'] += item.size
            else:
                wal_info['wal_until_next_num'] += 1
                wal_info['wal_until_next_size'] += item.size
            wal_info['wal_last'] = item.name
            wal_info['wal_last_timestamp'] = item.time

        # Calculate statistics only for complete backups
        # If the cron is not running for any reason, the required
        # WAL files could be missing
        if wal_info['wal_first'] and wal_info['wal_last']:
            # Estimate WAL ratio
            # Calculate the difference between the timestamps of
            # the first WAL (begin of backup) and the last WAL
            # associated to the current backup
            wal_info['wal_total_seconds'] = (
                wal_info['wal_last_timestamp'] -
                wal_info['wal_first_timestamp'])
            if wal_info['wal_total_seconds'] > 0:
                wal_info['wals_per_second'] = (
                    float(wal_info['wal_num'] +
                          wal_info['wal_until_next_num']) /
                    wal_info['wal_total_seconds'])

            # evaluation of compression ratio for basebackup WAL files
            wal_info['wal_theoretical_size'] = \
                wal_info['wal_num'] * float(backup_info.xlog_segment_size)
            try:
                wal_info['wal_compression_ratio'] = 1 - (
                    wal_info['wal_size'] /
                    wal_info['wal_theoretical_size'])
            except ZeroDivisionError:
                wal_info['wal_compression_ratio'] = 0.0

            # evaluation of compression ratio of WAL files
            wal_info['wal_until_next_theoretical_size'] = \
                wal_info['wal_until_next_num'] * \
                float(backup_info.xlog_segment_size)
            try:
                wal_info['wal_until_next_compression_ratio'] = 1 - (
                    wal_info['wal_until_next_size'] /
                    wal_info['wal_until_next_theoretical_size'])
            except ZeroDivisionError:
                wal_info['wal_until_next_compression_ratio'] = 0.0

        return wal_info

    def recover(self, backup_info, dest, tablespaces=None, remote_command=None,
                **kwargs):
        """
        Performs a recovery of a backup

        :param barman.infofile.BackupInfo backup_info: the backup to recover
        :param str dest: the destination directory
        :param dict[str,str]|None tablespaces: a tablespace
            name -> location map (for relocation)
        :param str|None remote_command: default None. The remote command to
            recover the base backup, in case of remote backup.
        :kwparam str|None target_tli: the target timeline
        :kwparam str|None target_time: the target time
        :kwparam str|None target_xid: the target xid
        :kwparam str|None target_name: the target name created previously with
                            pg_create_restore_point() function call
        :kwparam bool|None target_immediate: end recovery as soon as
            consistency is reached
        :kwparam bool exclusive: whether the recovery is exclusive or not
        :kwparam str|None target_action: the recovery target action
        :kwparam bool|None standby_mode: the standby mode
        """
        return self.backup_manager.recover(
            backup_info, dest, tablespaces, remote_command, **kwargs)

    def get_wal(self, wal_name, compression=None, output_directory=None,
                peek=None):
        """
        Retrieve a WAL file from the archive

        :param str wal_name: id of the WAL file to find into the WAL archive
        :param str|None compression: compression format for the output
        :param str|None output_directory: directory where to deposit the
            WAL file
        :param int|None peek: if defined list the next N WAL file
        """

        # If used through SSH identify the client to add it to logs
        source_suffix = ''
        ssh_connection = os.environ.get('SSH_CONNECTION')
        if ssh_connection:
            # The client IP is the first value contained in `SSH_CONNECTION`
            # which contains four space-separated values: client IP address,
            # client port number, server IP address, and server port number.
            source_suffix = ' (SSH host: %s)' % (ssh_connection.split()[0],)

        # Sanity check
        if not xlog.is_any_xlog_file(wal_name):
            output.error("'%s' is not a valid wal file name%s",
                         wal_name, source_suffix)
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

                wal_peek_file = self.get_wal_full_path(wal_peek_name)

                # If the next WAL file is found, output the name
                # and continue to the next one
                if os.path.exists(wal_peek_file):
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

        # Get the WAL file full path
        wal_file = self.get_wal_full_path(wal_name)

        # Check for file existence
        if not os.path.exists(wal_file):
            output.error("WAL file '%s' not found in server '%s'%s",
                         wal_name, self.config.name, source_suffix)
            return

        # If an output directory was provided write the file inside it
        # otherwise we use standard output
        if output_directory is not None:
            destination_path = os.path.join(output_directory, wal_name)
            try:
                destination = open(destination_path, 'w')
                output.info(
                    "Writing WAL '%s' for server '%s' into '%s' file%s",
                    wal_name, self.config.name, destination_path,
                    source_suffix)
            except IOError as e:
                output.error("Unable to open '%s' file%s: %s",
                             destination_path, source_suffix, e)
                return
        else:
            destination = sys.stdout
            _logger.info(
                "Writing WAL '%s' for server '%s' to standard output%s",
                wal_name, self.config.name, source_suffix)

        # Get a decompressor for the file (None if not compressed)
        wal_compressor = self.backup_manager.compression_manager \
            .get_compressor(compression=identify_compression(wal_file))

        # Get a compressor for the output (None if not compressed)
        # Here we need to handle explicitly the None value because we don't
        # want it ot fallback to the configured compression
        if compression is not None:
            out_compressor = self.backup_manager.compression_manager \
                .get_compressor(compression=compression)
        else:
            out_compressor = None

        # Initially our source is the stored WAL file and we do not have
        # any temporary file
        source_file = wal_file
        uncompressed_file = None
        compressed_file = None

        # If the required compression is different from the source we
        # decompress/compress it into the required format (getattr is
        # used here to gracefully handle None objects)
        if getattr(wal_compressor, 'compression', None) != \
                getattr(out_compressor, 'compression', None):
            # If source is compressed, decompress it into a temporary file
            if wal_compressor is not None:
                uncompressed_file = NamedTemporaryFile(
                    dir=self.config.wals_directory,
                    prefix='.%s.' % wal_name,
                    suffix='.uncompressed')
                # decompress wal file
                wal_compressor.decompress(source_file, uncompressed_file.name)
                source_file = uncompressed_file.name

            # If output compression is required compress the source
            # into a temporary file
            if out_compressor is not None:
                compressed_file = NamedTemporaryFile(
                    dir=self.config.wals_directory,
                    prefix='.%s.' % wal_name,
                    suffix='.compressed')
                out_compressor.compress(source_file, compressed_file.name)
                source_file = compressed_file.name

        # Copy the prepared source file to destination
        with open(source_file) as input_file:
            shutil.copyfileobj(input_file, destination)

        # Remove temp files
        if uncompressed_file is not None:
            uncompressed_file.close()
        if compressed_file is not None:
            compressed_file.close()

    def cron(self, wals=True, retention_policies=True):
        """
        Maintenance operations

        :param bool wals: WAL archive maintenance
        :param bool retention_policies: retention policy maintenance
        """
        try:
            # Actually this is the highest level of locking in the cron,
            # this stops the execution of multiple cron on the same server
            with ServerCronLock(self.config.barman_lock_directory,
                                self.config.name):
                # WAL management and maintenance
                if wals:
                    # Execute the archive-wal sub-process
                    self.cron_archive_wal()
                    if self.config.streaming_archiver:
                        # Spawn the receive-wal sub-process
                        self.cron_receive_wal()
                    else:
                        # Terminate the receive-wal sub-process if present
                        self.kill('receive-wal', fail_if_not_present=False)
                # Retention policies execution
                if retention_policies:
                    self.backup_manager.cron_retention_policy()
        except LockFileBusy:
            output.info(
                "Another cron process is already running on server %s. "
                "Skipping to the next server" % self.config.name)
        except LockFilePermissionDenied as e:
            output.error("Permission denied, unable to access '%s'" % e)
        except (OSError, IOError) as e:
            output.error("%s", e)

    def cron_archive_wal(self):
        """
        Method that handles the start of an 'archive-wal' sub-process.

        This method must be run protected by ServerCronLock
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
                    self.config.barman_lock_directory,
                    self.config.name):
                # Output and release the lock immediately
                output.info("Starting WAL archiving for server %s",
                            self.config.name, log=False)

            # Init a Barman sub-process object
            archive_process = BarmanSubProcess(
                subcommand='archive-wal',
                config=barman.__config__.config_file,
                args=[self.config.name])
            # Launch the sub-process
            archive_process.execute()

        except LockFileBusy:
            # Another archive process is running for the server,
            # warn the user and skip to the next sever.
            output.info(
                "Another archive-wal process is already running "
                "on server %s. Skipping to the next server"
                % self.config.name)

    def cron_receive_wal(self):
        """
        Method that handles the start of a 'receive-wal' sub process

        This method must be run protected by ServerCronLock
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
                    self.config.barman_lock_directory,
                    self.config.name):
                # Output and release the lock immediately
                output.info("Starting streaming archiver "
                            "for server %s",
                            self.config.name, log=False)

            # Start a new receive-wal process
            receive_process = BarmanSubProcess(
                subcommand='receive-wal',
                config=barman.__config__.config_file,
                args=[self.config.name])
            # Launch the sub-process
            receive_process.execute()

        except LockFileBusy:
            # Another receive-wal process is running for the server
            # exit without message
            _logger.debug("Another STREAMING ARCHIVER process is running for "
                          "server %s" % self.config.name)

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
            with ServerWalArchiveLock(self.config.barman_lock_directory,
                                      self.config.name):
                self.backup_manager.archive_wal(verbose)
        except LockFileBusy:
            # If another process is running for this server,
            # warn the user and skip to the next server
            output.info("Another archive-wal process is already running "
                        "on server %s. Skipping to the next server"
                        % self.config.name)

    def create_physical_repslot(self):
        """
        Create a physical replication slot using the streaming connection
        """
        if not self.streaming:
            output.error("Unable to create a physical replication slot: "
                         "streaming connection not configured")
            return

        # Replication slots are not supported by PostgreSQL < 9.4
        try:
            if self.streaming.server_version < 90400:
                output.error("Unable to create a physical replication slot: "
                             "not supported by '%s' "
                             "(9.4 or higher is required)" %
                             self.streaming.server_major_version)
                return
        except PostgresException as exc:
            msg = "Cannot connect to server '%s'" % self.config.name
            output.error(msg, log=False)
            _logger.error("%s: %s", msg, str(exc).strip())
            return

        if not self.config.slot_name:
            output.error("Unable to create a physical replication slot: "
                         "slot_name configuration option required")
            return

        output.info(
            "Creating physical replication slot '%s' on server '%s'",
            self.config.slot_name,
            self.config.name)

        try:
            self.streaming.create_physical_repslot(self.config.slot_name)
            output.info("Replication slot '%s' created", self.config.slot_name)
        except PostgresDuplicateReplicationSlot:
            output.error("Replication slot '%s' already exists",
                         self.config.slot_name)
        except PostgresReplicationSlotsFull:
            output.error("All replication slots for server '%s' are in use\n"
                         "Free one or increase the max_replication_slots "
                         "value on your PostgreSQL server.",
                         self.config.name)
        except PostgresException as exc:
            output.error(
                "Cannot create replication slot '%s' on server '%s': %s",
                self.config.slot_name,
                self.config.name,
                str(exc).strip())

    def drop_repslot(self):
        """
        Drop a replication slot using the streaming connection
        """
        if not self.streaming:
            output.error("Unable to drop a physical replication slot: "
                         "streaming connection not configured")
            return

        # Replication slots are not supported by PostgreSQL < 9.4
        try:
            if self.streaming.server_version < 90400:
                output.error("Unable to drop a physical replication slot: "
                             "not supported by '%s' (9.4 or higher is "
                             "required)" %
                             self.streaming.server_major_version)
                return
        except PostgresException as exc:
            msg = "Cannot connect to server '%s'" % self.config.name
            output.error(msg, log=False)
            _logger.error("%s: %s", msg, str(exc).strip())
            return

        if not self.config.slot_name:
            output.error("Unable to drop a physical replication slot: "
                         "slot_name configuration option required")
            return

        output.info(
            "Dropping physical replication slot '%s' on server '%s'",
            self.config.slot_name,
            self.config.name)

        try:
            self.streaming.drop_repslot(self.config.slot_name)
            output.info("Replication slot '%s' dropped", self.config.slot_name)
        except PostgresInvalidReplicationSlot:
            output.error("Replication slot '%s' does not exist",
                         self.config.slot_name)
        except PostgresReplicationSlotInUse as exc:
            output.error(
                "Cannot drop replication slot '%s' on server '%s' "
                "because it is in use.",
                self.config.slot_name,
                self.config.name)
        except PostgresException as exc:
            output.error(
                "Cannot drop replication slot '%s' on server '%s': %s",
                self.config.slot_name,
                self.config.name,
                str(exc).strip())

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
            output.error("Unable to start receive-wal process: "
                         "streaming_archiver option set to 'off' in "
                         "barman configuration file")
            return

        output.info("Starting receive-wal for server %s", self.config.name)
        try:
            # Take care of the receive-wal lock.
            # Only one receiving process per server is permitted
            with ServerWalReceiveLock(self.config.barman_lock_directory,
                                      self.config.name):
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
                output.info("Unable to reset the status of receive-wal "
                            "for server %s. Process is still running"
                            % self.config.name)
            else:
                output.info("Another receive-wal process is already running "
                            "for server %s." % self.config.name)

    @property
    def xlogdb_file_name(self):
        """
        The name of the file containing the XLOG_DB
        :return str: the name of the file that contains the XLOG_DB
        """
        return os.path.join(self.config.wals_directory, self.XLOG_DB)

    @contextmanager
    def xlogdb(self, mode='r'):
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
        if not os.path.exists(self.config.wals_directory):
            os.makedirs(self.config.wals_directory)
        xlogdb = self.xlogdb_file_name

        with ServerXLOGDBLock(self.config.barman_lock_directory,
                              self.config.name):
            # If the file doesn't exist and it is required to read it,
            # we open it in a+ mode, to be sure it will be created
            if not os.path.exists(xlogdb) and mode.startswith('r'):
                if '+' not in mode:
                    mode = "a%s+" % mode[1:]
                else:
                    mode = "a%s" % mode[1:]

            with open(xlogdb, mode) as f:

                # execute the block nested in the with statement
                try:
                    yield f

                finally:
                    # we are exiting the context
                    # if file is writable (mode contains w, a or +)
                    # make sure the data is written to disk
                    # http://docs.python.org/2/library/os.html#os.fsync
                    if any((c in 'wa+') for c in f.mode):
                        f.flush()
                        os.fsync(f.fileno())

    def report_backups(self):
        if not self.enforce_retention_policies:
            return dict()
        else:
            return self.config.retention_policy.report()

    def rebuild_xlogdb(self):
        """
        Rebuild the whole xlog database guessing it from the archive content.
        """
        return self.backup_manager.rebuild_xlogdb()

    def get_backup_ext_info(self, backup_info):
        """
        Return a dictionary containing all available information about a backup

        The result is equivalent to the sum of information from

         * BackupInfo object
         * the Server.get_wal_info() return value
         * the context in the catalog (if available)
         * the retention policy status

        :param backup_info: the target backup
        :rtype dict: all information about a backup
        """
        backup_ext_info = backup_info.to_dict()
        if backup_info.status == BackupInfo.DONE:
            try:
                previous_backup = self.backup_manager.get_previous_backup(
                    backup_ext_info['backup_id'])
                next_backup = self.backup_manager.get_next_backup(
                    backup_ext_info['backup_id'])
                if previous_backup:
                    backup_ext_info[
                        'previous_backup_id'] = previous_backup.backup_id
                else:
                    backup_ext_info['previous_backup_id'] = None
                if next_backup:
                    backup_ext_info['next_backup_id'] = next_backup.backup_id
                else:
                    backup_ext_info['next_backup_id'] = None
            except UnknownBackupIdException:
                # no next_backup_id and previous_backup_id items
                # means "Not available"
                pass
            backup_ext_info.update(self.get_wal_info(backup_info))
            if self.enforce_retention_policies:
                policy = self.config.retention_policy
                backup_ext_info['retention_policy_status'] = \
                    policy.backup_status(backup_info.backup_id)
            else:
                backup_ext_info['retention_policy_status'] = None

            # Check any child timeline exists
            children_timelines = self.get_children_timelines(
                backup_ext_info['timeline'],
                forked_after=backup_info.end_xlog)

            backup_ext_info['children_timelines'] = \
                children_timelines

        return backup_ext_info

    def show_backup(self, backup_info):
        """
        Output all available information about a backup

        :param backup_info: the target backup
        """
        try:
            backup_ext_info = self.get_backup_ext_info(backup_info)
            output.result('show_backup', backup_ext_info)
        except BadXlogSegmentName as e:
            output.error(
                "invalid xlog segment name %r\n"
                "HINT: Please run \"barman rebuild-xlogdb %s\" "
                "to solve this issue",
                str(e), self.config.name)
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
        sys_path = os.environ.get('PATH')
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
                output.info('Stopped process %s(%s)',
                            process.task, process.pid)
                return
            else:
                output.error('Cannot terminate process %s(%s)',
                             process.task, process.pid)
                return
        if fail_if_not_present:
            output.error('Termination of %s failed: '
                         'no such process for server %s',
                         task, self.config.name)

    def switch_wal(self, force=False, archive=None, archive_timeout=None):
        """
        Execute the switch-wal command on the target server
        """
        try:

            if force:
                # If called with force, execute a checkpoint before the
                # switch_wal command
                _logger.info('Force a CHECKPOINT before pg_switch_wal()')
                self.postgres.checkpoint()

            # Perform the switch_wal. expect a WAL name only if the switch
            # has been successfully executed, False otherwise.
            closed_wal = self.postgres.switch_wal()
            if closed_wal is None:
                # Something went wrong during the execution of the
                # pg_switch_wal command
                output.error("Unable to perform pg_switch_wal "
                             "for server '%s'." % self.config.name)
                return
            if closed_wal:
                # The switch_wal command have been executed successfully
                output.info(
                    "The WAL file %s has been closed on server '%s'" %
                    (closed_wal, self.config.name))
                # If the user has asked to wait for a WAL file to be archived,
                # wait until a new WAL file has been found
                # or the timeout has expired
                if archive:
                    output.info(
                        "Waiting for the WAL file %s from server '%s' "
                        "(max: %s seconds)",
                        closed_wal, self.config.name, archive_timeout)
                    # Wait for a new file until end_time
                    end_time = time.time() + archive_timeout
                    while time.time() < end_time:
                        self.backup_manager.archive_wal(verbose=False)

                        # Finish if the closed wal file is in the archive.
                        if os.path.exists(self.get_wal_full_path(closed_wal)):
                                break

                        # sleep a bit before retrying
                        time.sleep(.1)
                    else:
                        output.error("The WAL file %s has not been received "
                                     "in %s seconds",
                                     closed_wal, archive_timeout)

            else:
                # Is not necessary to perform a switch_wal
                output.info("No switch required for server '%s'" %
                            self.config.name)
        except PostgresIsInRecovery:
            output.info("No switch performed because server '%s' "
                        "is a standby." % self.config.name)
        except PostgresSuperuserRequired:
            # Superuser rights are required to perform the switch_wal
            output.error("Barman switch-wal requires superuser rights")

    def replication_status(self, target='all'):
        """
        Implements the 'replication-status' command.
        """
        if target == 'hot-standby':
            client_type = PostgreSQLConnection.STANDBY
        elif target == 'wal-streamer':
            client_type = PostgreSQLConnection.WALSTREAMER
        else:
            client_type = PostgreSQLConnection.ANY_STREAMING_CLIENT
        try:
            standby_info = self.postgres.get_replication_stats(client_type)
            if standby_info is None:
                output.error('Unable to connect to server %s' %
                             self.config.name)
            else:
                output.result('replication_status', self.config.name,
                              target, self.postgres.current_xlog_location,
                              standby_info)
        except PostgresUnsupportedFeature as e:
            output.info("  Requires PostgreSQL %s or higher", e)
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
            history_path = os.path.join(self.config.wals_directory,
                                        "%08X.history" % children_tli)
            # If the file doesn't exists, stop searching
            if not os.path.exists(history_path):
                break

            # Create the WalFileInfo object using the file
            wal_info = WalFileInfo.from_file(history_path)
            # Get content of the file. We need to pass a compressor manager
            # here to handle an eventual compression of the history file
            history_info = xlog.decode_history_file(
                wal_info,
                self.backup_manager.compression_manager)

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
