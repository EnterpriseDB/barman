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
This module represents a Server.
Barman is able to manage multiple servers.
"""

import itertools
import logging
import os
import shutil
import sys
from collections import namedtuple
from contextlib import contextmanager
from tempfile import NamedTemporaryFile

import barman
from barman import output, xlog
from barman.backup import BackupManager
from barman.command_wrappers import BarmanSubProcess
from barman.compression import identify_compression
from barman.infofile import BackupInfo, UnknownBackupIdException, WalFileInfo
from barman.lockfile import (LockFileBusy, LockFilePermissionDenied,
                             ServerBackupLock, ServerCronLock,
                             ServerWalArchiveLock, ServerWalReceiveLock,
                             ServerXLOGDBLock)
from barman.postgres import (ConninfoException, PostgresIsInRecovery,
                             PostgreSQLConnection, PostgresSuperuserRequired,
                             PostgresUnsupportedFeature, StreamingConnection)
from barman.process import ProcessManager
from barman.remote_status import RemoteStatusMixin
from barman.retention_policies import RetentionPolicyFactory
from barman.utils import human_readable_timedelta, pretty_size
from barman.wal_archiver import (ArchiverFailure, FileWalArchiver,
                                 StreamingWalArchiver, WalArchiver)

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
                           'archiver errors']

    def __init__(self, ignore_checks=NON_CRITICAL_CHECKS):
        """
        Silent Strategy constructor

        :param list ignore_checks: list of checks that can be ignored
        """
        self.ignore_list = ignore_checks
        self.check_result = []
        self.has_error = False

    def result(self, server_name, check, status, hint=None):
        """
        Store the result of a check (with no output).
        Log any check result (error or debug level).

        :param str server_name: the server is being checked
        :param str check: the check name
        :param bool status: True if succeeded
        :param str,None hint: hint to print if not None:
        """
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

    def result(self, server_name, check, status, hint=None):
        """
        Output Strategy constructor

        :param str server_name: the server being checked
        :param str check: the check name
        :param bool status: True if succeeded
        :param str,None hint: hint to print if not None:
        """
        # Call the basic method
        super(CheckOutputStrategy, self).result(
            server_name, check, status, hint)
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
        self.backup_manager = BackupManager(self)
        self.enforce_retention_policies = False
        self.postgres = None
        self.streaming = None
        self.archivers = []

        try:
            self.postgres = PostgreSQLConnection(config)
        # If the PostgreSQLConnection creation fails, disable the Server
        except ConninfoException as e:
            self.config.disabled = True
            self.config.msg_list.append("conninfo: " + str(e).strip())

        # Order of items in self.archivers list is important!
        # The files will be archived in that order.
        if self.config.archiver:
            try:
                self.archivers.append(FileWalArchiver(self.backup_manager))
            except AttributeError as e:
                _logger.debug(e)
                self.config.disabled = True
                self.config.msg_list.append('Unable to initialise the '
                                            'file based archiver')
        else:
            # Currently a server MUST have archiver set to on,
            # otherwise disable the server.
            self.config.disabled = True
            self.config.msg_list.append("The option archiver = off "
                                        "is not yet supported")

        if self.config.streaming_archiver:
            try:
                self.streaming = StreamingConnection(config)
                self.archivers.append(StreamingWalArchiver(
                    self.backup_manager))
            # If the StreamingConnection creation fails, disable the Server
            except ConninfoException as e:
                self.config.disabled = True
                self.config.msg_list.append("streaming_conninfo: " +
                                            str(e).strip())
            except AttributeError as e:
                _logger.debug(e)
                self.config.disabled = True
                self.config.msg_list.append('Unable to initialise the '
                                            'streaming archiver')
        if len(self.archivers) < 1:
            self.config.disabled = True
            self.config.msg_list.append(
                "Missing archiver for server %s. "
                "Enable at least the 'archiver' option in "
                "the server configuration"
                % config.name)

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

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
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

    def check_archive(self, check_strategy):
        """
        Checks WAL archive

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        # Check WAL archiving has been setup
        # NOTE: This check needs to be only visible if it fails
        with self.xlogdb() as fxlogdb:
            if os.fstat(fxlogdb.fileno()).st_size == 0:
                check_strategy.result(
                    self.config.name, 'WAL archive', False,
                    'please make sure WAL shipping is setup')

    def check_postgres(self, check_strategy):
        """
        Checks PostgreSQL connection

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        # Take the status of the remote server
        remote_status = self.get_remote_status()
        if remote_status.get('server_txt_version'):
            check_strategy.result(self.config.name, 'PostgreSQL', True)
        else:
            check_strategy.result(self.config.name, 'PostgreSQL', False)
            return
        # Check for superuser privileges in PostgreSQL
        if remote_status.get('is_superuser') is not None:
            if remote_status.get('is_superuser'):
                check_strategy.result(
                    self.config.name, 'superuser', True)
            else:
                check_strategy.result(
                    self.config.name, 'not superuser', False,
                    'superuser privileges for PostgreSQL connection required')

        if 'streaming_supported' in remote_status:
            hint = None

            # If a streaming connection is available,
            # add its status to the output of the check
            if remote_status['streaming_supported'] is None:
                hint = 'Streaming connection error'
            elif not remote_status['streaming_supported']:
                hint = ('Streaming connection not supported'
                        ' for PostgreSQL < 9.2')
            check_strategy.result(self.config.name, 'PostgreSQL streaming',
                                  remote_status.get('streaming'), hint)
        # Check wal_level parameter: must be different from 'minimal'
        # the parameter has been introduced in postgres >= 9.0
        if 'wal_level' in remote_status:
            if remote_status['wal_level'] != 'minimal':
                check_strategy.result(
                    self.config.name, 'wal_level', True)
            else:
                check_strategy.result(
                    self.config.name, 'wal_level', False,
                    "please set it to a higher level than 'minimal'")

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
        if not self.config.disabled:
            try:
                self._make_directories()
            except OSError as e:
                check_strategy.result(self.config.name, 'directories', False,
                                      "%s: %s" % (e.filename, e.strerror))
            else:
                check_strategy.result(self.config.name, 'directories', True)

    def check_configuration(self, check_strategy):
        """
        Check for error messages in the message list
        of the server and output eventual errors

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        if self.config.disabled:
            check_strategy.result(self.config.name, 'configuration', False)
            for conflict_paths in self.config.msg_list:
                output.info("\t\t%s" % conflict_paths)

    def check_retention_policy_settings(self, check_strategy):
        """
        Checks retention policy setting

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        if (self.config.retention_policy and
                not self.enforce_retention_policies):
            check_strategy.result(self.config.name,
                                  'retention policy settings', False,
                                  'see log')
        else:
            check_strategy.result(self.config.name,
                                  'retention policy settings', True)

    def check_backup_validity(self, check_strategy):
        """
        Check if backup validity requirements are satisfied

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        # first check: check backup maximum age
        if self.config.last_backup_maximum_age is not None:
            # get maximum age information
            backup_age = self.backup_manager.validate_last_backup_maximum_age(
                self.config.last_backup_maximum_age)

            # format the output
            check_strategy.result(
                self.config.name, 'backup maximum age',
                backup_age[0],
                "interval provided: %s, latest backup age: %s" % (
                    human_readable_timedelta(
                        self.config.last_backup_maximum_age), backup_age[1]))
        else:
            # last_backup_maximum_age provided by the user
            check_strategy.result(
                self.config.name, 'backup maximum age', True,
                "no last_backup_maximum_age provided")

    def check_archiver_errors(self, check_strategy):
        """
        Checks the presence of archiving errors

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the check
        """

        if os.path.isdir(self.config.errors_directory):
            errors = os.listdir(self.config.errors_directory)
        else:
            errors = []

        check_strategy.result(
            self.config.name,
            "archiver errors",
            len(errors) == 0,
            WalArchiver.summarise_error_files(errors)
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
        output.result('status', self.config.name,
                      "archive_command",
                      "PostgreSQL 'archive_command' setting",
                      remote_status['archive_command'] or
                      "FAILED (please set it accordingly to documentation)")
        last_wal = remote_status.get('last_archived_wal')
        # If PostgreSQL is >= 9.4 we have the last_archived_time
        if last_wal and remote_status.get('last_archived_time'):
                last_wal += ", at %s" % (
                    remote_status['last_archived_time'].ctime())
        output.result('status', self.config.name,
                      "last_archived_wal",
                      "Last archived WAL",
                      last_wal or "No WAL segment shipped yet")
        if remote_status['current_xlog']:
            output.result('status', self.config.name,
                          "current_xlog",
                          "Current WAL segment",
                          remote_status['current_xlog'])
        # Set output for WAL archive failures (PostgreSQL >= 9.4)
        if remote_status.get('failed_count') is not None:
            remote_fail = str(remote_status['failed_count'])
            if int(remote_status['failed_count']) > 0:
                remote_fail += " (%s at %s)" % (
                    remote_status['last_failed_wal'],
                    remote_status['last_failed_time'].ctime())
            output.result('status', self.config.name, 'failed_count',
                          'Failures of WAL archiver', remote_fail)
        # Add hourly archive rate if available (PostgreSQL >= 9.4) and > 0
        if remote_status.get('current_archived_wals_per_second'):
            output.result(
                'status', self.config.name,
                'server_archived_wals_per_hour',
                'Server WAL archiving rate', '%0.2f/hour' % (
                    3600 * remote_status['current_archived_wals_per_second']))

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
        result = dict([
            (key, getattr(self.config, key))
            for key in self.config.KEYS
        ])
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
                except xlog.BadXlogSegmentName as e:
                    output.error(
                        "invalid xlog segment name %r\n"
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
                wal_info['wal_num'] * float(xlog.XLOG_SEG_SIZE)
            try:
                wal_info['wal_compression_ratio'] = 1 - (
                    wal_info['wal_size'] /
                    wal_info['wal_theoretical_size'])
            except ZeroDivisionError:
                wal_info['wal_compression_ratio'] = 0.0

            # evaluation of compression ratio of WAL files
            wal_info['wal_until_next_theoretical_size'] = \
                wal_info['wal_until_next_num'] * float(xlog.XLOG_SEG_SIZE)
            try:
                wal_info['wal_until_next_compression_ratio'] = 1 - (
                    wal_info['wal_until_next_size'] /
                    wal_info['wal_until_next_theoretical_size'])
            except ZeroDivisionError:
                wal_info['wal_until_next_compression_ratio'] = 0.0

        return wal_info

    def recover(self, backup_info, dest, tablespaces=None, target_tli=None,
                target_time=None, target_xid=None, target_name=None,
                exclusive=False, remote_command=None):
        """
        Performs a recovery of a backup

        :param barman.infofile.BackupInfo backup_info: the backup to recover
        :param str dest: the destination directory
        :param dict[str,str]|None tablespaces: a tablespace
            name -> location map (for relocation)
        :param str|None target_tli: the target timeline
        :param str|None target_time: the target time
        :param str|None target_xid: the target xid
        :param str|None target_name: the target name created previously with
                            pg_create_restore_point() function call
        :param bool exclusive: whether the recovery is exclusive or not
        :param str|None remote_command: default None. The remote command to
            recover the base backup, in case of remote backup.
        """
        return self.backup_manager.recover(
            backup_info, dest, tablespaces, target_tli, target_time,
            target_xid, target_name, exclusive, remote_command)

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

        # Sanity check
        if not xlog.is_any_xlog_file(wal_name):
            output.error("'%s' is not a valid wal file name", wal_name)
            return

        # If peek is requested we only output a list of files
        if peek:
            # Get the next ``peek`` files following the provided ``wal_name``.
            # If ``wal_name`` is not a simple wal file,
            # we cannot guess the names of the following WAL files.
            # So ``wal_name`` is the only possible result, if exists.
            if xlog.is_wal_file(wal_name):
                wal_peek_list = itertools.islice(
                    xlog.generate_segment_names(wal_name), peek)
            else:
                wal_peek_list = [wal_name]
            # Output the content of wal_peek_list until we find a missing file
            for wal_peek_name in wal_peek_list:
                wal_peek_file = self.get_wal_full_path(wal_peek_name)
                # If ``wal_peek_file`` doesn't exist, stop the process
                if not os.path.exists(wal_peek_file):
                    return
                output.info(wal_peek_name, log=False)
            # Do not output anything else
            return

        # Get the WAL file full path
        wal_file = self.get_wal_full_path(wal_name)

        # Check for file existence
        if not os.path.exists(wal_file):
            output.error("WAL file '%s' not found in server '%s'",
                         wal_name, self.config.name)
            return

        # If an output directory was provided write the file inside it
        # otherwise we use standard output
        if output_directory is not None:
            destination_path = os.path.join(output_directory, wal_name)
            try:
                destination = open(destination_path, 'w')
                output.info("Writing WAL '%s' for server '%s' into '%s' file",
                            wal_name, self.config.name, destination_path)
            except IOError as e:
                output.error("Unable to open '%s' file: %s" %
                             destination_path, e)
                return
        else:
            destination = sys.stdout

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
                    output.error("Impossible to start a receive-wal process "
                                 "for server %s: %s" % (self.config.name, e))
        except LockFileBusy:
            # If another process is running for this server,
            if reset:
                output.info("Unable to reset the status of receive-wal "
                            "for server %s. Process is still running"
                            % self.config.name)
            else:
                output.info("Another receive-wal process is already running "
                            "for server %s." % self.config.name)

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
        xlogdb = os.path.join(self.config.wals_directory, self.XLOG_DB)

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
        return backup_ext_info

    def show_backup(self, backup_info):
        """
        Output all available information about a backup

        :param backup_info: the target backup
        """
        try:
            backup_ext_info = self.get_backup_ext_info(backup_info)
            output.result('show_backup', backup_ext_info)
        except xlog.BadXlogSegmentName as e:
            output.error(
                "invalid xlog segment name %r\n"
                "HINT: Please run \"barman rebuild-xlogdb %s\" "
                "to solve this issue" %
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

    def switch_xlog(self, force=False):
        """
        Execute the switch-xlog command on the target server
        """
        try:
            if force:
                # If called with force, execute a checkpoint before the
                # switch_xlog command
                _logger.info('Force a CHECKPOINT before pg_switch_xlog()')
                self.postgres.checkpoint()

            # Perform the switch_xlog. expect a WAL name only if the switch
            # has been successfully executed, False otherwise.
            switch_xlogfile = self.postgres.switch_xlog()
            if switch_xlogfile is None:
                # Something went wrong during the execution of the
                # pg_switch_xlog command
                output.error("Unable to perform pg_switch_xlog "
                             "for server '%s'." % self.config.name)
                return
            if switch_xlogfile:
                # The switch_xlog command have been executed successfully
                output.info(
                    "Switch to %s for server '%s'" %
                    (switch_xlogfile, self.config.name))
            else:
                # Is not necessary to perform a switch_xlog
                output.info("No switch required for server '%s'" %
                            self.config.name)
        except PostgresIsInRecovery:
            output.info("No switch performed because server '%s' "
                        "is a standby." % self.config.name)
        except PostgresSuperuserRequired:
            # Superuser rights are required to perform the switch_xlog
            output.error("Barman switch-xlog requires superuser rights")

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
