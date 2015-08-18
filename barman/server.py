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
This module represents a Server.
Barman is able to manage multiple servers.
"""

from collections import namedtuple
from contextlib import contextmanager
import logging
import os
import shutil
import sys
from tempfile import NamedTemporaryFile

import psycopg2
from psycopg2.extras import RealDictCursor

from barman import output
from barman.backup import BackupManager
from barman.compression import identify_compression
from barman.infofile import BackupInfo, UnknownBackupIdException, Tablespace, \
    WalFileInfo
from barman.lockfile import LockFileBusy, LockFilePermissionDenied, \
    ServerBackupLock, ServerCronLock, ServerXLOGDBLock
from barman.retention_policies import RetentionPolicyFactory
from barman.utils import human_readable_timedelta
import barman.xlog as xlog


_logger = logging.getLogger(__name__)


class ConninfoException(Exception):
    """
    Error parsing conninfo parameter
    """


class PostgresConnectionError(Exception):
    """
    Error connecting to PostgreSQL server.
    """


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

    def __init__(self):
        """
        Silent Strategy constructor
        """
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
            self.has_error = True
            _logger.error(
                "Check '%s' failed for server '%s'" %
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
        super(CheckOutputStrategy, self).__init__()

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


class Server(object):
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
        self.config = config
        self._conn = None
        self.server_txt_version = None
        self.server_version = None
        if self.config.conninfo is None:
            raise ConninfoException(
                'Missing conninfo parameter in barman configuration '
                'for server %s' % config.name)
        self.backup_manager = BackupManager(self)
        self.configuration_files = None
        self.enforce_retention_policies = False

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
            self.config.minimum_redundancy = int(self.config.minimum_redundancy)
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
                    # Reassign the configuration value (we keep it in one place)
                    self.config.wal_retention_policy = rp
                    _logger.debug(
                        'WAL retention policy for server %s: %s' % (
                            self.config.name, self.config.wal_retention_policy))
                except ValueError:
                    _logger.exception(
                        'Invalid wal_retention_policy setting "%s" '
                        'for server "%s" (fallback to "main")' % (
                            self.config.wal_retention_policy, self.config.name))
                    rp = RetentionPolicyFactory.create(
                        self, 'wal_retention_policy', 'main')
                    self.config.wal_retention_policy = rp

                self.enforce_retention_policies = True

            except ValueError:
                _logger.exception(
                    'Invalid retention_policy setting "%s" for server "%s"' % (
                        self.config.retention_policy, self.config.name))

    def check(self, check_strategy=__default_check_strategy):
        """
        Implements the 'server check' command and makes sure SSH and PostgreSQL
        connections work properly. It checks also that backup directories exist
        (and if not, it creates them).

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
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

    def check_postgres(self, check_strategy):
        """
        Checks PostgreSQL connection

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        # Take the status of the remote server
        try:
            remote_status = self.get_remote_status()
        except PostgresConnectionError:
            remote_status = None
        if remote_status is not None and remote_status['server_txt_version']:
            check_strategy.result(self.config.name, 'PostgreSQL', True)
        else:
            check_strategy.result(self.config.name, 'PostgreSQL', False)
            return
        # Check archive_mode parameter: must be on
        if remote_status['archive_mode'] == 'on':
            check_strategy.result(self.config.name, 'archive_mode', True)
        else:
            check_strategy.result(self.config.name, 'archive_mode', False,
                                  "please set it to 'on'")
        # Check wal_level parameter: must be different to 'minimal'
        if remote_status['wal_level'] != 'minimal':
            check_strategy.result(
                self.config.name, 'wal_level', True)
        else:
            check_strategy.result(
                self.config.name, 'wal_level', False,
                "please set it to a higher level than 'minimal'")

        if remote_status['archive_command'] and \
                remote_status['archive_command'] != '(disabled)':
            check_strategy.result(self.config.name, 'archive_command', True)

            # Report if the archiving process works without issues.
            # Skip if the archive_command check fails
            # It can be None if PostgreSQL is older than 9.4
            if remote_status.get('is_archiving') is not None:
                check_strategy.result(self.config.name, 'continuous archiving',
                                      remote_status['is_archiving'])
        else:
            check_strategy.result(self.config.name, 'archive_command', False,
                                  'please set it accordingly to documentation')

    def _make_directories(self):
        """
        Make backup directories in case they do not exist
        """
        for key in self.config.KEYS:
            if key.endswith('_directory') and hasattr(self.config, key):
                val = getattr(self.config, key)
                if val is not None and not os.path.isdir(val):
                    #noinspection PyTypeChecker
                    os.makedirs(val)

    def check_directories(self, check_strategy):
        """
        Checks backup directories and creates them if they do not exist

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """

        if self.config.disabled:
            check_strategy.result(self.config.name, 'directories', False)
            for conflict_paths in self.config.msg_list:
                output.info("\t%s" % conflict_paths)
        else:
            try:
                self._make_directories()
            except OSError, e:
                check_strategy.result(self.config.name, 'directories', False,
                                      "%s: %s" % (e.filename, e.strerror))
            else:
                check_strategy.result(self.config.name, 'directories', True)

    def check_retention_policy_settings(self, check_strategy):
        """
        Checks retention policy setting

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        if self.config.retention_policy and not self.enforce_retention_policies:
            check_strategy.result(self.config.name, 'retention policy settings',
                                  False, 'see log')
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

    def pgespresso_installed(self):
        """
        Returns true if pgexpresso extension is available
        """
        try:
            with self.pg_connect() as conn:
                # pg_extension is only available from Postgres 9.1+
                if self.server_version < 90100:
                    return False
                cur = conn.cursor()
                cur.execute("select count(*) from pg_extension "
                            "where extname = 'pgespresso'")
                q_result = cur.fetchone()[0]
                if q_result > 0:
                    return True
                else:
                    return False
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving pgespresso information: %s", e)
            return False

    def get_pg_stat_archiver(self):
        """
        This method gathers statistics from pg_stat_archiver
        (postgres 9.4+ or greater required)

        :return dict|None: a dictionary containing Postgres statistics from
            pg_stat_archiver or None
        """
        try:
            with self.pg_connect() as conn:
                # pg_stat_archiver is only available from Postgres 9.4+
                if self.server_version < 90400:
                    return None
                cur = conn.cursor(cursor_factory=RealDictCursor)
                # Select from pg_stat_archiver statistics view,
                # retrieving statistics about WAL archiver process activity,
                # also evaluating if the server is archiving without issues
                # and the archived WALs per second rate
                cur.execute(
                    "SELECT *, current_setting('archive_mode')::BOOLEAN "
                    "AND (last_failed_wal IS NULL "
                    "OR last_failed_wal <= last_archived_wal) "
                    "AS is_archiving, "
                    "CAST (archived_count AS NUMERIC) "
                    "/ EXTRACT (EPOCH FROM age(now(), stats_reset)) "
                    "AS current_archived_wals_per_second "
                    "FROM pg_stat_archiver")
                q_result = cur.fetchone()
                if q_result:
                    return q_result
                else:
                    return None
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving pg_stat_archive data: %s", e)
            return None

    def pg_is_in_recovery(self):
        """
        Returns true if PostgreSQL server is in recovery mode
        """
        try:
            with self.pg_connect() as conn:
                # pg_is_in_recovery is only available from Postgres 9.0+
                if self.server_version < 90000:
                    return False
                cur = conn.cursor()
                cur.execute("select pg_is_in_recovery()")
                q_result = cur.fetchone()[0]
                if q_result:
                    return True
                else:
                    return False
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error calling pg_is_in_recovery() function: %s", e)
            return None

    def get_remote_status(self):
        """
        Get the status of the remote server

        :return dict[str, None]: result of the server status query
        """
        # PostgreSQL settings to get from the server
        pg_settings = (
            'wal_level', 'archive_mode', 'archive_command', 'data_directory')
        pg_query_keys = (
            'server_txt_version', 'current_xlog', 'pgespresso_installed')

        # Initialise the result dictionary setting all the values to None
        result = dict.fromkeys(pg_settings + pg_query_keys, None)
        try:
            with self.pg_connect() as conn:
                for name in pg_settings:
                    result[name] = self.get_pg_setting(name)

                try:
                    cur = conn.cursor()
                    cur.execute("SELECT version()")
                    result['server_txt_version'] = cur.fetchone()[0].split()[1]
                except psycopg2.Error, e:
                    _logger.debug(
                        "Error retrieving PostgreSQL version: %s", e)

                result['pgespresso_installed'] = self.pgespresso_installed()

                try:
                    if not self.pg_is_in_recovery():
                        cur = conn.cursor()
                        cur.execute(
                            'SELECT pg_xlogfile_name('
                            'pg_current_xlog_location())')
                        result['current_xlog'] = cur.fetchone()[0]
                except psycopg2.Error, e:
                    _logger.debug("Error retrieving current xlog: %s", e)

                result.update(self.get_pg_configuration_files())

                # Add pg_stat_archiver statistics if the view is supported
                pg_stat_archiver = self.get_pg_stat_archiver()
                if pg_stat_archiver is not None:
                    result.update(pg_stat_archiver)

                # Merge additional status defined by the BackupManager
                result.update(self.backup_manager.get_remote_status())

        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.warn("Error retrieving PostgreSQL status: %s", e)
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

    @contextmanager
    def pg_connect(self):
        """
        A generic function to connect to Postgres using Psycopg2
        """
        myconn = self._conn is None
        if myconn:
            try:
                self._conn = psycopg2.connect(self.config.conninfo)
                self.server_version = self._conn.server_version
                if (self.server_version >= 90000 and
                        'application_name=' not in self.config.conninfo):
                    cur = self._conn.cursor()
                    cur.execute('SET application_name TO barman')
                    cur.close()
            # If psycopg2 fails to connect to the host,
            # raise the appropriate exception
            except psycopg2.DatabaseError as e:
                raise PostgresConnectionError(
                    "Cannot connect to postgres: %s" % e)
        try:
            yield self._conn
        finally:
            if myconn:
                self._conn.close()
                self._conn = None

    def get_pg_setting(self, name):
        """
        Get a postgres setting with a given name

        :param name: a parameter name
        """

        try:
            with self.pg_connect() as conn:
                cur = conn.cursor()
                cur.execute('SHOW "%s"' % name.replace('"', '""'))
                return cur.fetchone()[0]
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving PostgreSQL setting '%s': %s",
                          name.replace('"', '""'), e)
            return None

    def get_pg_tablespaces(self):
        """
        Returns a list of tablespaces or None if not present
        """

        try:
            with self.pg_connect() as conn:
                cur = conn.cursor()
                if self.server_version >= 90200:
                    cur.execute(
                        "SELECT spcname, oid, "
                        "pg_tablespace_location(oid) AS spclocation "
                        "FROM pg_tablespace "
                        "WHERE pg_tablespace_location(oid) != ''")
                else:
                    cur.execute(
                        "SELECT spcname, oid, spclocation "
                        "FROM pg_tablespace WHERE spclocation != ''")
                # Generate a list of tablespace objects
                return [Tablespace._make(item) for item in cur.fetchall()]
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving PostgreSQL tablespaces: %s", e)
            return None

    def get_pg_configuration_files(self):
        """
        Get postgres configuration files or an empty dictionary in case of error
        """
        if self.configuration_files:
            return self.configuration_files
        try:
            with self.pg_connect() as conn:
                cur = conn.cursor()
                cur.execute("SELECT name, setting FROM pg_settings "
                            "WHERE name IN ("
                            "'config_file', 'hba_file', 'ident_file')")
                self.configuration_files = {}
                for cname, cpath in cur.fetchall():
                    self.configuration_files[cname] = cpath

                # Retrieve additional configuration files
                cur.execute("SELECT DISTINCT sourcefile AS included_file "
                            "FROM pg_settings "
                            "WHERE sourcefile IS NOT NULL "
                            "AND sourcefile NOT IN "
                            "(SELECT setting FROM pg_settings "
                            "WHERE name = 'config_file') "
                            "ORDER BY 1")
                included_files = [included_file
                                  for included_file, in cur.fetchall()]
                if len(included_files) > 0:
                    self.configuration_files['included_files'] = included_files

                return self.configuration_files
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving PostgreSQL configuration files "
                          "location: %s", e)
            return {}

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
            first_backup = self.get_first_backup(BackupInfo.STATUS_ALL)
            if backup.backup_id == first_backup.backup_id \
                and backup.status in (BackupInfo.STARTED, BackupInfo.EMPTY):
                output.error("Cannot delete a running backup (%s %s)"
                             % (self.config.name, backup.backup_id))
            else:
                return self.backup_manager.delete_backup(backup)

        except LockFilePermissionDenied, e:
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
        except OSError, e:
            output.error('failed to create %s directory: %s',
                         e.filename, e.strerror)
            return

        try:
            # lock acquisition and backup execution
            with ServerBackupLock(self.config.barman_lock_directory, self.config.name):
                self.backup_manager.backup()
            # Archive incoming WALs and update WAL catalogue through cron
            self.cron(verbose=False, retention_policies=False)

        except LockFileBusy:
            output.error("Another backup process is running")

        except LockFilePermissionDenied, e:
            output.error("Permission denied, unable to access '%s'" % e)

    def get_available_backups(self, status_filter=BackupManager.DEFAULT_STATUS_FILTER):
        '''Get a list of available backups

        param: status_filter: the status of backups to return, default to BackupManager.DEFAULT_STATUS_FILTER
        '''
        return self.backup_manager.get_available_backups(status_filter)

    def get_last_backup(self, status_filter=BackupManager.DEFAULT_STATUS_FILTER):
        '''
        Get the last backup (if any) in the catalog

        :param status_filter: default DEFAULT_STATUS_FILTER. The status of the backup returned
        '''
        return self.backup_manager.get_last_backup(status_filter)

    def get_first_backup(self, status_filter=BackupManager.DEFAULT_STATUS_FILTER):
        '''
        Get the first backup (if any) in the catalog

        :param status_filter: default DEFAULT_STATUS_FILTER. The status of the backup returned
        '''
        return self.backup_manager.get_first_backup(status_filter)

    def list_backups(self):
        """
        Lists all the available backups for the server
        """
        retention_status = self.report_backups()
        backups = self.get_available_backups(BackupInfo.STATUS_ALL)
        for key in sorted(backups.iterkeys(), reverse=True):
            backup = backups[key]

            backup_size = 0
            wal_size = 0
            rstatus = None
            if backup.status == BackupInfo.DONE:
                wal_info = self.get_wal_info(backup)
                backup_size = backup.size or 0 + wal_info['wal_size']
                wal_size = wal_info['wal_until_next_size']
                if self.enforce_retention_policies and \
                        retention_status[backup.backup_id] != BackupInfo.VALID:
                    rstatus = retention_status[backup.backup_id]
            output.result('list_backup', backup, backup_size, wal_size, rstatus)

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

    def get_required_xlog_files(self, backup, target_tli=None, target_time=None,
                                target_xid=None):
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
        :param dict[str,str]|None tablespaces: a tablespace name -> location map
            (for relocation)
        :param str|None target_tli: the target timeline
        :param str|None target_time: the target time
        :param str|None target_xid: the target xid
        :param str|None target_name: the target name created previously with
                            pg_create_restore_point() function call
        :param bool exclusive: whether the recovery is exclusive or not
        :param str|None remote_command: default None. The remote command to recover
                               the base backup, in case of remote backup.
        """
        return self.backup_manager.recover(
            backup_info, dest, tablespaces, target_tli, target_time, target_xid,
            target_name, exclusive, remote_command)

    def get_wal(self, wal_name, compression=None, output_directory=None):
        """
        Retrieve a WAL file from the archive

        :param str wal_name: id of the WAL file to find into the WAL archive
        :param str|None compression: compression format for the output
        :param str|None output_directory: directory where to deposit the WAL file
        """
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
            out_compressor = self.backup_manager.compression_manager\
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

    def cron(self, verbose=True, wals=True, retention_policies=True):
        """
        Maintenance operations

        :param bool verbose: report even if no actions
        :param bool wals: WAL archive maintenance
        :param bool retention_policies: retention policy maintenance
        """
        try:
            with ServerCronLock(self.config.barman_lock_directory, self.config.name):
                # Standard maintenance (WAL archive)
                if wals:
                    self.backup_manager.cron(verbose=verbose)
                # Retention policy management
                if retention_policies:
                    self.backup_manager.cron_retention_policy()
        except LockFilePermissionDenied, e:
            output.error("Permission denied, unable to access '%s'" % e)
        except (OSError, IOError), e:
            output.error("%s", e)

    @contextmanager
    def xlogdb(self, mode='r'):
        """
        Context manager to access the xlogdb file.

        This method uses locking to make sure only one process is accessing
        the database at a time. The database file will be created if not exists.

        Usage example:

            with server.xlogdb('w') ad file:
                file.write(new_line)

        :param str mode: open the file with the required mode
            (default read-only)
        """
        if not os.path.exists(self.config.wals_directory):
            os.makedirs(self.config.wals_directory)
        xlogdb = os.path.join(self.config.wals_directory, self.XLOG_DB)

        with ServerXLOGDBLock(self.config.barman_lock_directory, self.config.name):
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
        backup_ext_info = self.get_backup_ext_info(backup_info)
        output.result('show_backup', backup_ext_info)
