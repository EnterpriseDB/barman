# Copyright (C) 2011-2014 2ndQuadrant Italia (Devise.IT S.r.L.)
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

import os
import re
import logging
from contextlib import contextmanager

import psycopg2
import sys
import shutil
from datetime import timedelta

from barman import output
from barman.config import BackupOptions
from barman.infofile import BackupInfo, UnknownBackupIdException, Tablespace, \
    WalFileInfo
from barman.lockfile import LockFile, LockFileBusy, \
    LockFilePermissionDenied
from barman.backup import BackupManager
from barman.command_wrappers import Command
from barman.retention_policies import RetentionPolicyFactory
from barman.utils import human_readable_timedelta
import xlog


_logger = logging.getLogger(__name__)


class Server(object):
    """
    This class represents a server to backup
    """
    XLOG_DB = "xlog.db"

    def __init__(self, config):
        ''' The Constructor.

        :param config: the server configuration
        '''
        self.config = config
        self.conn = None
        self.server_txt_version = None
        self.server_version = None
        self.ssh_options = config.ssh_command.split()
        self.ssh_command = self.ssh_options.pop(0)
        self.ssh_options.extend("-o BatchMode=yes -o StrictHostKeyChecking=no".split())
        self.backup_manager = BackupManager(self)
        self.configuration_files = None
        self.enforce_retention_policies = False

        # Set bandwidth_limit
        if self.config.bandwidth_limit:
            try:
                self.config.bandwidth_limit = int(self.config.bandwidth_limit)
            except ValueError:
                _logger.warning('Invalid bandwidth_limit "%s" for server "%s" (fallback to "0")'
                                % (self.config.bandwidth_limit, self.config.name))
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
                    _logger.warning("Invalid tablespace_bandwidth_limit rule '%s'" % (rule,))
            if len(rules) > 0:
                self.config.tablespace_bandwidth_limit = rules
            else:
                self.config.tablespace_bandwidth_limit = None

        # Set minimum redundancy (default 0)
        if self.config.minimum_redundancy.isdigit():
            self.config.minimum_redundancy = int(self.config.minimum_redundancy)
            if self.config.minimum_redundancy < 0:
                _logger.warning('Negative value of minimum_redundancy "%s" for server "%s" (fallback to "0")'
                            % (self.config.minimum_redundancy, self.config.name))
                self.config.minimum_redundancy = 0
        else:
            _logger.warning('Invalid minimum_redundancy "%s" for server "%s" (fallback to "0")'
                            % (self.config.minimum_redundancy, self.config.name))
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

    def check(self):
        """
        Implements the 'server check' command and makes sure SSH and PostgreSQL
        connections work properly. It checks also that backup directories exist
        (and if not, it creates them).
        """
        self.check_ssh()
        self.check_postgres()
        self.check_directories()
        # Check retention policies
        self.check_retention_policy_settings()
        # Check for backup validity
        self.check_backup_validity()
        # Executes the backup manager set of checks
        self.backup_manager.check()

    def check_ssh(self):
        """
        Checks SSH connection
        """
        cmd = Command(self.ssh_command, self.ssh_options)
        ret = cmd("true")
        if ret == 0:
            output.result('check', self.config.name, 'ssh', True)
        else:
            output.result('check', self.config.name, 'ssh', False,
                          'return code: %s' % ret)

    def check_postgres(self):
        """
        Checks PostgreSQL connection
        """
        remote_status = self.get_remote_status()
        if remote_status['server_txt_version']:
            output.result('check', self.config.name, 'PostgreSQL', True)
        else:
            output.result('check', self.config.name, 'PostgreSQL', False)
            return

        if BackupOptions.CONCURRENT_BACKUP in self.config.backup_options:
            if remote_status['pgespresso_installed']:
                output.result('check', self.config.name,
                        'pgespresso extension', True)
            else:
                output.result('check', self.config.name,
                          'pgespresso extension',
                          False,
                          'required for concurrent backups')

        if remote_status['archive_mode'] == 'on':
            output.result('check', self.config.name, 'archive_mode', True)
        else:
            output.result('check', self.config.name, 'archive_mode', False,
                          "please set it to 'on'")
        if remote_status['archive_command'] and\
                remote_status['archive_command'] != '(disabled)':
            output.result('check', self.config.name, 'archive_command', True)
        else:
            output.result('check', self.config.name, 'archive_command', False,
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

    def check_directories(self):
        """
        Checks backup directories and creates them if they do not exist
        """
        try:
            self._make_directories()
        except OSError, e:
            output.result('check', self.config.name, 'directories', False,
                          "%s: %s" % (e.filename, e.strerror))
        else:
            output.result('check', self.config.name, 'directories', True)

    def check_retention_policy_settings(self):
        """
        Checks retention policy setting
        """
        if self.config.retention_policy and not self.enforce_retention_policies:
            output.result('check', self.config.name,
                          'retention policy settings', False, 'see log')
        else:
            output.result('check', self.config.name,
                          'retention policy settings', True)

    def check_backup_validity(self):
        """
        Check if backup validity requirements are satisfied
        """
        # first check: check backup maximum age
        if self.config.last_backup_maximum_age is not None:
            # get maximum age informations
            backup_age = self.backup_manager.validate_last_backup_maximum_age(
                self.config.last_backup_maximum_age)

            # format the output
            output.result('check', self.config.name,
                          'backup maximum age', backup_age[0],
                          "interval provided: %s, latest backup age: %s" %
                          (human_readable_timedelta(
                              self.config.last_backup_maximum_age),
                           backup_age[1]))
        else:
            # last_backup_maximum_age provided by the user
            output.result('check', self.config.name,
                          'backup maximum age',
                          True,
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
                      remote_status['archive_command']
                      or "FAILED (please set it accordingly to documentation)")
        output.result('status', self.config.name,
                      "last_shipped_wal",
                      "Archive status",
                      "last shipped WAL segment %s" %
                      remote_status['last_shipped_wal']
                      or "No WAL segment shipped yet")
        if remote_status['current_xlog']:
            output.result('status', self.config.name,
                          "current_xlog",
                          "Current WAL segment",
                          remote_status['current_xlog'])

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
        self.status_postgres()
        self.status_retention_policies()
        # Executes the backup manager status info method
        self.backup_manager.status()

    def pg_espresso_installed(self):
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
        except psycopg2.Error, e:
            _logger.debug("Error retrieving pgespresso information: %s", e)
            return False

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
        except psycopg2.Error, e:
            _logger.debug("Error calling pg_is_in_recovery() function: %s", e)
            return None

    def get_remote_status(self):
        """
        Get the status of the remote server

        :return: result of the server status query
        """

        pg_settings = (
            'archive_mode', 'archive_command', 'data_directory')
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


                result['pgespresso_installed'] = self.pg_espresso_installed()

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
        except psycopg2.Error, e:
            _logger.warn("Error retrieving PostgreSQL status: %s", e)

        # TODO: replace with RemoteUnixCommand
        cmd = Command(self.ssh_command, self.ssh_options)
        result['last_shipped_wal'] = None
        if result['data_directory'] and result['archive_command']:
            archive_dir = os.path.join(result['data_directory'],
                                       'pg_xlog', 'archive_status')
            out = str(cmd.getoutput('ls', '-tr', archive_dir)[0])
            for line in out.splitlines():
                if line.endswith('.done'):
                    name = line[:-5]
                    if xlog.is_wal_file(name):
                        result['last_shipped_wal'] = line[:-5]
        return result

    def show(self):
        """
        Shows the server configuration
        """
        #Populate result map with all the required keys
        result = dict([
            (key, getattr(self.config, key))
            for key in self.config.KEYS
        ])
        remote_status = self.get_remote_status()
        result.update(remote_status)
        # backup maximum age section
        if self.config.last_backup_maximum_age is not None:
            age = self.backup_manager.validate_last_backup_maximum_age(
                self.config.last_backup_maximum_age)
            # if latest backup is between the limits of the
            # last_backup_maximum_age configuration, display how old is
            # the latest backup.
            if age[0]:
                msg = "%s (latest backup: %s )" % \
                    (human_readable_timedelta(
                        self.config.last_backup_maximum_age),
                     age[1])
            else:
                # if latest backup is outside the limits of the
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
        myconn = self.conn == None
        if myconn:
            self.conn = psycopg2.connect(self.config.conninfo)
            self.server_version = self.conn.server_version
            if (self.server_version >= 90000
                    and 'application_name=' not in self.config.conninfo):
                cur = self.conn.cursor()
                cur.execute('SET application_name TO barman')
                cur.close()
        try:
            yield self.conn
        finally:
            if myconn:
                self.conn.close()
                self.conn = None

    def get_pg_setting(self, name):
        """
        Get a postgres setting with a given name

        :param name: a parameter name
        """
        with self.pg_connect() as conn:
            try:
                cur = conn.cursor()
                cur.execute('SHOW "%s"' % name.replace('"', '""'))
                return cur.fetchone()[0]
            except psycopg2.Error, e:
                _logger.debug("Error retrieving PostgreSQL setting '%s': %s",
                              name.replace('"', '""'), e)
                return None

    def get_pg_tablespaces(self):
        """
        Returns a list of tablespaces or None if not present
        """
        with self.pg_connect() as conn:
            try:
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
            except psycopg2.Error, e:
                _logger.debug("Error retrieving PostgreSQL tablespaces: %s", e)
                return None

    def get_pg_configuration_files(self):
        """
        Get postgres configuration files or an empty dictionary in case of error
        """
        if self.configuration_files:
            return self.configuration_files
        with self.pg_connect() as conn:
            try:
                cur = conn.cursor()
                cur.execute("SELECT name, setting FROM pg_settings "
                            "WHERE name IN ("
                            "'config_file', 'hba_file', 'ident_file')")
                self.configuration_files = {}
                for cname, cpath in cur.fetchall():
                    self.configuration_files[cname] = cpath
                return self.configuration_files
            except psycopg2.Error, e:
                _logger.debug("Error retrieving PostgreSQL configuration files "
                              "location: %s", e)
                return {}

    def pg_start_backup(self, backup_label):
        """
        Execute a pg_start_backup

        :param str backup_label: label for the backup
        """
        with self.pg_connect() as conn:
            if (BackupOptions.CONCURRENT_BACKUP not in
                    self.config.backup_options and self.pg_is_in_recovery()):
                raise Exception(
                    'Unable to start a backup because of server recovery state')
            try:
                cur = conn.cursor()
                if self.server_version < 80400:
                    cur.execute(
                        'SELECT xlog_loc, (pg_xlogfile_name_offset(xlog_loc)).*, '
                        'now() FROM pg_start_backup(%s) as xlog_loc',
                        (backup_label,))
                else:
                    cur.execute(
                        'SELECT xlog_loc, (pg_xlogfile_name_offset(xlog_loc)).*, '
                        'now() FROM pg_start_backup(%s,%s) as xlog_loc',
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
        with self.pg_connect() as conn:

            if (BackupOptions.CONCURRENT_BACKUP in self.config.backup_options
                and not self.pg_espresso_installed()):
                raise Exception(
                    'pgespresso extension required for concurrent_backup')
            try:
                cur = conn.cursor()
                cur.execute('SELECT pgespresso_start_backup(%s,%s), now()',
                            (backup_label, self.config.immediate_checkpoint))
                return cur.fetchone()
            except psycopg2.Error, e:
                msg = "pgexpresso_start_backup(): %s" % e
                _logger.debug(msg)
                raise Exception(msg)

    def start_backup(self, label, backup_info):
        """
        start backup wrapper

        :param str label: label for the backup
        :param BackupInfo backup_info: backup information object
        :return:
        """
        if BackupOptions.CONCURRENT_BACKUP not in self.config.backup_options:
            start_row = self.pg_start_backup(label)
            start_xlog, start_file_name, start_file_offset, start_time = \
                start_row
            backup_info.set_attribute("status", "STARTED")
            backup_info.set_attribute("timeline",
                                      int(start_file_name[0:8], 16))
            backup_info.set_attribute("begin_xlog", start_xlog)
            backup_info.set_attribute("begin_wal", start_file_name)
            backup_info.set_attribute("begin_offset", start_file_offset)
            backup_info.set_attribute("begin_time", start_time)

        else:
            start_row = self.pgespresso_start_backup(label)
            backup_data, start_time = start_row
            wal_re = re.compile(
                '^START WAL LOCATION: (.*) \(file (.*)\)',
                re.MULTILINE)
            wal_info = wal_re.search(backup_data)
            backup_info.set_attribute("status", "STARTED")
            backup_info.set_attribute("timeline",
                                      int(wal_info.group(2)[0:8], 16))
            backup_info.set_attribute("begin_xlog", wal_info.group(1))
            backup_info.set_attribute("begin_wal", wal_info.group(2))
            backup_info.set_attribute("begin_offset",
                                      xlog.get_offset_from_location(
                                          wal_info.group(1)))
            backup_info.set_attribute("backup_label", backup_data)
            backup_info.set_attribute("begin_time", start_time)

    def pg_stop_backup(self):
        """
        Execute a pg_stop_backup
        """
        with self.pg_connect() as conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    'SELECT xlog_loc, (pg_xlogfile_name_offset(xlog_loc)).*, '
                    'now() FROM pg_stop_backup() as xlog_loc')
                return cur.fetchone()
            except psycopg2.Error, e:
                _logger.debug("Error issuing pg_stop_backup() command: %s", e)
                return None

    def pgespresso_stop_backup(self, backup_label):
        """
        Execute a pgespresso_stop_backup
        """
        with self.pg_connect() as conn:
            try:
                cur = conn.cursor()
                cur.execute('SELECT pgespresso_stop_backup(%s), now()',
                            (backup_label,))
                return cur.fetchone()
            except psycopg2.Error, e:
                _logger.debug(
                    "Error issuing pgespresso_stop_backup() command: %s", e)
                return None

    def stop_backup(self, backup_info):
        """
        stop backup wrapper

        :param backup_label: label for the backup
        :param immediate_checkpoint: Boolean for immediate checkpoint execution
        :param backup_info: backup_info object
        :return:
        """
        if BackupOptions.CONCURRENT_BACKUP not in self.config.backup_options:
            stop_row = self.pg_stop_backup()
            if stop_row:
                stop_xlog, stop_file_name, stop_file_offset, stop_time = \
                    stop_row
                backup_info.set_attribute("end_time", stop_time)
                backup_info.set_attribute("end_xlog", stop_xlog)
                backup_info.set_attribute("end_wal", stop_file_name)
                backup_info.set_attribute("end_offset", stop_file_offset)
            else:
                raise Exception('Cannot terminate exclusive backup. You might '
                        'have to manually execute pg_stop_backup() on your '
                        'Postgres server')
        else:
            stop_row = self.pgespresso_stop_backup(backup_info.backup_label)
            if stop_row:
                end_wal, stop_time = stop_row
                decoded_segment = xlog.decode_segment_name(end_wal)
                backup_info.set_attribute("end_time", stop_time)
                backup_info.set_attribute("end_xlog",
                                          "%X/%X" % (decoded_segment[1],
                                                     (decoded_segment[
                                                          2] + 1) << 24))
                backup_info.set_attribute("end_wal", end_wal)
                backup_info.set_attribute("end_offset", 0)
            else:
                raise Exception('Cannot terminate exclusive backup. You might '
                        'have to manually execute pg_espresso_abort_backup() '
                        'on your Postgres server')

    def delete_backup(self, backup):
        '''Deletes a backup

        :param backup: the backup to delete
        '''
        return self.backup_manager.delete_backup(backup)

    def backup(self):
        """
        Performs a backup for the server

        :param immediate_checkpoint: Boolean for immediate checkpoint execution
        """
        try:
            # check required backup directories exist
            self._make_directories()
        except OSError, e:
            output.error('failed to create %s directory: %s',
                         e.filename, e.strerror)
            return

        filename = os.path.join(
            self.config.barman_home, '.%s-backup.lock' % self.config.name)

        try:
            # lock acquisition and backup execution
            with LockFile(filename, raise_if_fail=True):
                self.backup_manager.backup()

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
        status_filter = BackupInfo.STATUS_NOT_EMPTY
        retention_status = self.report_backups()
        backups = self.get_available_backups(status_filter)
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
        Return the backup information for the given backup,
        or None if its status is not empty

        :param backup_id: the ID of the backup to return
        """
        try:
            backup = BackupInfo(self, backup_id=backup_id)
            if backup.status in BackupInfo.STATUS_NOT_EMPTY:
                return backup
            return None
        except Exception, e:
            _logger.debug("Error reading backup information for %s %s: %s",
                          self.config.name, backup_id, e, exc_info=1)
            return None

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
                wal_info = WalFileInfo.from_xlogdb_line(self, line)
                if wal_info.name < begin:
                    continue
                tli, _, _ = xlog.decode_segment_name(wal_info.name)
                if tli > target_tli:
                    continue
                yield wal_info.name
                if wal_info.name > end:
                    end = wal_info.name
                    if target_time and target_time < wal_info.time:
                        break
            # return all the remaining history files
            for line in fxlogdb:
                wal_info = WalFileInfo.from_xlogdb_line(self, line)
                if xlog.is_history_file(wal_info.name):
                    yield wal_info.name

    # TODO: merge with the previous
    def get_wal_until_next_backup(self, backup):
        """
        Get the xlog files between backup and the next

        :param BackupInfo backup: a backup object, the starting point
            to retrieve WALs
        """
        begin = backup.begin_wal
        next_end = None
        if self.get_next_backup(backup.backup_id):
            next_end = self.get_next_backup(backup.backup_id).end_wal
        backup_tli, _, _ = xlog.decode_segment_name(begin)

        with self.xlogdb() as fxlogdb:
            for line in fxlogdb:
                wal_info = WalFileInfo.from_xlogdb_line(self, line)
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

        # Estimate WAL ratio
        if wal_info['wal_last_timestamp']:
            # Calculate the difference between the timestamps of
            # the first WAL (begin of backup) and the last WAL
            # associated to the current backup
            wal_info['wal_total_seconds'] = (wal_info['wal_last_timestamp'] -
                                             wal_info['wal_first_timestamp'])
            if wal_info['wal_total_seconds'] > 0:
                wal_info['wals_per_second'] = (float(wal_info['wal_num']) /
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

    def recover(self, backup, dest, tablespaces=[], target_tli=None, target_time=None, target_xid=None, target_name=None, exclusive=False, remote_command=None):
        '''Performs a recovery of a backup'''
        return self.backup_manager.recover(backup, dest, tablespaces, target_tli, target_time, target_xid, target_name, exclusive, remote_command)

    def cron(self, verbose=True):
        """
        Maintenance operations
        """
        filename = os.path.join(self.config.barman_home,
                                '.%s-cron.lock' % self.config.name)
        try:
            with LockFile(filename, raise_if_fail=True, wait=True):
                return self.backup_manager.cron(verbose=verbose)
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

        xlogdb_lock = xlogdb + ".lock"
        with LockFile(xlogdb_lock, wait=True):
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
