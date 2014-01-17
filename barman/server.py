# Copyright (C) 2011-2013 2ndQuadrant Italia (Devise.IT S.r.L.)
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

'''This module represents a Server.
Barman is able to manage multiple servers.
'''

from barman import xlog, output
from barman.infofile import BackupInfo, UnknownBackupIdException
from barman.lockfile import lockfile
from barman.backup import BackupManager
from barman.command_wrappers import Command
from barman.retention_policies import RetentionPolicyFactory, SimpleWALRetentionPolicy
import os
import logging
import psycopg2
from contextlib import contextmanager
import itertools

_logger = logging.getLogger(__name__)

class Server(object):
    '''This class represents a server to backup'''
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
            except:
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
                except:
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
            _logger.warning('Unsupported retention_policy_mode "%s" for server "%s" (fallback to "auto")'
                            % (self.config.retention_policy_mode, self.config.name))
            self.config.retention_policy_mode = 'auto'

        # If retention_policy is present, enforce them
        if self.config.retention_policy:
            # Check wal_retention_policy
            if self.config.wal_retention_policy != 'main':
                _logger.warning('Unsupported wal_retention_policy value "%s" for server "%s" (fallback to "main")'
                                % (self.config.wal_retention_policy, self.config.name))
                self.config.wal_retention_policy = 'main'
            # Create retention policy objects
            try:
                rp = RetentionPolicyFactory.create(self,
                    'retention_policy', self.config.retention_policy)
                # Reassign the configuration value (we keep it in one place)
                self.config.retention_policy = rp
                _logger.info('Retention policy for server %s: %s' % (
                    self.config.name, self.config.retention_policy))
                try:
                    rp = RetentionPolicyFactory.create(self,
                        'wal_retention_policy', self.config.wal_retention_policy)
                    # Reassign the configuration value (we keep it in one place)
                    self.wal_retention_policy = rp
                    _logger.info('WAL retention policy for server %s: %s' % (
                        self.config.name, self.config.wal_retention_policy))
                except:
                    _logger.error('Invalid wal_retention_policy setting "%s" for server "%s" (fallback to "main")' % (
                        self.config.wal_retention_policy, self.config.name))
                    self.wal_retention_policy = SimpleWALRetentionPolicy (
                        self.retention_policy, self)

                self.enforce_retention_policies = True

            except:
                _logger.error('Invalid retention_policy setting "%s" for server "%s"' % (
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

    def check_directories(self):
        """
        Checks backup directories and creates them if they do not exist
        """
        error = None
        try:
            for key in self.config.KEYS:
                if key.endswith('_directory') and hasattr(self.config, key):
                    val = getattr(self.config, key)
                    if val is not None and not os.path.isdir(val):
                        #noinspection PyTypeChecker
                        os.makedirs(val)
        except OSError, e:
                error = e.strerror
        if not error:
            output.result('check', self.config.name, 'directories', True)
        else:
            output.result('check', self.config.name, 'directories', False,
                          error)

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

    def get_remote_status(self):
        '''Get the status of the remote server'''
        pg_settings = ('archive_mode', 'archive_command', 'data_directory')
        pg_query_keys = ('server_txt_version', 'current_xlog')
        result = dict.fromkeys(pg_settings + pg_query_keys, None)
        try:
            with self.pg_connect() as conn:
                for name in pg_settings:
                    result[name] = self.get_pg_setting(name)
                try:
                    cur = conn.cursor()
                    cur.execute("SELECT version()")
                    result['server_txt_version'] = cur.fetchone()[0].split()[1]
                except:
                    result['server_txt_version'] = None
                try:
                    cur = conn.cursor()
                    cur.execute('SELECT pg_xlogfile_name(pg_current_xlog_location())')
                    result['current_xlog'] = cur.fetchone()[0];
                except:
                    result['current_xlog'] = None
        except:
            pass
        cmd = Command(self.ssh_command, self.ssh_options)
        result['last_shipped_wal'] = None
        if result['data_directory'] and result['archive_command']:
            archive_dir = os.path.join(result['data_directory'], 'pg_xlog', 'archive_status')
            out = str(cmd.getoutput(None, 'ls', '-tr', archive_dir)[0])
            for line in out.splitlines():
                if line.endswith('.done'):
                    name = line[:-5]
                    if xlog.is_wal_file(name):
                        result['last_shipped_wal'] = line[:-5]
        return result

    def show(self):
        '''Shows the server configuration'''
        yield "Server %s:" % (self.config.name)
        for key in self.config.KEYS:
            if hasattr(self.config, key):
                yield "\t%s: %s" % (key, getattr(self.config, key))
        remote_status = self.get_remote_status()
        for key in remote_status:
            yield "\t%s: %s" % (key, remote_status[key])

        try:
            cf = self.get_pg_configuration_files()
            if cf:
                for key in sorted(cf.keys()):
                    yield "\t%s: %s" % (key, cf[key])
        except:
            yield "ERROR: cannot connect to the PostgreSQL Server"

    @contextmanager
    def pg_connect(self):
        '''A generic function to connect to Postgres using Psycopg2'''
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
        '''Get a postgres setting with a given name

        :param name: a parameter name
        '''
        with self.pg_connect() as conn:
            try:
                cur = conn.cursor()
                cur.execute('SHOW "%s"' % name.replace('"', '""'))
                return cur.fetchone()[0]
            except:
                return None

    def get_pg_tablespaces(self):
        '''Returns a list of tablespaces or None if not present'''
        with self.pg_connect() as conn:
            try:
                cur = conn.cursor()
                if self.server_version >= 90200:
                    cur.execute("SELECT spcname, oid, pg_tablespace_location(oid) AS spclocation FROM pg_tablespace WHERE pg_tablespace_location(oid) != ''")
                else:
                    cur.execute("SELECT spcname, oid, spclocation FROM pg_tablespace WHERE spclocation != ''")
                return cur.fetchall()
            except:
                return None

    def get_pg_configuration_files(self):
        '''Get postgres configuration files or None in case of error'''
        if self.configuration_files:
            return self.configuration_files
        with self.pg_connect() as conn:
            try:
                cur = conn.cursor()
                cur.execute("SELECT name, setting FROM pg_settings WHERE name IN ('config_file', 'hba_file', 'ident_file')")
                self.configuration_files = {}
                for cname, cpath in cur.fetchall():
                    self.configuration_files[cname] = cpath
                return self.configuration_files
            except:
                return None

    def pg_start_backup(self, backup_label, immediate_checkpoint):
        """
        Execute a pg_start_backup

        :param backup_label: label for the backup
        :param immediate_checkpoint Boolean for immediate checkpoint execution
        """
        with self.pg_connect() as conn:
            try:
                cur = conn.cursor()
                cur.execute('SELECT xlog_loc, (pg_xlogfile_name_offset(xlog_loc)).* from pg_start_backup(%s,%s) as xlog_loc',
                            (backup_label, immediate_checkpoint))
                return cur.fetchone()
            except:
                return None

    def pg_stop_backup(self):
        '''Execute a pg_stop_backup'''
        with self.pg_connect() as conn:
            try:
                cur = conn.cursor()
                cur.execute('SELECT xlog_loc, (pg_xlogfile_name_offset(xlog_loc)).* from pg_stop_backup() as xlog_loc')
                return cur.fetchone()
            except:
                return None

    def delete_backup(self, backup):
        '''Deletes a backup

        :param backup: the backup to delete
        '''
        return self.backup_manager.delete_backup(backup)

    def backup(self, immediate_checkpoint):
        '''Performs a backup for the server'''
        return self.backup_manager.backup(immediate_checkpoint)

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
        '''Return the backup information for the given backup,
        or None if its status is not empty

        :param backup_id: the ID of the backup to return
        '''
        try:
            backup = BackupInfo(self, backup_id=backup_id)
            if backup.status in BackupInfo.STATUS_NOT_EMPTY:
                return backup
            return None
        except:
            return None

    def get_previous_backup(self, backup_id):
        '''Get the previous backup (if any) from the catalog

        :param backup_id: the backup id from which return the previous
        '''
        return self.backup_manager.get_previous_backup(backup_id)

    def get_next_backup(self, backup_id):
        '''Get the next backup (if any) from the catalog

        :param backup_id: the backup id from which return the next
        '''
        return self.backup_manager.get_next_backup(backup_id)

    def get_required_xlog_files(self, backup, target_tli=None, target_time=None, target_xid=None):
        '''Get the xlog files required for a backup'''
        begin = backup.begin_wal
        end = backup.end_wal
        # If timeline isn't specified, assume it is the same timeline of the backup
        if not target_tli:
            target_tli, _, _ = xlog.decode_segment_name(end)
        with self.xlogdb() as fxlogdb:
            for line in fxlogdb:
                name, _, stamp, _ = self.xlogdb_parse_line(line)
                if name < begin: continue
                tli, _, _ = xlog.decode_segment_name(name)
                if tli > target_tli: continue
                yield name
                if name > end:
                    end = name
                    if target_time and target_time < stamp:
                        break
            # return all the remaining history files
            for line in fxlogdb:
                name, _, stamp, _ = self.xlogdb_parse_line(line)
                if xlog.is_history_file(name):
                    yield name

    # TODO: merge with the previous
    def get_wal_until_next_backup(self, backup):
        '''Get the xlog files between backup and the next

        :param backup: a backup object, the starting point to retrieve wals
        '''
        begin = backup.begin_wal
        next_end = None
        if self.get_next_backup(backup.backup_id):
            next_end = self.get_next_backup(backup.backup_id).end_wal
        backup_tli, _, _ = xlog.decode_segment_name(begin)

        with self.xlogdb() as fxlogdb:
            for line in fxlogdb:
                name, size, _, _ = self.xlogdb_parse_line(line)
                if name < begin: continue
                tli, _, _ = xlog.decode_segment_name(name)
                if tli > backup_tli: continue
                if not xlog.is_wal_file(name): continue
                if next_end and name > next_end:
                    break
                # count
                yield (name, size)

    def get_wal_info(self, backup_info):
        """
        Returns information about WALs for the given backup

        :param BackupInfo backup_info: the target backup
        """
        end = backup_info.end_wal

        # counters
        wal_info = dict.fromkeys(
            ('wal_num', 'wal_size',
             'wal_until_next_num', 'wal_until_next_size'), 0)
        wal_info['wal_last'] = None

        for name, size in self.get_wal_until_next_backup(backup_info):
                if name <= end:
                    wal_info['wal_num'] += 1
                    wal_info['wal_size'] += size
                else:
                    wal_info['wal_until_next_num'] += 1
                    wal_info['wal_until_next_size'] += size
                wal_info['wal_last'] = name
        return wal_info

    def recover(self, backup, dest, tablespaces=[], target_tli=None, target_time=None, target_xid=None, target_name=None, exclusive=False, remote_command=None):
        '''Performs a recovery of a backup'''
        return self.backup_manager.recover(backup, dest, tablespaces, target_tli, target_time, target_xid, target_name, exclusive, remote_command)

    def cron(self, verbose=True):
        '''Maintenance operations

        :param verbose: turn on verbose mode. default True
        '''
        return self.backup_manager.cron(verbose)

    @contextmanager
    def xlogdb(self, mode='r'):
        if not os.path.exists(self.config.wals_directory):
            os.makedirs(self.config.wals_directory)
        xlogdb = os.path.join(self.config.wals_directory, self.XLOG_DB)
        # If the file doesn't exist and it is required to read it,
        # we open it in a+ mode, to be sure it will be created
        if not os.path.exists(xlogdb) and mode.startswith('r'):
            if '+' not in mode:
                mode = "a%s+" % mode[1:]
            else:
                mode = "a%s" % mode[1:]
        xlogdb_lock = xlogdb + ".lock"
        with lockfile(xlogdb_lock, wait=True):
            with open(xlogdb, mode) as f:
                yield f

    def xlogdb_parse_line(self, line):
        '''Parse a line from xlog catalogue

        :param line: a line in the wal database to parse
        '''
        try:
            name, size, stamp, compression = line.split()
        except ValueError:
            # Old format compatibility (no compression)
            compression = None
            try:
                name, size, stamp = line.split()
            except:
                raise ValueError("cannot parse line: %r" % (line,))
        return name, int(size), float(stamp), compression

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
