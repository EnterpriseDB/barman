# Copyright (C) 2011, 2012 2ndQuadrant Italia (Devise.IT S.r.L.)
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

from barman import xlog, _pretty_size
from barman.lockfile import lockfile
from barman.backup import BackupInfo, BackupManager
from barman.command_wrappers import Command
import os
import psycopg2
from contextlib import contextmanager
import itertools

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

    def check_ssh(self):
        '''Checks SSH connection'''
        cmd = Command(self.ssh_command, self.ssh_options)
        ret = cmd("true")
        if ret == 0:
            yield ("\tssh: OK", True)
        else:
            yield ("\tssh: FAILED (return code: %s)" % (ret,), False)

    def check_postgres(self):
        '''
        Checks PostgreSQL connection
        '''
        remote_status = self.get_remote_status()
        if remote_status['server_txt_version']:
            yield ("\tPostgreSQL: OK", True)
        else:
            yield ("\tPostgreSQL: FAILED", False)
            return
        if remote_status['archive_mode'] == 'on':
            yield ("\tarchive_mode: OK", True)
        else:
            yield ("\tarchive_mode: FAILED (please set it to 'on')", False)
        if remote_status['archive_command'] and remote_status['archive_command'] != '(disabled)':
            yield ("\tarchive_command: OK", True)
        else:
            yield ("\tarchive_command: FAILED (please set it accordingly to documentation)", False)

    def check_directories(self):
        '''Checks backup directories and creates them if they do not exist'''
        error = None
        try:
            for key in self.config.KEYS:
                if key.endswith('_directory') and hasattr(self.config, key) and not os.path.isdir(getattr(self.config, key)):
                    os.makedirs(getattr(self.config, key))
        except OSError, e:
                error = e.strerror
        if not error:
            yield ("\tdirectories: OK", True)
        else:
            yield ("\tdirectories: FAILED (%s)" % (error,), False)

    def check(self):
        '''
        Implements the 'server check' command and makes sure SSH and PostgreSQL
        connections work properly. It checks also that backup directories exist
        (and if not, it creates them).
        '''
        status = [(("Server %s:" % (self.config.name,), True),)]
        #FIXME: Description makes sense in a "check" command?
        #if self.config.description: status.append(("\tdescription: %s" % (self.config.description),))
        status.append(self.check_ssh())
        status.append(self.check_postgres())
        status.append(self.check_directories())
        # Executes the backup manager set of checks
        status.append(self.backup_manager.check())

        return itertools.chain.from_iterable(status)

    def status_postgres(self):
        '''Status of PostgreSQL server'''
        remote_status = self.get_remote_status()
        if remote_status['server_txt_version']:
            yield "\tPostgreSQL version: %s " % (remote_status['server_txt_version'])
        else:
            yield "\tPostgreSQL version: FAILED trying to get PostgreSQL version"
            return
        if remote_status['data_directory']:
            yield "\tPostgreSQL Data directory: %s " % (remote_status['data_directory'])
        if remote_status['archive_command']:
            yield "\tarchive_command: %s " % (remote_status['archive_command'])
        else:
            yield "\tarchive_command: FAILED (please set it accordingly to documentation)"
        if remote_status['last_shipped_wal']:
            yield "\tarchive_status: last shipped WAL segment %s" % remote_status['last_shipped_wal']
        else:
            yield "\tarchive_status: No WAL segment shipped yet"
        if remote_status['current_xlog']:
            yield "\tcurrent_xlog: %s " % (remote_status['current_xlog'])

    def status(self):
        '''Implements the 'server status' command.'''
        status = [("Server %s:" % (self.config.name),)]
        if self.config.description: status.append(("\tdescription: %s" % (self.config.description),))
        status.append(self.status_postgres())
        # Executes the backup manager status info method
        status.append(self.backup_manager.status())
        return itertools.chain.from_iterable(status)

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
            out = cmd.getoutput(None, 'ls', '-tr', archive_dir)[0]
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
                if self.server_version > 90200:
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

    def pg_start_backup(self):
        '''Execute a pg_start_backup'''
        with self.pg_connect() as conn:
            try:
                cur = conn.cursor()
                cur.execute('SELECT xlog_loc, (pg_xlogfile_name_offset(xlog_loc)).* from pg_start_backup(%s) as xlog_loc', ('Barman backup',))
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

    def backup(self):
        '''Performs a backup for the server'''
        return self.backup_manager.backup()

    def get_available_backups(self, status_filter=BackupManager.DEFAULT_STATUS_FILTER):
        '''Get a list of available backups

        param: status_filter: the status of backups to return, default to BackupManager.DEFAULT_STATUS_FILTER
        '''
        return self.backup_manager.get_available_backups(status_filter)

    def list_backups(self):
        '''Lists all the available backups for the server'''
        status_filter = BackupInfo.STATUS_NOT_EMPTY
        backups = self.get_available_backups(status_filter)
        for key in sorted(backups.iterkeys(), reverse=True):
            backup = backups[key]
            if backup.status == BackupInfo.DONE:
                _, backup_wal_size, _, wal_until_next_size, _ = self.get_wal_info(backup)
                backup_size = _pretty_size(backup.size or 0 + backup_wal_size)
                wal_size = _pretty_size(wal_until_next_size)
                end_time = backup.end_time.ctime()
                if backup.tablespaces:
                    tablespaces = [("%s:%s" % (name, location))for name, _, location in backup.tablespaces]
                    yield ("%s %s - %s - Size: %s - WAL Size: %s (tablespaces: %s)"
                           % (self.config.name, backup.backup_id,
                              end_time, backup_size, wal_size,
                              ', '.join(tablespaces)))
                else:
                    yield ("%s %s - %s - Size: %s - WAL Size: %s"
                           % (self.config.name, backup.backup_id,
                              end_time, backup_size, wal_size))
            else:
                yield "%s %s - %s" % (self.config.name, backup.backup_id, backup.status)

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

    def get_wal_info(self, backup):
        '''Returns information about WALs for the given backup

        :param backup: a backup object of which return wal information
        '''
        end = backup.end_wal

        # counters
        wal_num = 0
        wal_size = 0
        wal_until_next_num = 0
        wal_until_next_size = 0
        wal_last = None

        for name, size in self.get_wal_until_next_backup(backup):
                if name <= end:
                    wal_num += 1
                    wal_size += size
                else:
                    wal_until_next_num += 1
                    wal_until_next_size += size
                wal_last = name
        return wal_num, wal_size, wal_until_next_num, wal_until_next_size, wal_last

    def recover(self, backup, dest, tablespaces=[], target_tli=None, target_time=None, target_xid=None, exclusive=False, remote_command=None):
        '''Performs a recovery of a backup'''
        return self.backup_manager.recover(backup, dest, tablespaces, target_tli, target_time, target_xid, exclusive, remote_command)

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
            name, size, stamp = line.split()
        return name, int(size), float(stamp), compression
