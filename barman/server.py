#
# barman - Backup and Recovery Manager for PostgreSQL
#
# Copyright (C) 2011  Devise.IT S.r.l. <info@2ndquadrant.it>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import psycopg2
from barman.command_wrappers import Command, RsyncPgData
import os
import datetime
import traceback
from barman.backup import Backup
import errno

class Server(object):
    """
    PostgreSQL server for backup
    """
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.server_txt_version = None
        self.server_version = None
        self.ssh_options = config.ssh_command.split()
        self.ssh_command = self.ssh_options.pop(0)
        self.ssh_options.extend("-o BatchMode=yes -o StrictHostKeyChecking=no".split())

    def _read_pgsql_info(self):
        """
        Checks PostgreSQL connection and retrieve version information
        """
        conn_is_mine = self.conn == None
        try:
            if conn_is_mine: self.conn = psycopg2.connect(self.config.conninfo)
            self.server_version = self.conn.server_version
            cur = self.conn.cursor()
            cur.execute("SELECT version()")
            self.server_txt_version = cur.fetchone()[0].split()[1]
            if conn_is_mine: self.conn.close()
        except Exception:
            return False
        else:
            return True

    def check_ssh(self):
        """
        Checks SSH connection
        """
        cmd = Command(self.ssh_command, self.ssh_options)
        ret = cmd("true")
        if ret == 0:
            return "\tssh: OK"
        else:
            return "\tssh: FAILED (return code: %s)" % (ret)

    def check_postgres(self):
        """
        Checks PostgreSQL connection
        """
        if self._read_pgsql_info():
            return "\tpgsql: OK (version: %s)" % (self.server_txt_version)
        else:
            return "\tpgsql: FAILED"

    def check_directories(self):
        """
        Checks backup directories and creates them if they do not exist
        """
        error = None
        try:
            for key in self.config.KEYS:
                if key.endswith('_directory') and hasattr(self.config, key) and not os.path.isdir(getattr(self.config, key)):
                    os.makedirs(getattr(self.config, key))
        except OSError, e:
                error = e.strerror
        if not error:
            return "\tdirectories: OK"
        else:
            return "\tdirectories: FAILED (%s)" % (error)

    def check(self):
        """
        Implements the 'server check' command and makes sure SSH and PostgreSQL
        connections work properly. It checks also that backup directories exist
        (and if not, it creates them).
        """
        yield "Server %s:" % (self.config.name)
        if self.config.description: yield "\tdescription: %s" % (self.config.description)
        yield self.check_ssh()
        yield self.check_postgres()
        yield self.check_directories()

    def show(self):
        """
        Shows the server configuration
        """
        yield "Server %s:" % (self.config.name)
        for key in self.config.KEYS:
            if hasattr(self.config, key):
                yield "\t%s: %s" % (key, getattr(self.config, key))

    def backup(self):
        """
        Performs a backup for the server
        """

        backup_stamp = datetime.datetime.now()

        backup_base = os.path.join(self.config.basebackups_directory, backup_stamp.strftime('%Y%m%dT%H%M%S'))

        backup_info = os.path.join(backup_base, 'backup.info')

        current_action = None
        info = None
        try:
            current_action = "creating destination directory (%s)" % backup_base
            os.makedirs(backup_base)
            current_action = "opening backup info file (%s)" % backup_info
            info = open(backup_info, 'w')
            print >> info, "server_name=%s" % self.config.name

            yield "Starting backup for server %s in %s" % (self.config.name, backup_base)

            current_action = "connecting to database (%s)" % self.config.conninfo
            conn = psycopg2.connect(self.config.conninfo)
            cur = conn.cursor()
            print >> info, "version=%s" % conn.server_version

            current_action = "detecting data directory"
            cur.execute('SHOW data_directory')
            data_directory = cur.fetchone()
            print >> info, "pgdata=%s" % data_directory

            current_action = "detecting tablespaces"
            cur.execute("SELECT spcname, oid, spclocation FROM pg_tablespace WHERE spclocation != ''")
            tablespaces = cur.fetchall();
            if len(tablespaces) > 0:
                print >> info, "tablespaces=%r" % tablespaces
                for oid, name, location in tablespaces:
                    yield "\t%s, %s, %s" % (oid, name, location)

            current_action = "issuing pg_start_backup command"
            cur.execute('SELECT xlog_loc, (pg_xlogfile_name_offset(xlog_loc)).* from pg_start_backup(%s) as xlog_loc', ('BaRman backup',))
            start_xlog, start_file_name, start_file_offset = cur.fetchone()
            yield "Backup begin at xlog location: %s (%s, %08X)" % (start_xlog, start_file_name, start_file_offset)
            print >> info, "timeline=%d" % int(start_file_name[0:8])
            print >> info, "begin_time=%s" % backup_stamp
            print >> info, "begin_xlog=%s" % start_xlog
            print >> info, "begin_wal=%s" % start_file_name
            print >> info, "begin_offset=%s" % start_file_offset


            current_action = "copying files"
            try:
                rsync = RsyncPgData(ssh=self.ssh_command, ssh_options=self.ssh_options)
                retval = rsync(':%s/' % data_directory, os.path.join(backup_base, 'pgdata'))
                if retval not in (0, 24):
                    raise Exception("ERROR: data transfer failure")
            except:
                raise
            else:
                current_action = "issuing pg_stop_backup command"
            finally:
                cur.execute('SELECT xlog_loc, (pg_xlogfile_name_offset(xlog_loc)).* from pg_stop_backup() as xlog_loc')
                stop_xlog, stop_file_name, stop_file_offset = cur.fetchone()
                print >> info, "end_time=%s" % datetime.datetime.now()
                print >> info, "end_xlog=%s" % stop_xlog
                print >> info, "end_wal=%s" % stop_file_name
                print >> info, "end_offset=%s" % stop_file_offset

            print >> info, "status=DONE"

        except:
            traceback.print_exc()
            if info:
                print >> info, "status=FAILED\nerror=failure %s" % current_action
            yield "Backup failed %s" % current_action
        else:
            yield "Backup end at xlog location: %s (%s, %08X)" % (stop_xlog, stop_file_name, stop_file_offset)
            yield "Backup completed"
        finally:
            if info:
                info.close()

    def list(self):
        """
        Lists all the available backups for the server
        """
        from glob import glob
        for file in glob("%s/*/backup.info" % self.config.basebackups_directory):
            backup = Backup(file)
            if backup.status != 'DONE':
                continue
            if backup.tablespaces:
                tablespaces = [("%s:%s" % (name, location))for name, _, location in backup.tablespaces]
                yield "%s - %s - %s (tablespaces: %s)" % (self.config.name, backup.backup_id, backup.begin_time, ', '.join(tablespaces))
            else:
                yield "%s - %s - %s" % (self.config.name, backup.backup_id, backup.begin_time)

    def get_backup_directory(self, backup_id):
        """
        Get the name of the directory for the given backup
        """
        return os.path.join(self.config.basebackups_directory, backup_id)
    
    def get_backup_info_file(self, backup_id):
        """
        Get the name of information file for the given backup
        """
        return os.path.join(self.get_backup_directory(backup_id), "backup.info")
    
    def recover(self, backup_id, dest, tablespaces=[], target_time=None, target_xid=None, exclusive=False):
        """
        Performs a recovery of a backup
        """
        backup_base = self.get_backup_directory(backup_id)
        backup_info_file = self.get_backup_info_file(backup_id)
        backup = Backup(backup_info_file)
        yield "Starting restore for server %s using backup %s " % (self.config.name, backup_id)
        yield "Destination directory: %s" % dest
        if backup.tablespaces:
            tblspc_dir = os.path.join(dest, 'pg_tblspc')
            if not os.path.exists(tblspc_dir):
                os.makedirs(tblspc_dir)
            for name, oid, location in backup.tablespaces:
                if name in tablespaces:
                    location = tablespaces[name]
                tblspc_file = os.path.join(tblspc_dir, str(oid))
                if os.path.exists(tblspc_file):
                    os.unlink(tblspc_file)
                if not os.path.isdir(location):
                    os.unlink(location)
                if not os.path.exists(location):
                    os.makedirs(location)
                os.symlink(location, tblspc_file)
                yield "\t%s, %s, %s" % (oid, name, location)
        yield "Copying the base backup."
        rsync = RsyncPgData(ssh=self.ssh_command, ssh_options=self.ssh_options)
        retval = rsync('%s/' % os.path.join(backup_base, 'pgdata'), dest)
        if retval != 0:
            raise Exception("ERROR: data transfer failure")
        # Copy wal segments
        yield "Copying required wal segments."
        wal_dest = os.path.join(dest, 'pg_xlog')
        if not os.path.exists(wal_dest):
            os.makedirs(wal_dest)
        rsync.from_file_list(backup.get_required_wal_segments(), "%s/" % self.config.wals_directory, wal_dest)
        if target_time or target_xid:
            yield "Generating recovery.conf"
            recovery = open(os.path.join(dest, 'recovery.conf'))
            print >> recovery, "restore_command = 'cp %s/%%f %%p'" % self.config.wals_directory
            if target_time:
                print >> recovery, "recovery_target_time = '%s'" % target_time
            if target_xid:
                print >> recovery, "recovery_target_xid = '%s'" % target_xid
                if exclusive:
                    print >> recovery, "recovery_target_inclusive = '%s'" % (not exclusive)
        yield "Restore done"
