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

class Server(object):
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.server_txt_version = None
        self.server_version = None
        self.ssh_options = config.ssh_command.split()
        self.ssh_command = self.ssh_options.pop(0)
        self.ssh_options.extend("-o BatchMode=yes -o StrictHostKeyChecking=no".split())

    def _read_pgsql_info(self):
        conn_is_mine = self.conn == None
        try:
            if conn_is_mine: self.conn = psycopg2.connect(self.config.conninfo)
            self.server_version = self.conn.server_version
            cur = self.conn.cursor()
            cur.execute("SELECT version()")
            self.server_txt_version = cur.fetchone()
            if conn_is_mine: self.conn.close()
        except Exception:
            return False
        else:
            return True

    def check_ssh(self):
        cmd = Command(self.ssh_command, self.ssh_options)
        ret = cmd("true")
        if ret == 0:
            return "\tssh: OK"
        else:
            return "\tssh: FAILED (return code: %s)" % (ret)

    def check_postgres(self):
        if self._read_pgsql_info():
            return "\tpgsql: OK (version: %s)" % (self.server_txt_version)
        else:
            return "\tpgsql: FAILED"

    def check(self):
        yield "Server %s:" % (self.config.name)
        if self.config.description: yield "\tdescription: %s" % (self.config.description)
        yield self.check_ssh()
        yield self.check_postgres()

    def backup(self):

        backup_stamp = datetime.datetime.now()

        backup_base = os.path.join(self.config.basebackups_directory, backup_stamp.strftime('%Y%m%dT%H%M%S'))

        backup_info = os.path.join(backup_base, 'backup.info')

        current_action = None
        info = None
        try:
            current_action = "creating destination directory (%s)" % backup_base
            os.makedirs(backup_base)
            current_action = "opening backup info file (%s)" % backup_info
            print >> info, "server=%s" % self.config.name
            info = open(backup_info, 'w')

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
                pass
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
        from glob import glob
        for file in glob("%s/*/backup.info" % self.config.basebackups_directory):
            backup = Backup(file)
            if backup.status == 'DONE':
                yield "%s - %s - %s" % (self.config.name, backup.backup_id, backup.begin_time)

    def recover(self, backup_id, dest, tablespaces=[], target_time=None, target_xid=None, exclusive=False):
        backup_base = os.path.join(self.config.basebackups_directory, backup_id)
        backup_info_file = os.path.join(backup_base, "backup.info")
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
        yield "TODO: generate recovery.conf" # TODO: generate recovery.conf
        yield "Restore done"
        return
