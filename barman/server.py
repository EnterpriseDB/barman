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

        self._read_pgsql_info()

        backup_stamp = datetime.datetime.now()

        backup_base = os.path.join(self.config.basebackups_directory, backup_stamp.strftime('%Y%m%dT%H%M%S'))

        backup_info = os.path.join(backup_base, 'backup.info')

        try:
            os.makedirs(backup_base)
            info = open(backup_info, 'w')
            print >> info, "server=%s" % self.config.name
            print >> info, "version=%s" % self.server_version

            yield "Starging backup for server %s in %s" % (self.config.name, backup_base)

            conn = psycopg2.connect(self.config.conninfo)
            cur = conn.cursor()

            cur.execute('SHOW data_directory')
            data_directory = cur.fetchone()
            yield "Data directory: %s" % (data_directory)
            print >> info, "pgdata=%s" % data_directory

            cur.execute("SELECT spcname, oid, spclocation FROM pg_tablespace WHERE spclocation != ''")
            tablespaces = cur.fetchall();
            if len(tablespaces) > 0:
                yield "Additional tablespaces detected:"
                print >> info, "tablespaces=%r" % tablespaces
                for oid, name, location in tablespaces:
                    yield "\t%s, %s, %s" % (oid, name, location)

            try:
                cur.execute('SELECT xlog_loc, (pg_xlogfile_name_offset(xlog_loc)).* from pg_start_backup(%s) as xlog_loc', ('BaRman backup',))
                start_xlog, start_file_name, start_file_offset = cur.fetchone()
                yield "Start location: %s (%s, %s)" % (start_xlog, start_file_name, start_file_offset)
                print >> info, "timeline=%d" % int(start_file_name[0:8])
                print >> info, "begin_time=%s" % backup_stamp
                print >> info, "begin_xlog=%s" % start_xlog
                print >> info, "begin_wal=%s" % start_file_name

                rsync = RsyncPgData(ssh=self.ssh_command, ssh_options=self.ssh_options)

                retval = rsync(':%s/' % data_directory, os.path.join(backup_base, 'pgdata'))
                if retval in (0, 24):
                    yield "Transfer completed"
                else:
                    yield "ERROR: data transfer failure"
            finally:
                cur.execute('SELECT xlog_loc, (pg_xlogfile_name_offset(xlog_loc)).* from pg_stop_backup() as xlog_loc')
                stop_xlog, stop_file_name, stop_file_offset = cur.fetchone()
                yield "Stop location: %s (%s, %s)" % (stop_xlog, stop_file_name, stop_file_offset)
                print >> info, "end_time=%s" % datetime.datetime.now()
                print >> info, "end_xlog=%s" % stop_xlog
                print >> info, "end_wal=%s" % stop_file_name

            yield "Writing backup info: %s" % backup_info
            print >> info, "status=DONE"

        except:
            yield "Backlup failed"
            print >> info, "status=FAILED"
            raise
        else:
            yield "Backlup completed"
        finally:
            info.close()
