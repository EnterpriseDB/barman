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

from barman import xlog
from barman.lockfile import lockfile
from barman.backup import Backup
from barman.command_wrappers import Command, RsyncPgData, Compressor, \
    Decompressor
from glob import glob
import datetime
import dateutil.parser
import logging
import os
import psycopg2
import time
import traceback

_logger = logging.getLogger(__name__)

class Server(object):

    XLOG_DB = "xlog.db"

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
        self.available_backups = None

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

    def get_available_backups(self):
        """
        Get a list of available backups
        """
        if not self.available_backups:
            self.available_backups = {}
            for filename in glob("%s/*/backup.info" % self.config.basebackups_directory):
                backup = Backup(self, filename)
                if backup.status != 'DONE':
                    continue
                self.available_backups[backup.backup_id] = backup
        return self.available_backups

    def list_backups(self):
        """
        Lists all the available backups for the server
        """
        for _, backup in self.get_available_backups().items():
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

    def get_previous_backup(self, backup_id):
        """
        Get the previous backup (if any) in the catalog
        """
        ids = sorted(self.get_available_backups().keys())
        try:
            current = ids.index(backup_id)
            if current > 0:
                return self.available_backups[ids[current - 1]]
            else:
                return None
        except ValueError:
            raise Exception('Could not find backup_id %s' % backup_id)

    def get_next_backup(self, backup_id):
        """
        Get the next backup (if any) in the catalog
        """
        ids = sorted(self.get_available_backups().keys())
        try:
            current = ids.index(backup_id)
            if current >= 0 and current < (len(ids) - 1):
                return self.available_backups[ids[current + 1]]
            else:
                return None
        except ValueError:
            raise Exception('Could not find backup_id %s' % backup_id)

    def get_required_xlog_files(self, backup, target_tli=None, target_time=None, target_xid=None):
        begin = backup.begin_wal
        end = backup.end_wal
        # If timeline isn't specified, assume it is the same timeline of the backup  
        if not target_tli:
            target_tli, _, _ = xlog.decode_segment_name(end)
        with open(os.path.join(self.config.wals_directory, self.XLOG_DB), 'r') as xlog_db:
            for line in xlog_db:
                name, _, stamp = line.split()
                if name < begin: continue
                tli, _, _ = xlog.decode_segment_name(name)
                if tli > target_tli: continue
                yield name
                if name > end:
                    end = name
                    if target_time and target_time < float(stamp):
                        break
            # return all the remaining history files
            for line in xlog_db:
                name, _, stamp = line.split()
                if xlog.is_history_file(name):
                    yield name

    def recover(self, backup_id, dest, tablespaces=[], target_tli=None, target_time=None, target_xid=None, exclusive=False):
        """
        Performs a recovery of a backup
        """
        for line in self.cron():
            yield line
        backup_base = self.get_backup_directory(backup_id)
        backup_info_file = self.get_backup_info_file(backup_id)
        backup = Backup(self, backup_info_file)
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
        if target_time or target_xid or target_tli != backup.timeline:
            wal_dest = os.path.join(dest, 'barman_xlog')
        else:
            wal_dest = os.path.join(dest, 'pg_xlog')
        if not os.path.exists(wal_dest):
            os.makedirs(wal_dest)
        xlogs = {}
        target_epoch = None
        if target_time:
            target_datetime = dateutil.parser.parse(target_time)
            target_epoch = time.mktime(target_datetime.timetuple()) + (target_datetime.microsecond / 1000000.)
        required_xlog_files = tuple(self.get_required_xlog_files(backup, target_tli, target_epoch, target_xid))
        for filename in required_xlog_files:
            hashdir = xlog.hash_dir(filename)
            if hashdir not in xlogs:
                xlogs[hashdir] = []
            xlogs[hashdir].append(filename)
        decompressor = None
        if self.config.decompression_filter:
            decompressor = Decompressor(self.config.decompression_filter)
        for prefix in xlogs:
            source_dir = os.path.join(self.config.wals_directory, prefix)
            if decompressor:
                for segment in xlogs[prefix]:
                    decompressor(os.path.join(source_dir, segment), os.path.join(wal_dest, segment))
            else:
                rsync.from_file_list(xlogs[prefix], "%s/" % os.path.join(self.config.wals_directory, prefix), wal_dest)
        if target_time or target_xid or target_tli != backup.timeline:
            yield "Generating recovery.conf"
            recovery = open(os.path.join(dest, 'recovery.conf'), 'w')
            print >> recovery, "restore_command = 'cp barman_xlog/%f %p'"
            print >> recovery, "recovery_end_command = 'rm -fr barman_xlog'"
            if target_time:
                print >> recovery, "recovery_target_time = '%s'" % target_time
            if target_tli:
                print >> recovery, "recovery_target_timeline = %s" % target_tli
            if target_xid:
                print >> recovery, "recovery_target_xid = '%s'" % target_xid
                if exclusive:
                    print >> recovery, "recovery_target_inclusive = '%s'" % (not exclusive)
        yield "Restore done"

    def cron(self, verbose=True):
        found = False
        compressor = None
        if self.config.compression_filter:
            compressor = Compressor(self.config.compression_filter, remove_origin=True)
        if verbose:
            yield "Processing xlog segments for %s" % self.config.name
        available_backups = self.get_available_backups()
        for filename in sorted(glob(os.path.join(self.config.incoming_wals_directory, '*'))):
            if not found and not verbose:
                yield "Processing xlog segments for %s" % self.config.name
            found = True
            if not len(available_backups):
                msg = "WARNING: no base backup available. Trashing file %s" % os.path.basename(filename)
                yield "\t%s" % msg
                _logger.warning(msg)
                os.unlink(filename)
                continue
            basename = os.path.basename(filename)
            destdir = os.path.join(self.config.wals_directory, xlog.hash_dir(basename))
            destfile = os.path.join(destdir, basename)
            time = os.stat(filename).st_mtime
            if not os.path.isdir(destdir):
                os.makedirs(destdir)
            if compressor:
                compressor(filename, destfile)
            else:
                os.rename(filename, destfile)
            size = os.stat(destfile).st_size
            xlogdb = os.path.join(self.config.wals_directory, self.XLOG_DB)
            xlogdb_lock = xlogdb + ".lock"
            with lockfile(xlogdb_lock, wait=True):
                with open(xlogdb, 'a') as f:
                    f.write("%s\t%s\t%s\n" % (basename, size, time))
            _logger.info('Processed file %s', filename)
            yield "\t%s" % os.path.basename(filename)
        if not found and verbose:
            yield "\tno file found"
