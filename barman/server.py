#
# barman - Backup and Recovery Manager for PostgreSQL
#
# Copyright (C) 2011  2ndQuadrant Italia (Devise.IT S.r.l.) <info@2ndquadrant.it>
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

from barman import xlog, _pretty_size
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
from contextlib import contextmanager
import itertools
import shutil

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

    def check_ssh(self):
        """
        Checks SSH connection
        """
        cmd = Command(self.ssh_command, self.ssh_options)
        ret = cmd("true")
        if ret == 0:
            yield "\tssh: OK"
        else:
            yield "\tssh: FAILED (return code: %s)" % (ret)

    def check_postgres(self):
        """
        Checks PostgreSQL connection
        """
        remote_status = self.get_remote_status()
        if remote_status['server_txt_version']:
            yield "\tPostgreSQL: OK"
        else:
            yield "\tPostgreSQL: FAILED"
            return
        if remote_status['archive_mode'] == 'on':
            yield "\tarchive_mode: OK"
        else:
            yield "\tarchive_mode: FAILED (please set it to 'on')"
        if remote_status['archive_command']:
            yield "\tarchive_command: OK"
        else:
            yield "\tarchive_command: FAILED (please set it accordingly to documentation)"

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
            yield "\tdirectories: OK"
        else:
            yield "\tdirectories: FAILED (%s)" % (error)

    def check(self):
        """
        Implements the 'server check' command and makes sure SSH and PostgreSQL
        connections work properly. It checks also that backup directories exist
        (and if not, it creates them).
        """
        status = [("Server %s:" % (self.config.name),)]
        #FIXME: Description makes sense in a "check" command?
        #if self.config.description: status.append(("\tdescription: %s" % (self.config.description),))
        status.append(self.check_ssh())
        status.append(self.check_postgres())
        status.append(self.check_directories())
        return itertools.chain.from_iterable(status)

    def status_postgres(self):
        """
        Status of PostgreSQL server
        """
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
        """
        Implements the 'server status' command.
        """
        status = [("Server %s:" % (self.config.name),)]
        if self.config.description: status.append(("\tdescription: %s" % (self.config.description),))
        status.append(self.status_postgres())
        return itertools.chain.from_iterable(status)

    def get_remote_status(self):
        pg_settings = ('archive_mode', 'archive_command', 'data_directory')
        result = {}
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
        cmd = Command(self.ssh_command, self.ssh_options)
        result['last_shipped_wal'] = None
        if result['data_directory'] and result['archive_command']:
            archive_dir = os.path.join(result['data_directory'], 'pg_xlog', 'archive_status')
            out = cmd.getoutput(None, 'ls', '-t', archive_dir)[0]
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
        yield "Server %s:" % (self.config.name)
        for key in self.config.KEYS:
            if hasattr(self.config, key):
                yield "\t%s: %s" % (key, getattr(self.config, key))
        remote_status = self.get_remote_status()
        for key in remote_status:
            yield "\t%s: %s" % (key, remote_status[key])

    @contextmanager
    def pg_connect(self):
        myconn = self.conn == None
        if myconn:
            self.conn = psycopg2.connect(self.config.conninfo)
            self.server_version = self.conn.server_version
        try:
            yield self.conn
        finally:
            if myconn: self.conn.close()

    def get_pg_setting(self, name):
        with self.pg_connect() as conn:
            try:
                cur = conn.cursor()
                cur.execute('SHOW "%s"' % name.replace('"', '""'))
                return cur.fetchone()[0]
            except:
                return None

    def delete_backup(self, backup):
        yield "Deleting backup %s for server %s" % (backup.backup_id, self.config.name)
        previous_backup = self.get_previous_backup(backup.backup_id)
        next_backup = self.get_next_backup(backup.backup_id)
        # remove the backup
        backup_dir = os.path.join(self.config.basebackups_directory, backup.backup_id)
        shutil.rmtree(backup_dir)
        if not previous_backup: # backup is the first one
            yield "Delete associated WAL segments:"
            remove_until = None
            if next_backup:
                remove_until = next_backup.begin_wal
            with self.xlogdb() as fxlogdb:
                xlogdb_new = fxlogdb.name + ".new"
                with open(xlogdb_new, 'w') as fxlogdb_new:
                    for line in fxlogdb:
                        name, _, _ = self.xlogdb_parse_line(line)
                        if remove_until and name >= remove_until:
                            fxlogdb_new.write(line)
                            continue
                        else:
                            yield "\t%s" % name
                            hashdir = os.path.join(self.config.wals_directory, xlog.hash_dir(name))
                            os.unlink(os.path.join(hashdir, name))
                            try:
                                os.removedirs(hashdir)
                            except:
                                pass
                os.rename(xlogdb_new, fxlogdb.name)
        yield "Done"

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
            with self.pg_connect() as conn:
                cur = conn.cursor()
                print >> info, "version=%s" % conn.server_version

                current_action = "detecting data directory"
                data_directory = self.get_pg_setting('data_directory')
                print >> info, "pgdata=%s" % data_directory

                current_action = "detecting tablespaces"
                cur.execute("SELECT spcname, oid, spclocation FROM pg_tablespace WHERE spclocation != ''")
                tablespaces = cur.fetchall();
                if len(tablespaces) > 0:
                    print >> info, "tablespaces=%r" % tablespaces
                    for oid, name, location in tablespaces:
                        yield "\t%s, %s, %s" % (oid, name, location)

                current_action = "issuing pg_start_backup command"
                cur.execute('SELECT xlog_loc, (pg_xlogfile_name_offset(xlog_loc)).* from pg_start_backup(%s) as xlog_loc', ('BaRMan backup',))
                start_xlog, start_file_name, start_file_offset = cur.fetchone()
                yield "Backup begin at xlog location: %s (%s, %08X)" % (start_xlog, start_file_name, start_file_offset)
                print >> info, "timeline=%d" % int(start_file_name[0:8])
                print >> info, "begin_time=%s" % backup_stamp
                print >> info, "begin_xlog=%s" % start_xlog
                print >> info, "begin_wal=%s" % start_file_name
                print >> info, "begin_offset=%s" % start_file_offset


                current_action = "copying files"
                backup_dest = os.path.join(backup_base, 'pgdata')
                try:
                    rsync = RsyncPgData(ssh=self.ssh_command, ssh_options=self.ssh_options)
                    retval = rsync(':%s/' % data_directory, backup_dest)
                    if retval not in (0, 24):
                        raise Exception("ERROR: data transfer failure")
                    current_action = "calculating backup size"
                    backup_size = 0
                    for dirpath, _, filenames in os.walk(backup_dest):
                        for f in filenames:
                            fp = os.path.join(dirpath, f)
                            backup_size += os.path.getsize(fp)
                    print >> info, "size=%s" % backup_size
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
        for backup in self.get_available_backups().values():
            _, backup_wal_size, _, wal_until_next_size, _ = self.get_wal_info(backup)
            backup_size = _pretty_size(backup.size or 0 + backup_wal_size)
            wal_size = _pretty_size(wal_until_next_size)
            end_time = backup.end_time.ctime()
            if backup.tablespaces:
                tablespaces = [("%s:%s" % (name, location))for name, _, location in backup.tablespaces]
                yield "%s %s - %s - Size: %s - WAL Size: %s (tablespaces: %s)" % (self.config.name, backup.backup_id, end_time, backup_size, wal_size, ', '.join(tablespaces))
            else:
                yield "%s %s - %s - Size: %s - WAL Size: %s" % (self.config.name, backup.backup_id, end_time, backup_size, wal_size)

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
        with self.xlogdb() as fxlogdb:
            for line in fxlogdb:
                name, _, stamp = self.xlogdb_parse_line(line)
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
                name, _, stamp = self.xlogdb_parse_line(line)
                if xlog.is_history_file(name):
                    yield name

    def get_wal_info(self, backup):
        begin = backup.begin_wal
        end = backup.end_wal
        next_end = None
        if self.get_next_backup(backup.backup_id):
            next_end = self.get_next_backup(backup.backup_id).end_wal
        backup_tli, _, _ = xlog.decode_segment_name(begin)

        # counters
        wal_num = 0
        wal_size = 0
        wal_until_next_num = 0
        wal_until_next_size = 0
        wal_last = None

        with self.xlogdb() as fxlogdb:
            for line in fxlogdb:
                name, size, _ = self.xlogdb_parse_line(line)
                if name < begin: continue
                tli, _, _ = xlog.decode_segment_name(name)
                if tli > backup_tli: continue
                if not xlog.is_wal_file(name): continue
                if next_end and name > next_end:
                    break
                # count
                if name <= end:
                    wal_num += 1
                    wal_size += size
                else:
                    wal_until_next_num += 1
                    wal_until_next_size += size
                wal_last = name
        return wal_num, wal_size, wal_until_next_num, wal_until_next_size, wal_last

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
        rsync = RsyncPgData()
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
        else:
            # avoid shipping of just recovered pg_xlog files
            status_dir = os.path.join(wal_dest, 'archive_status')
            os.makedirs(status_dir) # no need to check, it must not exist
            for filename in required_xlog_files:
                with file(os.path.join(status_dir, "%s.done" % filename), 'a') as f:
                    f.write('')
        yield "Restore done"

    def cron(self, verbose=True):
        found = False
        compressor = None
        if self.config.compression_filter:
            compressor = Compressor(self.config.compression_filter, remove_origin=False)
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
                shutil.copystat(filename, destfile)
                os.unlink(filename)
            else:
                os.rename(filename, destfile)
            size = os.stat(destfile).st_size
            with self.xlogdb('a') as fxlogdb:
                fxlogdb.write("%s\t%s\t%s\n" % (basename, size, time))
            _logger.info('Processed file %s', filename)
            yield "\t%s" % os.path.basename(filename)
        if not found and verbose:
            yield "\tno file found"

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
        name, size, stamp = line.split()
        return name, int(size), float(stamp)
