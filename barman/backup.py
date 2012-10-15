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

''' This module represents a bakup. '''

from barman import xlog, _pretty_size
from barman.command_wrappers import RsyncPgData, Command
from barman.compression import CompressionManager, CompressionIncompatibility
from glob import glob
import ast
import datetime
import dateutil.parser
import logging
import os
import shutil
import time
import tempfile
import re

_logger = logging.getLogger(__name__)

class BackupInfoBadInitialisation(Exception):
    '''Exception for a bad initialization error '''
    pass

class BackupInfo(object):
    '''This class contains information about a single backup '''

    '''Conversion to string '''
    EMPTY = 'EMPTY'
    STARTED = 'STARTED'
    FAILED = 'FAILED'
    DONE = 'DONE'
    STATUS_ALL = (EMPTY, STARTED, DONE, FAILED)
    STATUS_NOT_EMPTY = (STARTED, DONE, FAILED)
    '''PostgreSQL Physical base backup information '''
    KEYS = [ 'version', 'pgdata', 'tablespaces', 'timeline',
             'begin_time', 'begin_xlog', 'begin_wal', 'begin_offset',
             'size', 'end_time', 'end_xlog', 'end_wal', 'end_offset',
             'status', 'server_name', 'error', 'mode',
             'config_file', 'hba_file', 'ident_file',
    ]
    '''Attributes of the backup.info file '''
    TYPES = {'tablespaces':ast.literal_eval, # Treat the tablespaces as a literal Python list of tuples
             'timeline':int, # Timeline is an integer
             'begin_time':dateutil.parser.parse,
             'end_time':dateutil.parser.parse,
             'size':int,
    }
    '''Conversion from string '''
    TYPES_OUT = {'tablespaces':repr, # Treat the tablespaces as a literal Python list of tuples
    }

    def __init__(self, server, info_file=None, backup_id=None):
        '''Constructor '''
        # Initialises the attributes for the object based on the predefined keys
        self.__dict__.update(dict.fromkeys(self.KEYS))
        self.server = server
        self.config = server.config
        self.backup_manager = self.server.backup_manager
        if backup_id:
            # Cannot pass both info_file and backup_id
            if info_file:
                raise BackupInfoBadInitialisation()
            self.backup_id = backup_id
            info_file = self.get_filename()
            # Check if a backup info file for a given server and a given ID
            # already exists. If not, create it from scratch
            if not os.path.exists(info_file):
                info_file = None
                self.set_attribute("status", BackupInfo.EMPTY)
                self.set_attribute('server_name', self.config.name)
                self.set_attribute('mode', self.backup_manager.name)
        elif not info_file:
            raise BackupInfoBadInitialisation()

        if info_file:
            # Looks for a backup.info file
            if hasattr(info_file, 'read'): # We have been given a file-like object
                info = info_file
                filename = os.path.abspath(info_file.name)
            else: # Just a file name
                filename = os.path.abspath(info_file)
                info = open(info_file, 'r').readlines()
            # Detect the backup ID
            self.backup_id = self.detect_backup_id(filename)
            # TODO: detect mismatch between current server backup manager and the one on disk
            # Parses the backup.info file
            for line in info:
                try:
                    key, value = line.rstrip().split('=')
                except:
                    raise Exception('invalid line in backup file: %s' % line)
                if key in self.TYPES:
                    self.set_attribute(key, self.TYPES[key](value))
                else:
                    self.set_attribute(key, value)

    def get_required_wal_segments(self):
        '''Get the list of required WAL segments for the current backup'''
        return xlog.enumerate_segments(self.begin_wal, self.end_wal)

    def get_list_of_files(self, target):
        '''Get the list of files for the current backup'''
        # Walk down the base backup directory
        if target in ('data', 'standalone', 'full'):
            for root, _, files in os.walk(self.get_basebackup_directory()):
                for f in files:
                    yield os.path.join(root, f)
        if target in ('standalone'):
            # List all the WAL files for this backup
            for x in self.get_required_wal_segments():
                hashdir = os.path.join(self.config.wals_directory, xlog.hash_dir(x))
                yield os.path.join(hashdir, x)
        if target in ('wal', 'full'):
            for x, _ in self.server.get_wal_until_next_backup(self):
                hashdir = os.path.join(self.config.wals_directory, xlog.hash_dir(x))
                yield os.path.join(hashdir, x)

    def detect_backup_id(self, filename):
        '''Detect the backup ID from the name of the parent dir of the info file'''
        return os.path.basename(os.path.dirname(filename))

    def show(self):
        '''Show backup information'''
        yield "Backup %s:" % (self.backup_id)
        if self.status == BackupInfo.DONE:
            try:
                previous_backup = self.backup_manager.get_previous_backup(self.backup_id)
                next_backup = self.backup_manager.get_next_backup(self.backup_id)
                wal_num, wal_size, wal_until_next_num, wal_until_next_size, wal_last = self.server.get_wal_info(self)
                yield "  Server Name       : %s" % self.server_name
                yield "  Status:           : %s" % self.status
                yield "  PostgreSQL Version: %s" % self.version
                yield "  PGDATA directory  : %s" % self.pgdata
                if self.tablespaces:
                    yield "  Tablespaces:"
                    for name, _, location in self.tablespaces:
                        yield "    %s: %s" % (name, location)
                yield ""
                yield "  Base backup information:"
                yield "    Disk usage      : %s" % _pretty_size(self.size + wal_size)
                yield "    Timeline        : %s" % self.timeline
                yield "    Begin WAL       : %s" % self.begin_wal
                yield "    End WAL         : %s" % self.end_wal
                yield "    WAL number      : %s" % wal_num
                yield "    Begin time      : %s" % self.begin_time
                yield "    End time        : %s" % self.end_time
                yield "    Begin Offset    : %s" % self.begin_offset
                yield "    End Offset      : %s" % self.end_offset
                yield "    Begin XLOG      : %s" % self.begin_xlog
                yield "    End XLOG        : %s" % self.end_xlog
                yield ""
                yield "  WAL information:"
                yield "    No of files     : %s" % wal_until_next_num
                yield "    Disk usage      : %s" % _pretty_size(wal_until_next_size)
                yield "    Last available  : %s" % wal_last
                yield ""
                yield "  Catalog information:"
                if previous_backup:
                    yield "    Previous Backup : %s" % previous_backup.backup_id
                else:
                    yield "    Previous Backup : - (this is the oldest base backup)"
                if next_backup:
                    yield "    Next Backup     : %s" % next_backup.backup_id
                else:
                    yield "    Next Backup     : - (this is the latest base backup)"

            except:
                pass
        else:
            yield "  Server Name       : %s" % self.server_name
            yield "  Status:           : %s" % self.status
            if self.error:
                yield "  Error:            : %s" % self.error

    def get_basebackup_directory(self):
        '''
        Get the default filename for the backup.info file based on
        backup ID and server directory for base backups
        '''
        return os.path.join(self.config.basebackups_directory,
            self.backup_id)

    def get_filename(self):
        '''
        Get the default filename for the backup.info file based on
        backup ID and server directory for base backups
        '''
        return os.path.join(self.get_basebackup_directory(), 'backup.info')

    def set_attribute(self, key, value):
        '''Set a value for a given key'''
        if key not in self.KEYS:
            raise Exception('invalid key for backup info: %s' % key)
        self.__dict__[key] = value

    def save(self):
        '''Save a backup information file'''
        # Make sure the base backup directory exists
        dirname = self.get_basebackup_directory()
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        # Open the file for writing and flushes the content
        # of the dictionary in alphabetical order and ignoring
        # null values
        with open(self.get_filename(), 'w') as info:
            for key in sorted(self.KEYS):
                if not self.__dict__[key]:
                    continue
                if key in self.TYPES_OUT:
                    info.write("%s=%s\n" % (key, self.TYPES_OUT[key](self.__dict__[key])))
                else:
                    info.write("%s=%s\n" % (key, self.__dict__[key]))


class BackupManager(object):
    '''Manager of the backup archive for a server'''

    DEFAULT_STATUS_FILTER = (BackupInfo.DONE,)

    def __init__(self, server):
        '''Constructor'''
        self.name = "default"
        self.server = server
        self.config = server.config
        self.available_backups = {}
        self.compression_manager = CompressionManager(self.config)
        
        # used for error messages
        self.current_action = None

    def get_available_backups(self, status_filter=DEFAULT_STATUS_FILTER):
        '''
        Get a list of available backups

        :param status_filter: default DEFAULT_STATUS_FILTER. The status of the backup list returned
        '''
        if not isinstance(status_filter, tuple):
            status_filter = tuple(status_filter)
        if status_filter not in self.available_backups:
            available_backups = {}
            for filename in glob("%s/*/backup.info" % self.config.basebackups_directory):
                backup = BackupInfo(self.server, filename)
                if backup.status not in status_filter:
                        continue
                available_backups[backup.backup_id] = backup
            self.available_backups[status_filter] = available_backups
            return available_backups
        else:
            return self.available_backups[status_filter]

    def get_previous_backup(self, backup_id, status_filter=DEFAULT_STATUS_FILTER):
        '''
        Get the previous backup (if any) in the catalog

        :param status_filter: default DEFAULT_STATUS_FILTER. The status of the backup returned
        '''
        if not isinstance(status_filter, tuple):
            status_filter = tuple(status_filter)
        backup = BackupInfo(self.server, backup_id=backup_id)
        available_backups = self.get_available_backups(status_filter + (backup.status,))
        ids = sorted(available_backups.keys())
        try:
            current = ids.index(backup_id)
            while current > 0:
                res = available_backups[ids[current - 1]]
                if res.status in status_filter:
                    return res
                current -= 1
            else:
                return None
        except ValueError:
            raise Exception('Could not find backup_id %s' % backup_id)

    def get_next_backup(self, backup_id, status_filter=DEFAULT_STATUS_FILTER):
        '''
        Get the next backup (if any) in the catalog

        :param status_filter: default DEFAULT_STATUS_FILTER. The status of the backup returned
        '''
        if not isinstance(status_filter, tuple):
            status_filter = tuple(status_filter)
        backup = BackupInfo(self.server, backup_id=backup_id)
        available_backups = self.get_available_backups(status_filter + (backup.status,))
        ids = sorted(available_backups.keys())
        try:
            current = ids.index(backup_id)
            while current < (len(ids) - 1):
                res = available_backups[ids[current + 1]]
                if res.status in status_filter:
                    return res
                current += 1
            else:
                return None
        except ValueError:
            raise Exception('Could not find backup_id %s' % backup_id)

    def get_last_backup(self, status_filter=DEFAULT_STATUS_FILTER):
        '''
        Get the last backup (if any) in the catalog

        :param status_filter: default DEFAULT_STATUS_FILTER. The status of the backup returned
        '''
        available_backups = self.get_available_backups(status_filter)
        if len(available_backups) == 0:
            return None

        ids = sorted(available_backups.keys())
        return ids[-1]

    def get_first_backup(self, status_filter=DEFAULT_STATUS_FILTER):
        '''
        Get the first backup (if any) in the catalog

        :param status_filter: default DEFAULT_STATUS_FILTER. The status of the backup returned
        '''
        available_backups = self.get_available_backups(status_filter)
        if len(available_backups) == 0:
            return None

        ids = sorted(available_backups.keys())
        return ids[0]

    def delete_backup(self, backup):
        '''
        Delete a backup

        :param backup: the backup to delete
        '''
        yield "Deleting backup %s for server %s" % (backup.backup_id, self.config.name)
        previous_backup = self.get_previous_backup(backup.backup_id)
        next_backup = self.get_next_backup(backup.backup_id)
        # remove the backup
        self.delete_basebackup(backup)
        if not previous_backup:  # backup is the first one
            yield "Delete associated WAL segments:"
            remove_until = None
            if next_backup:
                remove_until = next_backup.begin_wal
            with self.server.xlogdb() as fxlogdb:
                xlogdb_new = fxlogdb.name + ".new"
                with open(xlogdb_new, 'w') as fxlogdb_new:
                    for line in fxlogdb:
                        name, _, _, _ = self.server.xlogdb_parse_line(line)
                        if remove_until and name >= remove_until:
                            fxlogdb_new.write(line)
                            continue
                        else:
                            yield "\t%s" % name
                            # Delete the WAL segment
                            self.delete_wal(name)
                os.rename(xlogdb_new, fxlogdb.name)
        yield "Done"


    def build_script_env(self, backup_info, phase):
        """
        Prepare the environment for executing a script
        """
        previous_backup = self.get_previous_backup(backup_info.backup_id)
        env = {}
        env['BARMAN_BACKUP_DIR'] = backup_info.get_basebackup_directory()
        env['BARMAN_SERVER'] = self.config.name
        env['BARMAN_CONFIGURATION'] = self.config.config.config_file
        env['BARMAN_BACKUP_ID'] = backup_info.backup_id
        env['BARMAN_PREVIOUS_ID'] =  previous_backup.backup_id if previous_backup else ''
        env['BARMAN_PHASE'] = phase
        env['BARMAN_STATUS'] = backup_info.status
        env['BARMAN_ERROR'] = backup_info.error or ''
        return env

    def run_pre_backup_script(self, backup_info):
        '''
        Run the pre_backup_script if configured.
        This method must never throw any exception
        '''
        try:
            script = self.config.pre_backup_script
            if script:
                _logger.info("Attempt to run pre_backup_script: %s", script)
                cmd = Command(
                    script,
                    env_append=self.build_script_env(backup_info, 'pre'),
                    shell=True, check=False)
                ret = cmd()
                _logger.info("pre_backup_script returned %d", ret)
        except Exception:
            _logger.exception('Exception running pre_backup_script')

    def run_post_backup_script(self, backup_info):
        '''
        Run the post_backup_script if configured.
        This method must never throw any exception
        '''
        try:
            script = self.config.post_backup_script
            if script:
                _logger.info("Attempt to run post_backup_script: %s", script)
                cmd = Command(
                    script,
                    env_append=self.build_script_env(backup_info, 'post'),
                    shell=True, check=False)
                ret = cmd()
                _logger.info("post_backup_script returned %d", ret)
        except Exception:
            _logger.exception('Exception running post_backup_script')

    def backup(self):
        '''
        Performs a backup for the server
        '''
        _logger.debug("initialising backup information")
        backup_stamp = datetime.datetime.now()
        self.current_action = "starting backup"
        backup_info = None
        try:
            backup_info = BackupInfo(self.server, backup_id=backup_stamp.strftime('%Y%m%dT%H%M%S'))
            backup_info.save()
            msg = "Starting backup for server %s in %s" % (self.config.name,
               backup_info.get_basebackup_directory())
            _logger.info(msg)
            yield msg

            # Run the pre-backup-script if present.
            self.run_pre_backup_script(backup_info)

            # Start the backup
            self.backup_start(backup_info)
            backup_info.set_attribute("begin_time", backup_stamp)
            backup_info.save()
            msg = "Backup start at xlog location: %s (%s, %08X)" % (backup_info.begin_xlog,
                backup_info.begin_wal, backup_info.begin_offset)
            yield msg
            _logger.info(msg)

            self.current_action = "copying files"
            _logger.debug(self.current_action)
            try:
                # Start the copy
                msg = "Copying files."
                yield msg
                _logger.info(msg)
                backup_size = self.backup_copy(backup_info)
                backup_info.set_attribute("size", backup_size)
                msg = "Copy done."
                yield msg
                _logger.info(msg)
            except:
                raise
            else:
                self.current_action = "issuing stop of the backup"
                msg = "Asking PostgreSQL server to finalize the backup."
                yield msg
                _logger.info(msg)
            finally:
                self.backup_stop(backup_info)

            backup_info.set_attribute("status", "DONE")

        except:
            if backup_info:
                backup_info.set_attribute("status", "FAILED")
                backup_info.set_attribute("error", "failure %s" % self.current_action)

            msg = "Backup failed %s" % self.current_action
            _logger.exception(msg)
            yield msg

        else:
            msg = "Backup end at xlog location: %s (%s, %08X)" % (backup_info.end_xlog,
                backup_info.end_wal, backup_info.end_offset)
            _logger.info(msg)
            yield msg
            msg = "Backup completed"
            _logger.info(msg)
            yield msg
        finally:
            if backup_info:
                backup_info.save()

            # Run the post-backup-script if present.
            self.run_post_backup_script(backup_info)


    def recover(self, backup, dest, tablespaces, target_tli, target_time, target_xid, exclusive, remote_command):
        '''
        Performs a recovery of a backup

        :param backup: the backup to recover
        :param dest: the destination directory
        :param tablespaces: a dictionary of tablespaces
        :param target_tli: the target timeline
        :param target_time: the target time
        :param target_xid: the target xid
        :param exclusive: whether the recovery is exlusive or not
        :param remote_command: default None. The remote command to recover the base backup,
                               in case of remote backup.
        '''
        for line in self.cron(False):
            yield line

        recovery_dest = 'local'
        if remote_command:
            recovery_dest = 'remote'
            rsync = RsyncPgData(ssh=remote_command)
        msg = "Starting %s restore for server %s using backup %s " % (recovery_dest, self.config.name, backup.backup_id)
        yield msg
        _logger.info(msg)

        msg = "Destination directory: %s" % dest
        yield msg
        _logger.info(msg)
        if backup.tablespaces:
            if remote_command:
                # TODO: remote dir preparation
                msg = "Skipping remote directory preparation, you must have done it by yourself."
                yield msg
                _logger.warning(msg)
            else:
                tblspc_dir = os.path.join(dest, 'pg_tblspc')
                if not os.path.exists(tblspc_dir):
                    os.makedirs(tblspc_dir)
                for name, oid, location in backup.tablespaces:
                    try:
                        if name in tablespaces:
                            location = tablespaces[name]
                        tblspc_file = os.path.join(tblspc_dir, str(oid))
                        if os.path.exists(tblspc_file):
                            os.unlink(tblspc_file)
                        if os.path.exists(location) and not os.path.isdir(location):
                            os.unlink(location)
                        if not os.path.exists(location):
                            os.makedirs(location)
                        # test permissiones
                        barman_write_check_file = os.path.join(location, '.barman_write_check')
                        file(barman_write_check_file, 'a').close()
                        os.unlink(barman_write_check_file)
                        os.symlink(location, tblspc_file)
                    except:
                        msg = "ERROR: unable to prepare '%s' tablespace (destination '%s')" % (name, location)
                        _logger.critical(msg)
                        raise SystemExit(msg)
                    yield "\t%s, %s, %s" % (oid, name, location)
        target_epoch = None
        if target_time:
            try:
                target_datetime = dateutil.parser.parse(target_time)
            except:
                msg = "ERROR: unable to parse the target time parameter %r" % target_time
                _logger.critical(msg)
                raise SystemExit(msg)
            target_epoch = time.mktime(target_datetime.timetuple()) + (target_datetime.microsecond / 1000000.)
        if target_time or target_xid or (target_tli and target_tli != backup.timeline):
            targets = {}
            if target_time:
                targets['time'] = str(target_datetime)
            if target_xid:
                targets['xid'] = str(target_xid)
            if target_tli and target_tli != backup.timeline:
                targets['timeline'] = str(target_tli)
            yield "Doing PITR. Recovery target %s" % \
                (", ".join(["%s: %r" % (k, v) for k, v in targets.items()]))

        # Copy the base backup
        msg = "Copying the base backup."
        yield msg
        _logger.info(msg)
        self.recover_basebackup_copy(backup, dest, remote_command)
        _logger.info("Base backup copied.")

        # Prepare WAL segments local directory
        msg = "Copying required wal segments."
        _logger.info(msg)
        yield msg
        if target_time or target_xid or (target_tli and target_tli != backup.timeline):
            wal_dest = os.path.join(dest, 'barman_xlog')
        else:
            wal_dest = os.path.join(dest, 'pg_xlog')
        # Retrieve the list of required WAL segments according to recovery options
        xlogs = {}
        required_xlog_files = tuple(self.server.get_required_xlog_files(backup, target_tli, target_epoch, target_xid))
        for filename in required_xlog_files:
            hashdir = xlog.hash_dir(filename)
            if hashdir not in xlogs:
                xlogs[hashdir] = []
            xlogs[hashdir].append(filename)
        # Check decompression options
        decompressor = self.compression_manager.get_decompressor()

        # Restore WAL segments
        self.recover_xlog_copy(decompressor, xlogs, wal_dest, remote_command)
        _logger.info("Wal segmets copied.")

        # Generate recovery.conf file (only if needed by PITR)
        if target_time or target_xid or (target_tli and target_tli != backup.timeline):
            msg = "Generating recovery.conf"
            yield  msg
            _logger.info(msg)
            if remote_command:
                tempdir = tempfile.mkdtemp(prefix='barman_recovery-')
                recovery = open(os.path.join(tempdir, 'recovery.conf'), 'w')
            else:
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
            recovery.close()
            if remote_command:
                recovery = rsync.from_file_list(['recovery.conf'], tempdir, ':%s' % dest)
                shutil.rmtree(tempdir)
            _logger.info('recovery.conf generated')
        else:
            # avoid shipping of just recovered pg_xlog files
            if remote_command:
                status_dir = tempfile.mkdtemp(prefix='barman_xlog_status-')
            else:
                status_dir = os.path.join(wal_dest, 'archive_status')
                os.makedirs(status_dir) # no need to check, it must not exist
            for filename in required_xlog_files:
                with file(os.path.join(status_dir, "%s.done" % filename), 'a') as f:
                    f.write('')
            if remote_command:
                retval = rsync('%s/' % status_dir, ':%s' % os.path.join(wal_dest, 'archive_status'))
                if retval != 0:
                    msg = "WARNING: unable to populate pg_xlog/archive_status dorectory"
                    yield msg
                    _logger.warning(msg)
                shutil.rmtree(status_dir)


        # Disable dangerous setting in the target data dir
        if remote_command:
            tempdir = tempfile.mkdtemp(prefix='barman_recovery-')
            pg_config = os.path.join(tempdir, 'postgresql.conf')
            shutil.copy2(os.path.join(backup.get_basebackup_directory(), 'pgdata', 'postgresql.conf'), pg_config)
        else:
            pg_config = os.path.join(dest, 'postgresql.conf')
        if self.pg_config_mangle(pg_config,
                              {'archive_command': 'false'},
                              "%s.origin" % pg_config):
            msg = "The archive_command was set to 'false' to prevent data losses."
            yield msg
            _logger.info(msg)

        # Find dangerous options in the configuration file (locations)
        clashes = self.pg_config_detect_possible_issues(pg_config)

        if remote_command:
            recovery = rsync.from_file_list(['postgresql.conf', 'postgresql.conf.origin'], tempdir, ':%s' % dest)
            shutil.rmtree(tempdir)


        yield ""
        yield "Your PostgreSQL server has been successfully prepared for recovery!"
        yield ""
        yield "Please review network and archive related settings in the PostgreSQL"
        yield "configuration file before starting the just recovered instance."
        yield ""
        if clashes:
            yield "WARNING: Before starting up the recovered PostgreSQL server,"
            yield "please review also the settings of the following configuration"
            yield "options as they might interfere with your current recovery attempt:"
            yield ""

            for name, value in sorted(clashes.items()):
                yield "    %s = %s" % (name, value)

            yield ""
        _logger.info("Recovery completed successful.")


    def cron(self, verbose):
        '''
        Executes maintenance operations, such as WAL trashing.

        :param verbose: print some information
        '''
        found = False
        compressor = self.compression_manager.get_compressor()
        with self.server.xlogdb('a') as fxlogdb:
            if verbose:
                yield "Processing xlog segments for %s" % self.config.name
            available_backups = self.get_available_backups(BackupInfo.STATUS_ALL)
            for filename in sorted(glob(os.path.join(self.config.incoming_wals_directory, '*'))):
                if not found and not verbose:
                    yield "Processing xlog segments for %s" % self.config.name
                found = True
                if not len(available_backups):
                    msg = "No base backup available. Trashing file %s" % os.path.basename(filename)
                    yield "\t%s" % msg
                    _logger.warning(msg)
                    os.unlink(filename)
                    continue
                # Archive the WAL file
                basename, size, time = self.cron_wal_archival(compressor, filename)

                # Updates the information of the WAL archive with the latest segement's
                fxlogdb.write("%s\t%s\t%s\t%s\n" % (basename, size, time, self.config.compression))
                _logger.info('Processed file %s', filename)
                yield "\t%s" % os.path.basename(filename)
        if not found and verbose:
            yield "\tno file found"


    #
    # Hooks
    #

    def delete_basebackup(self, backup):
        '''
        Delete the given base backup

        :param backup: the backup to delete
        '''
        backup_dir = backup.get_basebackup_directory();
        shutil.rmtree(backup_dir)

    def delete_wal(self, name):
        '''
        Delete a WAL segment, with the given name

        :param name: the name of the WAL to delete
        '''
        hashdir = os.path.join(self.config.wals_directory, xlog.hash_dir(name))
        os.unlink(os.path.join(hashdir, name))
        try:
            os.removedirs(hashdir)
        except:
            pass

    def backup_start(self, backup_info):
        '''
        Start of the backup

        :param backup_info: the backup information structure
        '''
        self.current_action = "connecting to database (%s)" % self.config.conninfo
        _logger.debug(self.current_action)

        # Set the PostgreSQL data directory
        self.current_action = "detecting data directory"
        _logger.debug(self.current_action)
        data_directory = self.server.get_pg_setting('data_directory')
        backup_info.set_attribute('pgdata', data_directory)

        # Set server version
        backup_info.set_attribute('version', self.server.server_version)

        # Set configuration files location
        cf = self.server.get_pg_configuration_files()
        if cf:
            for key in sorted(cf.keys()):
                backup_info.set_attribute(key, cf[key])

        # Get server version and tablespaces information
        self.current_action = "detecting tablespaces"
        _logger.debug(self.current_action)
        tablespaces = self.server.get_pg_tablespaces()
        if tablespaces and len(tablespaces) > 0:
            backup_info.set_attribute("tablespaces", tablespaces)
            for oid, name, location in tablespaces:
                msg = "\t%s, %s, %s" % (oid, name, location)
                _logger.info(msg)

        # Issue pg_start_backup on the PostgreSQL server
        self.current_action = "issuing pg_start_backup command"
        _logger.debug(self.current_action)
        start_row = self.server.pg_start_backup()
        if start_row:
            start_xlog, start_file_name, start_file_offset = start_row
            backup_info.set_attribute("status", "STARTED")
            backup_info.set_attribute("timeline", int(start_file_name[0:8], 16))
            backup_info.set_attribute("begin_xlog", start_xlog)
            backup_info.set_attribute("begin_wal", start_file_name)
            backup_info.set_attribute("begin_offset", start_file_offset)
        else:
            self.current_action = "starting the backup: PostgreSQL server is already in exclusive backup mode"
            raise Exception('concurrent exclusive backups are not allowed')

    def backup_copy(self, backup_info):
        '''
        Perform the copy of the backup.
        This function returns the size of the backup (in bytes)

        :param backup_info: the backup information structure
        '''
        backup_dest = os.path.join(backup_info.get_basebackup_directory(), 'pgdata')
        rsync = RsyncPgData(ssh=self.server.ssh_command, ssh_options=self.server.ssh_options)
        retval = rsync(':%s/' % backup_info.pgdata, backup_dest)
        if retval not in (0, 24):
            msg = "ERROR: data transfer failure"
            _logger.exception(msg)
            raise Exception(msg)

        # Copy configuration files (if not inside PGDATA)
        self.current_action = "copying configuration files"
        _logger.debug(self.current_action)
        cf = self.server.get_pg_configuration_files()
        if cf:
            for key in sorted(cf.keys()):
                # Consider only those that reside outside of the original PGDATA
                if cf[key]:
                    if cf[key].find(backup_info.pgdata) == 0:
                        self.current_action = "skipping %s as contained in %s directory" % (key, backup_info.pgdata)
                        _logger.debug(self.current_action)
                        continue
                    else:
                        self.current_action = "copying %s as outside %s directory" % (key, backup_info.pgdata)
                        _logger.info(self.current_action)
                        retval = rsync(':%s' % cf[key], backup_dest)
                        if retval not in (0, 24):
                            raise Exception("ERROR: data transfer failure")

        self.current_action = "calculating backup size"
        _logger.debug(self.current_action)
        backup_size = 0
        for dirpath, _, filenames in os.walk(backup_dest):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                backup_size += os.path.getsize(fp)
        return backup_size

    def backup_stop(self, backup_info):
        '''
        Stop the backup

        :param backup_info: the backup information structure
        '''
        stop_xlog, stop_file_name, stop_file_offset = self.server.pg_stop_backup()
        backup_info.set_attribute("end_time", datetime.datetime.now())
        backup_info.set_attribute("end_xlog", stop_xlog)
        backup_info.set_attribute("end_wal", stop_file_name)
        backup_info.set_attribute("end_offset", stop_file_offset)

    def recover_basebackup_copy(self, backup, dest, remote_command=None):
        '''
        Perform the actual copy of the base backup for recovery purposes

        :param backup: the backup to recover
        :param dest: the destination directory
        :param remote_command: default None. The remote command to recover the base backup,
                               in case of remote backup.
        '''
        rsync = RsyncPgData(ssh=remote_command)
        sourcedir = '%s/' % os.path.join(backup.get_basebackup_directory(), 'pgdata')
        if remote_command:
            dest = ':%s' % dest
        retval = rsync(sourcedir, dest)
        if retval != 0:
            raise Exception("ERROR: data transfer failure")
        # TODO: Manage different location for configuration files that were not within the data directory

    def recover_xlog_copy(self, decompressor, xlogs, wal_dest, remote_command=None):
        '''
        Restore WAL segments

        :param decompressor: the decompressor for the file (if any)
        :param xlogs: the xlog dictionary to recover
        :param wal_dest: the destination directory for xlog recover
        :param remote_command: default None. The remote command to recover the xlog,
                               in case of remote backup.
        '''
        rsync = RsyncPgData(ssh=remote_command)
        if remote_command:
            # If remote recovery tell rsync to copy them remotely
            wal_dest = ':%s' % wal_dest
        else:
            # we will not use rsync: destdir must exists
            if not os.path.exists(wal_dest):
                os.makedirs(wal_dest)
        if decompressor and remote_command:
            xlog_spool = tempfile.mkdtemp(prefix='barman_xlog-')
        for prefix in xlogs:
            source_dir = os.path.join(self.config.wals_directory, prefix)
            if decompressor:
                if remote_command:
                    for segment in xlogs[prefix]:
                        decompressor(os.path.join(source_dir, segment), os.path.join(xlog_spool, segment))
                    rsync.from_file_list(xlogs[prefix], xlog_spool, wal_dest)
                    for segment in xlogs[prefix]:
                        os.unlink(os.path.join(xlog_spool, segment))
                else:
                    # decompress directly to the right place
                    for segment in xlogs[prefix]:
                        decompressor(os.path.join(source_dir, segment), os.path.join(wal_dest, segment))
            else:
                rsync.from_file_list(xlogs[prefix], "%s/" % os.path.join(self.config.wals_directory, prefix), wal_dest)
        if decompressor and remote_command:
            shutil.rmtree(xlog_spool)


    def cron_wal_archival(self, compressor, filename):
        '''
        Archive a WAL segment from the incoming directory.
        This function returns the name, the size and the time of the WAL file.

        :param compressor: the compressor for the file (if any)
        :param filename: the name of the WAthe name of the WAL
        '''
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
        return basename, os.stat(destfile).st_size, time

    def check(self):
        '''
        This function performs some checks on the server.
        Returns 0 if all went well, 1 if any of the checks fails
        '''
        if not self.compression_manager.check():
            yield ("\tcompression settings: FAILED", False)
        else:
            status = 'OK'
            try:
                self.compression_manager.get_compressor()
            except CompressionIncompatibility, field:
                yield ("\tcompressor settings '%s': FAILED" % field, False)
                status = 'FAILED'
            try:
                self.compression_manager.get_decompressor()
            except CompressionIncompatibility, field:
                yield ("\tdecompressor settings '%s': FAILED" % field, False)
                status = 'FAILED'

            yield ("\tcompression settings: %s" % status, status == 'OK')

    def status(self):
        '''This function show the server status '''
        no_backups = len(self.get_available_backups())
        yield "\tNo. of available backups: %d" % no_backups
        if no_backups == 1:
            yield "\tfirst/last available backup: %s" % self.get_first_backup()
        elif no_backups > 1:
            yield "\tfirst available backup: %s" % self.get_first_backup()
            yield "\tlast available backup: %s" % self.get_last_backup()

    def pg_config_mangle(self, filename, settings, backup_filename=None):
        '''This method modifies the postgres configuration file,
        commenting settings passed as argument, and adding the barman ones.

        If backup_filename is True, it writes on a backup copy.

        :param filename: the Postgres configuration file
        :param settings: settings to mangle dictionary
        :param backup_filename: default False. If True, work on a copy
        '''
        if backup_filename:
            shutil.copy2(filename, backup_filename)

        with open(filename) as f:
            content = f.readlines()

        r = re.compile('^\s*([^\s=]+)\s*=\s*(.*)$')
        mangled = False
        with open(filename, 'w') as f:
            for line in content:
                rm = r.match(line)
                if rm:
                    key = rm.group(1)
                    if key in settings:
                        f.write("#BARMAN# %s" % line)
                        # TODO is it useful to handle none values?
                        f.write("%s = %s\n" % (key, settings[key]))
                        mangled = True
                        continue
                f.write(line)

        return mangled

    def pg_config_detect_possible_issues(self, filename):
        '''This method looks for any possible issue with PostgreSQL
        location options such as data_directory, config_file, etc.
        It returns a dictionary with the dangerous options that have been found.

        :param filename: the Postgres configuration file
        '''

        file_options = ['data_directory', 'config_file', 'hba_file',
                'ident_file', 'external_pid_file']
        clashes = {}

        with open(filename) as f:
            content = f.readlines()

        r = re.compile('^\s*([^\s=]+)\s*=\s*(.*)$')
        for line in content:
            rm = r.match(line)
            if rm:
                key = rm.group(1)
                if key in file_options:
                    clashes[key] = rm.group(2)

        return clashes
