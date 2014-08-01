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
This module represents a backup.
"""

from glob import glob
import datetime
from io import StringIO
import logging
import os
import shutil
import time
import tempfile
import re

import dateutil.parser
import dateutil.tz

from barman.infofile import WalFileInfo, BackupInfo, UnknownBackupIdException
from barman.fs import UnixLocalCommand, UnixRemoteCommand, FsOperationFailed
from barman import xlog, output
from barman.command_wrappers import Rsync, RsyncPgData, CommandFailedException
from barman.compression import CompressionManager, CompressionIncompatibility
from barman.hooks import HookScriptRunner
from barman.utils import human_readable_timedelta, mkpath
from barman.config import BackupOptions


_logger = logging.getLogger(__name__)


class DataTransferFailure(Exception):
    """
    Used to pass rsync failure details
    """


class BackupManager(object):
    '''Manager of the backup archive for a server'''

    DEFAULT_STATUS_FILTER = (BackupInfo.DONE,)
    DANGEROUS_OPTIONS = ['data_directory', 'config_file', 'hba_file',
            'ident_file', 'external_pid_file', 'ssl_cert_file',
            'ssl_key_file', 'ssl_ca_file', 'ssl_crl_file',
            'unix_socket_directory']
    def __init__(self, server):
        """
        Constructor
        """
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
            status_filter = tuple(status_filter,)
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
            raise UnknownBackupIdException('Could not find backup_id %s' % backup_id)

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
            raise UnknownBackupIdException('Could not find backup_id %s' % backup_id)

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
        """
        Delete a backup

        :param backup: the backup to delete
        """
        available_backups = self.get_available_backups()
        # Honour minimum required redundancy
        if backup.status == BackupInfo.DONE and self.server.config.minimum_redundancy >= len(available_backups):
            yield "Skipping delete of backup %s for server %s due to minimum redundancy requirements (%s)" % (
                backup.backup_id, self.config.name, self.server.config.minimum_redundancy)
            _logger.warning("Could not delete backup %s for server %s - minimum redundancy = %s, current size = %s"
                % (backup.backup_id, self.config.name, self.server.config.minimum_redundancy, len(available_backups)))
            return

        yield "Deleting backup %s for server %s" % (backup.backup_id, self.config.name)
        previous_backup = self.get_previous_backup(backup.backup_id)
        next_backup = self.get_next_backup(backup.backup_id)
        # remove the backup
        self.delete_basebackup(backup)
        # We are deleting the first available backup
        if not previous_backup:
            # In the case of exclusive backup (default), removes any WAL
            # files associated to the backup being deleted.
            # In the case of concurrent backup, removes only WAL files
            # prior to the start of the backup being deleted, as they
            # might be useful to any concurrent backup started immediately
            # after.
            remove_until = None # means to remove all WAL files
            if next_backup:
                remove_until = next_backup
            elif BackupOptions.CONCURRENT_BACKUP in self.config.backup_options:
                remove_until = backup
            yield "Delete associated WAL segments:"
            for name in self._remove_unused_wal_files(remove_until):
                yield "\t%s" % name
        yield "Done"

    def retry_backup_copy(self, target_function, *args, **kwargs):
        """
        Execute the copy of a base backup, retrying a given number of times

        :param target_function: the base backup copy function
        :param args: args for the copy function
        :param kwargs: kwargs of the copy function
        :return: the result of the copy function
        """
        attempts = 0
        while True:
            try:
                # if is not the first attempt, output the retry number
                if attempts >= 1:
                    output.warning("Copy of base backup: retry #%s", attempts)
                return target_function(*args, **kwargs)
            # catch rsync errors
            except DataTransferFailure, e:
                # exit condition: if retry number is lower than configured retry
                # limit, try again; otherwise exit.
                if attempts < self.config.basebackup_retry_times:
                    # Log the exception, for debugging purpose
                    _logger.exception("Failure in base backup copy: %s", e)
                    output.warning(
                        "Copy of base backup failed, waiting for next "
                        "attempt in %s seconds",
                        self.config.basebackup_retry_sleep)
                    # sleep for configured time. then try again
                    time.sleep(self.config.basebackup_retry_sleep)
                    attempts += 1
                else:
                    # if the max number of attempts is reached an there is still
                    # an error, exit re-raising the exception.
                    raise

    def backup(self):
        """
        Performs a backup for the server
        """
        _logger.debug("initialising backup information")
        self.current_action = "starting backup"
        backup_info = None
        try:
            backup_info = BackupInfo(
                self.server,
                backup_id=datetime.datetime.now().strftime('%Y%m%dT%H%M%S'))
            backup_info.save()
            output.info(
                "Starting backup for server %s in %s",
                self.config.name,
                backup_info.get_basebackup_directory())

            # Run the pre-backup-script if present.
            script = HookScriptRunner(self, 'backup_script', 'pre')
            script.env_from_backup_info(backup_info)
            script.run()

            # Start the backup, all the subsequent code must be wrapped in a
            # try except block which finally issue a backup_stop command
            self.backup_start(backup_info)
            try:
                # save any metadata changed by backup_start() call
                # This must be inside the try-except, because it could fail
                backup_info.save()

                # If we are the first backup, purge unused WAL files
                previous_backup = self.get_previous_backup(backup_info.backup_id)
                if not previous_backup:
                    self._remove_unused_wal_files(backup_info)

                output.info("Backup start at xlog location: %s (%s, %08X)",
                            backup_info.begin_xlog,
                            backup_info.begin_wal,
                            backup_info.begin_offset)

                # Start the copy
                self.current_action = "copying files"
                output.info("Copying files.")
                # perform the backup copy, honouring the retry option if set
                backup_size = self.retry_backup_copy(self.backup_copy,
                                                     backup_info)
                backup_info.set_attribute("size", backup_size)
                output.info("Copy done.")
            except:
                # we do not need to do anything here besides re-throwin the
                # exception. It will be handled in the external try block.
                raise
            else:
                self.current_action = "issuing stop of the backup"
                output.info("Asking PostgreSQL server to finalize the backup.")
            finally:
                self.backup_stop(backup_info)

            if BackupOptions.CONCURRENT_BACKUP in self.config.backup_options:
                self.current_action = "writing backup label"
                self._write_backup_label(backup_info)
            backup_info.set_attribute("status", "DONE")
        # Use BaseException instead of Exception to catch events like
        # KeyboardInterrupt (e.g.: CRTL-C)
        except BaseException, e:
            msg_lines = str(e).strip().splitlines()
            if backup_info:
                # Use only the first line of exception message
                # in backup_info error field
                backup_info.set_attribute("status", "FAILED")
                # If the exception has no attached message use the raw type name
                if len(msg_lines) == 0:
                    msg_lines = [type(e).__name__]
                backup_info.set_attribute(
                    "error",
                    "failure %s (%s)" % (
                        self.current_action, msg_lines[0]))

            output.exception("Backup failed %s: %s\n%s",
                         self.current_action, msg_lines[0],
                         '\n'.join(msg_lines[1:]))

        else:
            output.info("Backup end at xlog location: %s (%s, %08X)",
                            backup_info.end_xlog,
                            backup_info.end_wal,
                            backup_info.end_offset)
            output.info("Backup completed")
        finally:
            if backup_info:
                backup_info.save()

                # Run the post-backup-script if present.
                script = HookScriptRunner(self, 'backup_script', 'post')
                script.env_from_backup_info(backup_info)
                script.run()

        output.result('backup', backup_info)

    def recover(self, backup, dest, tablespaces, target_tli, target_time,
                target_xid, target_name, exclusive, remote_command):
        """
        Performs a recovery of a backup

        :param backup: the backup to recover
        :param dest: the destination directory
        :param tablespaces: a dictionary of tablespaces (for relocation)
        :param target_tli: the target timeline
        :param target_time: the target time
        :param target_xid: the target xid
        :param target_name: the target name created previously with
                            pg_create_restore_point() function call
        :param exclusive: whether the recovery is exclusive or not
        :param remote_command: default None. The remote command to recover
                               the base backup, in case of remote backup.
        """

        # run the cron to be sure the wal catalog is up to date
        self.server.cron(verbose=False)

        recovery_dest = 'local'

        if remote_command:
            recovery_dest = 'remote'
            rsync = RsyncPgData(
                ssh=remote_command,
                bwlimit=self.config.bandwidth_limit,
                network_compression=self.config.network_compression)
            try:
                # create a UnixRemoteCommand obj if is a remote recovery
                cmd = UnixRemoteCommand(remote_command)
            except FsOperationFailed:
                output.error(
                    "Unable to connect to the target host using the command "
                    "'%s'" % remote_command
                )
                return
        else:
            # if is a local recovery create a UnixLocalCommand
            cmd = UnixLocalCommand()
            # silencing static analysis tools
            rsync = None
        msg = "Starting %s restore for server %s using backup %s " % (
            recovery_dest, self.config.name, backup.backup_id)
        yield msg
        _logger.info(msg)

        # check destination directory. If doesn't exist create it
        try:
            cmd.create_dir_if_not_exists(dest)
        except FsOperationFailed, e:
            msg = ("unable to initialize destination directory "
                   "'%s': %s" % (dest, e))
            _logger.exception(msg)
            raise SystemExit(msg)

        msg = "Destination directory: %s" % dest
        yield msg
        _logger.info(msg)

        # initialize tablespace structure
        if backup.tablespaces:
            tblspc_dir = os.path.join(dest, 'pg_tblspc')
            try:
                # check for pg_tblspc dir into recovery destination folder.
                # if does not exists, create it
                cmd.create_dir_if_not_exists(tblspc_dir)
            except FsOperationFailed, e:
                msg = ("unable to initialize tablespace directory "
                       "'%s': %s" % (tblspc_dir, e))
                _logger.exception(msg)
                raise SystemExit(msg)

            for item in backup.tablespaces:

                # build the filename of the link under pg_tblspc directory
                pg_tblspc_file = os.path.join(tblspc_dir, str(item.oid))

                # by default a tablespace goes in the same location where
                # it was on the source server when the backup was taken
                location = item.location

                # if a relocation has been requested for this tablespace
                # use the user provided target directory
                if item.name in tablespaces:
                    location = tablespaces[item.name]

                try:
                    # remove the current link in pg_tblspc if exists
                    # (raise if it's a directory)
                    cmd.delete_if_exists(pg_tblspc_file)
                    # create tablespace location if not exists
                    # (raise if not possible)
                    cmd.create_dir_if_not_exists(location)
                    # check for write permission into destination directory
                    cmd.check_write_permission(location)
                    # create symlink between tablespace and recovery folder
                    cmd.create_symbolic_link(location, pg_tblspc_file)
                except FsOperationFailed, e:
                    msg = ("unable to prepare '%s' tablespace "
                           "(destination '%s'): %s" %
                           (item.name, location, e))
                    _logger.exception(msg)
                    raise SystemExit(msg)

                yield "\t%s, %s, %s" % (item.oid, item.name, location)

        wal_dest = os.path.join(dest, 'pg_xlog')
        target_epoch = None
        target_datetime = None
        if target_time:
            try:
                target_datetime = dateutil.parser.parse(target_time)
            except ValueError as e:
                msg = "unable to parse the target time parameter %r: %s" % (
                      target_time, e)
                _logger.exception(msg)
                raise SystemExit(msg)
            except Exception:
                # this should not happen, but there is a known bug in
                # dateutil.parser.parse() implementation
                # ref: https://bugs.launchpad.net/dateutil/+bug/1247643
                msg = "unable to parse the target time parameter %r" % (
                      target_time)
                _logger.exception(msg)
                raise SystemExit(msg)
            target_epoch = time.mktime(target_datetime.timetuple()) + (
                target_datetime.microsecond / 1000000.)
        if target_time or target_xid or (
                target_tli and target_tli != backup.timeline) or target_name:
            targets = {}
            if target_time:
                targets['time'] = str(target_datetime)
            if target_xid:
                targets['xid'] = str(target_xid)
            if target_tli and target_tli != backup.timeline:
                targets['timeline'] = str(target_tli)
            if target_name:
                targets['name'] = str(target_name)
            yield "Doing PITR. Recovery target %s" % \
                (", ".join(["%s: %r" % (k, v) for k, v in targets.items()]))
            wal_dest = os.path.join(dest, 'barman_xlog')

        # Retrieve the safe_horizon for smart copy
        # If the target directory contains a previous recovery, it is safe to
        # pick the least of the two backup "begin times" (the one we are
        # recovering now and the one previously recovered in the target
        # directory)
        #
        # noinspection PyBroadException
        try:
            backup_begin_time = backup.begin_time
            # Retrieve previously recovered backup metadata (if available)
            dest_info_txt = cmd.get_file_content(
                os.path.join(dest, '.barman-recover.info'))
            dest_info = BackupInfo(
                self.server,
                info_file=StringIO(dest_info_txt))
            dest_begin_time = dest_info.begin_time
            # Pick the earlier begin time. Both are tz-aware timestamps because
            # BackupInfo class ensure it
            safe_horizon = min(backup_begin_time, dest_begin_time)
            output.info("Using safe horizon time for smart rsync copy: %s",
                        safe_horizon)
        except FsOperationFailed, e:
            # Setting safe_horizon to None will effectively disable
            # the time-based part of smart_copy method. However it is still
            # faster than running all the transfers with checksum enabled.
            #
            # FsOperationFailed means the .barman-recover.info is not available
            # on destination directory
            safe_horizon = None
            _logger.warning('Unable to retrieve safe horizon time '
                            'for smart rsync copy: %s', e)
        except Exception, e:
            # Same as above, but something failed decoding .barman-recover.info
            # or comparing times, so log the full traceback
            safe_horizon = None
            _logger.exception('Error retrieving safe horizon time '
                              'for smart rsync copy: %s', e)

        # Copy the base backup
        output.info("Copying the base backup.")
        try:
            # perform the backup copy, honoring the retry option if set
            self.retry_backup_copy(self.recover_basebackup_copy, backup, dest,
                                   tablespaces, remote_command, safe_horizon)
        except DataTransferFailure, e:
            raise SystemExit("Failure copying base backup: %s" % (e,))
        _logger.info("Base backup copied.")

        # Prepare WAL segments local directory
        msg = "Copying required wal segments."
        _logger.info(msg)
        yield msg

        # Retrieve the list of required WAL segments
        # according to recovery options
        xlogs = {}
        required_xlog_files = tuple(
            self.server.get_required_xlog_files(backup, target_tli,
                                                target_epoch))
        for filename in required_xlog_files:
            hashdir = xlog.hash_dir(filename)
            if hashdir not in xlogs:
                xlogs[hashdir] = []
            xlogs[hashdir].append(filename)
        # Check decompression options
        compressor = self.compression_manager.get_compressor()

        # Restore WAL segments
        try:
            self.recover_xlog_copy(compressor, xlogs, wal_dest, remote_command)
        except DataTransferFailure, e:
            raise SystemExit("Failure copying WAL files: %s" % (e,))

        # Generate recovery.conf file (only if needed by PITR)
        if target_time or target_xid or (
                target_tli and target_tli != backup.timeline) or target_name:
            msg = "Generating recovery.conf"
            yield msg
            _logger.info(msg)
            if remote_command:
                tempdir = tempfile.mkdtemp(prefix='barman_recovery-')
                recovery = open(os.path.join(tempdir, 'recovery.conf'), 'w')
            else:
                recovery = open(os.path.join(dest, 'recovery.conf'), 'w')
            print >> recovery, "restore_command = 'cp barman_xlog/%f %p'"
            if backup.version >= 80400:
                print >> recovery, "recovery_end_command = 'rm -fr barman_xlog'"
            if target_time:
                print >> recovery, "recovery_target_time = '%s'" % target_time
            if target_tli:
                print >> recovery, "recovery_target_timeline = %s" % target_tli
            if target_xid:
                print >> recovery, "recovery_target_xid = '%s'" % target_xid
            if target_name:
                print >> recovery, "recovery_target_name = '%s'" % target_name
            if (target_xid or target_time) and exclusive:
                print >> recovery, "recovery_target_inclusive = '%s'" % (
                    not exclusive)
            recovery.close()
            if remote_command:
                # Uses plain rsync (without exclusions) to ship recovery.conf
                plain_rsync = Rsync(
                        ssh=remote_command,
                        bwlimit=self.config.bandwidth_limit,
                        network_compression=self.config.network_compression)
                try:
                    plain_rsync.from_file_list(['recovery.conf'],
                                              tempdir, ':%s' % dest)
                except CommandFailedException, e:
                    msg = (
                        'remote copy of recovery.conf failed: %s' % (e,))
                    _logger.exception(msg)
                    raise SystemExit(msg)

                shutil.rmtree(tempdir)
            _logger.info('recovery.conf generated')
        else:
            # avoid shipping of just recovered pg_xlog files
            if remote_command:
                status_dir = tempfile.mkdtemp(prefix='barman_xlog_status-')
            else:
                status_dir = os.path.join(wal_dest, 'archive_status')
                mkpath(status_dir)
            for filename in required_xlog_files:
                with open(os.path.join(status_dir, "%s.done" % filename),
                          'a') as f:
                    f.write('')
            if remote_command:
                try:
                    rsync('%s/' % status_dir,
                          ':%s' % os.path.join(wal_dest, 'archive_status'))
                except CommandFailedException:
                    msg = "unable to populate pg_xlog/archive_status directory"
                    _logger.warning(msg, exc_info=1)
                    raise SystemExit(msg)
                shutil.rmtree(status_dir)

        # Disable dangerous setting in the target data dir
        if remote_command:
            tempdir = tempfile.mkdtemp(prefix='barman_recovery-')
            pg_config = os.path.join(tempdir, 'postgresql.conf')
            shutil.copy2(
                os.path.join(backup.get_basebackup_directory(), 'pgdata',
                             'postgresql.conf'), pg_config)
        else:
            pg_config = os.path.join(dest, 'postgresql.conf')
        if self.pg_config_mangle(pg_config,
                                 {'archive_command': 'false'},
                                 "%s.origin" % pg_config):
            msg = "The archive_command was set to 'false' to prevent data " \
                  "losses."
            yield msg
            _logger.info(msg)

        # Find dangerous options in the configuration file (locations)
        clashes = self.pg_config_detect_possible_issues(pg_config)

        if remote_command:
            try:
                rsync.from_file_list(
                    ['postgresql.conf', 'postgresql.conf.origin'], tempdir,
                    ':%s' % dest)
            except CommandFailedException, e:
                msg = 'remote copy of configuration files failed: %s' % (e,)
                _logger.error(msg)
                raise SystemExit(msg)
            shutil.rmtree(tempdir)

        # Copy the backup.info file to the destination as ".barman-recover.info"
        if remote_command:
            try:
                rsync(backup.filename, ':%s/.barman-recover.info' % dest)
            except CommandFailedException, e:
                    msg = 'copy of recovery metadata file failed: %s' % (e,)
                    _logger.error(msg)
                    raise SystemExit(msg)
        else:
            backup.save(os.path.join(dest, '.barman-recover.info'))

        yield ""
        yield "Your PostgreSQL server has been successfully prepared for " \
              "recovery!"
        yield ""
        yield "Please review network and archive related settings in the " \
              "PostgreSQL"
        yield "configuration file before starting the just recovered instance."
        yield ""
        # With a PostgreSQL version older than 8.4, it is the user's
        # responsibility to delete the "barman_xlog" directory as the
        # restore_command option in recovery.conf is not supported
        if backup.version < 80400 and (target_time or target_xid or (
                target_tli and target_tli != backup.timeline) or target_name):
            yield "After the recovery, please remember to remove the " \
                  "\"barman_xlog\" directory"
            yield "inside the PostgreSQL data directory."
            yield ""
        if clashes:
            yield "WARNING: Before starting up the recovered PostgreSQL server,"
            yield "please review also the settings of the following " \
                  "configuration"
            yield "options as they might interfere with your current " \
                  "recovery attempt:"
            yield ""

            for name, value in sorted(clashes.items()):
                yield "    %s = %s" % (name, value)

            yield ""
        _logger.info("Recovery completed successful.")

    def cron(self, verbose=True):
        """
        Executes maintenance operations, such as WAL trashing.

        If verbose is set to False, outputs something only if there is
        at least one file

        :param bool verbose: report even if no actions
        """
        found = False
        compressor = self.compression_manager.get_compressor()
        with self.server.xlogdb('a') as fxlogdb:
            if verbose:
                output.info("Processing xlog segments for %s",
                            self.config.name,
                            log=False)
            available_backups = self.get_available_backups(
                BackupInfo.STATUS_ALL)
            for filename in sorted(glob(
                    os.path.join(self.config.incoming_wals_directory, '*'))):
                if not found and not verbose:
                    output.info("Processing xlog segments for %s",
                                self.config.name,
                                log=False)
                found = True
                # Delete xlog segments only if the backup is exclusive
                if (not len(available_backups) and
                        (BackupOptions.CONCURRENT_BACKUP not in
                             self.config.backup_options)):
                    output.info("\tNo base backup available. Trashing file %s"
                                " from server %s",
                                os.path.basename(filename), self.config.name)
                    os.unlink(filename)
                    continue
                # Report to the user the WAL file we are archiving
                output.info("\t%s", os.path.basename(filename), log=False)
                _logger.info("Archiving %s/%s",
                             self.config.name,
                             os.path.basename(filename))
                # Archive the WAL file
                wal_info = self.cron_wal_archival(compressor, filename)

                # Updates the information of the WAL archive with
                # the latest segments
                fxlogdb.write(wal_info.to_xlogdb_line())
        if not found and verbose:
            output.info("\tno file found", log=False)

        # Retention policy management
        if (self.server.enforce_retention_policies
                and self.config.retention_policy_mode == 'auto'):
            available_backups = self.get_available_backups(
                BackupInfo.STATUS_ALL)
            retention_status = self.config.retention_policy.report()
            for bid in sorted(retention_status.iterkeys()):
                if retention_status[bid] == BackupInfo.OBSOLETE:
                    output.info(
                        "Enforcing retention policy: removing backup %s for "
                        "server %s" % (bid, self.config.name))
                    for line in self.delete_backup(available_backups[bid]):
                        output.info(line)

    def delete_basebackup(self, backup):
        '''
        Delete the given base backup

        :param backup: the backup to delete
        '''
        backup_dir = backup.get_basebackup_directory();
        shutil.rmtree(backup_dir)

    def delete_wal(self, wal_info):
        """
        Delete a WAL segment, with the given name

        :param name: the name of the WAL to delete
        """

        try:
            os.unlink(wal_info.full_path)
            try:
                os.removedirs(os.path.dirname(wal_info.full_path))
            except OSError:
                # This is not an error condition
                # We always try to remove the the trailing directories,
                # this means that hashdir is not empty.
                pass
        except OSError:
            _logger.warning('Expected WAL file %s not found during delete',
                            wal_info.name, exc_info=1)

    def backup_start(self, backup_info):
        """
        Start of the backup

        :param BackupInfo backup_info: the backup information
        """
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

        # Get tablespaces information
        self.current_action = "detecting tablespaces"
        _logger.debug(self.current_action)
        tablespaces = self.server.get_pg_tablespaces()
        if tablespaces and len(tablespaces) > 0:
            backup_info.set_attribute("tablespaces", tablespaces)
            for item in tablespaces:
                msg = "\t%s, %s, %s" % (item.oid, item.name, item.location)
                _logger.info(msg)

        # Issue pg_start_backup on the PostgreSQL server
        self.current_action = "issuing start backup command"
        _logger.debug(self.current_action)
        label = "Barman backup %s %s" % (
            backup_info.server_name, backup_info.backup_id)
        self.server.start_backup(label, backup_info)

    def _raise_rsync_error(self, e, msg):
        """
        This method raises an exception and report the provided message to the
        user (both console and log file) along with the output of
        the failed rsync command.

        :param CommandFailedException e: The exception we are handling
        :param msg: a descriptive message on what we are trying to do
        :raise Exception: will contain the message provided in msg
        """
        details = msg
        details += "\nrsync error:\n"
        details += e.args[0]['out']
        details += e.args[0]['err']
        raise DataTransferFailure(details)

    def backup_copy(self, backup_info):
        """
        Perform the copy of the backup.
        This function returns the size of the backup (in bytes)

        :param backup_info: the backup information structure
        """

        # paths to be ignored from rsync
        exclude_and_protect = []

        # validate the bandwidth rules against the tablespace list
        tablespaces_bwlimit = {}

        if self.config.tablespace_bandwidth_limit and backup_info.tablespaces:
            valid_tablespaces = dict([
                (tablespace_data.name, tablespace_data.oid)
                for tablespace_data in backup_info.tablespaces])
            for tablespace, bwlimit in \
                    self.config.tablespace_bandwidth_limit.items():
                if tablespace in valid_tablespaces:
                    tablespace_dir = "pg_tblspc/%s" % (
                                     valid_tablespaces[tablespace],)
                    tablespaces_bwlimit[tablespace_dir] = bwlimit
                    exclude_and_protect.append(tablespace_dir)

        backup_dest = os.path.join(
            backup_info.get_basebackup_directory(), 'pgdata')

        # find tablespaces which need to be excluded from rsync command
        if backup_info.tablespaces is not None:
            exclude_and_protect += [
                # removes tablespaces that are located within PGDATA
                # as they are already being copied along with it
                tablespace_data.location[len(backup_info.pgdata):]
                for tablespace_data in backup_info.tablespaces
                if tablespace_data.location.startswith(backup_info.pgdata)
            ]

        # deal with tablespaces with a different bwlimit
        if len(tablespaces_bwlimit) > 0:
            # we are copying the tablespaces before the data directory,
            # so we need to create the 'pg_tblspc' directory
            mkpath(os.path.join(backup_dest, 'pg_tblspc'))
            for tablespace_dir, bwlimit in tablespaces_bwlimit.items():
                self.current_action = "copying tablespace '%s' with bwlimit " \
                                      "%d" % (tablespace_dir, bwlimit)
                _logger.debug(self.current_action)
                tb_rsync = RsyncPgData(
                    ssh=self.server.ssh_command,
                    ssh_options=self.server.ssh_options,
                    bwlimit=bwlimit,
                    network_compression=self.config.network_compression,
                    check=True)
                try:
                    tb_rsync(':%s/' %
                             os.path.join(backup_info.pgdata, tablespace_dir),
                             os.path.join(backup_dest, tablespace_dir))
                except CommandFailedException, e:
                    msg = "data transfer failure on directory '%s'" % \
                          os.path.join(backup_info.pgdata, tablespace_dir)
                    self._raise_rsync_error(e, msg)

        rsync = RsyncPgData(
            ssh=self.server.ssh_command,
            ssh_options=self.server.ssh_options,
            bwlimit=self.config.bandwidth_limit,
            exclude_and_protect=exclude_and_protect,
            network_compression=self.config.network_compression)
        try:
            rsync(':%s/' % backup_info.pgdata, backup_dest)
        except CommandFailedException, e:
            msg = "data transfer failure on directory '%s'" % \
                  backup_info.pgdata
            self._raise_rsync_error(e, msg)

        # at last copy pg_control
        try:
            rsync(':%s/global/pg_control' % (backup_info.pgdata,),
                  '%s/global/pg_control' % (backup_dest,))
        except CommandFailedException, e:
            msg = "data transfer failure on file '%s/global/pg_control'" % \
                  backup_info.pgdata
            self._raise_rsync_error(e, msg)

        # Copy configuration files (if not inside PGDATA)
        self.current_action = "copying configuration files"
        _logger.debug(self.current_action)
        cf = self.server.get_pg_configuration_files()
        if cf:
            for key in sorted(cf.keys()):
                # Consider only those that reside outside of the original PGDATA
                if cf[key]:
                    if cf[key].find(backup_info.pgdata) == 0:
                        self.current_action = \
                            "skipping %s as contained in %s directory" % (
                                key, backup_info.pgdata)
                        _logger.debug(self.current_action)
                        continue
                    else:
                        self.current_action = \
                            "copying %s as outside %s directory" % (
                                key, backup_info.pgdata)
                        _logger.info(self.current_action)
                        try:
                            rsync(':%s' % cf[key], backup_dest)

                        except CommandFailedException, e:
                            msg = "data transfer failure on file '%s'" % \
                                  cf[key]
                            self._raise_rsync_error(e, msg)

        # Calculate the base backup size
        self.current_action = "calculating backup size"
        _logger.debug(self.current_action)
        backup_size = 0
        for dirpath, _, filenames in os.walk(backup_dest):
            # execute fsync() on the containing directory
            dir_fd = os.open(dirpath, os.O_DIRECTORY)
            os.fsync(dir_fd)
            os.close(dir_fd)
            # execute fsync() on all the contained files
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                file_fd = os.open(file_path, os.O_RDONLY)
                backup_size += os.fstat(file_fd).st_size
                os.fsync(file_fd)
                os.close(file_fd)
        return backup_size

    def backup_stop(self, backup_info):
        '''
        Stop the backup

        :param backup_info: the backup information structure
        '''
        self.server.stop_backup(backup_info)

    def recover_basebackup_copy(self, backup, dest, tablespaces,
                                remote_command=None, safe_horizon=None):
        """
        Perform the actual copy of the base backup for recovery purposes

        :param backup: the backup to recover
        :param dest: the destination directory
        :param tablespaces: tablespace relocation options
        :param remote_command: default None. The remote command to recover
                               the base backup, in case of remote backup.
        :param datetime.datetime safe_horizon: anything after this time
            has to be checked with checksums
        """

        sourcedir = os.path.join(backup.get_basebackup_directory(), 'pgdata')

        # Dictionary for paths to be excluded from rsync
        exclude_and_protect = []
        # Dictionary for tablespace bandwith limit settings
        tablespaces_bwlimit = {}

        # find tablespaces which need to be excluded from rsync command
        if backup.tablespaces is not None:
            # Look for tablespaces that will be recovered
            # inside the destination PGDATA directory (including
            # relocated ones) and add them to the excluded files list
            for item in backup.tablespaces:
                # by default a tablespace goes in the same location where
                # it was on the source server when the backup was taken
                location = item.location

                # if a relocation has been requested for this tablespace
                # use the user provided target directory
                if item.name in tablespaces:
                    location = tablespaces[item.name]

                if location.startswith(dest):
                    exclude_and_protect.append(location[len(dest):])

        if remote_command:
            dest = ':%s' % dest

            # validate the bandwidth rules against the tablespace list
            if self.config.tablespace_bandwidth_limit and backup.tablespaces:
                # create a map containing the tablespace name as key and the
                # tablespace oid as value
                valid_tablespaces = dict([(tablespace_data.name, tablespace_data.oid)
                                          for tablespace_data in backup.tablespaces])
                for item, bwlimit in self.config.tablespace_bandwidth_limit.items():
                    if item in valid_tablespaces:
                        tablespace_dir = "pg_tblspc/%s" % (valid_tablespaces[item],)
                        tablespaces_bwlimit[tablespace_dir] = bwlimit
                        exclude_and_protect.append(tablespace_dir)

        rsync = RsyncPgData(
            ssh=remote_command,
            bwlimit=self.config.bandwidth_limit,
            exclude_and_protect=exclude_and_protect,
            network_compression=self.config.network_compression)
        try:
            rsync.smart_copy('%s/' % (sourcedir,), dest, safe_horizon)
        except CommandFailedException, e:
            msg = "data transfer failure on directory '%s'" % (dest[1:],)
            self._raise_rsync_error(e, msg)

        if remote_command and len(tablespaces_bwlimit) > 0:
            for tablespace_dir, bwlimit in tablespaces_bwlimit.items():
                _logger.debug(self.current_action)
                tb_rsync = RsyncPgData(
                    ssh=remote_command,
                    bwlimit=bwlimit,
                    network_compression=self.config.network_compression)
                try:
                    tb_rsync.smart_copy(
                        '%s/' % os.path.join(sourcedir, tablespace_dir),
                        os.path.join(dest, tablespace_dir),
                        safe_horizon)
                except CommandFailedException, e:
                    msg = "data transfer failure on directory '%s'" % (
                        tablespace_dir[1:],)
                    self._raise_rsync_error(e, msg)

        # TODO: Manage different location for configuration files
        # TODO: that were not within the data directory

    def recover_xlog_copy(self, compressor, xlogs, wal_dest,
                          remote_command=None):
        """
        Restore WAL segments

        :param compressor: the compressor for the file (if any)
        :param xlogs: the xlog dictionary to recover
        :param wal_dest: the destination directory for xlog recover
        :param remote_command: default None. The remote command to recover
               the xlog, in case of remote backup.
        """
        rsync = RsyncPgData(
            ssh=remote_command,
            bwlimit=self.config.bandwidth_limit,
            network_compression=self.config.network_compression)
        if remote_command:
            # If remote recovery tell rsync to copy them remotely
            # add ':' prefix to mark it as remote
            # add '/' suffix to ensure it is a directory
            wal_dest = ':%s/' % wal_dest
        else:
            # we will not use rsync: destdir must exists
            mkpath(wal_dest)
        if compressor and remote_command:
            xlog_spool = tempfile.mkdtemp(prefix='barman_xlog-')
        total_wals = sum(map(len, xlogs.values()))
        partial_count = 0
        for prefix in sorted(xlogs):
            batch_len = len(xlogs[prefix])
            partial_count += batch_len
            source_dir = os.path.join(self.config.wals_directory, prefix)
            _logger.info(
                "Starting copy of %s WAL files %s/%s from %s to %s",
                batch_len,
                partial_count,
                total_wals,
                xlogs[prefix][0],
                xlogs[prefix][-1])
            if compressor:
                if remote_command:
                    for segment in xlogs[prefix]:
                        compressor.decompress(os.path.join(source_dir, segment),
                                              os.path.join(xlog_spool, segment))
                    try:
                        rsync.from_file_list(xlogs[prefix],
                                             xlog_spool, wal_dest)
                    except CommandFailedException, e:
                        msg = "data transfer failure while copying WAL files " \
                              "to directory '%s'" % (wal_dest[1:],)
                        self._raise_rsync_error(e, msg)

                    # Cleanup files after the transfer
                    for segment in xlogs[prefix]:
                        os.unlink(os.path.join(xlog_spool, segment))
                else:
                    # decompress directly to the right place
                    for segment in xlogs[prefix]:
                        compressor.decompress(os.path.join(source_dir, segment),
                                              os.path.join(wal_dest, segment))
            else:
                try:
                    rsync.from_file_list(
                        xlogs[prefix],
                        "%s/" % os.path.join(
                            self.config.wals_directory, prefix),
                        wal_dest)
                except CommandFailedException, e:
                    msg = "data transfer failure while copying WAL files " \
                          "to directory '%s'" % (wal_dest[1:],)
                    self._raise_rsync_error(e, msg)

        _logger.info("Finished copying %s WAL files.", total_wals)

        if compressor and remote_command:
            shutil.rmtree(xlog_spool)

    def cron_wal_archival(self, compressor, filename):
        """
        Archive a WAL segment from the incoming directory.
        This function returns a WalFileInfo object.

        :param compressor: the compressor for the file (if any)
        :param filename: the name of the WAL file is being processed
        :return WalFileInfo:
        """
        basename = os.path.basename(filename)
        destdir = os.path.join(self.config.wals_directory, xlog.hash_dir(basename))
        destfile = os.path.join(destdir, basename)

        wal_info = WalFileInfo.from_file(filename, compression=None)

        # Run the pre_archive_script if present.
        script = HookScriptRunner(self, 'archive_script', 'pre')
        script.env_from_wal_info(wal_info)
        script.run()

        mkpath(destdir)
        if compressor:
            compressor.compress(filename, destfile)
            shutil.copystat(filename, destfile)
            os.unlink(filename)
        else:
            shutil.move(filename, destfile)

        # execute fsync() on the archived WAL containing directory
        dir_fd = os.open(os.path.dirname(destfile), os.O_DIRECTORY)
        os.fsync(dir_fd)
        os.close(dir_fd)
        # execute fsync() on the archived WAL file
        file_fd = os.open(destfile, os.O_RDONLY)
        os.fsync(file_fd)
        os.close(file_fd)

        wal_info = WalFileInfo.from_file(
            destfile,
            compression=compressor and compressor.compression)

        # Run the post_archive_script if present.
        script = HookScriptRunner(self, 'archive_script', 'post')
        script.env_from_wal_info(wal_info)
        script.run()

        return wal_info

    def check(self):
        """
        This function performs some checks on the server.
        Returns 0 if all went well, 1 if any of the checks fails
        """
        if self.config.compression and not self.compression_manager.check():
            output.result('check', self.config.name,
                          'compression settings', False)
        else:
            status = True
            try:
                self.compression_manager.get_compressor()
            except CompressionIncompatibility, field:
                output.result('check', self.config.name,
                              '%s setting' % field, False)
                status = False
            output.result('check', self.config.name,
                          'compression settings', status)

        # Minimum redundancy checks
        no_backups = len(self.get_available_backups())
        if no_backups < self.config.minimum_redundancy:
            status = False
        else:
            status = True
        output.result('check', self.config.name,
                      'minimum redundancy requirements', status,
                      'have %s backups, expected at least %s' %
                      (no_backups, self.config.minimum_redundancy))

    def status(self):
        """
        This function show the server status
        """
        #get number of backups
        no_backups = len(self.get_available_backups())
        output.result('status', self.config.name,
                      "backups_number",
                      "No. of available backups", no_backups)
        output.result('status', self.config.name,
                      "first_backup",
                      "First available backup",
                      self.get_first_backup())
        output.result('status', self.config.name,
                      "last_backup",
                      "Last available backup",
                      self.get_last_backup())
        # Minimum redundancy check. if number of backups minor than minimum
        # redundancy, fail.
        if no_backups < self.config.minimum_redundancy:
            output.result('status', self.config.name,
                          "minimum_redundancy",
                          "Minimum redundancy requirements",
                          "FAILED (%s/%s)" % (
                              no_backups,
                              self.config.minimum_redundancy))
        else:
            output.result('status', self.config.name,
                          "minimum_redundancy",
                          "Minimum redundancy requirements",
                          "satisfied (%s/%s)" % (
                              no_backups,
                              self.config.minimum_redundancy))

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

        clashes = {}

        with open(filename) as f:
            content = f.readlines()

        r = re.compile('^\s*([^\s=]+)\s*=\s*(.*)$')
        for line in content:
            rm = r.match(line)
            if rm:
                key = rm.group(1)
                if key in self.DANGEROUS_OPTIONS:
                    clashes[key] = rm.group(2)

        return clashes

    def rebuild_xlogdb(self):
        """
        Rebuild the whole xlog database guessing it from the archive content.
        """
        from os.path import isdir, join

        yield "Rebuilding xlogdb for server %s" % self.config.name
        root = self.config.wals_directory
        default_compression = self.config.compression
        wal_count = label_count = history_count = 0
        # lock the xlogdb as we are about replacing it completely
        with self.server.xlogdb('w') as fxlogdb:
            for name in sorted(os.listdir(root)):
                # ignore the xlogdb and its lockfile
                if name.startswith(self.server.XLOG_DB):
                    continue
                fullname = join(root, name)
                if isdir(fullname):
                    # all relevant files are in subdirectories
                    hash_dir = fullname
                    for wal_name in sorted(os.listdir(hash_dir)):
                        fullname = join(hash_dir, wal_name)
                        if isdir(fullname):
                            _logger.warning(
                                'unexpected directory '
                                'rebuilding the wal database: %s',
                                fullname)
                        else:
                            if xlog.is_wal_file(fullname):
                                wal_count += 1
                            elif xlog.is_backup_file(fullname):
                                label_count += 1
                            else:
                                _logger.warning(
                                    'unexpected file '
                                    'rebuilding the wal database: %s',
                                    fullname)
                                continue
                            wal_info = WalFileInfo.from_file(
                                fullname,
                                default_compression=default_compression)
                            fxlogdb.write(wal_info.to_xlogdb_line())
                else:
                    # only history files are here
                    if xlog.is_history_file(fullname):
                        history_count += 1
                        wal_info = WalFileInfo.from_file(
                            fullname,
                            default_compression=default_compression)
                        fxlogdb.write(wal_info.to_xlogdb_line())
                    else:
                        _logger.warning(
                            'unexpected file '
                            'rebuilding the wal database: %s',
                            fullname)

        yield 'Done rebuilding xlogdb for server %s ' \
            '(history: %s, backup_labels: %s, wal_file: %s)' % (
                self.config.name, history_count, label_count, wal_count)

    def _write_backup_label(self, backup_info):
        """
        Write backup_label file inside pgdata folder

        :param backup_info: the backup information structure
        """
        label_file = os.path.join(backup_info.get_basebackup_directory(),
                                  'pgdata/backup_label')

        with open(label_file, 'w') as f:
            f.write(backup_info.backup_label)

    def _remove_unused_wal_files(self, backup_info):
        """
        Remove WAL files which have been archived before the start of
        the provided backup.

        If no backup_info is provided delete all available WAL files

        :param backup_info: the backup information structure
        :return list: a list of removed WAL files
        """
        removed = []
        with self.server.xlogdb() as fxlogdb:
            xlogdb_new = fxlogdb.name + ".new"
            with open(xlogdb_new, 'w') as fxlogdb_new:
                for line in fxlogdb:
                    wal_info = WalFileInfo.from_xlogdb_line(self.server, line)
                    if backup_info and wal_info.name >= backup_info.begin_wal:
                        fxlogdb_new.write(wal_info.to_xlogdb_line())
                        continue
                    else:
                        # Delete the WAL segment
                        self.delete_wal(wal_info)
                        removed.append(wal_info.name)
            shutil.move(xlogdb_new, fxlogdb.name)
        return removed

    def validate_last_backup_maximum_age(self, last_backup_maximum_age):
        """
        Evaluate the age of the last available backup in a catalogue.
        If the last backup is older than the specified time interval (age),
        the function returns False. If within the requested age interval,
        the function returns True.

        :param timedate.timedelta last_backup_maximum_age: time interval
            representing the maximum allowed age for the last backup in a server
            catalogue
        :return tuple: a tuple containing the boolean result of the check and
            auxiliary information about the last backup current age
        """
        # Get the ID of the last available backup
        backup_id = self.get_last_backup()
        if backup_id:
            # Get the backup object
            backup = BackupInfo(self.server, backup_id=backup_id)
            now = datetime.datetime.now(dateutil.tz.tzlocal())
            # Evaluate the point of validity
            validity_time = now - last_backup_maximum_age
            # Pretty print of a time interval (age)
            msg = human_readable_timedelta(now - backup.end_time)
            # If the backup end time is older than the point of validity,
            # return False, otherwise return true
            if backup.end_time < validity_time:
                return False, msg
            else:
                return True, msg
        else:
            # If no backup is available return false
            return False, "No available backups"
