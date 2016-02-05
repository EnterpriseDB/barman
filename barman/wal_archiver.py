# Copyright (C) 2011-2016 2ndQuadrant Italia Srl
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
# along with Barman.  If not, see <http://www.gnu.org/licenses/>

import collections
import datetime
import filecmp
import logging
import os
import shutil
from abc import ABCMeta, abstractmethod
from distutils.version import LooseVersion as Version
from glob import glob

from barman import output, utils, xlog
from barman.backup import DuplicateWalFile, MatchingDuplicateWalFile
from barman.command_wrappers import (Command, CommandFailedException,
                                     PgReceiveXlog)
from barman.config import BackupOptions
from barman.hooks import (AbortedRetryHookScript, HookScriptRunner,
                          RetryHookScriptRunner)
from barman.infofile import WalFileInfo
from barman.remote_status import RemoteStatusMixin
from barman.utils import fsync_dir, mkpath

_logger = logging.getLogger(__name__)


class WalArchiverBatch(list):
    def __init__(self, items, errors=None, skip=None):
        """
        A WalArchiverBatch is a list of WalFileInfo which has two extra
        attribute list:

        * errors: containing a list of unrecognized files
        * skip: containing a list of skipped files.

        :param items: iterable from which initialize the list
        :param errors: an optional list of unrecognized files
        :param skip: an optional list of skipped files
        """
        super(WalArchiverBatch, self).__init__(items)
        self.skip = []
        self.errors = []
        if skip is not None:
            self.skip = skip
        if errors is not None:
            self.errors = errors


class ArchiverFailure(Exception):
    """
    Exception representing a failure during the execution of the archive process
    """


class WalArchiver(RemoteStatusMixin):
    """
    Base class for WAL archiver objects
    """

    __metaclass__ = ABCMeta

    def __init__(self, backup_manager, name):
        """
        Base class init method.

        :param backup_manager: The backup manager
        :param name: The name of this archiver
        :return:
        """
        self.backup_manager = backup_manager
        self.server = backup_manager.server
        self.config = backup_manager.config
        self.name = name
        super(WalArchiver, self).__init__()

    def receive_wal(self):
        """
        Manage reception of WAL files. Does nothing by default.
        Some archiver classes, like the StreamingWalArchiver, have a full
        implementation.
        """

    def archive(self, first_backup, fxlogdb, verbose=True):
        """
        Archive WAL files, discarding duplicates or those that are not valid.

        :param BackupInfo first_backup: BackupInfo of the oldest backup for the
            current server
        :param file fxlogdb: File object for xlogdb interactions
        :param boolean verbose: Flag for verbose output
        """
        compressor = self.backup_manager.compression_manager.get_compressor()
        stamp = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        found = False
        if verbose:
            output.info("Processing xlog segments from %s for %s",
                        self.name,
                        self.config.name,
                        log=False)
        batch = self.get_next_batch()
        for wal_info in batch:
            if not found and not verbose:
                output.info("Processing xlog segments from %s for %s",
                            self.name,
                            self.config.name,
                            log=False)
            found = True

            # Delete the xlog segment if no backup is present and
            # backup strategy is not concurrent and
            # the wal file is not a history file
            if (first_backup is None and
                    BackupOptions.CONCURRENT_BACKUP not in
                    self.config.backup_options and
                    not xlog.is_history_file(wal_info.name)):
                output.info("\tNo base backup available. "
                            "Trashing file %s from server %s",
                            wal_info.name, self.config.name)
                os.unlink(wal_info.orig_filename)
                continue
            # ... otherwise move the wal file in the error directory
            # if not relevant according to the first backup present
            elif not self.is_wal_relevant(wal_info, first_backup):
                error_dst = os.path.join(
                    self.config.errors_directory,
                    "%s.%s.error" % (wal_info.name, stamp))
                shutil.move(wal_info.orig_filename, error_dst)
                continue

            # Report to the user the WAL file we are archiving
            output.info("\t%s", wal_info.name, log=False)
            _logger.info("Archiving %s/%s", self.config.name, wal_info.name)
            # Archive the WAL file
            try:
                self.archive_wal(compressor, wal_info)
            except MatchingDuplicateWalFile:
                # We already have this file. Simply unlink the file.
                os.unlink(wal_info.orig_filename)
                continue
            except DuplicateWalFile:
                output.info("\tError: %s is already present in server %s. "
                            "File moved to errors directory.",
                            wal_info.name,
                            self.config.name)
                error_dst = os.path.join(
                    self.config.errors_directory,
                    "%s.%s.duplicate" % (wal_info.name,
                                         stamp))
                # TODO: cover corner case of duplication (unlikely,
                # but theoretically possible)
                shutil.move(wal_info.orig_filename, error_dst)
                continue
            except AbortedRetryHookScript as e:
                _logger.warning("Archiving of %s/%s aborted by "
                                "pre_archive_retry_script."
                                "Reason: %s" % (self.config.name,
                                                wal_info.name,
                                                e))
                return
            # Updates the information of the WAL archive with
            # the latest segments
            fxlogdb.write(wal_info.to_xlogdb_line())
            # flush and fsync for every line
            fxlogdb.flush()
            os.fsync(fxlogdb.fileno())
        if not found and verbose:
            output.info("\tno file found", log=False)
        if batch.errors:
            output.info("Some unknown objects have been found while "
                        "processing xlog segments for %s. "
                        "Objects moved to errors directory:",
                        self.config.name,
                        log=False)
            for error in batch.errors:
                output.info("\t%s", error)
                error_dst = os.path.join(
                    self.config.errors_directory,
                    "%s.%s.unknown" % (os.path.basename(error), stamp))
                shutil.move(error, error_dst)

    def archive_wal(self, compressor, wal_info):
        """
        Archive a WAL segment and return the updated WalFileInfo object

        :param compressor: the compressor for the file (if any)
        :param wal_info: WalFileInfo of the WAL file is being processed
        """

        src_file = wal_info.orig_filename
        src_dir = os.path.dirname(src_file)
        dst_file = wal_info.fullpath(self.server)
        tmp_file = dst_file + '.tmp'
        dst_dir = os.path.dirname(dst_file)

        error = None
        try:
            # Run the pre_archive_script if present.
            script = HookScriptRunner(self.backup_manager,
                                      'archive_script', 'pre')
            script.env_from_wal_info(wal_info, src_file)
            script.run()

            # Run the pre_archive_retry_script if present.
            retry_script = RetryHookScriptRunner(self.backup_manager,
                                                 'archive_retry_script',
                                                 'pre')
            retry_script.env_from_wal_info(wal_info, src_file)
            retry_script.run()

            # Check if destination already exists
            if os.path.exists(dst_file):
                src_uncompressed = src_file
                dst_uncompressed = dst_file
                dst_info = WalFileInfo.from_file(dst_file)
                try:
                    comp_manager = self.backup_manager.compression_manager
                    if dst_info.compression is not None:
                        dst_uncompressed = dst_file + '.uncompressed'
                        comp_manager.get_compressor(
                            dst_info.compression).decompress(
                                dst_file, dst_uncompressed)
                    if wal_info.compression:
                        src_uncompressed = src_file + '.uncompressed'
                        comp_manager.get_compressor(
                            wal_info.compression).decompress(
                                src_file, src_uncompressed)
                    # Directly compare files.
                    # When the files are identical
                    # raise a MatchingDuplicateWalFile exception,
                    # otherwise raise a DuplicateWalFile exception.
                    if filecmp.cmp(dst_uncompressed, src_uncompressed):
                        raise MatchingDuplicateWalFile(wal_info)
                    else:
                        raise DuplicateWalFile(wal_info)
                finally:
                    if src_uncompressed != src_file:
                        os.unlink(src_uncompressed)
                    if dst_uncompressed != dst_file:
                        os.unlink(dst_uncompressed)

            mkpath(dst_dir)
            # Compress the file only if not already compressed
            if compressor and not wal_info.compression:
                compressor.compress(src_file, tmp_file)
                shutil.copystat(src_file, tmp_file)
                os.rename(tmp_file, dst_file)
                os.unlink(src_file)
                # Update wal_info
                stat = os.stat(dst_file)
                wal_info.size = stat.st_size
                wal_info.compression = compressor.compression
            else:
                # Try to atomically rename the file. If successful,
                # the renaming will be an atomic operation
                # (this is a POSIX requirement).
                try:
                    os.rename(src_file, dst_file)
                except OSError:
                    # Source and destination are probably on different
                    # filesystems
                    shutil.copy2(src_file, tmp_file)
                    os.rename(tmp_file, dst_file)
                    os.unlink(src_file)
            # At this point the original file has been removed
            wal_info.orig_filename = None

            # Execute fsync() on the archived WAL file
            file_fd = os.open(dst_file, os.O_RDONLY)
            os.fsync(file_fd)
            os.close(file_fd)
            # Execute fsync() on the archived WAL containing directory
            fsync_dir(dst_dir)
            # Execute fsync() also on the incoming directory
            fsync_dir(src_dir)
        except Exception as e:
            # In case of failure save the exception for the post scripts
            error = e
            raise

        # Ensure the execution of the post_archive_retry_script and
        # the post_archive_script
        finally:
            # Run the post_archive_retry_script if present.
            try:
                retry_script = RetryHookScriptRunner(self,
                                                     'archive_retry_script',
                                                     'post')
                retry_script.env_from_wal_info(wal_info, dst_file, error)
                retry_script.run()
            except AbortedRetryHookScript, e:
                # Ignore the ABORT_STOP as it is a post-hook operation
                _logger.warning("Ignoring stop request after receiving "
                                "abort (exit code %d) from post-archive "
                                "retry hook script: %s",
                                e.hook.exit_status, e.hook.script)

            # Run the post_archive_script if present.
            script = HookScriptRunner(self, 'archive_script', 'post', error)
            script.env_from_wal_info(wal_info, dst_file)
            script.run()

    def is_wal_relevant(self, wal_info, first_backup):
        """
        Check the relevance of a WAL file according to a provided BackupInfo
        (usually the oldest on the server) to ensure that the WAL is newer than
        the start_wal of the backup.

        :param WalFileInfo wal_info: the WAL file we are checking
        :param BackupInfo first_backup: the backup used for the checks
            (usually the oldest available on the server)
        """

        # Skip history files
        if xlog.is_history_file(wal_info.name):
            return True

        # If the WAL file has a timeline smaller than the one of
        # the oldest backup it cannot be used in any way.
        wal_timeline = xlog.decode_segment_name(wal_info.name)[0]
        if wal_timeline < first_backup.timeline:
            output.info("\tThe timeline of the WAL file %s (%s), is lower "
                        "than the one of the oldest backup of "
                        "server %s (%s). Moving the WAL in "
                        "the error directory",
                        wal_info.name, wal_timeline, self.config.name,
                        first_backup.timeline)
            return False
        # Manage xlog segments older than the first backup
        if wal_info.name < first_backup.begin_wal:
            output.info("\tOlder than first backup of server %s. "
                        "Moving the WAL file %s in the error directory",
                        self.config.name, wal_info.name)
            return False
        return True

    @abstractmethod
    def get_next_batch(self):
        """
        Return a WalArchiverBatch containing the WAL files to be archived.

        :rtype: WalArchiverBatch
        """

    @abstractmethod
    def check(self, check_strategy):
        """
        Perform specific checks for the archiver - invoked
        by server.check_postgres

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """

    @staticmethod
    def summarise_error_files(error_files):
        """
        Summarise a error files list

        :param list[str] error_files: Error files list to summarise
        :return str: A summary, None if there are no error files
        """

        if not error_files:
            return None

        # The default value for this dictionary will be 0
        counters = collections.defaultdict(int)

        # Count the file types
        for name in error_files:
            if name.endswith(".error"):
                counters['not relevant'] += 1
            elif name.endswith(".duplicate"):
                counters['duplicates'] += 1
            elif name.endswith(".unknown"):
                counters['unknown'] += 1
            else:
                counters['unknown failure'] += 1

        # Return a summary list of the form: "item a: 2, item b: 5"
        return ', '.join("%s: %s" % entry for entry in counters.items())


class FileWalArchiver(WalArchiver):
    """
    Manager of file-based WAL archiving operations (aka 'log shipping').
    """

    def __init__(self, backup_manager):

        super(FileWalArchiver, self).__init__(backup_manager, 'file archival')

    def fetch_remote_status(self):
        """
        Returns the status of the FileWalArchiver.

        This method does not raise any exception in case of errors,
        but set the missing values to None in the resulting dictionary.

        :rtype: dict[str, None|str]
        """
        result = dict.fromkeys(
            ['archive_mode', 'archive_command'], None)
        postgres = self.backup_manager.server.postgres
        # If Postgres is not available we cannot detect anything
        if not postgres:
            return result
        # Query the database for 'archive_mode' and 'archive_command'
        result['archive_mode'] = postgres.get_setting('archive_mode')
        result['archive_command'] = postgres.get_setting('archive_command')

        # Add pg_stat_archiver statistics if the view is supported
        pg_stat_archiver = postgres.get_archiver_stats()
        if pg_stat_archiver is not None:
            result.update(pg_stat_archiver)

        return result

    def get_next_batch(self):
        """
        Returns the next batch of WAL files that have been archived through
        a PostgreSQL's 'archive_command' (in the 'incoming' directory)

        :return: WalArchiverBatch: list of WAL files
        """
        # List and sort all files in the incoming directory
        file_names = glob(os.path.join(
            self.config.incoming_wals_directory, '*'))
        file_names.sort()

        # Process anything that looks like a valid WAL file. Anything
        # else is treated like an error/anomaly
        files = []
        errors = []
        for file_name in file_names:
            if xlog.is_any_xlog_file(file_name) and os.path.isfile(file_name):
                files.append(file_name)
            else:
                errors.append(file_name)

        # Build the list of WalFileInfo
        wal_files = [WalFileInfo.from_file(f) for f in files]
        return WalArchiverBatch(wal_files, errors=errors)

    def check(self, check_strategy):
        """
        Perform additional checks for FileWalArchiver - invoked
        by server.check_postgres

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        remote_status = self.get_remote_status()
        # If archive_mode is None, there are issues connecting to PostgreSQL
        if remote_status['archive_mode'] is None:
            return
        # Check archive_mode parameter: must be on
        if remote_status['archive_mode'] in ('on', 'always'):
            check_strategy.result(self.name, 'archive_mode', True)
        else:
            msg = "please set it to 'on'"
            if self.server.postgres.server_version >= 90500:
                msg += " or 'always'"
            check_strategy.result(self.name, 'archive_mode', False, msg)

        if remote_status['archive_command'] and \
                remote_status['archive_command'] != '(disabled)':
            check_strategy.result(self.name, 'archive_command',
                                  True)

            # Report if the archiving process works without issues.
            # Skip if the archive_command check fails
            # It can be None if PostgreSQL is older than 9.4
            if remote_status.get('is_archiving') is not None:
                check_strategy.result(
                    self.name, 'continuous archiving',
                    remote_status['is_archiving'])
        else:
            check_strategy.result(
                self.name, 'archive_command', False,
                'please set it accordingly to documentation')


class StreamingWalArchiver(WalArchiver):
    """
    Object used for the management of streaming WAL archive operation.
    """

    def __init__(self, backup_manager):
        super(StreamingWalArchiver, self).__init__(backup_manager, 'streaming')

    def fetch_remote_status(self):
        """
        Execute checks for replication-based wal archiving

        This method does not raise any exception in case of errors,
        but set the missing values to None in the resulting dictionary.

        :rtype: dict[str, None|str]
        """
        result = dict.fromkeys(
            ('pg_receivexlog_compatible',
             'pg_receivexlog_installed',
             'pg_receivexlog_path',
             'pg_receivexlog_version'),
            None)

        # Check the server version from the streaming
        # connection
        streaming = self.backup_manager.server.streaming
        server_txt_version = None
        if streaming:
            server_txt_version = streaming.server_txt_version
        if server_txt_version:
            pg_version = Version(utils.simplify_version(server_txt_version))
        else:
            # No log here, it has already been logged in the
            # StreamingConnection class or during the Server initialization
            pg_version = None

        # Detect a pg_receivexlog executable
        pg_receivexlog = utils.which("pg_receivexlog",
                                     self.backup_manager.server.path)

        # Test pg_receivexlog existence
        if pg_receivexlog:
            result["pg_receivexlog_installed"] = True
            result["pg_receivexlog_path"] = pg_receivexlog
        else:
            result["pg_receivexlog_installed"] = False
            return result

        receivexlog = Command(pg_receivexlog, check=True)

        # Obtain the `pg_receivexlog` version
        try:
            receivexlog("--version")
            splitter_version = receivexlog.out.strip().split()
            result["pg_receivexlog_version"] = splitter_version[-1]
            receivexlog_version = Version(
                utils.simplify_version(result["pg_receivexlog_version"]))
        except CommandFailedException as e:
            receivexlog_version = None
            _logger.debug("Error invoking pg_receivexlog: %s", e)

        # If one of the version is unknown we cannot compare them
        if receivexlog_version is None or pg_version is None:
            return result

        # pg_receivexlog 9.2 is compatible only with PostgreSQL 9.2.
        if "9.2" == pg_version == receivexlog_version:
            result["pg_receivexlog_compatible"] = True

        # other versions are compatible with lesser versions of PostgreSQL
        # WARNING: The development versions of `pg_receivexlog` are considered
        # higher than the stable versions here, but this is not an issue
        # because it accepts everything that is less than
        # the `pg_receivexlog` version(e.g. '9.6' is less than '9.6devel')
        elif "9.2" < pg_version <= receivexlog_version:
            result["pg_receivexlog_compatible"] = True

        else:
            result["pg_receivexlog_compatible"] = False

        return result

    def receive_wal(self):
        """
        Creates a PgReceiveXlog object and issues the pg_receivexlog command
        for a specific server

        :raise ArchiverFailure: when something goes wrong
        """
        # Execute basic sanity checks on PostgreSQL connection
        postgres_status = self.server.streaming.get_remote_status()
        if postgres_status["streaming_supported"] is None:
            raise ArchiverFailure(
                'failed opening the PostgreSQL streaming connection')
        elif not postgres_status["streaming_supported"]:
            raise ArchiverFailure(
                'PostgreSQL version too old (%s < 9.2)' %
                self.server.streaming.server_txt_version)
        # Execute basic sanity checks on pg_receivexlog
        remote_status = self.get_remote_status()
        if not remote_status["pg_receivexlog_installed"]:
            raise ArchiverFailure(
                'pg_receivexlog not present in $PATH')
        if not remote_status['pg_receivexlog_compatible']:
            raise ArchiverFailure(
                'pg_receivexlog version not compatible with '
                'PostgreSQL server version')

        # Make sure we are not wasting precious PostgreSQL resources
        self.server.postgres.close()
        self.server.streaming.close()

        _logger.info('Activating WAL archiving through streaming protocol')
        try:
            receive = PgReceiveXlog(remote_status['pg_receivexlog_path'],
                                    self.config.streaming_conninfo,
                                    self.config.streaming_wals_directory)
            receive.execute()
        except CommandFailedException as e:
            _logger.error(e)
            raise ArchiverFailure("pg_receivexlog exited with an error. "
                                  "Check the logs for more information.")

    def get_next_batch(self):
        """
        Returns the next batch of WAL files that have been archived via
        streaming replication (in the 'streaming' directory)

        This method always leaves one file in the "streaming" directory,
        because the 'pg_receivexlog' process needs at least one file to
        detect the current streaming position after a restart.

        :return: WalArchiverBatch: list of WAL files
        """
        # List and sort all files in the incoming directory
        file_names = glob(os.path.join(
            self.config.streaming_wals_directory, '*'))
        file_names.sort()

        # Process anything that looks like a valid WAL file,
        # including partial ones.
        # Anything else is treated like an error/anomaly
        files = []
        skip = []
        errors = []
        for file_name in file_names:
            if xlog.is_wal_file(file_name) and os.path.isfile(file_name):
                files.append(file_name)
            elif xlog.is_partial_file(file_name) and os.path.isfile(file_name):
                skip.append(file_name)
            else:
                errors.append(file_name)
        # In case of more than a partial file, keep the last
        # and treat the rest as errors
        if len(skip) > 1:
            errors.extend(skip[:-1])
            skip = skip[-1:]

        # Keep the last full WAL file in case no partial file is present
        elif len(skip) == 0 and files:
            skip.append(files.pop())

        # Build the list of WalFileInfo
        wal_files = [WalFileInfo.from_file(f, compression=None) for f in files]
        return WalArchiverBatch(wal_files, errors=errors, skip=skip)

    def check(self, check_strategy):
        """
        Perform additional checks for FileWalArchiver - invoked
        by server.check_postgres

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        :param dict remote_status: remote status of the server
        """
        remote_status = self.get_remote_status()
        check_strategy.result(
            self.name, 'pg_receivexlog',
            remote_status['pg_receivexlog_installed'])
        hint = None
        if not remote_status['pg_receivexlog_compatible']:
            hint = "PostgreSQL version: %s, pg_receivexlog version: %s" % (
                self.server.streaming.server_txt_version,
                remote_status['pg_receivexlog_version']
            )
        check_strategy.result(
            self.name, 'pg_receivexlog compatible',
            remote_status['pg_receivexlog_compatible'], hint=hint)
