# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2011-2025
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
import filecmp
import logging
import os
import shutil
from abc import ABCMeta, abstractmethod
from distutils.version import LooseVersion as Version
from glob import glob

from barman import output, xlog
from barman.command_wrappers import CommandFailedException, PgReceiveXlog
from barman.exceptions import (
    AbortedRetryHookScript,
    ArchiverFailure,
    DuplicateWalFile,
    MatchingDuplicateWalFile,
)
from barman.hooks import HookScriptRunner, RetryHookScriptRunner
from barman.infofile import WalFileInfo
from barman.remote_status import RemoteStatusMixin
from barman.utils import fsync_dir, fsync_file, mkpath, with_metaclass
from barman.xlog import is_partial_file

_logger = logging.getLogger(__name__)


class WalArchiverQueue(list):
    def __init__(self, items, errors=None, skip=None, batch_size=0):
        """
        A WalArchiverQueue is a list of WalFileInfo which has two extra
        attribute list:

        * errors: containing a list of unrecognized files
        * skip: containing a list of skipped files.

        It also stores batch run size information in case
        it is requested by configuration, in order to limit the
        number of WAL files that are processed in a single
        run of the archive-wal command.

        :param items: iterable from which initialize the list
        :param batch_size: size of the current batch run (0=unlimited)
        :param errors: an optional list of unrecognized files
        :param skip: an optional list of skipped files
        """
        super(WalArchiverQueue, self).__init__(items)
        self.skip = []
        self.errors = []
        if skip is not None:
            self.skip = skip
        if errors is not None:
            self.errors = errors
        # Normalises batch run size
        if batch_size > 0:
            self.batch_size = batch_size
        else:
            self.batch_size = 0

    @property
    def size(self):
        """
        Number of valid WAL segments waiting to be processed (in total)

        :return int: total number of valid WAL files
        """
        return len(self)

    @property
    def run_size(self):
        """
        Number of valid WAL files to be processed in this run - takes
        in consideration the batch size

        :return int: number of valid WAL files for this batch run
        """
        # In case a batch size has been explicitly specified
        # (i.e. batch_size > 0), returns the minimum number between
        # batch size and the queue size. Otherwise, simply
        # returns the total queue size (unlimited batch size).
        if self.batch_size > 0:
            return min(self.size, self.batch_size)
        return self.size


class WalArchiver(with_metaclass(ABCMeta, RemoteStatusMixin)):
    """
    Base class for WAL archiver objects
    """

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

    def receive_wal(self, reset=False):
        """
        Manage reception of WAL files. Does nothing by default.
        Some archiver classes, like the StreamingWalArchiver, have a full
        implementation.

        :param bool reset: When set, resets the status of receive-wal
        :raise ArchiverFailure: when something goes wrong
        """

    def archive(self, verbose=True):
        """
        Archive WAL files, discarding duplicates or those that are not valid.

        :param boolean verbose: Flag for verbose output
        """
        compressor = self.backup_manager.compression_manager.get_default_compressor()
        encryption = self.backup_manager.encryption_manager.get_encryption()
        processed = 0
        header = "Processing xlog segments from %s for %s" % (
            self.name,
            self.config.name,
        )

        # Get the next batch of WAL files to be processed
        batch = self.get_next_batch()

        # Analyse the batch and properly log the information
        if batch.size:
            if batch.size > batch.run_size:
                # Batch mode enabled
                _logger.info(
                    "Found %s xlog segments from %s for %s."
                    " Archive a batch of %s segments in this run.",
                    batch.size,
                    self.name,
                    self.config.name,
                    batch.run_size,
                )
                header += " (batch size: %s)" % batch.run_size
            else:
                # Single run mode (traditional)
                _logger.info(
                    "Found %s xlog segments from %s for %s."
                    " Archive all segments in one run.",
                    batch.size,
                    self.name,
                    self.config.name,
                )
        else:
            _logger.info(
                "No xlog segments found from %s for %s.", self.name, self.config.name
            )

        # Print the header (verbose mode)
        if verbose:
            output.info(header, log=False)

        # Loop through all available WAL files
        for wal_info in batch:
            # Print the header (non verbose mode)
            if not processed and not verbose:
                output.info(header, log=False)

            # Exit when archive batch size is reached
            if processed >= batch.run_size:
                _logger.debug(
                    "Batch size reached (%s) - Exit %s process for %s",
                    batch.batch_size,
                    self.name,
                    self.config.name,
                )
                break

            processed += 1

            # Report to the user the WAL file we are archiving
            output.info("\t%s", wal_info.name, log=False)
            _logger.info(
                "Archiving segment %s of %s from %s: %s/%s",
                processed,
                batch.run_size,
                self.name,
                self.config.name,
                wal_info.name,
            )
            # Archive the WAL file
            try:
                self.archive_wal(compressor, encryption, wal_info)
            except MatchingDuplicateWalFile:
                # We already have this file. Simply unlink the file.
                os.unlink(wal_info.orig_filename)
                continue
            except DuplicateWalFile:
                self.server.move_wal_file_to_errors_directory(
                    wal_info.orig_filename, wal_info.name, "duplicate"
                )
                output.info(
                    "\tError: %s is already present in server %s. "
                    "File moved to errors directory.",
                    wal_info.name,
                    self.config.name,
                )
                continue
            except AbortedRetryHookScript as e:
                _logger.warning(
                    "Archiving of %s/%s aborted by "
                    "pre_archive_retry_script."
                    "Reason: %s" % (self.config.name, wal_info.name, e)
                )
                return

        if processed:
            _logger.debug(
                "Archived %s out of %s xlog segments from %s for %s",
                processed,
                batch.size,
                self.name,
                self.config.name,
            )
        elif verbose:
            output.info("\tno file found", log=False)

        if batch.errors:
            output.info(
                "Some unknown objects have been found while "
                "processing xlog segments for %s. "
                "Objects moved to errors directory:",
                self.config.name,
                log=False,
            )
            # Log unexpected files
            _logger.warning(
                "Archiver is about to move %s unexpected file(s) "
                "to errors directory for %s from %s",
                len(batch.errors),
                self.config.name,
                self.name,
            )
            for error in batch.errors:
                basename = os.path.basename(error)
                output.info("\t%s", basename, log=False)
                # Print informative log line.
                _logger.warning(
                    "Moving unexpected file for %s from %s: %s",
                    self.config.name,
                    self.name,
                    basename,
                )
                self.server.move_wal_file_to_errors_directory(
                    error, basename, "unknown"
                )

    def archive_wal(self, compressor, encryption, wal_info):
        """
        Archive a WAL segment and update the wal_info object

        :param compressor: the compressor for the file (if any)
        :param None|Encryption encryption: the encryptor for the file (if any)
        :param WalFileInfo wal_info: the WAL file is being processed
        """

        src_file = wal_info.orig_filename
        src_dir = os.path.dirname(src_file)
        dst_file = wal_info.fullpath(self.server)
        tmp_file = dst_file + ".tmp"
        dst_dir = os.path.dirname(dst_file)

        comp_manager = self.backup_manager.compression_manager

        error = None
        try:
            # Run the pre_archive_script if present.
            script = HookScriptRunner(self.backup_manager, "archive_script", "pre")
            script.env_from_wal_info(wal_info, src_file)
            script.run()

            # Run the pre_archive_retry_script if present.
            retry_script = RetryHookScriptRunner(
                self.backup_manager, "archive_retry_script", "pre"
            )
            retry_script.env_from_wal_info(wal_info, src_file)
            retry_script.run()

            # Check if destination already exists
            if os.path.exists(dst_file):
                dst_info = self.backup_manager.get_wal_file_info(dst_file)
                src_uncompressed = src_file
                dst_uncompressed = dst_file
                try:
                    # If the existing destination file is already encrypted, it can't be
                    # decrypted or uncompressed to perform any of the later comparisons
                    # (because we cannot assume the encryption passphrase is always
                    # available in the configuration).
                    if dst_info.encryption:
                        raise DuplicateWalFile(wal_info)
                    # If the existing file is already compressed, decompress it to a
                    # <dst_wal_path>.uncompressed file
                    if dst_info.compression is not None:
                        dst_uncompressed = dst_file + ".uncompressed"
                        comp_manager.get_compressor(dst_info.compression).decompress(
                            dst_file, dst_uncompressed
                        )
                    # If the source file is already compressed (because the user
                    # compressed it manually with a script in the archive_command),
                    # then decompress it to a <src_wal_path>.uncompressed file
                    if wal_info.compression:
                        src_uncompressed = src_file + ".uncompressed"
                        comp_manager.get_compressor(wal_info.compression).decompress(
                            src_file, src_uncompressed
                        )
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

            # List of intermediate files that will need to be removed after the archival
            files_to_remove = []
            # The current working file being touched
            current_file = src_file
            # If the bits of the file has changed e.g. due to compression or encryption
            content_changed = False
            # Compress the file if not already compressed
            if compressor and not wal_info.compression:
                compressor.compress(src_file, tmp_file)
                files_to_remove.append(current_file)
                current_file = tmp_file
                content_changed = True
                wal_info.compression = compressor.compression
            # Encrypt the file
            if encryption:
                encrypted_file = encryption.encrypt(current_file, dst_dir)
                files_to_remove.append(current_file)
                current_file = encrypted_file
                wal_info.encryption = encryption.NAME
                content_changed = True

            # Perform the real filesystem operation with the xlogdb lock taken.
            # This makes the operation atomic from the xlogdb file POV
            with self.server.xlogdb("a") as fxlogdb:
                # If the content has changed, it means the file was either compressed
                # or encrypted or both. In this case, we need to update its metadata
                if content_changed:
                    shutil.copystat(src_file, current_file)
                    stat = os.stat(current_file)
                    wal_info.size = stat.st_size

                # Try to atomically rename the file. If successful, the renaming will
                # be an atomic operation (this is a POSIX requirement).
                try:
                    os.rename(current_file, dst_file)
                except OSError:
                    # Source and destination are probably on different filesystems
                    shutil.copy2(current_file, tmp_file)
                    os.rename(tmp_file, dst_file)
                finally:
                    for file in files_to_remove:
                        os.unlink(file)

                # At this point the original file has been removed
                wal_info.orig_filename = None
                # Execute fsync() on the archived WAL file
                fsync_file(dst_file)
                # Execute fsync() on the archived WAL containing directory
                fsync_dir(dst_dir)
                # Execute fsync() also on the incoming directory
                fsync_dir(src_dir)
                # Updates the information of the WAL archive with
                # the latest segments
                fxlogdb.write(wal_info.to_xlogdb_line())
                # flush and fsync for every line
                fxlogdb.flush()
                os.fsync(fxlogdb.fileno())

        except Exception as e:
            # In case of failure save the exception for the post scripts
            error = e
            raise

        # Ensure the execution of the post_archive_retry_script and
        # the post_archive_script
        finally:
            # Run the post_archive_retry_script if present.
            try:
                retry_script = RetryHookScriptRunner(
                    self, "archive_retry_script", "post"
                )
                retry_script.env_from_wal_info(wal_info, dst_file, error)
                retry_script.run()
            except AbortedRetryHookScript as e:
                # Ignore the ABORT_STOP as it is a post-hook operation
                _logger.warning(
                    "Ignoring stop request after receiving "
                    "abort (exit code %d) from post-archive "
                    "retry hook script: %s",
                    e.hook.exit_status,
                    e.hook.script,
                )

            # Run the post_archive_script if present.
            script = HookScriptRunner(self, "archive_script", "post", error)
            script.env_from_wal_info(wal_info, dst_file)
            script.run()

    @abstractmethod
    def get_next_batch(self):
        """
        Return a WalArchiverQueue containing the WAL files to be archived.

        :rtype: WalArchiverQueue
        """

    @abstractmethod
    def check(self, check_strategy):
        """
        Perform specific checks for the archiver - invoked
        by server.check_postgres

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """

    @abstractmethod
    def status(self):
        """
        Set additional status info - invoked by Server.status()
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
                counters["not relevant"] += 1
            elif name.endswith(".duplicate"):
                counters["duplicates"] += 1
            elif name.endswith(".unknown"):
                counters["unknown"] += 1
            else:
                counters["unknown failure"] += 1

        # Return a summary list of the form: "item a: 2, item b: 5"
        return ", ".join("%s: %s" % entry for entry in counters.items())


class FileWalArchiver(WalArchiver):
    """
    Manager of file-based WAL archiving operations (aka 'log shipping').
    """

    def __init__(self, backup_manager):
        super(FileWalArchiver, self).__init__(backup_manager, "file archival")

    def fetch_remote_status(self):
        """
        Returns the status of the FileWalArchiver.

        This method does not raise any exception in case of errors,
        but set the missing values to None in the resulting dictionary.

        :rtype: dict[str, None|str]
        """
        result = dict.fromkeys(["archive_mode", "archive_command"], None)
        postgres = self.server.postgres
        # If Postgres is not available we cannot detect anything
        if not postgres:
            return result
        # Query the database for 'archive_mode' and 'archive_command'
        result["archive_mode"] = postgres.get_setting("archive_mode")
        result["archive_command"] = postgres.get_setting("archive_command")

        # Add pg_stat_archiver statistics if the view is supported
        pg_stat_archiver = postgres.get_archiver_stats()
        if pg_stat_archiver is not None:
            result.update(pg_stat_archiver)
        return result

    def get_next_batch(self):
        """
        Returns the next batch of WAL files that have been archived through
        a PostgreSQL's 'archive_command' (in the 'incoming' directory)

        :return: WalArchiverQueue: list of WAL files
        """
        # Get the batch size from configuration (0 = unlimited)
        batch_size = self.config.archiver_batch_size
        # List and sort all files in the incoming directory
        # IMPORTANT: the list is sorted, and this allows us to know that the
        # WAL stream we have is monotonically increasing. That allows us to
        # verify that a backup has all the WALs required for the restore.
        file_names = glob(os.path.join(self.config.incoming_wals_directory, "*"))
        file_names.sort()

        # Process anything that looks like a valid WAL file. Anything
        # else is treated like an error/anomaly
        files = []
        errors = []
        for file_name in file_names:
            # Ignore temporary files
            if file_name.endswith(".tmp"):
                continue
            if xlog.is_any_xlog_file(file_name) and os.path.isfile(file_name):
                files.append(file_name)
            else:
                errors.append(file_name)

        # Build the list of WalFileInfo
        wal_files = [
            WalFileInfo.from_file(
                filename=f,
                compression_manager=self.backup_manager.compression_manager,
                unidentified_compression=None,
                encryption_manager=self.backup_manager.encryption_manager,
            )
            for f in files
        ]
        return WalArchiverQueue(wal_files, batch_size=batch_size, errors=errors)

    def check(self, check_strategy):
        """
        Perform additional checks for FileWalArchiver - invoked
        by server.check_postgres

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """
        check_strategy.init_check("archive_mode")
        remote_status = self.get_remote_status()
        # If archive_mode is None, there are issues connecting to PostgreSQL
        if remote_status["archive_mode"] is None:
            return
        # Check archive_mode parameter: must be on
        if remote_status["archive_mode"] in ("on", "always"):
            check_strategy.result(self.config.name, True)
        else:
            msg = "please set it to 'on'"
            if self.server.postgres.server_version >= 90500:
                msg += " or 'always'"
            check_strategy.result(self.config.name, False, hint=msg)
        check_strategy.init_check("archive_command")
        if (
            remote_status["archive_command"]
            and remote_status["archive_command"] != "(disabled)"
        ):
            check_strategy.result(self.config.name, True, check="archive_command")

            # Report if the archiving process works without issues.
            # Skip if the archive_command check fails
            # It can be None if PostgreSQL is older than 9.4
            if remote_status.get("is_archiving") is not None:
                check_strategy.result(
                    self.config.name,
                    remote_status["is_archiving"],
                    check="continuous archiving",
                )
        else:
            check_strategy.result(
                self.config.name,
                False,
                hint="please set it accordingly to documentation",
            )

    def status(self):
        """
        Set additional status info - invoked by Server.status()
        """
        # We need to get full info here from the server
        remote_status = self.server.get_remote_status()

        # If archive_mode is None, there are issues connecting to PostgreSQL
        if remote_status["archive_mode"] is None:
            return

        output.result(
            "status",
            self.config.name,
            "archive_command",
            "PostgreSQL 'archive_command' setting",
            remote_status["archive_command"]
            or "FAILED (please set it accordingly to documentation)",
        )
        last_wal = remote_status.get("last_archived_wal")
        # If PostgreSQL is >= 9.4 we have the last_archived_time
        if last_wal and remote_status.get("last_archived_time"):
            last_wal += ", at %s" % (remote_status["last_archived_time"].ctime())
        output.result(
            "status",
            self.config.name,
            "last_archived_wal",
            "Last archived WAL",
            last_wal or "No WAL segment shipped yet",
        )
        # Set output for WAL archive failures (PostgreSQL >= 9.4)
        if remote_status.get("failed_count") is not None:
            remote_fail = str(remote_status["failed_count"])
            if int(remote_status["failed_count"]) > 0:
                remote_fail += " (%s at %s)" % (
                    remote_status["last_failed_wal"],
                    remote_status["last_failed_time"].ctime(),
                )
            output.result(
                "status",
                self.config.name,
                "failed_count",
                "Failures of WAL archiver",
                remote_fail,
            )
        # Add hourly archive rate if available (PostgreSQL >= 9.4) and > 0
        if remote_status.get("current_archived_wals_per_second"):
            output.result(
                "status",
                self.config.name,
                "server_archived_wals_per_hour",
                "Server WAL archiving rate",
                "%0.2f/hour"
                % (3600 * remote_status["current_archived_wals_per_second"]),
            )


class StreamingWalArchiver(WalArchiver):
    """
    Object used for the management of streaming WAL archive operation.
    """

    def __init__(self, backup_manager):
        super(StreamingWalArchiver, self).__init__(backup_manager, "streaming")

    def fetch_remote_status(self):
        """
        Execute checks for replication-based wal archiving

        This method does not raise any exception in case of errors,
        but set the missing values to None in the resulting dictionary.

        :rtype: dict[str, None|str]
        """
        remote_status = dict.fromkeys(
            (
                "pg_receivexlog_compatible",
                "pg_receivexlog_installed",
                "pg_receivexlog_path",
                "pg_receivexlog_supports_slots",
                "pg_receivexlog_synchronous",
                "pg_receivexlog_version",
            ),
            None,
        )

        # Test pg_receivexlog existence
        version_info = PgReceiveXlog.get_version_info(self.server.path)
        if version_info["full_path"]:
            remote_status["pg_receivexlog_installed"] = True
            remote_status["pg_receivexlog_path"] = version_info["full_path"]
            remote_status["pg_receivexlog_version"] = version_info["full_version"]
            pgreceivexlog_version = version_info["major_version"]
        else:
            remote_status["pg_receivexlog_installed"] = False
            return remote_status

        # Retrieve the PostgreSQL version
        pg_version = None
        if self.server.streaming is not None:
            pg_version = self.server.streaming.server_major_version

        # If one of the version is unknown we cannot compare them
        if pgreceivexlog_version is None or pg_version is None:
            return remote_status

        # pg_version is not None so transform into a Version object
        # for easier comparison between versions
        pg_version = Version(pg_version)

        # Set conservative default values (False) for modern features
        remote_status["pg_receivexlog_compatible"] = False
        remote_status["pg_receivexlog_supports_slots"] = False
        remote_status["pg_receivexlog_synchronous"] = False

        # pg_receivexlog 9.2 is compatible only with PostgreSQL 9.2.
        if "9.2" == pg_version == pgreceivexlog_version:
            remote_status["pg_receivexlog_compatible"] = True

        # other versions are compatible with lesser versions of PostgreSQL
        # WARNING: The development versions of `pg_receivexlog` are considered
        # higher than the stable versions here, but this is not an issue
        # because it accepts everything that is less than
        # the `pg_receivexlog` version(e.g. '9.6' is less than '9.6devel')
        elif "9.2" < pg_version <= pgreceivexlog_version:
            # At least PostgreSQL 9.3 is required here
            remote_status["pg_receivexlog_compatible"] = True

            # replication slots are supported starting from version 9.4
            if "9.4" <= pg_version <= pgreceivexlog_version:
                remote_status["pg_receivexlog_supports_slots"] = True

            # Synchronous WAL streaming requires replication slots
            # and pg_receivexlog >= 9.5
            if "9.4" <= pg_version and "9.5" <= pgreceivexlog_version:
                remote_status["pg_receivexlog_synchronous"] = self._is_synchronous()

        return remote_status

    def receive_wal(self, reset=False):
        """
        Creates a PgReceiveXlog object and issues the pg_receivexlog command
        for a specific server

        :param bool reset: When set reset the status of receive-wal
        :raise ArchiverFailure: when something goes wrong
        """
        # Ensure the presence of the destination directory
        mkpath(self.config.streaming_wals_directory)

        # Execute basic sanity checks on PostgreSQL connection
        streaming_status = self.server.streaming.get_remote_status()
        if streaming_status["streaming_supported"] is None:
            raise ArchiverFailure(
                "failed opening the PostgreSQL streaming connection "
                "for server %s" % (self.config.name)
            )
        elif not streaming_status["streaming_supported"]:
            raise ArchiverFailure(
                "PostgreSQL version too old (%s < 9.2)"
                % self.server.streaming.server_txt_version
            )
        # Execute basic sanity checks on pg_receivexlog
        command = "pg_receivewal"
        if self.server.streaming.server_version < 100000:
            command = "pg_receivexlog"
        remote_status = self.get_remote_status()
        if not remote_status["pg_receivexlog_installed"]:
            raise ArchiverFailure("%s not present in $PATH" % command)
        if not remote_status["pg_receivexlog_compatible"]:
            raise ArchiverFailure(
                "%s version not compatible with PostgreSQL server version" % command
            )

        # Execute sanity check on replication slot usage
        postgres_status = self.server.postgres.get_remote_status()
        if self.config.slot_name:
            # Check if slots are supported
            if not remote_status["pg_receivexlog_supports_slots"]:
                raise ArchiverFailure(
                    "Physical replication slot not supported by %s "
                    "(9.4 or higher is required)"
                    % self.server.streaming.server_txt_version
                )
            # Check if the required slot exists
            if postgres_status["replication_slot"] is None:
                if self.config.create_slot == "auto":
                    if not reset:
                        output.info(
                            "Creating replication slot '%s'", self.config.slot_name
                        )
                        self.server.create_physical_repslot()
                else:
                    raise ArchiverFailure(
                        "replication slot '%s' doesn't exist. "
                        "Please execute "
                        "'barman receive-wal --create-slot %s'"
                        % (self.config.slot_name, self.config.name)
                    )
            # Check if the required slot is available
            elif postgres_status["replication_slot"].active:
                raise ArchiverFailure(
                    "replication slot '%s' is already in use" % (self.config.slot_name,)
                )

        # Check if is a reset request
        if reset:
            self._reset_streaming_status(postgres_status, streaming_status)
            return

        # Check the size of the .partial WAL file and truncate it if needed
        self._truncate_partial_file_if_needed(postgres_status["xlog_segment_size"])

        # Make sure we are not wasting precious PostgreSQL resources
        self.server.close()

        _logger.info("Activating WAL archiving through streaming protocol")
        try:
            output_handler = PgReceiveXlog.make_output_handler(self.config.name + ": ")
            receive = PgReceiveXlog(
                connection=self.server.streaming,
                destination=self.config.streaming_wals_directory,
                command=remote_status["pg_receivexlog_path"],
                version=remote_status["pg_receivexlog_version"],
                app_name=self.config.streaming_archiver_name,
                path=self.server.path,
                slot_name=self.config.slot_name,
                synchronous=remote_status["pg_receivexlog_synchronous"],
                out_handler=output_handler,
                err_handler=output_handler,
            )
            # Finally execute the pg_receivexlog process
            receive.execute()
        except CommandFailedException as e:
            # Retrieve the return code from the exception
            ret_code = e.args[0]["ret"]
            if ret_code < 0:
                # If the return code is negative, then pg_receivexlog
                # was terminated by a signal
                msg = "%s terminated by signal: %s" % (command, abs(ret_code))
            else:
                # Otherwise terminated with an error
                msg = "%s terminated with error code: %s" % (command, ret_code)

            raise ArchiverFailure(msg)
        except KeyboardInterrupt:
            # This is a normal termination, so there is nothing to do beside
            # informing the user.
            output.info("SIGINT received. Terminate gracefully.")

    def _reset_streaming_status(self, postgres_status, streaming_status):
        """
        Reset the status of receive-wal by removing the .partial file that
        is marking the current position and creating one that is current with
        the PostgreSQL insert location
        """
        current_wal = xlog.location_to_xlogfile_name_offset(
            postgres_status["current_lsn"],
            streaming_status["timeline"],
            postgres_status["xlog_segment_size"],
        )["file_name"]
        restart_wal = current_wal
        if (
            postgres_status["replication_slot"]
            and postgres_status["replication_slot"].restart_lsn
        ):
            restart_wal = xlog.location_to_xlogfile_name_offset(
                postgres_status["replication_slot"].restart_lsn,
                streaming_status["timeline"],
                postgres_status["xlog_segment_size"],
            )["file_name"]
        restart_path = os.path.join(self.config.streaming_wals_directory, restart_wal)
        restart_partial_path = restart_path + ".partial"
        wal_files = sorted(
            glob(os.path.join(self.config.streaming_wals_directory, "*")), reverse=True
        )

        # Pick the newer file
        last = None
        for last in wal_files:
            if xlog.is_wal_file(last) or xlog.is_partial_file(last):
                break

        # Check if the status is already up-to-date
        if not last or last == restart_partial_path or last == restart_path:
            output.info("Nothing to do. Position of receive-wal is aligned.")
            return

        if os.path.basename(last) > current_wal:
            output.error(
                "The receive-wal position is ahead of PostgreSQL "
                "current WAL lsn (%s > %s)",
                os.path.basename(last),
                postgres_status["current_xlog"],
            )
            return

        output.info("Resetting receive-wal directory status")
        if xlog.is_partial_file(last):
            output.info("Removing status file %s" % last)
            os.unlink(last)
        output.info("Creating status file %s" % restart_partial_path)
        open(restart_partial_path, "w").close()

    def _truncate_partial_file_if_needed(self, xlog_segment_size):
        """
        Truncate .partial WAL file if size is not 0 or xlog_segment_size

        :param int xlog_segment_size:
        """
        # Retrieve the partial list (only one is expected)
        partial_files = glob(
            os.path.join(self.config.streaming_wals_directory, "*.partial")
        )

        # Take the last partial file, ignoring wrongly formatted file names
        last_partial = None
        for partial in partial_files:
            if not is_partial_file(partial):
                continue
            if not last_partial or partial > last_partial:
                last_partial = partial

        # Skip further work if there is no good partial file
        if not last_partial:
            return

        # If size is either 0 or wal_segment_size everything is fine...
        partial_size = os.path.getsize(last_partial)
        if partial_size == 0 or partial_size == xlog_segment_size:
            return

        # otherwise truncate the file to be empty. This is safe because
        # pg_receivewal pads the file to the full size before start writing.
        output.info(
            "Truncating partial file %s that has wrong size %s "
            "while %s was expected." % (last_partial, partial_size, xlog_segment_size)
        )
        open(last_partial, "wb").close()

    def get_next_batch(self):
        """
        Returns the next batch of WAL files that have been archived via
        streaming replication (in the 'streaming' directory)

        This method always leaves one file in the "streaming" directory,
        because the 'pg_receivexlog' process needs at least one file to
        detect the current streaming position after a restart.

        :return: WalArchiverQueue: list of WAL files
        """
        # Get the batch size from configuration (0 = unlimited)
        batch_size = self.config.streaming_archiver_batch_size
        # List and sort all files in the incoming directory.
        # IMPORTANT: the list is sorted, and this allows us to know that the
        # WAL stream we have is monotonically increasing. That allows us to
        # verify that a backup has all the WALs required for the restore.
        file_names = glob(os.path.join(self.config.streaming_wals_directory, "*"))
        file_names.sort()

        # Process anything that looks like a valid WAL file,
        # including partial ones and history files.
        # Anything else is treated like an error/anomaly
        files = []
        skip = []
        errors = []
        for file_name in file_names:
            # Ignore temporary files
            if file_name.endswith(".tmp"):
                continue
            # If the file doesn't exist, it has been renamed/removed while
            # we were reading the directory. Ignore it.
            if not os.path.exists(file_name):
                continue
            if not os.path.isfile(file_name):
                errors.append(file_name)
            elif xlog.is_partial_file(file_name):
                skip.append(file_name)
            elif xlog.is_any_xlog_file(file_name):
                files.append(file_name)
            else:
                errors.append(file_name)
        # In case of more than a partial file, keep the last
        # and treat the rest as normal files
        if len(skip) > 1:
            partials = skip[:-1]
            _logger.info(
                "Archiving partial files for server %s: %s"
                % (self.config.name, ", ".join([os.path.basename(f) for f in partials]))
            )
            files.extend(partials)
            skip = skip[-1:]

        # Keep the last full WAL file in case no partial file is present
        elif len(skip) == 0 and files:
            skip.append(files.pop())

        # Build the list of WalFileInfo
        wal_files = [
            WalFileInfo.from_file(
                filename=f,
                compression_manager=self.backup_manager.compression_manager,
                encryption_manager=self.backup_manager.encryption_manager,
                unidentified_compression=None,
                compression=None,
                encryption=None,
            )
            for f in files
        ]
        return WalArchiverQueue(
            wal_files, batch_size=batch_size, errors=errors, skip=skip
        )

    def check(self, check_strategy):
        """
        Perform additional checks for StreamingWalArchiver - invoked
        by server.check_postgres

        :param CheckStrategy check_strategy: the strategy for the management
             of the results of the various checks
        """

        check_strategy.init_check("pg_receivexlog")
        # Check the version of pg_receivexlog
        remote_status = self.get_remote_status()
        check_strategy.result(
            self.config.name, remote_status["pg_receivexlog_installed"]
        )
        hint = None
        check_strategy.init_check("pg_receivexlog compatible")
        if not remote_status["pg_receivexlog_compatible"]:
            pg_version = "Unknown"
            if self.server.streaming is not None:
                pg_version = self.server.streaming.server_txt_version
            hint = "PostgreSQL version: %s, pg_receivexlog version: %s" % (
                pg_version,
                remote_status["pg_receivexlog_version"],
            )
        check_strategy.result(
            self.config.name, remote_status["pg_receivexlog_compatible"], hint=hint
        )

        # Check if pg_receivexlog is running, by retrieving a list
        # of running 'receive-wal' processes from the process manager.
        receiver_list = self.server.process_manager.list("receive-wal")

        # If there's at least one 'receive-wal' process running for this
        # server, the test is passed
        check_strategy.init_check("receive-wal running")
        if receiver_list:
            check_strategy.result(self.config.name, True)
        else:
            check_strategy.result(
                self.config.name, False, hint="See the Barman log file for more details"
            )

    def _is_synchronous(self):
        """
        Check if receive-wal process is eligible for synchronous replication

        The receive-wal process is eligible for synchronous replication
        if `synchronous_standby_names` is configured and contains
        the value of `streaming_archiver_name`

        :rtype: bool
        """
        # Nothing to do if postgres connection is not working
        postgres = self.server.postgres
        if postgres is None or postgres.server_txt_version is None:
            return None

        # Check if synchronous WAL streaming can be enabled
        # by peeking 'synchronous_standby_names'
        postgres_status = postgres.get_remote_status()
        syncnames = postgres_status["synchronous_standby_names"]
        _logger.debug(
            "Look for '%s' in 'synchronous_standby_names': %s",
            self.config.streaming_archiver_name,
            syncnames,
        )
        # The receive-wal process is eligible for synchronous replication
        # if `synchronous_standby_names` is configured and contains
        # the value of `streaming_archiver_name`
        streaming_archiver_name = self.config.streaming_archiver_name
        synchronous = syncnames and (
            "*" in syncnames or streaming_archiver_name in syncnames
        )
        _logger.debug(
            "Synchronous WAL streaming for %s: %s", streaming_archiver_name, synchronous
        )
        return synchronous

    def status(self):
        """
        Set additional status info - invoked by Server.status()
        """
        # TODO: Add status information for WAL streaming
