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
import errno
import filecmp
import logging
import multiprocessing
import os
import shutil
import sys
from abc import ABCMeta, abstractmethod
from glob import glob
from tempfile import NamedTemporaryFile

from barman import output, xlog
from barman.cloud_providers import ObjectKeyAlreadyExists
from barman.command_wrappers import CommandFailedException, PgReceiveXlog
from barman.compression import InternalCompressor, compression_registry
from barman.exceptions import (
    AbortedRetryHookScript,
    ArchiverFailure,
    CompressionException,
    DuplicateWalFile,
    MatchingDuplicateWalFile,
)
from barman.hooks import HookScriptRunner, RetryHookScriptRunner
from barman.infofile import WalFileInfo
from barman.remote_status import RemoteStatusMixin
from barman.utils import LooseVersion as Version
from barman.utils import fsync_dir, fsync_file, mkpath, with_metaclass
from barman.xlog import is_partial_file

_logger = logging.getLogger(__name__)


class WalArchiverQueue(list):
    def __init__(self, items, errors=None, skip=None, batch_size=0, total_size=0):
        """
        A WalArchiverQueue is a list of WalFileInfo which has two extra
        attribute list:

        * errors: containing a list of unrecognized files
        * skip: containing a list of skipped files.

        It also stores batch run size information in case
        it is requested by configuration, in order to limit the
        number of WAL files that are processed in a single
        run of the archive-wal command.

        .. note::
            This class was originally designed to hold all the WAL files pending to be
            archived, and to process up to a certain number of files (the batch size).
            However, it is now used to hold only the files that are being processed in a
            single run of the archive-wal command. That change was made to avoid the
            overhead of having to create lots of WalFileInfo objects, even if only a
            subset of them would be used by the archiver in a given batch. We might want
            to rework or remove this class in the future, as it doesn't seem to add much
            value to the archiving process anymore.

        :param items: iterable from which initialize the list
        :param errors: an optional list of unrecognized files
        :param skip: an optional list of skipped files
        :param batch_size: size of the current batch run (0=unlimited)
        :param total_size: the total number of WAL files available for archiving.
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

        self.total_size = total_size
        self.run_size = len(self)


class WalStorageStrategy(metaclass=ABCMeta):
    """
    Abstract base class for WAL storage strategies.

    WAL storage strategies are used to effectively store WAL files in the
    connfigured destination, be it local filesystem or cloud storage.
    """

    def __init__(self, backup_manager, server):
        """
        Constructor.

        :param barman.backup_manager.BackupManager backup_manager: The backup manager
        :param barman.server.Server server: The server
        """
        self.backup_manager = backup_manager
        self.config = backup_manager.config
        self.server = server

    @abstractmethod
    def save(self, compressor, encryption, wal_info, **kwargs):
        """
        Effectively persist a WAL file according to the configured destination.

        :param compressor: the compressor for the file (if any)
        :param None|Encryption encryption: the encryptor for the file (if any)
        :param WalFileInfo wal_info: the WAL file is being processed
        :param kwargs: additional parameters for the storage strategy, if any
        """

    @abstractmethod
    def delete(self, wals_to_delete):
        """
        Delete WAL files according to the configured destination.

        :param dict[str, list[WalFileInfo]] wals_to_delete: A dictionary where key is
            the WAL directory name and value is a list of wal_info objects representing
            the WALs to be deleted in that directory.
        :return list[str]: a list of deleted WAL names.
        """

    def _run_pre_archive_scripts(self, wal_info, src_file):
        """
        Run the pre-archive scripts.

        :param WalFileInfo wal_info: the WAL file info object
        :param str src_file: the source WAL file path
        """
        # Run the pre_archive_script if present
        script = HookScriptRunner(self.backup_manager, "archive_script", "pre")
        script.env_from_wal_info(wal_info, src_file)
        script.run()

        # Run the pre_archive_retry_script if present
        retry_script = RetryHookScriptRunner(
            self.backup_manager, "archive_retry_script", "pre"
        )
        retry_script.env_from_wal_info(wal_info, src_file)
        retry_script.run()

    def _run_post_archive_scripts(self, wal_info, dest_file, error):
        """
        Run the post-archive scripts.

        :param WalFileInfo wal_info: the WAL file info object
        :param str dest_file: the destination WAL file path
        :param Exception|None error: the exception raised during
            the archival process, if any
        """
        try:
            retry_script = RetryHookScriptRunner(
                self.backup_manager, "archive_retry_script", "post"
            )
            retry_script.env_from_wal_info(wal_info, dest_file, error)
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

        # Run the post_archive_script if present
        script = HookScriptRunner(self.backup_manager, "archive_script", "post", error)
        script.env_from_wal_info(wal_info, dest_file)
        script.run()

    def _run_pre_delete_wal_scripts(self, wal_info):
        """
        Run the pre-delete hook-scripts, if any, on the given WAL.

        :param barman.infofile.WalFileInfo wal_info: WAL to run the script on.
        """
        # Run the pre_wal_delete_script if present.
        script = HookScriptRunner(self.backup_manager, "wal_delete_script", "pre")
        script.env_from_wal_info(wal_info)
        script.run()
        # Run the pre_wal_delete_retry_script if present.
        retry_script = RetryHookScriptRunner(
            self.backup_manager, "wal_delete_retry_script", "pre"
        )
        retry_script.env_from_wal_info(wal_info)
        retry_script.run()

    def _run_post_delete_wal_scripts(self, wal_info, error=None):
        """
        Run the post-delete hook-scripts, if any, on the given WAL.

        :param barman.infofile.WalFileInfo wal_info: WAL to run the script on.
        :param None|str error: error message in case a failure happened.
        """
        # Run the post_wal_delete_retry_script if present.
        try:
            retry_script = RetryHookScriptRunner(
                self.backup_manager, "wal_delete_retry_script", "post"
            )
            retry_script.env_from_wal_info(wal_info, None, error)
            retry_script.run()
        except AbortedRetryHookScript as e:
            # Ignore the ABORT_STOP as it is a post-hook operation
            _logger.warning(
                "Ignoring stop request after receiving "
                "abort (exit code %d) from post-wal-delete "
                "retry hook script: %s",
                e.hook.exit_status,
                e.hook.script,
            )
        # Run the post_wal_delete_script if present.
        script = HookScriptRunner(self.backup_manager, "wal_delete_script", "post")
        script.env_from_wal_info(wal_info, None, error)
        script.run()


class LocalWalStorageStrategy(WalStorageStrategy):
    """
    WAL storage strategy for local filesystem storage.

    .. note::
        For now, this class is also responsible for encrypting and compressing files
        before storing them, but in the future this responsibility should be moved
        elsewhere, desirably in a shared component, so that strategies' only
        responsibility is to actually persist files.
    """

    def __init__(self, backup_manager, server):
        super(LocalWalStorageStrategy, self).__init__(backup_manager, server)
        self.files_to_remove = []

    def _check_duplicate(self, src_file, dst_file, wal_info):
        """
        Check if the destination WAL file already exists in local storage, and if so,
        whether it is identical to the source file.

        :param str src_file: the source WAL file path
        :param str dst_file: the destination WAL file path
        :param WalFileInfo wal_info: the WAL file info object
        :raise DuplicateWalFile: if the destination file exists and is
            different from the source file
        :raise MatchingDuplicateWalFile: if the destination file exists and is
            identical to the source file
        """
        if not os.path.exists(dst_file):
            return

        comp_manager = self.backup_manager.compression_manager
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
            # Directly compare files. When the files are identical raise a
            # MatchingDuplicateWalFile exception, otherwise raise DuplicateWalFile
            if filecmp.cmp(dst_uncompressed, src_uncompressed):
                raise MatchingDuplicateWalFile(wal_info)
            else:
                raise DuplicateWalFile(wal_info)
        finally:
            if src_uncompressed != src_file:
                os.unlink(src_uncompressed)
            if dst_uncompressed != dst_file:
                os.unlink(dst_uncompressed)

    def _compress_file(self, compressor, src_file, dst_dir, wal_info):
        """
        Compress *src_file* to a "temp" file inside *dst_dir* and updates *wal_info*.

        .. note::
            The temporary compressed file will have the same name as the source file,
            with a ".compressed" suffix appended. It's created on the assumption that
            the caller will later remove the file or rename/move it to its final
            destination.

        :param compressor: the compressor to use
        :param str src_file: the file to compress
        :param str dst_dir: the directory where the compressed file will be created
        :param WalFileInfo wal_info: the WAL file info object
        :returns: the path to the compressed temporary file
        """
        tmp_file = "%s.compressed" % os.path.join(dst_dir, os.path.basename(src_file))
        compressor.compress(src_file, tmp_file)
        self.files_to_remove.append(src_file)
        wal_info.compression = compressor.compression
        return tmp_file

    def _encrypt_file(self, encryption, src_file, dst_dir, wal_info):
        """
        Encrypt *src_file* to a "temp" file inside *dst_dir* and updates *wal_info*.

        .. note::
            The temporary encrypted file will have the same name as the source file,
            with a ".gpg" suffix appended. It's created on the assumption that
            the caller will later remove the file or rename/move it to its final
            destination.

        :param encryption: the encryptor to use
        :param str src_file: the file to encrypt
        :param str dst_dir: the directory where the encrypted file will be created
        :param WalFileInfo wal_info: the WAL file info object
        :returns: the path to the encrypted temporary file
        """
        # The encrypted file will have the .gpg extension
        encrypted_file = encryption.encrypt(src_file, dst_dir)
        self.files_to_remove.append(src_file)
        wal_info.encryption = encryption.NAME
        return encrypted_file

    def _copy_stats(self, src_file, dst_file, wal_info):
        """
        Copy stats from *src_file* to *dst_file* and updates its *wal_info*.

        This is used preserve the metadata of the original file after compression or
        encryption, updating only the size in *wal_info* accordingly.

        :param str src_file: the source WAL file path
        :param str dst_file: the destination WAL file path
        :param WalFileInfo wal_info: the WAL file info object
        """
        shutil.copystat(src_file, dst_file)
        stat = os.stat(dst_file)
        wal_info.size = stat.st_size

    def _rename_or_copy_file(self, src_file, dst_file):
        """
        Rename or copy *src_file* to *dst_file*.

        Rename is attempted first, and if it fails (because the source and
        destination are on different filesystems), a copy is performed.

        :param str src_file: the source file path
        :param str dst_file: the destination file path
        """
        try:
            os.rename(src_file, dst_file)
        except OSError:
            shutil.copy2(src_file, dst_file)
            self.files_to_remove.append(src_file)

    def _remove_intermediary_files(self):
        """Remove any intermediary files created during the archival process"""
        for file in self.files_to_remove:
            try:
                os.unlink(file)
            except OSError as e:
                _logger.warning("Could not remove intermediary file %s: %s", file, e)
        self.files_to_remove.clear()

    def _fsync_contents(self, src_dir, dst_dir, dst_file):
        """
        Fsync the contents of source and destination directories and the
        destination file.

        :param str src_dir: the source directory path
        :param str dst_dir: the destination directory path
        :param str dst_file: the destination file path
        """
        # Fsync the destination file
        fsync_file(dst_file)
        # Fsync the source directory to ensure the removal of the original file
        fsync_dir(src_dir)
        # Fsync the target directory to ensure the presence of the new file
        fsync_dir(dst_dir)

    def save(self, compressor, encryption, wal_info, **kwargs):
        """
        Effectively persist a WAL file according to the configured destination.

        Compression and encryption are applied, if requested.

        :param compressor: the compressor for the file (if any)
        :param None|Encryption encryption: the encryptor for the file (if any)
        :param WalFileInfo wal_info: the WAL file is being processed
        :param kwargs: additional parameters for the storage strategy, if any
        :raises DuplicateWalFile: if the destination file exists and is
            different from the source file
        :raises MatchingDuplicateWalFile: if the destination file exists and is
            identical to the source file
        """
        src_file = wal_info.orig_filename
        src_dir = os.path.dirname(src_file)
        dst_file = wal_info.fullpath(self.server)
        dst_dir = os.path.dirname(dst_file)
        mkpath(dst_dir)

        current_file = src_file
        error = None
        try:
            self._run_pre_archive_scripts(wal_info, current_file)
            self._check_duplicate(current_file, dst_file, wal_info)

            if compressor and not wal_info.compression:
                current_file = self._compress_file(
                    compressor, current_file, dst_dir, wal_info
                )
            if encryption:
                current_file = self._encrypt_file(
                    encryption, current_file, dst_dir, wal_info
                )

            # Update stats, in case compression/encryption changed the current file
            if src_file != current_file:
                self._copy_stats(src_file, current_file, wal_info)

            self._rename_or_copy_file(current_file, dst_file)
            self._remove_intermediary_files()
            self._fsync_contents(src_dir, dst_dir, dst_file)
            # At this point the original file has been removed
            wal_info.orig_filename = None

        except Exception as e:
            error = e
            raise

        finally:
            self._run_post_archive_scripts(wal_info, dst_file, error)

    def delete(self, wals_to_delete):
        wals_deleted = []
        for wal_dir, wal_list in wals_to_delete.items():
            delete_directory = False
            # Each directory can contain up to 256 WAL files. If the deletion list
            # contains 256 entries, the entire directory can be safely deleted
            # Otherwise, check if all WALs in the directory are in the deletion list
            if len(wal_list) >= 256:
                delete_directory = True
            else:
                wal_names_to_delete = {wal_info.name for wal_info in wal_list}
                wal_names_in_dir = os.listdir(wal_dir)
                if set(wal_names_in_dir).issubset(wal_names_to_delete):
                    delete_directory = True
            # If the directory can be deleted, run the hook-scripts on each WAL file
            # before and after the rmtree. Otherwise, delete each WAL individually
            if delete_directory:
                self._delete_wal_directory(wal_dir, wal_list)
                wals_deleted.extend(wal_info.name for wal_info in wal_list)
            else:
                for wal_info in wal_list:
                    self._delete_wal_file(wal_info)
                    wals_deleted.append(wal_info.name)

        return wals_deleted

    def _delete_wal_directory(self, wal_dir, wal_list):
        for wal_info in wal_list:
            self._run_pre_delete_wal_scripts(wal_info)
        shutil.rmtree(wal_dir)
        for wal_info in wal_list:
            self._run_post_delete_wal_scripts(wal_info)

    def _delete_wal_file(self, wal_info):
        """
        Perform the actual deletion of the WAL file from local storage.

        :param WalFileInfo wal_info: the WAL file info object
        """
        self._run_pre_delete_wal_scripts(wal_info)
        error = None
        try:
            os.unlink(wal_info.fullpath(self.server))
            try:
                os.removedirs(os.path.dirname(wal_info.fullpath(self.server)))
            except OSError:
                # This is not an error condition
                # We always try to remove the trailing directories,
                # this means that hashdir is not empty.
                pass
        except OSError as e:
            error = "Ignoring deletion of WAL file %s for server %s: %s" % (
                wal_info.name,
                self.config.name,
                e,
            )
            output.warning(error)

        self._run_post_delete_wal_scripts(wal_info, error)

    def exists(self, wal_full_path):
        return os.path.exists(wal_full_path)

    def get_full_path(self, wal_name):
        # Build the path which contains the file
        hash_dir = os.path.join(self.config.wals_directory, xlog.hash_dir(wal_name))
        # Build the WAL file full path
        full_path = os.path.join(hash_dir, wal_name)
        return full_path


class CloudWalStorageStrategy(WalStorageStrategy):
    """
    WAL storage strategy for cloud storage.
    """

    def __init__(self, backup_manager, server):
        super(CloudWalStorageStrategy, self).__init__(backup_manager, server)
        self.cloud_interface = self.server.get_wal_cloud_interface()

    def _get_compression_extension(self, compression):
        """
        Return the file extension for the given compression algorithm.

        The *compression* value must be either ``None`` or a key present in
        :data:`~barman.compression.compression_registry` whose class defines an
        ``EXTENSION`` attribute. Passing an unrecognised algorithm or one without
        ``EXTENSION`` (e.g. ``pigz``, ``custom``) will raise ``KeyError`` or
        ``AttributeError`` respectively.

        :param str|None compression: the compression algorithm name
        :return: the file extension (e.g. ".gz") or "" if no compression
        :rtype: str
        """
        if not compression:
            return ""
        return compression_registry[compression].EXTENSION

    def _build_wal_object_key(self, wal_name, compression):
        """
        Build the full cloud object key for a WAL file.

        :param str wal_name: the WAL file name (e.g. ``000000010000000000000001``)
        :param str|None compression: the compression algorithm name used for the WAL
        :return: the full cloud object key, including compression extension if applicable
        :rtype: str
        """
        ext = self._get_compression_extension(compression)
        return (
            os.path.join(
                self.cloud_interface.path,
                self.config.name,
                "wals",
                xlog.hash_dir(wal_name),
                wal_name,
            )
            + ext
        )

    def _check_duplicate(self, wal_info, object_key):
        """
        Check if the cloud object *object_key* is identical to the source file.

        If the WAL was compressed, the ``decompress`` parameter of ``download_file``
        is used to decompress the cloud object before comparison.

        :param WalFileInfo wal_info: the WAL file info object
        :param str object_key: the cloud storage object key
        :raise DuplicateWalFile: if the destination file exists and is
            different from the source file
        :raise MatchingDuplicateWalFile: if the destination file exists and is
            identical to the source file
        """
        with NamedTemporaryFile(delete=True) as downloaded_file:
            self.cloud_interface.download_file(
                object_key, downloaded_file.name, decompress=wal_info.compression
            )
            if filecmp.cmp(wal_info.orig_filename, downloaded_file.name):
                raise MatchingDuplicateWalFile(wal_info)
            else:
                raise DuplicateWalFile(wal_info)

    def save(self, compressor, encryption, wal_info, **kwargs):
        """
        Effectively persist a WAL file according to the configured destination.

        If a *compressor* is provided, the WAL file is compressed in-memory before
        upload and the cloud key includes the compression extension. The *wal_info*
        object is updated with the compression type and compressed size so that
        xlogdb records accurate metadata.

        :param compressor: an :class:`~barman.compression.InternalCompressor` instance,
            or ``None`` if no compression is desired
        :param None|Encryption encryption: the encryptor for the file (if any)
        :param WalFileInfo wal_info: the WAL file is being processed
        :param kwargs: additional parameters for the storage strategy, if any:

            * "skip_delete": if ``True``, the source file will not be deleted after a
              successful upload.

        :raises CompressionException: if *compressor* is not an
            :class:`~barman.compression.InternalCompressor` instance

        .. note::
            Only ``InternalCompressor`` subclasses (gzip, bzip2, xz, zstd, lz4, snappy)
            are supported for cloud WAL storage. ``pigz`` and ``custom`` are not
            supported because they rely on external processes and cannot compress
            in-memory.

        .. note::
            Encryption is not yet supported for cloud WAL storage. The *encryption*
            parameter is kept for interface compatibility.
        """
        if compressor is not None and not isinstance(compressor, InternalCompressor):
            raise CompressionException(
                "Cloud WAL storage only supports in-memory compression algorithms "
                "(gzip, bzip2, xz, zstd, lz4, snappy). "
                "pigz and custom compressors are not supported."
            )
        error = None
        try:
            self._run_pre_archive_scripts(wal_info, wal_info.orig_filename)
            # Compress if a compressor is provided.
            upload_fileobj = None
            if compressor:
                # The source file is read and closed before uploading so we don't hold
                # and/or leak the fd.
                with open(wal_info.orig_filename, "rb") as fileobj:
                    upload_fileobj = compressor.compress_in_mem(fileobj)
                wal_info.compression = compressor.compression
                wal_info.size = upload_fileobj.getbuffer().nbytes
            else:
                upload_fileobj = open(wal_info.orig_filename, "rb")

            with upload_fileobj:
                key = self._build_wal_object_key(wal_info.name, wal_info.compression)
                try:
                    self.cloud_interface.upload_fileobj(
                        fileobj=upload_fileobj, key=key, fail_if_exists=True
                    )
                except ObjectKeyAlreadyExists:
                    self._check_duplicate(wal_info, key)
        except Exception as e:
            error = e
            raise
        finally:
            self._run_post_archive_scripts(wal_info, wal_info.orig_filename, error)

        if not kwargs.get("skip_delete", False):
            os.unlink(wal_info.orig_filename)
            wal_info.orig_filename = None

    def delete(self, wals_to_delete):
        wal_objects_to_delete = []
        for _, wal_list in wals_to_delete.items():
            for wal_info in wal_list:
                object_key = self._build_wal_object_key(
                    wal_info.name, wal_info.compression
                )
                wal_objects_to_delete.append(object_key)
                self._run_pre_delete_wal_scripts(wal_info)

        # Remove WALs without checking locks. Since base backup is already gone,
        # the WALs no longer have value, and it's not worth the overhead of checking
        # their lock status.
        self.cloud_interface.delete_objects(wal_objects_to_delete)

        wals_deleted = []
        for _, wal_list in wals_to_delete.items():
            for wal_info in wal_list:
                self._run_post_delete_wal_scripts(wal_info)
                wals_deleted.append(wal_info.name)

        return wals_deleted

    def exists(self, wal_full_path):
        return self.cloud_interface.check_object_existence(wal_full_path)

    def get_full_path(self, wal_name):
        """
        Construct the full cloud object key for a given WAL file name.

        .. note::
            This method uses the current compression configuration to determine
            the file extension. If the compression config has changed since the WAL
            was originally stored, this method will return a path that does not match
            the actual cloud object. Callers that need to handle WALs stored under a
            previous compression config should use bucket listing instead.

        :param str wal_name: the WAL file name
        :return: the full cloud object key
        :rtype: str
        """
        return self._build_wal_object_key(wal_name, self.config.compression)


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
        if self.server.use_wal_cloud_storage:
            self.wal_storage = CloudWalStorageStrategy(self.backup_manager, self.server)
        else:
            self.wal_storage = LocalWalStorageStrategy(self.backup_manager, self.server)
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
        if batch.run_size:
            if batch.total_size > batch.run_size:
                # Batch mode enabled
                _logger.info(
                    "Found %s xlog segments from %s for %s."
                    " Archive a batch of %s segments in this run.",
                    batch.total_size,
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
                    batch.total_size,
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
                with self.server.xlogdb("a") as fxlogdb:
                    self.wal_storage.save(compressor, encryption, wal_info)
                    fxlogdb.write(wal_info.to_xlogdb_line())
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
            if batch.total_size > batch.run_size:
                _logger.debug(
                    "Batch size reached (%s) - Exit %s process for %s",
                    batch.batch_size,
                    self.name,
                    self.config.name,
                )

            _logger.debug(
                "Archived %s out of %s xlog segments from %s for %s",
                processed,
                batch.total_size,
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
        total_size = len(file_names)
        # If batch size is set, limit the number of files to the batch size. The idea
        # is to avoid the overhead of creating several unused WalFileInfo objects. See
        # the note in the WalArchiverQueue class.
        if batch_size > 0:
            file_names = file_names[:batch_size]

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
                # We don't try to guess if the WAL is encrypted here. If the user sets
                # an archive_command which encrypts the WAL file, it's up to the user to
                # decrypt them later, and Barman won't do anything about it. We still
                # attempt to guess compression, though, because that's been the behavior
                # of Barman for a long time.
                encryption=None,
            )
            for f in files
        ]
        return WalArchiverQueue(
            wal_files, batch_size=batch_size, errors=errors, total_size=total_size
        )

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
        total_size = len(file_names)
        # If batch size is set, limit the number of files to the batch size. The idea
        # is to avoid the overhead of creating several unused WalFileInfo objects. See
        # the note in the WalArchiverQueue class.
        if batch_size > 0:
            file_names = file_names[:batch_size]

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
                # WAL files received through pg_receivewal are surely not encrypted nor
                # compressed, so we avoid the overhead of trying to guess such
                # algorithms.
                compression=None,
                encryption=None,
            )
            for f in files
        ]
        return WalArchiverQueue(
            wal_files,
            batch_size=batch_size,
            errors=errors,
            skip=skip,
            total_size=total_size,
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


class WalPrefetchWorker(multiprocessing.Process):
    """
    Custom worker class used to prefetch WAL files in parallel.

    It is a simple wrapper around :class:`multiprocessing.Process` that holds the WAL info
    to archive and provides a ``success`` property to check if the process was
    successful after finished.
    """

    def __init__(self, wal_info, *args, **kwargs):
        super(WalPrefetchWorker, self).__init__(*args, **kwargs)
        self.wal_info = wal_info

    @property
    def success(self):
        return self.exitcode == 0


class CloudWalArchiver:
    """
    WAL archiver for cloud storage destinations.

    This archiver uploads WAL files directly to cloud storage rather than storing them
    on the local filesystem. It supports parallel prefetching of additional WAL files
    that are ready for archival, improving throughput when Postgres ``archive_command``
    invokes this archiver.

    Unlike other archivers like :class:`FileWalArchiver` and :class:`StreamingWalArchiver`,
    which archive WALs from ``incoming`` and ``streaming`` directories on the Barman,
    host, this class is used to archive WALs living on the Postgres server itself i.e.
    WALs on the ``pg_wal`` directory.

    :cvar LAST_ARCHIVED_CACHE_FILE: Name of the cache file that stores the last
        archived WAL name.
    """

    LAST_ARCHIVED_CACHE_FILE = "cloud-wal-last-archived"

    def __init__(self, backup_manager):
        """
        Initialize the cloud WAL archiver.

        :param barman.backup_manager.BackupManager backup_manager: The backup manager
           of the server in use.
        """
        self.backup_manager = backup_manager
        self.config = backup_manager.config
        self.server = backup_manager.server
        self.wal_storage = self.server.wal_storage
        self.compression_manager = backup_manager.compression_manager
        self.encryption_manager = backup_manager.encryption_manager

    def archive(self, wal_path, parallel=0):
        """
        Archive a WAL file to cloud storage.

        When *parallel* > 1, the WAL files that immediately follow the requested one
        in the WAL sequence are checked for readiness (via ``.ready`` files in
        ``pg_wal/archive_status``). Those that are ready are eligible for prefetching.
        Up to ``parallel - 1`` extra WAL files are prefetched and uploaded concurrently
        in background worker processes.

        The requested WAL is always archived first. Prefetch workers are only started
        after the primary WAL has been successfully archived. ``xlogdb`` is updated for
        each successfully archived WAL. The last-archived cache is also updated at the
        end with the last WAL written to ``xlog.db`` as to enable skipping
        already-archived files on subsequent invocations.

        :param str wal_path: Full path to the WAL file to archive, as requested by
            Postgres's ``archive_command``.
        :param int parallel: Total number of WAL files to archive in parallel.
            ``0`` or ``1`` disables prefetching (only the requested WAL is archived).
            When ``> 1``, up to ``parallel - 1`` extra WALs are prefetched and
            archived concurrently.
        """
        if self._is_already_archived(wal_path):
            _logger.debug(
                "WAL file %s is already archived, skipping.",
                os.path.basename(wal_path),
            )
            return

        wal_info = self._build_wal_info(wal_path)

        # Infer xlog_segment_size from the WAL file size. The archive_command is
        # always invoked on a complete segment, so the file size equals the segment
        # size exactly — no postgres connection required.
        xlog_segment_size = os.path.getsize(wal_path)

        # Derive prefetch count from the single `parallel` parameter.
        # parallel=0 or 1 → no prefetching; parallel=N → N-1 prefetched WALs,
        # matching the semantics of cloud-wal-restore --parallel.
        n_prefetch = max(0, parallel - 1)

        # Discover prefetch candidates using WAL sequence ordering: compute the next
        # N segments after the requested one and check for .ready markers, stopping
        # at the first gap in the sequence.
        wals_to_prefetch = self._get_wals_to_prefetch(
            wal_path, n_prefetch, xlog_segment_size
        )
        actual_prefetch = min(n_prefetch, len(wals_to_prefetch))
        if actual_prefetch > 0:
            _logger.debug(
                "Prefetching %d WAL file(s) concurrently on server %s",
                actual_prefetch,
                self.config.name,
            )

        # Archive the requested WAL in the main process first.
        # Prefetch workers are only started after the primary WAL is successfully
        # archived — if it fails, no worker processes are spawned.
        try:
            self._archive_single_wal(wal_info)
        except Exception as e:
            output.error("Archival of WAL file %s failed: %s" % (wal_path, e))
            raise

        # Start a worker process for each WAL to prefetch, then wait for all of them.
        # The list is already capped to `n_prefetch` items by _get_wals_to_prefetch,
        # so no batching is needed.
        prefetch_workers = []
        for prefetch_wal_path in wals_to_prefetch:
            try:
                prefetch_wal_info = self._build_wal_info(prefetch_wal_path)
            except Exception as e:
                output.warning(
                    "Could not read metadata for WAL file %s: %s. Stopping prefetch.",
                    os.path.basename(prefetch_wal_path),
                    e,
                )
                break
            worker = WalPrefetchWorker(
                wal_info=prefetch_wal_info,
                target=self._prefetch_worker,
                args=(prefetch_wal_info,),
            )
            worker.start()
            prefetch_workers.append(worker)
        for worker in prefetch_workers:
            worker.join()

        # Update xlogdb and the last-archived cache based on the results of the archivals
        self._update_metadata(wal_info, prefetch_workers)

    def _is_already_archived(self, wal_path):
        """
        Check if a WAL file has already been archived based on the last-archived cache.

        Compares the WAL name against the cached last-archived WAL. Since WAL file
        names are lexicographically ordered by LSN, any WAL name <= the cached name
        has already been archived.

        :param str wal_path: Full path to the WAL file.
        :return: ``True`` if the WAL was already archived, ``False`` otherwise.
        :rtype: bool
        """
        wal_name = os.path.basename(wal_path)
        cached_wal = self._read_cloud_wal_last_archived()
        return bool(
            cached_wal and xlog.is_wal_file(wal_name) and wal_name <= cached_wal
        )

    def _get_wals_to_prefetch(self, requested_wal_path, number, xlog_segment_size):
        """
        Get the next WAL files in sequence that are ready for prefetched archival.

        Computes the names of the next *number* WAL files after *requested_wal_path*
        in the WAL sequence, then checks each for a corresponding ``.ready`` marker
        in ``pg_wal/archive_status``. Discovery stops at the first WAL in the sequence
        that does not have a ``.ready`` file — a gap in readiness means later WALs are
        unlikely to be ready either, and Postgres will request them soon anyway.

        Only actual WAL segment files are returned; ``.history`` and ``.backup`` files
        are not prefetched.

        :param str requested_wal_path: Full path to the WAL file being archived in the
            main process i.e. the WAL file requested by Postgres's ``archive_command``.
        :param int number: Maximum number of WAL files to return for prefetching.
        :param int xlog_segment_size: The WAL segment size in bytes, used to compute
            correct segment boundaries when generating the sequence.
        :return: List of WAL file paths ready for prefetching, in sequence order, up to
            *number* entries.
        :rtype: list[str]
        """
        # .history and .backup files are archived normally but do not trigger prefetch
        if not xlog.is_wal_file(requested_wal_path):
            return []

        if number <= 0:
            return []

        pg_wal_dir = os.path.dirname(requested_wal_path)
        archive_status_dir = os.path.join(pg_wal_dir, "archive_status")
        if not os.path.isdir(archive_status_dir):
            output.warning(
                "archive_status directory %s does not exist; prefetching skipped.",
                archive_status_dir,
            )
            return []

        requested_wal_name = os.path.basename(requested_wal_path)

        # Generate WAL names in sequence starting from the requested WAL.
        # The first name returned is the requested WAL itself, so we skip it.
        sequence = xlog.generate_segment_names(
            begin=requested_wal_name,
            xlog_segment_size=xlog_segment_size,
        )
        next(sequence)  # skip the requested WAL

        wals_to_prefetch = []
        for wal_name in sequence:
            if len(wals_to_prefetch) >= number:
                break

            ready_file = os.path.join(archive_status_dir, "%s.ready" % wal_name)
            if not os.path.exists(ready_file):
                # Stop at the first gap — later WALs in the sequence will not be
                # ready either, and Postgres will request them in order soon.
                _logger.debug(
                    "WAL %s is not ready for archival yet; stopping prefetch on"
                    " server %s.",
                    wal_name,
                    self.config.name,
                )
                break

            wal_path = os.path.join(pg_wal_dir, wal_name)
            if not os.path.exists(wal_path):
                # The WAL file was recycled by Postgres between the time .ready was
                # created and our scan.  Skip this candidate and continue — the file
                # is no longer available but the sequence may still continue.
                _logger.debug(
                    "WAL %s has a .ready marker but the file no longer exists"
                    " (recycled); skipping.",
                    wal_name,
                )
                continue

            _logger.debug(
                "Found WAL file %s ready for prefetch archival on server %s.",
                wal_name,
                self.config.name,
            )
            wals_to_prefetch.append(wal_path)

        return wals_to_prefetch

    def _build_wal_info(self, wal_path):
        """
        Create a WalFileInfo object for a given *wal_path*.

        :param str wal_path: Full path to the WAL file.
        :return: A WalFileInfo instance populated with metadata from the file.
        :rtype: barman.infofile.WalFileInfo
        """
        # The WAL file comes directly from pg_wal, so it is expected to be plain
        # (no compression or encryption applied by Postgres).
        return WalFileInfo.from_file(
            filename=wal_path,
            compression_manager=self.compression_manager,
            unidentified_compression=None,
            encryption_manager=self.encryption_manager,
            encryption=None,
        )

    def _prefetch_worker(self, wal_info):
        """
        Worker function that archives a WAL file in a subprocess.

        Invoked as the target of a :class:`WalPrefetchWorker` process. Archives
        the WAL file and exits with code 0 on success or 1 on failure. Exceptions
        are logged but not propagated, since the parent process checks success via
        the worker's exit code.

        :param barman.infofile.WalFileInfo wal_info: Metadata for the WAL file to
            archive.
        """
        try:
            self._archive_single_wal(wal_info)
        except Exception as e:
            output.warning("Prefetch of WAL file %s failed: %s" % (wal_info.name, e))
            sys.exit(1)

    def _archive_single_wal(self, wal_info):
        """
        Archive/Upload a single WAL file to cloud storage.

        Compresses and uploads the WAL file using the configured storage strategy.
        Handles duplicate detection: if an identical file already exists in cloud
        storage, the upload is silently skipped; if a different file exists with
        the same name, the local file is copied to the errors directory.

        :param barman.infofile.WalFileInfo wal_info: Metadata for the WAL file to
            archive.
        """
        compressor = self.compression_manager.get_default_compressor()
        encryption = self.encryption_manager.get_encryption()
        try:
            # We skip the delete because the WAL file is expected to be inside 'pg_wal',
            # not inside the 'incoming' directory.
            self.wal_storage.save(compressor, encryption, wal_info, skip_delete=True)
        except MatchingDuplicateWalFile:
            _logger.info(
                "WAL file %s is already archived in cloud storage, skipping.",
                wal_info.name,
            )
        except DuplicateWalFile:
            _logger.warning(
                "WAL file %s is already archived in cloud storage of server %s but "
                "with different content. Copying it to the errors directory.",
                wal_info.name,
                self.config.name,
            )
            # We copy instead of moving the WAL file to the errors directory because
            # this is a WAL inside 'pg_wal' and should not be modified by Barman.
            self._copy_wal_file_to_errors_directory(
                wal_info.orig_filename, wal_info.name, "duplicate"
            )

    def _copy_wal_file_to_errors_directory(self, src, file_name, suffix):
        """
        Copy a problematic WAL file to the errors directory.

        Unlike :meth:`Server.move_wal_file_to_errors_directory`, this method copies
        rather than moves the file, preserving the original. This is necessary when
        archiving directly from ``pg_wal``, since PostgreSQL owns those files and
        Barman should not remove them.

        :param str src: Full path to the source WAL file.
        :param str file_name: Base name of the WAL file (used for the destination name).
        :param str suffix: Suffix to append to the destination file name (e.g.,
            ``"duplicate"`` or ``"unknown"``).
        """
        error_dst = self.server.get_errors_dst(file_name, suffix)
        try:
            shutil.copy(src, error_dst)
        except IOError as e:
            if e.errno == errno.ENOENT:
                _logger.warning("%s not found" % src)

    def _update_metadata(self, wal_info, prefetch_workers):
        """
        Update archival metadata on ``xlogdb`` and update the last-archived cache.

        Appends a line to ``xlogdb`` for each successfully archived WAL (in order).
        The last WAL written to ``xlogdb`` is also written to the last-archived cache
        file.

        It stops writing to ``xlogdb`` at the first failure found, as to avoid having
        any holes in ``xlogdb``. E.g. if WAL files A, B, C and D are for archival, but
        C fails to be archived, only A and B are written to ``xlogdb`` (even though D
        might have been successfully archived). This maintains the last-archived cache
        consistent. If we were to also write D to ``xlogdb``, the last-archived cache
        would be updated with D, and thus the next attempt of C by Postgres would
        mistakenly consider it as already archived.

        :param barman.infofile.WalFileInfo wal_info: Metadata for the main WAL file.
        :param list[WalPrefetchWorker] prefetch_workers: List of prefetch workers,
            in the order their WAL files should appear in xlogdb.
        """
        with self.server.xlogdb("a") as fxlogdb:
            fxlogdb.write(wal_info.to_xlogdb_line())
            last_write_xlogdb = wal_info.name
            for worker in prefetch_workers:
                if worker.success:
                    fxlogdb.write(worker.wal_info.to_xlogdb_line())
                    last_write_xlogdb = worker.wal_info.name
                else:
                    break

        # Only update the last-archived cache for WAL segment files.
        # A .history from a new timeline (e.g. "00000003.history") sorts before a
        # WAL segment of the previous timeline (e.g. "000000020000000000000001"),
        # because '.' (0x2e) < '0' (0x30). If we cached the .history name, a
        # delayed old-timeline WAL would satisfy `wal_name <= cached` and be
        # wrongly skipped as already-archived. We also don't want to cache .backup or
        # .partial files, just in case.
        if xlog.is_wal_file(last_write_xlogdb):
            self._write_cloud_wal_last_archived(last_write_xlogdb)

    @property
    def last_archived_cache_path(self):
        """
        Return the full path to the cache file that stores the last archived WAL name.

        :return: Absolute path to the cache file.
        :rtype: str
        """
        return os.path.join(self.server.meta_directory, self.LAST_ARCHIVED_CACHE_FILE)

    def _read_cloud_wal_last_archived(self):
        """
        Read the name of the last archived WAL file from the cache file.

        :return: The name of the last archived WAL file, or ``None`` if not available.
        :rtype: str|None
        """
        try:
            with open(self.last_archived_cache_path, "r") as f:
                return f.read().strip()
        except (OSError, IOError) as e:
            _logger.debug(
                "Could not read last-archived cache at %s: %s",
                self.last_archived_cache_path,
                e,
            )
            return None

    def _write_cloud_wal_last_archived(self, wal_name):
        """
        Write the name of the last archived WAL file to the cache file atomically.

        Uses a write-to-temporary-file-then-rename pattern to ensure the cache file is
        never partially written. An incomplete write followed by a crash would leave a
        corrupt cache file that could cause future WALs to be incorrectly skipped.

        :param str wal_name: The name of the last archived WAL file.
        """
        tmp_path = None
        try:
            cache_dir = os.path.dirname(self.last_archived_cache_path)
            with NamedTemporaryFile(mode="w", dir=cache_dir, delete=False) as tmp:
                tmp_path = tmp.name
                tmp.write(wal_name)
            os.rename(tmp_path, self.last_archived_cache_path)
        except (OSError, IOError) as e:
            if tmp_path is not None:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
            _logger.warning(
                "Failed to write last archived WAL file name to cache file %s: %s",
                self.last_archived_cache_path,
                e,
            )
