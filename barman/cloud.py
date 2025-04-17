# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2025
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

import collections
import copy
import datetime
import errno
import json
import logging
import multiprocessing
import operator
import os
import shutil
import signal
import tarfile
import time
from abc import ABCMeta, abstractmethod, abstractproperty
from functools import partial
from io import BytesIO, RawIOBase
from tempfile import NamedTemporaryFile

from barman import xlog
from barman.annotations import KeepManagerMixinCloud
from barman.backup_executor import ConcurrentBackupStrategy, SnapshotBackupExecutor
from barman.clients import cloud_compression
from barman.clients.cloud_cli import get_missing_attrs
from barman.exceptions import (
    BackupException,
    BackupPreconditionException,
    BarmanException,
    ConfigurationException,
)
from barman.fs import UnixLocalCommand, path_allowed
from barman.infofile import BackupInfo, WalFileInfo
from barman.postgres_plumbing import EXCLUDE_LIST, PGDATA_EXCLUDE_LIST
from barman.utils import (
    BarmanEncoder,
    force_str,
    get_backup_info_from_name,
    human_readable_timedelta,
    is_backup_id,
    pretty_size,
    range_fun,
    total_seconds,
    with_metaclass,
)

try:
    # Python 3.x
    from queue import Empty as EmptyQueue
except ImportError:
    # Python 2.x
    from Queue import Empty as EmptyQueue


BUFSIZE = 16 * 1024
LOGGING_FORMAT = "%(asctime)s [%(process)s] %(levelname)s: %(message)s"

# Allowed compression algorithms
ALLOWED_COMPRESSIONS = {
    ".gz": "gzip",
    ".bz2": "bzip2",
    ".xz": "xz",
    ".snappy": "snappy",
    ".zst": "zstd",
    ".lz4": "lz4",
}

DEFAULT_DELIMITER = "/"


def configure_logging(config):
    """
    Get a nicer output from the Python logging package
    """
    verbosity = config.verbose - config.quiet
    log_level = max(logging.WARNING - verbosity * 10, logging.DEBUG)
    logging.basicConfig(format=LOGGING_FORMAT, level=log_level)


def copyfileobj_pad_truncate(src, dst, length=None):
    """
    Copy length bytes from fileobj src to fileobj dst.
    If length is None, copy the entire content.
    This method is used by the TarFileIgnoringTruncate.addfile().
    """
    if length == 0:
        return

    if length is None:
        shutil.copyfileobj(src, dst, BUFSIZE)
        return

    blocks, remainder = divmod(length, BUFSIZE)
    for _ in range(blocks):
        buf = src.read(BUFSIZE)
        dst.write(buf)
        if len(buf) < BUFSIZE:
            # End of file reached
            # The file must have  been truncated, so pad with zeroes
            dst.write(tarfile.NUL * (BUFSIZE - len(buf)))

    if remainder != 0:
        buf = src.read(remainder)
        dst.write(buf)
        if len(buf) < remainder:
            # End of file reached
            # The file must have  been truncated, so pad with zeroes
            dst.write(tarfile.NUL * (remainder - len(buf)))


class CloudProviderError(BarmanException):
    """
    This exception is raised when we get an error in the response from the
    cloud provider
    """


class CloudUploadingError(BarmanException):
    """
    This exception is raised when there are upload errors
    """


class TarFileIgnoringTruncate(tarfile.TarFile):
    """
    Custom TarFile class that ignore truncated or vanished files.
    """

    format = tarfile.PAX_FORMAT  # Use PAX format to better preserve metadata

    def addfile(self, tarinfo, fileobj=None):
        """
        Add the provided fileobj to the tar ignoring truncated or vanished
        files.

        This method completely replaces TarFile.addfile()
        """
        self._check("awx")

        tarinfo = copy.copy(tarinfo)

        buf = tarinfo.tobuf(self.format, self.encoding, self.errors)
        self.fileobj.write(buf)
        self.offset += len(buf)

        # If there's data to follow, append it.
        if fileobj is not None:
            copyfileobj_pad_truncate(fileobj, self.fileobj, tarinfo.size)
            blocks, remainder = divmod(tarinfo.size, tarfile.BLOCKSIZE)
            if remainder > 0:
                self.fileobj.write(tarfile.NUL * (tarfile.BLOCKSIZE - remainder))
                blocks += 1
            self.offset += blocks * tarfile.BLOCKSIZE

        self.members.append(tarinfo)


class CloudTarUploader(object):
    # This is the method we use to create new buffers
    # We use named temporary files, so we can pass them by name to
    # other processes
    _buffer = partial(
        NamedTemporaryFile, delete=False, prefix="barman-upload-", suffix=".part"
    )

    def __init__(
        self, cloud_interface, key, chunk_size, compression=None, max_bandwidth=None
    ):
        """
        A tar archive that resides on cloud storage

        :param CloudInterface cloud_interface: cloud interface instance
        :param str key: path inside the bucket
        :param str compression: required compression
        :param int chunk_size: the upload chunk size
        :param int max_bandwidth: the maximum amount of data per second that
          should be uploaded by this tar uploader
        """
        self.cloud_interface = cloud_interface
        self.key = key
        self.chunk_size = chunk_size
        self.max_bandwidth = max_bandwidth
        self.upload_metadata = None
        self.buffer = None
        self.counter = 0
        self.compressor = None
        # Some supported compressions (e.g. snappy) require CloudTarUploader to apply
        # compression manually rather than relying on the tar file.
        self.compressor = cloud_compression.get_compressor(compression)
        # If the compression is supported by tar then it will be added to the filemode
        # passed to tar_mode.
        tar_mode = cloud_compression.get_streaming_tar_mode("w", compression)
        # The value of 65536 for the chunk size is based on comments in the python-snappy
        # library which suggest it should be good for almost every scenario.
        # See: https://github.com/andrix/python-snappy/blob/0.6.0/snappy/snappy.py#L282
        self.tar = TarFileIgnoringTruncate.open(
            fileobj=self, mode=tar_mode, bufsize=64 << 10
        )
        self.size = 0
        self.stats = None
        self.time_of_last_upload = None
        self.size_of_last_upload = None

    def write(self, buf):
        if self.buffer and self.buffer.tell() > self.chunk_size:
            self.flush()
        if not self.buffer:
            self.buffer = self._buffer()
        if self.compressor:
            # If we have a custom compressor we must use it here
            compressed_buf = self.compressor.add_chunk(buf)
            self.buffer.write(compressed_buf)
            self.size += len(compressed_buf)
        else:
            # If there is no custom compressor then we are either not using
            # compression or tar has already compressed it - in either case we
            # just write the data to the buffer
            self.buffer.write(buf)
            self.size += len(buf)

    def _throttle_upload(self, part_size):
        """
        Throttles the upload according to the value of `self.max_bandwidth`.

        Waits until enough time has passed since the last upload that a new part can
        be uploaded without exceeding `self.max_bandwidth`. If sufficient time has
        already passed then this function will return without waiting.

        :param int part_size: Size in bytes of the part which is to be uplaoded.
        """
        if (self.time_of_last_upload and self.size_of_last_upload) is not None:
            min_time_to_next_upload = self.size_of_last_upload / self.max_bandwidth
            seconds_since_last_upload = (
                datetime.datetime.now() - self.time_of_last_upload
            ).total_seconds()
            if seconds_since_last_upload < min_time_to_next_upload:
                logging.info(
                    f"Uploaded {self.size_of_last_upload} bytes "
                    f"{seconds_since_last_upload} seconds ago which exceeds "
                    f"limit of {self.max_bandwidth} bytes/s"
                )
                time_to_wait = min_time_to_next_upload - seconds_since_last_upload
                logging.info(f"Throttling upload by waiting for {time_to_wait} seconds")
                time.sleep(time_to_wait)
        self.time_of_last_upload = datetime.datetime.now()
        self.size_of_last_upload = part_size

    def flush(self):
        if not self.upload_metadata:
            self.upload_metadata = self.cloud_interface.create_multipart_upload(
                self.key
            )

        part_size = self.buffer.tell()
        self.buffer.flush()
        self.buffer.seek(0, os.SEEK_SET)
        self.counter += 1
        if self.max_bandwidth:
            # Upload throttling is applied just before uploading the next part so that
            # compression and flushing have already happened before we start waiting.
            self._throttle_upload(part_size)
        self.cloud_interface.async_upload_part(
            upload_metadata=self.upload_metadata,
            key=self.key,
            body=self.buffer,
            part_number=self.counter,
        )
        self.buffer.close()
        self.buffer = None

    def close(self):
        if self.tar:
            self.tar.close()
        self.flush()
        self.cloud_interface.async_complete_multipart_upload(
            upload_metadata=self.upload_metadata,
            key=self.key,
            parts_count=self.counter,
        )
        self.stats = self.cloud_interface.wait_for_multipart_upload(self.key)


class CloudUploadController(object):
    def __init__(
        self,
        cloud_interface,
        key_prefix,
        max_archive_size,
        compression,
        min_chunk_size=None,
        max_bandwidth=None,
    ):
        """
        Create a new controller that upload the backup in cloud storage

        :param CloudInterface cloud_interface: cloud interface instance
        :param str|None key_prefix: path inside the bucket
        :param int max_archive_size: the maximum size of an archive
        :param str|None compression: required compression
        :param int|None min_chunk_size: the minimum size of a single upload part
        :param int|None max_bandwidth: the maximum amount of data per second that
          should be uploaded during the backup
        """

        self.cloud_interface = cloud_interface
        if key_prefix and key_prefix[0] == "/":
            key_prefix = key_prefix[1:]
        self.key_prefix = key_prefix
        if max_archive_size < self.cloud_interface.MAX_ARCHIVE_SIZE:
            self.max_archive_size = max_archive_size
        else:
            logging.warning(
                "max-archive-size too big. Capping it to to %s",
                pretty_size(self.cloud_interface.MAX_ARCHIVE_SIZE),
            )
            self.max_archive_size = self.cloud_interface.MAX_ARCHIVE_SIZE
        # We aim to a maximum of MAX_CHUNKS_PER_FILE / 2 chunks per file
        calculated_chunk_size = 2 * int(
            max_archive_size / self.cloud_interface.MAX_CHUNKS_PER_FILE
        )
        # Use whichever is higher - the calculated chunk_size, the requested
        # min_chunk_size or the cloud interface MIN_CHUNK_SIZE.
        possible_min_chunk_sizes = [
            calculated_chunk_size,
            cloud_interface.MIN_CHUNK_SIZE,
        ]
        if min_chunk_size is not None:
            possible_min_chunk_sizes.append(min_chunk_size)
        self.chunk_size = max(possible_min_chunk_sizes)
        self.compression = compression
        self.max_bandwidth = max_bandwidth
        self.tar_list = {}

        self.upload_stats = {}
        """Already finished uploads list"""

        self.copy_start_time = datetime.datetime.now()
        """Copy start time"""

        self.copy_end_time = None
        """Copy end time"""

    def _build_dest_name(self, name, count=0):
        """
        Get the destination tar name
        :param str name: the name prefix
        :param int count: the part count
        :rtype: str
        """
        components = [name]
        if count > 0:
            components.append("_%04d" % count)
        components.append(".tar")
        if self.compression == "gz":
            components.append(".gz")
        elif self.compression == "bz2":
            components.append(".bz2")
        elif self.compression == "snappy":
            components.append(".snappy")
        return "".join(components)

    def _get_tar(self, name):
        """
        Get a named tar file from cloud storage.
        Subsequent call with the same name return the same name
        :param str name: tar name
        :rtype: tarfile.TarFile
        """
        if name not in self.tar_list or not self.tar_list[name]:
            self.tar_list[name] = [
                CloudTarUploader(
                    cloud_interface=self.cloud_interface,
                    key=os.path.join(self.key_prefix, self._build_dest_name(name)),
                    chunk_size=self.chunk_size,
                    compression=self.compression,
                    max_bandwidth=self.max_bandwidth,
                )
            ]
        # If the current uploading file size is over DEFAULT_MAX_TAR_SIZE
        # Close the current file and open the next part
        uploader = self.tar_list[name][-1]
        if uploader.size > self.max_archive_size:
            uploader.close()
            uploader = CloudTarUploader(
                cloud_interface=self.cloud_interface,
                key=os.path.join(
                    self.key_prefix,
                    self._build_dest_name(name, len(self.tar_list[name])),
                ),
                chunk_size=self.chunk_size,
                compression=self.compression,
                max_bandwidth=self.max_bandwidth,
            )
            self.tar_list[name].append(uploader)
        return uploader.tar

    def upload_directory(self, label, src, dst, exclude=None, include=None):
        logging.info(
            "Uploading '%s' directory '%s' as '%s'",
            label,
            src,
            self._build_dest_name(dst),
        )
        for root, dirs, files in os.walk(src):
            tar_root = os.path.relpath(root, src)
            if not path_allowed(exclude, include, tar_root, True):
                continue
            try:
                self._get_tar(dst).add(root, arcname=tar_root, recursive=False)
            except EnvironmentError as e:
                if e.errno == errno.ENOENT:
                    # If a directory disappeared just skip it,
                    # WAL reply will take care during recovery.
                    continue
                else:
                    raise

            for item in files:
                tar_item = os.path.join(tar_root, item)
                if not path_allowed(exclude, include, tar_item, False):
                    continue
                logging.debug("Uploading %s", tar_item)
                try:
                    self._get_tar(dst).add(os.path.join(root, item), arcname=tar_item)
                except EnvironmentError as e:
                    if e.errno == errno.ENOENT:
                        # If a file disappeared just skip it,
                        # WAL reply will take care during recovery.
                        continue
                    else:
                        raise

    def add_file(self, label, src, dst, path, optional=False):
        if optional and not os.path.exists(src):
            return
        logging.info(
            "Uploading '%s' file from '%s' to '%s' with path '%s'",
            label,
            src,
            self._build_dest_name(dst),
            path,
        )
        tar = self._get_tar(dst)
        tar.add(src, arcname=path)

    def add_fileobj(self, label, fileobj, dst, path, mode=None, uid=None, gid=None):
        logging.info(
            "Uploading '%s' file to '%s' with path '%s'",
            label,
            self._build_dest_name(dst),
            path,
        )
        tar = self._get_tar(dst)
        tarinfo = tar.tarinfo(path)
        fileobj.seek(0, os.SEEK_END)
        tarinfo.size = fileobj.tell()
        if mode is not None:
            tarinfo.mode = mode
        if uid is not None:
            tarinfo.gid = uid
        if gid is not None:
            tarinfo.gid = gid
        fileobj.seek(0, os.SEEK_SET)
        tar.addfile(tarinfo, fileobj)

    def close(self):
        logging.info("Marking all the uploaded archives as 'completed'")
        for name in self.tar_list:
            if self.tar_list[name]:
                # Tho only opened file is the last one, all the others
                # have been already closed
                self.tar_list[name][-1].close()
                self.upload_stats[name] = [tar.stats for tar in self.tar_list[name]]
            self.tar_list[name] = None

        # Store the end time
        self.copy_end_time = datetime.datetime.now()

    def statistics(self):
        """
        Return statistics about the CloudUploadController object.

        :rtype: dict
        """
        logging.info("Calculating backup statistics")

        # This method can only run at the end of a non empty copy
        assert self.copy_end_time
        assert self.upload_stats

        # Initialise the result calculating the total runtime
        stat = {
            "total_time": total_seconds(self.copy_end_time - self.copy_start_time),
            "number_of_workers": self.cloud_interface.worker_processes_count,
            # Cloud uploads have no analysis
            "analysis_time": 0,
            "analysis_time_per_item": {},
            "copy_time_per_item": {},
            "serialized_copy_time_per_item": {},
        }

        # Calculate the time spent uploading
        upload_start = None
        upload_end = None
        serialized_time = datetime.timedelta(0)
        for name in self.upload_stats:
            name_start = None
            name_end = None
            total_time = datetime.timedelta(0)
            for index, data in enumerate(self.upload_stats[name]):
                logging.debug(
                    "Calculating statistics for file %s, index %s, data: %s",
                    name,
                    index,
                    json.dumps(data, indent=2, sort_keys=True, cls=BarmanEncoder),
                )
                if upload_start is None or upload_start > data["start_time"]:
                    upload_start = data["start_time"]
                if upload_end is None or upload_end < data["end_time"]:
                    upload_end = data["end_time"]
                if name_start is None or name_start > data["start_time"]:
                    name_start = data["start_time"]
                if name_end is None or name_end < data["end_time"]:
                    name_end = data["end_time"]
                parts = data["parts"]
                for num in parts:
                    part = parts[num]
                    total_time += part["end_time"] - part["start_time"]
                stat["serialized_copy_time_per_item"][name] = total_seconds(total_time)
                serialized_time += total_time
            # Cloud uploads have no analysis
            stat["analysis_time_per_item"][name] = 0
            stat["copy_time_per_item"][name] = total_seconds(name_end - name_start)

        # Store the total time spent by copying
        stat["copy_time"] = total_seconds(upload_end - upload_start)
        stat["serialized_copy_time"] = total_seconds(serialized_time)

        return stat


class FileUploadStatistics(dict):
    def __init__(self, *args, **kwargs):
        super(FileUploadStatistics, self).__init__(*args, **kwargs)
        start_time = datetime.datetime.now()
        self.setdefault("status", "uploading")
        self.setdefault("start_time", start_time)
        self.setdefault("parts", {})

    def set_part_end_time(self, part_number, end_time):
        part = self["parts"].setdefault(part_number, {"part_number": part_number})
        part["end_time"] = end_time

    def set_part_start_time(self, part_number, start_time):
        part = self["parts"].setdefault(part_number, {"part_number": part_number})
        part["start_time"] = start_time


class DecompressingStreamingIO(RawIOBase):
    """
    Provide an IOBase interface which decompresses streaming cloud responses.

    This is intended to wrap azure_blob_storage.StreamingBlobIO and
    aws_s3.StreamingBodyIO objects, transparently decompressing chunks while
    continuing to expose them via the read method of the IOBase interface.

    This allows TarFile to stream the uncompressed data directly from the cloud
    provider responses without requiring it to know anything about the compression.
    """

    # The value of 65536 for the chunk size is based on comments in the python-snappy
    # library which suggest it should be good for almost every scenario.
    # See: https://github.com/andrix/python-snappy/blob/0.6.0/snappy/snappy.py#L300
    COMPRESSED_CHUNK_SIZE = 65536

    def __init__(self, streaming_response, decompressor):
        """
        Create a new DecompressingStreamingIO object.

        A DecompressingStreamingIO object will be created which reads compressed
        bytes from streaming_response and decompresses them with the supplied
        decompressor.

        :param RawIOBase streaming_response: A file-like object which provides the
          data in the response streamed from the cloud provider.
        :param barman.clients.cloud_compression.ChunkedCompressor: A ChunkedCompressor
          object which provides a decompress(bytes) method to return the decompressed bytes.
        """
        self.streaming_response = streaming_response
        self.decompressor = decompressor
        self.buffer = bytes()

    def _read_from_uncompressed_buffer(self, n):
        """
        Read up to n bytes from the local buffer of uncompressed data.

        Removes up to n bytes from the local buffer and returns them. If n is
        greater than the length of the buffer then the entire buffer content is
        returned and the buffer is emptied.

        :param int n: The number of bytes to read
        :return: The bytes read from the local buffer
        :rtype: bytes
        """
        if n <= len(self.buffer):
            return_bytes = self.buffer[:n]
            self.buffer = self.buffer[n:]
            return return_bytes
        else:
            return_bytes = self.buffer
            self.buffer = bytes()
            return return_bytes

    def read(self, n=-1):
        """
        Read up to n bytes of uncompressed data from the wrapped IOBase.

        Bytes are initially read from the local buffer of uncompressed data. If more
        bytes are required then chunks of COMPRESSED_CHUNK_SIZE are read from the
        wrapped IOBase and decompressed in memory until >= n uncompressed bytes have
        been read. n bytes are then returned with any remaining bytes being stored
        in the local buffer for future requests.

        :param int n: The number of uncompressed bytes required
        :return: Up to n uncompressed bytes from the wrapped IOBase
        :rtype: bytes
        """
        uncompressed_bytes = self._read_from_uncompressed_buffer(n)
        if len(uncompressed_bytes) == n:
            return uncompressed_bytes

        while len(uncompressed_bytes) < n:
            compressed_bytes = self.streaming_response.read(self.COMPRESSED_CHUNK_SIZE)
            uncompressed_bytes += self.decompressor.decompress(compressed_bytes)
            if len(compressed_bytes) < self.COMPRESSED_CHUNK_SIZE:
                # If we got fewer bytes than we asked for then we're done
                break

        return_bytes = uncompressed_bytes[:n]
        self.buffer = uncompressed_bytes[n:]
        return return_bytes


class CloudInterface(with_metaclass(ABCMeta)):
    """
    Abstract base class which provides the interface between barman and cloud
    storage providers.

    Support for individual cloud providers should be implemented by inheriting
    from this class and providing implementations for the abstract methods.

    This class provides generic boilerplate for the asynchronous and parallel
    upload of objects to cloud providers which support multipart uploads.
    These uploads are carried out by worker processes which are spawned by
    _ensure_async and consume upload jobs from a queue. The public
    async_upload_part and async_complete_multipart_upload methods add jobs
    to this queue. When the worker processes consume the jobs they execute
    the synchronous counterparts to the async_* methods (_upload_part and
    _complete_multipart_upload) which must be implemented in CloudInterface
    sub-classes.

    Additional boilerplate for creating buckets and streaming objects as tar
    files is also provided.
    """

    @abstractproperty
    def MAX_CHUNKS_PER_FILE(self):
        """
        Maximum number of chunks allowed in a single file in cloud storage.
        The exact definition of chunk depends on the cloud provider, for example
        in AWS S3 a chunk would be one part in a multipart upload. In Azure a
        chunk would be a single block of a block blob.

        :type: int
        """
        pass

    @abstractproperty
    def MIN_CHUNK_SIZE(self):
        """
        Minimum size in bytes of a single chunk.

        :type: int
        """
        pass

    @abstractproperty
    def MAX_ARCHIVE_SIZE(self):
        """
        Maximum size in bytes of a single file in cloud storage.

        :type: int
        """
        pass

    @abstractproperty
    def MAX_DELETE_BATCH_SIZE(self):
        """
        The maximum number of objects which can be deleted in a single batch.

        :type: int
        """
        pass

    def __init__(self, url, jobs=2, tags=None, delete_batch_size=None):
        """
        Base constructor

        :param str url: url for the cloud storage resource
        :param int jobs: How many sub-processes to use for asynchronous
          uploading, defaults to 2.
        :param List[tuple] tags: List of tags as k,v tuples to be added to all
          uploaded objects
        :param int|None delete_batch_size: the maximum number of objects to be
          deleted in a single request
        """
        self.url = url
        self.tags = tags

        # We use the maximum allowed batch size by default.
        self.delete_batch_size = self.MAX_DELETE_BATCH_SIZE
        if delete_batch_size is not None:
            # If a specific batch size is requested we clamp it between 1 and the
            # maximum allowed batch size.
            self.delete_batch_size = max(
                1,
                min(delete_batch_size, self.MAX_DELETE_BATCH_SIZE),
            )

        # The worker process and the shared queue are created only when
        # needed
        self.queue = None
        self.result_queue = None
        self.errors_queue = None
        self.done_queue = None
        self.error = None
        self.abort_requested = False
        self.worker_processes_count = jobs
        self.worker_processes = []

        # The parts DB is a dictionary mapping each bucket key name to a list
        # of uploaded parts.
        # This structure is updated by the _refresh_parts_db method call
        self.parts_db = collections.defaultdict(list)

        # Statistics about uploads
        self.upload_stats = collections.defaultdict(FileUploadStatistics)

    def close(self):
        """
        Wait for all the asynchronous operations to be done
        """
        if self.queue:
            for _ in self.worker_processes:
                self.queue.put(None)

            for process in self.worker_processes:
                process.join()

    def _abort(self):
        """
        Abort all the operations
        """
        if self.queue:
            for process in self.worker_processes:
                os.kill(process.pid, signal.SIGINT)
        self.close()

    def _ensure_async(self):
        """
        Ensure that the asynchronous execution infrastructure is up
        and the worker process is running
        """
        if self.queue:
            return

        manager = multiprocessing.Manager()
        self.queue = manager.JoinableQueue(maxsize=self.worker_processes_count)
        self.result_queue = manager.Queue()
        self.errors_queue = manager.Queue()
        self.done_queue = manager.Queue()
        # Delay assigning the worker_processes list to the object until we have
        # finished spawning the workers so they do not get pickled by multiprocessing
        # (pickling the worker process references will fail in Python >= 3.8)
        worker_processes = []
        for process_number in range(self.worker_processes_count):
            process = multiprocessing.Process(
                target=self._worker_process_main, args=(process_number,)
            )
            process.start()
            worker_processes.append(process)
        self.worker_processes = worker_processes

    def _retrieve_results(self):
        """
        Receive the results from workers and update the local parts DB,
        making sure that each part list is sorted by part number
        """

        # Wait for all the current jobs to be completed
        self.queue.join()

        touched_keys = []
        while not self.result_queue.empty():
            result = self.result_queue.get()
            touched_keys.append(result["key"])
            self.parts_db[result["key"]].append(result["part"])

            # Save the upload end time of the part
            stats = self.upload_stats[result["key"]]
            stats.set_part_end_time(result["part_number"], result["end_time"])

        for key in touched_keys:
            self.parts_db[key] = sorted(
                self.parts_db[key], key=operator.itemgetter("PartNumber")
            )

        # Read the results of completed uploads
        while not self.done_queue.empty():
            result = self.done_queue.get()
            self.upload_stats[result["key"]].update(result)

        # Raise an error if a job failed
        self._handle_async_errors()

    def _handle_async_errors(self):
        """
        If an upload error has been discovered, stop the upload
        process, stop all the workers and raise an exception

        :return:
        """

        # If an error has already been reported, do nothing
        if self.error:
            return

        try:
            self.error = self.errors_queue.get_nowait()
        except EmptyQueue:
            return

        logging.error("Error received from upload worker: %s", self.error)
        self._abort()
        raise CloudUploadingError(self.error)

    def _worker_process_main(self, process_number):
        """
        Repeatedly grab a task from the queue and execute it, until a task
        containing "None" is grabbed, indicating that the process must stop.

        :param int process_number: the process number, used in the logging
        output
        """
        logging.info("Upload process started (worker %s)", process_number)

        # We create a new session instead of reusing the one
        # from the parent process to avoid any race condition
        self._reinit_session()

        while True:
            task = self.queue.get()
            if not task:
                self.queue.task_done()
                break

            try:
                self._worker_process_execute_job(task, process_number)
            except Exception as exc:
                logging.error(
                    "Upload error: %s (worker %s)", force_str(exc), process_number
                )
                logging.debug("Exception details:", exc_info=exc)
                self.errors_queue.put(force_str(exc))
            except KeyboardInterrupt:
                if not self.abort_requested:
                    logging.info(
                        "Got abort request: upload cancelled (worker %s)",
                        process_number,
                    )
                    self.abort_requested = True
            finally:
                self.queue.task_done()

        logging.info("Upload process stopped (worker %s)", process_number)

    def _worker_process_execute_job(self, task, process_number):
        """
        Exec a single task

        :param Dict task: task to execute
        :param int process_number: the process number, used in the logging
        output
        :return:
        """
        if task["job_type"] == "upload_part":
            if self.abort_requested:
                logging.info(
                    "Skipping '%s', part '%s' (worker %s)"
                    % (task["key"], task["part_number"], process_number)
                )
                os.unlink(task["body"])
                return
            else:
                logging.info(
                    "Uploading '%s', part '%s' (worker %s)"
                    % (task["key"], task["part_number"], process_number)
                )
                with open(task["body"], "rb") as fp:
                    part = self._upload_part(
                        task["upload_metadata"], task["key"], fp, task["part_number"]
                    )
                os.unlink(task["body"])
                self.result_queue.put(
                    {
                        "key": task["key"],
                        "part_number": task["part_number"],
                        "end_time": datetime.datetime.now(),
                        "part": part,
                    }
                )
        elif task["job_type"] == "complete_multipart_upload":
            if self.abort_requested:
                logging.info("Aborting %s (worker %s)" % (task["key"], process_number))
                self._abort_multipart_upload(task["upload_metadata"], task["key"])
                self.done_queue.put(
                    {
                        "key": task["key"],
                        "end_time": datetime.datetime.now(),
                        "status": "aborted",
                    }
                )
            else:
                logging.info(
                    "Completing '%s' (worker %s)" % (task["key"], process_number)
                )
                self._complete_multipart_upload(
                    task["upload_metadata"], task["key"], task["parts_metadata"]
                )
                self.done_queue.put(
                    {
                        "key": task["key"],
                        "end_time": datetime.datetime.now(),
                        "status": "done",
                    }
                )
        else:
            raise ValueError("Unknown task: %s", repr(task))

    def async_upload_part(self, upload_metadata, key, body, part_number):
        """
        Asynchronously upload a part into a multipart upload

        :param dict upload_metadata: Provider-specific metadata for this upload
          e.g. the multipart upload handle in AWS S3
        :param str key: The key to use in the cloud service
        :param any body: A stream-like object to upload
        :param int part_number: Part number, starting from 1
        """

        # If an error has already been reported, do nothing
        if self.error:
            return

        self._ensure_async()
        self._handle_async_errors()

        # Save the upload start time of the part
        stats = self.upload_stats[key]
        stats.set_part_start_time(part_number, datetime.datetime.now())

        # Pass the job to the uploader process
        self.queue.put(
            {
                "job_type": "upload_part",
                "upload_metadata": upload_metadata,
                "key": key,
                "body": body.name,
                "part_number": part_number,
            }
        )

    def async_complete_multipart_upload(self, upload_metadata, key, parts_count):
        """
        Asynchronously finish a certain multipart upload. This method grant
        that the final call to the cloud storage will happen after all the
        already scheduled parts have been uploaded.

        :param dict upload_metadata: Provider-specific metadata for this upload
          e.g. the multipart upload handle in AWS S3
        :param str key: The key to use in the cloud service
        :param int parts_count: Number of parts
        """

        # If an error has already been reported, do nothing
        if self.error:
            return

        self._ensure_async()
        self._handle_async_errors()

        # If parts_db has less then expected parts for this upload,
        # wait for the workers to send the missing metadata
        while len(self.parts_db[key]) < parts_count:
            # Wait for all the current jobs to be completed and
            # receive all available updates on worker status
            self._retrieve_results()

        # Finish the job in the uploader process
        self.queue.put(
            {
                "job_type": "complete_multipart_upload",
                "upload_metadata": upload_metadata,
                "key": key,
                "parts_metadata": self.parts_db[key],
            }
        )
        del self.parts_db[key]

    def wait_for_multipart_upload(self, key):
        """
        Wait for a multipart upload to be completed and return the result

        :param str key: The key to use in the cloud service
        """
        # The upload must exist
        assert key in self.upload_stats
        # async_complete_multipart_upload must have been called
        assert key not in self.parts_db

        # If status is still uploading the upload has not finished yet
        while self.upload_stats[key]["status"] == "uploading":
            # Wait for all the current jobs to be completed and
            # receive all available updates on worker status
            self._retrieve_results()

        return self.upload_stats[key]

    def setup_bucket(self):
        """
        Search for the target bucket. Create it if not exists
        """
        if self.bucket_exists is None:
            self.bucket_exists = self._check_bucket_existence()

        # Create the bucket if it doesn't exist
        if not self.bucket_exists:
            self._create_bucket()
            self.bucket_exists = True

    def extract_tar(self, key, dst):
        """
        Extract a tar archive from cloud to the local directory

        :param str key: The key identifying the tar archive
        :param str dst: Path of the directory into which the tar archive should
          be extracted
        """
        extension = os.path.splitext(key)[-1]
        compression = "" if extension == ".tar" else extension[1:]
        tar_mode = cloud_compression.get_streaming_tar_mode("r", compression)
        fileobj = self.remote_open(key, cloud_compression.get_compressor(compression))
        with tarfile.open(fileobj=fileobj, mode=tar_mode) as tf:
            tf.extractall(path=dst)

    @abstractmethod
    def _reinit_session(self):
        """
        Reinitialises any resources used to maintain a session with a cloud
        provider. This is called by child processes in order to avoid any
        potential race conditions around re-using the same session as the
        parent process.
        """

    @abstractmethod
    def test_connectivity(self):
        """
        Test that the cloud provider is reachable

        :return: True if the cloud provider is reachable, False otherwise
        :rtype: bool
        """

    @abstractmethod
    def _check_bucket_existence(self):
        """
        Check cloud storage for the target bucket

        :return: True if the bucket exists, False otherwise
        :rtype: bool
        """

    @abstractmethod
    def _create_bucket(self):
        """
        Create the bucket in cloud storage
        """

    @abstractmethod
    def list_bucket(self, prefix="", delimiter=DEFAULT_DELIMITER):
        """
        List bucket content in a directory manner

        :param str prefix:
        :param str delimiter:
        :return: List of objects and dirs right under the prefix
        :rtype: List[str]
        """

    @abstractmethod
    def download_file(self, key, dest_path, decompress):
        """
        Download a file from cloud storage

        :param str key: The key identifying the file to download
        :param str dest_path: Where to put the destination file
        :param str|None decompress: Compression scheme to use for decompression
        """

    @abstractmethod
    def remote_open(self, key, decompressor=None):
        """
        Open a remote object in cloud storage and returns a readable stream

        :param str key: The key identifying the object to open
        :param barman.clients.cloud_compression.ChunkedCompressor decompressor:
          A ChunkedCompressor object which will be used to decompress chunks of bytes
          as they are read from the stream
        :return: A file-like object from which the stream can be read or None if
          the key does not exist
        """

    @abstractmethod
    def upload_fileobj(self, fileobj, key, override_tags=None):
        """
        Synchronously upload the content of a file-like object to a cloud key

        :param fileobj IOBase: File-like object to upload
        :param str key: The key to identify the uploaded object
        :param List[tuple] override_tags: List of k,v tuples which should override any
          tags already defined in the cloud interface
        """

    @abstractmethod
    def create_multipart_upload(self, key):
        """
        Create a new multipart upload and return any metadata returned by the
        cloud provider.

        This metadata is treated as an opaque blob by CloudInterface and will
        be passed into the _upload_part, _complete_multipart_upload and
        _abort_multipart_upload methods.

        The implementations of these methods will need to handle this metadata in
        the way expected by the cloud provider.

        Some cloud services do not require multipart uploads to be explicitly
        created. In such cases the implementation can be a no-op which just
        returns None.

        :param key: The key to use in the cloud service
        :return: The multipart upload metadata
        :rtype: dict[str, str]|None
        """

    @abstractmethod
    def _upload_part(self, upload_metadata, key, body, part_number):
        """
        Upload a part into this multipart upload and return a dict of part
        metadata. The part metadata must contain the key "PartNumber" and can
        optionally contain any other metadata available (for example the ETag
        returned by S3).

        The part metadata will included in a list of metadata for all parts of
        the upload which is passed to the _complete_multipart_upload method.

        :param dict upload_metadata: Provider-specific metadata for this upload
          e.g. the multipart upload handle in AWS S3
        :param str key: The key to use in the cloud service
        :param object body: A stream-like object to upload
        :param int part_number: Part number, starting from 1
        :return: The part metadata
        :rtype: dict[str, None|str]
        """

    @abstractmethod
    def _complete_multipart_upload(self, upload_metadata, key, parts_metadata):
        """
        Finish a certain multipart upload

        :param dict upload_metadata: Provider-specific metadata for this upload
          e.g. the multipart upload handle in AWS S3
        :param str key: The key to use in the cloud service
        :param List[dict] parts_metadata: The list of metadata for the parts
          composing the multipart upload. Each part is guaranteed to provide a
          PartNumber and may optionally contain additional metadata returned by
          the cloud provider such as ETags.
        """

    @abstractmethod
    def _abort_multipart_upload(self, upload_metadata, key):
        """
        Abort a certain multipart upload

        The implementation of this method should clean up any dangling resources
        left by the incomplete upload.

        :param dict upload_metadata: Provider-specific metadata for this upload
          e.g. the multipart upload handle in AWS S3
        :param str key: The key to use in the cloud service
        """

    @abstractmethod
    def _delete_objects_batch(self, paths):
        """
        Delete a single batch of objects

        :param List[str] paths:
        """
        if len(paths) > self.MAX_DELETE_BATCH_SIZE:
            raise ValueError("Max batch size exceeded")

    def delete_objects(self, paths):
        """
        Delete the objects at the specified paths

        Deletes the objects defined by the supplied list of paths in batches
        specified by either batch_size or MAX_DELETE_BATCH_SIZE, whichever is
        lowest.

        :param List[str] paths:
        """
        errors = False
        for i in range_fun(0, len(paths), self.delete_batch_size):
            try:
                self._delete_objects_batch(paths[i : i + self.delete_batch_size])
            except CloudProviderError:
                # Don't let one error stop us from trying to delete any remaining
                # batches.
                errors = True

        if errors:
            raise CloudProviderError(
                "Error from cloud provider while deleting objects - "
                "please check the command output."
            )

    @abstractmethod
    def get_prefixes(self, prefix):
        """
        Return only the common prefixes under the supplied prefix.

        :param str prefix: The object key prefix under which the common prefixes
            will be found.
        :rtype: Iterator[str]
        :return: A list of unique prefixes immediately under the supplied prefix.
        """

    @abstractmethod
    def delete_under_prefix(self, prefix):
        """
        Delete all objects under the specified prefix.

        :param str prefix: The object key prefix under which all objects should be
            deleted.
        """


class CloudBackup(with_metaclass(ABCMeta)):
    """
    Abstract base class for taking cloud backups of PostgreSQL servers.

    This class handles the coordination of the physical backup copy with the PostgreSQL
    server via the PostgreSQL low-level backup API. This is handled by the
    _coordinate_backup method.

    Concrete classes will need to implement the following abstract methods which are
    called during the _coordinate_backup method:

        _take_backup
        _upload_backup_label
        _finalise_copy
        _add_stats_to_backup_info

    Implementations must also implement the public backup method which should carry
    out any prepartion and invoke _coordinate_backup.
    """

    def __init__(self, server_name, cloud_interface, postgres, backup_name=None):
        """
        :param str server_name: The name of the server being backed up.
        :param CloudInterface cloud_interface: The CloudInterface for interacting with
            the cloud object store.
        :param barman.postgres.PostgreSQLConnection|None postgres: A connection to the
            PostgreSQL instance being backed up.
        :param str|None backup_name: A friendly name which can be used to reference
            this backup in the future.
        """
        self.server_name = server_name
        self.cloud_interface = cloud_interface
        self.postgres = postgres
        self.backup_name = backup_name
        # Stats
        self.copy_start_time = None
        self.copy_end_time = None
        # Object properties set at backup time
        self.backup_info = None

    # The following abstract methods are called when coordinating the backup.
    # They are all specific to the backup copy mechanism so the implementation must
    # happen in the subclass.
    @abstractmethod
    def _take_backup(self):
        """
        Perform the actions necessary to create the backup.

        This method must be called between pg_backup_start and pg_backup_stop which
        is guaranteed to happen if the _coordinate_backup method is used.
        """

    @abstractmethod
    def _upload_backup_label(self):
        """
        Upload the backup label to cloud storage.
        """

    @abstractmethod
    def _finalise_copy(self):
        """
        Perform any finalisation required to complete the copy of backup data.
        """

    @abstractmethod
    def _add_stats_to_backup_info(self):
        """
        Add statistics about the backup to self.backup_info.
        """

    # The public facing backup method must also be implemented in concrete classes.
    @abstractmethod
    def backup(self):
        """
        External interface for performing a cloud backup of the postgres server.

        When providing an implementation of this method, concrete classes *must* set
        `self.backup_info` before coordinating the backup. Implementations *should*
        call `self._coordinate_backup` to carry out the backup process.
        """

    # The following concrete methods are independent of backup copy mechanism.
    def _start_backup(self):
        """
        Start the backup via the PostgreSQL backup API.
        """
        self.strategy = ConcurrentBackupStrategy(self.postgres, self.server_name)
        logging.info("Starting backup '%s'", self.backup_info.backup_id)
        self.strategy.start_backup(self.backup_info)

    def _stop_backup(self):
        """
        Stop the backup via the PostgreSQL backup API.
        """
        logging.info("Stopping backup '%s'", self.backup_info.backup_id)
        self.strategy.stop_backup(self.backup_info)

    def _create_restore_point(self):
        """
        Create a restore point named after this backup.
        """
        target_name = "barman_%s" % self.backup_info.backup_id
        self.postgres.create_restore_point(target_name)

    def _get_backup_info(self, server_name):
        """
        Create and return the backup_info for this CloudBackup.
        """
        backup_info = BackupInfo(
            backup_id=datetime.datetime.now().strftime("%Y%m%dT%H%M%S"),
            server_name=server_name,
        )
        backup_info.set_attribute("systemid", self.postgres.get_systemid())
        return backup_info

    def _upload_backup_info(self):
        """
        Upload the backup_info for this CloudBackup.
        """
        with BytesIO() as backup_info_file:
            key = os.path.join(
                self.cloud_interface.path,
                self.server_name,
                "base",
                self.backup_info.backup_id,
                "backup.info",
            )
            self.backup_info.save(file_object=backup_info_file)
            backup_info_file.seek(0, os.SEEK_SET)
            logging.info("Uploading '%s'", key)
            self.cloud_interface.upload_fileobj(backup_info_file, key)

    def _check_postgres_version(self):
        """
        Verify we are running against a supported PostgreSQL version.
        """
        if not self.postgres.is_minimal_postgres_version():
            raise BackupException(
                "unsupported PostgresSQL version %s. Expecting %s or above."
                % (
                    self.postgres.server_major_version,
                    self.postgres.minimal_txt_version,
                )
            )

    def _log_end_of_backup(self):
        """
        Write log lines indicating end of backup.
        """
        logging.info(
            "Backup end at LSN: %s (%s, %08X)",
            self.backup_info.end_xlog,
            self.backup_info.end_wal,
            self.backup_info.end_offset,
        )
        logging.info(
            "Backup completed (start time: %s, elapsed time: %s)",
            self.copy_start_time,
            human_readable_timedelta(datetime.datetime.now() - self.copy_start_time),
        )

    def _coordinate_backup(self):
        """
        Coordinate taking the backup with the PostgreSQL server.
        """
        try:
            # Store the start time
            self.copy_start_time = datetime.datetime.now()

            self._start_backup()

            self._take_backup()

            self._stop_backup()

            self._create_restore_point()

            self._upload_backup_label()

            self._finalise_copy()

            # Store the end time
            self.copy_end_time = datetime.datetime.now()

            # Store statistics about the copy
            self._add_stats_to_backup_info()

            # Set the backup status as DONE
            self.backup_info.set_attribute("status", BackupInfo.DONE)

        except BaseException as exc:
            # Mark the backup as failed and exit
            self.handle_backup_errors("uploading data", exc, self.backup_info)
            raise SystemExit(1)
        finally:
            # Add the name to the backup info
            if self.backup_name is not None:
                self.backup_info.set_attribute("backup_name", self.backup_name)
            try:
                self._upload_backup_info()
            except BaseException as exc:
                # Mark the backup as failed and exit
                self.handle_backup_errors(
                    "uploading backup.info file", exc, self.backup_info
                )
                raise SystemExit(1)

        self._log_end_of_backup()

    def handle_backup_errors(self, action, exc, backup_info):
        """
        Mark the backup as failed and exit

        :param str action: the upload phase that has failed
        :param BaseException exc: the exception that caused the failure
        :param barman.infofile.BackupInfo backup_info: the backup info file
        """
        msg_lines = force_str(exc).strip().splitlines()
        # If the exception has no attached message use the raw
        # type name
        if len(msg_lines) == 0:
            msg_lines = [type(exc).__name__]
        if backup_info:
            # Use only the first line of exception message
            # in backup_info error field
            backup_info.set_attribute("status", BackupInfo.FAILED)
            backup_info.set_attribute(
                "error", "failure %s (%s)" % (action, msg_lines[0])
            )
        logging.error("Backup failed %s (%s)", action, msg_lines[0])
        logging.debug("Exception details:", exc_info=exc)


class CloudBackupUploader(CloudBackup):
    """
    Uploads backups from a PostgreSQL server to cloud object storage.
    """

    def __init__(
        self,
        server_name,
        cloud_interface,
        max_archive_size,
        postgres,
        compression=None,
        backup_name=None,
        min_chunk_size=None,
        max_bandwidth=None,
    ):
        """
        Base constructor.

        :param str server_name: The name of the server as configured in Barman
        :param CloudInterface cloud_interface: The interface to use to
          upload the backup
        :param int max_archive_size: the maximum size of an uploading archive
        :param barman.postgres.PostgreSQLConnection|None postgres: A connection to the
            PostgreSQL instance being backed up.
        :param str compression: Compression algorithm to use
        :param str|None backup_name: A friendly name which can be used to reference
            this backup in the future.
        :param int min_chunk_size: the minimum size of a single upload part
        :param int max_bandwidth: the maximum amount of data per second that should
          be uploaded during the backup
        """
        super(CloudBackupUploader, self).__init__(
            server_name,
            cloud_interface,
            postgres,
            backup_name,
        )

        self.compression = compression
        self.max_archive_size = max_archive_size
        self.min_chunk_size = min_chunk_size
        self.max_bandwidth = max_bandwidth

        # Object properties set at backup time
        self.controller = None

    # The following methods add specific functionality required to upload backups to
    # cloud object storage.
    def _get_tablespace_location(self, tablespace):
        """
        Return the on-disk location of the supplied tablespace.

        This will usually just be the location of the tablespace however subclasses
        which run against Barman server will need to override this method.

        :param infofile.Tablespace tablespace: The tablespace whose location should be
            returned.
        :rtype: str
        :return: The path of the supplied tablespace.
        """
        return tablespace.location

    def _create_upload_controller(self, backup_id):
        """
        Create an upload controller from the specified backup_id

        :param str backup_id: The backup identifier
        :rtype: CloudUploadController
        :return: The upload controller
        """
        key_prefix = os.path.join(
            self.cloud_interface.path,
            self.server_name,
            "base",
            backup_id,
        )
        return CloudUploadController(
            self.cloud_interface,
            key_prefix,
            self.max_archive_size,
            self.compression,
            self.min_chunk_size,
            self.max_bandwidth,
        )

    def _backup_data_files(
        self, controller, backup_info, pgdata_dir, server_major_version
    ):
        """
        Perform the actual copy of the data files uploading it to cloud storage.

        First, it copies one tablespace at a time, then the PGDATA directory,
        then pg_control.

        Bandwidth limitation, according to configuration, is applied in
        the process.

        :param barman.cloud.CloudUploadController controller: upload controller
        :param barman.infofile.BackupInfo backup_info: backup information
        :param str pgdata_dir: Path to pgdata directory
        :param str server_major_version: Major version of the postgres server
          being backed up
        """

        # List of paths to be excluded by the PGDATA copy
        exclude = []

        # Process every tablespace
        if backup_info.tablespaces:
            for tablespace in backup_info.tablespaces:
                # If the tablespace location is inside the data directory,
                # exclude and protect it from being copied twice during
                # the data directory copy
                if tablespace.location.startswith(backup_info.pgdata + "/"):
                    exclude += [tablespace.location[len(backup_info.pgdata) :]]

                # Exclude and protect the tablespace from being copied again
                # during the data directory copy
                exclude += ["/pg_tblspc/%s" % tablespace.oid]

                # Copy the tablespace directory.
                # NOTE: Barman should archive only the content of directory
                #    "PG_" + PG_MAJORVERSION + "_" + CATALOG_VERSION_NO
                # but CATALOG_VERSION_NO is not easy to retrieve, so we copy
                #    "PG_" + PG_MAJORVERSION + "_*"
                # It could select some spurious directory if a development or
                # a beta version have been used, but it's good enough for a
                # production system as it filters out other major versions.
                controller.upload_directory(
                    label=tablespace.name,
                    src=self._get_tablespace_location(tablespace),
                    dst="%s" % tablespace.oid,
                    exclude=["/*"] + EXCLUDE_LIST,
                    include=["/PG_%s_*" % server_major_version],
                )

        # Copy PGDATA directory (or if that is itself a symlink, just follow it
        # and copy whatever it points to; we won't store the symlink in the tar
        # file)
        if os.path.islink(pgdata_dir):
            pgdata_dir = os.path.realpath(pgdata_dir)
        controller.upload_directory(
            label="pgdata",
            src=pgdata_dir,
            dst="data",
            exclude=PGDATA_EXCLUDE_LIST + EXCLUDE_LIST + exclude,
        )

        # At last copy pg_control
        controller.add_file(
            label="pg_control",
            src="%s/global/pg_control" % pgdata_dir,
            dst="data",
            path="global/pg_control",
        )

    def _backup_config_files(self, controller, backup_info):
        """
        Perform the backup of any external config files.

        :param barman.cloud.CloudUploadController controller: upload controller
        :param barman.infofile.BackupInfo backup_info: backup information
        """
        # Copy configuration files (if not inside PGDATA)
        external_config_files = backup_info.get_external_config_files()
        included_config_files = []
        for config_file in external_config_files:
            # Add included files to a list, they will be handled later
            if config_file.file_type == "include":
                included_config_files.append(config_file)
                continue

            # If the ident file is missing, it isn't an error condition
            # for PostgreSQL.
            # Barman is consistent with this behavior.
            optional = False
            if config_file.file_type == "ident_file":
                optional = True

            # Create the actual copy jobs in the controller
            controller.add_file(
                label=config_file.file_type,
                src=config_file.path,
                dst="data",
                path=os.path.basename(config_file.path),
                optional=optional,
            )

        # Check for any include directives in PostgreSQL configuration
        # Currently, include directives are not supported for files that
        # reside outside PGDATA. These files must be manually backed up.
        # Barman will emit a warning and list those files
        if any(included_config_files):
            msg = (
                "The usage of include directives is not supported "
                "for files that reside outside PGDATA.\n"
                "Please manually backup the following files:\n"
                "\t%s\n" % "\n\t".join(icf.path for icf in included_config_files)
            )
            logging.warning(msg)

    @property
    def _pgdata_dir(self):
        """
        The location of the PGDATA directory to be backed up.
        """
        return self.backup_info.pgdata

    # The remaining methods are the concrete implementations of the abstract methods from
    # the parent class.
    def _take_backup(self):
        """
        Make a backup by copying PGDATA, tablespaces and config to cloud storage.
        """
        self._backup_data_files(
            self.controller,
            self.backup_info,
            self._pgdata_dir,
            self.postgres.server_major_version,
        )
        self._backup_config_files(self.controller, self.backup_info)

    def _finalise_copy(self):
        """
        Close the upload controller, forcing the flush of any buffered uploads.
        """
        self.controller.close()

    def _upload_backup_label(self):
        """
        Upload the backup label to cloud storage.

        Upload is via the upload controller so that the backup label is added to the
        data tarball.
        """
        if self.backup_info.backup_label:
            pgdata_stat = os.stat(self.backup_info.pgdata)
            self.controller.add_fileobj(
                label="backup_label",
                fileobj=BytesIO(self.backup_info.backup_label.encode("UTF-8")),
                dst="data",
                path="backup_label",
                uid=pgdata_stat.st_uid,
                gid=pgdata_stat.st_gid,
            )

    def _add_stats_to_backup_info(self):
        """
        Adds statistics from the upload controller to the backup_info.
        """
        self.backup_info.set_attribute("copy_stats", self.controller.statistics())

    def backup(self):
        """
        Upload a Backup to cloud storage directly from a live PostgreSQL server.
        """
        server_name = "cloud"
        self.backup_info = self._get_backup_info(server_name)
        self.controller = self._create_upload_controller(self.backup_info.backup_id)

        self._check_postgres_version()

        self._coordinate_backup()


class CloudBackupUploaderBarman(CloudBackupUploader):
    """
    A cloud storage upload client for a preexisting backup on the Barman server.
    """

    def __init__(
        self,
        server_name,
        cloud_interface,
        max_archive_size,
        backup_dir,
        backup_id,
        backup_info_path,
        compression=None,
        min_chunk_size=None,
        max_bandwidth=None,
    ):
        """
        Create the cloud storage upload client for a backup in the specified
        location with the specified backup_id.

        :param str server_name: The name of the server as configured in Barman
        :param CloudInterface cloud_interface: The interface to use to
          upload the backup
        :param int max_archive_size: the maximum size of an uploading archive
        :param str backup_dir: Path to the directory containing the backup to
          be uploaded
        :param str backup_id: The id of the backup to upload
        :param str backup_info_path: Path of the ``backup.info`` file.
        :param str compression: Compression algorithm to use
        :param int min_chunk_size: the minimum size of a single upload part
        :param int max_bandwidth: the maximum amount of data per second that
          should be uploaded during the backup
        """
        super(CloudBackupUploaderBarman, self).__init__(
            server_name,
            cloud_interface,
            max_archive_size,
            compression=compression,
            postgres=None,
            min_chunk_size=min_chunk_size,
            max_bandwidth=max_bandwidth,
        )
        self.backup_dir = backup_dir
        self.backup_id = backup_id
        self.backup_info_path = backup_info_path

    def handle_backup_errors(self, action, exc):
        """
        Log that the backup upload has failed and exit

        This differs from the function in the superclass because it does not update
        the backup.info metadata (this must be left untouched since it relates to the
        original backup made with Barman).

        :param str action: the upload phase that has failed
        :param BaseException exc: the exception that caused the failure
        """
        msg_lines = force_str(exc).strip().splitlines()
        # If the exception has no attached message use the raw
        # type name
        if len(msg_lines) == 0:
            msg_lines = [type(exc).__name__]
        logging.error("Backup upload failed %s (%s)", action, msg_lines[0])
        logging.debug("Exception details:", exc_info=exc)

    def _get_tablespace_location(self, tablespace):
        """
        Return the on-disk location of the supplied tablespace.

        Combines the backup_dir and the tablespace OID to determine the location of
        the tablespace on the Barman server.

        :param infofile.Tablespace tablespace: The tablespace whose location should be
            returned.
        :rtype: str
        :return: The path of the supplied tablespace.
        """
        return os.path.join(self.backup_dir, str(tablespace.oid))

    @property
    def _pgdata_dir(self):
        """
        The location of the PGDATA directory to be backed up.
        """
        return os.path.join(self.backup_dir, "data")

    def _take_backup(self):
        """
        Make a backup by copying PGDATA and tablespaces to cloud storage.
        """
        self._backup_data_files(
            self.controller,
            self.backup_info,
            self._pgdata_dir,
            self.backup_info.pg_major_version(),
        )

    def backup(self):
        """
        Upload a Backup to cloud storage

        This deviates from other CloudBackup classes because it does not make use of
        the self._coordinate_backup function. This is because there is no need to
        coordinate the backup with a live PostgreSQL server, create a restore point
        or upload the backup label independently of the backup (it will already be in
        the base backup directoery).
        """
        # Read the backup_info file from disk as the backup has already been created
        self.backup_info = BackupInfo(self.backup_id)
        self.backup_info.load(filename=self.backup_info_path)
        self.controller = self._create_upload_controller(self.backup_id)
        try:
            self.copy_start_time = datetime.datetime.now()
            self._take_backup()

            # Closing the controller will finalize all the running uploads
            self.controller.close()

            # Store the end time
            self.copy_end_time = datetime.datetime.now()

            # Manually add backup.info
            with open(self.backup_info_path, "rb") as backup_info_file:
                self.cloud_interface.upload_fileobj(
                    backup_info_file,
                    key=os.path.join(self.controller.key_prefix, "backup.info"),
                )

        # Use BaseException instead of Exception to catch events like
        # KeyboardInterrupt (e.g.: CTRL-C)
        except BaseException as exc:
            # Mark the backup as failed and exit
            self.handle_backup_errors("uploading data", exc)
            raise SystemExit(1)

        logging.info(
            "Upload of backup completed (start time: %s, elapsed time: %s)",
            self.copy_start_time,
            human_readable_timedelta(datetime.datetime.now() - self.copy_start_time),
        )


class CloudBackupSnapshot(CloudBackup):
    """
    A cloud backup client using disk snapshots to create the backup.
    """

    def __init__(
        self,
        server_name,
        cloud_interface,
        snapshot_interface,
        postgres,
        snapshot_instance,
        snapshot_disks,
        backup_name=None,
    ):
        """
        Create the backup client for snapshot backups

        :param str server_name: The name of the server as configured in Barman
        :param CloudInterface cloud_interface: The interface to use to
          upload the backup
        :param SnapshotInterface snapshot_interface: The interface to use for
          creating a backup using snapshots
        :param barman.postgres.PostgreSQLConnection|None postgres: A connection to the
            PostgreSQL instance being backed up.
        :param str snapshot_instance: The name of the VM instance to which the disks
            to be backed up are attached.
        :param list[str] snapshot_disks: A list containing the names of the disks for
            which snapshots should be taken at backup time.
        :param str|None backup_name: A friendly name which can be used to reference
            this backup in the future.
        """
        super(CloudBackupSnapshot, self).__init__(
            server_name, cloud_interface, postgres, backup_name
        )
        self.snapshot_interface = snapshot_interface
        self.snapshot_instance = snapshot_instance
        self.snapshot_disks = snapshot_disks

    # The remaining methods are the concrete implementations of the abstract methods from
    # the parent class.
    def _finalise_copy(self):
        """
        Perform any finalisation required to complete the copy of backup data.

        This is a no-op for snapshot backups.
        """
        pass

    def _add_stats_to_backup_info(self):
        """
        Add statistics about the backup to self.backup_info.
        """
        self.backup_info.set_attribute(
            "copy_stats",
            {
                "copy_time": total_seconds(self.copy_end_time - self.copy_start_time),
                "total_time": total_seconds(self.copy_end_time - self.copy_start_time),
            },
        )

    def _upload_backup_label(self):
        """
        Upload the backup label to cloud storage.

        Snapshot backups just upload the backup label as a single object rather than
        adding it to a tar archive.
        """
        backup_label_key = os.path.join(
            self.cloud_interface.path,
            self.server_name,
            "base",
            self.backup_info.backup_id,
            "backup_label",
        )
        self.cloud_interface.upload_fileobj(
            BytesIO(self.backup_info.backup_label.encode("UTF-8")),
            backup_label_key,
        )

    def _take_backup(self):
        """
        Make a backup by creating snapshots of the specified disks.
        """
        volumes_to_snapshot = self.snapshot_interface.get_attached_volumes(
            self.snapshot_instance, self.snapshot_disks
        )
        cmd = UnixLocalCommand()
        SnapshotBackupExecutor.add_mount_data_to_volume_metadata(
            volumes_to_snapshot, cmd
        )
        self.snapshot_interface.take_snapshot_backup(
            self.backup_info,
            self.snapshot_instance,
            volumes_to_snapshot,
        )

    # The following method implements specific functionality for snapshot backups.
    def _check_backup_preconditions(self):
        """
        Perform additional checks for snapshot backups, specifically:

          - check that the VM instance for which snapshots should be taken exists
          - check that the expected disks are attached to that instance
          - check that the attached disks are mounted on the filesystem

        Raises a BackupPreconditionException if any of the checks fail.
        """
        if not self.snapshot_interface.instance_exists(self.snapshot_instance):
            raise BackupPreconditionException(
                "Cannot find compute instance %s" % self.snapshot_instance
            )

        cmd = UnixLocalCommand()
        (
            missing_disks,
            unmounted_disks,
        ) = SnapshotBackupExecutor.find_missing_and_unmounted_disks(
            cmd,
            self.snapshot_interface,
            self.snapshot_instance,
            self.snapshot_disks,
        )

        if len(missing_disks) > 0:
            raise BackupPreconditionException(
                "Cannot find disks attached to compute instance %s: %s"
                % (self.snapshot_instance, ", ".join(missing_disks))
            )
        if len(unmounted_disks) > 0:
            raise BackupPreconditionException(
                "Cannot find disks mounted on compute instance %s: %s"
                % (self.snapshot_instance, ", ".join(unmounted_disks))
            )

    # Specific implementation of the public-facing backup method.
    def backup(self):
        """
        Take a backup by creating snapshots of the specified disks.
        """
        self._check_backup_preconditions()

        self.backup_info = self._get_backup_info(self.server_name)

        self._check_postgres_version()

        self._coordinate_backup()


class BackupFileInfo(object):
    def __init__(self, oid=None, base=None, path=None, compression=None):
        self.oid = oid
        self.base = base
        self.path = path
        self.compression = compression
        self.additional_files = []


class CloudBackupCatalog(KeepManagerMixinCloud):
    """
    Cloud storage backup catalog
    """

    def __init__(self, cloud_interface, server_name):
        """
        Object responsible for retrieving backup catalog from cloud storage

        :param CloudInterface cloud_interface: The interface to use to
          upload the backup
        :param str server_name: The name of the server as configured in Barman
        """
        super(CloudBackupCatalog, self).__init__(
            cloud_interface=cloud_interface, server_name=server_name
        )
        self.cloud_interface = cloud_interface
        self.server_name = server_name
        self.prefix = os.path.join(self.cloud_interface.path, self.server_name, "base")
        self.wal_prefix = os.path.join(
            self.cloud_interface.path, self.server_name, "wals"
        )
        self._backup_list = None
        self._wal_paths = None
        self.unreadable_backups = []

    def get_backup_list(self):
        """
        Retrieve the list of available backup from cloud storage

        :rtype: Dict[str,BackupInfo]
        """
        if self._backup_list is None:
            backup_list = {}

            # get backups metadata
            for backup_dir in self.cloud_interface.list_bucket(self.prefix + "/"):
                # We want only the directories
                if backup_dir[-1] != "/":
                    continue
                backup_id = os.path.basename(backup_dir.rstrip("/"))
                try:
                    backup_info = self.get_backup_info(backup_id)
                except Exception as exc:
                    logging.warning(
                        "Unable to open backup.info file for %s: %s" % (backup_id, exc)
                    )
                    self.unreadable_backups.append(backup_id)
                    continue

                if backup_info:
                    backup_list[backup_id] = backup_info
            self._backup_list = backup_list
        return self._backup_list

    def remove_backup_from_cache(self, backup_id):
        """
        Remove backup with backup_id from the cached list. This is intended for
        cases where we want to update the state without firing lots of requests
        at the bucket.
        """
        if self._backup_list:
            self._backup_list.pop(backup_id)

    def get_wal_prefixes(self):
        """
        Return only the common prefixes under the wals prefix.
        """
        return self.cloud_interface.get_prefixes(self.wal_prefix)

    def get_wal_paths(self):
        """
        Retrieve a dict of WAL paths keyed by the WAL name from cloud storage
        """
        if self._wal_paths is None:
            wal_paths = {}
            for wal in self.cloud_interface.list_bucket(
                self.wal_prefix + "/", delimiter=""
            ):
                wal_basename = os.path.basename(wal)
                if xlog.is_any_xlog_file(wal_basename):
                    # We have an uncompressed xlog of some kind
                    wal_paths[wal_basename] = wal
                else:
                    # Allow one suffix for compression and try again
                    wal_name, suffix = os.path.splitext(wal_basename)
                    if suffix in ALLOWED_COMPRESSIONS and xlog.is_any_xlog_file(
                        wal_name
                    ):
                        wal_paths[wal_name] = wal
                    else:
                        # If it still doesn't look like an xlog file, ignore
                        continue

            self._wal_paths = wal_paths
        return self._wal_paths

    def remove_wal_from_cache(self, wal_name):
        """
        Remove named wal from the cached list. This is intended for cases where
        we want to update the state without firing lots of requests at the bucket.
        """
        if self._wal_paths:
            self._wal_paths.pop(wal_name)

    def _get_backup_info_from_name(self, backup_name):
        """
        Get the backup metadata for the named backup.

        :param str backup_name: The name of the backup for which the backup metadata
            should be retrieved
        :return BackupInfo|None: The backup metadata for the named backup
        """
        available_backups = self.get_backup_list().values()
        return get_backup_info_from_name(available_backups, backup_name)

    def parse_backup_id(self, backup_id):
        """
        Parse a backup identifier and return the matching backup ID. If the identifier
        is a backup ID it is returned, otherwise it is assumed to be a name.

        :param str backup_id: The backup identifier to be parsed
        :return str: The matching backup ID for the supplied identifier
        """
        if not is_backup_id(backup_id):
            backup_info = self._get_backup_info_from_name(backup_id)
            if backup_info is not None:
                return backup_info.backup_id
            else:
                raise ValueError(
                    "Unknown backup '%s' for server '%s'"
                    % (backup_id, self.server_name)
                )
        else:
            return backup_id

    def get_backup_info(self, backup_id):
        """
        Load a BackupInfo from cloud storage

        :param str backup_id: The backup id to load
        :rtype: BackupInfo
        """
        backup_info_path = os.path.join(self.prefix, backup_id, "backup.info")
        backup_info_file = self.cloud_interface.remote_open(backup_info_path)
        if backup_info_file is None:
            return None
        backup_info = BackupInfo(backup_id)
        backup_info.load(file_object=backup_info_file)
        return backup_info

    def get_backup_files(self, backup_info, allow_missing=False):
        """
        Get the list of expected files part of a backup

        :param BackupInfo backup_info: the backup information
        :param bool allow_missing: True if missing backup files are allowed, False
         otherwise. A value of False will cause a SystemExit to be raised if any
         files expected due to the `backup_info` content cannot be found.
        :rtype: dict[int, BackupFileInfo]
        """
        # Correctly format the source path
        source_dir = os.path.join(self.prefix, backup_info.backup_id)

        base_path = os.path.join(source_dir, "data")
        backup_files = {None: BackupFileInfo(None, base_path)}
        if backup_info.tablespaces:
            for tblspc in backup_info.tablespaces:
                base_path = os.path.join(source_dir, "%s" % tblspc.oid)
                backup_files[tblspc.oid] = BackupFileInfo(tblspc.oid, base_path)

        for item in self.cloud_interface.list_bucket(source_dir + "/"):
            for backup_file in backup_files.values():
                if item.startswith(backup_file.base):
                    # Automatically detect additional files
                    suffix = item[len(backup_file.base) :]
                    # Avoid to match items that are prefix of other items
                    if not suffix or suffix[0] not in (".", "_"):
                        logging.debug(
                            "Skipping spurious prefix match: %s|%s",
                            backup_file.base,
                            suffix,
                        )
                        continue
                    # If this file have a suffix starting with `_`,
                    # it is an additional file and we add it to the main
                    # BackupFileInfo ...
                    if suffix[0] == "_":
                        info = BackupFileInfo(backup_file.oid, base_path)
                        backup_file.additional_files.append(info)
                        ext = suffix.split(".", 1)[-1]
                    # ... otherwise this is the main file
                    else:
                        info = backup_file
                        ext = suffix[1:]
                    # Infer the compression from the file extension
                    if ext == "tar":
                        info.compression = None
                    elif ext == "tar.gz":
                        info.compression = "gzip"
                    elif ext == "tar.bz2":
                        info.compression = "bzip2"
                    elif ext == "tar.snappy":
                        info.compression = "snappy"
                    else:
                        logging.warning("Skipping unknown extension: %s", ext)
                        continue
                    info.path = item
                    logging.info(
                        "Found file from backup '%s' of server '%s': %s",
                        backup_info.backup_id,
                        self.server_name,
                        info.path,
                    )
                    break

        for backup_file in backup_files.values():
            logging_fun = logging.warning if allow_missing else logging.error
            if backup_file.path is None and backup_info.snapshots_info is None:
                logging_fun(
                    "Missing file %s.* for server %s",
                    backup_file.base,
                    self.server_name,
                )
                if not allow_missing:
                    raise SystemExit(1)

        return backup_files

    def get_latest_archived_wals_info(self):
        """
        Return a dictionary of timelines associated with the
        WalFileInfo of the last WAL file in the archive,
        or an empty dict if the archive doesn't contain any WAL file.

        :rtype: dict[str, WalFileInfo]
        """
        if not self.get_wal_paths():
            return dict()

        timelines = {}
        for name in sorted(self.get_wal_paths(), reverse=True):
            # Extract the timeline. If it is not valid, skip this directory
            try:
                timeline = name[0:8]
                int(timeline, 16)
            except ValueError:
                continue
                # If this timeline already has a file, skip this directory
            if timeline in timelines:
                continue
            timelines[timeline] = WalFileInfo(name=name)
            break
        # Return the timeline map
        return timelines


class CloudSnapshotInterface(with_metaclass(ABCMeta)):
    """Defines a common interface for handling cloud snapshots."""

    _required_config_for_backup = ("snapshot_disks", "snapshot_instance")
    _required_config_for_restore = ("snapshot_recovery_instance",)

    @classmethod
    def validate_backup_config(cls, config):
        """
        Additional validation for backup options.

        Raises a ConfigurationException if any required options are missing.

        :param argparse.Namespace config: The backup options provided at the command line.
        """
        missing_options = get_missing_attrs(config, cls._required_config_for_backup)
        if len(missing_options) > 0:
            raise ConfigurationException(
                "Incomplete options for snapshot backup - missing: %s"
                % ", ".join(missing_options)
            )

    @classmethod
    def validate_restore_config(cls, config):
        """
        Additional validation for restore options.

        Raises a ConfigurationException if any required options are missing.

        :param argparse.Namespace config: The backup options provided at the command line.
        """
        missing_options = get_missing_attrs(config, cls._required_config_for_restore)
        if len(missing_options) > 0:
            raise ConfigurationException(
                "Incomplete options for snapshot restore - missing: %s"
                % ", ".join(missing_options)
            )

    @abstractmethod
    def take_snapshot_backup(self, backup_info, instance_name, volumes):
        """
        Take a snapshot backup for the named instance.

        Implementations of this method must do the following:

            * Create a snapshot of the disk.
            * Set the snapshots_info field of the backup_info to a SnapshotsInfo
              implementation which contains the snapshot metadata required both
              by Barman and any third party tooling which needs to recover the
              snapshots.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information.
        :param str instance_name: The name of the VM instance to which the disks
            to be backed up are attached.
        :param dict[str,barman.cloud.VolumeMetadata] volumes: Metadata for the volumes
            to be backed up.
        """

    @abstractmethod
    def delete_snapshot_backup(self, backup_info):
        """
        Delete all snapshots for the supplied backup.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information.
        """

    @abstractmethod
    def get_attached_volumes(self, instance_name, disks=None, fail_on_missing=True):
        """
        Returns metadata for the volumes attached to this instance.

        Queries the cloud provider for metadata relating to the volumes attached to
        the named instance and returns a dict of `VolumeMetadata` objects, keyed by
        disk name.

        If the optional disks parameter is supplied then this method must return
        metadata for the disks in the supplied list only. A SnapshotBackupException
        must be raised if any of the supplied disks are not found to be attached to
        the instance.

        If the optional disks parameter is supplied then this method returns metadata
        for the disks in the supplied list only. If fail_on_missing is set to True then
        a SnapshotBackupException is raised if any of the supplied disks are not found
        to be attached to the instance.

        If the disks parameter is not supplied then this method must return a
        VolumeMetadata for all disks attached to this instance.

        :param str instance_name: The name of the VM instance to which the disks
            to be backed up are attached.
        :param list[str]|None disks: A list containing the names of disks to be
            backed up.
        :param bool fail_on_missing: Fail with a SnapshotBackupException if any
            specified disks are not attached to the instance.
        :rtype: dict[str, VolumeMetadata]
        :return: A dict of VolumeMetadata objects representing each volume
            attached to the instance, keyed by volume identifier.
        """

    @abstractmethod
    def instance_exists(self, instance_name):
        """
        Determine whether the named instance exists.

        :param str instance_name: The name of the VM instance to which the disks
            to be backed up are attached.
        :rtype: bool
        :return: True if the named instance exists, False otherwise.
        """


class VolumeMetadata(object):
    """
    Represents metadata for a single volume attached to a cloud VM.

    The main purpose of this class is to allow calling code to determine the mount
    point and mount options for an attached volume without needing to know the
    details of how these are determined for a specific cloud provider.

    Implementations must therefore:

    - Store metadata obtained from the cloud provider which can be used to resolve
      this volume to an attached and mounted volume on the instance. This will
      typically be a device name or something which can be resolved to a device name.
    - Provide an implementation of `resolve_mounted_volume` which executes commands
      on the cloud VM via a supplied UnixLocalCommand object in order to set the
      _mount_point and _mount_options properties.

    If the volume was cloned from a snapshot then the source snapshot identifier
    must also be stored in this class so that calling code can determine if/how/where
    a volume cloned from a given snapshot is mounted.
    """

    def __init__(self):
        self._mount_point = None
        self._mount_options = None

    @abstractmethod
    def resolve_mounted_volume(self, cmd):
        """
        Resolve the mount point and mount options using shell commands.

        This method must use cmd together with any additional private properties
        available in the provider-specific implementation in order to resolve the
        mount point and mount options for this volume.

        :param UnixLocalCommand cmd: Wrapper for local/remote commands on the instance
            to which this volume is attached.
        """

    @abstractproperty
    def source_snapshot(self):
        """
        The source snapshot from which this volume was cloned.

        :rtype: str|None
        :return: A snapshot identifier.
        """

    @property
    def mount_point(self):
        """
        The mount point at which this volume is currently mounted.

        This must be resolved using metadata obtained from the cloud provider which
        describes how the volume is attached to the VM.
        """
        return self._mount_point

    @property
    def mount_options(self):
        """
        The mount options with which this device is currently mounted.

        This must be resolved using metadata obtained from the cloud provider which
        describes how the volume is attached to the VM.
        """
        return self._mount_options


class SnapshotMetadata(object):
    """
    Represents metadata for a single snapshot.

    This class holds the snapshot metadata common to all snapshot providers.
    Currently this is the mount_options and the mount_point of the source disk for the
    snapshot at the time of the backup.

    The `identifier` and `device` properties are part of the public interface used
    within Barman so that the calling code can access the snapshot identifier and
    device path without having to worry about how these are composed from the snapshot
    metadata for each cloud provider.

    Specializations of this class must:

        1. Add their provider-specific fields to `_provider_fields`.
        2. Implement the `identifier` abstract property so that it returns a value which
           can identify the snapshot via the cloud provider API. An example would be
           the snapshot short name in GCP.
        3. Implement the `device` abstract property so that it returns a full device
           path to the location at which the source disk was attached to the compute
           instance.
    """

    _provider_fields = ()

    def __init__(self, mount_options=None, mount_point=None):
        """
        Constructor accepts properties generic to all snapshot providers.

        :param str mount_options: The mount options used for the source disk at the
            time of the backup.
        :param str mount_point: The mount point of the source disk at the time of
            the backup.
        """
        self.mount_options = mount_options
        self.mount_point = mount_point

    @classmethod
    def from_dict(cls, info):
        """
        Create a new SnapshotMetadata object from the raw metadata dict.

        This function will set the generic fields supported by SnapshotMetadata before
        iterating through fields listed in `cls._provider_fields`. This means
        subclasses do not need to override this method, they just need to add their
        fields to their own `_provider_fields`.

        :param dict[str,str] info: The raw snapshot metadata.
        :rtype: SnapshotMetadata
        """
        snapshot_info = cls()
        if "mount" in info:
            for field in ("mount_options", "mount_point"):
                try:
                    setattr(snapshot_info, field, info["mount"][field])
                except KeyError:
                    pass
        for field in cls._provider_fields:
            try:
                setattr(snapshot_info, field, info["provider"][field])
            except KeyError:
                pass
        return snapshot_info

    def to_dict(self):
        """
        Seralize this SnapshotMetadata object as a raw dict.

        This function will create a dict with the generic fields supported by
        SnapshotMetadata before iterating through fields listed in
        `self._provider_fields` and adding them to a special `provider` field.
        As long as they add their provider-specific fields to `_provider_fields`
        then subclasses do not need to override this method.

        :rtype: dict
        :return: A dict containing the metadata for this snapshot.
        """
        info = {
            "mount": {
                "mount_options": self.mount_options,
                "mount_point": self.mount_point,
            },
        }
        if len(self._provider_fields) > 0:
            info["provider"] = {}
            for field in self._provider_fields:
                info["provider"][field] = getattr(self, field)
        return info

    @abstractproperty
    def identifier(self):
        """
        An identifier which can reference the snapshot via the cloud provider.

        Subclasses must ensure this returns a string which can be used by Barman to
        reference the snapshot when interacting with the cloud provider API.

        :rtype: str
        :return: A snapshot identifier.
        """


class SnapshotsInfo(object):
    """
    Represents the snapshots_info field of backup metadata stored in BackupInfo.

    This class holds the metadata for a snapshot backup which is common to all
    snapshot providers. This is the list of SnapshotMetadata objects representing the
    individual snapshots.

    Specializations of this class must:

        1. Add their provider-specific fields to `_provider_fields`.
        2. Set their `_snapshot_metadata_cls` property to the required specialization of
           SnapshotMetadata.
        3. Set the provider property to the required value.
    """

    _provider_fields = ()
    _snapshot_metadata_cls = SnapshotMetadata

    def __init__(self, snapshots=None):
        """
        Constructor saves the list of snapshots if it is provided.

        :param list[SnapshotMetadata] snapshots: A list of metadata objects for each
            snapshot.
        """
        if snapshots is None:
            snapshots = []
        self.snapshots = snapshots
        self.provider = None

    @classmethod
    def from_dict(cls, info):
        """
        Create a new SnapshotsInfo object from the raw metadata dict.

        This function will iterate through fields listed in `cls._provider_fields`
        and add them to the instantiated object. It will then create a new
        SnapshotMetadata object (of the type specified in `cls._snapshot_metadata_cls`)
        for each snapshot in the raw dict.

        Subclasses do not need to override this method, they just need to add their
        fields to their own `_provider_fields` and override `_snapshot_metadata_cls`.

        :param dict info: The raw snapshots_info dict.
        :rtype: SnapshotsInfo
        :return: The SnapshotsInfo object representing the raw dict.
        """
        snapshots_info = cls()
        for field in cls._provider_fields:
            try:
                setattr(snapshots_info, field, info["provider_info"][field])
            except KeyError:
                pass
        snapshots_info.snapshots = [
            cls._snapshot_metadata_cls.from_dict(snapshot_info)
            for snapshot_info in info["snapshots"]
        ]
        return snapshots_info

    def to_dict(self):
        """
        Seralize this SnapshotMetadata object as a raw dict.

        This function will create a dict with the generic fields supported by
        SnapshotMetadata before iterating through fields listed in
        `self._provider_fields` and adding them to a special `provider_info` field.
        The SnapshotMetadata objects in `self.snapshots` are serialized into the
        dict via their own `to_dict` function.

        As long as they add their provider-specific fields to `_provider_fields`
        then subclasses do not need to override this method.

        :rtype: dict
        :return: A dict containing the metadata for this snapshot.
        """
        info = {"provider": self.provider}
        if len(self._provider_fields) > 0:
            info["provider_info"] = {}
            for field in self._provider_fields:
                info["provider_info"][field] = getattr(self, field)
        info["snapshots"] = [
            snapshot_info.to_dict() for snapshot_info in self.snapshots
        ]
        return info
