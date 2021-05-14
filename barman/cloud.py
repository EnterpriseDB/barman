# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2021
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

import bz2
import collections
import copy
import datetime
import errno
import gzip
import json
import logging
import multiprocessing
import operator
import os
import shutil
import signal
import tarfile
from functools import partial
from io import BytesIO, RawIOBase
from tempfile import NamedTemporaryFile

from barman.backup_executor import (ConcurrentBackupStrategy,
                                    ExclusiveBackupStrategy)
from barman.fs import path_allowed
from barman.infofile import BackupInfo
from barman.postgres_plumbing import EXCLUDE_LIST, PGDATA_EXCLUDE_LIST
from barman.utils import (BarmanEncoder, force_str, human_readable_timedelta,
                          pretty_size, total_seconds)

try:
    import boto3
    from botocore.exceptions import ClientError, EndpointConnectionError
except ImportError:
    raise SystemExit("Missing required python module: boto3")

try:
    # Python 3.x
    from urllib.parse import urlparse
except ImportError:
    # Python 2.x
    from urlparse import urlparse

try:
    # Python 3.x
    from queue import Empty as EmptyQueue
except ImportError:
    # Python 2.x
    from Queue import Empty as EmptyQueue


# S3 multipart upload limitations
# http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html
MAX_CHUNKS_PER_FILE = 10000
MIN_CHUNK_SIZE = 5 << 20

# S3 permit a maximum of 5TB per file
# https://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html
# This is a hard limit, while our upload procedure can go over the specified
# MAX_ARCHIVE_SIZE - so we set a maximum of 1TB per file
MAX_ARCHIVE_SIZE = 1 << 40

BUFSIZE = 16 * 1024
LOGGING_FORMAT = "%(asctime)s [%(process)s] %(levelname)s: %(message)s"


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


class CloudUploadingError(Exception):
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
                self.fileobj.write(
                    tarfile.NUL * (tarfile.BLOCKSIZE - remainder))
                blocks += 1
            self.offset += blocks * tarfile.BLOCKSIZE

        self.members.append(tarinfo)


class S3TarUploader(object):

    # This is the method we use to create new buffers
    # We use named temporary files, so we can pass them by name to
    # other processes
    _buffer = partial(NamedTemporaryFile, delete=False,
                      prefix='barman-upload-', suffix='.part')

    def __init__(self, cloud_interface, key,
                 compression=None, chunk_size=MIN_CHUNK_SIZE):
        """
        A tar archive that resides on S3

        :param CloudInterface cloud_interface: cloud interface instance
        :param str key: path inside the bucket
        :param str compression: required compression
        :param int chunk_size: the upload chunk size
        """
        self.cloud_interface = cloud_interface
        self.key = key
        self.mpu = None
        self.chunk_size = max(chunk_size, MIN_CHUNK_SIZE)
        self.buffer = None
        self.counter = 0
        tar_mode = 'w|%s' % (compression or '')
        self.tar = TarFileIgnoringTruncate.open(fileobj=self,
                                                mode=tar_mode)
        self.size = 0
        self.stats = None

    def write(self, buf):
        if self.buffer and self.buffer.tell() > self.chunk_size:
            self.flush()
        if not self.buffer:
            self.buffer = self._buffer()
        self.buffer.write(buf)
        self.size += len(buf)

    def flush(self):
        if not self.mpu:
            self.mpu = self.cloud_interface.create_multipart_upload(self.key)
        self.buffer.flush()
        self.buffer.seek(0, os.SEEK_SET)
        self.counter += 1
        self.cloud_interface.async_upload_part(
            mpu=self.mpu,
            key=self.key,
            body=self.buffer,
            part_number=self.counter)
        self.buffer.close()
        self.buffer = None

    def close(self):
        if self.tar:
            self.tar.close()
        self.flush()
        self.cloud_interface.async_complete_multipart_upload(
            mpu=self.mpu,
            key=self.key,
            parts_count=self.counter,
        )
        self.stats = self.cloud_interface.wait_for_multipart_upload(self.key)


class S3UploadController(object):
    def __init__(self, cloud_interface, key_prefix, max_archive_size,
                 compression):
        """
        Create a new controller that upload the backup in S3

        :param CloudInterface cloud_interface: cloud interface instance
        :param str|None key_prefix: path inside the bucket
        :param int max_archive_size: the maximum size of an archive
        :param str|None compression: required compression
        """

        self.cloud_interface = cloud_interface
        if key_prefix and key_prefix[0] == '/':
            key_prefix = key_prefix[1:]
        self.key_prefix = key_prefix
        if max_archive_size < MAX_ARCHIVE_SIZE:
            self.max_archive_size = max_archive_size
        else:
            logging.warning("max-archive-size too big. Capping it to to %s",
                            pretty_size(MAX_ARCHIVE_SIZE))
            self.max_archive_size = MAX_ARCHIVE_SIZE
        # We aim to a maximum of MAX_CHUNKS_PER_FILE / 2 chinks per file
        self.chunk_size = 2 * int(max_archive_size / MAX_CHUNKS_PER_FILE)
        self.compression = compression
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
        return "".join(components)

    def _get_tar(self, name):
        """
        Get a named S3 tar file.
        Subsequent call with the same name return the same name
        :param str name: tar name
        :rtype: tarfile.TarFile
        """
        if name not in self.tar_list or not self.tar_list[name]:

            self.tar_list[name] = [S3TarUploader(
                cloud_interface=self.cloud_interface,
                key=os.path.join(self.key_prefix, self._build_dest_name(name)),
                compression=self.compression,
                chunk_size=self.chunk_size,
            )]
        # If the current uploading file size is over DEFAULT_MAX_TAR_SIZE
        # Close the current file and open the next part
        uploader = self.tar_list[name][-1]
        if uploader.size > self.max_archive_size:
            uploader.close()
            uploader = S3TarUploader(
                cloud_interface=self.cloud_interface,
                key=os.path.join(
                    self.key_prefix,
                    self._build_dest_name(name, len(self.tar_list[name]))),
                compression=self.compression,
                chunk_size=self.chunk_size,
            )
            self.tar_list[name].append(uploader)
        return uploader.tar

    def upload_directory(self, label, src, dst, exclude=None, include=None):
        logging.info("Uploading '%s' directory '%s' as '%s'",
                     label, src, self._build_dest_name(dst))
        for root, dirs, files in os.walk(src):
            tar_root = os.path.relpath(root, src)
            if not path_allowed(exclude, include,
                                tar_root, True):
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
                if not path_allowed(exclude, include,
                                    tar_item, False):
                    continue
                logging.debug("Uploading %s", tar_item)
                try:
                    self._get_tar(dst).add(os.path.join(root, item),
                                           arcname=tar_item)
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
        logging.info("Uploading '%s' file from '%s' to '%s' with path '%s'",
                     label, src, self._build_dest_name(dst), path)
        tar = self._get_tar(dst)
        tar.add(src, arcname=path)

    def add_fileobj(self, label, fileobj, dst, path,
                    mode=None, uid=None, gid=None):
        logging.info("Uploading '%s' file to '%s' with path '%s'",
                     label, self._build_dest_name(dst), path)
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
                self.upload_stats[name] = [tar.stats
                                           for tar in self.tar_list[name]]
            self.tar_list[name] = None

        # Store the end time
        self.copy_end_time = datetime.datetime.now()

    def statistics(self):
        """
        Return statistics about the S3UploadController object.

        :rtype: dict
        """
        logging.info("Calculating backup statistics")

        # This method can only run at the end of a non empty copy
        assert self.copy_end_time
        assert self.upload_stats

        # Initialise the result calculating the total runtime
        stat = {
            'total_time': total_seconds(
                self.copy_end_time - self.copy_start_time),
            'number_of_workers': self.cloud_interface.worker_processes_count,
            # Cloud uploads have no analysis
            'analysis_time': 0,
            'analysis_time_per_item': {},
            'copy_time_per_item': {},
            'serialized_copy_time_per_item': {},
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
                    'Calculating statistics for file %s, index %s, data: %s',
                    name, index, json.dumps(data, indent=2, sort_keys=True,
                                            cls=BarmanEncoder))
                if upload_start is None or upload_start > data['start_time']:
                    upload_start = data['start_time']
                if upload_end is None or upload_end < data['end_time']:
                    upload_end = data['end_time']
                if name_start is None or name_start > data['start_time']:
                    name_start = data['start_time']
                if name_end is None or name_end < data['end_time']:
                    name_end = data['end_time']
                parts = data['parts']
                for num in parts:
                    part = parts[num]
                    total_time += part['end_time'] - part['start_time']
                stat['serialized_copy_time_per_item'][name] = total_seconds(
                    total_time)
                serialized_time += total_time
            # Cloud uploads have no analysis
            stat['analysis_time_per_item'][name] = 0
            stat['copy_time_per_item'][name] = total_seconds(
                name_end - name_start)

        # Store the total time spent by copying
        stat['copy_time'] = total_seconds(upload_end - upload_start)
        stat['serialized_copy_time'] = total_seconds(serialized_time)

        return stat


class FileUploadStatistics(dict):
    def __init__(self, *args, **kwargs):
        super(FileUploadStatistics, self).__init__(*args, **kwargs)
        start_time = datetime.datetime.now()
        self.setdefault('status', 'uploading')
        self.setdefault('start_time', start_time)
        self.setdefault('parts', {})

    def set_part_end_time(self, part_number, end_time):
        part = self['parts'].setdefault(part_number, {
            'part_number': part_number
        })
        part['end_time'] = end_time

    def set_part_start_time(self, part_number, start_time):
        part = self['parts'].setdefault(part_number, {
            'part_number': part_number
        })
        part['start_time'] = start_time


class StreamingBodyIO(RawIOBase):
    """
    Wrap a boto StreamingBody in the IOBase API.
    """
    def __init__(self, body):
        self.body = body

    def readable(self):
        return True

    def read(self, n=-1):
        n = None if n < 0 else n
        return self.body.read(n)


class CloudInterface(object):
    def __init__(self, url, encryption, jobs=2,
                 profile_name=None, endpoint_url=None):
        """
        Create a new S3 interface given the S3 destination url and the profile
        name

        :param str url: Full URL of the cloud destination/source
        :param str|None encryption: Encryption type string
        :param int jobs: How many sub-processes to use for asynchronous
          uploading, defaults to 2.
        :param str profile_name: Amazon auth profile identifier
        :param str endpoint_url: override default endpoint detection strategy
          with this one
        """
        self.url = url
        self.profile_name = profile_name
        self.encryption = encryption
        self.endpoint_url = endpoint_url

        # Extract information from the destination URL
        parsed_url = urlparse(url)
        # If netloc is not present, the s3 url is badly formatted.
        if parsed_url.netloc == '' or parsed_url.scheme != 's3':
            raise ValueError('Invalid s3 URL address: %s' % url)
        self.bucket_name = parsed_url.netloc
        self.bucket_exists = None
        self.path = parsed_url.path.lstrip('/')

        # Build a session, so we can extract the correct resource
        session = boto3.Session(profile_name=profile_name)
        self.s3 = session.resource('s3', endpoint_url=endpoint_url)

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

    def abort(self):
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

        self.queue = multiprocessing.JoinableQueue(
            maxsize=self.worker_processes_count)
        self.result_queue = multiprocessing.Queue()
        self.errors_queue = multiprocessing.Queue()
        self.done_queue = multiprocessing.Queue()
        for process_number in range(self.worker_processes_count):
            process = multiprocessing.Process(
                target=self.worker_process_main,
                args=(process_number,))
            process.start()
            self.worker_processes.append(process)

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
            stats.set_part_end_time(result["part_number"], result['end_time'])

        for key in touched_keys:
            self.parts_db[key] = sorted(
                self.parts_db[key],
                key=operator.itemgetter("PartNumber"))

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
        self.abort()
        raise CloudUploadingError(self.error)

    def worker_process_main(self, process_number):
        """
        Repeatedly grab a task from the queue and execute it, until a task
        containing "None" is grabbed, indicating that the process must stop.

        :param int process_number: the process number, used in the logging
        output
        """
        logging.info("Upload process started (worker %s)", process_number)

        # We create a new session instead of reusing the one
        # from the parent process to avoid any race condition
        session = boto3.Session(profile_name=self.profile_name)
        self.s3 = session.resource('s3', endpoint_url=self.endpoint_url)

        while True:
            task = self.queue.get()
            if not task:
                self.queue.task_done()
                break

            try:
                self.worker_process_execute_job(task, process_number)
            except Exception as exc:
                logging.error('Upload error: %s (worker %s)',
                              force_str(exc), process_number)
                logging.debug('Exception details:', exc_info=exc)
                self.errors_queue.put(force_str(exc))
            except KeyboardInterrupt:
                if not self.abort_requested:
                    logging.info('Got abort request: upload cancelled '
                                 '(worker %s)', process_number)
                    self.abort_requested = True
            finally:
                self.queue.task_done()

        logging.info("Upload process stopped (worker %s)", process_number)

    def worker_process_execute_job(self, task, process_number):
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
                    "Skipping '%s', part '%s' (worker %s)" % (
                        task["key"],
                        task["part_number"],
                        process_number))
                os.unlink(task["body"])
                return
            else:
                logging.info(
                    "Uploading '%s', part '%s' (worker %s)" % (
                        task["key"],
                        task["part_number"],
                        process_number))
                with open(task["body"], "rb") as fp:
                    part = self.upload_part(
                        task["mpu"],
                        task["key"],
                        fp,
                        task["part_number"])
                os.unlink(task["body"])
                self.result_queue.put(
                    {
                        "key": task["key"],
                        "part_number": task["part_number"],
                        "end_time": datetime.datetime.now(),
                        "part": part,
                    })
        elif task["job_type"] == "complete_multipart_upload":
            if self.abort_requested:
                logging.info(
                    "Aborting %s (worker %s)" % (
                        task["key"],
                        process_number))
                self.abort_multipart_upload(
                    task["mpu"],
                    task["key"])
                self.done_queue.put(
                    {
                        "key": task["key"],
                        "end_time": datetime.datetime.now(),
                        "status": "aborted"
                    })
            else:
                logging.info(
                    "Completing '%s' (worker %s)" % (
                        task["key"],
                        process_number))
                self.complete_multipart_upload(
                    task["mpu"],
                    task["key"],
                    task["parts"])
                self.done_queue.put(
                    {
                        "key": task["key"],
                        "end_time": datetime.datetime.now(),
                        "status": "done"
                    })
        else:
            raise ValueError("Unknown task: %s", repr(task))

    def test_connectivity(self):
        """
        Test the S3 connectivity trying to access a bucket
        """
        try:
            # We are not even interested in the existence of the bucket,
            # we just want to try if aws is reachable
            self.bucket_exists = self.check_bucket_existence(self.bucket_name)
            return True
        except EndpointConnectionError as exc:
            logging.error("Can't connect to Amazon AWS/S3: %s", exc)
            return False

    def check_bucket_existence(self, bucket_name):
        """
        Search for the target bucket
        """
        try:
            # Search the bucket on s3
            self.s3.meta.client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as exc:
            # If a client error is thrown, then check the error code.
            # If code was 404, then the bucket does not exist
            error_code = exc.response['Error']['Code']
            if error_code == '404':
                return False
            # Otherwise there is nothing else to do than re-raise the original
            # exception
            raise

    def setup_bucket(self):
        """
        Search for the target bucket. Create it if not exists
        """
        if self.bucket_exists is None:
            self.bucket_exists = self.check_bucket_existence(self.bucket_name)

        # Create the bucket if it doesn't exist
        if not self.bucket_exists:
            # Get the current region from client.
            # Do not use session.region_name here because it may be None
            region = self.s3.meta.client.meta.region_name
            logging.info(
                "Bucket '%s' does not exist, creating it on region '%s'",
                self.bucket_name, region)
            create_bucket_config = {
                'ACL': 'private',
            }
            # The location constraint is required during bucket creation
            # for all regions outside of us-east-1. This constraint cannot
            # be specified in us-east-1; specifying it in this region
            # results in a failure, so we will only
            # add it if we are deploying outside of us-east-1.
            # See https://github.com/boto/boto3/issues/125
            if region != 'us-east-1':
                create_bucket_config['CreateBucketConfiguration'] = {
                    'LocationConstraint': region,
                }
            self.s3.Bucket(self.bucket_name).create(**create_bucket_config)
            self.bucket_exists = True

    def list_bucket(self, prefix='', delimiter='/'):
        """
        List bucket content in a directory manner
        :param str prefix:
        :param str delimiter:
        :return: List of objects and dirs right under the prefix
        """
        if prefix.startswith(delimiter):
            prefix = prefix.lstrip(delimiter)

        res = self.s3.meta.client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix,
            Delimiter=delimiter)

        # List "folders"
        keys = res.get("CommonPrefixes")
        if keys is not None:
            for k in keys:
                yield k.get("Prefix")

        # List "files"
        objects = res.get("Contents")
        if objects is not None:
            for o in objects:
                yield o.get("Key")

    def download_file(self, key, dest_path, decompress):
        """
        Download a file from S3

        :param str key: The S3 key to download
        :param str dest_path: Where to put the destination file
        :param bool decompress: Whenever to decompress this file or not
        """
        # Open the remote file
        obj = self.s3.Object(self.bucket_name, key)
        remote_file = obj.get()['Body']

        # Write the dest file in binary mode
        with open(dest_path, 'wb') as dest_file:
            # If the file is not compressed, just copy its content
            if not decompress:
                shutil.copyfileobj(remote_file, dest_file)
                return

            if decompress == 'gzip':
                source_file = gzip.GzipFile(fileobj=remote_file, mode='rb')
            elif decompress == 'bzip2':
                source_file = bz2.BZ2File(remote_file, 'rb')
            else:
                raise ValueError("Unknown compression type: %s" % decompress)

            with source_file:
                shutil.copyfileobj(source_file, dest_file)

    def remote_open(self, key):
        """
        Open a remote S3 object and returns a readable stream

        Returns None is the the Key does not exist
        """
        try:
            obj = self.s3.Object(self.bucket_name, key)
            return StreamingBodyIO(obj.get()['Body'])
        except ClientError as exc:
            error_code = exc.response['Error']['Code']
            if error_code == 'NoSuchKey':
                return None
            else:
                raise

    def extract_tar(self, key, dst):
        """
        Extract a tar archive from cloud to the local directory
        """
        extension = os.path.splitext(key)[-1]
        compression = '' if extension == '.tar' else extension[1:]
        tar_mode = 'r|%s' % compression
        obj = self.s3.Object(self.bucket_name, key)
        fileobj = obj.get()['Body']
        with tarfile.open(fileobj=fileobj, mode=tar_mode) as tf:
            tf.extractall(path=dst)

    def upload_fileobj(self, fileobj, key):
        """
        Synchronously upload the content of a file-like object to a cloud key
        """
        additional_args = {}
        if self.encryption:
            additional_args['ServerSideEncryption'] = self.encryption

        self.s3.meta.client.upload_fileobj(
            Fileobj=fileobj,
            Bucket=self.bucket_name,
            Key=key,
            ExtraArgs=additional_args)

    def create_multipart_upload(self, key):
        """
        Create a new multipart upload

        :param key: The key to use in the cloud service
        :return: The multipart upload handle
        """
        return self.s3.meta.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=key)

    def async_upload_part(self, mpu, key, body, part_number):
        """
        Asynchronously upload a part into a multipart upload

        :param mpu: The multipart upload handle
        :param str key: The key to use in the cloud service
        :param any body: A stream-like object to upload
        :param int part_number: Part number, starting from 1
        :return: The part handle
        """

        # If an error has already been reported, do nothing
        if self.error:
            return

        self._ensure_async()
        self._handle_async_errors()

        # Save the upload start time of the part
        stats = self.upload_stats[key]
        stats.set_part_start_time(part_number, datetime.datetime.now())

        # If the body is a named temporary file use it directly
        # WARNING: this imply that the file will be deleted after the upload
        if hasattr(body, 'name') and hasattr(body, 'delete') and \
                not body.delete:
            fp = body
        else:
            # Write a temporary file with the part contents
            with NamedTemporaryFile(delete=False) as fp:
                shutil.copyfileobj(body, fp, BUFSIZE)

        # Pass the job to the uploader process
        self.queue.put({
            "job_type": "upload_part",
            "mpu": mpu,
            "key": key,
            "body": fp.name,
            "part_number": part_number,
        })

    def upload_part(self, mpu, key, body, part_number):
        """
        Upload a part into this multipart upload

        :param mpu: The multipart upload handle
        :param str key: The key to use in the cloud service
        :param object body: A stream-like object to upload
        :param int part_number: Part number, starting from 1
        :return: The part handle
        """
        part = self.s3.meta.client.upload_part(
            Body=body,
            Bucket=self.bucket_name,
            Key=key,
            UploadId=mpu["UploadId"],
            PartNumber=part_number)
        return {
            'PartNumber': part_number,
            'ETag': part['ETag'],
        }

    def async_complete_multipart_upload(self, mpu, key, parts_count):
        """
        Asynchronously finish a certain multipart upload. This method grant
        that the final S3 call will happen after all the already scheduled
        parts have been uploaded.

        :param mpu:  The multipart upload handle
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

        # Finish the job in S3 to the uploader process
        self.queue.put({
            "job_type": "complete_multipart_upload",
            "mpu": mpu,
            "key": key,
            "parts": self.parts_db[key],
        })
        del self.parts_db[key]

    def complete_multipart_upload(self, mpu, key, parts):
        """
        Finish a certain multipart upload

        :param mpu:  The multipart upload handle
        :param str key: The key to use in the cloud service
        :param parts: The list of parts composing the multipart upload
        """
        self.s3.meta.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=key,
            UploadId=mpu["UploadId"],
            MultipartUpload={"Parts": parts})

    def abort_multipart_upload(self, mpu, key):
        """
        Abort a certain multipart upload

        :param mpu:  The multipart upload handle
        :param str key: The key to use in the cloud service
        """
        self.s3.meta.client.abort_multipart_upload(
            Bucket=self.bucket_name,
            Key=key,
            UploadId=mpu["UploadId"])

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
        while self.upload_stats[key]['status'] == 'uploading':
            # Wait for all the current jobs to be completed and
            # receive all available updates on worker status
            self._retrieve_results()

        return self.upload_stats[key]


class S3BackupUploader(object):
    """
    S3 upload client
    """

    def __init__(self, server_name, postgres, cloud_interface,
                 max_archive_size, compression=None):
        """
        Object responsible for handling interactions with S3

        :param str server_name: The name of the server as configured in Barman
        :param PostgreSQLConnection postgres: The PostgreSQL connection info
        :param CloudInterface cloud_interface: The interface to use to
          upload the backup
        :param int max_archive_size: the maximum size of an uploading archive
        :param str compression: Compression algorithm to use
        """

        self.compression = compression
        self.server_name = server_name
        self.postgres = postgres
        self.cloud_interface = cloud_interface
        self.max_archive_size = max_archive_size

        # Stats
        self.copy_start_time = None
        self.copy_end_time = None

    def backup_copy(self, controller, backup_info):
        """
        Perform the actual copy of the backup uploading it to S3.

        First, it copies one tablespace at a time, then the PGDATA directory,
        and finally configuration files (if outside PGDATA).
        Bandwidth limitation, according to configuration, is applied in
        the process.
        This method is the core of base backup copy using Rsync+Ssh.

        :param barman.cloud.S3UploadController controller: upload controller
        :param barman.infofile.BackupInfo backup_info: backup information
        """

        # Store the start time
        self.copy_start_time = datetime.datetime.now()

        # List of paths to be excluded by the PGDATA copy
        exclude = []

        # Process every tablespace
        if backup_info.tablespaces:
            for tablespace in backup_info.tablespaces:
                # If the tablespace location is inside the data directory,
                # exclude and protect it from being copied twice during
                # the data directory copy
                if tablespace.location.startswith(backup_info.pgdata + '/'):
                    exclude += [
                        tablespace.location[len(backup_info.pgdata):]]

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
                    src=tablespace.location,
                    dst='%s' % tablespace.oid,
                    exclude=['/*'] + EXCLUDE_LIST,
                    include=['/PG_%s_*' %
                             self.postgres.server_major_version],
                )

        # Copy PGDATA directory
        controller.upload_directory(
            label='pgdata',
            src=backup_info.pgdata,
            dst='data',
            exclude=PGDATA_EXCLUDE_LIST + EXCLUDE_LIST + exclude
        )

        # At last copy pg_control
        controller.add_file(
            label='pg_control',
            src='%s/global/pg_control' % backup_info.pgdata,
            dst='data',
            path='global/pg_control'
        )

        # Copy configuration files (if not inside PGDATA)
        external_config_files = backup_info.get_external_config_files()
        included_config_files = []
        for config_file in external_config_files:
            # Add included files to a list, they will be handled later
            if config_file.file_type == 'include':
                included_config_files.append(config_file)
                continue

            # If the ident file is missing, it isn't an error condition
            # for PostgreSQL.
            # Barman is consistent with this behavior.
            optional = False
            if config_file.file_type == 'ident_file':
                optional = True

            # Create the actual copy jobs in the controller
            controller.add_file(
                label=config_file.file_type,
                src=config_file.path,
                dst='data',
                path=os.path.basename(config_file.path),
                optional=optional,
            )

        # Check for any include directives in PostgreSQL configuration
        # Currently, include directives are not supported for files that
        # reside outside PGDATA. These files must be manually backed up.
        # Barman will emit a warning and list those files
        if any(included_config_files):
            msg = ("The usage of include directives is not supported "
                   "for files that reside outside PGDATA.\n"
                   "Please manually backup the following files:\n"
                   "\t%s\n" %
                   "\n\t".join(icf.path for icf in included_config_files))
            logging.warning(msg)

    def backup(self):
        """
        Upload a Backup  to S3
        """
        server_name = 'cloud'
        backup_info = BackupInfo(
            backup_id=datetime.datetime.now().strftime('%Y%m%dT%H%M%S'),
            server_name=server_name,
        )
        backup_info.set_attribute("systemid", self.postgres.get_systemid())
        key_prefix = os.path.join(
            self.cloud_interface.path,
            self.server_name,
            'base',
            backup_info.backup_id
        )
        controller = S3UploadController(
            self.cloud_interface,
            key_prefix,
            self.max_archive_size,
            self.compression,
        )
        if self.postgres.server_version >= 90600 \
                or self.postgres.has_pgespresso:
            strategy = ConcurrentBackupStrategy(self.postgres, server_name)
        else:
            strategy = ExclusiveBackupStrategy(self.postgres, server_name)
        logging.info("Starting backup '%s'", backup_info.backup_id)
        strategy.start_backup(backup_info)
        try:
            self.backup_copy(controller, backup_info)
            logging.info("Stopping backup '%s'", backup_info.backup_id)
            strategy.stop_backup(backup_info)

            # Create a restore point after a backup
            target_name = 'barman_%s' % backup_info.backup_id
            self.postgres.create_restore_point(target_name)

            # Free the Postgres connection
            self.postgres.close()

            # Eventually, add the backup_label from the backup_info
            if backup_info.backup_label:
                pgdata_stat = os.stat(backup_info.pgdata)
                controller.add_fileobj(
                    label='backup_label',
                    fileobj=BytesIO(backup_info.backup_label.encode('UTF-8')),
                    dst='data',
                    path='backup_label',
                    uid=pgdata_stat.st_uid,
                    gid=pgdata_stat.st_gid,
                )

            # Closing the controller will finalize all the running uploads
            controller.close()

            # Store the end time
            self.copy_end_time = datetime.datetime.now()

            # Store statistics about the copy
            backup_info.set_attribute("copy_stats", controller.statistics())

            # Set the backup status as DONE
            backup_info.set_attribute("status", BackupInfo.DONE)

        # Use BaseException instead of Exception to catch events like
        # KeyboardInterrupt (e.g.: CTRL-C)
        except BaseException as exc:
            # Mark the backup as failed and exit
            self.handle_backup_errors("uploading data", backup_info, exc)
            raise SystemExit(1)
        finally:
            try:
                with BytesIO() as backup_info_file:
                    backup_info.save(file_object=backup_info_file)
                    backup_info_file.seek(0, os.SEEK_SET)
                    key = os.path.join(controller.key_prefix, 'backup.info')
                    logging.info("Uploading '%s'", key)
                    self.cloud_interface.upload_fileobj(backup_info_file, key)
            except BaseException as exc:
                # Mark the backup as failed and exit
                self.handle_backup_errors("uploading backup.info file",
                                          backup_info, exc)
                raise SystemExit(1)

        logging.info("Backup end at LSN: %s (%s, %08X)",
                     backup_info.end_xlog,
                     backup_info.end_wal,
                     backup_info.end_offset)
        logging.info(
            "Backup completed (start time: %s, elapsed time: %s)",
            self.copy_start_time,
            human_readable_timedelta(
                datetime.datetime.now() - self.copy_start_time))

    def handle_backup_errors(self, action, backup_info, exc):
        """
        Mark the backup as failed and exit

        :param str action: the upload phase that has failed
        :param barman.infofile.BackupInfo backup_info: the backup info file
        :param BaseException exc: the exception that caused the failure
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
                "error",
                "failure %s (%s)" % (action, msg_lines[0]))
        logging.error("Backup failed %s (%s)", action, msg_lines[0])
        logging.debug('Exception details:', exc_info=exc)


class BackupFileInfo(object):
    def __init__(self, oid=None, base=None, path=None, compression=None):
        self.oid = oid
        self.base = base
        self.path = path
        self.compression = compression
        self.additional_files = []


class S3BackupCatalog(object):
    """
    S3 backup catalog
    """
    def __init__(self, cloud_interface,
                 server_name):
        """
        Object responsible for retrievin backup catalog from S3

        :param CloudInterface cloud_interface: The interface to use to
          upload the backup
        :param str server_name: The name of the server as configured in Barman
        """

        self.cloud_interface = cloud_interface
        self.server_name = server_name
        self.prefix = os.path.join(
            self.cloud_interface.path,
            self.server_name,
            'base')
        self._backup_list = None

    def get_backup_list(self):
        """
        Retrieve the list of available backup from S3

        :rtype: Dict[str,BackupInfo]
        """
        if self._backup_list is None:

            backup_list = {}

            # get backups metadata
            for backup_dir in self.cloud_interface.list_bucket(
                    self.prefix + '/'):
                # We want only the directories
                if backup_dir[-1] != '/':
                    continue
                backup_id = os.path.basename(backup_dir.rstrip('/'))
                backup_info = self.get_backup_info(backup_id)

                if backup_info:
                    backup_list[backup_id] = backup_info
            self._backup_list = backup_list
        return self._backup_list

    def get_backup_info(self, backup_id):
        """
        Load a BackupInfo from S3

        :param str backup_id: The backup id to load
        :rtype: BackupInfo
        """
        backup_info_path = os.path.join(self.prefix, backup_id, 'backup.info')
        backup_info_file = self.cloud_interface.remote_open(backup_info_path)
        if backup_info_file is None:
            return None
        backup_info = BackupInfo(backup_id)
        backup_info.load(file_object=backup_info_file)
        return backup_info

    def get_backup_files(self, backup_info):
        """
        Get the list of expected files part of a backup

        :param BackupInfo backup_info: the backup information
        :rtype: dict[int, BackupFileInfo]
        """
        # Correctly format the source path on s3
        source_dir = os.path.join(self.prefix, backup_info.backup_id)

        base_path = os.path.join(source_dir, 'data')
        backup_files = {
            None: BackupFileInfo(None, base_path)
        }
        if backup_info.tablespaces:
            for tblspc in backup_info.tablespaces:
                base_path = os.path.join(source_dir, '%s' % tblspc.oid)
                backup_files[tblspc.oid] = BackupFileInfo(tblspc.oid,
                                                          base_path)

        for item in self.cloud_interface.list_bucket(source_dir + '/'):
            for backup_file in backup_files.values():
                if item.startswith(backup_file.base):
                    # Automatically detect additional files
                    suffix = item[len(backup_file.base):]
                    # Avoid to match items that are prefix of other items
                    if not suffix or suffix[0] not in ('.', '_'):
                        logging.debug("Skipping spurious prefix match: %s|%s",
                                      backup_file.base, suffix)
                        continue
                    # If this file have a suffix starting with `_`,
                    # it is an additional file and we add it to the main
                    # BackupFileInfo ...
                    if suffix[0] == '_':
                        info = BackupFileInfo(backup_file.oid, base_path)
                        backup_file.additional_files.append(info)
                        ext = suffix.split('.', 1)[-1]
                    # ... otherwise this is the main file
                    else:
                        info = backup_file
                        ext = suffix[1:]
                    # Infer the compression from the file extension
                    if ext == 'tar':
                        info.compression = None
                    elif ext == 'tar.gz':
                        info.compression = 'gzip'
                    elif ext == 'tar.bz2':
                        info.compression = 'bzip2'
                    else:
                        logging.warning("Skipping unknown extension: %s",
                                        ext)
                        continue
                    info.path = item
                    logging.info(
                        "Found file from backup '%s' of server '%s': %s",
                        backup_info.backup_id, self.server_name, info.path)
                    break

        for backup_file in backup_files.values():
            if backup_file.path is None:
                logging.error("Missing file %s.* for server %s",
                              backup_file.base,
                              self.server_name)
                raise SystemExit(1)

        return backup_files
