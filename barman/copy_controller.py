# Copyright (C) 2011-2018 2ndQuadrant Limited
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
Copy controller module

A copy controller will handle the copy between a series of files and directory,
and their final destination.
"""

import collections
import datetime
import logging
import os.path
import re
import shutil
import signal
import tempfile
from functools import partial
from multiprocessing import Lock, Pool

import dateutil.parser
import dateutil.tz

from barman.command_wrappers import RsyncPgData
from barman.exceptions import CommandFailedException, RsyncListFilesFailure
from barman.utils import human_readable_timedelta, total_seconds

_logger = logging.getLogger(__name__)
_logger_lock = Lock()

_worker_callable = None
"""
Global variable containing a callable used to execute the jobs.
Initialized by `_init_worker` and used by `_run_worker` function.
This variable must be None outside a multiprocessing worker Process.
"""

# Parallel copy bucket size (10GB)
BUCKET_SIZE = (1024 * 1024 * 1024 * 10)


def _init_worker(func):
    """
    Store the callable used to execute jobs passed to `_run_worker` function

    :param callable func: the callable to invoke for every job
    """
    global _worker_callable
    _worker_callable = func


def _run_worker(job):
    """
    Execute a job using the callable set using `_init_worker` function

    :param _RsyncJob job: the job to be executed
    """
    global _worker_callable
    assert _worker_callable is not None, \
        "Worker has not been initialized with `_init_worker`"

    # This is the entrypoint of the worker process. Since the KeyboardInterrupt
    # exceptions is handled by the main process, let's forget about Ctrl-C
    # here.
    # When the parent process will receive a KeyboardInterrupt, is will ask
    # the pool to terminate its workers and then terminate itself.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    return _worker_callable(job)


class _RsyncJob(object):
    """
    A job to be executed by a worker Process
    """
    def __init__(self, item_idx, description,
                 id=None, file_list=None, checksum=None):
        """
        :param int item_idx: The index of copy item containing this job
        :param str description: The description of the job, used for logging
        :param int id: Job ID (as in bucket)
        :param list[RsyncCopyController._FileItem] file_list: Path to the file
            containing the file list
        :param bool checksum: Whether to force the checksum verification
        """
        self.id = id
        self.item_idx = item_idx
        self.description = description
        self.file_list = file_list
        self.checksum = checksum

        # Statistics
        self.copy_start_time = None
        self.copy_end_time = None


class _FileItem(collections.namedtuple('_FileItem', 'mode size date path')):
    """
    This named tuple is used to store the content each line of the output
    of a "rsync --list-only" call
    """


class _RsyncCopyItem(object):
    """
    Internal data object that contains the information about one of the items
    that have to be copied during a RsyncCopyController run.
    """

    def __init__(self, label, src, dst,
                 exclude=None,
                 exclude_and_protect=None,
                 include=None,
                 is_directory=False,
                 bwlimit=None,
                 reuse=None,
                 item_class=None,
                 optional=False):
        """
        The "label" parameter is meant to be used for error messages
        and logging.

        If "src" or "dst" content begin with a ':' character, it is a remote
        path. Only local paths are supported in "reuse" argument.

        If "reuse" parameter is provided and is not None, it is used to
        implement the incremental copy. This only works if "is_directory" is
        True

        :param str label: a symbolic name for this item
        :param str src: source directory.
        :param str dst: destination directory.
        :param list[str] exclude: list of patterns to be excluded from the
            copy. The destination will be deleted if present.
        :param list[str] exclude_and_protect: list of patterns to be excluded
            from the copy. The destination will be preserved if present.
        :param list[str] include: list of patterns to be included in the
            copy even if excluded.
        :param bool is_directory: Whether the item points to a directory.
        :param bwlimit: bandwidth limit to be enforced. (KiB)
        :param str|None reuse: the reference path for incremental mode.
        :param str|None item_class: If specified carries a meta information
            about what the object to be copied is.
        :param bool optional: Whether a failure copying this object should be
            treated as a fatal failure. This only works if "is_directory" is
            False
        """
        self.label = label
        self.src = src
        self.dst = dst
        self.exclude = exclude
        self.exclude_and_protect = exclude_and_protect
        self.include = include
        self.is_directory = is_directory
        self.bwlimit = bwlimit
        self.reuse = reuse
        self.item_class = item_class
        self.optional = optional

        # Attributes that will e filled during the analysis
        self.temp_dir = None
        self.dir_file = None
        self.exclude_and_protect_file = None
        self.safe_list = None
        self.check_list = None

        # Statistics
        self.analysis_start_time = None
        self.analysis_end_time = None

        # Ensure that the user specified the item class, since it is mandatory
        # to correctly handle the item
        assert self.item_class

    def __str__(self):
        # Prepare strings for messages
        formatted_class = self.item_class
        formatted_name = self.src
        if self.src.startswith(':'):
            formatted_class = 'remote ' + self.item_class
            formatted_name = self.src[1:]
        formatted_class += ' directory' if self.is_directory else ' file'

        # Log the operation that is being executed
        if self.item_class in(RsyncCopyController.PGDATA_CLASS,
                              RsyncCopyController.PGCONTROL_CLASS):
            return "%s: %s" % (
                formatted_class, formatted_name)
        else:
            return "%s '%s': %s" % (
                formatted_class, self.label, formatted_name)


class RsyncCopyController(object):
    """
    Copy a list of files and directory to their final destination.
    """

    # Constants to be used as "item_class" values
    PGDATA_CLASS = "PGDATA"
    TABLESPACE_CLASS = "tablespace"
    PGCONTROL_CLASS = "pg_control"
    CONFIG_CLASS = "config"

    # This regular expression is used to parse each line of the output
    # of a "rsync --list-only" call. This regexp has been tested with any known
    # version of upstream rsync that is supported (>= 3.0.4)
    LIST_ONLY_RE = re.compile("""
        (?x) # Enable verbose mode

        ^ # start of the line

        # capture the mode (es. "-rw-------")
        (?P<mode>[-\w]+)
        \s+

        # size is an integer
        (?P<size>\d+)
        \s+

        # The date field can have two different form
        (?P<date>
            # "2014/06/05 18:00:00" if the sending rsync is compiled
            # with HAVE_STRFTIME
            [\d/]+\s+[\d:]+
        |
            # "Thu Jun  5 18:00:00 2014" otherwise
            \w+\s+\w+\s+\d+\s+[\d:]+\s+\d+
        )
        \s+

        # all the remaining characters are part of filename
        (?P<path>.+)

        $ # end of the line
    """)

    # This regular expression is used to ignore error messages regarding
    # vanished files that are not really an error. It is used because
    # in some cases rsync reports it with exit code 23 which could also mean
    # a fatal error
    VANISHED_RE = re.compile("""
        (?x) # Enable verbose mode
        (?i) # Case insensitive

        ^ # start of the line
        (
        # files which vanished before rsync start
        rsync:\ link_stat\ ".+"\ failed:\ No\ such\ file\ or\ directory\ \(2\)
        |
        # files which vanished after rsync start
        file\ has\ vanished:\ ".+"
        |
        # files which have been truncated during transfer
        rsync:\ read\ errors\ mapping\ ".+":\ No\ data\ available\ \(61\)
        |
        # final summary
        rsync\ error:\ .* \(code\ 23\)\ at\ main\.c\(\d+\)
            \ \[(generator|receiver)=[^\]]+\]
        )
        $ # end of the line
    """)

    def __init__(self, path=None, ssh_command=None, ssh_options=None,
                 network_compression=False,
                 reuse_backup=None, safe_horizon=None,
                 exclude=None, retry_times=0, retry_sleep=0, workers=1):
        """
        :param str|None path: the PATH where rsync executable will be searched
        :param str|None ssh_command: the ssh executable to be used
            to access remote paths
        :param list[str]|None ssh_options: list of ssh options to be used
            to access remote paths
        :param boolean network_compression: whether to use the network
            compression
        :param str|None reuse_backup: if "link" or "copy" enables
            the incremental copy feature
        :param datetime.datetime|None safe_horizon: if set, assumes that every
            files older than it are save to copy without checksum verification.
        :param list[str]|None exclude: list of patterns to be excluded
            from the copy
        :param int retry_times: The number of times to retry a failed operation
        :param int retry_sleep: Sleep time between two retry
        :param int workers: The number of parallel copy workers
        """

        super(RsyncCopyController, self).__init__()
        self.path = path
        self.ssh_command = ssh_command
        self.ssh_options = ssh_options
        self.network_compression = network_compression
        self.reuse_backup = reuse_backup
        self.safe_horizon = safe_horizon
        self.exclude = exclude
        self.retry_times = retry_times
        self.retry_sleep = retry_sleep
        self.workers = workers

        self.item_list = []
        """List of items to be copied"""

        self.rsync_cache = {}
        """A cache of RsyncPgData objects"""

        # Attributes used for progress reporting

        self.total_steps = None
        """Total number of steps"""

        self.current_step = None
        """Current step number"""

        self.temp_dir = None
        """Temp dir used to store the status during the copy"""

        # Statistics

        self.jobs_done = None
        """Already finished jobs list"""

        self.copy_start_time = None
        """Copy start time"""

        self.copy_end_time = None
        """Copy end time"""

    def add_directory(self, label, src, dst,
                      exclude=None,
                      exclude_and_protect=None,
                      include=None,
                      bwlimit=None, reuse=None, item_class=None):
        """
        Add a directory that we want to copy.

        If "src" or "dst" content begin with a ':' character, it is a remote
        path. Only local paths are supported in "reuse" argument.

        If "reuse" parameter is provided and is not None, it is used to
        implement the incremental copy. This only works if "is_directory" is
        True

        :param str label: symbolic name to be used for error messages
            and logging.
        :param str src: source directory.
        :param str dst: destination directory.
        :param list[str] exclude: list of patterns to be excluded from the
            copy. The destination will be deleted if present.
        :param list[str] exclude_and_protect: list of patterns to be excluded
            from the copy. The destination will be preserved if present.
        :param list[str] include: list of patterns to be included in the
            copy even if excluded.
        :param bwlimit: bandwidth limit to be enforced. (KiB)
        :param str|None reuse: the reference path for incremental mode.
        :param str item_class: If specified carries a meta information about
            what the object to be copied is.
        """
        self.item_list.append(
            _RsyncCopyItem(
                label=label,
                src=src,
                dst=dst,
                is_directory=True,
                bwlimit=bwlimit,
                reuse=reuse,
                item_class=item_class,
                optional=False,
                exclude=exclude,
                exclude_and_protect=exclude_and_protect,
                include=include))

    def add_file(self, label, src, dst, item_class=None, optional=False):
        """
        Add a file that we want to copy

        :param str label: symbolic name to be used for error messages
            and logging.
        :param str src: source directory.
        :param str dst: destination directory.
        :param str item_class: If specified carries a meta information about
            what the object to be copied is.
        :param bool optional: Whether a failure copying this object should be
            treated as a fatal failure.
        """
        self.item_list.append(
            _RsyncCopyItem(
                label=label,
                src=src,
                dst=dst,
                is_directory=False,
                bwlimit=None,
                reuse=None,
                item_class=item_class,
                optional=optional))

    def _rsync_factory(self, item):
        """
        Build the RsyncPgData object required for copying the provided item

        :param _RsyncCopyItem item: information about a copy operation
        :rtype: RsyncPgData
        """
        # If the object already exists, use it
        if item in self.rsync_cache:
            return self.rsync_cache[item]

        # Prepare the command arguments
        args = self._reuse_args(item.reuse)
        # Merge the global exclude with the one into the item object
        if self.exclude and item.exclude:
            exclude = self.exclude + item.exclude
        else:
            exclude = self.exclude or item.exclude

        # TODO: remove debug output or use it to progress tracking
        # By adding a double '--itemize-changes' option, the rsync
        # output will contain the full list of files that have been
        # touched, even those that have not changed
        args.append('--itemize-changes')
        args.append('--itemize-changes')

        # Build the rsync object that will execute the copy
        rsync = RsyncPgData(
            path=self.path,
            ssh=self.ssh_command,
            ssh_options=self.ssh_options,
            args=args,
            bwlimit=item.bwlimit,
            network_compression=self.network_compression,
            exclude=exclude,
            exclude_and_protect=item.exclude_and_protect,
            include=item.include,
            retry_times=self.retry_times,
            retry_sleep=self.retry_sleep,
            retry_handler=partial(self._retry_handler, item)
        )
        self.rsync_cache[item] = rsync
        return rsync

    def copy(self):
        """
        Execute the actual copy
        """
        # Store the start time
        self.copy_start_time = datetime.datetime.now()

        # Create a temporary directory to hold the file lists.
        self.temp_dir = tempfile.mkdtemp(suffix='', prefix='barman-')
        # The following try block is to make sure the temporary directory
        # will be removed on exit and all the pool workers
        # have been terminated.
        pool = None
        try:
            # Initialize the counters used by progress reporting
            self._progress_init()
            _logger.info("Copy started (safe before %r)", self.safe_horizon)

            # Execute some preliminary steps for each item to be copied
            for item in self.item_list:

                # The initial preparation is necessary only for directories
                if not item.is_directory:
                    continue

                # Store the analysis start time
                item.analysis_start_time = datetime.datetime.now()

                # Analyze the source and destination directory content
                _logger.info(self._progress_message(
                             "[global] analyze %s" % item))
                self._analyze_directory(item)

                # Prepare the target directories, removing any unneeded file
                _logger.info(self._progress_message(
                    "[global] create destination directories and delete "
                    "unknown files for %s" % item))
                self._create_dir_and_purge(item)

                # Store the analysis end time
                item.analysis_end_time = datetime.datetime.now()

            # Init the list of jobs done. Every job will be added to this list
            # once finished. The content will be used to calculate statistics
            # about the copy process.
            self.jobs_done = []

            # The jobs are executed using a parallel processes pool
            # Each job is generated by `self._job_generator`, it is executed by
            # `_run_worker` using `self._execute_job`, which has been set
            # calling `_init_worker` function during the Pool initialization.
            pool = Pool(processes=self.workers,
                        initializer=_init_worker,
                        initargs=(self._execute_job,))
            for job in pool.imap_unordered(_run_worker, self._job_generator(
                    exclude_classes=[self.PGCONTROL_CLASS])):
                # Store the finished job for further analysis
                self.jobs_done.append(job)

            # The PGCONTROL_CLASS items must always be copied last
            for job in pool.imap_unordered(_run_worker, self._job_generator(
                    include_classes=[self.PGCONTROL_CLASS])):
                # Store the finished job for further analysis
                self.jobs_done.append(job)

        except KeyboardInterrupt:
            _logger.info("Copy interrupted by the user (safe before %s)",
                         self.safe_horizon)
            raise
        except BaseException:
            _logger.info("Copy failed (safe before %s)", self.safe_horizon)
            raise
        else:
            _logger.info("Copy finished (safe before %s)", self.safe_horizon)
        finally:
            # The parent process may have finished naturally or have been
            # interrupted by an exception (i.e. due to a copy error or
            # the user pressing Ctrl-C).
            # At this point we must make sure that all the workers have been
            # correctly terminated before continuing.
            if pool:
                pool.terminate()
                pool.join()
            # Clean up the temp dir, any exception raised here is logged
            # and discarded to not clobber an eventual exception being handled.
            try:
                shutil.rmtree(self.temp_dir)
            except EnvironmentError as e:
                _logger.error("Error cleaning up '%s' (%s)", self.temp_dir, e)
            self.temp_dir = None

            # Store the end time
            self.copy_end_time = datetime.datetime.now()

    def _job_generator(self, include_classes=None, exclude_classes=None):
        """
        Generate the jobs to be executed by the workers

        :param list[str]|None include_classes: If not none, copy only the items
            which have one of the specified classes.
        :param list[str]|None exclude_classes: If not none, skip all items
            which have one of the specified classes.
        :rtype: iter[_RsyncJob]
        """
        for item_idx, item in enumerate(self.item_list):

            # Skip items of classes which are not required
            if include_classes and item.item_class not in include_classes:
                continue
            if exclude_classes and item.item_class in exclude_classes:
                continue

            # If the item is a directory then copy it in two stages,
            # otherwise copy it using a plain rsync
            if item.is_directory:

                # Copy the safe files using the default rsync algorithm
                msg = self._progress_message(
                    "[%%s] %%s copy safe files from %s" % item)
                phase_skipped = True
                for i, bucket in enumerate(
                        self._fill_buckets(item.safe_list)):
                    phase_skipped = False
                    yield _RsyncJob(item_idx,
                                    id=i,
                                    description=msg,
                                    file_list=bucket,
                                    checksum=False)
                if phase_skipped:
                    _logger.info(msg, 'global', 'skipping')

                # Copy the check files forcing rsync to verify the checksum
                msg = self._progress_message(
                    "[%%s] %%s copy files with checksum from %s" % item)
                phase_skipped = True
                for i, bucket in enumerate(
                        self._fill_buckets(item.check_list)):
                    phase_skipped = False
                    yield _RsyncJob(item_idx,
                                    id=i,
                                    description=msg,
                                    file_list=bucket,
                                    checksum=True)
                if phase_skipped:
                    _logger.info(msg, 'global', 'skipping')

            else:
                # Copy the file using plain rsync
                msg = self._progress_message("[%%s] %%s copy %s" % item)
                yield _RsyncJob(item_idx, description=msg)

    def _fill_buckets(self, file_list):
        """
        Generate buckets for parallel copy

        :param list[_FileItem] file_list: list of file to transfer
        :rtype: iter[list[_FileItem]]
        """
        # If there is only one worker, fall back to copying all file at once
        if self.workers < 2:
            yield file_list
            return

        # Create `self.workers` buckets
        buckets = [[] for _ in range(self.workers)]
        bucket_sizes = [0 for _ in range(self.workers)]
        pos = -1
        # Sort the list by size
        for entry in sorted(file_list, key=lambda item: item.size):
            # Try to fill the file in a bucket
            for i in range(self.workers):
                pos = (pos + 1) % self.workers
                new_size = bucket_sizes[pos] + entry.size
                if new_size < BUCKET_SIZE:
                    bucket_sizes[pos] = new_size
                    buckets[pos].append(entry)
                    break
            else:
                # All the buckets are filled, so return them all
                for i in range(self.workers):
                    if len(buckets[i]) > 0:
                        yield buckets[i]
                    # Clear the bucket
                    buckets[i] = []
                    bucket_sizes[i] = 0
                # Put the current file in the first bucket
                bucket_sizes[0] = entry.size
                buckets[0].append(entry)
                pos = 0
        # Send all the remaining buckets
        for i in range(self.workers):
            if len(buckets[i]) > 0:
                yield buckets[i]

    def _execute_job(self, job):
        """
        Execute a `_RsyncJob` in a worker process

        :type job: _RsyncJob
        """
        item = self.item_list[job.item_idx]
        if job.id is not None:
            bucket = 'bucket %s' % job.id
        else:
            bucket = 'global'
        # Build the rsync object required for the copy
        rsync = self._rsync_factory(item)
        # Store the start time
        job.copy_start_time = datetime.datetime.now()
        # Write in the log that the job is starting
        with _logger_lock:
            _logger.info(job.description, bucket, 'starting')
        if item.is_directory:
            # A directory item must always have checksum and file_list set
            assert job.file_list is not None, \
                'A directory item must not have a None `file_list` attribute'
            assert job.checksum is not None, \
                'A directory item must not have a None `checksum` attribute'

            # Generate a unique name for the file containing the list of files
            file_list_path = os.path.join(
                self.temp_dir, '%s_%s_%s.list' % (
                    item.label,
                    'check' if job.checksum else 'safe',
                    os.getpid()))

            # Write the list, one path per line
            with open(file_list_path, 'w') as file_list:
                for entry in job.file_list:
                    assert isinstance(entry, _FileItem), \
                        "expect %r to be a _FileItem" % entry
                    file_list.write(entry.path + "\n")

            self._copy(rsync,
                       item.src,
                       item.dst,
                       file_list=file_list_path,
                       checksum=job.checksum)
        else:
            # A file must never have checksum and file_list set
            assert job.file_list is None, \
                'A file item must have a None `file_list` attribute'
            assert job.checksum is None, \
                'A file item must have a None `checksum` attribute'
            rsync(item.src, item.dst, allowed_retval=(0, 23, 24))
            if rsync.ret == 23:
                if item.optional:
                    _logger.warning(
                        "Ignoring error reading %s", item)
                else:
                    raise CommandFailedException(dict(
                        ret=rsync.ret, out=rsync.out, err=rsync.err))
        # Store the stop time
        job.copy_end_time = datetime.datetime.now()
        # Write in the log that the job is finished
        with _logger_lock:
            _logger.info(job.description, bucket,
                         'finished (duration: %s)' % human_readable_timedelta(
                             job.copy_end_time - job.copy_start_time))
        # Return the job to the caller, for statistics purpose
        return job

    def _progress_init(self):
        """
        Init counters used by progress logging
        """
        self.total_steps = 0
        for item in self.item_list:
            # Directories require 4 steps, files only one
            if item.is_directory:
                self.total_steps += 4
            else:
                self.total_steps += 1
        self.current_step = 0

    def _progress_message(self, msg):
        """
        Log a message containing the progress

        :param str msg: the message
        :return srt: message to log
        """
        self.current_step += 1
        return "Copy step %s of %s: %s" % (
            self.current_step, self.total_steps, msg)

    def _reuse_args(self, reuse_directory):
        """
        If reuse_backup is 'copy' or 'link', build the rsync option to enable
        the reuse, otherwise returns an empty list

        :param str reuse_directory: the local path with data to be reused
        :rtype: list[str]
        """
        if self.reuse_backup in ('copy', 'link') and \
                reuse_directory is not None:
            return ['--%s-dest=%s' % (self.reuse_backup, reuse_directory)]
        else:
            return []

    def _retry_handler(self, item, command, args, kwargs, attempt, exc):
        """

        :param _RsyncCopyItem item: The item that is being processed
        :param RsyncPgData command: Command object being executed
        :param list args: command args
        :param dict kwargs: command kwargs
        :param int attempt: attempt number (starting from 0)
        :param CommandFailedException exc: the exception which caused the
            failure
        """
        _logger.warn("Failure executing rsync on %s (attempt %s)",
                     item, attempt)
        _logger.warn("Retrying in %s seconds", self.retry_sleep)

    def _analyze_directory(self, item):
        """
        Analyzes the status of source and destination directories identifying
        the files that are safe from the point of view of a PostgreSQL backup.

        The safe_horizon value is the timestamp of the beginning of the
        older backup involved in copy (as source or destination). Any files
        updated after that timestamp, must be checked as they could have been
        modified during the backup - and we do not reply WAL files to update
        them.

        The destination directory must exist.

        If the "safe_horizon" parameter is None, we cannot make any
        assumptions about what can be considered "safe", so we must check
        everything with checksums enabled.

        If "ref" parameter is provided and is not None, it is looked up
        instead of the "dst" dir. This is useful when we are copying files
        using '--link-dest' and '--copy-dest' rsync options.
        In this case, both the "dst" and "ref" dir must exist and
        the "dst" dir must be empty.

        If source or destination path begin with a ':' character,
        it is a remote path. Only local paths are supported in "ref" argument.

        :param _RsyncCopyItem item: information about a copy operation
        """

        # Build the rsync object required for the analysis
        rsync = self._rsync_factory(item)

        # If reference is not set we use dst as reference path
        ref = item.reuse
        if ref is None:
            ref = item.dst

        # Make sure the ref path ends with a '/' or rsync will add the
        # last path component to all the returned items during listing
        if ref[-1] != '/':
            ref += '/'

        # Build a hash containing all files present on reference directory.
        # Directories are not included
        try:
            ref_hash = dict((
                (item.path, item)
                for item in self._list_files(rsync, ref)
                if item.mode[0] != 'd'))
        except (CommandFailedException, RsyncListFilesFailure) as e:
            # Here we set ref_hash to None, thus disable the code that marks as
            # "safe matching" those destination files with different time or
            # size, even if newer than "safe_horizon". As a result, all files
            # newer than "safe_horizon" will be checked through checksums.
            ref_hash = None
            _logger.error(
                "Unable to retrieve reference directory file list. "
                "Using only source file information to decide which files"
                " need to be copied with checksums enabled: %s" % e)

        # The 'dir.list' file will contain every directory in the
        # source tree
        item.dir_file = os.path.join(self.temp_dir, '%s_dir.list' % item.label)
        dir_list = open(item.dir_file, 'w+')
        # The 'protect.list' file will contain a filter rule to protect
        # each file present in the source tree. It will be used during
        # the first phase to delete all the extra files on destination.
        item.exclude_and_protect_file = os.path.join(
            self.temp_dir, '%s_exclude_and_protect.filter' % item.label)
        exclude_and_protect_filter = open(item.exclude_and_protect_file,
                                          'w+')
        # The `safe_list` will contain all items older than
        # safe_horizon, as well as files that we know rsync will
        # check anyway due to a difference in mtime or size
        item.safe_list = []
        # The `check_list` will contain all items that need
        # to be copied with checksum option enabled
        item.check_list = []
        for entry in self._list_files(rsync, item.src):
            # If item is a directory, we only need to save it in 'dir.list'
            if entry.mode[0] == 'd':
                dir_list.write(entry.path + '\n')
                continue

            # Add every file in the source path to the list of files
            # to be protected from deletion ('exclude_and_protect.filter')
            exclude_and_protect_filter.write('P ' + entry.path + '\n')
            exclude_and_protect_filter.write('- ' + entry.path + '\n')

            # If source item is older than safe_horizon,
            # add it to 'safe.list'
            if self.safe_horizon and entry.date < self.safe_horizon:
                item.safe_list.append(entry)
                continue

            # If ref_hash is None, it means we failed to retrieve the
            # destination file list. We assume the only safe way is to
            # check every file that is older than safe_horizon
            if ref_hash is None:
                item.check_list.append(entry)
                continue

            # If source file differs by time or size from the matching
            # destination, rsync will discover the difference in any case.
            # It is then safe to skip checksum check here.
            dst_item = ref_hash.get(entry.path, None)
            if (dst_item is None or dst_item.size != entry.size or
                    dst_item.date != entry.date):
                item.safe_list.append(entry)
                continue

            # All remaining files must be checked with checksums enabled
            item.check_list.append(entry)

        # Close all the control files
        dir_list.close()
        exclude_and_protect_filter.close()

    def _create_dir_and_purge(self, item):
        """
        Create destination directories and delete any unknown file

        :param _RsyncCopyItem item: information about a copy operation
        """

        # Build the rsync object required for the analysis
        rsync = self._rsync_factory(item)

        # Create directories and delete any unknown file
        self._rsync_ignore_vanished_files(
            rsync,
            '--recursive',
            '--delete',
            '--files-from=%s' % item.dir_file,
            '--filter', 'merge %s' % item.exclude_and_protect_file,
            item.src, item.dst,
            check=True)

    def _copy(self, rsync, src, dst, file_list, checksum=False):
        """
        The method execute the call to rsync, using as source a
        a list of files, and adding the the checksum option if required by the
        caller.

        :param Rsync rsync: the Rsync object used to retrieve the list of files
            inside the directories
            for copy purposes
        :param str src: source directory
        :param str dst: destination directory
        :param str file_list: path to the file containing the sources for rsync
        :param bool checksum: if checksum argument for rsync is required
        """
        # Build the rsync call args
        args = ['--files-from=%s' % file_list]
        if checksum:
            # Add checksum option if needed
            args.append('--checksum')
        self._rsync_ignore_vanished_files(rsync, src, dst, *args, check=True)

    def _list_files(self, rsync, path):
        """
        This method recursively retrieves a list of files contained in a
        directory, either local or remote (if starts with ':')

        :param Rsync rsync: the Rsync object used to retrieve the list
        :param str path: the path we want to inspect
        :except CommandFailedException: if rsync call fails
        :except RsyncListFilesFailure: if rsync output can't be parsed
        """
        _logger.debug("list_files: %r", path)
        # Use the --no-human-readable option to avoid digit groupings
        # in "size" field with rsync >= 3.1.0.
        # Ref: http://ftp.samba.org/pub/rsync/src/rsync-3.1.0-NEWS
        rsync.get_output('--no-human-readable', '--list-only', '-r', path,
                         check=True)
        for line in rsync.out.splitlines():
            line = line.rstrip()
            match = self.LIST_ONLY_RE.match(line)
            if match:
                mode = match.group('mode')
                # no exceptions here: the regexp forces 'size' to be an integer
                size = int(match.group('size'))
                try:
                    date = dateutil.parser.parse(match.group('date'))
                    date = date.replace(tzinfo=dateutil.tz.tzlocal())
                except (TypeError, ValueError):
                    # This should not happen, due to the regexp
                    msg = ("Unable to parse rsync --list-only output line "
                           "(date): '%s'" % line)
                    _logger.exception(msg)
                    raise RsyncListFilesFailure(msg)
                path = match.group('path')
                yield _FileItem(mode, size, date, path)
            else:
                # This is a hard error, as we are unable to parse the output
                # of rsync. It can only happen with a modified or unknown
                # rsync version (perhaps newer than 3.1?)
                msg = ("Unable to parse rsync --list-only output line: "
                       "'%s'" % line)
                _logger.error(msg)
                raise RsyncListFilesFailure(msg)

    def _rsync_ignore_vanished_files(self, rsync, *args, **kwargs):
        """
        Wrap an Rsync.get_output() call and ignore missing args

        TODO: when rsync 3.1 will be widespread, replace this
            with --ignore-missing-args argument

        :param Rsync rsync: the Rsync object used to execute the copy
        """
        kwargs['allowed_retval'] = (0, 23, 24)
        rsync.get_output(*args, **kwargs)
        # If return code is 23 and there is any error which doesn't match
        # the VANISHED_RE regexp raise an error
        if rsync.ret == 23 and rsync.err is not None:
            for line in rsync.err.splitlines():
                match = self.VANISHED_RE.match(line.rstrip())
                if match:
                    continue
                else:
                    _logger.error("First rsync error line: %s", line)
                    raise CommandFailedException(dict(
                        ret=rsync.ret, out=rsync.out, err=rsync.err))
        return rsync.out, rsync.err

    def statistics(self):
        """
        Return statistics about the copy object.

        :rtype: dict
        """
        # This method can only run at the end of a non empty copy
        assert self.copy_end_time
        assert self.item_list
        assert self.jobs_done

        # Initialise the result calculating the total runtime
        stat = {
            'total_time': total_seconds(
                self.copy_end_time - self.copy_start_time),
            'number_of_workers': self.workers,
            'analysis_time_per_item': {},
            'copy_time_per_item': {},
            'serialized_copy_time_per_item': {},
        }

        # Calculate the time spent during the analysis of the items
        analysis_start = None
        analysis_end = None
        for item in self.item_list:
            # Some items don't require analysis
            if not item.analysis_end_time:
                continue
            # Build a human readable name to refer to an item in the output
            ident = item.label
            if (analysis_start is None or
                    analysis_start > item.analysis_start_time):
                analysis_start = item.analysis_start_time
            if (analysis_end is None or
                    analysis_end < item.analysis_end_time):
                analysis_end = item.analysis_end_time
            stat['analysis_time_per_item'][ident] = total_seconds(
                item.analysis_end_time -
                item.analysis_start_time)
        stat['analysis_time'] = total_seconds(analysis_end - analysis_start)

        # Calculate the time spent per job
        # WARNING: this code assumes that every item is copied separately,
        # so it's strictly tied to the `_job_generator` method code
        item_data = {}
        for job in self.jobs_done:
            # WARNING: the item contained in the job is not the same object
            # contained in self.item_list, as it has gone through two
            # pickling/unpickling cycle
            # Build a human readable name to refer to an item in the output
            ident = self.item_list[job.item_idx].label
            # If this is the first time we see this item we just store the
            # values from the job
            if ident not in item_data:
                item_data[ident] = {
                    'start': job.copy_start_time,
                    'end': job.copy_end_time,
                    'total_time': job.copy_end_time - job.copy_start_time
                }
            else:
                data = item_data[ident]
                if data['start'] > job.copy_start_time:
                    data['start'] = job.copy_start_time
                if data['end'] < job.copy_end_time:
                    data['end'] = job.copy_end_time
                data['total_time'] += job.copy_end_time - job.copy_start_time

        # Calculate the time spent copying
        copy_start = None
        copy_end = None
        serialized_time = datetime.timedelta(0)
        for ident in item_data:
            data = item_data[ident]
            if copy_start is None or copy_start > data['start']:
                copy_start = data['start']
            if copy_end is None or copy_end < data['end']:
                copy_end = data['end']
            stat['copy_time_per_item'][ident] = total_seconds(
                data['end'] - data['start'])
            stat['serialized_copy_time_per_item'][ident] = total_seconds(
                data['total_time'])
            serialized_time += data['total_time']
        # Store the total time spent by copying
        stat['copy_time'] = total_seconds(copy_end - copy_start)
        stat['serialized_copy_time'] = total_seconds(serialized_time)

        return stat
