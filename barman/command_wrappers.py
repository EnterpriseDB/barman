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
# along with Barman.  If not, see <http://www.gnu.org/licenses/>.

"""
This module contains a wrapper for shell commands
"""

from __future__ import print_function

import collections
import errno
import inspect
import logging
import os
import re
import select
import shutil
import signal
import subprocess
import sys
import tempfile

import dateutil.parser
import dateutil.tz

import barman.utils

_logger = logging.getLogger(__name__)


class CommandFailedException(Exception):
    """
    Exception representing a failed command
    """
    pass


class RsyncListFilesFailure(Exception):
    """
    Failure parsing the output of a "rsync --list-only" command
    """
    pass


class DataTransferFailure(Exception):
    """
    Used to pass rsync failure details
    """

    @classmethod
    def from_rsync_error(cls, e, msg):
        """
        This method build a DataTransferFailure exception and report the
        provided message to the user (both console and log file) along with
        the output of the failed rsync command.

        :param CommandFailedException e: The exception we are handling
        :param str msg: a descriptive message on what we are trying to do
        :return DataTransferFailure: will contain the message provided in msg
        """
        details = msg
        details += "\nrsync error:\n"
        details += e.args[0]['out']
        details += e.args[0]['err']
        return cls(details)


class StreamLineProcessor(object):
    """
    Class deputed to reading lines from a file object, using a buffered read.

    NOTE: This class never call os.read() twice in a row. And is designed to
    work with the select.select() method.
    """

    def __init__(self, fobject, handler):
        """
        :param file fobject: The file that is being read
        :param callable handler: The function (taking only one unicode string
         argument) which will be called for every line
        """
        self._file = fobject
        self._handler = handler
        self._buf = ''

    def fileno(self):
        """
        Method used by select.select() to get the underlying file descriptor.

        :rtype: the underlying file descriptor
        """
        return self._file.fileno()

    def process(self):
        """
        Read the ready data from the stream and for each line found invoke the
        handler.

        :return bool: True when End Of File has been reached
        """
        data = os.read(self._file.fileno(), 4096)
        # If nothing has been read, we reached the EOF
        if not data:
            self._file.close()
            # Handle the last line (always incomplete, maybe empty)
            self._handler(self._buf)
            return True
        self._buf += data.decode('utf-8')
        # If no '\n' is present, we just read a part of a very long line.
        # Nothing to do at the moment.
        if '\n' not in self._buf:
            return False
        tmp = self._buf.split('\n')
        # Leave the remainder in self._buf
        self._buf = tmp[-1]
        # Call the handler for each complete line.
        lines = tmp[:-1]
        for line in lines:
            self._handler(line)
        return False


class Command(object):
    """
    Simple wrapper for a shell command
    """

    def __init__(self, cmd, args=None, env_append=None, path=None, shell=False,
                 check=False, allowed_retval=(0,), debug=False,
                 close_fds=True, out_handler=None, err_handler=None):
        self.pipe = None
        self.cmd = cmd
        self.args = args if args is not None else []
        self.shell = shell
        self.close_fds = close_fds
        self.check = check
        self.allowed_retval = allowed_retval
        self.debug = debug
        self.ret = None
        self.out = None
        self.err = None
        # If env_append has been provided use it or replace with an empty dict
        env_append = env_append or {}
        # If path has been provided, replace it in the environment
        if path:
            env_append['PATH'] = path
        # If env_append contains anything, build an env dict to be used during
        # subprocess call, otherwise set it to None and let the subprocesses
        # inherit the parent environment
        if env_append:
            self.env = os.environ.copy()
            self.env.update(env_append)
        else:
            self.env = None
        # If an output handler has been provided use it, otherwise log the
        # stdout as INFO
        if out_handler:
            self.out_handler = out_handler
        else:
            self.out_handler = self.make_logging_handler(logging.INFO)
        # If an error handler has been provided use it, otherwise log the
        # stderr as WARNING
        if err_handler:
            self.err_handler = err_handler
        else:
            self.err_handler = self.make_logging_handler(logging.WARNING)

    @staticmethod
    def _restore_sigpipe():
        """restore default signal handler (http://bugs.python.org/issue1652)"""
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)  # pragma: no cover

    @staticmethod
    def _cmd_quote(cmd, args):
        """
        Quote all cmd's arguments.

        This is needed to avoid command string breaking.
        WARNING: this function does not protect against injection.
        """
        if args is not None and len(args) > 0:
            cmd = "%s '%s'" % (cmd, "' '".join(args))
        return cmd

    def __call__(self, *args, **kwargs):
        self.getoutput(*args, **kwargs)
        return self.ret

    def getoutput(self, *args, **kwargs):
        """
        Run the command and return the output and the error (if present)
        """
        out = []
        err = []
        # If check is true, it must be handled here
        check = kwargs.pop('check', self.check)
        self.execute(out_handler=out.append, err_handler=err.append,
                     check=False, *args, **kwargs)
        self.out = '\n'.join(out)
        self.err = '\n'.join(err)
        _logger.debug("Command stdout: %s", self.out)
        _logger.debug("Command stderr: %s", self.err)

        # Raise if check and the return code is not in the allowed list
        if check:
            self.check_return_value()
        return self.out, self.err

    def check_return_value(self):
        """
        Check the current return code and raise CommandFailedException when
        it's not in the allowed_retval list

        :raises: CommandFailedException
        """
        if self.ret not in self.allowed_retval:
            raise CommandFailedException(dict(
                ret=self.ret, out=self.out, err=self.err))

    def execute(self, *args, **kwargs):
        """
        Execute the command and pass the output to the configured handlers
        """
        # Check keyword arguments
        stdin = kwargs.pop('stdin', None)
        check = kwargs.pop('check', self.check)
        close_fds = kwargs.pop('close_fds', self.close_fds)
        out_handler = kwargs.pop('out_handler', self.out_handler)
        err_handler = kwargs.pop('err_handler', self.err_handler)
        if len(kwargs):
            raise TypeError('%s() got an unexpected keyword argument %r' %
                            (inspect.stack()[1][3], kwargs.popitem()[0]))

        # Reset status
        self.ret = None
        self.out = None
        self.err = None

        # Create the subprocess and save it in the current object to be usable
        # by signal handlers
        pipe = self._build_pipe(args, close_fds)
        self.pipe = pipe

        # Send the provided input and close the stdin descriptor
        if stdin:
            pipe.stdin.write(stdin)
        pipe.stdin.close()
        # Prepare the list of processors
        processors = [
            StreamLineProcessor(
                pipe.stdout, out_handler),
            StreamLineProcessor(
                pipe.stderr, err_handler)]

        # Read the streams until the subprocess exits
        self.pipe_processor_loop(processors)

        # Reap the zombie and read the exit code
        pipe.wait()
        self.ret = pipe.returncode

        # Remove the closed pipe from the object
        self.pipe = None
        if self.debug:
            print("Command return code: %s" % self.ret, file=sys.stderr)
        _logger.debug("Command return code: %s", self.ret)

        # Raise if check and the return code is not in the allowed list
        if check:
            self.check_return_value()
        return self.ret

    def _build_pipe(self, args, close_fds):
        """
        Build the Pipe object used by the Command

        The resulting command will be composed by:
           self.cmd + self.args + args

        :param args: extra arguments for the subprocess
        :param close_fds: if True all file descriptors except 0, 1 and 2
            will be closed before the child process is executed.
        :rtype: subprocess.Popen
        """
        # Append the argument provided to this method ot the base argument list
        args = self.args + list(args)
        # If shell is True, properly quote the command
        if self.shell:
            cmd = self._cmd_quote(self.cmd, args)
        else:
            cmd = [self.cmd] + args
        # Log the command we are about to execute
        if self.debug:
            print("Command: %r" % cmd, file=sys.stderr)
        _logger.debug("Command: %r", cmd)
        return subprocess.Popen(cmd, shell=self.shell, env=self.env,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                preexec_fn=self._restore_sigpipe,
                                close_fds=close_fds)

    @staticmethod
    def pipe_processor_loop(processors):
        """
        Process the output received through the pipe until all the provided
        StreamLineProcessor reach the EOF.

        :param list[StreamLineProcessor] processors: a list of
            StreamLineProcessor
        """
        # Loop until all the streams reaches the EOF
        while processors:
            try:
                ready = select.select(processors, [], [])[0]
            except select.error as e:
                # If the select call has been interrupted by a signal
                # just retry
                if e.args[0] == errno.EINTR:
                    continue
                raise

            # For each ready StreamLineProcessor invoke the process() method
            for stream in ready:
                eof = stream.process()
                # Got EOF on this stream
                if eof:
                    # Remove the stream from the list of valid processors
                    processors.remove(stream)

    @classmethod
    def make_logging_handler(cls, level, prefix=None):
        """
        Build a handler function that logs every line it receives.

        The resulting function logs its input at the specified level
        with an optional prefix.

        :param level: The log level to use
        :param prefix: An optional prefix to prepend to the line
        :return: handler function
        """
        class_logger = logging.getLogger(cls.__name__)

        def handler(line):
            if line:
                if prefix:
                    class_logger.log(level, "%s%s", prefix, line)
                else:
                    class_logger.log(level, "%s", line)
        return handler

    @staticmethod
    def make_output_handler(prefix=None):
        """
        Build a handler function which prints every line it receives.

        The resulting function prints (and log it at INFO level) its input
        with an optional prefix.

        :param prefix: An optional prefix to prepend to the line
        :return: handler function
        """

        # Import the output module inside the function to avoid circular
        # dependency
        from barman import output

        def handler(line):
            if line:
                if prefix:
                    output.info("%s%s", prefix, line)
                else:
                    output.info("%s", line)

        return handler

    def enable_signal_forwarding(self, signal_id):
        """
        Enable signal forwarding to the subprocess for a specified signal_id

        :param signal_id: The signal id to be forwarded
        """
        # Get the current signal handler
        old_handler = signal.getsignal(signal_id)

        def _handler(sig, frame):
            """
            This signal handler forward the signal to the subprocess then
            execute the original handler.
            """
            # Forward the signal to the subprocess
            if self.pipe:
                self.pipe.send_signal(signal_id)
            # If the old handler is callable
            if callable(old_handler):
                old_handler(sig, frame)
            # If we have got a SIGTERM, we must exit
            elif old_handler == signal.SIG_DFL and signal_id == signal.SIGTERM:
                sys.exit(128 + signal_id)

        # Set the signal handler
        signal.signal(signal_id, _handler)


class Rsync(Command):
    """
    This class is a wrapper for the rsync system command,
    which is used vastly by barman
    """

    #: This regular expression is used to parse each line of the output
    # of a "rsync --list-only" call. This regexp has been tested with any known
    # version of upstream rsync that is supported (>= 3.0.4)
    LIST_ONLY_RE = re.compile('''
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
    ''')

    #: This regular expression is used to ignore error messages regarding
    # vanished files that are not really an error. It is used because
    # in some cases rsync reports it with exit code 23 which could also mean
    # a fatal error
    VANISHED_RE = re.compile('''
        (?x) # Enable verbose mode

        ^ # start of the line
        (
        # files which vanished before rsync start
        rsync:\ link_stat\ ".+"\ failed:\ No\ such\ file\ or\ directory\ \(2\)
        |
        # files which vanished after rsync start
        file\ has\ vanished:\ ".+"
        |
        # final summary
        rsync\ error:\ .* \(code\ 23\)\ at\ main\.c\(\d+\)
            \ \[generator=[^\]]+\]
        )
        $ # end of the line
    ''')

    # This named tuple is used to parse each line of the output
    # of a "rsync --list-only" call
    FileItem = collections.namedtuple('FileItem', 'mode size date path')

    def __init__(self, rsync='rsync', args=None, ssh=None, ssh_options=None,
                 bwlimit=None, exclude_and_protect=None,
                 network_compression=None, check=True, allowed_retval=(0, 24),
                 path=None, **kwargs):
        options = []
        # Try to find rsync in system PATH using the which method.
        # If not found, rsync is not installed and this class cannot
        # work properly.
        # Raise CommandFailedException warning the user
        rsync_path = barman.utils.which(rsync, path)
        if not rsync_path:
            raise CommandFailedException('rsync not in system PATH: '
                                         'is rsync installed?')
        if ssh:
            options += ['-e', self._cmd_quote(ssh, ssh_options)]
        if network_compression:
            options += ['-z']
        if exclude_and_protect:
            for exclude_path in exclude_and_protect:
                options += ["--exclude=%s" % (exclude_path,),
                            "--filter=P_%s" % (exclude_path,)]
        if args:
            options += self._args_for_suse(args)
        if bwlimit is not None and bwlimit > 0:
            options += ["--bwlimit=%s" % bwlimit]
        Command.__init__(self, rsync, args=options, check=check,
                         allowed_retval=allowed_retval, path=path, **kwargs)

    def _args_for_suse(self, args):
        """
        Mangle args for SUSE compatibility

        See https://bugzilla.opensuse.org/show_bug.cgi?id=898513
        """
        # Prepend any argument starting with ':' with a space
        # Workaround for SUSE rsync issue
        return [' ' + a if a.startswith(':') else a for a in args]

    def getoutput(self, *args, **kwargs):
        """
        Run the command and return the output and the error (if present)
        """
        # Prepares args for SUSE
        args = self._args_for_suse(args)
        # Invoke the base class method
        return super(Rsync, self).getoutput(*args, **kwargs)

    def from_file_list(self, filelist, src, dst, *args, **kwargs):
        """
        This method copies filelist from src to dst.

        Returns the return code of the rsync command
        """
        if 'stdin' in kwargs:
            raise TypeError("from_file_list() doesn't support 'stdin' "
                            "keyword argument")
        input_string = ('\n'.join(filelist)).encode('UTF-8')
        _logger.debug("from_file_list: %r", filelist)
        kwargs['stdin'] = input_string
        self.getoutput('--files-from=-', src, dst, *args, **kwargs)
        return self.ret

    def list_files(self, path):
        """
        This method recursively retrieves a list of files contained in a
        directory, either local or remote (if starts with ':')

        :param str path: the path we want to inspect
        :except CommandFailedException: if rsync call fails
        :except RsyncListFilesFailure: if rsync output can't be parsed
        """
        _logger.debug("list_files: %r", path)
        # Use the --no-human-readable option to avoid digit groupings
        # in "size" field with rsync >= 3.1.0.
        # Ref: http://ftp.samba.org/pub/rsync/src/rsync-3.1.0-NEWS
        self.getoutput('--no-human-readable', '--list-only', '-r', path,
                       check=True)
        for line in self.out.splitlines():
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
                yield self.FileItem(mode, size, date, path)
            else:
                # This is a hard error, as we are unable to parse the output
                # of rsync. It can only happen with a modified or unknown
                # rsync version (perhaps newer than 3.1?)
                msg = ("Unable to parse rsync --list-only output line: "
                       "'%s'" % line)
                _logger.error(msg)
                raise RsyncListFilesFailure(msg)

    def _rsync_ignore_vanished_files(self, *args, **kwargs):
        """
        Wrap a getoutput() call and ignore missing args

        TODO: when rsync 3.1 will be widespread, replace this
            with --ignore-missing-args argument
        """
        try:
            self.getoutput(*args, **kwargs)
        except CommandFailedException:
            # if return code is different than 23
            # or there is any error which doesn't match the VANISHED_RE regexp
            # raise the error again
            if self.ret == 23 and self.err is not None:
                for line in self.err.splitlines():
                    match = self.VANISHED_RE.match(line.rstrip())
                    if match:
                        continue
                    else:
                        raise
            else:
                raise
        return self.out, self.err

    def smart_copy(self, src, dst, safe_horizon=None, ref=None):
        """
        Recursively copies files from "src" to "dst" in a way that is safe from
        the point of view of a PostgreSQL backup.
        The "safe_horizon" parameter is the timestamp of the beginning of the
        older backup involved in copy (as source or destination). Any files
        updated after that timestamp, must be checked as they could have been
        modified during the backup - and we do not reply WAL files to update
        them.

        The "dst" directory must exist.

        If the "safe_horizon" parameter is None, we cannot make any
        assumptions about what can be considered "safe", so we must check
        everything with checksums enabled.

        If "ref" parameter is provided and is not None, it is looked up
        instead of the "dst" dir. This is useful when we are copying files
        using '--link-dest' and '--copy-dest' rsync options.
        In this case, both the "dst" and "ref" dir must exist and
        the "dst" dir must be empty.

        If "src" or "dst" content begin with a ':' character, it is a remote
        path. Only local paths are supported in "ref" argument.

        :param str src: the source path
        :param str dst: the destination path
        :param datetime.datetime safe_horizon: anything after this time
            has to be checked
        :param str ref: the reference path
        :except CommandFailedException: If rsync failed at any time
        :except RsyncListFilesFailure: If source rsync output format is unknown
        """
        _logger.info("Smart copy: %r -> %r (ref: %r, safe before %r)",
                     src, dst, ref, safe_horizon)

        # If reference is not set we use dst as reference path
        if ref is None:
            ref = dst

        # Make sure the ref path ends with a '/' or rsync will add the
        # last path component to all the returned items during listing
        if ref[-1] != '/':
            ref += '/'

        # Build a hash containing all files present on reference directory.
        # Directories are not included
        _logger.info("Smart copy step 1/4: preparation")
        try:
            ref_hash = dict((
                (item.path, item)
                for item in self.list_files(ref)
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

        # We need a temporary directory to store the files containing the lists
        # we are building in order to instruct rsync about which files need to
        # be copied at different stages
        temp_dir = tempfile.mkdtemp(suffix='', prefix='barman-')
        try:
            # The 'dir.list' file will contain every directory in the
            # source tree
            dir_list = open(os.path.join(temp_dir, 'dir.list'), 'w+')
            # The 'safe.list' file will contain all files older than
            # safe_horizon, as well as files that we know rsync will
            # check anyway due to a difference in mtime or size
            safe_list = open(os.path.join(temp_dir, 'safe.list'), 'w+')
            # The 'check.list' file will contain all files that need
            # to be copied with checksum option enabled
            check_list = open(os.path.join(temp_dir, 'check.list'), 'w+')
            # The 'protect.list' file will contain a filter rule to protect
            # each file present in the source tree. It will be used during
            # the first phase to delete all the extra files on destination.
            exclude_and_protect_filter = open(
                os.path.join(temp_dir, 'exclude_and_protect.filter'), 'w+')
            for item in self.list_files(src):
                # If item is a directory, we only need to save it in 'dir.list'
                if item.mode[0] == 'd':
                    dir_list.write(item.path + '\n')
                    continue

                # Add every file in the source path to the list of files
                # to be protected from deletion ('exclude_and_protect.filter')
                exclude_and_protect_filter.write('P ' + item.path + '\n')
                exclude_and_protect_filter.write('- ' + item.path + '\n')

                # If source item is older than safe_horizon,
                # add it to 'safe.list'
                if safe_horizon and item.date < safe_horizon:
                    safe_list.write(item.path + '\n')
                    continue

                # If ref_hash is None, it means we failed to retrieve the
                # destination file list. We assume the only safe way is to
                # check every file that is older than safe_horizon
                if ref_hash is None:
                    check_list.write(item.path + '\n')
                    continue

                # If source file differs by time or size from the matching
                # destination, rsync will discover the difference in any case.
                # It is then safe to skip checksum check here.
                dst_item = ref_hash.get(item.path, None)
                if (dst_item is None or
                        dst_item.size != item.size or
                        dst_item.date != item.date):
                    safe_list.write(item.path + '\n')
                    continue

                # All remaining files must be checked with checksums enabled
                check_list.write(item.path + '\n')

            # Close all the control files
            dir_list.close()
            safe_list.close()
            check_list.close()
            exclude_and_protect_filter.close()

            # TODO: remove debug output
            # By adding a double '--itemize-changes' option, the rsync output
            # will contain the full list of files that have been touched, even
            # those that have not changed
            orig_args = self.args
            self.args = orig_args[:]  # clone the argument list
            self.args.append('--itemize-changes')
            self.args.append('--itemize-changes')

            # Create directories and delete/copy unknown files
            _logger.info("Smart copy step 2/4: create directories and "
                         "delete/copy unknown files")
            self._rsync_ignore_vanished_files(
                '--recursive',
                '--delete',
                '--files-from=%s' % dir_list.name,
                '--filter', 'merge %s' % exclude_and_protect_filter.name,
                src, dst,
                check=True)

            # Copy safe files
            _logger.info("Smart copy step 3/4: safe copy")
            self._rsync_ignore_vanished_files(
                '--files-from=%s' % safe_list.name,
                src, dst,
                check=True)

            # Copy remaining files with checksums
            _logger.info("Smart copy step 4/4: copy with checksums")
            self._rsync_ignore_vanished_files(
                '--checksum',
                '--files-from=%s' % check_list.name,
                src, dst,
                check=True)

            # TODO: remove debug output
            # Restore the original arguments for rsync
            self.args = orig_args
        finally:
            shutil.rmtree(temp_dir)
            _logger.info("Smart copy finished: %s -> %s (safe before %s)",
                         src, dst, safe_horizon)


class RsyncPgData(Rsync):
    """
    This class is a wrapper for rsync, specialised in sync-ing the
    Postgres data directory
    """

    def __init__(self, rsync='rsync', args=None, **kwargs):
        options = [
            '-rLKpts', '--delete-excluded', '--inplace',
            '--exclude=/pg_xlog/*',
            '--exclude=/pg_log/*',
            '--exclude=/recovery.conf',
            '--exclude=/postmaster.pid'
        ]
        if args:
            options += args
        Rsync.__init__(self, rsync, args=options, **kwargs)


class PgReceiveXlog(Command):
    """
    Wrapper class for pg_receivexlog
    """

    def __init__(self,
                 receivexlog='pg_receivexlog',
                 conn_string=None,
                 dest=None,
                 args=None,
                 check=True,
                 host=None,
                 port=None,
                 user=None,
                 **kwargs):
        options = [
            "--verbose",
            "--no-loop",
            "--directory=%s" % dest]
        # Pass the connections parameters
        if conn_string:
            options.append("--dbname=%s" % conn_string)
        if host:
            options.append("--host=%s" % host)
        if port:
            options.append("--port=%s" % port)
        if host:
            options.append("--username=%s" % user)
        # Add eventual other arguments
        if args:
            options += args
        Command.__init__(self, receivexlog, args=options, check=check,
                         **kwargs)
        self.enable_signal_forwarding(signal.SIGINT)
        self.enable_signal_forwarding(signal.SIGTERM)


class BarmanSubProcess(object):
    """
    Wrapper class for barman sub instances
    """

    def __init__(self, command=sys.argv[0], subcommand=None,
                 config=None, args=None):
        """
        Build a specific wrapper for all the barman sub-commands,
        providing an unified interface.

        :param str command: path to barman
        :param str subcommand: the barman sub-command
        :param str config: path to the barman configuration file.
        :param list[str] args: a list containing the sub-command args
            like the target server name
        """
        # The config argument is needed when the user explicitly
        # passes a configuration file, as the child process
        # must know the configuration file to use.
        #
        # The configuration file must always be propagated,
        # even in case of the default one.
        if not config:
            raise CommandFailedException(
                "No configuration file passed to barman subprocess")
        # Build the sub-command:
        # * be sure to run it with the right python interpreter
        # * pass the current configuration file with -c
        # * set it quiet with -q
        self.command = [sys.executable, command,
                        '-c', config, '-q', subcommand]
        # Handle args for the sub-command (like the server name)
        if args:
            self.command += args

    def execute(self):
        """
        Execute the command and pass the output to the configured handlers
        """
        _logger.debug("BarmanSubProcess: %r", self.command)
        # Redirect all descriptors to /dev/null
        devnull = open(os.devnull, 'a+')
        proc = subprocess.Popen(
            self.command,
            preexec_fn=os.setsid, close_fds=True,
            stdin=devnull, stdout=devnull, stderr=devnull)
        _logger.debug("BarmanSubProcess: subprocess started. pid: %s",
                      proc.pid)
