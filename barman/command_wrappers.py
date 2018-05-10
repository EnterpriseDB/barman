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
This module contains a wrapper for shell commands
"""

from __future__ import print_function

import errno
import inspect
import logging
import os
import select
import signal
import subprocess
import sys
import time

from distutils.version import LooseVersion as Version

import barman.utils
from barman.exceptions import CommandFailedException, CommandMaxRetryExceeded

_logger = logging.getLogger(__name__)


def _str(cmd_out):
    """
    Make a string from the output of a CommandWrapper execution.
    If input is None returns a literal 'None' string

    :param cmd_out: String or ByteString to convert
    :return str: a string
    """
    if hasattr(cmd_out, 'decode') and callable(cmd_out.decode):
        return cmd_out.decode('utf-8', 'replace')
    else:
        return str(cmd_out)


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
    Wrapper for a system command
    """

    def __init__(self, cmd, args=None, env_append=None, path=None, shell=False,
                 check=False, allowed_retval=(0,),
                 close_fds=True, out_handler=None, err_handler=None,
                 retry_times=0, retry_sleep=0, retry_handler=None):
        """
        If the `args` argument is specified the arguments will be always added
        to the ones eventually passed with the actual invocation.

        If the `env_append` argument is present its content will be appended to
        the environment of every invocation.

        The subprocess output and error stream will be processed through
        the output and error handler, respectively defined through the
        `out_handler` and `err_handler` arguments. If not provided every line
        will be sent to the log respectively at INFO and WARNING level.

        The `out_handler` and the `err_handler` functions will be invoked with
        one single argument, which is a string containing the line that is
        being processed.

        If the `close_fds` argument is True, all file descriptors
        except 0, 1 and 2 will be closed before the child process is executed.

        If the `check` argument is True, the exit code will be checked
        against the `allowed_retval` list, raising a CommandFailedException if
        not in the list.

        If `retry_times` is greater than 0, when the execution of a command
        terminates with an error, it will be retried for
        a maximum of `retry_times` times, waiting for `retry_sleep` seconds
        between every attempt.

        Everytime a command is retried the `retry_handler` is executed
        before running the command again. The retry_handler must be a callable
        that accepts the following fields:

         * the Command object
         * the arguments list
         * the keyword arguments dictionary
         * the number of the failed attempt
         * the exception containing the error

        An example of such a function is:

            > def retry_handler(command, args, kwargs, attempt, exc):
            >     print("Failed command!")

        Some of the keyword arguments can be specified both in the class
        constructor and during the method call. If specified in both places,
        the method arguments will take the precedence over
        the constructor arguments.

        :param str cmd: The command to exexute
        :param list[str]|None args: List of additional arguments to append
        :param dict[str.str]|None env_append: additional environment variables
        :param str path: PATH to be used while searching for `cmd`
        :param bool shell: If true, use the shell instead of an "execve" call
        :param bool check: Raise a CommandFailedException if the exit code
            is not present in `allowed_retval`
        :param list[int] allowed_retval: List of exit codes considered as a
            successful termination.
        :param bool close_fds: If set, close all the extra file descriptors
        :param callable out_handler: handler for lines sent on stdout
        :param callable err_handler: handler for lines sent on stderr
        :param int retry_times: number of allowed retry attempts
        :param int retry_sleep: wait seconds between every retry
        :param callable retry_handler: handler invoked during a command retry
        """
        self.pipe = None
        self.cmd = cmd
        self.args = args if args is not None else []
        self.shell = shell
        self.close_fds = close_fds
        self.check = check
        self.allowed_retval = allowed_retval
        self.retry_times = retry_times
        self.retry_sleep = retry_sleep
        self.retry_handler = retry_handler
        self.path = path
        self.ret = None
        self.out = None
        self.err = None
        # If env_append has been provided use it or replace with an empty dict
        env_append = env_append or {}
        # If path has been provided, replace it in the environment
        if path:
            env_append['PATH'] = path
        # Find the absolute path to the command to execute
        if not self.shell:
            full_path = barman.utils.which(self.cmd, self.path)
            if not full_path:
                raise CommandFailedException(
                    '%s not in PATH' % self.cmd)
            self.cmd = full_path
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

    def __call__(self, *args, **kwargs):
        """
        Run the command and return the exit code.

        The output and error strings are not returned, but they can be accessed
        as attributes of the Command object, as well as the exit code.

        If `stdin` argument is specified, its content will be passed to the
        executed command through the standard input descriptor.

        If the `close_fds` argument is True, all file descriptors
        except 0, 1 and 2 will be closed before the child process is executed.

        If the `check` argument is True, the exit code will be checked
        against the `allowed_retval` list, raising a CommandFailedException if
        not in the list.

        Every keyword argument can be specified both in the class constructor
        and during the method call. If specified in both places,
        the method arguments will take the precedence over
        the constructor arguments.

        :rtype: int
        :raise: CommandFailedException
        :raise: CommandMaxRetryExceeded
        """
        self.get_output(*args, **kwargs)
        return self.ret

    def get_output(self, *args, **kwargs):
        """
        Run the command and return the output and the error as a tuple.

        The return code is not returned, but it can be accessed as an attribute
        of the Command object, as well as the output and the error strings.

        If `stdin` argument is specified, its content will be passed to the
        executed command through the standard input descriptor.

        If the `close_fds` argument is True, all file descriptors
        except 0, 1 and 2 will be closed before the child process is executed.

        If the `check` argument is True, the exit code will be checked
        against the `allowed_retval` list, raising a CommandFailedException if
        not in the list.

        Every keyword argument can be specified both in the class constructor
        and during the method call. If specified in both places,
        the method arguments will take the precedence over
        the constructor arguments.

        :rtype: tuple[str, str]
        :raise: CommandFailedException
        :raise: CommandMaxRetryExceeded
        """
        attempt = 0
        while True:
            try:
                return self._get_output_once(*args, **kwargs)
            except CommandFailedException as exc:
                # Try again if retry number is lower than the retry limit
                if attempt < self.retry_times:
                    # If a retry_handler is defined, invoke it passing the
                    # Command instance and the exception
                    if self.retry_handler:
                        self.retry_handler(self, args, kwargs, attempt, exc)
                    # Sleep for configured time, then try again
                    time.sleep(self.retry_sleep)
                    attempt += 1
                else:
                    if attempt == 0:
                        # No retry requested by the user
                        # Raise the original exception
                        raise
                    else:
                        # If the max number of attempts is reached and
                        # there is still an error, exit raising
                        # a CommandMaxRetryExceeded exception and wrap the
                        # original one
                        raise CommandMaxRetryExceeded(exc)

    def _get_output_once(self, *args, **kwargs):
        """
        Run the command and return the output and the error as a tuple.

        The return code is not returned, but it can be accessed as an attribute
        of the Command object, as well as the output and the error strings.

        If `stdin` argument is specified, its content will be passed to the
        executed command through the standard input descriptor.

        If the `close_fds` argument is True, all file descriptors
        except 0, 1 and 2 will be closed before the child process is executed.

        If the `check` argument is True, the exit code will be checked
        against the `allowed_retval` list, raising a CommandFailedException if
        not in the list.

        Every keyword argument can be specified both in the class constructor
        and during the method call. If specified in both places,
        the method arguments will take the precedence over
        the constructor arguments.

        :rtype: tuple[str, str]
        :raises: CommandFailedException
        """
        out = []
        err = []
        # If check is true, it must be handled here
        check = kwargs.pop('check', self.check)
        allowed_retval = kwargs.pop('allowed_retval', self.allowed_retval)
        self.execute(out_handler=out.append, err_handler=err.append,
                     check=False, *args, **kwargs)
        self.out = '\n'.join(out)
        self.err = '\n'.join(err)

        # Ensure the output and the error or the command wrapper are
        # really unicode strings
        self.out = _str(self.out)
        self.err = _str(self.err)

        _logger.debug("Command stdout: %s", self.out)
        _logger.debug("Command stderr: %s", self.err)

        # Raise if check and the return code is not in the allowed list
        if check:
            self.check_return_value(allowed_retval)
        return self.out, self.err

    def check_return_value(self, allowed_retval):
        """
        Check the current return code and raise CommandFailedException when
        it's not in the allowed_retval list

        :param list[int] allowed_retval: list of return values considered
            success
        :raises: CommandFailedException
        """
        if self.ret not in allowed_retval:
            raise CommandFailedException(dict(
                ret=self.ret, out=self.out, err=self.err))

    def execute(self, *args, **kwargs):
        """
        Execute the command and pass the output to the configured handlers

        If `stdin` argument is specified, its content will be passed to the
        executed command through the standard input descriptor.

        The subprocess output and error stream will be processed through
        the output and error handler, respectively defined through the
        `out_handler` and `err_handler` arguments. If not provided every line
        will be sent to the log respectively at INFO and WARNING level.

        If the `close_fds` argument is True, all file descriptors
        except 0, 1 and 2 will be closed before the child process is executed.

        If the `check` argument is True, the exit code will be checked
        against the `allowed_retval` list, raising a CommandFailedException if
        not in the list.

        Every keyword argument can be specified both in the class constructor
        and during the method call. If specified in both places,
        the method arguments will take the precedence over
        the constructor arguments.

        :rtype: int
        :raise: CommandFailedException
        """
        # Check keyword arguments
        stdin = kwargs.pop('stdin', None)
        check = kwargs.pop('check', self.check)
        allowed_retval = kwargs.pop('allowed_retval', self.allowed_retval)
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
        _logger.debug("Command return code: %s", self.ret)

        # Raise if check and the return code is not in the allowed list
        if check:
            self.check_return_value(allowed_retval)
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
            cmd = full_command_quote(self.cmd, args)
        else:
            cmd = [self.cmd] + args

        # Log the command we are about to execute
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

    def __init__(self, rsync='rsync', args=None, ssh=None, ssh_options=None,
                 bwlimit=None, exclude=None, exclude_and_protect=None,
                 include=None, network_compression=None, path=None, **kwargs):
        """
        :param str rsync: rsync executable name
        :param list[str]|None args: List of additional argument to aways append
        :param str ssh: the ssh executable to be used when building
            the `-e` argument
        :param list[str] ssh_options: the ssh options to be used when building
            the `-e` argument
        :param str bwlimit: optional bandwidth limit
        :param list[str] exclude: list of file to be excluded from the copy
        :param list[str] exclude_and_protect: list of file to be excluded from
            the copy, preserving the destination if exists
        :param list[str] include: list of files to be included in the copy
            even if excluded.
        :param bool network_compression: enable the network compression
        :param str path: PATH to be used while searching for `cmd`
        :param bool check: Raise a CommandFailedException if the exit code
            is not present in `allowed_retval`
        :param list[int] allowed_retval: List of exit codes considered as a
            successful termination.
        """
        options = []
        if ssh:
            options += ['-e', full_command_quote(ssh, ssh_options)]
        if network_compression:
            options += ['-z']
        # Include patterns must be before the exclude ones, because the exclude
        # patterns actually short-circuit the directory traversal stage
        # when rsync finds the files to send.
        if include:
            for pattern in include:
                options += ["--include=%s" % (pattern,)]
        if exclude:
            for pattern in exclude:
                options += ["--exclude=%s" % (pattern,)]
        if exclude_and_protect:
            for pattern in exclude_and_protect:
                options += ["--exclude=%s" % (pattern,),
                            "--filter=P_%s" % (pattern,)]
        if args:
            options += self._args_for_suse(args)
        if bwlimit is not None and bwlimit > 0:
            options += ["--bwlimit=%s" % bwlimit]

        # By default check is on and the allowed exit code are 0 and 24
        if 'check' not in kwargs:
            kwargs['check'] = True
        if 'allowed_retval' not in kwargs:
            kwargs['allowed_retval'] = (0, 24)
        Command.__init__(self, rsync, args=options, path=path, **kwargs)

    def _args_for_suse(self, args):
        """
        Mangle args for SUSE compatibility

        See https://bugzilla.opensuse.org/show_bug.cgi?id=898513
        """
        # Prepend any argument starting with ':' with a space
        # Workaround for SUSE rsync issue
        return [' ' + a if a.startswith(':') else a for a in args]

    def get_output(self, *args, **kwargs):
        """
        Run the command and return the output and the error (if present)
        """
        # Prepares args for SUSE
        args = self._args_for_suse(args)
        # Invoke the base class method
        return super(Rsync, self).get_output(*args, **kwargs)

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
        self.get_output('--files-from=-', src, dst, *args, **kwargs)
        return self.ret


class RsyncPgData(Rsync):
    """
    This class is a wrapper for rsync, specialised in sync-ing the
    Postgres data directory
    """

    def __init__(self, rsync='rsync', args=None, **kwargs):
        """
        Constructor

        :param str rsync: command to run
        """
        options = ['-rLKpts', '--delete-excluded', '--inplace']
        if args:
            options += args
        Rsync.__init__(self, rsync, args=options, **kwargs)


class PostgreSQLClient(Command):
    """
    Superclass of all the PostgreSQL client commands.
    """

    COMMAND_ALTERNATIVES = None
    """
    Sometimes the name of a command has been changed during the PostgreSQL
    evolution. I.e. that happened with pg_receivexlog, that has been renamed
    to pg_receivewal. In that case, we should try using pg_receivewal (the
    newer auternative) and, if that command doesn't exist, we should try
    using `pg_receivewal`.

    This is a list of command names to be used to find the installed command.
    """

    def __init__(self,
                 connection,
                 command,
                 version=None,
                 app_name=None,
                 path=None,
                 **kwargs):
        """
        Constructor

        :param PostgreSQL connection: an object representing
          a database connection
        :param str command: the command to use
        :param Version version: the command version
        :param str app_name: the application name to use for the connection
        :param str path: additional path for executable retrieval
        """
        Command.__init__(self, command, path=path, **kwargs)

        if version and version >= Version("9.3"):
            # If version of the client is >= 9.3 we use the connection
            # string because allows the user to use all the parameters
            # supported by the libpq library to create a connection
            conn_string = connection.get_connection_string(app_name)
            self.args.append("--dbname=%s" % conn_string)
        else:
            # 9.2 version doesn't support
            # connection strings so the 'split' version of the conninfo
            # option is used instead.
            conn_params = connection.conn_parameters
            self.args.append("--host=%s" % conn_params.get('host', None))
            self.args.append("--port=%s" % conn_params.get('port', None))
            self.args.append("--username=%s" % conn_params.get('user', None))

        self.enable_signal_forwarding(signal.SIGINT)
        self.enable_signal_forwarding(signal.SIGTERM)

    @classmethod
    def find_command(cls, path=None):
        """
        Find the active command, given all the alternatives as set in the
        property named `COMMAND_ALTERNATIVES` in this class.

        :param str path: The path to use while searching for the command
        :rtype: Command
        """

        # TODO: Unit tests of this one

        # To search for an available command, testing if the command
        # exists in PATH is not sufficient. Debian will install wrappers for
        # all commands, even if the real command doesn't work.
        #
        # I.e. we may have a wrapper for `pg_receivewal` even it PostgreSQL
        # 10 isn't installed.
        #
        # This is an example of what can happen in this case:
        #
        # ```
        # $ pg_receivewal --version; echo $?
        # Error: pg_wrapper: pg_receivewal was not found in
        #   /usr/lib/postgresql/9.6/bin
        # 1
        # $ pg_receivexlog --version; echo $?
        # pg_receivexlog (PostgreSQL) 9.6.3
        # 0
        # ```
        #
        # That means we should not only ensure the existence of the command,
        # but we also need to invoke the command to see if it is a shim
        # or not.

        # Get the system path if needed
        if path is None:
            path = os.getenv('PATH')
        # If the path is None at this point we have nothing to search
        if path is None:
            path = ''

        # Search the requested executable in every directory present
        # in path and return a Command object first occurrence that exists,
        # is executable and runs without errors.
        for path_entry in path.split(os.path.pathsep):
            for cmd in cls.COMMAND_ALTERNATIVES:
                full_path = barman.utils.which(cmd, path_entry)

                # It doesn't exist try another
                if not full_path:
                    continue

                # It exists, let's try invoking it with `--version` to check if
                # it's real or not.
                try:
                    command = Command(full_path, path=path, check=True)
                    command("--version")
                    return command
                except CommandFailedException:
                    # It's only a inactive shim
                    continue

        # We don't have such a command
        raise CommandFailedException(
            'command not in PATH, tried: %s' %
            ' '.join(cls.COMMAND_ALTERNATIVES))

    @classmethod
    def get_version_info(cls, path=None):
        """
        Return a dictionary containing all the info about
        the version of the PostgreSQL client

        :param str path: the PATH env
        """
        if cls.COMMAND_ALTERNATIVES is None:
            raise NotImplementedError(
                "get_version_info cannot be invoked on %s" % cls.__name__)

        version_info = dict.fromkeys(('full_path',
                                      'full_version',
                                      'major_version'),
                                     None)

        # Get the version string
        try:
            command = cls.find_command(path)
        except CommandFailedException as e:
            _logger.debug("Error invoking %s: %s", cls.__name__, e)
            return version_info

        version_info['full_path'] = command.cmd
        # Parse the full text version
        try:
            full_version = command.out.strip().split()[-1]
            version_info['full_version'] = Version(full_version)
        except IndexError:
            _logger.debug("Error parsing %s version output",
                          version_info['full_path'])
            return version_info

        # Extract the major version
        version_info['major_version'] = Version(barman.utils.simplify_version(
            full_version))

        return version_info


class PgBaseBackup(PostgreSQLClient):
    """
    Wrapper class for the pg_basebackup system command
    """

    COMMAND_ALTERNATIVES = ['pg_basebackup']

    def __init__(self,
                 connection,
                 destination,
                 command,
                 version=None,
                 app_name=None,
                 bwlimit=None,
                 tbs_mapping=None,
                 immediate=False,
                 check=True,
                 args=None,
                 **kwargs):
        """
        Constructor

        :param PostgreSQL connection: an object representing
          a database connection
        :param str destination: destination directory path
        :param str command: the command to use
        :param Version version: the command version
        :param str app_name: the application name to use for the connection
        :param str bwlimit: bandwidth limit for pg_basebackup
        :param Dict[str, str] tbs_mapping: used for tablespace
        :param bool immediate: fast checkpoint identifier for pg_basebackup
        :param bool check: check if the return value is in the list of
          allowed values of the Command obj
        :param List[str] args: additional arguments
        """
        PostgreSQLClient.__init__(
            self,
            connection=connection, command=command,
            version=version, app_name=app_name,
            check=check, **kwargs)

        # Set the backup destination
        self.args += ['-v', '--no-password', '--pgdata=%s' % destination]

        if version and version >= Version("10"):
            # If version of the client is >= 10 it would use
            # a temporary replication slot by default to keep WALs.
            # We don't need it because Barman already stores the full
            # WAL stream, so we disable this feature to avoid wasting one slot.
            self.args += ['--no-slot']
            # We also need to specify that we do not want to fetch any WAL file
            self.args += ['--wal-method=none']

        # The tablespace mapping option is repeated once for each tablespace
        if tbs_mapping:
            for (tbs_source, tbs_destination) in tbs_mapping.items():
                self.args.append('--tablespace-mapping=%s=%s' %
                                 (tbs_source, tbs_destination))

        # Only global bandwidth limit is supported
        if bwlimit is not None and bwlimit > 0:
            self.args.append("--max-rate=%s" % bwlimit)

        # Immediate checkpoint
        if immediate:
            self.args.append("--checkpoint=fast")

        # Manage additional args
        if args:
            self.args += args


class PgReceiveXlog(PostgreSQLClient):
    """
    Wrapper class for pg_receivexlog
    """

    COMMAND_ALTERNATIVES = ["pg_receivewal", "pg_receivexlog"]

    def __init__(self,
                 connection,
                 destination,
                 command,
                 version=None,
                 app_name=None,
                 synchronous=False,
                 check=True,
                 slot_name=None,
                 args=None,
                 **kwargs):
        """
        Constructor

        :param PostgreSQL connection: an object representing
          a database connection
        :param str destination: destination directory path
        :param str command: the command to use
        :param Version version: the command version
        :param str app_name: the application name to use for the connection
        :param bool synchronous: request synchronous WAL streaming
        :param bool check: check if the return value is in the list of
          allowed values of the Command obj
        :param str slot_name: the replication slot name to use for the
          connection
        :param List[str] args: additional arguments
        """
        PostgreSQLClient.__init__(
            self,
            connection=connection, command=command,
            version=version, app_name=app_name,
            check=check, **kwargs)

        self.args += [
            "--verbose",
            "--no-loop",
            "--no-password",
            "--directory=%s" % destination]

        # Add the replication slot name if set in the configuration.
        if slot_name is not None:
            self.args.append('--slot=%s' % slot_name)
        # Request synchronous mode
        if synchronous:
            self.args.append('--synchronous')

        # Manage additional args
        if args:
            self.args += args


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


def shell_quote(arg):
    """
    Quote a string argument to be safely included in a shell command line.

    :param str arg: The script argument
    :return: The argument quoted
    """

    # This is an excerpt of the Bash manual page, and the same applies for
    # every Posix compliant shell:
    #
    #     A  non-quoted backslash (\) is the escape character.  It preserves
    #     the literal value of the next character that follows, with the
    #     exception of <newline>.  If a \<newline> pair appears, and the
    #     backslash is not itself quoted, the \<newline> is treated as a
    #     line continuation (that is, it is removed  from  the  input
    #     stream  and  effectively ignored).
    #
    #     Enclosing characters in single quotes preserves the literal value
    #     of each character within the quotes.  A single quote may not occur
    #     between single quotes, even when pre-ceded by a backslash.
    #
    # This means that, as long as the original string doesn't contain any
    # apostrophe character, it can be safely included between single quotes.
    #
    # If a single quote is contained in the string, we must terminate the
    # string with a quote, insert an apostrophe character escaping it with
    # a backslash, and then start another string using a quote character.

    assert arg is not None
    return "'%s'" % arg.replace("'", "'\\''")


def full_command_quote(command, args=None):
    """
    Produce a command with quoted arguments

    :param str command: the command to be executed
    :param list[str] args: the command arguments
    :rtype: str
    """
    if args is not None and len(args) > 0:
        return "%s %s" % (
            command, ' '.join([shell_quote(arg) for arg in args]))
    else:
        return command
