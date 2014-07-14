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
This module contains a wrapper for shell commands
"""
import inspect
import shutil

import sys
import signal
import subprocess
import os
import logging
import re
import collections
import tempfile
import dateutil.parser
import dateutil.tz

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


class Command(object):
    """
    Simple wrapper for a shell command
    """

    def __init__(self, cmd, args=None, env_append=None, shell=False,
                 check=False, allowed_retval=(0,), debug=False,
                 close_fds=True):
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
        if env_append:
            self.env = os.environ.copy()
            self.env.update(env_append)
        else:
            self.env = None

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
        # check keyword arguments
        stdin = kwargs.pop('stdin', None)
        check = kwargs.pop('check', self.check)
        close_fds = kwargs.pop('close_fds', self.close_fds)
        if len(kwargs):
            raise TypeError('%s() got an unexpected keyword argument %r' %
                            (inspect.stack()[1][3], kwargs.popitem()[0]))
        args = self.args + list(args)
        if self.shell:
            cmd = self._cmd_quote(self.cmd, args)
        else:
            cmd = [self.cmd] + args
        if self.debug:
            print >> sys.stderr, "Command: %r" % cmd
        _logger.debug("Command: %r", cmd)
        pipe = subprocess.Popen(cmd, shell=self.shell, env=self.env,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                preexec_fn=self._restore_sigpipe,
                                close_fds=close_fds)
        out, err = pipe.communicate(stdin)
        # Convert output to a proper unicode string
        self.out = out.decode('utf-8')
        self.err = err.decode('utf-8')
        self.ret = pipe.returncode
        if self.debug:
            print >> sys.stderr, "Command return code: %s" % self.ret
        _logger.debug("Command return code: %s", self.ret)
        _logger.debug("Command stdout: %s", self.out)
        _logger.debug("Command stderr: %s", self.err)
        if check and self.ret not in self.allowed_retval:
            raise CommandFailedException(dict(
                ret=self.ret, out=self.out, err=self.err))
        return self.out, self.err


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
        rsync\ error:\ .* \(code\ 23\)\ at\ main\.c\(\d+\)\ \[generator=[^\]]+\]
        )
        $ # end of the line
    ''')

    # This named tuple is used to parse each line of the output
    # of a "rsync --list-only" call
    FileItem = collections.namedtuple('FileItem', 'mode size date path')

    def __init__(self, rsync='rsync', args=None, ssh=None, ssh_options=None,
                 bwlimit=None, exclude_and_protect=None,
                 network_compression=None, check=True, allowed_retval=(0, 24),
                 **kwargs):
        options = []
        if ssh:
            options += ['-e', self._cmd_quote(ssh, ssh_options)]
        if network_compression:
            options += ['-z']
        if exclude_and_protect:
            for path in exclude_and_protect:
                options += ["--exclude=%s" % (path,), "--filter=P_%s" % (path,)]
        if args:
            options += args
        if bwlimit is not None and bwlimit > 0:
            options += ["--bwlimit=%s" % bwlimit]
        Command.__init__(self, rsync, args=options, check=check,
                         allowed_retval=allowed_retval, **kwargs)

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

    def smart_copy(self, src, dst, safe_horizon=None):
        """
        Recursively copies files from "src" to "dst" in a way that is safe from
        the point of view of a PostgreSQL backup.
        The "safe_horizon" parameter is the timestamp of the beginning of the
        older backup involved in copy (as source or destination). Any files
        updated after that timestamp, must be checked as they could have been
        modified during the backup - and we do not reply WAL files to update
        them.

        If the "safe_horizon" parameter is None, we cannot make any
        assumptions about what can be considered "safe", so we must check
        everything with checksums enabled.

        If src or dst argument begin with a ':' character, it is a remote path

        :param str src: the source path
        :param str dst: the destination path
        :param datetime.datetime safe_horizon: anything after this time
            has to be checked
        :except CommandFailedException: If rsync failed at any time
        :except RsyncListFilesFailure: If source rsync output format is unknown
        """
        _logger.info("Smart copy: %r -> %r (safe before %r)",
                     src, dst, safe_horizon)

        # Make sure the dst path ends with a '/' or rsync will add the
        # last path component to all the returned items during listing
        if dst[-1] != '/':
            dst += '/'

        # Build a hash containing all files present on destination.
        # Directories are not included
        _logger.info("Smart copy step 1/4: preparation")
        try:
            dst_hash = dict((
                (item.path, item)
                for item in self.list_files(dst)
                if item.mode[0] != 'd'))
        except (CommandFailedException, RsyncListFilesFailure):
            # Here we set dst_hash to None, thus disable the code that marks as
            # "safe matching" those destination files with different time or
            # size, even if newer than "safe_horizon". As a result, all files
            # newer than "safe_horizon" will be checked through checksums.
            dst_hash = None
            _logger.exception(
                "Unable to retrieve destination file list. "
                "Using only source file information to decide which files need "
                "to be copied with checksums enabled")

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
            # The 'protect.list' file will contain a filter rule to protect each
            # file present in the source tree. It will be used during
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

                # If dst_hash is None, it means we failed to retrieve the
                # destination file list. We assume the only safe way is to
                # check every file that is older than safe_horizon
                if dst_hash is None:
                    check_list.write(item.path + '\n')
                    continue

                # If source file differs by time or size from the matching
                # destination, rsync will discover the difference in any case.
                # It is then safe to skip checksum check here.
                dst_item = dst_hash.get(item.path, None)
                if (dst_item is None
                        or dst_item.size != item.size
                        or dst_item.date != item.date):
                    safe_list.write(item.path + '\n')
                    continue

                # All remaining files must be checked with checksums enabled
                check_list.write(item.path + '\n')

            # Close all the control files
            dir_list.close()
            safe_list.close()
            check_list.close()
            exclude_and_protect_filter.close()

            # TODO: remove debug output when the procedure is marked as 'stable'
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

            # TODO: remove debug output when the procedure is marked as 'stable'
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
