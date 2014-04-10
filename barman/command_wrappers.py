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

import sys
import signal
import subprocess
import os
import logging

_logger = logging.getLogger(__name__)


class CommandFailedException(Exception):
    """
    Exception which represents a failed command
    """
    pass


class Command(object):
    """
    Simple wrapper for a shell command
    """

    def __init__(self, cmd, args=None, env_append=None, shell=False,
                 check=False, allowed_retval=(0,), debug=False):
        self.cmd = cmd
        self.args = args if args is not None else []
        self.shell = shell
        self.check = check
        self.allowed_retval = allowed_retval
        self.debug = debug
        if env_append:
            self.env = os.environ.copy()
            self.env.update(env_append)
        else:
            self.env = None

    def _restore_sigpipe(self):
        """restore default signal handler (http://bugs.python.org/issue1652)"""
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)  # pragma: no cover

    def _cmd_quote(self, cmd, args):
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
        check = kwargs.pop('check', None)
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
                                preexec_fn=self._restore_sigpipe)
        self.out, self.err = pipe.communicate(stdin)
        self.ret = pipe.returncode
        if self.debug:
            print >> sys.stderr, "Command return code: %s" % self.ret
        _logger.debug("Command return code: %s", self.ret)
        _logger.debug("Command stdout: %s", self.out)
        _logger.debug("Command stderr: %s", self.err)
        is_checking = check if check is not None else self.check
        if is_checking and self.ret not in self.allowed_retval:
            raise CommandFailedException(dict(
                ret=self.ret, out=self.out, err=self.err))
        return self.out, self.err


class Rsync(Command):
    """
    This class is a wrapper for the rsync system command,
    which is used vastly by barman
    """

    def __init__(self, rsync='rsync', args=None, ssh=None, ssh_options=None,
                 bwlimit=None, exclude_and_protect=None,
                 network_compression=None, check=True, allowed_retval=(0,24),
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

    def from_file_list(self, filelist, src, dst):
        """
        This methods copies filelist from src to dst.

        Returns the return code of the rsync command
        """
        input_string = ('\n'.join(filelist)).encode('UTF-8')
        _logger.debug("from_file_list: %r", filelist)
        self.getoutput('--files-from=-', src, dst, stdin=input_string)
        return self.ret


class RsyncPgData(Rsync):
    """
    This class is a wrapper for rsync, specialized in Postgres data
    directory syncing
    """

    def __init__(self, rsync='rsync', args=None, **kwargs):
        options = [
            '-rLKptsc', '--delete-excluded', '--inplace',
            '--exclude=/pg_xlog/*',
            '--exclude=/pg_log/*',
            '--exclude=/recovery.conf',
            '--exclude=/postmaster.pid'
        ]
        if args:
            options += args
        Rsync.__init__(self, rsync, args=options, **kwargs)
