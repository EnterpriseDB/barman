# Copyright (C) 2011, 2012 2ndQuadrant Italia (Devise.IT S.r.L.)
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

''' This module contains a wrapper for shell commands
'''

import sys
import signal
import subprocess
import os
import logging

_logger = logging.getLogger(__name__)


class CommandFailedException(Exception):
    ''' Exception which represents a failed command '''
    pass


class Command(object):
    ''' Simple wrapper for a shell command '''
    def __init__(self, cmd, args=[], env_append=None, shell=False, check=False, debug=False):
        self.cmd = cmd
        self.args = args
        self.shell = shell
        self.check = check
        self.debug = debug
        if env_append:
            self.env = os.environ.copy()
            self.env.update(env_append)
        else:
            self.env = None

    def _cmd_quote(self, cmd, args):
        ''' Quote all cmd's arguments.

        This is needed to avoid command string breaking.
        WARNING: this function does not protect against injection.
        '''
        if args != None and len(args) > 0:
            cmd = "%s '%s'" % (cmd, "' '".join(args))
        return cmd

    def __call__(self, *args):
        def restore_sigpipe():
            "restore default signal handler (http://bugs.python.org/issue1652)"
            signal.signal(signal.SIGPIPE, signal.SIG_DFL)

        args = self.args + list(args)
        if self.shell:
            cmd = self._cmd_quote(self.cmd, args)
        else:
            cmd = [self.cmd] + args
        if self.debug:
            print >> sys.stderr, "__call__: %r" % (cmd)
        _logger.debug("__call__: %r", cmd)
        ret = subprocess.call(cmd, shell=self.shell, env=self.env, preexec_fn=restore_sigpipe)
        if self.debug:
            print >> sys.stderr, "__call__ return code: %s" % (ret)
        _logger.debug("__call__ return code: %s", ret)
        if self.check and ret != 0:
            raise CommandFailedException, ret
        return ret

    def getoutput(self, stdin=None, *args):
        ''' Return the output and the error (if present)
        '''
        args = self.args + list(args)
        if self.shell:
            cmd = self._cmd_quote(self.cmd, args)
        else:
            cmd = [self.cmd] + args
        if self.debug:
            print >> sys.stderr, "getoutput: %r" % (cmd)
        _logger.debug("getoutput: %r", cmd)
        pipe = subprocess.Popen(cmd, shell=self.shell, env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = pipe.communicate(stdin)
        ret = pipe.returncode
        if self.debug:
            print >> sys.stderr, "getoutput return code: %s" % (ret)
        _logger.debug("getoutput return code: %s", ret)
        if self.check and ret != 0:
            raise CommandFailedException, (ret , out, err)
        return out, err

class Rsync(Command):
    '''
    This class is a wrapper for the rsync system command,
    which is used vastly by barman
    '''
    def __init__(self, rsync='rsync', args=[], ssh=None, ssh_options=None, debug=False):
        if ssh:
            options = ['-e', self._cmd_quote(ssh, ssh_options)] + args
        else:
            options = args
        Command.__init__(self, rsync, options, debug=debug)

    def from_file_list(self, filelist, src, dst):
        ''' This methods copies filelist from src to dst.

        Returns the returncode of the rsync command
        '''
        def restore_sigpipe():
            ' Restore default signal handler (http://bugs.python.org/issue1652)'
            signal.signal(signal.SIGPIPE, signal.SIG_DFL)

        cmd = [self.cmd] + self.args + ['--files-from=-', src, dst]
        if self.debug:
            print >> sys.stderr, "RUN: %r" % (cmd)
        _logger.debug("RUN: %r", cmd)
        pipe = subprocess.Popen(cmd, preexec_fn=restore_sigpipe, stdin=subprocess.PIPE)
        pipe.communicate('\n'.join(filelist))
        _logger.debug("FILELIST: %r", filelist)
        ret = pipe.wait()
        if self.debug:
            print >> sys.stderr, "RET: %s" % (ret)
        _logger.debug("RUN: %s", ret)
        return ret

class RsyncPgData(Rsync):
    ''' This class is a wrapper for rsync, specialized in Postgres data directory syncing
    '''
    def __init__(self, rsync='rsync', args=[], ssh=None, ssh_options=None, debug=False):
        options = ['-rLKpts', '--delete-excluded', '--inplace',
                   '--exclude=/pg_xlog/*',
                   '--exclude=/pg_log/*',
                   '--exclude=/postmaster.pid'
                   ] + args
        Rsync.__init__(self, rsync, options, ssh, ssh_options, debug)

