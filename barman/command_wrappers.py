#
# barman - Backup and Recovery Manager for PostgreSQL
#
# Copyright (C) 2011  Devise.IT S.r.l. <info@2ndquadrant.it>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import sys
import signal
import subprocess
import os

class Command(object):
    """
    Simple wrapper for a shell command
    """

    def __init__(self, cmd, args=[], env_append=None, shell=False, debug=False):
        self.cmd = cmd
        self.args = args
        self.shell = shell
        self.debug = debug
        if env_append:
            self.env = os.environ.copy()
            self.env.update(env_append)
        else:
            self.env = None

    def _cmd_quote(self, cmd, args):
        if len(args) > 0:
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
            print >> sys.stderr, "RUN: %r" % (cmd)
        ret = subprocess.call(cmd, shell=self.shell, env=self.env, preexec_fn=restore_sigpipe)
        if self.debug:
            print >> sys.stderr, "RET: %s" % (ret)
        return ret

class Rsync(Command):
    def __init__(self, rsync='rsync', args=[], ssh=None, ssh_options=None, debug=False):
        if ssh:
            options = ['-e', self._cmd_quote(ssh, ssh_options)] + args
        else:
            options = args
        Command.__init__(self, rsync, options, debug=debug)

    def from_file_list(self, filelist, src, dst):
        def restore_sigpipe():
            "restore default signal handler (http://bugs.python.org/issue1652)"
            signal.signal(signal.SIGPIPE, signal.SIG_DFL)

        cmd = [self.cmd] + self.args + ['--files-from=-', src, dst]
        if self.debug:
            print >> sys.stderr, "RUN: %r" % (cmd)
        pipe = subprocess.Popen(cmd, preexec_fn=restore_sigpipe, stdin=subprocess.PIPE)
        pipe.communicate('\n'.join(filelist))
        ret = pipe.wait()
        if self.debug:
            print >> sys.stderr, "RET: %s" % (ret)
        return ret

class RsyncPgData(Rsync):
    def __init__(self, rsync='rsync', args=[], ssh=None, ssh_options=None, debug=False):
        options = ['-rLKpts', '--delete', '--inplace', '--exclude=/pg_xlog/*', '--exclude=/pg_log/*', '--exclude=/postmaster.pid'] + args
        Rsync.__init__(self, rsync, options, ssh, ssh_options, debug)
