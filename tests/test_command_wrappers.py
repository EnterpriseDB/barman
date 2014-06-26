# Copyright (C) 2013-2014 2ndQuadrant Italia (Devise.IT S.r.L.)
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

import unittest
import mock
from subprocess import PIPE
from barman import command_wrappers

try:
    from StringIO import StringIO
except ImportError:  # pragma: no cover
    from io import StringIO


def _mock_pipe(popen, ret=0, out='', err=''):
    pipe = popen.return_value
    pipe.communicate.return_value = (out.encode('utf-8'), err.encode('utf-8'))
    pipe.returncode = ret
    return pipe

@mock.patch('barman.command_wrappers.subprocess.Popen')
class CommandUnitTest(unittest.TestCase):
    def test_simple_invocation(self, popen):
        command = 'command'
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, ret, out, err)

        cmd = command_wrappers.Command(command)
        result = cmd()

        popen.assert_called_with(
            [command], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with(None)
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_failed_invocation(self, popen):
        command = 'command'
        ret = 1
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, ret, out, err)

        cmd = command_wrappers.Command(command)
        result = cmd()

        popen.assert_called_with(
            [command], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with(None)
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_check_failed_invocation(self, popen):
        command = 'command'
        ret = 1
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, ret, out, err)

        cmd = command_wrappers.Command(command, check=True)
        try:
            cmd()
        except command_wrappers.CommandFailedException as e:
            assert e.args[0]['ret'] == ret
            assert e.args[0]['out'] == out
            assert e.args[0]['err'] == err
        else:  # pragma: no cover
            self.fail('Exception expected')
        popen.assert_called_with(
            [command], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with(None)
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_shell_invocation(self, popen):
        command = 'test -n'
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, ret, out, err)

        cmd = command_wrappers.Command(command, shell=True)
        result = cmd('shell test')

        popen.assert_called_with(
            "test -n 'shell test'", shell=True, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with(None)
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_declaration_args_invocation(self, popen):
        command = 'command'
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, ret, out, err)

        cmd = command_wrappers.Command(command, args=['one', 'two'])
        result = cmd()

        popen.assert_called_with(
            [command, 'one', 'two'], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with(None)
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_call_args_invocation(self, popen):
        command = 'command'
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, ret, out, err)

        cmd = command_wrappers.Command(command)
        result = cmd('one', 'two')

        popen.assert_called_with(
            [command, 'one', 'two'], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with(None)
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_both_args_invocation(self, popen):
        command = 'command'
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, ret, out, err)

        cmd = command_wrappers.Command(command, args=['a', 'b'])
        result = cmd('one', 'two')

        popen.assert_called_with(
            [command, 'a', 'b', 'one', 'two'], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with(None)
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_env_invocation(self, popen):
        command = 'command'
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, ret, out, err)

        with mock.patch('os.environ', new={'TEST0': 'VAL0'}):
            cmd = command_wrappers.Command(command,
                                           env_append={'TEST1': 'VAL1',
                                                       'TEST2': 'VAL2'})
            result = cmd()

        popen.assert_called_with(
            [command], shell=False,
            env={'TEST0': 'VAL0', 'TEST1': 'VAL1', 'TEST2': 'VAL2'},
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with(None)
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_debug_invocation(self, popen):
        command = 'command'
        ret = 1
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, ret, out, err)

        stdout = StringIO()
        stderr = StringIO()
        with mock.patch.multiple('sys', stdout=stdout, stderr=stderr):
            cmd = command_wrappers.Command(command, debug=True)
            result = cmd()

        popen.assert_called_with(
            [command], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with(None)
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

        assert stdout.getvalue() == ""
        assert stderr.getvalue() == "Command: ['command']\n" \
                                    "Command return code: 1\n"

    def test_getoutput_invocation(self, popen):
        command = 'command'
        ret = 0
        out = 'out'
        err = 'err'
        stdin = 'in'

        pipe = _mock_pipe(popen, ret, out, err)

        with mock.patch('os.environ', new={'TEST0': 'VAL0'}):
            cmd = command_wrappers.Command(command,
                                           env_append={'TEST1': 'VAL1',
                                                       'TEST2': 'VAL2'})
            result = cmd.getoutput(stdin=stdin)

        popen.assert_called_with(
            [command], shell=False,
            env={'TEST0': 'VAL0', 'TEST1': 'VAL1', 'TEST2': 'VAL2'},
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with(stdin)
        assert result == (out, err)
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err


@mock.patch('barman.command_wrappers.subprocess.Popen')
class RsyncUnitTest(unittest.TestCase):
    def test_simple_invocation(self, popen):
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, ret, out, err)

        cmd = command_wrappers.Rsync()
        result = cmd('src', 'dst')

        popen.assert_called_with(
            ['rsync', 'src', 'dst'], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with(None)
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_args_invocation(self, popen):
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, ret, out, err)

        cmd = command_wrappers.Rsync(args=['a', 'b'])
        result = cmd('src', 'dst')

        popen.assert_called_with(
            ['rsync', 'a', 'b', 'src', 'dst'], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with(None)
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_custom_ssh_invocation(self, popen):
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, ret, out, err)

        cmd = command_wrappers.Rsync('/custom/rsync', ssh='/custom/ssh',
                                     ssh_options=['-c', 'arcfour'])
        result = cmd('src', 'dst')

        popen.assert_called_with(
            ['/custom/rsync', '-e', "/custom/ssh '-c' 'arcfour'", 'src', 'dst'],
            shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with(None)
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_protect_ssh_invocation(self, popen):
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, ret, out, err)

        cmd = command_wrappers.Rsync(exclude_and_protect=['foo', 'bar'])
        result = cmd('src', 'dst')

        popen.assert_called_with(
            ['rsync',
             '--exclude=foo', '--filter=P_foo',
             '--exclude=bar', '--filter=P_bar',
             'src', 'dst'],
            shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with(None)
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_bwlimit_ssh_invocation(self, popen):
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, ret, out, err)

        cmd = command_wrappers.Rsync(bwlimit=101)
        result = cmd('src', 'dst')

        popen.assert_called_with(
            ['rsync', '--bwlimit=101', 'src', 'dst'],
            shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with(None)
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_from_file_list_ssh_invocation(self, popen):
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, ret, out, err)

        cmd = command_wrappers.Rsync()
        result = cmd.from_file_list(['a', 'b', 'c'], 'src', 'dst')

        popen.assert_called_with(
            ['rsync', '--files-from=-', 'src', 'dst'],
            shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with('a\nb\nc'.encode('UTF-8'))
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err


@mock.patch('barman.command_wrappers.subprocess.Popen')
class RsyncPgdataUnitTest(unittest.TestCase):
    def _mock_pipe(self, popen, ret=0, out=None, err=None):
        pipe = popen.return_value
        pipe.communicate.return_value = (out, err)
        pipe.returncode = ret
        return pipe

    def test_simple_invocation(self, popen):
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, ret, out, err)

        cmd = command_wrappers.RsyncPgData()
        result = cmd('src', 'dst')

        popen.assert_called_with(
            [
                'rsync', '-rLKpts', '--delete-excluded', '--inplace',
                '--exclude=/pg_xlog/*', '--exclude=/pg_log/*',
                '--exclude=/recovery.conf',
                '--exclude=/postmaster.pid', 'src', 'dst'
            ],
            shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with(None)
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_args_invocation(self, popen):
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, ret, out, err)

        cmd = command_wrappers.RsyncPgData(args=['a', 'b'])
        result = cmd('src', 'dst')

        popen.assert_called_with(
            [
                'rsync', '-rLKpts', '--delete-excluded', '--inplace',
                '--exclude=/pg_xlog/*', '--exclude=/pg_log/*',
                '--exclude=/recovery.conf',
                '--exclude=/postmaster.pid', 'a', 'b', 'src', 'dst'
            ],
            shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.communicate.assert_called_with(None)
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err
