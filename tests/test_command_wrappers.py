# Copyright (C) 2013-2016 2ndQuadrant Italia Srl
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

import errno
import os
import select
import sys
from datetime import datetime
from logging import DEBUG, INFO, WARNING
from subprocess import PIPE

import dateutil.tz
import mock
import pytest

from barman import command_wrappers
from barman.command_wrappers import CommandFailedException, StreamLineProcessor

try:
    from StringIO import StringIO
except ImportError:  # pragma: no cover
    from io import StringIO


def _mock_pipe(popen, pipe_processor_loop, ret=0, out='', err=''):
    pipe = popen.return_value
    pipe.communicate.return_value = (out.encode('utf-8'), err.encode('utf-8'))
    pipe.returncode = ret

    # noinspection PyProtectedMember
    def ppl(processors):
        for processor in processors:
            if processor.fileno() == pipe.stdout.fileno.return_value:
                for line in out.split('\n'):
                    processor._handler(line)
            if processor.fileno() == pipe.stderr.fileno.return_value:
                for line in err.split('\n'):
                    processor._handler(line)
    pipe_processor_loop.side_effect = ppl
    return pipe


# noinspection PyMethodMayBeStatic
@mock.patch('barman.command_wrappers.Command.pipe_processor_loop')
@mock.patch('barman.command_wrappers.subprocess.Popen')
class TestCommand(object):

    def test_simple_invocation(self, popen, pipe_processor_loop):
        command = 'command'
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command)
        result = cmd()

        popen.assert_called_with(
            [command], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_multiline_output(self, popen, pipe_processor_loop):
        command = 'command'
        ret = 0
        out = 'line1\nline2\n'
        err = 'err1\nerr2\n'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command)
        result = cmd()

        popen.assert_called_with(
            [command], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_failed_invocation(self, popen, pipe_processor_loop):
        command = 'command'
        ret = 1
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command)
        result = cmd()

        popen.assert_called_with(
            [command], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_check_failed_invocation(self, popen, pipe_processor_loop):
        command = 'command'
        ret = 1
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command, check=True)
        with pytest.raises(command_wrappers.CommandFailedException) as excinfo:
            cmd()
        assert excinfo.value.args[0]['ret'] == ret
        assert excinfo.value.args[0]['out'] == out
        assert excinfo.value.args[0]['err'] == err

        popen.assert_called_with(
            [command], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_shell_invocation(self, popen, pipe_processor_loop):
        command = 'test -n'
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command, shell=True)
        result = cmd('shell test')

        popen.assert_called_with(
            "test -n 'shell test'", shell=True, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_declaration_args_invocation(self, popen, pipe_processor_loop):
        command = 'command'
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command, args=['one', 'two'])
        result = cmd()

        popen.assert_called_with(
            [command, 'one', 'two'], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_call_args_invocation(self, popen, pipe_processor_loop):
        command = 'command'
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command)
        result = cmd('one', 'two')

        popen.assert_called_with(
            [command, 'one', 'two'], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_both_args_invocation(self, popen, pipe_processor_loop):
        command = 'command'
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command, args=['a', 'b'])
        result = cmd('one', 'two')

        popen.assert_called_with(
            [command, 'a', 'b', 'one', 'two'], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_env_invocation(self, popen, pipe_processor_loop):
        command = 'command'
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

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
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_path_invocation(self, popen, pipe_processor_loop):
        command = 'command'
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        with mock.patch('os.environ', new={'TEST0': 'VAL0'}):
            cmd = command_wrappers.Command(command,
                                           path='/path/one:/path/two')
            result = cmd()

        popen.assert_called_with(
            [command], shell=False,
            env={'TEST0': 'VAL0', 'PATH': '/path/one:/path/two'},
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_env_path_invocation(self, popen, pipe_processor_loop):
        command = 'command'
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        with mock.patch('os.environ', new={'TEST0': 'VAL0'}):
            cmd = command_wrappers.Command(command,
                                           path='/path/one:/path/two',
                                           env_append={'TEST1': 'VAL1',
                                                       'TEST2': 'VAL2'})
            result = cmd()

        popen.assert_called_with(
            [command], shell=False,
            env={'TEST0': 'VAL0', 'TEST1': 'VAL1', 'TEST2': 'VAL2',
                 'PATH': '/path/one:/path/two'},
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_debug_invocation(self, popen, pipe_processor_loop):
        command = 'command'
        ret = 1
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

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
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

        assert stdout.getvalue() == ""
        assert stderr.getvalue() == "Command: ['command']\n" \
                                    "Command return code: 1\n"

    def test_getoutput_invocation(self, popen, pipe_processor_loop):
        command = 'command'
        ret = 0
        out = 'out'
        err = 'err'
        stdin = 'in'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

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
        pipe.stdin.write.assert_called_with(stdin)
        pipe.stdin.close.assert_called_once_with()
        assert result == (out, err)
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_execute_invocation(self, popen, pipe_processor_loop,
                                caplog):
        command = 'command'
        ret = 0
        out = 'out'
        err = 'err'
        stdin = 'in'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        with mock.patch('os.environ', new={'TEST0': 'VAL0'}):
            cmd = command_wrappers.Command(command,
                                           env_append={'TEST1': 'VAL1',
                                                       'TEST2': 'VAL2'})
            result = cmd.execute(stdin=stdin)

        popen.assert_called_with(
            [command], shell=False,
            env={'TEST0': 'VAL0', 'TEST1': 'VAL1', 'TEST2': 'VAL2'},
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.stdin.write.assert_called_with(stdin)
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out is None
        assert cmd.err is None
        assert ('Command', INFO, out) in caplog.record_tuples
        assert ('Command', WARNING, err) in caplog.record_tuples

    def test_execute_invocation_multiline(self, popen, pipe_processor_loop,
                                          caplog):
        command = 'command'
        ret = 0
        out = 'line1\nline2\n'
        err = 'err1\nerr2'  # no final newline here
        stdin = 'in'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        with mock.patch('os.environ', new={'TEST0': 'VAL0'}):
            cmd = command_wrappers.Command(command,
                                           env_append={'TEST1': 'VAL1',
                                                       'TEST2': 'VAL2'})
            result = cmd.execute(stdin=stdin)

        popen.assert_called_with(
            [command], shell=False,
            env={'TEST0': 'VAL0', 'TEST1': 'VAL1', 'TEST2': 'VAL2'},
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.stdin.write.assert_called_with(stdin)
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out is None
        assert cmd.err is None
        for line in out.splitlines():
            assert ('Command', INFO, line) in caplog.record_tuples
        assert ('Command', INFO, '') not in caplog.record_tuples
        assert ('Command', INFO, None) not in caplog.record_tuples
        for line in err.splitlines():
            assert ('Command', WARNING, line) in caplog.record_tuples
        assert ('Command', WARNING, '') not in caplog.record_tuples
        assert ('Command', WARNING, None) not in caplog.record_tuples

    def test_execute_check_failed_invocation(self, popen,
                                             pipe_processor_loop,
                                             caplog):
        command = 'command'
        ret = 1
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command, check=True)
        with pytest.raises(command_wrappers.CommandFailedException) as excinfo:
            cmd.execute()
        assert excinfo.value.args[0]['ret'] == ret
        assert excinfo.value.args[0]['out'] is None
        assert excinfo.value.args[0]['err'] is None

        popen.assert_called_with(
            [command], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert cmd.ret == ret
        assert cmd.out is None
        assert cmd.err is None
        assert ('Command', INFO, out) in caplog.record_tuples
        assert ('Command', WARNING, err) in caplog.record_tuples

    def test_handlers_multiline(self, popen, pipe_processor_loop, caplog):
        command = 'command'
        ret = 0
        out = 'line1\nline2\n'
        err = 'err1\nerr2'  # no final newline here
        stdin = 'in'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        out_list = []
        err_list = []
        with mock.patch('os.environ', new={'TEST0': 'VAL0'}):
            cmd = command_wrappers.Command(command,
                                           env_append={'TEST1': 'VAL1',
                                                       'TEST2': 'VAL2'},
                                           out_handler=out_list.append,
                                           err_handler=err_list.append)
            result = cmd.execute(stdin=stdin)

        popen.assert_called_with(
            [command], shell=False,
            env={'TEST0': 'VAL0', 'TEST1': 'VAL1', 'TEST2': 'VAL2'},
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.stdin.write.assert_called_with(stdin)
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out is None
        assert cmd.err is None
        assert '\n'.join(out_list) == out
        assert '\n'.join(err_list) == err

    def test_execute_handlers(self, popen, pipe_processor_loop, caplog):
        command = 'command'
        ret = 0
        out = 'out'
        err = 'err'
        stdin = 'in'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        with mock.patch('os.environ', new={'TEST0': 'VAL0'}):
            cmd = command_wrappers.Command(command,
                                           env_append={'TEST1': 'VAL1',
                                                       'TEST2': 'VAL2'})
            result = cmd.execute(
                stdin=stdin,
                out_handler=cmd.make_logging_handler(INFO, 'out: '),
                err_handler=cmd.make_logging_handler(WARNING, 'err: '),
            )

        popen.assert_called_with(
            [command], shell=False,
            env={'TEST0': 'VAL0', 'TEST1': 'VAL1', 'TEST2': 'VAL2'},
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.stdin.write.assert_called_with(stdin)
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out is None
        assert cmd.err is None
        assert ('Command', INFO, 'out: ' + out) in caplog.record_tuples
        assert ('Command', WARNING, 'err: ' + err) in caplog.record_tuples


# noinspection PyMethodMayBeStatic
class TestCommandPipeProcessorLoop(object):

    @mock.patch('barman.command_wrappers.select.select')
    @mock.patch('barman.command_wrappers.os.read')
    def test_ppl(self, read_mock, select_mock):
        # Simulate the two files
        stdout = mock.Mock(name='pipe.stdout')
        stdout.fileno.return_value = 65
        stderr = mock.Mock(name='pipe.stderr')
        stderr.fileno.return_value = 66

        # Recipients for results
        out_list = []
        err_list = []

        # StreamLineProcessors
        out_proc = StreamLineProcessor(stdout, out_list.append)
        err_proc = StreamLineProcessor(stderr, err_list.append)

        # The select call always returns all the streams
        select_mock.side_effect = [
            [[out_proc, err_proc], [], []],
            select.error(errno.EINTR),  # Test interrupted system call
            [[out_proc, err_proc], [], []],
            [[out_proc, err_proc], [], []],
        ]

        # The read calls return out and err interleaved
        # Lines are split in various ways, to test all the code paths
        read_mock.side_effect = ['line1\nl'.encode('utf-8'),
                                 'err'.encode('utf-8'),
                                 'ine2'.encode('utf-8'),
                                 '1\nerr2\n'.encode('utf-8'),
                                 '', '',
                                 Exception]  # Make sure it terminates

        command_wrappers.Command.pipe_processor_loop([out_proc, err_proc])

        # Check the calls order and the output
        assert read_mock.mock_calls == [
            mock.call(65, 4096),
            mock.call(66, 4096),
            mock.call(65, 4096),
            mock.call(66, 4096),
            mock.call(65, 4096),
            mock.call(66, 4096),
        ]
        assert out_list == ['line1', 'line2']
        assert err_list == ['err1', 'err2', '']

    @mock.patch('barman.command_wrappers.select.select')
    def test_ppl_select_failure(self, select_mock):
        # Test if select errors are passed through
        select_mock.side_effect = select.error('not good')

        with pytest.raises(select.error):
            command_wrappers.Command.pipe_processor_loop([None])


# noinspection PyMethodMayBeStatic
@mock.patch('barman.command_wrappers.Command.pipe_processor_loop')
@mock.patch('barman.command_wrappers.subprocess.Popen')
class TestRsync(object):

    def test_simple_invocation(self, popen, pipe_processor_loop):
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Rsync()
        result = cmd('src', 'dst')

        popen.assert_called_with(
            ['rsync', 'src', 'dst'], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_args_invocation(self, popen, pipe_processor_loop):
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Rsync(args=['a', 'b'])
        result = cmd('src', 'dst')

        popen.assert_called_with(
            ['rsync', 'a', 'b', 'src', 'dst'], shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    @mock.patch("barman.utils.which")
    def test_custom_ssh_invocation(self, mock_which,
                                   popen, pipe_processor_loop):
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)
        mock_which.return_value = True
        cmd = command_wrappers.Rsync('/custom/rsync', ssh='/custom/ssh',
                                     ssh_options=['-c', 'arcfour'])
        result = cmd('src', 'dst')

        mock_which.assert_called_with('/custom/rsync', None)
        popen.assert_called_with(
            ['/custom/rsync', '-e', "/custom/ssh '-c' 'arcfour'",
                'src', 'dst'],
            shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_rsync_build_failure(self, popen, pipe_processor_loop):
        """
        Simple test that checks if a CommandFailedException is raised
        when Rsync object is build with an invalid path or rsync
        is not in system path
        """
        # Pass an invalid path to Rsync class constructor.
        # Expect a CommandFailedException
        with pytest.raises(command_wrappers.CommandFailedException):
            command_wrappers.Rsync('/invalid/path/rsync')
        # Force the which method to return false, simulating rsync command not
        # present in system PATH. Expect a CommandFailedExceptiomn
        with mock.patch("barman.utils.which") as mock_which:
            mock_which.return_value = False
            with pytest.raises(command_wrappers.CommandFailedException):
                command_wrappers.Rsync(ssh_options=['-c', 'arcfour'])

    def test_protect_ssh_invocation(self, popen, pipe_processor_loop):
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        with mock.patch('os.environ.copy') as which_mock:
            which_mock.return_value = {}
            cmd = command_wrappers.Rsync(exclude_and_protect=['foo', 'bar'])
            result = cmd('src', 'dst')

        popen.assert_called_with(
            ['rsync',
             '--exclude=foo', '--filter=P_foo',
             '--exclude=bar', '--filter=P_bar',
             'src', 'dst'],
            shell=False, env=mock.ANY,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_bwlimit_ssh_invocation(self, popen, pipe_processor_loop):
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Rsync(bwlimit=101)
        result = cmd('src', 'dst')

        popen.assert_called_with(
            ['rsync', '--bwlimit=101', 'src', 'dst'],
            shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_from_file_list_ssh_invocation(self, popen, pipe_processor_loop):
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Rsync()
        result = cmd.from_file_list(['a', 'b', 'c'], 'src', 'dst')

        popen.assert_called_with(
            ['rsync', '--files-from=-', 'src', 'dst'],
            shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        pipe.stdin.write.assert_called_with('a\nb\nc'.encode('UTF-8'))
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_invocation_list_file(self, popen, pipe_processor_loop):
        """
        Unit test for dateutil package in list_file

        This test cover all list_file's code with correct parameters

        :param tmpdir: temporary folder
        :param popen: mock popen
        """
        # variables to be tested
        ret = 0
        out = 'drwxrwxrwt       69632 2015/02/09 15:01:00 tmp\n' \
              'drwxrwxrwt       69612 2015/02/19 15:01:22 tmp2'
        err = 'err'
        # created mock pipe
        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)
        # created rsync and launched list_files
        cmd = command_wrappers.Rsync()
        return_values = list(cmd.list_files('some/path'))

        # returned list must contain two elements
        assert len(return_values) == 2

        # assert call
        popen.assert_called_with(
            ['rsync', '--no-human-readable', '--list-only', '-r', 'some/path'],
            shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )

        # Rsync pipe must be called with no input
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()

        # assert tmp and tmp2 in test_list
        assert return_values[0] == cmd.FileItem(
            'drwxrwxrwt',
            69632,
            datetime(year=2015, month=2, day=9,
                     hour=15, minute=1, second=0,
                     tzinfo=dateutil.tz.tzlocal()),
            'tmp')
        assert return_values[1] == cmd.FileItem(
            'drwxrwxrwt',
            69612,
            datetime(year=2015, month=2, day=19,
                     hour=15, minute=1, second=22,
                     tzinfo=dateutil.tz.tzlocal()),
            'tmp2')


# noinspection PyMethodMayBeStatic
@mock.patch('barman.command_wrappers.Command.pipe_processor_loop')
@mock.patch('barman.command_wrappers.subprocess.Popen')
class TestRsyncPgdata(object):

    def test_simple_invocation(self, popen, pipe_processor_loop):
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

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
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_args_invocation(self, popen, pipe_processor_loop):
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

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
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err


# noinspection PyMethodMayBeStatic
class TestReceiveXlog(object):
    """
    Simple class for testing of the PgReceiveXlog obj
    """

    def test_init(self):
        """
        Test class build
        """
        receivexlog = command_wrappers.PgReceiveXlog()
        assert receivexlog.args == [
            "--dbname=None",
            "--verbose",
            "--no-loop",
            "--directory=None"
        ]
        assert receivexlog.cmd == 'pg_receivexlog'
        assert receivexlog.check is True
        assert receivexlog.close_fds is True
        assert receivexlog.allowed_retval == (0,)
        assert receivexlog.debug is False
        assert receivexlog.err_handler
        assert receivexlog.out_handler

    def test_init_args(self):
        """
        Test class build
        """
        receivexlog = command_wrappers.PgReceiveXlog('/path/to/pg_receivexlog',
                                                     args=['a', 'b'])
        assert receivexlog.args == [
            "--dbname=None",
            "--verbose",
            "--no-loop",
            "--directory=None",
            "a",
            "b",
        ]
        assert receivexlog.cmd == '/path/to/pg_receivexlog'
        assert receivexlog.check is True
        assert receivexlog.close_fds is True
        assert receivexlog.allowed_retval == (0,)
        assert receivexlog.debug is False
        assert receivexlog.err_handler
        assert receivexlog.out_handler

    @mock.patch('barman.command_wrappers.Command.pipe_processor_loop')
    @mock.patch('barman.command_wrappers.subprocess.Popen')
    def test_simple_invocation(self, popen, pipe_processor_loop, caplog):
        ret = 0
        out = 'out'
        err = 'err'

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.PgReceiveXlog()
        result = cmd.execute()

        popen.assert_called_with(
            [
                'pg_receivexlog', '--dbname=None', '--verbose', '--no-loop',
                '--directory=None',
            ],
            shell=False, env=None,
            stdout=PIPE, stderr=PIPE, stdin=PIPE,
            preexec_fn=mock.ANY, close_fds=True
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out is None
        assert cmd.err is None
        assert ('PgReceiveXlog', INFO, out) in caplog.record_tuples
        assert ('PgReceiveXlog', WARNING, err) in caplog.record_tuples


# noinspection PyMethodMayBeStatic
class TestBarmanSubProcess(object):
    """
    Simple class for testing of the BarmanSubProcess obj
    """

    def test_init_minimal_cmd(self):
        """
        Test class build with minimal params
        """
        subprocess = command_wrappers.BarmanSubProcess(
            subcommand='fake-cmd',
            config='fake_conf')
        assert subprocess.command == [
            sys.executable,
            sys.argv[0],
            "-c", "fake_conf",
            "-q",
            "fake-cmd",
        ]

        # Test for missing config
        with pytest.raises(CommandFailedException):
            command_wrappers.BarmanSubProcess(
                command='path/to/barman',
                subcommand='fake_cmd')

    def test_init_args(self):
        """
        Test class build
        """
        subprocess = command_wrappers.BarmanSubProcess(
            command='path/to/barman',
            subcommand='test-cmd',
            config='fake_conf',
            args=["a", "b"])
        assert subprocess.command == [
            sys.executable,
            "path/to/barman",
            "-c", "fake_conf",
            "-q",
            "test-cmd",
            'a',
            'b'
        ]

    @mock.patch('barman.command_wrappers.subprocess.Popen')
    def test_simple_invocation(self, popen_mock, caplog):
        popen_mock.return_value.pid = 12345
        subprocess = command_wrappers.BarmanSubProcess(
            command='path/to/barman',
            subcommand='fake-cmd',
            config='fake_conf')
        subprocess.execute()

        command = [
            sys.executable,
            "path/to/barman",
            "-c", "fake_conf",
            "-q",
            "fake-cmd",
        ]
        popen_mock.assert_called_with(
            command, preexec_fn=os.setsid,
            close_fds=True,
            stdin=mock.ANY, stdout=mock.ANY, stderr=mock.ANY)
        assert ('barman.command_wrappers', DEBUG,
                'BarmanSubProcess: ' + str(command)) in caplog.record_tuples
        assert ('barman.command_wrappers', DEBUG,
                'BarmanSubProcess: subprocess started. '
                'pid: 12345') in caplog.record_tuples
