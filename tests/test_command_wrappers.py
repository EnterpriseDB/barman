# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2021
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
from logging import DEBUG, INFO, WARNING
from subprocess import PIPE

import mock
import pytest

from barman import command_wrappers
from barman.command_wrappers import (
    PgReceiveXlog,
    StreamLineProcessor,
    full_command_quote,
    shell_quote,
)
from barman.exceptions import CommandFailedException, CommandMaxRetryExceeded
from testing_helpers import u


def _mock_pipe(popen, pipe_processor_loop, ret=0, out="", err=""):
    pipe = popen.return_value
    pipe.communicate.return_value = (out.encode("utf-8"), err.encode("utf-8"))
    pipe.returncode = ret

    # noinspection PyProtectedMember
    def ppl(processors):
        for processor in processors:
            if processor.fileno() == pipe.stdout.fileno.return_value:
                for line in out.split("\n"):
                    processor._handler(line)
            if processor.fileno() == pipe.stderr.fileno.return_value:
                for line in err.split("\n"):
                    processor._handler(line)

    pipe_processor_loop.side_effect = ppl
    return pipe


@pytest.fixture(autouse=True)
def which(request):
    with mock.patch("barman.utils.which") as which:
        which.side_effect = lambda cmd, path: cmd
        yield which


# noinspection PyMethodMayBeStatic
@mock.patch("barman.command_wrappers.Command.pipe_processor_loop")
@mock.patch("barman.command_wrappers.subprocess.Popen")
class TestCommand(object):
    def test_simple_invocation(self, popen, pipe_processor_loop):
        command = "command"
        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command)
        result = cmd()

        popen.assert_called_with(
            [command],
            shell=False,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_simple_encoding(self, popen, pipe_processor_loop):
        command = "command"
        ret = 0
        out = u("\xe2\x98\xae\xe2\x82\xac\xc3\xa8\xc3\xa9")
        err = u("\xc2\xaf\\_(\xe3\x83\x84)_/\xc2\xaf")

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command)
        result = cmd()

        popen.assert_called_with(
            [command],
            shell=False,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_multiline_output(self, popen, pipe_processor_loop):
        command = "command"
        ret = 0
        out = "line1\nline2\n"
        err = "err1\nerr2\n"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command)
        result = cmd()

        popen.assert_called_with(
            [command],
            shell=False,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_failed_invocation(self, popen, pipe_processor_loop):
        command = "command"
        ret = 1
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command)
        result = cmd()

        popen.assert_called_with(
            [command],
            shell=False,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_check_failed_invocation(self, popen, pipe_processor_loop):
        command = "command"
        ret = 1
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command, check=True)
        with pytest.raises(CommandFailedException) as excinfo:
            cmd()
        assert excinfo.value.args[0]["ret"] == ret
        assert excinfo.value.args[0]["out"] == out
        assert excinfo.value.args[0]["err"] == err

        popen.assert_called_with(
            [command],
            shell=False,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_shell_invocation(self, popen, pipe_processor_loop):
        command = "test -n"
        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command, shell=True)
        result = cmd("shell test")

        popen.assert_called_with(
            "test -n 'shell test'",
            shell=True,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_declaration_args_invocation(self, popen, pipe_processor_loop):
        command = "command"
        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command, args=["one", "two"])
        result = cmd()

        popen.assert_called_with(
            [command, "one", "two"],
            shell=False,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_call_args_invocation(self, popen, pipe_processor_loop):
        command = "command"
        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command)
        result = cmd("one", "two")

        popen.assert_called_with(
            [command, "one", "two"],
            shell=False,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_both_args_invocation(self, popen, pipe_processor_loop):
        command = "command"
        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command, args=["a", "b"])
        result = cmd("one", "two")

        popen.assert_called_with(
            [command, "a", "b", "one", "two"],
            shell=False,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_env_invocation(self, popen, pipe_processor_loop):
        command = "command"
        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        with mock.patch("os.environ", new={"TEST0": "VAL0"}):
            cmd = command_wrappers.Command(
                command, env_append={"TEST1": "VAL1", "TEST2": "VAL2"}
            )
            result = cmd()

        popen.assert_called_with(
            [command],
            shell=False,
            env={"TEST0": "VAL0", "TEST1": "VAL1", "TEST2": "VAL2"},
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_path_invocation(self, popen, pipe_processor_loop):
        command = "command"
        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        with mock.patch("os.environ", new={"TEST0": "VAL0"}):
            cmd = command_wrappers.Command(command, path="/path/one:/path/two")
            result = cmd()

        popen.assert_called_with(
            [command],
            shell=False,
            env={"TEST0": "VAL0", "PATH": "/path/one:/path/two"},
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_env_path_invocation(self, popen, pipe_processor_loop):
        command = "command"
        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        with mock.patch("os.environ", new={"TEST0": "VAL0"}):
            cmd = command_wrappers.Command(
                command,
                path="/path/one:/path/two",
                env_append={"TEST1": "VAL1", "TEST2": "VAL2"},
            )
            result = cmd()

        popen.assert_called_with(
            [command],
            shell=False,
            env={
                "TEST0": "VAL0",
                "TEST1": "VAL1",
                "TEST2": "VAL2",
                "PATH": "/path/one:/path/two",
            },
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_get_output_invocation(self, popen, pipe_processor_loop):
        command = "command"
        ret = 0
        out = "out"
        err = "err"
        stdin = "in"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        with mock.patch("os.environ", new={"TEST0": "VAL0"}):
            cmd = command_wrappers.Command(
                command, env_append={"TEST1": "VAL1", "TEST2": "VAL2"}
            )
            result = cmd.get_output(stdin=stdin)

        popen.assert_called_with(
            [command],
            shell=False,
            env={"TEST0": "VAL0", "TEST1": "VAL1", "TEST2": "VAL2"},
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        pipe.stdin.write.assert_called_with(stdin)
        pipe.stdin.close.assert_called_once_with()
        assert result == (out, err)
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_execute_invocation(self, popen, pipe_processor_loop, caplog):
        # See all logs
        caplog.set_level(0)
        command = "command"
        ret = 0
        out = "out"
        err = "err"
        stdin = "in"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        with mock.patch("os.environ", new={"TEST0": "VAL0"}):
            cmd = command_wrappers.Command(
                command, env_append={"TEST1": "VAL1", "TEST2": "VAL2"}
            )
            result = cmd.execute(stdin=stdin)

        popen.assert_called_with(
            [command],
            shell=False,
            env={"TEST0": "VAL0", "TEST1": "VAL1", "TEST2": "VAL2"},
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        pipe.stdin.write.assert_called_with(stdin)
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out is None
        assert cmd.err is None
        assert ("Command", INFO, out) in caplog.record_tuples
        assert ("Command", WARNING, err) in caplog.record_tuples

    def test_execute_invocation_multiline(self, popen, pipe_processor_loop, caplog):
        # See all logs
        caplog.set_level(0)

        command = "command"
        ret = 0
        out = "line1\nline2\n"
        err = "err1\nerr2"  # no final newline here
        stdin = "in"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        with mock.patch("os.environ", new={"TEST0": "VAL0"}):
            cmd = command_wrappers.Command(
                command, env_append={"TEST1": "VAL1", "TEST2": "VAL2"}
            )
            result = cmd.execute(stdin=stdin)

        popen.assert_called_with(
            [command],
            shell=False,
            env={"TEST0": "VAL0", "TEST1": "VAL1", "TEST2": "VAL2"},
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        pipe.stdin.write.assert_called_with(stdin)
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out is None
        assert cmd.err is None
        for line in out.splitlines():
            assert ("Command", INFO, line) in caplog.record_tuples
        assert ("Command", INFO, "") not in caplog.record_tuples
        assert ("Command", INFO, None) not in caplog.record_tuples
        for line in err.splitlines():
            assert ("Command", WARNING, line) in caplog.record_tuples
        assert ("Command", WARNING, "") not in caplog.record_tuples
        assert ("Command", WARNING, None) not in caplog.record_tuples

    def test_execute_check_failed_invocation(self, popen, pipe_processor_loop, caplog):
        # See all logs
        caplog.set_level(0)

        command = "command"
        ret = 1
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Command(command, check=True)
        with pytest.raises(CommandFailedException) as excinfo:
            cmd.execute()
        assert excinfo.value.args[0]["ret"] == ret
        assert excinfo.value.args[0]["out"] is None
        assert excinfo.value.args[0]["err"] is None

        popen.assert_called_with(
            [command],
            shell=False,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert cmd.ret == ret
        assert cmd.out is None
        assert cmd.err is None
        assert ("Command", INFO, out) in caplog.record_tuples
        assert ("Command", WARNING, err) in caplog.record_tuples

    def test_handlers_multiline(self, popen, pipe_processor_loop, caplog):
        command = "command"
        ret = 0
        out = "line1\nline2\n"
        err = "err1\nerr2"  # no final newline here
        stdin = "in"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        out_list = []
        err_list = []
        with mock.patch("os.environ", new={"TEST0": "VAL0"}):
            cmd = command_wrappers.Command(
                command,
                env_append={"TEST1": "VAL1", "TEST2": "VAL2"},
                out_handler=out_list.append,
                err_handler=err_list.append,
            )
            result = cmd.execute(stdin=stdin)

        popen.assert_called_with(
            [command],
            shell=False,
            env={"TEST0": "VAL0", "TEST1": "VAL1", "TEST2": "VAL2"},
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        pipe.stdin.write.assert_called_with(stdin)
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out is None
        assert cmd.err is None
        assert "\n".join(out_list) == out
        assert "\n".join(err_list) == err

    def test_execute_handlers(self, popen, pipe_processor_loop, caplog):
        # See all logs
        caplog.set_level(0)

        command = "command"
        ret = 0
        out = "out"
        err = "err"
        stdin = "in"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        with mock.patch("os.environ", new={"TEST0": "VAL0"}):
            cmd = command_wrappers.Command(
                command, env_append={"TEST1": "VAL1", "TEST2": "VAL2"}
            )
            result = cmd.execute(
                stdin=stdin,
                out_handler=cmd.make_logging_handler(INFO, "out: "),
                err_handler=cmd.make_logging_handler(WARNING, "err: "),
            )

        popen.assert_called_with(
            [command],
            shell=False,
            env={"TEST0": "VAL0", "TEST1": "VAL1", "TEST2": "VAL2"},
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        pipe.stdin.write.assert_called_with(stdin)
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out is None
        assert cmd.err is None
        assert ("Command", INFO, "out: " + out) in caplog.record_tuples
        assert ("Command", WARNING, "err: " + err) in caplog.record_tuples

    @mock.patch("time.sleep")
    @mock.patch("barman.command_wrappers.Command._get_output_once")
    def test_retry(
        self, get_output_no_retry_mock, sleep_mock, popen, pipe_processor_loop
    ):
        """
        Test the retry method

        :param mock.Mock get_output_no_retry_mock: simulate a
            Command._get_output_once() call
        :param mock.Mock sleep_mock: mimic the sleep timer
        :param mock.Mock popen: unused, mocked from the whole test class
        :param mock.Mock pipe_processor_loop: unused, mocked from the whole
            test class
        """

        command = "test string"
        cmd = command_wrappers.Command(
            command, check=True, retry_times=5, retry_sleep=10
        )

        # check for correct return value
        r = cmd.get_output("test string")
        get_output_no_retry_mock.assert_called_with("test string")
        assert get_output_no_retry_mock.return_value == r

        # check for correct number of calls and invocations of sleep method
        get_output_no_retry_mock.reset_mock()
        sleep_mock.reset_mock()
        expected = mock.Mock()
        get_output_no_retry_mock.side_effect = [
            CommandFailedException("testException"),
            expected,
        ]
        r = cmd.get_output("test string")
        assert get_output_no_retry_mock.call_count == 2
        assert sleep_mock.call_count == 1
        assert r == expected

        # check for correct number of tries and invocations of sleep method
        get_output_no_retry_mock.reset_mock()
        sleep_mock.reset_mock()
        e = CommandFailedException("testException")
        get_output_no_retry_mock.side_effect = [e, e, e, e, e, e]
        with pytest.raises(CommandMaxRetryExceeded) as exc_info:
            cmd.get_output("test string")
        assert exc_info.value.args == e.args
        assert sleep_mock.call_count == 5
        assert get_output_no_retry_mock.call_count == 6


# noinspection PyMethodMayBeStatic
class TestCommandPipeProcessorLoop(object):
    @mock.patch("barman.command_wrappers.select.select")
    @mock.patch("barman.command_wrappers.os.read")
    def test_ppl(self, read_mock, select_mock):
        # Simulate the two files
        stdout = mock.Mock(name="pipe.stdout")
        stdout.fileno.return_value = 65
        stderr = mock.Mock(name="pipe.stderr")
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
        read_mock.side_effect = [
            "line1\nl".encode("utf-8"),
            "err".encode("utf-8"),
            "ine2".encode("utf-8"),
            "1\nerr2\n".encode("utf-8"),
            "",
            "",
            Exception,
        ]  # Make sure it terminates

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
        assert out_list == ["line1", "line2"]
        assert err_list == ["err1", "err2", ""]

    @mock.patch("barman.command_wrappers.select.select")
    def test_ppl_select_failure(self, select_mock):
        # Test if select errors are passed through
        select_mock.side_effect = select.error("not good")

        with pytest.raises(select.error):
            command_wrappers.Command.pipe_processor_loop([None])


# noinspection PyMethodMayBeStatic
@mock.patch("barman.command_wrappers.Command.pipe_processor_loop")
@mock.patch("barman.command_wrappers.subprocess.Popen")
class TestRsync(object):
    def test_simple_invocation(self, popen, pipe_processor_loop):
        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Rsync()
        result = cmd("src", "dst")

        popen.assert_called_with(
            ["rsync", "src", "dst"],
            shell=False,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_args_invocation(self, popen, pipe_processor_loop):
        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Rsync(args=["a", "b"])
        result = cmd("src", "dst")

        popen.assert_called_with(
            ["rsync", "a", "b", "src", "dst"],
            shell=False,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_custom_ssh_invocation(self, popen, pipe_processor_loop, which):
        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)
        cmd = command_wrappers.Rsync(
            "/custom/rsync", ssh="/custom/ssh", ssh_options=["-c", "arcfour"]
        )
        result = cmd("src", "dst")

        which.assert_called_with("/custom/rsync", None)
        popen.assert_called_with(
            ["/custom/rsync", "-e", "/custom/ssh '-c' 'arcfour'", "src", "dst"],
            shell=False,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_rsync_build_failure(self, popen, pipe_processor_loop, which):
        """
        Simple test that checks if a CommandFailedException is raised
        when Rsync object is build with an invalid path or rsync
        is not in system path
        """
        which.side_effect = CommandFailedException()
        # Pass an invalid path to Rsync class constructor.
        # Expect a CommandFailedException
        with pytest.raises(CommandFailedException):
            command_wrappers.Rsync("/invalid/path/rsync")
        # Force the which method to return false, simulating rsync command not
        # present in system PATH. Expect a CommandFailedExceptiomn
        with mock.patch("barman.utils.which") as mock_which:
            mock_which.return_value = False
            with pytest.raises(CommandFailedException):
                command_wrappers.Rsync(ssh_options=["-c", "arcfour"])

    def test_protect_ssh_invocation(self, popen, pipe_processor_loop):
        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Rsync(exclude_and_protect=["foo", "bar"])
        result = cmd("src", "dst")

        popen.assert_called_with(
            [
                "rsync",
                "--exclude=foo",
                "--filter=P_foo",
                "--exclude=bar",
                "--filter=P_bar",
                "src",
                "dst",
            ],
            shell=False,
            env=mock.ANY,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_bwlimit_ssh_invocation(self, popen, pipe_processor_loop):
        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Rsync(bwlimit=101)
        result = cmd("src", "dst")

        popen.assert_called_with(
            ["rsync", "--bwlimit=101", "src", "dst"],
            shell=False,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_from_file_list_ssh_invocation(self, popen, pipe_processor_loop):
        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.Rsync()
        result = cmd.from_file_list(["a", "b", "c"], "src", "dst")

        popen.assert_called_with(
            ["rsync", "--files-from=-", "src", "dst"],
            shell=False,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        pipe.stdin.write.assert_called_with("a\nb\nc".encode("UTF-8"))
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err


# noinspection PyMethodMayBeStatic
@mock.patch("barman.command_wrappers.Command.pipe_processor_loop")
@mock.patch("barman.command_wrappers.subprocess.Popen")
class TestRsyncPgdata(object):
    def test_simple_invocation(self, popen, pipe_processor_loop):
        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.RsyncPgData()
        result = cmd("src", "dst")

        popen.assert_called_with(
            ["rsync", "-rLKpts", "--delete-excluded", "--inplace", "src", "dst"],
            shell=False,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err

    def test_args_invocation(self, popen, pipe_processor_loop):
        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)

        cmd = command_wrappers.RsyncPgData(args=["a", "b"])
        result = cmd("src", "dst")

        popen.assert_called_with(
            [
                "rsync",
                "-rLKpts",
                "--delete-excluded",
                "--inplace",
                "a",
                "b",
                "src",
                "dst",
            ],
            shell=False,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
            close_fds=True,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out == out
        assert cmd.err == err


class TestPgBaseBackup(object):
    """
    Simple class for testing of the PgBaseBackup obj
    """

    def test_init_simple(self, which):
        """
        Test class build
        """
        connection_mock = mock.MagicMock()
        connection_mock.get_connection_string.return_value = "test_conn"
        pgbasebackup = command_wrappers.PgBaseBackup(
            destination="/fake/path",
            command="/usr/bin/pg_basebackup",
            connection=connection_mock,
            version="9.3",
            app_name="fake_app_name",
        )
        assert pgbasebackup.args == [
            "--dbname=test_conn",
            "-v",
            "--no-password",
            "--pgdata=/fake/path",
        ]
        assert pgbasebackup.cmd == "/usr/bin/pg_basebackup"
        assert pgbasebackup.check is True
        assert pgbasebackup.close_fds is True
        assert pgbasebackup.allowed_retval == (0,)
        assert pgbasebackup.err_handler
        assert pgbasebackup.out_handler

        connection_mock.conn_parameters = {
            "host": "fake host",
            "port": "fake_port",
            "user": "fake_user",
        }
        pgbasebackup = command_wrappers.PgBaseBackup(
            destination="/fake/target",
            command="/usr/bin/pg_basebackup",
            connection=connection_mock,
            version="9.2",
            app_name="fake_app_name",
        )
        assert pgbasebackup.args == [
            "--host=fake host",
            "--port=fake_port",
            "--username=fake_user",
            "-v",
            "--no-password",
            "--pgdata=/fake/target",
        ]

        which.return_value = None
        which.side_effect = None
        with pytest.raises(CommandFailedException):
            # Expect an exception for pg_basebackup not in path
            command_wrappers.PgBaseBackup(
                destination="/fake/target",
                connection=connection_mock,
                command="fake/path",
                version="9.3",
                app_name="fake_app_name",
            )

    def test_init_args(self):
        """
        Test class build
        """
        connection_mock = mock.MagicMock()
        connection_mock.get_connection_string.return_value = "test_connstring"
        pg_basebackup = command_wrappers.PgBaseBackup(
            command="/path/to/pg_basebackup",
            connection=connection_mock,
            version="9.4",
            destination="/dest/dir",
            args=["a", "b"],
        )
        assert pg_basebackup.args == [
            "--dbname=test_connstring",
            "-v",
            "--no-password",
            "--pgdata=/dest/dir",
            "a",
            "b",
        ]
        assert pg_basebackup.cmd == "/path/to/pg_basebackup"
        assert pg_basebackup.check is True
        assert pg_basebackup.close_fds is True
        assert pg_basebackup.allowed_retval == (0,)
        assert pg_basebackup.err_handler
        assert pg_basebackup.out_handler

    def test_pg_basebackup10_no_wals(self):
        """
        Test that --no-slot and --wal-method options are correctly passed
        if pg_basebackup client version is >= 10
        """
        connection_mock = mock.MagicMock()
        connection_mock.get_connection_string.return_value = "test_connstring"
        pg_basebackup = command_wrappers.PgBaseBackup(
            command="/path/to/pg_basebackup",
            connection=connection_mock,
            version="10",
            destination="/dest/dir",
            args=["a", "b"],
        )
        assert pg_basebackup.args == [
            "--dbname=test_connstring",
            "-v",
            "--no-password",
            "--pgdata=/dest/dir",
            "--no-slot",
            "--wal-method=none",
            "a",
            "b",
        ]
        assert pg_basebackup.cmd == "/path/to/pg_basebackup"
        assert pg_basebackup.check is True
        assert pg_basebackup.close_fds is True
        assert pg_basebackup.allowed_retval == (0,)
        assert pg_basebackup.err_handler
        assert pg_basebackup.out_handler

    @mock.patch("barman.command_wrappers.Command.pipe_processor_loop")
    @mock.patch("barman.command_wrappers.subprocess.Popen")
    def test_simple_invocation(self, popen, pipe_processor_loop, caplog):
        # See all logs
        caplog.set_level(0)

        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)
        connection_mock = mock.MagicMock()
        connection_mock.get_connection_string.return_value = "fake_connstring"
        cmd = command_wrappers.PgBaseBackup(
            destination="/fake/target",
            command="/usr/bin/pg_basebackup",
            connection=connection_mock,
            version="9.4",
            app_name="fake_app_name",
        )
        result = cmd.execute()

        popen.assert_called_with(
            [
                "/usr/bin/pg_basebackup",
                "--dbname=fake_connstring",
                "-v",
                "--no-password",
                "--pgdata=/fake/target",
            ],
            close_fds=True,
            env=None,
            preexec_fn=mock.ANY,
            shell=False,
            stdout=mock.ANY,
            stderr=mock.ANY,
            stdin=mock.ANY,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out is None
        assert cmd.err is None
        assert ("PgBaseBackup", INFO, out) in caplog.record_tuples
        assert ("PgBaseBackup", WARNING, err) in caplog.record_tuples


# noinspection PyMethodMayBeStatic
class TestReceiveXlog(object):
    """
    Simple class for testing of the PgReceiveXlog obj
    """

    def test_init_simple(self):
        """
        Test class build
        """
        connection_mock = mock.MagicMock()
        connection_mock.get_connection_string.return_value = "test_conn"
        receivexlog = command_wrappers.PgReceiveXlog(
            destination="/fake/target",
            command="/usr/bin/pg_receivexlog",
            connection=connection_mock,
            version="9.3",
            app_name="fake_app_name",
        )
        assert receivexlog.args == [
            "--dbname=test_conn",
            "--verbose",
            "--no-loop",
            "--no-password",
            "--directory=/fake/target",
        ]
        assert receivexlog.cmd == "/usr/bin/pg_receivexlog"
        assert receivexlog.check is True
        assert receivexlog.close_fds is True
        assert receivexlog.allowed_retval == (0,)
        assert receivexlog.err_handler
        assert receivexlog.out_handler

        connection_mock.conn_parameters = {
            "host": "fake host",
            "port": "fake_port",
            "user": "fake_user",
        }
        receivexlog = command_wrappers.PgReceiveXlog(
            destination="/fake/target",
            command=connection_mock,
            connection=connection_mock,
            version="9.2",
            app_name="fake_app_name",
        )
        assert receivexlog.args == [
            "--host=fake host",
            "--port=fake_port",
            "--username=fake_user",
            "--verbose",
            "--no-loop",
            "--no-password",
            "--directory=/fake/target",
        ]

    def test_init_args(self):
        """
        Test class build
        """
        connection_mock = mock.MagicMock()
        connection_mock.get_connection_string.return_value = "test_connstring"
        receivexlog = command_wrappers.PgReceiveXlog(
            destination="/dest/dir",
            command="/path/to/pg_receivexlog",
            connection=connection_mock,
            version="9.4",
            args=["a", "b"],
        )
        assert receivexlog.args == [
            "--dbname=test_connstring",
            "--verbose",
            "--no-loop",
            "--no-password",
            "--directory=/dest/dir",
            "a",
            "b",
        ]
        assert receivexlog.cmd == "/path/to/pg_receivexlog"
        assert receivexlog.check is True
        assert receivexlog.close_fds is True
        assert receivexlog.allowed_retval == (0,)
        assert receivexlog.err_handler
        assert receivexlog.out_handler

    @mock.patch("barman.command_wrappers.Command.pipe_processor_loop")
    @mock.patch("barman.command_wrappers.subprocess.Popen")
    def test_simple_invocation(self, popen, pipe_processor_loop, caplog):
        # See all logs
        caplog.set_level(0)

        ret = 0
        out = "out"
        err = "err"

        pipe = _mock_pipe(popen, pipe_processor_loop, ret, out, err)
        connection_mock = mock.MagicMock()
        connection_mock.get_connection_string.return_value = "fake_connstring"
        cmd = command_wrappers.PgReceiveXlog(
            destination="/fake/target",
            command="/usr/bin/pg_receivexlog",
            connection=connection_mock,
            version="9.4",
            app_name="fake_app_name",
        )
        result = cmd.execute()

        popen.assert_called_with(
            [
                "/usr/bin/pg_receivexlog",
                "--dbname=fake_connstring",
                "--verbose",
                "--no-loop",
                "--no-password",
                "--directory=/fake/target",
            ],
            close_fds=True,
            shell=False,
            env=None,
            stdout=PIPE,
            stderr=PIPE,
            stdin=PIPE,
            preexec_fn=mock.ANY,
        )
        assert not pipe.stdin.write.called
        pipe.stdin.close.assert_called_once_with()
        assert result == ret
        assert cmd.ret == ret
        assert cmd.out is None
        assert cmd.err is None
        assert ("PgReceiveXlog", INFO, out) in caplog.record_tuples
        assert ("PgReceiveXlog", WARNING, err) in caplog.record_tuples

    @mock.patch("barman.utils.which")
    @mock.patch("barman.command_wrappers.Command")
    def test_find_command(self, command_mock, which_mock):
        """
        Test the `find_command` class method
        """

        which_mapping = {}
        which_mock.side_effect = lambda cmd, path=None: which_mapping.get(cmd, None)

        # Neither pg_receivewal, neither pg_receivexlog are
        # available, and the result is a CommandFailedException
        with pytest.raises(CommandFailedException):
            PgReceiveXlog.find_command(path="/test/bin:/other/bin")
        assert which_mock.mock_calls == [
            mock.call("pg_receivewal", "/test/bin"),
            mock.call("pg_receivexlog", "/test/bin"),
            mock.call("pg_receivewal", "/other/bin"),
            mock.call("pg_receivexlog", "/other/bin"),
        ]

        # pg_receivexlog is available, but pg_receivewal is not
        which_mapping["pg_receivexlog"] = "/usr/bin/pg_receivexlog"
        command_mock.reset_mock()
        which_mock.reset_mock()
        command = PgReceiveXlog.find_command(path="/test/bin")
        assert which_mock.mock_calls == [
            mock.call("pg_receivewal", "/test/bin"),
            mock.call("pg_receivexlog", "/test/bin"),
        ]
        assert command_mock.mock_calls == [
            mock.call("/usr/bin/pg_receivexlog", check=True, path="/test/bin"),
            mock.call()("--version"),
        ]
        assert command == command_mock.return_value

        # pg_receivewal is also available, but it's only a shim
        which_mapping["pg_receivewal"] = "/usr/bin/pg_receivewal"
        command_mock.reset_mock()
        which_mock.reset_mock()
        command_mock.return_value.side_effect = [CommandFailedException, None]
        command = PgReceiveXlog.find_command(path="/test/bin")
        assert which_mock.mock_calls == [
            mock.call("pg_receivewal", "/test/bin"),
            mock.call("pg_receivexlog", "/test/bin"),
        ]
        assert command_mock.mock_calls == [
            mock.call("/usr/bin/pg_receivewal", check=True, path="/test/bin"),
            mock.call()("--version"),
            mock.call("/usr/bin/pg_receivexlog", check=True, path="/test/bin"),
            mock.call()("--version"),
        ]
        assert command == command_mock.return_value

        # pg_receivewal is available and works well
        command_mock.reset_mock()
        which_mock.reset_mock()
        command_mock.return_value.side_effect = None
        command = PgReceiveXlog.find_command(path="/test/bin")
        assert which_mock.mock_calls == [
            mock.call("pg_receivewal", "/test/bin"),
        ]
        assert command_mock.mock_calls == [
            mock.call("/usr/bin/pg_receivewal", check=True, path="/test/bin"),
            mock.call()("--version"),
        ]
        assert command == command_mock.return_value

    @mock.patch("barman.command_wrappers.PostgreSQLClient.find_command")
    def test_get_version_info(self, find_command_mock):
        """
        Test the `get_version_info` class method
        """
        command_mock = find_command_mock.return_value
        command_mock.cmd = "/some/path/pg_receivewal"
        command_mock.out = "pg_receivewal (PostgreSQL) 11.7 (ev1 12) (ev2 2:3.4)"

        # Test with normal output
        version_info = PgReceiveXlog.get_version_info()
        assert version_info["full_path"] == "/some/path/pg_receivewal"
        assert version_info["full_version"] == "11.7"
        assert version_info["major_version"] == "11"

        # Test with development branch
        command_mock.out = "pg_receivewal 13devel"
        version_info = PgReceiveXlog.get_version_info()
        assert version_info["full_version"] == "13devel"
        assert version_info["major_version"] == "13"

        # Test with bad output
        command_mock.out = "pg_receivewal"
        version_info = PgReceiveXlog.get_version_info()
        assert version_info["full_path"] == "/some/path/pg_receivewal"
        assert version_info["full_version"] is None
        assert version_info["major_version"] is None

        # Test with invocation error
        find_command_mock.side_effect = CommandFailedException
        version_info = PgReceiveXlog.get_version_info()
        assert version_info["full_path"] is None
        assert version_info["full_version"] is None
        assert version_info["major_version"] is None


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
            subcommand="fake-cmd", config="fake_conf"
        )
        assert subprocess.command == [
            sys.executable,
            sys.argv[0],
            "-c",
            "fake_conf",
            "-q",
            "fake-cmd",
        ]

        # Test for missing config
        with pytest.raises(CommandFailedException):
            command_wrappers.BarmanSubProcess(
                command="path/to/barman", subcommand="fake_cmd"
            )

    def test_init_args(self):
        """
        Test class build
        """
        subprocess = command_wrappers.BarmanSubProcess(
            command="path/to/barman",
            subcommand="test-cmd",
            config="fake_conf",
            args=["a", "b"],
        )
        assert subprocess.command == [
            sys.executable,
            "path/to/barman",
            "-c",
            "fake_conf",
            "-q",
            "test-cmd",
            "a",
            "b",
        ]

    @mock.patch("barman.command_wrappers.subprocess.Popen")
    def test_simple_invocation(self, popen_mock, caplog):
        # See all logs
        caplog.set_level(0)

        popen_mock.return_value.pid = 12345
        subprocess = command_wrappers.BarmanSubProcess(
            command="path/to/barman", subcommand="fake-cmd", config="fake_conf"
        )
        subprocess.execute()

        command = [
            sys.executable,
            "path/to/barman",
            "-c",
            "fake_conf",
            "-q",
            "fake-cmd",
        ]
        popen_mock.assert_called_with(
            command,
            preexec_fn=os.setsid,
            close_fds=True,
            stdin=mock.ANY,
            stdout=mock.ANY,
            stderr=mock.ANY,
        )
        assert (
            "barman.command_wrappers",
            DEBUG,
            "BarmanSubProcess: " + str(command),
        ) in caplog.record_tuples
        assert (
            "barman.command_wrappers",
            DEBUG,
            "BarmanSubProcess: subprocess started. " "pid: 12345",
        ) in caplog.record_tuples


def test_shell_quote():
    """
    Test the shell_quote function
    """
    assert "''" == shell_quote("")
    assert "'a safe string'" == shell_quote("a safe string")
    assert "'an un$@fe string containing a '\\'' quote'" == shell_quote(
        "an un$@fe string containing a ' quote"
    )


def test_full_command_quote():
    """
    Test the full_command_quote function
    """
    assert "command" == full_command_quote("command")
    assert "a 'b' 'c'" == full_command_quote("a", ["b", "c"])
    assert "safe" == full_command_quote("safe", [])
    assert "a command 'with' 'unsafe '\\''argument'\\'''" == full_command_quote(
        "a command", ["with", "unsafe 'argument'"]
    )
