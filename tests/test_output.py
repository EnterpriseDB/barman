# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2023
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

import datetime
import json

import mock
import pytest
from dateutil import tz
from mock import PropertyMock
from testing_helpers import (
    build_backup_manager,
    build_test_backup_info,
    find_by_attr,
    mock_backup_ext_info,
)

from barman import output
from barman.cloud_providers.google_cloud_storage import (
    GcpSnapshotMetadata,
    GcpSnapshotsInfo,
)
from barman.infofile import BackupInfo
from barman.utils import BarmanEncoder, human_readable_timedelta, pretty_size

# Color output constants
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RESET = "\033[0m"


def teardown_module(module):
    """
    Set the output API to a functional state, after testing it
    """
    output.set_output_writer(output.DEFAULT_WRITER)


@pytest.fixture(autouse=True)
def barman_encoder():
    """
    This fixture detects when mock objects are serialized to JSON
    and raise a better error message
    """
    real_default = BarmanEncoder.default

    with mock.patch.object(BarmanEncoder, "default", autospec=True) as default:

        def extended_default(self, obj):
            if isinstance(obj, mock.Mock):
                raise Exception("Mock object serialization detected: %s", obj)
            return real_default(self, obj)

        default.side_effect = extended_default
        yield default


# noinspection PyMethodMayBeStatic
class TestOutputAPI(object):
    @staticmethod
    def _mock_writer():
        # install a fresh mocked output writer
        writer = mock.Mock()
        output.set_output_writer(writer)
        # reset the error status
        output.error_occurred = False
        return writer

    # noinspection PyProtectedMember,PyUnresolvedReferences
    @mock.patch.dict(output.AVAILABLE_WRITERS, mock=mock.Mock())
    def test_set_output_writer_close(self):
        old_writer = mock.Mock()
        output.set_output_writer(old_writer)

        assert output._writer == old_writer

        args = ("1", "two")
        kwargs = dict(three=3, four=5)
        output.set_output_writer("mock", *args, **kwargs)

        old_writer.close.assert_called_once_with()
        output.AVAILABLE_WRITERS["mock"].assert_called_once_with(*args, **kwargs)

    def test_debug(self, caplog):
        # See all logs
        caplog.set_level(0)

        # preparation
        writer = self._mock_writer()

        msg = "test message"
        output.debug(msg)

        # logging test
        for record in caplog.records:
            assert record.levelname == "DEBUG"
            assert record.name == __name__
        assert msg in caplog.text

        # writer test
        assert not writer.error_occurred.called
        writer.debug.assert_called_once_with(msg)

        # global status test
        assert not output.error_occurred

    def test_debug_with_args(self, caplog):
        # See all logs
        caplog.set_level(0)

        # preparation
        writer = self._mock_writer()

        msg = "test format %02d %s"
        args = (1, "2nd")
        output.debug(msg, *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == "DEBUG"
            assert record.name == __name__
        assert msg % args in caplog.text

        # writer test
        assert not writer.error_occurred.called
        writer.debug.assert_called_once_with(msg, *args)

        # global status test
        assert not output.error_occurred

    def test_debug_error(self, caplog):
        # See all logs
        caplog.set_level(0)

        # preparation
        writer = self._mock_writer()

        msg = "test message"
        output.debug(msg, is_error=True)

        # logging test
        for record in caplog.records:
            assert record.levelname == "DEBUG"
            assert record.name == __name__
        assert msg in caplog.text

        # writer test
        writer.error_occurred.assert_called_once_with()
        writer.debug.assert_called_once_with(msg)

        # global status test
        assert output.error_occurred

    def test_debug_with_kwargs(self):
        # preparation
        self._mock_writer()

        with pytest.raises(TypeError):
            output.debug("message", bad_arg=True)

    def test_info(self, caplog):
        # See all logs
        caplog.set_level(0)

        # preparation
        writer = self._mock_writer()

        msg = "test message"
        output.info(msg)

        # logging test
        for record in caplog.records:
            assert record.levelname == "INFO"
            assert record.name == __name__
        assert msg in caplog.text

        # writer test
        assert not writer.error_occurred.called
        writer.info.assert_called_once_with(msg)

        # global status test
        assert not output.error_occurred

    def test_info_with_args(self, caplog):
        # See all logs
        caplog.set_level(0)

        # preparation
        writer = self._mock_writer()

        msg = "test format %02d %s"
        args = (1, "2nd")
        output.info(msg, *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == "INFO"
            assert record.name == __name__
        assert msg % args in caplog.text

        # writer test
        assert not writer.error_occurred.called
        writer.info.assert_called_once_with(msg, *args)

        # global status test
        assert not output.error_occurred

    def test_info_error(self, caplog):
        # See all logs
        caplog.set_level(0)

        # preparation
        writer = self._mock_writer()

        msg = "test message"
        output.info(msg, is_error=True)

        # logging test
        for record in caplog.records:
            assert record.levelname == "INFO"
            assert record.name == __name__
        assert msg in caplog.text

        # writer test
        writer.error_occurred.assert_called_once_with()
        writer.info.assert_called_once_with(msg)

        # global status test
        assert output.error_occurred

    def test_warning(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = "test message"
        output.warning(msg)

        # logging test
        for record in caplog.records:
            assert record.levelname == "WARNING"
            assert record.name == __name__
        assert msg in caplog.text

        # writer test
        assert not writer.error_occurred.called
        writer.warning.assert_called_once_with(msg)

        # global status test
        assert not output.error_occurred

    def test_warning_with_args(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = "test format %02d %s"
        args = (1, "2nd")
        output.warning(msg, *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == "WARNING"
            assert record.name == __name__
        assert msg % args in caplog.text

        # writer test
        assert not writer.error_occurred.called
        writer.warning.assert_called_once_with(msg, *args)

        # global status test
        assert not output.error_occurred

    def test_warning_error(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = "test message"
        output.warning(msg, is_error=True)

        # logging test
        for record in caplog.records:
            assert record.levelname == "WARNING"
            assert record.name == __name__
        assert msg in caplog.text

        # writer test
        writer.error_occurred.assert_called_once_with()
        writer.warning.assert_called_once_with(msg)

        # global status test
        assert output.error_occurred

    def test_error(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = "test message"
        output.error(msg)

        # logging test
        for record in caplog.records:
            assert record.levelname == "ERROR"
            assert record.name == __name__
        assert msg in caplog.text

        # writer test
        writer.error_occurred.assert_called_once_with()
        writer.error.assert_called_once_with(msg)

        # global status test
        assert output.error_occurred

    def test_error_with_args(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = "test format %02d %s"
        args = (1, "2nd")
        output.error(msg, *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == "ERROR"
            assert record.name == __name__
        assert msg % args in caplog.text

        # writer test
        writer.error_occurred.assert_called_once_with()
        writer.error.assert_called_once_with(msg, *args)

        # global status test
        assert output.error_occurred

    def test_error_with_ignore(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = "test format %02d %s"
        args = (1, "2nd")
        output.error(msg, ignore=True, *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == "ERROR"
            assert record.name == __name__
        assert msg % args in caplog.text

        # writer test
        assert not writer.error_occurred.called
        writer.error.assert_called_once_with(msg, *args)

        # global status test
        assert not output.error_occurred

    def test_exception(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = "test message"
        try:
            raise ValueError("test exception")
        except ValueError:
            output.exception(msg)

        # logging test
        for record in caplog.records:
            assert record.levelname == "ERROR"
            assert record.name == __name__
        assert msg in caplog.text
        assert "Traceback" in caplog.text

        # writer test
        writer.error_occurred.assert_called_once_with()
        writer.exception.assert_called_once_with(msg)

        # global status test
        assert output.error_occurred

    def test_exception_with_args(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = "test format %02d %s"
        args = (1, "2nd")
        try:
            raise ValueError("test exception")
        except ValueError:
            output.exception(msg, *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == "ERROR"
            assert record.name == __name__
        assert msg % args in caplog.text
        assert "Traceback" in caplog.text

        # writer test
        writer.error_occurred.assert_called_once_with()
        writer.exception.assert_called_once_with(msg, *args)

        # global status test
        assert output.error_occurred

    def test_exception_with_ignore(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = "test format %02d %s"
        args = (1, "2nd")
        try:
            raise ValueError("test exception")
        except ValueError:
            output.exception(msg, ignore=True, *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == "ERROR"
            assert record.name == __name__
        assert msg % args in caplog.text
        assert "Traceback" in caplog.text

        # writer test
        assert not writer.error_occurred.called
        writer.exception.assert_called_once_with(msg, *args)

        # global status test
        assert not output.error_occurred

    def test_exception_with_raise(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = "test format %02d %s"
        args = (1, "2nd")

        try:
            raise ValueError("test exception")
        except ValueError:
            with pytest.raises(ValueError):
                output.exception(msg, raise_exception=True, *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == "ERROR"
            assert record.name == __name__
        assert msg % args in caplog.text
        assert "Traceback" in caplog.text

        # writer test
        writer.error_occurred.assert_called_once_with()
        writer.exception.assert_called_once_with(msg, *args)

        # global status test
        assert output.error_occurred

    def test_exception_with_raise_object(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = "test format %02d %s"
        args = (1, "2nd")

        try:
            raise ValueError("test exception")
        except ValueError:
            with pytest.raises(KeyError):
                output.exception(msg, raise_exception=KeyError(), *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == "ERROR"
            assert record.name == __name__
        assert msg % args in caplog.text
        assert "Traceback" in caplog.text

        # writer test
        writer.error_occurred.assert_called_once_with()
        writer.exception.assert_called_once_with(msg, *args)

        # global status test
        assert output.error_occurred

    def test_exception_with_raise_class(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = "test format %02d %s"
        args = (1, "2nd")

        try:
            raise ValueError("test exception")
        except ValueError:
            with pytest.raises(KeyError):
                output.exception(msg, raise_exception=KeyError, *args)
        assert msg % args in caplog.text
        assert "Traceback" in caplog.text

        # logging test
        for record in caplog.records:
            assert record.levelname == "ERROR"
            assert record.name == __name__

        # writer test
        writer.error_occurred.assert_called_once_with()
        writer.exception.assert_called_once_with(msg, *args)

        # global status test
        assert output.error_occurred

    def test_init(self):
        # preparation
        writer = self._mock_writer()

        args = ("1", "two")
        kwargs = dict(three=3, four=5)
        output.init("command", *args, **kwargs)
        output.init("another_command")

        # writer test
        writer.init_command.assert_called_once_with(*args, **kwargs)
        writer.init_another_command.assert_called_once_with()

    @mock.patch("sys.exit")
    def test_init_bad_command(self, exit_mock, caplog):
        # preparation
        writer = self._mock_writer()
        del writer.init_bad_command

        output.init("bad_command")

        # logging test
        for record in caplog.records:
            assert record.levelname == "ERROR"
        assert "bad_command" in caplog.text
        assert "Traceback" in caplog.text

        # writer test
        writer.error_occurred.assert_called_once_with()
        assert writer.exception.call_count == 1

        # exit with error
        assert exit_mock.called
        assert exit_mock.call_count == 1
        assert exit_mock.call_args[0] != 0

    def test_result(self):
        # preparation
        writer = self._mock_writer()

        args = ("1", "two")
        kwargs = dict(three=3, four=5)
        output.result("command", *args, **kwargs)
        output.result("another_command")

        # writer test
        writer.result_command.assert_called_once_with(*args, **kwargs)
        writer.result_another_command.assert_called_once_with()

    @mock.patch("sys.exit")
    def test_result_bad_command(self, exit_mock, caplog):
        # preparation
        writer = self._mock_writer()
        del writer.result_bad_command

        output.result("bad_command")

        # logging test
        for record in caplog.records:
            assert record.levelname == "ERROR"
        assert "bad_command" in caplog.text
        assert "Traceback" in caplog.text

        # writer test
        writer.error_occurred.assert_called_once_with()
        assert writer.exception.call_count == 1

        # exit with error
        assert exit_mock.called
        assert exit_mock.call_count == 1
        assert exit_mock.call_args[0] != 0

    def test_close(self):
        # preparation
        writer = self._mock_writer()

        output.close()

        writer.close.assert_called_once_with()

    @mock.patch("sys.exit")
    def test_close_and_exit(self, exit_mock):
        # preparation
        writer = self._mock_writer()

        output.close_and_exit()

        writer.close.assert_called_once_with()
        exit_mock.assert_called_once_with(0)

    @mock.patch("sys.exit")
    def test_close_and_exit_with_error(self, exit_mock):
        # preparation
        writer = self._mock_writer()
        output.error_occurred = True

        output.close_and_exit()

        writer.close.assert_called_once_with()
        assert exit_mock.called
        assert exit_mock.call_count == 1
        assert exit_mock.call_args[0] != 0


# noinspection PyMethodMayBeStatic
class TestConsoleWriter(object):
    row = "  {:<23}: {}"
    header_row = "  {}:"
    nested_row = "    {:<21}: {}"

    def test_debug(self, capsys):
        writer = output.ConsoleOutputWriter(debug=True)

        msg = "test message"
        writer.debug(msg)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == "DEBUG: " + msg + "\n"

        msg = "test arg %s"
        args = ("1st",)
        writer.debug(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == "DEBUG: " + msg % args + "\n"

        msg = "test args %d %s"
        args = (1, "two")
        writer.debug(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == "DEBUG: " + msg % args + "\n"

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.debug(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == "DEBUG: " + msg % kwargs + "\n"

    def test_debug_disabled(self, capsys):
        writer = output.ConsoleOutputWriter(debug=False)

        msg = "test message"
        writer.debug(msg)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test arg %s"
        args = ("1st",)
        writer.debug(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test args %d %s"
        args = (1, "two")
        writer.debug(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.debug(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

    def test_info_verbose(self, capsys):
        writer = output.ConsoleOutputWriter(quiet=False)

        msg = "test message"
        writer.info(msg)
        (out, err) = capsys.readouterr()
        assert out == msg + "\n"
        assert err == ""

        msg = "test arg %s"
        args = ("1st",)
        writer.info(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == msg % args + "\n"
        assert err == ""

        msg = "test args %d %s"
        args = (1, "two")
        writer.info(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == msg % args + "\n"
        assert err == ""

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.info(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == msg % kwargs + "\n"
        assert err == ""

    def test_info_quiet(self, capsys):
        writer = output.ConsoleOutputWriter(quiet=True)

        msg = "test message"
        writer.info(msg)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test arg %s"
        args = ("1st",)
        writer.info(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test args %d %s"
        args = (1, "two")
        writer.info(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.info(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

    def test_warning(self, capsys):
        writer = output.ConsoleOutputWriter()

        msg = "test message"
        writer.warning(msg)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == "WARNING: " + msg + "\n"

        msg = "test arg %s"
        args = ("1st",)
        writer.warning(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == "WARNING: " + msg % args + "\n"

        msg = "test args %d %s"
        args = (1, "two")
        writer.warning(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == "WARNING: " + msg % args + "\n"

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.warning(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == "WARNING: " + msg % kwargs + "\n"

    def test_error(self, capsys):
        writer = output.ConsoleOutputWriter()

        msg = "test message"
        writer.error(msg)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == "ERROR: " + msg + "\n"

        msg = "test arg %s"
        args = ("1st",)
        writer.error(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == "ERROR: " + msg % args + "\n"

        msg = "test args %d %s"
        args = (1, "two")
        writer.error(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == "ERROR: " + msg % args + "\n"

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.error(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == "ERROR: " + msg % kwargs + "\n"

    def test_exception(self, capsys):
        writer = output.ConsoleOutputWriter()

        msg = "test message"
        writer.exception(msg)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == "EXCEPTION: " + msg + "\n"

        msg = "test arg %s"
        args = ("1st",)
        writer.exception(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == "EXCEPTION: " + msg % args + "\n"

        msg = "test args %d %s"
        args = (1, "two")
        writer.exception(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == "EXCEPTION: " + msg % args + "\n"

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.exception(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == "EXCEPTION: " + msg % kwargs + "\n"

    def test_colored_warning(self, capsys, monkeypatch):
        monkeypatch.setattr(output, "ansi_colors_enabled", True)
        writer = output.ConsoleOutputWriter()

        msg = "test message"
        writer.warning(msg)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == YELLOW + "WARNING: " + msg + RESET + "\n"

        msg = "test arg %s"
        args = ("1st",)
        writer.warning(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == YELLOW + "WARNING: " + msg % args + RESET + "\n"

        msg = "test args %d %s"
        args = (1, "two")
        writer.warning(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == YELLOW + "WARNING: " + msg % args + RESET + "\n"

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.warning(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == YELLOW + "WARNING: " + msg % kwargs + RESET + "\n"

    def test_colored_error(self, capsys, monkeypatch):
        monkeypatch.setattr(output, "ansi_colors_enabled", True)
        writer = output.ConsoleOutputWriter()

        msg = "test message"
        writer.error(msg)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == RED + "ERROR: " + msg + RESET + "\n"

        msg = "test arg %s"
        args = ("1st",)
        writer.error(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == RED + "ERROR: " + msg % args + RESET + "\n"

        msg = "test args %d %s"
        args = (1, "two")
        writer.error(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == RED + "ERROR: " + msg % args + RESET + "\n"

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.error(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == RED + "ERROR: " + msg % kwargs + RESET + "\n"

    def test_colored_exception(self, capsys, monkeypatch):
        monkeypatch.setattr(output, "ansi_colors_enabled", True)
        writer = output.ConsoleOutputWriter()

        msg = "test message"
        writer.exception(msg)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == RED + "EXCEPTION: " + msg + RESET + "\n"

        msg = "test arg %s"
        args = ("1st",)
        writer.exception(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == RED + "EXCEPTION: " + msg % args + RESET + "\n"

        msg = "test args %d %s"
        args = (1, "two")
        writer.exception(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == RED + "EXCEPTION: " + msg % args + RESET + "\n"

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.exception(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == RED + "EXCEPTION: " + msg % kwargs + RESET + "\n"

    def test_init_check(self, capsys):
        writer = output.ConsoleOutputWriter()

        server = "test"

        writer.init_check(server, True, False)
        (out, err) = capsys.readouterr()
        assert out == "Server %s:\n" % server
        assert err == ""

    def test_result_check_ok(self, capsys):
        writer = output.ConsoleOutputWriter()
        output.error_occurred = False

        server = "test"
        check = "test check"

        writer.result_check(server, check, True)
        (out, err) = capsys.readouterr()
        assert out == "\t%s: OK\n" % check
        assert err == ""
        assert not output.error_occurred

    def test_result_check_ok_hint(self, capsys):
        writer = output.ConsoleOutputWriter()
        output.error_occurred = False

        server = "test"
        check = "test check"
        hint = "do something"

        writer.result_check(server, check, True, hint)
        (out, err) = capsys.readouterr()
        assert out == "\t%s: OK (%s)\n" % (check, hint)
        assert err == ""
        assert not output.error_occurred

    def test_result_check_failed(self, capsys):
        writer = output.ConsoleOutputWriter()
        output.error_occurred = False

        server = "test"
        check = "test check"

        writer.result_check(server, check, False)
        (out, err) = capsys.readouterr()
        assert out == "\t%s: FAILED\n" % check
        assert err == ""
        assert output.error_occurred

        # Test an inactive server
        # Shows error, but does not change error_occurred
        output.error_occurred = False
        writer.init_check(server, False, False)
        (out, err) = capsys.readouterr()
        assert out == "Server %s (inactive):\n" % server
        assert err == ""
        assert not output.error_occurred

        writer.result_check(server, check, False)
        (out, err) = capsys.readouterr()
        assert out == "\t%s: FAILED\n" % check
        assert err == ""
        assert not output.error_occurred

        # Test a disabled server
        # Shows error, and change error_occurred
        output.error_occurred = False
        writer.init_check(server, True, True)
        (out, err) = capsys.readouterr()
        assert out == "Server %s (WARNING: disabled):\n" % server
        assert err == ""
        assert not output.error_occurred

        writer.result_check(server, check, False)
        (out, err) = capsys.readouterr()
        assert out == "\t%s: FAILED\n" % check
        assert err == ""
        assert output.error_occurred

    def test_result_check_failed_hint(self, capsys):
        writer = output.ConsoleOutputWriter()
        output.error_occurred = False

        server = "test"
        check = "test check"
        hint = "do something"

        writer.result_check(server, check, False, hint)
        (out, err) = capsys.readouterr()
        assert out == "\t%s: FAILED (%s)\n" % (check, hint)
        assert err == ""
        assert output.error_occurred

    def test_result_check_ok_color(self, capsys, monkeypatch):
        monkeypatch.setattr(output, "ansi_colors_enabled", True)
        writer = output.ConsoleOutputWriter()
        output.error_occurred = False

        server = "test"
        check = "test check"

        writer.result_check(server, check, True)
        (out, err) = capsys.readouterr()
        assert out == "\t%s: %sOK%s\n" % (check, GREEN, RESET)
        assert err == ""
        assert not output.error_occurred

    def test_result_check_ok_hint_color(self, capsys, monkeypatch):
        monkeypatch.setattr(output, "ansi_colors_enabled", True)
        writer = output.ConsoleOutputWriter()
        output.error_occurred = False

        server = "test"
        check = "test check"
        hint = "do something"

        writer.result_check(server, check, True, hint)
        (out, err) = capsys.readouterr()
        assert out == "\t%s: %sOK%s (%s)\n" % (check, GREEN, RESET, hint)
        assert err == ""
        assert not output.error_occurred

    def test_result_check_failed_color(self, capsys, monkeypatch):
        monkeypatch.setattr(output, "ansi_colors_enabled", True)
        writer = output.ConsoleOutputWriter()
        output.error_occurred = False

        server = "test"
        check = "test check"

        writer.result_check(server, check, False)
        (out, err) = capsys.readouterr()
        assert out == "\t%s: %sFAILED%s\n" % (check, RED, RESET)
        assert err == ""
        assert output.error_occurred

        # Test an inactive server
        # Shows error, but does not change error_occurred
        output.error_occurred = False
        writer.init_check(server, False, False)
        (out, err) = capsys.readouterr()
        assert out == "Server %s (inactive):\n" % server
        assert err == ""
        assert not output.error_occurred

        writer.result_check(server, check, False)
        (out, err) = capsys.readouterr()
        assert out == "\t%s: %sFAILED%s\n" % (check, RED, RESET)
        assert err == ""
        assert not output.error_occurred

        # Test a disabled server
        # Shows error, and change error_occurred
        output.error_occurred = False
        writer.init_check(server, True, True)
        (out, err) = capsys.readouterr()
        assert out == "Server %s (WARNING: disabled):\n" % server
        assert err == ""
        assert not output.error_occurred

        writer.result_check(server, check, False)
        (out, err) = capsys.readouterr()
        assert out == "\t%s: %sFAILED%s\n" % (check, RED, RESET)
        assert err == ""
        assert output.error_occurred

    def test_result_check_failed_hint_color(self, capsys, monkeypatch):
        monkeypatch.setattr(output, "ansi_colors_enabled", True)
        writer = output.ConsoleOutputWriter()
        output.error_occurred = False

        server = "test"
        check = "test check"
        hint = "do something"

        writer.result_check(server, check, False, hint)
        (out, err) = capsys.readouterr()
        assert out == "\t%s: %sFAILED%s (%s)\n" % (check, RED, RESET, hint)
        assert err == ""
        assert output.error_occurred

    def test_init_list_backup(self):
        writer = output.ConsoleOutputWriter()

        writer.init_list_backup("test server")
        assert not writer.minimal

        writer.init_list_backup("test server", True)
        assert writer.minimal

    def test_result_list_backup(self, capsys):
        # mock the backup info
        bi = build_test_backup_info()
        backup_size = 12345
        wal_size = 54321
        retention_status = "test status"

        writer = output.ConsoleOutputWriter()

        # test minimal
        writer.init_list_backup(bi.server_name, True)
        writer.result_list_backup(bi, backup_size, wal_size, retention_status)
        writer.close()
        (out, err) = capsys.readouterr()
        assert writer.minimal
        assert bi.backup_id in out
        assert err == ""

        # test status=DONE output
        writer.init_list_backup(bi.server_name, False)
        writer.result_list_backup(bi, backup_size, wal_size, retention_status)
        writer.close()
        (out, err) = capsys.readouterr()
        assert not writer.minimal
        assert bi.server_name in out
        assert bi.backup_id in out
        assert str(bi.end_time.ctime()) in out
        assert "Size: " + pretty_size(backup_size) in out
        assert "WAL Size: " + pretty_size(wal_size) in out
        assert err == ""

        # test status = FAILED output
        bi = build_test_backup_info(status=BackupInfo.FAILED)
        writer.init_list_backup(bi.server_name, False)
        writer.result_list_backup(bi, backup_size, wal_size, retention_status)
        writer.close()
        (out, err) = capsys.readouterr()
        assert not writer.minimal
        assert "%s %s - S - %s" % (bi.server_name, bi.backup_id, bi.status) in out

    def test_result_list_backup_with_backup_name(self, capsys):
        # GIVEN a backup info with a backup_name
        bi = build_test_backup_info(
            backup_name="named backup",
        )
        backup_size = 12345
        wal_size = 54321
        retention_status = "test status"

        # WHEN the list_backup output is generated in Plain form
        console_writer = output.ConsoleOutputWriter()
        console_writer.init_list_backup(bi.server_name, False)
        console_writer.result_list_backup(bi, backup_size, wal_size, retention_status)
        console_writer.close()

        # THEN the console output contains the backup name
        out, _err = capsys.readouterr()
        assert "%s %s '%s'" % (bi.server_name, bi.backup_id, bi.backup_name) in out

    def test_result_show_backup_any_mode(self, capsys):
        """
        Unit test for the show-backup command that display information in the
        console that are common between any type of backup method.

        This unit tests checks if the fields common to any backup mode/method are
        correctly rendered to the standard output.

        :param capsys: mock fixture for stdout/stderr
        """
        backup_manager = build_backup_manager(
            main_conf={"backup_options": "concurrent_backup"}
        )
        backup_info = build_test_backup_info(
            server=backup_manager.server,
            backup_id="12345",
            summarize_wal="on",
            cluster_size=2048,
            deduplicated_size=1234,
            systemid="systemid",
            data_checksums="on",
        )

        wal_per_second = 0.01
        ext_info = mock_backup_ext_info(
            backup_info=backup_info,
            children_timelines=(mock.Mock(tli="1"), mock.Mock(tli="2")),
            wal_until_next_compression_ratio=1.5,
            wals_per_second=wal_per_second,
            est_dedup_size=1024,
            deduplication_ratio=0.99,
            wal_rate=wal_per_second * 3600,
            wal_compression_ratio=0.5,
        )

        writer = output.ConsoleOutputWriter()

        # test minimal
        writer.result_show_backup(ext_info)
        writer.close()
        (out, err) = capsys.readouterr()
        assert f'Backup {ext_info["backup_id"]}' in out

        # Header rows
        header_rows = [
            "Server information",
            "Tablespaces",
            "Base backup information",
            "WAL information",
            "Catalog information",
        ]
        for h_row in header_rows:
            assert TestConsoleWriter.header_row.format(h_row) in out

        # Rows
        rows = [
            ("Server Name", ext_info["server_name"]),
            ("System Id", ext_info["systemid"]),
            ("Status", ext_info["status"]),
            ("PostgreSQL Version", ext_info["version"]),
            ("PGDATA directory", ext_info["pgdata"]),
            ("Estimated Cluster Size", pretty_size(ext_info["cluster_size"])),
        ]

        for _row in rows:
            assert TestConsoleWriter.row.format(_row[0], _row[1]) in out

        backup_size_output = "{} ({} with WALs)".format(
            pretty_size(ext_info["deduplicated_size"]),
            pretty_size(ext_info["deduplicated_size"] + ext_info["wal_size"]),
        )
        wal_compression_output = "{percent:.2%}".format(
            percent=ext_info["wal_compression_ratio"]
        )
        compression_rate_output = "{percent:.2%}".format(
            percent=ext_info["wal_until_next_compression_ratio"]
        )

        # Nested rows
        nested_rows = [
            ("Checksums", ext_info["data_checksums"]),
            ("WAL summarizer", ext_info["summarize_wal"]),
            ("Backup Method", ext_info["mode"]),
            ("Backup Size", backup_size_output),
            ("WAL Size", pretty_size(ext_info["wal_size"])),
            ("Timeline", str(ext_info["timeline"])),
            ("Begin WAL", ext_info["begin_wal"]),
            ("End WAL", ext_info["end_wal"]),
            ("WAL number", ext_info["wal_num"]),
            ("WAL compression ratio", wal_compression_output),
            ("Begin time", str(ext_info["begin_time"])),
            ("End time", str(ext_info["end_time"])),
            ("Begin Offset", str(ext_info["begin_offset"])),
            ("End Offset", str(ext_info["end_offset"])),
            ("Begin LSN", str(ext_info["begin_xlog"])),
            ("End LSN", str(ext_info["end_xlog"])),
            ("No of files", ext_info["wal_until_next_num"]),
            ("Disk usage", pretty_size(ext_info["wal_until_next_size"])),
            ("WAL rate", "%0.2f/hour" % (wal_per_second * 3600)),
            ("Compression ratio", compression_rate_output),
            ("Last available", ext_info["wal_last"]),
            ("Reachable timelines", "1, 2"),
            ("Retention Policy", "not enforced"),
            ("Previous Backup", "- (this is the oldest base backup)"),
            ("Next Backup", "- (this is the latest base backup)"),
        ]
        for name, _, location in ext_info["tablespaces"]:
            nested_rows.append((name, location))

        for n_row in nested_rows:
            assert TestConsoleWriter.nested_row.format(n_row[0], n_row[1]) in out
        assert err == ""

    @pytest.mark.parametrize(
        ("backup_method", "backup_type", "parent_backup_id", "chidren_backup_ids"),
        [
            ("postgres", "full", None, ["1234", "123456"]),
            ("postgres", "incremental", 12345, ["1234", "123456"]),
            ("rsync", None, None, None),
        ],
    )
    def test_result_show_backup_specific_fields_by_backup_type(
        self, backup_method, backup_type, parent_backup_id, chidren_backup_ids, capsys
    ):
        """
        Unit test for the show-backup command that display specific information
        in the console for each type of the following backups: 'rsync',
        'incremental' and 'full'.

        This unit tests checks if the fields that are related to those types of
        backups are correctly rendered to the standard output.

        :param backup_method: backup_method mock parameter
        :param backup_type: backup_type mock parameter
        :param parent_backup_id: parent_backup_id mock parameter
        :param chidren_backup_ids: chidren_backup_ids mock parameter
        :param capsys: mock fixture for stdout/stderr
        """
        backup_manager = build_backup_manager(
            main_conf={
                "backup_method": backup_method,
                "backup_options": "concurrent_backup",
            }
        )
        backup_info = build_test_backup_info(
            server=backup_manager.server,
            backup_id="1",
            summarize_wal="on",
            cluster_size=2048,
            deduplicated_size=1234,
            systemid="systemid",
            parent_backup_id=parent_backup_id,
            children_backup_ids=chidren_backup_ids,
        )

        wal_per_second = 0.01
        ext_info = mock_backup_ext_info(
            backup_info=backup_info,
            root_backup_id="root",
            chain_size="2",
            est_dedup_size=1024,
            deduplication_ratio=0.99,
            wal_rate=wal_per_second * 3600,
            backup_type=backup_type,
        )

        writer = output.ConsoleOutputWriter()

        writer.result_show_backup(ext_info)
        writer.close()
        (out, err) = capsys.readouterr()

        resources_saved_output = "{} ({})".format(
            pretty_size(ext_info["est_dedup_size"]),
            "{percent:.2%}".format(percent=ext_info["deduplication_ratio"]),
        )

        # Output for both 'rsync' and 'incremental' backup type
        if ext_info["backup_type"] in ("rsync", "incremental"):
            assert (
                TestConsoleWriter.nested_row.format(
                    "Resources saved", resources_saved_output
                )
                in out
            )
        # Output for 'postgres' backup method
        if ext_info["mode"] == "postgres":
            # Output for 'full' and 'incremental' backup type
            full = [
                ("Backup Type", ext_info["backup_type"]),
                ("Children Backup(s)", ext_info["children_backup_ids"]),
            ]
            for n_row in full:
                assert TestConsoleWriter.nested_row.format(n_row[0], n_row[1]) in out

            # Output only for 'incremental' backup type
            incremental = [
                ("Root Backup", ext_info["root_backup_id"]),
                ("Parent Backup", ext_info["parent_backup_id"]),
                ("Backup chain size", ext_info["chain_size"]),
            ]
            if ext_info["backup_type"] == "incremental":
                for n_row in incremental:
                    assert (
                        TestConsoleWriter.nested_row.format(n_row[0], n_row[1]) in out
                    )
        assert err == ""

    def test_result_show_backup_any_mode_copy_stats(self, capsys):
        """
        Unit test for the show-backup command that display information in the
        console about copy statistics.

        This unit tests checks if the copy statistics are correctly rendered
        to the standard output.

        :param capsys: mock fixture for stdout/stderr
        """
        backup_manager = build_backup_manager(
            main_conf={"backup_options": "concurrent_backup"}
        )
        backup_info = build_test_backup_info(
            server=backup_manager.server,
            backup_id="12345",
            cluster_size=2048,
            copy_stats={"analysis_time": 2, "copy_time": 1, "number_of_workers": 1},
            deduplicated_size=1234,
            systemid="systemid",
        )
        ext_info = mock_backup_ext_info(
            backup_info=backup_info,
            est_dedup_size=1024,
            deduplication_ratio=0.99,
            copy_time=backup_info.copy_stats["copy_time"],
            analysis_time=backup_info.copy_stats["analysis_time"],
            number_of_workers=backup_info.copy_stats["number_of_workers"],
            estimated_throughput=1,
        )

        writer = output.ConsoleOutputWriter()

        writer.result_show_backup(ext_info)
        writer.close()
        (out, err) = capsys.readouterr()

        copy_time_output = human_readable_timedelta(
            datetime.timedelta(seconds=ext_info["copy_time"])
        )
        copy_time_output += " + {} startup".format(
            human_readable_timedelta(
                datetime.timedelta(seconds=ext_info["analysis_time"])
            )
        )
        assert TestConsoleWriter.nested_row.format("Copy time", copy_time_output) in out
        est_throughput_output = "{}/s".format(
            pretty_size(ext_info["estimated_throughput"])
        )
        if "number_of_workers" in ext_info and ext_info["number_of_workers"] > 1:
            est_throughput_output += " (%s jobs)" % ext_info["number_of_workers"]
        assert (
            TestConsoleWriter.nested_row.format(
                "Estimated throughput", est_throughput_output
            )
            in out
        )
        assert err == ""

    def test_result_show_backup_with_backup_name(self, capsys):
        # GIVEN a backup info with a backup_name
        ext_info = mock_backup_ext_info(
            backup_name="named backup",
            status=BackupInfo.DONE,
            wals_per_second=0.1,
            est_dedup_size=1024,
            deduplication_ratio=0.5,
        )

        # WHEN the list_backup output is generated in Plain form
        console_writer = output.ConsoleOutputWriter()

        console_writer.init_list_backup(ext_info["server_name"], False)
        console_writer.result_show_backup(ext_info)
        console_writer.close()

        # THEN the output contains the backup name
        out, _err = capsys.readouterr()
        assert "  Backup Name            : %s" % ext_info["backup_name"] in out

    def test_result_show_backup_with_snapshots_info_gcp(self, capsys):
        # GIVEN a backup info with snapshots_info
        snapshots_info = GcpSnapshotsInfo(
            project="test_project",
            snapshots=[
                GcpSnapshotMetadata(
                    mount_point="/opt/mount0",
                    mount_options="rw,noatime",
                    device_name="dev0",
                    snapshot_name="snapshot0",
                    snapshot_project="test_project",
                ),
                GcpSnapshotMetadata(
                    mount_point="/opt/mount1",
                    mount_options="rw",
                    device_name="dev1",
                    snapshot_name="snapshot1",
                    snapshot_project="test_project",
                ),
            ],
        )
        ext_info = mock_backup_ext_info(
            snapshots_info=snapshots_info,
            status=BackupInfo.DONE,
            wals_per_second=0.1,
            cluster_size=2048,
            est_dedup_size=1024,
            deduplication_ratio=0.5,
        )

        # WHEN the show output is generated in Plain form
        console_writer = output.ConsoleOutputWriter()

        console_writer.init_list_backup(ext_info["server_name"], False)
        console_writer.result_show_backup(ext_info)
        console_writer.close()

        # THEN the output contains the snapshot info
        out, _err = capsys.readouterr()
        assert "   project              : %s" % snapshots_info.project in out
        for snapshot_metadata in snapshots_info.snapshots:
            assert (
                "    snapshot_name        : %s" % snapshot_metadata.snapshot_name in out
            )
            assert (
                "    snapshot_project     : %s" % snapshot_metadata.snapshot_project
                in out
            )
            assert (
                "    device_name          : %s" % snapshot_metadata.device_name in out
            )
            assert (
                "    Mount point          : %s" % snapshot_metadata.mount_point in out
            )
            assert (
                "    Mount options        : %s" % snapshot_metadata.mount_options in out
            )

    def test_result_show_backup_error(self, capsys):
        # mock the backup ext info
        msg = "test error message"
        ext_info = mock_backup_ext_info(status=BackupInfo.FAILED, error=msg)

        writer = output.ConsoleOutputWriter()

        # test minimal
        writer.result_show_backup(ext_info)
        writer.close()
        (out, err) = capsys.readouterr()
        assert ext_info["server_name"] in out
        assert ext_info["backup_id"] in out
        assert ext_info["status"] in out
        assert str(ext_info["end_time"]) not in out
        assert msg in out
        assert err == ""

    def test_init_status(self, capsys):
        writer = output.ConsoleOutputWriter()

        server = "test"

        writer.init_status(server)
        (out, err) = capsys.readouterr()
        assert out == "Server %s:\n" % server
        assert err == ""

    def test_result_status(self, capsys):
        writer = output.ConsoleOutputWriter()

        server = "test"
        name = "test name"
        description = "test description"
        message = "test message"

        writer.result_status(server, name, description, message)
        (out, err) = capsys.readouterr()
        assert out == "\t%s: %s\n" % (description, message)
        assert err == ""

    def test_result_status_non_str(self, capsys):
        writer = output.ConsoleOutputWriter()

        server = "test"
        name = "test name"
        description = "test description"
        message = 1

        writer.result_status(server, name, description, message)
        (out, err) = capsys.readouterr()
        assert out == "\t%s: %s\n" % (description, message)
        assert err == ""

    def test_redact_passwords(self, capsys):
        writer = output.ConsoleOutputWriter()

        msg = "message with password=SHAME_ON_ME inside"
        writer.info(msg)
        (out, err) = capsys.readouterr()
        assert out == "message with password=*REDACTED* inside\n"
        assert err == ""

        msg = "some postgresql://me:SECRET@host:5432/mydb conn"
        writer.info(msg)
        (out, err) = capsys.readouterr()
        assert out == "some postgresql://me:*REDACTED*@host:5432/mydb conn\n"
        assert err == ""

    def test_readact_passwords_in_json(self, capsys):
        writer = output.ConsoleOutputWriter()

        msg = '{"conninfo": "dbname=t password=SHAME_ON_ME", "a": "b"}'
        writer.info(msg)
        (out, err) = capsys.readouterr()
        json_out = '{"conninfo": "dbname=t password=*REDACTED*", "a": "b"}\n'
        assert out == json_out
        assert err == ""


# noinspection PyMethodMayBeStatic
class TestJsonWriter(object):
    # Fixed start and end timestamps for backup/recovery timestamps
    begin_time = datetime.datetime(2022, 7, 4, 9, 15, 35, tzinfo=tz.tzutc())
    begin_epoch = "1656926135"
    end_time = datetime.datetime(2022, 7, 4, 9, 22, 37, tzinfo=tz.tzutc())
    end_epoch = "1656926557"

    def test_debug(self, capsys):
        writer = output.JsonOutputWriter(debug=True)

        msg = "test message"
        msg2 = "second message"

        writer.debug(msg)
        writer.debug(msg2)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg in json_output["_DEBUG"]
        assert msg2 in json_output["_DEBUG"]
        assert err == ""

        msg = "test arg %s"
        args = ("1st",)
        writer.debug(msg, *args)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg % args in json_output["_DEBUG"]
        assert err == ""

        msg = "test args %d %s"
        args = (1, "two")
        writer.debug(msg, *args)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg % args in json_output["_DEBUG"]
        assert err == ""

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.debug(msg, kwargs)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg % kwargs in json_output["_DEBUG"]
        assert err == ""

    def test_debug_disabled(self, capsys):
        writer = output.JsonOutputWriter(debug=False)

        msg = "test message"
        writer.debug(msg)
        writer.close()
        (out, err) = capsys.readouterr()
        assert out == "{}"
        assert err == ""

        msg = "test arg %s"
        args = ("1st",)
        writer.debug(msg, *args)
        writer.close()
        (out, err) = capsys.readouterr()
        assert out == "{}"
        assert err == ""

        msg = "test args %d %s"
        args = (1, "two")
        writer.debug(msg, *args)
        writer.close()
        (out, err) = capsys.readouterr()
        assert out == "{}"
        assert err == ""

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.debug(msg, kwargs)
        writer.close()
        (out, err) = capsys.readouterr()
        assert out == "{}"
        assert err == ""

    def test_info_verbose(self, capsys):
        writer = output.JsonOutputWriter(quiet=False)

        msg = "test message"
        msg2 = "second message"
        writer.info(msg)
        writer.info(msg2)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg in json_output["_INFO"]
        assert msg2 in json_output["_INFO"]
        assert err == ""

        msg = "test arg %s"
        args = ("1st",)
        writer.info(msg, *args)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg % args in json_output["_INFO"]
        assert err == ""

        msg = "test args %d %s"
        args = (1, "two")
        writer.info(msg, *args)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg % args in json_output["_INFO"]
        assert err == ""

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.info(msg, kwargs)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg % kwargs in json_output["_INFO"]
        assert err == ""

    def test_info_quiet(self, capsys):
        writer = output.JsonOutputWriter(quiet=True)

        msg = "test message"
        writer.info(msg)
        writer.close()
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test arg %s"
        args = ("1st",)
        writer.info(msg, *args)
        writer.close()
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test args %d %s"
        args = (1, "two")
        writer.info(msg, *args)
        writer.close()
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.info(msg, kwargs)
        writer.close()
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

    def test_warning(self, capsys):
        writer = output.JsonOutputWriter()

        msg = "test message"
        msg2 = "second message"

        writer.warning(msg)
        writer.warning(msg2)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg in json_output["_WARNING"]
        assert msg2 in json_output["_WARNING"]
        assert err == ""

        msg = "test arg %s"
        args = ("1st",)
        writer.warning(msg, *args)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg % args in json_output["_WARNING"]
        assert err == ""

        msg = "test args %d %s"
        args = (1, "two")
        writer.warning(msg, *args)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg % args in json_output["_WARNING"]
        assert err == ""

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.warning(msg, kwargs)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg % kwargs in json_output["_WARNING"]
        assert err == ""

    def test_error(self, capsys):
        writer = output.JsonOutputWriter()

        msg = "test message"
        msg2 = "second message"
        writer.error(msg)
        writer.error(msg2)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg in json_output["_ERROR"]
        assert msg2 in json_output["_ERROR"]
        assert err == ""

        msg = "test arg %s"
        args = ("1st",)
        writer.error(msg, *args)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg % args in json_output["_ERROR"]
        assert err == ""

        msg = "test args %d %s"
        args = (1, "two")
        writer.error(msg, *args)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg % args in json_output["_ERROR"]
        assert err == ""

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.error(msg, kwargs)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg % kwargs in json_output["_ERROR"]
        assert err == ""

    def test_exception(self, capsys):
        writer = output.JsonOutputWriter()

        msg = "test message"
        msg2 = "second message"
        writer.exception(msg)
        writer.exception(msg2)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg in json_output["_EXCEPTION"]
        assert msg2 in json_output["_EXCEPTION"]
        assert err == ""

        msg = "test arg %s"
        args = ("1st",)
        writer.exception(msg, *args)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg % args in json_output["_EXCEPTION"]
        assert err == ""

        msg = "test args %d %s"
        args = (1, "two")
        writer.exception(msg, *args)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg % args in json_output["_EXCEPTION"]
        assert err == ""

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.exception(msg, kwargs)
        writer.close()
        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert msg % kwargs in json_output["_EXCEPTION"]
        assert err == ""

    def test_init_check(self, capsys):
        writer = output.JsonOutputWriter()

        server = "test"

        writer.init_check(server, True, False)
        writer.close()

        (out, err) = capsys.readouterr()
        json_output = json.loads(out)

        assert server in json_output
        assert err == ""

    def test_result_check_ok(self, capsys):
        writer = output.JsonOutputWriter()
        output.error_occurred = False

        server = "test"
        check = "test check"

        writer.init_check(server, active=True, disabled=False)
        writer.result_check(server, check, True)
        writer.close()

        (out, err) = capsys.readouterr()
        json_output = json.loads(out)

        assert "OK" == json_output[server][check.replace(" ", "_")]["status"]
        assert err == ""
        assert not output.error_occurred

    def test_result_check_ok_hint(self, capsys):
        writer = output.JsonOutputWriter()
        output.error_occurred = False

        server = "test"
        check = "test check"
        hint = "do something"

        writer.init_check(server, active=True, disabled=False)
        writer.result_check(server, check, True, hint)
        writer.close()

        (out, err) = capsys.readouterr()
        json_output = json.loads(out)

        assert "OK" == json_output[server][check.replace(" ", "_")]["status"]
        assert hint == json_output[server][check.replace(" ", "_")]["hint"]
        assert err == ""
        assert not output.error_occurred

    def test_result_check_failed(self, capsys):
        writer = output.JsonOutputWriter()
        output.error_occurred = False

        server = "test"
        check = "test check"

        writer.init_check(server, active=True, disabled=False)
        writer.result_check(server, check, False)
        writer.close()

        (out, err) = capsys.readouterr()
        json_output = json.loads(out)

        check_key = check.replace(" ", "_")
        assert "FAILED" == json_output[server][check_key]["status"]
        assert err == ""
        assert output.error_occurred

        # Test an inactive server
        # Shows error, but does not change error_occurred
        output.error_occurred = False
        writer.init_check(server, active=False, disabled=False)
        writer.result_check(server, check, False)
        writer.close()

        (out, err) = capsys.readouterr()
        json_output = json.loads(out)

        check_key = check.replace(" ", "_")
        assert "FAILED" == json_output[server][check_key]["status"]
        assert err == ""
        assert not output.error_occurred

    def test_result_check_failed_hint(self, capsys):
        writer = output.JsonOutputWriter()
        output.error_occurred = False

        server = "test"
        check = "test check"
        hint = "do something"

        writer.init_check(server, active=True, disabled=False)
        writer.result_check(server, check, False, hint)
        writer.close()

        (out, err) = capsys.readouterr()
        json_output = json.loads(out)

        check_key = check.replace(" ", "_")
        assert "FAILED" == json_output[server][check_key]["status"]
        assert hint == json_output[server][check_key]["hint"]
        assert err == ""
        assert output.error_occurred

    def test_init_list_backup(self, capsys):
        writer = output.JsonOutputWriter()

        server_name = "test server"
        writer.init_list_backup(server_name)
        writer.close()

        (out, err) = capsys.readouterr()
        json_output = json.loads(out)

        assert not writer.minimal
        assert server_name in json_output

        writer.init_list_backup(server_name, True)
        writer.close()

        (out, err) = capsys.readouterr()
        json_output = json.loads(out)

        assert writer.minimal
        assert server_name in json_output

    @mock.patch.dict("os.environ", {"TZ": "US/Eastern"})
    def test_result_list_backup(self, capsys):
        # mock the backup info
        bi = build_test_backup_info(begin_time=self.begin_time, end_time=self.end_time)
        backup_size = 12345
        wal_size = 54321
        retention_status = "test status"

        writer = output.JsonOutputWriter()

        # test minimal
        writer.init_list_backup(bi.server_name, True)
        writer.result_list_backup(bi, backup_size, wal_size, retention_status)
        writer.close()

        (out, err) = capsys.readouterr()
        json_output = json.loads(out)

        assert writer.minimal
        assert bi.backup_id in json_output[bi.server_name]
        assert err == ""

        # test status=DONE output
        writer.init_list_backup(bi.server_name, False)
        writer.result_list_backup(bi, backup_size, wal_size, retention_status)
        writer.close()

        (out, err) = capsys.readouterr()
        json_output = json.loads(out)

        assert not writer.minimal
        assert bi.server_name in json_output

        backup = find_by_attr(json_output[bi.server_name], "backup_id", bi.backup_id)
        assert bi.backup_id == backup["backup_id"]
        assert str(bi.end_time.ctime()) == backup["end_time"]
        assert self.end_epoch == backup["end_time_timestamp"]
        assert pretty_size(backup_size) == backup["size"]
        assert pretty_size(wal_size) == backup["wal_size"]
        assert err == ""

        # test status = FAILED output
        bi = build_test_backup_info(status=BackupInfo.FAILED)
        writer.init_list_backup(bi.server_name, False)
        writer.result_list_backup(bi, backup_size, wal_size, retention_status)
        writer.close()

        (out, err) = capsys.readouterr()
        json_output = json.loads(out)

        assert not writer.minimal
        assert bi.server_name in json_output
        backup = find_by_attr(json_output[bi.server_name], "backup_id", bi.backup_id)
        assert bi.backup_id == backup["backup_id"]
        assert bi.status == backup["status"]

    def test_result_list_backup_with_backup_name(self, capsys):
        # GIVEN a backup info with a backup_name
        bi = build_test_backup_info(
            backup_name="named backup",
            begin_time=self.begin_time,
            end_time=self.end_time,
        )
        backup_size = 12345
        wal_size = 54321
        retention_status = "test status"

        # WHEN the list_backup output is generated in JSON form
        json_writer = output.JsonOutputWriter()
        json_writer.init_list_backup(bi.server_name, False)
        json_writer.result_list_backup(bi, backup_size, wal_size, retention_status)
        json_writer.close()

        # THEN the json output contains the backup name
        out, _err = capsys.readouterr()
        json_output = json.loads(out)

        assert json_output[bi.server_name][0]["backup_id"] == bi.backup_id
        assert json_output[bi.server_name][0]["backup_name"] == bi.backup_name

    def test_result_list_backup_types(self, capsys):
        # GIVEN a backup info with specific backup types
        backup_types = ["rsync", "incremental", "full", "snapshot"]
        backup_size = 12345
        wal_size = 54321
        retention_status = "test status"

        for backup_type in backup_types:
            bi = build_test_backup_info(
                server_name="test_server",
                backup_id="test_backup_id",
                status="DONE",
            )

            # Mock the backup_type property
            type_mock = PropertyMock(return_value=backup_type)
            type(bi).backup_type = type_mock

            # WHEN the list_backup output is generated
            json_writer = output.JsonOutputWriter()
            json_writer.init_list_backup(bi.server_name, False)
            json_writer.result_list_backup(bi, backup_size, wal_size, retention_status)
            json_writer.close()

            # Capture the output
            out, err = capsys.readouterr()

            # THEN the JSON output contains the correct backup type
            json_output = json.loads(out)
            backup = find_by_attr(
                json_output[bi.server_name], "backup_id", bi.backup_id
            )
            assert backup["backup_type"] == backup_type
            assert err == ""
            type_mock.assert_called_once()

    @mock.patch.dict("os.environ", {"TZ": "US/Eastern"})
    def test_result_show_backup(self, capsys):
        """
        Unit test for the show-backup command that display information about a
        backup with a json format.

        This unit tests checks if all fields of a backup are correctly in
        the result dict to be rendered as json.

        :param capsys: mock fixture for stdout/stderr
        """
        backup_manager = build_backup_manager(
            main_conf={"backup_options": "concurrent_backup"}
        )
        backup_info = build_test_backup_info(
            server=backup_manager.server,
            backup_id="12345",
            summarize_wal="on",
            cluster_size=2048,
            deduplicated_size=1234,
            systemid="systemid",
            data_checksums="on",
            begin_time=self.begin_time,
            end_time=self.end_time,
        )

        wal_per_second = 0.01
        ext_info = mock_backup_ext_info(
            backup_info=backup_info,
            children_timelines=(mock.Mock(tli="1"), mock.Mock(tli="2")),
            wal_until_next_compression_ratio=1.5,
            wals_per_second=wal_per_second,
            est_dedup_size=1024,
            deduplication_ratio=0.99,
            wal_rate=wal_per_second * 3600,
            wal_compression_ratio=0.5,
            copy_time=1,
            analysis_time=2,
            number_of_workers=1,
            backup_type="incremental",
            root_backup_id="1234",
            retention_policy_status="VALID",
            previous_backup_id="12345",
            next_backup_id="123456",
        )
        server_name = ext_info["server_name"]

        writer = output.JsonOutputWriter()
        writer.result_show_backup(ext_info)
        writer.close()

        (out, err) = capsys.readouterr()
        json_output = json.loads(out)

        base_information = json_output[server_name]["base_backup_information"]
        wal_information = json_output[server_name]["wal_information"]
        server_information = json_output[server_name]["server_information"]
        catalog_information = json_output[server_name]["catalog_information"]

        assert server_name in json_output
        assert ext_info["backup_id"] == json_output[server_name]["backup_id"]
        assert ext_info["status"] == json_output[server_name]["status"]
        assert ext_info["systemid"] == json_output[server_name]["system_id"]
        assert ext_info["version"] == json_output[server_name]["postgresql_version"]
        assert ext_info["pgdata"] == json_output[server_name]["pgdata_directory"]
        assert (
            pretty_size(ext_info["cluster_size"])
            == json_output[server_name]["cluster_size"]
        )
        assert (
            ext_info["cluster_size"] == json_output[server_name]["cluster_size_bytes"]
        )

        assert ext_info["data_checksums"] == server_information["data_checksums"]
        assert ext_info["summarize_wal"] == server_information["summarize_wal"]

        assert ext_info["mode"] == base_information["backup_method"]
        if ext_info["backup_type"] == "incremental":
            assert ext_info["root_backup_id"] == catalog_information["root_backup_id"]
            assert (
                ext_info["parent_backup_id"] == catalog_information["parent_backup_id"]
            )
            assert ext_info["chain_size"] == catalog_information["chain_size"]
        if ext_info["mode"] == "postgres":
            assert ext_info["backup_type"] == base_information["backup_type"]
            assert (
                ext_info["children_backup_ids"]
                == catalog_information["children_backup_ids"]
            )

        assert (
            pretty_size(ext_info["deduplicated_size"])
            == base_information["backup_size"]
        )
        assert ext_info["deduplicated_size"] == base_information["backup_size_bytes"]
        assert (
            pretty_size(ext_info["deduplicated_size"] + ext_info["wal_size"])
            == base_information["backup_size_with_wals"]
        )
        assert (
            ext_info["deduplicated_size"] + ext_info["wal_size"]
            == base_information["backup_size_with_wals_bytes"]
        )
        assert pretty_size(ext_info["wal_size"]) == base_information["wal_size"]
        assert ext_info["wal_size"] == base_information["wal_size_bytes"]

        assert (
            pretty_size(ext_info["est_dedup_size"])
            == base_information["resources_saved"]
        )
        assert ext_info["est_dedup_size"] == base_information["resources_saved_bytes"]
        resources_saved_percentage = "{percent:.2%}".format(
            percent=ext_info["deduplication_ratio"]
        )
        assert (
            resources_saved_percentage == base_information["resources_saved_percentage"]
        )

        assert ext_info["timeline"] == base_information["timeline"]
        assert ext_info["begin_wal"] == base_information["begin_wal"]
        assert ext_info["end_wal"] == base_information["end_wal"]
        assert ext_info["wal_num"] == base_information["wal_num"]

        wal_compression_ratio = "{percent:.2%}".format(
            percent=ext_info["wal_compression_ratio"]
        )
        assert wal_compression_ratio == base_information["wal_compression_ratio"]

        assert str(ext_info["begin_time"]) == base_information["begin_time"]
        assert str(ext_info["end_time"]) == base_information["end_time"]

        assert self.end_epoch == base_information["end_time_timestamp"]
        assert self.begin_epoch == base_information["begin_time_timestamp"]

        copy_time = human_readable_timedelta(
            datetime.timedelta(seconds=ext_info["copy_time"])
        )
        analysis_time = human_readable_timedelta(
            datetime.timedelta(seconds=ext_info["analysis_time"])
        )
        assert copy_time == base_information["copy_time"]
        assert analysis_time == base_information["analysis_time"]
        assert ext_info["copy_time"] == base_information["copy_time_seconds"]
        throughput_output = "%s/s" % pretty_size(
            ext_info["deduplicated_size"] / ext_info["copy_time"]
        )
        assert throughput_output == base_information["throughput"]
        assert (
            ext_info["deduplicated_size"] / ext_info["copy_time"]
            == base_information["throughput_bytes"]
        )
        assert ext_info["number_of_workers"] == base_information["number_of_workers"]

        assert ext_info["begin_offset"] == base_information["begin_offset"]
        assert ext_info["end_offset"] == base_information["end_offset"]
        assert ext_info["begin_xlog"] == base_information["begin_lsn"]
        assert ext_info["end_xlog"] == base_information["end_lsn"]

        for name, _, location in ext_info["tablespaces"]:
            tablespace = find_by_attr(
                json_output[server_name]["tablespaces"], "name", name
            )
            assert name == tablespace["name"]
            assert location == tablespace["location"]

        assert ext_info["wal_until_next_num"] == wal_information["no_of_files"]
        assert (
            pretty_size(ext_info["wal_until_next_size"])
            == wal_information["disk_usage"]
        )
        assert ext_info["wal_until_next_size"] == wal_information["disk_usage_bytes"]

        wal_rate = "%0.2f/hour" % (ext_info["wals_per_second"] * 3600)
        assert wal_rate == wal_information["wal_rate"]
        assert ext_info["wals_per_second"] == wal_information["wal_rate_per_second"]

        compression_ratio = "{percent:.2%}".format(
            percent=ext_info["wal_until_next_compression_ratio"]
        )
        assert compression_ratio == wal_information["compression_ratio"]
        assert ext_info["wal_last"] == wal_information["last_available"]

        assert (
            ext_info["retention_policy_status"]
            == catalog_information["retention_policy"]
        )
        assert ext_info["previous_backup_id"] == catalog_information["previous_backup"]
        assert ext_info["next_backup_id"] == catalog_information["next_backup"]

        assert err == ""

    def test_result_show_backup_with_backup_name(self, capsys):
        backup_manager = build_backup_manager(
            main_conf={"backup_options": "concurrent_backup"}
        )
        backup_info = build_test_backup_info(
            backup_name="named backup",
            server=backup_manager.server,
            cluster_size=2048,
            deduplicated_size=1234,
        )

        wal_per_second = 0.01
        ext_info = mock_backup_ext_info(
            backup_info=backup_info,
            est_dedup_size=1024,
            deduplication_ratio=0.99,
            wal_rate=wal_per_second * 3600,
        )

        # WHEN the list_backup output is generated in JSON form
        json_writer = output.JsonOutputWriter()

        # THEN the output contains the backup name
        json_writer.result_show_backup(ext_info)
        json_writer.close()

        out, _err = capsys.readouterr()
        json_output = json.loads(out)

        assert (
            json_output[ext_info["server_name"]]["backup_id"] == ext_info["backup_id"]
        )
        assert (
            json_output[ext_info["server_name"]]["backup_name"]
            == ext_info["backup_name"]
        )

    def test_result_show_backup_with_snapshots_info(self, capsys):
        # GIVEN a backup info with snapshots_info
        snapshots_info = GcpSnapshotsInfo(
            project="test_project",
            snapshots=[
                GcpSnapshotMetadata(
                    mount_point="/opt/mount0",
                    mount_options="rw,noatime",
                    device_name="dev0",
                    snapshot_name="snapshot0",
                    snapshot_project="test_project",
                ),
                GcpSnapshotMetadata(
                    mount_point="/opt/mount1",
                    mount_options="rw",
                    device_name="dev1",
                    snapshot_name="snapshot1",
                    snapshot_project="test_project",
                ),
            ],
        )
        backup_manager = build_backup_manager(
            main_conf={"backup_options": "concurrent_backup"}
        )
        backup_info = build_test_backup_info(
            server=backup_manager.server,
            cluster_size=2048,
            deduplicated_size=1234,
            snapshots_info=snapshots_info,
        )

        wal_per_second = 0.01
        ext_info = mock_backup_ext_info(
            backup_info=backup_info,
            est_dedup_size=1024,
            deduplication_ratio=0.99,
            wal_rate=wal_per_second * 3600,
        )

        # WHEN the show backup output is generated in JSON form
        console_writer = output.JsonOutputWriter()

        console_writer.result_show_backup(ext_info)
        console_writer.close()

        # THEN the output contains the snapshot info
        out, _err = capsys.readouterr()
        json_output = json.loads(out)
        assert (
            json_output[ext_info["server_name"]]["snapshots_info"]
            == snapshots_info.to_dict()
        )

    def test_result_show_backup_error(self, capsys):
        # mock the backup ext info
        msg = "test error message"
        ext_info = mock_backup_ext_info(status=BackupInfo.FAILED, error=msg)
        server_name = ext_info["server_name"]

        writer = output.JsonOutputWriter()
        writer.result_show_backup(ext_info)
        writer.close()

        (out, err) = capsys.readouterr()
        json_output = json.loads(out)

        assert server_name in json_output
        assert ext_info["backup_id"] == json_output[server_name]["backup_id"]
        assert ext_info["status"] == json_output[server_name]["status"]
        assert "base_backup_information" not in json_output[server_name]
        assert msg == json_output[server_name]["error"]
        assert err == ""

    @mock.patch.dict("os.environ", {"TZ": "US/Eastern"})
    def test_result_recovery(self, capsys):
        recovery_info = {
            "changes": [],
            "warnings": [],
            "missing_files": [],
            "delete_barman_wal": False,
            "get_wal": False,
            "recovery_start_time": self.begin_time,
        }

        writer = output.JsonOutputWriter()
        writer.result_recovery(recovery_info)
        writer.close()

        (out, err) = capsys.readouterr()
        json_output = json.loads(out)

        assert self.begin_epoch == json_output["recovery_start_time_timestamp"]

    def test_init_status(self, capsys):
        writer = output.JsonOutputWriter()

        server = "test"

        writer.init_status(server)
        writer.close()

        (out, err) = capsys.readouterr()
        json_output = json.loads(out)

        assert server in json_output
        assert err == ""

    def test_result_status(self, capsys):
        writer = output.JsonOutputWriter()

        server = "test"
        name = "test_name"
        description = "test description"
        message = "test message"

        writer.init_status(server)
        writer.result_status(server, name, description, message)
        writer.close()

        (out, err) = capsys.readouterr()
        json_output = json.loads(out)
        assert (
            dict(description=description, message=message) == json_output[server][name]
        )
        assert err == ""

    def test_result_status_non_str(self, capsys):
        writer = output.JsonOutputWriter()

        server = "test"
        name = "test_name"
        description = "test description"
        message = 1

        writer.init_status(server)
        writer.result_status(server, name, description, message)
        writer.close()

        (out, err) = capsys.readouterr()
        json_output = json.loads(out)

        assert (
            dict(description=description, message=str(message))
            == json_output[server][name]
        )
        assert err == ""


# noinspection PyMethodMayBeStatic
class TestNagiosWriter(object):
    def test_debug(self, capsys):
        writer = output.NagiosOutputWriter()

        msg = "test message"
        writer.debug(msg)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test arg %s"
        args = ("1st",)
        writer.debug(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test args %d %s"
        args = (1, "two")
        writer.debug(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.debug(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

    def test_debug_disabled(self, capsys):
        writer = output.NagiosOutputWriter(debug=False)

        msg = "test message"
        writer.debug(msg)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test arg %s"
        args = ("1st",)
        writer.debug(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test args %d %s"
        args = (1, "two")
        writer.debug(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.debug(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

    def test_info(self, capsys):
        writer = output.NagiosOutputWriter()

        msg = "test message"
        writer.info(msg)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test arg %s"
        args = ("1st",)
        writer.info(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test args %d %s"
        args = (1, "two")
        writer.info(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.info(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

    def test_warning(self, capsys):
        writer = output.NagiosOutputWriter()

        msg = "test message"
        writer.warning(msg)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test arg %s"
        args = ("1st",)
        writer.warning(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test args %d %s"
        args = (1, "two")
        writer.warning(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.warning(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

    def test_error(self, capsys):
        writer = output.NagiosOutputWriter()

        msg = "test message"
        writer.error(msg)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test arg %s"
        args = ("1st",)
        writer.error(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test args %d %s"
        args = (1, "two")
        writer.error(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.error(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

    def test_exception(self, capsys):
        writer = output.NagiosOutputWriter()

        msg = "test message"
        writer.exception(msg)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test arg %s"
        args = ("1st",)
        writer.exception(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test args %d %s"
        args = (1, "two")
        writer.exception(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

        msg = "test kwargs %(num)d %(string)s"
        kwargs = dict(num=1, string="two")
        writer.exception(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ""
        assert err == ""

    def test_no_server_result_check(self, capsys):
        writer = output.NagiosOutputWriter()
        output.error_occurred = False

        writer.close()
        (out, err) = capsys.readouterr()
        assert out == "BARMAN OK - No server configured\n"
        assert err == ""
        assert not output.error_occurred

    def test_single_result_check(self, capsys):
        writer = output.NagiosOutputWriter()
        output.error_occurred = False

        # one server with no error
        writer.result_check("a", "test", True, None)

        writer.close()
        (out, err) = capsys.readouterr()
        assert out == "BARMAN OK - Ready to serve the Espresso backup for a\n"
        assert err == ""
        assert not output.error_occurred

    def test_result_check(self, capsys):
        writer = output.NagiosOutputWriter()
        output.error_occurred = False

        # three server with no error
        writer.result_check("a", "test", True, None)
        writer.result_check("b", "test", True, None)
        writer.result_check("c", "test", True, None)
        writer.result_check("c", "backup minimum size", True, 789, perfdata=789)

        writer.close()
        (out, err) = capsys.readouterr()
        assert (
            out == "BARMAN OK - Ready to serve the Espresso backup "
            "for 3 servers * a * b * c|c=789B\n"
        )
        assert err == ""
        assert not output.error_occurred

    def test_result_check_single_ignore(self, capsys):
        writer = output.NagiosOutputWriter()
        output.error_occurred = False

        # three server with no error
        writer.result_check("a", "test", True, None)
        writer.active = False
        writer.result_check("b", "test", False, None)
        writer.result_check("c", "test", False, None)

        writer.close()
        (out, err) = capsys.readouterr()
        assert (
            out == "BARMAN OK - Ready to serve the Espresso backup "
            "for a * IGNORING: b * IGNORING: c\n"
        )
        assert err == ""
        assert not output.error_occurred

    def test_result_check_multiple_ignore(self, capsys):
        writer = output.NagiosOutputWriter()
        output.error_occurred = False

        # three server with no error
        writer.result_check("a", "test", True, None)
        writer.result_check("b", "test", True, None)
        writer.active = False
        writer.result_check("c", "test", False, None)

        writer.close()
        (out, err) = capsys.readouterr()
        assert (
            out == "BARMAN OK - Ready to serve the Espresso backup "
            "for 2 servers * a * b * IGNORING: c\n"
        )
        assert err == ""
        assert not output.error_occurred

    def test_result_check_all_ignore(self, capsys):
        writer = output.NagiosOutputWriter()
        output.error_occurred = False

        # three server with no error
        writer.active = False
        writer.result_check("a", "test", False, None)
        writer.result_check("b", "test", False, None)
        writer.result_check("c", "test", False, None)

        writer.close()
        (out, err) = capsys.readouterr()
        assert (
            out == "BARMAN OK - No server configured "
            "* IGNORING: a * IGNORING: b * IGNORING: c\n"
        )
        assert err == ""
        assert not output.error_occurred

    def test_single_result_check_error(self, capsys):
        writer = output.NagiosOutputWriter()
        output.error_occurred = False

        # one server with one error
        writer.result_check("a", "test", False, None)

        writer.close()
        (out, err) = capsys.readouterr()
        assert (
            out == "BARMAN CRITICAL - server a has issues * "
            "a FAILED: test\na.test: FAILED\n"
        )
        assert err == ""
        assert output.error_occurred
        assert output.error_exit_code == 2

    def test_result_check_error(self, capsys):
        writer = output.NagiosOutputWriter()
        output.error_occurred = False

        # three server with one error
        writer.result_check("a", "test", True, None)
        writer.result_check("b", "test", False, "hint")
        writer.result_check("c", "test", True, None)
        writer.result_check("c", "wal size", True, 789, perfdata=789)

        writer.close()
        (out, err) = capsys.readouterr()
        assert (
            out == "BARMAN CRITICAL - 1 server out of 3 have issues * "
            "b FAILED: test|c_wals=789B\nb.test: FAILED (hint)\n"
        )
        assert err == ""
        assert output.error_occurred
        assert output.error_exit_code == 2
