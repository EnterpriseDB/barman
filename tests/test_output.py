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

import mock
import pytest

from barman import output
from barman.infofile import BackupInfo
from barman.utils import pretty_size
from testing_helpers import build_test_backup_info, mock_backup_ext_info


def teardown_module(module):
    """
    Set the output API to a functional state, after testing it
    """
    output.set_output_writer(output.DEFAULT_WRITER)


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

        args = ('1', 'two')
        kwargs = dict(three=3, four=5)
        output.set_output_writer('mock', *args, **kwargs)

        old_writer.close.assert_called_once_with()
        output.AVAILABLE_WRITERS['mock'].assert_called_once_with(*args,
                                                                 **kwargs)

    def test_debug(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = 'test message'
        output.debug(msg)

        # logging test
        for record in caplog.records:
            assert record.levelname == 'DEBUG'
            assert record.name == __name__
        assert msg in caplog.text

        # writer test
        assert not writer.error_occurred.called
        writer.debug.assert_called_once_with(msg)

        # global status test
        assert not output.error_occurred

    def test_debug_with_args(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = 'test format %02d %s'
        args = (1, '2nd')
        output.debug(msg, *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == 'DEBUG'
            assert record.name == __name__
        assert msg % args in caplog.text

        # writer test
        assert not writer.error_occurred.called
        writer.debug.assert_called_once_with(msg, *args)

        # global status test
        assert not output.error_occurred

    def test_debug_error(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = 'test message'
        output.debug(msg, is_error=True)

        # logging test
        for record in caplog.records:
            assert record.levelname == 'DEBUG'
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
            output.debug('message', bad_arg=True)

    def test_info(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = 'test message'
        output.info(msg)

        # logging test
        for record in caplog.records:
            assert record.levelname == 'INFO'
            assert record.name == __name__
        assert msg in caplog.text

        # writer test
        assert not writer.error_occurred.called
        writer.info.assert_called_once_with(msg)

        # global status test
        assert not output.error_occurred

    def test_info_with_args(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = 'test format %02d %s'
        args = (1, '2nd')
        output.info(msg, *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == 'INFO'
            assert record.name == __name__
        assert msg % args in caplog.text

        # writer test
        assert not writer.error_occurred.called
        writer.info.assert_called_once_with(msg, *args)

        # global status test
        assert not output.error_occurred

    def test_info_error(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = 'test message'
        output.info(msg, is_error=True)

        # logging test
        for record in caplog.records:
            assert record.levelname == 'INFO'
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

        msg = 'test message'
        output.warning(msg)

        # logging test
        for record in caplog.records:
            assert record.levelname == 'WARNING'
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

        msg = 'test format %02d %s'
        args = (1, '2nd')
        output.warning(msg, *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == 'WARNING'
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

        msg = 'test message'
        output.warning(msg, is_error=True)

        # logging test
        for record in caplog.records:
            assert record.levelname == 'WARNING'
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

        msg = 'test message'
        output.error(msg)

        # logging test
        for record in caplog.records:
            assert record.levelname == 'ERROR'
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

        msg = 'test format %02d %s'
        args = (1, '2nd')
        output.error(msg, *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == 'ERROR'
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

        msg = 'test format %02d %s'
        args = (1, '2nd')
        output.error(msg, ignore=True, *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == 'ERROR'
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

        msg = 'test message'
        try:
            raise ValueError('test exception')
        except ValueError:
            output.exception(msg)

        # logging test
        for record in caplog.records:
            assert record.levelname == 'ERROR'
            assert record.name == __name__
        assert msg in caplog.text
        assert 'Traceback' in caplog.text

        # writer test
        writer.error_occurred.assert_called_once_with()
        writer.exception.assert_called_once_with(msg)

        # global status test
        assert output.error_occurred

    def test_exception_with_args(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = 'test format %02d %s'
        args = (1, '2nd')
        try:
            raise ValueError('test exception')
        except ValueError:
            output.exception(msg, *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == 'ERROR'
            assert record.name == __name__
        assert msg % args in caplog.text
        assert 'Traceback' in caplog.text

        # writer test
        writer.error_occurred.assert_called_once_with()
        writer.exception.assert_called_once_with(msg, *args)

        # global status test
        assert output.error_occurred

    def test_exception_with_ignore(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = 'test format %02d %s'
        args = (1, '2nd')
        try:
            raise ValueError('test exception')
        except ValueError:
            output.exception(msg, ignore=True, *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == 'ERROR'
            assert record.name == __name__
        assert msg % args in caplog.text
        assert 'Traceback' in caplog.text

        # writer test
        assert not writer.error_occurred.called
        writer.exception.assert_called_once_with(msg, *args)

        # global status test
        assert not output.error_occurred

    def test_exception_with_raise(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = 'test format %02d %s'
        args = (1, '2nd')

        try:
            raise ValueError('test exception')
        except ValueError:
            with pytest.raises(ValueError):
                output.exception(msg, raise_exception=True, *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == 'ERROR'
            assert record.name == __name__
        assert msg % args in caplog.text
        assert 'Traceback' in caplog.text

        # writer test
        writer.error_occurred.assert_called_once_with()
        writer.exception.assert_called_once_with(msg, *args)

        # global status test
        assert output.error_occurred

    def test_exception_with_raise_object(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = 'test format %02d %s'
        args = (1, '2nd')

        try:
            raise ValueError('test exception')
        except ValueError:
            with pytest.raises(KeyError):
                output.exception(msg, raise_exception=KeyError(), *args)

        # logging test
        for record in caplog.records:
            assert record.levelname == 'ERROR'
            assert record.name == __name__
        assert msg % args in caplog.text
        assert 'Traceback' in caplog.text

        # writer test
        writer.error_occurred.assert_called_once_with()
        writer.exception.assert_called_once_with(msg, *args)

        # global status test
        assert output.error_occurred

    def test_exception_with_raise_class(self, caplog):
        # preparation
        writer = self._mock_writer()

        msg = 'test format %02d %s'
        args = (1, '2nd')

        try:
            raise ValueError('test exception')
        except ValueError:
            with pytest.raises(KeyError):
                output.exception(msg, raise_exception=KeyError, *args)
        assert msg % args in caplog.text
        assert 'Traceback' in caplog.text

        # logging test
        for record in caplog.records:
            assert record.levelname == 'ERROR'
            assert record.name == __name__

        # writer test
        writer.error_occurred.assert_called_once_with()
        writer.exception.assert_called_once_with(msg, *args)

        # global status test
        assert output.error_occurred

    def test_init(self):
        # preparation
        writer = self._mock_writer()

        args = ('1', 'two')
        kwargs = dict(three=3, four=5)
        output.init('command', *args, **kwargs)
        output.init('another_command')

        # writer test
        writer.init_command.assert_called_once_with(*args, **kwargs)
        writer.init_another_command.assert_called_once_with()

    @mock.patch('sys.exit')
    def test_init_bad_command(self, exit_mock, caplog):
        # preparation
        writer = self._mock_writer()
        del writer.init_bad_command

        output.init('bad_command')

        # logging test
        for record in caplog.records:
            assert record.levelname == 'ERROR'
        assert 'bad_command' in caplog.text
        assert 'Traceback' in caplog.text

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

        args = ('1', 'two')
        kwargs = dict(three=3, four=5)
        output.result('command', *args, **kwargs)
        output.result('another_command')

        # writer test
        writer.result_command.assert_called_once_with(*args, **kwargs)
        writer.result_another_command.assert_called_once_with()

    @mock.patch('sys.exit')
    def test_result_bad_command(self, exit_mock, caplog):
        # preparation
        writer = self._mock_writer()
        del writer.result_bad_command

        output.result('bad_command')

        # logging test
        for record in caplog.records:
            assert record.levelname == 'ERROR'
        assert 'bad_command' in caplog.text
        assert 'Traceback' in caplog.text

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

    @mock.patch('sys.exit')
    def test_close_and_exit(self, exit_mock):
        # preparation
        writer = self._mock_writer()

        output.close_and_exit()

        writer.close.assert_called_once_with()
        exit_mock.assert_called_once_with(0)

    @mock.patch('sys.exit')
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

    def test_debug(self, capsys):
        writer = output.ConsoleOutputWriter(debug=True)

        msg = 'test message'
        writer.debug(msg)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == 'DEBUG: ' + msg + '\n'

        msg = 'test arg %s'
        args = ('1st',)
        writer.debug(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == 'DEBUG: ' + msg % args + '\n'

        msg = 'test args %d %s'
        args = (1, 'two')
        writer.debug(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == 'DEBUG: ' + msg % args + '\n'

        msg = 'test kwargs %(num)d %(string)s'
        kwargs = dict(num=1, string='two')
        writer.debug(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == 'DEBUG: ' + msg % kwargs + '\n'

    def test_debug_disabled(self, capsys):
        writer = output.ConsoleOutputWriter(debug=False)

        msg = 'test message'
        writer.debug(msg)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test arg %s'
        args = ('1st',)
        writer.debug(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test args %d %s'
        args = (1, 'two')
        writer.debug(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test kwargs %(num)d %(string)s'
        kwargs = dict(num=1, string='two')
        writer.debug(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

    def test_info_verbose(self, capsys):
        writer = output.ConsoleOutputWriter(quiet=False)

        msg = 'test message'
        writer.info(msg)
        (out, err) = capsys.readouterr()
        assert out == msg + '\n'
        assert err == ''

        msg = 'test arg %s'
        args = ('1st',)
        writer.info(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == msg % args + '\n'
        assert err == ''

        msg = 'test args %d %s'
        args = (1, 'two')
        writer.info(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == msg % args + '\n'
        assert err == ''

        msg = 'test kwargs %(num)d %(string)s'
        kwargs = dict(num=1, string='two')
        writer.info(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == msg % kwargs + '\n'
        assert err == ''

    def test_info_quiet(self, capsys):
        writer = output.ConsoleOutputWriter(quiet=True)

        msg = 'test message'
        writer.info(msg)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test arg %s'
        args = ('1st',)
        writer.info(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test args %d %s'
        args = (1, 'two')
        writer.info(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test kwargs %(num)d %(string)s'
        kwargs = dict(num=1, string='two')
        writer.info(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

    def test_warning(self, capsys):
        writer = output.ConsoleOutputWriter()

        msg = 'test message'
        writer.warning(msg)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == 'WARNING: ' + msg + '\n'

        msg = 'test arg %s'
        args = ('1st',)
        writer.warning(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == 'WARNING: ' + msg % args + '\n'

        msg = 'test args %d %s'
        args = (1, 'two')
        writer.warning(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == 'WARNING: ' + msg % args + '\n'

        msg = 'test kwargs %(num)d %(string)s'
        kwargs = dict(num=1, string='two')
        writer.warning(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == 'WARNING: ' + msg % kwargs + '\n'

    def test_error(self, capsys):
        writer = output.ConsoleOutputWriter()

        msg = 'test message'
        writer.error(msg)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == 'ERROR: ' + msg + '\n'

        msg = 'test arg %s'
        args = ('1st',)
        writer.error(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == 'ERROR: ' + msg % args + '\n'

        msg = 'test args %d %s'
        args = (1, 'two')
        writer.error(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == 'ERROR: ' + msg % args + '\n'

        msg = 'test kwargs %(num)d %(string)s'
        kwargs = dict(num=1, string='two')
        writer.error(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == 'ERROR: ' + msg % kwargs + '\n'

    def test_exception(self, capsys):
        writer = output.ConsoleOutputWriter()

        msg = 'test message'
        writer.exception(msg)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == 'EXCEPTION: ' + msg + '\n'

        msg = 'test arg %s'
        args = ('1st',)
        writer.exception(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == 'EXCEPTION: ' + msg % args + '\n'

        msg = 'test args %d %s'
        args = (1, 'two')
        writer.exception(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == 'EXCEPTION: ' + msg % args + '\n'

        msg = 'test kwargs %(num)d %(string)s'
        kwargs = dict(num=1, string='two')
        writer.exception(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == 'EXCEPTION: ' + msg % kwargs + '\n'

    def test_init_check(self, capsys):
        writer = output.ConsoleOutputWriter()

        server = 'test'

        writer.init_check(server, True)
        (out, err) = capsys.readouterr()
        assert out == 'Server %s:\n' % server
        assert err == ''

    def test_result_check_ok(self, capsys):
        writer = output.ConsoleOutputWriter()
        output.error_occurred = False

        server = 'test'
        check = 'test check'

        writer.result_check(server, check, True)
        (out, err) = capsys.readouterr()
        assert out == '\t%s: OK\n' % check
        assert err == ''
        assert not output.error_occurred

    def test_result_check_ok_hint(self, capsys):
        writer = output.ConsoleOutputWriter()
        output.error_occurred = False

        server = 'test'
        check = 'test check'
        hint = 'do something'

        writer.result_check(server, check, True, hint)
        (out, err) = capsys.readouterr()
        assert out == '\t%s: OK (%s)\n' % (check, hint)
        assert err == ''
        assert not output.error_occurred

    def test_result_check_failed(self, capsys):
        writer = output.ConsoleOutputWriter()
        output.error_occurred = False

        server = 'test'
        check = 'test check'

        writer.result_check(server, check, False)
        (out, err) = capsys.readouterr()
        assert out == '\t%s: FAILED\n' % check
        assert err == ''
        assert output.error_occurred

        # Test an inactive server
        # Shows error, but does not change error_occurred
        output.error_occurred = False
        writer.init_check(server, False)
        (out, err) = capsys.readouterr()
        assert out == 'Server %s:\n' % server
        assert err == ''
        assert not output.error_occurred

        writer.result_check(server, check, False)
        (out, err) = capsys.readouterr()
        assert out == '\t%s: FAILED\n' % check
        assert err == ''
        assert not output.error_occurred

    def test_result_check_failed_hint(self, capsys):
        writer = output.ConsoleOutputWriter()
        output.error_occurred = False

        server = 'test'
        check = 'test check'
        hint = 'do something'

        writer.result_check(server, check, False, hint)
        (out, err) = capsys.readouterr()
        assert out == '\t%s: FAILED (%s)\n' % (check, hint)
        assert err == ''
        assert output.error_occurred

    def test_init_list_backup(self):
        writer = output.ConsoleOutputWriter()

        writer.init_list_backup('test server')
        assert not writer.minimal

        writer.init_list_backup('test server', True)
        assert writer.minimal

    def test_result_list_backup(self, capsys):
        # mock the backup info
        bi = build_test_backup_info()
        backup_size = 12345
        wal_size = 54321
        retention_status = 'test status'

        writer = output.ConsoleOutputWriter()

        # test minimal
        writer.init_list_backup(bi.server_name, True)
        writer.result_list_backup(bi, backup_size, wal_size, retention_status)
        writer.close()
        (out, err) = capsys.readouterr()
        assert writer.minimal
        assert bi.backup_id in out
        assert err == ''

        # test status=DONE output
        writer.init_list_backup(bi.server_name, False)
        writer.result_list_backup(bi, backup_size, wal_size, retention_status)
        writer.close()
        (out, err) = capsys.readouterr()
        assert not writer.minimal
        assert bi.server_name in out
        assert bi.backup_id in out
        assert str(bi.end_time.ctime()) in out
        for name, _, location in bi.tablespaces:
            assert '%s:%s' % (name, location)
        assert 'Size: ' + pretty_size(backup_size) in out
        assert 'WAL Size: ' + pretty_size(wal_size) in out
        assert err == ''

        # test status = FAILED output
        bi = build_test_backup_info(status=BackupInfo.FAILED)
        writer.init_list_backup(bi.server_name, False)
        writer.result_list_backup(bi, backup_size, wal_size, retention_status)
        writer.close()
        (out, err) = capsys.readouterr()
        assert not writer.minimal
        assert bi.server_name in out
        assert bi.backup_id in out
        assert bi.status in out

    def test_result_show_backup(self, capsys):
        # mock the backup ext info
        wal_per_second = 0.01
        ext_info = mock_backup_ext_info(wals_per_second=wal_per_second)

        writer = output.ConsoleOutputWriter()

        # test minimal
        writer.result_show_backup(ext_info)
        writer.close()
        (out, err) = capsys.readouterr()
        assert ext_info['server_name'] in out
        assert ext_info['backup_id'] in out
        assert ext_info['status'] in out
        assert str(ext_info['end_time']) in out
        for name, _, location in ext_info['tablespaces']:
            assert '%s: %s' % (name, location) in out
        assert (pretty_size(ext_info['size'] + ext_info['wal_size'])) in out
        assert (pretty_size(ext_info['wal_until_next_size'])) in out
        assert 'WAL rate             : %0.2f/hour' % \
               (wal_per_second * 3600) in out
        # TODO: this test can be expanded
        assert err == ''

    def test_result_show_backup_error(self, capsys):
        # mock the backup ext info
        msg = 'test error message'
        ext_info = mock_backup_ext_info(status=BackupInfo.FAILED, error=msg)

        writer = output.ConsoleOutputWriter()

        # test minimal
        writer.result_show_backup(ext_info)
        writer.close()
        (out, err) = capsys.readouterr()
        assert ext_info['server_name'] in out
        assert ext_info['backup_id'] in out
        assert ext_info['status'] in out
        assert str(ext_info['end_time']) not in out
        assert msg in out
        assert err == ''

    def test_init_status(self, capsys):
        writer = output.ConsoleOutputWriter()

        server = 'test'

        writer.init_status(server)
        (out, err) = capsys.readouterr()
        assert out == 'Server %s:\n' % server
        assert err == ''

    def test_result_status(self, capsys):
        writer = output.ConsoleOutputWriter()

        server = 'test'
        name = 'test name'
        description = 'test description'
        message = 'test message'

        writer.result_status(server, name, description, message)
        (out, err) = capsys.readouterr()
        assert out == '\t%s: %s\n' % (description, message)
        assert err == ''

    def test_result_status_non_str(self, capsys):
        writer = output.ConsoleOutputWriter()

        server = 'test'
        name = 'test name'
        description = 'test description'
        message = 1

        writer.result_status(server, name, description, message)
        (out, err) = capsys.readouterr()
        assert out == '\t%s: %s\n' % (description, message)
        assert err == ''


# noinspection PyMethodMayBeStatic
class TestNagiosWriter(object):

    def test_debug(self, capsys):
        writer = output.NagiosOutputWriter()

        msg = 'test message'
        writer.debug(msg)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test arg %s'
        args = ('1st',)
        writer.debug(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test args %d %s'
        args = (1, 'two')
        writer.debug(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test kwargs %(num)d %(string)s'
        kwargs = dict(num=1, string='two')
        writer.debug(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

    def test_debug_disabled(self, capsys):
        writer = output.NagiosOutputWriter(debug=False)

        msg = 'test message'
        writer.debug(msg)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test arg %s'
        args = ('1st',)
        writer.debug(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test args %d %s'
        args = (1, 'two')
        writer.debug(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test kwargs %(num)d %(string)s'
        kwargs = dict(num=1, string='two')
        writer.debug(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

    def test_info(self, capsys):
        writer = output.NagiosOutputWriter()

        msg = 'test message'
        writer.info(msg)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test arg %s'
        args = ('1st',)
        writer.info(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test args %d %s'
        args = (1, 'two')
        writer.info(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test kwargs %(num)d %(string)s'
        kwargs = dict(num=1, string='two')
        writer.info(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

    def test_warning(self, capsys):
        writer = output.NagiosOutputWriter()

        msg = 'test message'
        writer.warning(msg)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test arg %s'
        args = ('1st',)
        writer.warning(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test args %d %s'
        args = (1, 'two')
        writer.warning(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test kwargs %(num)d %(string)s'
        kwargs = dict(num=1, string='two')
        writer.warning(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

    def test_error(self, capsys):
        writer = output.NagiosOutputWriter()

        msg = 'test message'
        writer.error(msg)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test arg %s'
        args = ('1st',)
        writer.error(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test args %d %s'
        args = (1, 'two')
        writer.error(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test kwargs %(num)d %(string)s'
        kwargs = dict(num=1, string='two')
        writer.error(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

    def test_exception(self, capsys):
        writer = output.NagiosOutputWriter()

        msg = 'test message'
        writer.exception(msg)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test arg %s'
        args = ('1st',)
        writer.exception(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test args %d %s'
        args = (1, 'two')
        writer.exception(msg, *args)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

        msg = 'test kwargs %(num)d %(string)s'
        kwargs = dict(num=1, string='two')
        writer.exception(msg, kwargs)
        (out, err) = capsys.readouterr()
        assert out == ''
        assert err == ''

    def test_single_result_check(self, capsys):
        writer = output.NagiosOutputWriter()
        output.error_occurred = False

        # one server with no error
        writer.result_check('a', 'test', True, None)

        writer.close()
        (out, err) = capsys.readouterr()
        assert out == 'BARMAN OK - Ready to serve the Espresso backup ' \
                      'for a\n'
        assert err == ''
        assert not output.error_occurred

    def test_result_check(self, capsys):
        writer = output.NagiosOutputWriter()
        output.error_occurred = False

        # three server with no error
        writer.result_check('a', 'test', True, None)
        writer.result_check('b', 'test', True, None)
        writer.result_check('c', 'test', True, None)

        writer.close()
        (out, err) = capsys.readouterr()
        assert out == 'BARMAN OK - Ready to serve the Espresso backup ' \
                      'for 3 server(s) * a * b * c\n'
        assert err == ''
        assert not output.error_occurred

    def test_single_result_check_error(self, capsys):
        writer = output.NagiosOutputWriter()
        output.error_occurred = False

        # one server with one error
        writer.result_check('a', 'test', False, None)

        writer.close()
        (out, err) = capsys.readouterr()
        assert out == 'BARMAN CRITICAL - server a has issues * ' \
                      'a FAILED: test\na.test: FAILED\n'
        assert err == ''
        assert output.error_occurred
        assert output.error_exit_code == 2

    def test_result_check_error(self, capsys):
        writer = output.NagiosOutputWriter()
        output.error_occurred = False

        # three server with one error
        writer.result_check('a', 'test', True, None)
        writer.result_check('b', 'test', False, None)
        writer.result_check('c', 'test', True, None)

        writer.close()
        (out, err) = capsys.readouterr()
        assert out == 'BARMAN CRITICAL - 1 server out of 3 have issues * ' \
                      'b FAILED: test\nb.test: FAILED\n'
        assert err == ''
        assert output.error_occurred
        assert output.error_exit_code == 2
