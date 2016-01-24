# Copyright (C) 2011-2016 2ndQuadrant Italia Srl
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

import decimal
import json
import logging
from datetime import datetime, timedelta

import mock

import barman.utils


# noinspection PyMethodMayBeStatic
class TestDropPrivileges(object):
    def mock_pwd_entry(self, user, home, uid, gid):
        pwd_entry = mock.MagicMock(name='pwd_entry_%s' % uid)
        pwd_entry.pw_name = user
        pwd_entry.pw_dir = home
        pwd_entry.pw_uid = uid
        pwd_entry.pw_gid = gid
        return pwd_entry

    def mock_grp_entry(self, gid, members):
        grp_entry = mock.MagicMock(name='grp_entry_%s' % gid)
        grp_entry.gr_gid = gid
        grp_entry.gr_mem = members
        return grp_entry

    @mock.patch('barman.utils.grp')
    @mock.patch('barman.utils.pwd')
    @mock.patch('barman.utils.os')
    def test_change_user(self, os, pwd, grp):
        current_uid = 100

        user = 'tester'
        home = '/test/dir'
        uid = 101
        gid = 201
        groups = {110: False, 200: True, 250: False, 300: True, 400: True}

        # configure os
        os.getuid.return_value = current_uid
        os.environ = {'HOME': '/current/home'}

        # configure pwd
        pw = self.mock_pwd_entry(user, home, uid, gid)
        pwd.getpwnam.return_value = pw

        # configure group
        group_list = []
        for _id in groups:
            group_list.append(
                self.mock_grp_entry(_id, [user] if groups[_id] else [])
            )
        grp.getgrall.return_value = group_list

        barman.utils.drop_privileges(user)

        os.setgid.assert_called_with(gid)
        os.setuid.assert_called_with(uid)
        os.setgroups.assert_called_with(
            [_id for _id in groups if groups[_id]] + [gid])
        assert os.environ['HOME'] == home

    @mock.patch('barman.utils.grp')
    @mock.patch('barman.utils.pwd')
    @mock.patch('barman.utils.os')
    def test_same_user(self, os, pwd, grp):
        current_uid = 101

        user = 'tester'
        home = '/test/dir'
        uid = 101
        gid = 201
        groups = {110: False, 200: True, 250: False, 300: True, 400: True}

        # configure os
        os.getuid.return_value = current_uid

        # configure pwd
        pw = self.mock_pwd_entry(user, home, uid, gid)
        pwd.getpwnam.return_value = pw

        # configure group
        group_list = []
        for _id in groups:
            group_list.append(
                self.mock_grp_entry(_id, [user] if groups[_id] else [])
            )
        grp.getgrall.return_value = group_list

        barman.utils.drop_privileges(user)

        assert not os.setgid.called
        assert not os.setuid.called
        assert not os.setegid.called
        assert not os.seteuid.called
        assert not os.setgroups.called
        assert not os.environ.__setitem__.called


# noinspection PyMethodMayBeStatic
class TestParseLogLevel(object):
    def test_int_to_int(self):
        assert barman.utils.parse_log_level(1) == 1

    def test_str_to_int(self):
        assert barman.utils.parse_log_level('1') == 1

    def test_symbolic_to_int(self):
        assert barman.utils.parse_log_level('INFO') == 20

    def test_symbolic_case_to_int(self):
        assert barman.utils.parse_log_level('INFO') == 20

    def test_unknown(self):
        assert barman.utils.parse_log_level('unknown') is None


# noinspection PyMethodMayBeStatic
@mock.patch('barman.utils.os')
class TestMkpath(object):
    def test_path_exists(self, mock_os):
        mock_os.path.isdir.return_value = True
        test_path = '/path/to/create'
        barman.utils.mkpath(test_path)
        assert mock_os.makedirs.called is False

    def test_path_not_exists(self, mock_os):
        mock_os.path.isdir.return_value = False
        test_path = '/path/to/create'
        barman.utils.mkpath(test_path)
        mock_os.makedirs.assert_called_with(test_path)

    def test_path_error(self, mock_os):
        mock_os.path.isdir.return_value = False
        mock_os.makedirs.side_effect = OSError()
        test_path = '/path/to/create'
        try:
            barman.utils.mkpath(test_path)
        except OSError:
            pass
        else:  # pragma: no cover
            self.fail('Missing exception OSError')
        mock_os.makedirs.assert_called_with(test_path)


# noinspection PyMethodMayBeStatic,PyUnresolvedReferences
@mock.patch.multiple('barman.utils', logging=mock.DEFAULT, mkpath=mock.DEFAULT,
                     _logger=mock.DEFAULT)
class TestConfigureLogging(object):
    def test_simple_call(self, **mocks):
        barman.utils.configure_logging(None)

        # no file -> no calls to mkpath()
        assert mocks['mkpath'].called == 0

        # check if root has an handler and a level
        logging_mock = mocks['logging']
        logging_mock.root.setLevel.assert_called_with(logging.INFO)
        logging_mock.root.addHandler.assert_called_with(mock.ANY)

        # check if the handler has a formatter
        handler_mock = logging_mock.root.addHandler.call_args[0][0]
        handler_mock.setFormatter.assert_called_with(mock.ANY)

    def test_file_call(self, **mocks):
        test_file = '/test/log/file.log'
        barman.utils.configure_logging(log_file=test_file)

        mocks['mkpath'].assert_called_with('/test/log')

        # check if root has an handler and a level
        logging_mock = mocks['logging']
        logging_mock.root.setLevel.assert_called_with(logging.INFO)
        logging_mock.root.addHandler.assert_called_with(mock.ANY)

        # check if the handler has a formatter
        handler_mock = logging_mock.root.addHandler.call_args[0][0]
        handler_mock.setFormatter.assert_called_with(mock.ANY)

    def test_file_level_call(self, **mocks):
        test_file = '/test/log/file.log'
        test_level = logging.DEBUG
        barman.utils.configure_logging(log_file=test_file,
                                       log_level=test_level)

        mocks['mkpath'].assert_called_with('/test/log')

        # check if root has an handler and a level
        logging_mock = mocks['logging']
        logging_mock.root.setLevel.assert_called_with(test_level)
        logging_mock.root.addHandler.assert_called_with(mock.ANY)

    def test_file_format_call(self, **mocks):
        test_file = '/test/log/file.log'
        test_format = 'log_format'
        barman.utils.configure_logging(log_file=test_file,
                                       log_format=test_format)

        mocks['mkpath'].assert_called_with('/test/log')

        # check if root has an handler and a level
        logging_mock = mocks['logging']
        logging_mock.root.setLevel.assert_called_with(logging.INFO)
        logging_mock.root.addHandler.assert_called_with(mock.ANY)

        # check if the handler has a formatter
        handler_mock = logging_mock.root.addHandler.call_args[0][0]
        handler_mock.setFormatter.assert_called_with(mock.ANY)

        # check if the formatter has the given format
        formatter_mock = handler_mock.setFormatter.return_value
        formatter_mock.asset_called_with(test_format)

    def test_file_error_mkdir(self, **mocks):
        test_file = '/test/log/file.log'

        # raise an error, missing directory
        mocks['mkpath'].side_effect = OSError()

        barman.utils.configure_logging(log_file=test_file)

        mocks['mkpath'].assert_called_with('/test/log')

        # check if root has an handler and a level
        logging_mock = mocks['logging']
        logging_mock.root.setLevel.assert_called_with(logging.INFO)
        logging_mock.root.addHandler.assert_called_with(mock.ANY)

        # check if the handler has a formatter
        handler_mock = logging_mock.root.addHandler.call_args[0][0]
        handler_mock.setFormatter.assert_called_with(mock.ANY)

        # check if a warning has been raised
        mocks['_logger'].warn.assert_called_with(mock.ANY)

    def test_file_error_file(self, **mocks):
        test_file = '/test/log/file.log'

        # raise an error opening the file
        logging_mock = mocks['logging']
        logging_mock.handlers.WatchedFileHandler.side_effect = IOError()

        barman.utils.configure_logging(log_file=test_file)

        mocks['mkpath'].assert_called_with('/test/log')

        # check if root has an handler and a level
        logging_mock.root.setLevel.assert_called_with(logging.INFO)
        logging_mock.root.addHandler.assert_called_with(mock.ANY)

        # check if the handler has a formatter
        handler_mock = logging_mock.root.addHandler.call_args[0][0]
        handler_mock.setFormatter.assert_called_with(mock.ANY)

        # check if a warning has been raised
        mocks['_logger'].warn.assert_called_with(mock.ANY)


# noinspection PyMethodMayBeStatic
class TestPrettySize(object):

    def test_1000(self):
        val = 10
        base = 1000
        assert barman.utils.pretty_size(val, base) == '10 B'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10.0 kB'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10.0 MB'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10.0 GB'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10.0 TB'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10.0 PB'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10.0 EB'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10.0 ZB'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10.0 YB'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10000.0 YB'

    def test_1024(self):
        val = 10
        base = 1024
        assert barman.utils.pretty_size(val, base) == '10 B'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10.0 KiB'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10.0 MiB'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10.0 GiB'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10.0 TiB'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10.0 PiB'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10.0 EiB'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10.0 ZiB'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10.0 YiB'
        val *= base
        assert barman.utils.pretty_size(val, base) == '10240.0 YiB'

    def test_float(self):

        assert barman.utils.pretty_size(1234, 1000) == \
            barman.utils.pretty_size(1234.0, 1000)
        assert barman.utils.pretty_size(1234, 1024) == \
            barman.utils.pretty_size(1234.0, 1024)


# noinspection PyMethodMayBeStatic
class TestHumanReadableDelta(object):
    """
    Test class for the utility method human_readable_timedelta.
    """
    def test_one_day(self):
        """
        Test output for a 1 day timedelta.
        """
        td = timedelta(days=1)
        assert barman.utils.human_readable_timedelta(td) == '1 day'

    def test_two_days(self):
        """
        Test output for a 2 days timedelta.
        """
        td = timedelta(days=2)
        assert barman.utils.human_readable_timedelta(td) == '2 days'

    def test_one_hour(self):
        """
        Test output for a 1 hour timedelta.
        """
        td = timedelta(seconds=3600)
        assert barman.utils.human_readable_timedelta(td) == '1 hour'

    def test_two_hours(self):
        """
        Test output for a 2 hours timedelta.
        """
        td = timedelta(seconds=7200)
        assert barman.utils.human_readable_timedelta(td) == '2 hours'

    def test_one_minute(self):
        """
        Test output for a 1 minute timedelta.
        """
        td = timedelta(seconds=60)
        assert barman.utils.human_readable_timedelta(td) == '1 minute'

    def test_two_minutes(self):
        """
        Test output for a 2 minutes timedelta.
        """
        td = timedelta(seconds=120)
        assert barman.utils.human_readable_timedelta(td) == '2 minutes'

    def test_one_hour_two_mins(self):
        """
        Test output for a 1 hour, 2 minutes timedelta.
        """
        td = timedelta(seconds=3720)
        assert barman.utils.human_readable_timedelta(td) == '1 hour, 2 minutes'

    def test_one_day_three_hour_two_mins(self):
        """
        Test output for a 1 day, 3 hour, 2 minutes timedelta.
        """
        td = timedelta(days=1, seconds=10920)
        assert barman.utils.human_readable_timedelta(td) == '1 day, ' \
                                                            '3 hours, ' \
                                                            '2 minutes'

    def test_180_days_three_hour_4_mins(self):
        """
        Test output for 180 days, 3 hours, 4 minutes.
        """
        td = timedelta(days=180, seconds=11040)
        assert barman.utils.human_readable_timedelta(td) == '180 days, ' \
                                                            '3 hours, ' \
                                                            '4 minutes'

    def test_seven_days(self):
        """
        Test output for a 1 week timedelta.
        """
        td = timedelta(weeks=1)
        assert barman.utils.human_readable_timedelta(td) == '7 days'


# noinspection PyMethodMayBeStatic
class TestBarmanEncoder(object):
    """
    Test BarmanEncoder object
    """
    def test_complex_objects(self):
        """
        Test the BarmanEncoder on special objects
        """
        # Test encoding with an object that provides a to_json() method
        to_json_mock = mock.Mock(name="to_json_mock")
        to_json_mock.to_json.return_value = "json_value"
        assert json.dumps(
            to_json_mock, cls=barman.utils.BarmanEncoder) == '"json_value"'

        # Test encoding with an object that provides a ctime() method
        assert json.dumps(
            datetime(2015, 1, 10, 12, 34, 56),
            cls=barman.utils.BarmanEncoder) == '"Sat Jan 10 12:34:56 2015"'

        # Test encoding with a timedelta object
        assert json.dumps(
            timedelta(days=35, seconds=12345),
            cls=barman.utils.BarmanEncoder) == '"35 days, 3 hours, 25 minutes"'

        # Test encoding with a Decimal object
        num = decimal.Decimal("123456789.9876543210")
        assert json.dumps(
            num, cls=barman.utils.BarmanEncoder) == repr(float(num))

        # Test encoding with a raw string object (simulated)
        string_value = mock.Mock(name="string_value", wraps="string_value")
        string_value.attach_mock(mock.Mock(), "decode")
        string_value.decode.return_value = "decoded_value"
        assert json.dumps(
            string_value, cls=barman.utils.BarmanEncoder) == '"decoded_value"'
        string_value.decode.assert_called_once_with('utf-8', 'replace')

    def test_simple_objects(self):
        """
        Test the BarmanEncoder on simple objects
        """
        assert json.dumps(
            [{"a": 1}, "test"],
            cls=barman.utils.BarmanEncoder) == '[{"a": 1}, "test"]'
