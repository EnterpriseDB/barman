# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2011-2025
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
import os
import re
import signal
import sys
from argparse import ArgumentTypeError
from datetime import datetime, timedelta
from distutils.version import LooseVersion

import mock
import pytest
from dateutil import tz
from testing_helpers import (
    build_backup_manager,
    build_mocked_server,
    build_test_backup_info,
)

import barman.utils
from barman.infofile import BackupInfo, WalFileInfo, load_datetime_tz
from barman.lockfile import LockFile

LOGFILE_NAME = "logfile.log"


# noinspection PyMethodMayBeStatic
class TestDropPrivileges(object):
    def mock_pwd_entry(self, user, home, uid, gid):
        pwd_entry = mock.MagicMock(name="pwd_entry_%s" % uid)
        pwd_entry.pw_name = user
        pwd_entry.pw_dir = home
        pwd_entry.pw_uid = uid
        pwd_entry.pw_gid = gid
        return pwd_entry

    def mock_grp_entry(self, gid, members):
        grp_entry = mock.MagicMock(name="grp_entry_%s" % gid)
        grp_entry.gr_gid = gid
        grp_entry.gr_mem = members
        return grp_entry

    @mock.patch("barman.utils.grp")
    @mock.patch("barman.utils.pwd")
    @mock.patch("barman.utils.os")
    def test_change_user(self, os, pwd, grp):
        current_uid = 100

        user = "tester"
        home = "/test/dir"
        uid = 101
        gid = 201
        groups = {110: False, 200: True, 250: False, 300: True, 400: True}

        # configure os
        os.getuid.return_value = current_uid
        os.environ = {"HOME": "/current/home"}

        # configure pwd
        pw = self.mock_pwd_entry(user, home, uid, gid)
        pwd.getpwnam.return_value = pw

        # configure group
        group_list = []
        for _id in groups:
            group_list.append(self.mock_grp_entry(_id, [user] if groups[_id] else []))
        grp.getgrall.return_value = group_list

        barman.utils.drop_privileges(user)

        os.setgid.assert_called_with(gid)
        os.setuid.assert_called_with(uid)
        os.setgroups.assert_called_with([_id for _id in groups if groups[_id]] + [gid])
        assert os.environ["HOME"] == home

    @mock.patch("barman.utils.grp")
    @mock.patch("barman.utils.pwd")
    @mock.patch("barman.utils.os")
    def test_same_user(self, os, pwd, grp):
        current_uid = 101

        user = "tester"
        home = "/test/dir"
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
            group_list.append(self.mock_grp_entry(_id, [user] if groups[_id] else []))
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
        assert barman.utils.parse_log_level("1") == 1

    def test_symbolic_to_int(self):
        assert barman.utils.parse_log_level("INFO") == 20

    def test_symbolic_case_to_int(self):
        assert barman.utils.parse_log_level("INFO") == 20

    def test_unknown(self):
        assert barman.utils.parse_log_level("unknown") is None


# noinspection PyMethodMayBeStatic
class TestGetLogLevels(object):
    def test_get_log_levels(self):
        levels = list(barman.utils.get_log_levels())
        assert len(levels) > 0
        for level in levels:
            assert isinstance(level, barman.utils._string_types)


# noinspection PyMethodMayBeStatic
@mock.patch("barman.utils.os")
class TestMkpath(object):
    def test_path_exists(self, mock_os):
        mock_os.path.isdir.return_value = True
        test_path = "/path/to/create"
        barman.utils.mkpath(test_path)
        assert mock_os.makedirs.called is False

    def test_path_not_exists(self, mock_os):
        mock_os.path.isdir.return_value = False
        test_path = "/path/to/create"
        barman.utils.mkpath(test_path)
        mock_os.makedirs.assert_called_with(test_path)

    def test_path_error(self, mock_os):
        mock_os.path.isdir.return_value = False
        mock_os.makedirs.side_effect = OSError()
        test_path = "/path/to/create"
        try:
            barman.utils.mkpath(test_path)
        except OSError:
            pass
        else:  # pragma: no cover
            self.fail("Missing exception OSError")
        mock_os.makedirs.assert_called_with(test_path)


# noinspection PyMethodMayBeStatic,PyUnresolvedReferences
@mock.patch.multiple(
    "barman.utils", logging=mock.DEFAULT, mkpath=mock.DEFAULT, _logger=mock.DEFAULT
)
class TestConfigureLogging(object):
    def test_simple_call(self, **mocks):
        barman.utils.configure_logging(None)

        # no file -> no calls to mkpath()
        assert mocks["mkpath"].called == 0

        # check if root has an handler and a level
        logging_mock = mocks["logging"]
        logging_mock.root.setLevel.assert_called_with(logging.INFO)
        logging_mock.root.addHandler.assert_called_with(mock.ANY)

        # check if the handler has a formatter
        handler_mock = logging_mock.root.addHandler.call_args[0][0]
        handler_mock.setFormatter.assert_called_with(mock.ANY)

    def test_file_call(self, **mocks):
        test_file = "/test/log/file.log"
        barman.utils.configure_logging(log_file=test_file)

        mocks["mkpath"].assert_called_with("/test/log")

        # check if root has an handler and a level
        logging_mock = mocks["logging"]
        logging_mock.root.setLevel.assert_called_with(logging.INFO)
        logging_mock.root.addHandler.assert_called_with(mock.ANY)

        # check if the handler has a formatter
        handler_mock = logging_mock.root.addHandler.call_args[0][0]
        handler_mock.setFormatter.assert_called_with(mock.ANY)

    def test_file_level_call(self, **mocks):
        test_file = "/test/log/file.log"
        test_level = logging.DEBUG
        barman.utils.configure_logging(log_file=test_file, log_level=test_level)

        mocks["mkpath"].assert_called_with("/test/log")

        # check if root has an handler and a level
        logging_mock = mocks["logging"]
        logging_mock.root.setLevel.assert_called_with(test_level)
        logging_mock.root.addHandler.assert_called_with(mock.ANY)

    def test_file_format_call(self, **mocks):
        test_file = "/test/log/file.log"
        test_format = "log_format"
        barman.utils.configure_logging(log_file=test_file, log_format=test_format)

        mocks["mkpath"].assert_called_with("/test/log")

        # check if root has an handler and a level
        logging_mock = mocks["logging"]
        logging_mock.root.setLevel.assert_called_with(logging.INFO)
        logging_mock.root.addHandler.assert_called_with(mock.ANY)

        # check if the formatter has the given format
        logging_mock.Formatter.assert_called_once_with(test_format)
        formatter_mock = logging_mock.Formatter.return_value

        # check if the handler has the right formatter
        handler_mock = logging_mock.root.addHandler.call_args[0][0]
        handler_mock.setFormatter.assert_called_with(formatter_mock)

    def test_file_error_mkdir(self, **mocks):
        test_file = "/test/log/file.log"

        # raise an error, missing directory
        mocks["mkpath"].side_effect = OSError()

        barman.utils.configure_logging(log_file=test_file)

        mocks["mkpath"].assert_called_with("/test/log")

        # check if root has an handler and a level
        logging_mock = mocks["logging"]
        logging_mock.root.setLevel.assert_called_with(logging.INFO)
        logging_mock.root.addHandler.assert_called_with(mock.ANY)

        # check if the handler has a formatter
        handler_mock = logging_mock.root.addHandler.call_args[0][0]
        handler_mock.setFormatter.assert_called_with(mock.ANY)

        # check if a warning has been raised
        mocks["_logger"].warning.assert_called_with(mock.ANY)

    def test_file_error_file(self, **mocks):
        test_file = "/test/log/file.log"

        # raise an error opening the file
        logging_mock = mocks["logging"]
        logging_mock.handlers.WatchedFileHandler.side_effect = IOError()

        barman.utils.configure_logging(log_file=test_file)

        mocks["mkpath"].assert_called_with("/test/log")

        # check if root has an handler and a level
        logging_mock.root.setLevel.assert_called_with(logging.INFO)
        logging_mock.root.addHandler.assert_called_with(mock.ANY)

        # check if the handler has a formatter
        handler_mock = logging_mock.root.addHandler.call_args[0][0]
        handler_mock.setFormatter.assert_called_with(mock.ANY)

        # check if a warning has been raised
        mocks["_logger"].warning.assert_called_with(mock.ANY)


# noinspection PyMethodMayBeStatic
class TestPrettySize(object):
    def test_1000(self):
        val = 10
        base = 1000
        assert barman.utils.pretty_size(val, base) == "10 B"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10.0 kB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10.0 MB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10.0 GB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10.0 TB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10.0 PB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10.0 EB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10.0 ZB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10.0 YB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10000.0 YB"

    def test_1024(self):
        val = 10
        base = 1024
        assert barman.utils.pretty_size(val, base) == "10 B"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10.0 KiB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10.0 MiB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10.0 GiB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10.0 TiB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10.0 PiB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10.0 EiB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10.0 ZiB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10.0 YiB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "10240.0 YiB"

    def test_negative_1000(self):
        val = -10
        base = 1000
        assert barman.utils.pretty_size(val, base) == "-10 B"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10.0 kB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10.0 MB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10.0 GB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10.0 TB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10.0 PB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10.0 EB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10.0 ZB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10.0 YB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10000.0 YB"

    def test_negative_1024(self):
        val = -10
        base = 1024
        assert barman.utils.pretty_size(val, base) == "-10 B"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10.0 KiB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10.0 MiB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10.0 GiB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10.0 TiB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10.0 PiB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10.0 EiB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10.0 ZiB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10.0 YiB"
        val *= base
        assert barman.utils.pretty_size(val, base) == "-10240.0 YiB"

    def test_float(self):
        assert barman.utils.pretty_size(1234, 1000) == barman.utils.pretty_size(
            1234.0, 1000
        )
        assert barman.utils.pretty_size(1234, 1024) == barman.utils.pretty_size(
            1234.0, 1024
        )


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
        assert barman.utils.human_readable_timedelta(td) == "1 day"

    def test_two_days(self):
        """
        Test output for a 2 days timedelta.
        """
        td = timedelta(days=2)
        assert barman.utils.human_readable_timedelta(td) == "2 days"

    def test_one_hour(self):
        """
        Test output for a 1 hour timedelta.
        """
        td = timedelta(seconds=3600)
        assert barman.utils.human_readable_timedelta(td) == "1 hour"

    def test_two_hours(self):
        """
        Test output for a 2 hours timedelta.
        """
        td = timedelta(seconds=7200)
        assert barman.utils.human_readable_timedelta(td) == "2 hours"

    def test_one_minute(self):
        """
        Test output for a 1 minute timedelta.
        """
        td = timedelta(seconds=60)
        assert barman.utils.human_readable_timedelta(td) == "1 minute"

    def test_two_minutes(self):
        """
        Test output for a 2 minutes timedelta.
        """
        td = timedelta(seconds=120)
        assert barman.utils.human_readable_timedelta(td) == "2 minutes"

    def test_one_hour_two_mins(self):
        """
        Test output for a 1 hour, 2 minutes timedelta.
        """
        td = timedelta(seconds=3720)
        assert barman.utils.human_readable_timedelta(td) == "1 hour, 2 minutes"

    def test_one_day_three_hour_two_mins(self):
        """
        Test output for a 1 day, 3 hour, 2 minutes timedelta.
        """
        td = timedelta(days=1, seconds=10920)
        assert (
            barman.utils.human_readable_timedelta(td) == "1 day, "
            "3 hours, "
            "2 minutes"
        )

    def test_180_days_three_hour_4_mins(self):
        """
        Test output for 180 days, 3 hours, 4 minutes.
        """
        td = timedelta(days=180, seconds=11040)
        assert (
            barman.utils.human_readable_timedelta(td) == "180 days, "
            "3 hours, "
            "4 minutes"
        )

    def test_seven_days(self):
        """
        Test output for a 1 week timedelta.
        """
        td = timedelta(weeks=1)
        assert barman.utils.human_readable_timedelta(td) == "7 days"


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
        assert (
            json.dumps(to_json_mock, cls=barman.utils.BarmanEncoder) == '"json_value"'
        )

        # Test encoding with an object that provides a ctime() method
        assert (
            json.dumps(
                datetime(2015, 1, 10, 12, 34, 56), cls=barman.utils.BarmanEncoder
            )
            == '"Sat Jan 10 12:34:56 2015"'
        )

        # Test encoding with a timedelta object
        assert (
            json.dumps(
                timedelta(days=35, seconds=12345), cls=barman.utils.BarmanEncoder
            )
            == '"35 days, 3 hours, '
            '25 minutes, 45 seconds"'
        )
        assert (
            json.dumps(timedelta(seconds=0.1), cls=barman.utils.BarmanEncoder)
            == '"less than one second"'
        )

        # Test encoding with a Decimal object
        num = decimal.Decimal("123456789.9876543210")
        assert json.dumps(num, cls=barman.utils.BarmanEncoder) == repr(float(num))

        # Test encoding with a raw string object (simulated)
        string_value = mock.Mock(name="string_value", wraps="string_value")
        string_value.attach_mock(mock.Mock(), "decode")
        string_value.decode.return_value = "decoded_value"
        assert (
            json.dumps(string_value, cls=barman.utils.BarmanEncoder)
            == '"decoded_value"'
        )
        string_value.decode.assert_called_once_with("utf-8", "replace")

    def test_against_zero_values(self):
        # Test encoding with a Decimal object
        num = decimal.Decimal("0")
        assert json.dumps(num, cls=barman.utils.BarmanEncoder) == repr(float(num))

        assert json.dumps("", cls=barman.utils.BarmanEncoder) == '""'

    def test_simple_objects(self):
        """
        Test the BarmanEncoder on simple objects
        """
        assert (
            json.dumps([{"a": 1}, "test"], cls=barman.utils.BarmanEncoder)
            == '[{"a": 1}, "test"]'
        )

    def test_version_objects(self):
        """
        Test the BarmanEncoder on distutils version objects
        """
        json_dump = json.dumps(LooseVersion("9.5.3"), cls=barman.utils.BarmanEncoder)
        assert json_dump == '"9.5.3"'


class TestBarmanEncoderV2(object):
    """
    Test BarmanEncoderV2 object
    """

    @pytest.mark.skipif(
        sys.version_info < (3, 6), reason="Requires Python 3.6 or higher"
    )
    def test_datetime_object_from_py36(self):
        dt = datetime(2015, 1, 10, 12, 34, 56)

        # Test date without tzinfo. should raise an exception
        with pytest.raises(ValueError):
            json.dumps(dt, cls=barman.utils.BarmanEncoderV2)
        dttz = dt.astimezone()
        tz_str = dttz.isoformat()[-6:]
        assert re.match(r"^[\+-][0-1][0-9]:[0-6][0-9]$", tz_str)
        assert json.dumps(
            dttz, cls=barman.utils.BarmanEncoderV2
        ) == '"2015-01-10T12:34:56{}"'.format(tz_str)

    @pytest.mark.skipif(sys.version_info > (2,), reason="Requires Python 2")
    def test_datetime_object_before_py36(self):
        dt = datetime(2015, 1, 10, 12, 34, 56)

        # Test date without tzinfo. should raise an exception
        with pytest.raises(ValueError):
            json.dumps(dt, cls=barman.utils.BarmanEncoderV2)

        dttz = dt.replace(tzinfo=tz.tzlocal())
        tz_str = dttz.isoformat()[-6:]
        assert re.match(r"^[\+-][0-1][0-9]:[0-6][0-9]$", tz_str)
        assert json.dumps(
            dttz, cls=barman.utils.BarmanEncoderV2
        ) == '"2015-01-10T12:34:56{}"'.format(tz_str)

    def test_complex_objects(self):
        """
        Test the BarmanEncoder on special objects
        """
        # Test encoding with an object that provides a to_json() method
        to_json_mock = mock.Mock(name="to_json_mock")
        to_json_mock.to_json.return_value = "json_value"
        assert (
            json.dumps(to_json_mock, cls=barman.utils.BarmanEncoderV2) == '"json_value"'
        )

        # Test encoding with a timedelta object
        assert (
            json.dumps(
                timedelta(days=35, seconds=12345), cls=barman.utils.BarmanEncoderV2
            )
            == '"35 days, 3 hours, '
            '25 minutes, 45 seconds"'
        )
        assert (
            json.dumps(timedelta(seconds=0.1), cls=barman.utils.BarmanEncoderV2)
            == '"less than one second"'
        )

        # Test encoding with a Decimal object
        num = decimal.Decimal("123456789.9876543210")
        assert json.dumps(num, cls=barman.utils.BarmanEncoderV2) == repr(float(num))

        # Test encoding with a raw string object (simulated)
        string_value = mock.Mock(name="string_value", wraps="string_value")
        string_value.attach_mock(mock.Mock(), "decode")
        string_value.decode.return_value = "decoded_value"
        assert (
            json.dumps(string_value, cls=barman.utils.BarmanEncoderV2)
            == '"decoded_value"'
        )
        string_value.decode.assert_called_once_with("utf-8", "replace")

    def test_simple_objects(self):
        """
        Test the BarmanEncoderV2 on simple objects
        """
        assert (
            json.dumps([{"a": 1}, "test"], cls=barman.utils.BarmanEncoderV2)
            == '[{"a": 1}, "test"]'
        )

    def test_version_objects(self):
        """
        Test the BarmanEncoderV2 on distutils version objects
        """
        json_dump = json.dumps(LooseVersion("9.5.3"), cls=barman.utils.BarmanEncoderV2)
        assert json_dump == '"9.5.3"'


# noinspection PyMethodMayBeStatic
@mock.patch("barman.utils.signal.signal")
@mock.patch("barman.utils.signal.alarm")
class TestTimeout(object):
    """
    Simple class for timeout context manager testing
    """

    def test_timeout_enter(self, alarm_mock, signal_mock):
        # Test: normal call
        signal_mock.return_value = signal.SIG_DFL
        with barman.utils.timeout(3):
            signal_mock.assert_called_once_with(signal.SIGALRM, mock.ANY)
            alarm_mock.assert_called_once_with(3)

        # Use to simulate another alarm running
        def fake_handler(signum, frame):
            pass

        # Test: forbid nesting
        signal_mock.reset_mock()
        alarm_mock.reset_mock()
        signal_mock.return_value = fake_handler
        with pytest.raises(AssertionError):
            with barman.utils.timeout(3):
                pass
        signal_mock.assert_called_with(signal.SIGALRM, mock.ANY)
        signal_mock.assert_called_with(signal.SIGALRM, fake_handler)
        assert not alarm_mock.called

    def test_timeout_exit(self, alarm_mock, signal_mock):
        # test: normal call
        signal_mock.return_value = signal.SIG_DFL
        with barman.utils.timeout(3):
            # Reset the mocks, we are only interested to the exit actions
            signal_mock.reset_mock()
            alarm_mock.reset_mock()
        signal_mock.assert_called_once_with(signal.SIGALRM, signal.SIG_DFL)
        alarm_mock.assert_called_once_with(0)

        # test: exception handling
        with pytest.raises(ZeroDivisionError):
            with barman.utils.timeout(3):
                # Reset the mocks, we are only interested to the exit actions
                signal_mock.reset_mock()
                alarm_mock.reset_mock()
                raise ZeroDivisionError("Fake Error")
        signal_mock.assert_called_once_with(signal.SIGALRM, signal.SIG_DFL)
        alarm_mock.assert_called_once_with(0)


# noinspection PyMethodMayBeStatic
class TestSimplifyVersion(object):
    """
    Tests for simplify_version function
    """

    def test_normal_releases(self):
        assert barman.utils.simplify_version("9.1.2") == "9.1"
        assert barman.utils.simplify_version("10.1") == "10"

    def test_dev_releases(self):
        assert barman.utils.simplify_version("9.1devel") == "9.1"
        assert barman.utils.simplify_version("10devel") == "10"

    def test_rc_releases(self):
        assert barman.utils.simplify_version("9.1rc1") == "9.1"
        assert barman.utils.simplify_version("10rc1") == "10"

    def test_beta_releases(self):
        assert barman.utils.simplify_version("9.1beta4") == "9.1"
        assert barman.utils.simplify_version("10beta4") == "10"


class TestPowerOfTwo(object):
    """
    Test for the is_power_of_two function
    """

    def test_zero(self):
        assert not barman.utils.is_power_of_two(0)

    def test_power_of_two(self):
        assert barman.utils.is_power_of_two(64)

    def test_not_power_of_two(self):
        assert not barman.utils.is_power_of_two(33)

    def test_none(self):
        assert not barman.utils.is_power_of_two(None)


class TestForceText(object):
    """
    Test for the force_text function
    """

    def test_force_text(self):
        """
        Force text normal usage
        """
        accented = "\u0227\u0188\u0188\u1e17\u019e\u0167\u1e17\u1e13"

        class Test:
            if sys.version_info[0] >= 3:

                def __str__(self):
                    return accented

            else:

                def __str__(self):
                    return accented.encode("utf-8")

        class TestU:
            if sys.version_info[0] >= 3:

                def __str__(self):
                    return accented

                def __bytes__(self):
                    return b"Wrong"

            else:

                def __str__(self):
                    return b"Wrong"

                def __unicode__(self):
                    return accented

        assert barman.utils.force_str(Test()) == accented
        assert barman.utils.force_str(TestU()) == accented
        assert barman.utils.force_str(Exception(Test())) == accented
        assert barman.utils.force_str(Exception(TestU())) == accented
        assert barman.utils.force_str(1) == "1"
        assert barman.utils.force_str("foo") == "foo"
        assert barman.utils.force_str(("foo", "bar")) == "('foo', 'bar')"

    def test_force_text_exception(self):
        """
        Broken __unicode__/__str__ actually raises an error.
        """

        class MyString(object):
            def __str__(self):
                return b"\xc3\xa0\xc3\xa8\xc3\xac\xc3\xb2\xc3\xb9"

            __unicode__ = __str__

        # str(s) raises a TypeError on python 3 if the result is not a text
        # type.
        # python 2 fails when it tries converting from str to unicode (via
        # ASCII).
        obj = MyString()
        assert barman.utils.force_str(obj) == repr(obj)


class TestCheckSize(object):
    @pytest.mark.parametrize(
        "size, bytes",
        [
            ["12345", 12345],
            ["4321B", 4321],
            ["12kB", 12 << 10],
            ["300MB", 300 << 20],
            ["20GB", 20 << 30],
            ["1TB", 1 << 40],
            ["12kiB", 12 * 10**3],
            ["300MiB", 300 * 10**6],
            ["20GiB", 20 * 10**9],
            ["1TiB", 1 * 10**12],
        ],
    )
    def test_parse(self, size, bytes):
        assert barman.utils.check_size(size) == bytes

    def test_parse_error(self):
        with pytest.raises(ArgumentTypeError):
            barman.utils.check_size("1X2")

    def test_negative_size(self):
        with pytest.raises(ArgumentTypeError):
            barman.utils.check_size("-1")

    def test_none(self):
        assert barman.utils.check_size(None) is None


class TestLocksCleanup(object):
    def test_locks_cleanup(self, caplog, tmpdir):
        # Configure logging
        log_file = tmpdir.join(LOGFILE_NAME)
        log_file.ensure
        test_level = logging.DEBUG
        barman.utils.configure_logging(log_file=log_file.strpath, log_level=test_level)
        # Case 1: delete the lockfile after acquisition
        # Create a lockfile directory
        lockfile_directory = tmpdir.mkdir("lock_dir")
        # Create a fake lock
        lock_file_path = lockfile_directory.join(".test_lock_file1.lock")
        lock_file_path.ensure()
        # Invoke the cleanup method on the fake lockfile
        barman.utils.lock_files_cleanup(lockfile_directory, True)
        assert "%s deleted" % lock_file_path.strpath in caplog.text

    def test_busy_lock_skip(self, caplog, tmpdir):
        # Configure logging
        log_file = tmpdir.join(LOGFILE_NAME)
        log_file.ensure
        test_level = logging.DEBUG
        barman.utils.configure_logging(log_file=log_file.strpath, log_level=test_level)
        # Case 2: check that an acquired lockfile is correctly skipped
        lockfile_directory = tmpdir.mkdir("lock_dir")
        lock_file_path2 = lockfile_directory.join(".test_lock_file2.lock")
        # Acquire the lock and keep it locked, run the method,
        # expect it to be skipped.
        lock = LockFile(lock_file_path2.strpath)
        with lock as locked:
            if locked:
                barman.utils.lock_files_cleanup(lockfile_directory, True)
        # check log for skip message
        assert (
            "%s file lock already acquired, skipping removal" % lock_file_path2.strpath
        ) in caplog.text

    def test_auto_clean_disabled(self, caplog, tmpdir):
        # Configure logging
        log_file = tmpdir.join(LOGFILE_NAME)
        log_file.ensure
        test_level = logging.DEBUG
        barman.utils.configure_logging(log_file=log_file.strpath, log_level=test_level)
        # Case 2: check that an acquired lockfile is correctly skipped
        # Acquire the lock and keep it locked, run the method,
        # expect it to be skipped.
        fakepath = "/fake/lockdir/"
        barman.utils.lock_files_cleanup(fakepath, False)
        # check log for skip message
        assert ("Auto-cleanup of '%s' directory disabled" % fakepath) in caplog.text


class TestCheckTli(object):
    @pytest.mark.parametrize("tli", ["1", "2", "current", "latest"])
    def test_parse(self, tli):
        assert barman.utils.check_tli(tli)

    @pytest.mark.parametrize("tli", ["0", "-1", "newest"])
    def test_parse_error(self, tli):
        with pytest.raises(ArgumentTypeError):
            barman.utils.check_tli(tli)


class TestSHA256(object):
    def test_get_name(self):
        sha = barman.utils.SHA256()
        assert "SHA256" == sha.get_name()

    def test_checksum(self):
        sha = barman.utils.SHA256()
        ref_checksum = (
            "9c6609fc5111405ea3f5bb3d1f6b5a5efd19a0cec53d85893fd96d265439cd5b"
        )
        assert ref_checksum == sha.checksum("Some content".encode("utf-8"))

    def test_checksum_from_str(self):
        sha = barman.utils.SHA256()
        ref_checksum = (
            "9c6609fc5111405ea3f5bb3d1f6b5a5efd19a0cec53d85893fd96d265439cd5b"
        )
        assert ref_checksum == sha.checksum_from_str("Some content")


class TestCheckBackupNames(object):
    @pytest.mark.parametrize(
        ("backup_id", "expected_result"),
        (
            ("19700101T000000", True),
            ("20380119T031408", True),
            ("2038011T031408", False),
            ("20380119T03140", False),
            ("20201109t065300", False),
            ("a name not an ID", False),
        ),
    )
    def test_is_backup_id(self, backup_id, expected_result):
        # GIVEN a backup id
        # WHEN is_backup_id is called
        # THEN backup IDs produce a True result and non-backup IDs produce False
        assert barman.utils.is_backup_id(backup_id) is expected_result

    @pytest.mark.parametrize(
        ("backup_name", "error_message"),
        (
            (None, "Backup name cannot be None"),
            ("", "Backup name cannot be empty"),
            (
                "20380119T031408",
                "Backup name '20380119T031408' is not allowed: backup ID",
            ),
            ("latest", "Backup name 'latest' is not allowed: reserved word"),
            ("last", "Backup name 'last' is not allowed: reserved word"),
            ("first", "Backup name 'first' is not allowed: reserved word"),
            ("oldest", "Backup name 'oldest' is not allowed: reserved word"),
            ("last-failed", "Backup name 'last-failed' is not allowed: reserved word"),
        ),
    )
    def test_check_backup_name_failure(self, backup_name, error_message):
        # GIVEN a backup name
        # WHEN check_backup_name is called
        # THEN an ArgumentTypeError is raised
        with pytest.raises(ArgumentTypeError) as exc:
            barman.utils.check_backup_name(backup_name)
        # AND the exception has the expected message
        assert str(exc.value) == error_message

    @pytest.mark.parametrize("backup_name", ("backup_name", "backup name with spaces"))
    def test_check_backup_name_success(self, backup_name):
        # GIVEN a backup name
        # WHEN check_backup_name is called
        checked_backup_name = barman.utils.check_backup_name(backup_name)
        # THEN the result is the backup name
        assert checked_backup_name == backup_name

    @pytest.fixture
    def mock_backup_info_list(self):
        return [
            mock.Mock(backup_name="this name matches one", backup_id="20200101T000000"),
            mock.Mock(backup_name="this name matches two", backup_id="20200101T010000"),
            mock.Mock(backup_name="this name matches two", backup_id="20200101T020000"),
        ]

    def test_get_backup_info_from_name_match(self, mock_backup_info_list):
        # GIVEN a list of backup info objects
        # WHEN get_backup_info_from_name is called with a name which matches one backup
        backup_name = "this name matches one"
        backup_info = barman.utils.get_backup_info_from_name(
            mock_backup_info_list, backup_name
        )

        # THEN a single backup info is returned which has the expected ID
        expected_id = [
            backup.backup_id
            for backup in mock_backup_info_list
            if backup.backup_name == backup_name
        ][0]
        assert backup_info.backup_id == expected_id

    def test_get_backup_info_from_name_multiple_match(self, mock_backup_info_list):
        # GIVEN a list of backup info objects
        # WHEN get_backup_info_from_name is called with a name which matches two backups
        backup_name = "this name matches two"

        # THEN a ValueError is raised
        with pytest.raises(ValueError) as exc:
            barman.utils.get_backup_info_from_name(mock_backup_info_list, backup_name)

        # AND the exception has the expected message
        matching_backup_ids = [
            backup.backup_id
            for backup in mock_backup_info_list
            if backup.backup_name == backup_name
        ]
        assert str(exc.value) == (
            "Multiple backups found matching name 'this name matches two' "
            "(try using backup ID instead): %s"
        ) % " ".join(matching_backup_ids)

    def test_get_backup_info_from_name_no_match(self, mock_backup_info_list):
        # GIVEN a list of backup info objects
        # WHEN get_backup_info_from_name is called with a name which matches no backups
        backup_name = "this name matches nothing"
        backup_info = barman.utils.get_backup_info_from_name(
            mock_backup_info_list, backup_name
        )
        # THEN None is returned
        assert backup_info is None

    @pytest.mark.parametrize(
        "shortcut",
        (
            "latest",
            "last",
            "oldest",
            "first",
            "last-failed",
            "latest-full",
            "last-full",
            "12345",
        ),
    )
    @mock.patch("barman.utils.is_backup_id")
    def test_get_backup_id_using_shortcut(
        self,
        mock_is_backup_id,
        shortcut,
    ):
        backup_manager = build_backup_manager()
        server = backup_manager.server
        barman.utils.get_backup_id_using_shortcut(server, shortcut, BackupInfo)
        if shortcut in ("latest", "last"):
            server.get_last_backup_id.assert_called_once_with()
        elif shortcut in ("oldest", "first"):
            server.get_first_backup_id.assert_called_once_with()
        elif shortcut in ("last-failed"):
            server.get_last_backup_id.assert_called_once_with([BackupInfo.FAILED])
        elif shortcut in ("latest-full", "last-full"):
            server.get_last_full_backup_id.assert_called_once_with()
        else:
            mock_is_backup_id.assert_called_once_with(shortcut)

    def test_get_backup_info_from_name(self):
        """
        Test the `get_backup_info_from_name` from utils will return the correct
        backup based on a named backup.
        """
        backup_manager = build_backup_manager()

        available_backups = {
            "20250108T120000": {
                "status": "DONE",
            },
            "20250107T120000": {
                "status": "DONE",
            },
            "20250106T120000": {
                "status": "DONE",
                "backup_name": "my_test_backup",
            },
        }

        backups = dict(
            (
                bkp_id,
                build_test_backup_info(
                    server=backup_manager.server, backup_id=bkp_id, **bkp_metadata
                ),
            )
            for bkp_id, bkp_metadata in available_backups.items()
        )

        backup_info = barman.utils.get_backup_info_from_name(
            backups.values(), "my_test_backup"
        )

        assert backup_info == backups["20250106T120000"]


class TestBackupRecoveryTargets(object):
    @pytest.mark.parametrize(
        ("target_time", "expected_backup_id"),
        [
            ("2025-01-08 12:15:00", "20250108T120000"),
            ("2025-01-07 12:15:00", "20250107T120000"),
            ("2025-01-06 12:15:00", "20250106T120000"),
        ],
    )
    def test_get_backup_id_from_target_time(self, target_time, expected_backup_id):
        """
        Test the get_backup_id_from_target_time from utils will return the correct
        backup based on recovery target "target_time".
        """
        backup_manager = build_backup_manager()

        available_backups = {
            "20250108T120000": {
                "end_time": load_datetime_tz("2025-01-08 12:00:00"),
                "end_xlog": "3/61000000",
                "status": "DONE",
            },
            "20250107T120000": {
                "end_time": load_datetime_tz("2025-01-07 12:00:00"),
                "end_xlog": "3/5E000000",
                "status": "DONE",
            },
            "20250106T120000": {
                "end_time": load_datetime_tz("2025-01-06 12:00:00"),
                "end_xlog": "3/5B000000",
                "status": "DONE",
            },
        }

        backups = dict(
            (
                bkp_id,
                build_test_backup_info(
                    server=backup_manager.server, backup_id=bkp_id, **bkp_metadata
                ),
            )
            for bkp_id, bkp_metadata in available_backups.items()
        )

        backup_id = barman.utils.get_backup_id_from_target_time(
            backups.values(), target_time
        )

        assert backup_id == expected_backup_id

    def test_get_backup_id_from_target_time_raise_error(self):
        """
        Test the get_backup_id_from_target_time from utils will raise the correct error
        and output the correct error.
        """
        available_backups = mock.Mock()

        target_time = "2025-10-1020:00:00"

        error_msg = (
            "Unable to parse the target time parameter '2025-10-1020:00:00': "
            "Unknown string format: 2025-10-1020:00:00"
        )

        with pytest.raises(ValueError, match=error_msg):
            _ = barman.utils.get_backup_id_from_target_time(
                available_backups.values(), target_time
            )

    @pytest.mark.parametrize(
        ("target_lsn", "expected_backup_id"),
        [
            ("3/61000000", "20250108T120000"),
            ("3/62000000", "20250108T120000"),
            ("3/5E000000", "20250107T120000"),
            ("3/5F000000", "20250107T120000"),
            ("3/5B000000", "20250106T120000"),
            ("3/5C000000", "20250106T120000"),
        ],
    )
    def test_get_backup_id_from_target_lsn(self, target_lsn, expected_backup_id):
        """
        Test the get_backup_id_from_target_lsn from utils will return the correct
        backup based on recovery target "target_lsn".
        """
        backup_manager = build_backup_manager()

        available_backups = {
            "20250108T120000": {
                "end_time": load_datetime_tz("2025-01-08 12:00:00"),
                "end_xlog": "3/61000000",
                "status": "DONE",
            },
            "20250107T120000": {
                "end_time": load_datetime_tz("2025-01-07 12:00:00"),
                "end_xlog": "3/5E000000",
                "status": "DONE",
            },
            "20250106T120000": {
                "end_time": load_datetime_tz("2025-01-06 12:00:00"),
                "end_xlog": "3/5B000000",
                "status": "DONE",
            },
        }

        backups = dict(
            (
                bkp_id,
                build_test_backup_info(
                    server=backup_manager.server, backup_id=bkp_id, **bkp_metadata
                ),
            )
            for bkp_id, bkp_metadata in available_backups.items()
        )

        backup_id = barman.utils.get_backup_id_from_target_lsn(
            backups.values(), target_lsn
        )

        assert backup_id == expected_backup_id

    @pytest.mark.parametrize(
        ("target_tli", "expected_backup_id"),
        [
            (3, None),
            (2, "20250111T120000"),
            (1, "20250108T120000"),
        ],
    )
    def test_get_backup_id_from_target_tli(self, target_tli, expected_backup_id):
        """
        Test the get_backup_id_from_target_tli from utils will return the correct
        backup based on recovery target "target_tli".
        """
        backup_manager = build_backup_manager()

        available_backups = {
            "20250111T120000": {
                "end_time": load_datetime_tz("2025-01-11 12:00:00"),
                "end_xlog": "3/FF000000",
                "status": "DONE",
                "timeline": 2,
            },
            "20250110T120000": {
                "end_time": load_datetime_tz("2025-01-10 12:00:00"),
                "end_xlog": "3/74000000",
                "status": "DONE",
                "timeline": 2,
            },
            "20250109T120000": {
                "end_time": load_datetime_tz("2025-01-09 12:00:00"),
                "end_xlog": "3/70000000",
                "status": "DONE",
                "timeline": 2,
            },
            "20250108T120000": {
                "end_time": load_datetime_tz("2025-01-08 12:00:00"),
                "end_xlog": "3/61000000",
                "status": "DONE",
                "timeline": 1,
            },
            "20250107T120000": {
                "end_time": load_datetime_tz("2025-01-07 12:00:00"),
                "end_xlog": "3/5E000000",
                "status": "DONE",
                "timeline": 1,
            },
            "20250106T120000": {
                "end_time": load_datetime_tz("2025-01-06 12:00:00"),
                "end_xlog": "3/5B000000",
                "status": "DONE",
                "timeline": 1,
            },
        }

        backups = dict(
            (
                bkp_id,
                build_test_backup_info(
                    server=backup_manager.server, backup_id=bkp_id, **bkp_metadata
                ),
            )
            for bkp_id, bkp_metadata in available_backups.items()
        )

        backup_id = barman.utils.get_backup_id_from_target_tli(
            backups.values(), target_tli
        )

        assert backup_id == expected_backup_id

    @pytest.mark.parametrize(
        ("target_tli", "expected_target_tli"),
        [("current", 1), ("latest", 3), ("1", 1), (None, None)],
    )
    def test_parse_target_tli_with_backup_id(self, target_tli, expected_target_tli):
        """
        Test the parse_target_tli from utils will return the correct
        timeline based on shortcuts (``latest``, ``current``), string value and None.
        """
        server = build_mocked_server()

        current_bkp_metadata = {
            "backup_id": "20250108T120000",
            "end_time": load_datetime_tz("2025-01-08 12:00:00"),
            "end_xlog": "3/61000000",
            "status": "DONE",
            "timeline": 1,
        }
        backup_info = build_test_backup_info(server=server, **current_bkp_metadata)

        mock_get_latest_archived_wals_info = (
            server.backup_manager.get_latest_archived_wals_info
        )
        mock_get_latest_archived_wals_info.return_value = {
            "00000001": WalFileInfo(),
            "00000002": WalFileInfo(),
            "00000003": WalFileInfo(),
        }
        calculated_target_tli = barman.utils.parse_target_tli(
            server.backup_manager, target_tli, backup_info
        )

        assert calculated_target_tli == expected_target_tli

    @pytest.mark.parametrize(
        ("target_tli", "expected_target_tli"),
        [("current", None), ("latest", 3), ("1", 1), (None, None)],
    )
    def test_parse_target_tli_without_backup_id(self, target_tli, expected_target_tli):
        """
        Test the parse_target_tli from utils will return the correct
        timeline or error msg based on shortcuts (``latest``, ``current``), string value
        and None.
        """
        server = build_mocked_server()
        mock_get_latest_archived_wals_info = (
            server.backup_manager.get_latest_archived_wals_info
        )
        mock_get_latest_archived_wals_info.return_value = {
            "00000001": WalFileInfo(),
            "00000002": WalFileInfo(),
            "00000003": WalFileInfo(),
        }
        if target_tli == "current":
            with pytest.raises(ValueError) as e:
                _ = barman.utils.parse_target_tli(server, target_tli)

            assert (
                "'current' is not a valid timeline keyword when recovering without a backup_id"
                in str(e)
            )
        else:
            calculated_target_tli = barman.utils.parse_target_tli(
                server.backup_manager, target_tli
            )

            assert calculated_target_tli == expected_target_tli

    @pytest.mark.parametrize(
        ("target_tli", "error_msg"),
        [
            ("a1", "'a1' is not a valid timeline keyword"),
            ("1.0", "'1.0' is not a valid timeline keyword"),
        ],
    )
    def test_parse_target_tli_invalid_keyword(self, target_tli, error_msg):
        """
        Test the parse_target_tli from utils will raise an error
        when using incorrect targets.
        """
        server = build_mocked_server()

        backup_info = None

        with pytest.raises(ValueError) as e:
            _ = barman.utils.parse_target_tli(server, target_tli, backup_info)

        assert error_msg in str(e)


class TestAWSSnapshotLock(object):
    """
    Tests AWS Snapshot Lock checks
    """

    def test_check_aws_expiration_date_format(self):
        """
        Tests if check_aws_expiration_date properly creates a datetime object
        with the specific format.
        """
        assert isinstance(
            barman.utils.check_aws_expiration_date_format("2024-01-01T00:00:00.000Z"),
            datetime,
        )

    @pytest.mark.parametrize(
        "timestamp", ["2024-01-01", "2024-01-01T00:00:00", "2024-01-01T00:00:00.000"]
    )
    def test_check_aws_expiration_date_format_error(self, timestamp):
        """
        Tests if check_aws_expiration_date raises an ArgumentTypeError if the value
        is in the wrong format.
        """
        with pytest.raises(ArgumentTypeError):
            barman.utils.check_aws_expiration_date_format(timestamp)

    @pytest.mark.parametrize("duration", ["1", "12000", "36500"])
    def test_check_aws_snapshot_lock_duration_range(self, duration):
        """
        Tests if check_aws_snapshot_lock_duration_range is properly set as an integer
        in the range limit.
        """
        assert barman.utils.check_aws_snapshot_lock_duration_range(duration) == int(
            duration
        )

    @pytest.mark.parametrize("duration", ["-1", "0", "36501"])
    def test_check_aws_snapshot_lock_duration_range_error(self, duration):
        """
        Tests if check_aws_snapshot_lock_duration_range raises an ArgumentTypeError if
        the value is outside the range.
        """
        with pytest.raises(ValueError):
            barman.utils.check_aws_snapshot_lock_duration_range(duration)

    @pytest.mark.parametrize("duration", ["1", "30", "72"])
    def test_check_aws_snapshot_lock_cool_off_period_range(self, duration):
        """
        Tests if check_aws_snapshot_lock_cool_off_period_range is properly set as an
        integer in the range limit.
        """
        assert barman.utils.check_aws_snapshot_lock_cool_off_period_range(
            duration
        ) == int(duration)

    @pytest.mark.parametrize("duration", ["-1", "0", "73"])
    def test_check_aws_snapshot_lock_cool_off_period_range_error(self, duration):
        """
        Tests if check_aws_snapshot_lock_cool_off_period_range raises an
        ArgumentTypeError if the value is outside the range.
        """
        with pytest.raises(ValueError):
            barman.utils.check_aws_snapshot_lock_cool_off_period_range(duration)

    @pytest.mark.parametrize("mode", ["governance", "compliance"])
    def test_check_aws_snapshot_lock_mode(self, mode):
        """
        Tests if check_aws_snapshot_lock_cool_off_period_range raises an
        ArgumentTypeError if the value is outside the range.
        """
        assert barman.utils.check_aws_snapshot_lock_mode(mode) == str(mode)

    @pytest.mark.parametrize("mode", ["nogovernance", "nocompliance"])
    def test_check_aws_snapshot_lock_mode_error(self, mode):
        """
        Tests if check_aws_snapshot_lock_cool_off_period_range raises an
        ArgumentTypeError if the value is outside the range.
        """
        with pytest.raises(ValueError):
            barman.utils.check_aws_snapshot_lock_mode(mode) == str(mode)


class TestEditConfig:
    def test_edit_config_existing_section(self, tmpdir):
        # Create a temporary file
        temp_file = tmpdir.join("test.ini")
        temp_file.write(b"[Section1]\nkey1 = value1\nkey2 = value2\n")
        temp_file.ensure(file=True)
        # Call the edit_config function
        lines = barman.utils.edit_config(
            temp_file.strpath, "Section1", "key2", "new_value"
        )

        # Verify the updated lines
        assert lines == ["[Section1]\n", "key1 = value1\n", "key2 = new_value\n"]

        # Clean up the temporary file
        os.remove(temp_file.strpath)

    def test_edit_config_new_section(self, tmpdir):
        # Create a temporary file
        temp_file = tmpdir.join("test.ini")
        temp_file.write(b"[Section1]\nkey1 = value1\n")
        temp_file.ensure(file=True)

        # Call the edit_config function
        lines = barman.utils.edit_config(
            temp_file.strpath, "Section2", "key1", "value1"
        )

        # Verify the updated lines
        assert lines == [
            "[Section1]\n",
            "key1 = value1\n",
            "[Section2]\n",
            "key1 = value1\n",
        ]

        # Clean up the temporary file
        os.remove(temp_file.strpath)

    def test_edit_config_existing_option(self, tmpdir):
        # Create a temporary file

        temp_file = tmpdir.join("test.ini")
        temp_file.write(b"[Section1]\nkey1 = value1\nkey2 = value2\n")
        temp_file.ensure(file=True)

        # Call the edit_config function
        lines = barman.utils.edit_config(
            temp_file.strpath, "Section1", "key1", "new_value"
        )

        # Verify the updated lines
        assert lines == ["[Section1]\n", "key1 = new_value\n", "key2 = value2\n"]

        # Clean up the temporary file
        os.remove(temp_file.strpath)

    def test_edit_config_new_file(self, tmpdir):
        # Create a temporary file
        temp_file = tmpdir.join("test.ini")
        file_path = temp_file.strpath
        temp_file.ensure(file=True)

        # Call the edit_config function
        lines = barman.utils.edit_config(file_path, "Section1", "key1", "value1")

        # Verify the updated lines
        assert lines == ["[Section1]\n", "key1 = value1\n"]

        # Clean up the temporary file
        os.remove(file_path)


class TestAWSTags:
    """
    Unit tests for AWS tag related utility functions in `barman.utils`.

    This test class verifies the behavior of the `check_tag` and
    `has_duplicate_first_keys` function. The first parses and validates tag strings in
    the format "key,value". It checks for correct parsing, validation of key and value
    lengths, reserved prefixes, allowed characters, and proper error handling for
    invalid inputs. The second ...TODO
    """

    def test_check_tag_valid_tag(self):
        """
        Test that valid tag strings are correctly parsed into key-value tuples.

        - Checks parsing of simple and complex tag strings.
        """
        assert barman.utils.check_tag("owner,data-team") == ("owner", "data-team")
        assert barman.utils.check_tag("key,value with spaces") == (
            "key",
            "value with spaces",
        )
        assert barman.utils.check_tag(
            "key_with.allowed:chars@/+=-,value_with.allowed:chars@/+=-"
        ) == ("key_with.allowed:chars@/+=-", "value_with.allowed:chars@/+=-")

    def test_check_tag_none_or_empty(self):
        """
        Test that `None` or empty strings return `None` from `check_tag`.
        """
        assert barman.utils.check_tag("") is None
        assert barman.utils.check_tag(None) is None

    def test_check_tag_missing_comma(self):
        """
        Test that a tag string missing a comma raises a `ValueError`.

        :raises ValueError: If the tag format is invalid.
        """
        with pytest.raises(
            ValueError, match="Invalid tag format. Expected 'key,value'."
        ):
            barman.utils.check_tag("keyvalue")

    def test_check_tag_key_too_short(self):
        """
        Test that a tag key with zero length raises a `ValueError`.

        :raises ValueError: If the tag key is too short.
        """
        with pytest.raises(
            ValueError, match="Tag key must be between 1 and 128 characters."
        ):
            barman.utils.check_tag(",value")

    def test_check_tag_key_too_long(self):
        """
        Test that a tag key longer than 128 characters raises a `ValueError`.

        :raises ValueError: If the tag key is too long.
        """
        long_key = "k" * 129
        with pytest.raises(
            ValueError, match="Tag key must be between 1 and 128 characters."
        ):
            barman.utils.check_tag(f"{long_key},value")

    def test_check_tag_key_reserved_prefix(self):
        """
        Test that a tag key starting with the reserved prefix 'aws:' raises a
        `ValueError`.

        :raises ValueError: If the tag key starts with 'aws:'.
        """
        with pytest.raises(
            ValueError, match="Tag key cannot start with the reserved prefix 'aws:'."
        ):
            barman.utils.check_tag("aws:owner,value")

    def test_check_tag_key_invalid_characters(self):
        """
        Test that a tag key containing invalid characters raises a `ValueError`.

        :raises ValueError: If the tag key contains invalid characters.
        """
        with pytest.raises(ValueError, match="Tag key contains invalid characters."):
            barman.utils.check_tag("key$,value")

    def test_check_tag_value_too_long(self):
        """
        Test that a tag value longer than 256 characters raises a `ValueError`.

        :raises ValueError: If the tag value is too long.
        """
        long_val = "v" * 257
        with pytest.raises(
            ValueError, match="Tag value must be 256 characters or less."
        ):
            barman.utils.check_tag(f"key,{long_val}")

    def test_check_tag_value_invalid_characters(self):
        """
        Test that a tag value containing invalid characters raises a `ValueError`.

        :raises ValueError: If the tag value contains invalid characters.
        """
        with pytest.raises(ValueError, match="Tag value contains invalid characters."):
            barman.utils.check_tag("key,value$")

    def test_check_tag_strip_spaces(self):
        """
        Test that leading and trailing spaces in key and value are stripped before
        validation and parsing.
        """
        assert barman.utils.check_tag("  key  ,  value  ") == ("key", "value")


class TestIsSubdirectory:
    def test_direct_subdirectory(self, tmpdir):
        parent = tmpdir.mkdir("parent")
        child = parent.mkdir("child")
        assert barman.utils.is_subdirectory(str(parent), str(child)) is True

    def test_nested_subdirectory(self, tmpdir):
        parent = tmpdir.mkdir("parent")
        child = parent.mkdir("child").mkdir("grandchild")
        assert barman.utils.is_subdirectory(str(parent), str(child)) is True

    def test_same_directory(self, tmpdir):
        parent = tmpdir.mkdir("parent")
        assert barman.utils.is_subdirectory(str(parent), str(parent)) is True

    def test_not_a_subdirectory(self, tmpdir):
        parent = tmpdir.mkdir("parent")
        sibling = tmpdir.mkdir("sibling")
        assert barman.utils.is_subdirectory(str(parent), str(sibling)) is False

    def test_relative_paths(self, tmpdir):
        parent = tmpdir.mkdir("parent")
        parent.mkdir("child")
        cwd = os.getcwd()
        try:
            os.chdir(str(tmpdir))
            assert barman.utils.is_subdirectory("parent", "parent/child") is True
        finally:
            os.chdir(cwd)

    def test_root_and_subdirectory(self, tmpdir):
        # On Unix, '/' is the root
        child = tmpdir.mkdir("child")
        assert barman.utils.is_subdirectory("/", str(child)) is True

    def test_empty_paths(self):
        # Empty path resolves to the current directory
        assert barman.utils.is_subdirectory("", "") is True
        assert barman.utils.is_subdirectory("/", "") is True
        assert barman.utils.is_subdirectory("", "/tmp") is False


class TestGetMajorVersion:
    def test_none_input(self):
        assert barman.utils.get_major_version(None) is None

    def test_no_version_in_string(self):
        assert barman.utils.get_major_version("no version here") is None

    def test_integer_version(self):
        assert barman.utils.get_major_version("PostgreSQL 17") == "17"
        assert barman.utils.get_major_version("version: 42") == "42"
        assert barman.utils.get_major_version("v1") == "1"

    def test_decimal_version(self):
        assert barman.utils.get_major_version("PostgreSQL 17.5") == "17"
        assert barman.utils.get_major_version("version: 42.0") == "42"
        assert barman.utils.get_major_version("v1.2") == "1"

    def test_version_embedded_in_text(self):
        assert barman.utils.get_major_version("abc 9.6devel") == "9"
        assert barman.utils.get_major_version("release 10rc1") == "10"
        assert barman.utils.get_major_version("foo 11.2beta") == "11"

    def test_multiple_numbers(self):
        # Only the first number should be considered
        assert barman.utils.get_major_version("version 12.3 and 13.4") == "12"
        assert barman.utils.get_major_version("foo 8 bar 9.1") == "8"

    def test_leading_text(self):
        assert barman.utils.get_major_version("v17.5.3") == "17"
        assert barman.utils.get_major_version("release-20.1") == "20"

    def test_trailing_text(self):
        assert barman.utils.get_major_version("17.5beta") == "17"
        assert barman.utils.get_major_version("10devel") == "10"

    def test_leading_client_name_text(self):
        assert (
            barman.utils.get_major_version("pg_combinebackup (PostgreSQL) 17.5") == "17"
        )
        assert barman.utils.get_major_version("pg_basebackup (PostgreSQL) 17.5") == "17"
        assert (
            barman.utils.get_major_version("pg_verifybackup (PostgreSQL) 17.5") == "17"
        )


class TestUtils:
    def test_get_path_device_number(self, tmpdir):
        incoming_dir = tmpdir.mkdir("some_dir")
        expected_dev_number = os.stat(incoming_dir.strpath).st_dev
        assert expected_dev_number == barman.utils.get_path_device_number(
            incoming_dir.strpath
        )
