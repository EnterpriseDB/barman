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

"""
This module contains utility functions used in Barman.
"""

import datetime
import decimal
import errno
import grp
import hashlib
import json
import logging
import logging.handlers
import os
import pwd
import re
import signal
import sys
from abc import ABCMeta, abstractmethod
from argparse import ArgumentTypeError
from contextlib import contextmanager
from distutils.version import Version
from glob import glob

from dateutil import tz

from barman import lockfile, xlog
from barman.exceptions import TimeoutError

_logger = logging.getLogger(__name__)


if sys.version_info[0] >= 3:
    _text_type = str
    _string_types = str
else:
    _text_type = unicode  # noqa
    _string_types = basestring  # noqa

RESERVED_BACKUP_IDS = (
    "latest",
    "last",
    "oldest",
    "first",
    "last-failed",
    "latest-full",
    "last-full",
    "auto",
)


def drop_privileges(user):
    """
    Change the system user of the current python process.

    It will only work if called as root or as the target user.

    :param string user: target user
    :raise KeyError: if the target user doesn't exists
    :raise OSError: when the user change fails
    """
    pw = pwd.getpwnam(user)
    if pw.pw_uid == os.getuid():
        return
    groups = [e.gr_gid for e in grp.getgrall() if pw.pw_name in e.gr_mem]
    groups.append(pw.pw_gid)
    os.setgroups(groups)
    os.setgid(pw.pw_gid)
    os.setuid(pw.pw_uid)
    os.environ["HOME"] = pw.pw_dir


def mkpath(directory):
    """
    Recursively create a target directory.

    If the path already exists it does nothing.

    :param str directory: directory to be created
    """
    if not os.path.isdir(directory):
        os.makedirs(directory)


def configure_logging(
    log_file,
    log_level=logging.INFO,
    log_format="%(asctime)s %(name)s %(levelname)s: %(message)s",
):
    """
    Configure the logging module

    :param str,None log_file: target file path. If None use standard error.
    :param int log_level: min log level to be reported in log file.
        Default to INFO
    :param str log_format: format string used for a log line.
        Default to "%(asctime)s %(name)s %(levelname)s: %(message)s"
    """
    warn = None
    handler = logging.StreamHandler()
    if log_file:
        log_file = os.path.abspath(log_file)
        log_dir = os.path.dirname(log_file)
        try:
            mkpath(log_dir)
            handler = logging.handlers.WatchedFileHandler(log_file, encoding="utf-8")
        except (OSError, IOError):
            # fallback to standard error
            warn = (
                "Failed opening the requested log file. "
                "Using standard error instead."
            )
    formatter = logging.Formatter(log_format)
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    if warn:
        # this will be always displayed because the default level is WARNING
        _logger.warn(warn)
    logging.root.setLevel(log_level)


def parse_log_level(log_level):
    """
    Convert a log level to its int representation as required by
    logging module.

    :param log_level: An integer or a string
    :return: an integer or None if an invalid argument is provided
    """
    try:
        log_level_int = int(log_level)
    except ValueError:
        log_level_int = logging.getLevelName(str(log_level).upper())
    if isinstance(log_level_int, int):
        return log_level_int
    return None


# noinspection PyProtectedMember
def get_log_levels():
    """
    Return a list of available log level names
    """
    try:
        level_to_name = logging._levelToName
    except AttributeError:
        level_to_name = dict(
            [
                (key, logging._levelNames[key])
                for key in logging._levelNames
                if isinstance(key, int)
            ]
        )
    for level in sorted(level_to_name):
        yield level_to_name[level]


def pretty_size(size, unit=1024):
    """
    This function returns a pretty representation of a size value

    :param int|long|float size: the number to to prettify
    :param int unit: 1000 or 1024 (the default)
    :rtype: str
    """
    suffixes = ["B"] + [i + {1000: "B", 1024: "iB"}[unit] for i in "KMGTPEZY"]
    if unit == 1000:
        suffixes[1] = "kB"  # special case kB instead of KB
    # cast to float to avoid losing decimals
    size = float(size)
    for suffix in suffixes:
        if abs(size) < unit or suffix == suffixes[-1]:
            if suffix == suffixes[0]:
                return "%d %s" % (size, suffix)
            else:
                return "%.1f %s" % (size, suffix)
        else:
            size /= unit


def human_readable_timedelta(timedelta):
    """
    Given a time interval, returns a human readable string

    :param timedelta: the timedelta to transform in a human readable form
    """
    delta = abs(timedelta)
    # Calculate time units for the given interval
    time_map = {
        "day": int(delta.days),
        "hour": int(delta.seconds / 3600),
        "minute": int(delta.seconds / 60) % 60,
        "second": int(delta.seconds % 60),
    }
    # Build the resulting string
    time_list = []

    # 'Day' part
    if time_map["day"] > 0:
        if time_map["day"] == 1:
            time_list.append("%s day" % time_map["day"])
        else:
            time_list.append("%s days" % time_map["day"])

    # 'Hour' part
    if time_map["hour"] > 0:
        if time_map["hour"] == 1:
            time_list.append("%s hour" % time_map["hour"])
        else:
            time_list.append("%s hours" % time_map["hour"])

    # 'Minute' part
    if time_map["minute"] > 0:
        if time_map["minute"] == 1:
            time_list.append("%s minute" % time_map["minute"])
        else:
            time_list.append("%s minutes" % time_map["minute"])

    # 'Second' part
    if time_map["second"] > 0:
        if time_map["second"] == 1:
            time_list.append("%s second" % time_map["second"])
        else:
            time_list.append("%s seconds" % time_map["second"])

    human = ", ".join(time_list)

    # Take care of timedelta when is shorter than a second
    if delta < datetime.timedelta(seconds=1):
        human = "less than one second"

    # If timedelta is negative append 'ago' suffix
    if delta != timedelta:
        human += " ago"
    return human


def total_seconds(timedelta):
    """
    Compatibility method because the total_seconds method has been introduced
    in Python 2.7

    :param timedelta: a timedelta object
    :rtype: float
    """
    if hasattr(timedelta, "total_seconds"):
        return timedelta.total_seconds()
    else:
        secs = (timedelta.seconds + timedelta.days * 24 * 3600) * 10**6
        return (timedelta.microseconds + secs) / 10.0**6


def timestamp(datetime_value):
    """
    Compatibility method because datetime.timestamp is not available in Python 2.7.

    :param datetime.datetime datetime_value: A datetime object to be converted
        into a timestamp.
    :rtype: float
    """
    try:
        return datetime_value.timestamp()
    except AttributeError:
        return total_seconds(
            datetime_value - datetime.datetime(1970, 1, 1, tzinfo=tz.tzutc())
        )


def range_fun(*args, **kwargs):
    """
    Compatibility method required while we still support Python 2.7.

    This can be removed when Python 2.7 support is dropped and calling code can
    reference `range` directly.
    """
    try:
        return xrange(*args, **kwargs)
    except NameError:
        return range(*args, **kwargs)


def which(executable, path=None):
    """
    This method is useful to find if a executable is present into the
    os PATH

    :param str executable: The name of the executable to find
    :param str|None path: An optional search path to override the current one.
    :return str|None: the path of the executable or None
    """
    # Get the system path if needed
    if path is None:
        path = os.getenv("PATH")
    # If the path is None at this point we have nothing to search
    if path is None:
        return None
    # If executable is an absolute path, check if it exists and is executable
    # otherwise return failure.
    if os.path.isabs(executable):
        if os.path.exists(executable) and os.access(executable, os.X_OK):
            return executable
        else:
            return None
    # Search the requested executable in every directory present in path and
    # return the first occurrence that exists and is executable.
    for file_path in path.split(os.path.pathsep):
        file_path = os.path.join(file_path, executable)
        # If the file exists and is executable return the full path.
        if os.path.exists(file_path) and os.access(file_path, os.X_OK):
            return file_path
    # If no matching file is present on the system return None
    return None


class BarmanEncoder(json.JSONEncoder):
    """
    Custom JSON encoder used for BackupInfo encoding

    This encoder supports the following types:

    * dates and timestamps if they have a ctime() method.
    * objects that implement the 'to_json' method.
    * binary strings (python 3)
    """

    method_list = [
        "_to_json",
        "_datetime_to_str",
        "_timedelta_to_str",
        "_decimal_to_float",
        "binary_to_str",
        "version_to_str",
    ]

    def default(self, obj):
        # Go through all methods until one returns something
        for method in self.method_list:
            res = getattr(self, method)(obj)
            if res is not None:
                return res

        # Let the base class default method raise the TypeError
        return super(BarmanEncoder, self).default(obj)

    @staticmethod
    def _to_json(obj):
        """
        # If the object implements to_json() method use it
        :param obj:
        :return: None|str
        """
        if hasattr(obj, "to_json"):
            return obj.to_json()

    @staticmethod
    def _datetime_to_str(obj):
        """
        Serialise date and datetime objects using ctime() method
        :param obj:
        :return: None|str
        """
        if hasattr(obj, "ctime") and callable(obj.ctime):
            return obj.ctime()

    @staticmethod
    def _timedelta_to_str(obj):
        """
        Serialise timedelta objects using human_readable_timedelta()
        :param obj:
        :return: None|str
        """
        if isinstance(obj, datetime.timedelta):
            return human_readable_timedelta(obj)

    @staticmethod
    def _decimal_to_float(obj):
        """
        Serialise Decimal objects using their string representation
        WARNING: When deserialized they will be treat as float values which have a lower precision
        :param obj:
        :return: None|float
        """
        if isinstance(obj, decimal.Decimal):
            return float(obj)

    @staticmethod
    def binary_to_str(obj):
        """
        Binary strings must be decoded before using them in an unicode string
        :param obj:
        :return: None|str
        """
        if hasattr(obj, "decode") and callable(obj.decode):
            return obj.decode("utf-8", "replace")

    @staticmethod
    def version_to_str(obj):
        """
        Manage (Loose|Strict)Version objects as strings.
        :param obj:
        :return: None|str
        """
        if isinstance(obj, Version):
            return str(obj)


class BarmanEncoderV2(BarmanEncoder):
    """
    This class purpose is to replace default datetime encoding from ctime to isoformat (ISO 8601).
    Next major barman version will use this new format. So this class will be merged back to BarmanEncoder.
    """

    @staticmethod
    def _datetime_to_str(obj):
        """
        Try set output isoformat for this datetime. Date must have tzinfo set.
        :param obj:
        :return: None|str
        """
        if isinstance(obj, datetime.datetime):
            if obj.tzinfo is None:
                raise ValueError(
                    'Got naive datetime. Expecting tzinfo for date: "{}"'.format(obj)
                )
            return obj.isoformat()


def fsync_dir(dir_path):
    """
    Execute fsync on a directory ensuring it is synced to disk

    :param str dir_path: The directory to sync
    :raise OSError: If fail opening the directory
    """
    dir_fd = os.open(dir_path, os.O_DIRECTORY)
    try:
        os.fsync(dir_fd)
    except OSError as e:
        # On some filesystem doing a fsync on a directory
        # raises an EINVAL error. Ignoring it is usually safe.
        if e.errno != errno.EINVAL:
            raise
    finally:
        os.close(dir_fd)


def fsync_file(file_path):
    """
    Execute fsync on a file ensuring it is synced to disk

    Returns the file stats

    :param str file_path: The file to sync
    :return: file stat
    :raise OSError: If something fails
    """
    file_fd = os.open(file_path, os.O_RDONLY)
    file_stat = os.fstat(file_fd)
    try:
        os.fsync(file_fd)
        return file_stat
    except OSError as e:
        # On some filesystem doing a fsync on a O_RDONLY fd
        # raises an EACCES error. In that case we need to try again after
        # reopening as O_RDWR.
        if e.errno != errno.EACCES:
            raise
    finally:
        os.close(file_fd)

    file_fd = os.open(file_path, os.O_RDWR)
    try:
        os.fsync(file_fd)
    finally:
        os.close(file_fd)

    return file_stat


def simplify_version(version_string):
    """
    Simplify a version number by removing the patch level

    :param version_string: the version number to simplify
    :return str: the simplified version number
    """
    if version_string is None:
        return None
    version = version_string.split(".")
    # If a development/beta/rc version, split out the string part
    unreleased = re.search(r"[^0-9.]", version[-1])
    if unreleased:
        last_component = version.pop()
        number = last_component[: unreleased.start()]
        string = last_component[unreleased.start() :]
        version += [number, string]
    return ".".join(version[:-1])


def with_metaclass(meta, *bases):
    """
    Function from jinja2/_compat.py. License: BSD.

    Create a base class with a metaclass.

    :param type meta: Metaclass to add to base class
    """

    # This requires a bit of explanation: the basic idea is to make a
    # dummy metaclass for one level of class instantiation that replaces
    # itself with the actual metaclass.
    class Metaclass(type):
        def __new__(mcs, name, this_bases, d):
            return meta(name, bases, d)

    return type.__new__(Metaclass, "temporary_class", (), {})


@contextmanager
def timeout(timeout_duration):
    """
    ContextManager responsible for timing out the contained
    block of code after a defined time interval.
    """

    # Define the handler for the alarm signal
    def handler(signum, frame):
        raise TimeoutError()

    # set the timeout handler
    previous_handler = signal.signal(signal.SIGALRM, handler)
    if previous_handler != signal.SIG_DFL and previous_handler != signal.SIG_IGN:
        signal.signal(signal.SIGALRM, previous_handler)
        raise AssertionError("Another timeout is already defined")
    # set the timeout duration
    signal.alarm(timeout_duration)
    try:
        # Execute the contained block of code
        yield
    finally:
        # Reset the signal
        signal.alarm(0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)


def is_power_of_two(number):
    """
    Check if a number is a power of two or not
    """

    # Returns None if number is set to None.
    if number is None:
        return None

    # This is a fast method to check for a power of two.
    #
    # A power of two has this structure:  100000 (one or more zeroes)
    # This is the same number minus one:  011111 (composed by ones)
    # This is the bitwise and:            000000
    #
    # This is true only for every power of two
    return number != 0 and (number & (number - 1)) == 0


def file_hash(file_path, buffer_size=1024 * 16, hash_algorithm="sha256"):
    """
    Calculate the checksum for the provided file path with a specific
    hashing algorithm

    :param str file_path: path of the file to read
    :param int buffer_size: read buffer size, default 16k
    :param str hash_algorithm: sha256 | md5
    :return str: Hexadecimal md5 string
    """
    hash_func = hashlib.new(hash_algorithm)
    with open(file_path, "rb") as file_object:
        while 1:
            buf = file_object.read(buffer_size)
            if not buf:
                break
            hash_func.update(buf)
    return hash_func.hexdigest()


# Might be better to use stream instead of full file content. As done in file_hash.
# Might create performance issue for large files.
class ChecksumAlgorithm(with_metaclass(ABCMeta)):
    @abstractmethod
    def checksum(self, value):
        """
        Creates hash hexadecimal string from input byte
        :param value: Value to create checksum from
        :type value: byte

        :return: Return the digest value as a string of hexadecimal digits.
        :rtype: str
        """

    def checksum_from_str(self, value, encoding="utf-8"):
        """
        Creates hash hexadecimal string from input string
        :param value: Value to create checksum from
        :type value: str
        :param encoding: The encoding in which to encode the string.
        :type encoding: str
        :return: Return the digest value as a string of hexadecimal digits.
        :rtype: str
        """
        return self.checksum(value.encode(encoding))

    def get_name(self):
        return self.__class__.__name__


class SHA256(ChecksumAlgorithm):
    def checksum(self, value):
        """
        Creates hash hexadecimal string from input byte
        :param value: Value to create checksum from
        :type value: byte

        :return: Return the digest value as a string of hexadecimal digits.
        :rtype: str
        """
        sha = hashlib.sha256(value)
        return sha.hexdigest()


def force_str(obj, encoding="utf-8", errors="replace"):
    """
    Force any object to an unicode string.

    Code inspired by Django's force_text function
    """
    # Handle the common case first for performance reasons.
    if issubclass(type(obj), _text_type):
        return obj
    try:
        if issubclass(type(obj), _string_types):
            obj = obj.decode(encoding, errors)
        else:
            if sys.version_info[0] >= 3:
                if isinstance(obj, bytes):
                    obj = _text_type(obj, encoding, errors)
                else:
                    obj = _text_type(obj)
            elif hasattr(obj, "__unicode__"):
                obj = _text_type(obj)
            else:
                obj = _text_type(bytes(obj), encoding, errors)
    except (UnicodeDecodeError, TypeError):
        if isinstance(obj, Exception):
            # If we get to here, the caller has passed in an Exception
            # subclass populated with non-ASCII bytestring data without a
            # working unicode method. Try to handle this without raising a
            # further exception by individually forcing the exception args
            # to unicode.
            obj = " ".join(force_str(arg, encoding, errors) for arg in obj.args)
        else:
            # As last resort, use a repr call to avoid any exception
            obj = repr(obj)
    return obj


def redact_passwords(text):
    """
    Redact passwords from the input text.

    Password are found in these two forms:

    Keyword/Value Connection Strings:
    - host=localhost port=5432 dbname=mydb password=SHAME_ON_ME
    Connection URIs:
    - postgresql://[user[:password]][netloc][:port][/dbname]

    :param str text: Input content
    :return: String with passwords removed
    """

    # Remove passwords as found in key/value connection strings
    text = re.sub("password=('(\\'|[^'])+'|[^ '\"]*)", "password=*REDACTED*", text)

    # Remove passwords in connection URLs
    text = re.sub(r"(?<=postgresql:\/\/)([^ :@]+:)([^ :@]+)?@", r"\1*REDACTED*@", text)

    return text


def check_non_negative(value):
    """
    Check for a positive integer option

    :param value: str containing the value to check
    """
    if value is None:
        return None
    try:
        int_value = int(value)
    except Exception:
        raise ArgumentTypeError("'%s' is not a valid non negative integer" % value)
    if int_value < 0:
        raise ArgumentTypeError("'%s' is not a valid non negative integer" % value)
    return int_value


def check_positive(value):
    """
    Check for a positive integer option

    :param value: str containing the value to check
    """
    if value is None:
        return None
    try:
        int_value = int(value)
    except Exception:
        raise ArgumentTypeError("'%s' is not a valid input" % value)
    if int_value < 1:
        raise ArgumentTypeError("'%s' is not a valid positive integer" % value)
    return int_value


def check_aws_expiration_date_format(value):
    """
    Check user input for aws expiration date timestamp with a specific format.

    :param value: str containing the value to check.
    :raise ValueError: Fails with an invalid date.
    """
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
    try:
        # Attempt to parse the input date string into a datetime object
        return datetime.datetime.strptime(value, fmt)
    except ValueError:
        raise ArgumentTypeError(
            "Invalid date: '%s'. Expected format is '%s'." % (value, fmt)
        )


def check_aws_snapshot_lock_duration_range(value):
    """
    Check for AWS Snapshot Lock duration range option

    :param value: str containing the value to check
    """
    if value is None:
        return None
    try:
        int_value = int(value)
    except Exception:
        raise ArgumentTypeError("'%s' is not a valid input" % value)

    if not 1 <= int_value <= 36500:
        raise ValueError(
            "aws_snapshot_lock_duration must be between 1 and 36,500 days."
        )

    return int_value


def check_aws_snapshot_lock_cool_off_period_range(value):
    """
    Check for AWS Snapshot Lock cool-off period range option

    :param value: str containing the value to check
    """
    if value is None:
        return None
    try:
        int_value = int(value)
    except Exception:
        raise ArgumentTypeError("'%s' is not a valid input" % value)

    if not 1 <= int_value <= 72:
        raise ValueError(
            "aws_snapshot_lock_cool_off_period must be between 1 and 72 hours."
        )

    return int_value


def check_aws_snapshot_lock_mode(value):
    """
    Replication slot names may only contain lower case letters, numbers,
    and the underscore character. This function parse a replication slot name

    :param str value: slot_name value
    :return:
    """

    AWS_SNAPSHOT_LOCK_MODE = ["governance", "compliance"]
    if value is None:
        return None

    value = value.lower()
    if value not in AWS_SNAPSHOT_LOCK_MODE:
        raise ValueError(
            "Invalid AWS snapshot lock mode. "
            "Please specify either 'governance' or 'compliance'. "
            "Ensure that the mode you choose aligns with your snapshot locking "
            "requirements."
        )
    return value


def check_tli(value):
    """
    Check for a positive integer option, and also make "current" and "latest" acceptable values

    :param value: str containing the value to check
    """
    if value is None:
        return None
    if value in ["current", "latest"]:
        return value
    else:
        return check_positive(value)


def check_size(value):
    """
    Check user input for a human readable size

    :param value: str containing the value to check
    """
    if value is None:
        return None
    # Ignore cases
    value = value.upper()
    try:
        # If value ends with `B` we try to parse the multiplier,
        # otherwise it is a plain integer
        if value[-1] == "B":
            # By default we use base=1024, if the value ends with `iB`
            # it is a SI value and we use base=1000
            if value[-2] == "I":
                base = 1000
                idx = 3
            else:
                base = 1024
                idx = 2
            multiplier = base
            # Parse the multiplicative prefix
            for prefix in "KMGTPEZY":
                if value[-idx] == prefix:
                    int_value = int(float(value[:-idx]) * multiplier)
                    break
                multiplier *= base
            else:
                # If we do not find the prefix, remove the unit
                # and try to parse the remainder as an integer
                # (e.g. '1234B')
                int_value = int(value[: -idx + 1])
        else:
            int_value = int(value)
    except ValueError:
        raise ArgumentTypeError("'%s' is not a valid size string" % value)
    if int_value is None or int_value < 1:
        raise ArgumentTypeError("'%s' is not a valid size string" % value)
    return int_value


def check_backup_name(backup_name):
    """
    Verify that a backup name is not a backup ID or reserved identifier.

    Returns the backup name if it is a valid backup name and raises an exception
    otherwise. A backup name is considered valid if it is not None, not empty,
    does not match the backup ID format and is not any other reserved backup
    identifier.

    :param str backup_name: The backup name to be checked.
    :return str: The backup name.
    """
    if backup_name is None:
        raise ArgumentTypeError("Backup name cannot be None")
    if backup_name == "":
        raise ArgumentTypeError("Backup name cannot be empty")
    if is_backup_id(backup_name):
        raise ArgumentTypeError(
            "Backup name '%s' is not allowed: backup ID" % backup_name
        )
    if backup_name in (RESERVED_BACKUP_IDS):
        raise ArgumentTypeError(
            "Backup name '%s' is not allowed: reserved word" % backup_name
        )
    return backup_name


def is_backup_id(backup_id):
    """
    Checks whether the supplied identifier is a backup ID.

    :param str backup_id: The backup identifier to check.
    :return bool: True if the backup matches the backup ID regex, False otherwise.
    """
    return bool(re.match(r"(\d{8})T\d{6}$", backup_id))


def get_backup_info_from_name(backups, backup_name):
    """
    Get the backup metadata for the named backup.

    :param list[BackupInfo] backups: A list of BackupInfo objects which should be
        searched for the named backup.
    :param str backup_name: The name of the backup for which the backup metadata
        should be retrieved.
    :return BackupInfo|None: The backup metadata for the named backup.
    """
    matching_backups = [
        backup for backup in backups if backup.backup_name == backup_name
    ]
    if len(matching_backups) > 1:
        matching_backup_ids = " ".join(
            [backup.backup_id for backup in matching_backups]
        )
        msg = (
            "Multiple backups found matching name '%s' "
            "(try using backup ID instead): %s"
        ) % (backup_name, matching_backup_ids)

        raise ValueError(msg)
    elif len(matching_backups) == 1:
        return matching_backups[0]


def get_backup_id_using_shortcut(server, shortcut, BackupInfo):
    """
    Get backup ID from one of Barman shortcuts.

    :param str server: The obj where to look from.
    :param str shortcut: pattern to search.
    :param BackupInfo BackupInfo: Place where we keep some Barman constants.
    :return str backup_id|None: The backup ID for the provided shortcut.
    """
    backup_id = None

    if shortcut in ("latest", "last"):
        backup_id = server.get_last_backup_id()
    elif shortcut in ("oldest", "first"):
        backup_id = server.get_first_backup_id()
    elif shortcut in ("last-failed"):
        backup_id = server.get_last_backup_id([BackupInfo.FAILED])
    elif shortcut in ("latest-full", "last-full"):
        backup_id = server.get_last_full_backup_id()
    elif is_backup_id(shortcut):
        backup_id = shortcut

    return backup_id


def lock_files_cleanup(lock_dir, lock_directory_cleanup):
    """
    Get all the lock files in the lock directory
    and try to acquire every single one.
    If the file is not locked, remove it.

    This method is part of cron and should help
    keeping clean the lockfile directory.
    """
    if not lock_directory_cleanup:
        # Auto cleanup of lockfile directory disabled.
        # Log for debug only and return
        _logger.debug("Auto-cleanup of '%s' directory disabled" % lock_dir)
        return
    _logger.info("Cleaning up lockfiles directory.")
    for filename in glob(os.path.join(lock_dir, ".*.lock")):
        lock = lockfile.LockFile(filename, raise_if_fail=False, wait=False)
        with lock as locked:
            # if we have the lock we can remove the file
            if locked:
                try:
                    _logger.debug("deleting %s" % filename)
                    os.unlink(filename)
                    _logger.debug("%s deleted" % filename)
                except FileNotFoundError:
                    # IF we are trying to remove an already removed file, is not
                    # a big deal, just pass.
                    pass
            else:
                _logger.debug(
                    "%s file lock already acquired, skipping removal" % filename
                )


def edit_config(file, section, option, value, lines=None):
    """
    Utility method that given a file and a config section allows to:
        - add a new section if at least a key-value content is provided
        - add a new key-value to a config section
        - change a section value

    :param file: the path to the file to edit
    :type file: str
    :param section: the config section to edit or to add
    :type section: str
    :param option: the config key to edit or add
    :type option: str
    :param value: the value for the config key to update or add
    :type value: str
    :param lines: optional parameter containing the set of lines of the file to update
    :type lines: list
    :return: the updated lines of the file
    """
    conf_section = False
    idx = 0

    if lines is None:
        try:
            with open(file, "r") as config:
                lines = config.readlines()
        except FileNotFoundError:
            lines = []
    eof = len(lines) - 1
    for idx, line in enumerate(lines):
        # next section
        if conf_section and line.strip().startswith("["):
            lines.insert(idx - 1, option + " = " + value)
            break
        # Option found, update value
        elif conf_section and line.strip().replace(" ", "").startswith(option + "="):
            lines.pop(idx)
            lines.insert(idx, option + " = " + value + "\n")
            break
        # End of file reached, append lines
        elif conf_section and idx == eof:
            lines.append(option + " = " + value + "\n")
            break
        # Section found
        if line.strip() == "[" + section + "]":
            conf_section = True
    # Section not found, create a new section and append option
    if not conf_section:
        # Note: we need to use 2 append, otherwise the section matching is not
        # going to work
        lines.append("[" + section + "]\n")
        lines.append(option + " = " + value + "\n")
    return lines


def get_backup_id_from_target_time(available_backups, target_time, target_tli=None):
    """
    Get backup ID from the catalog based on the recovery_target *target_time* and
    *target_tli*.

    :param list[BackupInfo] available_backups: List of BackupInfo objects.
    :param str target_time: The target value with timestamp format
        ``%Y-%m-%d %H:%M:%S`` with or without timezone.
    :param int|None target_tli: Target timeline value, if a specific one is required.
    :return str|None: ID of the backup.
    :raises:
        :exc:`ValueError`: If *target_time* is an invalid value.
    """
    from barman.infofile import load_datetime_tz

    try:
        parsed_target = load_datetime_tz(target_time)
    except Exception as e:
        raise ValueError(
            "Unable to parse the target time parameter %r: %s" % (target_time, e)
        )
    for candidate_backup in sorted(
        available_backups, key=lambda backup: backup.backup_id, reverse=True
    ):
        if candidate_backup.end_time <= parsed_target:
            if target_tli is not None and candidate_backup.timeline != target_tli:
                continue
            return candidate_backup.backup_id
    return None


def get_backup_id_from_target_lsn(available_backups, target_lsn, target_tli=None):
    """
    Get backup ID from the catalog based on the recovery_target *target_lsn* and
    *target_tli*.

    :param list[BackupInfo] available_backups: List of BackupInfo objects.
    :param str target_lsn: The target value with lsn format, e.g.,
        ``3/64000000``.
    :param int|None target_tli: Target timeline value, if a specific one is required.
    :return str|None: ID of the backup.
    """
    parsed_target = xlog.parse_lsn(target_lsn)
    for candidate_backup in sorted(
        available_backups, key=lambda backup: backup.backup_id, reverse=True
    ):
        if xlog.parse_lsn(candidate_backup.end_xlog) <= parsed_target:
            if target_tli is not None and candidate_backup.timeline != target_tli:
                continue
            return candidate_backup.backup_id
    return None


def get_last_backup_id(available_backups):
    """
    Get last backup ID from the catalog.

    :param list[BackupInfo] available_backups: List of BackupInfo objects.
    :return str|None: ID of the backup.
    """
    if len(available_backups) == 0:
        return None

    backups = sorted(available_backups, key=lambda backup: backup.backup_id)
    return backups[-1].backup_id


def get_backup_id_from_target_tli(available_backups, target_tli):
    """
    Get backup ID from the catalog based on the recovery_target *target_tli*.

    :param list[BackupInfo] available_backups: Dict values of BackupInfo objects.
    :param int target_tli: Target timeline value.
    :return str|None: ID of the backup.
    """
    if len(available_backups) == 0:
        return None

    for candidate_backup in sorted(
        available_backups, key=lambda backup: backup.backup_id, reverse=True
    ):
        if candidate_backup.timeline == target_tli:
            return candidate_backup.backup_id
    return None


def parse_target_tli(obj, target_tli, backup_info=None):
    """
    Parse target timeline shorcut, ``latest`` and ``current``.

    .. note::
        This method is used in two other methods that are part of the recovery of a
        a backup (:meth:`_set_pitr_targets` and :meth:`get_required_xlog_files`).
        When called with a *backup_info*, it means that the recover operation uses a
        specific backup for recovery and ``current`` is allowed in this case because
        this backup is considered the ``current`` backup.
        When called without a *backup_info*, it means that the recover operation is
        going to fetch the best backup for recovery, so ``current`` is not allowed
        because there is no ``current`` backup.

        This method can also be used in the cloud script to retrieve the latest WAL
        from the cloud catalog when shortcut is ``latest``.

    :param BackupManager|CloudBackupCatalog obj: A BackupManager or a CloudBackupCatalog
        object.
    :param str|int target_tli: Target timeline value. Accepts both an integer
        representing the timeline, or keywords accepted by Postgres, such as ``current``
        and ``latest``.
    :param None|BackupInfo backup_info: Backup info object.
    :return int|None: ID of the timeline.
    :raise ValueError: if *target_tli* is an invalid value.
    """
    parsed_target_tli = target_tli
    if target_tli and type(target_tli) is str:
        if target_tli == "current":
            if backup_info is None:
                raise ValueError(
                    "'%s' is not a valid timeline keyword when recovering"
                    " without a backup_id" % target_tli
                )
            else:
                parsed_target_tli = backup_info.timeline
        elif target_tli == "latest":
            valid_timelines = obj.get_latest_archived_wals_info()
            parsed_target_tli = int(max(valid_timelines.keys()), 16)
        elif target_tli.isdigit():
            parsed_target_tli = int(target_tli)
        else:
            raise ValueError("'%s' is not a valid timeline keyword" % target_tli)
    return parsed_target_tli
