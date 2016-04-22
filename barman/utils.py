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

"""
This module contains utility functions used in Barman.
"""

import datetime
import decimal
import errno
import grp
import json
import logging
import logging.handlers
import os
import pwd

_logger = logging.getLogger(__name__)


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
    os.environ['HOME'] = pw.pw_dir


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
        log_format="%(asctime)s %(name)s %(levelname)s: %(message)s"):
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
            handler = logging.handlers.WatchedFileHandler(log_file)
        except (OSError, IOError):
            # fallback to standard error
            warn = "Failed opening the requested log file. " \
                   "Using standard error instead."
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


def pretty_size(size, unit=1024):
    """
    This function returns a pretty representation of a size value

    :param int|long|float size: the number to to prettify
    :param int unit: 1000 or 1024 (the default)
    :rtype: str
    """
    suffixes = ["B"] + [i + {1000: "B", 1024: "iB"}[unit] for i in "KMGTPEZY"]
    if unit == 1000:
        suffixes[1] = 'kB'  # special case kB instead of KB
    # cast to float to avoid loosing decimals
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
        'day': int(delta.days % 365),
        'hour': int(delta.seconds / 3600),
        'minute': int(delta.seconds / 60) % 60,
    }
    # Build the resulting string
    time_list = []

    # 'Day' part
    if time_map['day'] > 0:
        if time_map['day'] == 1:
            time_list.append('%s day' % time_map['day'])
        else:
            time_list.append('%s days' % time_map['day'])

    # 'Hour' part
    if time_map['hour'] > 0:
        if time_map['hour'] == 1:
            time_list.append('%s hour' % time_map['hour'])
        else:
            time_list.append('%s hours' % time_map['hour'])

    # 'Minute' part
    if time_map['minute'] > 0:
        if time_map['minute'] == 1:
            time_list.append('%s minute' % time_map['minute'])
        else:
            time_list.append('%s minutes' % time_map['minute'])

    human = ', '.join(time_list)
    # If timedelta is negative append 'ago' suffix
    if delta != timedelta:
        human += " ago"
    return human


def which(executable, path=None):
    """
    This method is useful to find if a executable is present into the
    os PATH

    :param str executable: The name of the executable to find
    :param str|None path: An optional search path to override the current one.
    :return str|None: the path of the executable or None
    """
    # Get the system path and split.
    if path is None:
        path = os.getenv('PATH')
    # If executable is an absolute path, check if it exists and is executable
    # otherwise return failure.
    if os.path.isabs(executable):
        if os.path.exists(executable) and os.access(executable, os.X_OK):
            return executable
        else:
            return None
    # Search the requested executable in eery directory present in path and
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
    def default(self, obj):
        # If the object implements to_json() method use it
        if hasattr(obj, 'to_json'):
            return obj.to_json()
        # Serialise date and datetime objects using ctime() method
        if hasattr(obj, 'ctime') and callable(obj.ctime):
            return obj.ctime()
        # Serialise timedelta objects using human_readable_timedelta()
        if isinstance(obj, datetime.timedelta):
            return human_readable_timedelta(obj)
        # Serialise Decimal objects using their string representation
        # WARNING: When deserialized they will be treat as float values
        # which have a lower precision
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        # Binary strings must be decoded before using them in
        # an unicode string
        if hasattr(obj, 'decode') and callable(obj.decode):
            return obj.decode('utf-8', 'replace')
        # Let the base class default method raise the TypeError
        return super(BarmanEncoder, self).default(obj)


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
    os.close(dir_fd)


def simplify_version(version_string):
    """
    Simplify a version number using a major.minor format

    :param version_string: the version number to simplify
    :return str: the simplified version number
    """
    if version_string is None:
        return None
    version = version_string.split('.')
    return '.'.join(version[:2])


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
    return type.__new__(Metaclass, 'temporary_class', (), {})
