# Copyright (C) 2011-2014 2ndQuadrant Italia (Devise.IT S.r.L.)
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

import logging
import logging.handlers
import os
import pwd
import grp
import json
from barman.retention_policies import RetentionPolicy

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
    os.setegid(pw.pw_gid)
    os.seteuid(pw.pw_uid)
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
    Convert a log level to its int representation as required by logging module.

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

    :param int,long,float size: the number to to prettify
    :param int unit: 1000 or 1024 (the default)
    :rtype : str
    """
    suffixes = ["B"] + [i + {1000: "B", 1024: "iB"}[unit] for i in "KMGTPEZY"]
    if unit == 1000:
        suffixes[1] = 'kB'  # special case kB instead of KB
    # cast to float to avoid loosing decimals
    size = float(size)
    for suffix in suffixes:
        if size < unit or suffix == suffixes[-1]:
            if suffix == suffixes[0]:
                return "%d %s" % (size, suffix)
            else:
                return "%.1f %s" % (size, suffix)
        else:
            size /= unit


class BarmanEncoder(json.JSONEncoder):
    """
    Custom JSON encoder used for BackupInfo encoding

    This encoder is able to serialize dates and timestamps if
    they have a ctime() method.

    This encoder is able to serialize RetentionPolicy objects
    """
    def default(self, obj):
        if hasattr(obj, 'ctime') and callable(obj.ctime):
            return obj.ctime()
        if isinstance(obj, RetentionPolicy):
            return "%s %s" % (obj.mode, obj.value )
        # Let the base class default method raise the TypeError
        return super(BarmanEncoder, self).default(obj)
