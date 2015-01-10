# Copyright (C) 2011-2015 2ndQuadrant Italia (Devise.IT S.r.L.)
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

""" This module contains functions to retrieve information
about xlog files
"""

import os
import re

# xlog file segment name parser (regular expression)
_xlog_re = re.compile(r'''
    ^
    ([\dA-Fa-f]{8})                    # everything has a timeline
    (?:
        ([\dA-Fa-f]{8})([\dA-Fa-f]{8}) # segment name, if a wal file
        (?:\.[\dA-Fa-f]{8}\.backup)?   # and optional offset, if a backup label
    |
        \.history                      # or only .history, if a history file
    )
    $
    ''', re.VERBOSE)
# xlog location parser for concurrent backup (regular expression)
_location_re = re.compile(r'^([\dA-F]+)/([\dA-F]+)$')

# Taken from xlog_internal.h from PostgreSQL sources
XLOG_SEG_SIZE = 1 << 24
XLOG_SEG_PER_FILE = 0xffffffff // XLOG_SEG_SIZE
XLOG_FILE_SIZE = XLOG_SEG_SIZE * XLOG_SEG_PER_FILE


class BadXlogSegmentName(Exception):
    """
    Exception for a bad xlog name
    """
    pass


def is_any_xlog_file(path):
    """
    Return True if the xlog is either a WAL segment, a .backup file
    or a .history file, False otherwise.

    It supports either a full file path or a simple file name.

    :param str path: the file name to test
    :rtype: bool
    """
    match = _xlog_re.match(os.path.basename(path))
    if match:
        return True
    return False


def is_history_file(path):
    """
    Return True if the xlog is a .history file, False otherwise

    It supports either a full file path or a simple file name.

    :param str path: the file name to test
    :rtype: bool
    """
    match = _xlog_re.search(os.path.basename(path))
    if match and match.group(0).endswith('.history'):
        return True
    return False


def is_backup_file(path):
    """
    Return True if the xlog is a .backup file, False otherwise

    It supports either a full file path or a simple file name.

    :param str path: the file name to test
    :rtype: bool
    """
    match = _xlog_re.search(os.path.basename(path))
    if match and match.group(0).endswith('.backup'):
        return True
    return False


def is_wal_file(path):
    """
    Return True if the xlog is a regular xlog file, False otherwise

    It supports either a full file path or a simple file name.

    :param str path: the file name to test
    :rtype: bool
    """
    match = _xlog_re.search(os.path.basename(path))
    if match \
            and not match.group(0).endswith('.backup')\
            and not match.group(0).endswith('.history'):
        return True
    return False


def decode_segment_name(path):
    """
    Retrieve the timeline, log ID and segment ID from the name of a xlog segment

    It can handle either a full file path or a simple file name.

    :param str path: the file name to decode
    :rtype: list[int]
    """
    match = _xlog_re.match(os.path.basename(path))
    if not match:
        raise BadXlogSegmentName("invalid xlog segment name '%s'" % path)
    return [int(x, 16) if x else None for x in match.groups()]


def encode_segment_name(tli, log, seg):
    """
    Build the xlog segment name based on timeline, log ID and segment ID

    :param int tli: timeline number
    :param int log: log number
    :param int seg: segment number
    :return str: segment file name
    """
    return "%08X%08X%08X" % (tli, log, seg)


def encode_history_file_name(tli):
    """
    Build the history file name based on timeline

    :return str: history file name
    """
    return "%08X.history" % (tli,)


def enumerate_segments(begin, end, version):
    """
    Get the list of xlog segments from begin to end (included)

    :param str begin: begin segment name
    :param str end: end segment name
    :param int version: postgres version as an integer (e.g. 90301 for 9.3.1)
    :rtype: collections.Iterable[str]
    """
    begin_tli, begin_log, begin_seg = decode_segment_name(begin)
    end_tli, end_log, end_seg = decode_segment_name(end)

    # this method don't support timeline changes
    assert begin_tli == end_tli, (
        "Begin segment (%s) and end segment (%s) "
        "must have the same timeline part" % (begin, end))

    # Start from the first xlog and sequentially enumerates the segments
    # to the end
    cur_log, cur_seg = begin_log, begin_seg
    while cur_log < end_log or (cur_log == end_log and cur_seg <= end_seg):
        yield encode_segment_name(begin_tli, cur_log, cur_seg)
        cur_seg += 1
        if cur_seg > XLOG_SEG_PER_FILE or (
                version < 90300 and cur_seg == XLOG_SEG_PER_FILE):
            cur_seg = 0
            cur_log += 1


def hash_dir(path):
    """
    Get the directory where the xlog segment will be stored

    It can handle either a full file path or a simple file name.

    :param str|unicode path: xlog file name
    :return str: directory name
    """
    tli, log, _ = decode_segment_name(path)
    # tli is always not None
    if log is not None:
        return "%08X%08X" % (tli, log)
    else:
        return ''


def get_offset_from_location(location):
    """
    Calculate a xlog segment offset starting from a xlog location.

    :param location: a complete xlog location
    :return int: a xlog segment offset
    """
    match = _location_re.match(location)
    if match:
        xlo = int(match.group(2), 16)
        return xlo % XLOG_SEG_SIZE
    else:
        return None
