#
# barman - Backup and Recovery Manager for PostgreSQL
#
# Copyright (C) 2011  Devise.IT S.r.l. <info@2ndquadrant.it>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import re

_xlog_re = re.compile(r'^([\dA-Fa-f]{8})(?:([\dA-Fa-f]{8})([\dA-Fa-f]{8})(?:\.[\dA-Fa-f]{8}\.backup)?|\.history)$')
"""
xlog file segment name parser (regular expression)
"""

# Taken from xlog_internal.h from PostgreSQL sources
XLOG_SEG_SIZE = 1 << 24
XLOG_SEG_PER_FILE = 0xffffffff / XLOG_SEG_SIZE
XLOG_FILE_SIZE = XLOG_SEG_SIZE * XLOG_SEG_PER_FILE

class BadXlogSegmentName(Exception):
    pass

def is_history_file(name):
    return type(name) == str and name.endswith('.history')

def is_backup_file(name):
    return type(name) == str and name.endswith('.backup')

def is_wal_file(name):
    return not is_backup_file(name) and not is_history_file(name)

def decode_segment_name(name):
    """
    Retrieve the timeline, log ID and segment ID from the name of a xlog segment
    """
    match = _xlog_re.match(name)
    if not match:
        raise BadXlogSegmentName, "invalid xlog segmant name '%s'" % name
    return [int(x, 16) if x else None for x in match.groups()]

def encode_segment_name(tli, log, seg):
    """
    Build the xlog segment name based on timeline, log ID and segment ID
    """
    return "%08X%08X%08X" % (tli, log, seg)

def encode_history_file_name(tli):
    """
    Build the history file name based on timeline
    """
    return "%08X.history" % (tli)

def enumerate_segments(begin, end):
    """
    Get the list of xlog segments from begin to end (included)
    """
    begin_tli, begin_log, begin_seg = decode_segment_name(begin)
    end_tli, end_log, end_seg = decode_segment_name(end)
    assert begin_tli == end_tli, "Begin segment (%s) and end segment (%s) must have the same timeline part" % (begin, end) # this method don't support timeline changes

    # Start from the first xlog and sequentially enumerates the segments to the end
    cur_log, cur_seg = begin_log, begin_seg
    while cur_log < end_log or (cur_log == end_log and cur_seg <= end_seg):
        yield encode_segment_name(begin_tli, cur_log, cur_seg)
        cur_seg += 1
        if cur_seg >= XLOG_SEG_PER_FILE:
            cur_seg = 0
            cur_log += 1

def hash_dir(name):
    """
    Get the directory where the xlog segment will be stored
    """
    _, log, _ = decode_segment_name(name)
    if log != None:
        return name[0:16]
    else:
        return ''
