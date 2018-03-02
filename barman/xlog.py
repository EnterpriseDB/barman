# Copyright (C) 2011-2018 2ndQuadrant Limited
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
This module contains functions to retrieve information about xlog
files
"""

import collections
import os
import re
from tempfile import NamedTemporaryFile

from barman.exceptions import BadHistoryFileContents, BadXlogSegmentName

# xlog file segment name parser (regular expression)
_xlog_re = re.compile(r'''
    ^
    ([\dA-Fa-f]{8})                    # everything has a timeline
    (?:
        ([\dA-Fa-f]{8})([\dA-Fa-f]{8}) # segment name, if a wal file
        (?:                            # and optional
            \.[\dA-Fa-f]{8}\.backup    # offset, if a backup label
        |
            \.partial                  # partial, if a partial file
        )?
    |
        \.history                      # or only .history, if a history file
    )
    $
    ''', re.VERBOSE)

# xlog location parser for concurrent backup (regular expression)
_location_re = re.compile(r'^([\dA-F]+)/([\dA-F]+)$')

# Taken from xlog_internal.h from PostgreSQL sources

#: XLOG_SEG_SIZE is the size of a single WAL file.  This must be a power of 2
#: and larger than XLOG_BLCKSZ (preferably, a great deal larger than
#: XLOG_BLCKSZ).
DEFAULT_XLOG_SEG_SIZE = 1 << 24

#: This namedtuple is a container for the information
#: contained inside history files
HistoryFileData = collections.namedtuple(
    'HistoryFileData',
    'tli parent_tli switchpoint reason')


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


def is_partial_file(path):
    """
    Return True if the xlog is a .partial file, False otherwise

    It supports either a full file path or a simple file name.

    :param str path: the file name to test
    :rtype: bool
    """
    match = _xlog_re.search(os.path.basename(path))
    if match and match.group(0).endswith('.partial'):
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
    if (match and
            not match.group(0).endswith('.backup') and
            not match.group(0).endswith('.history') and
            not match.group(0).endswith('.partial')):
        return True
    return False


def decode_segment_name(path):
    """
    Retrieve the timeline, log ID and segment ID
    from the name of a xlog segment

    It can handle either a full file path or a simple file name.

    :param str path: the file name to decode
    :rtype: list[int]
    """
    name = os.path.basename(path)
    match = _xlog_re.match(name)
    if not match:
        raise BadXlogSegmentName(name)
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


def xlog_segments_per_file(xlog_segment_size):
    """
    Given that WAL files are named using the following pattern:

        <timeline_number><xlog_file_number><xlog_segment_number>

    this is the number of XLOG segments in an XLOG file. By XLOG file
    we don't mean an actual file on the filesystem, but the definition
    used in the PostgreSQL sources: meaning a set of files containing the
    same file number.

    :param int xlog_segment_size: The XLOG segment size in bytes
    :return int: The number of segments in an XLOG file
    """
    return 0xffffffff // xlog_segment_size


def xlog_file_size(xlog_segment_size):
    """
    Given that WAL files are named using the following pattern:

        <timeline_number><xlog_file_number><xlog_segment_number>

    this is the size in bytes of an XLOG file, which is composed on many
    segments. See the documentation of `xlog_segments_per_file` for a
    commentary on the definition of `XLOG` file.

    :param int xlog_segment_size: The XLOG segment size in bytes
    :return int: The size of an XLOG file
    """
    return xlog_segment_size * xlog_segments_per_file(xlog_segment_size)


def generate_segment_names(begin, end=None, version=None,
                           xlog_segment_size=None):
    """
    Generate a sequence of XLOG segments starting from ``begin``
    If an ``end`` segment is provided the sequence will terminate after
    returning it, otherwise the sequence will never terminate.

    If the XLOG segment size is known, this generator is precise,
    switching to the next file when required.

    It the XLOG segment size is unknown, this generator will generate
    all the possible XLOG file names.
    The size of an XLOG segment can be every power of 2 between
    the XLOG block size (8Kib) and the size of a log segment (4Gib)

    :param str begin: begin segment name
    :param str|None end: optional end segment name
    :param int|None version: optional postgres version as an integer
        (e.g. 90301 for 9.3.1)
    :param int xlog_segment_size: the size of a XLOG segment
    :rtype: collections.Iterable[str]
    :raise: BadXlogSegmentName
    """
    begin_tli, begin_log, begin_seg = decode_segment_name(begin)
    end_tli, end_log, end_seg = None, None, None
    if end:
        end_tli, end_log, end_seg = decode_segment_name(end)

        # this method doesn't support timeline changes
        assert begin_tli == end_tli, (
            "Begin segment (%s) and end segment (%s) "
            "must have the same timeline part" % (begin, end))

    # If version is less than 9.3 the last segment must be skipped
    skip_last_segment = version is not None and version < 90300

    # This is the number of XLOG segments in an XLOG file. By XLOG file
    # we don't mean an actual file on the filesystem, but the definition
    # used in the PostgreSQL sources: a set of files containing the
    # same file number.
    if xlog_segment_size:
        # The generator is operating is precise and correct mode:
        # knowing exactly when a switch to the next file is required
        xlog_seg_per_file = xlog_segments_per_file(xlog_segment_size)
    else:
        # The generator is operating only in precise mode: generating every
        # possible XLOG file name.
        xlog_seg_per_file = 0x7ffff

    # Start from the first xlog and generate the segments sequentially
    # If ``end`` has been provided, the while condition ensure the termination
    # otherwise this generator will never stop
    cur_log, cur_seg = begin_log, begin_seg
    while end is None or \
            cur_log < end_log or \
            (cur_log == end_log and cur_seg <= end_seg):
        yield encode_segment_name(begin_tli, cur_log, cur_seg)
        cur_seg += 1
        if cur_seg > xlog_seg_per_file or (
                skip_last_segment and cur_seg == xlog_seg_per_file):
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


def parse_lsn(lsn_string):
    """
    Transform a string XLOG location, formatted as %X/%X, in the corresponding
    numeric representation

    :param str lsn_string: the string XLOG location, i.e. '2/82000168'
    :rtype: int
    """
    lsn_list = lsn_string.split('/')
    if len(lsn_list) != 2:
        raise ValueError('Invalid LSN: %s', lsn_string)

    return (int(lsn_list[0], 16) << 32) + int(lsn_list[1], 16)


def diff_lsn(lsn_string1, lsn_string2):
    """
    Calculate the difference in bytes between two string XLOG location,
    formatted as %X/%X

    Tis function is a Python implementation of
    the ``pg_xlog_location_diff(str, str)`` PostgreSQL function.

    :param str lsn_string1: the string XLOG location, i.e. '2/82000168'
    :param str lsn_string2: the string XLOG location, i.e. '2/82000168'
    :rtype: int
    """

    # If one the input is None returns None
    if lsn_string1 is None or lsn_string2 is None:
        return None
    return parse_lsn(lsn_string1) - parse_lsn(lsn_string2)


def format_lsn(lsn):
    """
    Transform a numeric XLOG location, in the corresponding %X/%X string
    representation

    :param int lsn: numeric XLOG location
    :rtype: str
    """
    return "%X/%X" % (lsn >> 32, lsn & 0xFFFFFFFF)


def location_to_xlogfile_name_offset(location, timeline, xlog_segment_size):
    """
    Convert transaction log location string to file_name and file_offset

    This is a reimplementation of pg_xlogfile_name_offset PostgreSQL function

    This method returns a dictionary containing the following data:

         * file_name
         * file_offset

    :param str location: XLOG location
    :param int timeline: timeline
    :param int xlog_segment_size: the size of a XLOG segment
    :rtype: dict
    """
    lsn = parse_lsn(location)
    log = lsn >> 32
    seg = (lsn & xlog_file_size(xlog_segment_size)) >> 24
    offset = lsn & 0xFFFFFF
    return {
        'file_name': encode_segment_name(timeline, log, seg),
        'file_offset': offset,
    }


def location_from_xlogfile_name_offset(file_name, file_offset):
    """
    Convert file_name and file_offset to a transaction log location.

    This is the inverted function of PostgreSQL's pg_xlogfile_name_offset
    function.

    :param str file_name: a WAL file name
    :param int file_offset: a numeric offset
    :rtype: str
    """
    decoded_segment = decode_segment_name(file_name)
    location = ((decoded_segment[1] << 32) +
                (decoded_segment[2] << 24) +
                file_offset)
    return format_lsn(location)


def decode_history_file(wal_info, comp_manager):
    """
    Read an history file and parse its contents.

    Each line in the file represents a timeline switch, each field is
    separated by tab, empty lines are ignored and lines starting with '#'
    are comments.

    Each line is composed by three fields: parentTLI, switchpoint and reason.
    "parentTLI" is the ID of the parent timeline.
    "switchpoint" is the WAL position where the switch happened
    "reason" is an human-readable explanation of why the timeline was changed

    The method requires a CompressionManager object to handle the eventual
     compression of the history file.

    :param barman.infofile.WalFileInfo wal_info: history file obj
    :param comp_manager: compression manager used in case
        of history file compression
    :return List[HistoryFileData]: information from the history file
    """

    path = wal_info.orig_filename
    # Decompress the file if needed
    if wal_info.compression:
        # Use a NamedTemporaryFile to avoid explicit cleanup
        uncompressed_file = NamedTemporaryFile(
            dir=os.path.dirname(path),
            prefix='.%s.' % wal_info.name,
            suffix='.uncompressed')
        path = uncompressed_file.name
        comp_manager.get_compressor(wal_info.compression).decompress(
            wal_info.orig_filename, path)

    # Extract the timeline from history file name
    tli, _, _ = decode_segment_name(wal_info.name)

    lines = []
    with open(path) as fp:
        for line in fp:
            line = line.strip()
            # Skip comments and empty lines
            if line.startswith("#"):
                continue
            # Skip comments and empty lines
            if len(line) == 0:
                continue
            # Use tab as separator
            contents = line.split('\t')
            if len(contents) != 3:
                # Invalid content of the line
                raise BadHistoryFileContents(path)

            history = HistoryFileData(
                tli=tli,
                parent_tli=int(contents[0]),
                switchpoint=parse_lsn(contents[1]),
                reason=contents[2])
            lines.append(history)

    # Empty history file or containing invalid content
    if len(lines) == 0:
        raise BadHistoryFileContents(path)
    else:
        return lines
