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

import ast
import os
import re

class Backup(object):

    KEYS = [ 'version', 'pgdata', 'tablespaces', 'timeline',
             'begin_time', 'begin_xlog', 'begin_wal', 'begin_offset',
             'end_time', 'end_xlog', 'end_wal', 'end_offset',
             'status',
    ]

    WAL_re = re.compile(r'([\dA-Fa-f]{8})([\dA-Fa-f]{8})([\dA-Fa-f]{8})')

    XLOG_SEG_SIZE = 1 << 24
    XLOG_SEG_PER_FILE = 0xffffffff / XLOG_SEG_SIZE
    XLOG_FILE_SIZE = XLOG_SEG_SIZE * XLOG_SEG_PER_FILE

    def __init__(self, file=None, backup_id=None):
        self.__dict__.update(dict.fromkeys(self.KEYS))
        if file:
            if hasattr(file, 'read'):
                info = file
                filename = os.path.abspath(file.name)
            else:
                filename = os.path.abspath(file)
                info = open(file, 'r').readlines()
            if not backup_id:
                self.backup_id = os.path.basename(os.path.dirname(filename))
            else:
                self.backup_id = backup_id
            for line in info:
                try:
                    key, value = line.rstrip().split('=')
                except:
                    continue
                if key not in Backup.KEYS:
                    raise Exception('invalid key in backup file: %s' % key)
                if key == 'tablespaces':
                    self.__dict__[key] = ast.literal_eval(value)
                else:
                    self.__dict__[key] = value

    def _segment_name(self, tli, log, seg):
        return "%08X%08X%08X" % (tli, log, seg)

    def get_required_wal_segments(self):
        begin_tli, begin_log, begin_seg = [int(x, 16) for x in self.WAL_re.match(self.begin_wal).groups()]
        end_tli, end_log, end_seg = [int(x, 16) for x in self.WAL_re.match(self.end_wal).groups()]
        assert begin_tli == end_tli

        cur_log, cur_seg = begin_log, begin_seg
        while cur_log < end_log or cur_seg <= end_seg:
            yield self._segment_name(begin_tli, cur_log, cur_seg)
            cur_seg += 1
            if cur_seg >= self.XLOG_SEG_PER_FILE:
                cur_seg = 0
                cur_log += 1
