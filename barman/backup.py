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
from barman import xlog
import logging
import dateutil.parser

_logger = logging.getLogger(__name__)

class Backup(object):
    """
    PostgreSQL Physical base backup
    """

    KEYS = [ 'version', 'pgdata', 'tablespaces', 'timeline',
             'begin_time', 'begin_xlog', 'begin_wal', 'begin_offset',
             'size', 'end_time', 'end_xlog', 'end_wal', 'end_offset',
             'status', 'server_name', 'error'
    ]
    """
    Attributes of the backup.info file
    """

    TYPES = {'tablespaces':ast.literal_eval, # Treat the tablespaces as a literal Python list of tuples
             'timeline':int, # Timeline is an integer
             'begin_time':dateutil.parser.parse,
             'end_time':dateutil.parser.parse,
             'size':int,
    }
    """
    Conversion from string
    """

    def __init__(self, server, info_file=None):
        """
        Constructor
        """
        # Initialises the attributes for the object based on the predefined keys
        self.__dict__.update(dict.fromkeys(self.KEYS))
        self.server = server
        self.backup_id = None
        if info_file:
            # Looks for a backup.info file
            if hasattr(info_file, 'read'): # We have been given a file-like object
                info = info_file
                filename = os.path.abspath(info_file.name)
            else: # Just a file name
                filename = os.path.abspath(info_file)
                info = open(info_file, 'r').readlines()
            # Detect the backup ID from the name of the parent dir
            self.backup_id = os.path.basename(os.path.dirname(filename))
            # Parses the backup.info file
            for line in info:
                try:
                    key, value = line.rstrip().split('=')
                except:
                    raise Exception('invalid line in backup file: %s' % line)
                if key not in self.KEYS:
                    raise Exception('invalid key in backup file: %s' % key)
                if key in self.TYPES:
                    self.__dict__[key] = self.TYPES[key](value)
                else:
                    self.__dict__[key] = value

    def get_required_wal_segments(self):
        """
        Get the list of required WAL segments for the current backup
        """
        for filename in xlog.enumerate_segments(self.begin_wal, self.end_wal):
            yield filename

    def show(self):
        """
        Show backup information
        """
        yield "Backup %s:" % (self.backup_id)
        if self.status == 'DONE':
            try:
                previous_backup = self.server.get_previous_backup(self.backup_id)
                next_backup = self.server.get_next_backup(self.backup_id)
                yield "  Server Name       : %s" % self.server_name
                yield "  PostgreSQL Version: %s" % self.version
                yield "  PGDATA directory  : %s" % self.pgdata
                if self.tablespaces:
                    yield "  Tablespaces:"
                    for name, _, location in self.tablespaces:
                        yield "    %s: %s" % (name, location)
                yield ""
                yield "  Base backup information:"
                yield "    Disk usage      : TODO"
                yield "    Timeline        : %s" % self.timeline
                yield "    Begin WAL       : %s" % self.begin_wal
                yield "    End WAL         : %s" % self.end_wal
                yield "    Begin time      : %s" % self.begin_time
                yield "    End time        : %s" % self.end_time
                yield "    Begin Offset    : %s" % self.begin_offset
                yield "    End Offset      : %s" % self.end_offset
                yield "    Begin XLOG      : %s" % self.begin_xlog
                yield "    End XLOG        : %s" % self.end_xlog
                yield ""
                yield "  WAL information:"
                yield "    No of files     : TODO"
                yield "    Disk usage      : TODO"
                yield "    Last available  : TODO"
                yield ""
                yield "  Catalog information:"
                if previous_backup:
                    yield "    Previous Backup : %s" % previous_backup.backup_id
                else:
                    yield "    Previous Backup : - (this is the oldest base backup)"
                if next_backup:
                    yield "    Next Backup     : %s" % next_backup.backup_id
                else:
                    yield "    Next Backup     : - (this is the latest base backup)"

            except:
                pass
        else:
            yield "\tUnavailable"
