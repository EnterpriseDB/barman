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
PostgreSQL Plumbing module

This module contain low-level PostgreSQL related information, such as the
on-disk structure and the name of the core functions in different PostgreSQL
versions.
"""


PGDATA_EXCLUDE_LIST = [
    # Exclude this to avoid log files copy
    '/pg_log/*',
    # Exclude this for (PostgreSQL < 10) to avoid WAL files copy
    '/pg_xlog/*',
    # This have been renamed on PostgreSQL 10
    '/pg_wal/*',
    # We handle this on a different step of the copy
    '/global/pg_control',
]

EXCLUDE_LIST = [
    # Files: see excludeFiles const in PostgreSQL source
    'pgsql_tmp*',
    'postgresql.auto.conf.tmp',
    'current_logfiles.tmp',
    'pg_internal.init',
    'postmaster.pid',
    'postmaster.opts',
    'recovery.conf',
    'standby.signal',

    # Directories: see excludeDirContents const in PostgreSQL source
    'pg_dynshmem/*',
    'pg_notify/*',
    'pg_replslot/*',
    'pg_serial/*',
    'pg_stat_tmp/*',
    'pg_snapshots/*',
    'pg_subtrans/*',
]


def function_name_map(server_version):
    """
    Return a map with function and directory names according to the current
    PostgreSQL version.

    Each entry has the `current` name as key and the name for the specific
    version as value.

    :param number|None server_version: Version of PostgreSQL as returned by
        psycopg2 (i.e. 90301 represent PostgreSQL 9.3.1). If the version
        is None, default to the latest PostgreSQL version
    :rtype: dict[str]
    """

    if server_version and server_version < 100000:
        return {
            'pg_switch_wal': 'pg_switch_xlog',
            'pg_walfile_name': 'pg_xlogfile_name',
            'pg_wal': 'pg_xlog',
            'pg_walfile_name_offset': 'pg_xlogfile_name_offset',
            'pg_last_wal_replay_lsn': 'pg_last_xlog_replay_location',
            'pg_current_wal_lsn': 'pg_current_xlog_location',
            'pg_current_wal_insert_lsn': 'pg_current_xlog_insert_location',
            'pg_last_wal_receive_lsn': 'pg_last_xlog_receive_location',
            'sent_lsn': 'sent_location',
            'write_lsn': 'write_location',
            'flush_lsn': 'flush_location',
            'replay_lsn': 'replay_location',
        }

    return {
        'pg_switch_wal': 'pg_switch_wal',
        'pg_walfile_name': 'pg_walfile_name',
        'pg_wal': 'pg_wal',
        'pg_walfile_name_offset': 'pg_walfile_name_offset',
        'pg_last_wal_replay_lsn': 'pg_last_wal_replay_lsn',
        'pg_current_wal_lsn': 'pg_current_wal_lsn',
        'pg_current_wal_insert_lsn': 'pg_current_wal_insert_lsn',
        'pg_last_wal_receive_lsn': 'pg_last_wal_receive_lsn',
        'sent_lsn': 'sent_lsn',
        'write_lsn': 'write_lsn',
        'flush_lsn': 'flush_lsn',
        'replay_lsn': 'replay_lsn',
    }
