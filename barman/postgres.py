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
This module represents the interface towards a PostgreSQL server.
"""

import atexit
import logging
from abc import ABCMeta

import psycopg2
from psycopg2.extensions import STATUS_IN_TRANSACTION
from psycopg2.extras import RealDictCursor

from barman.infofile import Tablespace
from barman.remote_status import RemoteStatusMixin
from barman.utils import with_metaclass

_logger = logging.getLogger(__name__)


class ConninfoException(Exception):
    """
    Error for missing or failed parsing of the conninfo parameter (DSN)
    """


class PostgresConnectionError(Exception):
    """
    Error connecting to the PostgreSQL server
    """


class PostgresSuperuserRequired(Exception):
    """
    Superuser access is required
    """


class PostgresIsInRecovery(Exception):
    """
    PostgreSQL is in recovery, so no write operations are allowed
    """


class PostgresUnsupportedFeature(Exception):
    """
    Unsupported feature
    """


_live_connections = []
"""
List of connections to be closed at the interpreter shutdown
"""


@atexit.register
def _atexit():
    """
    Ensure that all the connections are correctly closed
    at interpreter shutdown
    """
    # Take a copy of the list because the conn.close() method modify it
    for conn in list(_live_connections):
        _logger.warn(
            "Forcing %s cleanup during process shut down.",
            conn.__class__.__name__)
        conn.close()


class PostgreSQL(with_metaclass(ABCMeta, RemoteStatusMixin)):
    """
    This abstract class represents a generic interface to a PostgreSQL server.
    """

    def __init__(self, config, conninfo):
        """
        Abstract base class constructor for PostgreSQL interface.

        :param barman.config.ServerConfig config: the server configuration
        :param str conninfo: Connection information (aka DSN)
        """
        super(PostgreSQL, self).__init__()
        assert conninfo
        self.config = config
        self.conninfo = conninfo
        self._conn = None
        # Build a dictionary with connection info parameters
        # This is mainly used to speed up search in conninfo
        try:
            self.conn_parameters = self.parse_dsn(conninfo)
        except (ValueError, TypeError) as e:
            _logger.debug(e)
            raise ConninfoException('Cannot connect to postgres: "%s" '
                                    'is not a valid connection string' %
                                    conninfo)

    @staticmethod
    def parse_dsn(dsn):
        """
        Parse connection parameters from 'conninfo'

        :param str dsn: Connection information (aka DSN)
        :rtype: dict[str,str]
        """
        # TODO: this might be made more robust in the future
        return dict(x.split('=', 1) for x in dsn.split(' '))

    def connect(self):
        """
        Generic function for Postgres connection (using psycopg2)
        """
        if not self._conn:
            try:
                self._conn = psycopg2.connect(self.conninfo)
            # If psycopg2 fails to connect to the host,
            # raise the appropriate exception
            except psycopg2.DatabaseError as e:
                raise PostgresConnectionError(
                    "Cannot connect to postgres: %s" % str(e).strip())
            # Register the connection to the live connections list
            _live_connections.append(self)
        return self._conn

    def close(self):
        """
        Close the connection to PostgreSQL
        """
        if self._conn:
            if self._conn.status == STATUS_IN_TRANSACTION:
                self._conn.rollback()
            self._conn.close()
            self._conn = None
            # Remove the connection from the live connections list
            _live_connections.remove(self)

    def _cursor(self, *args, **kwargs):
        """
        Return a cursor
        """
        conn = self.connect()
        return conn.cursor(*args, **kwargs)

    @property
    def server_version(self):
        """
        Version of PostgreSQL (returned by psycopg2)
        """
        conn = self.connect()
        return conn.server_version


class StreamingConnection(PostgreSQL):
    """
    This class represents a streaming connection to a PostgreSQL server.
    """

    def __init__(self, config):
        """
        Streaming connection constructor

        :param barman.config.ServerConfig config: the server configuration
        """
        if config.streaming_conninfo is None:
            raise ConninfoException(
                'Missing streaming_conninfo parameter in barman configuration '
                'for server %s' % config.name
            )
        super(StreamingConnection, self).__init__(config,
                                                  config.streaming_conninfo)
        # Make sure we connect using the 'replication' option which
        # triggers streaming replication protocol communication
        if 'replication' not in self.conn_parameters:
            self.conn_parameters['replication'] = 'true'
            self.conninfo += ' replication=true'

    def connect(self):
        """
        Connect to the PostgreSQL server. It reuses an existing connection.

        :returns: the connection to the server
        """
        if not self._conn:
            # Build a connection and set autocommit
            self._conn = super(StreamingConnection, self).connect()
            self._conn.autocommit = True
        return self._conn

    @property
    def server_txt_version(self):
        """
        Human readable version of PostgreSQL (calculated from server_version)
        """
        try:
            conn = self.connect()
            major = int(conn.server_version / 10000)
            minor = int(conn.server_version / 100 % 100)
            patch = int(conn.server_version % 100)
            return "%d.%d.%d" % (major, minor, patch)
        except PostgresConnectionError as e:
            _logger.debug("Error retrieving PostgreSQL version: %s",
                          str(e).strip())
            return None

    def fetch_remote_status(self):
        """
        Returns the status of the connection to the PostgreSQL server.

        This method does not raise any exception in case of errors,
        but set the missing values to None in the resulting dictionary.

        :rtype: dict[str, None|str]
        """
        result = dict.fromkeys(
            ('streaming_supported', 'streaming', 'systemid',
                'timeline', 'xlogpos'),
            None)
        try:
            # If the server is too old to support `pg_receivexlog`,
            # exit immediately.
            # This needs to be protected by the try/except because
            # `self.server_version` can raise a PostgresConnectionError
            if self.server_version < 90200:
                result["streaming_supported"] = False
                return result
            result["streaming_supported"] = True
            # Execute a IDENTIFY_SYSYEM to check the connection
            cursor = self._cursor()
            cursor.execute("IDENTIFY_SYSTEM")
            row = cursor.fetchone()
            # If something has been returned, barman is connected
            # to a replication backend
            if row:
                result['streaming'] = True
                # IDENTIFY_SYSTEM always return at least two values
                result['systemid'] = row[0]
                result['timeline'] = row[1]
                # PostgreSQL 9.1+ returns also the current xlog flush location
                if len(row) > 2:
                    result['xlogpos'] = row[2]
        except psycopg2.ProgrammingError:
            # This is not a streaming connection
            result['streaming'] = False
        except PostgresConnectionError as e:
            _logger.warn("Error retrieving PostgreSQL status: %s",
                         str(e).strip())
        return result


class PostgreSQLConnection(PostgreSQL):
    """
    This class represents a standard client connection to a PostgreSQL server.
    """

    # Streaming replication client types
    STANDBY = 1
    WALSTREAMER = 2
    ANY_STREAMING_CLIENT = (STANDBY, WALSTREAMER)

    def __init__(self, config):
        """
        PostgreSQL connection constructor.

        :param barman.config.ServerConfig config: the server configuration
        """
        # Check that 'conninfo' option is properly set
        if config.conninfo is None:
            raise ConninfoException(
                'Missing conninfo parameter in barman configuration '
                'for server %s' % config.name)
        super(PostgreSQLConnection, self).__init__(config, config.conninfo)
        self.configuration_files = None

    def connect(self):
        """
        Connect to the PostgreSQL server. It reuses an existing connection.
        """
        if self._conn:
            return self._conn

        self._conn = super(PostgreSQLConnection, self).connect()
        if (self._conn.server_version >= 90000 and
                'application_name' not in self.conn_parameters):
            try:
                cur = self._conn.cursor()
                cur.execute('SET application_name TO barman')
                cur.close()
            # If psycopg2 fails to set the application name,
            # raise the appropriate exception
            except psycopg2.ProgrammingError as e:
                raise PostgresConnectionError(
                    "Cannot set the application name: %s" % str(e).strip())
        return self._conn

    @property
    def server_txt_version(self):
        """
        Human readable version of PostgreSQL (returned by the server)
        """
        try:
            cur = self._cursor()
            cur.execute("SELECT version()")
            return cur.fetchone()[0].split()[1]
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving PostgreSQL version: %s",
                          str(e).strip())
            return None

    @property
    def has_pgespresso(self):
        """
        Returns true if the `pgespresso` extension is available
        """
        try:
            # pg_extension is only available from Postgres 9.1+
            if self.server_version < 90100:
                return False
            cur = self._cursor()
            cur.execute("SELECT count(*) FROM pg_extension "
                        "WHERE extname = 'pgespresso'")
            q_result = cur.fetchone()[0]
            return q_result > 0
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving pgespresso information: %s",
                          str(e).strip())
            return None

    @property
    def is_in_recovery(self):
        """
        Returns true if PostgreSQL server is in recovery mode (hot standby)
        """
        try:
            # pg_is_in_recovery is only available from Postgres 9.0+
            if self.server_version < 90000:
                return False
            cur = self._cursor()
            cur.execute("SELECT pg_is_in_recovery()")
            return cur.fetchone()[0]
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error calling pg_is_in_recovery() function: %s",
                          str(e).strip())
            return None

    @property
    def is_superuser(self):
        """
        Returns true if current user has superuser privileges
        """
        try:
            cur = self._cursor()
            cur.execute('SELECT usesuper FROM pg_user '
                        'WHERE usename = CURRENT_USER')
            return cur.fetchone()[0]
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error calling is_superuser() function: %s",
                          str(e).strip())
            return None

    @property
    def current_xlog(self):
        """
        Get current WAL file from PostgreSQL

        :return str: current WAL file in PostgreSQL
        """
        try:
            if not self.is_in_recovery:
                cur = self._cursor()
                cur.execute(
                    'SELECT pg_xlogfile_name('
                    'pg_current_xlog_location())')
                return cur.fetchone()[0]
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving current xlog: %s",
                          str(e).strip())
            return None

    @property
    def current_xlog_location(self):
        """
        Get current WAL location from PostgreSQL

        :return str: current WAL location in PostgreSQL
        """
        try:
            if not self.is_in_recovery:
                cur = self._cursor()
                cur.execute(
                    'SELECT pg_current_xlog_location()')
                return cur.fetchone()[0]
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving current xlog location: %s",
                          str(e).strip())
            return None

    @property
    def current_size(self):
        """
        Returns the total size of the PostgreSQL server (requires superuser)
        """
        if not self.is_superuser:
            return None

        try:
            cur = self._cursor()
            cur.execute(
                "SELECT sum(pg_tablespace_size(oid)) "
                "FROM pg_tablespace")
            return cur.fetchone()[0]
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving PostgreSQL total size: %s",
                          str(e).strip())
            return None

    def get_archiver_stats(self):
        """
        This method gathers statistics from pg_stat_archiver.
        Only for Postgres 9.4+ or greater. If not available, returns None.

        :return dict|None: a dictionary containing Postgres statistics from
            pg_stat_archiver or None
        """
        try:
            # pg_stat_archiver is only available from Postgres 9.4+
            if self.server_version < 90400:
                return None
            cur = self._cursor(cursor_factory=RealDictCursor)
            # Select from pg_stat_archiver statistics view,
            # retrieving statistics about WAL archiver process activity,
            # also evaluating if the server is archiving without issues
            # and the archived WALs per second rate.
            #
            # We are using current_settings to check for archive_mode=always.
            # current_setting does normalise its output so we can just
            # check for 'always' settings using a direct string
            # comparison
            cur.execute(
                "SELECT *, "
                "current_setting('archive_mode') IN ('on', 'always') "
                "AND (last_failed_wal IS NULL "
                "OR last_failed_wal LIKE '%.history' "
                "AND substring(last_failed_wal from 1 for 8) "
                "<= substring(last_archived_wal from 1 for 8) "
                "OR last_failed_wal <= last_archived_wal) "
                "AS is_archiving, "
                "CAST (archived_count AS NUMERIC) "
                "/ EXTRACT (EPOCH FROM age(now(), stats_reset)) "
                "AS current_archived_wals_per_second "
                "FROM pg_stat_archiver")
            return cur.fetchone()
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving pg_stat_archive data: %s",
                          str(e).strip())
            return None

    def fetch_remote_status(self):
        """
        Get the status of the PostgreSQL server

        This method does not raise any exception in case of errors,
        but set the missing values to None in the resulting dictionary.

        :rtype: dict[str, None|str]
        """
        # PostgreSQL settings to get from the server (requiring superuser)
        pg_superuser_settings = [
            'data_directory']
        # PostgreSQL settings to get from the server
        pg_settings = []
        pg_query_keys = [
            'server_txt_version',
            'is_superuser',
            'current_xlog',
            'pgespresso_installed']
        # Initialise the result dictionary setting all the values to None
        result = dict.fromkeys(pg_superuser_settings +
                               pg_settings +
                               pg_query_keys,
                               None)
        try:
            # check for wal_level only if the version is >= 9.0
            if self.server_version >= 90000:
                pg_settings.append('wal_level')

            # retrieves superuser settings
            if self.is_superuser:
                for name in pg_superuser_settings:
                    result[name] = self.get_setting(name)

            # retrieves standard settings
            for name in pg_settings:
                result[name] = self.get_setting(name)

            result['is_superuser'] = self.is_superuser
            result['server_txt_version'] = self.server_txt_version
            result['pgespresso_installed'] = self.has_pgespresso
            result['current_xlog'] = self.current_xlog
            result['current_size'] = self.current_size

            result.update(self.get_configuration_files())
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.warn("Error retrieving PostgreSQL status: %s",
                         str(e).strip())
        return result

    def get_setting(self, name):
        """
        Get a Postgres setting with a given name

        :param name: a parameter name
        """
        try:
            cur = self._cursor()
            cur.execute('SHOW "%s"' % name.replace('"', '""'))
            return cur.fetchone()[0]
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving PostgreSQL setting '%s': %s",
                          name.replace('"', '""'), str(e).strip())
            return None

    def get_tablespaces(self):
        """
        Returns a list of tablespaces or None if not present
        """
        try:
            cur = self._cursor()
            if self.server_version >= 90200:
                cur.execute(
                    "SELECT spcname, oid, "
                    "pg_tablespace_location(oid) AS spclocation "
                    "FROM pg_tablespace "
                    "WHERE pg_tablespace_location(oid) != ''")
            else:
                cur.execute(
                    "SELECT spcname, oid, spclocation "
                    "FROM pg_tablespace WHERE spclocation != ''")
            # Generate a list of tablespace objects
            return [Tablespace._make(item) for item in cur.fetchall()]
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving PostgreSQL tablespaces: %s",
                          str(e).strip())
            return None

    def get_configuration_files(self):
        """
        Get postgres configuration files or an empty dictionary
        in case of error

        :rtype: dict
        """
        if self.configuration_files:
            return self.configuration_files
        try:
            self.configuration_files = {}
            cur = self._cursor()
            cur.execute(
                "SELECT name, setting FROM pg_settings "
                "WHERE name IN ('config_file', 'hba_file', 'ident_file')")
            for cname, cpath in cur.fetchall():
                self.configuration_files[cname] = cpath

            # Retrieve additional configuration files
            # If PostgresSQL is older than 8.4 disable this check
            if self.server_version >= 80400:
                cur.execute(
                    "SELECT DISTINCT sourcefile AS included_file "
                    "FROM pg_settings "
                    "WHERE sourcefile IS NOT NULL "
                    "AND sourcefile NOT IN "
                    "(SELECT setting FROM pg_settings "
                    "WHERE name = 'config_file') "
                    "ORDER BY 1")
                # Extract the values from the containing single element tuples
                included_files = [included_file
                                  for included_file, in cur.fetchall()]
                if len(included_files) > 0:
                    self.configuration_files['included_files'] = included_files

        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving PostgreSQL configuration files "
                          "location: %s", str(e).strip())
            self.configuration_files = {}

        return self.configuration_files

    def create_restore_point(self, target_name):
        """
        Create a restore point with the given target name

        The method executes the pg_create_restore_point() function through
        a PostgreSQL connection. Only for Postgres versions >= 9.1 when not
        in replication.

        If requirements are not met, the operation is skipped.

        :param str target_name: name of the restore point

        :returns: the restore point LSN
        :rtype: str|None
        """
        if self.server_version < 90100:
            return None

        # Not possible if on a standby
        # Called inside the pg_connect context to reuse the connection
        if self.is_in_recovery:
            return None

        try:
            cur = self._cursor()
            cur.execute(
                "SELECT pg_create_restore_point(%s)", [target_name])
            return cur.fetchone()[0]
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug('Error issuing pg_create_restore_point()'
                          'command: %s', str(e).strip())
            return None

    def start_exclusive_backup(self, backup_label):
        """
        Calls pg_start_backup() on the PostgreSQL server

        :param str backup_label: label for the backup returned by Postgres
        :rtype: tuple
        """
        try:
            conn = self.connect()

            # Issue a rollback to release any unneeded lock
            conn.rollback()

            # Start the exclusive backup
            cur = conn.cursor()
            if self.server_version < 80400:
                cur.execute(
                    "SELECT xlog_loc, "
                    "(pg_xlogfile_name_offset(xlog_loc)).*, "
                    "now() FROM pg_start_backup(%s) as xlog_loc",
                    (backup_label,))
            else:
                cur.execute(
                    "SELECT xlog_loc, "
                    "(pg_xlogfile_name_offset(xlog_loc)).*, "
                    "now() FROM pg_start_backup(%s,%s) as xlog_loc",
                    (backup_label,
                     self.config.immediate_checkpoint))

            start_row = cur.fetchone()

            # Rollback to release the transaction, as the connection
            # is to be retained until the end of backup
            conn.rollback()

            return start_row
        except (PostgresConnectionError, psycopg2.Error) as e:
            msg = "pg_start_backup(): %s" % str(e).strip()
            _logger.debug(msg)
            raise Exception(msg)

    def stop_exclusive_backup(self):
        """
        Calls pg_stop_backup() on the PostgreSQL server

        :returns: a tuple with the result of the pg_stop_backup() call or None
        :rtype: tuple|None
        """
        try:
            conn = self.connect()
            # Issue a rollback to release any unneeded lock
            conn.rollback()
            cur = conn.cursor()
            cur.execute(
                'SELECT xlog_loc, (pg_xlogfile_name_offset(xlog_loc)).*, '
                'now() FROM pg_stop_backup() as xlog_loc')
            return cur.fetchone()
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug('Error issuing pg_stop_backup() command: %s',
                          str(e).strip())
            return None

    def pgespresso_start_backup(self, backup_label):
        """
        Execute a pgespresso_start_backup

        :param str backup_label: label for the backup
        :rtype: tuple
        """
        try:
            conn = self.connect()

            # Issue a rollback to release any unneeded lock
            conn.rollback()

            # Start the concurrent backup
            cur = conn.cursor()
            cur.execute(
                'SELECT pgespresso_start_backup(%s,%s), now()',
                (backup_label, self.config.immediate_checkpoint))
            start_row = cur.fetchone()

            # Rollback to release the transaction, as the connection
            # is to be retained until the end of backup
            conn.rollback()

            return start_row
        except (PostgresConnectionError, psycopg2.Error) as e:
            msg = "pgespresso_start_backup(): %s" % str(e).strip()
            _logger.debug(msg)
            raise Exception(msg)

    def pgespresso_stop_backup(self, backup_label):
        """
        Execute a pgespresso_stop_backup

        :param str backup_label: label of the backup
        :returns: a string containing the result of the
            pgespresso_stop_backup call or None
        :rtype: tuple|None
        """
        try:
            conn = self.connect()
            # Issue a rollback to release any unneeded lock
            conn.rollback()
            cur = conn.cursor()
            cur.execute("SELECT pgespresso_stop_backup(%s), now()",
                        (backup_label,))
            return cur.fetchone()
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug(
                "Error issuing pgespresso_stop_backup() command: %s",
                str(e).strip())
            return None

    def switch_xlog(self):
        """
        Execute a pg_switch_xlog()

        To be SURE of the switch of a xlog, we collect the xlogfile name
        before and after the switch.
        If the name of the xlog file post switch is greater than the one
        collected before the switch have been executed.
        Returns an empty string otherwise.

        The method returns None if something went wrong during the execution
        of the pg_switch_xlog command.

        :rtype: str|None
        """
        try:
            conn = self.connect()
            # Requires superuser privilege
            if not self.is_superuser:
                raise PostgresSuperuserRequired()

            # If this server is in recovery there is nothing to do
            if self.is_in_recovery:
                raise PostgresIsInRecovery()

            cur = conn.cursor()
            # Collect the xlog file name before the switch
            cur.execute('SELECT pg_xlogfile_name('
                        'pg_current_xlog_insert_location())')
            pre_switch = cur.fetchone()[0]
            # Switch
            cur.execute('SELECT pg_xlogfile_name(pg_switch_xlog())')
            # Collect the xlog file name after the switch
            cur.execute('SELECT pg_xlogfile_name('
                        'pg_current_xlog_insert_location())')
            post_switch = cur.fetchone()[0]
            if pre_switch < post_switch:
                return post_switch
            else:
                return ''
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug(
                "Error issuing pg_switch_xlog() command: %s",
                str(e).strip())
            return None

    def checkpoint(self):
        """
        Execute a checkpoint
        """
        try:
            conn = self.connect()

            # Requires superuser privilege
            if not self.is_superuser:
                raise PostgresSuperuserRequired()

            cur = conn.cursor()
            cur.execute("CHECKPOINT")
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug(
                "Error issuing CHECKPOINT: %s",
                str(e).strip())

    def get_replication_stats(self, client_type=STANDBY):
        """
        Returns streaming replication information
        """
        try:
            cur = self._cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)

            # Without superuser rights, this function is useless
            # TODO: provide a simplified version for non-superusers
            if not self.is_superuser:
                raise PostgresSuperuserRequired()

            # pg_stat_replication is a system view that contains one
            # row per WAL sender process with information about the
            # replication status of a standby server. It has been
            # introduced in PostgreSQL 9.1. Current fields are:
            #
            # - pid (procpid in 9.1)
            # - usesysid
            # - usename
            # - application_name
            # - client_addr
            # - client_hostname
            # - client_port
            # - backend_start
            # - backend_xmin (9.4+)
            # - state
            # - sent_location
            # - write_location
            # - flush_location
            # - replay_location
            # - sync_priority
            # - sync_state
            #

            if self.server_version < 90100:
                raise PostgresUnsupportedFeature('9.1')

            if self.server_version >= 90400:
                # Current implementation (9.4+)
                what = "* "
            elif self.server_version >= 90200:
                # PostgreSQL 9.2/9.3
                what = "pid," \
                    "usesysid," \
                    "usename," \
                    "application_name," \
                    "client_addr," \
                    "client_hostname," \
                    "client_port," \
                    "backend_start," \
                    "CAST (NULL AS xid) AS backend_xmin," \
                    "state," \
                    "sent_location," \
                    "write_location," \
                    "flush_location," \
                    "replay_location," \
                    "sync_priority," \
                    "sync_state "
            else:
                # PostgreSQL 9.1
                what = "procpid AS pid," \
                    "usesysid," \
                    "usename," \
                    "application_name," \
                    "client_addr," \
                    "client_hostname," \
                    "client_port," \
                    "backend_start," \
                    "CAST (NULL AS xid) AS backend_xmin," \
                    "state," \
                    "sent_location," \
                    "write_location," \
                    "flush_location," \
                    "replay_location," \
                    "sync_priority," \
                    "sync_state "

            # Streaming client
            if client_type == self.STANDBY:
                # Standby server
                where = 'WHERE replay_location IS NOT NULL '
            elif client_type == self.WALSTREAMER:
                # WAL streamer
                where = 'WHERE replay_location IS NULL '
            else:
                where = ''

            # Execute the query
            cur.execute(
                "SELECT %s, "
                "CASE WHEN pg_is_in_recovery() THEN NULL ELSE "
                "pg_xlog_location_diff(sent_location, "
                "pg_current_xlog_location()) END AS sent_diff, "
                "CASE WHEN pg_is_in_recovery() THEN NULL ELSE "
                "pg_xlog_location_diff(write_location, "
                "pg_current_xlog_location()) END AS write_diff, "
                "CASE WHEN pg_is_in_recovery() THEN NULL ELSE "
                "pg_xlog_location_diff(flush_location, "
                "pg_current_xlog_location()) END AS flush_diff, "
                "CASE WHEN pg_is_in_recovery() THEN NULL ELSE "
                "pg_xlog_location_diff(replay_location, "
                "pg_current_xlog_location()) END AS replay_diff "
                "FROM pg_stat_replication "
                "%s"
                "ORDER BY sync_state DESC, sync_priority" % (what, where))

            # Generate a list of standby objects
            return cur.fetchall()
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving status of standby servers: %s",
                          str(e).strip())
            return None
