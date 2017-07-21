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
This module represents the interface towards a PostgreSQL server.
"""

import atexit
import logging
from abc import ABCMeta

import psycopg2
from psycopg2.errorcodes import (DUPLICATE_OBJECT, OBJECT_IN_USE,
                                 UNDEFINED_OBJECT)
from psycopg2.extensions import STATUS_IN_TRANSACTION
from psycopg2.extras import DictCursor, NamedTupleCursor

from barman.exceptions import (ConninfoException, PostgresAppNameError,
                               PostgresConnectionError,
                               PostgresDuplicateReplicationSlot,
                               PostgresException,
                               PostgresInvalidReplicationSlot,
                               PostgresIsInRecovery,
                               PostgresReplicationSlotInUse,
                               PostgresReplicationSlotsFull,
                               PostgresSuperuserRequired,
                               PostgresUnsupportedFeature)
from barman.infofile import Tablespace
from barman.remote_status import RemoteStatusMixin
from barman.utils import simplify_version, with_metaclass
from barman.xlog import DEFAULT_XLOG_SEG_SIZE

# This is necessary because the CONFIGURATION_LIMIT_EXCEEDED constant
# has been added in psycopg2 2.5, but Barman supports version 2.4.2+ so
# in case of import error we declare a constant providing the correct value.
try:
    from psycopg2.errorcodes import CONFIGURATION_LIMIT_EXCEEDED
except ImportError:
    CONFIGURATION_LIMIT_EXCEEDED = '53400'


_logger = logging.getLogger(__name__)

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

    CHECK_QUERY = 'SELECT 1'

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
        self.allow_reconnect = True
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
        return dict(x.split('=', 1) for x in dsn.split())

    @staticmethod
    def encode_dsn(parameters):
        """
        Build a connection string from a dictionary of connection
        parameters

        :param dict[str,str] parameters: Connection parameters
        :rtype: str
        """
        # TODO: this might be made more robust in the future
        return ' '.join(
            ["%s=%s" % (k, v) for k, v in sorted(parameters.items())])

    def get_connection_string(self, application_name=None):
        """
        Return the connection string, adding the application_name parameter
        if requested, unless already defined by user in the connection string

        :param str application_name: the application_name to add
        :return str: the connection string
        """
        conn_string = self.conninfo
        # check if the application name is already defined by user
        if application_name and 'application_name' not in self.conn_parameters:
            # Then add the it to the connection string
            conn_string += ' application_name=%s' % application_name
        return conn_string

    def connect(self):
        """
        Generic function for Postgres connection (using psycopg2)
        """

        if not self._check_connection():
            try:
                self._conn = psycopg2.connect(self.conninfo)
            # If psycopg2 fails to connect to the host,
            # raise the appropriate exception
            except psycopg2.DatabaseError as e:
                raise PostgresConnectionError(str(e).strip())
            # Register the connection to the list of live connections
            _live_connections.append(self)
        return self._conn

    def _check_connection(self):
        """
        Return false if the connection is broken

        :rtype: bool
        """
        # If the connection is not present return False
        if not self._conn:
            return False

        # Check if the connection works by running 'SELECT 1'
        cursor = None
        try:
            cursor = self._conn.cursor()
            cursor.execute(self.CHECK_QUERY)
        except psycopg2.DatabaseError:
            # Connection is broken, so we need to reconnect
            self.close()
            # Raise an error if reconnect is not allowed
            if not self.allow_reconnect:
                raise PostgresConnectionError(
                    "Connection lost, reconnection not allowed")
            return False
        finally:
            if cursor:
                cursor.close()

        return True

    def close(self):
        """
        Close the connection to PostgreSQL
        """
        if self._conn:
            # If the connection is still alive, rollback and close it
            if not self._conn.closed:
                if self._conn.status == STATUS_IN_TRANSACTION:
                    self._conn.rollback()
                self._conn.close()
            # Remove the connection from the live connections list
            self._conn = None
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

    @property
    def server_txt_version(self):
        """
        Human readable version of PostgreSQL (calculated from server_version)

        :rtype: str|None
        """
        try:
            conn = self.connect()
            major = int(conn.server_version / 10000)
            minor = int(conn.server_version / 100 % 100)
            patch = int(conn.server_version % 100)
            if major < 10:
                return "%d.%d.%d" % (major, minor, patch)
            if minor != 0:
                _logger.warning(
                    "Unexpected non zero minor version %s in %s",
                    minor, conn.server_version)
            return "%d.%d" % (major, patch)
        except PostgresConnectionError as e:
            _logger.debug("Error retrieving PostgreSQL version: %s",
                          str(e).strip())
            return None

    @property
    def server_major_version(self):
        """
        PostgreSQL major version (calculated from server_txt_version)

        :rtype: str|None
        """
        result = self.server_txt_version
        if result is not None:
            return simplify_version(result)
        return None


class StreamingConnection(PostgreSQL):
    """
    This class represents a streaming connection to a PostgreSQL server.
    """

    CHECK_QUERY = 'IDENTIFY_SYSTEM'

    def __init__(self, config):
        """
        Streaming connection constructor

        :param barman.config.ServerConfig config: the server configuration
        """
        if config.streaming_conninfo is None:
            raise ConninfoException(
                "Missing 'streaming_conninfo' parameter for server '%s'"
                % config.name)

        super(StreamingConnection, self).__init__(config,
                                                  config.streaming_conninfo)
        # Make sure we connect using the 'replication' option which
        # triggers streaming replication protocol communication
        self.conn_parameters['replication'] = 'true'
        # ensure that the datestyle is set to iso, working around an
        # issue in some psycopg2 versions
        self.conn_parameters['options'] = '-cdatestyle=iso'
        # Override 'dbname' parameter. This operation is required to mimic
        # the behaviour of pg_receivexlog and pg_basebackup
        self.conn_parameters['dbname'] = 'replication'
        # Rebuild the conninfo string from the modified parameter lists
        self.conninfo = self.encode_dsn(self.conn_parameters)

    def connect(self):
        """
        Connect to the PostgreSQL server. It reuses an existing connection.

        :returns: the connection to the server
        """
        if self._check_connection():
            return self._conn

        # Build a connection and set autocommit
        self._conn = super(StreamingConnection, self).connect()
        self._conn.autocommit = True
        return self._conn

    def fetch_remote_status(self):
        """
        Returns the status of the connection to the PostgreSQL server.

        This method does not raise any exception in case of errors,
        but set the missing values to None in the resulting dictionary.

        :rtype: dict[str, None|str]
        """
        result = dict.fromkeys(
            ('connection_error', 'streaming_supported',
                'streaming', 'systemid',
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
            result['connection_error'] = str(e).strip()
            _logger.warn("Error retrieving PostgreSQL status: %s",
                         str(e).strip())
        return result

    def create_physical_repslot(self, slot_name):
        """
        Create a physical replication slot using the streaming connection
        :param str slot_name: Replication slot name
        """
        cursor = self._cursor()
        try:
            # In the following query, the slot name is directly passed
            # to the CREATE_REPLICATION_SLOT command, without any
            # quoting. This is a characteristic of the streaming
            # connection, otherwise if will fail with a generic
            # "syntax error"
            cursor.execute('CREATE_REPLICATION_SLOT %s PHYSICAL' % slot_name)
        except psycopg2.DatabaseError as exc:
            if exc.pgcode == DUPLICATE_OBJECT:
                # A replication slot with the same name exists
                raise PostgresDuplicateReplicationSlot()
            elif exc.pgcode == CONFIGURATION_LIMIT_EXCEEDED:
                # Unable to create a new physical replication slot.
                # All slots are full.
                raise PostgresReplicationSlotsFull()
            else:
                raise PostgresException(str(exc).strip())

    def drop_repslot(self, slot_name):
        """
        Drop a physical replication slot using the streaming connection
        :param str slot_name: Replication slot name
        """
        cursor = self._cursor()
        try:
            # In the following query, the slot name is directly passed
            # to the DROP_REPLICATION_SLOT command, without any
            # quoting. This is a characteristic of the streaming
            # connection, otherwise if will fail with a generic
            # "syntax error"
            cursor.execute('DROP_REPLICATION_SLOT %s' % slot_name)
        except psycopg2.DatabaseError as exc:
            if exc.pgcode == UNDEFINED_OBJECT:
                # A replication slot with the that name does not exist
                raise PostgresInvalidReplicationSlot()
            if exc.pgcode == OBJECT_IN_USE:
                # The replication slot is still in use
                raise PostgresReplicationSlotInUse()
            else:
                raise PostgresException(str(exc).strip())


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
                "Missing 'conninfo' parameter for server '%s'" % config.name)
        super(PostgreSQLConnection, self).__init__(config, config.conninfo)
        self.configuration_files = None

    def connect(self):
        """
        Connect to the PostgreSQL server. It reuses an existing connection.
        """
        if self._check_connection():
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
                raise PostgresAppNameError(str(e).strip())
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
    def current_xlog_info(self):
        """
        Get detailed information about the current WAL position in PostgreSQL.

        This method returns a dictionary containing the following data:

         * location
         * file_name
         * file_offset
         * timestamp

        When executed on a standby server file_name and file_offset are always
        None

        :rtype: psycopg2.extras.DictRow
        """
        try:
            cur = self._cursor(cursor_factory=DictCursor)
            if not self.is_in_recovery:
                cur.execute(
                    "SELECT location, "
                    "({pg_walfile_name_offset}(location)).*, "
                    "CURRENT_TIMESTAMP AS timestamp "
                    "FROM {pg_current_wal_lsn}() AS location"
                    .format(**self.name_map))
                return cur.fetchone()
            else:
                cur.execute(
                    "SELECT location, "
                    "NULL AS file_name, "
                    "NULL AS file_offset, "
                    "CURRENT_TIMESTAMP AS timestamp "
                    "FROM {pg_last_wal_replay_lsn}() AS location"
                    .format(**self.name_map))
                return cur.fetchone()
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving current xlog "
                          "detailed information: %s",
                          str(e).strip())
        return None

    @property
    def current_xlog_file_name(self):
        """
        Get current WAL file from PostgreSQL

        :return str: current WAL file in PostgreSQL
        """
        current_xlog_info = self.current_xlog_info
        if current_xlog_info is not None:
            return current_xlog_info['file_name']
        return None

    @property
    def xlog_segment_size(self):
        """
        Retrieve the size of one WAL file.

        In PostgreSQL 11, users will be able to change the WAL size
        at runtime. Up to PostgreSQL 10, included, the WAL size can be changed
        at compile time

        :return: The wal size (In bytes)
        """

        # Prior to PostgreSQL 8.4, the wal segment size was not configurable,
        # even in compilation
        if self.server_version < 80400:
            return DEFAULT_XLOG_SEG_SIZE

        try:
            cur = self._cursor(cursor_factory=DictCursor)

            # We can't use the `get_setting` method here, because it
            # use `SHOW`, returning an human readable value such as "16MB",
            # while we prefer a raw value such as 16777216.
            cur.execute("SELECT setting "
                        "FROM pg_settings "
                        "WHERE name='wal_block_size'")
            result = cur.fetchone()
            wal_block_size = int(result[0])

            cur.execute("SELECT setting "
                        "FROM pg_settings "
                        "WHERE name='wal_segment_size'")
            result = cur.fetchone()
            wal_segment_size = int(result[0])
            return wal_block_size * wal_segment_size
        except ValueError as e:
            _logger.error("Error retrieving current xlog "
                          "segment size: %s",
                          str(e).strip())
            return None

    @property
    def current_xlog_location(self):
        """
        Get current WAL location from PostgreSQL

        :return str: current WAL location in PostgreSQL
        """
        current_xlog_info = self.current_xlog_info
        if current_xlog_info is not None:
            return current_xlog_info['location']
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
            cur = self._cursor(cursor_factory=DictCursor)
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
                "OR last_failed_time <= last_archived_time) "
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
            'is_in_recovery',
            'current_xlog',
            'pgespresso_installed',
            'replication_slot_support',
            'replication_slot',
            'synchronous_standby_names',
        ]
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
            result['is_in_recovery'] = self.is_in_recovery
            result['server_txt_version'] = self.server_txt_version
            result['pgespresso_installed'] = self.has_pgespresso
            result['current_xlog'] = self.current_xlog_file_name
            result['current_size'] = self.current_size

            result.update(self.get_configuration_files())

            # Retrieve the replication_slot status
            result["replication_slot_support"] = False
            if self.server_version >= 90400:
                result["replication_slot_support"] = True
                if self.config.slot_name is not None:
                    result["replication_slot"] = (
                        self.get_replication_slot(self.config.slot_name))

            # Retrieve the list of synchronous standby names
            result["synchronous_standby_names"] = []
            if self.server_version >= 90100:
                result["synchronous_standby_names"] = (
                    self.get_synchronous_standby_names())

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

    def start_exclusive_backup(self, label):
        """
        Calls pg_start_backup() on the PostgreSQL server

        This method returns a dictionary containing the following data:

         * location
         * file_name
         * file_offset
         * timestamp

        :param str label: descriptive string to identify the backup
        :rtype: psycopg2.extras.DictRow
        """
        try:
            conn = self.connect()

            # Rollback to release the transaction, as the pg_start_backup
            # invocation can last up to PostgreSQL's checkpoint_timeout
            conn.rollback()

            # Start an exclusive backup
            cur = conn.cursor(cursor_factory=DictCursor)
            if self.server_version < 80400:
                cur.execute(
                    "SELECT location, "
                    "({pg_walfile_name_offset}(location)).*, "
                    "now() AS timestamp "
                    "FROM pg_start_backup(%s) AS location"
                    .format(**self.name_map),
                    (label,))
            else:
                cur.execute(
                    "SELECT location, "
                    "({pg_walfile_name_offset}(location)).*, "
                    "now() AS timestamp "
                    "FROM pg_start_backup(%s,%s) AS location"
                    .format(**self.name_map),
                    (label, self.config.immediate_checkpoint))

            start_row = cur.fetchone()

            # Rollback to release the transaction, as the connection
            # is to be retained until the end of backup
            conn.rollback()

            return start_row
        except (PostgresConnectionError, psycopg2.Error) as e:
            msg = "pg_start_backup(): %s" % str(e).strip()
            _logger.debug(msg)
            raise PostgresException(msg)

    def start_concurrent_backup(self, label):
        """
        Calls pg_start_backup on the PostgreSQL server using the
        API introduced with version 9.6

        This method returns a dictionary containing the following data:

         * location
         * timeline
         * timestamp

        :param str label: descriptive string to identify the backup
        :rtype: psycopg2.extras.DictRow
        """
        try:
            conn = self.connect()

            # Rollback to release the transaction, as the pg_start_backup
            # invocation can last up to PostgreSQL's checkpoint_timeout
            conn.rollback()

            # Start the backup using the api introduced in postgres 9.6
            cur = conn.cursor(cursor_factory=DictCursor)
            cur.execute(
                "SELECT location, "
                "(SELECT timeline_id "
                "FROM pg_control_checkpoint()) AS timeline, "
                "now() AS timestamp "
                "FROM pg_start_backup(%s, %s, FALSE) AS location",
                (label, self.config.immediate_checkpoint))
            start_row = cur.fetchone()

            # Rollback to release the transaction, as the connection
            # is to be retained until the end of backup
            conn.rollback()

            return start_row
        except (PostgresConnectionError, psycopg2.Error) as e:
            msg = "pg_start_backup command: %s" % (str(e).strip(),)
            _logger.debug(msg)
            raise PostgresException(msg)

    def stop_exclusive_backup(self):
        """
        Calls pg_stop_backup() on the PostgreSQL server

        This method returns a dictionary containing the following data:

         * location
         * file_name
         * file_offset
         * timestamp

        :rtype: psycopg2.extras.DictRow
        """
        try:
            conn = self.connect()

            # Rollback to release the transaction, as the pg_stop_backup
            # invocation could will wait until the current WAL file is shipped
            conn.rollback()

            # Stop the backup
            cur = conn.cursor(cursor_factory=DictCursor)
            cur.execute(
                "SELECT location, "
                "({pg_walfile_name_offset}(location)).*, "
                "now() AS timestamp "
                "FROM pg_stop_backup() AS location"
                .format(**self.name_map)
            )

            return cur.fetchone()
        except (PostgresConnectionError, psycopg2.Error) as e:
            msg = "Error issuing pg_stop_backup command: %s" % str(e).strip()
            _logger.debug(msg)
            raise PostgresException(
                'Cannot terminate exclusive backup. '
                'You might have to manually execute pg_stop_backup '
                'on your PostgreSQL server')

    def stop_concurrent_backup(self):
        """
        Calls pg_stop_backup on the PostgreSQL server using the
        API introduced with version 9.6

        This method returns a dictionary containing the following data:

         * location
         * timeline
         * backup_label
         * timestamp

        :rtype: psycopg2.extras.DictRow
        """
        try:
            conn = self.connect()

            # Rollback to release the transaction, as the pg_stop_backup
            # invocation could will wait until the current WAL file is shipped
            conn.rollback()

            # Stop the backup  using the api introduced with version 9.6
            cur = conn.cursor(cursor_factory=DictCursor)
            cur.execute(
                'SELECT end_row.lsn AS location, '
                '(SELECT CASE WHEN pg_is_in_recovery() '
                'THEN min_recovery_end_timeline ELSE timeline_id END '
                'FROM pg_control_checkpoint(), pg_control_recovery()'
                ') AS timeline, '
                'end_row.labelfile AS backup_label, '
                'now() AS timestamp FROM pg_stop_backup(FALSE) AS end_row')

            return cur.fetchone()
        except (PostgresConnectionError, psycopg2.Error) as e:
            msg = "Error issuing pg_stop_backup command: %s" % str(e).strip()
            _logger.debug(msg)
            raise PostgresException(msg)

    def pgespresso_start_backup(self, label):
        """
        Execute a pgespresso_start_backup

        This method returns a dictionary containing the following data:

         * backup_label
         * timestamp

        :param str label: descriptive string to identify the backup
        :rtype: psycopg2.extras.DictRow
        """
        try:
            conn = self.connect()

            # Rollback to release the transaction,
            # as the pgespresso_start_backup invocation can last
            # up to PostgreSQL's checkpoint_timeout
            conn.rollback()

            # Start the concurrent backup using pgespresso
            cur = conn.cursor(cursor_factory=DictCursor)
            cur.execute(
                'SELECT pgespresso_start_backup(%s,%s) AS backup_label, '
                'now() AS timestamp',
                (label, self.config.immediate_checkpoint))

            start_row = cur.fetchone()

            # Rollback to release the transaction, as the connection
            # is to be retained until the end of backup
            conn.rollback()

            return start_row
        except (PostgresConnectionError, psycopg2.Error) as e:
            msg = "pgespresso_start_backup(): %s" % str(e).strip()
            _logger.debug(msg)
            raise PostgresException(msg)

    def pgespresso_stop_backup(self, backup_label):
        """
        Execute a pgespresso_stop_backup

        This method returns a dictionary containing the following data:

         * end_wal
         * timestamp

        :param str backup_label: backup label as returned
            by pgespress_start_backup
        :rtype: psycopg2.extras.DictRow
        """
        try:
            conn = self.connect()
            # Issue a rollback to release any unneeded lock
            conn.rollback()
            cur = conn.cursor(cursor_factory=DictCursor)
            cur.execute("SELECT pgespresso_stop_backup(%s) AS end_wal, "
                        "now() AS timestamp",
                        (backup_label,))
            return cur.fetchone()
        except (PostgresConnectionError, psycopg2.Error) as e:
            msg = "Error issuing pgespresso_stop_backup() command: %s" % (
                str(e).strip())
            _logger.debug(msg)
            raise PostgresException(
                '%s\n'
                'HINT: You might have to manually execute '
                'pgespresso_abort_backup() on your PostgreSQL '
                'server' % msg)

    def switch_wal(self):
        """
        Execute a pg_switch_wal()

        To be SURE of the switch of a xlog, we collect the xlogfile name
        before and after the switch.
        The method returns the just closed xlog file name if the current xlog
        file has changed, it returns an empty string otherwise.

        The method returns None if something went wrong during the execution
        of the pg_switch_wal command.

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
            cur.execute('SELECT {pg_walfile_name}('
                        '{pg_current_wal_insert_lsn}())'
                        .format(**self.name_map))
            pre_switch = cur.fetchone()[0]
            # Switch
            cur.execute('SELECT {pg_walfile_name}({pg_switch_wal}())'
                        .format(**self.name_map))
            # Collect the xlog file name after the switch
            cur.execute('SELECT {pg_walfile_name}('
                        '{pg_current_wal_insert_lsn}())'
                        .format(**self.name_map))
            post_switch = cur.fetchone()[0]
            if pre_switch < post_switch:
                return pre_switch
            else:
                return ''
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug(
                "Error issuing {pg_switch_wal}() command: %s"
                .format(**self.name_map),
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
            cur = self._cursor(cursor_factory=NamedTupleCursor)

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
            # - sent_lsn (sent_location before 10)
            # - write_lsn (write_location before 10)
            # - flush_lsn (flush_location before 10)
            # - replay_lsn (replay_location before 10)
            # - sync_priority
            # - sync_state
            #

            if self.server_version < 90100:
                raise PostgresUnsupportedFeature('9.1')

            from_repslot = ""
            if self.server_version >= 100000:
                # Current implementation (10+)
                what = "r.*, rs.slot_name"
                # Look for replication slot name
                from_repslot = "LEFT JOIN pg_replication_slots rs " \
                               "ON (r.pid = rs.active_pid) "
            elif self.server_version >= 90500:
                # PostgreSQL 9.5/9.6
                what = "pid, " \
                    "usesysid, " \
                    "usename, " \
                    "application_name, " \
                    "client_addr, " \
                    "client_hostname, " \
                    "client_port, " \
                    "backend_start, " \
                    "backend_xmin, " \
                    "state, " \
                    "sent_location AS sent_lsn, " \
                    "write_location AS write_lsn, " \
                    "flush_location AS flush_lsn, " \
                    "replay_location AS replay_lsn, " \
                    "sync_priority, " \
                    "sync_state, " \
                    "rs.slot_name"
                # Look for replication slot name
                from_repslot = "LEFT JOIN pg_replication_slots rs " \
                               "ON (r.pid = rs.active_pid) "
            elif self.server_version >= 90400:
                # PostgreSQL 9.4
                what = "pid, " \
                    "usesysid, " \
                    "usename, " \
                    "application_name, " \
                    "client_addr, " \
                    "client_hostname, " \
                    "client_port, " \
                    "backend_start, " \
                    "backend_xmin, " \
                    "state, " \
                    "sent_location AS sent_lsn, " \
                    "write_location AS write_lsn, " \
                    "flush_location AS flush_lsn, " \
                    "replay_location AS replay_lsn, " \
                    "sync_priority, " \
                    "sync_state"
            elif self.server_version >= 90200:
                # PostgreSQL 9.2/9.3
                what = "pid, " \
                    "usesysid, " \
                    "usename, " \
                    "application_name, " \
                    "client_addr, " \
                    "client_hostname, " \
                    "client_port, " \
                    "backend_start, " \
                    "CAST (NULL AS xid) AS backend_xmin, " \
                    "state, " \
                    "sent_location AS sent_lsn, " \
                    "write_location AS write_lsn, " \
                    "flush_location AS flush_lsn, " \
                    "replay_location AS replay_lsn, " \
                    "sync_priority, " \
                    "sync_state"
            else:
                # PostgreSQL 9.1
                what = "procpid AS pid, " \
                    "usesysid, " \
                    "usename, " \
                    "application_name, " \
                    "client_addr, " \
                    "client_hostname, " \
                    "client_port, " \
                    "backend_start, " \
                    "CAST (NULL AS xid) AS backend_xmin, " \
                    "state, " \
                    "sent_location AS sent_lsn, " \
                    "write_location AS write_lsn, " \
                    "flush_location AS flush_lsn, " \
                    "replay_location AS replay_lsn, " \
                    "sync_priority, " \
                    "sync_state"

            # Streaming client
            if client_type == self.STANDBY:
                # Standby server
                where = 'WHERE {replay_lsn} IS NOT NULL '.format(
                    **self.name_map)
            elif client_type == self.WALSTREAMER:
                # WAL streamer
                where = 'WHERE {replay_lsn} IS NULL '.format(
                    **self.name_map)
            else:
                where = ''

            # Execute the query
            cur.execute(
                "SELECT %s, "
                "pg_is_in_recovery() AS is_in_recovery, "
                "CASE WHEN pg_is_in_recovery() "
                "  THEN {pg_last_wal_receive_lsn}() "
                "  ELSE {pg_current_wal_lsn}() "
                "END AS current_lsn "
                "FROM pg_stat_replication r "
                "%s"
                "%s"
                "ORDER BY sync_state DESC, sync_priority"
                .format(**self.name_map)
                % (what, from_repslot, where))

            # Generate a list of standby objects
            return cur.fetchall()
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.debug("Error retrieving status of standby servers: %s",
                          str(e).strip())
            return None

    def get_replication_slot(self, slot_name):
        """
        Retrieve from the PostgreSQL server a physical replication slot
        with a specific slot_name.

        This method returns a dictionary containing the following data:

         * slot_name
         * active
         * restart_lsn

        :param str slot_name: the replication slot name
        :rtype: psycopg2.extras.DictRow
        """
        if self.server_version < 90400:
            # Raise exception if replication slot are not supported
            # by PostgreSQL version
            raise PostgresUnsupportedFeature('9.4')
        else:
            cur = self._cursor(cursor_factory=NamedTupleCursor)
            try:
                cur.execute("SELECT slot_name, "
                            "active, "
                            "restart_lsn "
                            "FROM pg_replication_slots "
                            "WHERE slot_type = 'physical' "
                            "AND slot_name = '%s'" % slot_name)
                # Retrieve the replication slot information
                return cur.fetchone()
            except (PostgresConnectionError, psycopg2.Error) as e:
                _logger.debug("Error retrieving replication_slots: %s",
                              str(e).strip())
                raise

    def get_synchronous_standby_names(self):
        """
        Retrieve the list of named synchronous standby servers from PostgreSQL

        This method returns a list of names

        :return list: synchronous standby names
        """
        if self.server_version < 90100:
            # Raise exception if synchronous replication is not supported
            raise PostgresUnsupportedFeature('9.1')
        else:
            synchronous_standby_names = (
                self.get_setting('synchronous_standby_names'))
            # Return empty list if not defined
            if synchronous_standby_names is None:
                return []
            # Normalise the list of sync standby names
            # On PostgreSQL 9.6 it is possible to specify the number of
            # required synchronous standby using this format:
            # n (name1, name2, ... nameN).
            # We only need the name list, so we discard everything else.

            # The name list starts after the first parenthesis or at pos 0
            names_start = synchronous_standby_names.find('(') + 1
            names_end = synchronous_standby_names.rfind(')')
            if names_end < 0:
                names_end = len(synchronous_standby_names)
            names_list = synchronous_standby_names[names_start:names_end]
            return [x.strip() for x in names_list.split(',')]

    @property
    def name_map(self):
        """
        Return a map with function and directory names according to the current
        PostgreSQL version.

        Each entry has the `current` name as key and the name for the specific
        version as value.

        :rtype: dict[str]
        """
        # Avoid raising an error if the connection is not available
        try:
            server_version = self.server_version
        except PostgresConnectionError:
            _logger.debug('Impossible to detect the PostgreSQL version, '
                          'name_map will return names from latest version')
            server_version = None

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
