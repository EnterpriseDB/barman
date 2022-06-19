# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2022
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

import datetime

import psycopg2
import pytest
from mock import PropertyMock, call, patch
from psycopg2.errorcodes import DUPLICATE_OBJECT, UNDEFINED_OBJECT

from barman.exceptions import (
    PostgresConnectionError,
    PostgresDuplicateReplicationSlot,
    PostgresException,
    PostgresInvalidReplicationSlot,
    PostgresIsInRecovery,
    BackupFunctionsAccessRequired,
    PostgresObsoleteFeature,
    PostgresSuperuserRequired,
    PostgresUnsupportedFeature,
)
from barman.postgres import PostgreSQLConnection
from barman.xlog import DEFAULT_XLOG_SEG_SIZE
from testing_helpers import build_real_server


class MockProgrammingError(psycopg2.ProgrammingError):
    """
    Mock class for psycopg2 ProgrammingError
    """

    def __init__(self, pgcode=None, pgerror=None):
        # pgcode and pgerror are read only attributes and the ProgrammingError
        # class is written in native code. The only way to set these attribute
        # is to use the private method '__setstate__', which is also native
        self.__setstate__({"pgcode": pgcode, "pgerror": pgerror})


# noinspection PyMethodMayBeStatic
class TestPostgres(object):
    def test_connection_error(self):
        """
        simple test for missing conninfo
        """
        # Test with wrong configuration
        server = build_real_server(main_conf={"conninfo": ""})
        assert server.config.msg_list
        assert (
            "PostgreSQL connection: Missing 'conninfo' parameter "
            "for server 'main'" in server.config.msg_list
        )

    @patch("barman.postgres.psycopg2.connect")
    def test_connect_and_close(self, pg_connect_mock):
        """
        Check pg_connect method behaviour on error
        """
        # Setup server
        server = build_real_server()
        server.postgres.conninfo = "valid conninfo"
        conn_mock = pg_connect_mock.return_value
        conn_mock.server_version = 90401
        conn_mock.closed = False
        cursor_mock = conn_mock.cursor.return_value

        # Connection failure
        pg_connect_mock.side_effect = psycopg2.DatabaseError
        with pytest.raises(PostgresConnectionError):
            server.postgres.connect()

        # Good connection but error setting the application name
        pg_connect_mock.side_effect = None
        cursor_mock.execute.side_effect = psycopg2.ProgrammingError
        with pytest.raises(PostgresConnectionError):
            server.postgres.connect()

        # Good connection
        cursor_mock.execute.side_effect = None
        conn = server.postgres.connect()
        pg_connect_mock.assert_called_with("valid conninfo")
        assert conn is conn_mock

        # call again and make sure it returns the cached connection
        pg_connect_mock.reset_mock()

        new_conn = server.postgres.connect()

        assert new_conn is conn_mock
        assert not pg_connect_mock.called

        # call again with a broken connection
        pg_connect_mock.reset_mock()
        conn_mock.cursor.side_effect = [psycopg2.DatabaseError, cursor_mock]

        new_conn = server.postgres.connect()

        assert new_conn is conn_mock
        pg_connect_mock.assert_called_with("valid conninfo")

        # close it
        pg_connect_mock.reset_mock()
        conn_mock.cursor.side_effect = None
        conn_mock.closed = False

        server.postgres.close()

        assert conn_mock.close.called

        # close it with an already closed connection
        pg_connect_mock.reset_mock()
        conn_mock.closed = True

        server.postgres.close()

        assert not conn_mock.close.called

        # open again and verify that it is a new object
        pg_connect_mock.reset_mock()
        conn_mock.closed = False

        server.postgres.connect()

        pg_connect_mock.assert_called_with("valid conninfo")

        server.postgres.close()

        assert conn_mock.close.called

    @patch("barman.postgres.psycopg2.connect")
    def test_connect_error(self, connect_mock):
        """
        Check pg_connect method behaviour on error
        """
        # Setup temp dir and server
        server = build_real_server()
        server.postgres.conninfo = "not valid conninfo"
        connect_mock.side_effect = psycopg2.DatabaseError
        # expect pg_connect to raise a PostgresConnectionError
        with pytest.raises(PostgresConnectionError):
            server.postgres.connect()
        connect_mock.assert_called_with("not valid conninfo")

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_server_txt_version(self, conn_mock):
        """
        simple test for the server_txt_version property
        """
        # Build a server
        server = build_real_server()
        cursor_mock = conn_mock.return_value.cursor.return_value

        # Connection error
        conn_mock.side_effect = PostgresConnectionError
        assert server.postgres.server_txt_version is None

        # Communication error
        conn_mock.side_effect = None
        cursor_mock.execute.side_effect = psycopg2.ProgrammingError
        assert server.postgres.server_txt_version is None

        # Good connection
        cursor_mock.execute.side_effect = None
        cursor_mock.fetchone.return_value = (
            "PostgreSQL 9.4.5 on x86_64-apple-darwin15.0.0, compiled by "
            "Apple LLVM version 7.0.0 (clang-700.1.76), 64-bit",
        )

        assert server.postgres.server_txt_version == "9.4.5"
        cursor_mock.execute.assert_called_with("SELECT version()")

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_server_txt_version_epas(self, conn_mock):
        """
        Verify server_txt_version returns the correct Postgres version
        against EPAS 9.6 and 10, which both return "EnterpriseDB" in the
        response to `SELECT version();`.

        Versions 11 and above return the Postgres version string with the
        EnterpriseDB details appended so do not require special handling.
        """
        # Build a server
        server = build_real_server()
        cursor_mock = conn_mock.return_value.cursor.return_value

        # EPAS 9.6 returns an extra version field which must be discarded
        cursor_mock.fetchone.return_value = (
            "EnterpriseDB 9.6.23.31 on x86_64-pc-linux-gnu, compiled by "
            "gcc (GCC) 4.8.5 20150623 (Red Hat 4.8.5-44), 64-bit",
        )
        assert server.postgres.server_txt_version == "9.6.23"

        # EPAS 10 also returns an extra field relative to the corresponding
        # PostgreSQL version and it must be discarded
        cursor_mock.fetchone.return_value = (
            "EnterpriseDB 10.18.28 on x86_64-pc-linux-gnu, compiled by "
            "gcc (GCC) 4.8.5 20150623 (Red Hat 4.8.5-36), 64-bit",
        )
        assert server.postgres.server_txt_version == "10.18"

    @patch("barman.postgres.PostgreSQLConnection.connect")
    @patch(
        "barman.postgres.PostgreSQLConnection.is_in_recovery", new_callable=PropertyMock
    )
    def test_create_restore_point(self, is_in_recovery_mock, conn_mock):
        """
        Basic test for the _restore_point method
        """
        # Simulate a master connection
        is_in_recovery_mock.return_value = False

        server = build_real_server()
        # Test 1: Postgres 9.0 expect None as result
        conn_mock.return_value.server_version = 90000

        restore_point = server.postgres.create_restore_point("Test_20151026T092241")
        assert restore_point is None

        # Simulate a master connection
        is_in_recovery_mock.return_value = True

        # Test 2: Postgres 9.1 in recovery (standby) expect None as result
        conn_mock.return_value.server_version = 90100

        restore_point = server.postgres.create_restore_point("Test_20151026T092241")
        assert restore_point is None

        # Test 3: Postgres 9.1 check mock calls
        is_in_recovery_mock.return_value = False

        assert server.postgres.create_restore_point("Test_20151026T092241")

        cursor_mock = conn_mock.return_value.cursor.return_value
        cursor_mock.execute.assert_called_with(
            "SELECT pg_create_restore_point(%s)", ["Test_20151026T092241"]
        )
        assert cursor_mock.fetchone.called

        # Test error management
        cursor_mock.execute.side_effect = psycopg2.Error
        assert server.postgres.create_restore_point("Test_20151026T092241") is None

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_stop_exclusive_backup(self, conn_mock):
        """
        Basic test for the stop_exclusive_backup method

        :param conn_mock: a mock that imitates a connection to PostgreSQL
        """
        # Build a server
        server = build_real_server()

        # Test call on master, PostgreSQL older than 10
        conn_mock.return_value.server_version = 90300
        # Expect no errors on normal call
        assert server.postgres.stop_exclusive_backup()
        # check the correct invocation of the execute method
        cursor_mock = conn_mock.return_value.cursor.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT location, "
            "(pg_xlogfile_name_offset(location)).*, "
            "now() AS timestamp "
            "FROM pg_stop_backup() AS location"
        )

        # Test call on master, PostgreSQL 10
        conn_mock.reset_mock()
        conn_mock.return_value.server_version = 100000
        # Expect no errors on normal call
        assert server.postgres.stop_exclusive_backup()
        # check the correct invocation of the execute method
        cursor_mock = conn_mock.return_value.cursor.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT location, "
            "(pg_walfile_name_offset(location)).*, "
            "now() AS timestamp "
            "FROM pg_stop_backup() AS location"
        )

        # Test call on PostgreSQL 15
        conn_mock.reset_mock()
        conn_mock.return_value.server_version = 150000
        # Expect a PostgresObsoleteFeature exception when attempting
        # to use exclusive backup with >=150000
        with pytest.raises(PostgresObsoleteFeature):
            server.postgres.stop_exclusive_backup()

        # Test Error: Setup the mock to trigger an exception
        # expect the method to raise a PostgresException
        conn_mock.reset_mock()
        cursor_mock.execute.side_effect = psycopg2.Error
        # Check that the method raises a PostgresException
        with pytest.raises(PostgresException):
            server.postgres.stop_exclusive_backup()

    @pytest.mark.parametrize(
        ("server_version", "expected_stop_call"),
        [(140000, "pg_stop_backup(FALSE)"), (150000, "pg_backup_stop()")],
    )
    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_stop_concurrent_backup(self, conn, server_version, expected_stop_call):
        """
        Basic test for the stop_concurrent_backup method

        :param conn: a mock that imitates a connection to PostgreSQL
        """
        # Build a server
        server = build_real_server()
        conn.return_value.server_version = server_version

        # Expect no errors on normal call
        assert server.postgres.stop_concurrent_backup()

        # check the correct invocation of the execute method
        cursor_mock = conn.return_value.cursor.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT end_row.lsn AS location, "
            "(SELECT CASE WHEN pg_is_in_recovery() "
            "THEN min_recovery_end_timeline "
            "ELSE timeline_id END "
            "FROM pg_control_checkpoint(), pg_control_recovery()"
            ") AS timeline, "
            "end_row.labelfile AS backup_label, "
            "now() AS timestamp "
            "FROM %s AS end_row" % expected_stop_call
        )

        # Test 2: Setup the mock to trigger an exception
        # expect the method to raise a PostgresException
        conn.reset_mock()
        cursor_mock.execute.side_effect = psycopg2.Error
        # Check that the method raises a PostgresException
        with pytest.raises(PostgresException):
            server.postgres.stop_concurrent_backup()

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_pgespresso_stop_backup(self, conn):
        """
        Basic test for pgespresso_stop_backup method
        """
        # Build a server
        server = build_real_server()

        # Test 1: Expect no error and the correct call sequence
        assert server.postgres.pgespresso_stop_backup("test_label")

        cursor_mock = conn.return_value.cursor.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT pgespresso_stop_backup(%s) AS end_wal, now() AS timestamp",
            ("test_label",),
        )

        # Test 2: Setup the mock to trigger an exception
        # expect the method to raise PostgresException
        conn.reset_mock()
        cursor_mock.execute.side_effect = psycopg2.Error
        # Check that the method raises a PostgresException
        with pytest.raises(PostgresException):
            server.postgres.pgespresso_stop_backup("test_label")

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_start_exclusive_backup(self, conn):
        """
        Simple test for start_exclusive_backup method of
        the RsyncBackupExecutor class
        """
        # Build a server
        server = build_real_server()
        backup_label = "test label"

        # Expect no errors
        conn.return_value.server_version = 90300
        assert server.postgres.start_exclusive_backup(backup_label)

        # check for the correct call on the execute method
        cursor_mock = conn.return_value.cursor.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT location, "
            "(pg_xlogfile_name_offset(location)).*, "
            "now() AS timestamp "
            "FROM pg_start_backup(%s,%s) AS location",
            ("test label", False),
        )
        conn.return_value.rollback.assert_has_calls([call(), call()])
        # reset the mock for the next test
        conn.reset_mock()

        # 8.3 test
        conn.return_value.server_version = 80300
        assert server.postgres.start_exclusive_backup(backup_label)
        # check for the correct call on the execute method
        cursor_mock.execute.assert_called_once_with(
            "SELECT location, "
            "(pg_xlogfile_name_offset(location)).*, "
            "now() AS timestamp "
            "FROM pg_start_backup(%s) AS location",
            ("test label",),
        )
        conn.return_value.rollback.assert_has_calls([call(), call()])
        # reset the mock for the next test
        conn.reset_mock()

        # 10 test
        conn.return_value.server_version = 100000
        assert server.postgres.start_exclusive_backup(backup_label)
        # check for the correct call on the execute method
        cursor_mock.execute.assert_called_once_with(
            "SELECT location, "
            "(pg_walfile_name_offset(location)).*, "
            "now() AS timestamp "
            "FROM pg_start_backup(%s,%s) AS location",
            ("test label", False),
        )
        conn.return_value.rollback.assert_has_calls([call(), call()])
        # reset the mock for the next test
        conn.reset_mock()

        # Test call on PostgreSQL 15
        conn.return_value.server_version = 150000
        # Expect a PostgresObsoleteFeature exception when attempting
        # to use exclusive backup with >=150000
        with pytest.raises(PostgresObsoleteFeature):
            server.postgres.start_exclusive_backup(backup_label)
        # check for the correct call on the execute method
        conn.reset_mock()

        # test error management
        cursor_mock.execute.side_effect = psycopg2.Error
        with pytest.raises(PostgresException):
            server.postgres.start_exclusive_backup(backup_label)
        conn.return_value.rollback.assert_called_once_with()

    @pytest.mark.parametrize(
        ("server_version", "expected_start_fun", "expected_start_args"),
        [
            (140000, "pg_start_backup", "%s, %s, FALSE"),
            (150000, "pg_backup_start", "%s, %s"),
        ],
    )
    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_start_concurrent_backup(
        self, conn, server_version, expected_start_fun, expected_start_args
    ):
        """
        Simple test for start_exclusive_backup method of
        the RsyncBackupExecutor class
        """
        # Build a server
        server = build_real_server()
        label = "test label"

        conn.return_value.server_version = server_version

        # Expect no errors
        assert server.postgres.start_concurrent_backup(label)

        # check for the correct call on the execute method
        cursor_mock = conn.return_value.cursor.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT location, "
            "(SELECT timeline_id "
            "FROM pg_control_checkpoint()) AS timeline, "
            "now() AS timestamp "
            "FROM %s(%s) AS location" % (expected_start_fun, expected_start_args),
            ("test label", False),
        )
        conn.return_value.rollback.assert_has_calls([call(), call()])
        # reset the mock for the next test
        conn.reset_mock()

        # test error management
        cursor_mock.execute.side_effect = psycopg2.Error
        with pytest.raises(PostgresException):
            server.postgres.start_concurrent_backup(label)
        conn.return_value.rollback.assert_called_once_with()

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_pgespresso_start_backup(self, conn):
        """
        Simple test for _pgespresso_start_backup method
        of the RsyncBackupExecutor class
        """
        # Build and configure a server
        server = build_real_server()
        backup_label = "test label"

        # expect no errors
        assert server.postgres.pgespresso_start_backup(backup_label)

        cursor_mock = conn.return_value.cursor.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT pgespresso_start_backup(%s,%s) AS backup_label, "
            "now() AS timestamp",
            (backup_label, server.config.immediate_checkpoint),
        )
        conn.return_value.rollback.assert_has_calls([call(), call()])
        # reset the mock for the next test
        conn.reset_mock()

        # Test 2: Setup the mock to trigger an exception
        # expect the method to return None
        cursor_mock.execute.side_effect = psycopg2.Error
        # Check that the method returns None as result
        with pytest.raises(Exception):
            server.postgres.pgespresso_start_backup("test_label")
        conn.return_value.rollback.assert_called_once_with()

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_get_setting(self, conn):
        """
        Simple test for retrieving settings from the database
        """
        # Build and configure a server
        server = build_real_server()

        # expect no errors
        server.postgres.get_setting("test_setting")
        cursor_mock = conn.return_value.cursor.return_value
        cursor_mock.execute.assert_called_once_with(
            'SHOW "%s"' % "test_setting".replace('"', '""')
        )
        # reset the mock for the second test
        conn.reset_mock()

        # Test 2: Setup the mock to trigger an exception
        # expect the method to return None
        cursor_mock.execute.side_effect = psycopg2.Error
        # Check that the method returns None as result
        assert server.postgres.get_setting("test_setting") is None

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_get_systemid(self, conn):
        """
        Simple test for retrieving the systemid from the database
        """
        # Build and configure a server
        server = build_real_server()
        conn.return_value.server_version = 90600

        # expect no errors
        server.postgres.get_systemid()
        cursor_mock = conn.return_value.cursor.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT system_identifier::text FROM pg_control_system()"
        )
        # reset the mock for the second test
        conn.reset_mock()

        # Test 2: Setup the mock to trigger an exception
        # expect the method to return None
        cursor_mock.execute.side_effect = psycopg2.Error
        # Check that the method returns None as result
        assert server.postgres.get_systemid() is None
        # reset the mock for the third test
        conn.reset_mock()

        # Test 3: setup the mock to return a PostgreSQL version that
        # don't support pg_control_system()
        conn.return_value.server_version = 90500
        assert server.postgres.get_systemid() is None

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_get_tablespaces(self, conn):
        """
        Simple test for pg_start_backup method of the RsyncBackupExecutor class
        """
        # Build a server
        server = build_real_server()
        cursor_mock = conn.return_value.cursor.return_value
        cursor_mock.fetchall.return_value = [("tbs1", "1234", "/tmp")]
        # Expect no errors
        conn.return_value.server_version = 90400
        tbs = server.postgres.get_tablespaces()
        # check for the correct call on the execute method
        cursor_mock.execute.assert_called_once_with(
            "SELECT spcname, oid, "
            "pg_tablespace_location(oid) AS spclocation "
            "FROM pg_tablespace "
            "WHERE pg_tablespace_location(oid) != ''"
        )
        assert tbs == [("tbs1", "1234", "/tmp")]
        conn.reset_mock()

        # 8.3 test
        conn.return_value.server_version = 80300
        cursor_mock.fetchall.return_value = [("tbs2", "5234", "/tmp1")]
        tbs = server.postgres.get_tablespaces()
        # check for the correct call on the execute method
        cursor_mock.execute.assert_called_once_with(
            "SELECT spcname, oid, spclocation "
            "FROM pg_tablespace WHERE spclocation != ''"
        )
        assert tbs == [("tbs2", "5234", "/tmp1")]

        conn.reset_mock()
        # test error management
        cursor_mock.execute.side_effect = psycopg2.Error
        assert server.postgres.get_tablespaces() is None

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_get_archiver_stats(self, conn):
        """
        Simple test for pg_start_backup method of the RsyncBackupExecutor class
        """
        # Build a server
        server = build_real_server()
        cursor_mock = conn.return_value.cursor.return_value
        # expect None as result for server version <9.4
        conn.return_value.server_version = 80300
        assert server.postgres.get_archiver_stats() is None

        # expect no errors with version >= 9.4
        conn.reset_mock()
        conn.return_value.server_version = 90400
        cursor_mock.fetchone.return_value = {"a": "b"}
        assert server.postgres.get_archiver_stats() == {"a": "b"}
        # check for the correct call on the execute method
        cursor_mock.execute.assert_called_once_with(
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
            "FROM pg_stat_archiver"
        )
        conn.reset_mock()

        # test error management
        cursor_mock.execute.side_effect = psycopg2.Error
        assert server.postgres.get_archiver_stats() is None

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_get_configuration_files(self, conn_mock):
        """
        simple test for the get_configuration_files method
        """
        # Build a server
        server = build_real_server()
        conn_mock.return_value.server_version = 80400
        cursor_mock = conn_mock.return_value.cursor.return_value
        test_conf_files = [
            ("config_file", "/tmp/postgresql.conf"),
            ("hba_file", "/tmp/pg_hba.conf"),
            ("ident_file", "/tmp/pg_ident.conf"),
        ]
        cursor_mock.fetchall.side_effect = [test_conf_files, [("/test/file",)]]
        retval = server.postgres.get_configuration_files()

        assert retval == server.postgres.configuration_files
        assert server.postgres.configuration_files == dict(
            test_conf_files + [("included_files", ["/test/file"])]
        )
        cursor_mock.execute.assert_any_call(
            "SELECT name, setting FROM pg_settings "
            "WHERE name IN ('config_file', 'hba_file', 'ident_file')"
        )
        cursor_mock.execute.assert_any_call(
            "SELECT DISTINCT sourcefile AS included_file "
            "FROM pg_settings "
            "WHERE sourcefile IS NOT NULL "
            "AND sourcefile NOT IN "
            "(SELECT setting FROM pg_settings "
            "WHERE name = 'config_file') "
            "ORDER BY 1"
        )

        # Call it again, should not fetch the data twice
        conn_mock.reset_mock()
        retval = server.postgres.get_configuration_files()
        assert retval == server.postgres.configuration_files
        assert not cursor_mock.execute.called

        # Reset mock and configuration files
        conn_mock.reset_mock()
        server.postgres.configuration_files = None

        # Test error management
        cursor_mock.execute.side_effect = PostgresConnectionError
        assert server.postgres.get_configuration_files() == {}

        cursor_mock.execute.side_effect = psycopg2.ProgrammingError
        assert server.postgres.get_configuration_files() == {}

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_has_pgespresso(self, conn_mock):
        """
        simple test for has_pgespresso property
        """
        # Build a server
        server = build_real_server()
        cursor_mock = conn_mock.return_value.cursor.return_value

        # Too old
        conn_mock.return_value.server_version = 90000
        assert not server.postgres.has_pgespresso

        # Extension present
        conn_mock.return_value.server_version = 90100
        cursor_mock.fetchone.return_value = [1]
        assert server.postgres.has_pgespresso
        cursor_mock.execute.assert_called_once_with(
            "SELECT count(*) FROM pg_extension WHERE extname = 'pgespresso'"
        )

        # Extension not present
        cursor_mock.fetchone.return_value = [0]
        assert not server.postgres.has_pgespresso

        # Reset mock
        conn_mock.reset_mock()

        # Test error management
        cursor_mock.execute.side_effect = PostgresConnectionError
        assert server.postgres.has_pgespresso is None

        cursor_mock.execute.side_effect = psycopg2.ProgrammingError
        assert server.postgres.has_pgespresso is None

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_is_in_recovery(self, conn_mock):
        """
        simple test for is_in_recovery property
        """
        # Build a server
        server = build_real_server()
        cursor_mock = conn_mock.return_value.cursor.return_value

        # Too old
        conn_mock.return_value.server_version = 80400
        assert not server.postgres.is_in_recovery

        # In recovery
        conn_mock.return_value.server_version = 90100
        cursor_mock.fetchone.return_value = [True]
        assert server.postgres.is_in_recovery
        cursor_mock.execute.assert_called_once_with("SELECT pg_is_in_recovery()")

        # Not in recovery
        cursor_mock.fetchone.return_value = [False]
        assert not server.postgres.is_in_recovery

        # Reset mock
        conn_mock.reset_mock()

        # Test error management
        cursor_mock.execute.side_effect = PostgresConnectionError
        assert server.postgres.is_in_recovery is None

        cursor_mock.execute.side_effect = psycopg2.ProgrammingError
        assert server.postgres.is_in_recovery is None

    @patch("barman.postgres.PostgreSQLConnection.connect")
    @patch(
        "barman.postgres.PostgreSQLConnection.is_in_recovery", new_callable=PropertyMock
    )
    def test_current_xlog_info(self, is_in_recovery_mock, conn_mock):
        """
        Test correct select xlog_loc
        """
        # Build and configure a server using a mock
        server = build_real_server()
        cursor_mock = conn_mock.return_value.cursor.return_value
        timestamp = datetime.datetime(2016, 3, 30, 17, 4, 20, 271376)
        current_xlog_info = dict(
            location="0/35000528",
            file_name="000000010000000000000035",
            file_offset=1320,
            timestamp=timestamp,
        )
        cursor_mock.fetchone.return_value = current_xlog_info

        # Test call on master, PostgreSQL older than 10
        conn_mock.return_value.server_version = 90300
        is_in_recovery_mock.return_value = False
        remote_loc = server.postgres.current_xlog_info
        assert remote_loc == current_xlog_info
        cursor_mock.execute.assert_called_once_with(
            "SELECT location, (pg_xlogfile_name_offset(location)).*, "
            "CURRENT_TIMESTAMP AS timestamp "
            "FROM pg_current_xlog_location() AS location"
        )

        # Check call on standby, PostgreSQL older than 10
        conn_mock.reset_mock()
        conn_mock.return_value.server_version = 90300
        is_in_recovery_mock.return_value = True
        current_xlog_info["file_name"] = None
        current_xlog_info["file_offset"] = None
        remote_loc = server.postgres.current_xlog_info
        assert remote_loc == current_xlog_info
        cursor_mock.execute.assert_called_once_with(
            "SELECT location, NULL AS file_name, NULL AS file_offset, "
            "CURRENT_TIMESTAMP AS timestamp "
            "FROM pg_last_xlog_replay_location() AS location"
        )

        # Test call on master, PostgreSQL 10
        conn_mock.reset_mock()
        conn_mock.return_value.server_version = 100000
        is_in_recovery_mock.return_value = False
        remote_loc = server.postgres.current_xlog_info
        assert remote_loc == current_xlog_info
        cursor_mock.execute.assert_called_once_with(
            "SELECT location, (pg_walfile_name_offset(location)).*, "
            "CURRENT_TIMESTAMP AS timestamp "
            "FROM pg_current_wal_lsn() AS location"
        )

        # Check call on standby, PostgreSQL 10
        conn_mock.reset_mock()
        conn_mock.return_value.server_version = 100000
        is_in_recovery_mock.return_value = True
        current_xlog_info["file_name"] = None
        current_xlog_info["file_offset"] = None
        remote_loc = server.postgres.current_xlog_info
        assert remote_loc == current_xlog_info
        cursor_mock.execute.assert_called_once_with(
            "SELECT location, NULL AS file_name, NULL AS file_offset, "
            "CURRENT_TIMESTAMP AS timestamp "
            "FROM pg_last_wal_replay_lsn() AS location"
        )

        # Reset mock
        conn_mock.reset_mock()

        # Test error management
        cursor_mock.execute.side_effect = PostgresConnectionError
        assert server.postgres.current_xlog_info is None

        cursor_mock.execute.side_effect = psycopg2.ProgrammingError
        assert server.postgres.current_xlog_info is None

    @patch("barman.postgres.PostgreSQLConnection.connect")
    @patch(
        "barman.postgres.PostgreSQLConnection.is_in_recovery", new_callable=PropertyMock
    )
    def test_current_xlog_file_name(self, is_in_recovery_mock, conn_mock):
        """
        simple test for current_xlog property
        """
        # Build a server
        server = build_real_server()
        conn_mock.return_value.server_version = 90300
        cursor_mock = conn_mock.return_value.cursor.return_value

        timestamp = datetime.datetime(2016, 3, 30, 17, 4, 20, 271376)
        cursor_mock.fetchone.return_value = dict(
            location="0/35000528",
            file_name="000000010000000000000035",
            file_offset=1320,
            timestamp=timestamp,
        )

        # Special way to mock a property
        is_in_recovery_mock.return_value = False
        assert server.postgres.current_xlog_file_name == ("000000010000000000000035")

        # Reset mock
        conn_mock.reset_mock()

        # Test error management
        cursor_mock.execute.side_effect = PostgresConnectionError
        assert server.postgres.current_xlog_file_name is None

        cursor_mock.execute.side_effect = psycopg2.ProgrammingError
        assert server.postgres.current_xlog_file_name is None

    @patch("barman.postgres.psycopg2.connect")
    @patch(
        "barman.postgres.PostgreSQLConnection.xlog_segment_size",
        new_callable=PropertyMock,
    )
    @patch(
        "barman.postgres.PostgreSQLConnection.checkpoint_timeout",
        new_callable=PropertyMock,
    )
    @patch(
        "barman.postgres.PostgreSQLConnection.archive_timeout",
        new_callable=PropertyMock,
    )
    @patch(
        "barman.postgres.PostgreSQLConnection.is_in_recovery", new_callable=PropertyMock
    )
    @patch(
        "barman.postgres.PostgreSQLConnection.has_backup_privileges",
        new_callable=PropertyMock,
    )
    @patch(
        "barman.postgres.PostgreSQLConnection.is_superuser", new_callable=PropertyMock
    )
    @patch(
        "barman.postgres.PostgreSQLConnection.server_txt_version",
        new_callable=PropertyMock,
    )
    @patch(
        "barman.postgres.PostgreSQLConnection.has_pgespresso", new_callable=PropertyMock
    )
    @patch(
        "barman.postgres.PostgreSQLConnection.current_xlog_info",
        new_callable=PropertyMock,
    )
    @patch(
        "barman.postgres.PostgreSQLConnection.current_size", new_callable=PropertyMock
    )
    @patch("barman.postgres.PostgreSQLConnection.get_configuration_files")
    @patch("barman.postgres.PostgreSQLConnection.get_setting")
    @patch("barman.postgres.PostgreSQLConnection.get_synchronous_standby_names")
    @patch("barman.postgres.PostgreSQLConnection.get_systemid")
    def test_get_remote_status(
        self,
        get_systemid_mock,
        get_synchronous_standby_names_mock,
        get_setting_mock,
        get_configuration_files_mock,
        current_size_mock,
        current_xlog_info,
        has_pgespresso_mock,
        server_txt_version_mock,
        is_superuser_mock,
        has_backup_privileges_mock,
        is_in_recovery_mock,
        archive_timeout_mock,
        checkpoint_timeout_mock,
        xlog_segment_size,
        conn_mock,
    ):
        """
        simple test for the fetch_remote_status method
        """
        # Build a server
        server = build_real_server()
        current_xlog_info.return_value = {
            "location": "DE/ADBEEF",
            "file_name": "00000001000000DE00000000",
            "file_offset": 11386607,
            "timestamp": datetime.datetime(2016, 3, 30, 17, 4, 20, 271376),
        }
        current_size_mock.return_value = 497354072
        has_pgespresso_mock.return_value = True
        server_txt_version_mock.return_value = "9.5.0"
        is_in_recovery_mock.return_value = False
        has_backup_privileges_mock.return_value = True
        is_superuser_mock.return_value = True
        get_configuration_files_mock.return_value = {"a": "b"}
        get_synchronous_standby_names_mock.return_value = []
        conn_mock.return_value.server_version = 90500
        archive_timeout_mock.return_value = 300
        checkpoint_timeout_mock.return_value = 600
        xlog_segment_size.return_value = 2 << 22
        get_systemid_mock.return_value = 6721602258895701769

        settings = {
            "data_directory": "a directory",
            "wal_level": "a wal_level value",
            "hot_standby": "a hot_standby value",
            "max_wal_senders": "a max_wal_senderse value",
            "data_checksums": "a data_checksums",
            "max_replication_slots": "a max_replication_slots value",
            "wal_compression": "a wal_compression value",
            "wal_keep_segments": "a wal_keep_segments value",
            "wal_keep_size": "a wal_keep_size value",
        }

        get_setting_mock.side_effect = lambda x: settings.get(x, "unknown")

        # Test PostgreSQL < 9.6
        result = server.postgres.fetch_remote_status()
        assert result == {
            "a": "b",
            "is_superuser": True,
            "has_backup_privileges": True,
            "is_in_recovery": False,
            "current_lsn": "DE/ADBEEF",
            "current_xlog": "00000001000000DE00000000",
            "data_directory": "a directory",
            "pgespresso_installed": True,
            "server_txt_version": "9.5.0",
            "wal_level": "a wal_level value",
            "current_size": 497354072,
            "replication_slot_support": True,
            "replication_slot": None,
            "synchronous_standby_names": [],
            "archive_timeout": 300,
            "checkpoint_timeout": 600,
            "wal_keep_segments": "a wal_keep_segments value",
            "hot_standby": "a hot_standby value",
            "max_wal_senders": "a max_wal_senderse value",
            "data_checksums": "a data_checksums",
            "max_replication_slots": "a max_replication_slots value",
            "wal_compression": "a wal_compression value",
            "xlog_segment_size": 8388608,
            "postgres_systemid": None,
        }

        # Test PostgreSQL 9.6
        conn_mock.return_value.server_version = 90600
        result = server.postgres.fetch_remote_status()
        assert result == {
            "a": "b",
            "is_superuser": True,
            "has_backup_privileges": True,
            "is_in_recovery": False,
            "current_lsn": "DE/ADBEEF",
            "current_xlog": "00000001000000DE00000000",
            "data_directory": "a directory",
            "pgespresso_installed": True,
            "server_txt_version": "9.5.0",
            "wal_level": "a wal_level value",
            "current_size": 497354072,
            "replication_slot_support": True,
            "replication_slot": None,
            "synchronous_standby_names": [],
            "archive_timeout": 300,
            "checkpoint_timeout": 600,
            "wal_keep_segments": "a wal_keep_segments value",
            "hot_standby": "a hot_standby value",
            "max_wal_senders": "a max_wal_senderse value",
            "data_checksums": "a data_checksums",
            "max_replication_slots": "a max_replication_slots value",
            "wal_compression": "a wal_compression value",
            "xlog_segment_size": 8388608,
            "postgres_systemid": 6721602258895701769,
        }

        # Test PostgreSQL 13
        conn_mock.return_value.server_version = 130000
        result = server.postgres.fetch_remote_status()
        assert result == {
            "a": "b",
            "is_superuser": True,
            "has_backup_privileges": True,
            "is_in_recovery": False,
            "current_lsn": "DE/ADBEEF",
            "current_xlog": "00000001000000DE00000000",
            "data_directory": "a directory",
            "pgespresso_installed": True,
            "server_txt_version": "9.5.0",
            "wal_level": "a wal_level value",
            "current_size": 497354072,
            "replication_slot_support": True,
            "replication_slot": None,
            "synchronous_standby_names": [],
            "archive_timeout": 300,
            "checkpoint_timeout": 600,
            "wal_keep_size": "a wal_keep_size value",
            "hot_standby": "a hot_standby value",
            "max_wal_senders": "a max_wal_senderse value",
            "data_checksums": "a data_checksums",
            "max_replication_slots": "a max_replication_slots value",
            "wal_compression": "a wal_compression value",
            "xlog_segment_size": 8388608,
            "postgres_systemid": 6721602258895701769,
        }

        # Test error management
        server.postgres.close()
        conn_mock.side_effect = psycopg2.DatabaseError
        assert server.postgres.fetch_remote_status() == {
            "is_superuser": None,
            "is_in_recovery": None,
            "current_xlog": None,
            "data_directory": None,
            "pgespresso_installed": None,
            "server_txt_version": None,
            "replication_slot_support": None,
            "replication_slot": None,
            "synchronous_standby_names": None,
            "postgres_systemid": None,
        }

        get_setting_mock.side_effect = psycopg2.ProgrammingError
        assert server.postgres.fetch_remote_status() == {
            "is_superuser": None,
            "is_in_recovery": None,
            "current_xlog": None,
            "data_directory": None,
            "pgespresso_installed": None,
            "server_txt_version": None,
            "replication_slot_support": None,
            "replication_slot": None,
            "synchronous_standby_names": None,
            "postgres_systemid": None,
        }

    @patch("barman.postgres.PostgreSQLConnection.connect")
    @patch(
        "barman.postgres.PostgreSQLConnection.is_in_recovery", new_callable=PropertyMock
    )
    @patch(
        "barman.postgres.PostgreSQLConnection.is_superuser", new_callable=PropertyMock
    )
    def test_checkpoint(self, is_superuser_mock, is_in_recovery_mock, conn_mock):
        """
        Simple test for the execution of a checkpoint on a given server
        """
        # Build a server
        server = build_real_server()
        cursor_mock = conn_mock.return_value.cursor.return_value
        is_in_recovery_mock.return_value = False
        is_superuser_mock.return_value = True
        # Execute the checkpoint method
        server.postgres.checkpoint()
        # Check for the right invocation
        cursor_mock.execute.assert_called_with("CHECKPOINT")

        cursor_mock.reset_mock()
        # Missing required permissions
        is_in_recovery_mock.return_value = False
        is_superuser_mock.return_value = False
        with pytest.raises(PostgresSuperuserRequired):
            server.postgres.checkpoint()
        assert not cursor_mock.execute.called

    @patch("barman.postgres.PostgreSQLConnection.connect")
    @patch(
        "barman.postgres.PostgreSQLConnection.is_in_recovery", new_callable=PropertyMock
    )
    @patch(
        "barman.postgres.PostgreSQLConnection.has_backup_privileges",
        new_callable=PropertyMock,
    )
    def test_switch_wal(
        self, has_backup_privileges_mock, is_in_recovery_mock, conn_mock
    ):
        """
        Simple test for the execution of a switch of a xlog on a given server
        """
        # Build a server
        server = build_real_server()
        cursor_mock = conn_mock.return_value.cursor.return_value
        is_in_recovery_mock.return_value = False
        has_backup_privileges_mock.return_value = True

        # Test for the response of a correct switch for PostgreSQL < 10
        conn_mock.return_value.server_version = 90100
        cursor_mock.fetchone.side_effect = [
            ("000000010000000000000001",),
            ("000000010000000000000002",),
        ]
        xlog = server.postgres.switch_wal()

        # Check for the right invocation for PostgreSQL < 10
        assert xlog == "000000010000000000000001"
        cursor_mock.execute.assert_has_calls(
            [
                call("SELECT pg_xlogfile_name(pg_current_xlog_insert_location())"),
                call("SELECT pg_xlogfile_name(pg_switch_xlog())"),
                call("SELECT pg_xlogfile_name(pg_current_xlog_insert_location())"),
            ]
        )

        # Test for the response of a correct switch for PostgreSQL 10
        conn_mock.return_value.server_version = 100000
        cursor_mock.reset_mock()
        cursor_mock.fetchone.side_effect = [
            ("000000010000000000000001",),
            ("000000010000000000000002",),
        ]
        xlog = server.postgres.switch_wal()

        # Check for the right invocation for PostgreSQL 10
        assert xlog == "000000010000000000000001"
        cursor_mock.execute.assert_has_calls(
            [
                call("SELECT pg_walfile_name(pg_current_wal_insert_lsn())"),
                call("SELECT pg_walfile_name(pg_switch_wal())"),
                call("SELECT pg_walfile_name(pg_current_wal_insert_lsn())"),
            ]
        )

        cursor_mock.reset_mock()
        # The switch has not been executed
        cursor_mock.fetchone.side_effect = [
            ("000000010000000000000001",),
            ("000000010000000000000001",),
        ]
        xlog = server.postgres.switch_wal()
        # Check for the right invocation
        assert xlog == ""

        cursor_mock.reset_mock()
        # Missing required permissions
        is_in_recovery_mock.return_value = False
        has_backup_privileges_mock.return_value = False
        with pytest.raises(BackupFunctionsAccessRequired):
            server.postgres.switch_wal()
        # Check for the right invocation
        assert not cursor_mock.execute.called

        cursor_mock.reset_mock()
        # Server in recovery
        is_in_recovery_mock.return_value = True
        has_backup_privileges_mock.return_value = True
        with pytest.raises(PostgresIsInRecovery):
            server.postgres.switch_wal()
        # Check for the right invocation
        assert not cursor_mock.execute.called

    @patch("barman.postgres.PostgreSQLConnection.connect")
    @patch(
        "barman.postgres.PostgreSQLConnection.server_version", new_callable=PropertyMock
    )
    @patch(
        "barman.postgres.PostgreSQLConnection.has_backup_privileges",
        new_callable=PropertyMock,
    )
    def test_get_replication_stats(
        self, has_backup_privileges_mock, server_version_mock, conn_mock
    ):
        """
        Simple test for the execution of get_replication_stats on a server
        """
        # Build a server
        server = build_real_server()
        cursor_mock = conn_mock.return_value.cursor.return_value
        has_backup_privileges_mock.return_value = True

        # 10 ALL
        cursor_mock.reset_mock()
        server_version_mock.return_value = 100000
        standby_info = server.postgres.get_replication_stats(
            PostgreSQLConnection.ANY_STREAMING_CLIENT
        )
        assert standby_info is cursor_mock.fetchall.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT r.*, rs.slot_name, "
            "pg_is_in_recovery() AS is_in_recovery, "
            "CASE WHEN pg_is_in_recovery() "
            "  THEN pg_last_wal_receive_lsn() "
            "  ELSE pg_current_wal_lsn() "
            "END AS current_lsn "
            "FROM pg_stat_replication r "
            "LEFT JOIN pg_replication_slots rs ON (r.pid = rs.active_pid) "
            "WHERE (rs.slot_type IS NULL OR rs.slot_type = 'physical') "
            "ORDER BY sync_state DESC, sync_priority"
        )

        # 10 ALL WALSTREAMER
        cursor_mock.reset_mock()
        server_version_mock.return_value = 100000
        standby_info = server.postgres.get_replication_stats(
            PostgreSQLConnection.WALSTREAMER
        )
        assert standby_info is cursor_mock.fetchall.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT r.*, rs.slot_name, "
            "pg_is_in_recovery() AS is_in_recovery, "
            "CASE WHEN pg_is_in_recovery() "
            "  THEN pg_last_wal_receive_lsn() "
            "  ELSE pg_current_wal_lsn() "
            "END AS current_lsn "
            "FROM pg_stat_replication r "
            "LEFT JOIN pg_replication_slots rs ON (r.pid = rs.active_pid) "
            "WHERE (rs.slot_type IS NULL OR rs.slot_type = 'physical') "
            "AND replay_lsn IS NULL "
            "ORDER BY sync_state DESC, sync_priority"
        )

        # 10 ALL STANDBY
        cursor_mock.reset_mock()
        server_version_mock.return_value = 100000
        standby_info = server.postgres.get_replication_stats(
            PostgreSQLConnection.STANDBY
        )
        assert standby_info is cursor_mock.fetchall.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT r.*, rs.slot_name, "
            "pg_is_in_recovery() AS is_in_recovery, "
            "CASE WHEN pg_is_in_recovery() "
            "  THEN pg_last_wal_receive_lsn() "
            "  ELSE pg_current_wal_lsn() "
            "END AS current_lsn "
            "FROM pg_stat_replication r "
            "LEFT JOIN pg_replication_slots rs ON (r.pid = rs.active_pid) "
            "WHERE (rs.slot_type IS NULL OR rs.slot_type = 'physical') "
            "AND replay_lsn IS NOT NULL "
            "ORDER BY sync_state DESC, sync_priority"
        )

        # 9.5 ALL
        cursor_mock.reset_mock()
        server_version_mock.return_value = 90500
        standby_info = server.postgres.get_replication_stats(
            PostgreSQLConnection.ANY_STREAMING_CLIENT
        )
        assert standby_info is cursor_mock.fetchall.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT pid, usesysid, usename, application_name, client_addr, "
            "client_hostname, client_port, "
            "backend_start, backend_xmin, state, "
            "sent_location AS sent_lsn, "
            "write_location AS write_lsn, "
            "flush_location AS flush_lsn, "
            "replay_location AS replay_lsn, "
            "sync_priority, sync_state, rs.slot_name, "
            "pg_is_in_recovery() AS is_in_recovery, "
            "CASE WHEN pg_is_in_recovery() "
            "  THEN pg_last_xlog_receive_location() "
            "  ELSE pg_current_xlog_location() "
            "END AS current_lsn "
            "FROM pg_stat_replication r "
            "LEFT JOIN pg_replication_slots rs ON (r.pid = rs.active_pid) "
            "WHERE (rs.slot_type IS NULL OR rs.slot_type = 'physical') "
            "ORDER BY sync_state DESC, sync_priority"
        )

        # 9.4 ALL
        cursor_mock.reset_mock()
        server_version_mock.return_value = 90400
        standby_info = server.postgres.get_replication_stats(
            PostgreSQLConnection.ANY_STREAMING_CLIENT
        )
        assert standby_info is cursor_mock.fetchall.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT pid, usesysid, usename, application_name, client_addr, "
            "client_hostname, client_port, "
            "backend_start, backend_xmin, state, "
            "sent_location AS sent_lsn, "
            "write_location AS write_lsn, "
            "flush_location AS flush_lsn, "
            "replay_location AS replay_lsn, "
            "sync_priority, sync_state, "
            "pg_is_in_recovery() AS is_in_recovery, "
            "CASE WHEN pg_is_in_recovery() "
            "  THEN pg_last_xlog_receive_location() "
            "  ELSE pg_current_xlog_location() "
            "END AS current_lsn "
            "FROM pg_stat_replication r "
            "ORDER BY sync_state DESC, sync_priority"
        )

        # 9.4 WALSTREAMER
        cursor_mock.reset_mock()
        server_version_mock.return_value = 90400
        standby_info = server.postgres.get_replication_stats(
            PostgreSQLConnection.WALSTREAMER
        )
        assert standby_info is cursor_mock.fetchall.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT pid, usesysid, usename, application_name, client_addr, "
            "client_hostname, client_port, "
            "backend_start, backend_xmin, state, "
            "sent_location AS sent_lsn, "
            "write_location AS write_lsn, "
            "flush_location AS flush_lsn, "
            "replay_location AS replay_lsn, "
            "sync_priority, sync_state, "
            "pg_is_in_recovery() AS is_in_recovery, "
            "CASE WHEN pg_is_in_recovery() "
            "  THEN pg_last_xlog_receive_location() "
            "  ELSE pg_current_xlog_location() "
            "END AS current_lsn "
            "FROM pg_stat_replication r "
            "WHERE replay_location IS NULL "
            "ORDER BY sync_state DESC, sync_priority"
        )

        # 9.4 STANDBY
        cursor_mock.reset_mock()
        server_version_mock.return_value = 90400
        standby_info = server.postgres.get_replication_stats(
            PostgreSQLConnection.STANDBY
        )
        assert standby_info is cursor_mock.fetchall.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT pid, usesysid, usename, application_name, client_addr, "
            "client_hostname, client_port, "
            "backend_start, backend_xmin, state, "
            "sent_location AS sent_lsn, "
            "write_location AS write_lsn, "
            "flush_location AS flush_lsn, "
            "replay_location AS replay_lsn, "
            "sync_priority, sync_state, "
            "pg_is_in_recovery() AS is_in_recovery, "
            "CASE WHEN pg_is_in_recovery() "
            "  THEN pg_last_xlog_receive_location() "
            "  ELSE pg_current_xlog_location() "
            "END AS current_lsn "
            "FROM pg_stat_replication r "
            "WHERE replay_location IS NOT NULL "
            "ORDER BY sync_state DESC, sync_priority"
        )

        # 9.2 ALL
        cursor_mock.reset_mock()
        server_version_mock.return_value = 90200
        standby_info = server.postgres.get_replication_stats(
            PostgreSQLConnection.ANY_STREAMING_CLIENT
        )
        assert standby_info is cursor_mock.fetchall.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT pid, usesysid, usename, application_name, client_addr, "
            "client_hostname, client_port, backend_start, "
            "CAST (NULL AS xid) AS backend_xmin, "
            "state, "
            "sent_location AS sent_lsn, "
            "write_location AS write_lsn, "
            "flush_location AS flush_lsn, "
            "replay_location AS replay_lsn, "
            "sync_priority, sync_state, "
            "pg_is_in_recovery() AS is_in_recovery, "
            "CASE WHEN pg_is_in_recovery() "
            "  THEN pg_last_xlog_receive_location() "
            "  ELSE pg_current_xlog_location() "
            "END AS current_lsn "
            "FROM pg_stat_replication r "
            "ORDER BY sync_state DESC, sync_priority"
        )

        # 9.2 WALSTREAMER
        cursor_mock.reset_mock()
        server_version_mock.return_value = 90200
        standby_info = server.postgres.get_replication_stats(
            PostgreSQLConnection.WALSTREAMER
        )
        assert standby_info is cursor_mock.fetchall.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT pid, usesysid, usename, application_name, client_addr, "
            "client_hostname, client_port, backend_start, "
            "CAST (NULL AS xid) AS backend_xmin, "
            "state, "
            "sent_location AS sent_lsn, "
            "write_location AS write_lsn, "
            "flush_location AS flush_lsn, "
            "replay_location AS replay_lsn, "
            "sync_priority, sync_state, "
            "pg_is_in_recovery() AS is_in_recovery, "
            "CASE WHEN pg_is_in_recovery() "
            "  THEN pg_last_xlog_receive_location() "
            "  ELSE pg_current_xlog_location() "
            "END AS current_lsn "
            "FROM pg_stat_replication r "
            "WHERE replay_location IS NULL "
            "ORDER BY sync_state DESC, sync_priority"
        )

        # 9.2 STANDBY
        cursor_mock.reset_mock()
        server_version_mock.return_value = 90200
        standby_info = server.postgres.get_replication_stats(
            PostgreSQLConnection.STANDBY
        )
        assert standby_info is cursor_mock.fetchall.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT pid, usesysid, usename, application_name, client_addr, "
            "client_hostname, client_port, backend_start, "
            "CAST (NULL AS xid) AS backend_xmin, "
            "state, "
            "sent_location AS sent_lsn, "
            "write_location AS write_lsn, "
            "flush_location AS flush_lsn, "
            "replay_location AS replay_lsn, "
            "sync_priority, sync_state, "
            "pg_is_in_recovery() AS is_in_recovery, "
            "CASE WHEN pg_is_in_recovery() "
            "  THEN pg_last_xlog_receive_location() "
            "  ELSE pg_current_xlog_location() "
            "END AS current_lsn "
            "FROM pg_stat_replication r "
            "WHERE replay_location IS NOT NULL "
            "ORDER BY sync_state DESC, sync_priority"
        )

        # 9.1 ALL
        cursor_mock.reset_mock()
        server_version_mock.return_value = 90100
        standby_info = server.postgres.get_replication_stats(
            PostgreSQLConnection.ANY_STREAMING_CLIENT
        )
        assert standby_info is cursor_mock.fetchall.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT procpid AS pid, usesysid, usename, application_name, "
            "client_addr, client_hostname, client_port, backend_start, "
            "CAST (NULL AS xid) AS backend_xmin, "
            "state, "
            "sent_location AS sent_lsn, "
            "write_location AS write_lsn, "
            "flush_location AS flush_lsn, "
            "replay_location AS replay_lsn, "
            "sync_priority, sync_state, "
            "pg_is_in_recovery() AS is_in_recovery, "
            "CASE WHEN pg_is_in_recovery() "
            "  THEN pg_last_xlog_receive_location() "
            "  ELSE pg_current_xlog_location() "
            "END AS current_lsn "
            "FROM pg_stat_replication r "
            "ORDER BY sync_state DESC, sync_priority"
        )

        # 9.1 WALSTREAMER
        cursor_mock.reset_mock()
        server_version_mock.return_value = 90100
        standby_info = server.postgres.get_replication_stats(
            PostgreSQLConnection.WALSTREAMER
        )
        assert standby_info is cursor_mock.fetchall.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT procpid AS pid, usesysid, usename, application_name, "
            "client_addr, client_hostname, client_port, backend_start, "
            "CAST (NULL AS xid) AS backend_xmin, "
            "state, "
            "sent_location AS sent_lsn, "
            "write_location AS write_lsn, "
            "flush_location AS flush_lsn, "
            "replay_location AS replay_lsn, "
            "sync_priority, sync_state, "
            "pg_is_in_recovery() AS is_in_recovery, "
            "CASE WHEN pg_is_in_recovery() "
            "  THEN pg_last_xlog_receive_location() "
            "  ELSE pg_current_xlog_location() "
            "END AS current_lsn "
            "FROM pg_stat_replication r "
            "WHERE replay_location IS NULL "
            "ORDER BY sync_state DESC, sync_priority"
        )

        # 9.1 STANDBY
        cursor_mock.reset_mock()
        server_version_mock.return_value = 90100
        standby_info = server.postgres.get_replication_stats(
            PostgreSQLConnection.STANDBY
        )
        assert standby_info is cursor_mock.fetchall.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT procpid AS pid, usesysid, usename, application_name, "
            "client_addr, client_hostname, client_port, backend_start, "
            "CAST (NULL AS xid) AS backend_xmin, "
            "state, "
            "sent_location AS sent_lsn, "
            "write_location AS write_lsn, "
            "flush_location AS flush_lsn, "
            "replay_location AS replay_lsn, "
            "sync_priority, sync_state, "
            "pg_is_in_recovery() AS is_in_recovery, "
            "CASE WHEN pg_is_in_recovery() "
            "  THEN pg_last_xlog_receive_location() "
            "  ELSE pg_current_xlog_location() "
            "END AS current_lsn "
            "FROM pg_stat_replication r "
            "WHERE replay_location IS NOT NULL "
            "ORDER BY sync_state DESC, sync_priority"
        )

        cursor_mock.reset_mock()
        # Missing required permissions
        has_backup_privileges_mock.return_value = False
        with pytest.raises(BackupFunctionsAccessRequired):
            server.postgres.get_replication_stats(
                PostgreSQLConnection.ANY_STREAMING_CLIENT
            )
        # Check for the right invocation
        assert not cursor_mock.execute.called

        cursor_mock.reset_mock()
        # Too old version (9.0)
        has_backup_privileges_mock.return_value = True
        server_version_mock.return_value = 90000
        with pytest.raises(PostgresUnsupportedFeature):
            server.postgres.get_replication_stats(
                PostgreSQLConnection.ANY_STREAMING_CLIENT
            )
        # Check for the right invocation
        assert not cursor_mock.execute.called

    @patch("barman.postgres.PostgreSQLConnection.connect")
    @patch(
        "barman.postgres.PostgreSQLConnection.server_version", new_callable=PropertyMock
    )
    @patch(
        "barman.postgres.PostgreSQLConnection.is_superuser", new_callable=PropertyMock
    )
    def test_get_replication_slot(
        self, is_superuser_mock, server_version_mock, conn_mock
    ):
        """
        Simple test for the execution of get_replication_slots on a server
        """
        # Build a server
        server = build_real_server()
        server.config.slot_name = "test"
        cursor_mock = conn_mock.return_value.cursor.return_value
        is_superuser_mock.return_value = True

        # Supported version 9.4
        cursor_mock.reset_mock()
        server_version_mock.return_value = 90400
        replication_slot = server.postgres.get_replication_slot(server.config.slot_name)
        assert replication_slot is cursor_mock.fetchone.return_value
        cursor_mock.execute.assert_called_once_with(
            "SELECT slot_name, "
            "active, "
            "restart_lsn "
            "FROM pg_replication_slots "
            "WHERE slot_type = 'physical' "
            "AND slot_name = '%s'" % server.config.slot_name
        )

        # Too old version (3.0)
        server_version_mock.return_value = 90300
        with pytest.raises(PostgresUnsupportedFeature):
            server.postgres.get_replication_slot(server.config.slot_name)

    @patch("barman.postgres.PostgreSQLConnection.get_setting")
    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_get_synchronous_standby_names(self, conn_mock, setting_mock):
        """
        Simple test for retrieving settings from the database
        """
        # Build and configure a server
        server = build_real_server()

        # Unsupported version: 9.0
        conn_mock.return_value.server_version = 90000

        with pytest.raises(PostgresUnsupportedFeature):
            server.postgres.get_synchronous_standby_names()

        # Supported version: 9.1
        conn_mock.return_value.server_version = 90100

        setting_mock.return_value = "a, bc, def"
        names = server.postgres.get_synchronous_standby_names()
        setting_mock.assert_called_once_with("synchronous_standby_names")
        assert names == ["a", "bc", "def"]

        setting_mock.reset_mock()
        setting_mock.return_value = "a,bc,def"
        names = server.postgres.get_synchronous_standby_names()
        setting_mock.assert_called_once_with("synchronous_standby_names")
        assert names == ["a", "bc", "def"]

        setting_mock.reset_mock()
        setting_mock.return_value = " a, bc, def "
        names = server.postgres.get_synchronous_standby_names()
        setting_mock.assert_called_once_with("synchronous_standby_names")
        assert names == ["a", "bc", "def"]

        setting_mock.reset_mock()
        setting_mock.return_value = "2(a, bc, def)"
        names = server.postgres.get_synchronous_standby_names()
        setting_mock.assert_called_once_with("synchronous_standby_names")
        assert names == ["a", "bc", "def"]

        setting_mock.reset_mock()
        setting_mock.return_value = " 1 ( a, bc, def ) "
        names = server.postgres.get_synchronous_standby_names()
        setting_mock.assert_called_once_with("synchronous_standby_names")
        assert names == ["a", "bc", "def"]

        setting_mock.reset_mock()
        setting_mock.return_value = " a "
        names = server.postgres.get_synchronous_standby_names()
        setting_mock.assert_called_once_with("synchronous_standby_names")
        assert names == ["a"]

        setting_mock.reset_mock()
        setting_mock.return_value = "1(a)"
        names = server.postgres.get_synchronous_standby_names()
        setting_mock.assert_called_once_with("synchronous_standby_names")
        assert names == ["a"]

        setting_mock.reset_mock()
        setting_mock.return_value = '1(a, "b-c")'
        names = server.postgres.get_synchronous_standby_names()
        setting_mock.assert_called_once_with("synchronous_standby_names")
        assert names == ["a", "b-c"]

        setting_mock.reset_mock()
        setting_mock.return_value = "*"
        names = server.postgres.get_synchronous_standby_names()
        setting_mock.assert_called_once_with("synchronous_standby_names")
        assert names == ["*"]

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_xlog_segment_size(self, conn_mock):
        """
        Test the xlog_segment_size method
        """

        default_wal_file_size = 16777216

        # Build a server
        server = build_real_server()
        conn_mock.return_value.server_version = 110000
        cursor_mock = conn_mock.return_value.cursor.return_value
        cursor_mock.fetchone.side_effect = [[str(default_wal_file_size)]]

        result = server.postgres.xlog_segment_size
        assert result == default_wal_file_size

        execute_calls = [
            call("SELECT setting FROM pg_settings WHERE name='wal_segment_size'"),
        ]
        cursor_mock.execute.assert_has_calls(execute_calls)

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_xlog_segment_size_10(self, conn_mock):
        """
        Test the xlog_segment_size method
        """

        default_wal_file_size = 16777216
        default_wal_block_size = 8192
        default_wal_segments_number = 2048

        # Build a server
        server = build_real_server()
        conn_mock.return_value.server_version = 100000
        cursor_mock = conn_mock.return_value.cursor.return_value
        cursor_mock.fetchone.side_effect = [
            [str(default_wal_segments_number)],
            [str(default_wal_block_size)],
        ]

        result = server.postgres.xlog_segment_size
        assert result == default_wal_file_size

        execute_calls = [
            call("SELECT setting FROM pg_settings WHERE name='wal_segment_size'"),
            call("SELECT setting FROM pg_settings WHERE name='wal_block_size'"),
        ]
        cursor_mock.execute.assert_has_calls(execute_calls)

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_xlog_segment_size_83(self, conn_mock):
        """
        If you use PostgreSQL 8.3 you can't change the WAL segment size even
        at compilation level. Barman shouldn't ask the server for this data,
        as this will result in an error
        """

        # Build a server
        server = build_real_server()
        conn_mock.return_value.server_version = 80300
        cursor_mock = conn_mock.return_value.cursor.return_value

        result = server.postgres.xlog_segment_size
        assert result == DEFAULT_XLOG_SEG_SIZE

        cursor_mock.execute.assert_not_called()

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_name_map(self, conn_mock):
        """
        Test the `name_map` behaviour
        :return:
        """
        server = build_real_server()

        conn_mock.return_value.server_version = 150000
        map_15 = server.postgres.name_map
        assert map_15

        conn_mock.return_value.server_version = 100000
        map_10 = server.postgres.name_map
        assert map_10

        conn_mock.return_value.server_version = 90300
        map_93 = server.postgres.name_map
        assert map_93

        conn_mock.side_effect = PostgresConnectionError
        map_error = server.postgres.name_map
        assert map_15 == map_error

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_switch_wal_function(self, conn_mock):
        """
        Test the `switch_wal_function` name
        :return:
        """
        server = build_real_server()

        conn_mock.return_value.server_version = 90300
        assert server.postgres.name_map["pg_switch_wal"] == "pg_switch_xlog"

        conn_mock.return_value.server_version = 100000
        assert server.postgres.name_map["pg_switch_wal"] == "pg_switch_wal"

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_xlogfile_name_function(self, conn_mock):
        """
        Test the `xlogfile_name_function` property.
        :return:
        """
        server = build_real_server()

        conn_mock.return_value.server_version = 90300
        assert server.postgres.name_map["pg_walfile_name"] == "pg_xlogfile_name"

        conn_mock.return_value.server_version = 100000
        assert server.postgres.name_map["pg_walfile_name"] == "pg_walfile_name"

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_xlogfile_name_offset_function(self, conn_mock):
        """
        Test the `xlogfile_name_function` property.
        :return:
        """
        server = build_real_server()

        conn_mock.return_value.server_version = 90300
        assert (
            server.postgres.name_map["pg_walfile_name_offset"]
            == "pg_xlogfile_name_offset"
        )

        conn_mock.return_value.server_version = 100000
        assert (
            server.postgres.name_map["pg_walfile_name_offset"]
            == "pg_walfile_name_offset"
        )

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_xlog_directory(self, conn_mock):
        """
        Test the `xlog_directory` property.
        :return:
        """
        server = build_real_server()

        conn_mock.return_value.server_version = 90300
        assert server.postgres.name_map["pg_wal"] == "pg_xlog"

        conn_mock.return_value.server_version = 100000
        assert server.postgres.name_map["pg_wal"] == "pg_wal"

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_last_xlog_replay_location_function(self, conn_mock):
        """
        Test the `last_xlog_replay_location_function` property.
        :return:
        """
        server = build_real_server()

        conn_mock.return_value.server_version = 90300
        assert (
            server.postgres.name_map["pg_last_wal_replay_lsn"]
            == "pg_last_xlog_replay_location"
        )

        conn_mock.return_value.server_version = 100000
        assert (
            server.postgres.name_map["pg_last_wal_replay_lsn"]
            == "pg_last_wal_replay_lsn"
        )

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_current_xlog_location_function(self, conn_mock):
        """
        Test the `current_xlog_location_function` property
        :return:
        """
        server = build_real_server()

        conn_mock.return_value.server_version = 90300
        assert (
            server.postgres.name_map["pg_current_wal_lsn"] == "pg_current_xlog_location"
        )

        conn_mock.return_value.server_version = 100000
        assert server.postgres.name_map["pg_current_wal_lsn"] == "pg_current_wal_lsn"

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_current_xlog_insert_location_function(self, conn_mock):
        """
        Test the `current_xlog_insert_location_function` property
        :return:
        """
        server = build_real_server()

        conn_mock.return_value.server_version = 90300
        assert (
            server.postgres.name_map["pg_current_wal_insert_lsn"]
            == "pg_current_xlog_insert_location"
        )

        conn_mock.return_value.server_version = 100000
        assert (
            server.postgres.name_map["pg_current_wal_insert_lsn"]
            == "pg_current_wal_insert_lsn"
        )

    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_last_xlog_receive_location_function(self, conn_mock):
        """
        Test the `current_xlog_insert_location_function` property
        :return:
        """
        server = build_real_server()

        conn_mock.return_value.server_version = 90300
        assert (
            server.postgres.name_map["pg_last_wal_receive_lsn"]
            == "pg_last_xlog_receive_location"
        )

        conn_mock.return_value.server_version = 100000
        assert (
            server.postgres.name_map["pg_last_wal_receive_lsn"]
            == "pg_last_wal_receive_lsn"
        )


# noinspection PyMethodMayBeStatic
class TestStreamingConnection(object):
    def test_connection_error(self):
        """
        simple test for streaming_archiver without streaming_conninfo
        """
        # Test with wrong configuration
        server = build_real_server(
            main_conf={"streaming_archiver": True, "streaming_conninfo": ""}
        )
        assert server.config.msg_list
        assert (
            "Streaming connection: Missing 'streaming_conninfo' "
            "parameter for server 'main'" in server.config.msg_list
        )
        server = build_real_server(
            main_conf={
                "streaming_archiver": True,
                "streaming_conninfo": "host=/test "
                "port=5496 "
                "user=test "
                "dbname=test_db",
            }
        )
        assert server.streaming.conn_parameters["dbname"] == "replication"
        assert (
            server.streaming.conninfo == "dbname=replication "
            "host=/test "
            "options=-cdatestyle=iso "
            "port=5496 "
            "replication=true "
            "user=test"
        )

    @patch("barman.postgres.psycopg2.connect")
    def test_fetch_remote_status(self, conn_mock):
        """
        simple test for the fetch_remote_status method
        """
        # Build a server
        server = build_real_server(
            main_conf={"streaming_archiver": True, "streaming_conninfo": "dummy=param"}
        )

        # Too old PostgreSQL
        conn_mock.return_value.server_version = 90100
        result = server.streaming.fetch_remote_status()
        assert result["streaming_supported"] is False
        assert result["streaming"] is None

        # Working streaming connection
        conn_mock.return_value.server_version = 90300
        cursor_mock = conn_mock.return_value.cursor.return_value
        cursor_mock.fetchone.return_value = ("12345", 1, "DE/ADBEEF")
        result = server.streaming.fetch_remote_status()
        cursor_mock.execute.assert_called_with("IDENTIFY_SYSTEM")
        assert result["streaming_supported"] is True
        assert result["streaming"] is True

        # Working non-streaming connection
        conn_mock.reset_mock()
        cursor_mock.execute.side_effect = psycopg2.ProgrammingError
        result = server.streaming.fetch_remote_status()
        cursor_mock.execute.assert_called_with("IDENTIFY_SYSTEM")
        assert result["streaming_supported"] is True
        assert result["streaming"] is False

        # Connection failed
        server.streaming.close()
        conn_mock.reset_mock()
        conn_mock.side_effect = psycopg2.DatabaseError
        result = server.streaming.fetch_remote_status()
        assert result["streaming_supported"] is None
        assert result["streaming"] is None

    @patch("barman.postgres.PostgreSQL.connect")
    def test_streaming_server_txt_version(self, conn_mock):
        """
        simple test for the server_txt_version property
        """
        # Build a server
        server = build_real_server(
            main_conf={"streaming_archiver": True, "streaming_conninfo": "dummy=param"}
        )

        # Connection error
        conn_mock.side_effect = PostgresConnectionError
        assert server.streaming.server_txt_version is None

        # Good connection
        conn_mock.side_effect = None

        conn_mock.return_value.server_version = 80300
        assert server.streaming.server_txt_version == "8.3.0"

        conn_mock.return_value.server_version = 90000
        assert server.streaming.server_txt_version == "9.0.0"

        conn_mock.return_value.server_version = 90005
        assert server.streaming.server_txt_version == "9.0.5"

        conn_mock.return_value.server_version = 100001
        assert server.streaming.server_txt_version == "10.1"

        conn_mock.return_value.server_version = 110011
        assert server.streaming.server_txt_version == "11.11"

        conn_mock.return_value.server_version = 0
        assert server.streaming.server_txt_version == "0.0.0"

    @patch("barman.postgres.psycopg2.connect")
    def test_streaming_create_repslot(self, connect_mock):
        # Build a server
        server = build_real_server(
            main_conf={"streaming_archiver": True, "streaming_conninfo": "dummy=param"}
        )

        # Test replication slot creation
        cursor_mock = connect_mock.return_value.cursor.return_value
        server.streaming.create_physical_repslot("test_repslot")
        cursor_mock.execute.assert_called_once_with(
            "CREATE_REPLICATION_SLOT test_repslot PHYSICAL"
        )

        # Test replication slot already existent
        cursor_mock = connect_mock.return_value.cursor.return_value
        cursor_mock.execute.side_effect = MockProgrammingError(DUPLICATE_OBJECT)

        with pytest.raises(PostgresDuplicateReplicationSlot):
            server.streaming.create_physical_repslot("test_repslot")
            cursor_mock.execute.assert_called_once_with(
                "CREATE_REPLICATION_SLOT test_repslot PHYSICAL"
            )

    @patch("barman.postgres.psycopg2.connect")
    def test_streaming_drop_repslot(self, connect_mock):
        # Build a server
        server = build_real_server(
            main_conf={"streaming_archiver": True, "streaming_conninfo": "dummy=param"}
        )

        # Test replication slot creation
        cursor_mock = connect_mock.return_value.cursor.return_value
        server.streaming.drop_repslot("test_repslot")
        cursor_mock.execute.assert_called_once_with(
            "DROP_REPLICATION_SLOT test_repslot"
        )

        # Test replication slot already existent
        cursor_mock = connect_mock.return_value.cursor.return_value
        cursor_mock.execute.side_effect = MockProgrammingError(UNDEFINED_OBJECT)

        with pytest.raises(PostgresInvalidReplicationSlot):
            server.streaming.drop_repslot("test_repslot")
            cursor_mock.execute.assert_called_once_with(
                "DROP_REPLICATION_SLOT test_repslot"
            )

        server.streaming.close()
