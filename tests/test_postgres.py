# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2023
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
from multiprocessing import Queue

try:
    from queue import Queue as SyncQueue
except ImportError:
    from Queue import Queue as SyncQueue

import psycopg2
import pytest
from mock import Mock, PropertyMock, call, patch
from psycopg2.errorcodes import DUPLICATE_OBJECT, UNDEFINED_OBJECT

from barman.exceptions import (
    PostgresConnectionError,
    PostgresDuplicateReplicationSlot,
    PostgresException,
    PostgresInvalidReplicationSlot,
    PostgresIsInRecovery,
    BackupFunctionsAccessRequired,
    PostgresObsoleteFeature,
    PostgresCheckpointPrivilegesRequired,
    PostgresUnsupportedFeature,
)
from barman.postgres import (
    PostgreSQLConnection,
    StandbyPostgreSQLConnection,
    PostgreSQL,
)
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

    @pytest.mark.parametrize(
        ("int_version", "expected_str_version"),
        [(90600, "9.6.0"), (102200, "10.0"), (140000, "14.0"), (150000, "15.0")],
    )
    def test_int_version_to_string_version(self, int_version, expected_str_version):
        class VoidPostgreSQL(PostgreSQL):
            def __init__(self):
                pass

            def fetch_remote_status(self):
                pass

        pg = VoidPostgreSQL()
        str_version = pg.int_version_to_string_version(int_version)

        assert str_version == expected_str_version

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

        # Simulate a master connection
        is_in_recovery_mock.return_value = True

        # Test : Postgres 9.6 in recovery (standby) expect None as result
        conn_mock.return_value.server_version = 90600

        restore_point = server.postgres.create_restore_point("Test_20151026T092241")
        assert restore_point is None

        # Test : Postgres 9.6 check mock calls
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
        [
            (130000, "pg_stop_backup(FALSE)"),
            (140000, "pg_stop_backup(FALSE)"),
            (150000, "pg_backup_stop()"),
        ],
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

        # for PostgreSQL 14 and above idle_session_timeout will
        # be disabled in the method, resulting in 2 execute calls
        if server_version >= 140000:
            assert cursor_mock.execute.call_count == 2
            cursor_mock.execute.assert_has_calls(
                [
                    call("RESET idle_session_timeout"),
                    call(
                        "SELECT end_row.lsn AS location, "
                        "(SELECT CASE WHEN pg_is_in_recovery() "
                        "THEN min_recovery_end_timeline "
                        "ELSE timeline_id END "
                        "FROM pg_control_checkpoint(), pg_control_recovery()"
                        ") AS timeline, "
                        "end_row.labelfile AS backup_label, "
                        "now() AS timestamp "
                        "FROM %s AS end_row" % expected_stop_call
                    ),
                ]
            )
        else:
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
    def test_start_exclusive_backup(self, conn):
        """
        Simple test for start_exclusive_backup method of
        the RsyncBackupExecutor class
        """
        # Build a server
        server = build_real_server()
        backup_label = "test label"

        # Expect no errors
        conn.return_value.server_version = 90600
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
            (130000, "pg_start_backup", "%s, %s, FALSE"),
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

        # for PostgreSQL 14 and above idle_session_timeout will
        # be enabled in the method, resulting in 2 execute calls
        if server_version >= 140000:
            assert cursor_mock.execute.call_count == 2
            cursor_mock.execute.assert_has_calls(
                [
                    call("SET idle_session_timeout TO 0"),
                    call(
                        "SELECT location, "
                        "(SELECT timeline_id "
                        "FROM pg_control_checkpoint()) AS timeline, "
                        "now() AS timestamp "
                        "FROM %s(%s) AS location"
                        % (expected_start_fun, expected_start_args),
                        ("test label", False),
                    ),
                ]
            )
        else:
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

    @pytest.mark.parametrize(
        ("version", "expected"), [(90600, True), (100000, True), (90500, False)]
    )
    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_is_minimal_postgres_version(self, conn, version, expected):
        server = build_real_server()

        # conn.return_value.server_version = 90600
        conn.return_value.server_version = version
        assert server.postgres.is_minimal_postgres_version() == expected

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
        conn.return_value.server_version = 90600
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

        # expect no errors with version >= 9.4
        conn.reset_mock()
        conn.return_value.server_version = 90600
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
    def test_is_in_recovery(self, conn_mock):
        """
        simple test for is_in_recovery property
        """
        # Build a server
        server = build_real_server()
        cursor_mock = conn_mock.return_value.cursor.return_value

        # In recovery
        conn_mock.return_value.server_version = 90600
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
            "max_wal_senders": "a max_wal_senders value",
            "data_checksums": "a data_checksums",
            "max_replication_slots": "a max_replication_slots value",
            "wal_compression": "a wal_compression value",
            "wal_keep_segments": "a wal_keep_segments value",
            "wal_keep_size": "a wal_keep_size value",
        }

        get_setting_mock.side_effect = lambda x: settings.get(x, "unknown")

        # Test PostgreSQL < Minimal
        conn_mock.return_value.server_version = 90500
        server_txt_version_mock.return_value = "9.5.0"
        result = server.postgres.fetch_remote_status()
        assert result == {
            "a": "b",
            "is_superuser": True,
            "has_backup_privileges": True,
            "is_in_recovery": False,
            "current_lsn": "DE/ADBEEF",
            "current_xlog": "00000001000000DE00000000",
            "data_directory": "a directory",
            "server_txt_version": "9.5.0",
            "wal_level": "a wal_level value",
            "current_size": 497354072,
            "replication_slot_support": True,
            "replication_slot": None,
            "synchronous_standby_names": [],
            "version_supported": False,
            "archive_timeout": 300,
            "checkpoint_timeout": 600,
            "wal_keep_segments": "a wal_keep_segments value",
            "hot_standby": "a hot_standby value",
            "max_wal_senders": "a max_wal_senders value",
            "data_checksums": "a data_checksums",
            "max_replication_slots": "a max_replication_slots value",
            "wal_compression": "a wal_compression value",
            "xlog_segment_size": 8388608,
            "postgres_systemid": 6721602258895701769,
            "has_monitoring_privileges": True,
        }

        # Test PostgreSQL 9.6
        conn_mock.return_value.server_version = 90600
        server_txt_version_mock.return_value = "9.6.0"
        result = server.postgres.fetch_remote_status()
        assert result == {
            "a": "b",
            "is_superuser": True,
            "has_backup_privileges": True,
            "is_in_recovery": False,
            "current_lsn": "DE/ADBEEF",
            "current_xlog": "00000001000000DE00000000",
            "data_directory": "a directory",
            "server_txt_version": "9.6.0",
            "wal_level": "a wal_level value",
            "current_size": 497354072,
            "replication_slot_support": True,
            "replication_slot": None,
            "synchronous_standby_names": [],
            "version_supported": True,
            "archive_timeout": 300,
            "checkpoint_timeout": 600,
            "wal_keep_segments": "a wal_keep_segments value",
            "hot_standby": "a hot_standby value",
            "max_wal_senders": "a max_wal_senders value",
            "data_checksums": "a data_checksums",
            "max_replication_slots": "a max_replication_slots value",
            "wal_compression": "a wal_compression value",
            "xlog_segment_size": 8388608,
            "postgres_systemid": 6721602258895701769,
            "has_monitoring_privileges": True,
        }

        # Test PostgreSQL 13
        conn_mock.return_value.server_version = 130000
        server_txt_version_mock.return_value = "13.0"
        result = server.postgres.fetch_remote_status()
        assert result == {
            "a": "b",
            "is_superuser": True,
            "has_backup_privileges": True,
            "is_in_recovery": False,
            "current_lsn": "DE/ADBEEF",
            "current_xlog": "00000001000000DE00000000",
            "data_directory": "a directory",
            "server_txt_version": "13.0",
            "wal_level": "a wal_level value",
            "current_size": 497354072,
            "replication_slot_support": True,
            "replication_slot": None,
            "synchronous_standby_names": [],
            "version_supported": True,
            "archive_timeout": 300,
            "checkpoint_timeout": 600,
            "wal_keep_size": "a wal_keep_size value",
            "hot_standby": "a hot_standby value",
            "max_wal_senders": "a max_wal_senders value",
            "data_checksums": "a data_checksums",
            "max_replication_slots": "a max_replication_slots value",
            "wal_compression": "a wal_compression value",
            "xlog_segment_size": 8388608,
            "postgres_systemid": 6721602258895701769,
            "has_monitoring_privileges": True,
        }

        # Test error management
        server.postgres.close()
        conn_mock.side_effect = psycopg2.DatabaseError
        assert server.postgres.fetch_remote_status() == {
            "is_superuser": None,
            "is_in_recovery": None,
            "current_xlog": None,
            "data_directory": None,
            "server_txt_version": None,
            "replication_slot_support": None,
            "replication_slot": None,
            "synchronous_standby_names": None,
            "version_supported": None,
            "postgres_systemid": None,
        }

        get_setting_mock.side_effect = psycopg2.ProgrammingError
        assert server.postgres.fetch_remote_status() == {
            "is_superuser": None,
            "is_in_recovery": None,
            "current_xlog": None,
            "data_directory": None,
            "server_txt_version": None,
            "replication_slot_support": None,
            "replication_slot": None,
            "synchronous_standby_names": None,
            "version_supported": None,
            "postgres_systemid": None,
        }

    @patch("barman.postgres.PostgreSQLConnection.connect")
    @patch(
        "barman.postgres.PostgreSQLConnection.is_superuser", new_callable=PropertyMock
    )
    @patch(
        "barman.postgres.PostgreSQLConnection.server_version", new_callable=PropertyMock
    )
    def test_has_checkpoint_privileges(
        self, server_version_mock, is_su_mock, conn_mock
    ):
        server = build_real_server()
        cursor_mock = conn_mock.return_value.cursor.return_value

        # test PostgreSQL 13 and below
        server_version_mock.return_value = 139999
        is_su_mock.return_value = False
        assert not server.postgres.has_checkpoint_privileges
        is_su_mock.return_value = True
        assert server.postgres.has_checkpoint_privileges

        # test PostgreSQL 14 and above
        server_version_mock.return_value = 140000

        # no superuser, no pg_checkpoint -> False
        is_su_mock.return_value = False
        cursor_mock.fetchone.side_effect = [(False,)]
        assert not server.postgres.has_checkpoint_privileges
        cursor_mock.execute.assert_called_with(
            "select pg_has_role(CURRENT_USER ,'pg_checkpoint', 'MEMBER');"
        )

        # no superuser, pg_checkpoint -> True
        cursor_mock.reset_mock()
        is_su_mock.return_value = False
        cursor_mock.fetchone.side_effect = [(True,)]
        assert server.postgres.has_checkpoint_privileges
        cursor_mock.execute.assert_called_with(
            "select pg_has_role(CURRENT_USER ,'pg_checkpoint', 'MEMBER');"
        )

        # superuser, no pg_checkpoint -> True
        cursor_mock.reset_mock()
        is_su_mock.return_value = True
        cursor_mock.fetchone.side_effect = [(False,)]
        assert server.postgres.has_checkpoint_privileges

        # superuser, pg_checkpoint -> True
        cursor_mock.reset_mock()
        is_su_mock.return_value = True
        cursor_mock.fetchone.side_effect = [(True,)]
        assert server.postgres.has_checkpoint_privileges

    @patch("barman.postgres.PostgreSQLConnection.connect")
    @patch(
        "barman.postgres.PostgreSQLConnection.is_in_recovery", new_callable=PropertyMock
    )
    @patch(
        "barman.postgres.PostgreSQLConnection.has_checkpoint_privileges",
        new_callable=PropertyMock,
    )
    def test_checkpoint(self, has_cp_priv_mock, is_in_recovery_mock, conn_mock):
        """
        Simple test for the execution of a checkpoint on a given server
        """
        # Build a server
        server = build_real_server()
        cursor_mock = conn_mock.return_value.cursor.return_value
        is_in_recovery_mock.return_value = False
        has_cp_priv_mock.return_value = True
        # Execute the checkpoint method
        server.postgres.checkpoint()
        # Check for the right invocation
        cursor_mock.execute.assert_called_with("CHECKPOINT")

        cursor_mock.reset_mock()
        # Missing required permissions
        is_in_recovery_mock.return_value = False
        has_cp_priv_mock.return_value = False
        with pytest.raises(PostgresCheckpointPrivilegesRequired):
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
        "barman.postgres.PostgreSQLConnection.has_monitoring_privileges",
        new_callable=PropertyMock,
    )
    def test_get_replication_stats(
        self, has_monitoring_privileges_mock, server_version_mock, conn_mock
    ):
        """
        Simple test for the execution of get_replication_stats on a server
        """
        # Build a server
        server = build_real_server()
        cursor_mock = conn_mock.return_value.cursor.return_value
        has_monitoring_privileges_mock.return_value = True

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

        # 9.6 ALL
        cursor_mock.reset_mock()
        server_version_mock.return_value = 90600
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

        cursor_mock.reset_mock()
        # Missing required permissions
        has_monitoring_privileges_mock.return_value = False
        with pytest.raises(BackupFunctionsAccessRequired):
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

        conn_mock.return_value.server_version = 90600
        map_96 = server.postgres.name_map
        assert map_96

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

        conn_mock.return_value.server_version = 90600
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

        conn_mock.return_value.server_version = 90600
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

        conn_mock.return_value.server_version = 90600
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

        conn_mock.return_value.server_version = 90600
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

        conn_mock.return_value.server_version = 90600
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

        conn_mock.return_value.server_version = 90600
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

        conn_mock.return_value.server_version = 90600
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

        conn_mock.return_value.server_version = 90600
        assert (
            server.postgres.name_map["pg_last_wal_receive_lsn"]
            == "pg_last_xlog_receive_location"
        )

        conn_mock.return_value.server_version = 100000
        assert (
            server.postgres.name_map["pg_last_wal_receive_lsn"]
            == "pg_last_wal_receive_lsn"
        )

    @pytest.mark.parametrize(
        ("is_superuser", "query_response", "expected_has_monitoring"),
        (
            # If we are a superuser then has_monitoring_privileges should always
            # return True
            (True, [False], True),
            (True, [True], True),
            # If the query returns False then has_monitoring_privileges should return
            # False
            (False, [False], False),
            # If the query returns True then has_monitoring_privileges should return
            # True
            (False, [True], True),
        ),
    )
    @patch(
        "barman.postgres.PostgreSQLConnection.is_superuser", new_callable=PropertyMock
    )
    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_has_monitoring_privileges(
        self,
        conn_mock,
        mock_is_superuser,
        is_superuser,
        query_response,
        expected_has_monitoring,
    ):
        """
        Verify that has_monitoring_privileges executes the expected query and returns
        the correct result.
        """
        # GIVEN a server managed by Barman
        server = build_real_server()
        cursor_mock = conn_mock.return_value.cursor.return_value
        # AND is_superuser is set to the specified value
        mock_is_superuser.return_value = is_superuser
        # AND the permissions check returns the specified result
        cursor_mock.fetchone.side_effect = [query_response]

        # WHEN has_monitoring_privileges is called
        has_monitoring = server.postgres.has_monitoring_privileges

        # THEN the correct query was executed if we weren't a superuser
        if not is_superuser:
            cursor_mock.execute.assert_called_once_with(
                """
            SELECT
            (
                pg_has_role(CURRENT_USER, 'pg_monitor', 'MEMBER')
                OR
                (
                    pg_has_role(CURRENT_USER, 'pg_read_all_settings', 'MEMBER')
                    AND pg_has_role(CURRENT_USER, 'pg_read_all_stats', 'MEMBER')
                )
            )
            """
            )

        # AND the expected response is returned
        assert has_monitoring == expected_has_monitoring

    @patch(
        "barman.postgres.PostgreSQLConnection.is_superuser", new_callable=PropertyMock
    )
    @patch("barman.postgres.PostgreSQLConnection.connect")
    def test_has_monitoring_privileges_exception(self, conn_mock, mock_is_superuser):
        """
        Verify that a connection error results in a None return value.
        """
        # GIVEN a server managed by Barman
        server = build_real_server()
        cursor_mock = conn_mock.return_value.cursor.return_value
        # AND we are not a superuser
        mock_is_superuser.return_value = False

        # WHEN a PostgresConnectionError is raised during the query
        cursor_mock.fetchone.side_effect = PostgresConnectionError
        has_monitoring = server.postgres.has_monitoring_privileges

        # THEN a None value was returned
        assert has_monitoring is None


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

    @pytest.mark.parametrize("server_version", (90100, 90200, 90300, 90500))
    @patch("barman.postgres.psycopg2.connect")
    def test_fetch_remote_status_for_unsupported_pg_version(
        self, conn_mock, server_version
    ):
        # Build a server
        server = build_real_server(
            main_conf={"streaming_archiver": True, "streaming_conninfo": "dummy=param"}
        )
        conn_mock.return_value.server_version = server_version
        result = server.streaming.fetch_remote_status()
        assert result["version_supported"] is False

    @patch("barman.postgres.psycopg2.connect")
    def test_fetch_remote_status(self, conn_mock):
        """
        simple test for the fetch_remote_status method
        """
        # Build a server
        server = build_real_server(
            main_conf={"streaming_archiver": True, "streaming_conninfo": "dummy=param"}
        )

        # Working streaming connection
        conn_mock.return_value.server_version = PostgreSQL.MINIMAL_VERSION
        cursor_mock = conn_mock.return_value.cursor.return_value
        cursor_mock.fetchone.return_value = ("12345", 1, "DE/ADBEEF")
        result = server.streaming.fetch_remote_status()
        assert result["version_supported"] is True
        cursor_mock.execute.assert_called_with("IDENTIFY_SYSTEM")
        assert result["streaming_supported"] is True
        assert result["streaming"] is True

        # Working non-streaming connection
        conn_mock.reset_mock()
        conn_mock.return_value.server_version = PostgreSQL.MINIMAL_VERSION
        cursor_mock.execute.side_effect = psycopg2.ProgrammingError
        result = server.streaming.fetch_remote_status()
        cursor_mock.execute.assert_called_with("IDENTIFY_SYSTEM")
        assert result["streaming_supported"] is True
        assert result["streaming"] is False

        # Connection failed
        server.streaming.close()
        conn_mock.reset_mock()
        conn_mock.return_value.server_version = PostgreSQL.MINIMAL_VERSION
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


class TestStandbyPostgreSQLConnection(object):
    _standby_conninfo = "db=standby"
    _primary_conninfo = "db=primary"

    @patch("barman.postgres.PostgreSQLConnection")
    @patch("barman.postgres.super")
    def test_close(self, mock_super, mock_psql_conn):
        """Verify that close calls close on both the standby and the primary"""
        # GIVEN a connection to a standby PostgreSQL instance
        mock_standby_conn = mock_super.return_value
        mock_standby_conn.close = Mock()
        mock_primary_conn = mock_psql_conn.return_value
        standby = StandbyPostgreSQLConnection(
            self._standby_conninfo, self._primary_conninfo
        )

        # WHEN the connection is closed
        standby.close()

        # THEN the connection to the standby is closed
        mock_standby_conn.close.assert_called_once_with()

        # AND the connection to the primary is closed
        mock_primary_conn.close.assert_called_once_with()

    @patch("barman.postgres.PostgreSQLConnection")
    @patch("barman.postgres.super")
    def test_switch_wal(self, mock_super, mock_psql_conn):
        """Verify that switch_wal executes a WAL switch via the primary conn"""
        # GIVEN a connection to a standby PostgreSQL instance
        mock_standby_conn = mock_super.return_value
        mock_standby_conn.switch_wal = Mock()
        mock_primary_conn = mock_psql_conn.return_value
        standby = StandbyPostgreSQLConnection(
            self._standby_conninfo, self._primary_conninfo
        )

        # WHEN switch_wal is called
        standby.switch_wal()

        # THEN switch_wal is called on the connection to the primary
        mock_primary_conn.switch_wal.assert_called_once_with()

        # AND switch_wal is not called on the connection to the standby
        mock_standby_conn.switch_wal.assert_not_called()

    @patch("barman.postgres.PostgreSQLConnection")
    @patch("barman.postgres.super")
    def test_switch_wal_in_background(self, _mock_super, mock_psql_conn):
        """
        Verify switch_wal_in_background runs the expected number of times and
        uses the primary connection in the child process not the parent process.
        """
        # GIVEN a connection to a standby PostgreSQL instance
        main_proc_primary_conn = Mock()
        child_proc_primary_conn = Mock()
        mock_psql_conn.side_effect = [main_proc_primary_conn, child_proc_primary_conn]
        standby = StandbyPostgreSQLConnection(
            self._standby_conninfo, self._primary_conninfo
        )

        # WHEN switch_wal_in_background is called with times=2
        times = 2
        standby.switch_wal_in_background(Queue(), times, 0)

        # THEN switch_wal is called on the primary conn in the child process
        # exactly twice
        assert child_proc_primary_conn.switch_wal.call_count == times

        # AND switch_wal is not called on the primary conn in the parent process
        assert main_proc_primary_conn.switch_wal.call_count == 0

        # AND the child process primary conn was closed
        child_proc_primary_conn.close.assert_called_once()

    @patch("barman.postgres.PostgreSQLConnection")
    @patch("barman.postgres.super")
    def test_switch_wal_in_background_short_circuits(self, _mock_super, mock_psql_conn):
        """Verify switch_wal_in_background runs until times are exceeded"""
        # GIVEN a connection to a standby PostgreSQL instance
        mock_primary_conn = mock_psql_conn.return_value
        standby = StandbyPostgreSQLConnection(
            self._standby_conninfo, self._primary_conninfo
        )
        # AND a queue where the first message is the request to stop
        queue = Queue()
        queue.put(True)

        # WHEN switch_wal_in_background is called with times=2
        times = 2
        standby.switch_wal_in_background(queue, times, 0)

        # THEN switch_wal is never called
        assert mock_primary_conn.switch_wal.call_count == 0

    @patch("barman.postgres.PostgreSQLConnection")
    @patch("barman.postgres.super")
    def test_switch_wal_in_background_stops_when_asked(
        self, _mock_super, mock_psql_conn
    ):
        """Verify switch_wal_in_background runs until times are exceeded"""
        # GIVEN a connection to a standby PostgreSQL instance
        mock_primary_conn = mock_psql_conn.return_value
        standby = StandbyPostgreSQLConnection(
            self._standby_conninfo, self._primary_conninfo
        )
        # AND a queue where the second message is the request to stop
        # Note: We use a synchronous Queue rather than the multiprocessing
        # version so that the behaviour of the function under test is deterministic.
        queue = SyncQueue()
        queue.put(False)
        queue.put(True)

        # WHEN switch_wal_in_background is called with times=2
        times = 2
        standby.switch_wal_in_background(queue, times, 0)

        # THEN switch_wal is called on the primary exactly once
        assert mock_primary_conn.switch_wal.call_count == 1

    @patch("barman.postgres.PostgreSQLConnection")
    @patch("barman.postgres.super")
    def test_switch_wal_in_background_calls_checkpoint(
        self, _mock_super, mock_psql_conn, caplog
    ):
        """
        Verify switch_wal_in_background runs the expected number of times and
        then executes a checkpoint after `primary_checkpoint_timeout` value.
        """
        # GIVEN a connection to a standby PostgreSQL instance
        main_proc_primary_conn = Mock()
        child_proc_primary_conn = Mock()
        mock_psql_conn.side_effect = [main_proc_primary_conn, child_proc_primary_conn]
        standby = StandbyPostgreSQLConnection(
            self._standby_conninfo, self._primary_conninfo
        )

        # WHEN switch_wal_in_background is called with times=2 AND primary_checkpoint_timeout
        # is set to 5 seconds
        standby.primary_checkpoint_timeout = 5
        times = 2
        start_time = datetime.datetime.now()
        standby.switch_wal_in_background(Queue(), times, 0)
        end_time = datetime.datetime.now()

        # THEN switch_wal is called on the primary conn in the child process
        # 3 times (2 before the timer and 1 after the checkpoint)
        assert child_proc_primary_conn.switch_wal.call_count == times + 1

        # AND switch_wal is not called on the primary conn in the parent process
        assert main_proc_primary_conn.switch_wal.call_count == 0

        # AND the checkpoint is called on the primary conn in the child process
        assert child_proc_primary_conn.checkpoint.call_count == 1

        # AND the duration between the start and end of the function is greater than
        # the primary_checkpoint_timeout value
        assert (end_time - start_time).total_seconds() > 5

        # AND a warning is logged that the checkpoint was called
        assert (
            "Barman attempted to switch WALs %s times on the primary "
            "server, but the backup has not yet completed. "
            "A checkpoint will be forced on the primary server "
            "in %s seconds to ensure the backup can complete."
            % (times, standby.primary_checkpoint_timeout)
            in caplog.text
        )

        # AND the child process primary conn was closed
        child_proc_primary_conn.close.assert_called_once()

    @pytest.mark.parametrize(
        "stop_fun", ("stop_concurrent_backup", "stop_exclusive_backup")
    )
    @patch("barman.postgres.Queue")
    @patch("barman.postgres.Process")
    @patch("barman.postgres.PostgreSQLConnection")
    @patch("barman.postgres.super")
    def test_stop_backup(
        self, mock_super, mock_psql_conn, mock_process, mock_queue, stop_fun
    ):
        """Verify stop_{concurrent,exclusive}_backup calls correct functions"""
        # GIVEN a connection to a standby PostgreSQL instance
        mock_standby_conn = mock_super.return_value
        setattr(mock_standby_conn, stop_fun, Mock())
        mock_done_q = mock_queue.return_value
        standby = StandbyPostgreSQLConnection(
            self._standby_conninfo, self._primary_conninfo
        )

        # WHEN stop_concurrent_backup is called
        getattr(standby, stop_fun)()

        # THEN stop_concurrent_backup is called on the standby exactly once
        assert getattr(mock_standby_conn, stop_fun).call_count == 1

        # AND Process is called with the switch_wal_in_background target
        mock_process.assert_called_once_with(
            target=standby.switch_wal_in_background, args=(mock_done_q,)
        )
        # AND the process was started and joined
        mock_process.return_value.start.assert_called_once()
        mock_process.return_value.join.assert_called_once()

        # AND mock_done_q.put is called exactly once with the argument True
        mock_done_q.put.assert_called_once_with(True)
