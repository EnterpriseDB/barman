# Copyright (C) 2013-2016 2ndQuadrant Italia Srl
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

import psycopg2
import pytest
from mock import PropertyMock, call, patch

from barman.postgres import PostgresConnectionError
from testing_helpers import build_real_server


# noinspection PyMethodMayBeStatic
class TestPostgres(object):

    def test_connection_error(self):
        """
        simple test for missing conninfo
        """
        # Test with wrong configuration
        server = build_real_server(main_conf={'conninfo': ''})
        assert server.config.msg_list
        assert 'Missing conninfo parameter in barman ' \
            'configuration for server main' in server.config.msg_list

    @patch('barman.postgres.psycopg2.connect')
    def test_connect_and_close(self, pg_connect_mock):
        """
        Check pg_connect method beaviour on error
        """
        # Setup server
        server = build_real_server()
        server.postgres.conninfo = "valid conninfo"
        conn_mock = pg_connect_mock.return_value
        conn_mock.server_version = 90401
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

        # close it
        pg_connect_mock.reset_mock()

        server.postgres.close()

        assert conn_mock.close.called

        # open again and verify that it is a new object
        pg_connect_mock.reset_mock()

        server.postgres.connect()

        pg_connect_mock.assert_called_with("valid conninfo")

    @patch('barman.postgres.psycopg2.connect')
    def test_connect_error(self, connect_mock):
        """
        Check pg_connect method beaviour on error
        """
        # Setup temp dir and server
        server = build_real_server()
        server.postgres.conninfo = "not valid conninfo"
        connect_mock.side_effect = psycopg2.DatabaseError
        # expect pg_connect to raise a PostgresConnectionError
        with pytest.raises(PostgresConnectionError):
            server.postgres.connect()
        connect_mock.assert_called_with("not valid conninfo")

    @patch('barman.postgres.PostgreSQLConnection.connect')
    def test_streaming_server_txt_version(self, conn_mock):
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
            "Apple LLVM version 7.0.0 (clang-700.1.76), 64-bit",)

        assert server.postgres.server_txt_version == '9.4.5'
        cursor_mock.execute.assert_called_with("SELECT version()")

    @patch('barman.postgres.PostgreSQLConnection.connect')
    @patch('barman.postgres.PostgreSQLConnection.is_in_recovery',
           new_callable=PropertyMock)
    def test_create_restore_point(self, is_in_recovery_mock, conn_mock):
        """
        Basic test for the _restore_point method
        """
        # Simulate a master connection
        is_in_recovery_mock.return_value = False

        server = build_real_server()
        # Test 1: Postgres 9.0 expect None as result
        conn_mock.return_value.server_version = 90000

        restore_point = server.postgres.create_restore_point(
            "Test_20151026T092241")
        assert restore_point is None

        # Simulate a master connection
        is_in_recovery_mock.return_value = True

        # Test 2: Postgres 9.1 in recovery (standby) expect None as result
        conn_mock.return_value.server_version = 90100

        restore_point = server.postgres.create_restore_point(
            "Test_20151026T092241")
        assert restore_point is None

        # Test 3: Postgres 9.1 check mock calls
        is_in_recovery_mock.return_value = False

        assert server.postgres.create_restore_point("Test_20151026T092241")

        cursor_mock = conn_mock.return_value.cursor.return_value
        cursor_mock.execute.assert_called_with(
            "SELECT pg_create_restore_point(%s)", ['Test_20151026T092241'])
        assert cursor_mock.fetchone.called

        # Test error management
        cursor_mock.execute.side_effect = psycopg2.Error
        assert server.postgres.create_restore_point(
            "Test_20151026T092241") is None

    @patch('barman.postgres.PostgreSQLConnection.connect')
    def test_stop_exclusive_backup(self, conn):
        """
        Basic test for the stop_exclusive_backup method

        :param conn: a mock that imitates a connection to PostgreSQL
        """
        # Build a server
        server = build_real_server()

        # Expect no errors on normal call
        assert server.postgres.stop_exclusive_backup()

        # check the correct invocation of the execute method
        cursor_mock = conn.return_value.cursor.return_value
        cursor_mock.execute.assert_called_once_with(
            'SELECT xlog_loc, (pg_xlogfile_name_offset(xlog_loc)).*, now() '
            'FROM pg_stop_backup() as xlog_loc'
        )
        # Test 2: Setup the mock to trigger an exception
        # expect the method to return None
        conn.reset_mock()
        cursor_mock.execute.side_effect = psycopg2.Error
        # Check that the method returns None as result
        assert server.postgres.stop_exclusive_backup() is None

    @patch('barman.postgres.PostgreSQLConnection.connect')
    def test_pgespresso_stop_backup(self, conn):
        """
        Basic test for pgespresso_stop_backup method
        """
        # Build a server
        server = build_real_server()

        # Test 1: Expect no error and the correct call sequence
        assert server.postgres.pgespresso_stop_backup('test_label')

        cursor_mock = conn.return_value.cursor.return_value
        cursor_mock.execute.assert_called_once_with(
            'SELECT pgespresso_stop_backup(%s), now()', ('test_label',)
        )

        # Test 2: Setup the mock to trigger an exception
        # expect the method to return None
        conn.reset_mock()
        cursor_mock.execute.side_effect = psycopg2.Error
        # Check that the method returns None as result
        assert server.postgres.pgespresso_stop_backup('test_label') is None

    @patch('barman.postgres.PostgreSQLConnection.connect')
    def test_start_exclusive_backup(self, conn):
        """
        Simple test for start_exclusive_backup method of
        the RsyncBackupExecutor class
        """
        # Build a server
        server = build_real_server()
        backup_label = 'test label'

        # Expect no errors
        conn.return_value.server_version = 90300
        assert server.postgres.start_exclusive_backup(backup_label)

        # check for the correct call on the execute method
        cursor_mock = conn.return_value.cursor.return_value
        cursor_mock.execute.assert_called_once_with(
            'SELECT xlog_loc, '
            '(pg_xlogfile_name_offset(xlog_loc)).*, '
            'now() FROM pg_start_backup(%s,%s) as xlog_loc',
            ('test label', False)
        )
        conn.return_value.rollback.assert_has_calls([call(), call()])
        # reset the mock for the next test
        conn.reset_mock()

        # 8.3 test
        conn.return_value.server_version = 80300
        assert server.postgres.start_exclusive_backup(backup_label)
        # check for the correct call on the execute method
        cursor_mock.execute.assert_called_once_with(
            'SELECT xlog_loc, '
            '(pg_xlogfile_name_offset(xlog_loc)).*, '
            'now() FROM pg_start_backup(%s) as xlog_loc',
            ('test label',)
        )
        conn.return_value.rollback.assert_has_calls([call(), call()])
        # reset the mock for the next test
        conn.reset_mock()

        # test error management
        cursor_mock.execute.side_effect = psycopg2.Error
        with pytest.raises(Exception):
            server.postgres.start_exclusive_backup(backup_label)
        conn.return_value.rollback.assert_called_once_with()

    @patch('barman.postgres.PostgreSQLConnection.connect')
    def test_pgespresso_start_backup(self, conn):
        """
        Simple test for _pgespresso_start_backup method
        of the RsyncBackupExecutor class
        """
        # Build and configure a server
        server = build_real_server()
        backup_label = 'test label'

        # expect no errors
        assert server.postgres.pgespresso_start_backup(backup_label)

        cursor_mock = conn.return_value.cursor.return_value
        cursor_mock.execute.assert_called_once_with(
            'SELECT pgespresso_start_backup(%s,%s), now()',
            (backup_label, server.postgres.config.immediate_checkpoint)
        )
        conn.return_value.rollback.assert_has_calls([call(), call()])
        # reset the mock for the next test
        conn.reset_mock()

        # Test 2: Setup the mock to trigger an exception
        # expect the method to return None
        cursor_mock.execute.side_effect = psycopg2.Error
        # Check that the method returns None as result
        with pytest.raises(Exception):
            server.postgres.pgespresso_start_backup('test_label')
        conn.return_value.rollback.assert_called_once_with()

    @patch('barman.postgres.PostgreSQLConnection.connect')
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
        assert server.postgres.get_setting('test_setting') is None

    @patch('barman.postgres.PostgreSQLConnection.connect')
    def test_get_tablespaces(self, conn):
        """
        Simple test for pg_start_backup method of the RsyncBackupExecutor class
        """
        # Build a server
        server = build_real_server()
        cursor_mock = conn.return_value.cursor.return_value
        cursor_mock.fetchall.return_value = [
            ("tbs1", "1234", "/tmp")
        ]
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
        cursor_mock.fetchall.return_value = [
            ("tbs2", "5234", "/tmp1")
        ]
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

    @patch('barman.postgres.PostgreSQLConnection.connect')
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
        cursor_mock.fetchone.return_value = {'a': 'b'}
        assert server.postgres.get_archiver_stats() == {'a': 'b'}
        # check for the correct call on the execute method
        cursor_mock.execute.assert_called_once_with(
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
        conn.reset_mock()

        # test error management
        cursor_mock.execute.side_effect = psycopg2.Error
        assert server.postgres.get_archiver_stats() is None

    @patch('barman.postgres.PostgreSQLConnection.connect')
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
            ("ident_file", "/tmp/pg_ident.conf")]
        cursor_mock.fetchall.side_effect = [test_conf_files, [('/test/file',)]]
        retval = server.postgres.get_configuration_files()

        assert retval == server.postgres.configuration_files
        assert server.postgres.configuration_files == dict(
            test_conf_files + [("included_files", ["/test/file"])])
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

    @patch('barman.postgres.PostgreSQLConnection.connect')
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
            "SELECT count(*) FROM pg_extension "
            "WHERE extname = 'pgespresso'")

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

    @patch('barman.postgres.PostgreSQLConnection.connect')
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
        cursor_mock.execute.assert_called_once_with(
            "SELECT pg_is_in_recovery()")

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

    @patch('barman.postgres.PostgreSQLConnection.connect')
    @patch('barman.postgres.PostgreSQLConnection.is_in_recovery',
           new_callable=PropertyMock)
    def test_current_xlog(self, is_in_recovery_mock, conn_mock):
        """
        simple test for current_xlog property
        """
        # Build a server
        server = build_real_server()
        cursor_mock = conn_mock.return_value.cursor.return_value

        cursor_mock.fetchone.return_value = ['000000010000000000000006']
        # Special way to mock a property
        is_in_recovery_mock.return_value = False
        assert server.postgres.current_xlog == '000000010000000000000006'
        cursor_mock.execute.assert_called_once_with(
            'SELECT pg_xlogfile_name(pg_current_xlog_location())')

        # Reset mock
        conn_mock.reset_mock()

        # Test error management
        cursor_mock.execute.side_effect = PostgresConnectionError
        assert server.postgres.current_xlog is None

        cursor_mock.execute.side_effect = psycopg2.ProgrammingError
        assert server.postgres.current_xlog is None

    @patch('barman.postgres.psycopg2.connect')
    @patch('barman.postgres.PostgreSQLConnection.is_in_recovery',
           new_callable=PropertyMock)
    @patch('barman.postgres.PostgreSQLConnection.is_superuser',
           new_callable=PropertyMock)
    @patch('barman.postgres.PostgreSQLConnection.server_txt_version',
           new_callable=PropertyMock)
    @patch('barman.postgres.PostgreSQLConnection.has_pgespresso',
           new_callable=PropertyMock)
    @patch('barman.postgres.PostgreSQLConnection.current_xlog',
           new_callable=PropertyMock)
    @patch('barman.postgres.PostgreSQLConnection.current_size',
           new_callable=PropertyMock)
    @patch('barman.postgres.PostgreSQLConnection.get_configuration_files')
    @patch('barman.postgres.PostgreSQLConnection.get_setting')
    def test_get_remote_status(self, get_setting_mock,
                               get_configuration_files_mock,
                               current_size_mock,
                               current_xlog_mock,
                               has_pgespresso_mock,
                               server_txt_version_mock,
                               is_superuser_mock,
                               is_in_recovery_mock,
                               conn_mock):
        """
        simple test for the fetch_remote_status method
        """
        # Build a server
        server = build_real_server()
        current_xlog_mock.return_value = 'DE/ADBEEF'
        current_size_mock.return_value = 497354072
        has_pgespresso_mock.return_value = True
        server_txt_version_mock.return_value = '9.1.0'
        is_in_recovery_mock.return_value = False
        is_superuser_mock.return_value = True
        get_configuration_files_mock.return_value = {'a': 'b'}
        get_setting_mock.return_value = 'dummy_setting'
        conn_mock.return_value.server_version = 90100

        result = server.postgres.fetch_remote_status()

        assert result == {
            'a': 'b',
            'is_superuser': True,
            'current_xlog': 'DE/ADBEEF',
            'data_directory': 'dummy_setting',
            'pgespresso_installed': True,
            'server_txt_version': '9.1.0',
            'wal_level': 'dummy_setting',
            'current_size': 497354072}

        # Test error management
        server.postgres.close()
        conn_mock.side_effect = psycopg2.DatabaseError
        assert server.postgres.fetch_remote_status() == {
            'is_superuser': None,
            'current_xlog': None,
            'data_directory': None,
            'pgespresso_installed': None,
            'server_txt_version': None}

        get_setting_mock.side_effect = psycopg2.ProgrammingError
        assert server.postgres.fetch_remote_status() == {
            'is_superuser': None,
            'current_xlog': None,
            'data_directory': None,
            'pgespresso_installed': None,
            'server_txt_version': None}


# noinspection PyMethodMayBeStatic
class TestStreamingConnection(object):

    def test_connection_error(self):
        """
        simple test for streaming_archiver without streaming_conninfo
        """
        # Test with wrong configuration
        server = build_real_server(main_conf={
            'streaming_archiver': True,
            'streaming_conninfo': ''})
        assert server.config.msg_list
        assert 'Missing streaming_conninfo parameter in barman ' \
            'configuration for server main' in server.config.msg_list

    @patch('barman.postgres.psycopg2.connect')
    def test_fetch_remote_status(self, conn_mock):
        """
        simple test for the fetch_remote_status method
        """
        # Build a server
        server = build_real_server(
            main_conf={
                'streaming_archiver': True,
                'streaming_conninfo': 'dummy=param'})

        # Too old PostgreSQL
        conn_mock.return_value.server_version = 90100
        result = server.streaming.fetch_remote_status()
        assert result["streaming_supported"] is False
        assert result['streaming'] is None

        # Working streaming connection
        conn_mock.return_value.server_version = 90300
        cursor_mock = conn_mock.return_value.cursor.return_value
        cursor_mock.fetchone.return_value = ('12345', 1, 'DE/ADBEEF')
        result = server.streaming.fetch_remote_status()
        cursor_mock.execute.assert_called_once_with("IDENTIFY_SYSTEM")
        assert result["streaming_supported"] is True
        assert result['streaming'] is True

        # Working non-streaming connection
        conn_mock.reset_mock()
        cursor_mock.execute.side_effect = psycopg2.ProgrammingError
        result = server.streaming.fetch_remote_status()
        cursor_mock.execute.assert_called_once_with("IDENTIFY_SYSTEM")
        assert result["streaming_supported"] is True
        assert result['streaming'] is False

        # Connection failed
        server.streaming.close()
        conn_mock.reset_mock()
        conn_mock.side_effect = psycopg2.DatabaseError
        result = server.streaming.fetch_remote_status()
        assert result["streaming_supported"] is None
        assert result['streaming'] is None

    @patch('barman.postgres.PostgreSQL.connect')
    def test_streaming_server_txt_version(self, conn_mock):
        """
        simple test for the server_txt_version property
        """
        # Build a server
        server = build_real_server(
            main_conf={
                'streaming_archiver': True,
                'streaming_conninfo': 'dummy=param'})

        # Connection error
        conn_mock.side_effect = PostgresConnectionError
        assert server.streaming.server_txt_version is None

        # Good connection
        conn_mock.side_effect = None

        conn_mock.return_value.server_version = 80300
        assert server.streaming.server_txt_version == '8.3.0'

        conn_mock.return_value.server_version = 90000
        assert server.streaming.server_txt_version == '9.0.0'

        conn_mock.return_value.server_version = 90005
        assert server.streaming.server_txt_version == '9.0.5'

        conn_mock.return_value.server_version = 100201
        assert server.streaming.server_txt_version == '10.2.1'

        conn_mock.return_value.server_version = 101811
        assert server.streaming.server_txt_version == '10.18.11'

        conn_mock.return_value.server_version = 0
        assert server.streaming.server_txt_version == '0.0.0'
