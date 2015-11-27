# Copyright (C) 2013-2015 2ndQuadrant Italia Srl
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
from mock import PropertyMock, patch

from barman.server import PostgresConnectionError
from testing_helpers import build_real_server


# noinspection PyMethodMayBeStatic
class TestPostgres(object):
    def test_connect_error(self):
        """
        Check pg_connect method beaviour on error
        """
        # Setup temp dir and server
        server = build_real_server()
        # Set an invalid conninfo parameter.
        server.postgres.config.conninfo = "not valid conninfo"
        # expect pg_connect to raise a PostgresConnectionError
        with pytest.raises(PostgresConnectionError):
            server.postgres.connect()

    @patch('barman.postgres.PostgreSQLConnection.connect')
    @patch('barman.postgres.PostgreSQLConnection.is_in_recovery',
           new_callable=PropertyMock)
    def test_create_restore_point(self, is_in_recovery_mock, conn):
        """
        Basic test for the _restore_point method
        """
        # Simulate a master connection
        is_in_recovery_mock.return_value = False

        server = build_real_server()
        # Test 1: Postgres 9.0 expect None as result
        conn.return_value.server_version = 90000

        restore_point = server.postgres.create_restore_point(
            "Test_20151026T092241")
        assert restore_point is None

        # Simulate a master connection
        is_in_recovery_mock.return_value = True

        # Test 2: Postgres 9.1 in recovery (standby) expect None as result
        conn.return_value.server_version = 90100

        restore_point = server.postgres.create_restore_point(
            "Test_20151026T092241")
        assert restore_point is None

        # Test 3: Postgres 9.1 check mock calls
        is_in_recovery_mock.return_value = False

        server.postgres.create_restore_point("Test_20151026T092241")
        conn.return_value.cursor.return_value.execute.assert_called_with(
            "SELECT pg_create_restore_point(%s)", ['Test_20151026T092241'])
        assert conn.return_value.cursor.return_value.fetchone.called is True

        # test error management
        conn.return_value.cursor.return_value.execute.side_effect = psycopg2.Error
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

        # expect no errors
        server.postgres.stop_exclusive_backup()
        # check the correct invocation of the execute method
        conn.return_value.cursor.return_value.execute.assert_called_once_with(
            'SELECT xlog_loc, (pg_xlogfile_name_offset(xlog_loc)).*, now() '
            'FROM pg_stop_backup() as xlog_loc'
        )
        # reset the mock for the second test
        conn.reset_mock()
        # Test 2: Setup the mock to trigger an exception
        # expect the method to return None
        conn.return_value.cursor.return_value.execute.side_effect = psycopg2.Error
        # Check that the method returns None as result
        assert server.postgres.stop_exclusive_backup() is None

    @patch('barman.postgres.PostgreSQLConnection.connect')
    def test_pgespresso_stop_backup(self, conn):
        """
        Basic test for _pgespresso_stop_backup
        """
        # Build a server
        server = build_real_server()

        # Test 1: Expect no error and the correct call sequence
        server.postgres.pgespresso_stop_backup('test_label')
        conn.return_value.cursor.return_value.execute.assert_called_once_with(
            'SELECT pgespresso_stop_backup(%s), now()', ('test_label',)
        )
        # reset the mock for the second test
        conn.reset_mock()

        # Test 2: Setup the mock to trigger an exception
        # expect the method to return None
        conn.return_value.cursor.return_value.execute.side_effect = psycopg2.Error
        # Check that the method returns None as result
        assert server.postgres.pgespresso_stop_backup('test_label') is None

    @patch('barman.postgres.PostgreSQLConnection.connect')
    def test_start_exclusive_backup(self, conn):
        """
        Simple test for start_exclusive_backup method of the RsyncBackupExecutor class
        """
        # Build a server
        server = build_real_server()
        backup_label = 'test label'

        # Expect no errors
        conn.return_value.server_version = 90300
        server.postgres.start_exclusive_backup(backup_label)
        # check for the correct call on the execute method
        conn.return_value.cursor.return_value.execute.assert_called_once_with(
            'SELECT xlog_loc, '
            '(pg_xlogfile_name_offset(xlog_loc)).*, '
            'now() FROM pg_start_backup(%s,%s) as xlog_loc',
            ('test label', False)
        )
        conn.reset_mock()

        # 8.3 test
        conn.return_value.server_version = 80300
        server.postgres.start_exclusive_backup(backup_label)
        # check for the correct call on the execute method
        conn.return_value.cursor.return_value.execute.assert_called_once_with(
            'SELECT xlog_loc, '
            '(pg_xlogfile_name_offset(xlog_loc)).*, '
            'now() FROM pg_start_backup(%s) as xlog_loc',
            ('test label',)
        )

        conn.reset_mock()
        # test error management
        conn.return_value.cursor.return_value.execute.side_effect = psycopg2.Error
        with pytest.raises(Exception):
            server.postgres.start_exclusive_backup(backup_label)

    @patch('barman.postgres.PostgreSQLConnection.connect')
    def test_pgespresso_start_backup(self, conn):
        """
        Simple test for _pgespresso_start_backup method
        of the RsyncBackupExecutor class
        """
        server = build_real_server()
        backup_label = 'test label'

        # expect no errors
        server.postgres.pgespresso_start_backup(backup_label)
        conn.return_value.cursor.return_value.execute.assert_called_once_with(
            'SELECT pgespresso_start_backup(%s,%s), now()',
            (backup_label, server.postgres.config.immediate_checkpoint)
        )
        # reset the mock for the second test
        conn.reset_mock()

        # Test 2: Setup the mock to trigger an exception
        # expect the method to return None
        conn.return_value.cursor.return_value.execute.side_effect = psycopg2.Error
        # Check that the method returns None as result
        with pytest.raises(Exception):
            server.postgres.pgespresso_start_backup('test_label')

    @patch('barman.postgres.PostgreSQLConnection.connect')
    def test_get_setting(self, conn):
        """
        Simple test for retrieving settings from the database
        """
        # Build and configure a server

        server = build_real_server()

        # expect no errors
        server.postgres.get_setting("test_setting")
        conn.return_value.cursor.return_value.execute.assert_called_once_with(
            'SHOW "%s"' % "test_setting".replace('"', '""')
        )
        # reset the mock for the second test
        conn.reset_mock()

        # Test 2: Setup the mock to trigger an exception
        # expect the method to return None
        conn.return_value.cursor.return_value.execute.side_effect = psycopg2.Error
        # Check that the method returns None as result
        assert server.postgres.get_setting('test_setting') is None

    @patch('barman.postgres.PostgreSQLConnection.connect')
    def test_get_tablespaces(self, conn):
        """
        Simple test for pg_start_backup method of the RsyncBackupExecutor class
        """
        # Build a server
        server = build_real_server()
        conn.return_value.cursor.return_value.fetchall.return_value = [
            ("tbs1", "1234", "/tmp")
        ]
        # Expect no errors
        conn.return_value.server_version = 90400
        tbs = server.postgres.get_tablespaces()
        # check for the correct call on the execute method
        conn.return_value.cursor.return_value.execute.assert_called_once_with(
            "SELECT spcname, oid, "
            "pg_tablespace_location(oid) AS spclocation "
            "FROM pg_tablespace "
            "WHERE pg_tablespace_location(oid) != ''"
        )
        assert tbs
        assert tbs[0].name == 'tbs1'
        conn.reset_mock()

        # 8.3 test
        conn.return_value.server_version = 80300
        conn.return_value.cursor.return_value.fetchall.return_value = [
            ("tbs1", "1234", "/tmp")
        ]
        tbs = server.postgres.get_tablespaces()
        # check for the correct call on the execute method
        conn.return_value.cursor.return_value.execute.assert_called_once_with(
            "SELECT spcname, oid, spclocation "
            "FROM pg_tablespace WHERE spclocation != ''"
        )
        assert tbs
        assert tbs[0].name == 'tbs1'

        conn.reset_mock()
        # test error management
        conn.return_value.cursor.return_value.execute.side_effect = psycopg2.Error
        assert server.postgres.get_tablespaces() is None

    @patch('barman.postgres.PostgreSQLConnection.connect')
    def test_get_archiver_stats(self, conn):
        """
        Simple test for pg_start_backup method of the RsyncBackupExecutor class
        """
        # Build a server
        server = build_real_server()
        # expect None as result for server version <9.4
        conn.return_value.server_version = 80300
        assert server.postgres.get_archiver_stats() is None

        # expect no errors with version >= 9.4
        conn.reset_mock()
        conn.return_value.server_version = 90400
        server.postgres.get_archiver_stats()
        # check for the correct call on the execute method
        conn.return_value.cursor.return_value.execute.assert_called_once_with(
            "SELECT *, current_setting('archive_mode')::BOOLEAN "
            "AND (last_failed_wal IS NULL "
            "OR last_failed_wal <= last_archived_wal) "
            "AS is_archiving, "
            "CAST (archived_count AS NUMERIC) "
            "/ EXTRACT (EPOCH FROM age(now(), stats_reset)) "
            "AS current_archived_wals_per_second "
            "FROM pg_stat_archiver"
        )
        conn.reset_mock()

        # test error management
        conn.return_value.cursor.return_value.execute.side_effect = psycopg2.Error
        assert server.postgres.get_archiver_stats() is None

    @patch('barman.postgres.PostgreSQLConnection.connect')
    def test_get_configuration_files(self, conn):
        """
        simple test for the get_configuration_files method
        """
        # Build a server
        server = build_real_server()
        conn.return_value.cursor.return_value.fetchall.side_effect = [[
            ("config_file", "/tmp/postgresql.conf"),
            ("hba_file", "/tmp/pg_hba.conf"),
            ("ident_file", "/tmp/pg_ident.conf")], []
        ]
        server.postgres.get_configuration_files()
        assert server.postgres.configuration_files
        assert server.postgres.configuration_files[
            'config_file'] == "/tmp/postgresql.conf"
        assert server.postgres.configuration_files[
            'hba_file'] == "/tmp/pg_hba.conf"
        assert server.postgres.configuration_files[
            'ident_file'] == "/tmp/pg_ident.conf"

        # check for the correct queries
        conn.return_value.cursor.return_value.execute.assert_any_call(
            "SELECT name, setting FROM pg_settings "
            "WHERE name IN ('config_file', 'hba_file', 'ident_file')"
        )
        conn.return_value.cursor.return_value.execute.assert_any_call(
            "SELECT DISTINCT sourcefile AS included_file "
            "FROM pg_settings "
            "WHERE sourcefile IS NOT NULL "
            "AND sourcefile NOT IN "
            "(SELECT setting FROM pg_settings "
            "WHERE name = 'config_file') "
            "ORDER BY 1"
        )
        # reset mock and configuration files
        conn.reset_mock()
        server.postgres.configuration_files = None
        # test error management
        conn.return_value.cursor.return_value.execute.side_effect = psycopg2.Error
        assert server.postgres.get_configuration_files() == {}

    @patch('barman.postgres.StreamingConnection.connect')
    def test_get_streaming_remote_status(self, conn):
        """
        simple test for the get_configuration_files method
        """
        # Build a server
        server = build_real_server(
            main_conf={
                'streaming_archiver': True,
                'streaming_conninfo': 'dummy=param'})

        # Working streaming connection
        conn.return_value.server_version = 90300
        result = server.streaming.get_remote_status()
        assert result['streaming'] is True

        # Working non-streaming connection
        cursor_mock = conn.return_value.cursor.return_value
        cursor_mock.execute.assert_called_once_with("IDENTIFY_SYSTEM")
        conn.reset_mock()
        cursor_mock.execute.side_effect = psycopg2.ProgrammingError
        result = server.streaming.get_remote_status()
        assert result['streaming'] is False

        # Connection failed
        cursor_mock.execute.assert_called_once_with("IDENTIFY_SYSTEM")
        conn.reset_mock()
        conn.side_effect = PostgresConnectionError
        result = server.streaming.get_remote_status()
        assert result['streaming'] is None

    @patch('barman.postgres.StreamingConnection.connect')
    def test_streaming_server_txt_version(self, conn):
        """
        simple test for the server_txt_version property
        """
        # Build a server
        server = build_real_server(
            main_conf={
                'streaming_archiver': True,
                'streaming_conninfo': 'dummy=param'})

        conn.return_value.server_version = 80300
        assert server.streaming.server_txt_version == '8.3.0'

        conn.return_value.server_version = 90000
        assert server.streaming.server_txt_version == '9.0.0'

        conn.return_value.server_version = 90005
        assert server.streaming.server_txt_version == '9.0.5'

        conn.return_value.server_version = 100201
        assert server.streaming.server_txt_version == '10.2.1'

        conn.return_value.server_version = 101811
        assert server.streaming.server_txt_version == '10.18.11'

        conn.return_value.server_version = 0
        assert server.streaming.server_txt_version == '0.0.0'
