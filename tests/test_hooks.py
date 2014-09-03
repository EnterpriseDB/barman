# Copyright (C) 2013-2014 2ndQuadrant Italia (Devise.IT S.r.L.)
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

import unittest
from mock import MagicMock, patch
import time
from barman.infofile import UnknownBackupIdException
from barman.version import __version__ as version
from barman.hooks import HookScriptRunner


class HooksUnitTest(unittest.TestCase):
    @staticmethod
    def build_backup_manager(server_name, script_file):
        backup_manager = MagicMock(name='backup_manager')
        backup_manager.server.config.config.config_file = script_file
        backup_manager.server.config.name = server_name
        backup_manager.config = backup_manager.server.config
        return backup_manager

    @patch('barman.hooks.Command')
    def test_general(self, command_mock):
        # BackupManager mock
        backup_manager = self.build_backup_manager('test_server', 'test_file')
        backup_manager.config.pre_test_hook = 'not_existent_script'

        # Command mock executed by HookScriptRunner
        command_mock.return_value.return_value = 0

        # the actual test
        script = HookScriptRunner(backup_manager, 'test_hook', 'pre')
        expected_env = {
            'BARMAN_PHASE': 'pre',
            'BARMAN_VERSION': version,
            'BARMAN_SERVER': 'test_server',
            'BARMAN_CONFIGURATION': 'test_file',
            'BARMAN_HOOK': 'test_hook',
        }
        assert script.run() == 0
        assert command_mock.call_count == 1
        assert command_mock.call_args[1]['env_append'] == expected_env

    @patch('barman.hooks.Command')
    def test_general_error(self, command_mock):
        # BackupManager mock
        backup_manager = self.build_backup_manager('test_server', 'test_file')
        backup_manager.config.pre_test_hook = 'not_existent_script'

        # Command mock executed by HookScriptRunner
        command_mock.return_value.return_value = 0

        # the actual test
        script = HookScriptRunner(backup_manager,
                                  'test_hook', 'pre', 'Generic Failure')
        expected_env = {
            'BARMAN_PHASE': 'pre',
            'BARMAN_VERSION': version,
            'BARMAN_SERVER': 'test_server',
            'BARMAN_CONFIGURATION': 'test_file',
            'BARMAN_HOOK': 'test_hook',
            'BARMAN_ERROR': 'Generic Failure',
        }
        assert script.run() == 0
        assert command_mock.call_count == 1
        assert command_mock.call_args[1]['env_append'] == expected_env

    @patch('barman.hooks.Command')
    def test_general_no_phase(self, command_mock):
        # BackupManager mock
        backup_manager = self.build_backup_manager('test_server', 'test_file')
        backup_manager.config.test_hook = 'not_existent_script'

        # Command mock executed by HookScriptRunner
        command_mock.return_value.return_value = 0

        # the actual test
        script = HookScriptRunner(backup_manager, 'test_hook')
        expected_env = {
            'BARMAN_VERSION': version,
            'BARMAN_SERVER': 'test_server',
            'BARMAN_CONFIGURATION': 'test_file',
            'BARMAN_HOOK': 'test_hook',
        }
        assert script.run() == 0
        assert command_mock.call_count == 1
        assert command_mock.call_args[1]['env_append'] == expected_env

    @patch('barman.hooks.Command')
    def test_missing_config(self, command_mock):
        # BackupManager mock
        backup_manager = self.build_backup_manager('test_server', 'test_file')

        # if configuration line is missing then the script is disabled
        del backup_manager.config.pre_test_hook

        # Command mock executed by HookScriptRunner
        command_mock.side_effect = Exception('Test error')
        command_mock.return_value.return_value = 0

        # the actual test
        script = HookScriptRunner(backup_manager, 'test_hook', 'pre')
        assert script.run() is None  # disabled script
        assert script.exception is None
        assert command_mock.call_count == 0

    @patch('barman.hooks.Command')
    def test_no_exception(self, command_mock):
        # BackupManager mock
        backup_manager = self.build_backup_manager('test_server', 'test_file')
        backup_manager.config.pre_test_hook = 'not_existent_script'

        # Command mock executed by HookScriptRunner
        expected_exception = Exception('Test error')
        command_mock.side_effect = expected_exception
        command_mock.return_value.return_value = 0

        # the actual test
        script = HookScriptRunner(backup_manager, 'test_hook', 'pre')
        assert script.run() is None  # exception
        assert script.exception == expected_exception
        assert command_mock.call_count == 1

    @patch('barman.hooks.Command')
    def test_backup_info(self, command_mock):
        # BackupManager mock
        backup_manager = self.build_backup_manager('test_server', 'test_file')
        backup_manager.config.pre_test_hook = 'not_existent_script'
        backup_manager.get_previous_backup.return_value.backup_id = '987654321'

        # BackupInfo mock
        backup_info = MagicMock(name='backup_info')
        backup_info.get_basebackup_directory.return_value = 'backup_directory'
        backup_info.backup_id = '123456789XYZ'
        backup_info.error = None
        backup_info.status = 'OK'

        # the actual test
        script = HookScriptRunner(backup_manager, 'test_hook', 'pre')
        script.env_from_backup_info(backup_info)
        expected_env = {
            'BARMAN_PHASE': 'pre',
            'BARMAN_VERSION': version,
            'BARMAN_SERVER': 'test_server',
            'BARMAN_CONFIGURATION': 'test_file',
            'BARMAN_HOOK': 'test_hook',
            'BARMAN_BACKUP_DIR': 'backup_directory',
            'BARMAN_BACKUP_ID': '123456789XYZ',
            'BARMAN_ERROR': '',
            'BARMAN_STATUS': 'OK',
            'BARMAN_PREVIOUS_ID': '987654321',
        }
        script.run()
        assert command_mock.call_count == 1
        assert command_mock.call_args[1]['env_append'] == expected_env

    @patch('barman.hooks.Command')
    def test_backup_info_corner_cases(self, command_mock):
        # BackupManager mock
        backup_manager = self.build_backup_manager('test_server', 'test_file')
        backup_manager.config.post_test_hook = 'not_existent_script'
        backup_manager.get_previous_backup.return_value = None

        # BackupInfo mock
        backup_info = MagicMock(name='backup_info')
        backup_info.get_basebackup_directory.return_value = 'backup_directory'
        backup_info.backup_id = '123456789XYZ'
        backup_info.error = 'Test error'
        backup_info.status = 'FAILED'

        # the actual test
        script = HookScriptRunner(backup_manager, 'test_hook', 'post')
        script.env_from_backup_info(backup_info)
        expected_env = {
            'BARMAN_PHASE': 'post',
            'BARMAN_VERSION': version,
            'BARMAN_SERVER': 'test_server',
            'BARMAN_CONFIGURATION': 'test_file',
            'BARMAN_HOOK': 'test_hook',
            'BARMAN_BACKUP_DIR': 'backup_directory',
            'BARMAN_BACKUP_ID': '123456789XYZ',
            'BARMAN_ERROR': 'Test error',
            'BARMAN_STATUS': 'FAILED',
            'BARMAN_PREVIOUS_ID': '',
        }
        script.run()
        assert command_mock.call_count == 1
        assert command_mock.call_args[1]['env_append'] == expected_env

    @patch('barman.hooks.Command')
    def test_backup_info_exception(self, command_mock):
        # BackupManager mock
        backup_manager = self.build_backup_manager('test_server', 'test_file')
        backup_manager.config.pre_test_hook = 'not_existent_script'
        backup_manager.get_previous_backup.side_effect = \
            UnknownBackupIdException()

        # BackupInfo mock
        backup_info = MagicMock(name='backup_info')
        backup_info.get_basebackup_directory.return_value = 'backup_directory'
        backup_info.backup_id = '123456789XYZ'
        backup_info.error = None
        backup_info.status = 'OK'

        # the actual test
        script = HookScriptRunner(backup_manager, 'test_hook', 'pre')
        script.env_from_backup_info(backup_info)
        expected_env = {
            'BARMAN_PHASE': 'pre',
            'BARMAN_VERSION': version,
            'BARMAN_SERVER': 'test_server',
            'BARMAN_CONFIGURATION': 'test_file',
            'BARMAN_HOOK': 'test_hook',
            'BARMAN_BACKUP_DIR': 'backup_directory',
            'BARMAN_BACKUP_ID': '123456789XYZ',
            'BARMAN_ERROR': '',
            'BARMAN_STATUS': 'OK',
            'BARMAN_PREVIOUS_ID': '',
        }
        script.run()
        assert command_mock.call_count == 1
        assert command_mock.call_args[1]['env_append'] == expected_env

    @patch('barman.hooks.Command')
    def test_wal_info(self, command_mock):
        # BackupManager mock
        backup_manager = self.build_backup_manager('test_server', 'test_file')
        backup_manager.config.pre_test_hook = 'not_existent_script'

        # WalFileInfo mock
        wal_info = MagicMock(name='wal_info')
        wal_info.name = 'XXYYZZAABBCC'
        wal_info.size = 1234567
        wal_info.time = 1337133713
        wal_info.compression = 'gzip'
        wal_info.fullpath.return_value = '/incoming/directory'

        # the actual test
        script = HookScriptRunner(backup_manager, 'test_hook', 'pre')
        script.env_from_wal_info(wal_info)
        expected_env = {
            'BARMAN_PHASE': 'pre',
            'BARMAN_VERSION': version,
            'BARMAN_SERVER': 'test_server',
            'BARMAN_CONFIGURATION': 'test_file',
            'BARMAN_HOOK': 'test_hook',
            'BARMAN_SEGMENT': 'XXYYZZAABBCC',
            'BARMAN_FILE': '/incoming/directory',
            'BARMAN_SIZE': '1234567',
            'BARMAN_TIMESTAMP': '1337133713',
            'BARMAN_COMPRESSION': 'gzip',
        }
        script.run()
        assert command_mock.call_count == 1
        assert command_mock.call_args[1]['env_append'] == expected_env

    @patch('barman.hooks.Command')
    def test_wal_info_corner_cases(self, command_mock):
        # BackupManager mock
        backup_manager = self.build_backup_manager('test_server', 'test_file')
        backup_manager.config.pre_test_hook = 'not_existent_script'

        # WalFileInfo mock
        timestamp = time.time()
        wal_info = MagicMock(name='wal_info')
        wal_info.name = 'XXYYZZAABBCC'
        wal_info.size = 1234567
        wal_info.time = timestamp
        wal_info.compression = None
        wal_info.fullpath.return_value = '/incoming/directory'

        # the actual test
        script = HookScriptRunner(backup_manager, 'test_hook', 'pre')
        script.env_from_wal_info(wal_info)
        expected_env = {
            'BARMAN_PHASE': 'pre',
            'BARMAN_VERSION': version,
            'BARMAN_SERVER': 'test_server',
            'BARMAN_CONFIGURATION': 'test_file',
            'BARMAN_HOOK': 'test_hook',
            'BARMAN_SEGMENT': 'XXYYZZAABBCC',
            'BARMAN_FILE': '/incoming/directory',
            'BARMAN_SIZE': '1234567',
            'BARMAN_TIMESTAMP': str(timestamp),
            'BARMAN_COMPRESSION': '',
        }
        script.run()
        assert command_mock.call_count == 1
        assert command_mock.call_args[1]['env_append'] == expected_env


if __name__ == '__main__':
    unittest.main()
