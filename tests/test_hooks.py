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

import time

import pytest
from mock import MagicMock, patch

from barman.hooks import (AbortedRetryHookScript, HookScriptRunner,
                          RetryHookScriptRunner)
from barman.infofile import UnknownBackupIdException
from barman.version import __version__ as version
from testing_helpers import build_backup_manager


class TestHooks(object):

    @patch('barman.hooks.Command')
    def test_general(self, command_mock):
        # BackupManager mock
        backup_manager = build_backup_manager(name='test_server')
        backup_manager.config.pre_test_hook = 'not_existent_script'

        # Command mock executed by HookScriptRunner
        command_mock.return_value.return_value = 0

        # the actual test
        script = HookScriptRunner(backup_manager, 'test_hook', 'pre')
        expected_env = {
            'BARMAN_PHASE': 'pre',
            'BARMAN_VERSION': version,
            'BARMAN_SERVER': 'test_server',
            'BARMAN_CONFIGURATION': 'build_config_from_dicts',
            'BARMAN_HOOK': 'test_hook',
            'BARMAN_RETRY': '0',
        }
        assert script.run() == 0
        assert command_mock.call_count == 1
        assert command_mock.call_args[1]['env_append'] == expected_env

    @patch('barman.hooks.Command')
    def test_general_error(self, command_mock):
        # BackupManager mock
        backup_manager = build_backup_manager(name='test_server')
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
            'BARMAN_CONFIGURATION': 'build_config_from_dicts',
            'BARMAN_HOOK': 'test_hook',
            'BARMAN_ERROR': 'Generic Failure',
            'BARMAN_RETRY': '0',
        }
        assert script.run() == 0
        assert command_mock.call_count == 1
        assert command_mock.call_args[1]['env_append'] == expected_env

    @patch('barman.hooks.Command')
    def test_general_no_phase(self, command_mock):
        # BackupManager mock
        backup_manager = build_backup_manager(name='test_server')
        backup_manager.config.test_hook = 'not_existent_script'

        # Command mock executed by HookScriptRunner
        command_mock.return_value.return_value = 0

        # the actual test
        script = HookScriptRunner(backup_manager, 'test_hook')
        expected_env = {
            'BARMAN_VERSION': version,
            'BARMAN_SERVER': 'test_server',
            'BARMAN_CONFIGURATION': 'build_config_from_dicts',
            'BARMAN_HOOK': 'test_hook',
            'BARMAN_RETRY': '0',
        }
        assert script.run() == 0
        assert command_mock.call_count == 1
        assert command_mock.call_args[1]['env_append'] == expected_env

    @patch('barman.hooks.Command')
    def test_missing_config(self, command_mock):
        # BackupManager mock
        backup_manager = build_backup_manager(name='test_server')

        # Make sure 'pre_test_hook' doesn't exists in configuration
        # (it should not happen)
        if hasattr(backup_manager.config, 'pre_test_hook'):
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
        backup_manager = build_backup_manager(name='test_server')
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
        backup_manager = build_backup_manager(name='test_server')
        backup_manager.config.pre_test_hook = 'not_existent_script'
        backup_manager.get_previous_backup = MagicMock()
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
            'BARMAN_CONFIGURATION': 'build_config_from_dicts',
            'BARMAN_HOOK': 'test_hook',
            'BARMAN_BACKUP_DIR': 'backup_directory',
            'BARMAN_BACKUP_ID': '123456789XYZ',
            'BARMAN_ERROR': '',
            'BARMAN_STATUS': 'OK',
            'BARMAN_PREVIOUS_ID': '987654321',
            'BARMAN_RETRY': '0',
        }
        script.run()
        assert command_mock.call_count == 1
        assert command_mock.call_args[1]['env_append'] == expected_env

    @patch('barman.hooks.Command')
    def test_backup_info_corner_cases(self, command_mock):
        # BackupManager mock
        backup_manager = build_backup_manager(name='test_server')
        backup_manager.config.post_test_hook = 'not_existent_script'
        backup_manager.get_previous_backup = MagicMock()
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
            'BARMAN_CONFIGURATION': 'build_config_from_dicts',
            'BARMAN_HOOK': 'test_hook',
            'BARMAN_BACKUP_DIR': 'backup_directory',
            'BARMAN_BACKUP_ID': '123456789XYZ',
            'BARMAN_ERROR': 'Test error',
            'BARMAN_STATUS': 'FAILED',
            'BARMAN_PREVIOUS_ID': '',
            'BARMAN_RETRY': '0',
        }
        script.run()
        assert command_mock.call_count == 1
        assert command_mock.call_args[1]['env_append'] == expected_env

    @patch('barman.hooks.Command')
    def test_backup_info_exception(self, command_mock):
        # BackupManager mock
        backup_manager = build_backup_manager(name='test_server')
        backup_manager.config.pre_test_hook = 'not_existent_script'
        backup_manager.get_previous_backup = MagicMock()
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
            'BARMAN_CONFIGURATION': 'build_config_from_dicts',
            'BARMAN_HOOK': 'test_hook',
            'BARMAN_BACKUP_DIR': 'backup_directory',
            'BARMAN_BACKUP_ID': '123456789XYZ',
            'BARMAN_ERROR': '',
            'BARMAN_STATUS': 'OK',
            'BARMAN_PREVIOUS_ID': '',
            'BARMAN_RETRY': '0',
        }
        script.run()
        assert command_mock.call_count == 1
        assert command_mock.call_args[1]['env_append'] == expected_env

    @patch('barman.hooks.Command')
    def test_wal_info(self, command_mock):
        # BackupManager mock
        backup_manager = build_backup_manager(name='test_server')
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
            'BARMAN_CONFIGURATION': 'build_config_from_dicts',
            'BARMAN_HOOK': 'test_hook',
            'BARMAN_SEGMENT': 'XXYYZZAABBCC',
            'BARMAN_FILE': '/incoming/directory',
            'BARMAN_SIZE': '1234567',
            'BARMAN_TIMESTAMP': '1337133713',
            'BARMAN_COMPRESSION': 'gzip',
            'BARMAN_RETRY': '0',
            'BARMAN_ERROR': '',
        }
        script.run()
        assert command_mock.call_count == 1
        assert command_mock.call_args[1]['env_append'] == expected_env

    @patch('barman.hooks.Command')
    def test_wal_info_corner_cases(self, command_mock):
        # BackupManager mock
        backup_manager = build_backup_manager(name='test_server')
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
        script.env_from_wal_info(wal_info, '/somewhere', Exception('BOOM!'))
        expected_env = {
            'BARMAN_PHASE': 'pre',
            'BARMAN_VERSION': version,
            'BARMAN_SERVER': 'test_server',
            'BARMAN_CONFIGURATION': 'build_config_from_dicts',
            'BARMAN_HOOK': 'test_hook',
            'BARMAN_SEGMENT': 'XXYYZZAABBCC',
            'BARMAN_FILE': '/somewhere',
            'BARMAN_SIZE': '1234567',
            'BARMAN_TIMESTAMP': str(timestamp),
            'BARMAN_COMPRESSION': '',
            'BARMAN_RETRY': '0',
            'BARMAN_ERROR': 'BOOM!',
        }
        script.run()
        assert command_mock.call_count == 1
        assert command_mock.call_args[1]['env_append'] == expected_env

    @patch('barman.hooks.time.sleep')
    @patch('barman.hooks.Command')
    def test_retry_hooks(self, command_mock, sleep_mock):
        # BackupManager mock
        backup_manager = build_backup_manager(name='test_server')
        backup_manager.config.pre_test_retry_hook = 'not_existent_script'

        # Command mock executed by HookScriptRunner
        command_mock.return_value.return_value = 0

        # the actual test
        script = RetryHookScriptRunner(backup_manager, 'test_retry_hook',
                                       'pre')
        expected_env = {
            'BARMAN_PHASE': 'pre',
            'BARMAN_VERSION': version,
            'BARMAN_SERVER': 'test_server',
            'BARMAN_CONFIGURATION': 'build_config_from_dicts',
            'BARMAN_HOOK': 'test_retry_hook',
            'BARMAN_RETRY': '1',
        }
        assert script.run() == 0
        assert command_mock.call_count == 1
        assert command_mock.call_args[1]['env_append'] == expected_env

    @patch('barman.hooks.time.sleep')
    @patch('barman.hooks.Command')
    def test_retry_hooks_with_retry(self, command_mock, sleep_mock):
        # BackupManager mock
        backup_manager = build_backup_manager(name='test_server')
        backup_manager.config.pre_test_retry_hook = 'not_existent_script'

        # Command mock executed by HookScriptRunner
        command_mock.return_value.side_effect = [
            1, 1, 1, RetryHookScriptRunner.EXIT_SUCCESS]

        # the actual test
        script = RetryHookScriptRunner(backup_manager, 'test_retry_hook',
                                       'pre')
        expected_env = {
            'BARMAN_PHASE': 'pre',
            'BARMAN_VERSION': version,
            'BARMAN_SERVER': 'test_server',
            'BARMAN_CONFIGURATION': 'build_config_from_dicts',
            'BARMAN_HOOK': 'test_retry_hook',
            'BARMAN_RETRY': '1',
        }
        # Shorten wait time after failures
        script.ATTEMPTS_BEFORE_NAP = 2
        script.BREAK_TIME = 1
        script.NAP_TIME = 1
        assert script.run() == RetryHookScriptRunner.EXIT_SUCCESS
        assert command_mock.call_count == 4
        assert command_mock.call_args[1]['env_append'] == expected_env
        command_mock.reset_mock()
        # Command mock executed by HookScriptRunner
        command_mock.return_value.side_effect = [
            1, 2, 3, 4, 5, 6, RetryHookScriptRunner.EXIT_ABORT_CONTINUE]

        # the actual test
        script = RetryHookScriptRunner(backup_manager, 'test_retry_hook',
                                       'pre')
        expected_env = {
            'BARMAN_PHASE': 'pre',
            'BARMAN_VERSION': version,
            'BARMAN_SERVER': 'test_server',
            'BARMAN_CONFIGURATION': 'build_config_from_dicts',
            'BARMAN_HOOK': 'test_retry_hook',
            'BARMAN_RETRY': '1',
        }
        # Shorten wait time after failures
        script.ATTEMPTS_BEFORE_NAP = 2
        script.BREAK_TIME = 1
        script.NAP_TIME = 1
        assert script.run() == RetryHookScriptRunner.EXIT_ABORT_CONTINUE
        assert command_mock.call_count == 7
        assert command_mock.call_args[1]['env_append'] == expected_env

    @patch('barman.hooks.time.sleep')
    @patch('barman.hooks.Command')
    def test_retry_hook_abort(self, command_mock, sleep_mock):
        # BackupManager mock
        backup_manager = build_backup_manager(name='test_server')
        backup_manager.config.pre_test_retry_hook = 'not_existent_script'

        # Command mock executed by HookScriptRunner
        command_mock.return_value.return_value = \
            RetryHookScriptRunner.EXIT_ABORT_STOP

        # the actual test
        script = RetryHookScriptRunner(backup_manager, 'test_retry_hook',
                                       'pre')
        with pytest.raises(AbortedRetryHookScript) as excinfo:
            assert script.run() == RetryHookScriptRunner.EXIT_ABORT_STOP
        assert str(excinfo.value) == \
            "Abort 'pre_test_retry_hook' retry hook script " \
            "(not_existent_script, exit code: 63)"
