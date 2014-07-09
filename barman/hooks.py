# Copyright (C) 2011-2014 2ndQuadrant Italia (Devise.IT S.r.L.)
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
This module contains the logic to run hook scripts
"""

import logging
from barman import version
from barman.command_wrappers import Command
from barman.infofile import UnknownBackupIdException

_logger = logging.getLogger(__name__)


class HookScriptRunner(object):
    def __init__(self, backup_manager, name, phase=None, error=None,
                 **extra_env):
        """
        Execute a hook script managing its environment
        """
        self.backup_manager = backup_manager
        self.name = name
        self.extra_env = extra_env
        self.phase = phase
        self.error = error

        self.environment = None
        self.exit_status = None
        self.exception = None
        self.script = None

        self.reset()

    def reset(self):
        """
        Reset the status of the class.
        """
        self.environment = dict(self.extra_env)
        config_file = self.backup_manager.config.config.config_file
        self.environment.update({
            'BARMAN_VERSION': version.__version__,
            'BARMAN_SERVER': self.backup_manager.config.name,
            'BARMAN_CONFIGURATION': config_file,
            'BARMAN_HOOK': self.name,
        })
        if self.error:
            self.environment['BARMAN_ERROR'] = self.error
        if self.phase:
            self.environment['BARMAN_PHASE'] = self.phase
            script_config_name = "%s_%s" % (self.phase, self.name)
        else:
            script_config_name = self.name
        self.script = getattr(self.backup_manager.config, script_config_name,
                              None)
        self.exit_status = None
        self.exception = None

    def env_from_backup_info(self, backup_info):
        """
        Prepare the environment for executing a script

        :param BackupInfo backup_info: the backup metadata
        """
        try:
            previous_backup = self.backup_manager.get_previous_backup(
                backup_info.backup_id)
            if previous_backup:
                previous_backup_id = previous_backup.backup_id
            else:
                previous_backup_id = ''
        except UnknownBackupIdException:
            previous_backup_id = ''
        self.environment.update({
            'BARMAN_BACKUP_DIR': backup_info.get_basebackup_directory(),
            'BARMAN_BACKUP_ID': backup_info.backup_id,
            'BARMAN_PREVIOUS_ID': previous_backup_id,
            'BARMAN_STATUS': backup_info.status,
            'BARMAN_ERROR': backup_info.error or '',
        })

    def env_from_wal_info(self, wal_info):
        """
        Prepare the environment for executing a script

        :param WalFileInfo wal_info: the backup metadata
        """
        self.environment.update({
            'BARMAN_SEGMENT': wal_info.name,
            'BARMAN_FILE': wal_info.full_path,
            'BARMAN_SIZE': str(wal_info.size),
            'BARMAN_TIMESTAMP': str(wal_info.time),
            'BARMAN_COMPRESSION': wal_info.compression or '',
        })

    def run(self):
        """
        Run a a hook script if configured.
        This method must never throw any exception
        """
        # noinspection PyBroadException
        try:
            if self.script:
                _logger.debug("Attempt to run %s: %s", self.name, self.script)
                cmd = Command(
                    self.script,
                    env_append=self.environment,
                    shell=True, check=False)
                self.exit_status = cmd()
                if self.exit_status != 0:
                    details = "%s returned %d\n" \
                              "Output details:\n" \
                              % (self.script, self.exit_status)
                    details += cmd.out
                    details += cmd.err
                    _logger.warning(details)
                else:
                    _logger.debug("%s returned %d",
                                  self.script,
                                  self.exit_status)
                return self.exit_status
        except Exception as e:
            _logger.exception('Exception running %s', self.name)
            self.exception = e
            return None
