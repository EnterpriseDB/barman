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
This module contains the logic to run hook scripts
"""

import json
import logging
import time

from barman import version
from barman.command_wrappers import Command
from barman.exceptions import AbortedRetryHookScript, UnknownBackupIdException

_logger = logging.getLogger(__name__)


class HookScriptRunner(object):
    def __init__(self, backup_manager, name, phase=None, error=None,
                 retry=False, **extra_env):
        """
        Execute a hook script managing its environment
        """
        self.backup_manager = backup_manager
        self.name = name
        self.extra_env = extra_env
        self.phase = phase
        self.error = error
        self.retry = retry

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
            'BARMAN_RETRY': str(1 if self.retry else 0),
        })
        if self.error:
            self.environment['BARMAN_ERROR'] = str(self.error)
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
        try:
            next_backup = self.backup_manager.get_next_backup(
                backup_info.backup_id)
            if next_backup:
                next_backup_id = next_backup.backup_id
            else:
                next_backup_id = ''
        except UnknownBackupIdException:
            next_backup_id = ''
        self.environment.update({
            'BARMAN_BACKUP_DIR': backup_info.get_basebackup_directory(),
            'BARMAN_BACKUP_ID': backup_info.backup_id,
            'BARMAN_PREVIOUS_ID': previous_backup_id,
            'BARMAN_NEXT_ID': next_backup_id,
            'BARMAN_STATUS': backup_info.status,
            'BARMAN_ERROR': backup_info.error or '',
        })

    def env_from_wal_info(self, wal_info, full_path=None, error=None):
        """
        Prepare the environment for executing a script

        :param WalFileInfo wal_info: the backup metadata
        :param str full_path: override wal_info.fullpath() result
        :param str|Exception error: An error message in case of failure
        """
        self.environment.update({
            'BARMAN_SEGMENT': wal_info.name,
            'BARMAN_FILE': str(full_path if full_path is not None else
                               wal_info.fullpath(self.backup_manager.server)),
            'BARMAN_SIZE': str(wal_info.size),
            'BARMAN_TIMESTAMP': str(wal_info.time),
            'BARMAN_COMPRESSION': wal_info.compression or '',
            'BARMAN_ERROR': str(error or '')
        })

    def env_from_recover(self, backup_info, dest, tablespaces, remote_command,
                         error=None, **kwargs):
        """
        Prepare the environment for executing a script

        :param BackupInfo backup_info: the backup metadata
        :param str dest: the destination directory
        :param dict[str,str]|None tablespaces: a tablespace name -> location
            map (for relocation)
        :param str|None remote_command: default None. The remote command
            to recover the base backup, in case of remote backup.
        :param str|Exception error: An error message in case of failure
        """
        self.env_from_backup_info(backup_info)

        # Prepare a JSON representation of tablespace map
        tablespaces_map = ''
        if tablespaces:
            tablespaces_map = json.dumps(tablespaces, sort_keys=True)

        # Prepare a JSON representation of additional recovery options
        # Skip any empty argument
        kwargs_filtered = dict([(k, v) for k, v in kwargs.items() if v])
        recover_options = ''
        if kwargs_filtered:
            recover_options = json.dumps(kwargs_filtered, sort_keys=True)

        self.environment.update({
            'BARMAN_DESTINATION_DIRECTORY': str(dest),
            'BARMAN_TABLESPACES': tablespaces_map,
            'BARMAN_REMOTE_COMMAND': str(remote_command or ''),
            'BARMAN_RECOVER_OPTIONS': recover_options,
            'BARMAN_ERROR': str(error or '')
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
                    path=self.backup_manager.server.path,
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


class RetryHookScriptRunner(HookScriptRunner):

    """
    A 'retry' hook script is a special kind of hook script that Barman
    tries to run indefinitely until it either returns a SUCCESS or
    ABORT exit code.
    Retry hook scripts are executed immediately before (pre) and after (post)
    the command execution. Standard hook scripts are executed immediately
    before (pre) and after (post) the retry hook scripts.
    """

    # Failed attempts before sleeping for NAP_TIME seconds
    ATTEMPTS_BEFORE_NAP = 5
    # Short break after a failure (in seconds)
    BREAK_TIME = 3
    # Long break (nap, in seconds) after ATTEMPTS_BEFORE_NAP failures
    NAP_TIME = 60
    # ABORT (and STOP) exit code
    EXIT_ABORT_STOP = 63
    # ABORT (and CONTINUE) exit code
    EXIT_ABORT_CONTINUE = 62
    # SUCCESS exit code
    EXIT_SUCCESS = 0

    def __init__(self, backup_manager, name, phase=None, error=None,
                 **extra_env):
        super(RetryHookScriptRunner, self).__init__(
            backup_manager, name, phase, error, retry=True, **extra_env)

    def run(self):
        """
        Run a a 'retry' hook script, if required by configuration.

        Barman will retry to run the script indefinitely until it returns
        a EXIT_SUCCESS, or an EXIT_ABORT_CONTINUE, or an EXIT_ABORT_STOP code.
        There are BREAK_TIME seconds of sleep between every try.
        Every ATTEMPTS_BEFORE_NAP failures, Barman will sleep
        for NAP_TIME seconds.
        """
        # If there is no script, exit
        if self.script is not None:
            # Keep track of the number of attempts
            attempts = 1
            while True:
                # Run the script using the standard hook method (inherited)
                super(RetryHookScriptRunner, self).run()

                # Run the script until it returns EXIT_ABORT_CONTINUE,
                # or an EXIT_ABORT_STOP, or EXIT_SUCCESS
                if self.exit_status in (self.EXIT_ABORT_CONTINUE,
                                        self.EXIT_ABORT_STOP,
                                        self.EXIT_SUCCESS):
                    break

                # Check for the number of attempts
                if attempts <= self.ATTEMPTS_BEFORE_NAP:
                    attempts += 1
                    # Take a short break
                    _logger.debug("Retry again in %d seconds", self.BREAK_TIME)
                    time.sleep(self.BREAK_TIME)
                else:
                    # Reset the attempt number and take a longer nap
                    _logger.debug("Reached %d failures. Take a nap "
                                  "then retry again in %d seconds",
                                  self.ATTEMPTS_BEFORE_NAP,
                                  self.NAP_TIME)
                    attempts = 1
                    time.sleep(self.NAP_TIME)

            # Outside the loop check for the exit code.
            if self.exit_status == self.EXIT_ABORT_CONTINUE:
                # Warn the user if the script exited with EXIT_ABORT_CONTINUE
                # Notify EXIT_ABORT_CONTINUE exit status because success and
                # failures are already managed in the superclass run method
                _logger.warning("%s was aborted (got exit status %d, "
                                "Barman resumes)",
                                self.script,
                                self.exit_status)
            elif self.exit_status == self.EXIT_ABORT_STOP:
                # Log the error and raise AbortedRetryHookScript exception
                _logger.error("%s was aborted (got exit status %d, "
                              "Barman requested to stop)",
                              self.script,
                              self.exit_status)
                raise AbortedRetryHookScript(self)

            return self.exit_status
