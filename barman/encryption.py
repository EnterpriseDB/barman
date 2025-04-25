# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2011-2025
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
This module is responsible to manage the encryption features of Barman
"""

import logging

from barman.command_wrappers import Command, Handler
from barman.exceptions import CommandFailedException, EncryptionCommandException


def get_passphrase_from_command(command):
    """
    Execute a shell command to retrieve a passphrase.

    This function runs the given shell *command*, captures its standard output,
    and returns the value as a :class`bytearray`. It's commonly used to retrieve
    a decryption passphrase in non-interactive workflows.

    :param command: The shell command to execute.
    :type command: str
    :return: The passphrase from the command output.
    :rtype: bytearray
    :raises EncryptionCommandException: If the command fails.
    :raises ValueError: If the command returns a falsy output.
    """
    # Create a logger specifically for the encryption passphrase command.
    # Set its level above CRITICAL to effectively disable all logging from this logger.
    # Also, prevent the logger from propagating messages to ancestor loggers.
    # We do both things to avoid leaking the passphrase through log messages.
    _logger = logging.getLogger("encryption_passphrase_command")
    _logger.setLevel(logging.CRITICAL + 1)
    _logger.propagate = False
    # We set the level as CRITICAL here just because we need to pass some level to the
    # handler. But any level will be ingored, given that the logger is set to a level
    # above CRITICAL.
    silent_handler = Handler(_logger, logging.CRITICAL)
    try:
        # Although the passphrase is expected to be written to stdout, we also silent
        # the stderr output of the command, just in case the command writes something to
        # it by mistake.
        cmd = Command(
            cmd=command,
            shell=True,
            check=True,
            out_handler=silent_handler,
            err_handler=silent_handler,
        )
        out, _ = cmd.get_output()
    except CommandFailedException as e:
        raise EncryptionCommandException(f"Command failed: {e}") from e

    if not out:
        raise ValueError("The command returned an empty passphrase")
    return bytearray(out.encode())
