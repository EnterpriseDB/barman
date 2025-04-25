# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2025
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

import logging

import mock
import pytest

from barman.encryption import get_passphrase_from_command
from barman.exceptions import CommandFailedException, EncryptionCommandException


class TestEncryptionHelperFuncs:

    @mock.patch("barman.encryption.logging.getLogger")
    @mock.patch("barman.encryption.Command.get_output")
    @mock.patch("barman.encryption.Command.__init__")
    @mock.patch("barman.encryption.Handler")
    def test_get_passphrase_from_command_valid_command(
        self,
        mock_handler,
        mock_cmd__init__,
        mock_cmd_get_out,
        mock_get_logger,
        capsys,
        caplog,
    ):
        """
        Test :func:`barman.encryption.get_passphrase_from_command` with valid commands.
        """
        mock_logger = mock.MagicMock()
        mock_handler.return_value = mock.MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_cmd__init__.return_value = None
        passphrase = "valid_passphrase"
        mock_cmd_get_out.return_value = (passphrase, None)

        result = get_passphrase_from_command("echo %s" % passphrase)

        # Logger test
        mock_get_logger.assert_called_once_with("encryption_passphrase_command")
        mock_logger.setLevel.assert_called_once_with(logging.CRITICAL + 1)
        assert mock_logger.propagate is False
        mock_handler.assert_called_once_with(mock_logger, logging.CRITICAL)

        # Command __init__ test
        mock_cmd__init__.assert_called_once_with(
            cmd="echo valid_passphrase",
            shell=True,
            check=True,
            out_handler=mock_handler.return_value,
            err_handler=mock_handler.return_value,
        )

        # Command.get_output() test
        mock_cmd_get_out.assert_called_once_with()

        assert result == bytearray(b"valid_passphrase")

        # Check if there is no leak of the passphrase in the output or logs
        cap = capsys.readouterr()
        all_output = cap.out + cap.err + "".join(caplog.messages)
        assert passphrase not in all_output

    @mock.patch("barman.encryption.Command.get_output")
    @mock.patch("barman.encryption.Command.__init__")
    @mock.patch("barman.encryption.Handler")
    def test_get_passphrase_from_command_raises_value_error(
        self, mock_handler, mock_cmd_init, mock_get_output
    ):
        """
        Test :func:`barman.encryption.get_passphrase_from_command` when the command
        returns an error when an error is found or when the output is falsy.
        """
        mock_cmd_init.return_value = None
        mock_get_output.return_value = ("", "")
        with pytest.raises(
            ValueError, match="The command returned an empty passphrase"
        ):
            get_passphrase_from_command("invalid_command")
        mock_cmd_init.assert_called_once_with(
            cmd="invalid_command",
            shell=True,
            check=True,
            out_handler=mock_handler.return_value,
            err_handler=mock_handler.return_value,
        )
        mock_get_output.assert_called_once_with()

    @mock.patch("barman.encryption.Command.get_output")
    @mock.patch("barman.encryption.Command.__init__")
    @mock.patch("barman.encryption.Handler")
    def test_get_passphrase_from_command_raises_encryption_command_exception(
        self, mock_handler, mock_cmd_init, mock_get_output
    ):
        """
        Test :func:`barman.encryption.get_passphrase_from_command` raises RuntimeError
        when a :exc:`CommandFailedException` is found.
        """

        mock_cmd_init.side_effect = CommandFailedException("Command failed")
        mock_get_output.return_value = None
        with pytest.raises(
            EncryptionCommandException, match="Command failed: Command failed"
        ):
            get_passphrase_from_command("failing_command")
        mock_cmd_init.assert_called_once_with(
            cmd="failing_command",
            shell=True,
            check=True,
            out_handler=mock_handler.return_value,
            err_handler=mock_handler.return_value,
        )
