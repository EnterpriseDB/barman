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
from unittest.mock import patch

import mock
import pytest
from mock import MagicMock, Mock

from barman.encryption import (
    EncryptionManager,
    GPGEncryption,
    get_passphrase_from_command,
)
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


class TestGPGEncryption:
    """Test GPG encryption"""

    @patch("barman.encryption.GPG")
    def test_encrypt(self, mock_gpg):
        # GIVEN a file and a destination directory
        file = "path/to/a/file"
        dest = "path/to/destination"

        # Initialize the encryptor
        encryptor = GPGEncryption(key_id="test-key-id")

        # WHEN called without setting `preserve_filename`
        encryptor.encrypt(file, dest)

        # THEN the encrypted file is saved in the destination directory with `.gpg`
        output_file = "path/to/destination/file.gpg"
        mock_gpg.assert_called_once_with(
            action="encrypt",
            recipient="test-key-id",
            input_filepath=file,
            output_filepath=output_file,
            path=encryptor.path,
        )
        mock_gpg.return_value.assert_called_once()

        # Resets the mock
        mock_gpg.reset_mock()

        # WHEN called setting `preserve_filename` as `True`
        encryptor.encrypt(file, dest, preserve_filename=True)

        # THEN the encrypted file is saved in the destination directory with `.gpg`
        output_file = "path/to/destination/file"
        mock_gpg.assert_called_once_with(
            action="encrypt",
            recipient="test-key-id",
            input_filepath=file,
            output_filepath=output_file,
            path=encryptor.path,
        )
        mock_gpg.return_value.assert_called_once()

    @patch("barman.encryption.GPG")
    def test_decrypt(self, mock_gpg):
        """
        Test the :meth:`decrypt` method of the :class:`GPGEncryption` class.
        This test verifies that:
        - The `decrypt` method correctly initializes the GPG decryption process
            with the provided source file, destination file, and passphrase.
        - The GPG instance's :meth:`execute` method is called with the correct
            passphrase.
        Mocks:
        - `mock_gpg`: Mock object for the GPG instance to simulate GPG behavior.
        Assertions:
        - The GPG decryption process is invoked with the expected parameters.
        - The :meth:`execute` method of the GPG instance is called with the correct
          passphrase.
        """
        src = "/path/to/SOMEFILE.gpg"
        dst = "/path/to/decrypted_files"
        passphrase = b"dummy_passphrase"

        # Mock GPG behavior
        mock_gpg_instance = MagicMock()
        mock_gpg.return_value = mock_gpg_instance

        # Initialize GPGEncryption and call decrypt
        encryption = GPGEncryption()
        output = encryption.decrypt(src, dst, passphrase=passphrase)

        # Assert GPG was called with the correct parameters
        mock_gpg.assert_called_once_with(
            action="decrypt",
            input_filepath=src,
            output_filepath=dst + "/SOMEFILE",
            path=encryption.path,
        )
        mock_gpg_instance.assert_called_once_with(stdin=passphrase)
        assert output == dst + "/SOMEFILE"

        mock_gpg.reset_mock()
        mock_gpg_instance.reset_mock()
        mock_gpg.return_value = mock_gpg_instance

        src = "/path/to/SOMEFILE_NO_EXTENSION"
        dst = "/path/to/decrypted_files"
        passphrase = b"dummy_passphrase"
        output = encryption.decrypt(src, dst, passphrase=passphrase)

        mock_gpg.assert_called_once_with(
            action="decrypt",
            input_filepath=src,
            output_filepath=dst + "/SOMEFILE_NO_EXTENSION",
            path=encryption.path,
        )
        mock_gpg_instance.assert_called_once_with(stdin=passphrase)
        assert output == dst + "/SOMEFILE_NO_EXTENSION"

    @patch("barman.encryption.GPG")
    def test_decrypt_raise_exceptions(self, mock_gpg):
        """
        Test the :meth:`decrypt` method of the :class:`GPGEncryption` class.
        This test verifies that:
        - The `decrypt` method correctly initializes the GPG decryption process
            with the provided source file, destination file, and passphrase.
        - The GPG instance's :meth:`execute` method is called with the correct
            passphrase.
        Mocks:
        - `mock_gpg`: Mock object for the GPG instance to simulate GPG behavior.
        Assertions:
        - The GPG decryption process is invoked with the expected parameters.
        - The :meth:`execute` method of the GPG instance is called with the correct
          passphrase.
        """
        src = "/path/to/SOMEFILE.gpg"
        dst = "/path/to/decrypted_files"
        passphrase = b""

        # Mock GPG behavior
        mock_gpg_instance = MagicMock()
        mock_gpg.return_value = mock_gpg_instance
        mock_gpg_instance.side_effect = CommandFailedException("No passphrase given")
        # Initialize GPGEncryption and call decrypt
        encryption = GPGEncryption()
        with pytest.raises(
            ValueError, match="Error: No passphrase provided for decryption."
        ):
            _ = encryption.decrypt(src, dst, passphrase=passphrase)

        # Assert GPG was called with the correct parameters
        mock_gpg.assert_called_once_with(
            action="decrypt",
            input_filepath=src,
            output_filepath=dst + "/SOMEFILE",
            path=encryption.path,
        )
        mock_gpg_instance.assert_called_once_with(stdin=passphrase)

        mock_gpg.reset_mock()
        mock_gpg_instance.reset_mock()
        mock_gpg_instance.side_effect = CommandFailedException("Bad passphrase")
        with pytest.raises(
            ValueError, match="Error: Bad passphrase provided for decryption."
        ):
            _ = encryption.decrypt(src, dst, passphrase=passphrase)

        # Assert GPG was called with the correct parameters
        mock_gpg.assert_called_once_with(
            action="decrypt",
            input_filepath=src,
            output_filepath=dst + "/SOMEFILE",
            path=encryption.path,
        )
        mock_gpg_instance.assert_called_once_with(stdin=passphrase)

        mock_gpg.reset_mock()
        mock_gpg_instance.reset_mock()
        mock_gpg_instance.side_effect = CommandFailedException(
            "ANY OTHER ERROR MESSAGE"
        )
        with pytest.raises(CommandFailedException, match="ANY OTHER ERROR MESSAGE"):
            _ = encryption.decrypt(src, dst, passphrase=passphrase)

        # Assert GPG was called with the correct parameters
        mock_gpg.assert_called_once_with(
            action="decrypt",
            input_filepath=src,
            output_filepath=dst + "/SOMEFILE",
            path=encryption.path,
        )
        mock_gpg_instance.assert_called_once_with(stdin=passphrase)


class TestEncryptionManager:
    """Test EncryptionManager"""

    def test_validate_config(self):
        """
        Assert that the ``validate_config`` method calls the respective validator.
        """
        # Case 1: an unknown encryption option in the configuration is given
        mock_config = Mock(encryption="invalid_option")
        with pytest.raises(
            ValueError, match="Invalid encryption option: invalid_option"
        ):
            EncryptionManager(mock_config).validate_config()

        # Case 2: a valid configuration is given, assert it calls its validator
        mock_config = Mock(encryption="gpg")
        with patch(
            "barman.encryption.EncryptionManager._validate_gpg"
        ) as mock_validator:
            EncryptionManager(mock_config).validate_config()
            mock_validator.assert_called_once()

    def test_get_encryption(self):
        """
        Assert that the ``get_encryption`` method calls the respective initializer.
        """
        mock_config = Mock(encryption="gpg")
        with patch(
            "barman.encryption.EncryptionManager._initialize_gpg"
        ) as mock_validator:
            EncryptionManager(mock_config).get_encryption()
            mock_validator.assert_called_once()

    @patch("barman.encryption.GPGEncryption", return_value=Mock())
    def test_initialize_gpg(self, mock_gpg_encryption):
        """
        Assert that the GPG inializer initialize the respective class correctly.
        """
        # Case 1: GIVEN a valid configuration for GPG encryption
        mock_config = Mock(
            encryption="gpg",
            encryption_key_id="5C55714E386324A9F2B35F647E29F",
        )
        # WHEN calling get_encryption
        encryption_instance = EncryptionManager(
            mock_config, path="/a/random/path"
        ).get_encryption()
        # THEN the correct encryption instance is returned
        mock_gpg_encryption.assert_called_once_with(
            mock_config.encryption_key_id, path="/a/random/path"
        )
        assert encryption_instance == mock_gpg_encryption.return_value

    def test_validate_gpg(self):
        """
        Assert that the GPG validations against the configuartion works correctly.
        """
        # Case 1: GIVEN a configuration without an encryption key ID
        mock_config = Mock(
            encryption="gpg",
            encryption_key_id=None,
            backup_method="postgres",
            backup_compression="none",
            backup_compression_format="tar",
        )
        # WHEN calling _validate_gpg, the appropriate exception is raised
        with pytest.raises(
            ValueError, match="Encryption is set as gpg, but encryption_key_id is unset"
        ):
            EncryptionManager(mock_config)._validate_gpg()

        # Case 2: GIVEN a configuration with an invalid backup_method
        mock_config = Mock(
            encryption="gpg",
            encryption_key_id="5C55714E386324A9F2B35F647E29F",
            backup_method="rsync",
            backup_compression="none",
            backup_compression_format="tar",
        )
        # WHEN calling _validate_gpg, the appropriate exception is raised
        with pytest.raises(
            ValueError,
            match="Encryption is set as gpg, but backup_method != postgres",
        ):
            EncryptionManager(mock_config)._validate_gpg()

        # Case 3: GIVEN a configuration without a backup_compression
        mock_config = Mock(
            encryption="gpg",
            encryption_key_id="5C55714E386324A9F2B35F647E29F",
            backup_method="postgres",
            backup_compression=None,
            backup_compression_format="tar",
        )
        # WHEN calling _validate_gpg, the appropriate exception is raised
        with pytest.raises(
            ValueError,
            match="Encryption is set as gpg, but backup_compression is unset",
        ):
            EncryptionManager(mock_config)._validate_gpg()

        # Case 4: GIVEN a configuration with an invalid backup_compression_format
        mock_config = Mock(
            encryption="gpg",
            encryption_key_id="5C55714E386324A9F2B35F647E29F",
            backup_method="postgres",
            backup_compression="none",
            backup_compression_format="plain",
        )
        # WHEN calling _validate_gpg, the appropriate exception is raised
        with pytest.raises(
            ValueError,
            match="Encryption is set as gpg, but backup_compression_format != tar",
        ):
            EncryptionManager(mock_config)._validate_gpg()

        # Case 5: GIVEN a correct configuration
        mock_config = Mock(
            encryption="gpg",
            encryption_key_id="5C55714E386324A9F2B35F647E29F",
            backup_method="postgres",
            backup_compression="none",
            backup_compression_format="tar",
        )
        # THEN no exception is raised
        EncryptionManager(mock_config)._validate_gpg()
