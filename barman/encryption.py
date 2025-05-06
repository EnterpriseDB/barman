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
import os
import subprocess
from abc import ABC, abstractmethod

from barman.command_wrappers import GPG, Command, Handler
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


class Encryption(ABC):
    """
    Abstract class for handling encryption.

    :cvar NAME: The name of the encryption
    """

    NAME = None

    def __init__(self, path=None):
        """
        Constructor.

        :param None|str path: An optional path to prepend to the system ``PATH`` when
            locating binaries.
        """
        self.path = path

    @abstractmethod
    def encrypt(self, file, dest):
        """
        Encrypts a given *file*.

        :param str file: The full path to the file to be encrypted
        :param str dest: The destination directory for the encrypted file
        :returns str: The path to the encrypted file
        """
        pass

    @abstractmethod
    def decrypt(self, file, dest, **kwargs):
        """
        Decrypts a given *file*.

        :param str file: The full path to the file to be decrypted.
        :param str dest: The destination directory for the decrypted file.
        :returns str: The path to the decrypted file.
        """
        pass

    @staticmethod
    @abstractmethod
    def recognize_encryption(filename):
        """
        Check if a file is encrypted with the class' encryption algorithm.

        :param str filename: The path to the file to be checked
        :returns bool: ``True`` if the encryption type is recognized, ``False``
            otherwise
        """


class GPGEncryption(Encryption):
    """
    Implements the GPG encryption and decryption logic.

    :cvar NAME: The name of the encryption
    """

    NAME = "gpg"

    def __init__(self, key_id=None, path=None):
        """
        Initialize a :class:`GPGEncryption` instance.

        .. note::
            If encrypting, a GPG key ID is required and is used throughout
            the instance's lifetime.

        :param None|str key_id: A valid key ID of an existing GPG key available in the
            system. Only used for encryption.
        :param None|str path: An optional path to prepend to the system ``PATH`` when
            locating GPG binaries
        """
        super(GPGEncryption, self).__init__(path)
        self.key_id = key_id

    def encrypt(self, file, dest):
        dest_filename = os.path.basename(file) + ".gpg"
        output = os.path.join(dest, dest_filename)
        gpg = GPG(
            action="encrypt",
            recipient=self.key_id,
            input_filepath=file,
            output_filepath=output,
            path=self.path,
        )
        gpg()
        return output

    def decrypt(self, file, dest, **kwargs):
        """
        Decrypts a *file* using GPG and a provided passphrase.

        This method uses GPG to decrypt a given *file* and output the decrypted file
        under the *dest* directory. The decryption process requires a valid passphrase,
        which is given through the *passphrase* keyworded argument. If the decryption
        fails due to an incorrect or missing passphrase, appropriate exceptions are
        raised.

        :param str file: The full path to the file to be decrypted.
        :param str dest: The destination directory for the decrypted file.
        :kwparam bytearray passphrase: The passphrase used to decrypt the file.
        :returns str: The path to the decrypted file.
        :raises ValueError: If no passphrase is provided or if the passphrase is
            incorrect.
        """
        filename = os.path.basename(file)
        # The file may or may not have a .gpg extension -- for example, Barman archives
        # WAL files without the extension, even if the file is encrypted.
        # The decrypted file should not contain the extension, so we remove it, if
        # present.
        if filename.lower().endswith(".gpg"):
            filename, _ = os.path.splitext(filename)
        output = os.path.join(dest, filename)
        gpg_decrypt = GPG(
            action="decrypt",
            input_filepath=file,
            output_filepath=output,
            path=self.path,
        )
        try:
            passphrase = kwargs.get("passphrase")
            gpg_decrypt(stdin=passphrase)
        except CommandFailedException as e:
            if "No passphrase given" in str(e):
                raise ValueError("Error: No passphrase provided for decryption.")
            if "Bad passphrase" in str(e):
                raise ValueError("Error: Bad passphrase provided for decryption.")
            raise e
        return output

    @staticmethod
    def recognize_encryption(filename):
        try:
            process = subprocess.run(
                ["file", "--brief", filename],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                text=True,
            )
            output = process.stdout.upper()
            return "PGP" in output and "ENCRYPTED" in output
        except subprocess.CalledProcessError:
            return False


class EncryptionManager:
    """
    Manager class to validate encryption configuration and initialize instances of
    :class:`barman.encryption.Encryption`.

    :cvar REGISTRY: The registry of available encryption classes. Each key is a
        supported ``config.encryption`` algorithm. The corresponding value is a tuple
        of 3 items: the respective class of the encryption algorithm, a method used
        to validate the ``config`` object for its respective encryption, and
        a method used to instantiate the class used by the algorithm.
    """

    REGISTRY = {"gpg": (GPGEncryption, "_validate_gpg", "_initialize_gpg")}

    def __init__(self, config, path=None):
        """
        Initialize an encryption manager instance.

        :param barman.config.ServerConfig config: A server configuration object
        :param None|str path: An optional path to prepend to the system ``PATH`` when
            locating binaries
        """
        self.config = config
        self.path = path

    def get_encryption(self, encryption=None):
        """
        Get an encryption instance for the requested encryption type.

        :param None|str encryption: The encryption requested. If not passed, falls back
            to ``config.encryption``. This flexibility is useful for cases where
            encryption is disabled midway, i.e. no longer present in ``config``, but an
            encryption instance is still needed, e.g. for decrypting an old backup.
        :returns None|:class:`barman.encryption.Encryption`: A respective encryption
            instance, if *encryption* is set, otherwise ``None``.
        :raises ValueError: If the encryption handler is unknown
        """
        encryption = encryption or self.config.encryption
        entry = self.REGISTRY.get(encryption)
        if entry:
            return getattr(self, entry[2])()
        return None

    def validate_config(self):
        """
        Validate the configuration parameters against the present encryption.

        :raises ValueError: If the configuration is invalid for the present encryption
        """
        entry = self.REGISTRY.get(self.config.encryption)
        if not entry:
            raise ValueError("Invalid encryption option: %s" % self.config.encryption)
        getattr(self, entry[1])()

    def _validate_gpg(self):
        """
        Validate required configuration for GPG encryption.

        :raises ValueError: If the configuration is invalid
        """
        if not self.config.encryption_key_id:
            raise ValueError("Encryption is set as gpg, but encryption_key_id is unset")
        elif self.config.backup_method != "postgres":
            raise ValueError("Encryption is set as gpg, but backup_method != postgres")
        elif not self.config.backup_compression:
            raise ValueError(
                "Encryption is set as gpg, but backup_compression is unset"
            )
        elif self.config.backup_compression_format != "tar":
            raise ValueError(
                "Encryption is set as gpg, but backup_compression_format != tar"
            )

    def _initialize_gpg(self):
        """
        Initialize a GPG encryption instance.

        :returns: barman.encryption.GPGEncryption instance
        """
        return GPGEncryption(self.config.encryption_key_id, path=self.path)

    @classmethod
    def identify_encryption(cls, filename):
        """
        Try to identify the encryption algorithm of a file.
        :param str filename: The path of the file to identify
        :returns: The encryption name, if found
        """
        for klass, _, _ in sorted(cls.REGISTRY.values()):
            if klass.recognize_encryption(filename):
                return klass.NAME
        return None
