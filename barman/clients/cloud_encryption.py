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


from abc import ABCMeta, abstractmethod

from barman.utils import with_metaclass


class ChunkedEncryptor(with_metaclass(ABCMeta, object)):
    """
    Base class for all ChunkedEncryptors
    """

    @abstractmethod
    def add_chunk(self, data):
        """
        Encrypts the supplied data and returns all the encrypted bytes.

        :param bytes data: The chunk of data to be encrypted
        :return: The encrypted data
        :rtype: bytes
        """

    @abstractmethod
    def close(self):
        """
        Ends the encryption stream and returns closing bytes.

        :return: closing data for the encryption stream
        :rtype: bytes
        """

    @abstractmethod
    def decrypt(self, data):
        """
        Decrypts the supplied chunk of data and returns at least part of the
        unencrypted data.

        If the chunk is the final part of the encrypted data, the signature
        will be checked and an exception will be thrown in case it doesn't match.

        :param bytes data: The chunk of data to be decrypted
        :return: The decrypted data
        :rtype: bytes
        """


class XChaCha20Poly1305Encryptor(ChunkedEncryptor):
    """
    A ChunkedEncryptor implementation based on pycryptodome
    """

    def __init__(self):
        crypto = _try_import_crypto()

    def add_chunk(self, data):
        """
        Encryptes the supplied data and returns all the encrypted bytes.

        :param bytes data: The chunk of data to be encrypted
        :return: The encrypted data
        :rtype: bytes
        """
        return data

    def close(self):
        """
        Ends the encryption stream and returns closing bytes.

        :return: closing data for the encryption stream
        :rtype: bytes
        """
        return b''

    def decrypt(self, data):
        """
        Decompresses the supplied chunk of data and returns at least part of the
        unencrypted data.

        :param bytes data: The chunk of data to be deencrypted
        :return: The deencrypted data
        :rtype: bytes
        """
        return data


def get_encryptor(encryption):
    """
    Helper function which returns a ChunkedEncryptor for the specified encryption
    algorithm.

    :param str encryption: The encryption algorithm to use.
    :return: A ChunkedEncryptor capable of enrypting and decrypting using the
      specified encryption.
    :rtype: ChunkedEncryptor
    """
    if encryption == "XChaCha20-poly1305":
        return XChaCha20Poly1305Encryptor()
    return None


def encrypt(wal_file, encryption):
    """
    Encryptes the supplied *wal_file* and returns a file-like object containing the
    encrypted data.

    :param IOBase wal_file: A file-like object containing the WAL file data.
    :param str encryption: The encryption algorithm to apply. Currently only
      ``XChaCha20-poly1305`` is supported.
    :return: The encrypted data
    :rtype: BytesIO
    """
    encryptor = get_internal_encryptor(encryption, encryption_level)
    return encryptor.compress_in_mem(wal_file)


def decrypts_to_file(blob, dest_file, encryption):
    """
    Decompresses the supplied *blob* of data into the *dest_file* file-like object using
    the specified encryption.

    :param IOBase blob: A file-like object containing the encrypted data.
    :param IOBase dest_file: A file-like object into which the unencrypted data
      should be written.
    :param str encryption: The encryption algorithm to apply.
    :rtype: None
    """
    encryptor = get_internal_encryptor(encryption)
    encryptor.decrypts_to_fileobj(blob, dest_file)
