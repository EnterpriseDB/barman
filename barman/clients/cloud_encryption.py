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

import os
import io
import logging
import json
import base64
import struct

from abc import ABCMeta, abstractmethod

from barman.encryption import _try_import_cryptoCipherChaCha20Poly1305
from barman.utils import with_metaclass

_logger = logging.getLogger(__name__)

class EncryptionHeader:
    """
    The header used for encrypted files

    <magic><headerlength><json header>
    """
    is_valid = False
    magic = b'PGBARMAN'
    headerlength = None
    headerdict = None
    dataoffset = None

    def __init__(self, inbytes):
        """
        Tries to decompose the header from <inbytes>. Assumes the header is completely
        contained in <inbytes>. Use is_valid() to check validity.
        """
        # file should start with magic
        if inbytes[:len(self.magic)] != self.magic:
            _logger.error('Encrypted file does not start with required magic')
        else:
            # get the length of the header
            self.headerlength = struct.unpack_from('<H', inbytes, offset=len(self.magic))[0]
            # extract the header
            headerbytes = inbytes[len(self.magic)+struct.calcsize('<H'):][:self.headerlength]
            if len(headerbytes) != self.headerlength:
                # we couldnt extract the entire header from the byte array
                _logger.error('Supplied byte array does not contain complete encryption header')
            else:
                self.headerdict = json.loads(headerbytes)
                self.dataoffset = len(self.magic) + struct.calcsize('<H') + self.headerlength
                self.is_valid = True

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

    TODO: this design is not clean : it handles decryption too !
    """

    def __init__(self,config):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug('init XChaCha20Poly1305Encryptor')
        self.cryptoCipherChaCha20Poly1305 = _try_import_cryptoCipherChaCha20Poly1305()
        self.encryptionKey = None
        if 'key256b64' in config:
            self.encryptionKey = base64.b64decode(config['key256b64'])
        else:
            raise KeyError('XChaCha20Poly1305Encryptor config must contain a key256b64 key')

        if len(self.encryptionKey) != 32:
            raise ValueError(f'XChaCha20Poly1305Encryptor needs a 256 bit key, got {len(self.encryptionKey)*8} bit')

        self.encryptor = None
        self.decryptor = None

    def add_chunk(self, data):
        """
        Encryptes the supplied data and returns all the encrypted bytes.

        :param bytes data: The chunk of data to be encrypted
        :return: The encrypted data
        :rtype: bytes
        """
        #self._logger.debug(f'add chunk {len(data)}')

        ret = b'' # data to return
        header = { 'cipher': 'XChaCha20-poly1305' }

        # an encrypted file is of the form
        # PGBARMAN<headerlength:UINT16 LE><header in compact json><nonce 256bit><data><hmac>
        if not self.encryptor: # this is the first chunk and encryption hasnt started
            self._logger.info('initializing XChaCha20-poly1305 encryptor')
            magic = 'PGBARMAN'.encode('ascii')
            jsonHeader = json.dumps(header,ensure_ascii=True,separators=(',',':')).encode('ascii')
            headerlength = struct.pack('<H',len(jsonHeader)) # UINT16 LE
            nonce = os.urandom(24)

            self.encryptor = self.cryptoCipherChaCha20Poly1305.new(
                key=self.encryptionKey,
                nonce=nonce)

            ret += magic + headerlength + jsonHeader + nonce
            self.encryptor.update(ret) # this is only the header

        ret += self.encryptor.encrypt(data)

        return ret

    def close(self):
        """
        Ends the encryption stream and returns closing bytes.

        :return: closing data for the encryption stream
        :rtype: bytes
        """
        self._logger.info('writing signature (digest/hmac) to encrypted file')

        return self.encryptor.digest()

    def decrypt(self, data):
        """
        Decompresses the supplied chunk of data and returns at least part of the
        unencrypted data.

        :param bytes data: The chunk of data to be deencrypted
        :return: The deencrypted data
        :rtype: bytes
        """
        ret = b''
        # an encrypted file is of the form
        # PGBARMAN<headerlength:UINT16 LE><header in compact json><nonce 256bit><data><hmac>
        if not self.decryptor: # this is the first chunk and encryption hasnt started
            self._logger.info('initializing XChaCha20-poly1305 decryptor')
            encHeader = EncryptionHeader(data)
            if not encHeader.is_valid:
                raise SystemError('encrypted file header is not valid')
            # get the nonce - 24 bytes after the header
            nonce = data[encHeader.dataoffset:encHeader.dataoffset+24]
            self.decryptor = self.cryptoCipherChaCha20Poly1305.new(
                key=self.encryptionKey,
                nonce=nonce)
            # we need to update with the plain header
            self.decryptor.update(data[:encHeader.dataoffset+24])
            ret = self.decryptor.decrypt(data[encHeader.dataoffset+24:])
        else:
            ret = self.decryptor.decrypt(data)

        return ret

class genericStreamingDecryptor:
    """
    Generic decryptor, morphing into a specific Encryptor when the header is read
    and the cipher is known.
    Implements the write() method for streaming tar usage
    """

    def __init__(self,fd):
      pass

def get_encryptor(encryption):
    """
    Helper function which returns a ChunkedEncryptor for the specified encryption
    algorithm.

    :param dict encryption: the encryption configuration
    :return: A ChunkedEncryptor capable of enrypting and decrypting using the
      specified encryption.
    :rtype: ChunkedEncryptor
    """
    if 'cipher' in encryption:
        if encryption['cipher'] == "XChaCha20-poly1305":
            return XChaCha20Poly1305Encryptor(config=encryption)
    return None


def encrypt(infile, encryption):
    """
    Encryptes the supplied *file-like object* and returns a file-like object containing the
    encrypted data.

    :param IOBase infile: A file-like object containing the WAL file data.
    :param dict encryption: The encryption config
    :return: The encrypted data
    :rtype: BytesIO
    """
    ret = io.BytesIO()
    encryptor = get_encryptor(encryption)
    chunksize = 64*1024
    # do an empty write so the encryption is properly initialized
    # this is necessary so an empty file gets a proper header and hmac
    ret.write(encryptor.add_chunk(b''))
    # start reading data
    chunk = infile.read(chunksize)
    while chunk:
        ret.write(encryptor.add_chunk(chunk))
        chunk = infile.read(chunksize)
    # write closing data
    ret.write(encryptor.close())

    # set the stream for reading
    ret.seek(0)

    return ret


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
