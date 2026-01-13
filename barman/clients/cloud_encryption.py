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

class EncryptionConfiguration:
    """
    Wrapper around the client encryption configuration file.
    Usually /etc/barman/client-encryption.json

    The configuration file is a dict with named profiles. A profile is a dict
    containing a <cipher> and a number of arbitrary keys defining the encryption.

    There must always be a "default" profile which is used for encryption. This
    should be a string pointing to the profile to use.

    Decryption is done with the profile named in the header of the decryption
    stream.

    e.g.
    {
        "default": "XChaCha-january-2026",
        "XChaCha-january-2026": {
            "cipher": "XChaCha-poly1305",
            "key256b64": "..."
            },
        "XChaCha-december-2025": {
            ...
            },
        ...
    }
    """
    # the json that was read
    configuration = None

    def __init__(self, *, filename=None, data=None, fd=None):
        """
        Initialize the configuration with either ( in this order ):
        * filename : open and read the file, convert to dict
        * fd : read from the file-like object, convert to dict
        * data : ascii decode the bytes, convert to dict

        If <filename> is given but the file does not exist, initializes configuration to return
        None for the default profile, effectively turning off encryption.

        :param str filename: the path to open
        :param file-like object fd: stream to read
        :param bytes data: byte array containing the json
        """
        if filename and os.path.exists(filename):
            fd = open(filename, 'rb')
            data = fd.read()
        elif filename: # filename given but does not exist
            _logger.info(f'requested client encryption config "{filename}" does not exist')
            data = b'{"default": "no_encryption", "no_encryption": null}'
        elif fd:
            data = fd.read()

        if data:
            self.configuration = json.loads(data.decode('ascii'))
        else:
            raise ValueError('Initializing EncryptionConfiguratie needs either filename, fd or data')

        if filename and os.path.exists(filename): fd.close()

    def get_profile(self, name):
        """
        Return the profile dict corresponding to the profile name
        """
        if name == 'default':
            name = self.configuration['default']

        ret = self.configuration[name]

        # add the profile name to the returned dict -- is used to add into the encryption header
        if ret:
            ret["_profile_name"] = name

        return ret

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

    @abstractmethod
    def validate_decryption(self):
        """
        Check the signature of the decrypted stream. Returns True of False.
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
        self.config = config
        if 'key256b64' in self.config:
            self.encryptionKey = base64.b64decode(self.config['key256b64'])
        else:
            raise KeyError('XChaCha20Poly1305Encryptor config must contain a key256b64 key')

        if '_profile_name' in self.config:
            self.profile_name = self.config['_profile_name']
        else:
            raise KeyError('XChaCha20Poly1305Encryptor config must contain a _profile_name key')

        if len(self.encryptionKey) != 32:
            raise ValueError(f'XChaCha20Poly1305Encryptor needs a 256 bit key, got {len(self.encryptionKey)*8} bit')

        self.encryptor = None
        self.decryptor = None
        self.final16 = b'' # see the decrypt method for more information

    def add_chunk(self, data):
        """
        Encryptes the supplied data and returns all the encrypted bytes.

        :param bytes data: The chunk of data to be encrypted
        :return: The encrypted data
        :rtype: bytes
        """
        #self._logger.debug(f'add chunk {len(data)}')

        ret = b'' # data to return
        header = { 'cipher': 'XChaCha20-poly1305', 'profile': self.profile_name }

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
            # remove the header from the data
            data = data[encHeader.dataoffset+24:]

        # we don't know it this is the last datachunk, so we keep the last 16 bytes
        # which *could* be the signature.
        # first, add the final16 back to the data - these were clearly not the signature
        data = self.final16 + data
        # take the final 16 from this data, maybe these are the final ones
        self.final16 = data[-16:]
        data = data[:-16]
        # now decrypt the data
        ret = self.decryptor.decrypt(data)

        return ret

    def validate_decryption(self):
        """
        Check if the last 16 bytes of data we put aside, matches the calculated hmac of the stream

        Possible problem : if the stream was used by tarfile, it will not read the complete stream.
        An extreme edgecase would be when the tar data exactly coincides with the end of a chunk.
        In that case, we do not have the final16.
        """
        try:
            self.decryptor.verify(self.final16)
            return True
        except ValueError as e:
            _logger.error(f'ValueError on decryption verify: {str(e)}')
            return False

def get_encryptor(encryption, profile_name='default'):
    """
    Helper function which returns a ChunkedEncryptor for the specified encryption
    algorithm.

    :param EncryptionConfiguration encryption: the encryption configuration
    :return: A ChunkedEncryptor capable of enrypting and decrypting using the
      specified encryption.
    :rtype: ChunkedEncryptor
    """
    profile = encryption.get_profile(profile_name)
    if not profile:
        return None
    if profile['cipher'] == "XChaCha20-poly1305":
        return XChaCha20Poly1305Encryptor(config=profile)
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
    encryptor = get_encryptor(encryption, profile_name='default')
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


def decrypt(infile, outfile, encryption):
    """
    Decrypts from the supplied *infile* into the *outfile* file-like objects using
    the specified encryption config.

    :param IOBase infile: A file-like object containing the encrypted data.
    :param IOBase outfile: A file-like object into which the unencrypted data
      should be written.
    :param str encryption: The available encryption configurations
    :rtype: None
    """
    chunksize = 64*1024
    decryptedStream = DecryptingReadableStreamIO(infile, encryption)
    buf = decryptedStream.read(chunksize)
    while len(buf) > 0:
        outfile.write(buf)
        decryptedStream = DecryptingReadableStreamIO(infile, encryption)

