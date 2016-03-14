# Copyright (C) 2011-2016 2ndQuadrant Italia Srl
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
This module is responsible to manage the compression features of Barman
"""

import bz2
import gzip
import logging
import shutil
from abc import ABCMeta, abstractmethod
from contextlib import closing

from barman.command_wrappers import Command, CommandFailedException
from barman.utils import with_metaclass

_logger = logging.getLogger(__name__)


class CompressionIncompatibility(Exception):
    """
    Exception for compression incompatibility
    """


class CompressionManager(object):
    def __init__(self, config, path):
        """
        Compression manager
        """
        self.config = config
        self.path = path

    def check(self, compression=None):
        """
        This method returns True if the compression specified in the
        configuration file is present in the register, otherwise False
        """
        if not compression:
            compression = self.config.compression
        if compression not in compression_registry:
            return False
        return True

    def get_compressor(self, compression=None):
        """
        Returns a new compressor instance

        :param str compression: Compression name
        """
        if not compression:
            compression = self.config.compression
            # Check if the requested compression mechanism is allowed
        if self.check(compression):
            return compression_registry[compression](
                config=self.config, compression=compression, path=self.path)
        else:
            return None


def identify_compression(filename):
    """
    Try to guess the compression algorithm of a file

    :param filename: the pat of the file to identify
    :rtype: str
    """
    # TODO: manage multiple decompression methods for the same
    # compression algorithm (e.g. what to do when gzip is detected?
    # should we use gzip or pigz?)
    with open(filename, 'rb') as f:
        file_start = f.read(MAGIC_MAX_LENGTH)
    for file_type, cls in sorted(compression_registry.items()):
        if cls.validate(file_start):
            return file_type
    return None


class Compressor(with_metaclass(ABCMeta, object)):
    """
    Base class for all the compressors
    """

    MAGIC = None

    def __init__(self, config, compression, path=None):
        self.config = config
        self.compression = compression
        self.path = path

    @classmethod
    def validate(cls, file_start):
        """
        Guess if the first bytes of a file are compatible with the compression
        implemented by this class

        :param file_start: a binary string representing the first few
            bytes of a file
        :rtype: bool
        """
        return cls.MAGIC and file_start.startswith(cls.MAGIC)

    @abstractmethod
    def compress(self, src, dst):
        """
        Abstract Method for compression method

        :param str src: source file path
        :param str dst: destination file path
        """

    @abstractmethod
    def decompress(self, src, dst):
        """
        Abstract method for decompression method

        :param str src: source file path
        :param str dst: destination file path
        """


class CommandCompressor(Compressor):
    """
    Base class for compressors built on external commands
    """

    def __init__(self, config, compression, path=None):

        super(CommandCompressor, self).__init__(
            config, compression, path)

        self._compress = None
        self._decompress = None

    def compress(self, src, dst):
        """
        Compress using the specific command defined in the sublcass

        :param src: source file to compress
        :param dst: destination of the decompression
        """
        return self._compress(src, dst)

    def decompress(self, src, dst):
        """
        Decompress using the specific command defined in the sublcass

        :param src: source file to decompress
        :param dst: destination of the decompression
        """
        return self._decompress(src, dst)

    def _build_command(self, pipe_command):
        """
        Build the command string and create the actual Command object

        :param pipe_command: the command used to compress/decompress
        :rtype: Command
        """
        command = 'command(){ '
        command += pipe_command
        command += ' > "$2" < "$1"'
        command += ';}; command'
        return Command(command, shell=True, check=True, path=self.path)


class InternalCompressor(Compressor):
    """
    Base class for compressors built on python libraries
    """

    def compress(self, src, dst):
        """
        Compress using the object defined in the sublcass

        :param src: source file to compress
        :param dst: destination of the decompression
        """
        try:
            with open(src, 'rb') as istream:
                with closing(self._compressor(dst)) as ostream:
                    shutil.copyfileobj(istream, ostream)
        except Exception as e:
            # you won't get more information from the compressors anyway
            raise CommandFailedException(dict(ret=None, err=str(e), out=None))
        return 0

    def decompress(self, src, dst):
        """
        Decompress using the object defined in the sublcass

        :param src: source file to decompress
        :param dst: destination of the decompression
        """
        try:
            with closing(self._decompressor(src)) as istream:
                with open(dst, 'wb') as ostream:
                    shutil.copyfileobj(istream, ostream)
        except Exception as e:
            # you won't get more information from the compressors anyway
            raise CommandFailedException(dict(ret=None, err=str(e), out=None))
        return 0

    @abstractmethod
    def _decompressor(self, src):
        """
        Abstract decompressor factory method

        :param src: source file path
        :return: a file-like readable decompressor object
        """

    @abstractmethod
    def _compressor(self, dst):
        """
        Abstract compressor factory method

        :param dst: destination file path
        :return: a file-like writable compressor object
        """


class GZipCompressor(CommandCompressor):
    """
    Predefined compressor with GZip
    """

    MAGIC = b'\x1f\x8b\x08'

    def __init__(self, config, compression, path=None):
        super(GZipCompressor, self).__init__(
            config, compression, path)
        self._compress = self._build_command('gzip -c')
        self._decompress = self._build_command('gzip -c -d')


class PyGZipCompressor(InternalCompressor):
    """
    Predefined compressor that uses GZip Python libraries
    """

    MAGIC = b'\x1f\x8b\x08'

    def __init__(self, config, compression, path=None):
        super(PyGZipCompressor, self).__init__(
            config, compression, path)

        # Default compression level used in system gzip utility
        self._level = -1  # Z_DEFAULT_COMPRESSION constant of zlib

    def _compressor(self, name):
        return gzip.GzipFile(name, mode='wb', compresslevel=self._level)

    def _decompressor(self, name):
        return gzip.GzipFile(name, mode='rb')


class PigzCompressor(CommandCompressor):
    """
    Predefined compressor with Pigz

    Note that pigz on-disk is the same as gzip, so the MAGIC value of this
    class is the same
    """

    MAGIC = b'\x1f\x8b\x08'

    def __init__(self, config, compression, path=None):
        super(PigzCompressor, self).__init__(
            config, compression, path)
        self._compress = self._build_command('pigz -c')
        self._decompress = self._build_command('pigz -c -d')


class BZip2Compressor(CommandCompressor):
    """
    Predefined compressor with BZip2
    """

    MAGIC = b'\x42\x5a\x68'

    def __init__(self, config, compression, path=None):
        super(BZip2Compressor, self).__init__(
            config, compression, path)
        self._compress = self._build_command('bzip2 -c')
        self._decompress = self._build_command('bzip2 -c -d')


class PyBZip2Compressor(InternalCompressor):
    """
    Predefined compressor with BZip2 Python libraries
    """

    MAGIC = b'\x42\x5a\x68'

    def __init__(self, config, compression, path=None):
        super(PyBZip2Compressor, self).__init__(
            config, compression, path)

        # Default compression level used in system gzip utility
        self._level = 9

    def _compressor(self, name):
        return bz2.BZ2File(name, mode='wb', compresslevel=self._level)

    def _decompressor(self, name):
        return bz2.BZ2File(name, mode='rb')


class CustomCompressor(CommandCompressor):
    """
    Custom compressor
    """

    def __init__(self, config, compression, path=None):
        if not config.custom_compression_filter:
            raise CompressionIncompatibility("custom_compression_filter")
        if not config.custom_decompression_filter:
            raise CompressionIncompatibility("custom_decompression_filter")

        super(CustomCompressor, self).__init__(
            config, compression, path)
        self._compress = self._build_command(
            config.custom_compression_filter)
        self._decompress = self._build_command(
            config.custom_decompression_filter)


# a dictionary mapping all supported compression schema
# to the class implementing it
# WARNING: items in this dictionary are extracted using alphabetical order
# It's important that gzip and bzip2 are positioned before their variants
compression_registry = {
    'gzip': GZipCompressor,
    'pigz': PigzCompressor,
    'bzip2': BZip2Compressor,
    'pygzip': PyGZipCompressor,
    'pybzip2': PyBZip2Compressor,
    'custom': CustomCompressor,
}

#: The longest string needed to identify a compression schema
MAGIC_MAX_LENGTH = max(len(x.MAGIC or '')
                       for x in compression_registry.values())
