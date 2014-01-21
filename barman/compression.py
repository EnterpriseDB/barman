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
This module is responsible to manage the compression features of Barman
"""

from barman.command_wrappers import Command

import logging

_logger = logging.getLogger(__name__)


class CompressionIncompatibility(Exception):
    """
    Exception for compression incompatibility
    """


class CompressionManager(object):
    def __init__(self, config):
        """
        Compression manager
        """
        self.config = config

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

    def get_compressor(self, remove_origin=False, debug=False,
                       compression=None):
        """
        Returns a new compressor instance
        """
        if not compression:
            compression = self.config.compression
            # Check if the requested compression mechanism is allowed
        if self.check(compression):
            return compression_registry[compression](
                config=self.config, compression=compression,
                remove_origin=remove_origin, debug=debug)
        else:
            return None


def identify_compression(filename):
    """
    Try to guess the compression algorithm of a file

    :param filename: the pat of the file to identify
    :rtype: str
    """
    with open(filename, 'rb') as f:
        file_start = f.read(MAGIC_MAX_LENGTH)
    for file_type, cls in compression_registry.iteritems():
        if cls.validate(file_start):
            return file_type
    return None


class Compressor(object):
    """
    Abstract base class for all compressors
    """

    MAGIC = None

    def __init__(self, config, compression, remove_origin=False, debug=False):
        self.config = config
        self.compression = compression
        self.remove_origin = remove_origin
        self.debug = debug
        self.compress = None
        self.decompres = None

    def _build_command(self, pipe_command):
        """
        Build the command string and create the actual Command object

        :param pipe_command: the command used to compress/decompress
        :rtype: Command
        """
        command = 'command(){ '
        command += pipe_command
        command += ' > "$2" < "$1"'
        if self.remove_origin:
            command += ' && rm -f "$1"'
        command += ';}; command'
        return Command(command, shell=True, check=True, debug=self.debug)

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


class GZipCompressor(Compressor):
    """
    Predefined compressor with GZip
    """

    MAGIC = b'\x1f\x8b\x08'

    def __init__(self, config, compression, remove_origin=False, debug=False):
        super(GZipCompressor, self).__init__(
            config, compression, remove_origin, debug)
        self.compress = self._build_command('gzip -c')
        self.decompress = self._build_command('gzip -c -d')


class BZip2Compressor(Compressor):
    """
    Predefined compressor with BZip2
    """

    MAGIC = b'\x42\x5a\x68'

    def __init__(self, config, compression, remove_origin=False, debug=False):
        super(BZip2Compressor, self).__init__(
            config, compression, remove_origin, debug)
        self.compress = self._build_command('bzip2 -c')
        self.decompress = self._build_command('bzip2 -c -d')


class CustomCompressor(Compressor):
    """
    Custom compressor
    """

    def __init__(self, config, compression, remove_origin=False, debug=False):
        if not config.custom_compression_filter:
            raise CompressionIncompatibility("custom_compression_filter")
        if not config.custom_decompression_filter:
            raise CompressionIncompatibility("custom_decompression_filter")

        super(CustomCompressor, self).__init__(
            config, compression, remove_origin, debug)
        self.compress = self._build_command(
            config.custom_compression_filter)
        self.decompress = self._build_command(
            config.custom_decompression_filter)


#: a dictionary mapping all supported compression schema
#: to the class implementing it
compression_registry = {
    'gzip': GZipCompressor,
    'bzip2': BZip2Compressor,
    'custom': CustomCompressor,
}

#: The longest string needed to identify a compression schema
MAGIC_MAX_LENGTH = reduce(
    max, [len(x.MAGIC or '') for x in compression_registry.values()], 0)
