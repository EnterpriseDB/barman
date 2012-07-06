# Copyright (C) 2011, 2012 2ndQuadrant Italia (Devise.IT S.r.L.)
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

''' This module is responsible to manage the compression features of Barman
'''

from barman.command_wrappers import Command

import logging

_logger = logging.getLogger(__name__)


class CompressionIncompatibility(Exception):
    ''' Exception for compression incompatibility
    '''
    pass


class CompressionManager(object):
    ''' Compression manager
    '''
    def __init__(self, config):
        self.config = config
        # Check if the configured compression mechanism is allowed
        if not config.compression or len(config.compression) == 0 or not self.check():
            self.compressor_class = None
            self.decompressor_class = None
        else:
            self.compressor_class = compression_registry[config.compression][0]
            self.decompressor_class = compression_registry[config.compression][1]

    def check(self):
        ''' This method returns True if the compression specified in the
        configuration file is present in the register, otherwise False
        '''
        if self.config.compression not in compression_registry:
            return False
        return True

    def get_compressor(self, remove_origin=False, debug=False):
        ''' Returns a new compressor instance (first item in the registry)
        '''
        return self.compressor_class and self.compressor_class(self.config, remove_origin, debug)

    def get_decompressor(self, remove_origin=False, debug=False):
        ''' Return a new decompressor instance (second item in the registry)
        '''
        return self.decompressor_class and self.decompressor_class(self.config, remove_origin, debug)

class GZipCompressor(Command):
    '''
    Predefined compressor with GZip
    '''
    def __init__(self, config, remove_origin=False, debug=False):
        self.remove_origin = remove_origin
        if remove_origin:
            command = 'compress(){ gzip -c > "$2" < "$1" && rm -f "$1";}; compress'
        else:
            command = 'compress(){ gzip -c > "$2" < "$1";}; compress'
        Command.__init__(self, command, shell=True, check=True, debug=debug)

class GZipDecompressor(Command):
    '''
    Predefined decompressor with GZip
    '''
    def __init__(self, config, remove_origin=False, debug=False):
        self.remove_origin = remove_origin
        if remove_origin:
            command = 'decompress(){ gzip -c -d > "$2" < "$1" && rm -f "$1";}; decompress'
        else:
            command = 'decompress(){ gzip -c -d > "$2" < "$1";}; decompress'
        Command.__init__(self, command, shell=True, check=True, debug=debug)

class BZip2Compressor(Command):
    ''' Predefined compressor with BZip2
    '''
    def __init__(self, config, remove_origin=False, debug=False):
        self.remove_origin = remove_origin
        if remove_origin:
            command = 'compress(){ bzip2 -c > "$2" < "$1" && rm -f "$1";}; compress'
        else:
            command = 'compress(){ bzip2 -c > "$2" < "$1";}; compress'
        Command.__init__(self, command, shell=True, check=True, debug=debug)

class BZip2Decompressor(Command):
    ''' Predefined decompressor with BZip2
    '''
    def __init__(self, config, remove_origin=False, debug=False):
        self.remove_origin = remove_origin
        if remove_origin:
            command = 'decompress(){ bzip2 -c -d > "$2" < "$1" && rm -f "$1";}; decompress'
        else:
            command = 'decompress(){ bzip2 -c -d > "$2" < "$1";}; decompress'
        Command.__init__(self, command, shell=True, check=True, debug=debug)

class CustomCompressor(Command):
    ''' Custom compressor
    '''
    def __init__(self, config, remove_origin=False, debug=False):
        if not config.custom_compression_filter:
            raise CompressionIncompatibility("custom_compression_filter")
        self.remove_origin = remove_origin
        if remove_origin:
            template = 'compress(){ %s > "$2" < "$1" && rm -f "$1";}; compress'
        else:
            template = 'compress(){ %s > "$2" < "$1";}; compress'
        Command.__init__(self, template % config.custom_compression_filter, shell=True, check=True, debug=debug)

class CustomDecompressor(Command):
    ''' Custom decompressor
    '''
    def __init__(self, config, remove_origin=False, debug=False):
        if not config.custom_decompression_filter:
            raise CompressionIncompatibility("custom_decompression_filter")
        self.remove_origin = remove_origin
        if remove_origin:
            template = 'decompress(){ %s > "$2" < "$1" && rm -f "$1";}; decompress'
        else:
            template = 'decompress(){ %s > "$2" < "$1";}; decompress'
        Command.__init__(self, template % config.custom_decompression_filter, shell=True, check=True, debug=debug)

compression_registry = {
    None: (None, None),
    'gzip': (GZipCompressor, GZipDecompressor),
    'bzip2': (BZip2Compressor, BZip2Decompressor),
    'custom': (CustomCompressor, CustomDecompressor),
}

