# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2011-2022
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

import binascii
import bz2
import gzip
import logging
import shutil
from abc import ABCMeta, abstractmethod, abstractproperty
from contextlib import closing, contextmanager
from distutils.version import LooseVersion as Version

import barman.infofile
from barman.command_wrappers import Command
from barman.exceptions import (
    CommandFailedException,
    CompressionException,
    CompressionIncompatibility,
)
from barman.utils import force_str, with_metaclass

_logger = logging.getLogger(__name__)


class CompressionManager(object):
    def __init__(self, config, path):
        """
        Compression manager
        """
        self.config = config
        self.path = path
        self.unidentified_compression = None

        if self.config.compression == "custom":
            # If Barman is set to use the custom compression and no magic is
            # configured, it assumes that every unidentified file is custom
            # compressed.
            if self.config.custom_compression_magic is None:
                self.unidentified_compression = self.config.compression
            # If custom_compression_magic is set then we should not assume
            # unidentified files are custom compressed and should rely on the
            # magic for identification instead.
            elif type(config.custom_compression_magic) == str:
                # Since we know the custom compression magic we can now add it
                # to the class property.
                compression_registry["custom"].MAGIC = binascii.unhexlify(
                    config.custom_compression_magic[2:]
                )

        # Set the longest string needed to identify a compression schema.
        # This happens at instantiation time because we need to include the
        # custom_compression_magic from the config (if set).
        self.MAGIC_MAX_LENGTH = max(
            len(x.MAGIC or "") for x in compression_registry.values()
        )

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

    def get_default_compressor(self):
        """
        Returns a new default compressor instance
        """
        return self.get_compressor(self.config.compression)

    def get_compressor(self, compression):
        """
        Returns a new compressor instance

        :param str compression: Compression name or none
        """
        # Check if the requested compression mechanism is allowed
        if compression and self.check(compression):
            return compression_registry[compression](
                config=self.config, compression=compression, path=self.path
            )
        return None

    def get_wal_file_info(self, filename):
        """
        Populate a WalFileInfo object taking into account the server
        configuration.

        Set compression to 'custom' if no compression is identified
        and Barman is configured to use custom compression.

        :param str filename: the path of the file to identify
        :rtype: barman.infofile.WalFileInfo
        """
        return barman.infofile.WalFileInfo.from_file(
            filename,
            compression_manager=self,
            unidentified_compression=self.unidentified_compression,
        )

    def identify_compression(self, filename):
        """
        Try to guess the compression algorithm of a file

        :param str filename: the path of the file to identify
        :rtype: str
        """
        # TODO: manage multiple decompression methods for the same
        # compression algorithm (e.g. what to do when gzip is detected?
        # should we use gzip or pigz?)
        with open(filename, "rb") as f:
            file_start = f.read(self.MAGIC_MAX_LENGTH)
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
        super(CommandCompressor, self).__init__(config, compression, path)

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
        command = "barman_command(){ "
        command += pipe_command
        command += ' > "$2" < "$1"'
        command += ";}; barman_command"
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
            with open(src, "rb") as istream:
                with closing(self._compressor(dst)) as ostream:
                    shutil.copyfileobj(istream, ostream)
        except Exception as e:
            # you won't get more information from the compressors anyway
            raise CommandFailedException(dict(ret=None, err=force_str(e), out=None))
        return 0

    def decompress(self, src, dst):
        """
        Decompress using the object defined in the sublcass

        :param src: source file to decompress
        :param dst: destination of the decompression
        """
        try:
            with closing(self._decompressor(src)) as istream:
                with open(dst, "wb") as ostream:
                    shutil.copyfileobj(istream, ostream)
        except Exception as e:
            # you won't get more information from the compressors anyway
            raise CommandFailedException(dict(ret=None, err=force_str(e), out=None))
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

    MAGIC = b"\x1f\x8b\x08"

    def __init__(self, config, compression, path=None):
        super(GZipCompressor, self).__init__(config, compression, path)
        self._compress = self._build_command("gzip -c")
        self._decompress = self._build_command("gzip -c -d")


class PyGZipCompressor(InternalCompressor):
    """
    Predefined compressor that uses GZip Python libraries
    """

    MAGIC = b"\x1f\x8b\x08"

    def __init__(self, config, compression, path=None):
        super(PyGZipCompressor, self).__init__(config, compression, path)

        # Default compression level used in system gzip utility
        self._level = -1  # Z_DEFAULT_COMPRESSION constant of zlib

    def _compressor(self, name):
        return gzip.GzipFile(name, mode="wb", compresslevel=self._level)

    def _decompressor(self, name):
        return gzip.GzipFile(name, mode="rb")


class PigzCompressor(CommandCompressor):
    """
    Predefined compressor with Pigz

    Note that pigz on-disk is the same as gzip, so the MAGIC value of this
    class is the same
    """

    MAGIC = b"\x1f\x8b\x08"

    def __init__(self, config, compression, path=None):
        super(PigzCompressor, self).__init__(config, compression, path)
        self._compress = self._build_command("pigz -c")
        self._decompress = self._build_command("pigz -c -d")


class BZip2Compressor(CommandCompressor):
    """
    Predefined compressor with BZip2
    """

    MAGIC = b"\x42\x5a\x68"

    def __init__(self, config, compression, path=None):
        super(BZip2Compressor, self).__init__(config, compression, path)
        self._compress = self._build_command("bzip2 -c")
        self._decompress = self._build_command("bzip2 -c -d")


class PyBZip2Compressor(InternalCompressor):
    """
    Predefined compressor with BZip2 Python libraries
    """

    MAGIC = b"\x42\x5a\x68"

    def __init__(self, config, compression, path=None):
        super(PyBZip2Compressor, self).__init__(config, compression, path)

        # Default compression level used in system gzip utility
        self._level = 9

    def _compressor(self, name):
        return bz2.BZ2File(name, mode="wb", compresslevel=self._level)

    def _decompressor(self, name):
        return bz2.BZ2File(name, mode="rb")


class CustomCompressor(CommandCompressor):
    """
    Custom compressor
    """

    def __init__(self, config, compression, path=None):
        if (
            config.custom_compression_filter is None
            or type(config.custom_compression_filter) != str
        ):
            raise CompressionIncompatibility("custom_compression_filter")
        if (
            config.custom_decompression_filter is None
            or type(config.custom_decompression_filter) != str
        ):
            raise CompressionIncompatibility("custom_decompression_filter")

        super(CustomCompressor, self).__init__(config, compression, path)
        self._compress = self._build_command(config.custom_compression_filter)
        self._decompress = self._build_command(config.custom_decompression_filter)


# a dictionary mapping all supported compression schema
# to the class implementing it
# WARNING: items in this dictionary are extracted using alphabetical order
# It's important that gzip and bzip2 are positioned before their variants
compression_registry = {
    "gzip": GZipCompressor,
    "pigz": PigzCompressor,
    "bzip2": BZip2Compressor,
    "pygzip": PyGZipCompressor,
    "pybzip2": PyBZip2Compressor,
    "custom": CustomCompressor,
}


def get_pg_basebackup_compression(server):
    """
    Factory method which returns an instantiated PgBaseBackupCompression subclass
    for the backup_compression option in config for the supplied server.

    :param barman.server.Server server: the server for which the
      PgBaseBackupCompression should be constructed
    """
    if server.config.backup_compression is None:
        return
    try:
        return {"gzip": GZipPgBaseBackupCompression}[server.config.backup_compression](
            server.config
        )
    except KeyError:
        raise CompressionException(
            "Barman does not support pg_basebackup compression: %s"
            % server.config.backup_compression
        )


class PgBaseBackupCompression(with_metaclass(ABCMeta, object)):
    """
    Represents the pg_basebackup compression options and provides functionality
    required by the backup process which depends on those options.
    """

    def __init__(self, config):
        """
        Constructor for the PgBaseBackupCompression abstract base class.

        :param barman.config.ServerConfig config: the server configuration
        """
        self.type = config.backup_compression
        self.format = config.backup_compression_format
        self.level = config.backup_compression_level
        self.location = config.backup_compression_location

    @abstractproperty
    def suffix(self):
        """The filename suffix expected for this compression"""

    def with_suffix(self, basename):
        """
        Append the suffix to the supplied basename.

        :param str basename: The basename (without compression suffix) of the
          file to be opened.
        """
        return ".".join((basename, self.suffix))

    @abstractmethod
    @contextmanager
    def open(self, basename):
        """
        Open file at path/basename for reading.

        :param str basename: The basename (without compression suffix) of the
          file to be opened.
        """

    def validate(self, server, remote_status):
        """
        Validate pg_basebackup compression options.

        :param barman.server.Server server: the server for which the
          compression options should be validated.
        :param dict remote_status: the status of the pg_basebackup command
        """
        if self.location is not None and self.location == "server":
            # "backup_location = server" requires pg_basebackup >= 15
            if remote_status["pg_basebackup_version"] < Version("15"):
                server.config.disabled = True
                server.config.msg_list.append(
                    "backup_compression_location = server requires "
                    "pg_basebackup 15 or greater"
                )
            # "backup_location = server" requires PostgreSQL >= 15
            if server.postgres.server_version < 150000:
                server.config.disabled = True
                server.config.msg_list.append(
                    "backup_compression_location = server requires "
                    "PostgreSQL 15 or greater"
                )

        # plain backup format is only allowed when compression is on the server
        if self.format == "plain" and self.location != "server":
            server.config.disabled = True
            server.config.msg_list.append(
                "backup_compression_format plain is not compatible with "
                "backup_compression_location %s" % self.location
            )


class GZipPgBaseBackupCompression(PgBaseBackupCompression):
    suffix = "gz"

    @contextmanager
    def open(self, basename):
        """
        Open file at path/basename for reading, uncompressing with the GZip algorithm.

        :param str basename: The basename (without compression suffix) of the
          file to be opened.
        """
        yield gzip.open(self.with_suffix(basename), "rb")

    def validate(self, server, remote_status):
        """
        Validate gzip-specific options.

        :param barman.server.Server server: the server for which the
          compression options should be validated.
        :param dict remote_status: the status of the pg_basebackup command
        """
        super(GZipPgBaseBackupCompression, self).validate(server, remote_status)
        if self.level is not None and (self.level < 1 or self.level > 9):
            server.config.disabled = True
            server.config.msg_list.append(
                "backup_compression_level %d unsupported by "
                "pg_basebackup compression %s" % (self.level, self.type)
            )
