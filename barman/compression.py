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
This module is responsible to manage the compression features of Barman
"""

import binascii
import bz2
import gzip
import logging
import lzma
import shutil
from abc import ABCMeta, abstractmethod, abstractproperty
from contextlib import closing
from distutils.version import LooseVersion as Version
from io import BytesIO
from types import SimpleNamespace

from barman.command_wrappers import Command
from barman.exceptions import (
    CommandFailedException,
    CompressionException,
    CompressionIncompatibility,
    FileNotFoundException,
)
from barman.fs import unix_command_factory
from barman.utils import force_str, with_metaclass

_logger = logging.getLogger(__name__)


class CompressionManager(object):
    def __init__(self, config, path):
        """

        :param config: barman.config.ServerConfig
        :param path: str
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
            elif isinstance(config.custom_compression_magic, str):
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

    :cvar MAGIC: Magic bytes used to identify the compression format
    :cvar LEVEL_MIN: Minimum compression level supported by the compression algorithm
    :cvar LEVEL_MAX: Maximum compression level supported by the compression algorithm
    :cvar LEVEL_LOW: Mapped level to ``low``
    :cvar LEVEL_MEDIUM: Mapped level to ``medium``
    :cvar LEVEL_HIGH: Mapped level to ``high``
    """

    MAGIC = None
    LEVEL_MAX = None
    LEVEL_MIN = None
    LEVEL_LOW = None
    LEVEL_MEDIUM = None
    LEVEL_HIGH = None

    def __init__(self, config, compression, path=None):
        """

        :param config: barman.config.ServerConfig
        :param compression: str compression name
        :param path: str|None
        """
        self.config = config
        self.compression = compression
        self.path = path
        if isinstance(config.compression_level, int):
            if self.LEVEL_MAX is not None and config.compression_level > self.LEVEL_MAX:
                _logger.debug(
                    "Compression level %s out of range for %s, using %s instead"
                    % (config.compression_level, config.compression, self.LEVEL_MAX)
                )
                self.level = self.LEVEL_MAX
            elif (
                self.LEVEL_MIN is not None and config.compression_level < self.LEVEL_MIN
            ):
                _logger.debug(
                    "Compression level %s out of range for %s, using %s instead"
                    % (config.compression_level, config.compression, self.LEVEL_MIN)
                )
                self.level = self.LEVEL_MIN
            else:
                self.level = config.compression_level
        elif config.compression_level == "low":
            self.level = self.LEVEL_LOW
        elif config.compression_level == "high":
            self.level = self.LEVEL_HIGH
        else:
            self.level = self.LEVEL_MEDIUM

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
        """
        :param config: barman.config.ServerConfig
        :param compression: str compression name
        :param path: str|None
        """
        super(CommandCompressor, self).__init__(config, compression, path)

        self._compress = None
        self._decompress = None

    def compress(self, src, dst):
        """
        Compress using the specific command defined in the subclass

        :param src: source file to compress
        :param dst: destination of the decompression
        """
        return self._compress(src, dst)

    def decompress(self, src, dst):
        """
        Decompress using the specific command defined in the subclass

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
        Compress using the object defined in the subclass

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
        Decompress using the object defined in the subclass

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

    @abstractmethod
    def compress_in_mem(self, fileobj):
        """
        Compresses the given file-object in memory

        :param fileobj: source file-object to be compressed
        :return: a compressed file-object

        .. note::
            When implementing this method, the compressed file-object position must be
            set to ``0`` before returning it, as it is likely to be read again afterwards.
        """

    @abstractmethod
    def decompress_in_mem(self, fileobj):
        """
        Decompresses the given file-object in memory

        :param fileobj: source file-object to be decompressed
        :return: a decompressed file-object
        """

    def decompress_to_fileobj(self, src_fileobj, dest_fileobj):
        """
        Decompresses the given file-object on the especified file-object

        :param src_fileobj: source file-object to be decompressed
        :param dest_fileobj: destination file-object to have the decompressed content
        """
        decompressed_fileobj = self.decompress_in_mem(src_fileobj)
        shutil.copyfileobj(decompressed_fileobj, dest_fileobj)


class GZipCompressor(CommandCompressor):
    """
    Predefined compressor with GZip
    """

    MAGIC = b"\x1f\x8b\x08"
    LEVEL_MIN = 1
    LEVEL_MAX = 9
    LEVEL_LOW = 1
    LEVEL_MEDIUM = 6
    LEVEL_HIGH = 9

    def __init__(self, config, compression, path=None):
        """

        :param config: barman.config.ServerConfig
        :param compression: str compression name
        :param path: str|None
        """
        super(GZipCompressor, self).__init__(config, compression, path)
        self._compress = self._build_command("gzip -c -%s" % self.level)
        self._decompress = self._build_command("gzip -c -d")


class PyGZipCompressor(InternalCompressor):
    """
    Predefined compressor that uses GZip Python libraries
    """

    MAGIC = b"\x1f\x8b\x08"
    LEVEL_MIN = 1
    LEVEL_MAX = 9
    LEVEL_LOW = 1
    LEVEL_MEDIUM = 6
    LEVEL_HIGH = 9

    def __init__(self, config, compression, path=None):
        """

        :param config: barman.config.ServerConfig
        :param compression: str compression name
        :param path: str|None
        """
        super(PyGZipCompressor, self).__init__(config, compression, path)

    def _compressor(self, name):
        return gzip.GzipFile(name, mode="wb", compresslevel=self.level)

    def _decompressor(self, name):
        return gzip.GzipFile(name, mode="rb")

    def compress_in_mem(self, fileobj):
        in_mem_gzip = BytesIO()
        with gzip.GzipFile(
            fileobj=in_mem_gzip, mode="wb", compresslevel=self.level
        ) as gz:
            shutil.copyfileobj(fileobj, gz)
        in_mem_gzip.seek(0)
        return in_mem_gzip

    def decompress_in_mem(self, fileobj):
        return gzip.GzipFile(fileobj=fileobj, mode="rb")


class PigzCompressor(CommandCompressor):
    """
    Predefined compressor with Pigz

    Note that pigz on-disk is the same as gzip, so the MAGIC value of this
    class is the same
    """

    MAGIC = b"\x1f\x8b\x08"
    LEVEL_MIN = 1
    LEVEL_MAX = 9
    LEVEL_LOW = 1
    LEVEL_MEDIUM = 6
    LEVEL_HIGH = 9

    def __init__(self, config, compression, path=None):
        """

        :param config: barman.config.ServerConfig
        :param compression: str compression name
        :param path: str|None
        """
        super(PigzCompressor, self).__init__(config, compression, path)
        self._compress = self._build_command("pigz -c -%s" % self.level)
        self._decompress = self._build_command("pigz -c -d")


class BZip2Compressor(CommandCompressor):
    """
    Predefined compressor with BZip2
    """

    MAGIC = b"\x42\x5a\x68"
    LEVEL_MIN = 1
    LEVEL_MAX = 9
    LEVEL_LOW = 1
    LEVEL_MEDIUM = 5
    LEVEL_HIGH = 9

    def __init__(self, config, compression, path=None):
        """

        :param config: barman.config.ServerConfig
        :param compression: str compression name
        :param path: str|None
        """
        super(BZip2Compressor, self).__init__(config, compression, path)
        self._compress = self._build_command("bzip2 -c -%s" % self.level)
        self._decompress = self._build_command("bzip2 -c -d")


class PyBZip2Compressor(InternalCompressor):
    """
    Predefined compressor with BZip2 Python libraries
    """

    MAGIC = b"\x42\x5a\x68"
    LEVEL_MIN = 1
    LEVEL_MAX = 9
    LEVEL_LOW = 1
    LEVEL_MEDIUM = 5
    LEVEL_HIGH = 9

    def _compressor(self, name):
        return bz2.BZ2File(name, mode="wb", compresslevel=self.level)

    def _decompressor(self, name):
        return bz2.BZ2File(name, mode="rb")

    def compress_in_mem(self, fileobj):
        in_mem_bz2 = BytesIO(bz2.compress(fileobj.read(), compresslevel=self.level))
        in_mem_bz2.seek(0)
        return in_mem_bz2

    def decompress_in_mem(self, fileobj):
        return bz2.BZ2File(fileobj, "rb")


class XZCompressor(InternalCompressor):
    """
    Predefined compressor with XZ Python library
    """

    MAGIC = b"\xfd7zXZ\x00"
    LEVEL_MIN = 1
    LEVEL_MAX = 9
    LEVEL_LOW = 1
    LEVEL_MEDIUM = 3
    LEVEL_HIGH = 5

    def _compressor(self, dst):
        return lzma.open(dst, mode="wb", preset=self.level)

    def _decompressor(self, src):
        return lzma.open(src, mode="rb")

    def compress_in_mem(self, fileobj):
        in_mem_xz = BytesIO(lzma.compress(fileobj.read(), preset=self.level))
        in_mem_xz.seek(0)
        return in_mem_xz

    def decompress_in_mem(self, fileobj):
        return lzma.open(fileobj, "rb")


def _try_import_zstd():
    try:
        import zstandard
    except ImportError:
        raise SystemExit("Missing required python module: zstandard")
    return zstandard


class ZSTDCompressor(InternalCompressor):
    """
    Predefined compressor with zstd
    """

    MAGIC = b"(\xb5/\xfd"
    LEVEL_MIN = -22
    LEVEL_MAX = 22
    LEVEL_LOW = 1
    LEVEL_MEDIUM = 4
    LEVEL_HIGH = 9

    def __init__(self, config, compression, path=None):
        """
        Constructor.
        :param config: barman.config.ServerConfig
        :param compression: str compression name
        :param path: str|None
        """
        super(ZSTDCompressor, self).__init__(config, compression, path)
        self._zstd = None

    @property
    def zstd(self):
        if self._zstd is None:
            self._zstd = _try_import_zstd()
        return self._zstd

    def _compressor(self, dst):
        return self.zstd.ZstdCompressor(level=self.level).stream_writer(
            open(dst, mode="wb")
        )

    def _decompressor(self, src):
        return self.zstd.ZstdDecompressor().stream_reader(open(src, mode="rb"))

    def compress_in_mem(self, fileobj):
        in_mem_zstd = BytesIO()
        self.zstd.ZstdCompressor(level=self.level).copy_stream(fileobj, in_mem_zstd)
        in_mem_zstd.seek(0)
        return in_mem_zstd

    def decompress_in_mem(self, fileobj):
        return self.zstd.ZstdDecompressor().stream_reader(fileobj)


def _try_import_lz4():
    try:
        import lz4.frame
    except ImportError:
        raise SystemExit("Missing required python module: lz4")
    return lz4


class LZ4Compressor(InternalCompressor):
    """
    Predefined compressor with lz4
    """

    MAGIC = b"\x04\x22\x4d\x18"
    LEVEL_MIN = 0
    LEVEL_MAX = 16
    LEVEL_LOW = 0
    LEVEL_MEDIUM = 6
    LEVEL_HIGH = 10

    def __init__(self, config, compression, path=None):
        """
        Constructor.
        :param config: barman.config.ServerConfig
        :param compression: str compression name
        :param path: str|None
        """
        super(LZ4Compressor, self).__init__(config, compression, path)
        self._lz4 = None

    @property
    def lz4(self):
        if self._lz4 is None:
            self._lz4 = _try_import_lz4()
        return self._lz4

    def _compressor(self, dst):
        return self.lz4.frame.open(dst, mode="wb", compression_level=self.level)

    def _decompressor(self, src):
        return self.lz4.frame.open(src, mode="rb")

    def compress_in_mem(self, fileobj):
        in_mem_lz4 = BytesIO(
            self.lz4.frame.compress(fileobj.read(), compression_level=self.level)
        )
        in_mem_lz4.seek(0)
        return in_mem_lz4

    def decompress_in_mem(self, fileobj):
        return self.lz4.frame.open(fileobj, mode="rb")


def _try_import_snappy():
    try:
        import snappy
    except ImportError:
        raise SystemExit("Missing required python module: python-snappy")
    return snappy


class SnappyCompressor(InternalCompressor):

    MAGIC = b"\xff\x06\x00\x00sNaPpY"

    def __init__(self, config, compression, path=None):
        """
        Constructor.
        :param config: barman.config.ServerConfig
        :param compression: str compression name
        :param path: str|None
        """
        super(SnappyCompressor, self).__init__(config, compression, path)
        self._snappy = None

    @property
    def snappy(self):
        if self._snappy is None:
            self._snappy = _try_import_snappy()
        return self._snappy

    def _compressor(self, dst):
        """Snappy library does not provide an interface which returns file-objects"""
        return None

    def _decompressor(self, src):
        """Snappy library does not provide an interface which returns file-objects"""
        return None

    def compress(self, src, dst):
        """
        Snappy-compress the source file-object to the destination file-object

        :param src: source file to compress
        :param dst: destination of the decompression
        """
        try:
            with open(src, "rb") as istream:
                with open(dst, "wb") as ostream:
                    compressed_fileobj = self.compress_in_mem(istream)
                    shutil.copyfileobj(compressed_fileobj, ostream)
        except Exception as e:
            raise CommandFailedException(dict(ret=None, err=force_str(e), out=None))
        return 0

    def decompress(self, src, dst):
        """
        Decompress the source file-object to the destination file-object

        :param src: source file to decompress
        :param dst: destination of the decompression
        """
        try:
            with open(src, "rb") as istream:
                with open(dst, "wb") as ostream:
                    decompressed_fileobj = self.decompress_in_mem(istream)
                    shutil.copyfileobj(decompressed_fileobj, ostream)
        except Exception as e:
            raise CommandFailedException(dict(ret=None, err=force_str(e), out=None))
        return 0

    def compress_in_mem(self, fileobj):
        in_mem_snappy = BytesIO()
        self.snappy.stream_compress(fileobj, in_mem_snappy)
        in_mem_snappy.seek(0)
        return in_mem_snappy

    def decompress_in_mem(self, fileobj):
        decompressed_file = BytesIO()
        self.snappy.stream_decompress(fileobj, decompressed_file)
        decompressed_file.seek(0)
        return decompressed_file

    def decompress_to_fileobj(self, src_fileobj, dest_fileobj):
        """
        Decompresses the given file-object on the especified file-object

        :param src_fileobj: source file-object to be decompressed
        :param dest_fileobj: destination file-object to have the decompressed content

        .. note::
            We override this method to avoid redundant work. As Snappy can stream the
            result directly to a specified object, there is no need for intermediate
            objects as used in the parent class implementation.
        """
        self.snappy.stream_decompress(src_fileobj, dest_fileobj)


class CustomCompressor(CommandCompressor):
    """
    Custom compressor
    """

    def __init__(self, config, compression, path=None):
        """

        :param config: barman.config.ServerConfig
        :param compression: str compression name
        :param path: str|None
        """
        if config.custom_compression_filter is None or not isinstance(
            config.custom_compression_filter, str
        ):
            raise CompressionIncompatibility("custom_compression_filter")
        if config.custom_decompression_filter is None or not isinstance(
            config.custom_decompression_filter, str
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
    "xz": XZCompressor,
    "zstd": ZSTDCompressor,
    "lz4": LZ4Compressor,
    "snappy": SnappyCompressor,
    "custom": CustomCompressor,
}


def get_pg_basebackup_compression(server):
    """
    Factory method which returns an instantiated PgBaseBackupCompression subclass
    for the backup_compression option in config for the supplied server.

    :param barman.server.Server server: the server for which the
      PgBaseBackupCompression should be constructed
    :return GZipPgBaseBackupCompression
    """
    if server.config.backup_compression is None:
        return
    pg_base_backup_cfg = PgBaseBackupCompressionConfig(
        server.config.backup_compression,
        server.config.backup_compression_format,
        server.config.backup_compression_level,
        server.config.backup_compression_location,
        server.config.backup_compression_workers,
    )
    base_backup_compression_option = None
    compression = None
    if server.config.backup_compression == GZipCompression.name:
        # Create PgBaseBackupCompressionOption
        base_backup_compression_option = GZipPgBaseBackupCompressionOption(
            pg_base_backup_cfg
        )
        compression = GZipCompression(unix_command_factory())

    if server.config.backup_compression == LZ4Compression.name:
        base_backup_compression_option = LZ4PgBaseBackupCompressionOption(
            pg_base_backup_cfg
        )
        compression = LZ4Compression(unix_command_factory())

    if server.config.backup_compression == ZSTDCompression.name:
        base_backup_compression_option = ZSTDPgBaseBackupCompressionOption(
            pg_base_backup_cfg
        )
        compression = ZSTDCompression(unix_command_factory())

    if server.config.backup_compression == NoneCompression.name:
        base_backup_compression_option = NonePgBaseBackupCompressionOption(
            pg_base_backup_cfg
        )
        compression = NoneCompression(unix_command_factory())

    if base_backup_compression_option is None or compression is None:
        # We got to the point where the compression is not handled
        raise CompressionException(
            "Barman does not support pg_basebackup compression: %s"
            % server.config.backup_compression
        )
    return PgBaseBackupCompression(
        pg_base_backup_cfg, base_backup_compression_option, compression
    )


class PgBaseBackupCompressionConfig(object):
    """Should become a dataclass"""

    def __init__(
        self,
        backup_compression,
        backup_compression_format,
        backup_compression_level,
        backup_compression_location,
        backup_compression_workers,
    ):
        self.type = backup_compression
        self.format = backup_compression_format
        self.level = backup_compression_level
        self.location = backup_compression_location
        self.workers = backup_compression_workers


class PgBaseBackupCompressionOption(object):
    """This class is in charge of validating pg_basebackup compression options"""

    def __init__(self, pg_base_backup_config):
        """

        :param pg_base_backup_config: PgBaseBackupCompressionConfig
        """
        self.config = pg_base_backup_config

    def validate(self, pg_server_version, remote_status):
        """
        Validate pg_basebackup compression options.

        :param pg_server_version int: the server for which the
          compression options should be validated.
        :param dict remote_status: the status of the pg_basebackup command
        :return List: List of Issues (str) or empty list
        """
        issues = []
        if self.config.location is not None and self.config.location == "server":
            # "backup_location = server" requires pg_basebackup >= 15
            if remote_status["pg_basebackup_version"] < Version("15"):
                issues.append(
                    "backup_compression_location = server requires "
                    "pg_basebackup 15 or greater"
                )
            # "backup_location = server" requires PostgreSQL >= 15
            if pg_server_version < 150000:
                issues.append(
                    "backup_compression_location = server requires "
                    "PostgreSQL 15 or greater"
                )

        # plain backup format is only allowed when compression is on the server
        if self.config.format == "plain" and self.config.location != "server":
            issues.append(
                "backup_compression_format plain is not compatible with "
                "backup_compression_location %s" % self.config.location
            )
        return issues


class GZipPgBaseBackupCompressionOption(PgBaseBackupCompressionOption):
    def validate(self, pg_server_version, remote_status):
        """
        Validate gzip-specific options.

        :param pg_server_version int: the server for which the
          compression options should be validated.
        :param dict remote_status: the status of the pg_basebackup command
        :return List: List of Issues (str) or empty list
        """
        issues = super(GZipPgBaseBackupCompressionOption, self).validate(
            pg_server_version, remote_status
        )
        levels = list(range(1, 10))
        levels.append(-1)
        if self.config.level is not None and remote_status[
            "pg_basebackup_version"
        ] < Version("15"):
            # version prior to 15 allowed gzip compression 0
            levels.append(0)
            if self.config.level not in levels:
                issues.append(
                    "backup_compression_level %d unsupported by compression algorithm."
                    " %s expects a compression level between -1 and 9 (-1 will use default compression level)."
                    % (self.config.level, self.config.type)
                )
        if (
            self.config.level is not None
            and remote_status["pg_basebackup_version"] >= Version("15")
            and self.config.level not in levels
        ):
            msg = (
                "backup_compression_level %d unsupported by compression algorithm."
                " %s expects a compression level between 1 and 9 (-1 will use default compression level)."
                % (self.config.level, self.config.type)
            )
            if self.config.level == 0:
                msg += "\nIf you need to create an archive not compressed, you should set `backup_compression = none`."
            issues.append(msg)
        if self.config.workers is not None:
            issues.append(
                "backup_compression_workers is not compatible with compression %s"
                % self.config.type
            )
        return issues


class LZ4PgBaseBackupCompressionOption(PgBaseBackupCompressionOption):
    def validate(self, pg_server_version, remote_status):
        """
        Validate lz4-specific options.

        :param pg_server_version int: the server for which the
          compression options should be validated.
        :param dict remote_status: the status of the pg_basebackup command
        :return List: List of Issues (str) or empty list
        """
        issues = super(LZ4PgBaseBackupCompressionOption, self).validate(
            pg_server_version, remote_status
        )
        # "lz4" compression requires pg_basebackup >= 15
        if remote_status["pg_basebackup_version"] < Version("15"):
            issues.append(
                "backup_compression = %s requires "
                "pg_basebackup 15 or greater" % self.config.type
            )

        if self.config.level is not None and (
            self.config.level < 0 or self.config.level > 12
        ):
            issues.append(
                "backup_compression_level %d unsupported by compression algorithm."
                " %s expects a compression level between 1 and 12 (0 will use default compression level)."
                % (self.config.level, self.config.type)
            )
        if self.config.workers is not None:
            issues.append(
                "backup_compression_workers is not compatible with compression %s."
                % self.config.type
            )
        return issues


class ZSTDPgBaseBackupCompressionOption(PgBaseBackupCompressionOption):
    def validate(self, pg_server_version, remote_status):
        """
        Validate zstd-specific options.

        :param pg_server_version int: the server for which the
          compression options should be validated.
        :param dict remote_status: the status of the pg_basebackup command
        :return List: List of Issues (str) or empty list
        """
        issues = super(ZSTDPgBaseBackupCompressionOption, self).validate(
            pg_server_version, remote_status
        )
        # "zstd" compression requires pg_basebackup >= 15
        if remote_status["pg_basebackup_version"] < Version("15"):
            issues.append(
                "backup_compression = %s requires "
                "pg_basebackup 15 or greater" % self.config.type
            )

        # Minimal config level comes from zstd library `STD_minCLevel()` and is
        # commonly set to -131072.
        if self.config.level is not None and (
            self.config.level < -131072 or self.config.level > 22
        ):
            issues.append(
                "backup_compression_level %d unsupported by compression algorithm."
                " '%s' expects a compression level between -131072 and 22 (3 will use default compression level)."
                % (self.config.level, self.config.type)
            )
        if self.config.workers is not None and (
            type(self.config.workers) is not int or self.config.workers < 0
        ):
            issues.append(
                "backup_compression_workers should be a positive integer: '%s' is invalid."
                % self.config.workers
            )
        return issues


class NonePgBaseBackupCompressionOption(PgBaseBackupCompressionOption):
    def validate(self, pg_server_version, remote_status):
        """
        Validate none compression specific options.

        :param pg_server_version int: the server for which the
          compression options should be validated.
        :param dict remote_status: the status of the pg_basebackup command
        :return List: List of Issues (str) or empty list
        """
        issues = super(NonePgBaseBackupCompressionOption, self).validate(
            pg_server_version, remote_status
        )

        if self.config.level is not None and (self.config.level != 0):
            issues.append(
                "backup_compression %s only supports backup_compression_level 0."
                % self.config.type
            )
        if self.config.workers is not None:
            issues.append(
                "backup_compression_workers is not compatible with compression '%s'."
                % self.config.type
            )
        return issues


class PgBaseBackupCompression(object):
    """
    Represents the pg_basebackup compression options and provides functionality
    required by the backup process which depends on those options.
    This is a facade that interacts with appropriate classes
    """

    def __init__(
        self,
        pg_basebackup_compression_cfg,
        pg_basebackup_compression_option,
        compression,
    ):
        """
        Constructor for the PgBaseBackupCompression facade that handles base_backup class related.

        :param pg_basebackup_compression_cfg PgBaseBackupCompressionConfig: pg_basebackup compression  configuration
        :param pg_basebackup_compression_option PgBaseBackupCompressionOption:
        :param compression Compression:

        """
        self.config = pg_basebackup_compression_cfg
        self.options = pg_basebackup_compression_option
        self.compression = compression

    def with_suffix(self, basename):
        """
        Append the suffix to the supplied basename.

        :param str basename: The basename (without compression suffix) of the
          file to be opened.
        """
        return "%s.%s" % (basename, self.compression.file_extension)

    def get_file_content(self, filename, archive):
        """
        Returns archive specific file content
        :param filename: str
        :param archive: str
        :return: str
        """
        return self.compression.get_file_content(filename, archive)

    def validate(self, pg_server_version, remote_status):
        """
        Validate pg_basebackup compression options.

        :param pg_server_version int: the server for which the
          compression options should be validated.
        :param dict remote_status: the status of the pg_basebackup command
        :return List: List of Issues (str) or empty list
        """
        return self.options.validate(pg_server_version, remote_status)


class Compression(with_metaclass(ABCMeta, object)):
    """
    Abstract class meant to represent compression interface
    """

    @abstractproperty
    def name(self):
        """

        :return:
        """

    @abstractproperty
    def file_extension(self):
        """

        :return:
        """

    @abstractmethod
    def uncompress(self, src, dst, exclude=None, include_args=None):
        """

        :param src: source file path without compression extension
        :param dst: destination path
        :param exclude: list of filepath in the archive to exclude from the extraction
        :param include_args: list of filepath in the archive to extract.
        :return:
        """

    @abstractmethod
    def get_file_content(self, filename, archive):
        """

        :param filename: str file to search for in the archive (requires its full path within the archive)
        :param archive: str archive path/name without extension
        :return: string content
        """

    def validate_src_and_dst(self, src):
        if src is None or src == "":
            raise ValueError("Source path should be a string")

    def validate_dst(self, dst):
        if dst is None or dst == "":
            raise ValueError("Destination path should be a string")


class GZipCompression(Compression):
    name = "gzip"
    file_extension = "tar.gz"

    def __init__(self, command):
        """

        :param command: barman.fs.UnixLocalCommand
        """
        self.command = command

    def uncompress(self, src, dst, exclude=None, include_args=None):
        """

        :param src: source file path without compression extension
        :param dst: destination path
        :param exclude: list of filepath in the archive to exclude from the extraction
        :param include_args: list of filepath in the archive to extract.
        :return:
        """
        self.validate_dst(src)
        self.validate_dst(dst)
        exclude = [] if exclude is None else exclude
        exclude_args = []
        for name in exclude:
            exclude_args.append("--exclude")
            exclude_args.append(name)
        include_args = [] if include_args is None else include_args
        args = ["-xzf", src, "--directory", dst]
        args.extend(exclude_args)
        args.extend(include_args)
        ret = self.command.cmd("tar", args=args)
        out, err = self.command.get_last_output()
        if ret != 0:
            raise CommandFailedException(
                "Error decompressing %s into %s: %s" % (src, dst, err)
            )
        else:
            return self.command.get_last_output()

    def get_file_content(self, filename, archive):
        """

        :param filename: str file to search for in the archive (requires its full path within the archive)
        :param archive: str archive path/name without extension
        :return: string content
        """
        full_archive_name = "%s.%s" % (archive, self.file_extension)
        args = ["-xzf", full_archive_name, "-O", filename, "--occurrence"]
        ret = self.command.cmd("tar", args=args)
        out, err = self.command.get_last_output()
        if ret != 0:
            if "Not found in archive" in err:
                raise FileNotFoundException(
                    err + "archive name: %s" % full_archive_name
                )
            else:
                raise CommandFailedException(
                    "Error reading %s into archive %s: (%s)"
                    % (filename, full_archive_name, err)
                )
        else:
            return out


class LZ4Compression(Compression):
    name = "lz4"
    file_extension = "tar.lz4"

    def __init__(self, command):
        """

        :param command: barman.fs.UnixLocalCommand
        """
        self.command = command

    def uncompress(self, src, dst, exclude=None, include_args=None):
        """

        :param src: source file path without compression extension
        :param dst: destination path
        :param exclude: list of filepath in the archive to exclude from the extraction
        :param include_args: list of filepath in the archive to extract.
        :return:
        """
        self.validate_dst(src)
        self.validate_dst(dst)
        exclude = [] if exclude is None else exclude
        exclude_args = []
        for name in exclude:
            exclude_args.append("--exclude")
            exclude_args.append(name)
        include_args = [] if include_args is None else include_args
        args = ["--use-compress-program", "lz4", "-xf", src, "--directory", dst]
        args.extend(exclude_args)
        args.extend(include_args)
        ret = self.command.cmd("tar", args=args)
        out, err = self.command.get_last_output()
        if ret != 0:
            raise CommandFailedException(
                "Error decompressing %s into %s: %s" % (src, dst, err)
            )
        else:
            return self.command.get_last_output()

    def get_file_content(self, filename, archive):
        """

        :param filename: str file to search for in the archive (requires its full path within the archive)
        :param archive: str archive path/name without extension
        :return: string content
        """
        full_archive_name = "%s.%s" % (archive, self.file_extension)
        args = [
            "--use-compress-program",
            "lz4",
            "-xf",
            full_archive_name,
            "-O",
            filename,
            "--occurrence",
        ]
        ret = self.command.cmd("tar", args=args)
        out, err = self.command.get_last_output()
        if ret != 0:
            if "Not found in archive" in err:
                raise FileNotFoundException(
                    err + "archive name: %s" % full_archive_name
                )
            else:
                raise CommandFailedException(
                    "Error reading %s into archive %s: (%s)"
                    % (filename, full_archive_name, err)
                )
        else:
            return out


class ZSTDCompression(Compression):
    name = "zstd"
    file_extension = "tar.zst"

    def __init__(self, command):
        """

        :param command: barman.fs.UnixLocalCommand
        """
        self.command = command

    def uncompress(self, src, dst, exclude=None, include_args=None):
        """

        :param src: source file path without compression extension
        :param dst: destination path
        :param exclude: list of filepath in the archive to exclude from the extraction
        :param include_args: list of filepath in the archive to extract.
        :return:
        """
        self.validate_dst(src)
        self.validate_dst(dst)
        exclude = [] if exclude is None else exclude
        exclude_args = []
        for name in exclude:
            exclude_args.append("--exclude")
            exclude_args.append(name)
        include_args = [] if include_args is None else include_args
        args = ["--use-compress-program", "zstd", "-xf", src, "--directory", dst]
        args.extend(exclude_args)
        args.extend(include_args)
        ret = self.command.cmd("tar", args=args)
        out, err = self.command.get_last_output()
        if ret != 0:
            raise CommandFailedException(
                "Error decompressing %s into %s: %s" % (src, dst, err)
            )
        else:
            return self.command.get_last_output()

    def get_file_content(self, filename, archive):
        """

        :param filename: str file to search for in the archive (requires its full path within the archive)
        :param archive: str archive path/name without extension
        :return: string content
        """
        full_archive_name = "%s.%s" % (archive, self.file_extension)
        args = [
            "--use-compress-program",
            "zstd",
            "-xf",
            full_archive_name,
            "-O",
            filename,
            "--occurrence",
        ]
        ret = self.command.cmd("tar", args=args)
        out, err = self.command.get_last_output()
        if ret != 0:
            if "Not found in archive" in err:
                raise FileNotFoundException(
                    err + "archive name: %s" % full_archive_name
                )
            else:
                raise CommandFailedException(
                    "Error reading %s into archive %s: (%s)"
                    % (filename, full_archive_name, err)
                )
        else:
            return out


class NoneCompression(Compression):
    name = "none"
    file_extension = "tar"

    def __init__(self, command):
        """

        :param command: barman.fs.UnixLocalCommand
        """
        self.command = command

    def uncompress(self, src, dst, exclude=None, include_args=None):
        """

        :param src: source file path without compression extension
        :param dst: destination path
        :param exclude: list of filepath in the archive to exclude from the extraction
        :param include_args: list of filepath in the archive to extract.
        :return:
        """
        self.validate_dst(src)
        self.validate_dst(dst)
        exclude = [] if exclude is None else exclude
        exclude_args = []
        for name in exclude:
            exclude_args.append("--exclude")
            exclude_args.append(name)
        include_args = [] if include_args is None else include_args
        args = ["-xf", src, "--directory", dst]
        args.extend(exclude_args)
        args.extend(include_args)
        ret = self.command.cmd("tar", args=args)
        out, err = self.command.get_last_output()
        if ret != 0:
            raise CommandFailedException(
                "Error decompressing %s into %s: %s" % (src, dst, err)
            )
        else:
            return self.command.get_last_output()

    def get_file_content(self, filename, archive):
        """

        :param filename: str file to search for in the archive (requires its full path within the archive)
        :param archive: str archive path/name without extension
        :return: string content
        """
        full_archive_name = "%s.%s" % (archive, self.file_extension)
        args = ["-xf", full_archive_name, "-O", filename, "--occurrence"]
        ret = self.command.cmd("tar", args=args)
        out, err = self.command.get_last_output()
        if ret != 0:
            if "Not found in archive" in err:
                raise FileNotFoundException(
                    err + "archive name: %s" % full_archive_name
                )
            else:
                raise CommandFailedException(
                    "Error reading %s into archive %s: (%s)"
                    % (filename, full_archive_name, err)
                )
        else:
            return out


def get_server_config_minimal(compression, compression_level):
    """
    Returns a placeholder for a :class:`~barman.config.ServerConfig` object with all compression
    parameters relevant to :class:`barman.compression.CompressionManager` filled.

    :param str compression: a valid compression algorithm option
    :param str|int|None: a compression level for the specified algorithm
    :return: a fake server config object
    :rtype: SimpleNamespace
    """
    return SimpleNamespace(
        compression=compression,
        compression_level=compression_level,
        custom_compression_magic=None,
        custom_compression_filter=None,
        custom_decompression_filter=None,
    )


def get_internal_compressor(compression, compression_level=None):
    """
    Get a :class:`barman.compression.InternalCompressor`
    for the specified *compression* algorithm

    :param str compression: a valid compression algorithm
    :param str|int|None: a compression level for the specified algorithm
    :return: the respective internal compressor
    :rtype: barman.compression.InternalCompressor
    :raises ValueError: if the compression received is unkown to Barman
    """
    # Replace gzip and bzip2 with their respective internal-compressor options so that
    # we are able to compress/decompress in-memory, avoiding forking an OS process
    if compression == "gzip":
        compression = "pygzip"
    elif compression == "bzip2":
        compression = "pybzip2"
    # Use a fake server config so we can reuse the logic of barman.compression module
    server_config = get_server_config_minimal(compression, compression_level)
    comp_manager = CompressionManager(server_config, None)
    compressor = comp_manager.get_compressor(compression)
    if compressor is None:
        raise ValueError("Unknown compression type: %s" % compression)
    return compressor
