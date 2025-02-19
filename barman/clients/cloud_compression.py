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
from types import SimpleNamespace

from barman.compression import CompressionManager, _try_import_snappy
from barman.utils import with_metaclass


class ChunkedCompressor(with_metaclass(ABCMeta, object)):
    """
    Base class for all ChunkedCompressors
    """

    @abstractmethod
    def add_chunk(self, data):
        """
        Compresses the supplied data and returns all the compressed bytes.

        :param bytes data: The chunk of data to be compressed
        :return: The compressed data
        :rtype: bytes
        """

    @abstractmethod
    def decompress(self, data):
        """
        Decompresses the supplied chunk of data and returns at least part of the
        uncompressed data.

        :param bytes data: The chunk of data to be decompressed
        :return: The decompressed data
        :rtype: bytes
        """


class SnappyCompressor(ChunkedCompressor):
    """
    A ChunkedCompressor implementation based on python-snappy
    """

    def __init__(self):
        snappy = _try_import_snappy()
        self.compressor = snappy.StreamCompressor()
        self.decompressor = snappy.StreamDecompressor()

    def add_chunk(self, data):
        """
        Compresses the supplied data and returns all the compressed bytes.

        :param bytes data: The chunk of data to be compressed
        :return: The compressed data
        :rtype: bytes
        """
        return self.compressor.add_chunk(data)

    def decompress(self, data):
        """
        Decompresses the supplied chunk of data and returns at least part of the
        uncompressed data.

        :param bytes data: The chunk of data to be decompressed
        :return: The decompressed data
        :rtype: bytes
        """
        return self.decompressor.decompress(data)


def get_compressor(compression):
    """
    Helper function which returns a ChunkedCompressor for the specified compression
    algorithm. Currently only snappy is supported. The other compression algorithms
    supported by barman cloud use the decompression built into TarFile.

    :param str compression: The compression algorithm to use. Can be set to snappy
      or any compression supported by the TarFile mode string.
    :return: A ChunkedCompressor capable of compressing and decompressing using the
      specified compression.
    :rtype: ChunkedCompressor
    """
    if compression == "snappy":
        return SnappyCompressor()
    return None


def get_streaming_tar_mode(mode, compression):
    """
    Helper function used in streaming uploads and downloads which appends the supplied
    compression to the raw filemode (either r or w) and returns the result. Any
    compression algorithms supported by barman-cloud but not Python TarFile are
    ignored so that barman-cloud can apply them itself.

    :param str mode: The file mode to use, either r or w.
    :param str compression: The compression algorithm to use. Can be set to snappy
      or any compression supported by the TarFile mode string.
    :return: The full filemode for a streaming tar file
    :rtype: str
    """
    if compression == "snappy" or compression is None:
        return "%s|" % mode
    else:
        return "%s|%s" % (mode, compression)


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


def compress(wal_file, compression, compression_level):
    """
    Compresses the supplied *wal_file* and returns a file-like object containing the
    compressed data.

    :param IOBase wal_file: A file-like object containing the WAL file data.
    :param str compression: The compression algorithm to apply. Can be one of:
      ``bzip2``, ``gzip``, ``snappy``, ``zstd``, ``lz4``, ``xz``.
    :param str|int|None: The compression level for the specified algorithm.
    :return: The compressed data
    :rtype: BytesIO
    """
    compressor = get_internal_compressor(compression, compression_level)
    return compressor.compress_in_mem(wal_file)


def decompress_to_file(blob, dest_file, compression):
    """
    Decompresses the supplied *blob* of data into the *dest_file* file-like object using
    the specified compression.

    :param IOBase blob: A file-like object containing the compressed data.
    :param IOBase dest_file: A file-like object into which the uncompressed data
      should be written.
    :param str compression: The compression algorithm to apply. Can be one of:
      ``bzip2``, ``gzip``, ``snappy``, ``zstd``, ``lz4``, ``xz``.
    :rtype: None
    """
    compressor = get_internal_compressor(compression)
    compressor.decompress_to_fileobj(blob, dest_file)
