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

from barman.compression import (
    _try_import_lz4,
    _try_import_snappy,
    get_internal_compressor,
)
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

    def flush(self):
        """
        Flushes any remaining compressed data and returns the final bytes.

        This method should be called after all data has been compressed with
        add_chunk() to ensure any buffered data and end markers are written.
        The default implementation returns an empty bytes object.

        :return: Any remaining compressed data
        :rtype: bytes
        """
        return b""


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


class Lz4Compressor(ChunkedCompressor):
    """
    A ChunkedCompressor implementation based on lz4.

    Uses lz4.frame for streaming compression and decompression. The compressor
    maintains state across add_chunk() calls and requires flush() to be called
    at the end to write the frame end marker.
    """

    def __init__(self):
        lz4 = _try_import_lz4()
        self._lz4_frame = lz4.frame
        self._compressor = None
        self._decompressor = None
        self._started = False
        self._flushed = False

    def add_chunk(self, data):
        """
        Compresses the supplied data and returns the compressed bytes.

        On the first call, this initializes the lz4 frame and writes the header.
        Subsequent calls compress additional data within the same frame.

        :param bytes data: The chunk of data to be compressed
        :return: The compressed data
        :rtype: bytes
        """
        if self._compressor is None:
            self._compressor = self._lz4_frame.LZ4FrameCompressor(auto_flush=True)

        if not self._started:
            self._started = True
            return self._compressor.begin() + self._compressor.compress(data)
        return self._compressor.compress(data)

    def decompress(self, data):
        """
        Decompresses the supplied chunk of data and returns the uncompressed data.

        The LZ4FrameDecompressor handles streaming decompression and buffering
        of partial frames automatically.

        :param bytes data: The chunk of data to be decompressed
        :return: The decompressed data
        :rtype: bytes
        """
        if self._decompressor is None:
            self._decompressor = self._lz4_frame.LZ4FrameDecompressor()
        return self._decompressor.decompress(data)

    def flush(self):
        """
        Flushes any remaining data and returns the frame end marker.

        This must be called after all data has been compressed to ensure the
        lz4 frame is properly terminated. Subsequent calls return empty bytes.

        :return: The frame end marker bytes
        :rtype: bytes
        """
        if self._compressor is not None and self._started and not self._flushed:
            self._flushed = True
            return self._compressor.flush()
        return b""


def get_compressor(compression):
    """
    Helper function which returns a ChunkedCompressor for the specified compression
    algorithm. Snappy and lz4 are supported. The other compression algorithms
    supported by barman cloud use the decompression built into TarFile.

    :param str compression: The compression algorithm to use. Can be set to snappy,
      lz4, or any compression supported by the TarFile mode string.
    :return: A ChunkedCompressor capable of compressing and decompressing using the
      specified compression.
    :rtype: ChunkedCompressor
    """
    if compression == "snappy":
        return SnappyCompressor()
    if compression == "lz4":
        return Lz4Compressor()
    return None


def get_streaming_tar_mode(mode, compression):
    """
    Helper function used in streaming uploads and downloads which appends the supplied
    compression to the raw filemode (either r or w) and returns the result. Any
    compression algorithms supported by barman-cloud but not Python TarFile are
    ignored so that barman-cloud can apply them itself.

    :param str mode: The file mode to use, either r or w.
    :param str compression: The compression algorithm to use. Can be set to snappy,
      lz4, or any compression supported by the TarFile mode string.
    :return: The full filemode for a streaming tar file
    :rtype: str
    """
    # Compression algorithms that require manual handling (not built into TarFile)
    if compression in ("snappy", "lz4") or compression is None:
        return "%s|" % mode
    else:
        return "%s|%s" % (mode, compression)


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
