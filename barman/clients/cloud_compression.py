# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2023
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

import bz2
import gzip
import struct
import shutil
from abc import ABCMeta, abstractmethod
from io import BytesIO

from crc32c import crc32c

from barman.utils import with_metaclass


def _try_import_snappy():
    try:
        from cramjam import snappy
    except ImportError:
        raise SystemExit("Missing required python module: python-snappy")
    return snappy


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


def _masked_crc32c(data):
    # see the framing format specification
    crc = crc32c(data)
    return (((crc >> 15) | (crc << 17)) + 0xA282EAD8) & 0xFFFFFFFF


class UncompressError(Exception):
    pass


_STREAM_IDENTIFIER = b"sNaPpY"
_COMPRESSED_CHUNK = 0x00
_UNCOMPRESSED_CHUNK = 0x01
_IDENTIFIER_CHUNK = 0xFF
_RESERVED_UNSKIPPABLE = (0x02, 0x80)  # chunk ranges are [inclusive, exclusive)
_RESERVED_SKIPPABLE = (0x80, 0xFF)


class SnappyDecompressor(object):
    """
    A mashup of the python-snappy StreamDecompressor, which breaks the incoming
    data into chunks (which cramjam will not do for us) and then uses cramjam
    to actually decompress the chunks.
    """

    __slots__ = ["_buf", "_header_found", "_decompressor"]

    def __init__(self, snappy):
        self._buf = bytearray()
        self._header_found = False
        self._decompressor = snappy

    @staticmethod
    def check_format(data):
        """Checks that the given data starts with snappy framing format
        stream identifier.
        Raises UncompressError if it doesn't start with the identifier.
        :return: None
        """
        if len(data) < 6:
            raise UncompressError("Too short data length")
        chunk_type = struct.unpack("<L", data[:4])[0]
        size = chunk_type >> 8
        chunk_type &= 0xFF
        if chunk_type != _IDENTIFIER_CHUNK or size != len(_STREAM_IDENTIFIER):
            raise UncompressError("stream missing snappy identifier")
        chunk = data[4 : 4 + size]
        if chunk != _STREAM_IDENTIFIER:
            raise UncompressError("stream has invalid snappy identifier")

    def decompress(self, data):
        """Decompress 'data', returning a string containing the uncompressed
        data corresponding to at least part of the data in string. This data
        should be concatenated to the output produced by any preceding calls to
        the decompress() method. Some of the input data may be preserved in
        internal buffers for later processing.
        """
        self._buf.extend(data)
        uncompressed = bytearray()
        while True:
            if len(self._buf) < 4:
                return bytes(uncompressed)
            chunk_type = struct.unpack("<L", self._buf[:4])[0]
            size = chunk_type >> 8
            chunk_type &= 0xFF
            if not self._header_found:
                if chunk_type != _IDENTIFIER_CHUNK or size != len(_STREAM_IDENTIFIER):
                    raise UncompressError("stream missing snappy identifier")
                self._header_found = True
            if (
                _RESERVED_UNSKIPPABLE[0] <= chunk_type
                and chunk_type < _RESERVED_UNSKIPPABLE[1]
            ):
                raise UncompressError("stream received unskippable but unknown chunk")
            if len(self._buf) < 4 + size:
                return bytes(uncompressed)
            chunk, self._buf = self._buf[4 : 4 + size], self._buf[4 + size :]
            if chunk_type == _IDENTIFIER_CHUNK:
                if chunk != _STREAM_IDENTIFIER:
                    raise UncompressError("stream has invalid snappy identifier")
                continue
            if (
                _RESERVED_SKIPPABLE[0] <= chunk_type
                and chunk_type < _RESERVED_SKIPPABLE[1]
            ):
                continue
            assert chunk_type in (_COMPRESSED_CHUNK, _UNCOMPRESSED_CHUNK)
            crc, chunk = chunk[:4], chunk[4:]
            if chunk_type == _COMPRESSED_CHUNK:
                chunk = self._decompressor.decompress_raw(chunk)
            if struct.pack("<L", _masked_crc32c(chunk)) != crc:
                raise UncompressError("crc mismatch")
            uncompressed += chunk

    def flush(self):
        """All pending input is processed, and a string containing the
        remaining uncompressed output is returned. After calling flush(), the
        decompress() method cannot be called again; the only realistic action
        is to delete the object.
        """
        if self._buf != b"":
            raise UncompressError("chunk truncated")
        return b""


class SnappyCompressor(ChunkedCompressor):
    """
    A ChunkedCompressor implementation based on python-snappy
    """

    def __init__(self):
        snappy = _try_import_snappy()
        self.compressor = snappy.Compressor()
        self.decompressor = SnappyDecompressor(snappy)

    def add_chunk(self, data):
        """
        Compresses the supplied data and returns all the compressed bytes.

        :param bytes data: The chunk of data to be compressed
        :return: The compressed data
        :rtype: bytes
        """
        self.compressor.compress(data)
        # TODO also finish() at some point
        return self.compressor.flush()

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


def compress(wal_file, compression):
    """
    Compresses the supplied wal_file and returns a file-like object containing the
    compressed data.
    :param IOBase wal_file: A file-like object containing the WAL file data.
    :param str compression: The compression algorithm to apply. Can be one of:
      bzip2, gzip, snappy.
    :return: The compressed data
    :rtype: BytesIO
    """
    if compression == "snappy":
        in_mem_snappy = BytesIO()
        snappy = _try_import_snappy()
        snappy.stream_compress(wal_file, in_mem_snappy)
        in_mem_snappy.seek(0)
        return in_mem_snappy
    elif compression == "gzip":
        # Create a BytesIO for in memory compression
        in_mem_gzip = BytesIO()
        with gzip.GzipFile(fileobj=in_mem_gzip, mode="wb") as gz:
            # copy the gzipped data in memory
            shutil.copyfileobj(wal_file, gz)
        in_mem_gzip.seek(0)
        return in_mem_gzip
    elif compression == "bzip2":
        # Create a BytesIO for in memory compression
        in_mem_bz2 = BytesIO(bz2.compress(wal_file.read()))
        in_mem_bz2.seek(0)
        return in_mem_bz2
    else:
        raise ValueError("Unknown compression type: %s" % compression)


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


def decompress_to_file(blob, dest_file, compression):
    """
    Decompresses the supplied blob of data into the dest_file file-like object using
    the specified compression.

    :param IOBase blob: A file-like object containing the compressed data.
    :param IOBase dest_file: A file-like object into which the uncompressed data
      should be written.
    :param str compression: The compression algorithm to apply. Can be one of:
      bzip2, gzip, snappy.
    :rtype: None
    """
    if compression == "snappy":
        snappy = _try_import_snappy()
        snappy.stream_decompress(blob, dest_file)
        return
    elif compression == "gzip":
        source_file = gzip.GzipFile(fileobj=blob, mode="rb")
    elif compression == "bzip2":
        source_file = bz2.BZ2File(blob, "rb")
    else:
        raise ValueError("Unknown compression type: %s" % compression)

    with source_file:
        shutil.copyfileobj(source_file, dest_file)
