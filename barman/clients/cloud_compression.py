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

import bz2
import gzip
import lzma
import shutil
from abc import ABCMeta, abstractmethod
from io import BytesIO

from barman.compression import _try_import_lz4, _try_import_zstd
from barman.utils import with_metaclass


def _try_import_snappy():
    try:
        import snappy
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


def compress(wal_file, compression):
    """
    Compresses the supplied wal_file and returns a file-like object containing the
    compressed data.
    :param IOBase wal_file: A file-like object containing the WAL file data.
    :param str compression: The compression algorithm to apply. Can be one of:
      bzip2, gzip, snappy, zstd, lz4, xz.
    :return: The compressed data
    :rtype: BytesIO
    """
    if compression == "snappy":
        in_mem_snappy = BytesIO()
        snappy = _try_import_snappy()
        snappy.stream_compress(wal_file, in_mem_snappy)
        in_mem_snappy.seek(0)
        return in_mem_snappy
    elif compression == "zstd":
        in_mem_zstd = BytesIO()
        zstd = _try_import_zstd()
        zstd.ZstdCompressor().copy_stream(wal_file, in_mem_zstd)
        in_mem_zstd.seek(0)
        return in_mem_zstd
    elif compression == "lz4":
        lz4 = _try_import_lz4()
        in_mem_lz4 = BytesIO(lz4.frame.compress(wal_file.read()))
        in_mem_lz4.seek(0)
        return in_mem_lz4
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
    elif compression == "xz":
        in_mem_xz = BytesIO(lzma.compress(wal_file.read()))
        in_mem_xz.seek(0)
        return in_mem_xz
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
      bzip2, gzip, snappy, zstd, lz4, xz.
    :rtype: None
    """
    if compression == "snappy":
        snappy = _try_import_snappy()
        snappy.stream_decompress(blob, dest_file)
        return
    elif compression == "zstd":
        zstd = _try_import_zstd()
        source_file = zstd.ZstdDecompressor().stream_reader(blob)
    elif compression == "lz4":
        lz4 = _try_import_lz4()
        source_file = lz4.frame.open(blob, mode="rb")
    elif compression == "gzip":
        source_file = gzip.GzipFile(fileobj=blob, mode="rb")
    elif compression == "bzip2":
        source_file = bz2.BZ2File(blob, "rb")
    elif compression == "xz":
        source_file = lzma.open(blob, "rb")
    else:
        raise ValueError("Unknown compression type: %s" % compression)

    with source_file:
        shutil.copyfileobj(source_file, dest_file)
