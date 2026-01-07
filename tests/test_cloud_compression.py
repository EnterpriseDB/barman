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

"""
Tests for barman.clients.cloud_compression module.
"""

import pytest

from barman.clients.cloud_compression import (
    ChunkedCompressor,
    Lz4Compressor,
    SnappyCompressor,
    get_compressor,
    get_streaming_tar_mode,
)


class TestGetCompressor:
    """Tests for the get_compressor helper function."""

    def test_returns_snappy_compressor_for_snappy(self):
        """Verify get_compressor returns SnappyCompressor for 'snappy'."""
        compressor = get_compressor("snappy")
        assert isinstance(compressor, SnappyCompressor)

    def test_returns_lz4_compressor_for_lz4(self):
        """Verify get_compressor returns Lz4Compressor for 'lz4'."""
        compressor = get_compressor("lz4")
        assert isinstance(compressor, Lz4Compressor)

    def test_returns_none_for_gzip(self):
        """Verify get_compressor returns None for TarFile-native compression."""
        compressor = get_compressor("gz")
        assert compressor is None

    def test_returns_none_for_bzip2(self):
        """Verify get_compressor returns None for TarFile-native compression."""
        compressor = get_compressor("bz2")
        assert compressor is None

    def test_returns_none_for_none(self):
        """Verify get_compressor returns None for None compression."""
        compressor = get_compressor(None)
        assert compressor is None


class TestGetStreamingTarMode:
    """Tests for the get_streaming_tar_mode helper function."""

    @pytest.mark.parametrize(
        "mode,compression,expected",
        [
            ("w", None, "w|"),
            ("r", None, "r|"),
            ("w", "snappy", "w|"),
            ("r", "snappy", "r|"),
            ("w", "lz4", "w|"),
            ("r", "lz4", "r|"),
            ("w", "gz", "w|gz"),
            ("r", "gz", "r|gz"),
            ("w", "bz2", "w|bz2"),
            ("r", "bz2", "r|bz2"),
        ],
    )
    def test_returns_correct_mode(self, mode, compression, expected):
        """Verify get_streaming_tar_mode returns correct tar mode string."""
        result = get_streaming_tar_mode(mode, compression)
        assert result == expected


class TestLz4Compressor:
    """Tests for the Lz4Compressor class."""

    def test_inherits_from_chunked_compressor(self):
        """Verify Lz4Compressor is a ChunkedCompressor."""
        compressor = Lz4Compressor()
        assert isinstance(compressor, ChunkedCompressor)

    def test_compress_and_decompress_single_chunk(self):
        """Verify round-trip compression/decompression of a single chunk."""
        compressor = Lz4Compressor()
        original_data = b"Hello, World! This is test data for lz4 compression."

        # Compress
        compressed = compressor.add_chunk(original_data)
        compressed += compressor.flush()

        # Decompress with a new compressor instance
        decompressor = Lz4Compressor()
        decompressed = decompressor.decompress(compressed)

        assert decompressed == original_data

    def test_compress_and_decompress_multiple_chunks(self):
        """Verify round-trip compression/decompression of multiple chunks."""
        compressor = Lz4Compressor()
        chunks = [
            b"First chunk of data. " * 100,
            b"Second chunk of data. " * 100,
            b"Third chunk of data. " * 100,
        ]

        # Compress all chunks
        compressed_data = b""
        for chunk in chunks:
            compressed_data += compressor.add_chunk(chunk)
        compressed_data += compressor.flush()

        # Decompress
        decompressor = Lz4Compressor()
        decompressed = decompressor.decompress(compressed_data)

        assert decompressed == b"".join(chunks)

    def test_flush_returns_bytes(self):
        """Verify flush returns bytes object."""
        compressor = Lz4Compressor()
        compressor.add_chunk(b"test data")
        result = compressor.flush()
        assert isinstance(result, bytes)

    def test_flush_without_compression_returns_empty_bytes(self):
        """Verify flush returns empty bytes if no compression was done."""
        compressor = Lz4Compressor()
        result = compressor.flush()
        assert result == b""

    def test_streaming_decompression(self):
        """Verify decompression works when data arrives in small chunks."""
        compressor = Lz4Compressor()
        original_data = b"Test data for streaming decompression. " * 50

        # Compress
        compressed = compressor.add_chunk(original_data)
        compressed += compressor.flush()

        # Decompress in small chunks (simulating streaming)
        decompressor = Lz4Compressor()
        decompressed = b""
        chunk_size = 64  # Small chunks to simulate streaming
        for i in range(0, len(compressed), chunk_size):
            chunk = compressed[i : i + chunk_size]
            decompressed += decompressor.decompress(chunk)

        assert decompressed == original_data

    def test_compression_ratio(self):
        """Verify lz4 actually compresses repetitive data."""
        compressor = Lz4Compressor()
        # Highly repetitive data should compress well
        original_data = b"AAAA" * 10000

        compressed = compressor.add_chunk(original_data)
        compressed += compressor.flush()

        # Compressed data should be smaller than original
        assert len(compressed) < len(original_data)

    def test_large_data_compression(self):
        """Verify lz4 handles large data (simulating 10MB+ backup chunks)."""
        compressor = Lz4Compressor()
        # Create 5MB of random-ish data
        original_data = (b"PostgreSQL backup data block %d. " % i for i in range(150000))
        original_data = b"".join(original_data)

        # Compress in multiple chunks (simulating CloudTarUploader behavior)
        chunk_size = 1024 * 1024  # 1MB chunks
        compressed_data = b""
        for i in range(0, len(original_data), chunk_size):
            chunk = original_data[i : i + chunk_size]
            compressed_data += compressor.add_chunk(chunk)
        compressed_data += compressor.flush()

        # Decompress
        decompressor = Lz4Compressor()
        decompressed = decompressor.decompress(compressed_data)

        assert decompressed == original_data

    def test_empty_chunk_handling(self):
        """Verify lz4 handles empty chunks correctly."""
        compressor = Lz4Compressor()

        # Add some data first
        compressed = compressor.add_chunk(b"some data")
        # Add empty chunk
        compressed += compressor.add_chunk(b"")
        # Add more data
        compressed += compressor.add_chunk(b" more data")
        compressed += compressor.flush()

        decompressor = Lz4Compressor()
        decompressed = decompressor.decompress(compressed)

        assert decompressed == b"some data more data"

    def test_flush_called_multiple_times(self):
        """Verify flush can be called multiple times safely."""
        compressor = Lz4Compressor()
        compressor.add_chunk(b"test data")

        # First flush should return end marker
        result1 = compressor.flush()
        assert isinstance(result1, bytes)

        # Subsequent flushes should return empty bytes
        result2 = compressor.flush()
        assert result2 == b""

    def test_decompressor_reuse(self):
        """Verify a single decompressor can decompress multiple frames."""
        # Compress two separate pieces of data
        compressor1 = Lz4Compressor()
        data1 = b"First piece of data"
        compressed1 = compressor1.add_chunk(data1) + compressor1.flush()

        compressor2 = Lz4Compressor()
        data2 = b"Second piece of data"
        compressed2 = compressor2.add_chunk(data2) + compressor2.flush()

        # Decompress both with separate decompressors (each frame needs its own)
        decompressor1 = Lz4Compressor()
        decompressed1 = decompressor1.decompress(compressed1)
        assert decompressed1 == data1

        decompressor2 = Lz4Compressor()
        decompressed2 = decompressor2.decompress(compressed2)
        assert decompressed2 == data2


class TestSnappyCompressor:
    """Tests for the SnappyCompressor class."""

    def test_inherits_from_chunked_compressor(self):
        """Verify SnappyCompressor is a ChunkedCompressor."""
        compressor = SnappyCompressor()
        assert isinstance(compressor, ChunkedCompressor)

    def test_compress_and_decompress_single_chunk(self):
        """Verify round-trip compression/decompression of a single chunk."""
        compressor = SnappyCompressor()
        original_data = b"Hello, World! This is test data for snappy compression."

        # Compress
        compressed = compressor.add_chunk(original_data)

        # Decompress with a new compressor instance
        decompressor = SnappyCompressor()
        decompressed = decompressor.decompress(compressed)

        assert decompressed == original_data

    def test_flush_returns_empty_bytes(self):
        """Verify flush returns empty bytes (snappy doesn't need finalization)."""
        compressor = SnappyCompressor()
        compressor.add_chunk(b"test data")
        result = compressor.flush()
        assert result == b""


class TestChunkedCompressorInterface:
    """Tests for the ChunkedCompressor abstract interface."""

    @pytest.mark.parametrize("compression", ["snappy", "lz4"])
    def test_compressor_has_required_methods(self, compression):
        """Verify all compressors implement required interface methods."""
        compressor = get_compressor(compression)
        assert hasattr(compressor, "add_chunk")
        assert hasattr(compressor, "decompress")
        assert hasattr(compressor, "flush")
        assert callable(compressor.add_chunk)
        assert callable(compressor.decompress)
        assert callable(compressor.flush)

    @pytest.mark.parametrize("compression", ["snappy", "lz4"])
    def test_compressor_roundtrip(self, compression):
        """Verify all compressors can round-trip data correctly."""
        compressor = get_compressor(compression)
        original_data = b"Test data for compression round-trip."

        compressed = compressor.add_chunk(original_data)
        compressed += compressor.flush()

        decompressor = get_compressor(compression)
        decompressed = decompressor.decompress(compressed)

        assert decompressed == original_data
