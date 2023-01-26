# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2023
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

import base64
import os

import mock
import pytest
import tarfile
import io
from barman.compression import (
    BZip2Compressor,
    CommandCompressor,
    CompressionManager,
    Compressor,
    CustomCompressor,
    GZipCompressor,
    PyBZip2Compressor,
    PyGZipCompressor,
    get_pg_basebackup_compression,
    PgBaseBackupCompression,
    PgBaseBackupCompressionOption,
    GZipPgBaseBackupCompressionOption,
    GZipCompression,
    LZ4PgBaseBackupCompressionOption,
    LZ4Compression,
    ZSTDPgBaseBackupCompressionOption,
    ZSTDCompression,
)
from barman.exceptions import (
    CompressionException,
    FileNotFoundException,
    CommandFailedException,
)
from testing_helpers import build_mocked_server, get_compression_config

# Filename patterns used by the tests
ZIP_FILE = "%s/zipfile.zip"
ZIP_FILE_UNCOMPRESSED = "%s/zipfile.uncompressed"
BZIP2_FILE = "%s/bzipfile.bz2"
BZIP2_FILE_UNCOMPRESSED = "%s/bzipfile.uncompressed"


def _tar_file(items):
    """Helper to create an in-memory tar file with multiple files."""
    tar_fileobj = io.BytesIO()
    tf = tarfile.TarFile.open(mode="w|", fileobj=tar_fileobj)
    for item_name, item_bytes in items:
        ti = tarfile.TarInfo(name=item_name)
        content_as_bytes = item_bytes.encode("utf-8")
        ti.size = len(content_as_bytes)
        tf.addfile(ti, io.BytesIO(content_as_bytes))
    tf.close()
    tar_fileobj.seek(0)
    return tar_fileobj


# noinspection PyMethodMayBeStatic
class TestCompressionManager(object):
    def test_compression_manager_creation(self):
        # prepare mock obj
        config_mock = mock.Mock()
        comp_manager = CompressionManager(config_mock, None)
        assert comp_manager

    def test_check_compression_none(self):
        # prepare mock obj
        config_mock = mock.Mock()
        config_mock.compression = "custom"
        comp_manager = CompressionManager(config_mock, None)
        assert comp_manager.check() is True

    def test_check_with_compression(self):
        # prepare mock obj
        config_mock = mock.Mock()
        comp_manager = CompressionManager(config_mock, None)
        assert comp_manager.check("test_compression") is False

    @pytest.fixture(scope="function")
    def _reset_custom_compressor(self):
        """
        A function-scoped fixture which explicitly sets the MAGIC class property
        of the CustomCompressor to None in order to reset the class to its initial
        state so as to prevent side effects between unit tests.
        """
        CustomCompressor.MAGIC = None

    def test_get_compressor_custom(self, _reset_custom_compressor):
        # GIVEN a Barman config which specifies custom compression
        config_mock = mock.Mock()
        config_mock.compression = "custom"
        config_mock.custom_compression_filter = "test_custom_compression_filter"
        config_mock.custom_decompression_filter = "test_custom_decompression_filter"
        # AND the custom compression magic bytes are set
        config_mock.custom_compression_magic = "0x28b52ffd"

        # WHEN the compression manager is created
        comp_manager = CompressionManager(config_mock, None)

        # THEN a default compressor can be obtained
        assert comp_manager.get_default_compressor() is not None

        # AND the magic bytes of the compressor match those in the config
        assert comp_manager.get_default_compressor().MAGIC == b"\x28\xb5\x2f\xfd"

        # AND unidentified_compression is set to None as there is no need
        # to make the legacy assumption that unidentified compression means
        # custom compression
        assert comp_manager.unidentified_compression is None

        # AND the value of MAGIC_MAX_LENGTH equals the length of the magic bytes
        assert comp_manager.MAGIC_MAX_LENGTH == 4

    def test_get_compressor_custom_nomagic(self, _reset_custom_compressor):
        # GIVEN a Barman config which specifies custom compression
        config_mock = mock.Mock()
        config_mock.compression = "custom"
        config_mock.custom_compression_filter = "test_custom_compression_filter"
        config_mock.custom_decompression_filter = "test_custom_decompression_filter"
        # AND no magic bytes are set
        config_mock.custom_compression_magic = None

        # WHEN the compression manager is created
        comp_manager = CompressionManager(config_mock, None)

        # THEN a default compressor can be obtained
        assert comp_manager.get_default_compressor() is not None

        # AND the magic bytes of the compressor are None
        assert comp_manager.get_default_compressor().MAGIC is None

        # AND unidentified_compression is set to "custom" as this assumption
        # is the legacy way of identifying custom compression, used when magic
        # bytes is not set
        assert comp_manager.unidentified_compression == "custom"

        # AND the value of MAGIC_MAX_LENGTH equals the max length of the default
        # compressions
        assert comp_manager.MAGIC_MAX_LENGTH == 3

    def test_get_compressor_gzip(self):
        # prepare mock obj
        config_mock = mock.Mock()
        config_mock.compression = "gzip"

        # check custom compression method creation
        comp_manager = CompressionManager(config_mock, None)
        assert comp_manager.get_default_compressor() is not None

    def test_get_compressor_bzip2(self):
        # prepare mock obj
        config_mock = mock.Mock()
        config_mock.compression = "bzip2"

        # check custom compression method creation
        comp_manager = CompressionManager(config_mock, None)
        assert comp_manager.get_default_compressor() is not None

    def test_get_compressor_invalid(self):
        # prepare mock obj
        config_mock = mock.Mock()

        # check custom compression method creation
        comp_manager = CompressionManager(config_mock, None)
        assert comp_manager.get_compressor("test_compression") is None

    def test_identify_compression(self, tmpdir):
        # prepare mock obj
        config_mock = mock.Mock()
        config_mock.compression = "bzip2"

        # check custom compression method creation
        comp_manager = CompressionManager(config_mock, None)
        assert comp_manager.get_default_compressor() is not None

        bz2_tmp_file = tmpdir.join("test_file")
        # "test" in bz2 compression
        bz2_tmp_file.write(
            base64.b64decode(
                b"QlpoOTFBWSZTWczDcdQAAAJBgAAQAgAMACAAIZpoM00Zl4u5IpwoSGZhuOoA"
            ),
            mode="wb",
        )

        compression_bz2 = comp_manager.identify_compression(bz2_tmp_file.strpath)

        assert compression_bz2 == "bzip2"

        zip_tmp_file = tmpdir.join("test_file")
        # "test" in bz2 compression
        zip_tmp_file.write(
            base64.b64decode(b"H4sIAF0ssFIAAytJLS7hAgDGNbk7BQAAAA=="), mode="wb"
        )

        # check custom compression method creation
        compression_zip = comp_manager.identify_compression(zip_tmp_file.strpath)
        assert compression_zip == "gzip"


class TestCompressor(object):
    """Test the class methods of the base class for the compressors"""

    @pytest.mark.parametrize(
        ("magic", "bytes_to_validate", "is_valid"),
        [
            (b"\x42\x5a\x68", b"\x42\x5a\x68", True),
            (b"\x42\x5a\x68", b"\x42\x5a\x68\xff\x12\x00", True),
            (b"\x42\x5a\x68", b"\x42\x5b\x68\xff\x12\x00", False),
            (b"\x42", b"\x42\x5b\x68\xff\x12\x00", True),
            (b"\x42", b"", False),
        ],
    )
    def test_validate(self, magic, bytes_to_validate, is_valid):
        """Verifies the validate class method behaviour"""
        # GIVEN a Compressor class with a specific MAGIC
        Compressor.MAGIC = magic

        # WHEN validate is called with a string of bytes starting with the MAGIC
        # THEN validate returns True
        assert Compressor.validate(bytes_to_validate) is is_valid


# noinspection PyMethodMayBeStatic
class TestCommandCompressors(object):
    def test_creation(self):
        # Prepare mock obj
        config_mock = mock.Mock()

        compressor = CommandCompressor(
            config=config_mock, compression="dummy_compressor"
        )

        assert compressor is not None
        assert compressor.config == config_mock
        assert compressor.compression == "dummy_compressor"

    def test_build_command(self):
        # prepare mock obj
        config_mock = mock.Mock()

        compressor = CommandCompressor(
            config=config_mock, compression="dummy_compressor"
        )

        command = compressor._build_command("dummy_command")

        assert (
            command.cmd == "barman_command()"
            '{ dummy_command > "$2" < "$1";}; barman_command'
        )

    def test_gzip(self, tmpdir):

        config_mock = mock.Mock()

        compression_manager = CompressionManager(config_mock, tmpdir.strpath)

        compressor = GZipCompressor(config=config_mock, compression="gzip")

        src = tmpdir.join("sourcefile")
        src.write("content")

        compressor.compress(src.strpath, ZIP_FILE % tmpdir.strpath)
        assert os.path.exists(ZIP_FILE % tmpdir.strpath)
        compression_zip = compression_manager.identify_compression(
            ZIP_FILE % tmpdir.strpath
        )
        assert compression_zip == "gzip"

        compressor.decompress(
            ZIP_FILE % tmpdir.strpath,
            ZIP_FILE_UNCOMPRESSED % tmpdir.strpath,
        )

        f = open(ZIP_FILE_UNCOMPRESSED % tmpdir.strpath).read()
        assert f == "content"

    def test_bzip2(self, tmpdir):

        config_mock = mock.Mock()

        compression_manager = CompressionManager(config_mock, tmpdir.strpath)

        compressor = BZip2Compressor(config=config_mock, compression="bzip2")

        src = tmpdir.join("sourcefile")
        src.write("content")

        compressor.compress(src.strpath, BZIP2_FILE % tmpdir.strpath)
        assert os.path.exists(BZIP2_FILE % tmpdir.strpath)
        compression_zip = compression_manager.identify_compression(
            BZIP2_FILE % tmpdir.strpath
        )
        assert compression_zip == "bzip2"

        compressor.decompress(
            BZIP2_FILE % tmpdir.strpath,
            BZIP2_FILE_UNCOMPRESSED % tmpdir.strpath,
        )

        f = open(BZIP2_FILE_UNCOMPRESSED % tmpdir.strpath).read()
        assert f == "content"


# noinspection PyMethodMayBeStatic
class TestInternalCompressors(object):
    def test_gzip(self, tmpdir):

        config_mock = mock.Mock()

        compression_manager = CompressionManager(config_mock, tmpdir.strpath)

        compressor = PyGZipCompressor(config=config_mock, compression="pygzip")

        src = tmpdir.join("sourcefile")
        src.write("content")

        compressor.compress(src.strpath, ZIP_FILE % tmpdir.strpath)
        assert os.path.exists(ZIP_FILE % tmpdir.strpath)
        compression_zip = compression_manager.identify_compression(
            ZIP_FILE % tmpdir.strpath
        )
        assert compression_zip == "gzip"

        compressor.decompress(
            ZIP_FILE % tmpdir.strpath,
            ZIP_FILE_UNCOMPRESSED % tmpdir.strpath,
        )

        f = open(ZIP_FILE_UNCOMPRESSED % tmpdir.strpath).read()
        assert f == "content"

    def test_bzip2(self, tmpdir):

        config_mock = mock.Mock()

        compression_manager = CompressionManager(config_mock, tmpdir.strpath)

        compressor = PyBZip2Compressor(config=config_mock, compression="pybzip2")

        src = tmpdir.join("sourcefile")
        src.write("content")

        compressor.compress(src.strpath, BZIP2_FILE % tmpdir.strpath)
        assert os.path.exists(BZIP2_FILE % tmpdir.strpath)
        compression_zip = compression_manager.identify_compression(
            BZIP2_FILE % tmpdir.strpath,
        )
        assert compression_zip == "bzip2"

        compressor.decompress(
            BZIP2_FILE % tmpdir.strpath,
            BZIP2_FILE_UNCOMPRESSED % tmpdir.strpath,
        )

        f = open(BZIP2_FILE_UNCOMPRESSED % tmpdir.strpath).read()
        assert f == "content"


# noinspection PyMethodMayBeStatic
class TestCustomCompressor(object):
    def test_custom_compressor_creation(self):
        config_mock = mock.Mock()
        config_mock.custom_compression_filter = "dummy_compression_filter"
        config_mock.custom_decompression_filter = "dummy_decompression_filter"

        compressor = CustomCompressor(config=config_mock, compression="custom")

        assert compressor is not None
        assert compressor._compress.cmd == (
            'barman_command(){ dummy_compression_filter > "$2" < "$1";}; '
            "barman_command"
        )
        assert compressor._decompress.cmd == (
            'barman_command(){ dummy_decompression_filter > "$2" < "$1";}; '
            "barman_command"
        )


class TestPgBaseBackupCompression(object):
    """
    Test the classes used to encapsulate implementation details of backups taken
    with pg_basebackup compression.
    """

    @pytest.mark.parametrize(
        (
            "compression",
            "expected_class",
            "expected_option_class",
            "expected_compression_class",
        ),
        [
            # A value of None for backup_compression should result in a NoneType
            (None, type(None), None, None),
            # A value of gzip for backup_compression should result in a
            # PgBaseBackupCompression with appropriate attributes
            (
                "gzip",
                PgBaseBackupCompression,
                GZipPgBaseBackupCompressionOption,
                GZipCompression,
            ),
            # Test lz4 scenario
            (
                "lz4",
                PgBaseBackupCompression,
                LZ4PgBaseBackupCompressionOption,
                LZ4Compression,
            ),
            # Test zstd scenario
            (
                "zstd",
                PgBaseBackupCompression,
                ZSTDPgBaseBackupCompressionOption,
                ZSTDCompression,
            ),
        ],
    )
    def test_get_pg_basebackup_compression(
        self,
        compression,
        expected_class,
        expected_option_class,
        expected_compression_class,
    ):
        """
        Verifies that get_pg_basebackup_compression returns an instance of the
        correct class for the compression specified in the server config.
        """
        server = build_mocked_server(
            global_conf={"backup_method": "postgres", "backup_compression": compression}
        )
        base_backup_compression = get_pg_basebackup_compression(server)

        assert type(base_backup_compression) == expected_class
        if base_backup_compression is not None:
            assert type(base_backup_compression.options) == expected_option_class
            assert (
                type(base_backup_compression.compression) == expected_compression_class
            )

    def test_get_pg_basebackup_compression_not_supported(self):
        """
        Verifies that get_pg_basebackup_compression raises an exception if it is
        asked for a compression we do not support.
        In practice such errors would be caught at config validation time however
        this exception will make debugging easier in the event that a validation bug
        allows an unsupported compression through.
        """
        server = build_mocked_server(global_conf={"backup_method": "postgres"})
        # Set backup_compression directly so that config validation doesn't catch it
        server.config.backup_compression = "rle"
        with pytest.raises(CompressionException) as exc:
            get_pg_basebackup_compression(server)

        assert "Barman does not support pg_basebackup compression: rle" in str(
            exc.value
        )

    def test_with_suffix(self):
        """Verifies with_suffix returns expected result."""
        # GIVEN a PgBaseBackupCompression instance
        compression_mock = mock.Mock()
        compression_mock.file_extension = "tar.gz"
        base_backup_compression = PgBaseBackupCompression(
            mock.Mock(), mock.Mock(), compression_mock
        )

        # THEN with_suffix calls compression with_suffix method
        full_archive_name = base_backup_compression.with_suffix("append_to_this")
        assert full_archive_name == "append_to_this.tar.gz"


class TestPgBaseBackupCompressionOption:
    @pytest.mark.parametrize(
        ("client_version", "server_version", "compression_options", "expected_errors"),
        [
            # For pg_basebackup < 15 backup_location = client is allowed for any server
            # version because it is the only option supported by the client
            (
                "14",
                140000,
                {"backup_compression": "gzip", "backup_compression_location": "client"},
                [],
            ),
            (
                "14",
                150000,
                {"backup_compression": "gzip", "backup_compression_location": "client"},
                [],
            ),
            # backup_location = server is not allowed for pg_basebackup < 15 regardless
            # of server version
            (
                "14",
                140000,
                {"backup_compression": "gzip", "backup_compression_location": "server"},
                [
                    "backup_compression_location = server requires pg_basebackup 15 or greater",
                    "backup_compression_location = server requires PostgreSQL 15 or greater",
                ],
            ),
            (
                "14",
                150000,
                {"backup_compression": "gzip", "backup_compression_location": "server"},
                [
                    "backup_compression_location = server requires pg_basebackup 15 or greater",
                ],
            ),
            # For pg_basebackup >= 15 and PG < 15, backup_location = client is allowed
            # implicitly because it is the only available option supported by the server
            (
                "15",
                140000,
                {"backup_compression": "gzip", "backup_compression_location": "client"},
                [],
            ),
            # For pg_basebackup >= 15 and PG >= 15, both client and server are allowed
            # because they are both supported on the client and the server
            (
                "15",
                150000,
                {"backup_compression": "gzip", "backup_compression_location": "client"},
                [],
            ),
            (
                "15",
                150000,
                {"backup_compression": "gzip", "backup_compression_location": "server"},
                [],
            ),
            # backup_compression_format = tar is allowed regardless of compression location
            (
                "15",
                150000,
                {
                    "backup_compression": "gzip",
                    "backup_compression_location": "client",
                    "backup_compression_format": "tar",
                },
                [],
            ),
            (
                "15",
                150000,
                {
                    "backup_compression": "gzip",
                    "backup_compression_location": "server",
                    "backup_compression_format": "tar",
                },
                [],
            ),
            # backup_compression_format = plain is allowed if compression location = server
            (
                "15",
                150000,
                {
                    "backup_compression": "gzip",
                    "backup_compression_location": "server",
                    "backup_compression_format": "plain",
                },
                [],
            ),
            # backup_compression_format = plain is not allowed if compression is
            # performed on the client
            (
                "15",
                150000,
                {
                    "backup_compression": "gzip",
                    "backup_compression_location": "client",
                    "backup_compression_format": "plain",
                },
                [
                    "backup_compression_format plain is not compatible with backup_compression_location client"
                ],
            ),
        ],
    )
    def test_validate(
        self, client_version, server_version, compression_options, expected_errors
    ):
        # GIVEN a PgBaseBackupCompressionOption with specific options
        compression_config = get_compression_config(compression_options)
        compression_option = PgBaseBackupCompressionOption(compression_config)
        # AND the remote status reports the pg_basebackup version
        remote_status = {"pg_basebackup_version": client_version}

        # WHEN validate is called
        validation_issues = compression_option.validate(server_version, remote_status)
        # Then the expected errors are in the returned list if any
        assert len(validation_issues) == len(expected_errors)
        assert sorted(validation_issues) == sorted(expected_errors)


class TestGZipPgBaseBackupCompressionOption(object):
    @pytest.mark.parametrize(
        ("compression_options", "expected_errors"),
        [
            ({"backup_compression": "gzip", "backup_compression_level": 1}, []),
            ({"backup_compression": "gzip", "backup_compression_level": 9}, []),
            (
                {"backup_compression": "gzip", "backup_compression_level": 0},
                [
                    "backup_compression_level 0 unsupported by pg_basebackup compression gzip"
                ],
            ),
            (
                {"backup_compression": "gzip", "backup_compression_level": 10},
                [
                    "backup_compression_level 10 unsupported by pg_basebackup compression gzip"
                ],
            ),
            (
                {
                    "backup_compression": "gzip",
                    "backup_compression_level": 9,
                    "backup_compression_workers": 3,
                },
                ["backup_compression_workers is not compatible with compression gzip"],
            ),
        ],
    )
    def test_validate(self, compression_options, expected_errors):
        """Verifies supported config options pass validation."""
        # GIVEN compression options
        compression_config = get_compression_config(compression_options)
        compression_option = GZipPgBaseBackupCompressionOption(compression_config)
        # AND a remote_status object
        remote_status = mock.Mock()
        # AND a Server version
        server_version = mock.Mock()
        # WHEN the compression is validated
        validation_issues = compression_option.validate(server_version, remote_status)
        # THEN issues should match expected ones if any
        assert len(validation_issues) == len(expected_errors)
        assert sorted(validation_issues) == sorted(expected_errors)


class TestLZ4PgBaseBackupCompressionOption(object):
    @pytest.mark.parametrize(
        ("client_version", "server_version", "compression_options", "expected_errors"),
        [
            (
                "15",
                15000,
                {"backup_compression": "lz4", "backup_compression_level": 1},
                [],
            ),
            (
                "15",
                14000,
                {"backup_compression": "lz4", "backup_compression_level": 9},
                [],
            ),
            (
                "15",
                15000,
                {"backup_compression": "lz4", "backup_compression_level": 0},
                [
                    "backup_compression_level 0 unsupported by pg_basebackup compression lz4"
                ],
            ),
            (
                "15",
                15000,
                {"backup_compression": "lz4", "backup_compression_level": 13},
                [
                    "backup_compression_level 13 unsupported by pg_basebackup compression lz4"
                ],
            ),
            (
                "14",
                15000,
                {"backup_compression": "lz4", "backup_compression_level": 14},
                [
                    "backup_compression = lz4 requires pg_basebackup 15 or greater",
                    "backup_compression_level 14 unsupported by pg_basebackup compression lz4",
                ],
            ),
            (
                "15",
                14000,
                {
                    "backup_compression": "lz4",
                    "backup_compression_level": 9,
                    "backup_compression_workers": 2,
                },
                ["backup_compression_workers is not compatible with compression lz4"],
            ),
        ],
    )
    def test_validate(
        self, client_version, server_version, compression_options, expected_errors
    ):
        """Verifies supported config options pass validation."""
        # GIVEN compression options
        compression_config = get_compression_config(compression_options)
        compression_option = LZ4PgBaseBackupCompressionOption(compression_config)
        # AND a remote_status object
        remote_status = {"pg_basebackup_version": client_version}
        # WHEN the compression is validated
        validation_issues = compression_option.validate(server_version, remote_status)
        # THEN issues should match expected ones if any
        assert len(validation_issues) == len(expected_errors)
        assert sorted(validation_issues) == sorted(expected_errors)


class TestZSTDPgBaseBackupCompressionOption(object):
    @pytest.mark.parametrize(
        ("client_version", "server_version", "compression_options", "expected_errors"),
        [
            (
                "15",
                15000,
                {"backup_compression": "zstd", "backup_compression_level": 1},
                [],
            ),
            (
                "15",
                14000,
                {"backup_compression": "zstd", "backup_compression_level": 22},
                [],
            ),
            (
                "15",
                14000,
                {
                    "backup_compression": "zstd",
                    "backup_compression_level": 22,
                    "backup_compression_workers": 0,
                },
                [],
            ),
            (
                "15",
                15000,
                {"backup_compression": "zstd", "backup_compression_level": 0},
                [
                    "backup_compression_level 0 unsupported by pg_basebackup compression zstd"
                ],
            ),
            (
                "15",
                15000,
                {"backup_compression": "zstd", "backup_compression_level": 23},
                [
                    "backup_compression_level 23 unsupported by pg_basebackup compression zstd"
                ],
            ),
            (
                "14",
                15000,
                {"backup_compression": "zstd", "backup_compression_level": 23},
                [
                    "backup_compression = zstd requires pg_basebackup 15 or greater",
                    "backup_compression_level 23 unsupported by pg_basebackup compression zstd",
                ],
            ),
            (
                "14",
                15000,
                {
                    "backup_compression": "zstd",
                    "backup_compression_level": 23,
                    "backup_compression_workers": -1,
                },
                [
                    "backup_compression = zstd requires pg_basebackup 15 or greater",
                    "backup_compression_level 23 unsupported by pg_basebackup compression zstd",
                    "backup_compression_workers should be a positive integer: '-1' is invalid",
                ],
            ),
        ],
    )
    def test_validate(
        self, client_version, server_version, compression_options, expected_errors
    ):
        """Verifies supported config options pass validation."""
        # GIVEN compression options
        compression_config = get_compression_config(compression_options)
        compression_option = ZSTDPgBaseBackupCompressionOption(compression_config)
        # AND a remote_status object
        remote_status = {"pg_basebackup_version": client_version}
        # WHEN the compression is validated
        validation_issues = compression_option.validate(server_version, remote_status)
        # THEN issues should match expected ones if any
        assert len(validation_issues) == len(expected_errors)
        assert sorted(validation_issues) == sorted(expected_errors)


COMMON_UNCOMPRESS_ARGS = (
    ("src", "dst", "exclude", "include", "expected_error"),
    [
        # Simple src, dest case should cause the correct command to be called
        ("/path/to/source", "/path/to/dest", None, None, None),
        # Empty strings and None values for src or dst should raise an error
        ("", "/path/to/dest", None, None, ValueError),
        (None, "/path/to/dest", None, None, ValueError),
        ("/path/to/src", "", None, None, ValueError),
        ("/path/to/src", None, None, None, ValueError),
        # Exclude arguments should be appended
        (
            "/path/to/source",
            "/path/to/dest",
            ["/path/to/exclude", "/another/path/to/exclude"],
            None,
            None,
        ),
        # Include arguments should be appended
        (
            "/path/to/source",
            "/path/to/dest",
            None,
            ["path/to/include", "/another/path/to/include"],
            None,
        ),
        # Both include and exclude arguments should be appended
        (
            "/path/to/source",
            "/path/to/dest",
            ["/path/to/exclude", "/another/path/to/exclude"],
            ["path/to/include", "/another/path/to/include"],
            None,
        ),
    ],
)


class TestGZipCompression(object):
    @pytest.mark.parametrize(*COMMON_UNCOMPRESS_ARGS)
    def test_uncompress(self, src, dst, exclude, include, expected_error):
        # GIVEN a GZipCompression object
        command = mock.Mock()
        command.cmd.return_value = 0
        command.get_last_output.return_value = ("all good", "")
        gzip_compression = GZipCompression(command)

        # WHEN uncompress is called with the source and destination
        # THEN the command is called once
        # AND if we expect an error, that error is raised
        if expected_error is not None:
            with pytest.raises(ValueError):
                gzip_compression.uncompress(
                    src, dst, exclude=exclude, include_args=include
                )
            # THEN command.cmd was not called
            command.cmd.assert_not_called()
        # OR if we don't expect an error
        else:
            gzip_compression.uncompress(src, dst, exclude=exclude, include_args=include)
            # THEN command.cmd was called
            command.cmd.assert_called_once()
            # AND the first argument was "tar"
            assert command.cmd.call_args_list[0][0][0] == "tar"
            # AND the basic arguments are present
            assert command.cmd.call_args_list[0][1]["args"][:4] == [
                "-xzf",
                src,
                "--directory",
                dst,
            ]
            # AND if we expected exclude args they are present
            remaining_args = " ".join(command.cmd.call_args_list[0][1]["args"][4:])
            if exclude is not None:
                for exclude_arg in exclude:
                    assert "--exclude %s" % exclude_arg in remaining_args
            # AND if we expected include args they are present
            if include is not None:
                for include_arg in include:
                    assert include_arg in remaining_args

    def test_tar_failure_raises_exception(self):
        """Verify a nonzero return code from tar raises an exception"""
        # GIVEN a GZipCompression object
        # AND a tar command which returns status 2 and an error
        command = mock.Mock()
        command.cmd.return_value = 2
        command.get_last_output.return_value = ("", "some error")
        gzip_compression = GZipCompression(command)

        # WHEN uncompress is called
        # THEN a CommandFailedException is raised
        with pytest.raises(CommandFailedException) as exc:
            gzip_compression.uncompress("/path/to/src", "/path/to/dst")

        # AND the exception message contains the command stderr
        assert "some error" in str(exc.value)

    def test_get_file_content(self):
        # Given a tar.gz compressed archive on disk containing specified files
        archive_path = "/path/to/archive"
        label_file_name = "label"
        label_file_path_in_archive = os.path.join("label/file/path", label_file_name)
        file_label_content = "expected file label content"

        # AND a Mock Command
        command = mock.Mock()
        command.cmd.return_value = 0
        command.get_last_output.return_value = (file_label_content, "")
        # THEN getting a specific file content format the archive with a GZipCompression instance
        gz_compression = GZipCompression(command)
        read_content = gz_compression.get_file_content(
            label_file_path_in_archive, archive_path
        )
        # SHOULD retrieve the expected content
        assert file_label_content == read_content

        # AND command.cmd was called
        args = [
            "-xzf",
            archive_path + ".tar.gz",
            "-O",
            label_file_path_in_archive,
            "--occurrence",
        ]

        command.cmd.assert_called_once()
        command.cmd.assert_called_once_with("tar", args=args)

    def test_get_file_content_file_not_found(self):
        """
        Verifies an exception is thrown if get_file_content is called on a file which does
        not exist.
        """
        # Given a tar.gz compressed archive on disk containing specified files
        archive_path = "/path/to/archive"
        missing_file_name = "label"
        label_file_path_in_archive = os.path.join("label/file/path", missing_file_name)

        # AND a Mock Command that simulates tar response in case of missing file.
        command = mock.Mock()
        command.cmd.return_value = 1
        expected_exception_message = (
            "tar: base/%s: Not found in archive\n"
            "tar: Error exit delayed from previous errors.\n"
            "archive name: %s.tar.gz"
            % (
                missing_file_name,
                archive_path,
            )
        )
        tar_error_message = (
            "tar: base/%s: Not found in archive\n"
            "tar: Error exit delayed from previous errors.\n" % missing_file_name
        )
        command.get_last_output.return_value = (
            "",
            tar_error_message,
        )
        # AND getting a specific file content format the archive with a GZipCompression instance
        gz_compression = GZipCompression(command)

        # THEN getting missing file content
        # SHOULD Raise an exception
        with pytest.raises(FileNotFoundException) as exc:
            gz_compression.get_file_content(label_file_path_in_archive, archive_path)
        assert expected_exception_message == str(exc.value)


class TestLZ4Compression(object):
    @pytest.mark.parametrize(*COMMON_UNCOMPRESS_ARGS)
    def test_uncompress(self, src, dst, exclude, include, expected_error):
        # GIVEN a LZ4Compression object
        command = mock.Mock()
        command.cmd.return_value = 0
        command.get_last_output.return_value = ("all good", "")
        lz4_compression = LZ4Compression(command)

        # WHEN uncompress is called with the source and destination
        # THEN the command is called once
        # AND if we expect an error, that error is raised
        if expected_error is not None:
            with pytest.raises(ValueError):
                lz4_compression.uncompress(
                    src, dst, exclude=exclude, include_args=include
                )
            # THEN command.cmd was not called
            command.cmd.assert_not_called()
        # OR if we don't expect an error
        else:
            lz4_compression.uncompress(src, dst, exclude=exclude, include_args=include)
            # THEN command.cmd was called
            command.cmd.assert_called_once()
            # AND the first argument was "tar"
            assert command.cmd.call_args_list[0][0][0] == "tar"
            # AND the basic arguments are present
            common_args = command.cmd.call_args_list[0][1]["args"][:6]
            specific_args = command.cmd.call_args_list[0][1]["args"][6:]
            assert common_args == [
                "--use-compress-program",
                "lz4",
                "-xf",
                src,
                "--directory",
                dst,
            ]
            # AND if we expected exclude args they are present
            remaining_args = " ".join(specific_args)
            if exclude is not None:
                for exclude_arg in exclude:
                    assert "--exclude %s" % exclude_arg in remaining_args
            # AND if we expected include args they are present
            if include is not None:
                for include_arg in include:
                    assert include_arg in remaining_args

    def test_tar_failure_raises_exception(self):
        """Verify a nonzero return code from tar raises an exception"""
        # GIVEN a LZ4Compression object
        # AND a tar command which returns status 2 and an error
        command = mock.Mock()
        command.cmd.return_value = 2
        command.get_last_output.return_value = ("", "some error")
        lz4_compression = LZ4Compression(command)

        # WHEN uncompress is called
        # THEN a CommandFailedException is raised
        with pytest.raises(CommandFailedException) as exc:
            lz4_compression.uncompress("/path/to/src", "/path/to/dst")

        # AND the exception message contains the command stderr
        assert "some error" in str(exc.value)

    def test_get_file_content(self):
        """
        Cannot create a tar.lz4 to actually test this without
        :param tmpdir:
        :return:
        """
        # Given a tar.lz4 compressed archive on disk containing specified files
        archive_path = "/path/to/archive"
        label_file_name = "label"
        label_file_path_in_archive = os.path.join("label/file/path", label_file_name)
        file_label_content = "expected file label content"

        # AND a Mock Command
        command = mock.Mock()
        command.cmd.return_value = 0
        command.get_last_output.return_value = (file_label_content, "")
        # THEN getting a specific file content format the archive with a LZ4Compression instance
        lz4_compression = LZ4Compression(command)
        read_content = lz4_compression.get_file_content(
            label_file_path_in_archive, archive_path
        )
        # SHOULD retrieve the expected content
        assert file_label_content == read_content

        # AND command.cmd was called
        args = [
            "--use-compress-program",
            "lz4",
            "-xf",
            archive_path + ".tar.lz4",
            "-O",
            label_file_path_in_archive,
            "--occurrence",
        ]
        command.cmd.assert_called_once()
        command.cmd.assert_called_once_with("tar", args=args)

    def test_get_file_content_file_not_found(self):
        """
        Verifies an exception is thrown if get_file_content is called on a file which does
        not exist.
        """
        # Given a tar.lz4 compressed archive on disk containing specified files (or missing in that case)
        archive_path = "/path/to/archive"
        missing_file_name = "label"
        label_file_path_in_archive = os.path.join("label/file/path", missing_file_name)

        # AND a Mock Command that simulates tar response in case of missing file.
        command = mock.Mock()
        command.cmd.return_value = 1
        expected_exception_message = (
            "tar: base/%s: Not found in archive\n"
            "tar: Error exit delayed from previous errors.\n"
            "archive name: %s.tar.lz4"
            % (
                missing_file_name,
                archive_path,
            )
        )
        tar_error_message = (
            "tar: base/%s: Not found in archive\n"
            "tar: Error exit delayed from previous errors.\n" % missing_file_name
        )
        command.get_last_output.return_value = (
            "",
            tar_error_message,
        )
        # AND getting a specific file content format the archive with a LZ4Compression instance
        lz4_compression = LZ4Compression(command)

        # THEN getting missing file content
        # SHOULD Raise an exception
        with pytest.raises(FileNotFoundException) as exc:
            lz4_compression.get_file_content(label_file_path_in_archive, archive_path)
        assert expected_exception_message == str(exc.value)


class TestZSTDCompression(object):
    @pytest.mark.parametrize(*COMMON_UNCOMPRESS_ARGS)
    def test_uncompress(self, src, dst, exclude, include, expected_error):
        # GIVEN a ZSTDCompression object
        command = mock.Mock()
        command.cmd.return_value = 0
        command.get_last_output.return_value = ("all good", "")
        zstd_compression = ZSTDCompression(command)

        # WHEN uncompress is called with the source and destination
        # THEN the command is called once
        # AND if we expect an error, that error is raised
        if expected_error is not None:
            with pytest.raises(ValueError):
                zstd_compression.uncompress(
                    src, dst, exclude=exclude, include_args=include
                )
            # THEN command.cmd was not called
            command.cmd.assert_not_called()
        # OR if we don't expect an error
        else:
            zstd_compression.uncompress(src, dst, exclude=exclude, include_args=include)
            # THEN command.cmd was called
            command.cmd.assert_called_once()
            # AND the first argument was "tar"
            assert command.cmd.call_args_list[0][0][0] == "tar"
            # AND the basic arguments are present
            common_args = command.cmd.call_args_list[0][1]["args"][:6]
            specific_args = command.cmd.call_args_list[0][1]["args"][6:]
            assert common_args == [
                "--use-compress-program",
                "zstd",
                "-xf",
                src,
                "--directory",
                dst,
            ]
            # AND if we expected exclude args they are present
            remaining_args = " ".join(specific_args)
            if exclude is not None:
                for exclude_arg in exclude:
                    assert "--exclude %s" % exclude_arg in remaining_args
            # AND if we expected include args they are present
            if include is not None:
                for include_arg in include:
                    assert include_arg in remaining_args

    def test_tar_failure_raises_exception(self):
        """Verify a nonzero return code from tar raises an exception"""
        # GIVEN a ZSTDCompression object
        # AND a tar command which returns status 2 and an error
        command = mock.Mock()
        command.cmd.return_value = 2
        command.get_last_output.return_value = ("", "some error")
        zstd_compression = ZSTDCompression(command)

        # WHEN uncompress is called
        # THEN a CommandFailedException is raised
        with pytest.raises(CommandFailedException) as exc:
            zstd_compression.uncompress("/path/to/src", "/path/to/dst")

        # AND the exception message contains the command stderr
        assert "some error" in str(exc.value)

    def test_get_file_content(self):
        """
        Cannot create a tar.zst to actually test this without installing dependency. So we fake it
        :param tmpdir:
        :return:
        """
        # Given a tar.zst compressed archive on disk containing specified files
        archive_path = "/path/to/archive"
        label_file_name = "label"
        label_file_path_in_archive = os.path.join("label/file/path", label_file_name)
        file_label_content = "expected file label content"

        # AND a Mock Command
        command = mock.Mock()
        command.cmd.return_value = 0
        command.get_last_output.return_value = (file_label_content, "")
        # THEN getting a specific file content format the archive with a ZSTDCompression instance
        zstd_compression = ZSTDCompression(command)
        read_content = zstd_compression.get_file_content(
            label_file_path_in_archive, archive_path
        )
        # SHOULD retrieve the expected content
        assert file_label_content == read_content

        # AND command.cmd was called
        args = [
            "--use-compress-program",
            "zstd",
            "-xf",
            archive_path + ".tar.zst",
            "-O",
            label_file_path_in_archive,
            "--occurrence",
        ]
        command.cmd.assert_called_once()
        command.cmd.assert_called_once_with("tar", args=args)

    def test_get_file_content_file_not_found(self):
        """
        Verifies an exception is thrown if get_file_content is called on a file which does
        not exist.
        """
        # Given a tar.zst compressed archive on disk containing specified files (or missing in that case)
        archive_path = "/path/to/archive"
        missing_file_name = "label"
        label_file_path_in_archive = os.path.join("label/file/path", missing_file_name)

        # AND a Mock Command that simulates tar response in case of missing file.
        command = mock.Mock()
        command.cmd.return_value = 1
        expected_exception_message = (
            "tar: base/%s: Not found in archive\n"
            "tar: Error exit delayed from previous errors.\n"
            "archive name: %s.tar.zst"
            % (
                missing_file_name,
                archive_path,
            )
        )
        tar_error_message = (
            "tar: base/%s: Not found in archive\n"
            "tar: Error exit delayed from previous errors.\n" % missing_file_name
        )
        command.get_last_output.return_value = (
            "",
            tar_error_message,
        )
        # AND getting a specific file content format the archive with a ZSTDCompression instance
        zstd_compression = ZSTDCompression(command)

        # THEN getting missing file content
        # SHOULD Raise an exception
        with pytest.raises(FileNotFoundException) as exc:
            zstd_compression.get_file_content(label_file_path_in_archive, archive_path)
        assert expected_exception_message == str(exc.value)
