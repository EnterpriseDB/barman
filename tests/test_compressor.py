# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2022
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
import gzip
import os

import mock
import pytest

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
    GZipPgBaseBackupCompression,
    PgBaseBackupCompression,
)
from barman.exceptions import CompressionException
from testing_helpers import build_mocked_server

# Filename patterns used by the tests
ZIP_FILE = "%s/zipfile.zip"
ZIP_FILE_UNCOMPRESSED = "%s/zipfile.uncompressed"
BZIP2_FILE = "%s/bzipfile.bz2"
BZIP2_FILE_UNCOMPRESSED = "%s/bzipfile.uncompressed"


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
        ("compression", "expected_class"),
        [
            # A value of None for backup_compression should result in a NoneType
            (None, type(None)),
            # A value of gzip for backup_compression should result in a
            # GZipPgBaseBackupCompression
            ("gzip", GZipPgBaseBackupCompression),
        ],
    )
    def test_get_pg_basebackup_compression(self, compression, expected_class):
        """
        Verifies that get_pg_basebackup_compression returns an instance of the
        correct class for the compression specified in the server config.
        """
        server = build_mocked_server(
            global_conf={"backup_method": "postgres", "backup_compression": compression}
        )
        compression = get_pg_basebackup_compression(server)
        assert type(compression) == expected_class

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
        # GIVEN a concrete PgBaseBackupCompression which defines its suffix
        class BasicPgBaseBackupCompression(PgBaseBackupCompression):
            suffix = "basic"

            def open(self, _base):
                pass

        # WHEN it is instantiated
        basic_compression = BasicPgBaseBackupCompression(mock.Mock())

        # THEN with_suffix appends the suffix
        assert basic_compression.with_suffix("append_to_this") == "append_to_this.basic"

    def test_validate(self):
        """
        Verifies PgBaseBackupCompression.validate behaviour.
        Currently this will always pass because there is no general validation.
        """
        # GIVEN a concrete PgBaseBackupCompression which defines its suffix
        class BasicPgBaseBackupCompression(PgBaseBackupCompression):
            suffix = "basic"

            def open(self, _base):
                pass

        basic_compression = BasicPgBaseBackupCompression(mock.Mock())
        # AND a server with compression enabled
        server = build_mocked_server(
            global_conf={"backup_method": "postgres", "backup_compression": "gzip"}
        )

        # WHEN validate is called
        basic_compression.validate(server)

        # THEN the server is not disabled
        assert not server.config.disabled

        # AND no errors are added to the server's list
        assert len(server.config.msg_list) == 0


class TestGZipPgBaseBackupCompression(object):
    """
    Test the gzip pg_basebackup implementation details.
    """

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
        ],
    )
    def test_validate(self, compression_options, expected_errors):
        """Verifies supported config options pass validation."""
        # GIVEN a server with the specified compression options
        compression_options["backup_method"] = "postgres"
        server = build_mocked_server(global_conf=compression_options)
        # AND a GZipPgBaseBackupCompression for that server
        backup_compression = GZipPgBaseBackupCompression(server.config)

        # WHEN the compression is validated
        backup_compression.validate(server)

        # THEN if no errors are expected the server is not disabled
        if len(expected_errors) == 0:
            assert not server.config.disabled
            # AND no errors are added to the server's list
            assert len(server.config.msg_list) == 0
        # OR the server is disabled
        else:
            assert server.config.disabled
            # AND the expected errors are added to the server's list
            assert len(server.config.msg_list) == len(expected_errors)

    def test_open(self, tmpdir):
        """
        Verifies open returns a readable file-like object when passed a path to
        a gzip-compressed file.
        """
        # GIVEN a server with the specified compression options
        server = build_mocked_server(
            global_conf={
                "backup_method": "postgres",
                "backup_compression": "gzip",
            }
        )
        # AND a GZipPgBaseBackupCompression for that server
        backup_compression = GZipPgBaseBackupCompression(server.config)
        # AND a gzip-compressed file on disk containing specified content
        file_content = "expected file content".encode("utf-8")
        file_path = str(tmpdir.join("compressed_file"))

        with gzip.open(".".join((file_path, "gz")), mode="wb") as gz:
            gz.write(file_content)

        # WHEN open is called on that file
        with backup_compression.open(file_path) as compressed_file:
            # THEN a readable stream containing the contents is returned
            content = compressed_file.read()

        # AND the content matches the expected file content
        assert content == file_content

    def test_open_not_found(self):
        """
        Verifies an exception is thrown if open is called on a file which does
        not exist.
        """
        # GIVEN a server with the specified compression options
        server = build_mocked_server(
            global_conf={
                "backup_method": "postgres",
                "backup_compression": "gzip",
            }
        )
        # AND a GZipPgBaseBackupCompression for that server
        backup_compression = GZipPgBaseBackupCompression(server.config)
        # AND a path which points to nowhere
        file_path = "/path/to/nowhere/one/would/reasonably/expect"

        # WHEN open is called on that file
        # THEN an exception is raised
        with pytest.raises(Exception) as exc:
            with backup_compression.open(file_path):
                pass

        # AND the exception message refers to the full path
        assert ".".join((file_path, "gz")) in str(exc.value)
