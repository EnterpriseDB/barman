# Copyright (C) 2013-2014 2ndQuadrant Italia (Devise.IT S.r.L.)
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

import mock
import base64
from barman.compression import identify_compression, Compressor, \
    CustomCompressor, CompressionManager


class TestCompressionManager(object):
    def test_compression_manager_creation(self):
        #prepare mock obj
        config_mock = mock.Mock()
        comp_manager = CompressionManager(config_mock)
        assert comp_manager

    def test_check_compression_none(self):
        #prepare mock obj
        config_mock = mock.Mock()
        config_mock.compression = "custom"
        comp_manager = CompressionManager(config_mock)
        assert comp_manager.check() is True

    def test_check_with_compression(self):
        #prepare mock obj
        config_mock = mock.Mock()
        comp_manager = CompressionManager(config_mock)
        assert comp_manager.check('test_compression') is False

    def test_get_compressor_custom(self):
        #prepare mock obj
        config_mock = mock.Mock()
        config_mock.compression = "custom"
        config_mock.custom_compression_filter = "test_custom_compression_filter"
        config_mock.custom_decompression_filter = \
            "test_custom_decompression_filter"

        # check custom compression method creation
        comp_manager = CompressionManager(config_mock)
        assert comp_manager.get_compressor() is not None

    def test_get_compressor_gzip(self):
        #prepare mock obj
        config_mock = mock.Mock()
        config_mock.compression = "gzip"

        # check custom compression method creation
        comp_manager = CompressionManager(config_mock)
        assert comp_manager.get_compressor() is not None

    def test_get_compressor_bzip2(self):
        #prepare mock obj
        config_mock = mock.Mock()
        config_mock.compression = "bzip2"

        # check custom compression method creation
        comp_manager = CompressionManager(config_mock)
        assert comp_manager.get_compressor() is not None

    def test_get_compressor_invalid(self):
        #prepare mock obj
        config_mock = mock.Mock()

        # check custom compression method creation
        comp_manager = CompressionManager(config_mock)
        assert comp_manager.get_compressor("test_compression") is None


class TestIdentifyCompression(object):
    def test_identify_compression(self, tmpdir):
        bz2_tmp_file = tmpdir.join("test_file")
        # "test" in bz2 compression
        bz2_tmp_file.write(base64.b64decode(
            b"QlpoOTFBWSZTWczDcdQAAAJBgAAQAgAMACAAIZpoM00Zl4u5IpwoSGZhuOoA"),
            mode='wb')

        compression_bz2 = identify_compression(bz2_tmp_file.strpath)
        assert compression_bz2 == "bzip2"

        zip_tmp_file = tmpdir.join("test_file")
        # "test" in bz2 compression
        zip_tmp_file.write(base64.b64decode(
            b"H4sIAF0ssFIAAytJLS7hAgDGNbk7BQAAAA=="),
            mode='wb')

        # check custom compression method creation
        compression_zip = identify_compression(zip_tmp_file.strpath)
        assert compression_zip == "gzip"


class TestCompressor(object):
    def test_compressor_creation(self):
        #prepare mock obj
        config_mock = mock.Mock()

        compressor = Compressor(config=config_mock,
                                compression="dummy_compressor")

        assert compressor is not None
        assert compressor.config == config_mock
        assert compressor.compression == "dummy_compressor"
        assert compressor.debug is False
        assert compressor.remove_origin is False
        assert compressor.compress is None
        assert compressor.decompres is None

    def test_build_command(self):
        #prepare mock obj
        config_mock = mock.Mock()

        compressor = Compressor(config=config_mock,
                                compression="dummy_compressor")

        command = compressor._build_command("dummy_command")

        assert command.cmd == 'command(){ dummy_command > "$2" < "$1";}; ' \
                              'command'

        compressor = Compressor(config=config_mock,
                                compression="dummy_compressor",
                                remove_origin=True)

        command = compressor._build_command("dummy_command")

        assert command.cmd == 'command(){ dummy_command > "$2" < "$1" && rm ' \
                              '-f "$1";}; command'


class TestCustomCompressor(object):
    def testCustomCompressorCreation(self):
        config_mock = mock.Mock()
        config_mock.custom_compression_filter = 'dummy_compression_filter'
        config_mock.custom_decompression_filter = 'dummy_decompression_filter'

        compressor = CustomCompressor(config=config_mock,
                                      compression="custom")

        assert compressor is not None
        assert compressor.compress.cmd == 'command(){ dummy_compression_filter > "$2" < "$1";}; command'
        assert compressor.decompress.cmd == 'command(){ dummy_decompression_filter > "$2" < "$1";}; command'

    def test_validate(self):
        config_mock = mock.Mock()
        config_mock.custom_compression_filter = 'dummy_compression_filter'
        config_mock.custom_decompression_filter = 'dummy_decompression_filter'

        compressor = CustomCompressor(config=config_mock,
                                    compression="custom")

        validate = compressor.validate('custom')

        assert validate is None
