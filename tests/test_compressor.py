# Copyright (C) 2013-2016 2ndQuadrant Italia Srl
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

from barman.compression import (BZip2Compressor, CommandCompressor,
                                CompressionManager, CustomCompressor,
                                GZipCompressor, PyBZip2Compressor,
                                PyGZipCompressor, identify_compression)


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
        assert comp_manager.check('test_compression') is False

    def test_get_compressor_custom(self):
        # prepare mock obj
        config_mock = mock.Mock()
        config_mock.compression = "custom"
        config_mock.custom_compression_filter = (
            "test_custom_compression_filter")
        config_mock.custom_decompression_filter = (
            "test_custom_decompression_filter")

        # check custom compression method creation
        comp_manager = CompressionManager(config_mock, None)
        assert comp_manager.get_compressor() is not None

    def test_get_compressor_gzip(self):
        # prepare mock obj
        config_mock = mock.Mock()
        config_mock.compression = "gzip"

        # check custom compression method creation
        comp_manager = CompressionManager(config_mock, None)
        assert comp_manager.get_compressor() is not None

    def test_get_compressor_bzip2(self):
        # prepare mock obj
        config_mock = mock.Mock()
        config_mock.compression = "bzip2"

        # check custom compression method creation
        comp_manager = CompressionManager(config_mock, None)
        assert comp_manager.get_compressor() is not None

    def test_get_compressor_invalid(self):
        # prepare mock obj
        config_mock = mock.Mock()

        # check custom compression method creation
        comp_manager = CompressionManager(config_mock, None)
        assert comp_manager.get_compressor("test_compression") is None


# noinspection PyMethodMayBeStatic
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


# noinspection PyMethodMayBeStatic
class TestCommandCompressors(object):

    def test_creation(self):
        # Prepare mock obj
        config_mock = mock.Mock()

        compressor = CommandCompressor(config=config_mock,
                                       compression="dummy_compressor")

        assert compressor is not None
        assert compressor.config == config_mock
        assert compressor.compression == "dummy_compressor"

    def test_build_command(self):
        # prepare mock obj
        config_mock = mock.Mock()

        compressor = CommandCompressor(config=config_mock,
                                       compression="dummy_compressor")

        command = compressor._build_command("dummy_command")

        assert command.cmd == 'command(){ dummy_command > "$2" < "$1";}; ' \
                              'command'

    def test_gzip(self, tmpdir):

        config_mock = mock.Mock()

        compressor = GZipCompressor(config=config_mock, compression='gzip')

        src = tmpdir.join('sourcefile')
        src.write('content')

        compressor.compress(src.strpath, '%s/zipfile.zip' % tmpdir.strpath)
        assert os.path.exists('%s/zipfile.zip' % tmpdir.strpath)
        compression_zip = identify_compression('%s/zipfile.zip' %
                                               tmpdir.strpath)
        assert compression_zip == "gzip"

        compressor.decompress('%s/zipfile.zip' % tmpdir.strpath,
                              '%s/zipfile.uncompressed' % tmpdir.strpath)

        f = open('%s/zipfile.uncompressed' % tmpdir.strpath).read()
        assert f == 'content'

    def test_bzip2(self, tmpdir):

        config_mock = mock.Mock()

        compressor = BZip2Compressor(config=config_mock, compression='bzip2')

        src = tmpdir.join('sourcefile')
        src.write('content')

        compressor.compress(src.strpath, '%s/bzipfile.bz2' % tmpdir.strpath)
        assert os.path.exists('%s/bzipfile.bz2' % tmpdir.strpath)
        compression_zip = identify_compression('%s/bzipfile.bz2' %
                                               tmpdir.strpath)
        assert compression_zip == "bzip2"

        compressor.decompress('%s/bzipfile.bz2' % tmpdir.strpath,
                              '%s/bzipfile.uncompressed' % tmpdir.strpath)

        f = open('%s/bzipfile.uncompressed' % tmpdir.strpath).read()
        assert f == 'content'


# noinspection PyMethodMayBeStatic
class TestInternalCompressors(object):

    def test_gzip(self, tmpdir):

        config_mock = mock.Mock()

        compressor = PyGZipCompressor(config=config_mock, compression='pygzip')

        src = tmpdir.join('sourcefile')
        src.write('content')

        compressor.compress(src.strpath, '%s/zipfile.zip' % tmpdir.strpath)
        assert os.path.exists('%s/zipfile.zip' % tmpdir.strpath)
        compression_zip = identify_compression('%s/zipfile.zip' %
                                               tmpdir.strpath)
        assert compression_zip == "gzip"

        compressor.decompress('%s/zipfile.zip' % tmpdir.strpath,
                              '%s/zipfile.uncompressed' % tmpdir.strpath)

        f = open('%s/zipfile.uncompressed' % tmpdir.strpath).read()
        assert f == 'content'

    def test_bzip2(self, tmpdir):

        config_mock = mock.Mock()

        compressor = PyBZip2Compressor(config=config_mock,
                                       compression='pybzip2')

        src = tmpdir.join('sourcefile')
        src.write('content')

        compressor.compress(src.strpath, '%s/bzipfile.bz2' % tmpdir.strpath)
        assert os.path.exists('%s/bzipfile.bz2' % tmpdir.strpath)
        compression_zip = identify_compression('%s/bzipfile.bz2' %
                                               tmpdir.strpath)
        assert compression_zip == "bzip2"

        compressor.decompress('%s/bzipfile.bz2' % tmpdir.strpath,
                              '%s/bzipfile.uncompressed' % tmpdir.strpath)

        f = open('%s/bzipfile.uncompressed' % tmpdir.strpath).read()
        assert f == 'content'


# noinspection PyMethodMayBeStatic
class TestCustomCompressor(object):
    def test_custom_compressor_creation(self):
        config_mock = mock.Mock()
        config_mock.custom_compression_filter = 'dummy_compression_filter'
        config_mock.custom_decompression_filter = 'dummy_decompression_filter'

        compressor = CustomCompressor(config=config_mock,
                                      compression="custom")

        assert compressor is not None
        assert compressor._compress.cmd == (
            'command(){ dummy_compression_filter > "$2" < "$1";}; command')
        assert compressor._decompress.cmd == (
            'command(){ dummy_decompression_filter > "$2" < "$1";}; command')

    def test_validate(self):
        config_mock = mock.Mock()
        config_mock.custom_compression_filter = 'dummy_compression_filter'
        config_mock.custom_decompression_filter = 'dummy_decompression_filter'

        compressor = CustomCompressor(config=config_mock,
                                      compression="custom")

        validate = compressor.validate('custom')

        assert validate is None
