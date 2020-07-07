# Copyright (C) 2011-2020 2ndQuadrant Limited
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

import itertools

import pytest
from mock import mock

import barman.exceptions
from barman import xlog
from barman.compression import CompressionManager
from barman.infofile import WalFileInfo


# noinspection PyMethodMayBeStatic
class Test(object):

    def test_encode_segment_name(self):
        assert xlog.encode_segment_name(
            0, 0, 0) == '000000000000000000000000'
        assert xlog.encode_segment_name(
            1, 1, 1) == '000000010000000100000001'
        assert xlog.encode_segment_name(
            10, 10, 10) == '0000000A0000000A0000000A'
        assert xlog.encode_segment_name(
            17, 17, 17) == '000000110000001100000011'
        assert xlog.encode_segment_name(
            0, 2, 1) == '000000000000000200000001'
        assert xlog.encode_segment_name(
            1, 0, 2) == '000000010000000000000002'
        assert xlog.encode_segment_name(
            2, 1, 0) == '000000020000000100000000'

    def test_decode_segment_name(self):
        assert xlog.decode_segment_name(
            '000000000000000000000000') == [0, 0, 0]
        assert xlog.decode_segment_name(
            '000000010000000100000001') == [1, 1, 1]
        assert xlog.decode_segment_name(
            '0000000A0000000A0000000A') == [10, 10, 10]
        assert xlog.decode_segment_name(
            '000000110000001100000011') == [17, 17, 17]
        assert xlog.decode_segment_name(
            '000000000000000200000001') == [0, 2, 1]
        assert xlog.decode_segment_name(
            '000000010000000000000002') == [1, 0, 2]
        assert xlog.decode_segment_name(
            '000000020000000100000000') == [2, 1, 0]
        assert xlog.decode_segment_name(
            '00000001000000000000000A.00000020.backup') == [1, 0, 10]
        assert xlog.decode_segment_name(
            '00000001.history') == [1, None, None]
        with pytest.raises(barman.exceptions.BadXlogSegmentName):
            xlog.decode_segment_name('00000000000000000000000')
        with pytest.raises(barman.exceptions.BadXlogSegmentName):
            xlog.decode_segment_name('0000000000000000000000000')
        with pytest.raises(barman.exceptions.BadXlogSegmentName):
            xlog.decode_segment_name('000000000000X00000000000')

    def test_generate_segment_names_xlog_file_size_known(self):
        assert tuple(
            xlog.generate_segment_names(
                '0000000100000001000000FD',
                '000000010000000200000002',
                90200,
                xlog.DEFAULT_XLOG_SEG_SIZE
            )) == (
                '0000000100000001000000FD',
                '0000000100000001000000FE',
                '000000010000000200000000',
                '000000010000000200000001',
                '000000010000000200000002')
        assert tuple(
            xlog.generate_segment_names(
                '0000000100000001000000FD',
                '0000000100000001000000FF',
                90200,
                xlog.DEFAULT_XLOG_SEG_SIZE
            )) == (
                '0000000100000001000000FD',
                '0000000100000001000000FE')

        assert tuple(
            xlog.generate_segment_names(
                '0000000100000001000000FD',
                '000000010000000200000002',
                90300,
                xlog.DEFAULT_XLOG_SEG_SIZE
            )) == (
                '0000000100000001000000FD',
                '0000000100000001000000FE',
                '0000000100000001000000FF',
                '000000010000000200000000',
                '000000010000000200000001',
                '000000010000000200000002')

        assert tuple(
            xlog.generate_segment_names(
                '0000000100000001000000FD',
                '0000000100000001000000FF',
                90300,
                xlog.DEFAULT_XLOG_SEG_SIZE
            )) == (
                '0000000100000001000000FD',
                '0000000100000001000000FE',
                '0000000100000001000000FF')

        # Test the behaviour of generate_segment_names at log boundaries
        # for recent versions
        assert tuple(
            itertools.islice(
                xlog.generate_segment_names(
                    '0000000300000004000000FD',
                    xlog_segment_size=xlog.DEFAULT_XLOG_SEG_SIZE),
                6)
        ) == (
            '0000000300000004000000FD',
            '0000000300000004000000FE',
            '0000000300000004000000FF',
            '000000030000000500000000',
            '000000030000000500000001',
            '000000030000000500000002',
        )

        # Test the behaviour of generate_segment_names at log boundaries
        # for versions < 9.3
        assert tuple(
            itertools.islice(
                xlog.generate_segment_names(
                    '0000000300000004000000FD',
                    version=90201,
                    xlog_segment_size=xlog.DEFAULT_XLOG_SEG_SIZE),
                6)
        ) == (
            '0000000300000004000000FD',
            '0000000300000004000000FE',
            '000000030000000500000000',
            '000000030000000500000001',
            '000000030000000500000002',
            '000000030000000500000003',
        )

        # Test the number of items produced between two segments
        assert sum(
            1 for _ in
            xlog.generate_segment_names(
                '000000040000000500000067',
                '000000040000000700000067',
                xlog_segment_size=xlog.DEFAULT_XLOG_SEG_SIZE
            )) == 513

        # The number of items produced between the same two segments is lower
        # with version < 9.3
        assert sum(
            1 for _ in
            xlog.generate_segment_names(
                '000000040000000500000067',
                '000000040000000700000067',
                version=90201,
                xlog_segment_size=xlog.DEFAULT_XLOG_SEG_SIZE
            )) == 511

    def test_generate_segment_names_xlog_file_size_unknown(self):
        assert tuple(
            xlog.generate_segment_names(
                '0000000100000001000000FD',
                '000000010000000100000102',
                90200
            )) == (
                '0000000100000001000000FD',
                '0000000100000001000000FE',
                '0000000100000001000000FF',
                '000000010000000100000100',
                '000000010000000100000101',
                '000000010000000100000102')

        assert tuple(
            xlog.generate_segment_names(
                '00000001000000010007FFFE',
                '000000010000000200000002',
                90300
            )) == (
                '00000001000000010007FFFE',
                '00000001000000010007FFFF',
                '000000010000000200000000',
                '000000010000000200000001',
                '000000010000000200000002')

        # The last segment of a file is skipped in
        # PostgreSQL < 9.3
        assert tuple(
            xlog.generate_segment_names(
                '00000001000000010007FFFE',
                '000000010000000200000002',
                90200
            )) == (
                '00000001000000010007FFFE',
                '000000010000000200000000',
                '000000010000000200000001',
                '000000010000000200000002')

    def test_hash_dir(self):
        assert xlog.hash_dir(
            '000000000000000200000001') == '0000000000000002'
        assert xlog.hash_dir(
            '000000010000000000000002') == '0000000100000000'
        assert xlog.hash_dir(
            'test/000000020000000100000000') == '0000000200000001'
        assert xlog.hash_dir(
            '00000001.history') == ''
        assert xlog.hash_dir(
            '00000002.history') == ''
        assert xlog.hash_dir(
            '00000001000000000000000A.00000020.backup') == '0000000100000000'
        assert xlog.hash_dir(
            '00000002000000050000000A.00000020.backup') == '0000000200000005'
        with pytest.raises(barman.exceptions.BadXlogSegmentName):
            xlog.hash_dir('00000000000000000000000')
        with pytest.raises(barman.exceptions.BadXlogSegmentName):
            xlog.hash_dir('0000000000000000000000000')
        with pytest.raises(barman.exceptions.BadXlogSegmentName):
            xlog.hash_dir('000000000000X00000000000')

    def test_is_any_xlog_file(self):
        assert xlog.is_any_xlog_file('000000000000000200000001')
        assert xlog.is_any_xlog_file('test1/000000000000000200000001')
        assert xlog.is_any_xlog_file(
            '00000001000000000000000A.00000020.backup')
        assert xlog.is_any_xlog_file(
            'test2/00000001000000000000000A.00000020.backup')
        assert xlog.is_any_xlog_file(
            '00000001000000000000000A.partial')
        assert xlog.is_any_xlog_file(
            'test2/00000001000000000000000A.partial')
        assert xlog.is_any_xlog_file('00000002.history')
        assert xlog.is_any_xlog_file('test3/00000002.history')
        assert not xlog.is_any_xlog_file('00000000000000000000000')
        assert not xlog.is_any_xlog_file('0000000000000000000000000')
        assert not xlog.is_any_xlog_file('000000000000X00000000000')
        assert not xlog.is_any_xlog_file('00000001000000000000000A.backup')
        assert not xlog.is_any_xlog_file(
            'test.00000001000000000000000A.00000020.backup')
        assert not xlog.is_any_xlog_file(
            'test.00000001000000000000000A.00000020.partial')
        assert not xlog.is_any_xlog_file('00000001000000000000000A.history')

    def test_history_file(self):
        assert not xlog.is_history_file('000000000000000200000001')
        assert not xlog.is_history_file(
            '00000001000000000000000A.00000020.backup')
        assert xlog.is_history_file('00000002.history')
        assert xlog.is_history_file('test/00000002.history')
        assert not xlog.is_history_file('00000000000000000000000')
        assert not xlog.is_history_file('0000000000000000000000000')
        assert not xlog.is_history_file('000000000000X00000000000')
        assert not xlog.is_history_file('00000001000000000000000A.backup')
        assert not xlog.is_any_xlog_file(
            'test.00000001000000000000000A.00000020.backup')
        assert not xlog.is_history_file('00000001000000000000000A.history')
        assert not xlog.is_history_file('00000001000000000000000A.partial')
        assert not xlog.is_history_file('00000001.partial')

    def test_backup_file(self):
        assert not xlog.is_backup_file('000000000000000200000001')
        assert xlog.is_backup_file(
            '00000001000000000000000A.00000020.backup')
        assert xlog.is_backup_file(
            'test/00000001000000000000000A.00000020.backup')
        assert not xlog.is_backup_file('00000002.history')
        assert not xlog.is_backup_file('00000000000000000000000')
        assert not xlog.is_backup_file('0000000000000000000000000')
        assert not xlog.is_backup_file('000000000000X00000000000')
        assert not xlog.is_backup_file('00000001000000000000000A.backup')
        assert not xlog.is_any_xlog_file(
            'test.00000001000000000000000A.00000020.backup')
        assert not xlog.is_backup_file('00000001000000000000000A.history')
        assert not xlog.is_backup_file('00000001000000000000000A.partial')
        assert not xlog.is_backup_file(
            '00000001000000000000000A.00000020.partial')

    def test_partial_file(self):
        assert not xlog.is_partial_file('000000000000000200000001')
        assert xlog.is_partial_file('00000001000000000000000A.partial')
        assert xlog.is_partial_file('test/00000001000000000000000A.partial')
        assert not xlog.is_partial_file('00000002.history')
        assert not xlog.is_partial_file('00000000000000000000000.partial')
        assert not xlog.is_partial_file('0000000000000000000000000.partial')
        assert not xlog.is_partial_file('000000000000X00000000000.partial')
        assert not xlog.is_partial_file(
            '00000001000000000000000A.00000020.partial')
        assert not xlog.is_any_xlog_file(
            'test.00000001000000000000000A.partial')
        assert not xlog.is_partial_file('00000001.partial')

    def test_is_wal_file(self):
        assert xlog.is_wal_file('000000000000000200000001')
        assert xlog.is_wal_file('test/000000000000000200000001')
        assert not xlog.is_wal_file('00000001000000000000000A.00000020.backup')
        assert not xlog.is_wal_file('00000002.history')
        assert not xlog.is_wal_file('00000000000000000000000')
        assert not xlog.is_wal_file('0000000000000000000000000')
        assert not xlog.is_wal_file('000000000000X00000000000')
        assert not xlog.is_wal_file('00000001000000000000000A.backup')
        assert not xlog.is_any_xlog_file(
            'test.00000001000000000000000A.00000020.backup')
        assert not xlog.is_wal_file('00000001000000000000000A.history')
        assert not xlog.is_wal_file('00000001000000000000000A.partial')

    def test_encode_history_filename(self):
        assert xlog.encode_history_file_name(1) == '00000001.history'
        assert xlog.encode_history_file_name(10) == '0000000A.history'
        assert xlog.encode_history_file_name(33) == '00000021.history'
        assert xlog.encode_history_file_name(328) == '00000148.history'

    def test_decode_history_file(self, tmpdir):
        compressor = mock.Mock()

        # Regular history file
        p = tmpdir.join('00000002.history')
        p.write('1\t2/83000168\tat restore point "myrp"\n')
        wal_info = WalFileInfo.from_file(p.strpath)
        result = xlog.HistoryFileData(
            tli=2,
            parent_tli=1,
            reason='at restore point "myrp"',
            switchpoint=0x283000168)
        assert xlog.decode_history_file(wal_info, compressor) == [result]
        assert len(compressor.mock_calls) == 0

        # Comments must be skipped
        p = tmpdir.join('00000003.history')
        p.write('# Comment\n1\t2/83000168\tat restore point "testcomment"\n')
        wal_info = WalFileInfo.from_file(p.strpath)
        result = xlog.HistoryFileData(
            tli=3,
            parent_tli=1,
            reason='at restore point "testcomment"',
            switchpoint=0x283000168)
        assert xlog.decode_history_file(wal_info, compressor) == [result]
        assert len(compressor.mock_calls) == 0

        # History file with comments and empty lines
        p = tmpdir.join('00000004.history')
        p.write('# Comment\n\n1\t2/83000168\ttesting "testemptyline"\n')
        wal_info = WalFileInfo.from_file(p.strpath)
        result = xlog.HistoryFileData(
            tli=4,
            parent_tli=1,
            reason='testing "testemptyline"',
            switchpoint=0x283000168)
        assert xlog.decode_history_file(wal_info, compressor) == [result]
        assert len(compressor.mock_calls) == 0

        # Test compression handling Fix for bug #66 on github
        config_mock = mock.Mock()
        config_mock.compression = "gzip"

        # check custom compression method creation
        comp_manager = CompressionManager(config_mock, None)
        u = tmpdir.join('00000005.uncompressed')
        p = tmpdir.join('00000005.history')
        u.write('1\t2/83000168\tat restore point "myrp"\n')
        result = xlog.HistoryFileData(
            tli=5,
            parent_tli=1,
            reason='at restore point "myrp"',
            switchpoint=0x283000168)
        comp_manager.get_compressor('gzip').compress(u.strpath,
                                                     p.strpath)
        wal_info = WalFileInfo.from_file(p.strpath)
        assert xlog.decode_history_file(wal_info, comp_manager) == [result]

        with pytest.raises(barman.exceptions.BadHistoryFileContents):
            # Empty file
            p.write('')
            assert xlog.decode_history_file(wal_info, compressor)
            assert len(compressor.mock_calls) == 0

        with pytest.raises(barman.exceptions.BadHistoryFileContents):
            # Missing field
            p.write('1\t2/83000168')
            assert xlog.decode_history_file(wal_info, compressor)
            assert len(compressor.mock_calls) == 0

        with pytest.raises(barman.exceptions.BadHistoryFileContents):
            # Unattended field
            p.write('1\t2/83000168\tat restore point "myrp"\ttest')
            assert xlog.decode_history_file(wal_info, compressor)
            assert len(compressor.mock_calls) == 0

    def test_parse_lsn(self):
        assert xlog.parse_lsn('2/8300168') == (
            (2 << 32) + 0x8300168)
        assert xlog.parse_lsn('FFFFFFFF/FFFFFFFF') == (
            (0xFFFFFFFF << 32) + 0xFFFFFFFF)
        assert xlog.parse_lsn('0/0') == 0
        with pytest.raises(ValueError):
            xlog.parse_lsn('DEADBEEF')

    def test_format_lsn(self):
        assert xlog.format_lsn(0x123456789ABCDEF) == '1234567/89ABCDEF'

    def test_diff_lsn(self):
        assert xlog.diff_lsn('2/8300168', '1/8300168') == 1 << 32
        assert xlog.diff_lsn('2/8300168', '2/8100168') == 0x200000
        assert xlog.diff_lsn(None, '2/8100168') is None
        assert xlog.diff_lsn('2/8300168', None) is None

    @pytest.mark.parametrize("size, name, offset, lsn", [
        [24, '000000030000000A00000012', 0x345678, 'A/12345678'],
        [28, '000000030000000A00000001', 0x2345678, 'A/12345678'],
        [20, '000000030000000A00000123', 0x45678, 'A/12345678'],
        [26, '000000030000011300000022', 0xB5ADC0, '113/88B5ADC0'],
        [28, '000000030000005600000003', 0x80537A8, '56/380537A8'],
    ])
    def test_location_to_xlogfile_name_offset(self, size, name, offset, lsn):
        result = xlog.location_to_xlogfile_name_offset(
            lsn, 3, 1 << size)
        assert result == {
            'file_name': name,
            'file_offset': offset,
        }

    @pytest.mark.parametrize("size, name, offset, lsn", [
        [24, '000000030000000A00000012', 0x345678, 'A/12345678'],
        [28, '000000030000000A00000001', 0x2345678, 'A/12345678'],
        [20, '000000030000000A00000123', 0x45678, 'A/12345678'],
        [26, '000000030000011300000022', 0xB5ADC0, '113/88B5ADC0'],
        [28, '000000030000005600000003', 0x80537A8, '56/380537A8'],
    ])
    def test_location_from_xlogfile_name_offset(self, size, name, offset, lsn):
        assert xlog.location_from_xlogfile_name_offset(
            name,
            offset,
            1 << size) == lsn

    @pytest.mark.parametrize("segments, size", [
        # There are 1023 segments with 4 MiB XLOG file size
        [1023, 1 << 22],
        # There are 511 segments with 8 MiB XLOG file size
        [511, 1 << 23],
        # There are 255 segments with 16 MiB (default) XLOG file size
        [255, 1 << 24],
        # There are 127 segments with 32 MiB XLOG file size
        [127, 1 << 25],
        # There are 63 segments with 64 MiB XLOG file size
        [63, 1 << 26],
        # There are 31 segments with 128 MiB XLOG file size
        [31, 1 << 27],
        # There are 15 segments with 256 MiB XLOG file size
        [15, 1 << 28],
        # There are 7 segments with 512 MiB XLOG file size
        [7, 1 << 29],
        # There are 3 segments with 1 GiB XLOG file size
        [3, 1 << 30],
    ])
    def test_xlog_segment_in_file(self, segments, size):
        assert segments == xlog.xlog_segments_per_file(size)

    @pytest.mark.parametrize("mask, size", [
        # There are 255 segments with default XLOG file size
        [0xff000000, 1 << 24],
        # There are 63 segments with 64 MiB XLOG file size
        [0xfc000000, 1 << 26],
        # There are 1023 segments with 4 MiB XLOG file size
        [0xffc00000, 1 << 22]
    ])
    def test_xlog_segment_mask(self, mask, size):
        assert mask == xlog.xlog_segment_mask(size)
