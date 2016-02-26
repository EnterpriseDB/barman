# Copyright (C) 2011-2016 2ndQuadrant Italia Srl
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

from barman import xlog


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
        with pytest.raises(xlog.BadXlogSegmentName):
            xlog.decode_segment_name('00000000000000000000000')
        with pytest.raises(xlog.BadXlogSegmentName):
            xlog.decode_segment_name('0000000000000000000000000')
        with pytest.raises(xlog.BadXlogSegmentName):
            xlog.decode_segment_name('000000000000X00000000000')

    def test_generate_segment_names(self):
        assert tuple(
            xlog.generate_segment_names(
                '0000000100000001000000FD',
                '000000010000000200000002',
                90200
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
                90200
            )) == (
                '0000000100000001000000FD',
                '0000000100000001000000FE')

        assert tuple(
            xlog.generate_segment_names(
                '0000000100000001000000FD',
                '000000010000000200000002',
                90300
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
                90300
            )) == (
                '0000000100000001000000FD',
                '0000000100000001000000FE',
                '0000000100000001000000FF')

        # Test the behaviour of generate_segment_names at log boundaries
        # for recent versions
        assert tuple(itertools.islice(
            xlog.generate_segment_names(
                '0000000300000004000000FD'), 6
            )) == (
                '0000000300000004000000FD',
                '0000000300000004000000FE',
                '0000000300000004000000FF',
                '000000030000000500000000',
                '000000030000000500000001',
                '000000030000000500000002')
        # Test the behaviour of generate_segment_names at log boundaries
        # for versions < 9.3
        assert tuple(itertools.islice(
            xlog.generate_segment_names(
                '0000000300000004000000FD',
                version=90201), 6
            )) == (
                '0000000300000004000000FD',
                '0000000300000004000000FE',
                '000000030000000500000000',
                '000000030000000500000001',
                '000000030000000500000002',
                '000000030000000500000003')

        # Test the number of items produced between two segments
        assert sum(
            1 for _ in
            xlog.generate_segment_names(
                '000000040000000500000067',
                '000000040000000700000067'
            )) == 513

        # The number of items produced between the same two segments is lower
        # with version < 9.3
        assert sum(
            1 for _ in
            xlog.generate_segment_names(
                '000000040000000500000067',
                '000000040000000700000067',
                version=90201
            )) == 511

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
        with pytest.raises(xlog.BadXlogSegmentName):
            xlog.hash_dir('00000000000000000000000')
        with pytest.raises(xlog.BadXlogSegmentName):
            xlog.hash_dir('0000000000000000000000000')
        with pytest.raises(xlog.BadXlogSegmentName):
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

    def test_get_offset_from_location(self):
        assert xlog.get_offset_from_location('0/1B0C7A0') == 11585440
        assert xlog.get_offset_from_location('9AFB/5B13FD70') == 1310064
        assert xlog.get_offset_from_location('9B02/29883178') == 8925560
        assert xlog.get_offset_from_location('BLAH') is None
