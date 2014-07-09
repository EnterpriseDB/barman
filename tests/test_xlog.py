# Copyright (C) 2011-2014 2ndQuadrant Italia (Devise.IT S.r.L.)
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

    def test_enumerate_segments(self):
        assert tuple(
            xlog.enumerate_segments(
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
            xlog.enumerate_segments(
                '0000000100000001000000FD',
                '0000000100000001000000FF',
                90200
            )) == (
                '0000000100000001000000FD',
                '0000000100000001000000FE')

        assert tuple(
            xlog.enumerate_segments(
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
            xlog.enumerate_segments(
                '0000000100000001000000FD',
                '0000000100000001000000FF',
                90300
            )) == (
                '0000000100000001000000FD',
                '0000000100000001000000FE',
                '0000000100000001000000FF',)

    def test_hash_dir(self):
        assert xlog.hash_dir(
            '000000000000000200000001') == '0000000000000002'
        assert xlog.hash_dir(
            '000000010000000000000002') == '0000000100000000'
        assert xlog.hash_dir(
            '000000020000000100000000') == '0000000200000001'
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