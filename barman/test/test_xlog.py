# Copyright (C) 2011, 2012 2ndQuadrant Italia (Devise.IT S.r.L.)
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

import unittest
from barman import xlog

class Test(unittest.TestCase):

    def testEncodeSegmentName(self):
        self.assertEqual(xlog.encode_segment_name(0, 0, 0), '000000000000000000000000')
        self.assertEqual(xlog.encode_segment_name(1, 1, 1), '000000010000000100000001')
        self.assertEqual(xlog.encode_segment_name(10, 10, 10), '0000000A0000000A0000000A')
        self.assertEqual(xlog.encode_segment_name(17, 17, 17), '000000110000001100000011')
        self.assertEqual(xlog.encode_segment_name(0, 2, 1), '000000000000000200000001')
        self.assertEqual(xlog.encode_segment_name(1, 0, 2), '000000010000000000000002')
        self.assertEqual(xlog.encode_segment_name(2, 1, 0), '000000020000000100000000')

    def testDecodeSegmentName(self):
        self.assertEqual(xlog.decode_segment_name('000000000000000000000000'), [0, 0, 0])
        self.assertEqual(xlog.decode_segment_name('000000010000000100000001'), [1, 1, 1])
        self.assertEqual(xlog.decode_segment_name('0000000A0000000A0000000A'), [10, 10, 10])
        self.assertEqual(xlog.decode_segment_name('000000110000001100000011'), [17, 17, 17])
        self.assertEqual(xlog.decode_segment_name('000000000000000200000001'), [0, 2, 1])
        self.assertEqual(xlog.decode_segment_name('000000010000000000000002'), [1, 0, 2])
        self.assertEqual(xlog.decode_segment_name('000000020000000100000000'), [2, 1, 0])
        self.assertRaises(xlog.BadXlogSegmentName, xlog.decode_segment_name, '00000000000000000000000')
        self.assertRaises(xlog.BadXlogSegmentName, xlog.decode_segment_name, '0000000000000000000000000')
        self.assertRaises(xlog.BadXlogSegmentName, xlog.decode_segment_name, '000000000000X00000000000')
        self.assertEqual(xlog.decode_segment_name('00000001000000000000000A.00000020.backup'), [1, 0, 10])
        self.assertEqual(xlog.decode_segment_name('00000001.history'), [1, None, None])

    def testEnumerateSegments(self):
        self.assertEqual(
            tuple(xlog.enumerate_segments('0000000100000001000000FD', '000000010000000200000002')),
            ('0000000100000001000000FD',
             '0000000100000001000000FE',
             '000000010000000200000000',
             '000000010000000200000001',
             '000000010000000200000002'))
        self.assertEqual(
            tuple(xlog.enumerate_segments('0000000100000001000000FD', '0000000100000001000000FF')),
            ('0000000100000001000000FD',
             '0000000100000001000000FE'))

    def testHashDir(self):
        self.assertEqual(xlog.hash_dir('000000000000000200000001'), '0000000000000002')
        self.assertEqual(xlog.hash_dir('000000010000000000000002'), '0000000100000000')
        self.assertEqual(xlog.hash_dir('000000020000000100000000'), '0000000200000001')
        self.assertEqual(xlog.hash_dir('00000001.history'), '')
        self.assertEqual(xlog.hash_dir('00000002.history'), '')
        self.assertEqual(xlog.hash_dir('00000001000000000000000A.00000020.backup'), '0000000100000000')
        self.assertEqual(xlog.hash_dir('00000002000000050000000A.00000020.backup'), '0000000200000005')
        self.assertRaises(xlog.BadXlogSegmentName, xlog.hash_dir, '00000000000000000000000')
        self.assertRaises(xlog.BadXlogSegmentName, xlog.hash_dir, '0000000000000000000000000')
        self.assertRaises(xlog.BadXlogSegmentName, xlog.hash_dir, '000000000000X00000000000')

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
