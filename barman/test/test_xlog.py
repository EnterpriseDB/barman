'''
Created on 30/set/2011

@author: mnencia
'''
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
