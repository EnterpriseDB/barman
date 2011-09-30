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

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
