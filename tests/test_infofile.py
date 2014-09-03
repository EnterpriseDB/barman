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
from datetime import datetime
import os
import mock
import pytest
from dateutil.tz import tzoffset, tzlocal
from barman.infofile import Field, FieldListFile, WalFileInfo, load_datetime_tz


#noinspection PyMethodMayBeStatic
class TestField(object):
    def test_field_creation(self):
        field = Field('test_field')
        assert field

    def test_field_with_arguments(self):
        dump_function = lambda x: str(x)
        load_function = lambda x: int(x)
        default = 10
        docstring = 'Test Docstring'
        field = Field('test_field', dump_function, load_function, default,
                      docstring)
        assert field
        assert field.name == 'test_field'
        assert field.to_str == dump_function
        assert field.from_str == load_function
        assert field.default == default
        assert field.__doc__ == docstring

    def test_field_dump_decorator(self):
        test_field = Field('test_field')
        dump_function = lambda x: str(x)
        test_field = test_field.dump(dump_function)
        assert test_field.to_str == dump_function

    def test_field_load_decorator(self):
        test_field = Field('test_field')
        load_function = lambda x: int(x)
        test_field = test_field.dump(load_function)
        assert test_field.to_str == load_function


class DummyFieldListFile(FieldListFile):
    dummy = Field('dummy', dump=str, load=int, default=12, doc='dummy_field')


#noinspection PyMethodMayBeStatic
class TestFieldListFile(object):
    def test_field_list_file_creation(self):
        with pytest.raises(AttributeError):
            FieldListFile(test_argument=11)

        field = FieldListFile()
        assert field

    def test_subclass_creation(self):
        with pytest.raises(AttributeError):
            DummyFieldListFile(test_argument=11)

        field = DummyFieldListFile()
        assert field
        assert field.dummy == 12

        field = DummyFieldListFile(dummy=13)
        assert field
        assert field.dummy == 13

    def test_subclass_access(self):
        dummy = DummyFieldListFile()

        dummy.dummy = 14

        assert dummy.dummy == 14

        with pytest.raises(AttributeError):
            del dummy.dummy

    def test_subclass_load(self, tmpdir):
        tmp_file = tmpdir.join("test_file")
        tmp_file.write('dummy=15\n')
        dummy = DummyFieldListFile()
        dummy.load(tmp_file.strpath)
        assert dummy.dummy == 15

    def test_subclass_save(self, tmpdir):
        tmp_file = tmpdir.join("test_file")
        dummy = DummyFieldListFile(dummy=16)
        dummy.save(tmp_file.strpath)
        assert 'dummy=16' in tmp_file.read()

    def test_subclass_from_meta_file(self, tmpdir):
        tmp_file = tmpdir.join("test_file")
        tmp_file.write('dummy=17\n')
        dummy = DummyFieldListFile.from_meta_file(tmp_file.strpath)
        assert dummy.dummy == 17

    def test_subclass_items(self):
        dummy = DummyFieldListFile()
        dummy.dummy = 18
        assert list(dummy.items()) == [('dummy', '18')]

    def test_subclass_repr(self):
        dummy = DummyFieldListFile()
        dummy.dummy = 18
        assert repr(dummy) == "DummyFieldListFile(dummy='18')"


# noinspection PyMethodMayBeStatic
class TestWallFileInfo(object):
    def test_from_file_no_compression(self, tmpdir):
        tmp_file = tmpdir.join("000000000000000000000001")
        tmp_file.write('dummy_content\n')
        stat = os.stat(tmp_file.strpath)
        wfile_info = WalFileInfo.from_file(tmp_file.strpath)
        assert wfile_info.name == tmp_file.basename
        assert wfile_info.size == stat.st_size
        assert wfile_info.time == stat.st_mtime
        assert wfile_info.filename == '%s.meta' % tmp_file.strpath
        assert wfile_info.relpath() == '0000000000000000/000000000000000000000001'

    @mock.patch('barman.infofile.identify_compression')
    def test_from_file_compression(self, id_compression, tmpdir):
        #prepare
        id_compression.return_value = 'test_compression'

        tmp_file = tmpdir.join("000000000000000000000001")
        tmp_file.write('dummy_content\n')
        wfile_info = WalFileInfo.from_file(tmp_file.strpath)
        assert wfile_info.name == tmp_file.basename
        assert wfile_info.size == tmp_file.size()
        assert wfile_info.time == tmp_file.mtime()
        assert wfile_info.filename == '%s.meta' % tmp_file.strpath
        assert wfile_info.compression == 'test_compression'
        assert wfile_info.relpath() == '0000000000000000/000000000000000000000001'

    @mock.patch('barman.infofile.identify_compression')
    def test_from_file_default_compression(self, id_compression, tmpdir):
        #prepare
        id_compression.return_value = None

        tmp_file = tmpdir.join("00000001000000E500000064")
        tmp_file.write('dummy_content\n')
        wfile_info = WalFileInfo.from_file(
            tmp_file.strpath,
            default_compression='test_default_compression')
        assert wfile_info.name == tmp_file.basename
        assert wfile_info.size == tmp_file.size()
        assert wfile_info.time == tmp_file.mtime()
        assert wfile_info.filename == '%s.meta' % tmp_file.strpath
        assert wfile_info.compression == 'test_default_compression'
        assert wfile_info.relpath() == '00000001000000E5/00000001000000E500000064'

    @mock.patch('barman.infofile.identify_compression')
    def test_from_file_override_compression(self, id_compression, tmpdir):
        #prepare
        id_compression.return_value = None

        tmp_file = tmpdir.join("000000000000000000000001")
        tmp_file.write('dummy_content\n')
        wfile_info = WalFileInfo.from_file(
            tmp_file.strpath,
            default_compression='test_default_compression',
            compression='test_override_compression')
        assert wfile_info.name == tmp_file.basename
        assert wfile_info.size == tmp_file.size()
        assert wfile_info.time == tmp_file.mtime()
        assert wfile_info.filename == '%s.meta' % tmp_file.strpath
        assert wfile_info.compression == 'test_override_compression'
        assert wfile_info.relpath() == '0000000000000000/000000000000000000000001'

    @mock.patch('barman.infofile.identify_compression')
    def test_from_file_override(self, id_compression, tmpdir):
        #prepare
        id_compression.return_value = None

        tmp_file = tmpdir.join("000000000000000000000001")
        tmp_file.write('dummy_content\n')

        wfile_info = WalFileInfo.from_file(
            tmp_file.strpath,
            name="000000000000000000000002")
        assert wfile_info.name == '000000000000000000000002'
        assert wfile_info.size == tmp_file.size()
        assert wfile_info.time == tmp_file.mtime()
        assert wfile_info.filename == '%s.meta' % tmp_file.strpath
        assert wfile_info.compression is None
        assert wfile_info.relpath() == '0000000000000000/000000000000000000000002'

        wfile_info = WalFileInfo.from_file(
            tmp_file.strpath,
            size=42)
        assert wfile_info.name == tmp_file.basename
        assert wfile_info.size == 42
        assert wfile_info.time == tmp_file.mtime()
        assert wfile_info.filename == '%s.meta' % tmp_file.strpath
        assert wfile_info.compression is None
        assert wfile_info.relpath() == '0000000000000000/000000000000000000000001'

        wfile_info = WalFileInfo.from_file(
            tmp_file.strpath,
            time=43)
        assert wfile_info.name == tmp_file.basename
        assert wfile_info.size == tmp_file.size()
        assert wfile_info.time == 43
        assert wfile_info.filename == '%s.meta' % tmp_file.strpath
        assert wfile_info.compression is None
        assert wfile_info.relpath() == '0000000000000000/000000000000000000000001'

    def test_to_xlogdb_line(self):
        wfile_info = WalFileInfo()
        wfile_info.name = '000000000000000000000002'
        wfile_info.size = 42
        wfile_info.time = 43
        wfile_info.compression = None
        assert wfile_info.relpath() == '0000000000000000/000000000000000000000002'

        assert wfile_info.to_xlogdb_line() == '000000000000000000000002\t42\t43\tNone\n'

    def test_from_xlogdb_line(self):
        """
        Test the conversion from a string to a WalFileInfo file
        """
        # build a WalFileInfo object
        wfile_info = WalFileInfo()
        wfile_info.name = '000000000000000000000001'
        wfile_info.size = 42
        wfile_info.time = 43
        wfile_info.compression = None
        assert wfile_info.relpath() == '0000000000000000/000000000000000000000001'

        # mock a server object
        server = mock.Mock(name='server')
        server.config.wals_directory = '/tmp/wals'

        # parse the string
        info_file = wfile_info.from_xlogdb_line(server,
                                               '000000000000000000000001\t'
                                               '42\t43\tNone\n')

        assert list(wfile_info.items()) == list(info_file.items())

    def test_timezone_aware_parser(self):
        """
        Test the timezone_aware_parser method with different string
        formats
        """
        # test case 1 string with timezone info
        tz_string = '2009/05/13 19:19:30 -0400'
        result = load_datetime_tz(tz_string)
        assert result.tzinfo == tzoffset(None, -14400)

        # test case 2 string with timezone info with a different format
        tz_string = '2004-04-09T21:39:00-08:00'
        result = load_datetime_tz(tz_string)
        assert result.tzinfo == tzoffset(None, -28800)

        # test case 3 string without timezone info,
        # expecting tzlocal() as timezone
        tz_string = str(datetime.now())
        result = load_datetime_tz(tz_string)
        assert result.tzinfo == tzlocal()

        # test case 4 string with a wrong timezone format,
        # expecting tzlocal() as timezone
        tz_string = '16:08:12 05/08/03 AEST'
        result = load_datetime_tz(tz_string)
        assert result.tzinfo == tzlocal()
