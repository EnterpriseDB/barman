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

import json
import os
from datetime import datetime

import mock
import pytest
from dateutil.tz import tzlocal, tzoffset

from barman.infofile import (BackupInfo, Field, FieldListFile, WalFileInfo,
                             load_datetime_tz)
from testing_helpers import build_mocked_server


BASE_BACKUP_INFO = """backup_label=None
begin_offset=40
begin_time=2014-12-22 09:25:22.561207+01:00
begin_wal=000000010000000000000004
begin_xlog=0/4000028
config_file=/fakepath/postgresql.conf
end_offset=184
end_time=2014-12-22 09:25:27.410470+01:00
end_wal=000000010000000000000004
end_xlog=0/40000B8
error=None
hba_file=/fakepath/pg_hba.conf
ident_file=/fakepath/pg_ident.conf
mode=default
pgdata=/fakepath/data
server_name=fake-9.4-server
size=20935690
status=DONE
tablespaces=[('fake_tbs', 16384, '/fake_tmp/tbs')]
timeline=1
version=90400"""


def test_load_datetime_tz():
    """
    Unit test for load_datetime_tz function

    This test covers all load_datetime_tz code with correct parameters
    and checks that a ValueError is raised when called with a bad parameter.
    """
    # try to load a tz-less timestamp
    assert load_datetime_tz("2012-12-15 10:14:51.898000") == \
        datetime(2012, 12, 15, 10, 14, 51, 898000,
                 tzinfo=tzlocal())

    # try to load a tz-aware timestamp
    assert load_datetime_tz("2012-12-15 10:14:51.898000 +0100") == \
        datetime(2012, 12, 15, 10, 14, 51, 898000,
                 tzinfo=tzoffset('GMT+1', 3600))

    # try to load an incorrect date
    with pytest.raises(ValueError):
        load_datetime_tz("Invalid datetime")


# noinspection PyMethodMayBeStatic
class TestField(object):
    def test_field_creation(self):
        field = Field('test_field')
        assert field

    def test_field_with_arguments(self):
        dump_function = str
        load_function = int
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
        dump_function = str
        test_field = test_field.dump(dump_function)
        assert test_field.to_str == dump_function

    def test_field_load_decorator(self):
        test_field = Field('test_field')
        load_function = int
        test_field = test_field.dump(load_function)
        assert test_field.to_str == load_function


class DummyFieldListFile(FieldListFile):
    dummy = Field('dummy', dump=str, load=int, default=12, doc='dummy_field')


# noinspection PyMethodMayBeStatic
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
class TestWalFileInfo(object):
    def test_from_file_no_compression(self, tmpdir):
        tmp_file = tmpdir.join("000000000000000000000001")
        tmp_file.write('dummy_content\n')
        stat = os.stat(tmp_file.strpath)
        wfile_info = WalFileInfo.from_file(tmp_file.strpath)
        assert wfile_info.name == tmp_file.basename
        assert wfile_info.size == stat.st_size
        assert wfile_info.time == stat.st_mtime
        assert wfile_info.filename == '%s.meta' % tmp_file.strpath
        assert wfile_info.relpath() == (
            '0000000000000000/000000000000000000000001')

    @mock.patch('barman.infofile.identify_compression')
    def test_from_file_compression(self, id_compression, tmpdir):
        # prepare
        id_compression.return_value = 'test_compression'

        tmp_file = tmpdir.join("000000000000000000000001")
        tmp_file.write('dummy_content\n')
        wfile_info = WalFileInfo.from_file(tmp_file.strpath)
        assert wfile_info.name == tmp_file.basename
        assert wfile_info.size == tmp_file.size()
        assert wfile_info.time == tmp_file.mtime()
        assert wfile_info.filename == '%s.meta' % tmp_file.strpath
        assert wfile_info.compression == 'test_compression'
        assert wfile_info.relpath() == (
            '0000000000000000/000000000000000000000001')

    @mock.patch('barman.infofile.identify_compression')
    def test_from_file_default_compression(self, id_compression, tmpdir):
        # prepare
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
        assert wfile_info.relpath() == (
            '00000001000000E5/00000001000000E500000064')

    @mock.patch('barman.infofile.identify_compression')
    def test_from_file_override_compression(self, id_compression, tmpdir):
        # prepare
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
        assert wfile_info.relpath() == (
            '0000000000000000/000000000000000000000001')

    @mock.patch('barman.infofile.identify_compression')
    def test_from_file_override(self, id_compression, tmpdir):
        # prepare
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
        assert wfile_info.relpath() == (
            '0000000000000000/000000000000000000000002')

        wfile_info = WalFileInfo.from_file(
            tmp_file.strpath,
            size=42)
        assert wfile_info.name == tmp_file.basename
        assert wfile_info.size == 42
        assert wfile_info.time == tmp_file.mtime()
        assert wfile_info.filename == '%s.meta' % tmp_file.strpath
        assert wfile_info.compression is None
        assert wfile_info.relpath() == (
            '0000000000000000/000000000000000000000001')

        wfile_info = WalFileInfo.from_file(
            tmp_file.strpath,
            time=43)
        assert wfile_info.name == tmp_file.basename
        assert wfile_info.size == tmp_file.size()
        assert wfile_info.time == 43
        assert wfile_info.filename == '%s.meta' % tmp_file.strpath
        assert wfile_info.compression is None
        assert wfile_info.relpath() == (
            '0000000000000000/000000000000000000000001')

    def test_to_xlogdb_line(self):
        wfile_info = WalFileInfo()
        wfile_info.name = '000000000000000000000002'
        wfile_info.size = 42
        wfile_info.time = 43
        wfile_info.compression = None
        assert wfile_info.relpath() == (
            '0000000000000000/000000000000000000000002')

        assert wfile_info.to_xlogdb_line() == (
            '000000000000000000000002\t42\t43\tNone\n')

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
        assert wfile_info.relpath() == (
            '0000000000000000/000000000000000000000001')

        # mock a server object
        server = mock.Mock(name='server')
        server.config.wals_directory = '/tmp/wals'

        # parse the string
        info_file = wfile_info.from_xlogdb_line(
            '000000000000000000000001\t42\t43\tNone\n')

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


# noinspection PyMethodMayBeStatic
class TestBackupInfo(object):

    def test_backup_info_from_file(self, tmpdir):
        """
        Test the initialization of a BackupInfo object
        loading data from a backup.info file
        """
        # we want to test the loading of BackupInfo data from local file.
        # So we create a file into the tmpdir containing a
        # valid BackupInfo dump
        infofile = tmpdir.join("backup.info")
        infofile.write(BASE_BACKUP_INFO)
        # Mock the server, we don't need it at the moment
        server = build_mocked_server()
        # load the data from the backup.info file
        b_info = BackupInfo(server, info_file=infofile.strpath)
        assert b_info
        assert b_info.begin_offset == 40
        assert b_info.begin_wal == '000000010000000000000004'
        assert b_info.timeline == 1
        assert isinstance(b_info.tablespaces, list)
        assert b_info.tablespaces[0].name == 'fake_tbs'
        assert b_info.tablespaces[0].oid == 16384
        assert b_info.tablespaces[0].location == '/fake_tmp/tbs'

    def test_backup_info_from_empty_file(self, tmpdir):
        """
        Test the initialization of a BackupInfo object
        loading data from a backup.info file
        """
        # we want to test the loading of BackupInfo data from local file.
        # So we create a file into the tmpdir containing a
        # valid BackupInfo dump
        infofile = tmpdir.join("backup.info")
        infofile.write('')
        # Mock the server, we don't need it at the moment
        server = build_mocked_server(name='test_server')
        server.backup_manager.name = 'test_mode'
        # load the data from the backup.info file
        b_info = BackupInfo(server, info_file=infofile.strpath)
        assert b_info
        assert b_info.server_name == 'test_server'
        assert b_info.mode == 'test_mode'

    def test_backup_info_from_backup_id(self, tmpdir):
        """
        Test the initialization of a BackupInfo object
        using a backup_id as argument
        """
        # We want to test the loading system using a backup_id.
        # So we create a backup.info file into the tmpdir then
        # we instruct the configuration on the position of the
        # testing backup.info file
        server = build_mocked_server(
            main_conf={
                'basebackups_directory': tmpdir.strpath
            },
        )
        infofile = tmpdir.mkdir('fake_name').join('backup.info')
        infofile.write(BASE_BACKUP_INFO)
        # Load the backup.info file using the backup_id
        b_info = BackupInfo(server, backup_id="fake_name")
        assert b_info
        assert b_info.begin_offset == 40
        assert b_info.begin_wal == '000000010000000000000004'
        assert b_info.timeline == 1
        assert isinstance(b_info.tablespaces, list)
        assert b_info.tablespaces[0].name == 'fake_tbs'
        assert b_info.tablespaces[0].oid == 16384
        assert b_info.tablespaces[0].location == '/fake_tmp/tbs'

    def test_backup_info_save(self, tmpdir):
        """
        Test the save method of a BackupInfo object
        """
        # Check the saving method.
        # Load a backup.info file, modify the BackupInfo object
        # then save it.
        server = build_mocked_server(
            main_conf={
                'basebackups_directory': tmpdir.strpath
            },
        )
        backup_dir = tmpdir.mkdir('fake_name')
        infofile = backup_dir.join('backup.info')
        b_info = BackupInfo(server, backup_id="fake_name")
        b_info.status = BackupInfo.FAILED
        b_info.save()
        # read the file looking for the modified line
        for line in infofile.readlines():
            if line.startswith("status"):
                assert line.strip() == "status=FAILED"

    def test_backup_info_version(self, tmpdir):
        """
        Simple test for backup_version management.
        """
        server = build_mocked_server(
            main_conf={
                'basebackups_directory': tmpdir.strpath
            },
        )

        # new version
        backup_dir = tmpdir.mkdir('fake_backup_id')
        backup_dir.mkdir('data')
        backup_dir.join('backup.info')
        b_info = BackupInfo(server, backup_id="fake_backup_id")
        assert b_info.backup_version == 2

        # old version
        backup_dir = tmpdir.mkdir('another_fake_backup_id')
        backup_dir.mkdir('pgdata')
        backup_dir.join('backup.info')
        b_info = BackupInfo(server, backup_id="another_fake_backup_id")
        assert b_info.backup_version == 1

    def test_data_dir(self, tmpdir):
        """
        Simple test for the method that is responsible of the build of the
        path to the datadir and to the tablespaces dir according
        with backup_version
        """
        server = build_mocked_server(
            main_conf={
                'basebackups_directory': tmpdir.strpath
            },
        )

        # Build a fake v2 backup
        backup_dir = tmpdir.mkdir('fake_backup_id')
        data_dir = backup_dir.mkdir('data')
        info_file = backup_dir.join('backup.info')
        info_file.write(BASE_BACKUP_INFO)
        b_info = BackupInfo(server, backup_id="fake_backup_id")

        # Check that the paths are built according with version
        assert b_info.backup_version == 2
        assert b_info.get_data_directory() == data_dir.strpath
        assert b_info.get_data_directory(16384) == (backup_dir.strpath +
                                                    '/16384')

        # Build a fake v1 backup
        backup_dir = tmpdir.mkdir('another_fake_backup_id')
        pgdata_dir = backup_dir.mkdir('pgdata')
        info_file = backup_dir.join('backup.info')
        info_file.write(BASE_BACKUP_INFO)
        b_info = BackupInfo(server, backup_id="another_fake_backup_id")

        # Check that the paths are built according with version
        assert b_info.backup_version == 1
        assert b_info.get_data_directory(16384) == \
            backup_dir.strpath + '/pgdata/pg_tblspc/16384'
        assert b_info.get_data_directory() == pgdata_dir.strpath

        # Check that an exception is raised if an invalid oid
        # is provided to the method
        with pytest.raises(ValueError):
            b_info.get_data_directory(12345)

        # Check that a ValueError exception is raised with an
        # invalid oid when the tablespaces list is None
        b_info.tablespaces = None
        # and expect a value error
        with pytest.raises(ValueError):
            b_info.get_data_directory(16384)

    def test_to_json(self, tmpdir):
        server = build_mocked_server(
            main_conf={
                'basebackups_directory': tmpdir.strpath
            },
        )

        # Build a fake backup
        backup_dir = tmpdir.mkdir('fake_backup_id')
        info_file = backup_dir.join('backup.info')
        info_file.write(BASE_BACKUP_INFO)
        b_info = BackupInfo(server, backup_id="fake_backup_id")

        # This call should not raise
        assert json.dumps(b_info.to_json())

    def test_from_json(self, tmpdir):
        server = build_mocked_server(
            main_conf={
                'basebackups_directory': tmpdir.strpath
            },
        )

        # Build a fake backup
        backup_dir = tmpdir.mkdir('fake_backup_id')
        info_file = backup_dir.join('backup.info')
        info_file.write(BASE_BACKUP_INFO)
        b_info = BackupInfo(server, backup_id="fake_backup_id")

        # Build another BackupInfo from the json dump
        new_binfo = BackupInfo.from_json(server, b_info.to_json())

        assert b_info.to_dict() == new_binfo.to_dict()
