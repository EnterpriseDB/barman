# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2023
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

import copy
import json
import os
import warnings
from datetime import datetime

import mock
import pytest
from dateutil.tz import tzlocal, tzoffset
from mock import patch
from barman.cloud_providers.aws_s3 import AwsSnapshotMetadata, AwsSnapshotsInfo
from barman.cloud_providers.azure_blob_storage import (
    AzureSnapshotMetadata,
    AzureSnapshotsInfo,
)
from barman.cloud_providers.google_cloud_storage import (
    GcpSnapshotMetadata,
    GcpSnapshotsInfo,
)

from barman.infofile import (
    BackupInfo,
    Field,
    FieldListFile,
    LocalBackupInfo,
    SyntheticBackupInfo,
    WalFileInfo,
    load_datetime_tz,
    dump_backup_ids,
    load_backup_ids,
)
from testing_helpers import (
    build_backup_manager,
    build_mocked_server,
    build_real_server,
    build_test_backup_info,
)

BASE_BACKUP_INFO = """backup_label=None
begin_offset=40
begin_time=2014-12-22 09:25:22.561207+01:00
begin_wal=000000010000000000000004
begin_xlog=0/4000028
children_backup_ids=None
config_file=/fakepath/postgresql.conf
end_offset=184
end_time=2014-12-22 09:25:27.410470+01:00
end_wal=000000010000000000000004
end_xlog=0/40000B8
error=None
hba_file=/fakepath/pg_hba.conf
ident_file=/fakepath/pg_ident.conf
mode=default
parent_backup_id=None
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
    assert load_datetime_tz("2012-12-15 10:14:51.898000") == datetime(
        2012, 12, 15, 10, 14, 51, 898000, tzinfo=tzlocal()
    )

    # try to load a tz-aware timestamp
    assert load_datetime_tz("2012-12-15 10:14:51.898000 +0100") == datetime(
        2012, 12, 15, 10, 14, 51, 898000, tzinfo=tzoffset("GMT+1", 3600)
    )

    # try to load an incorrect date
    with pytest.raises(ValueError):
        load_datetime_tz("Invalid datetime")


@pytest.mark.parametrize(
    ("input", "expected"),
    [
        (None, None),
        (["SOME_BACKUP_ID"], "SOME_BACKUP_ID"),
        (["SOME_BACKUP_ID_1", "SOME_BACKUP_ID_2"], "SOME_BACKUP_ID_1,SOME_BACKUP_ID_2"),
    ],
)
def test_dump_backup_ids(input, expected):
    """
    Unit tests for :func:`dump_backup_ids`.
    """
    assert dump_backup_ids(input) == expected


@pytest.mark.parametrize(
    ("input", "expected"),
    [
        (None, None),
        ("SOME_BACKUP_ID", ["SOME_BACKUP_ID"]),
        ("SOME_BACKUP_ID_1,SOME_BACKUP_ID_2", ["SOME_BACKUP_ID_1", "SOME_BACKUP_ID_2"]),
    ],
)
def test_load_backup_ids(input, expected):
    """
    Unit tests for :func:`load_backup_ids`.
    """
    assert load_backup_ids(input) == expected


# noinspection PyMethodMayBeStatic
class TestField(object):
    def test_field_creation(self):
        field = Field("test_field")
        assert field

    def test_field_with_arguments(self):
        dump_function = str
        load_function = int
        default = 10
        docstring = "Test Docstring"
        field = Field("test_field", dump_function, load_function, default, docstring)
        assert field
        assert field.name == "test_field"
        assert field.to_str == dump_function
        assert field.from_str == load_function
        assert field.default == default
        assert field.__doc__ == docstring

    def test_field_dump_decorator(self):
        test_field = Field("test_field")
        dump_function = str
        test_field = test_field.dump(dump_function)
        assert test_field.to_str == dump_function

    def test_field_load_decorator(self):
        test_field = Field("test_field")
        load_function = int
        test_field = test_field.dump(load_function)
        assert test_field.to_str == load_function


class DummyFieldListFile(FieldListFile):
    dummy = Field("dummy", dump=str, load=int, default=12, doc="dummy_field")


class HideIfNullFieldListFile(FieldListFile):
    dummy = Field("dummy", dump=str, load=int, default=None, doc="dummy_field")
    _hide_if_null = "dummy"


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
        tmp_file.write("dummy=15\n")
        dummy = DummyFieldListFile()
        dummy.load(tmp_file.strpath)
        assert dummy.dummy == 15

    def test_subclass_save(self, tmpdir):
        tmp_file = tmpdir.join("test_file")
        dummy = DummyFieldListFile(dummy=16)
        dummy.save(tmp_file.strpath)
        assert "dummy=16" in tmp_file.read()

    def test_subclass_from_meta_file(self, tmpdir):
        tmp_file = tmpdir.join("test_file")
        tmp_file.write("dummy=17\n")
        dummy = DummyFieldListFile.from_meta_file(tmp_file.strpath)
        assert dummy.dummy == 17

    def test_subclass_items(self):
        dummy = DummyFieldListFile()
        dummy.dummy = 18
        assert list(dummy.items()) == [("dummy", "18")]

    def test_subclass_repr(self):
        dummy = DummyFieldListFile()
        dummy.dummy = 18
        assert repr(dummy) == "DummyFieldListFile(dummy='18')"

    def test_hide_if_null_when_null(self, tmpdir):
        # GIVEN a FieldListFile where the only field should be hidden if null
        dummy = HideIfNullFieldListFile()

        # WHEN the field is not set
        # THEN the field is not included in the items
        assert list(dummy.items()) == []

        # AND the field is not included in the repr
        assert repr(dummy) == "HideIfNullFieldListFile()"

        # AND the field is not saved
        tmp_file = tmpdir.join("test_file")
        dummy.save(tmp_file.strpath)
        assert "dummy" not in tmp_file.read()

    def test_hide_if_null_when_not_null(self, tmpdir):
        # GIVEN a FieldListFile where the only field should be hidden if null
        dummy = HideIfNullFieldListFile()

        # WHEN the field is set
        dummy_value = 32
        dummy.dummy = dummy_value

        # THEN the field is included in the items
        assert list(dummy.items()) == [("dummy", "%s" % dummy_value)]

        # AND the field is included in the repr
        assert repr(dummy) == "HideIfNullFieldListFile(dummy='%s')" % dummy_value

        # AND the field is saved
        tmp_file = tmpdir.join("test_file")
        dummy.save(tmp_file.strpath)
        assert "dummy=%s" % dummy_value in tmp_file.read()


# noinspection PyMethodMayBeStatic
class TestWalFileInfo(object):
    @mock.patch("barman.compression.CompressionManager")
    def test_from_file_no_compression(self, mock_compression_manager, tmpdir):
        tmp_file = tmpdir.join("000000000000000000000001")
        tmp_file.write("dummy_content\n")
        stat = os.stat(tmp_file.strpath)
        wfile_info = WalFileInfo.from_file(tmp_file.strpath, mock_compression_manager)
        assert wfile_info.name == tmp_file.basename
        assert wfile_info.size == stat.st_size
        assert wfile_info.time == stat.st_mtime
        assert wfile_info.filename == "%s.meta" % tmp_file.strpath
        assert wfile_info.relpath() == ("0000000000000000/000000000000000000000001")

    @mock.patch("barman.compression.CompressionManager")
    def test_from_file_compression(self, mock_compression_manager, tmpdir):
        # prepare
        mock_compression_manager.identify_compression.return_value = "test_compression"

        tmp_file = tmpdir.join("000000000000000000000001")
        tmp_file.write("dummy_content\n")
        wfile_info = WalFileInfo.from_file(tmp_file.strpath, mock_compression_manager)
        assert wfile_info.name == tmp_file.basename
        assert wfile_info.size == tmp_file.size()
        assert wfile_info.time == tmp_file.mtime()
        assert wfile_info.filename == "%s.meta" % tmp_file.strpath
        assert wfile_info.compression == "test_compression"
        assert wfile_info.relpath() == ("0000000000000000/000000000000000000000001")

    @mock.patch("barman.compression.CompressionManager")
    def test_from_file_unidentified_compression(self, mock_compression_manager, tmpdir):
        # prepare
        mock_compression_manager.identify_compression.return_value = None
        tmp_file = tmpdir.join("00000001000000E500000064")
        tmp_file.write("dummy_content\n")
        wfile_info = WalFileInfo.from_file(
            tmp_file.strpath,
            mock_compression_manager,
            unidentified_compression="test_unidentified_compression",
        )
        assert wfile_info.name == tmp_file.basename
        assert wfile_info.size == tmp_file.size()
        assert wfile_info.time == tmp_file.mtime()
        assert wfile_info.filename == "%s.meta" % tmp_file.strpath
        assert wfile_info.compression == "test_unidentified_compression"
        assert wfile_info.relpath() == ("00000001000000E5/00000001000000E500000064")

    @mock.patch("barman.compression.CompressionManager")
    def test_from_file_override_compression(self, mock_compression_manager, tmpdir):
        # prepare
        mock_compression_manager.identify_compression.return_value = None

        tmp_file = tmpdir.join("000000000000000000000001")
        tmp_file.write("dummy_content\n")
        wfile_info = WalFileInfo.from_file(
            tmp_file.strpath,
            mock_compression_manager,
            compression="test_override_compression",
        )
        assert wfile_info.name == tmp_file.basename
        assert wfile_info.size == tmp_file.size()
        assert wfile_info.time == tmp_file.mtime()
        assert wfile_info.filename == "%s.meta" % tmp_file.strpath
        assert wfile_info.compression == "test_override_compression"
        assert wfile_info.relpath() == ("0000000000000000/000000000000000000000001")

    @mock.patch("barman.compression.CompressionManager")
    def test_from_file_override(self, compression_manager, tmpdir):
        # prepare
        compression_manager.identify_compression.return_value = None
        compression_manager.unidentified_compression = None

        tmp_file = tmpdir.join("000000000000000000000001")
        tmp_file.write("dummy_content\n")

        wfile_info = WalFileInfo.from_file(
            tmp_file.strpath, compression_manager, name="000000000000000000000002"
        )
        assert wfile_info.name == "000000000000000000000002"
        assert wfile_info.size == tmp_file.size()
        assert wfile_info.time == tmp_file.mtime()
        assert wfile_info.filename == "%s.meta" % tmp_file.strpath
        assert wfile_info.compression is None
        assert wfile_info.relpath() == ("0000000000000000/000000000000000000000002")

        wfile_info = WalFileInfo.from_file(
            tmp_file.strpath, compression_manager, size=42
        )
        assert wfile_info.name == tmp_file.basename
        assert wfile_info.size == 42
        assert wfile_info.time == tmp_file.mtime()
        assert wfile_info.filename == "%s.meta" % tmp_file.strpath
        assert wfile_info.compression is None
        assert wfile_info.relpath() == ("0000000000000000/000000000000000000000001")

        wfile_info = WalFileInfo.from_file(
            tmp_file.strpath, compression_manager, time=43
        )
        assert wfile_info.name == tmp_file.basename
        assert wfile_info.size == tmp_file.size()
        assert wfile_info.time == 43
        assert wfile_info.filename == "%s.meta" % tmp_file.strpath
        assert wfile_info.compression is None
        assert wfile_info.relpath() == ("0000000000000000/000000000000000000000001")

    def test_to_xlogdb_line(self):
        wfile_info = WalFileInfo()
        wfile_info.name = "000000000000000000000002"
        wfile_info.size = 42
        wfile_info.time = 43
        wfile_info.compression = None
        assert wfile_info.relpath() == ("0000000000000000/000000000000000000000002")

        assert wfile_info.to_xlogdb_line() == (
            "000000000000000000000002\t42\t43\tNone\n"
        )

    def test_from_xlogdb_line(self):
        """
        Test the conversion from a string to a WalFileInfo file
        """
        # build a WalFileInfo object
        wfile_info = WalFileInfo()
        wfile_info.name = "000000000000000000000001"
        wfile_info.size = 42
        wfile_info.time = 43
        wfile_info.compression = None
        assert wfile_info.relpath() == ("0000000000000000/000000000000000000000001")

        # mock a server object
        server = mock.Mock(name="server")
        server.config.wals_directory = "/tmp/wals"

        # parse the string
        info_file = wfile_info.from_xlogdb_line(
            "000000000000000000000001\t42\t43\tNone\n"
        )

        assert list(wfile_info.items()) == list(info_file.items())

    def test_timezone_aware_parser(self):
        """
        Test the timezone_aware_parser method with different string
        formats
        """
        # test case 1 string with timezone info
        tz_string = "2009/05/13 19:19:30 -0400"
        result = load_datetime_tz(tz_string)
        assert result.tzinfo == tzoffset(None, -14400)

        # test case 2 string with timezone info with a different format
        tz_string = "2004-04-09T21:39:00-08:00"
        result = load_datetime_tz(tz_string)
        assert result.tzinfo == tzoffset(None, -28800)

        # test case 3 string without timezone info,
        # expecting tzlocal() as timezone
        tz_string = str(datetime.now())
        result = load_datetime_tz(tz_string)
        assert result.tzinfo == tzlocal()

        # test case 4 string with a wrong timezone format,
        # expecting tzlocal() as timezone
        tz_string = "16:08:12 05/08/03 AEST"
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", message="tzname AEST identified but not understood."
            )
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
        b_info = LocalBackupInfo(server, info_file=infofile.strpath)
        assert b_info
        assert b_info.begin_offset == 40
        assert b_info.begin_wal == "000000010000000000000004"
        assert b_info.timeline == 1
        assert isinstance(b_info.tablespaces, list)
        assert b_info.tablespaces[0].name == "fake_tbs"
        assert b_info.tablespaces[0].oid == 16384
        assert b_info.tablespaces[0].location == "/fake_tmp/tbs"

    def test_backup_info_from_empty_file(self, tmpdir):
        """
        Test the initialization of a BackupInfo object
        loading data from a backup.info file
        """
        # we want to test the loading of BackupInfo data from local file.
        # So we create a file into the tmpdir containing a
        # valid BackupInfo dump
        infofile = tmpdir.join("backup.info")
        infofile.write("")
        # Mock the server, we don't need it at the moment
        server = build_mocked_server(name="test_server")
        server.backup_manager.mode = "test-mode"
        # load the data from the backup.info file
        b_info = LocalBackupInfo(server, info_file=infofile.strpath)
        assert b_info
        assert b_info.server_name == "test_server"
        assert b_info.mode == "test-mode"

    def test_mode(self):
        # build a backup manager with a rsync executor (exclusive)
        backup_manager = build_backup_manager()
        # check the result of the mode property
        assert backup_manager.executor.mode == "rsync-concurrent"
        # build a backup manager with a postgres executor
        #  (strategy without mode)
        backup_manager = build_backup_manager(global_conf={"backup_method": "postgres"})
        # check the result of the mode property
        assert backup_manager.executor.mode == "postgres"

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
            main_conf={"basebackups_directory": tmpdir.strpath},
        )
        infofile = tmpdir.mkdir("fake_name").join("backup.info")
        infofile.write(BASE_BACKUP_INFO)
        # Load the backup.info file using the backup_id
        b_info = LocalBackupInfo(server, backup_id="fake_name")
        assert b_info
        assert b_info.begin_offset == 40
        assert b_info.begin_wal == "000000010000000000000004"
        assert b_info.timeline == 1
        assert isinstance(b_info.tablespaces, list)
        assert b_info.tablespaces[0].name == "fake_tbs"
        assert b_info.tablespaces[0].oid == 16384
        assert b_info.tablespaces[0].location == "/fake_tmp/tbs"

    def test_backup_info_save(self, tmpdir):
        """
        Test the save method of a BackupInfo object
        """
        # Check the saving method.
        # Load a backup.info file, modify the BackupInfo object
        # then save it.
        server = build_mocked_server(
            main_conf={"basebackups_directory": tmpdir.strpath},
        )
        backup_dir = tmpdir.mkdir("fake_name")
        infofile = backup_dir.join("backup.info")
        b_info = LocalBackupInfo(server, backup_id="fake_name")
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
            main_conf={"basebackups_directory": tmpdir.strpath},
        )

        # new version
        backup_dir = tmpdir.mkdir("fake_backup_id")
        backup_dir.mkdir("data")
        backup_dir.join("backup.info")
        b_info = LocalBackupInfo(server, backup_id="fake_backup_id")
        assert b_info.backup_version == 2

        # old version
        backup_dir = tmpdir.mkdir("another_fake_backup_id")
        backup_dir.mkdir("pgdata")
        backup_dir.join("backup.info")
        b_info = LocalBackupInfo(server, backup_id="another_fake_backup_id")
        assert b_info.backup_version == 1

    def test_data_dir(self, tmpdir):
        """
        Simple test for the method that is responsible of the build of the
        path to the datadir and to the tablespaces dir according
        with backup_version
        """
        server = build_mocked_server(
            main_conf={"basebackups_directory": tmpdir.strpath},
        )

        # Build a fake v2 backup
        backup_dir = tmpdir.mkdir("fake_backup_id")
        data_dir = backup_dir.mkdir("data")
        info_file = backup_dir.join("backup.info")
        info_file.write(BASE_BACKUP_INFO)
        b_info = LocalBackupInfo(server, backup_id="fake_backup_id")

        # Check that the paths are built according with version
        assert b_info.backup_version == 2
        assert b_info.get_data_directory() == data_dir.strpath
        assert b_info.get_data_directory(16384) == (backup_dir.strpath + "/16384")

        # Build a fake v1 backup
        backup_dir = tmpdir.mkdir("another_fake_backup_id")
        pgdata_dir = backup_dir.mkdir("pgdata")
        info_file = backup_dir.join("backup.info")
        info_file.write(BASE_BACKUP_INFO)
        b_info = LocalBackupInfo(server, backup_id="another_fake_backup_id")

        # Check that the paths are built according with version
        assert b_info.backup_version == 1
        assert (
            b_info.get_data_directory(16384)
            == backup_dir.strpath + "/pgdata/pg_tblspc/16384"
        )
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
            main_conf={"basebackups_directory": tmpdir.strpath},
        )

        # Build a fake backup
        backup_dir = tmpdir.mkdir("fake_backup_id")
        info_file = backup_dir.join("backup.info")
        info_file.write(BASE_BACKUP_INFO)
        b_info = LocalBackupInfo(server, backup_id="fake_backup_id")

        # This call should not raise
        assert json.dumps(b_info.to_json())

    def test_from_json(self, tmpdir):
        server = build_mocked_server(
            main_conf={"basebackups_directory": tmpdir.strpath},
        )

        # Build a fake backup
        backup_dir = tmpdir.mkdir("fake_backup_id")
        info_file = backup_dir.join("backup.info")
        info_file.write(BASE_BACKUP_INFO)
        b_info = LocalBackupInfo(server, backup_id="fake_backup_id")

        # Build another BackupInfo from the json dump
        new_binfo = LocalBackupInfo.from_json(server, b_info.to_json())

        assert b_info.to_dict() == new_binfo.to_dict()

    def test_xlog_segment_size(self, tmpdir):
        """
        Test the `xlog_segment_size` field of BackupInfo
        """

        # Create an empty backup info file, to test the
        # default value of xlog_segment_size. It's relevent
        # also for retrocompatibility with backup info which
        # doesn't contain the xlog_segment_size field.

        infofile = tmpdir.join("backup.info")
        infofile.write("")

        # Mock the server, we don't need it at the moment
        server = build_mocked_server(name="test_server")
        server.backup_manager.mode = "test-mode"

        # load the data from the backup.info file
        b_info = LocalBackupInfo(server, info_file=infofile.strpath)
        assert b_info.xlog_segment_size == 1 << 24

    @mock.patch("barman.postgres.PostgreSQLConnection.connect")
    def test_backupinfo_load(self, connect_mock, tmpdir):
        server = build_real_server(
            main_conf={"basebackups_directory": tmpdir.strpath},
        )

        # Build a fake backup info and try to load id, to ensure that we won't
        # need a PostgreSQL connection to do that
        backup_dir = tmpdir.mkdir("fake_backup_id")
        info_file = backup_dir.join("backup.info")
        info_file.write(BASE_BACKUP_INFO)

        # Monkey patch the PostgreSQL connection function to raise a
        # RuntimeError
        connect_mock.side_effect = RuntimeError

        # The following constructor will raise a RuntimeError if we are
        # needing a PostgreSQL connection
        LocalBackupInfo(server, backup_id="fake_backup_id")

    def test_pg_version(self, tmpdir):
        """
        Test handling of postgres version in BackupInfo object
        """
        infofile = tmpdir.join("backup.info")
        infofile.write(BASE_BACKUP_INFO)
        server = build_mocked_server()
        b_info = LocalBackupInfo(server, info_file=infofile.strpath)
        # BASE_BACKUP_INFO has version 90400 so expect 9.4
        assert b_info.pg_major_version() == "9.4"
        assert b_info.wal_directory() == "pg_xlog"
        # Set backup_info.version to 100600 so expect 10
        b_info.version = 100600
        assert b_info.pg_major_version() == "10"
        assert b_info.wal_directory() == "pg_wal"

    def test_with_backup_name(self, tmpdir):
        """
        Test that backup name is included in file and output if set.
        """
        # GIVEN a backup.info file for a server
        server = build_mocked_server(
            main_conf={"basebackups_directory": tmpdir.strpath},
        )
        backup_dir = tmpdir.mkdir("fake_name")
        infofile = backup_dir.join("backup.info")
        b_info = LocalBackupInfo(server, backup_id="fake_name")
        b_info.status = BackupInfo.DONE

        # WHEN a backup_name is set
        b_info.backup_name = "test name"
        b_info.save()

        # THEN the backup name is written to the file
        assert "backup_name=test name" in infofile.read()
        # AND the backup name is included in the JSON output
        assert b_info.to_json()["backup_name"] == "test name"

    def test_with_no_backup_name(self, tmpdir):
        """
        Test that backup name is not included in file and output if not set.
        """
        # GIVEN a backup.info file for a server
        server = build_mocked_server(
            main_conf={"basebackups_directory": tmpdir.strpath},
        )
        backup_dir = tmpdir.mkdir("fake_name")
        infofile = backup_dir.join("backup.info")
        b_info = LocalBackupInfo(server, backup_id="fake_name")
        b_info.status = BackupInfo.DONE

        # WHEN no backup_name is set
        b_info.save()

        # THEN the backup name is not written to the file
        assert "backup_name" not in infofile.read()
        # AND the backup name is not included in the JSON output
        assert "backup_name" not in b_info.to_json().keys()

    def test_with_snapshots_info_gcp(self, tmpdir):
        """
        Test that snapshots_info is included in file and output if set.
        """
        # GIVEN a backup.info file for a server
        server = build_mocked_server(
            main_conf={"basebackups_directory": tmpdir.strpath},
        )
        backup_dir = tmpdir.mkdir("fake_name")
        infofile = backup_dir.join("backup.info")
        b_info = LocalBackupInfo(server, backup_id="fake_name")
        b_info.status = BackupInfo.DONE

        # WHEN snapshots_info is set and the file is saved
        snapshots_info = GcpSnapshotsInfo(
            project="project_name",
            snapshots=[
                GcpSnapshotMetadata(
                    mount_point="/opt/mount0",
                    mount_options="rw",
                    device_name="dev0",
                    snapshot_name="short_snapshot_name",
                    snapshot_project="project_name",
                )
            ],
        )
        b_info.snapshots_info = snapshots_info
        b_info.save()

        # THEN a new BackupInfo created from the saved file has the SnapshotsInfo attributes
        new_backup_info = LocalBackupInfo(server, info_file=infofile.strpath)
        assert new_backup_info.snapshots_info.provider == "gcp"
        assert new_backup_info.snapshots_info.project == "project_name"
        snapshot0 = new_backup_info.snapshots_info.snapshots[0]
        assert snapshot0.mount_point == "/opt/mount0"
        assert snapshot0.mount_options == "rw"
        assert snapshot0.device_name == "dev0"
        assert snapshot0.snapshot_name == "short_snapshot_name"
        assert snapshot0.snapshot_project == "project_name"

        # AND the snapshots_info is included in the JSON output
        snapshots_json = b_info.to_json()["snapshots_info"]
        assert snapshots_json["provider"] == "gcp"
        assert snapshots_json["provider_info"]["project"] == "project_name"
        snapshot0_json = snapshots_json["snapshots"][0]
        assert snapshot0_json["mount"]["mount_point"] == "/opt/mount0"
        assert snapshot0_json["mount"]["mount_options"] == "rw"
        assert snapshot0_json["provider"]["device_name"] == "dev0"
        assert snapshot0_json["provider"]["snapshot_name"] == "short_snapshot_name"
        assert snapshot0_json["provider"]["snapshot_project"] == "project_name"

    def test_with_snapshots_info_azure(self, tmpdir):
        """
        Test that snapshots_info is included in file and output if set.
        """
        # GIVEN a backup.info file for a server
        server = build_mocked_server(
            main_conf={"basebackups_directory": tmpdir.strpath},
        )
        backup_dir = tmpdir.mkdir("fake_name")
        infofile = backup_dir.join("backup.info")
        b_info = LocalBackupInfo(server, backup_id="fake_name")
        b_info.status = BackupInfo.DONE

        # WHEN snapshots_info is set and the file is saved
        snapshots_info = AzureSnapshotsInfo(
            subscription_id="test-subscription",
            resource_group="test-rg",
            snapshots=[
                AzureSnapshotMetadata(
                    mount_point="/opt/mount0",
                    mount_options="rw",
                    lun="10",
                    snapshot_name="short_snapshot_name",
                    location="uksouth",
                )
            ],
        )
        b_info.snapshots_info = snapshots_info
        b_info.save()

        # THEN a new BackupInfo created from the saved file has the SnapshotsInfo attributes
        new_backup_info = LocalBackupInfo(server, info_file=infofile.strpath)
        assert new_backup_info.snapshots_info.provider == "azure"
        assert new_backup_info.snapshots_info.subscription_id == "test-subscription"
        snapshot0 = new_backup_info.snapshots_info.snapshots[0]
        assert snapshot0.mount_point == "/opt/mount0"
        assert snapshot0.mount_options == "rw"
        assert snapshot0.lun == "10"
        assert snapshot0.snapshot_name == "short_snapshot_name"
        assert snapshot0.location == "uksouth"

        # AND the snapshots_info is included in the JSON output
        snapshots_json = b_info.to_json()["snapshots_info"]
        assert snapshots_json["provider"] == "azure"
        assert snapshots_json["provider_info"]["subscription_id"] == "test-subscription"
        snapshot0_json = snapshots_json["snapshots"][0]
        assert snapshot0_json["mount"]["mount_point"] == "/opt/mount0"
        assert snapshot0_json["mount"]["mount_options"] == "rw"
        assert snapshot0_json["provider"]["lun"] == "10"
        assert snapshot0_json["provider"]["snapshot_name"] == "short_snapshot_name"
        assert snapshot0_json["provider"]["location"] == "uksouth"

    def test_with_snapshots_info_aws(self, tmpdir):
        """
        Test that snapshots_info is included in file and output if set.
        """
        # GIVEN a backup.info file for a server
        server = build_mocked_server(
            main_conf={"basebackups_directory": tmpdir.strpath},
        )
        backup_dir = tmpdir.mkdir("fake_name")
        infofile = backup_dir.join("backup.info")
        b_info = LocalBackupInfo(server, backup_id="fake_name")
        b_info.status = BackupInfo.DONE

        # WHEN snapshots_info is set and the file is saved
        snapshots_info = AwsSnapshotsInfo(
            account_id="0123456789",
            region="eu-west-2",
            snapshots=[
                AwsSnapshotMetadata(
                    mount_point="/opt/mount0",
                    mount_options="rw",
                    device_name="/dev/sdf",
                    snapshot_name="user-assigned name",
                    snapshot_id="snap-0123",
                )
            ],
        )
        b_info.snapshots_info = snapshots_info
        b_info.save()

        # THEN a new BackupInfo created from the saved file has the SnapshotsInfo attributes
        new_backup_info = LocalBackupInfo(server, info_file=infofile.strpath)
        assert new_backup_info.snapshots_info.provider == "aws"
        assert new_backup_info.snapshots_info.account_id == "0123456789"
        snapshot0 = new_backup_info.snapshots_info.snapshots[0]
        assert snapshot0.mount_point == "/opt/mount0"
        assert snapshot0.mount_options == "rw"
        assert snapshot0.device_name == "/dev/sdf"
        assert snapshot0.snapshot_name == "user-assigned name"
        assert snapshot0.snapshot_id == "snap-0123"

        # AND the snapshots_info is included in the JSON output
        snapshots_json = b_info.to_json()["snapshots_info"]
        assert snapshots_json["provider"] == "aws"
        assert snapshots_json["provider_info"]["account_id"] == "0123456789"
        snapshot0_json = snapshots_json["snapshots"][0]
        assert snapshot0_json["mount"]["mount_point"] == "/opt/mount0"
        assert snapshot0_json["mount"]["mount_options"] == "rw"
        assert snapshot0_json["provider"]["device_name"] == "/dev/sdf"
        assert snapshot0_json["provider"]["snapshot_name"] == "user-assigned name"
        assert snapshot0_json["provider"]["snapshot_id"] == "snap-0123"

    def test_with_no_snapshots_info(self, tmpdir):
        """
        Test that snapshots_info is not included in file and output if not set.
        """
        # GIVEN a backup.info file for a server
        server = build_mocked_server(
            main_conf={"basebackups_directory": tmpdir.strpath},
        )
        backup_dir = tmpdir.mkdir("fake_name")
        infofile = backup_dir.join("backup.info")
        b_info = LocalBackupInfo(server, backup_id="fake_name")
        b_info.status = BackupInfo.DONE

        # WHEN no snapshots_info is set
        b_info.save()

        # THEN the backup name is not written to the file
        assert "snapshots_info" not in infofile.read()
        # AND the backup name is not included in the JSON output
        assert "snapshots_info" not in b_info.to_json().keys()


class TestLocalBackupInfo:
    """
    Unit tests for :class:`LocalBackupInfo`.
    """

    @pytest.fixture
    def backup_info(self, tmpdir):
        """
        Create a new instance of :class:`LocalBackupInfo`.

        :return LocalBackupInfo: an instance of a local backup info.
        """
        infofile = tmpdir.join("backup.info")
        infofile.write(BASE_BACKUP_INFO)
        # Mock the server, we don't need it at the moment
        server = build_mocked_server()
        # load the data from the backup.info file
        return LocalBackupInfo(server, info_file=infofile.strpath)

    @mock.patch("barman.infofile.LocalBackupInfo.get_data_directory")
    def test_get_backup_manifest_path(self, mock_get_data_dir, backup_info):
        """
        Ensure :meth:`LocalBackupInfo.get_backup_manifest_path` returns the expected
        path for its ``backup_manifest`` file.
        """
        expected = "/some/random/path/backup_manifest"
        mock_get_data_dir.return_value = "/some/random/path"
        assert backup_info.get_backup_manifest_path() == expected

    def test_get_parent_backup_info_no_parent(self, backup_info):
        """
        Ensure :meth:`LocalBackupInfo.get_parent_backup_info` returns ``None`` if the
        backup doesn't have a parent backup.
        """
        backup_info.parent_backup_id = None
        assert backup_info.get_parent_backup_info() is None

    def test_get_parent_backup_info_empty_parent(self, backup_info):
        """
        Ensure :meth:`LocalBackupInfo.get_parent_backup_info` returns ``None`` if the
        backup has a parent backup, but the parent backup is empty.
        """
        backup_info.parent_backup_id = "SOME_ID"

        with patch("barman.infofile.LocalBackupInfo") as mock:
            mock.return_value.status = BackupInfo.EMPTY
            assert backup_info.get_parent_backup_info() is None
            mock.assert_called_once_with(backup_info.server, backup_id="SOME_ID")

    def test_get_parent_backup_info_parent_ok(self, backup_info):
        """
        Ensure :meth:`LocalBackupInfo.get_parent_backup_info` returns the backup info
        object of the parent.
        """
        backup_info.parent_backup_id = "SOME_ID"

        with patch("barman.infofile.LocalBackupInfo") as mock:
            mock.return_value.status = BackupInfo.DONE
            assert backup_info.get_parent_backup_info() is mock.return_value
            mock.assert_called_once_with(backup_info.server, backup_id="SOME_ID")

    def test_get_child_backup_info_no_parent(self, backup_info):
        """
        Ensure :meth:`LocalBackupInfo.get_child_backup_info` returns ``None`` if the
        backup doesn't have children backups.
        """
        backup_info.children_backup_ids = None
        assert backup_info.get_child_backup_info("SOME_ID") is None

    def test_get_child_backup_info_not_a_child(self, backup_info):
        """
        Ensure :meth:`LocalBackupInfo.get_child_backup_info` returns ``None`` if the
        backup has children, but requested ID is not a child.
        """
        backup_info.children_backup_ids = ["SOME_CHILD_ID_1", "SOME_CHILD_ID_2"]
        assert backup_info.get_child_backup_info("SOME_ID") is None

    def test_get_child_backup_info_empty_child(self, backup_info):
        """
        Ensure :meth:`LocalBackupInfo.get_child_backup_info` returns ``None`` if the
        backup has children, but requested ID is from an empty child.
        """
        backup_info.children_backup_ids = ["SOME_CHILD_ID_1", "SOME_CHILD_ID_2"]

        with patch("barman.infofile.LocalBackupInfo") as mock:
            mock.return_value.status = BackupInfo.EMPTY
            assert backup_info.get_child_backup_info("SOME_CHILD_ID_1") is None
            mock.assert_called_once_with(
                backup_info.server,
                backup_id="SOME_CHILD_ID_1",
            )

    def test_get_child_backup_info_child_ok(self, backup_info):
        """
        Ensure :meth:`LocalBackupInfo.get_child_backup_info` returns the backup info
        object of the requested child.
        """
        backup_info.children_backup_ids = ["SOME_CHILD_ID_1", "SOME_CHILD_ID_2"]

        with patch("barman.infofile.LocalBackupInfo") as mock:
            mock.return_value.status = BackupInfo.DONE
            assert (
                backup_info.get_child_backup_info("SOME_CHILD_ID_1")
                is mock.return_value
            )
            mock.assert_called_once_with(
                backup_info.server,
                backup_id="SOME_CHILD_ID_1",
            )

    def test_walk_to_root(self, backup_info):
        """
        Unit test for :meth:`LocalBackupInfo.walk_to_root` method.

        This test checks if the method correctly walks through all the parent backups
        of the current backup and returns a generator of :class:`LocalBackupInfo`
        objects for each parent backup.
        """
        # Create a LocalBackupInfo used as a model for the parent backups
        # inside the side_effect function `provide_parent_backup_info`
        model_backup_info = LocalBackupInfo(
            backup_info.server, backup_id="model_backup"
        )

        def provide_parent_backup_info(server, backup_id):
            """
            Helper function to provide a :class:`LocalBackupInfo` object for a given
            backup ID.
            """
            next_backup_id = int(backup_id[-1]) + 1
            parent_backup_info = copy.copy(model_backup_info)
            parent_backup_info.backup_id = backup_id
            parent_backup_info.status = BackupInfo.DONE
            if next_backup_id < 4:
                parent_backup_info.parent_backup_id = (
                    "parent_backup_id%s" % next_backup_id
                )
            else:
                parent_backup_info.parent_backup_id = None
            return parent_backup_info

        # Create parent backup info objects
        backup_info.parent_backup_id = "parent_backup_id1"
        with mock.patch(
            "barman.infofile.LocalBackupInfo",
            side_effect=provide_parent_backup_info,
        ):
            # Call the walk_to_root method
            result = list(backup_info.walk_to_root(return_self=False))

            # Check if the method correctly walks through all the parent backups
            # in the correct order
            assert len(result) == 3
            assert result[0].backup_id == "parent_backup_id1"
            assert result[1].backup_id == "parent_backup_id2"
            assert result[2].backup_id == "parent_backup_id3"

        # Test case for when the method is set to also return the current backup
        backup_info.backup_id = "incremental_backup_id"
        with mock.patch(
            "barman.infofile.LocalBackupInfo",
            side_effect=provide_parent_backup_info,
        ):
            # Call the walk_to_root method with include_self=True
            result = list(backup_info.walk_to_root())

            # Check if the method includes the current backup and walks through all
            # the parent backups in the correct order and ALSO yields the current
            # backup
            assert len(result) == 4
            assert result[0].backup_id == "incremental_backup_id"
            assert result[1].backup_id == "parent_backup_id1"
            assert result[2].backup_id == "parent_backup_id2"
            assert result[3].backup_id == "parent_backup_id3"

    def test_walk_backups_tree(self):
        """
        Unit test for the :meth:`LocalBackupInfo.walk_backups_tree` method.
        """
        # Create a mock server
        server = build_mocked_server()
        # Create a LocalBackupInfo used as a model for the parent backups
        # inside the side_effect function `provide_parent_backup_info`
        model_backup_info = LocalBackupInfo(server, backup_id="model_backup")

        def provide_child_backup_info(server, backup_id):
            """
            Helper function to provide a :class:`LocalBackupInfo` object for a given
            backup ID of a child backup.
            """
            if backup_id == "child_backup1":
                child1_backup_info = copy.copy(model_backup_info)
                child1_backup_info.backup_id = "child_backup1"
                child1_backup_info.status = BackupInfo.DONE
                child1_backup_info.parent_backup_id = "root_backup"
                child1_backup_info.children_backup_ids = ["child_backup3"]
                return child1_backup_info
            if backup_id == "child_backup2":
                child2_backup_info = copy.copy(model_backup_info)
                child2_backup_info.backup_id = "child_backup2"
                child2_backup_info.status = BackupInfo.DONE
                child2_backup_info.parent_backup_id = "root_backup"
                return child2_backup_info
            if backup_id == "child_backup3":
                child3_backup_info = copy.copy(model_backup_info)
                child3_backup_info.backup_id = "child_backup3"
                child3_backup_info.status = BackupInfo.DONE
                child3_backup_info.parent_backup_id = "child_backup1"
                child3_backup_info.children_backup_ids = []
                return child3_backup_info

        # Create a root backup info object
        # the final structure of the backups tree is:
        #          root_backup
        #          /         \
        #   child_backup1 child_backup2
        #       /
        # child_backup3
        root_backup_info = LocalBackupInfo(server, backup_id="root_backup")
        root_backup_info.status = BackupInfo.DONE
        root_backup_info.children_backup_ids = ["child_backup1", "child_backup2"]

        # Mock the `LocalBackupInfo` constructor to return the corresponding backup info objects
        with patch(
            "barman.infofile.LocalBackupInfo",
            side_effect=provide_child_backup_info,
        ):
            # Call the `walk_backups_tree` method on the root backup info
            backups = list(root_backup_info.walk_backups_tree())
            # Assert that the backups are returned in the correct order
            # We want to walk through the tree in a depth-first post order,
            # so leaf nodes are visited first, then their parent, and so on.
            assert len(backups) == 4
            assert backups[0].backup_id == "child_backup3"
            assert backups[1].backup_id == "child_backup1"
            assert backups[2].backup_id == "child_backup2"
            assert backups[3].backup_id == "root_backup"

            # Call the `walk_backups_tree` method on the root backup info
            backups = list(root_backup_info.walk_backups_tree(return_self=False))
            # Assert that the backups are returned in the correct order
            # We want to walk through the tree in a depth-first post order,
            # so leaf nodes are visited first, then their parent, and so on.
            assert len(backups) == 3
            assert backups[0].backup_id == "child_backup3"
            assert backups[1].backup_id == "child_backup1"
            assert backups[2].backup_id == "child_backup2"

    def test_true_is_full_and_eligible_for_incremental(self):
        """
        Test that the function applies the correct conditions for a full backup
        that is eligible for incremental mode. The backup_method should be `postgres`,
        the summarize_wal should be `on` and parent_backup_id should be `None`
        """
        backup_method = "postgres"
        summarize_wal = "on"
        parent_backup_id = None

        pg_backup_manager = build_backup_manager(
            main_conf={"backup_method": backup_method}
        )

        backup_info = build_test_backup_info(
            server=pg_backup_manager.server,
            backup_id="12345",
            parent_backup_id=parent_backup_id,
            summarize_wal=summarize_wal,
        )

        assert backup_info.is_full_and_eligible_for_incremental()

    @pytest.mark.parametrize(
        ("summarize_wal", "parent_backup_id"),
        (("on", "12345678"), ("off", None), ("off", "12345678")),
    )
    def test_false_is_full_and_eligible_for_incremental(
        self, summarize_wal, parent_backup_id
    ):
        """
        Test that the function applies the correct conditions for a full backup
        that is eligible for incremental mode. The backup_method should be `postgres`,
        the summarize_wal should be `on` and parent_backup_id should be `None`
        """
        backup_method = "postgres"

        backup_manager = build_backup_manager(
            main_conf={"backup_method": backup_method}
        )

        backup_info = build_test_backup_info(
            server=backup_manager.server,
            backup_id="12345",
            parent_backup_id=parent_backup_id,
            summarize_wal=summarize_wal,
        )

        assert not backup_info.is_full_and_eligible_for_incremental()

        backup_method = "rsync"

        backup_manager = build_backup_manager(
            main_conf={"backup_method": backup_method}
        )

        backup_info = build_test_backup_info(
            server=backup_manager.server,
            backup_id="12345",
            parent_backup_id=parent_backup_id,
            summarize_wal=summarize_wal,
        )

        assert not backup_info.is_full_and_eligible_for_incremental()


class TestSyntheticBackupInfo:
    """
    this class tests the methods of the SyntheticBackupInfo object
    """

    def test_init_synthetic_backup_info_with_backup_id(self):
        """
        Unit test for the __init__ method using backup_id.

        Create mock server and a SyntheticBackupInfo object.

        This unit tests checks:
            * base_directory parameter
            * backup_id parameter
            * instance type
        """
        server = build_mocked_server()
        base_directory = "fake/path/"
        backup_id = "fake_name"
        obj = SyntheticBackupInfo(
            server=server,
            base_directory=base_directory,
            backup_id=backup_id,
            info_file=None,
        )
        assert obj.base_directory == base_directory
        assert obj.backup_id == backup_id
        assert isinstance(obj, SyntheticBackupInfo)

    def test_init_synthetic_backup_info_with_info_file(self, tmpdir):
        """
        Unit test for the __init__ method using info_file.

        Create mock server and a SyntheticBackupInfo object.

        This unit tests checks:
            * base_directory parameter
            * filename parameter
            * instance type
        """
        server = build_mocked_server()
        base_directory = "fake/path/"
        backup_id = "fake_name"
        infofile = tmpdir.mkdir(backup_id).join("backup.info")
        infofile.write(BASE_BACKUP_INFO)
        obj = SyntheticBackupInfo(
            server=server,
            base_directory=base_directory,
            backup_id=None,
            info_file=infofile.strpath,
        )
        assert obj.base_directory == base_directory
        assert obj.filename == infofile.strpath
        assert isinstance(obj, SyntheticBackupInfo)

    def test_get_basebackup_directory(self):
        """
        Unit test for the get_basebackup_directory.

        Create mock server and a SyntheticBackupInfo object.

        This unit tests checks if the method returns the correct path based on
        base_directory and backup_id.
        """
        server = build_mocked_server()
        backup_info = SyntheticBackupInfo(
            server=server, base_directory="/fake/path/", backup_id="fake_name"
        )
        directory = backup_info.get_basebackup_directory()
        assert directory == "/fake/path/fake_name"
