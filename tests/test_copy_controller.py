# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2021
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

import multiprocessing.dummy
import os
from datetime import datetime

import dateutil.tz
import mock
import pytest
from mock import patch

from barman.copy_controller import (
    BUCKET_SIZE,
    RsyncCopyController,
    _FileItem,
    _RsyncCopyItem,
)
from barman.exceptions import CommandFailedException, RsyncListFilesFailure
from testing_helpers import (
    build_backup_manager,
    build_real_server,
    build_test_backup_info,
)


# noinspection PyMethodMayBeStatic
class TestRsyncCopyController(object):
    """
    This class tests the methods of the RsyncCopyController object
    """

    def test_rsync_backup_executor_init(self):
        """
        Test the construction of a RsyncCopyController
        """

        # Build the prerequisites
        backup_manager = build_backup_manager()
        server = backup_manager.server
        config = server.config
        executor = server.executor

        # Test
        assert RsyncCopyController(
            path=server.path,
            ssh_command=executor.ssh_command,
            ssh_options=executor.ssh_options,
            network_compression=config.network_compression,
            reuse_backup=None,
            safe_horizon=None,
        )

    def test_reuse_args(self):
        """
        Simple test for the _reuse_args method

        The method is necessary for the execution of incremental backups,
        we need to test that the method build correctly the rsync option that
        enables the incremental backup
        """
        # Build the prerequisites
        backup_manager = build_backup_manager()
        server = backup_manager.server
        config = server.config
        executor = server.executor

        rcc = RsyncCopyController(
            path=server.path,
            ssh_command=executor.ssh_command,
            ssh_options=executor.ssh_options,
            network_compression=config.network_compression,
            reuse_backup=None,
            safe_horizon=None,
        )

        reuse_dir = "some/dir"

        # Test for disabled incremental
        assert rcc._reuse_args(reuse_dir) == []

        # Test for link incremental
        rcc.reuse_backup = "link"
        assert rcc._reuse_args(reuse_dir) == ["--link-dest=some/dir"]

        # Test for copy incremental
        rcc.reuse_backup = "copy"
        assert rcc._reuse_args(reuse_dir) == ["--copy-dest=some/dir"]

    @patch("barman.copy_controller.Pool", new=multiprocessing.dummy.Pool)
    @patch("barman.copy_controller.RsyncPgData")
    @patch("barman.copy_controller.RsyncCopyController._analyze_directory")
    @patch("barman.copy_controller.RsyncCopyController._create_dir_and_purge")
    @patch("barman.copy_controller.RsyncCopyController._copy")
    @patch("tempfile.mkdtemp")
    @patch("signal.signal")
    def test_full_copy(
        self,
        signal_mock,
        tempfile_mock,
        copy_mock,
        create_and_purge_mock,
        analyse_mock,
        rsync_mock,
        tmpdir,
    ):
        """
        Test the execution of a full copy
        """

        # Build the prerequisites
        tempdir = tmpdir.mkdir("tmp")
        tempfile_mock.return_value = tempdir.strpath
        server = build_real_server(
            global_conf={"barman_home": tmpdir.mkdir("home").strpath}
        )
        config = server.config
        executor = server.backup_manager.executor

        rcc = RsyncCopyController(
            path=server.path,
            ssh_command=executor.ssh_command,
            ssh_options=executor.ssh_options,
            network_compression=config.network_compression,
            reuse_backup=None,
            safe_horizon=None,
        )

        backup_info = build_test_backup_info(
            server=server,
            pgdata="/pg/data",
            config_file="/etc/postgresql.conf",
            hba_file="/pg/data/pg_hba.conf",
            ident_file="/pg/data/pg_ident.conf",
            begin_xlog="0/2000028",
            begin_wal="000000010000000000000002",
            begin_offset=28,
        )
        backup_info.save()
        # This is to check that all the preparation is done correctly
        assert os.path.exists(backup_info.filename)

        # Silence the access to result properties
        rsync_mock.return_value.out = ""
        rsync_mock.return_value.err = ""
        rsync_mock.return_value.ret = 0

        # Mock analyze directory
        def analyse_func(item):
            label = item.label
            item.dir_file = label + "_dir_file"
            item.exclude_and_protect_file = label + "_exclude_and_protect_file"
            item.safe_list = [_FileItem("mode", 1, "date", "path")]
            item.check_list = [_FileItem("mode", 1, "date", "path")]

        analyse_mock.side_effect = analyse_func

        rcc.add_directory(
            label="tbs1",
            src=":/fake/location/",
            dst=backup_info.get_data_directory(16387),
            reuse=None,
            bwlimit=None,
            item_class=rcc.TABLESPACE_CLASS,
        )
        rcc.add_directory(
            label="tbs2",
            src=":/another/location/",
            dst=backup_info.get_data_directory(16405),
            reuse=None,
            bwlimit=None,
            item_class=rcc.TABLESPACE_CLASS,
        )
        rcc.add_directory(
            label="pgdata",
            src=":/pg/data/",
            dst=backup_info.get_data_directory(),
            reuse=None,
            bwlimit=None,
            item_class=rcc.PGDATA_CLASS,
            exclude=[
                "/pg_xlog/*",
                "/pg_log/*",
                "/log/*",
                "/recovery.conf",
                "/postmaster.pid",
            ],
            exclude_and_protect=["pg_tblspc/16387", "pg_tblspc/16405"],
        )
        rcc.add_file(
            label="pg_control",
            src=":/pg/data/global/pg_control",
            dst="%s/global/pg_control" % backup_info.get_data_directory(),
            item_class=rcc.PGCONTROL_CLASS,
        )
        rcc.add_file(
            label="config_file",
            src=":/etc/postgresql.conf",
            dst=backup_info.get_data_directory(),
            item_class=rcc.CONFIG_CLASS,
            optional=False,
        )
        rcc.copy()

        # Check the order of calls to the Rsync mock
        assert rsync_mock.mock_calls == [
            mock.call(
                network_compression=False,
                args=[
                    "--ignore-missing-args",
                    "--itemize-changes",
                    "--itemize-changes",
                ],
                bwlimit=None,
                ssh="ssh",
                path=None,
                ssh_options=[
                    "-c",
                    '"arcfour"',
                    "-p",
                    "22",
                    "postgres@pg01.nowhere",
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    "StrictHostKeyChecking=no",
                ],
                exclude=None,
                exclude_and_protect=None,
                include=None,
                retry_sleep=0,
                retry_times=0,
                retry_handler=mock.ANY,
            ),
            mock.call(
                network_compression=False,
                args=[
                    "--ignore-missing-args",
                    "--itemize-changes",
                    "--itemize-changes",
                ],
                bwlimit=None,
                ssh="ssh",
                path=None,
                ssh_options=[
                    "-c",
                    '"arcfour"',
                    "-p",
                    "22",
                    "postgres@pg01.nowhere",
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    "StrictHostKeyChecking=no",
                ],
                exclude=None,
                exclude_and_protect=None,
                include=None,
                retry_sleep=0,
                retry_times=0,
                retry_handler=mock.ANY,
            ),
            mock.call(
                network_compression=False,
                args=[
                    "--ignore-missing-args",
                    "--itemize-changes",
                    "--itemize-changes",
                ],
                bwlimit=None,
                ssh="ssh",
                path=None,
                ssh_options=[
                    "-c",
                    '"arcfour"',
                    "-p",
                    "22",
                    "postgres@pg01.nowhere",
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    "StrictHostKeyChecking=no",
                ],
                exclude=[
                    "/pg_xlog/*",
                    "/pg_log/*",
                    "/log/*",
                    "/recovery.conf",
                    "/postmaster.pid",
                ],
                exclude_and_protect=["pg_tblspc/16387", "pg_tblspc/16405"],
                include=None,
                retry_sleep=0,
                retry_times=0,
                retry_handler=mock.ANY,
            ),
            mock.call(
                network_compression=False,
                args=[
                    "--ignore-missing-args",
                    "--itemize-changes",
                    "--itemize-changes",
                ],
                bwlimit=None,
                ssh="ssh",
                path=None,
                ssh_options=[
                    "-c",
                    '"arcfour"',
                    "-p",
                    "22",
                    "postgres@pg01.nowhere",
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    "StrictHostKeyChecking=no",
                ],
                exclude=None,
                exclude_and_protect=None,
                include=None,
                retry_sleep=0,
                retry_times=0,
                retry_handler=mock.ANY,
            ),
            mock.call()(
                ":/etc/postgresql.conf",
                backup_info.get_data_directory(),
                allowed_retval=(0, 23, 24),
            ),
            mock.call(
                network_compression=False,
                args=[
                    "--ignore-missing-args",
                    "--itemize-changes",
                    "--itemize-changes",
                ],
                bwlimit=None,
                ssh="ssh",
                path=None,
                ssh_options=[
                    "-c",
                    '"arcfour"',
                    "-p",
                    "22",
                    "postgres@pg01.nowhere",
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    "StrictHostKeyChecking=no",
                ],
                exclude=None,
                exclude_and_protect=None,
                include=None,
                retry_sleep=0,
                retry_times=0,
                retry_handler=mock.ANY,
            ),
            mock.call()(
                ":/pg/data/global/pg_control",
                "%s/global/pg_control" % backup_info.get_data_directory(),
                allowed_retval=(0, 23, 24),
            ),
        ]

        # Check calls to _analyse_directory method
        assert analyse_mock.mock_calls == [
            mock.call(item) for item in rcc.item_list if item.is_directory
        ]

        # Check calls to _create_dir_and_purge method
        assert create_and_purge_mock.mock_calls == [
            mock.call(item) for item in rcc.item_list if item.is_directory
        ]

        # Utility function to build the file_list name
        def file_list_name(label, kind):
            return "%s/%s_%s_%s.list" % (tempdir.strpath, label, kind, os.getpid())

        # Check the order of calls to the copy method
        # All the file_list arguments are None because the analyze part
        # has not really been executed
        assert copy_mock.mock_calls == [
            mock.call(
                mock.ANY,
                ":/fake/location/",
                backup_info.get_data_directory(16387),
                checksum=False,
                file_list=file_list_name("tbs1", "safe"),
            ),
            mock.call(
                mock.ANY,
                ":/fake/location/",
                backup_info.get_data_directory(16387),
                checksum=True,
                file_list=file_list_name("tbs1", "check"),
            ),
            mock.call(
                mock.ANY,
                ":/another/location/",
                backup_info.get_data_directory(16405),
                checksum=False,
                file_list=file_list_name("tbs2", "safe"),
            ),
            mock.call(
                mock.ANY,
                ":/another/location/",
                backup_info.get_data_directory(16405),
                checksum=True,
                file_list=file_list_name("tbs2", "check"),
            ),
            mock.call(
                mock.ANY,
                ":/pg/data/",
                backup_info.get_data_directory(),
                checksum=False,
                file_list=file_list_name("pgdata", "safe"),
            ),
            mock.call(
                mock.ANY,
                ":/pg/data/",
                backup_info.get_data_directory(),
                checksum=True,
                file_list=file_list_name("pgdata", "check"),
            ),
        ]

    @patch("barman.copy_controller.RsyncCopyController._rsync_factory")
    def test_list_files(self, rsync_factory_mock):
        """
        Unit test for RsyncCopyController._list_file's code
        """
        # Mock rsync invocation
        rsync_mock = mock.Mock(name="Rsync()")
        rsync_mock.ret = 0
        rsync_mock.out = (
            "drwxrwxrwt       69632 2015/02/09 15:01:00 tmp\n"
            "drwxrwxrwt       69612 Thu Feb 19 15:01:22 2015 tmp2"
        )
        rsync_mock.err = "err"

        # Mock _rsync_factory() invocation
        rsync_factory_mock.return_value = rsync_mock

        # Create an item to inspect
        item = _RsyncCopyItem(
            label="pgdata",
            src=":/pg/data/",
            dst="/some/dir",
            is_directory=True,
            item_class=RsyncCopyController.PGDATA_CLASS,
            optional=False,
        )

        # Test the _list_files internal method
        rcc = RsyncCopyController()
        return_values = list(rcc._list_files(item, "some/path"))

        # Returned list must contain two elements
        assert len(return_values) == 2

        # Verify that _rsync_factory has been called correctly
        assert rsync_factory_mock.mock_calls == [
            mock.call(item),
        ]

        # Check rsync.get_output has called correctly
        rsync_mock.get_output.assert_called_with(
            "--no-human-readable", "--list-only", "-r", "some/path", check=True
        )

        # Check the result
        assert return_values[0] == _FileItem(
            "drwxrwxrwt",
            69632,
            datetime(
                year=2015,
                month=2,
                day=9,
                hour=15,
                minute=1,
                second=0,
                tzinfo=dateutil.tz.tzlocal(),
            ),
            "tmp",
        )
        assert return_values[1] == _FileItem(
            "drwxrwxrwt",
            69612,
            datetime(
                year=2015,
                month=2,
                day=19,
                hour=15,
                minute=1,
                second=22,
                tzinfo=dateutil.tz.tzlocal(),
            ),
            "tmp2",
        )

        # Test the _list_files internal method with a wrong output (added TZ)
        rsync_mock.out = "drwxrwxrwt       69612 Thu Feb 19 15:01:22 CET 2015 tmp2\n"

        rcc = RsyncCopyController()
        with pytest.raises(RsyncListFilesFailure):
            # The list() call is needed to consume the generator
            list(rcc._list_files(rsync_mock, "some/path"))

        # Check rsync.get_output has called correctly
        rsync_mock.get_output.assert_called_with(
            "--no-human-readable", "--list-only", "-r", "some/path", check=True
        )

    def test_fill_buckets(self):
        """
        Unit test for RsyncCopyController._fill_buckets's code
        """

        # Create a fake file list af about 525 GB of files
        filedate = datetime(
            year=2015,
            month=2,
            day=19,
            hour=15,
            minute=1,
            second=22,
            tzinfo=dateutil.tz.tzlocal(),
        )
        file_list = []
        total_size = 0
        for i in range(1001):
            # We are using a prime number to get a non-correlable distribution
            # of file sizes in the buckets
            size = 1048583 * i
            file_list.append(_FileItem("drwxrwxrwt", size, filedate, "tmp%08d" % i))
            total_size += size

        # Test the _fill_buckets internal method with only one worker:
        # the result must be a bucket with the same list passed as argument
        rcc = RsyncCopyController(workers=1)
        buckets = list(rcc._fill_buckets(file_list))
        assert len(buckets) == 1
        assert buckets[0] == file_list

        # Test the _fill_buckets internal method with multiple workers
        # the result must be a bucket with the same list passed as argument
        for workers in range(2, 17):
            rcc = RsyncCopyController(workers=workers)
            buckets = list(rcc._fill_buckets(file_list))
            # There is enough buckets to contains all the files
            assert len(buckets) >= int(total_size / BUCKET_SIZE)
            for i, bucket in enumerate(buckets):
                size = sum([f.size for f in bucket])
                # The bucket is not bigger than BUCKET_SIZE
                assert size < BUCKET_SIZE, "Bucket %s (%s) size %s too big" % (
                    i,
                    workers,
                    size,
                )
                # The bucket cannot be empty
                assert len(bucket), "Bucket %s (%s) is empty" % (i, workers)

    @patch("barman.copy_controller.RsyncCopyController._list_files")
    def test_analyze_directory(self, list_files_mock, tmpdir):
        """
        Unit test for RsyncCopyController._analyze_directory's code
        """

        # Build file list for ref
        ref_list = [
            _FileItem(
                "drwxrwxrwt",
                69632,
                datetime(
                    year=2015,
                    month=2,
                    day=9,
                    hour=15,
                    minute=1,
                    second=0,
                    tzinfo=dateutil.tz.tzlocal(),
                ),
                ".",
            ),
            _FileItem(
                "drwxrwxrwt",
                69612,
                datetime(
                    year=2015,
                    month=2,
                    day=19,
                    hour=15,
                    minute=1,
                    second=22,
                    tzinfo=dateutil.tz.tzlocal(),
                ),
                "tmp",
            ),
            _FileItem(
                "-rw-r--r--",
                69632,
                datetime(
                    year=2015,
                    month=2,
                    day=20,
                    hour=18,
                    minute=15,
                    second=33,
                    tzinfo=dateutil.tz.tzlocal(),
                ),
                "tmp/safe",
            ),
            _FileItem(
                "-rw-r--r--",
                69612,
                datetime(
                    year=2015,
                    month=2,
                    day=20,
                    hour=19,
                    minute=15,
                    second=33,
                    tzinfo=dateutil.tz.tzlocal(),
                ),
                "tmp/check",
            ),
            _FileItem(
                "-rw-r--r--",
                69612,
                datetime(
                    year=2015,
                    month=2,
                    day=20,
                    hour=19,
                    minute=15,
                    second=33,
                    tzinfo=dateutil.tz.tzlocal(),
                ),
                "tmp/diff_time",
            ),
            _FileItem(
                "-rw-r--r--",
                69612,
                datetime(
                    year=2015,
                    month=2,
                    day=20,
                    hour=19,
                    minute=15,
                    second=33,
                    tzinfo=dateutil.tz.tzlocal(),
                ),
                "tmp/diff_size",
            ),
        ]

        # Build the list for source adding a new file, ...
        src_list = ref_list + [
            _FileItem(
                "-rw-r--r--",
                69612,
                datetime(
                    year=2015,
                    month=2,
                    day=20,
                    hour=22,
                    minute=15,
                    second=33,
                    tzinfo=dateutil.tz.tzlocal(),
                ),
                "tmp/new",
            ),
        ]
        # ... changing the timestamp one old file ...
        src_list[4] = _FileItem(
            "-rw-r--r--",
            69612,
            datetime(
                year=2015,
                month=2,
                day=20,
                hour=20,
                minute=15,
                second=33,
                tzinfo=dateutil.tz.tzlocal(),
            ),
            "tmp/diff_time",
        )
        # ... and changing the size of another
        src_list[5] = _FileItem(
            "-rw-r--r--",
            77777,
            datetime(
                year=2015,
                month=2,
                day=20,
                hour=19,
                minute=15,
                second=33,
                tzinfo=dateutil.tz.tzlocal(),
            ),
            "tmp/diff_size",
        )

        # Apply it to _list_files calls
        list_files_mock.side_effect = [ref_list, src_list]

        # Build the prerequisites
        server = build_real_server(
            global_conf={"barman_home": tmpdir.mkdir("home").strpath}
        )
        config = server.config
        executor = server.backup_manager.executor

        # Create the RsyncCopyController putting the safe_horizon between
        # the tmp/safe and tmp2/check timestamps
        rcc = RsyncCopyController(
            path=server.path,
            ssh_command=executor.ssh_command,
            ssh_options=executor.ssh_options,
            network_compression=config.network_compression,
            reuse_backup=None,
            safe_horizon=datetime(
                year=2015,
                month=2,
                day=20,
                hour=19,
                minute=0,
                second=0,
                tzinfo=dateutil.tz.tzlocal(),
            ),
        )

        backup_info = build_test_backup_info(
            server=server,
            pgdata="/pg/data",
            config_file="/etc/postgresql.conf",
            hba_file="/pg/data/pg_hba.conf",
            ident_file="/pg/data/pg_ident.conf",
            begin_xlog="0/2000028",
            begin_wal="000000010000000000000002",
            begin_offset=28,
        )
        backup_info.save()
        # This is to check that all the preparation is done correctly
        assert os.path.exists(backup_info.filename)

        # Add a temp dir (usually created by copy method
        rcc.temp_dir = tmpdir.mkdir("tmp").strpath

        # Create an item to inspect
        item = _RsyncCopyItem(
            label="pgdata",
            src=":/pg/data/",
            dst=backup_info.get_data_directory(),
            is_directory=True,
            item_class=rcc.PGDATA_CLASS,
            optional=False,
        )

        # Then run the _analyze_directory method
        rcc._analyze_directory(item)

        # Verify that _list_files has been called correctly
        assert list_files_mock.mock_calls == [
            mock.call(item, backup_info.get_data_directory() + "/"),
            mock.call(item, ":/pg/data/"),
        ]

        # Check the result
        # 1) The list of directories should be there and should contain all
        # the directories
        assert item.dir_file
        assert open(item.dir_file).read() == (".\ntmp\n")
        # The exclude_and_protect file should be populated correctly with all
        # the files in the source
        assert item.exclude_and_protect_file
        assert open(item.exclude_and_protect_file).read() == (
            "P /tmp/safe\n"
            "- /tmp/safe\n"
            "P /tmp/check\n"
            "- /tmp/check\n"
            "P /tmp/diff_time\n"
            "- /tmp/diff_time\n"
            "P /tmp/diff_size\n"
            "- /tmp/diff_size\n"
            "P /tmp/new\n"
            "- /tmp/new\n"
        )
        # The check list must contain identical files after the safe_horizon
        assert len(item.check_list) == 1
        assert item.check_list[0].path == "tmp/check"
        # The safe list must contain every file that is not in check and is
        # present in the source
        assert len(item.safe_list) == 4
        assert item.safe_list[0].path == "tmp/safe"
        assert item.safe_list[1].path == "tmp/diff_time"
        assert item.safe_list[2].path == "tmp/diff_size"
        assert item.safe_list[3].path == "tmp/new"

    @patch("barman.copy_controller.RsyncCopyController._rsync_factory")
    @patch("barman.copy_controller.RsyncCopyController._rsync_ignore_vanished_files")
    def test_create_dir_and_purge(self, rsync_ignore_mock, rsync_factory_mock, tmpdir):
        """
        Unit test for RsyncCopyController._create_dir_and_purge's code
        """
        # Build the prerequisites
        server = build_real_server(
            global_conf={"barman_home": tmpdir.mkdir("home").strpath}
        )
        config = server.config
        executor = server.backup_manager.executor

        # Create the RsyncCopyController putting the safe_horizon between
        # the tmp/safe and tmp2/check timestamps
        rcc = RsyncCopyController(
            path=server.path,
            ssh_command=executor.ssh_command,
            ssh_options=executor.ssh_options,
            network_compression=config.network_compression,
            reuse_backup=None,
            safe_horizon=datetime(
                year=2015,
                month=2,
                day=20,
                hour=19,
                minute=0,
                second=0,
                tzinfo=dateutil.tz.tzlocal(),
            ),
        )

        backup_info = build_test_backup_info(
            server=server,
            pgdata="/pg/data",
            config_file="/etc/postgresql.conf",
            hba_file="/pg/data/pg_hba.conf",
            ident_file="/pg/data/pg_ident.conf",
            begin_xlog="0/2000028",
            begin_wal="000000010000000000000002",
            begin_offset=28,
        )
        backup_info.save()
        # This is to check that all the preparation is done correctly
        assert os.path.exists(backup_info.filename)

        # Create an item to inspect
        item = _RsyncCopyItem(
            label="pgdata",
            src=":/pg/data/",
            dst=backup_info.get_data_directory(),
            is_directory=True,
            item_class=rcc.PGDATA_CLASS,
            optional=False,
        )

        # Then run the _create_dir_and_purge method
        rcc._create_dir_and_purge(item)

        # Verify that _rsync_factory has been called correctly
        assert rsync_factory_mock.mock_calls == [
            mock.call(item),
        ]

        # Verify that _rsync_ignore_vanished_files has been called correctly
        assert rsync_ignore_mock.mock_calls == [
            mock.call(
                rsync_factory_mock.return_value,
                "--recursive",
                "--delete",
                "--files-from=None",
                "--filter",
                "merge None",
                ":/pg/data/",
                backup_info.get_data_directory(),
                check=True,
            ),
        ]

    @patch("barman.copy_controller.RsyncCopyController._rsync_ignore_vanished_files")
    def test_copy(self, rsync_ignore_mock, tmpdir):
        """
        Unit test for RsyncCopyController._copy's code
        """
        # Build the prerequisites
        server = build_real_server(
            global_conf={"barman_home": tmpdir.mkdir("home").strpath}
        )
        config = server.config
        executor = server.backup_manager.executor

        # Create the RsyncCopyController putting the safe_horizon between
        # the tmp/safe and tmp2/check timestamps
        rcc = RsyncCopyController(
            path=server.path,
            ssh_command=executor.ssh_command,
            ssh_options=executor.ssh_options,
            network_compression=config.network_compression,
            reuse_backup=None,
            safe_horizon=datetime(
                year=2015,
                month=2,
                day=20,
                hour=19,
                minute=0,
                second=0,
                tzinfo=dateutil.tz.tzlocal(),
            ),
        )

        backup_info = build_test_backup_info(
            server=server,
            pgdata="/pg/data",
            config_file="/etc/postgresql.conf",
            hba_file="/pg/data/pg_hba.conf",
            ident_file="/pg/data/pg_ident.conf",
            begin_xlog="0/2000028",
            begin_wal="000000010000000000000002",
            begin_offset=28,
        )
        backup_info.save()
        # This is to check that all the preparation is done correctly
        assert os.path.exists(backup_info.filename)

        # Create an rsync mock
        rsync_mock = mock.Mock(name="Rsync()")

        # Then run the _copy method
        rcc._copy(
            rsync_mock,
            ":/pg/data/",
            backup_info.get_data_directory(),
            "/path/to/file.list",
            checksum=True,
        )

        # Verify that _rsync_ignore_vanished_files has been called correctly
        assert rsync_ignore_mock.mock_calls == [
            mock.call(
                rsync_mock,
                ":/pg/data/",
                backup_info.get_data_directory(),
                "--files-from=/path/to/file.list",
                "--checksum",
                check=True,
            ),
        ]

        # Try again without checksum
        rsync_ignore_mock.reset_mock()
        rcc._copy(
            rsync_mock,
            ":/pg/data/",
            backup_info.get_data_directory(),
            "/path/to/file.list",
            checksum=False,
        )

        # Verify that _rsync_ignore_vanished_files has been called correctly
        assert rsync_ignore_mock.mock_calls == [
            mock.call(
                rsync_mock,
                ":/pg/data/",
                backup_info.get_data_directory(),
                "--files-from=/path/to/file.list",
                check=True,
            ),
        ]

    def test_rsync_ignore_vanished_files(self):
        """
        Unit test for RsyncCopyController._rsync_ignore_vanished_files's code
        """
        # Create the RsyncCopyController
        rcc = RsyncCopyController()

        # Create an rsync mock
        rsync_mock = mock.Mock(name="Rsync()")
        rsync_mock.out = "out"
        rsync_mock.err = "err"
        rsync_mock.ret = 0

        # Then run the _copy method
        out, err = rcc._rsync_ignore_vanished_files(rsync_mock, 1, 2, a=3, b=4)

        # Verify that rsync has been called correctly
        assert rsync_mock.mock_calls == [
            mock.call.get_output(1, 2, a=3, b=4, allowed_retval=(0, 23, 24))
        ]

        # Verify the result
        assert out == rsync_mock.out
        assert err == rsync_mock.err

        # Check with return code != 0
        # 24 - Partial transfer due to vanished source files
        rsync_mock.reset_mock()
        rsync_mock.ret = 24
        rcc._rsync_ignore_vanished_files(rsync_mock, 1, 2, a=3, b=4)

        # Check with return code != 0
        # 23 - Partial transfer due to error
        # This should raise because the error contains an invalid response
        rsync_mock.reset_mock()
        rsync_mock.ret = 23
        with pytest.raises(CommandFailedException):
            rcc._rsync_ignore_vanished_files(rsync_mock, 1, 2, a=3, b=4)

        # Check with return code != 0
        # 23 - Partial transfer due to error
        # This should not raise
        rsync_mock.reset_mock()
        rsync_mock.ret = 23
        rsync_mock.err = (
            # a file has vanished before rsync start
            'rsync: link_stat "a/file" failed: No such file or directory (2)\n'
            # files which vanished after rsync start
            'file has vanished: "some/other/file"\n'
            # files which have been truncated during transfer
            'rsync: read errors mapping "/truncated": No data available (61)\n'
            # final summary
            "rsync error: some files/attrs were not transferred "
            "(see previous errors) (code 23) at main.c(1249) "
            "[generator=3.0.6]\n"
        )
        rcc._rsync_ignore_vanished_files(rsync_mock, 1, 2, a=3, b=4)

        # Check with return code != 0
        # 23 - Partial transfer due to error
        # Version with 'receiver' as error source
        # This should not raise
        rsync_mock.reset_mock()
        rsync_mock.ret = 23
        rsync_mock.err = (
            # a file has vanished before rsync start
            'rsync: link_stat "a/file" failed: No such file or directory (2)\n'
            # files which vanished after rsync start
            'file has vanished: "some/other/file"\n'
            # files which have been truncated during transfer
            'rsync: read errors mapping "/truncated": No data available (61)\n'
            # final summary
            "rsync error: some files/attrs were not transferred "
            "(see previous errors) (code 23) at main.c(1249) "
            "[Receiver=3.1.2]\n"
        )
        rcc._rsync_ignore_vanished_files(rsync_mock, 1, 2, a=3, b=4)

    # This test runs for 1, 4 and 16 workers
    @pytest.mark.parametrize("workers", [1, 4, 16])
    @patch("barman.copy_controller.Pool", new=multiprocessing.dummy.Pool)
    @patch("barman.copy_controller.RsyncPgData")
    @patch("barman.copy_controller.RsyncCopyController._analyze_directory")
    @patch("barman.copy_controller.RsyncCopyController._create_dir_and_purge")
    @patch("barman.copy_controller.RsyncCopyController._copy")
    @patch("tempfile.mkdtemp")
    @patch("signal.signal")
    def test_statistics(
        self,
        signal_mock,
        tempfile_mock,
        copy_mock,
        create_and_purge_mock,
        analyse_mock,
        rsync_mock,
        tmpdir,
        workers,
    ):
        """
        Unit test for RsyncCopyController.statistics's code
        """

        # Do a fake copy run to populate the start/stop timestamps.
        # The steps are the same of the full run test
        tempdir = tmpdir.mkdir("tmp")
        tempfile_mock.return_value = tempdir.strpath
        server = build_real_server(
            global_conf={"barman_home": tmpdir.mkdir("home").strpath}
        )
        config = server.config
        executor = server.backup_manager.executor

        rcc = RsyncCopyController(
            path=server.path,
            ssh_command=executor.ssh_command,
            ssh_options=executor.ssh_options,
            network_compression=config.network_compression,
            reuse_backup=None,
            safe_horizon=None,
            workers=workers,
        )

        backup_info = build_test_backup_info(
            server=server,
            pgdata="/pg/data",
            config_file="/etc/postgresql.conf",
            hba_file="/pg/data/pg_hba.conf",
            ident_file="/pg/data/pg_ident.conf",
            begin_xlog="0/2000028",
            begin_wal="000000010000000000000002",
            begin_offset=28,
        )
        backup_info.save()
        # This is to check that all the preparation is done correctly
        assert os.path.exists(backup_info.filename)

        # Silence the access to result properties
        rsync_mock.return_value.out = ""
        rsync_mock.return_value.err = ""
        rsync_mock.return_value.ret = 0

        # Mock analyze directory
        def analyse_func(item):
            label = item.label
            item.dir_file = label + "_dir_file"
            item.exclude_and_protect_file = label + "_exclude_and_protect_file"
            item.safe_list = [_FileItem("mode", 1, "date", "path")]
            item.check_list = [_FileItem("mode", 1, "date", "path")]

        analyse_mock.side_effect = analyse_func

        rcc.add_directory(
            label="tbs1",
            src=":/fake/location/",
            dst=backup_info.get_data_directory(16387),
            reuse=None,
            bwlimit=None,
            item_class=rcc.TABLESPACE_CLASS,
        )
        rcc.add_directory(
            label="tbs2",
            src=":/another/location/",
            dst=backup_info.get_data_directory(16405),
            reuse=None,
            bwlimit=None,
            item_class=rcc.TABLESPACE_CLASS,
        )
        rcc.add_directory(
            label="pgdata",
            src=":/pg/data/",
            dst=backup_info.get_data_directory(),
            reuse=None,
            bwlimit=None,
            item_class=rcc.PGDATA_CLASS,
            exclude=[
                "/pg_xlog/*",
                "/pg_log/*",
                "/log/*",
                "/recovery.conf",
                "/postmaster.pid",
            ],
            exclude_and_protect=["pg_tblspc/16387", "pg_tblspc/16405"],
        )
        rcc.add_file(
            label="pg_control",
            src=":/pg/data/global/pg_control",
            dst="%s/global/pg_control" % backup_info.get_data_directory(),
            item_class=rcc.PGCONTROL_CLASS,
        )
        rcc.add_file(
            label="config_file",
            src=":/etc/postgresql.conf",
            dst=backup_info.get_data_directory(),
            item_class=rcc.CONFIG_CLASS,
            optional=False,
        )
        # Do the fake run
        rcc.copy()

        # Calculate statistics
        result = rcc.statistics()

        # We cannot check the actual result because it is not predictable,
        # so we check that every value is present and is a number and it is
        # greather than 0
        assert result.get("analysis_time") > 0
        assert "analysis_time_per_item" in result
        for tbs in ("pgdata", "tbs1", "tbs2"):
            assert result["analysis_time_per_item"][tbs] > 0

        assert result.get("copy_time") > 0
        assert "copy_time_per_item" in result
        assert "serialized_copy_time_per_item" in result
        for tbs in ("pgdata", "tbs1", "tbs2", "config_file", "pg_control"):
            assert result["copy_time_per_item"][tbs] > 0
            assert result["serialized_copy_time_per_item"][tbs] > 0

        assert result.get("number_of_workers") == rcc.workers
        assert result.get("total_time") > 0

    def test_rsync_copy_item_class(self):
        # A value for the item_class attribute is mandatory for this resource
        with pytest.raises(AssertionError):
            _RsyncCopyItem("symbolic_name", "source", "destination")
