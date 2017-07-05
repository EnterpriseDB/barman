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

import multiprocessing.dummy
import os
from datetime import datetime

import dateutil.tz
import mock
from mock import patch

from barman.copy_controller import RsyncCopyController, _FileItem
from testing_helpers import (build_backup_manager, build_real_server,
                             build_test_backup_info)


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
            safe_horizon=None)

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
            safe_horizon=None)

        reuse_dir = "some/dir"

        # Test for disabled incremental
        assert rcc._reuse_args(reuse_dir) == []

        # Test for link incremental
        rcc.reuse_backup = 'link'
        assert rcc._reuse_args(reuse_dir) == \
            ['--link-dest=some/dir']

        # Test for copy incremental
        rcc.reuse_backup = 'copy'
        assert rcc._reuse_args(reuse_dir) == \
            ['--copy-dest=some/dir']

    @patch('barman.copy_controller.Pool',
           new=multiprocessing.dummy.Pool)
    @patch('barman.copy_controller.RsyncPgData')
    @patch('barman.copy_controller.RsyncCopyController._analyze_directory')
    @patch('barman.copy_controller.RsyncCopyController._create_dir_and_purge')
    @patch('barman.copy_controller.RsyncCopyController._copy')
    @patch('tempfile.mkdtemp')
    @patch('signal.signal')
    def test_full_copy(self, signal_mock, tempfile_mock, copy_mock,
                       create_and_purge_mock, analyse_mock, rsync_mock,
                       tmpdir):
        """
        Test the execution of a full copy
        """

        # Build the prerequisites
        tempdir = tmpdir.mkdir('tmp')
        tempfile_mock.return_value = tempdir.strpath
        server = build_real_server(global_conf={
            'barman_home': tmpdir.mkdir('home').strpath
        })
        config = server.config
        executor = server.backup_manager.executor

        rcc = RsyncCopyController(
            path=server.path,
            ssh_command=executor.ssh_command,
            ssh_options=executor.ssh_options,
            network_compression=config.network_compression,
            reuse_backup=None,
            safe_horizon=None)

        backup_info = build_test_backup_info(
            server=server,
            pgdata="/pg/data",
            config_file="/etc/postgresql.conf",
            hba_file="/pg/data/pg_hba.conf",
            ident_file="/pg/data/pg_ident.conf",
            begin_xlog="0/2000028",
            begin_wal="000000010000000000000002",
            begin_offset=28)
        backup_info.save()
        # This is to check that all the preparation is done correctly
        assert os.path.exists(backup_info.filename)

        # Silence the access to result properties
        rsync_mock.return_value.out = ''
        rsync_mock.return_value.err = ''
        rsync_mock.return_value.ret = 0

        # Mock analyze directory
        def analyse_func(item):
            l = item.label
            item.dir_file = l + '_dir_file'
            item.exclude_and_protect_file = l + '_exclude_and_protect_file'
            item.safe_list = [_FileItem('mode', 1, 'date', 'path')]
            item.check_list = [_FileItem('mode', 1, 'date', 'path')]
        analyse_mock.side_effect = analyse_func

        rcc.add_directory(
            label='tbs1',
            src=':/fake/location/',
            dst=backup_info.get_data_directory(16387),
            reuse=None,
            bwlimit=None,
            item_class=rcc.TABLESPACE_CLASS),
        rcc.add_directory(
            label='tbs2',
            src=':/another/location/',
            dst=backup_info.get_data_directory(16405),
            reuse=None,
            bwlimit=None,
            item_class=rcc.TABLESPACE_CLASS),
        rcc.add_directory(
            label='pgdata',
            src=':/pg/data/',
            dst=backup_info.get_data_directory(),
            reuse=None,
            bwlimit=None,
            item_class=rcc.PGDATA_CLASS,
            exclude=['/pg_xlog/*',
                     '/pg_log/*',
                     '/recovery.conf',
                     '/postmaster.pid'],
            exclude_and_protect=['pg_tblspc/16387', 'pg_tblspc/16405']),
        rcc.add_file(
            label='pg_control',
            src=':/pg/data/global/pg_control',
            dst='%s/global/pg_control' % backup_info.get_data_directory(),
            item_class=rcc.PGCONTROL_CLASS),
        rcc.add_file(
            label='config_file',
            src=':/etc/postgresql.conf',
            dst=backup_info.get_data_directory(),
            item_class=rcc.CONFIG_CLASS,
            optional=False),
        rcc.copy(),

        # Check the order of calls to the Rsync mock
        assert rsync_mock.mock_calls == [
            mock.call(network_compression=False,
                      args=['--itemize-changes',
                            '--itemize-changes'],
                      bwlimit=None, ssh='ssh', path=None,
                      ssh_options=['-c', '"arcfour"', '-p', '22',
                                   'postgres@pg01.nowhere', '-o',
                                   'BatchMode=yes', '-o',
                                   'StrictHostKeyChecking=no'],
                      exclude=None, exclude_and_protect=None, include=None,
                      retry_sleep=0, retry_times=0, retry_handler=mock.ANY),
            mock.call(network_compression=False,
                      args=['--itemize-changes',
                            '--itemize-changes'],
                      bwlimit=None, ssh='ssh', path=None,
                      ssh_options=['-c', '"arcfour"', '-p', '22',
                                   'postgres@pg01.nowhere', '-o',
                                   'BatchMode=yes', '-o',
                                   'StrictHostKeyChecking=no'],
                      exclude=None, exclude_and_protect=None, include=None,
                      retry_sleep=0, retry_times=0, retry_handler=mock.ANY),
            mock.call(network_compression=False,
                      args=['--itemize-changes',
                            '--itemize-changes'],
                      bwlimit=None, ssh='ssh', path=None,
                      ssh_options=['-c', '"arcfour"', '-p', '22',
                                   'postgres@pg01.nowhere', '-o',
                                   'BatchMode=yes', '-o',
                                   'StrictHostKeyChecking=no'],
                      exclude=[
                          '/pg_xlog/*',
                          '/pg_log/*',
                          '/recovery.conf',
                          '/postmaster.pid'],
                      exclude_and_protect=[
                          'pg_tblspc/16387',
                          'pg_tblspc/16405'],
                      include=None,
                      retry_sleep=0, retry_times=0, retry_handler=mock.ANY),
            mock.call(network_compression=False,
                      args=['--itemize-changes',
                            '--itemize-changes'],
                      bwlimit=None, ssh='ssh', path=None,
                      ssh_options=['-c', '"arcfour"', '-p', '22',
                                   'postgres@pg01.nowhere', '-o',
                                   'BatchMode=yes', '-o',
                                   'StrictHostKeyChecking=no'],
                      exclude=None, exclude_and_protect=None, include=None,
                      retry_sleep=0, retry_times=0, retry_handler=mock.ANY),
            mock.call()(
                ':/etc/postgresql.conf',
                backup_info.get_data_directory(),
                allowed_retval=(0, 23, 24)),
            mock.call(network_compression=False,
                      args=['--itemize-changes',
                            '--itemize-changes'],
                      bwlimit=None, ssh='ssh', path=None,
                      ssh_options=['-c', '"arcfour"', '-p', '22',
                                   'postgres@pg01.nowhere', '-o',
                                   'BatchMode=yes', '-o',
                                   'StrictHostKeyChecking=no'],
                      exclude=None, exclude_and_protect=None, include=None,
                      retry_sleep=0, retry_times=0, retry_handler=mock.ANY),
            mock.call()(
                ':/pg/data/global/pg_control',
                '%s/global/pg_control' % backup_info.get_data_directory(),
                allowed_retval=(0, 23, 24)),
        ]

        # Check calls to _analyse_directory method
        assert analyse_mock.mock_calls == [
            mock.call(item) for item in rcc.item_list
            if item.is_directory
        ]

        # Check calls to _create_dir_and_purge method
        assert create_and_purge_mock.mock_calls == [
            mock.call(item) for item in rcc.item_list
            if item.is_directory
        ]

        # Utility function to build the file_list name
        def file_list_name(label, kind):
            return '%s/%s_%s_%s.list' % (
                tempdir.strpath,
                label,
                kind,
                os.getpid())

        # Check the order of calls to the copy method
        # All the file_list arguments are None because the analyze part
        # has not really been executed
        assert copy_mock.mock_calls == [
            mock.call(
                mock.ANY, ':/fake/location/',
                backup_info.get_data_directory(16387), checksum=False,
                file_list=file_list_name('tbs1', 'safe')),
            mock.call(
                mock.ANY, ':/fake/location/',
                backup_info.get_data_directory(16387), checksum=True,
                file_list=file_list_name('tbs1', 'check')),
            mock.call(
                mock.ANY, ':/another/location/',
                backup_info.get_data_directory(16405), checksum=False,
                file_list=file_list_name('tbs2', 'safe')),
            mock.call(
                mock.ANY, ':/another/location/',
                backup_info.get_data_directory(16405), checksum=True,
                file_list=file_list_name('tbs2', 'check')),
            mock.call(
                mock.ANY, ':/pg/data/',
                backup_info.get_data_directory(), checksum=False,
                file_list=file_list_name('pgdata', 'safe')),
            mock.call(
                mock.ANY, ':/pg/data/',
                backup_info.get_data_directory(), checksum=True,
                file_list=file_list_name('pgdata', 'check')),
        ]

    def test_list_files(self):
        """
        Unit test for RsyncCopyController_list_file's code
        """
        # Mock rsync invocation
        rsync_mock = mock.Mock(name='Rsync()')
        rsync_mock.ret = 0
        rsync_mock.out = 'drwxrwxrwt       69632 2015/02/09 15:01:00 tmp\n' \
                         'drwxrwxrwt       69612 2015/02/19 15:01:22 tmp2'
        rsync_mock.err = 'err'

        # Test the _list_files internal method
        rcc = RsyncCopyController()
        return_values = list(rcc._list_files(rsync_mock, 'some/path'))

        # Returned list must contain two elements
        assert len(return_values) == 2

        # Check rsync.get_output has called correctly
        rsync_mock.get_output.assert_called_with(
            '--no-human-readable', '--list-only', '-r', 'some/path',
            check=True)

        # Check the result
        assert return_values[0] == _FileItem(
            'drwxrwxrwt',
            69632,
            datetime(year=2015, month=2, day=9,
                     hour=15, minute=1, second=0,
                     tzinfo=dateutil.tz.tzlocal()),
            'tmp')
        assert return_values[1] == _FileItem(
            'drwxrwxrwt',
            69612,
            datetime(year=2015, month=2, day=19,
                     hour=15, minute=1, second=22,
                     tzinfo=dateutil.tz.tzlocal()),
            'tmp2')
