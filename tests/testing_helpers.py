# Copyright (C) 2014-2016 2ndQuadrant Italia Srl
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

from datetime import datetime, timedelta

import mock
from dateutil import tz

from barman.backup import BackupManager
from barman.config import BackupOptions, Config
from barman.infofile import BackupInfo, Tablespace
from barman.server import Server

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO


def build_test_backup_info(
        backup_id='1234567890',
        begin_offset=40,
        begin_time=None,
        begin_wal='000000010000000000000002',
        begin_xlog='0/2000028',
        config_file='/pgdata/location/postgresql.conf',
        end_offset=184,
        end_time=None,
        end_wal='000000010000000000000002',
        end_xlog='0/20000B8',
        error=None,
        hba_file='/pgdata/location/pg_hba.conf',
        ident_file='/pgdata/location/pg_ident.conf',
        mode='default',
        pgdata='/pgdata/location',
        server_name='test_server',
        size=12345,
        status=BackupInfo.DONE,
        included_files=None,
        tablespaces=(
            ('tbs1', 16387, '/fake/location'),
            ('tbs2', 16405, '/another/location'),
        ),
        timeline=1,
        version=90302,
        server=None):
    """
    Create an 'Ad Hoc' BackupInfo object for testing purposes.

    A BackupInfo object is the barman representation of a physical backup,
    for testing purposes is necessary to build a BackupInfo avoiding the usage
    of Mock/MagicMock classes as much as possible.

    :param str backup_id: the id of the backup
    :param int begin_offset: begin_offset of the backup
    :param datetime.datetime|None begin_time: begin_time of the backup
    :param str begin_wal: begin_wal of the backup
    :param str begin_xlog: begin_xlog of the backup
    :param str config_file: config file of the backup
    :param int end_offset: end_offset of the backup
    :param datetime.datetime|None end_time: end_time of the backup
    :param str end_wal: begin_xlog of the backup
    :param str end_xlog: end_xlog of the backup
    :param str|None error: error message for the backup
    :param str hba_file: hba_file for the backup
    :param str ident_file: ident_file for the backup
    :param str mode: mode of execution of the backup
    :param str pgdata: pg_data dir of the backup
    :param str server_name: server name for the backup
    :param int size: dimension of the backup
    :param str status: status of the execution of the backup
    :param list|None included_files: a list of extra configuration files
    :param list|tuple|None tablespaces: a list of tablespaces for the backup
    :param int timeline: timeline of the backup
    :param int version: postgres version of the backup
    :param barman.server.Server|None server: Server object for the backup
    :rtype: barman.infofile.BackupInfo
    """
    if begin_time is None:
        begin_time = datetime.now(tz.tzlocal()) - timedelta(minutes=10)
    if end_time is None:
        end_time = datetime.now(tz.tzlocal())

    # Generate a list of tablespace objects (don't use a list comprehension
    # or in python 2.x the 'item' variable will leak to the main context)
    if tablespaces is not None:
        tablespaces = list(Tablespace._make(item) for item in tablespaces)

    # Manage the server for the Backup info: if no server is provided
    # by the caller use a Mock with a basic configuration
    if server is None:
        server = mock.Mock(name=server_name)
        server.config = build_config_from_dicts().get_server('main')
        server.backup_manager.name = 'default'

    backup_info = BackupInfo(**locals())
    return backup_info


def mock_backup_ext_info(
        backup_info=None,
        previous_backup_id=None,
        next_backup_id=None,
        wal_num=1,
        wal_size=123456,
        wal_until_next_num=18,
        wal_until_next_size=2345678,
        wals_per_second=0.01,
        wal_first='000000010000000000000014',
        wal_first_timestamp=None,
        wal_last='000000010000000000000014',
        wal_last_timestamp=None,
        retention_policy_status=None,
        wal_compression_ratio=0.0,
        wal_until_next_compression_ratio=0.0,
        **kwargs):

    # make a dictionary with all the arguments
    ext_info = dict(locals())
    del ext_info['backup_info']

    if backup_info is None:
        backup_info = build_test_backup_info(**kwargs)

    # merge the backup_info values
    ext_info.update(backup_info.to_dict())

    return ext_info


def build_config_from_dicts(global_conf=None, main_conf=None,
                            test_conf=None,
                            config_name=None):
    """
    Utility method, generate a barman.config.Config object

    It has  a minimal configuration and a single server called "main".
    All options can be override using the optional arguments

    :param dict[str,str|None]|None global_conf: using this dictionary
        it is possible to override or add new values to the [barman] section
    :param dict[str,str|None]|None main_conf: using this dictionary
        it is possible to override/add new values to the [main] section
    :return barman.config.Config: a barman configuration object
    """
    # base barman section
    base_barman = {
        'barman_home': '/some/barman/home',
        'barman_user': '{USER}',
        'log_file': '%(barman_home)s/log/barman.log'
    }
    # base main section
    base_main = {
        'description': '" Text with quotes "',
        'ssh_command': 'ssh -c "arcfour" -p 22 postgres@pg01.nowhere',
        'conninfo': 'host=pg01.nowhere user=postgres port=5432'
    }
    # base test section
    base_test = {
        'description': '" Text with quotes "',
        'ssh_command': 'ssh -c "arcfour" -p 22 postgres@pg02.nowhere',
        'conninfo': 'host=pg02.nowhere user=postgres port=5433'
    }
    # update map values of the two sections
    if global_conf is not None:
        base_barman.update(global_conf)
    if main_conf is not None:
        base_main.update(main_conf)
    if test_conf is not None:
        base_test.update(test_conf)

    # writing the StringIO obj with the barman and main sections
    config_file = StringIO()
    config_file.write('\n[barman]\n')
    for key in base_barman.keys():
        config_file.write('%s = %s\n' % (key, base_barman[key]))

    config_file.write('[main]\n')
    for key in base_main.keys():
        config_file.write('%s = %s\n' % (key, base_main[key]))

    config_file.write('[test]\n')
    for key in base_test.keys():
        config_file.write('%s = %s\n' % (key, base_main[key]))

    config_file.seek(0)
    config = Config(config_file)
    config.config_file = config_name or 'build_config_from_dicts'
    return config


def build_config_dictionary(config_keys=None):
    """
    Utility method, generate a dict useful for config comparison

    It has a 'basic' format and every key could be overwritten the
    config_keys parameter.

    :param dict[str,str|None]|None config_keys: using this dictionary
        it is possible to override or add new values to the base dictionary.
    :return dict: a dictionary representing a barman configuration
    """
    # Basic dictionary
    base_config = {
        'active': True,
        'archiver': True,
        'config': None,
        'backup_directory': '/some/barman/home/main',
        'backup_options': BackupOptions(BackupOptions.EXCLUSIVE_BACKUP,
                                        "", ""),
        'bandwidth_limit': None,
        'barman_home': '/some/barman/home',
        'basebackups_directory': '/some/barman/home/main/base',
        'barman_lock_directory': '/some/barman/home',
        'compression': None,
        'conninfo': 'host=pg01.nowhere user=postgres port=5432',
        'backup_method': 'rsync',
        'custom_compression_filter': None,
        'custom_decompression_filter': None,
        'description': ' Text with quotes ',
        'immediate_checkpoint': False,
        'incoming_wals_directory': '/some/barman/home/main/incoming',
        'minimum_redundancy': '0',
        'name': 'main',
        'network_compression': False,
        'post_backup_script': None,
        'pre_backup_script': None,
        'post_backup_retry_script': None,
        'pre_backup_retry_script': None,
        'recovery_options': set(),
        'retention_policy': None,
        'retention_policy_mode': 'auto',
        'reuse_backup': None,
        'ssh_command': 'ssh -c "arcfour" -p 22 postgres@pg01.nowhere',
        'tablespace_bandwidth_limit': None,
        'wal_retention_policy': 'main',
        'wals_directory': '/some/barman/home/main/wals',
        'basebackup_retry_sleep': 30,
        'basebackup_retry_times': 0,
        'post_archive_script': None,
        'streaming_conninfo': 'host=pg01.nowhere user=postgres port=5432',
        'pre_archive_script': None,
        'post_archive_retry_script': None,
        'pre_archive_retry_script': None,
        'last_backup_maximum_age': None,
        'disabled': False,
        'msg_list': [],
        'path_prefix': None,
        'streaming_archiver': False,
        'streaming_wals_directory': '/some/barman/home/main/streaming',
        'errors_directory': '/some/barman/home/main/errors',
    }
    # Check for overriding keys
    if config_keys is not None:
        base_config.update(config_keys)
    return base_config


def build_real_server(global_conf=None, main_conf=None):
    """
    Build a real Server object built from a real configuration

    :param dict[str,str|None]|None global_conf: using this dictionary
        it is possible to override or add new values to the [barman] section
    :param dict[str,str|None]|None main_conf: using this dictionary
        it is possible to override/add new values to the [main] section
    :return barman.server.Server: a barman Server object
    """
    return Server(build_config_from_dicts(
        global_conf=global_conf, main_conf=main_conf).get_server('main'))


def build_mocked_server(name=None, config=None,
                        global_conf=None, main_conf=None):
    """
    Build a mock server object
    :param barman.config.ServerConfig config: use this object to build the
        server
    :param dict[str,str|None]|None global_conf: using this dictionary
        it is possible to override or add new values to the [barman] section
    :param dict[str,str|None]|None main_conf: using this dictionary
        it is possible to override/add new values to the [main] section
    :rtype: barman.server.Server
    """
    # instantiate a retention policy object using mocked parameters
    server = mock.MagicMock(name='barman.server.Server')

    if not config:
        server.config = build_config_from_dicts(
            global_conf=global_conf,
            main_conf=main_conf).get_server('main')
    else:
        server.config = config
    server.config.name = name or 'main'
    return server


def build_backup_manager(server=None, name=None, config=None,
                         global_conf=None, main_conf=None):
    """
    Instantiate a BackupManager object using mocked parameters

    The compression_manager member is mocked

    :param barman.server.Server|None server: Optionsl Server object
    :rtype: barman.backup.BackupManager
    """
    if server is None:
        server = build_mocked_server(name, config, global_conf, main_conf)
    with mock.patch("barman.backup.CompressionManager"):
        manager = BackupManager(server=server)
    server.backup_manager = manager
    return manager
