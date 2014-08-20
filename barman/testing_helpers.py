# Copyright (C) 2014 2ndQuadrant Italia (Devise.IT S.r.L.)
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
try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

import mock
from dateutil import tz

from barman.config import Config
from barman.infofile import BackupInfo, Tablespace


def mock_backup_info(backup_id='1234567890',
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
                     tablespaces=[
                         ['tbs1', 16387, '/fake/location'],
                         ['tbs2', 16405, '/another/location'],
                     ],
                     timeline=1,
                     version=90302):
    if begin_time is None:
        begin_time = datetime.now(tz.tzlocal()) - timedelta(minutes=10)
    if end_time is None:
        end_time = datetime.now(tz.tzlocal())

    # Generate a list of tablespace objects (don't use a list comprehension
    # or in python 2.x the 'item' variable will leak to the main context)
    tablespaces = list(Tablespace._make(item) for item in tablespaces)

    # make a dictionary with all the arguments
    to_dict = dict(locals())

    # generate a property on the mock for every key in to_dict
    bi_mock = mock.Mock()
    for key in to_dict:
        setattr(bi_mock, key, to_dict[key])

    bi_mock.to_dict.return_value = to_dict
    bi_mock.to_json.return_value = to_dict

    return bi_mock


def mock_backup_ext_info(backup_info=None,
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
        backup_info = mock_backup_info(**kwargs)

    # merge the backup_info values
    ext_info.update(backup_info.to_dict())

    return ext_info


def build_config_from_dicts(global_conf=None, main_conf=None):
    """
    Utility method, generate a barman.config.Config object

    It has  a minimal configuration and a single server called "main".
    All options can be override using the optional arguments
    :param dict|None global_conf: using this dictionary is possible
        to override/add new values to the [barman] section
    :param dict|None main_conf: using this dictionary is possible
        to override/add new values to the [main] section
    :return barman.config.Config: a barman configuration object
    """
    # base barman section
    base_barman = {
        'barman_home': '/srv/barman',
        'barman_user': '{USER}',
        'log_file': '%(barman_home)s/log/barman.log'
    }
    # base main section
    base_main = {
        'description': '" Text with quotes "',
        'ssh_command': 'ssh -c "arcfour" -p 22 postgres@pg01',
        'conninfo': 'host=pg01 user=postgres port=5432'
    }
    # update map values of the two sections
    if global_conf is not None:
        base_barman.update(global_conf)
    if main_conf is not None:
        base_main.update(main_conf)

    # writing the StringIO obj with the barman and main sections
    config_file = StringIO()
    config_file.write('\n[barman]\n')
    for key in base_barman.keys():
        config_file.write('%s = %s\n' % (key, base_barman[key]))

    config_file.write('[main]\n')
    for key in base_main.keys():
        config_file.write('%s = %s\n' % (key, base_main[key]))
    config_file.seek(0)
    config = Config(config_file)
    return config