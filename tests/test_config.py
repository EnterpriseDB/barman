# Copyright (C) 2011-2014 2ndQuadrant Italia (Devise.IT S.r.L.)
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

import os
import unittest
from datetime import timedelta
import pytest

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO
from barman.config import Config, parse_time_interval


TEST_CONFIG = """
[barman]
barman_home = /srv/barman
barman_user = {USER}
compression = gzip
log_file = /srv/barman/log/barman.log
log_level = INFO
retention_policy = redundancy 2
wal_retention_policy = base
[main]
active = true
description = Main PostgreSQL Database
ssh_command = ssh -c arcfour -p 22 postgres@pg01
conninfo = host=pg01 user=postgres port=5432
backup_directory = main
basebackups_directory = base
wals_directory = wals
incoming_wals_directory = incoming
lock_file = main.lock
compression_filter = bzip2 -c -9
decompression_filter = bzip2 -c -d
retention_policy = redundancy 3
wal_retention_policy = base
last_backup_maximum_age = '1 day'
[web]
active = true
description = Web applications database
ssh_command = ssh -I ~/.ssh/web01_rsa -c arcfour -p 22 postgres@web01
conninfo = host=web01 user=postgres port=5432
compression =
last_backup_maximum_age = '1 day'
"""

TEST_CONFIG_MAIN = {
    'active': True,
    'backup_directory': 'main',
    'backup_options': 'exclusive_backup',
    'bandwidth_limit': None,
    'barman_home': '/srv/barman',
    'basebackups_directory': 'base',
    'compression': 'gzip',
    'conninfo': 'host=pg01 user=postgres port=5432',
    'custom_compression_filter': None,
    'custom_decompression_filter': None,
    'description': 'Main PostgreSQL Database',
    'immediate_checkpoint': False,
    'incoming_wals_directory': 'incoming',
    'lock_file': 'main.lock',
    'minimum_redundancy': '0',
    'name': 'main',
    'network_compression': False,
    'post_backup_script': None,
    'pre_backup_script': None,
    'retention_policy': 'redundancy 3',
    'retention_policy_mode': 'auto',
    'ssh_command': 'ssh -c arcfour -p 22 postgres@pg01',
    'tablespace_bandwidth_limit': None,
    'wal_retention_policy': 'base',
    'wals_directory': 'wals',
    'basebackup_retry_sleep': 10,
    'basebackup_retry_times': 1,
    'post_archive_script': None,
    'pre_archive_script': None,
    'last_backup_maximum_age': timedelta(1),
}

TEST_CONFIG_WEB = {
    'active': True,
    'backup_directory': '/srv/barman/web',
    'backup_options': 'exclusive_backup',
    'bandwidth_limit': None,
    'barman_home': '/srv/barman',
    'basebackups_directory': '/srv/barman/web/base',
    'compression': None,
    'conninfo': 'host=web01 user=postgres port=5432',
    'custom_compression_filter': None,
    'custom_decompression_filter': None,
    'description': 'Web applications database',
    'immediate_checkpoint': False,
    'incoming_wals_directory': '/srv/barman/web/incoming',
    'lock_file': '/srv/barman/web/web.lock',
    'minimum_redundancy': '0',
    'name': 'web',
    'network_compression': False,
    'post_backup_script': None,
    'pre_backup_script': None,
    'retention_policy': 'redundancy 2',
    'retention_policy_mode': 'auto',
    'ssh_command': 'ssh -I ~/.ssh/web01_rsa -c arcfour -p 22 postgres@web01',
    'tablespace_bandwidth_limit': None,
    'wal_retention_policy': 'base',
    'wals_directory': '/srv/barman/web/wals',
    'basebackup_retry_sleep': 10,
    'basebackup_retry_times': 1,
    'post_archive_script': None,
    'pre_archive_script': None,
    'last_backup_maximum_age': timedelta(1),
    }

MINIMAL_CONFIG = """
[barman]
barman_home = /srv/barman
barman_user = {USER}
log = %(barman_home)s/log/barman.log
[main]
description = " Text with quotes "
ssh_command = ssh -c "arcfour" -p 22 postgres@pg01
conninfo = host=pg01 user=postgres port=5432
"""

MINIMAL_CONFIG_MAIN = {
    'barman_home': '/srv/barman',
    'name': 'main',
    'active': True,
    'description': ' Text with quotes ',
    'ssh_command': 'ssh -c "arcfour" -p 22 postgres@pg01',
    'conninfo': 'host=pg01 user=postgres port=5432',
    'backup_directory': '/srv/barman/main',
    'basebackups_directory': '/srv/barman/main/base',
    'wals_directory': '/srv/barman/main/wals',
    'incoming_wals_directory': '/srv/barman/main/incoming',
    'lock_file': '/srv/barman/main/main.lock',
    'compression': None,
    'custom_compression_filter': None,
    'custom_decompression_filter': None,
    'retention_policy': None,
    'wal_retention_policy': 'main',
    'post_backup_script': None,
    'pre_backup_script': None,
    'minimum_redundancy': '0',
    'retention_policy_mode': 'auto',
    'bandwidth_limit': None,
    'tablespace_bandwidth_limit': None,
    'immediate_checkpoint': False,
    'network_compression': False,
    'backup_options': 'exclusive_backup',
    'basebackup_retry_sleep': 10,
    'basebackup_retry_times': 1,
    'post_archive_script': None,
    'pre_archive_script': None,
    'last_backup_maximum_age': None,
}

class Test(unittest.TestCase):

    def test_server_list(self):
        fp = StringIO(TEST_CONFIG.format(**os.environ))
        c = Config(fp)
        dbs = c.server_names()
        self.assertEqual(set(dbs), set(['main', 'web']))

    def test_config(self):
        self.maxDiff = None
        fp = StringIO(TEST_CONFIG.format(**os.environ))
        c = Config(fp)

        main = c.get_server('main')
        expected = dict(config=c)
        expected.update(TEST_CONFIG_MAIN)
        assert main.__dict__ == expected

        web = c.get_server('web')
        expected = dict(config=c)
        expected.update(TEST_CONFIG_WEB)
        assert web.__dict__ == expected


    def test_quotes(self):
        fp = StringIO(MINIMAL_CONFIG.format(**os.environ))
        c = Config(fp)
        main = c.get_server('main')
        self.assertEqual(main.description, ' Text with quotes ')
        self.assertEqual(main.ssh_command, 'ssh -c "arcfour" -p 22 postgres@pg01')

    def test_interpolation(self):
        self.maxDiff = None
        fp = StringIO(MINIMAL_CONFIG.format(**os.environ))
        c = Config(fp)
        main = c.get_server('main')

        expected = dict(config=c)
        expected.update(MINIMAL_CONFIG_MAIN)
        assert main.__dict__ == expected

    def test_parse_time_interval(self):
        """
        basic test the parsing method for timedelta values
        pass a value, check if is correctly transformed in a timedelta
        """

        # 1 day
        val = parse_time_interval('1 day')
        assert val == timedelta(days=1)
        # 2 weeks
        val = parse_time_interval('2 weeks')
        assert val == timedelta(days=14)
        # 3 months
        val = parse_time_interval('3 months')
        assert val == timedelta(days=93)
        # this string is something that the regexp cannot manage,
        # so we expect a ValueError exception
        with pytest.raises(ValueError):
            parse_time_interval('test_string')

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
