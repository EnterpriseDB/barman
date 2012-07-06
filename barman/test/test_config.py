# Copyright (C) 2011, 2012 2ndQuadrant Italia (Devise.IT S.r.L.)
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
from cStringIO import StringIO
from barman.config import Config


TEST_CONFIG = """
[barman]
barman_home = /srv/barman
barman_user = {USER}
compression = gzip
decompression = gzip
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
[web]
active = true
description = Web applications database
ssh_command = ssh -I ~/.ssh/web01_rsa -c arcfour -p 22 postgres@web01
conninfo = host=web01 user=postgres port=5432
"""

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
    'active': 'true',
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
    'wal_retention_policy': None,
}

class Test(unittest.TestCase):

    def test_server_list(self):
        fp = StringIO(TEST_CONFIG.format(**os.environ))
        c = Config(fp)
        dbs = c.server_names()
        self.assertEqual(set(dbs), set(['main', 'web']))

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
        self.assertEqual(main.__dict__, MINIMAL_CONFIG_MAIN)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
