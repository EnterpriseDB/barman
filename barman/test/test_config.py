'''
Created on 24/ago/2011

@author: mnencia
'''

import unittest
from cStringIO import StringIO
from barman.config import Config


TEST_CONFIG = """
[barman]
barman_home = /srv/barman
compression_filter = gzip -c -9
decompression_filter = gzip -c -d
log = log/barman.log
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
log = %(barman_home)s/log/barman.log
compression_filter = bzip2 -c -9
decompression_filter = bzip2 -c -d
[main]
ssh_command = ssh -c arcfour -p 22 postgres@pg01
conninfo = host=pg01 user=postgres port=5432
"""

MINIMAL_CONFIG_MAIN = {
    'barman_home': '/srv/barman',
    'name': 'main',
    'active': 'true',
    'description': None,
    'ssh_command': 'ssh -c arcfour -p 22 postgres@pg01',
    'conninfo': 'host=pg01 user=postgres port=5432',
    'backup_directory': '/srv/barman/main',
    'basebackups_directory': '/srv/barman/main/base',
    'wals_directory': '/srv/barman/main/wals',
    'incoming_wals_directory': '/srv/barman/main/incoming',
    'lock_file': '/srv/barman/main/main.lock',
    'compression_filter': 'bzip2 -c -9',
    'decompression_filter': 'bzip2 -c -d',
    'retention_policy': None,
    'wal_retention_policy': None,
}

class Test(unittest.TestCase):

    def test_database_list(self):
        fp = StringIO(TEST_CONFIG)
        c = Config(fp)
        dbs = c.server_names()
        self.assertEqual(set(dbs), set(['main', 'web']))

    def test_minimal(self):
        fp = StringIO(MINIMAL_CONFIG)
        c = Config(fp)
        main = c.get_server('main')
        self.assertEqual(main.__dict__, MINIMAL_CONFIG_MAIN)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
