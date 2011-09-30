#
# barman - Backup and Recovery Manager for PostgreSQL
#
# Copyright (C) 2011  Devise.IT S.r.l. <info@2ndquadrant.it>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
import re
from ConfigParser import ConfigParser, NoOptionError

class Server(object):

    KEYS = ['active', 'description', 'ssh_command', 'conninfo',
        'backup_directory', 'basebackups_directory',
        'wals_directory', 'incoming_wals_directory', 'lock_file',
        'compression_filter', 'decompression_filter',
        'retention_policy', 'wal_retention_policy',
    ]

    BARMAN_KEYS = ['compression_filter', 'decompression_filter',
        'retention_policy', 'wal_retention_policy',
    ]

    DEFAULTS = {
        'active': 'true',
        'backup_directory': r'%(barman_home)s/%(name)s',
        'basebackups_directory': r'%(backup_directory)s/base',
        'wals_directory': r'%(backup_directory)s/wals',
        'incoming_wals_directory': r'%(backup_directory)s/incoming',
        'lock_file': r'%(backup_directory)s/%(name)s.lock',
    }

    def __init__(self, config, name):
        self.name = name
        self.barman_home = config.get('barman', 'barman_home')
        for key in Server.KEYS:
            value = config.get(name, key, self.__dict__)
            if value == None and key in Server.BARMAN_KEYS:
                value = config.get('barman', key)
            if value == None and Server.DEFAULTS.has_key(key):
                value = Server.DEFAULTS[key] % self.__dict__
            setattr(self, key, value)


class Config(object):

    CONFIG_FILES = ['~/.barman.conf', '/etc/barman.conf']

    def __init__(self, filename=None):
        self._config = ConfigParser()
        if filename:
            if hasattr(filename, 'read'):
                self._config.readfp(filename)
            else:
                self._config.read(os.path.expanduser(filename))
        else:
            self._config.read([os.path.expanduser(filename) for filename in self.CONFIG_FILES])
        self._servers = None


    def get(self, section, option, defaults={}):
        if not self._config.has_section(section):
            return None
        try:
            value = self._config.get(section, option, False, defaults)
            if value != None:
                value = self._QUOTERE.sub(lambda m: m.group(2), value)
            return value
        except NoOptionError:
            return None

    _QUOTERE = re.compile(r"""^(["'])(.*)\1$""")

    def _populate_servers(self):
        if self._servers != None:
            return
        self._servers = {}
        for section in self._config.sections():
            if section == 'barman':
                continue # skip global settings
            self._servers[section] = Server(self, section)


    def server_names(self):
        self._populate_servers()
        return self._servers.keys()


    def servers(self):
        self._populate_servers()
        return self._servers.values()


    def get_server(self, name):
        self._populate_servers()
        return self._servers.get(name, None)


# easy config diagnostic with python -m
if __name__ == "__main__":
    print "Active configuration settings:"
    r = Config()
    for section in r._config.sections():
        print "Section: %s" % section
        for option in r._config.options(section):
            print "\t%s = %s " % (option, r.get(section, option))
