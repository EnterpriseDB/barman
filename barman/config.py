# Copyright (C) 2011-2013 2ndQuadrant Italia (Devise.IT S.r.L.)
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

""" This module is responsible for all the things related to
Barman configuration, such as parsing configuration file.
"""

import os
import re
from ConfigParser import ConfigParser, NoOptionError
import logging.handlers
from glob import iglob

_logger = logging.getLogger(__name__)

FORBIDDEN_SERVER_NAMES = ['all']

DEFAULT_USER = 'barman'
DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s: %(message)s"


class Server(object):
    """This class represents a server."""
    KEYS = [
        'active', 'description', 'ssh_command', 'conninfo',
        'backup_directory', 'basebackups_directory',
        'wals_directory', 'incoming_wals_directory', 'lock_file',
        'compression', 'custom_compression_filter',
        'custom_decompression_filter', 'retention_policy_mode',
        'retention_policy',
        'wal_retention_policy', 'pre_backup_script', 'post_backup_script',
        'minimum_redundancy', 'bandwidth_limit', 'tablespace_bandwidth_limit',
        'immediate_checkpoint',
    ]

    BARMAN_KEYS = [
        'compression', 'custom_compression_filter',
        'custom_decompression_filter', 'retention_policy_mode',
        'retention_policy',
        'wal_retention_policy', 'pre_backup_script', 'post_backup_script',
        'configuration_files_directory',
        'minimum_redundancy', 'bandwidth_limit', 'tablespace_bandwidth_limit'
    ]

    DEFAULTS = {
        'active': 'true',
        'backup_directory': r'%(barman_home)s/%(name)s',
        'basebackups_directory': r'%(backup_directory)s/base',
        'wals_directory': r'%(backup_directory)s/wals',
        'incoming_wals_directory': r'%(backup_directory)s/incoming',
        'lock_file': r'%(backup_directory)s/%(name)s.lock',
        'retention_policy_mode': 'auto',
        'wal_retention_policy': 'main',
        'minimum_redundancy': '0',
        'immediate_checkpoint': 'false',
    }

    def __init__(self, config, name):
        self.config = config
        self.name = name
        self.barman_home = config.get('barman', 'barman_home')
        for key in Server.KEYS:
            value = config.get(name, key, self.__dict__)
            if value is None and key in Server.BARMAN_KEYS:
                value = config.get('barman', key)
            if value is None and key in Server.DEFAULTS:
                value = Server.DEFAULTS[key] % self.__dict__
            setattr(self, key, value)


class Config(object):
    """This class represents the barman configuration.

    Default configuration files are /etc/barman.conf,
    /etc/barman/barman.conf
    and ~/.barman.conf for a per-user configuration
    """
    CONFIG_FILES = [
        '~/.barman.conf',
        '/etc/barman.conf',
        '/etc/barman/barman.conf',
    ]

    _QUOTE_RE = re.compile(r"""^(["'])(.*)\1$""")

    def __init__(self, filename=None):
        self._config = ConfigParser()
        if filename:
            if hasattr(filename, 'read'):
                self._config.readfp(filename)
            else:
                self._config.read(os.path.expanduser(filename))
        else:
            for path in self.CONFIG_FILES:
                full_path = os.path.expanduser(path)
                if os.path.exists(full_path) \
                    and full_path in self._config.read(full_path):
                    filename = full_path
                    break
        self.config_file = filename
        self._servers = None
        self._parse_global_config()
        self._load_configuration_files_directory()

    def get(self, section, option, defaults=None):
        """Method to get the value from a given section from
        Barman configuration
        """
        if not self._config.has_section(section):
            return None
        try:
            value = self._config.get(section, option, raw=False, vars=defaults)
            if value == 'None':
                value = None
            if value is not None:
                value = self._QUOTE_RE.sub(lambda m: m.group(2), value)
            return value
        except NoOptionError:
            return None

    def _parse_global_config(self):
        """This method parses the configuration file"""
        self.barman_home = self.get('barman', 'barman_home')
        self.user = self.get('barman', 'barman_user') \
            or DEFAULT_USER
        self.log_file = self.get('barman', 'log_file')
        self.log_format = self.get('barman', 'log_format') \
            or DEFAULT_LOG_FORMAT
        self.log_level = self.get('barman', 'log_level') \
            or DEFAULT_LOG_LEVEL
        self._global_config = set(self._config.items('barman'))

    def _is_global_config_changed(self):
        """Return true if something has changed in global configuration"""
        return self._global_config != set(self._config.items('barman'))

    def _load_configuration_files_directory(self):
        """Read the "configuration_files_directory" option and loads all the
        configuration files with the .conf suffix that lie in that folder
        """

        config_files_directory = self.get('barman',
                                          'configuration_files_directory')

        if not config_files_directory:
            return

        if not os.path.isdir(os.path.expanduser(config_files_directory)):
            _logger.warn(
                'Ignoring the "configuration_files_directory" option as "%s" '
                'is not a directory',
                config_files_directory)
            return

        for cfile in sorted(iglob(
                os.path.join(os.path.expanduser(config_files_directory),
                             '*.conf'))):
            filename = os.path.basename(cfile)
            if os.path.isfile(cfile):
                # Load a file
                _logger.info('Including configuration file: %s', filename)
                self._config.read(cfile)
                if self._is_global_config_changed():
                    msg = "the configuration file %s contains a not empty [" \
                          "barman] section" % filename
                    _logger.fatal(msg)
                    raise SystemExit("FATAL: %s" % msg)
            else:
                # Add an info that a file has been discarded
                _logger.warn('Discarding configuration file: %s (not a file)',
                             filename)

    def _populate_servers(self):
        """Populate server list from configuration file"""
        if self._servers is not None:
            return
        self._servers = {}
        for section in self._config.sections():
            if section == 'barman':
                continue  # skip global settings
            if section in FORBIDDEN_SERVER_NAMES:
                msg = "the reserved word '%s' is not allowed as server name. " \
                      "Please rename it." % section
                _logger.fatal(msg)
                raise SystemExit("FATAL: %s" % msg)
            self._servers[section] = Server(self, section)

    def server_names(self):
        """This method returns a list of server names"""
        self._populate_servers()
        return self._servers.keys()

    def servers(self):
        """This method returns a list of server parameters"""
        self._populate_servers()
        return self._servers.values()

    def get_server(self, name):
        """Get the server specifying its name"""
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
