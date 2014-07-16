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

"""
This module is responsible for all the things related to
Barman configuration, such as parsing configuration file.
"""
import inspect

import os
import re
from ConfigParser import ConfigParser, NoOptionError
import logging.handlers
from glob import iglob
from barman import output
import datetime

_logger = logging.getLogger(__name__)

FORBIDDEN_SERVER_NAMES = ['all']

DEFAULT_USER = 'barman'
DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_LOG_FORMAT = "%(asctime)s [%(process)s] %(name)s " \
                     "%(levelname)s: %(message)s"

_TRUE_RE = re.compile(r"""^(true|t|yes|1)$""", re.IGNORECASE)
_FALSE_RE = re.compile(r"""^(false|f|no|0)$""", re.IGNORECASE)
_TIME_INTERVAL_RE = re.compile(r"""
      ^\s*
      (\d+)\s+(day|month|week)s?  # N (day|month|week) with optional 's'
      \s*$
      """, re.IGNORECASE | re.VERBOSE)


class CsvOption(set):

    """
    Base class for CSV options.

    Given a comma delimited string, this class is a list containing the
    submitted options.
    Internally, it uses a set in order to avoid option replication.
    Allowed values for the CSV option are contained in the 'value_list'
    attribute.
    The 'conflicts' attribute specifies for any value, the list of
    values that are prohibited (and thus generate a conflict).
    If a conflict is found, raises a ValueError exception.
    """
    value_list = []
    conflicts = {}

    def __init__(self, value, key, source):
        # Invoke parent class init and initialize an empty set
        super(CsvOption, self).__init__()

        # Parse not None values
        if value is not None:
            self.parse(value, key, source)

        # Validates the object structure before returning the new instance
        self.validate(key, source)

    def parse(self, value, key, source):
        """
        Parses a list of values and correctly assign the set of values
        (removing duplication) and checking for conflicts.
        """
        if value == '':
            return
        values_list = value.split(',')
        for val in values_list:
            val = val.strip().lower()
            if val in self.value_list:
                # check for conflicting values. if a conflict is
                # found the option is not valid then, raise exception.
                if val in self.conflicts and self.conflicts[val] in self:
                    raise ValueError("Invalid configuration value '%s' for "
                                     "key %s in %s: cannot contain both "
                                     "'%s' and '%s'."
                                     "Configuration directive ignored." %
                                     (val, key, source, val,
                                      self.conflicts[val]))
                else:
                    #otherwise use parsed value
                    self.add(val)
            else:
                # not allowed value, reject the configuration
                raise ValueError("Invalid configuration value '%s' for "
                                 "key %s in %s: Unknown option" %
                                 (val, key, source))

    def validate(self, key, source):
        """
        Override this method for special validation needs
        """

    def to_json(self):
        """
        Output representation of the obj for JSON serialization

        The result is a string which can be parsed by the same class
        """
        return ",".join(self)


class BackupOptions(CsvOption):
    """
    Extends CsvOption class providing all the details for the backup_options
    field
    """
    # constants containing labels for allowed values
    EXCLUSIVE_BACKUP = 'exclusive_backup'
    CONCURRENT_BACKUP = 'concurrent_backup'

    #list holding all the allowed values for the BackupOption class
    value_list = [EXCLUSIVE_BACKUP, CONCURRENT_BACKUP]
    # map holding all the possible conflicts between the allowed values
    conflicts = {
        EXCLUSIVE_BACKUP: CONCURRENT_BACKUP,
        CONCURRENT_BACKUP: EXCLUSIVE_BACKUP, }

    def validate(self, key, source):
        """
        Validates backup_option values: currently it makes sure
        that either exclusive_backup or concurrent_backup are set.
        """
        if self.CONCURRENT_BACKUP not in self \
                and self.EXCLUSIVE_BACKUP not in self:
            raise ValueError("Invalid configuration value for "
                             "key %s in %s: it must contain either "
                             "exclusive_backup or concurrent_backup option"
                             % (key, source))


def parse_boolean(value):
    """
    Parse a string to a boolean value

    :param str value: string representing a boolean
    :raises ValueError: if the string is an invalid boolean representation
    """
    if _TRUE_RE.match(value):
        return True
    if _FALSE_RE.match(value):
        return False
    raise ValueError("Invalid boolean representation (use 'true' or 'false')")


def parse_time_interval(value):
    """
    Parse a string, transforming it in a time interval.
    Accepted format: N (day|month|week)s

    :param str value: the string to evaluate
    """
    # if empty string or none return none
    if value is None or value == '':
        return None
    result = _TIME_INTERVAL_RE.match(value)
    # if the string doesn't match, the option is invalid
    if not result:
        raise ValueError("Invalid value for a time interval %s" %
                         value)
    # if the int conversion
    value = int(result.groups()[0])
    unit = result.groups()[1][0].lower()

    # Calculates the time delta
    if unit == 'd':
        time_delta = datetime.timedelta(days=value)
    elif unit == 'w':
        time_delta = datetime.timedelta(weeks=value)
    elif unit == 'm':
        time_delta = datetime.timedelta(days=(31 * value))
    else:
        # This should never happen
        raise ValueError("Invalid unit time %s" % unit)

    return time_delta


class Server(object):
    """
    This class represents a server.
    """

    KEYS = [
        'active', 'description', 'ssh_command', 'conninfo',
        'backup_directory', 'basebackups_directory',
        'wals_directory', 'incoming_wals_directory', 'lock_file',
        'compression', 'custom_compression_filter',
        'custom_decompression_filter', 'retention_policy_mode',
        'retention_policy',
        'wal_retention_policy', 'pre_backup_script', 'post_backup_script',
        'pre_archive_script', 'post_archive_script',
        'minimum_redundancy', 'bandwidth_limit', 'tablespace_bandwidth_limit',
        'backup_options', 'immediate_checkpoint', 'network_compression',
        'basebackup_retry_times', 'basebackup_retry_sleep',
        'last_backup_maximum_age',
    ]

    BARMAN_KEYS = [
        'compression', 'custom_compression_filter',
        'custom_decompression_filter', 'retention_policy_mode',
        'retention_policy',
        'wal_retention_policy', 'pre_backup_script', 'post_backup_script',
        'pre_archive_script', 'post_archive_script',
        'configuration_files_directory',
        'minimum_redundancy', 'bandwidth_limit', 'tablespace_bandwidth_limit',
        'backup_options', 'immediate_checkpoint', 'network_compression',
        'basebackup_retry_times', 'basebackup_retry_sleep',
        'last_backup_maximum_age',
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
        'backup_options': "%s" % BackupOptions.EXCLUSIVE_BACKUP,
        'immediate_checkpoint': 'false',
        'network_compression': 'false',
        'basebackup_retry_times': '0',
        'basebackup_retry_sleep': '30',
    }

    PARSERS = {
        'active': parse_boolean,
        'immediate_checkpoint': parse_boolean,
        'network_compression': parse_boolean,
        'backup_options': BackupOptions,
        'basebackup_retry_times': int,
        'basebackup_retry_sleep': int,
        'last_backup_maximum_age': parse_time_interval,
    }

    def invoke_parser(self, key, source, value, new_value):
        """
        Function used for parsing configuration values.
        If needed, it uses special parsers from the PARSERS map,
        and handles parsing exceptions.

        Uses two values (value and new_value) to manage
        configuration hierarchy (server config overwrites global config).

        :param str key: the name of the configuration option
        :param str source: the section that contains the configuration option
        :param value: the old value of the option if present.
        :param str new_value: the new value that needs to be parsed
        :return: the parsed value of a configuration option
        """
        # If the new value is None, returns the old value
        if new_value is None:
            return value
        # If we have a parser for the current key, use it to obtain the
        # actual value. If an exception is thrown, print a warning and
        # ignore the value.
        # noinspection PyBroadException
        if key in self.PARSERS:
            parser = self.PARSERS[key]
            try:
                # If the parser is a subclass of the CsvOption class
                # we need a different invocation, which passes not only
                # the value to the parser, but also the key name
                # and the section that contains the configuration
                if inspect.isclass(parser) \
                        and issubclass(parser, CsvOption):
                    value = parser(new_value, key, source)
                else:
                    value = parser(new_value)
            except Exception, e:
                output.warning("Invalid configuration value '%s' for key %s"
                               " in %s: %s",
                               value, key, source, e)
                _logger.exception(e)
        else:
            value = new_value
        return value

    def __init__(self, config, name):
        self.config = config
        self.name = name
        self.barman_home = config.get('barman', 'barman_home')
        config.validate_server_config(self.name)
        for key in Server.KEYS:
            value = None
            # Get the setting from the [name] section of config file
            # A literal None value is converted to an empty string
            new_value = config.get(name, key, self.__dict__, none_value='')
            source = '[%s] section' % name
            value = self.invoke_parser(key, source, value, new_value)
            # If the setting isn't present in [name] section of config file
            # check if it has to be inherited from the [barman] section
            if value is None and key in Server.BARMAN_KEYS:
                new_value = config.get('barman',
                                       key,
                                       self.__dict__,
                                       none_value='')
                source = '[barman] section'
                value = self.invoke_parser(key, source, value, new_value)
            # If the setting isn't present in [name] section of config file
            # and is not inherited from global section use its default
            # (if present)
            if value is None and key in Server.DEFAULTS:
                new_value = Server.DEFAULTS[key] % self.__dict__
                source = 'DEFAULTS'
                value = self.invoke_parser(key, source, value, new_value)
            # An empty string is a None value (bypassing inheritance
            # from global configuration)
            if value is not None and value == '':
                value = None
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

    def get(self, section, option, defaults=None, none_value=None):
        """Method to get the value from a given section from
        Barman configuration
        """
        if not self._config.has_section(section):
            return None
        try:
            value = self._config.get(section, option, raw=False, vars=defaults)
            if value.lower() == 'none':
                value = none_value
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

    def load_configuration_files_directory(self):
        """
        Read the "configuration_files_directory" option and load all the
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
                _logger.debug('Including configuration file: %s', filename)
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
        """
        Get the configuration of the specified server

        :param str name: the server name
        """
        self._populate_servers()
        return self._servers.get(name, None)

    def validate_global_config(self):
        """
        Validate global configuration parameters
        """
        # Check for the existence of unexpected parameters in the
        # global section of the configuration file
        keys = ['barman_home',
                'barman_user',
                'log_file',
                'log_level',
                'configuration_files_directory']
        keys.extend(Server.KEYS)
        self._validate_with_keys(self._global_config,
                                 keys, 'barman')

    def validate_server_config(self, server):
        """
        Validate configuration parameters for a specified server

        :param str server: the server name
        """
        # Check for the existence of unexpected parameters in the
        # server section of the configuration file
        self._validate_with_keys(self._config.items(server),
                                 Server.KEYS, server)

    @staticmethod
    def _validate_with_keys(config_items, allowed_keys, section):
        """
        Check every config parameter against a list of allowed keys

        :param config_items: list of tuples containing provided parameters
            along with their values
        :param allowed_keys: list of allowed keys
        :param section: source section (for error reporting)
        """
        for parameter in config_items:
            # if the parameter name is not in the list of allowed values,
            # then output a warning
            name = parameter[0]
            if name not in allowed_keys:
                output.warning('Invalid configuration option "%s" in [%s] '
                               'section.', name, section)


# easy raw config diagnostic with python -m
# noinspection PyProtectedMember
def _main():
    print "Active configuration settings:"
    r = Config()
    r.load_configuration_files_directory()
    for section in r._config.sections():
        print "Section: %s" % section
        for option in r._config.options(section):
            print "\t%s = %s " % (option, r.get(section, option))


if __name__ == "__main__":
    _main()
