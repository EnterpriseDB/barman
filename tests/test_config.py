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
from datetime import timedelta

import mock
import pytest

from barman.testing_helpers import build_config_from_dicts


try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO
from barman.config import Config, parse_time_interval, BackupOptions
from mock import patch


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
    'backup_options': BackupOptions(BackupOptions.EXCLUSIVE_BACKUP, "", ""),
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
    'basebackup_retry_sleep': 30,
    'basebackup_retry_times': 0,
    'post_archive_script': None,
    'pre_archive_script': None,
    'last_backup_maximum_age': timedelta(1),
}

TEST_CONFIG_WEB = {
    'active': True,
    'backup_directory': '/srv/barman/web',
    'backup_options': BackupOptions(BackupOptions.EXCLUSIVE_BACKUP, "", ""),
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
    'basebackup_retry_sleep': 30,
    'basebackup_retry_times': 0,
    'post_archive_script': None,
    'pre_archive_script': None,
    'last_backup_maximum_age': timedelta(1),
}

MINIMAL_CONFIG = """
[barman]
barman_home = /srv/barman
barman_user = {USER}
log_file = %(barman_home)s/log/barman.log
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
    'backup_options': BackupOptions(BackupOptions.EXCLUSIVE_BACKUP, "", ""),
    'basebackup_retry_sleep': 30,
    'basebackup_retry_times': 0,
    'post_archive_script': None,
    'pre_archive_script': None,
    'last_backup_maximum_age': None,
}


# noinspection PyMethodMayBeStatic
class Test(object):

    def test_server_list(self):
        fp = StringIO(TEST_CONFIG.format(**os.environ))
        c = Config(fp)
        dbs = c.server_names()
        assert set(dbs) == set(['main', 'web'])

    def test_config(self):
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
        assert main.description == ' Text with quotes '
        assert main.ssh_command == 'ssh -c "arcfour" -p 22 postgres@pg01'

    def test_interpolation(self):
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

# noinspection PyMethodMayBeStatic
class TestCsvParsing(object):
    """
    Csv parser test class
    """

    def test_csv_values_global_exclusive(self):
        """
        test case
        global value: backup_options = exclusive_backup
        server value: backup_options =
        expected: backup_options = exclusive_backup

        Empty value is not allowed in BackupOptions class, so we expect the
        configuration parser to fall back to the global value.
        """
        # add backup_options configuration to minimal configuration string
        c = build_config_from_dicts(
            {'backup_options': BackupOptions.EXCLUSIVE_BACKUP},
            {'backup_options': ''})
        main = c.get_server('main')

        # create the expected dictionary
        expected = dict(config=c)
        expected.update(MINIMAL_CONFIG_MAIN)

        assert main.__dict__ == expected

    @patch('barman.config.output')
    def test_csv_values_global_conflict(self, out_mock):
        """
        test case
        global value: backup_options = exclusive_backup, concurrent_backup
        server value: backup_options =
        expected: backup_options = exclusive_backup

        Empty value is not allowed in BackupOptions class, so we expect the
        configuration parser to fall back to the global value.
        The global backup_options holds conflicting parameters, so we expect the
        config builder to fall back to ignore de directive.
        """
        # build a string with conflicting values
        conflict = "%s, %s" % (BackupOptions.EXCLUSIVE_BACKUP,
                               BackupOptions.CONCURRENT_BACKUP)
        # add backup_options to minimal configuration string
        c = build_config_from_dicts(
            {'backup_options': conflict},
            None)
        main = c.get_server('main')
        # create the expected dictionary
        expected = dict(config=c)
        expected.update(MINIMAL_CONFIG_MAIN)
        assert main.__dict__ == expected
        # use the mocked output class to verify the presence of the warning
        # for a bad configuration parameter
        out_mock.warning.assert_called_with("Invalid configuration value '%s' "
                                            "for key %s in %s: %s", None,
                                            'backup_options',
                                            '[barman] section', mock.ANY)

    @patch('barman.config.output')
    def test_csv_values_invalid_server_value(self, out_mock):
        """
        test case
        global: backup_options = exclusive_backup
        server: backup_options = none_of_your_business
        result = backup_options = exclusive_backup

        The 'none_of_your_business' value on server section,
        is not an allowed value for the BackupOptions class,
        We expect to the config builder to fallback to the global
        'exclusive_backup' value
        """
        # add backup_options to minimal configuration string
        c = build_config_from_dicts(
            {'backup_options': BackupOptions.EXCLUSIVE_BACKUP},
            {'backup_options': 'none_of_your_business'})
        main = c.get_server('main')
        # create the expected dictionary
        expected = dict(config=c)
        expected.update(MINIMAL_CONFIG_MAIN)
        assert main.__dict__ == expected
        # use the mocked output class to verify the presence of the warning
        # for a bad configuration parameter
        out_mock.warning.assert_called_with("Invalid configuration value '%s' "
                                            "for key %s in %s: %s",
                                            None, 'backup_options',
                                            '[main] section', mock.ANY)

    @patch('barman.config.output')
    def test_csv_values_multikey_invalid_server_value(self, out_mock):
        """
        test case
        globale: backup_options = concurrent_backup
        server: backup_options = exclusive_backup, none_of_your_business
        risultato = backup_options = concurrent_backup

        the 'none_of_your_business' value on server section invalidates the
        whole csv string, because is not an allowed value of the BackupOptions
        class.
        We expect to fallback to the global 'concurrent_backup' value.
        """
        # build a string with a wrong value
        wrong_parameters = "%s, %s" % (BackupOptions.EXCLUSIVE_BACKUP,
                                       'none_of_your_business')
        # add backup_options to minimal configuration string
        c = build_config_from_dicts(
            {'backup_options': BackupOptions.CONCURRENT_BACKUP},
            {'backup_options': wrong_parameters})
        main = c.get_server('main')
        # create the expected dictionary
        expected = dict(config=c)
        expected.update(MINIMAL_CONFIG_MAIN)
        # override the backup_options value in the expected dictionary
        expected['backup_options'] = BackupOptions(
            BackupOptions.CONCURRENT_BACKUP, "", "")

        assert main.__dict__ == expected
        # use the mocked output class to verify the presence of the warning
        # for a bad configuration parameter
        out_mock.warning.assert_called_with("Invalid configuration value '%s' "
                                            "for key %s in %s: %s", None,
                                            'backup_options',
                                            '[main] section', mock.ANY)

    def test_csv_values_global_concurrent(self):
        """
        test case
        global value: backup_options = concurrent_backup
        expected: backup_options = concurrent_backup

        Simple test for concurrent_backup option parsing
        """
        # add backup_options to minimal configuration string
        c = build_config_from_dicts(
            {'backup_options': BackupOptions.CONCURRENT_BACKUP},
            None)
        main = c.get_server('main')

        expected = dict(config=c)
        expected.update(MINIMAL_CONFIG_MAIN)
        # override the backup_options value in the expected dictionary
        expected['backup_options'] = BackupOptions(
            BackupOptions.CONCURRENT_BACKUP, "", "")

        assert main.__dict__ == expected

    def test_backup_option_parser(self):
        """
        Test of the BackupOption class alone.

        Builds the class using 'concurrent_backup', then using
        'exclusive_backup' as values.
        Then tests for ValueError conditions
        """
        # Builds using the two allowed values
        assert set([BackupOptions.CONCURRENT_BACKUP]) == \
            BackupOptions(BackupOptions.CONCURRENT_BACKUP, "", "")
        assert set([BackupOptions.EXCLUSIVE_BACKUP]) == \
            BackupOptions(BackupOptions.EXCLUSIVE_BACKUP, "", "")
        # build using a not allowed value
        with pytest.raises(ValueError):
            BackupOptions("test_string", "", "")
        # conflicting values error
        with pytest.raises(ValueError):
            conflict = "%s, %s" % (BackupOptions.EXCLUSIVE_BACKUP,
                                   BackupOptions.CONCURRENT_BACKUP)
            BackupOptions(conflict, "", "")

    @patch('barman.config.output')
    def test_invalid_option_output(self, out_mock):
        """
        Test the config behavior with unknown options
        """
        # build a configuration with a a server and a global unknown vale. then
        # add them to the minimal configuration.
        c = build_config_from_dicts(
            {'test_global_option': 'invalid_value'},
            {'test_server_option': 'invalid_value'})
        c.validate_global_config()
        # use the mocked output class to verify the presence of the warning
        # for a unknown configuration parameter in the barman subsection
        out_mock.warning.assert_called_with(
            'Invalid configuration option "%s" in [%s] section.',
            'test_global_option',
            'barman')
        #parse main section
        c.get_server('main')
        # use the mocked output class to verify the presence of the warning
        # for a unknown configuration parameter in the server subsection
        out_mock.warning.assert_called_with(
            'Invalid configuration option "%s" in [%s] section.',
            'test_server_option',
            'main')
