# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2011-2023
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

from datetime import timedelta

import mock
import pytest
from mock import MagicMock, Mock, mock_open, patch

from barman.config import (
    BackupOptions,
    ConfigMapping,
    Config,
    RecoveryOptions,
    parse_backup_compression,
    parse_backup_compression_format,
    parse_backup_compression_location,
    parse_si_suffix,
    parse_recovery_staging_path,
    parse_slot_name,
    parse_snapshot_disks,
    parse_time_interval,
)
import testing_helpers

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

BAD_CONFIG = """
[barman]
"""

MINIMAL_CONFIG = """
[barman]
barman_home = /some/barman/home
barman_user = barman
log_file = %(barman_home)s/log/barman.log
[main]
archiver = on
description = " Text with quotes "
ssh_command = ssh -c "arcfour" -p 22 postgres@pg01.nowhere
conninfo = host=pg01.nowhere user=postgres port=5432
"""

TEST_CONFIG_BARMAN = """
[barman]
barman_home = /some/barman/home
barman_user = {USER}
compression = gzip
log_file = /some/barman/home/log/barman.log
log_level = INFO
retention_policy = redundancy 2
wal_retention_policy = base
"""

TEST_CONFIG_MAIN = """
[main]
active = true
archiver = on
description = Main PostgreSQL Database
ssh_command = ssh -c arcfour -p 22 postgres@pg01.nowhere
conninfo = host=pg01.nowhere user=postgres port=5432
backup_directory =  /some/barman/home/main
basebackups_directory = /some/barman/home/main/base
wals_directory = wals
incoming_wals_directory = /some/barman/home/main/incoming
backup_compression = "none"
custom_compression_filter = bzip2 -c -9
custom_compression_magic = 0x425a68
custom_decompression_filter = bzip2 -c -d
reuse_backup = link
retention_policy = redundancy 3
wal_retention_policy = base
last_backup_maximum_age = '1 day'
last_backup_minimum_size = '1 Mi'
last_wal_maximum_age = '1 hour'
"""

TEST_CONFIG_WEB = """
[web]
active = true
archiver = on
description = Web applications database
ssh_command = ssh -I ~/.ssh/web01_rsa -c arcfour -p 22 postgres@web01
conninfo = host=web01 user=postgres port=5432
compression =
last_backup_maximum_age = '1 day'
last_backup_minimum_size = '1 Mi'
last_wal_maximum_age = '1 hour'
"""

TEST_CONFIG = TEST_CONFIG_BARMAN + TEST_CONFIG_MAIN + TEST_CONFIG_WEB


class TestConfigMapping(object):
    """
    Test class for :class:`ConfigMapping:`.
    """

    def setup_method(self):
        self.cm = ConfigMapping(strict=False)

    @patch("barman.config.ConfigParser")
    def test_read_config_config_parser(self, mock_parser):
        """
        Test :meth:`ConfigMapping.read_config` with several input values.

        Make sure the correct :class:`ConfigParser` methods are according to
        the arguments of :meth:`ConfigMapping.read_config`.
        """

        # First round: test interface with `ConfigParser`
        # file descriptor with `read` method and `name` attribute -- Python 3
        filename = Mock()
        mock_parser.return_value = MagicMock()
        assert self.cm.read_config(filename) == [filename.name]
        mock_parser.assert_called_once_with(strict=False)
        mock_parser.return_value.read_file.assert_called_once_with(filename)
        mock_parser.return_value.readfp.assert_not_called()
        mock_parser.return_value.read.assert_not_called()

        # file descriptor with `read` method and `name` attribute -- Python 2
        filename = Mock()
        mock_parser.reset_mock()
        mock_parser.return_value = MagicMock()
        delattr(mock_parser.return_value, "read_file")
        assert self.cm.read_config(filename) == [filename.name]
        mock_parser.assert_called_once_with(strict=False)
        mock_parser.return_value.readfp.assert_called_once_with(filename)
        mock_parser.return_value.read.assert_not_called()

        # file descriptor with `read` method but no `name` attribute -- Python 3
        filename = Mock()
        mock_parser.reset_mock()
        delattr(filename, "name")
        mock_parser.return_value = MagicMock()
        assert self.cm.read_config(filename) == [None]
        mock_parser.assert_called_once_with(strict=False)
        mock_parser.return_value.read_file.assert_called_once_with(filename)
        mock_parser.return_value.readfp.assert_not_called()
        mock_parser.return_value.read.assert_not_called()

        # file descriptor with `read` method but no `name` attribute -- Python 2
        filename = Mock()
        mock_parser.reset_mock()
        delattr(filename, "name")
        mock_parser.return_value = MagicMock()
        delattr(mock_parser.return_value, "read_file")
        assert self.cm.read_config(filename) == [None]
        mock_parser.assert_called_once_with(strict=False)
        mock_parser.return_value.readfp.assert_called_once_with(filename)
        mock_parser.return_value.read.assert_not_called()

        # file path
        filename = "/some/path"
        mock_parser.reset_mock()
        mock_parser.return_value = MagicMock()
        mock_parser.return_value.read.return_value = ["/some/path"]
        assert self.cm.read_config(filename) == [filename]
        mock_parser.return_value.read_file.assert_not_called()
        mock_parser.return_value.readfp.assert_not_called()
        mock_parser.return_value.read.assert_called_once_with(filename)

    # @patch("barman.config.ConfigParser", MagicMock)
    def test_read_config_mapping(self):
        """
        Test mappping of a :meth:`ConfigMapping.read_config`.

        Make sure the mapping of configuration options to files performed by
        :meth:`ConfigMapping.read_config` occurs as expected.

        Also check if :meth:`ConfigMapping.get_config_source` returns the
        expected values based on the mapping that was built.
        """
        # Start with global config
        expected = {
            "barman": {
                "barman_home": "/etc/barman.conf",
                "barman_user": "/etc/barman.conf",
                "compression": "/etc/barman.conf",
                "log_file": "/etc/barman.conf",
                "log_level": "/etc/barman.conf",
                "retention_policy": "/etc/barman.conf",
                "wal_retention_policy": "/etc/barman.conf",
            }
        }

        with patch("builtins.open", mock_open(read_data=TEST_CONFIG_BARMAN)):
            assert self.cm.read_config("/etc/barman.conf") == ["/etc/barman.conf"]

        assert self.cm._mapping == expected

        # Add config of `main` server
        expected["main"] = {
            "active": "/etc/barman.d/main.conf",
            "archiver": "/etc/barman.d/main.conf",
            "description": "/etc/barman.d/main.conf",
            "ssh_command": "/etc/barman.d/main.conf",
            "conninfo": "/etc/barman.d/main.conf",
            "backup_directory": "/etc/barman.d/main.conf",
            "basebackups_directory": "/etc/barman.d/main.conf",
            "wals_directory": "/etc/barman.d/main.conf",
            "incoming_wals_directory": "/etc/barman.d/main.conf",
            "backup_compression": "/etc/barman.d/main.conf",
            "custom_compression_filter": "/etc/barman.d/main.conf",
            "custom_compression_magic": "/etc/barman.d/main.conf",
            "custom_decompression_filter": "/etc/barman.d/main.conf",
            "reuse_backup": "/etc/barman.d/main.conf",
            "retention_policy": "/etc/barman.d/main.conf",
            "wal_retention_policy": "/etc/barman.d/main.conf",
            "last_backup_maximum_age": "/etc/barman.d/main.conf",
            "last_backup_minimum_size": "/etc/barman.d/main.conf",
            "last_wal_maximum_age": "/etc/barman.d/main.conf",
        }

        with patch("builtins.open", mock_open(read_data=TEST_CONFIG_MAIN)):
            assert self.cm.read_config("/etc/barman.d/main.conf") == [
                "/etc/barman.d/main.conf"
            ]

        assert self.cm._mapping == expected

        # Add config of `web` server
        expected["web"] = {
            "active": "/etc/barman.d/web.conf",
            "archiver": "/etc/barman.d/web.conf",
            "description": "/etc/barman.d/web.conf",
            "ssh_command": "/etc/barman.d/web.conf",
            "conninfo": "/etc/barman.d/web.conf",
            "compression": "/etc/barman.d/web.conf",
            "last_backup_maximum_age": "/etc/barman.d/web.conf",
            "last_backup_minimum_size": "/etc/barman.d/web.conf",
            "last_wal_maximum_age": "/etc/barman.d/web.conf",
        }

        with patch("builtins.open", mock_open(read_data=TEST_CONFIG_WEB)):
            assert self.cm.read_config("/etc/barman.d/web.conf") == [
                "/etc/barman.d/web.conf"
            ]

        assert self.cm._mapping == expected

        # Override config of `web` server
        expected["web"]["active"] = "/etc/barman.d/web-override.conf"

        with patch("builtins.open", mock_open(read_data="[web]\nactive=false")):
            assert self.cm.read_config("/etc/barman.d/web-override.conf") == [
                "/etc/barman.d/web-override.conf"
            ]

        assert self.cm._mapping == expected

        # Get mapping of an invalid server
        assert self.cm.get_config_source("random", "active") == "default"
        # Get mapping of an invalid option
        assert self.cm.get_config_source("main", "random") == "default"
        # Get mapping of a valid server and option
        assert self.cm.get_config_source("main", "active") == "/etc/barman.d/main.conf"
        # Get mapping of a valid server and option, but which is defined in the
        # global `barman` section instead of on the server section
        assert self.cm.get_config_source("main", "log_level") == "/etc/barman.conf"


# noinspection PyMethodMayBeStatic
class TestConfig(object):
    """
    Test class for the configuration object
    """

    def test_server_list(self):
        """
        Test parsing of a config file
        """
        fp = StringIO(TEST_CONFIG)
        c = Config(fp)
        dbs = c.server_names()
        assert set(dbs) == set(["main", "web"])

    def test_config_file_existence(self, capsys):
        """
        Test for the existence of a config file
        """
        # Check that an SystemExit is raised if no configuration
        # file is present inside the default configuration directories
        with patch("os.path.exists") as exists_mock:
            exists_mock.return_value = False
            with pytest.raises(SystemExit):
                Config(None)
        # Check that a SystemExit is raised if the user defined
        # configuration file does not exists
        with pytest.raises(SystemExit):
            Config("/very/fake/path/to.file")

    def test_missing_barman_home(self, capsys):
        """
        Test that an exception is raised if barman_home is missing
        """
        config = Config(StringIO(BAD_CONFIG))
        with pytest.raises(SystemExit) as exc_info:
            config.validate_global_config()
        _out, err = capsys.readouterr()

        assert "Your configuration is missing required parameters. Exiting." == str(
            exc_info.value
        )
        assert 'Parameter "barman_home" is required in [barman] section.\n'

    def test_config(self):
        """
        Test for a basic configuration object construction
        """
        fp = StringIO(TEST_CONFIG)
        c = Config(fp)

        main = c.get_server("main")
        # create the expected dictionary
        expected = testing_helpers.build_config_dictionary(
            {
                "config": main.config,
                "autogenerate_manifest": False,
                "backup_compression": "none",
                "backup_compression_format": None,
                "backup_compression_level": None,
                "backup_compression_location": None,
                "backup_compression_workers": None,
                "compression": "gzip",
                "last_backup_maximum_age": timedelta(1),
                "last_backup_minimum_size": 1048576,
                "last_wal_maximum_age": timedelta(hours=1),
                "lock_directory_cleanup": True,
                "retention_policy": "redundancy 3",
                "reuse_backup": "link",
                "description": "Main PostgreSQL Database",
                "ssh_command": "ssh -c arcfour -p 22 postgres@pg01.nowhere",
                "wal_retention_policy": "base",
                "custom_compression_filter": "bzip2 -c -9",
                "custom_compression_magic": "0x425a68",
                "wals_directory": "wals",
                "custom_decompression_filter": "bzip2 -c -d",
                "backup_method": "rsync",
                "max_incoming_wals_queue": None,
                "primary_conninfo": None,
                "primary_checkpoint_timeout": 0,
            }
        )
        assert main.__dict__ == expected

        web = c.get_server("web")
        # create the expected dictionary
        expected = testing_helpers.build_config_dictionary(
            {
                "config": web.config,
                "autogenerate_manifest": False,
                "backup_directory": "/some/barman/home/web",
                "basebackups_directory": "/some/barman/home/web/base",
                "backup_compression": None,
                "backup_compression_format": None,
                "backup_compression_level": None,
                "backup_compression_location": None,
                "backup_compression_workers": None,
                "compression": None,
                "conninfo": "host=web01 user=postgres port=5432",
                "description": "Web applications database",
                "incoming_wals_directory": "/some/barman/home/web/incoming",
                "name": "web",
                "reuse_backup": None,
                "retention_policy": "redundancy 2",
                "custom_compression_magic": None,
                "wals_directory": "/some/barman/home/web/wals",
                "wal_retention_policy": "base",
                "last_backup_maximum_age": timedelta(1),
                "last_backup_minimum_size": 1048576,
                "last_wal_maximum_age": timedelta(hours=1),
                "lock_directory_cleanup": True,
                "ssh_command": "ssh -I ~/.ssh/web01_rsa -c arcfour "
                "-p 22 postgres@web01",
                "streaming_conninfo": "host=web01 user=postgres port=5432",
                "streaming_wals_directory": "/some/barman/home/web/streaming",
                "errors_directory": "/some/barman/home/web/errors",
                "max_incoming_wals_queue": None,
                "primary_conninfo": None,
                "primary_checkpoint_timeout": 0,
            }
        )
        assert web.__dict__ == expected

    def test_quotes(self):
        """
        Test quotes management during configuration parsing
        """
        fp = StringIO(MINIMAL_CONFIG)
        c = Config(fp)
        main = c.get_server("main")
        assert main.description == " Text with quotes "
        assert main.ssh_command == 'ssh -c "arcfour" ' "-p 22 postgres@pg01.nowhere"

    def test_interpolation(self):
        """
        Basic interpolation test
        """
        fp = StringIO(MINIMAL_CONFIG)
        c = Config(fp)
        main = c.get_server("main")

        # create the expected dictionary
        expected = testing_helpers.build_config_dictionary({"config": main.config})
        assert main.__dict__ == expected

    def test_parse_time_interval(self):
        """
        basic test the parsing method for timedelta values
        pass a value, check if is correctly transformed in a timedelta
        """
        # 6 hours
        val = parse_time_interval("6 hours")
        assert val == timedelta(hours=6)
        # 1 day
        val = parse_time_interval("1 day")
        assert val == timedelta(days=1)
        # 2 weeks
        val = parse_time_interval("2 weeks")
        assert val == timedelta(days=14)
        # 3 months
        val = parse_time_interval("3 months")
        assert val == timedelta(days=93)
        # this string is something that the regexp cannot manage,
        # so we expect a ValueError exception
        with pytest.raises(ValueError):
            parse_time_interval("test_string")

    def test_primary_ssh_command(self):
        """
        test command at server and global level

        test case 1:
        global: Nothing
        server: "barman@backup1.nowhere"
        expected: "barman@backup1.nowhere"

        test case 2:
        global: "barman@backup2.nowhere"
        server: Nothing
        expected: "barman@backup2.nowhere"

        test case 3:
        global: "barman@backup3.nowhere"
        server: "barman@backup4.nowhere"
        expected: "barman@backup4.nowhere"
        """

        # test case 1
        # primary_ssh_command set only for server main
        c = testing_helpers.build_config_from_dicts(
            global_conf=None,
            main_conf={
                "primary_ssh_command": "barman@backup1.nowhere",
            },
        )
        main = c.get_server("main")
        expected = testing_helpers.build_config_dictionary(
            {
                "config": c,
                "primary_ssh_command": "barman@backup1.nowhere",
            }
        )
        assert main.__dict__ == expected

        # test case 2
        # primary_ssh_command set only globally
        c = testing_helpers.build_config_from_dicts(
            global_conf={
                "primary_ssh_command": "barman@backup2.nowhere",
            },
            main_conf=None,
        )
        main = c.get_server("main")
        expected = testing_helpers.build_config_dictionary(
            {
                "config": c,
                "primary_ssh_command": "barman@backup2.nowhere",
            }
        )
        assert main.__dict__ == expected

        # test case 3
        # primary_ssh_command set both globally and on server main
        c = testing_helpers.build_config_from_dicts(
            global_conf={
                "primary_ssh_command": "barman@backup3.nowhere",
            },
            main_conf={
                "primary_ssh_command": "barman@backup4.nowhere",
            },
        )
        main = c.get_server("main")
        expected = testing_helpers.build_config_dictionary(
            {
                "config": c,
                "primary_ssh_command": "barman@backup4.nowhere",
            }
        )
        assert main.__dict__ == expected

    def test_parse_si_suffix(self):
        """
        basic test the parsing method for timedelta values
        pass a value, check if is correctly transformed in a timedelta
        """
        # A simple integer is acceptable
        val = parse_si_suffix("12345678")
        assert val == 12345678
        # 2 k -> 2000
        val = parse_si_suffix("2 k")
        assert val == 2000
        # 3Ki -> 3072
        val = parse_si_suffix("3Ki")
        assert val == 3072
        # 52M -> 52000000
        val = parse_si_suffix("52M")
        assert val == 52000000
        # 13Gi
        val = parse_si_suffix("13Gi")
        assert val == 13958643712
        # 99 Ti
        val = parse_si_suffix("99 Ti")
        assert val == 108851651149824
        # this string is something that the regexp cannot manage,
        # so we expect a ValueError exception
        with pytest.raises(ValueError):
            parse_si_suffix("12 bunnies")

    def test_server_conflict_paths(self):
        """
        Test for the presence of conflicting paths for a server
        """
        # Build a configuration with conflicts:
        # basebackups_directory = /some/barman/home/main/wals
        # wals_directory = /some/barman/home/main/wals
        c = testing_helpers.build_config_from_dicts(
            main_conf={
                "archiver": "on",
                "basebackups_directory": "/some/barman/home/main/wals",
                "description": " Text with quotes ",
            }
        )
        main = c.get_server("main")
        # create the expected dictionary
        expected = testing_helpers.build_config_dictionary(
            {
                "config": main.config,
                "disabled": True,
                "basebackups_directory": "/some/barman/home/main/wals",
                "msg_list": [
                    "Conflicting path: wals_directory=/some/barman/home/main/wals "
                    "conflicts with 'basebackups_directory' "
                    "for server 'main'"
                ],
                "description": "Text with quotes",
            }
        )
        assert main.__dict__ == expected

    def test_populate_servers(self):
        """
        Test for the presence of conflicting paths in configuration between all
        the servers
        """
        c = testing_helpers.build_config_from_dicts(
            global_conf=None,
            main_conf={
                "backup_directory": "/some/barman/home/main",
            },
            test_conf={
                "backup_directory": "/some/barman/home/main",
            },
        )

        # attribute servers_msg_list is empty before _populate_server()
        assert len(c.servers_msg_list) == 0

        c._populate_servers()

        # after _populate_servers() if there is a global paths error
        # servers_msg_list is created in configuration
        assert c.servers_msg_list
        assert len(c.servers_msg_list) == 6

    def test_populate_servers_following_symlink(self, tmpdir):
        """
        Test for the presence of conflicting paths in configuration between all
        the servers
        """
        incoming_dir = tmpdir.mkdir("incoming")
        wals_dir = tmpdir.join("wal")
        wals_dir.mksymlinkto(incoming_dir.strpath)

        c = testing_helpers.build_config_from_dicts(
            global_conf=None,
            main_conf={
                "basebackups_directory": incoming_dir.strpath,
                "incoming_wals_directory": incoming_dir.strpath,
                "wals_directory": wals_dir.strpath,
                "backup_directory": tmpdir.strpath,
            },
        )

        c._populate_servers()

        # If there is one or more path errors are present,
        # the msg_list of the 'main' server is populated during
        # the creation of the server configuration object
        assert len(c._servers["main"].msg_list) == 2
        symlink = 0
        for msg in c._servers["main"].msg_list:
            # Check for symlinks presence
            if "(symlink to: " in msg:
                symlink += 1
        assert symlink == 1

    def test_parse_recovery_staging_path(self):
        """
        Test the parse_recovery_staging_path method
        """
        assert parse_recovery_staging_path(None) is None
        assert parse_recovery_staging_path("/any/path") == "/any/path"
        with pytest.raises(ValueError):
            parse_recovery_staging_path("here/it/is")

    def test_parse_slot_name(self):
        """
        Test the parse_slot_name method
        :return:
        """

        # If the slot name is None, is really None
        assert parse_slot_name(None) is None

        # If the slot name is valid then it will passed intact
        assert parse_slot_name("barman_slot_name") == "barman_slot_name"

        # If the slot name is not valid but can be fixed by putting
        # the name in lower case, then it will be fixed
        assert parse_slot_name("Barman_slot_Name") == "barman_slot_name"

        # Even this slot name can be fixed
        assert parse_slot_name("Barman_2_slot_name") == "barman_2_slot_name"

        # If the slot name is not valid and lowering its case don't fix it,
        # we will raise a ValueError
        with pytest.raises(ValueError):
            parse_slot_name("Barman_(slot_name)")

        with pytest.raises(ValueError):
            parse_slot_name("barman slot name")

    @pytest.mark.parametrize(
        ("disk_names", "is_allowed"),
        (
            # Strings where each comma-separated string is non-empty are allowed
            ["disk0", True],
            ["disk0,disk1", True],
            ["disk0,disk1,disk2", True],
            # Empty values are not allowed
            ["disk0,,disk2", False],
            ["", False],
        ),
    )
    def test_parse_snapshot_disks(self, disk_names, is_allowed):
        # GIVEN a list of disk names
        # WHEN parse_snapshot_disks is called
        # THEN if the value is allowed we have a list of disk names
        if is_allowed:
            assert parse_snapshot_disks(disk_names) == disk_names.split(",")
        # AND if the value is not allowed we receive a ValueError
        else:
            with pytest.raises(ValueError):
                parse_snapshot_disks(disk_names)

    @pytest.mark.parametrize(
        ("compression", "is_allowed"),
        (
            ("gzip", True),
            ("lz4", True),
            ("zstd", True),
            ("none", True),
            ("lizard", False),
            ("1", False),
        ),
    )
    def test_parse_backup_compression(self, compression, is_allowed):
        """
        Test allowed and disallowed backup_compression values
        """
        if is_allowed:
            assert parse_backup_compression(compression) == compression
        else:
            with pytest.raises(ValueError):
                parse_backup_compression(compression)

    @pytest.mark.parametrize(
        ("location", "is_allowed"),
        (
            ("client", True),
            ("server", True),
            ("lizard", False),
            ("1", False),
        ),
    )
    def test_parse_backup_compression_location(self, location, is_allowed):
        """
        Test allowed and disallowed backup_compression_location values
        """
        if is_allowed:
            assert parse_backup_compression_location(location) == location
        else:
            with pytest.raises(ValueError):
                parse_backup_compression(location)

    @pytest.mark.parametrize(
        ("format", "is_allowed"),
        (
            ("tar", True),
            ("plain", True),
            ("lizard", False),
            ("1", False),
        ),
    )
    def test_parse_backup_compression_format(self, format, is_allowed):
        """
        Test allowed and disallowed backup_compression_format values
        """
        if is_allowed:
            assert parse_backup_compression_format(format) == format
        else:
            with pytest.raises(ValueError):
                parse_backup_compression(format)

    def test_global_config_to_json(self):
        """Check :meth:`Config.global_config_to_json` returns expected results.

        Make sure it works as expected both when ``with_source`` argument is
        ``True`` or ``False``.
        """
        global_conf = {
            "barman_home": "/some/barman/home",
            "barman_user": "barman",
            "compression": "gzip",
            "log_file": "/some/barman/home/log/barman.log",
            "log_level": "INFO",
            "retention_policy": "redundancy 2",
            "wal_retention_policy": "base",
        }
        c = testing_helpers.build_config_from_dicts(global_conf=global_conf)

        # Check result when `with_source` is `False`
        expected = {
            "barman_home": "/some/barman/home",
            "log_level": "INFO",
            "wal_retention_policy": "base",
            "retention_policy": "redundancy 2",
            "compression": "gzip",
            "barman_user": "barman",
            "log_file": "/some/barman/home/log/barman.log",
            "archiver": "True",
        }
        assert c.global_config_to_json(False) == expected

        # Check result when `with_source` is `True`
        for config in c._config._mapping["barman"]:
            c._config._mapping["barman"][config] = "/etc/barman.conf"

        for key, value in expected.items():
            expected[key] = {
                "source": "/etc/barman.conf",
                "value": value,
            }

        assert c.global_config_to_json(True) == expected

    def test_get_config_source(self):
        """Check :meth:`Config.get_config_source` calls the expected method.

        It is basically a wrapper for :meth:`ConfigMapping.get_config_source`.
        """
        c = testing_helpers.build_config_from_dicts()

        with patch.object(ConfigMapping, "get_config_source") as mock:
            c.get_config_source("section", "option")
            mock.assert_called_once_with("section", "option")


class TestServerConfig(object):
    def test_update_msg_list_and_disable_server(self):
        c = testing_helpers.build_config_from_dicts(
            global_conf={
                "archiver": "on",
                "backup_options": BackupOptions.EXCLUSIVE_BACKUP,
            },
            main_conf={"backup_options": ""},
        )
        main = c.get_server("main")
        assert main.disabled is False
        msg1 = "An issue occurred"
        main.update_msg_list_and_disable_server(msg1)

        assert main.disabled is True
        assert main.msg_list == [msg1]

        msg2 = "This config is not valid"
        main.update_msg_list_and_disable_server(msg2)
        assert main.msg_list == [msg1, msg2]
        assert main.disabled is True

        msg3 = "error wrong path"
        msg4 = "No idea"
        main.update_msg_list_and_disable_server([msg3, msg4])
        assert main.msg_list == [msg1, msg2, msg3, msg4]
        assert main.disabled is True

    def test_to_json(self):
        """
        Check if :meth:`ServerConfig.to_json` returns the expected results.

        We check both when ``with_source`` argument is ``True`` and ``False``.
        """
        global_conf = {
            "barman_home": "/some/barman/home",
            "barman_user": "barman",
            "compression": "gzip",
            "log_file": "/some/barman/home/log/barman.log",
            "log_level": "INFO",
            "retention_policy": "redundancy 2",
            "wal_retention_policy": "base",
        }
        main_conf = {
            "active": "true",
            "archiver": "on",
            "description": "Main PostgreSQL Database",
            "ssh_command": "ssh -c arcfour -p 22 postgres@pg01.nowhere",
            "conninfo": "host=pg01.nowhere user=postgres port=5432",
            "backup_directory": "/some/barman/home/main",
            "basebackups_directory": "/some/barman/home/main/base",
            "wals_directory": "wals",
            "incoming_wals_directory": "/some/barman/home/main/incoming",
            "backup_compression": '"none"',
            "custom_compression_filter": "bzip2 -c -9",
            "custom_compression_magic": "0x425a68",
            "custom_decompression_filter": "bzip2 -c -d",
            "reuse_backup": "link",
            "retention_policy": "redundancy 3",
            "wal_retention_policy": "base",
            "last_backup_maximum_age": "'1 day'",
            "last_backup_minimum_size": "'1 Mi'",
            "last_wal_maximum_age": "'1 hour'",
        }
        c = testing_helpers.build_config_from_dicts(
            global_conf=global_conf,
            main_conf=main_conf,
        )
        main = c.get_server("main")

        # Check `to_json(with_source=False)` works as expected
        expected_override = {
            "last_wal_maximum_age": timedelta(seconds=3600),
            "description": "Main PostgreSQL Database",
            "custom_compression_filter": "bzip2 -c -9",
            "custom_decompression_filter": "bzip2 -c -d",
            "ssh_command": "ssh -c arcfour -p 22 postgres@pg01.nowhere",
            "custom_compression_magic": "0x425a68",
            "reuse_backup": "link",
            "last_backup_maximum_age": timedelta(days=1),
            "wal_retention_policy": "base",
            "retention_policy": "redundancy 3",
            "compression": "gzip",
            "backup_compression": "none",
            "wals_directory": "wals",
            "last_backup_minimum_size": 1048576,
        }
        expected = testing_helpers.build_config_dictionary(expected_override)
        del expected["config"]
        assert main.to_json(False) == expected

        # Check `to_json(with_source=True)` works as expected
        for key, value in expected.items():
            source = "default"

            if key in global_conf:
                source = "/etc/barman.conf"

            if key in main_conf:
                source = "/etc/barman.d/main.conf"

            expected[key] = {"source": source, "value": value}

        for config in main.config._config._mapping["barman"]:
            main.config._config._mapping["barman"][config] = "/etc/barman.conf"

        for config in main.config._config._mapping["main"]:
            main.config._config._mapping["main"][config] = "/etc/barman.d/main.conf"

        assert main.to_json(True) == expected


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
        c = testing_helpers.build_config_from_dicts(
            global_conf={
                "archiver": "on",
                "backup_options": BackupOptions.EXCLUSIVE_BACKUP,
            },
            main_conf={"backup_options": ""},
        )
        main = c.get_server("main")

        # create the expected dictionary
        expected = testing_helpers.build_config_dictionary({"config": main.config})

        assert main.__dict__ == expected

    @patch("barman.config.output")
    def test_csv_values_global_conflict(self, out_mock):
        """
        test case
        global value: backup_options = exclusive_backup, concurrent_backup
        server value: backup_options =
        expected: backup_options = exclusive_backup

        Empty value is not allowed in BackupOptions class, so we expect the
        configuration parser to fall back to the global value.
        The global backup_options holds conflicting parameters, so we expect
        the config builder to fall back to ignore de directive.

        :param out_mock: Mock the output
        """
        # build a string with conflicting values
        conflict = "%s, %s" % (
            BackupOptions.EXCLUSIVE_BACKUP,
            BackupOptions.CONCURRENT_BACKUP,
        )
        # add backup_options to minimal configuration string
        c = testing_helpers.build_config_from_dicts(
            global_conf={"archiver": "on", "backup_options": conflict}, main_conf=None
        )
        main = c.get_server("main")
        # create the expected dictionary
        expected = testing_helpers.build_config_dictionary({"config": main.config})
        assert main.__dict__ == expected
        # use the mocked output class to verify the presence of the warning
        # for a bad configuration parameter
        out_mock.warning.assert_called_with(
            "Ignoring invalid configuration value '%s' for key %s in %s: %s",
            "exclusive_backup, concurrent_backup",
            "backup_options",
            "[barman] section",
            mock.ANY,
        )

    @patch("barman.config.output")
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
        c = testing_helpers.build_config_from_dicts(
            global_conf={
                "archiver": "on",
                "backup_options": BackupOptions.EXCLUSIVE_BACKUP,
            },
            main_conf={"backup_options": "none_of_your_business"},
        )
        main = c.get_server("main")

        # create the expected dictionary
        expected = testing_helpers.build_config_dictionary(
            {
                "config": main.config,
                "backup_options": set([BackupOptions.EXCLUSIVE_BACKUP]),
            }
        )
        assert main.__dict__ == expected
        # use the mocked output class to verify the presence of the warning
        # for a bad configuration parameter
        out_mock.warning.assert_called_with(
            "Ignoring invalid configuration value '%s' for key %s in %s: %s",
            "none_of_your_business",
            "backup_options",
            "[main] section",
            mock.ANY,
        )

    @patch("barman.config.output")
    def test_csv_values_multikey_invalid_server_value(self, out_mock):
        """
        test case
        global: backup_options = concurrent_backup
        server: backup_options = exclusive_backup, none_of_your_business
        result = backup_options = concurrent_backup

        the 'none_of_your_business' value on server section invalidates the
        whole csv string, because is not an allowed value of the BackupOptions
        class.
        We expect to fallback to the global 'concurrent_backup' value.
        """
        # build a string with a wrong value
        wrong_parameters = "%s, %s" % (
            BackupOptions.EXCLUSIVE_BACKUP,
            "none_of_your_business",
        )
        # add backup_options to minimal configuration string
        c = testing_helpers.build_config_from_dicts(
            global_conf={
                "archiver": "on",
                "backup_options": BackupOptions.CONCURRENT_BACKUP,
            },
            main_conf={"backup_options": wrong_parameters},
        )
        main = c.get_server("main")
        # create the expected dictionary
        expected = testing_helpers.build_config_dictionary(
            {
                "config": main.config,
                "backup_options": set(["concurrent_backup"]),
            }
        )
        assert main.__dict__ == expected
        # use the mocked output class to verify the presence of the warning
        # for a bad configuration parameter
        out_mock.warning.assert_called_with(
            "Ignoring invalid configuration value '%s' for key %s in %s: %s",
            "exclusive_backup, none_of_your_business",
            "backup_options",
            "[main] section",
            mock.ANY,
        )

    def test_csv_values_global_concurrent(self):
        """
        test case
        global value: backup_options = concurrent_backup
        expected: backup_options = concurrent_backup

        Simple test for concurrent_backup option parsing
        """
        # add backup_options to minimal configuration string
        c = testing_helpers.build_config_from_dicts(
            global_conf={
                "archiver": "on",
                "backup_options": BackupOptions.CONCURRENT_BACKUP,
            },
            main_conf=None,
        )
        main = c.get_server("main")
        # create the expected dictionary
        expected = testing_helpers.build_config_dictionary(
            {
                "config": main.config,
                "backup_options": set(["concurrent_backup"]),
                "backup_method": "rsync",
            }
        )
        assert main.__dict__ == expected

    def test_backup_option_parser(self):
        """
        Test of the BackupOption class alone.

        Builds the class using 'concurrent_backup', then using
        'exclusive_backup' as values.
        Then tests for ValueError conditions
        """
        # Builds using the two allowed values
        assert set([BackupOptions.CONCURRENT_BACKUP]) == BackupOptions(
            BackupOptions.CONCURRENT_BACKUP, "", ""
        )
        assert set([BackupOptions.EXCLUSIVE_BACKUP]) == BackupOptions(
            BackupOptions.EXCLUSIVE_BACKUP, "", ""
        )
        # build using a not allowed value
        with pytest.raises(ValueError):
            BackupOptions("test_string", "", "")
        # conflicting values error
        with pytest.raises(ValueError):
            conflict = "%s, %s" % (
                BackupOptions.EXCLUSIVE_BACKUP,
                BackupOptions.CONCURRENT_BACKUP,
            )
            BackupOptions(conflict, "", "")

    def test_csv_values_recovery_options(self):
        """
        Simple test for recovery_options values: '' and get-wal

        test case
        global value: recovery_options = ''
        expected: recovery_options = None

        test case
        global value: recovery_options = 'get-wal'
        expected: recovery_options = empty RecoveryOptions obj
        """
        # Build configuration with empty recovery_options
        c = testing_helpers.build_config_from_dicts(
            global_conf={"archiver": "on", "recovery_options": ""}, main_conf=None
        )
        main = c.get_server("main")

        expected = testing_helpers.build_config_dictionary(
            {
                "config": c,
                "recovery_options": RecoveryOptions("", "", ""),
            }
        )
        assert main.__dict__ == expected

        # Build configuration with recovery_options set to get-wal
        c = testing_helpers.build_config_from_dicts(
            global_conf={"archiver": "on", "recovery_options": "get-wal"},
            main_conf=None,
        )
        main = c.get_server("main")

        expected = testing_helpers.build_config_dictionary(
            {
                "config": c,
                "recovery_options": RecoveryOptions("get-wal", "", ""),
            }
        )
        assert main.__dict__ == expected

    def test_recovery_option_parser(self):
        """
        Test of the RecoveryOptions class.

        Builds the class using '', then using
        'get-wal' as values.
        Tests for ValueError conditions
        """
        # Builds using the two allowed values
        assert set([]) == RecoveryOptions("", "", "")
        assert set([RecoveryOptions.GET_WAL]) == RecoveryOptions(
            RecoveryOptions.GET_WAL, "", ""
        )
        # build using a not allowed value
        with pytest.raises(ValueError):
            BackupOptions("test_string", "", "")

    @patch("barman.config.output")
    def test_invalid_option_output(self, out_mock):
        """
        Test the config behavior with unknown options
        """
        # build a configuration with a a server and a global unknown vale. then
        # add them to the minimal configuration.
        c = testing_helpers.build_config_from_dicts(
            {"test_global_option": "invalid_value"},
            {"test_server_option": "invalid_value"},
        )
        c.validate_global_config()
        # use the mocked output class to verify the presence of the warning
        # for a unknown configuration parameter in the barman subsection
        out_mock.warning.assert_called_with(
            'Invalid configuration option "%s" in [%s] section.',
            "test_global_option",
            "barman",
        )
        # parse main section
        c.get_server("main")
        # use the mocked output class to verify the presence of the warning
        # for a unknown configuration parameter in the server subsection
        out_mock.warning.assert_called_with(
            'Invalid configuration option "%s" in [%s] section.',
            "test_server_option",
            "main",
        )
