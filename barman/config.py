# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2011-2025
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

:data COMPRESSIONS: A list of supported compression algorithms for WAL files.
:data COMPRESSION_LEVELS: A list of supported compression levels for WAL files.
"""

import collections
import datetime
import inspect
import json
import logging.handlers
import os
import re
import sys
from copy import deepcopy
from glob import iglob
from typing import List

from barman import output, utils
from barman.compression import compression_registry

try:
    from ConfigParser import ConfigParser, NoOptionError
except ImportError:
    from configparser import ConfigParser, NoOptionError


# create a namedtuple object called PathConflict with 'label' and 'server'
PathConflict = collections.namedtuple("PathConflict", "label server")

_logger = logging.getLogger(__name__)

FORBIDDEN_SERVER_NAMES = ["all"]

DEFAULT_USER = "barman"
DEFAULT_CLEANUP = "true"
DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_LOG_FORMAT = "%(asctime)s [%(process)s] %(name)s %(levelname)s: %(message)s"

_TRUE_RE = re.compile(r"""^(true|t|yes|1|on)$""", re.IGNORECASE)
_FALSE_RE = re.compile(r"""^(false|f|no|0|off)$""", re.IGNORECASE)
_TIME_INTERVAL_RE = re.compile(
    r"""
      ^\s*
      # N (day|month|week|hour) with optional 's'
      (\d+)\s+(day|month|week|hour)s?
      \s*$
      """,
    re.IGNORECASE | re.VERBOSE,
)
_SLOT_NAME_RE = re.compile("^[0-9a-z_]+$")
_SI_SUFFIX_RE = re.compile(r"""(\d+)\s*(k|Ki|M|Mi|G|Gi|T|Ti)?\s*$""")

REUSE_BACKUP_VALUES = ("copy", "link", "off")

# Possible copy methods for backups (must be all lowercase)
BACKUP_METHOD_VALUES = ["rsync", "postgres", "local-rsync", "snapshot"]

CREATE_SLOT_VALUES = ["manual", "auto"]

# Config values relating to pg_basebackup compression
BASEBACKUP_COMPRESSIONS = ["gzip", "lz4", "zstd", "none"]

# WAL compression options
COMPRESSIONS = compression_registry.keys()

# WAL compression level options
COMPRESSION_LEVELS = ["low", "medium", "high"]

# Encryption options
ENCRYPTION_VALUES = ["none", "gpg"]


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
        if not value:
            return
        values_list = value.split(",")
        for val in sorted(values_list):
            val = val.strip().lower()
            if val in self.value_list:
                # check for conflicting values. if a conflict is
                # found the option is not valid then, raise exception.
                if val in self.conflicts and self.conflicts[val] in self:
                    raise ValueError(
                        "Invalid configuration value '%s' for "
                        "key %s in %s: cannot contain both "
                        "'%s' and '%s'."
                        "Configuration directive ignored."
                        % (val, key, source, val, self.conflicts[val])
                    )
                else:
                    # otherwise use parsed value
                    self.add(val)
            else:
                # not allowed value, reject the configuration
                raise ValueError(
                    "Invalid configuration value '%s' for "
                    "key %s in %s: Unknown option" % (val, key, source)
                )

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
    EXCLUSIVE_BACKUP = "exclusive_backup"
    CONCURRENT_BACKUP = "concurrent_backup"
    EXTERNAL_CONFIGURATION = "external_configuration"

    # list holding all the allowed values for the BackupOption class
    value_list = [EXCLUSIVE_BACKUP, CONCURRENT_BACKUP, EXTERNAL_CONFIGURATION]
    # map holding all the possible conflicts between the allowed values
    conflicts = {
        EXCLUSIVE_BACKUP: CONCURRENT_BACKUP,
        CONCURRENT_BACKUP: EXCLUSIVE_BACKUP,
    }


class RecoveryOptions(CsvOption):
    """
    Extends CsvOption class providing all the details for the recovery_options
    field
    """

    # constants containing labels for allowed values
    GET_WAL = "get-wal"

    # list holding all the allowed values for the RecoveryOptions class
    value_list = [GET_WAL]


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
    raise ValueError(
        "Invalid boolean representation (must be one in: "
        "true|t|yes|1|on | false|f|no|0|off)"
    )


def parse_time_interval(value):
    """
    Parse a string, transforming it in a time interval.
    Accepted format: N (day|month|week)s

    :param str value: the string to evaluate
    """
    # if empty string or none return none
    if value is None or value == "":
        return None
    result = _TIME_INTERVAL_RE.match(value)
    # if the string doesn't match, the option is invalid
    if not result:
        raise ValueError("Invalid value for a time interval %s" % value)
    # if the int conversion
    value = int(result.groups()[0])
    unit = result.groups()[1][0].lower()

    # Calculates the time delta
    if unit == "d":
        time_delta = datetime.timedelta(days=value)
    elif unit == "w":
        time_delta = datetime.timedelta(weeks=value)
    elif unit == "m":
        time_delta = datetime.timedelta(days=(31 * value))
    elif unit == "h":
        time_delta = datetime.timedelta(hours=value)
    else:
        # This should never happen
        raise ValueError("Invalid unit time %s" % unit)

    return time_delta


def parse_si_suffix(value):
    """
    Parse a string, transforming it into integer and multiplying by
    the SI or IEC suffix
    eg a suffix of Ki multiplies the integer value by 1024
    and returns the new value

    Accepted format: N (k|Ki|M|Mi|G|Gi|T|Ti)

    :param str value: the string to evaluate
    """
    # if empty string or none return none
    if value is None or value == "":
        return None
    result = _SI_SUFFIX_RE.match(value)
    if not result:
        raise ValueError("Invalid value for a number %s" % value)
    # if the int conversion
    value = int(result.groups()[0])
    unit = result.groups()[1]

    # Calculates the value
    if unit == "k":
        value *= 1000
    elif unit == "Ki":
        value *= 1024
    elif unit == "M":
        value *= 1000000
    elif unit == "Mi":
        value *= 1048576
    elif unit == "G":
        value *= 1000000000
    elif unit == "Gi":
        value *= 1073741824
    elif unit == "T":
        value *= 1000000000000
    elif unit == "Ti":
        value *= 1099511627776

    return value


def parse_reuse_backup(value):
    """
    Parse a string to a valid reuse_backup value.

    Valid values are "copy", "link" and "off"

    :param str value: reuse_backup value
    :raises ValueError: if the value is invalid
    """
    if value is None:
        return None
    if value.lower() in REUSE_BACKUP_VALUES:
        return value.lower()
    raise ValueError(
        "Invalid value (use '%s' or '%s')"
        % ("', '".join(REUSE_BACKUP_VALUES[:-1]), REUSE_BACKUP_VALUES[-1])
    )


def parse_backup_compression(value):
    """
    Parse a string to a valid backup_compression value.

    :param str value: backup_compression value
    :raises ValueError: if the value is invalid
    """
    if value is None:
        return None
    if value.lower() in BASEBACKUP_COMPRESSIONS:
        return value.lower()
    raise ValueError(
        "Invalid value '%s'(must be one in: %s)" % (value, BASEBACKUP_COMPRESSIONS)
    )


def parse_backup_compression_format(value):
    """
    Parse a string to a valid backup_compression format value.

    Valid values are "plain" and "tar"

    :param str value: backup_compression_location value
    :raises ValueError: if the value is invalid
    """
    if value is None:
        return None
    if value.lower() in ("plain", "tar"):
        return value.lower()
    raise ValueError("Invalid value (must be either `plain` or `tar`)")


def parse_backup_compression_location(value):
    """
    Parse a string to a valid backup_compression location value.

    Valid values are "client" and "server"

    :param str value: backup_compression_location value
    :raises ValueError: if the value is invalid
    """
    if value is None:
        return None
    if value.lower() in ("client", "server"):
        return value.lower()
    raise ValueError("Invalid value (must be either `client` or `server`)")


def parse_encryption(value):
    """
    Parse a string to valid encryption value.

    Valid values are defined in :data:`ENCRYPTION_VALUES`.

    :param str value: string value to be parsed
    :raises ValueError: if the *value* is invalid
    """
    if value is not None:
        value = value.lower()
        if value == "none":
            return None
        if value not in ENCRYPTION_VALUES:
            raise ValueError(
                "Invalid encryption value '%s'. Allowed values are: %s."
                % (value, ", ".join(ENCRYPTION_VALUES))
            )
    return value


def parse_backup_method(value):
    """
    Parse a string to a valid backup_method value.

    Valid values are contained in BACKUP_METHOD_VALUES list

    :param str value: backup_method value
    :raises ValueError: if the value is invalid
    """
    if value is None:
        return None
    if value.lower() in BACKUP_METHOD_VALUES:
        return value.lower()
    raise ValueError(
        "Invalid value (must be one in: '%s')" % ("', '".join(BACKUP_METHOD_VALUES))
    )


def parse_compression(value):
    """
    Parse a string to a valid compression option.

    Valid values are the compression algorithms supported by Barman, as defined in
    :data:`barman.compression.compression_registry`.

    :param str value: compression value
    :raises ValueError: if the value is invalid
    """
    if value:
        value = value.lower()
        if value not in COMPRESSIONS:
            raise ValueError(
                "Invalid value: '%s' (must be one in: %s)"
                % (value, ", ".join(COMPRESSIONS))
            )
    return value


def parse_compression_level(value):
    """
    Parse a string to a valid compression level option.

    Valid values are ``low``, ``medium``, ``high`` and any integer number.

    :param str value: compression_level value
    :raises ValueError: if the value is invalid
    """
    if value:
        value = value.lower()
        # Handle negative compression levels
        # Among the supported, only zstd allows negatives for now
        if value.lstrip("-").isdigit():
            value = int(value)
        elif value not in COMPRESSION_LEVELS:
            raise ValueError(
                "Invalid value: '%s' (must be one in [%s] or an acceptable integer)"
                % (value, ", ".join(COMPRESSION_LEVELS))
            )
    return value


def parse_staging_path(value):
    if value is None or os.path.isabs(value):
        return value
    raise ValueError("Invalid value : '%s' (must be an absolute path)" % value)


def parse_slot_name(value):
    """
    Replication slot names may only contain lower case letters, numbers,
    and the underscore character. This function parse a replication slot name

    :param str value: slot_name value
    :return:
    """
    if value is None:
        return None

    value = value.lower()
    if not _SLOT_NAME_RE.match(value):
        raise ValueError(
            "Replication slot names may only contain lower case letters, "
            "numbers, and the underscore character."
        )
    return value


def parse_snapshot_disks(value):
    """
    Parse a comma separated list of names used to reference disks managed by a cloud
    provider.

    :param str value: Comma separated list of disk names
    :return: List of disk names
    """
    disk_names = value.split(",")
    # Verify each parsed disk is not an empty string
    for disk_name in disk_names:
        if disk_name == "":
            raise ValueError(disk_names)
    return disk_names


def parse_create_slot(value):
    """
    Parse a string to a valid create_slot value.

    Valid values are "manual" and "auto"

    :param str value: create_slot value
    :raises ValueError: if the value is invalid
    """
    if value is None:
        return None
    value = value.lower()
    if value in CREATE_SLOT_VALUES:
        return value
    raise ValueError(
        "Invalid value (use '%s' or '%s')"
        % ("', '".join(CREATE_SLOT_VALUES[:-1]), CREATE_SLOT_VALUES[-1])
    )


class BaseConfig(object):
    """
    Contains basic methods for handling configuration of Servers and Models.

    You are expected to inherit from this class and define at least the
    :cvar:`PARSERS` dictionary with a mapping of parsers for each suported
    configuration option.
    """

    PARSERS = {}

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
                if inspect.isclass(parser) and issubclass(parser, CsvOption):
                    value = parser(new_value, key, source)
                else:
                    value = parser(new_value)
            except Exception as e:
                output.warning(
                    "Ignoring invalid configuration value '%s' for key %s in %s: %s",
                    new_value,
                    key,
                    source,
                    e,
                )
        else:
            value = new_value
        return value


class ServerConfig(BaseConfig):
    """
    This class represents the configuration for a specific Server instance.
    """

    KEYS = [
        "active",
        "archiver",
        "archiver_batch_size",
        "autogenerate_manifest",
        "aws_await_snapshots_timeout",
        "aws_snapshot_lock_mode",
        "aws_snapshot_lock_duration",
        "aws_snapshot_lock_cool_off_period",
        "aws_snapshot_lock_expiration_date",
        "aws_profile",
        "aws_region",
        "azure_credential",
        "azure_resource_group",
        "azure_subscription_id",
        "backup_compression",
        "backup_compression_format",
        "backup_compression_level",
        "backup_compression_location",
        "backup_compression_workers",
        "backup_directory",
        "backup_method",
        "backup_options",
        "bandwidth_limit",
        "basebackup_retry_sleep",
        "basebackup_retry_times",
        "basebackups_directory",
        "check_timeout",
        "cluster",
        "compression",
        "compression_level",
        "conninfo",
        "custom_compression_filter",
        "custom_decompression_filter",
        "custom_compression_magic",
        "description",
        "disabled",
        "encryption",
        "encryption_key_id",
        "encryption_passphrase_command",
        "errors_directory",
        "forward_config_path",
        "gcp_project",
        "gcp_zone",
        "immediate_checkpoint",
        "incoming_wals_directory",
        "keepalive_interval",
        "last_backup_maximum_age",
        "last_backup_minimum_size",
        "last_wal_maximum_age",
        "local_staging_path",
        "max_incoming_wals_queue",
        "minimum_redundancy",
        "network_compression",
        "parallel_jobs",
        "parallel_jobs_start_batch_period",
        "parallel_jobs_start_batch_size",
        "path_prefix",
        "post_archive_retry_script",
        "post_archive_script",
        "post_backup_retry_script",
        "post_backup_script",
        "post_delete_script",
        "post_delete_retry_script",
        "post_recovery_retry_script",
        "post_recovery_script",
        "post_wal_delete_script",
        "post_wal_delete_retry_script",
        "pre_archive_retry_script",
        "pre_archive_script",
        "pre_backup_retry_script",
        "pre_backup_script",
        "pre_delete_script",
        "pre_delete_retry_script",
        "pre_recovery_retry_script",
        "pre_recovery_script",
        "pre_wal_delete_script",
        "pre_wal_delete_retry_script",
        "primary_checkpoint_timeout",
        "primary_conninfo",
        "primary_ssh_command",
        "recovery_options",
        "recovery_staging_path",
        "create_slot",
        "retention_policy",
        "retention_policy_mode",
        "reuse_backup",
        "slot_name",
        "snapshot_disks",
        "snapshot_gcp_project",  # Deprecated, replaced by gcp_project
        "snapshot_instance",
        "snapshot_provider",
        "snapshot_zone",  # Deprecated, replaced by gcp_zone
        "ssh_command",
        "streaming_archiver",
        "streaming_archiver_batch_size",
        "streaming_archiver_name",
        "streaming_backup_name",
        "streaming_conninfo",
        "streaming_wals_directory",
        "tablespace_bandwidth_limit",
        "wal_conninfo",
        "wal_retention_policy",
        "wal_streaming_conninfo",
        "wals_directory",
        "worm_mode",
        "xlogdb_directory",
    ]

    BARMAN_KEYS = [
        "archiver",
        "archiver_batch_size",
        "autogenerate_manifest",
        "aws_await_snapshots_timeout",
        "aws_snapshot_lock_mode",
        "aws_snapshot_lock_duration",
        "aws_snapshot_lock_cool_off_period",
        "aws_snapshot_lock_expiration_date",
        "aws_profile",
        "aws_region",
        "azure_credential",
        "azure_resource_group",
        "azure_subscription_id",
        "backup_compression",
        "backup_compression_format",
        "backup_compression_level",
        "backup_compression_location",
        "backup_compression_workers",
        "backup_method",
        "backup_options",
        "bandwidth_limit",
        "basebackup_retry_sleep",
        "basebackup_retry_times",
        "check_timeout",
        "compression",
        "compression_level",
        "configuration_files_directory",
        "create_slot",
        "custom_compression_filter",
        "custom_decompression_filter",
        "custom_compression_magic",
        "encryption",
        "encryption_key_id",
        "encryption_passphrase_command",
        "forward_config_path",
        "gcp_project",
        "immediate_checkpoint",
        "keepalive_internval",
        "last_backup_maximum_age",
        "last_backup_minimum_size",
        "last_wal_maximum_age",
        "local_staging_path",
        "max_incoming_wals_queue",
        "minimum_redundancy",
        "network_compression",
        "parallel_jobs",
        "parallel_jobs_start_batch_period",
        "parallel_jobs_start_batch_size",
        "path_prefix",
        "post_archive_retry_script",
        "post_archive_script",
        "post_backup_retry_script",
        "post_backup_script",
        "post_delete_script",
        "post_delete_retry_script",
        "post_recovery_retry_script",
        "post_recovery_script",
        "post_wal_delete_script",
        "post_wal_delete_retry_script",
        "pre_archive_retry_script",
        "pre_archive_script",
        "pre_backup_retry_script",
        "pre_backup_script",
        "pre_delete_script",
        "pre_delete_retry_script",
        "pre_recovery_retry_script",
        "pre_recovery_script",
        "pre_wal_delete_script",
        "pre_wal_delete_retry_script",
        "primary_ssh_command",
        "recovery_options",
        "recovery_staging_path",
        "retention_policy",
        "retention_policy_mode",
        "reuse_backup",
        "slot_name",
        "snapshot_gcp_project",  # Deprecated, replaced by gcp_project
        "snapshot_provider",
        "streaming_archiver",
        "streaming_archiver_batch_size",
        "streaming_archiver_name",
        "streaming_backup_name",
        "tablespace_bandwidth_limit",
        "wal_retention_policy",
        "worm_mode",
        "xlogdb_directory",
    ]

    DEFAULTS = {
        "active": "true",
        "archiver": "off",
        "archiver_batch_size": "0",
        "autogenerate_manifest": "false",
        "aws_await_snapshots_timeout": "3600",
        "backup_directory": "%(barman_home)s/%(name)s",
        "backup_method": "rsync",
        "backup_options": "",
        "basebackup_retry_sleep": "30",
        "basebackup_retry_times": "0",
        "basebackups_directory": "%(backup_directory)s/base",
        "check_timeout": "30",
        "cluster": "%(name)s",
        "compression_level": "medium",
        "disabled": "false",
        "encryption": "none",
        "errors_directory": "%(backup_directory)s/errors",
        "forward_config_path": "false",
        "immediate_checkpoint": "false",
        "incoming_wals_directory": "%(backup_directory)s/incoming",
        "keepalive_interval": "60",
        "minimum_redundancy": "0",
        "network_compression": "false",
        "parallel_jobs": "1",
        "parallel_jobs_start_batch_period": "1",
        "parallel_jobs_start_batch_size": "10",
        "primary_checkpoint_timeout": "0",
        "recovery_options": "",
        "create_slot": "manual",
        "retention_policy_mode": "auto",
        "streaming_archiver": "off",
        "streaming_archiver_batch_size": "0",
        "streaming_archiver_name": "barman_receive_wal",
        "streaming_backup_name": "barman_streaming_backup",
        "streaming_conninfo": "%(conninfo)s",
        "streaming_wals_directory": "%(backup_directory)s/streaming",
        "wal_retention_policy": "main",
        "wals_directory": "%(backup_directory)s/wals",
        "worm_mode": "off",
        "xlogdb_directory": "%(wals_directory)s",
    }

    FIXED = [
        "disabled",
    ]

    PARSERS = {
        "active": parse_boolean,
        "archiver": parse_boolean,
        "archiver_batch_size": int,
        "autogenerate_manifest": parse_boolean,
        "aws_await_snapshots_timeout": int,
        "aws_snapshot_lock_duration": int,
        "aws_snapshot_lock_cool_off_period": int,
        "backup_compression": parse_backup_compression,
        "backup_compression_format": parse_backup_compression_format,
        "backup_compression_level": int,
        "backup_compression_location": parse_backup_compression_location,
        "backup_compression_workers": int,
        "backup_method": parse_backup_method,
        "backup_options": BackupOptions,
        "basebackup_retry_sleep": int,
        "basebackup_retry_times": int,
        "check_timeout": int,
        "compression": parse_compression,
        "compression_level": parse_compression_level,
        "disabled": parse_boolean,
        "encryption": parse_encryption,
        "forward_config_path": parse_boolean,
        "keepalive_interval": int,
        "immediate_checkpoint": parse_boolean,
        "last_backup_maximum_age": parse_time_interval,
        "last_backup_minimum_size": parse_si_suffix,
        "last_wal_maximum_age": parse_time_interval,
        "local_staging_path": parse_staging_path,
        "max_incoming_wals_queue": int,
        "network_compression": parse_boolean,
        "parallel_jobs": int,
        "parallel_jobs_start_batch_period": int,
        "parallel_jobs_start_batch_size": int,
        "primary_checkpoint_timeout": int,
        "recovery_options": RecoveryOptions,
        "recovery_staging_path": parse_staging_path,
        "create_slot": parse_create_slot,
        "reuse_backup": parse_reuse_backup,
        "snapshot_disks": parse_snapshot_disks,
        "streaming_archiver": parse_boolean,
        "streaming_archiver_batch_size": int,
        "slot_name": parse_slot_name,
        "worm_mode": parse_boolean,
    }

    def __init__(self, config, name):
        self.msg_list = []
        self.config = config
        self.name = name
        self.barman_home = config.barman_home
        self.barman_lock_directory = config.barman_lock_directory
        self.lock_directory_cleanup = config.lock_directory_cleanup
        self.config_changes_queue = config.config_changes_queue
        config.validate_server_config(self.name)
        for key in ServerConfig.KEYS:
            value = None
            # Skip parameters that cannot be configured by users
            if key not in ServerConfig.FIXED:
                # Get the setting from the [name] section of config file
                # A literal None value is converted to an empty string
                new_value = config.get(name, key, self.__dict__, none_value="")
                source = "[%s] section" % name
                value = self.invoke_parser(key, source, value, new_value)
                # If the setting isn't present in [name] section of config file
                # check if it has to be inherited from the [barman] section
                if value is None and key in ServerConfig.BARMAN_KEYS:
                    new_value = config.get("barman", key, self.__dict__, none_value="")
                    source = "[barman] section"
                    value = self.invoke_parser(key, source, value, new_value)
            # If the setting isn't present in [name] section of config file
            # and is not inherited from global section use its default
            # (if present)
            if value is None and key in ServerConfig.DEFAULTS:
                new_value = ServerConfig.DEFAULTS[key] % self.__dict__
                source = "DEFAULTS"
                value = self.invoke_parser(key, source, value, new_value)
            # An empty string is a None value (bypassing inheritance
            # from global configuration)
            if value is not None and value == "" or value == "None":
                value = None
            setattr(self, key, value)
        self._active_model_file = os.path.join(
            self.backup_directory, ".active-model.auto"
        )
        self.active_model = None

    def apply_model(self, model, from_cli=False):
        """Apply config from a model named *name*.

        :param model: the model to be applied.
        :param from_cli: ``True`` if this function has been called by the user
            through a command, e.g. ``barman-config-switch``. ``False`` if it
            has been called internally by Barman. ``INFO`` messages are written
            in the first case, ``DEBUG`` messages in the second case.
        """
        writer_func = getattr(output, "info" if from_cli else "debug")

        if self.cluster != model.cluster:
            output.error(
                "Model '%s' has 'cluster=%s', which is not compatible with "
                "'cluster=%s' from server '%s'"
                % (
                    model.name,
                    model.cluster,
                    self.cluster,
                    self.name,
                )
            )

            return

        # No need to apply the same model twice
        if self.active_model is not None and model.name == self.active_model.name:
            writer_func(
                "Model '%s' is already active for server '%s', "
                "skipping..." % (model.name, self.name)
            )

            return

        writer_func("Applying model '%s' to server '%s'" % (model.name, self.name))

        for option, value in model.get_override_options():
            old_value = getattr(self, option)

            if old_value != value:
                writer_func(
                    "Changing value of option '%s' for server '%s' "
                    "from '%s' to '%s' through the model '%s'"
                    % (option, self.name, old_value, value, model.name)
                )

                setattr(self, option, value)

        if from_cli:
            # If the request came from the CLI, like from 'barman config-switch'
            # then we need to persist the change to disk. On the other hand, if
            # Barman is calling this method on its own, that's because it previously
            # already read the active model from that file, so there is no need
            # to persist it again to disk
            with open(self._active_model_file, "w") as f:
                f.write(model.name)

        self.active_model = model

    def reset_model(self):
        """Reset the active model for this server, if any."""
        output.info("Resetting the active model for the server %s" % (self.name))

        if os.path.isfile(self._active_model_file):
            os.remove(self._active_model_file)

        self.active_model = None

    def to_json(self, with_source=False):
        """
        Return an equivalent dictionary that can be encoded in json

        :param with_source: if we should include the source file that provides
            the effective value for each configuration option.

        :return: a dictionary. The structure depends on *with_source* argument:

            * If ``False``: key is the option name, value is its value;
            * If ``True``: key is the option name, value is a dict with a
              couple keys:

              * ``value``: the value of the option;
              * ``source``: the file which provides the effective value, if
                the option has been configured by the user, otherwise ``None``.
        """
        json_dict = dict(vars(self))

        # remove references that should not go inside the
        # `servers -> SERVER -> config` key in the barman diagnose output
        # ideally we should change this later so we only consider configuration
        # options, as things like `msg_list` are going to the `config` key,
        # i.e. we might be interested in considering only `ServerConfig.KEYS`
        # here instead of `vars(self)`
        for key in ["config", "_active_model_file", "active_model"]:
            del json_dict[key]

        # options that are override by the model
        override_options = set()

        if self.active_model:
            override_options = {
                option for option, _ in self.active_model.get_override_options()
            }

        if with_source:
            for option, value in json_dict.items():
                name = self.name

                if option in override_options:
                    name = self.active_model.name

                json_dict[option] = {
                    "value": value,
                    "source": self.config.get_config_source(name, option),
                }

        return json_dict

    def get_bwlimit(self, tablespace=None):
        """
        Return the configured bandwidth limit for the provided tablespace

        If tablespace is None, it returns the global bandwidth limit

        :param barman.infofile.Tablespace tablespace: the tablespace to copy
        :rtype: str
        """
        # Default to global bandwidth limit
        bwlimit = self.bandwidth_limit

        if tablespace:
            # A tablespace can be copied using a per-tablespace bwlimit
            tbl_bw_limit = self.tablespace_bandwidth_limit
            if tbl_bw_limit and tablespace.name in tbl_bw_limit:
                bwlimit = tbl_bw_limit[tablespace.name]

        return bwlimit

    def update_msg_list_and_disable_server(self, msg_list):
        """
        Will take care of upgrading msg_list
        :param msg_list: str|list can be either a string or a list of strings
        """
        if not msg_list:
            return
        if type(msg_list) is not list:
            msg_list = [msg_list]

        self.msg_list.extend(msg_list)
        self.disabled = True

    def get_wal_conninfo(self):
        """
        Return WAL-specific conninfo strings for this server.

        Returns the value of ``wal_streaming_conninfo`` and ``wal_conninfo`` if they
        are set in the configuration. If ``wal_conninfo`` is unset then it will
        be given the value of ``wal_streaming_conninfo``. If ``wal_streaming_conninfo``
        is unset then fall back to ``streaming_conninfo`` and ``conninfo``.

        :rtype: (str,str)
        :return: Tuple consisting of the ``wal_streaming_conninfo`` and
            ``wal_conninfo``.
        """
        # If `wal_streaming_conninfo` is not set, fall back to `streaming_conninfo`
        wal_streaming_conninfo = self.wal_streaming_conninfo or self.streaming_conninfo

        # If `wal_conninfo` is not set, fall back to `wal_streaming_conninfo`. If
        # `wal_streaming_conninfo` is not set, fall back to `conninfo`.
        if self.wal_conninfo is not None:
            wal_conninfo = self.wal_conninfo
        elif self.wal_streaming_conninfo is not None:
            wal_conninfo = self.wal_streaming_conninfo
        else:
            wal_conninfo = self.conninfo
        return wal_streaming_conninfo, wal_conninfo


class ModelConfig(BaseConfig):
    """
    This class represents the configuration for a specific model of a server.

    :cvar KEYS: list of configuration options that are allowed in a model.
    :cvar REQUIRED_KEYS: list of configuration options that must always be set
        when defining a configuration model.
    :cvar PARSERS: mapping of parsers for the configuration options, if they
        need special handling.
    """

    # Keys from ServerConfig which are not allowed in a configuration model.
    # They are mostly related with paths or hooks, which are not expected to
    # be changed at all with a model.
    _KEYS_BLACKLIST = {
        # Path related options
        "backup_directory",
        "basebackups_directory",
        "errors_directory",
        "incoming_wals_directory",
        "streaming_wals_directory",
        "wals_directory",
        # Although xlogdb_directory could be set with the same value for two
        # servers (the xlog.db is now called SERVER-xlog.db, avoiding conflicts)
        # we exclude it from models to follow the same pattern defined for all
        # the path settings.
        "xlogdb_directory",
        # Hook related options
        "post_archive_retry_script",
        "post_archive_script",
        "post_backup_retry_script",
        "post_backup_script",
        "post_delete_script",
        "post_delete_retry_script",
        "post_recovery_retry_script",
        "post_recovery_script",
        "post_wal_delete_script",
        "post_wal_delete_retry_script",
        "pre_archive_retry_script",
        "pre_archive_script",
        "pre_backup_retry_script",
        "pre_backup_script",
        "pre_delete_script",
        "pre_delete_retry_script",
        "pre_recovery_retry_script",
        "pre_recovery_script",
        "pre_wal_delete_script",
        "pre_wal_delete_retry_script",
    }

    KEYS = list((set(ServerConfig.KEYS) | {"model"}) - _KEYS_BLACKLIST)

    REQUIRED_KEYS = [
        "cluster",
        "model",
    ]

    PARSERS = deepcopy(ServerConfig.PARSERS)
    PARSERS.update({"model": parse_boolean})
    for key in _KEYS_BLACKLIST:
        PARSERS.pop(key, None)

    def __init__(self, config, name):
        self.config = config
        self.name = name
        config.validate_model_config(self.name)
        for key in ModelConfig.KEYS:
            value = None
            # Get the setting from the [name] section of config file
            # A literal None value is converted to an empty string
            new_value = config.get(name, key, self.__dict__, none_value="")
            source = "[%s] section" % name
            value = self.invoke_parser(key, source, value, new_value)
            # An empty string is a None value
            if value is not None and value == "" or value == "None":
                value = None
            setattr(self, key, value)

    def get_override_options(self):
        """
        Get a list of options which values in the server should be override.

        :yield: tuples os option name and value which should override the value
            specified in the server with the value specified in the model.
        """
        for option in set(self.KEYS) - set(self.REQUIRED_KEYS):
            value = getattr(self, option)

            if value is not None:
                yield option, value

    def to_json(self, with_source=False):
        """
        Return an equivalent dictionary that can be encoded in json

        :param with_source: if we should include the source file that provides
            the effective value for each configuration option.

        :return: a dictionary. The structure depends on *with_source* argument:

            * If ``False``: key is the option name, value is its value;
            * If ``True``: key is the option name, value is a dict with a
              couple keys:

              * ``value``: the value of the option;
              * ``source``: the file which provides the effective value, if
                the option has been configured by the user, otherwise ``None``.
        """
        json_dict = {}

        for option in self.KEYS:
            value = getattr(self, option)

            if with_source:
                value = {
                    "value": value,
                    "source": self.config.get_config_source(self.name, option),
                }

            json_dict[option] = value

        return json_dict


class ConfigMapping(ConfigParser):
    """Wrapper for :class:`ConfigParser`.

    Extend the facilities provided by a :class:`ConfigParser` object, and
    additionally keep track of the source file for each configuration option.

    This is very useful as Barman allows the user to provide configuration
    options spread over multiple files in the system, so one can know which
    file provides the value for a configuration option in use.

    .. note::
        When using this class you are expected to use :meth:`read_config`
        instead of any ``read*`` method exposed by :class:`ConfigParser`.
    """

    def __init__(self, *args, **kwargs):
        """Create a new instance of :class:`ConfigMapping`.

        .. note::
            We save *args* and *kwargs* so we can instantiate a temporary
            :class:`ConfigParser` with similar options on :meth:`read_config`.

        :param args: positional arguments to be passed down to
            :class:`ConfigParser`.

        :param kwargs: keyword arguments to be passed down to
            :class:`ConfigParser`.
        """
        self._args = args
        self._kwargs = kwargs
        self._mapping = {}
        super().__init__(*args, **kwargs)

    def read_config(self, filename):
        """
        Read and merge configuration options from *filename*.

        :param filename: path to a configuration file or its file descriptor
            in reading mode.

        :return: a list of file names which were able to be parsed, so we are
            compliant with the return value of :meth:`ConfigParser.read`. In
            practice the list will always contain at most one item. If
            *filename* is a descriptor with no ``name`` attribute, the
            corresponding entry in the list will be ``None``.
        """
        filenames = []
        tmp_parser = ConfigParser(*self._args, **self._kwargs)

        # A file descriptor
        if hasattr(filename, "read"):
            try:
                # Python 3.x
                tmp_parser.read_file(filename)
            except AttributeError:
                # Python 2.x
                tmp_parser.readfp(filename)
            if hasattr(filename, "name"):
                filenames.append(filename.name)
            else:
                filenames.append(None)
        # A file path
        else:
            for name in tmp_parser.read(filename):
                filenames.append(name)

        # Merge configuration options from the temporary parser into the global
        # parser, and update the mapping of options
        for section in tmp_parser.sections():
            if not self.has_section(section):
                self.add_section(section)
                self._mapping[section] = {}

            for option, value in tmp_parser[section].items():
                self.set(section, option, value)
                self._mapping[section][option] = filenames[0]

        return filenames

    def get_config_source(self, section, option):
        """Get the source INI file from which a config value comes from.

        :param section: the section of the configuration option.
        :param option: the name of the configuraion option.

        :return: the file that provides the effective value for *section* ->
            *option*. If no such configuration exists in the mapping, we assume
            it has a default value and return the ``default`` string.
        """
        source = self._mapping.get(section, {}).get(option, None)

        # The config was not defined on the server section, but maybe under
        # `barman` section?
        if source is None and section != "barman":
            source = self._mapping.get("barman", {}).get(option, None)

        return source or "default"


class Config(object):
    """This class represents the barman configuration.

    Default configuration files are /etc/barman.conf,
    /etc/barman/barman.conf
    and ~/.barman.conf for a per-user configuration
    """

    CONFIG_FILES = [
        "~/.barman.conf",
        "/etc/barman.conf",
        "/etc/barman/barman.conf",
    ]

    _QUOTE_RE = re.compile(r"""^(["'])(.*)\1$""")

    def __init__(self, filename=None):
        #  In Python 3 ConfigParser has changed to be strict by default.
        #  Barman wants to preserve the Python 2 behavior, so we are
        #  explicitly building it passing strict=False.
        try:
            # Python 3.x
            self._config = ConfigMapping(strict=False)
        except TypeError:
            # Python 2.x
            self._config = ConfigMapping()
        if filename:
            # If it is a file descriptor
            if hasattr(filename, "read"):
                self._config.read_config(filename)
            # If it is a path
            else:
                # check for the existence of the user defined file
                if not os.path.exists(filename):
                    sys.exit("Configuration file '%s' does not exist" % filename)
                self._config.read_config(os.path.expanduser(filename))
        else:
            # Check for the presence of configuration files
            # inside default directories
            for path in self.CONFIG_FILES:
                full_path = os.path.expanduser(path)
                if os.path.exists(full_path) and full_path in self._config.read_config(
                    full_path
                ):
                    filename = full_path
                    break
            else:
                sys.exit(
                    "Could not find any configuration file at "
                    "default locations.\n"
                    "Check Barman's documentation for more help."
                )
        self.config_file = filename
        self._servers = None
        self._models = None
        self.servers_msg_list = []
        self._parse_global_config()

    def get(self, section, option, defaults=None, none_value=None):
        """Method to get the value from a given section from
        Barman configuration
        """
        if not self._config.has_section(section):
            return None
        try:
            value = self._config.get(section, option, raw=False, vars=defaults)
            if value == "None":
                value = none_value
            if value is not None:
                value = self._QUOTE_RE.sub(lambda m: m.group(2), value)
            return value
        except NoOptionError:
            return None

    def get_config_source(self, section, option):
        """Get the source INI file from which a config value comes from.

        .. seealso:
            See :meth:`ConfigMapping.get_config_source` for details on the
            interface as this method is just a wrapper for that.
        """
        return self._config.get_config_source(section, option)

    def _parse_global_config(self):
        """
        This method parses the global [barman] section
        """
        self.barman_home = self.get("barman", "barman_home")
        self.config_changes_queue = (
            self.get("barman", "config_changes_queue")
            or "%s/cfg_changes.queue" % self.barman_home
        )
        self.barman_lock_directory = (
            self.get("barman", "barman_lock_directory") or self.barman_home
        )
        self.lock_directory_cleanup = parse_boolean(
            self.get("barman", "lock_directory_cleanup") or DEFAULT_CLEANUP
        )
        self.user = self.get("barman", "barman_user") or DEFAULT_USER
        self.log_file = self.get("barman", "log_file")
        self.log_format = self.get("barman", "log_format") or DEFAULT_LOG_FORMAT
        self.log_level = self.get("barman", "log_level") or DEFAULT_LOG_LEVEL
        # save the raw barman section to be compared later in
        # _is_global_config_changed() method
        self._global_config = set(self._config.items("barman"))

    def global_config_to_json(self, with_source=False):
        """
        Return an equivalent dictionary that can be encoded in json

        :param with_source: if we should include the source file that provides
            the effective value for each configuration option.

        :return: a dictionary. The structure depends on *with_source* argument:

            * If ``False``: key is the option name, value is its value;
            * If ``True``: key is the option name, value is a dict with a
              couple keys:

              * ``value``: the value of the option;
              * ``source``: the file which provides the effective value, if
                the option has been configured by the user, otherwise ``None``.
        """
        json_dict = dict(self._global_config)

        if with_source:
            for option, value in json_dict.items():
                json_dict[option] = {
                    "value": value,
                    "source": self.get_config_source("barman", option),
                }

        return json_dict

    def _is_global_config_changed(self):
        """Return true if something has changed in global configuration"""
        return self._global_config != set(self._config.items("barman"))

    def load_configuration_files_directory(self):
        """
        Read the "configuration_files_directory" option and load all the
        configuration files with the .conf suffix that lie in that folder
        """

        config_files_directory = self.get("barman", "configuration_files_directory")

        if not config_files_directory:
            return

        if not os.path.isdir(os.path.expanduser(config_files_directory)):
            _logger.warn(
                'Ignoring the "configuration_files_directory" option as "%s" '
                "is not a directory",
                config_files_directory,
            )
            return

        for cfile in sorted(
            iglob(os.path.join(os.path.expanduser(config_files_directory), "*.conf"))
        ):
            self.load_config_file(cfile)

    def load_config_file(self, cfile):
        filename = os.path.basename(cfile)
        if os.path.exists(cfile):
            if os.path.isfile(cfile):
                # Load a file
                _logger.debug("Including configuration file: %s", filename)
                self._config.read_config(cfile)
                if self._is_global_config_changed():
                    msg = (
                        "the configuration file %s contains a not empty [barman] section"
                        % filename
                    )
                    _logger.fatal(msg)
                    raise SystemExit("FATAL: %s" % msg)
            else:
                # Add an warning message that a file has been discarded
                _logger.warn("Discarding configuration file: %s (not a file)", filename)
        else:
            # Add an warning message that a file has been discarded
            _logger.warn("Discarding configuration file: %s (not found)", filename)

    def _is_model(self, name):
        """
        Check if section *name* is a model.

        :param name: name of the config section.

        :return: ``True`` if section *name* is a model, ``False`` otherwise.

        :raises:
            :exc:`ValueError`: re-raised if thrown by :func:`parse_boolean`.
        """
        try:
            value = self._config.get(name, "model")
        except NoOptionError:
            return False

        try:
            return parse_boolean(value)
        except ValueError as exc:
            raise exc

    def _populate_servers_and_models(self):
        """
        Populate server list and model list from configuration file

        Also check for paths errors in configuration.
        If two or more paths overlap in
        a single server, that server is disabled.
        If two or more directory paths overlap between
        different servers an error is raised.
        """

        # Populate servers
        if self._servers is not None and self._models is not None:
            return
        self._servers = {}
        self._models = {}
        # Cycle all the available configurations sections
        for section in self._config.sections():
            if section == "barman":
                # skip global settings
                continue
            # Exit if the section has a reserved name
            if section in FORBIDDEN_SERVER_NAMES:
                msg = (
                    "the reserved word '%s' is not allowed as server name."
                    "Please rename it." % section
                )
                _logger.fatal(msg)
                raise SystemExit("FATAL: %s" % msg)
            if self._is_model(section):
                # Create a ModelConfig object
                self._models[section] = ModelConfig(self, section)
            else:
                # Create a ServerConfig object
                self._servers[section] = ServerConfig(self, section)

        # Check for conflicting paths in Barman configuration
        self._check_conflicting_paths()

        # Apply models if the hidden files say so
        self._apply_models()

    def _check_conflicting_paths(self):
        """
        Look for conflicting paths intra-server and inter-server
        """

        # All paths in configuration
        servers_paths = {}
        # Global errors list
        self.servers_msg_list = []

        # Cycle all the available configurations sections
        for section in sorted(self.server_names()):
            # Paths map
            section_conf = self._servers[section]
            config_paths = {
                "backup_directory": section_conf.backup_directory,
                "basebackups_directory": section_conf.basebackups_directory,
                "errors_directory": section_conf.errors_directory,
                "incoming_wals_directory": section_conf.incoming_wals_directory,
                "streaming_wals_directory": section_conf.streaming_wals_directory,
                "wals_directory": section_conf.wals_directory,
            }

            # Check for path errors
            for label, path in sorted(config_paths.items()):
                # If the path does not conflict with the others, add it to the
                # paths map
                real_path = os.path.realpath(path)
                if real_path not in servers_paths:
                    servers_paths[real_path] = PathConflict(label, section)
                else:
                    if section == servers_paths[real_path].server:
                        # Internal path error.
                        # Insert the error message into the server.msg_list
                        if real_path == path:
                            self._servers[section].msg_list.append(
                                "Conflicting path: %s=%s conflicts with "
                                "'%s' for server '%s'"
                                % (
                                    label,
                                    path,
                                    servers_paths[real_path].label,
                                    servers_paths[real_path].server,
                                )
                            )
                        else:
                            # Symbolic link
                            self._servers[section].msg_list.append(
                                "Conflicting path: %s=%s (symlink to: %s) "
                                "conflicts with '%s' for server '%s'"
                                % (
                                    label,
                                    path,
                                    real_path,
                                    servers_paths[real_path].label,
                                    servers_paths[real_path].server,
                                )
                            )
                        # Disable the server
                        self._servers[section].disabled = True
                    else:
                        # Global path error.
                        # Insert the error message into the global msg_list
                        if real_path == path:
                            self.servers_msg_list.append(
                                "Conflicting path: "
                                "%s=%s for server '%s' conflicts with "
                                "'%s' for server '%s'"
                                % (
                                    label,
                                    path,
                                    section,
                                    servers_paths[real_path].label,
                                    servers_paths[real_path].server,
                                )
                            )
                        else:
                            # Symbolic link
                            self.servers_msg_list.append(
                                "Conflicting path: "
                                "%s=%s (symlink to: %s) for server '%s' "
                                "conflicts with '%s' for server '%s'"
                                % (
                                    label,
                                    path,
                                    real_path,
                                    section,
                                    servers_paths[real_path].label,
                                    servers_paths[real_path].server,
                                )
                            )

    def _apply_models(self):
        """
        For each Barman server, check for a pre-existing active model.

        If a hidden file with a pre-existing active model file exists, apply
        that on top of the server configuration.
        """
        for server in self.servers():
            active_model = None

            try:
                with open(server._active_model_file, "r") as f:
                    active_model = f.read().strip()
            except FileNotFoundError:
                # If a file does not exist, even if the server has models
                # defined, none of them has ever been applied
                continue

            if active_model.strip() == "":
                # Try to protect itself from a bogus file
                continue

            model = self.get_model(active_model)

            if model is None:
                # The model used to exist, but it's no longer avaialble for
                # some reason
                server.update_msg_list_and_disable_server(
                    [
                        "Model '%s' is set as the active model for the server "
                        "'%s' but the model does not exist."
                        % (active_model, server.name)
                    ]
                )

                continue

            server.apply_model(model)

    def server_names(self):
        """This method returns a list of server names"""
        self._populate_servers_and_models()
        return self._servers.keys()

    def servers(self):
        """This method returns a list of server parameters"""
        self._populate_servers_and_models()
        return self._servers.values()

    def get_server(self, name):
        """
        Get the configuration of the specified server

        :param str name: the server name
        """
        self._populate_servers_and_models()
        return self._servers.get(name, None)

    def model_names(self):
        """Get a list of model names.

        :return: a :class:`list` of configured model names.
        """
        self._populate_servers_and_models()
        return self._models.keys()

    def models(self):
        """Get a list of models.

        :return: a :class:`list` of configured :class:`ModelConfig` objects.
        """
        self._populate_servers_and_models()
        return self._models.values()

    def get_model(self, name):
        """Get the configuration of the specified model.

        :param name: the model name.

        :return: a :class:`ModelConfig` if the model exists, otherwise
            ``None``.
        """
        self._populate_servers_and_models()
        return self._models.get(name, None)

    def validate_global_config(self):
        """
        Validate global configuration parameters
        """
        # Check for the existence of unexpected parameters in the
        # global section of the configuration file
        required_keys = [
            "barman_home",
        ]
        self._detect_missing_keys(self._global_config, required_keys, "barman")

        keys = [
            "barman_home",
            "barman_lock_directory",
            "barman_user",
            "lock_directory_cleanup",
            "config_changes_queue",
            "log_file",
            "log_level",
            "configuration_files_directory",
        ]
        keys.extend(ServerConfig.KEYS)
        self._validate_with_keys(self._global_config, keys, "barman")

    def validate_server_config(self, server):
        """
        Validate configuration parameters for a specified server

        :param str server: the server name
        """
        # Check for the existence of unexpected parameters in the
        # server section of the configuration file
        self._validate_with_keys(self._config.items(server), ServerConfig.KEYS, server)

    def validate_model_config(self, model):
        """
        Validate configuration parameters for a specified model.

        :param model: the model name.
        """
        # Check for the existence of unexpected parameters in the
        # model section of the configuration file
        self._validate_with_keys(self._config.items(model), ModelConfig.KEYS, model)
        # Check for keys that are missing, but which are required
        self._detect_missing_keys(
            self._config.items(model), ModelConfig.REQUIRED_KEYS, model
        )

    @staticmethod
    def _detect_missing_keys(config_items, required_keys, section):
        """
        Check config for any missing required keys

        :param config_items: list of tuples containing provided parameters
            along with their values
        :param required_keys: list of required keys
        :param section: source section (for error reporting)
        """
        missing_key_detected = False

        config_keys = [item[0] for item in config_items]
        for req_key in required_keys:
            # if a required key is not found, then print an error
            if req_key not in config_keys:
                output.error(
                    'Parameter "%s" is required in [%s] section.' % (req_key, section),
                )
                missing_key_detected = True
        if missing_key_detected:
            raise SystemExit(
                "Your configuration is missing required parameters. Exiting."
            )

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
                output.warning(
                    'Invalid configuration option "%s" in [%s] ' "section.",
                    name,
                    section,
                )


class BaseChange:
    """
    Base class for change objects.

    Provides methods for equality comparison, hashing, and conversion
    to tuple and dictionary.
    """

    _fields = []

    def __eq__(self, other):
        """
        Equality support.

        :param other: other object to compare this one against.
        """
        if isinstance(other, self.__class__):
            return self.as_tuple() == other.as_tuple()
        return False

    def __hash__(self):
        """
        Hash/set support.

        :return: a hash of the tuple created though :meth:`as_tuple`.
        """
        return hash(self.as_tuple())

    def as_tuple(self) -> tuple:
        """
        Convert to a tuple, ordered as :attr:`_fields`.

        :return: tuple of values for :attr:`_fields`.
        """
        return tuple(vars(self)[k] for k in self._fields)

    def as_dict(self):
        """
        Convert to a dictionary, using :attr:`_fields` as keys.

        :return: a dictionary where keys are taken from :attr:`_fields` and values are the corresponding values for those fields.
        """
        return {k: vars(self)[k] for k in self._fields}


class ConfigChange(BaseChange):
    """
    Represents a configuration change received.

    :ivar key str: The key of the configuration change.
    :ivar value str: The value of the configuration change.
    :ivar config_file Optional[str]: The configuration file associated with the change, or ``None``.
    """

    _fields = ["key", "value", "config_file"]

    def __init__(self, key, value, config_file=None):
        """
        Initialize a :class:`ConfigChange` object.

        :param key str: the configuration setting to be changed.
        :param value str: the new configuration value.
        :param config_file Optional[str]: configuration file associated with the change, if any, or ``None``.
        """
        self.key = key
        self.value = value
        self.config_file = config_file

    @classmethod
    def from_dict(cls, obj):
        """
        Factory method for creating :class:`ConfigChange` objects from a dictionary.

        :param obj: Dictionary representing the configuration change.
        :type obj: :class:`dict`
        :return: Configuration change object.
        :rtype: :class:`ConfigChange`
        :raises:
            :exc:`ValueError`: If the dictionary is malformed.
        """
        if set(obj.keys()) == set(cls._fields):
            return cls(**obj)
        raise ValueError("Malformed configuration change serialization: %r" % obj)


class ConfigChangeSet(BaseChange):
    """Represents a set of :class:`ConfigChange` for a given configuration section.

    :ivar section str: name of the configuration section related with the changes.
    :ivar changes_set List[:class:`ConfigChange`]: list of configuration changes to be applied to the section.
    """

    _fields = ["section", "changes_set"]

    def __init__(self, section, changes_set=None):
        """Initialize a new :class:`ConfigChangeSet` object.

        :param section str: name of the configuration section related with the changes.
        :param changes_set List[ConfigChange]: list of configuration changes to be applied to the *section*.
        """
        self.section = section
        self.changes_set = changes_set
        if self.changes_set is None:
            self.changes_set = []

    @classmethod
    def from_dict(cls, obj):
        """
        Factory for configuration change objects.

        Generates configuration change objects starting from a dictionary with
        the same fields.

        .. note::
            Handles both :class:`ConfigChange` and :class:`ConfigChangeSet` mapping.

        :param obj: Dictionary representing the configuration changes set.
        :type obj: :class:`dict`
        :return: Configuration set of changes.
        :rtype: :class:`ConfigChangeSet`
        :raises:
            :exc:`ValueError`: If the dictionary is malformed.
        """
        if set(obj.keys()) == set(cls._fields):
            if len(obj["changes_set"]) > 0 and not isinstance(
                obj["changes_set"][0], ConfigChange
            ):
                obj["changes_set"] = [
                    ConfigChange.from_dict(c) for c in obj["changes_set"]
                ]
            return cls(**obj)
        if set(obj.keys()) == set(ConfigChange._fields):
            return ConfigChange(**obj)
        raise ValueError("Malformed configuration change serialization: %r" % obj)


class ConfigChangesQueue:
    """
    Wraps the management of the config changes queue.

    The :class:`ConfigChangesQueue` class provides methods to read, write, and manipulate
    a queue of configuration changes. It is designed to be used as a context manager
    to ensure proper opening and closing of the queue file.

    Once instantiated the queue can be accessed using the :attr:`queue` property.
    """

    def __init__(self, queue_file):
        """
        Initialize the :class:`ConfigChangesQueue` object.

        :param queue_file str: file where to persist the queue of changes to be processed.
        """
        self.queue_file = queue_file
        self._queue = None
        self.open()

    @staticmethod
    def read_file(path) -> List[ConfigChangeSet]:
        """
        Reads a json file containing a list of configuration changes.

        :return: the list of :class:`ConfigChangeSet` to be applied to Barman configuration sections.
        """
        try:
            with open(path, "r") as queue_file:
                # Read the queue if exists
                return json.load(queue_file, object_hook=ConfigChangeSet.from_dict)
        except FileNotFoundError:
            return []
        except json.JSONDecodeError:
            output.warning(
                "Malformed or empty configuration change queue: %s" % queue_file.name
            )
            return []

    def __enter__(self):
        """
        Enter method for context manager.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Closes the resource when exiting the context manager.
        """
        self.close()

    @property
    def queue(self):
        """
        Returns the queue object.

        If the queue object is not yet initialized, it will be opened before returning.

        :return: the queue object.
        """
        if self._queue is None:
            self.open()

        return self._queue

    def open(self):
        """Open and parse the :attr:`queue_file` into :attr:`_queue`."""
        self._queue = self.read_file(self.queue_file)

    def close(self):
        """Write the new content and close the :attr:`queue_file`."""
        with open(self.queue_file + ".tmp", "w") as queue_file:
            # Dump the configuration change list into the queue file
            json.dump(self._queue, queue_file, cls=ConfigChangeSetEncoder, indent=2)

        # Juggle with the queue files to ensure consistency of
        # the queue even if Shelver is interrupted abruptly
        old_file_name = self.queue_file + ".old"
        try:
            os.rename(self.queue_file, old_file_name)
        except FileNotFoundError:
            old_file_name = None
        os.rename(self.queue_file + ".tmp", self.queue_file)
        if old_file_name:
            os.remove(old_file_name)
        self._queue = None


class ConfigChangesProcessor:
    """
    The class is responsible for processing the config changes to
    apply to the barman config
    """

    def __init__(self, config):
        """Initialize a new :class:`ConfigChangesProcessor` object,

        :param config Config: the Barman configuration.
        """
        self.config = config
        self.applied_changes = []

    def receive_config_changes(self, changes):
        """
        Process all the configuration *changes*.

        :param changes Dict[str, str]: each key is the name of a section to be updated, and the value is a dictionary of configuration options along with their values that should be updated in such section.
        """
        # Get all the available configuration change files in order
        changes_list = []
        for section in changes:
            original_section = deepcopy(section)
            section_name = None
            scope = section.pop("scope")

            if scope not in ["server", "model"]:
                output.warning(
                    "%r has been ignored because 'scope' is "
                    "invalid: '%s'. It should be either 'server' "
                    "or 'model'.",
                    original_section,
                    scope,
                )
                continue
            elif scope == "server":
                try:
                    section_name = section.pop("server_name")
                except KeyError:
                    output.warning(
                        "%r has been ignored because 'server_name' is missing.",
                        original_section,
                    )
                    continue
            elif scope == "model":
                try:
                    section_name = section.pop("model_name")
                except KeyError:
                    output.warning(
                        "%r has been ignored because 'model_name' is missing.",
                        original_section,
                    )
                    continue

            server_obj = self.config.get_server(section_name)
            model_obj = self.config.get_model(section_name)

            if scope == "server":
                # the section already exists as a model
                if model_obj is not None:
                    output.warning(
                        "%r has been ignored because '%s' is a model, not a server.",
                        original_section,
                        section_name,
                    )
                    continue
            elif scope == "model":
                # the section already exists as a server
                if server_obj is not None:
                    output.warning(
                        "%r has been ignored because '%s' is a server, not a model.",
                        original_section,
                        section_name,
                    )
                    continue

                # If the model does not exist yet in Barman
                if model_obj is None:
                    # 'model=on' is required for models, so force that if the
                    # user forgot 'model' or set it to something invalid
                    section["model"] = "on"

                    if "cluster" not in section:
                        output.warning(
                            "%r has been ignored because it is a "
                            "new model but 'cluster' is missing.",
                            original_section,
                        )
                        continue

            # Instantiate the ConfigChangeSet object
            chg_set = ConfigChangeSet(section=section_name)
            for json_cng in section:
                file_name = self.config._config.get_config_source(
                    section_name, json_cng
                )
                # if the configuration change overrides a default value
                # then the source file is ".barman.auto.conf"
                if file_name == "default":
                    file_name = os.path.expanduser(
                        "%s/.barman.auto.conf" % self.config.barman_home
                    )
                chg = None
                # Instantiate the configuration change object
                chg = ConfigChange(
                    json_cng,
                    section[json_cng],
                    file_name,
                )
                chg_set.changes_set.append(chg)
            changes_list.append(chg_set)

        # If there are no configuration change we've nothing to do here
        if len(changes_list) == 0:
            _logger.debug("No valid changes submitted")
            return

        # Extend the queue with the new changes
        with ConfigChangesQueue(self.config.config_changes_queue) as changes_queue:
            changes_queue.queue.extend(changes_list)

    def process_conf_changes_queue(self):
        """
        Process the configuration changes in the queue.

        This method iterates over the configuration changes in the queue and applies them one by one.
        If an error occurs while applying a change, it logs the error and raises an exception.

        :raises:
            :exc:`Exception`: If an error occurs while applying a change.

        """
        try:
            chgs_set = None
            with ConfigChangesQueue(self.config.config_changes_queue) as changes_queue:
                # Cycle and apply the configuration changes
                while len(changes_queue.queue) > 0:
                    chgs_set = changes_queue.queue[0]
                    try:
                        self.apply_change(chgs_set)
                    except Exception as e:
                        # Log that something went horribly wrong and re-raise
                        msg = "Unable to process a set of changes. Exiting."
                        output.error(msg)
                        _logger.debug(
                            "Error while processing %s. \nError: %s"
                            % (
                                json.dumps(
                                    chgs_set, cls=ConfigChangeSetEncoder, indent=2
                                ),
                                e,
                            ),
                        )
                        raise e

                    # Remove the configuration change once succeeded
                    changes_queue.queue.pop(0)
                    self.applied_changes.append(chgs_set)

        except Exception as err:
            _logger.error("Cannot execute %s: %s", chgs_set, err)

    def apply_change(self, changes):
        """
        Apply the given changes to the configuration files.

        :param changes List[ConfigChangeSet]: list of sections and their configuration options to be updated.
        """
        changed_files = dict()
        for chg in changes.changes_set:
            changed_files[chg.config_file] = utils.edit_config(
                chg.config_file,
                changes.section,
                chg.key,
                chg.value,
                changed_files.get(chg.config_file),
            )
            output.info(
                "Changing value of option '%s' for section '%s' "
                "from '%s' to '%s' through config-update."
                % (
                    chg.key,
                    changes.section,
                    self.config.get(changes.section, chg.key),
                    chg.value,
                )
            )
        for file, lines in changed_files.items():
            with open(file, "w") as cfg_file:
                cfg_file.writelines(lines)


class ConfigChangeSetEncoder(json.JSONEncoder):
    """
    JSON encoder for :class:`ConfigChange` and :class:`ConfigChangeSet` objects.
    """

    def default(self, obj):
        if isinstance(obj, (ConfigChange, ConfigChangeSet)):
            # Let the base class default method raise the TypeError
            return dict(obj.as_dict())
        return super().default(obj)


# easy raw config diagnostic with python -m
# noinspection PyProtectedMember
def _main():
    print("Active configuration settings:")
    r = Config()
    r.load_configuration_files_directory()
    for section in r._config.sections():
        print("Section: %s" % section)
        for option in r._config.options(section):
            print(
                "\t%s = %s (from %s)"
                % (option, r.get(section, option), r.get_config_source(section, option))
            )


if __name__ == "__main__":
    _main()
