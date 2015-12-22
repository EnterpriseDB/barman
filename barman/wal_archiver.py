# Copyright (C) 2011-2015 2ndQuadrant Italia Srl
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
# along with Barman.  If not, see <http://www.gnu.org/licenses/>

import logging
import os
from abc import ABCMeta, abstractmethod
from distutils.version import LooseVersion as Version
from glob import glob

import psycopg2

from barman import utils, xlog
from barman.command_wrappers import Command, CommandFailedException
from barman.infofile import WalFileInfo
from barman.postgres import PostgresConnectionError

_logger = logging.getLogger(__name__)


class WalArchiverBatch(list):
    def __init__(self, items, errors=None, skip=None):
        """
        A WalArchiverBatch is a list of WalFileInfo which has two extra
        attribute list:

        * errors: containing a list of unrecognized files
        * skip: containing a list of skipped files.

        :param items: iterable from which initialize the list
        :param errors: an optional list of unrecognized files
        :param skip: an optional list of skipped files
        """
        super(WalArchiverBatch, self).__init__(items)
        self.skip = []
        self.errors = []
        if skip is not None:
            self.skip = skip
        if errors is not None:
            self.errors = errors


class WalArchiver(object):
    """
    Base class for WAL archiver objects
    """

    __metaclass__ = ABCMeta

    def __init__(self, backup_manager):
        """
        Base class init method.

        :param backup_manager:
        :return:
        """
        self.backup_manager = backup_manager
        self.server = backup_manager.server
        self.config = backup_manager.config

    @abstractmethod
    def get_remote_status(self):
        """
        Execute basic checks
        """

    @abstractmethod
    def get_next_batch(self):
        """
        Return a WalArchiverBatch containing the WAL files to be archived.

        :rtype: WalArchiverBatch
        """


class FileWalArchiver(WalArchiver):
    """
    Manager of file-based WAL archiving operations (aka 'log shipping').
    """

    def __init__(self, backup_manager):

        super(FileWalArchiver, self).__init__(backup_manager)

    def get_remote_status(self):
        """
        Returns the status of the FileWalArchiver.

        This method does not raise exceptions in case of error,
        but set the missing values to None.

        :return dict[str, None]: component status variables
        """
        result = dict.fromkeys(
            ['archive_mode', 'archive_command'], None)
        postgres = self.backup_manager.server.postgres
        try:
            # Query the database for 'archive_mode' and 'archive_command'
            result['archive_mode'] = postgres.get_setting('archive_mode')
            result['archive_command'] = postgres.get_setting('archive_command')
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.warn("Error retrieving PostgreSQL status: %s", e)

        # Add pg_stat_archiver statistics if the view is supported
        pg_stat_archiver = postgres.get_archiver_stats()
        if pg_stat_archiver is not None:
            result.update(pg_stat_archiver)

        return result

    def get_next_batch(self):
        """
        Returns the next batch of WAL files that have been archived from
        a PostgreSQL's 'archive_command'

        :return: WalArchiverBatch: list of WAL files
        """
        # List and sort all files in the incoming directory
        file_names = glob(os.path.join(
            self.config.incoming_wals_directory, '*'))
        file_names.sort()

        # Process anything that looks like a valid WAL file. Anything
        # else is treated like an error/anomaly
        files = []
        errors = []
        for file_name in file_names:
            if xlog.is_any_xlog_file(file_name) and os.path.isfile(file_name):
                files.append(file_name)
            else:
                errors.append(file_name)

        # Build the list of WalFileInfo
        wal_files = [WalFileInfo.from_file(f) for f in files]
        return WalArchiverBatch(wal_files, errors=errors)


class StreamingWalArchiver(WalArchiver):
    """
    Object used for the management of streaming WAL archive operation.
    """

    def __init__(self, backup_manager):
        super(StreamingWalArchiver, self).__init__(backup_manager)

    def get_remote_status(self):
        """
        Execute checks for replication-based wal archiving
        :return dict[str]: result of archive checks
        """
        result = dict.fromkeys(
            ('pg_receivexlog_compatible',
                'pg_receivexlog_installed',
                'pg_receivexlog_path',
                'pg_receivexlog_version'),
            None)

        try:
            streaming = self.backup_manager.server.streaming
            pg_version = Version(
                utils.simplify_version(streaming.server_txt_version))
        except (PostgresConnectionError, psycopg2.Error) as e:
            _logger.warn("Error retrieving PostgreSQL version: %s", e)
            return result

        # Detect a pg_receivexlog executable
        pg_receivexlog = utils.which("pg_receivexlog",
                                     self.backup_manager.server.path)

        # Test pg_receivexlog existence
        if pg_receivexlog:
            result["pg_receivexlog_installed"] = True
            result["pg_receivexlog_path"] = pg_receivexlog
        else:
            result["pg_receivexlog_installed"] = False
            return result

        receivexlog = Command(pg_receivexlog, check=True)

        # Obtain the `pg_receivexlog` version
        try:
            receivexlog("--version")
            splitter_version = receivexlog.out.strip().split()
            result["pg_receivexlog_version"] = splitter_version[-1]
            receivexlog_version = Version(
                utils.simplify_version(result["pg_receivexlog_version"]))
        except CommandFailedException as e:
            _logger.debug("Error invoking pg_receivexlog: %s", e)
            return result

        # pg_receivexlog 9.2 is compatible only with PostgreSQL 9.2.
        if "9.2" == pg_version == receivexlog_version:
            result["pg_receivexlog_compatible"] = True

        # other versions are compatible with lesser versions of PostgreSQL
        # WARNING: The development versions of `pg_receivexlog` are considered
        # higher than the stable versions here, but this is not an issue
        # because it accepts everything that is less than
        # the `pg_receivexlog` version(e.g. '9.6' is less than '9.6devel')
        elif "9.2" < pg_version <= receivexlog_version:
            result["pg_receivexlog_compatible"] = True

        else:
            result["pg_receivexlog_compatible"] = False

        return result

    def get_next_batch(self):
        """
        Returns the next batch of WAL files that have been archived via
        streaming replication

        :return: WalArchiverBatch: list of WAL files
        """
        # List and sort all files in the incoming directory
        file_names = glob(os.path.join(
            self.config.streaming_wals_directory, '*'))
        file_names.sort()

        # Process anything that looks like a valid WAL file,
        # including partial ones.
        # Anything else is treated like an error/anomaly
        files = []
        skip = []
        errors = []
        for file_name in file_names:
            if xlog.is_wal_file(file_name) and os.path.isfile(file_name):
                files.append(file_name)
            elif xlog.is_partial_file(file_name) and os.path.isfile(file_name):
                skip.append(file_name)
            else:
                errors.append(file_name)
        # In case of more than a partial file, keep the last
        # and treat the rest as errors
        if len(skip) > 1:
            errors.extend(skip[:-1])
            skip = skip[-1:]

        # Keep the last full WAL file in case no partial file is present
        elif len(skip) == 0 and files:
            skip.append(files.pop())

        # Build the list of WalFileInfo
        wal_files = [WalFileInfo.from_file(f, compression=None) for f in files]
        return WalArchiverBatch(wal_files,
                                errors=errors,
                                skip=skip)
