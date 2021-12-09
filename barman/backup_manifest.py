# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2021
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

import logging
import os
import json

import barman.storage.local_file_manager
import barman.utils


class BackupManifest:
    name = "backup_manifest"

    def __init__(self, path, file_manager, checksum_algorithm):
        """
        :param path: backup directory
        :type path: str
        :param file_manager: File manager
        :type file_manager: barman.

        """
        self.files = []
        self.path = path
        self.file_manager = file_manager
        self.checksum_algorithm = checksum_algorithm

    def create_backup_manifest(self):
        if self.file_manager.file_exist(self._get_manifest_file_path()):
            msg = (
                "%s already exists. Skip file creation."
                % self._get_manifest_file_path()
            )
            logging.info(msg)
            print(msg)
            return

        self._create_files_metadata()
        manifest = {
            "PostgreSQL-Backup-Manifest-Version": 1,
            "Files": self.files,
        }
        # Convert to text
        # Might be useful to implement a JSONEncoder
        str_manifest = json.dumps(manifest, indent=2)
        # str_manifest = str_manifest[:-1] + ','
        str_manifest = str_manifest[:-2] + ",\n"
        # Create checksum from string without last '}' and ',' instead
        manifest_checksum = self.checksum_algorithm.checksum_from_str(str_manifest)
        last_line = '"Manifest-Checksum": "%s"}\n' % manifest_checksum
        full_manifest = str_manifest + last_line

        print(full_manifest)
        self.file_manager.save_content_to_file(
            self._get_manifest_file_path(), full_manifest.encode(), file_mode="wb"
        )

    def _create_files_metadata(self):
        """
        Parse all files in backup directory and get file identity values for each one of them.
        """
        file_list = self.file_manager.get_file_list(self.path)
        for filepath in file_list:
            # Create FileEntity
            identity = FileIdentity(
                filepath, self.path, self.file_manager, self.checksum_algorithm
            )
            self.files.append(identity.get_value())

    def _get_manifest_file_path(self):
        """
        Generates backup-manifest file path
        :return: backup-manifest file path
        :rtype: str
        """
        return os.path.join(self.path, self.name)


class FileIdentity:
    """
    This class purpose is to aggregate file information for backup-manifest.
    """

    def __init__(self, file_path, dir_path, file_manager, checksum_algorithm):
        """
        :param file_path: File path to analyse
        :type file_path: str
        :param dir_path: Backup directory path
        :type dir_path: str
        :param file_manager:
        :type file_manager: barman.storage.FileManager
        :param checksum_algorithm: Object that will create checksum from bytes
        :type checksum_algorithm:
        """
        self.file_path = file_path
        self.dir_path = dir_path
        self.file_manager = file_manager
        self.checksum_algorithm = checksum_algorithm

    def get_value(self):
        """
        Returns a dictionary containing FileIdentity values
        """
        stats = self.file_manager.get_file_stats(self.file_path)
        return {
            "Size": stats.get_size(),
            "Last-Modified": stats.get_last_modified(),
            "Checksum-Algorithm": self.checksum_algorithm.get_name(),
            "Path": self._get_relative_path(),
            "Checksum": self._get_checksum(),
        }

    def _get_relative_path(self):
        """
        :return: file path from directory path
        :rtype: string
        """
        if not self.file_path.startswith(self.dir_path):
            msg = "Expecting %s to start with %s" % (self.file_path, self.dir_path)
            raise AttributeError(msg)
        return self.file_path.split(self.dir_path)[1].strip("/")

    def _get_checksum(self):
        """
        :return: file checksum
        :rtype: str
        """
        content = self.file_manager.get_file_content(self.file_path)
        return self.checksum_algorithm.checksum(content)


def _main_():
    backup_dir = ""
    checksum = barman.utils.SHA256()
    file_manager = barman.storage.local_file_manager.LocalFileManager()
    backup_manifest = BackupManifest(backup_dir, file_manager, checksum)
    backup_manifest.create_backup_manifest()