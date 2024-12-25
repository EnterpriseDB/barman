# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2025
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

from .file_manager import FileManager
from .file_stats import FileStats


class LocalFileManager(FileManager):
    def file_exist(self, file_path):
        """
        Tests if file exists
        :param file_path: File path
        :type file_path: string

        :return: True if file exists False otherwise
        :rtype: bool
        """
        return os.path.isfile(file_path)

    def get_file_stats(self, file_path):
        """
        Tests if file exists
        :param file_path: File path
        :type file_path: string

        :return:
        :rtype: FileStats
        """
        if not self.file_exist(file_path):
            raise IOError("Missing file " + file_path)
        sts = os.stat(file_path)
        return FileStats(sts.st_size, sts.st_mtime)

    def get_file_list(self, path):
        """
        List all files within a path, including subdirectories
        :param path: Path to analyze
        :type path: string
        :return: List of file path
        :rtype: list
        """
        if not os.path.isdir(path):
            raise NotADirectoryError(path)
        file_list = []
        for root, dirs, files in os.walk(path):
            file_list.extend(
                list(map(lambda x, prefix=root: os.path.join(prefix, x), files))
            )
        return file_list

    def get_file_content(self, file_path, file_mode="rb"):
        with open(file_path, file_mode) as reader:
            content = reader.read()
        return content

    def save_content_to_file(self, file_path, content, file_mode="wb"):
        """ """
        with open(file_path, file_mode) as writer:
            writer.write(content)
