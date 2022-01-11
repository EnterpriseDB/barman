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

import pytest
from mock import patch
from barman.storage.local_file_manager import LocalFileManager


class TestLocalFileManager(object):
    @pytest.fixture
    def file_content(self):
        return "this is \n a very useful\t content."

    def test_file_exists_false(self, tmpdir):
        file_manager = LocalFileManager()
        assert not file_manager.file_exist(str(tmpdir + "/some_file"))

    def test_file_exist_true(self, tmpdir):
        source = tmpdir.join("some_file")
        source.write("something", ensure=True)
        file_manager = LocalFileManager()
        assert file_manager.file_exist(source.strpath)

    def test_get_file_stats_file_not_found(self, tmpdir):
        source = tmpdir.join("some_file")

        file_manager = LocalFileManager()
        with pytest.raises(IOError):
            file_manager.get_file_stats(source.strpath)

    @patch("barman.storage.local_file_manager.FileStats")
    def test_get_file_stats(self, file_stat, tmpdir):
        source = tmpdir.join("some_file")
        source.write("something", ensure=True)
        expected_mtime = source.mtime()

        file_manager = LocalFileManager()
        file_manager.get_file_stats(source.strpath)

        file_stat.assert_called_once_with(9, expected_mtime)

    def test_get_file_list(self, tmpdir):
        tmpdir.join("subdir").mkdir()
        tmpdir.join("subdir/file").write("")
        tmpdir.join("some_file").write("")

        base_dir = tmpdir.strpath
        expected_list = [base_dir + "/some_file", base_dir + "/subdir/file"]
        file_manager = LocalFileManager()
        file_list = file_manager.get_file_list(base_dir)
        assert expected_list == file_list

    def test_get_file_content_string_mode(self, tmpdir, file_content):
        a_file = tmpdir.join("some_file")
        a_file.write(file_content, ensure=True)

        file_manager = LocalFileManager()
        read_content = file_manager.get_file_content(a_file.strpath, file_mode="r")
        assert file_content == read_content

    def test_get_file_content_binary_mode(self, tmpdir, file_content):
        a_file = tmpdir.join("some_file")
        a_file.write(file_content, ensure=True)

        file_manager = LocalFileManager()
        read_content = file_manager.get_file_content(a_file.strpath, file_mode="rb")
        assert file_content.encode("utf-8") == read_content

    def test_save_content_to_file_string_mode(self, tmpdir, file_content):
        a_file = tmpdir.join("some_file")
        file_manager = LocalFileManager()
        file_manager.save_content_to_file(a_file.strpath, file_content, file_mode="w")

        read_content = a_file.read()
        assert file_content == read_content

    def test_save_content_to_file_binary_mode(self, tmpdir, file_content):
        a_file = tmpdir.join("some_file")
        file_manager = LocalFileManager()
        file_manager.save_content_to_file(
            a_file.strpath, file_content.encode(), file_mode="wb"
        )

        read_content = a_file.read()
        assert file_content == read_content
