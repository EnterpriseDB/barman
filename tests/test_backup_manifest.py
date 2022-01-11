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
import os
from mock import patch
from barman.backup_manifest import FileIdentity, BackupManifest
from barman.utils import SHA256


class TestFileIdentity:
    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "file_path": "/somewhere/over/the/rainbow",
                "dir_path": "/somewhere/over/",
                "expected": "the/rainbow",
            },
            {
                "file_path": "/somewhere/over/the/rainbow",
                "dir_path": "/somewhere/over",
                "expected": "the/rainbow",
            },
            {
                "file_path": ".somewhere/over/the/rainbow",
                "dir_path": ".somewhere/over/",
                "expected": "the/rainbow",
            },
        ],
    )
    @patch("barman.storage.file_manager.FileManager")
    @patch("barman.utils.ChecksumAlgorithm")
    def test_get_relative_path(self, file_manager, checksum_algorithm, test_case):
        file_identity = FileIdentity(
            test_case["file_path"],
            test_case["dir_path"],
            file_manager,
            checksum_algorithm,
        )
        assert test_case["expected"] == file_identity._get_relative_path()

    @patch("barman.storage.file_manager.FileManager")
    @patch("barman.utils.ChecksumAlgorithm")
    def test_get_relative_path_error(self, file_manager, checksum_algorithm):
        file_path = "/somewhere/over/the/rainbow"
        dir_path = "/somewhere/else/over/"
        file_identity = FileIdentity(
            file_path, dir_path, file_manager, checksum_algorithm
        )
        with pytest.raises(AttributeError):
            file_identity._get_relative_path()

    @patch("barman.storage.file_manager.FileManager")
    @patch("barman.utils.ChecksumAlgorithm")
    def test_get_value(self, file_manager, checksum_algorithm):
        file_path = "/somewhere/over/the/rainbow"
        dir_path = "/somewhere/over/"

        expected_size = 251
        expected_date = "this is the date"
        expected_checksum_name = "checksum_name"
        expected_checksum = "xyz1"
        file_stats = file_manager.get_file_stats.return_value
        file_stats.get_size.return_value = expected_size
        file_stats.get_last_modified.return_value = expected_date
        checksum_algorithm.get_name.return_value = expected_checksum_name
        checksum_algorithm.checksum.return_value = expected_checksum

        file_identity = FileIdentity(
            file_path, dir_path, file_manager, checksum_algorithm
        )
        identity_value = file_identity.get_value()
        expected_value = {
            "Size": expected_size,
            "Last-Modified": expected_date,
            "Checksum-Algorithm": expected_checksum_name,
            "Path": "the/rainbow",
            "Checksum": expected_checksum,
        }
        assert expected_value == identity_value


class TestBackupManifest:
    @patch("barman.storage.file_manager.FileManager")
    @patch("barman.utils.ChecksumAlgorithm")
    def test_get_manifest_file_path(self, file_manager, checksum_algorithm):
        backup_path = "/backup/dir/"

        backup_manifest = BackupManifest(backup_path, file_manager, checksum_algorithm)
        assert (
            "/backup/dir/backup_manifest" == backup_manifest._get_manifest_file_path()
        )

    @patch("barman.backup_manifest.FileIdentity")
    @patch("barman.utils.ChecksumAlgorithm")
    @patch("barman.storage.file_manager.FileManager")
    def test_create_files_metadata(
        self, file_manager, checksum_algorithm, file_identity
    ):
        backup_path = "/backup/dir/"

        file_name_1 = "file1"
        file_name_2 = "file2"
        file_name_3 = "base/file3"
        file_manager.get_file_list.return_value = [
            os.path.join(backup_path, file_name_1),
            os.path.join(backup_path, file_name_2),
            os.path.join(backup_path, file_name_3),
        ]

        expected_files = [
            {"Path": file_name_1},
            {"Path": file_name_2},
            {"Path": file_name_3},
        ]
        file_identity_instance = file_identity.return_value
        file_identity_instance.get_value.side_effect = expected_files

        backup_manifest = BackupManifest(backup_path, file_manager, checksum_algorithm)
        backup_manifest._create_files_metadata()

        assert backup_manifest.files == expected_files

    @patch("barman.backup_manifest.FileIdentity")
    @patch("barman.storage.file_manager")
    def test_create_backup_manifest(self, file_manager, file_identity, tmpdir):
        # tmpdir is used to get a random path. files are not used either. only their path
        file1 = tmpdir.join("file")
        file2 = tmpdir.join("subdir/other_file")

        file_manager.file_exist.return_value = False
        file_manager.get_file_list.return_value = [file1.strpath, file2.strpath]

        file_identity_instance = file_identity.return_value
        file_identity_instance.get_value.side_effect = [
            {
                "Size": 7,
                "Last-Modified": "2021-12-13 15:46:57",
                "Checksum-Algorithm": "SHA256",
                "Path": "file",
                "Checksum": "fd0edbe123e4edcec85407b10ce35900dc6cc2c64104be1b8275d3120df8dda6",
            },
            {
                "Size": 351,
                "Last-Modified": "2021-12-13 15:46:57",
                "Checksum-Algorithm": "SHA256",
                "Path": "subdir/other_file",
                "Checksum": "ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f73",
            },
        ]

        checksum = SHA256()
        backup_manifest = BackupManifest(tmpdir.strpath, file_manager, checksum)
        backup_manifest.create_backup_manifest()

        expected_manifest_path = tmpdir.join("backup_manifest")
        expected_manifest_full = """{"PostgreSQL-Backup-Manifest-Version": 1,
"Files": [
{"Checksum": "fd0edbe123e4edcec85407b10ce35900dc6cc2c64104be1b8275d3120df8dda6", "Checksum-Algorithm": "SHA256", "Last-Modified": "2021-12-13 15:46:57", "Path": "file", "Size": 7},
{"Checksum": "ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f73", "Checksum-Algorithm": "SHA256", "Last-Modified": "2021-12-13 15:46:57", "Path": "subdir/other_file", "Size": 351}],
"Manifest-Checksum": "b829d668b87cf5d8eded3a1267185bc1ba98960f3ddde4cb44859b3f0b9c58fd"}\n"""
        file_manager.save_content_to_file.assert_called_once_with(
            expected_manifest_path,
            expected_manifest_full.encode("utf-8"),
            file_mode="wb",
        )

    @patch("barman.utils.ChecksumAlgorithm")
    @patch("barman.storage.file_manager.FileManager")
    def test_manifest_already_exist(self, file_manager, checksum_algorithm):
        file_manager.file_exist.return_value = True
        backup_manifest = BackupManifest("/path", file_manager, checksum_algorithm)
        backup_manifest.create_backup_manifest()
        file_manager.get_file_list.assert_not_called()
