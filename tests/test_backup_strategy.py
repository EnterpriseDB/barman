# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2022
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

import mock
import os
import pytest

from io import BytesIO
from tarfile import TarFile, TarInfo

from barman.backup_executor import PostgresBackupStrategy
from barman.compression import (
    PgBaseBackupCompression,
    GZipPgBaseBackupCompressionOption,
)
from barman.exceptions import FileNotFoundException
from barman.exceptions import BackupException
from barman.infofile import LocalBackupInfo

from testing_helpers import build_mocked_server, get_compression_config


def _tar_file(items):
    """Helper to create an in-memory tar file with multiple files."""
    tar_fileobj = BytesIO()
    tf = TarFile.open(mode="w|", fileobj=tar_fileobj)
    for item_name, item_bytes in items:
        ti = TarInfo(name=item_name)
        content_as_bytes = item_bytes.encode("utf-8")
        ti.size = len(content_as_bytes)
        tf.addfile(ti, BytesIO(content_as_bytes))
    tf.close()
    tar_fileobj.seek(0)
    return tar_fileobj


class TestPostgresBackupStrategy(object):
    """
    Tests for behaviour specific to PostgresBackupStrategy.
    """

    @pytest.mark.parametrize(
        ("compression", "format", "should_set_backup_info"),
        [
            (None, "tar", False),
            (None, "plain", False),
            ("gzip", "tar", True),
            ("gzip", "plain", False),
        ],
    )
    @mock.patch("barman.backup_executor.PostgresBackupStrategy._read_backup_label")
    @mock.patch(
        "barman.backup_executor.PostgresBackupStrategy._backup_info_from_backup_label"
    )
    @mock.patch(
        "barman.backup_executor.PostgresBackupStrategy._backup_info_from_stop_location"
    )
    def test_stop_backup_sets_backup_info(
        self,
        _mock_backup_info_from_stop_location,
        _mock_backup_info_from_backup_label,
        _mock_read_backup_label,
        compression,
        format,
        should_set_backup_info,
    ):
        """
        Verifies that the compression is set appropriately in backup_info when
        stopping the backup for the given compression and format.
        """
        # GIVEN a server configured for pg_basebackup compression
        compression_options = {
            "backup_method": "postgres",
            "backup_compression": compression,
            "backup_compression_format": format,
        }
        server = build_mocked_server(global_conf=compression_options)
        compression_config = get_compression_config(compression_options)
        compression_option = GZipPgBaseBackupCompressionOption(compression_config)
        # AND a PgBaseBackupCompression for the configured compression
        if compression is not None:
            backup_compression = PgBaseBackupCompression(
                compression_config, compression_option, mock.Mock()
            )
        else:
            backup_compression = None
        # AND a BackupInfo representing an ongoing backup
        backup_info = LocalBackupInfo(server=server, backup_id="fake_id")
        # AND a PostgresBackupStrategy with the configured compression
        mock_postgres = mock.Mock()
        strategy = PostgresBackupStrategy(
            mock_postgres, "test-server", backup_compression
        )

        # WHEN stop_backup is called with the BackupInfo
        strategy.stop_backup(backup_info)

        # THEN the compression field of the BackupInfo is set to the
        # expected compression *if* the compression/format combination
        # is gzip/tar
        if should_set_backup_info:
            assert backup_info.compression == compression
        # OR compression/format is any other combination, compression
        # field should not be set
        else:
            assert backup_info.compression is None

    def test_read_backup_label_from_compressed_backup(self):
        """
        Verifies that the backup_label can be read from the backup archive
        when PgBaseBackupCompression.read_file_from_archive returns backup_label content.
        """
        expected_backup_label = "mock_backup_label_content"
        backup_archive_path = "/path/to/backup/archive"

        # GIVEN a (mock) PgBaseBackupCompression for the configured compression
        backup_compression = mock.Mock()
        backup_compression.type = "gzip"
        # AND the PgBaseBackupCompression returns backup_label content
        backup_compression.get_file_content.return_value = expected_backup_label

        # AND a BackupInfo representing an ongoing backup
        backup_info = mock.Mock()
        backup_info.compression = "gzip"
        backup_info.get_data_directory.return_value = backup_archive_path
        # AND a PostgresBackupStrategy with the configured compression
        mock_postgres = mock.Mock()
        strategy = PostgresBackupStrategy(
            mock_postgres, "test-server", backup_compression
        )

        # WHEN _read_backup_label is called with the BackupInfo
        strategy._read_backup_label(backup_info)

        # THEN the backup_label is extracted and set in the backup_info
        backup_info.set_attribute.assert_called_once_with(
            "backup_label", expected_backup_label
        )

        # AND PgBaseBackupCompression.open was called with the correct path
        backup_compression.get_file_content.assert_called_once_with(
            "backup_label",
            os.path.join(backup_archive_path, "/path/to/backup/archive", "base"),
        )

    def test_read_backup_label_from_compressed_backup_not_found(self):
        """
        Verifies that an exception is raised if the backup_label cannot be found
        when PgBaseBackupCompression.read_file_from_archive raises an exception.
        """
        backup_archive_path = "/path/to/backup/archive"

        # GIVEN a (mock) PgBaseBackupCompression for the configured compression
        backup_compression = mock.Mock()
        backup_compression.type = "gzip"
        backup_compression.with_suffix.return_value = os.path.join(
            backup_archive_path, "base.tar.gz"
        )
        # AND the PgBaseBackupCompression raises an exception when trying to read file
        backup_compression.get_file_content.side_effect = FileNotFoundException()
        # AND a BackupInfo representing an ongoing backup
        backup_info = mock.Mock()
        backup_info.compression = "gzip"
        backup_info.get_data_directory.return_value = backup_archive_path
        # AND a PostgresBackupStrategy with the configured compression
        mock_postgres = mock.Mock()
        strategy = PostgresBackupStrategy(
            mock_postgres, "test-server", backup_compression
        )

        # WHEN _read_backup_label is called with the BackupInfo
        # THEN a BackupException is raised
        with pytest.raises(BackupException) as exc:
            strategy._read_backup_label(backup_info)

        # AND the exception has the expected message
        assert str(exc.value) == "Could not find backup_label in %s" % os.path.join(
            backup_archive_path, "base.tar.gz"
        )

    @mock.patch("barman.backup_executor.BackupStrategy._read_backup_label")
    @mock.patch(
        "barman.backup_executor.PostgresBackupStrategy._read_compressed_backup_label"
    )
    @mock.patch(
        "barman.backup_executor.PostgresBackupStrategy._backup_info_from_backup_label"
    )
    @mock.patch(
        "barman.backup_executor.PostgresBackupStrategy._backup_info_from_stop_location"
    )
    def test_read_backup_label_from_uncompressed_backup(
        self,
        _mock_backup_info_from_stop_location,
        _mock_backup_info_from_backup_label,
        _mock_read_compressed_backup_label,
        _mock_read_backup_label,
    ):
        """
        Verifies that the BackupStrategy._read_backup_label method is used when
        backups are not compressed.
        """
        # GIVEN a backup_info with the default compression of None
        backup_info = mock.Mock()
        backup_info.compression = None
        # AND a PostgresBackupStrategy with no compression
        mock_postgres = mock.Mock()
        strategy = PostgresBackupStrategy(mock_postgres, "test-server", None)

        # WHEN _read_backup_label is called with the BackupInfo
        strategy._read_backup_label(backup_info)

        # THEN the we do not attempt to read the compressed backup label
        _mock_read_compressed_backup_label.assert_not_called()
        # AND the superclass method to read the backup label is called
        _mock_read_backup_label.assert_called_once()
