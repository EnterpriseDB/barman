# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2022
#
# Client Utilities for Barman, Backup and Recovery Manager for PostgreSQL
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

import datetime
import mock
import pytest

from barman.annotations import KeepManager
from barman.clients import cloud_backup_delete
from barman.cloud import CloudBackupCatalog
from barman.utils import is_backup_id

from testing_helpers import interpolate_wals


class TestCloudBackupDeleteArguments(object):
    """Test handling of command line arguments."""

    def test_fails_if_no_backup_id_or_policy_is_provided(self, capsys):
        """If no backup id is provided then exit"""
        with pytest.raises(SystemExit):
            cloud_backup_delete.main(["cloud_storage_url", "test_server"])

        _, err = capsys.readouterr()
        assert (
            "one of the arguments -b/--backup-id -r/--retention-policy is required"
            in err
        )

    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_exits_on_connectivity_test(self, get_cloud_interface_mock):
        """If the -t option is used we check connectivity and exit."""
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.test_connectivity.return_value = True
        with pytest.raises(SystemExit) as exc:
            cloud_backup_delete.main(
                ["cloud_storage_url", "test_server", "--backup-id", "backup_id", "-t"]
            )
        assert exc.value.code == 0
        cloud_interface_mock.test_connectivity.assert_called_once()


class TestCloudBackupDelete(object):
    """Test the interaction of barman-cloud-backup-delete with the cloud provider."""

    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_fails_on_connectivity_test_failure(self, get_cloud_interface_mock):
        """If connectivity test fails we exit."""
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.test_connectivity.return_value = False
        with pytest.raises(SystemExit) as exc:
            cloud_backup_delete.main(
                ["cloud_storage_url", "test_server", "--backup-id", "backup_id"]
            )
        assert exc.value.code == 2
        cloud_interface_mock.test_connectivity.assert_called_once()

    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_fails_if_bucket_not_found(self, get_cloud_interface_mock, caplog):
        """If the bucket does not exist we exit with status 1."""
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.bucket_name = "no_bucket_here"
        cloud_interface_mock.bucket_exists = False
        with pytest.raises(SystemExit) as exc:
            cloud_backup_delete.main(
                [
                    "s3://cloud_storage_url/no_bucket_here",
                    "test_server",
                    "--backup-id",
                    "backup_id",
                ]
            )
        assert exc.value.code == 1
        assert "Bucket no_bucket_here does not exist" in caplog.text

    def _create_mock_file_info(self, path):
        file_info = mock.MagicMock()
        file_info.path = path
        file_info.additional_files = []
        return file_info

    def _get_mock_backup_files(self, file_paths):
        """Return a dict of mock BackupFileInfo objects keyed by file name."""
        return dict(
            [name, self._create_mock_file_info(path)]
            for name, path in file_paths.items()
        )

    def _create_backup_metadata(self, backup_ids, begin_wals={}, end_wals={}):
        """
        Helper for tests which creates mock BackupFileInfo and BackupInfo objects
        which are returned in a dict keyed by backup_id.

        If begin_wals has an entry for a given backup_id then the begin_wal and
        timeline values will be set according to the wal name in begin_wals.

        This is used by tests for two purposes:
          1. Creating a mock CloudBackupCatalog.
          2. Providing the data needed to verify that the expected backups were deleted.
        """
        backup_metadata = {}
        for backup in backup_ids:
            backup_name = None
            if isinstance(backup, tuple):
                backup_id, backup_name = backup
            else:
                backup_id = backup
            backup_metadata[backup_id] = {}
            mock_backup_files = self._get_mock_backup_files(
                {
                    16388: "%s/16388" % backup_id,
                    None: "%s/data.tar" % backup_id,
                }
            )
            backup_metadata[backup_id]["files"] = mock_backup_files
            backup_info = mock.MagicMock(name="backup_info")
            backup_info.backup_id = backup_id
            if backup_name:
                backup_info.backup_name = backup_name
            backup_info.status = "DONE"
            backup_info.end_time = datetime.datetime.strptime(
                backup_id, "%Y%m%dT%H%M%S"
            ) + datetime.timedelta(hours=1)
            try:
                backup_info.begin_wal = begin_wals[backup_id]
                backup_info.timeline = int(backup_info.begin_wal[:8])
            except KeyError:
                pass
            try:
                backup_info.end_wal = end_wals[backup_id]
            except KeyError:
                pass
            backup_metadata[backup_id]["info"] = backup_info
        return backup_metadata

    def _get_sorted_files_for_backup(self, backup_metadata, backup_id):
        """
        Helper function for tests which retrieves the path of each file in the backup
        sorted by OID, including any additional files which may be present.

        This is used by tests to verify that all files associated with the backup were
        deleted.
        """
        files_for_backup = []
        for _oid, backup_file in sorted(
            backup_metadata[backup_id]["files"].items(),
            key=lambda x: x[0] if x[0] else -1,
        ):
            # Only include the main file if it has a path. If path is None then
            # the tests are not expecting the main file to have been deleted.
            if backup_file.path:
                files_for_backup.append(backup_file.path)
            additional_files = [
                additional_file.path for additional_file in backup_file.additional_files
            ]
            files_for_backup += sorted(additional_files)
        return files_for_backup

    def _create_catalog(slef, backup_metadata, wals=[]):
        """
        Create a mock CloudBackupCatalog from the supplied data so that we can provide
        a work-alike CloudBackupCatalog to the code-under-test without also including
        the CloudBackupCatalog logic in the tests.
        """
        # Copy so that we don't affect the state the tests are using when the
        # code under test removes backups from the mock catalog
        backup_state = backup_metadata.copy()

        def get_backup_info(backup_id):
            return backup_state[backup_id]["info"]

        def get_backup_list():
            return dict(
                [backup_id, metadata["info"]]
                for backup_id, metadata in backup_state.items()
            )

        def remove_backup_from_cache(backup_id):
            del backup_state[backup_id]

        def get_backup_files(backup_info, allow_missing=False):
            return backup_state[backup_info.backup_id]["files"]

        def get_wal_paths():
            return dict([wal, "wals/%s.gz" % wal] for wal in wals)

        def remove_wal_from_cache(wal_name):
            wals.remove(wal_name)

        # Just enough code to resolve backup names to backup IDs in the mock catalog.
        # This allows the tests to verify the delete code is using parse_backup_id
        # correctly (though obviously it doesn't verify parse_backup_id does the
        # right thing - this is covered in test_cloud.py).
        def parse_backup_id(backup_id):
            if not is_backup_id(backup_id):
                return [
                    b_id
                    for b_id, backup in backup_state.items()
                    if backup["info"].backup_name == backup_id
                ][0]
            else:
                return backup_id

        catalog = mock.Mock(CloudBackupCatalog)
        catalog.configure_mock(
            **{
                "prefix": "",
                "unreadable_backups": [],
                "get_backup_info.side_effect": get_backup_info,
                "get_backup_list.side_effect": get_backup_list,
                "remove_backup_from_cache.side_effect": remove_backup_from_cache,
                "get_backup_files.side_effect": get_backup_files,
                "get_wal_paths.side_effect": get_wal_paths,
                "remove_wal_from_cache.side_effect": remove_wal_from_cache,
                "parse_backup_id.side_effect": parse_backup_id,
            }
        )
        catalog.should_keep_backup.return_value = False
        catalog.get_keep_target.return_value = None
        return catalog

    def _verify_cloud_interface_calls(self, get_cloud_interface_mock, expected_calls):
        """
        Verify that cloud_interface.delete_objects was called only with the arguments
        provided in expected_calls and only in that order.
        """
        cloud_interface_mock = get_cloud_interface_mock.return_value
        assert len(cloud_interface_mock.delete_objects.call_args_list) == len(
            expected_calls
        )
        for i, call_args in enumerate(expected_calls):
            assert (
                call_args == cloud_interface_mock.delete_objects.call_args_list[i][0][0]
            )

    def _verify_only_these_backups_deleted(
        self, get_cloud_interface_mock, backup_metadata, backup_ids, wals={}
    ):
        """
        Helper function which allows tests to verify that the provided list of
        backup_ids were fully deleted via cloud_interface.delete_objects. We
        verify that the backups were deleted and that no other deletions were
        made.

        For each backup we verify that:
          1. All files associated with the backup were deleted (including additional
             files specified in the BackupFileInfo object).
          2. Then, the backup.info file for the backup was deleted.
          3. Optionally (if a list of WALs exists in `wals` for the backup being
             deleted) that the expected WALs were deleted.
        """
        call_args = []
        for backup_id in backup_ids:
            call_args.append(
                self._get_sorted_files_for_backup(backup_metadata, backup_id)
            )
            call_args.append(["%s/backup.info" % backup_id])
            try:
                call_args.append(wals[backup_id])
            except KeyError:
                # Not all tests expect WALs to be deleted so silently continue here
                pass
        self._verify_cloud_interface_calls(
            get_cloud_interface_mock,
            call_args,
        )

    @pytest.mark.parametrize("backup_id_arg", ("20210723T095432", "backup name"))
    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_delete_single_backup(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, backup_id_arg
    ):
        """
        Tests that files for the specified backup are deleted via the cloud provider.
        """
        # GIVEN a backup catalog with one named backup and no WALs
        backup_id = "20210723T095432"
        backup_metadata = self._create_backup_metadata([(backup_id, "backup name")])

        # AND a CloudBackupCatalog which returns the backup_info for only that backup
        cloud_backup_catalog_mock.return_value = self._create_catalog(backup_metadata)

        # WHEN barman-cloud-backup-delete runs, specifying either a backup ID or name
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--backup-id", backup_id_arg]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # that backup and the backup.info file for that backup
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock, backup_metadata, [backup_id]
        )

    @pytest.mark.parametrize("backup_id_arg", ("20210723T095432", "backup name"))
    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_delete_archival_backup(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, backup_id_arg, caplog
    ):
        """Test that attempting to delete an archival backup fails"""
        # GIVEN a backup catalog with one named backup and no WALs
        backup_id = "20210723T095432"
        backup_metadata = self._create_backup_metadata([(backup_id, "backup name")])

        # AND a CloudBackupCatalog which returns the backup_info for only that backup
        cloud_backup_catalog_mock.return_value = self._create_catalog(backup_metadata)

        # AND the backup is archival
        cloud_backup_catalog_mock.return_value.should_keep_backup.return_value = True

        # WHEN barman-cloud-backup-delete runs, specifying the backup ID or name
        with pytest.raises(SystemExit) as exc:
            cloud_backup_delete.main(
                ["cloud_storage_url", "test_server", "--backup-id", backup_id_arg]
            )

        # THEN we exit with status 1
        assert exc.value.code == 1

        # AND log a helpful message
        assert (
            "Skipping delete of backup 20210723T095432 for server test_server "
            "as it has a current keep request. If you really want to delete this "
            "backup please remove the keep and try again."
        ) in caplog.text

    @pytest.mark.parametrize("backup_id_arg", ("20210723T095432", "backup name"))
    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_delete_missing_files(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, backup_id_arg
    ):
        """
        Tests that any backup files which are expected but not found are
        ignored and do not affect the success of the backup deletion.
        """
        # GIVEN a backup catalog with one named backup and no WALs
        backup_id = "20210723T095432"
        backup_metadata = self._create_backup_metadata([(backup_id, "backup name")])

        # AND the tablespace archive is missing
        backup_metadata[backup_id]["files"][16388].path = None

        # AND a CloudBackupCatalog which returns the backup_info for only that backup
        cloud_backup_catalog_mock.return_value = self._create_catalog(backup_metadata)

        # WHEN barman-cloud-backup-delete runs, specifying the backup ID or name
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--backup-id", backup_id_arg]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # that backup which exist along with the backup.info file for that backup.
        # There was no delete attempt for the missing tablespace archive.
        backup_metadata[backup_id]["files"].pop(16388)
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock, backup_metadata, [backup_id]
        )

    @pytest.mark.parametrize("backup_id_arg", ("20210723T095432", "backup name"))
    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_delete_missing_files_with_additional_files(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, backup_id_arg
    ):
        """
        Tests that if a file which has additional files is missing, the additional
        files are still deleted.
        """
        # GIVEN a backup catalog with one named backup and no WALs
        backup_id = "20210723T095432"
        backup_metadata = self._create_backup_metadata([(backup_id, "backup name")])

        # AND the PGDATA file has additional files
        backup_metadata[backup_id]["files"][None].additional_files = [
            # AND the order of additional_files is not sorted by path
            self._create_mock_file_info("%s/data_0002.tar" % backup_id),
            self._create_mock_file_info("%s/data_0001.tar" % backup_id),
        ]

        # AND the main PGDATA file is missing
        backup_metadata[backup_id]["files"][None].path = None

        # AND a CloudBackupCatalog which returns the backup_info for only that backup
        cloud_backup_catalog_mock.return_value = self._create_catalog(backup_metadata)

        # WHEN barman-cloud-backup-delete runs, specifying the backup ID or name
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--backup-id", backup_id_arg]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # that backup which exist along with the backup.info file for that backup.
        # There was no delete attempt for the missing PGDATA archive but there are
        # deletes for the additional files. This is implicitly tested because files
        # with a path of None are excluded from the verification of call args.
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock, backup_metadata, [backup_id]
        )

    @pytest.mark.parametrize("backup_id_arg", ("20210723T095432", "backup name"))
    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_delete_additional_files(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, backup_id_arg
    ):
        """
        Tests that additional files (created due to --max-archive-size being
        exceeded) are also deleted.
        """
        # GIVEN a backup catalog with one named backup and no WALs
        backup_id = "20210723T095432"
        backup_metadata = self._create_backup_metadata([(backup_id, "backup name")])

        # AND the tablespace and PGDATA files both have additional files
        backup_metadata[backup_id]["files"][16388].additional_files = [
            self._create_mock_file_info("%s/16388_0001.tar" % backup_id),
            self._create_mock_file_info("%s/16388_0002.tar" % backup_id),
        ]
        backup_metadata[backup_id]["files"][None].additional_files = [
            # AND the order of additional_files is not sorted by path
            self._create_mock_file_info("%s/data_0002.tar" % backup_id),
            self._create_mock_file_info("%s/data_0001.tar" % backup_id),
        ]

        # AND a CloudBackupCatalog which returns the backup_info for only that backup
        cloud_backup_catalog_mock.return_value = self._create_catalog(backup_metadata)

        # WHEN barman-cloud-backup-delete runs, specifying the backup ID or name
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--backup-id", backup_id_arg]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # that backup and the additional files in the expected sort order
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock, backup_metadata, [backup_id]
        )

    @pytest.mark.parametrize("backup_id_arg", ("20210723T095432", "backup name"))
    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_delete_one_of_multiple_backups(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, backup_id_arg
    ):
        """
        Tests that *only* files for the specified backup are deleted via the cloud
        provider and files for other backups are preserved.
        """
        # GIVEN a backup catalog with two backups and no WALs
        backup_id = "20210723T095432"
        backup_metadata = self._create_backup_metadata(
            ["20210723T085432", (backup_id, "backup name")]
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        cloud_backup_catalog_mock.return_value = self._create_catalog(backup_metadata)

        # WHEN barman-cloud-backup-delete runs, specifying the backup ID or name
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--backup-id", backup_id_arg]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # the specified ID
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock, backup_metadata, [backup_id]
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_deletion_of_missing_backup(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, caplog
    ):
        """
        Tests that a backup ID which cannot be found is handled quietly.
        """
        # GIVEN an empty backup catalog
        catalog = cloud_backup_catalog_mock.return_value
        catalog.get_backup_info.return_value = None
        catalog.should_keep_backup.return_value = False

        # AND a backup_id which is not in the catalog
        backup_id = "20210723T095432"
        catalog.parse_backup_id.return_value = backup_id

        # WHEN barman-cloud-backup-delete runs, specifying the backup ID
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--backup-id", backup_id]
        )

        # THEN we complete successfully and nothing was deleted
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.delete_objects.assert_not_called()

        # AND the logs contain a warning
        assert "Backup 20210723T095432 does not exist" in caplog.text

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_delete_by_redundancy_policy(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """
        Test that only files for the backups which are not needed to meet
        the retention policy are deleted.
        """
        # GIVEN a backup catalog with four backups and no WALs
        out_of_policy_backup_ids = ["20210723T095432", "20210722T095432"]
        in_policy_backup_ids = ["20210724T095432", "20210725T095432"]
        backup_metadata = self._create_backup_metadata(
            in_policy_backup_ids + out_of_policy_backup_ids
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        cloud_backup_catalog_mock.return_value = self._create_catalog(backup_metadata)

        # WHEN barman-cloud-backup-delete runs, specifying a redundancy policy with
        # two copies
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--retention-policy", "REDUNDANCY 2"]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # the backups which are not required to meet the policy
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock, backup_metadata, sorted(out_of_policy_backup_ids)
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_delete_by_redundancy_policy_preserves_archival_backups(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """
        Test that files related to archival backups (those with a keep annotation)
        are preserved when deleting by retention policy.
        """
        # GIVEN a backup catalog with four backups and no WALs
        out_of_policy_backup_ids = ["20210723T095432", "20210722T095432"]
        in_policy_backup_ids = ["20210724T095432", "20210725T095432"]
        backup_metadata = self._create_backup_metadata(
            in_policy_backup_ids + out_of_policy_backup_ids
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        cloud_backup_catalog_mock.return_value = self._create_catalog(backup_metadata)

        # AND one of the out-of-policy backups is archival
        def should_keep_backup(backup_id, use_cache=True):
            return (
                backup_id == "20210722T095432" and KeepManager.TARGET_STANDALONE or None
            )

        cloud_backup_catalog_mock.return_value.get_keep_target.side_effect = (
            should_keep_backup
        )

        # WHEN barman-cloud-backup-delete runs, specifying a redundancy policy with
        # two copies
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--retention-policy", "REDUNDANCY 2"]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # the backup which is not required to meet the policy and is not archival
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock, backup_metadata, ["20210723T095432"]
        )

    @mock.patch("barman.retention_policies.datetime")
    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_delete_by_recovery_window_policy(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, datetime_mock
    ):
        """
        Test that only files for the backups which are not needed to meet
        the recovery window retention policy are deleted.
        """
        # GIVEN a backup catalog with four daily backups and no WALs
        out_of_policy_backup_ids = ["20210723T095432", "20210722T095432"]
        in_policy_backup_ids = ["20210724T095432", "20210725T095432"]
        backup_metadata = self._create_backup_metadata(
            in_policy_backup_ids + out_of_policy_backup_ids
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        cloud_backup_catalog_mock.return_value = self._create_catalog(backup_metadata)

        # AND a system time between one and two days after the most recent backup
        datetime_mock.now.return_value = datetime.datetime(2021, 7, 27)

        # WHEN barman-cloud-backup-delete runs, specifying a recovery window policy of
        # 2 days
        cloud_backup_delete.main(
            [
                "cloud_storage_url",
                "test_server",
                "--retention-policy",
                "RECOVERY WINDOW OF 2 DAYS",
            ]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # the backups which are not required to meet the policy
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock, backup_metadata, sorted(out_of_policy_backup_ids)
        )

    @mock.patch("barman.retention_policies.datetime")
    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_delete_by_recovery_window_policy_preserves_archival_backups(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, datetime_mock
    ):
        """
        Test that only files for the backups which are not needed to meet
        the recovery window retention policy are deleted.
        """
        # GIVEN a backup catalog with four daily backups and no WALs
        out_of_policy_backup_ids = ["20210723T095432", "20210722T095432"]
        in_policy_backup_ids = ["20210724T095432", "20210725T095432"]
        backup_metadata = self._create_backup_metadata(
            in_policy_backup_ids + out_of_policy_backup_ids
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        cloud_backup_catalog_mock.return_value = self._create_catalog(backup_metadata)

        # AND a system time between one and two days after the most recent backup
        datetime_mock.now.return_value = datetime.datetime(2021, 7, 27)

        # AND one of the out-of-policy backups is archival
        def should_keep_backup(backup_id, use_cache=True):
            return (
                backup_id == "20210722T095432" and KeepManager.TARGET_STANDALONE or None
            )

        cloud_backup_catalog_mock.return_value.get_keep_target.side_effect = (
            should_keep_backup
        )

        # WHEN barman-cloud-backup-delete runs, specifying a recovery window policy of
        # 2 days
        cloud_backup_delete.main(
            [
                "cloud_storage_url",
                "test_server",
                "--retention-policy",
                "RECOVERY WINDOW OF 2 DAYS",
            ]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # the backup which is not required to meet the policy and is not archival
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock, backup_metadata, ["20210723T095432"]
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_delete_by_redundancy_policy_no_obsolete_backups(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """
        Test that when there are no obsolete backups we exit successfully and nothing
        is deleted.
        """
        # GIVEN a backup catalog with three backups and no WALs
        in_policy_backup_ids = ["20210723T095432", "20210724T095432", "20210725T095432"]
        backup_metadata = self._create_backup_metadata(in_policy_backup_ids)

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        cloud_backup_catalog_mock.return_value = self._create_catalog(backup_metadata)

        # WHEN barman-cloud-backup-delete runs, specifying a redundancy policy with
        # three copies
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--retention-policy", "REDUNDANCY 3"]
        )

        # THEN the cloud interface was not used to delete anything and, implicitly,
        # no errors were returned
        cloud_interface_mock = get_cloud_interface_mock.return_value
        assert len(cloud_interface_mock.delete_objects.call_args_list) == 0

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_delete_by_unsupported_redundancy_policy(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, caplog
    ):
        """
        Test that when the supplied redundancy policy is unsupported we provide
        a meaningful error.
        """
        # WHEN barman-cloud-backup-delete runs, specifying a redundancy policy
        # which is unsupported
        with pytest.raises(SystemExit) as exc:
            cloud_backup_delete.main(
                [
                    "cloud_storage_url",
                    "test_server",
                    "--retention-policy",
                    "THIS IS NOT VALID",
                ]
            )

        # THEN we exit with status 3
        assert exc.value.code == 3

        # AND we logged a useful message
        assert "Cannot parse option retention_policy: THIS IS NOT VALID" in caplog.text

        # AND the cloud interface was not used to delete anything
        cloud_interface_mock = get_cloud_interface_mock.return_value
        assert len(cloud_interface_mock.delete_objects.call_args_list) == 0

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_error_on_delete(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, caplog
    ):
        """
        Test that when the cloud interface returns an error deleting backup files,
        we exit with an error and do not delete the backup.info file, clean up any
        WALs or delete any other backups.
        """
        # GIVEN a backup catalog with three backups where the oldest has a
        # begin_wal value
        out_of_policy_backup_ids = ["20210723T095432", "20210722T095432"]
        in_policy_backup_ids = ["20210725T095432"]
        begin_wals = {out_of_policy_backup_ids[1]: "00000010000000000000076"}
        backup_metadata = self._create_backup_metadata(
            in_policy_backup_ids + out_of_policy_backup_ids, begin_wals=begin_wals
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of two WALs, one which pre-dates the oldest backup
        wals = ["000000010000000000000075", "000000010000000000000076"]
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata, wals=wals
        )

        # AND the cloud provider returns an error on delete via CloudInterface the
        # first time it is called
        cloud_interface_mock = get_cloud_interface_mock.return_value
        self._should_error = True

        def mock_delete_objects(objects):
            if self._should_error:
                self._should_error = False
                raise Exception("Something went wrong on delete")
            else:
                return True

        cloud_interface_mock.delete_objects.side_effect = mock_delete_objects

        # WHEN barman-cloud-backup-delete runs, specifying a redundancy policy with
        # one copy
        with pytest.raises(SystemExit) as exc:
            cloud_backup_delete.main(
                [
                    "cloud_storage_url",
                    "test_server",
                    "--retention-policy",
                    "REDUNDANCY 1",
                ]
            )

        # THEN an error was logged when the first backup could not be deleted
        assert (
            "Could not delete backup 20210722T095432: Something went wrong on delete"
            in caplog.text
        )

        # AND we exit with status 1
        assert exc.value.code == 1

        # AND the cloud interface was used to delete objects only once - for
        # the backup that failed. It was not called to delete the backup.info file
        # of that backup, nor was it called to clean up WALs associated
        # with that backup. It was also not called to delete subsequent backups.
        assert len(cloud_interface_mock.delete_objects.call_args_list) == 1

        expected_deleted_objects = self._get_sorted_files_for_backup(
            backup_metadata, out_of_policy_backup_ids[1]
        )
        assert (
            mock.call(expected_deleted_objects)
            in cloud_interface_mock.delete_objects.call_args_list
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_error_when_listing_backups(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, caplog
    ):
        """
        Tests that if any error occurs when reading backup.info files we halt. In such
        cases it is unsafe to continue (we might end up deleting WALs which are needed
        by backups which have been moved to another storage class, for example).

        Although barman-cloud-backup-list is optimistic and continues to list the
        bucket contents, barman-cloud-backup-delete cannot safely proceed after listing
        the remaining backups.
        """
        # GIVEN a backup catalog with two backups and no WALs
        backup_id = "20210723T095432"
        broken_backup_id = "20210724T095432"
        backup_metadata = self._create_backup_metadata([backup_id, broken_backup_id])

        # AND a CloudBackupCatalog which returns the backup_info for only that backup
        catalog = cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata
        )

        # AND the cloud provider was unable to read one of the backups
        catalog.unreadable_backups = [broken_backup_id]

        # WHEN barman-cloud-backup-delete runs, specifying the backup ID
        with pytest.raises(SystemExit) as exc:
            cloud_backup_delete.main(
                ["cloud_storage_url", "test_server", "--backup-id", backup_id]
            )

        # THEN we exit with status 1
        assert exc.value.code == 1

        # AND we log a useful message
        assert (
            "Cannot read the following backups: ['20210724T095432']\n"
            "Unsafe to proceed with deletion due to failure reading backup catalog"
        ) in caplog.text

        # AND the cloud interface was not used to delete anything
        cloud_interface_mock = get_cloud_interface_mock.return_value
        cloud_interface_mock.delete_objects.assert_not_called()

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_no_wal_cleanup_when_older_backups_left(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """Tests that no WALs are cleaned up when an older backup remains."""
        # GIVEN a backup catalog with two backups with begin_wal values
        backup_id = "20210723T095432"
        older_backup_id = "20210722T095432"
        begin_wals = {
            backup_id: "000000010000000000000075",
            older_backup_id: "000000010000000000000073",
        }
        backup_metadata = self._create_backup_metadata(
            [backup_id, older_backup_id], begin_wals=begin_wals
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of three WALs
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata,
            wals=[
                "000000010000000000000073",
                "000000010000000000000074",
                "000000010000000000000075",
            ],
        )

        # WHEN barman-cloud-backup-delete runs, specifying the backup ID
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--backup-id", backup_id]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # that backup and no WALs were deleted
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock, backup_metadata, [backup_id], wals={}
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_wals_cleaned_up_after_deleting_only_backup(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """
        Tests that WALs which pre-date the begin_wal of the last remaining
        backup are deleted. Any WALs including and after the begin_wal of
        that backup are preserved.
        """
        # GIVEN a backup catalog with one backup with a begin_wal value
        backup_id = "20210723T095432"
        begin_wals = {
            backup_id: "000000010000000000000076",
        }
        backup_metadata = self._create_backup_metadata(
            [backup_id], begin_wals=begin_wals
        )

        # AND a CloudBackupCatalog which returns the backup_info for only that backup
        # and a list of four WALs
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata,
            wals=[
                "000000010000000000000074",
                "000000010000000000000075",
                "000000010000000000000076",
                "000000010000000000000077",
            ],
        )

        # WHEN barman-cloud-backup-delete runs, specifying the backup ID
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--backup-id", backup_id]
        )

        # THEN the cloud provider was only asked to delete the backup files and backup.info
        # for the deleted backup and the WALs which pre-date the deleted backup
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock,
            backup_metadata,
            [backup_id],
            wals={
                backup_id: [
                    "wals/000000010000000000000074.gz",
                    "wals/000000010000000000000075.gz",
                ]
            },
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_wals_cleaned_up_after_deleting_oldest_backup(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """
        Tests that WALs which pre-date the begin_wal of the oldest remaining
        backup are deleted.
        """
        # GIVEN a backup catalog with two backups with begin_wal values
        backup_id = "20210723T095432"
        oldest_backup_id = "20210722T095432"
        begin_wals = {
            backup_id: "000000010000000000000076",
            oldest_backup_id: "000000010000000000000075",
        }
        backup_metadata = self._create_backup_metadata(
            [backup_id, oldest_backup_id], begin_wals=begin_wals
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of four WALs plus the backup label
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata,
            wals=[
                "000000010000000000000073",
                "000000010000000000000074",
                "000000010000000000000075",
                "000000010000000000000075.00000028.backup",
                "000000010000000000000076.00000028.backup",
                "000000010000000000000076",
            ],
        )

        # WHEN barman-cloud-backup-delete runs, specifying the oldest backup ID
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--backup-id", oldest_backup_id]
        )

        # THEN the cloud provider was only asked to delete the backup files and backup.info
        # for the deleted backup and the WALs which pre-date the oldest remaining backup
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock,
            backup_metadata,
            [oldest_backup_id],
            wals={
                oldest_backup_id: [
                    "wals/000000010000000000000073.gz",
                    "wals/000000010000000000000074.gz",
                    "wals/000000010000000000000075.00000028.backup.gz",
                    "wals/000000010000000000000075.gz",
                ]
            },
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_history_files_preserved_when_cleaning_up_wals(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """
        Tests that history files are not deleted even if they "pre-date" the backup
        being deleted.
        """
        # GIVEN a backup catalog with two backups with begin_wal values
        backup_id = "20210723T095432"
        oldest_backup_id = "20210722T095432"
        begin_wals = {
            backup_id: "000000010000000000000076",
            oldest_backup_id: "000000010000000000000075",
        }
        backup_metadata = self._create_backup_metadata(
            [backup_id, oldest_backup_id], begin_wals=begin_wals
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of four WALs including a history file
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata,
            wals=[
                "00000001.history",
                "000000010000000000000073",
                "000000010000000000000074",
                "000000010000000000000075",
                "000000010000000000000076",
            ],
        )

        # WHEN barman-cloud-backup-delete runs, specifying the oldest backup ID
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--backup-id", oldest_backup_id]
        )

        # THEN the cloud provider was only asked to delete the backup files and backup.info
        # for the deleted backup and the WALs which pre-date the oldest remaining backup
        # and *not* the history file.
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock,
            backup_metadata,
            [oldest_backup_id],
            wals={
                oldest_backup_id: [
                    "wals/000000010000000000000073.gz",
                    "wals/000000010000000000000074.gz",
                    "wals/000000010000000000000075.gz",
                ]
            },
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_no_backups_or_wals_deleted_when_dry_run_set(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, capsys
    ):
        """
        Tests that when there are eligible backups and WALs to delete, nothing
        is actually deleted when the --dry-run flag is used.
        """
        # GIVEN a backup catalog with two backups with begin_wal values
        backup_id = "20210723T095432"
        oldest_backup_id = "20210722T095432"
        begin_wals = {
            backup_id: "000000010000000000000076",
            oldest_backup_id: "000000010000000000000075",
        }
        backup_metadata = self._create_backup_metadata(
            [backup_id, oldest_backup_id], begin_wals=begin_wals
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of four WALs
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata,
            wals=[
                "000000010000000000000073",
                "000000010000000000000074",
                "000000010000000000000075",
                "000000010000000000000076",
            ],
        )

        # WHEN barman-cloud-backup-delete runs, specifying the oldest backup ID
        # AND the --dry-run flag is used
        cloud_backup_delete.main(
            [
                "cloud_storage_url",
                "test_server",
                "--backup-id",
                oldest_backup_id,
                "--dry-run",
            ]
        )

        # THEN the cloud provider does not request any deletions
        cloud_interface_mock = get_cloud_interface_mock.return_value
        assert len(cloud_interface_mock.delete_objects.call_args_list) == 0

        # AND details of skipped deletions are printed to stdout
        out, _err = capsys.readouterr()
        assert (
            "Skipping deletion of objects ['20210722T095432/data.tar', "
            "'20210722T095432/16388', '20210722T095432/backup.info'] "
            "due to --dry-run option"
        ) in out
        assert (
            "Skipping deletion of objects ['wals/000000010000000000000073.gz', "
            "'wals/000000010000000000000074.gz', 'wals/000000010000000000000075.gz'] "
            "due to --dry-run option"
        ) in out

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_wals_cleaned_up_after_deleting_by_retention_policy(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """
        Tests that WALs which pre-date the begin_wal of the oldest remaining
        backup are deleted.
        """
        # GIVEN a backup catalog with four backups with begin_wal values
        out_of_policy_backup_ids = ["20210722T095432", "20210723T095432"]
        in_policy_backup_ids = ["20210724T095432", "20210725T095432"]
        begin_wals = {
            out_of_policy_backup_ids[0]: "000000010000000000000076",
            out_of_policy_backup_ids[1]: "000000010000000000000078",
            in_policy_backup_ids[0]: "00000001000000000000007A",
            in_policy_backup_ids[1]: "00000001000000000000007C",
        }
        backup_metadata = self._create_backup_metadata(
            out_of_policy_backup_ids + in_policy_backup_ids, begin_wals=begin_wals
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of eight WALs
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata,
            wals=[
                "000000010000000000000075",
                "000000010000000000000076",
                "000000010000000000000077",
                "000000010000000000000078",
                "000000010000000000000079",
                "00000001000000000000007A",
                "00000001000000000000007B",
                "00000001000000000000007C",
            ],
        )

        # WHEN barman-cloud-backup-delete runs, specifying the oldest backup ID
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--retention-policy", "REDUNDANCY 2"]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # the backups which are not required to meet the policy
        # AND after each backup was deleted, the WALs which pre-date the oldest
        # remaining backup were deleted too.
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock,
            backup_metadata,
            out_of_policy_backup_ids,
            wals={
                out_of_policy_backup_ids[0]: [
                    "wals/000000010000000000000075.gz",
                    "wals/000000010000000000000076.gz",
                    "wals/000000010000000000000077.gz",
                ],
                out_of_policy_backup_ids[1]: [
                    "wals/000000010000000000000078.gz",
                    "wals/000000010000000000000079.gz",
                ],
            },
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_wals_on_other_timelines_are_preserved(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """
        Tests that WALs which pre-date the begin_wal of the oldest remaining
        backup but are on a different timeline to the deleted backup do not get
        deleted.
        """
        # GIVEN a backup catalog with two backups with begin_wal values
        # AND an additional backup on another timeline
        backup_id = "20210723T095432"
        oldest_backup_id = "20210722T095432"
        alt_timeline_backup_id = "20210723T105432"
        begin_wals = {
            backup_id: "000000020000000000000076",
            oldest_backup_id: "000000020000000000000075",
            alt_timeline_backup_id: "000000010000000000000074",
        }
        backup_metadata = self._create_backup_metadata(
            [backup_id, oldest_backup_id, alt_timeline_backup_id], begin_wals=begin_wals
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of WALs on both timelines
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata,
            wals=[
                "000000020000000000000073",
                "000000020000000000000074",
                "000000020000000000000075",
                "000000020000000000000076",
                "000000010000000000000073",
                "000000010000000000000074",
            ],
        )

        # WHEN barman-cloud-backup-delete runs, specifying the oldest backup ID
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--backup-id", oldest_backup_id]
        )

        # THEN the cloud provider was only asked to delete the backup files and
        # backup.info for the deleted backup and only delete the associated WALs
        # which are on the same timeline as the deleted backup.
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock,
            backup_metadata,
            [oldest_backup_id],
            wals={
                oldest_backup_id: [
                    "wals/000000020000000000000073.gz",
                    "wals/000000020000000000000074.gz",
                    "wals/000000020000000000000075.gz",
                ]
            },
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_wals_are_preserved_if_older_backup_exists_on_alt_timeline(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """
        Tests that WALs are not deleted if there is an older backup which is on
        an alternative timeline. Even though those WALs are not required by the
        backup on that timeline the current logic will not delete them because
        the logic for finding a previous backup is not timeline-aware.

        This is equivalent behaviour with the existing WAL cleanup implementation
        in Barman and is intented to prevent the deletion of in-use WALs in
        complex multi-timeline scenarios.
        """
        # GIVEN a backup catalog with two backups with begin_wal values
        # AND an additional backup on another timeline which is older than
        # the other backups
        backup_id = "20210723T095432"
        oldest_backup_id = "20210722T095432"
        alt_timeline_backup_id = "20210721T105432"
        begin_wals = {
            backup_id: "000000020000000000000076",
            oldest_backup_id: "000000020000000000000075",
            alt_timeline_backup_id: "000000010000000000000074",
        }
        backup_metadata = self._create_backup_metadata(
            [backup_id, oldest_backup_id, alt_timeline_backup_id], begin_wals=begin_wals
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of WALs on both timelines
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata,
            wals=[
                "000000020000000000000073",
                "000000020000000000000074",
                "000000020000000000000075",
                "000000020000000000000076",
                "000000010000000000000073",
                "000000010000000000000074",
            ],
        )

        # WHEN barman-cloud-backup-delete runs, specifying the oldest backup ID
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--backup-id", oldest_backup_id]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # that backup and no WALs were deleted
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock, backup_metadata, [oldest_backup_id], wals={}
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_wals_on_timelines_with_no_backups_are_deleted(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """
        Tests that WALs which are on a timeline for which there are no backups
        are cleaned up.

        This is equivalent behaviour to Barman and ensures that any WALs which
        were not cleaned up in a multi-timeline scenario do eventually get
        cleaned up once all backups which reference them are gone.
        """
        # GIVEN a backup catalog with two backups with begin_wal values
        backup_id = "20210723T095432"
        oldest_backup_id = "20210722T095432"
        begin_wals = {
            backup_id: "000000020000000000000076",
            oldest_backup_id: "000000020000000000000075",
        }
        backup_metadata = self._create_backup_metadata(
            [backup_id, oldest_backup_id], begin_wals=begin_wals
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of WALs across two timelines where timeline 1 has no backups
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata,
            wals=[
                "000000020000000000000075",
                "000000020000000000000076",
                "000000010000000000000076",
                "000000010000000000000077",
            ],
        )

        # WHEN barman-cloud-backup-delete runs, specifying the oldest backup ID
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--backup-id", oldest_backup_id]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # that backup
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock,
            backup_metadata,
            [oldest_backup_id],
            # AND we expect the WALs on the other timeline to have been cleaned up
            # when that backup was deleted, along with eligible WALs on the deleted
            # backup's timeline
            wals={
                oldest_backup_id: [
                    "wals/000000010000000000000076.gz",
                    "wals/000000010000000000000077.gz",
                    "wals/000000020000000000000075.gz",
                ]
            },
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_no_wal_cleanup_when_oldest_is_keep_full(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """
        Tests that no WALs are cleaned up when the oldest backup is archival
        with a keep:full recovery target.
        """
        # GIVEN a backup catalog with three backups with begin_wal values
        backup_id = "20210724T095432"
        target_backup_id = "20210723T095432"
        oldest_backup_id = "20210722T095432"
        begin_wals = {
            backup_id: "000000010000000000000077",
            target_backup_id: "000000010000000000000075",
            oldest_backup_id: "000000010000000000000073",
        }
        backup_metadata = self._create_backup_metadata(
            [backup_id, target_backup_id, oldest_backup_id], begin_wals=begin_wals
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of five WALs
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata,
            wals=interpolate_wals(
                "000000010000000000000073", "000000010000000000000077"
            ),
        )

        # AND the oldest backup is archival with a full recovery target
        def get_keep_target(backup_id, use_cache=True):
            return backup_id == "20210722T095432" and KeepManager.TARGET_FULL or None

        cloud_backup_catalog_mock.return_value.get_keep_target.side_effect = (
            get_keep_target
        )

        # WHEN barman-cloud-backup-delete runs, specifying the backup ID of the second
        # oldest backup
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--backup-id", target_backup_id]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # that backup and no WALs were deleted
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock, backup_metadata, [target_backup_id], wals={}
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_no_wal_cleanup_when_oldest_is_keep_standalone(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """
        Tests that no WALs are cleaned up when the oldest backup is archival
        with a keep:standalone recovery target.
        """
        # GIVEN a backup catalog with three backups with begin_wal and end_wal values
        backup_id = "20210724T095432"
        target_backup_id = "20210723T095432"
        oldest_backup_id = "20210722T095432"
        begin_wals = {
            backup_id: "000000010000000000000077",
            target_backup_id: "000000010000000000000075",
            oldest_backup_id: "000000010000000000000073",
        }
        end_wals = {
            backup_id: "000000010000000000000078",
            target_backup_id: "000000010000000000000076",
            oldest_backup_id: "000000010000000000000074",
        }
        backup_metadata = self._create_backup_metadata(
            [backup_id, target_backup_id, oldest_backup_id],
            begin_wals=begin_wals,
            end_wals=end_wals,
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of five WALs
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata,
            wals=interpolate_wals(
                "000000010000000000000073", "000000010000000000000077"
            ),
        )

        # AND the oldest backup is archival with a standalone recovery target
        def get_keep_target(backup_id, use_cache=True):
            return (
                backup_id == "20210722T095432" and KeepManager.TARGET_STANDALONE or None
            )

        cloud_backup_catalog_mock.return_value.get_keep_target.side_effect = (
            get_keep_target
        )

        # WHEN barman-cloud-backup-delete runs, specifying the backup ID of the
        # second oldest backup
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--backup-id", target_backup_id]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # that backup and no WALs were deleted
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock, backup_metadata, [target_backup_id], wals={}
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_wals_cleanup_when_oldest_is_keep_standalone_deletion_by_retention(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """
        Tests >=oldest.begin_wal and <=oldest.end_wal are preserved when the
        oldest backup is archival with keep:standalone and we are deleting by
        retention policy.
        """
        # GIVEN a backup catalog with three backups with begin_wal and end_wal values
        backup_id = "20210724T095432"
        target_backup_id = "20210723T095432"
        oldest_backup_id = "20210722T095432"
        begin_wals = {
            backup_id: "00000001000000000000007B",
            target_backup_id: "000000010000000000000077",
            oldest_backup_id: "000000010000000000000073",
        }
        end_wals = {
            backup_id: "00000001000000000000007D",
            target_backup_id: "000000010000000000000079",
            oldest_backup_id: "000000010000000000000075",
        }
        backup_metadata = self._create_backup_metadata(
            [backup_id, target_backup_id, oldest_backup_id],
            begin_wals=begin_wals,
            end_wals=end_wals,
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of WALs
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata,
            wals=interpolate_wals(
                "000000010000000000000072", "00000001000000000000007D"
            ),
        )

        # AND the oldest backup is archival with a standalone recovery target
        def get_keep_target(backup_id, use_cache=True):
            return (
                backup_id == "20210722T095432" and KeepManager.TARGET_STANDALONE or None
            )

        cloud_backup_catalog_mock.return_value.get_keep_target.side_effect = (
            get_keep_target
        )

        # WHEN barman-cloud-backup-delete runs, specifying a redundancy retention
        # policy
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--retention-policy", "REDUNDANCY 1"]
        )

        # THEN the cloud interface was used to delete the files associated with
        # the second oldest backup (the newest is required to meet the policy and
        # the oldest is an archival backup)
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock,
            backup_metadata,
            [target_backup_id],
            # AND we expect only the WALs before the archival backup begin_wal and after
            # the archival backup end_wal but before the latest backup begin_wal to have
            # been deleted
            wals={
                target_backup_id: [
                    "wals/000000010000000000000072.gz",
                    "wals/000000010000000000000076.gz",
                    "wals/000000010000000000000077.gz",
                    "wals/000000010000000000000078.gz",
                    "wals/000000010000000000000079.gz",
                    "wals/00000001000000000000007A.gz",
                ]
            },
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_wals_cleanup_when_all_oldest_are_keep_standalone_deletion_by_retention(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """
        Tests >=oldest.begin_wal and <=oldest.end_wal are preserved for all
        standalone archival backups when all backups up to oldest are standalone
        and we are deleting by retention policy.
        """
        # GIVEN a backup catalog with four backups with begin_wal and end_wal values
        backup_id = "20210724T095432"
        target_backup_id = "20210723T095432"
        second_oldest_backup_id = "20210722T095432"
        oldest_backup_id = "20210721T095432"
        begin_wals = {
            backup_id: "00000001000000000000007B",
            target_backup_id: "000000010000000000000077",
            second_oldest_backup_id: "000000010000000000000073",
            oldest_backup_id: "00000001000000000000006F",
        }
        end_wals = {
            backup_id: "00000001000000000000007D",
            target_backup_id: "000000010000000000000079",
            second_oldest_backup_id: "000000010000000000000075",
            oldest_backup_id: "000000010000000000000071",
        }
        backup_metadata = self._create_backup_metadata(
            [backup_id, target_backup_id, second_oldest_backup_id, oldest_backup_id],
            begin_wals=begin_wals,
            end_wals=end_wals,
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of WALs
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata,
            wals=interpolate_wals(
                "00000001000000000000006E", "00000001000000000000007D"
            ),
        )

        # AND the oldest two backups are archival with a standalone recovery target
        def get_keep_target(backup_id, use_cache=True):
            return (
                backup_id == "20210721T095432"
                and KeepManager.TARGET_STANDALONE
                or backup_id == "20210722T095432"
                and KeepManager.TARGET_STANDALONE
                or None
            )

        cloud_backup_catalog_mock.return_value.get_keep_target.side_effect = (
            get_keep_target
        )

        # WHEN barman-cloud-backup-delete runs, specifying a redundancy retention
        # policy
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--retention-policy", "REDUNDANCY 1"]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # the oldest non-archival backup
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock,
            backup_metadata,
            [target_backup_id],
            # AND we expect only the WALs before the archival backup begin_wal and after
            # the archival backup end_wal but before the latest backup begin_wal to have
            # been deleted
            wals={
                target_backup_id: [
                    "wals/00000001000000000000006E.gz",
                    "wals/000000010000000000000072.gz",
                    "wals/000000010000000000000076.gz",
                    "wals/000000010000000000000077.gz",
                    "wals/000000010000000000000078.gz",
                    "wals/000000010000000000000079.gz",
                    "wals/00000001000000000000007A.gz",
                ]
            },
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_wals_cleanup_when_oldest_two_nokeep_and_standalone_deletion_by_retention(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """
        Tests >=oldest.begin_wal and <=oldest.end_wal are preserved for the
        standalone archival backup when the oldest backup is not archival, the
        second oldest is archival both are out-of-policy.
        """
        # GIVEN a backup catalog with four backups with begin_wal and end_wal values
        backup_id = "20210724T095432"
        target_backup_id = "20210723T095432"
        second_oldest_backup_id = "20210722T095432"
        oldest_backup_id = "20210721T095432"
        begin_wals = {
            backup_id: "00000001000000000000007B",
            target_backup_id: "000000010000000000000077",
            second_oldest_backup_id: "000000010000000000000073",
            oldest_backup_id: "00000001000000000000006F",
        }
        end_wals = {
            backup_id: "00000001000000000000007D",
            target_backup_id: "000000010000000000000079",
            second_oldest_backup_id: "000000010000000000000075",
            oldest_backup_id: "000000010000000000000071",
        }
        backup_metadata = self._create_backup_metadata(
            [backup_id, target_backup_id, second_oldest_backup_id, oldest_backup_id],
            begin_wals=begin_wals,
            end_wals=end_wals,
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of WALs
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata,
            wals=interpolate_wals(
                "00000001000000000000006E", "00000001000000000000007D"
            ),
        )

        # AND the second oldest backup is archival with a standalone recovery target
        def get_keep_target(backup_id, use_cache=True):
            return (
                backup_id == "20210722T095432" and KeepManager.TARGET_STANDALONE or None
            )

        cloud_backup_catalog_mock.return_value.get_keep_target.side_effect = (
            get_keep_target
        )

        # WHEN barman-cloud-backup-delete runs, specifying a redundancy retention
        # policy
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--retention-policy", "REDUNDANCY 1"]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # the two out-of-policy backups which are not archival
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock,
            backup_metadata,
            [oldest_backup_id, target_backup_id],
            wals={
                # AND all WALs for the oldest backup up to the next backup were
                # deleted because it was non-archival
                oldest_backup_id: [
                    "wals/00000001000000000000006E.gz",
                    "wals/00000001000000000000006F.gz",
                    "wals/000000010000000000000070.gz",
                    "wals/000000010000000000000071.gz",
                    "wals/000000010000000000000072.gz",
                ],
                # AND all WALs from but not including the end_wal of the archival
                # backup up to but not including the begin_wal of the newest backup
                # were deleted - therefore implicitly the WALs of the archival backup
                # were preserved
                target_backup_id: [
                    "wals/000000010000000000000076.gz",
                    "wals/000000010000000000000077.gz",
                    "wals/000000010000000000000078.gz",
                    "wals/000000010000000000000079.gz",
                    "wals/00000001000000000000007A.gz",
                ],
            },
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_no_wal_cleanup_when_oldest_two_full_and_standalone(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """
        Tests >=oldest.begin_wal and <=oldest.end_wal are preserved for the
        standalone archival backup when the oldest backup is not archival, the
        second oldest is archival both are out-of-policy.
        """
        # GIVEN a backup catalog with four backups with begin_wal and end_wal values
        backup_id = "20210724T095432"
        target_backup_id = "20210723T095432"
        second_oldest_backup_id = "20210722T095432"
        oldest_backup_id = "20210721T095432"
        begin_wals = {
            backup_id: "00000001000000000000007B",
            target_backup_id: "000000010000000000000077",
            second_oldest_backup_id: "000000010000000000000073",
            oldest_backup_id: "00000001000000000000006F",
        }
        end_wals = {
            backup_id: "00000001000000000000007D",
            target_backup_id: "000000010000000000000079",
            second_oldest_backup_id: "000000010000000000000075",
            oldest_backup_id: "000000010000000000000071",
        }
        backup_metadata = self._create_backup_metadata(
            [backup_id, target_backup_id, second_oldest_backup_id, oldest_backup_id],
            begin_wals=begin_wals,
            end_wals=end_wals,
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of WALs
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata,
            wals=interpolate_wals(
                "00000001000000000000006E", "00000001000000000000007D"
            ),
        )

        # AND the oldest backup is a full archival backup and the second oldest
        # backup is a standalone archival backup
        def get_keep_target(backup_id, use_cache=True):
            return (
                backup_id == "20210721T095432"
                and KeepManager.TARGET_FULL
                or backup_id == "20210722T095432"
                and KeepManager.TARGET_STANDALONE
                or None
            )

        cloud_backup_catalog_mock.return_value.get_keep_target.side_effect = (
            get_keep_target
        )

        # WHEN barman-cloud-backup-delete runs, specifying a redundancy retention
        # policy
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--retention-policy", "REDUNDANCY 1"]
        )

        # THEN the cloud interface was only used to delete the files associated with
        # the non-archival, out-of-policy backup and no WALs were deleted
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock,
            backup_metadata,
            [target_backup_id],
            # AND no WALs are cleaned up
            wals={},
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_backup_wal_preserved_when_oldest_is_keep_standalone_deletion_by_retention(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock
    ):
        """
        Verify .backup WALs are preserved for standalone archival backups.
        """
        # GIVEN a backup catalog with three backups with begin_wal and end_wal values
        backup_id = "20210724T095432"
        target_backup_id = "20210723T095432"
        oldest_backup_id = "20210722T095432"
        begin_wals = {
            backup_id: "00000001000000000000007B",
            target_backup_id: "000000010000000000000077",
            oldest_backup_id: "000000010000000000000073",
        }
        end_wals = {
            backup_id: "00000001000000000000007D",
            target_backup_id: "000000010000000000000079",
            oldest_backup_id: "000000010000000000000075",
        }
        backup_metadata = self._create_backup_metadata(
            [backup_id, target_backup_id, oldest_backup_id],
            begin_wals=begin_wals,
            end_wals=end_wals,
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of WALs, including a .backup WAL for the oldest backup
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata,
            wals=interpolate_wals(
                "000000010000000000000072", "000000010000000000000075"
            )
            + ["000000010000000000000075.00000028.backup"]
            + interpolate_wals("000000010000000000000076", "00000001000000000000007D"),
        )

        # AND the oldest backup is archival with a standalone recovery target
        def get_keep_target(backup_id, use_cache=True):
            return (
                backup_id == "20210722T095432" and KeepManager.TARGET_STANDALONE or None
            )

        cloud_backup_catalog_mock.return_value.get_keep_target.side_effect = (
            get_keep_target
        )

        # WHEN barman-cloud-backup-delete runs, specifying a redundancy retention
        # policy
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--retention-policy", "REDUNDANCY 1"]
        )

        # THEN the cloud interface was used to delete the files associated with
        # the second oldest backup (the newest is required to meet the policy and
        # the oldest is an archival backup)
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock,
            backup_metadata,
            [target_backup_id],
            # AND we expect only the WALs before the archival backup begin_wal and after
            # the archival backup end_wal but before the latest backup begin_wal to have
            # been deleted
            wals={
                target_backup_id: [
                    "wals/000000010000000000000072.gz",
                    "wals/000000010000000000000076.gz",
                    "wals/000000010000000000000077.gz",
                    "wals/000000010000000000000078.gz",
                    "wals/000000010000000000000079.gz",
                    "wals/00000001000000000000007A.gz",
                ]
            },
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_error_on_delete_wal(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, caplog
    ):
        """
        Test that when the cloud interface returns an error when deleting WALs
        we log the error but continue deleting backups.
        """
        # GIVEN a backup catalog with four backups with begin_wal values
        out_of_policy_backup_ids = ["20210722T095432", "20210723T095432"]
        in_policy_backup_ids = ["20210724T095432", "20210725T095432"]
        begin_wals = {
            out_of_policy_backup_ids[0]: "000000010000000000000076",
            out_of_policy_backup_ids[1]: "000000010000000000000078",
            in_policy_backup_ids[0]: "00000001000000000000007A",
            in_policy_backup_ids[1]: "00000001000000000000007C",
        }
        backup_metadata = self._create_backup_metadata(
            out_of_policy_backup_ids + in_policy_backup_ids, begin_wals=begin_wals
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of eight WALs
        wals = [
            "000000010000000000000075",
            "000000010000000000000076",
            "000000010000000000000077",
            "000000010000000000000078",
            "000000010000000000000079",
            "00000001000000000000007A",
            "00000001000000000000007B",
            "00000001000000000000007C",
        ]
        cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata, wals=wals
        )

        # AND the cloud provider returns an error on delete via CloudInterface
        # the first time it is called with WALs
        cloud_interface_mock = get_cloud_interface_mock.return_value
        self._should_error = True

        def mock_delete_objects(objects):
            if any(o.split("/")[0] == "wals" for o in objects) and self._should_error:
                self._should_error = False
                raise Exception("Something went wrong on delete")
            else:
                return True

        cloud_interface_mock.delete_objects.side_effect = mock_delete_objects

        # WHEN barman-cloud-backup-delete runs, specifying a redundancy policy with
        # one copy
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--retention-policy", "REDUNDANCY 2"]
        )

        # THEN an error was logged when the first backup could not be deleted
        assert (
            "Could not delete the following WALs for backup 20210722T095432: "
            "['wals/000000010000000000000075.gz', 'wals/000000010000000000000076.gz', "
            "'wals/000000010000000000000077.gz'], Reason: Something went wrong on "
            "delete" in caplog.text
        )

        # AND the cloud interface was only used to delete the files associated with
        # the out-of-policy backups
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock,
            backup_metadata,
            out_of_policy_backup_ids,
            # AND we expect the WALs for each backup to have been cleaned up after each
            # backup deletion
            wals={
                out_of_policy_backup_ids[0]: [
                    "wals/000000010000000000000075.gz",
                    "wals/000000010000000000000076.gz",
                    "wals/000000010000000000000077.gz",
                ],
                # AND the WALs which could not be deleted with the first backup are cleaned
                # up after deletion of the second backup
                out_of_policy_backup_ids[1]: [
                    "wals/000000010000000000000075.gz",
                    "wals/000000010000000000000076.gz",
                    "wals/000000010000000000000077.gz",
                    "wals/000000010000000000000078.gz",
                    "wals/000000010000000000000079.gz",
                ],
            },
        )

    @mock.patch("barman.clients.cloud_backup_delete.CloudBackupCatalog")
    @mock.patch("barman.clients.cloud_backup_delete.get_cloud_interface")
    def test_error_on_list_wal(
        self, get_cloud_interface_mock, cloud_backup_catalog_mock, caplog
    ):
        """
        Test that when the cloud interface returns an error when listing WALs
        we log the error but continue deleting backups.
        """
        # GIVEN a backup catalog with four backups with begin_wal values
        out_of_policy_backup_ids = ["20210722T095432", "20210723T095432"]
        in_policy_backup_ids = ["20210724T095432", "20210725T095432"]
        begin_wals = {
            out_of_policy_backup_ids[0]: "000000010000000000000076",
            out_of_policy_backup_ids[1]: "000000010000000000000078",
            in_policy_backup_ids[0]: "00000001000000000000007A",
            in_policy_backup_ids[1]: "00000001000000000000007C",
        }
        backup_metadata = self._create_backup_metadata(
            out_of_policy_backup_ids + in_policy_backup_ids, begin_wals=begin_wals
        )

        # AND a CloudBackupCatalog which returns the backup_info for only those backups
        # and a list of eight WALs
        wals = [
            "000000010000000000000075",
            "000000010000000000000076",
            "000000010000000000000077",
            "000000010000000000000078",
            "000000010000000000000079",
            "00000001000000000000007A",
            "00000001000000000000007B",
            "00000001000000000000007C",
        ]

        catalog = cloud_backup_catalog_mock.return_value = self._create_catalog(
            backup_metadata, wals=wals
        )
        # AND the cloud provider returns an error when listing WALs the first time it is
        # called
        self._should_error = True

        original_get_wal_paths_mock = catalog.get_wal_paths.side_effect

        def mock_get_wal_paths():
            if self._should_error:
                self._should_error = False
                raise Exception("Something went wrong")
            else:
                return original_get_wal_paths_mock()

        catalog.get_wal_paths.side_effect = mock_get_wal_paths

        # WHEN barman-cloud-backup-delete runs, specifying a redundancy policy with
        # one copy
        cloud_backup_delete.main(
            ["cloud_storage_url", "test_server", "--retention-policy", "REDUNDANCY 2"]
        )

        # THEN an error was logged when the WALs for the first backup could not be listed
        assert (
            "Cannot clean up WALs for backup 20210722T095432 because an error "
            "occurred listing WALs: Something went wrong" in caplog.text
        )

        # AND the cloud interface was only used to delete the files associated with
        # the out-of-policy backups
        self._verify_only_these_backups_deleted(
            get_cloud_interface_mock,
            backup_metadata,
            out_of_policy_backup_ids,
            # AND we expect the WALs to have been cleaned up only after the second backup
            # was deleted (when listing the WAL paths succeeded)
            wals={
                # AND the WAL which could not be deleted with the first backup is cleaned
                # up after deletion of the second backup
                out_of_policy_backup_ids[1]: [
                    "wals/000000010000000000000075.gz",
                    "wals/000000010000000000000076.gz",
                    "wals/000000010000000000000077.gz",
                    "wals/000000010000000000000078.gz",
                    "wals/000000010000000000000079.gz",
                ]
            },
        )
