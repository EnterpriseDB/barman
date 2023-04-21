# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2023
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

import logging
import mock
import pytest

from google.api_core.exceptions import NotFound

from barman.cloud import CloudProviderError

from barman.cloud_providers import (
    CloudProviderUnsupported,
    get_snapshot_interface,
    get_snapshot_interface_from_backup_info,
    get_snapshot_interface_from_server_config,
)
from barman.cloud_providers.google_cloud_storage import GcpCloudSnapshotInterface
from barman.exceptions import (
    BarmanException,
    ConfigurationException,
    SnapshotBackupException,
)


class TestGetSnapshotInterface(object):
    """
    Verify get_snapshot_interface creates the required CloudSnapshotInterface
    """

    @pytest.mark.parametrize(
        ("snapshot_provider", "interface_cls"),
        [("aws", None), ("azure", None), ("gcp", GcpCloudSnapshotInterface)],
    )
    @mock.patch(
        "barman.cloud_providers.google_cloud_storage.import_google_cloud_compute"
    )
    def test_from_config_cloud_provider(
        self, _mock_google_cloud_compute, snapshot_provider, interface_cls
    ):
        """Verify supported and unsupported cloud providers with server config."""
        # GIVEN a server config with the specified snapshot provider
        mock_config = mock.Mock(snapshot_provider=snapshot_provider)

        # WHEN get_snapshot_interface_from_server_config is called
        if interface_cls:
            # THEN supported providers return the expected interface
            assert isinstance(
                get_snapshot_interface_from_server_config(mock_config), interface_cls
            )
        else:
            # AND unsupported providers raise the expected exception
            with pytest.raises(CloudProviderUnsupported) as exc:
                get_snapshot_interface_from_server_config(mock_config)
            assert "Unsupported snapshot provider: {}".format(snapshot_provider) == str(
                exc.value
            )

    def test_from_config_gcp_no_project(self):
        """
        Verify an exception is raised for gcp snapshots with no project in server config.
        """
        # GIVEN a server config with the gcp snapshot provider and no snapshot_gcp_project
        mock_config = mock.Mock(snapshot_provider="gcp", snapshot_gcp_project=None)
        # WHEN get snapshot_interface_from_server_config is called
        with pytest.raises(ConfigurationException) as exc:
            get_snapshot_interface_from_server_config(mock_config)
        # THEN the expected exception is raised
        assert (
            "snapshot_gcp_project option must be set when snapshot_provider is gcp"
            in str(exc.value)
        )

    @pytest.mark.parametrize(
        ("snapshot_provider", "interface_cls"),
        [("aws", None), ("azure", None), ("gcp", GcpCloudSnapshotInterface)],
    )
    @mock.patch(
        "barman.cloud_providers.google_cloud_storage.import_google_cloud_compute"
    )
    def test_from_backup_info_cloud_provider(
        self, _mock_google_cloud_compute, snapshot_provider, interface_cls
    ):
        """Verify supported and unsupported cloud providers with backup_info."""
        # GIVEN a backup_info with snapshots_info containing the specified snapshot
        # provider
        mock_backup_info = mock.Mock(
            snapshots_info=mock.Mock(provider=snapshot_provider)
        )

        # WHEN get_snapshot_interface_from_server_config is called
        if interface_cls:
            # THEN supported providers return the expected interface
            assert isinstance(
                get_snapshot_interface_from_backup_info(mock_backup_info), interface_cls
            )
        else:
            # AND unsupported providers raise the expected exception
            with pytest.raises(CloudProviderUnsupported) as exc:
                get_snapshot_interface_from_backup_info(mock_backup_info)
            assert "Unsupported snapshot provider in backup info: {}".format(
                snapshot_provider
            ) == str(exc.value)

    def test_from_backup_info_gcp_no_project(self):
        """
        Verify an exception is raised for gcp snapshots with no project in backup_info.
        """
        # GIVEN a server config with the gcp snapshot provider and no snapshot_gcp_project
        mock_backup_info = mock.Mock(
            snapshots_info=mock.Mock(provider="gcp", project=None)
        )
        # WHEN get snapshot_interface_from_backup_info is called
        with pytest.raises(BarmanException) as exc:
            get_snapshot_interface_from_backup_info(mock_backup_info)
        # THEN the expected exception is raised
        assert "backup_info has snapshot provider 'gcp' but project is not set" in str(
            exc.value
        )

    @pytest.mark.parametrize(
        ("cloud_provider", "interface_cls"),
        [
            ("aws-s3", None),
            ("azure-blob-storage", None),
            ("google-cloud-storage", GcpCloudSnapshotInterface),
        ],
    )
    @mock.patch(
        "barman.cloud_providers.google_cloud_storage.import_google_cloud_compute"
    )
    def test_from_args_cloud_provider(
        self, _mock_google_cloud_compute, cloud_provider, interface_cls
    ):
        """Verify supported and unsupported cloud providers with config args."""
        # GIVEN a cloud config with the specified snapshot provider
        mock_config = mock.Mock(cloud_provider=cloud_provider)

        # WHEN get_snapshot_interface_from_server_config is called
        if interface_cls:
            # THEN supported providers return the expected interface
            assert isinstance(get_snapshot_interface(mock_config), interface_cls)
        else:
            # AND unsupported providers raise the expected exception
            with pytest.raises(CloudProviderUnsupported) as exc:
                get_snapshot_interface(mock_config)
            assert "No snapshot provider for cloud provider: {}".format(
                cloud_provider
            ) == str(exc.value)

    def test_from_args_gcp_no_project(self):
        """
        Verify an exception is raised for gcp snapshots with no project in args.
        """
        # GIVEN a cloud config with the specified snapshot provider and a missing
        # snapshot_gcp_project argument
        mock_config = mock.Mock(
            cloud_provider="google-cloud-storage", snapshot_gcp_project=None
        )

        # WHEN get snapshot_interface_from_backup_info is called
        with pytest.raises(ConfigurationException) as exc:
            get_snapshot_interface(mock_config)
        # AND the exception has the expected message
        assert (
            "--snapshot-gcp-project option must be set for snapshot backups when "
            "cloud provider is google-cloud-storage"
        ) == str(exc.value)


class TestGcpCloudSnapshotInterface(object):
    """
    Verify behaviour of the GcpCloudSnapshotInterface class.
    """

    backup_id = "20380119T031407"
    gcp_disks = (
        {
            "name": "test_disk_0",
            "device_name": "dev0",
            "physical_block_size": 1024,
            "size_gb": 1,
        },
        {
            "name": "test_disk_1",
            "device_name": "dev1",
            "physical_block_size": 2048,
            "size_gb": 10,
        },
        {
            "name": "test_disk_2",
            "device_name": "dev2",
            "physical_block_size": 4096,
            "size_gb": 100,
        },
    )
    gcp_zone = "us-east1-b"
    gcp_project = "test_project"
    gcp_instance_name = "test_instance"
    server_name = "test_server"

    def _get_disk_link(self, project, zone, disk_name):
        return "projects/{}/zones/{}/disks/{}".format(project, zone, disk_name)

    def _get_snapshot_name(self, disk_name):
        return "{}-{}".format(disk_name, self.backup_id.lower())

    def _get_device_path(self, device):
        return "/dev/disk/by-id/google-{}".format(device)

    @mock.patch(
        "barman.cloud_providers.google_cloud_storage.import_google_cloud_compute"
    )
    def test_init_with_null_project(self, _mock_google_cloud_compute):
        """
        Verify creating GcpCloudSnapshotInterface fails if gcp_project is not set.
        """
        # GIVEN a null project
        gcp_project = None

        # WHEN a GcpCloudSnapshotInterface is created
        # THEN a TypeError is raised
        with pytest.raises(TypeError) as exc:
            GcpCloudSnapshotInterface(gcp_project)

        # AND the expected message is included
        assert str(exc.value) == "project cannot be None"

    @pytest.fixture
    def mock_google_cloud_compute(self):
        with mock.patch(
            "barman.cloud_providers.google_cloud_storage.import_google_cloud_compute"
        ) as mock_import_google_cloud_compute:
            yield mock_import_google_cloud_compute.return_value

    def test_init(self, mock_google_cloud_compute):
        """
        Verify creating GcpCloudSnapshotInterface creates the necessary GCP clients.
        """
        # GIVEN a non-null project
        gcp_project = self.gcp_project
        # WHEN a GcpCloudSnapshotInterface is created
        snapshot_interface = GcpCloudSnapshotInterface(gcp_project)
        # THEN a SnapshotsClient was created
        assert snapshot_interface.client == (
            mock_google_cloud_compute.SnapshotsClient.return_value
        )
        # AND a DisksClient was created
        assert snapshot_interface.disks_client == (
            mock_google_cloud_compute.DisksClient.return_value
        )
        # AND an InstancesClient was created
        assert snapshot_interface.instances_client == (
            mock_google_cloud_compute.InstancesClient.return_value
        )

    def test_take_snapshot(self, mock_google_cloud_compute, caplog):
        """
        Verify that _take_snapshot calls the GCP library and waits for the result.
        """
        # GIVEN a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project)
        # AND a backup_info for a given server name and backup ID
        backup_info = mock.Mock(backup_id=self.backup_id, server_name=self.server_name)
        # AND a mock SnapshotsClient which returns a successful response
        mock_snapshots_client = mock_google_cloud_compute.SnapshotsClient.return_value
        mock_resp = mock_snapshots_client.insert.return_value
        mock_resp.result.return_value = True
        mock_resp.error_code = None
        mock_resp.warnings = None
        # AND log level is INFO
        caplog.set_level(logging.INFO)

        # WHEN _take_snapshot is called
        snapshot_name = snapshot_interface._take_snapshot(
            backup_info,
            self.gcp_zone,
            self.gcp_disks[0]["name"],
        )

        # THEN insert is called on the SnapshotsClient with the expected args
        expected_disk_name = self.gcp_disks[0]["name"]
        expected_full_disk_name = self._get_disk_link(
            self.gcp_project,
            self.gcp_zone,
            self.gcp_disks[0]["name"],
        )
        expected_snapshot_name = self._get_snapshot_name(expected_disk_name)
        mock_snapshots_client.insert.assert_called_once_with(
            {
                "project": self.gcp_project,
                "snapshot_resource": {
                    "name": expected_snapshot_name,
                    "source_disk": expected_full_disk_name,
                },
            }
        )
        # AND result() was called on the response to await completion of the snapshot
        mock_resp.result.assert_called_once()
        # AND the name of the snapshot was returned
        assert snapshot_name == expected_snapshot_name
        # AND the expected log output occurred
        expected_log_content = (
            "Taking snapshot '{}' of disk '{}'".format(
                expected_snapshot_name, expected_disk_name
            ),
            "Waiting for snapshot '{}' completion".format(expected_snapshot_name),
            "Snapshot '{}' completed".format(expected_snapshot_name),
        )
        for expected_log, log_line in zip(
            expected_log_content, caplog.text.split("\n")
        ):
            assert expected_log in log_line

    def test_take_snapshot_warnings(self, mock_google_cloud_compute, caplog):
        """
        Verify that warnings are logged if present in the snapshots response.
        """
        # GIVEN a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project)
        # AND a backup_info for a given server name and backup ID
        backup_info = mock.Mock(backup_id=self.backup_id, server_name=self.server_name)
        # AND a mock SnapshotsClient which returns a successful response
        mock_snapshots_client = mock_google_cloud_compute.SnapshotsClient.return_value
        mock_resp = mock_snapshots_client.insert.return_value
        mock_resp.result.return_value = True
        mock_resp.error_code = None
        # AND the response has warnings
        mock_resp.warnings = [mock.Mock(code="123", message="warning message")]

        # WHEN _take_snapshot is called
        snapshot_name = snapshot_interface._take_snapshot(
            backup_info,
            self.gcp_zone,
            self.gcp_disks[0]["name"],
        )

        # THEN the warning is included in the log output
        assert (
            "Warnings encountered during snapshot {}: 123:warning message".format(
                snapshot_name
            )
            in caplog.text
        )

    def test_take_snapshot_failed(self, mock_google_cloud_compute):
        """
        Verify that _take_snapshot raises an exception on failure.
        """
        # GIVEN a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project)
        # AND a backup_info for a given server name and backup ID
        backup_info = mock.Mock(backup_id=self.backup_id, server_name=self.server_name)
        # AND a mock SnapshotsClient which returns a failed response
        mock_snapshots_client = mock_google_cloud_compute.SnapshotsClient.return_value
        mock_resp = mock_snapshots_client.insert.return_value
        mock_resp.result.return_value = True
        mock_resp.error_code = "503"
        mock_resp.error_message = "test error message"

        # WHEN _take_snapshot is called
        # THEN a CloudProviderError is raised
        with pytest.raises(CloudProviderError) as exc:
            snapshot_interface._take_snapshot(
                backup_info,
                self.gcp_zone,
                self.gcp_disks[0]["name"],
            )

        # AND the exception message contains the snapshot name, error code and error
        # message
        expected_snapshot_name = self._get_snapshot_name(self.gcp_disks[0]["name"])
        expected_message = "Snapshot '{}' failed with error code {}: {}".format(
            expected_snapshot_name,
            mock_resp.error_code,
            mock_resp.error_message,
        )
        assert str(exc.value) == expected_message

    def _get_mock_instances_client(self, gcp_project, gcp_zone, gcp_instance, disks):
        """
        Helper which create a mock instances client for the given project/zone/instance
        with the specified disks attached as the specified device.
        """

        def get_fun(instance, zone, project):
            if instance == gcp_instance and zone == gcp_zone and project == gcp_project:
                mock_attached_disks = [
                    mock.Mock(
                        device_name=disk["device_name"],
                        source=self._get_disk_link(project, zone, disk["name"]),
                    )
                    for disk in disks
                ]
                return mock.Mock(disks=mock_attached_disks)
            else:
                raise NotFound("instance not found")

        return mock.Mock(get=get_fun)

    def _get_mock_disks_client(self, gcp_project, gcp_zone, disks):
        """
        Helper which creates a mock disks client for the given project/zone with the
        specified disks available with the specified physical_block_size and size_gb.
        """
        disk_metadata = dict(
            (
                disk["name"],
                mock.Mock(
                    physical_block_size_bytes=disk["physical_block_size"],
                    self_link="projects/{}/zones/{}/disks/{}".format(
                        gcp_project, gcp_zone, disk["name"]
                    ),
                    size_gb=disk["size_gb"],
                    source_snapshot="source_snapshot" in disk
                    and disk["source_snapshot"]
                    or None,
                ),
            )
            for disk in disks
        )

        def get_fun(disk, zone, project):
            if zone == self.gcp_zone and project == self.gcp_project:
                try:
                    return disk_metadata[disk]
                except KeyError:
                    raise NotFound("disk not found")

        return mock.Mock(get=get_fun)

    def _get_mock_snapshots_client(self):
        """
        Helper which returns a mock snapshots client that always succeeds.
        """
        snapshots_client = mock.Mock()
        snapshots_client.insert.return_value = mock.Mock(error_code=None, warnings=None)
        snapshots_client.delete.return_value = mock.Mock(error_code=None, warnings=None)
        return snapshots_client

    @pytest.mark.parametrize("number_of_disks", (1, 2, 3))
    def test_take_snapshot_backup(
        self,
        number_of_disks,
        mock_google_cloud_compute,
    ):
        """
        Verify that take_snapshot_backup takes the required snapshots and updates the
        backup_info when prerequisites are met.
        """
        # GIVEN a set of disks
        disks = self.gcp_disks[:number_of_disks]
        # AND a backup_info for a given server name and backup ID
        backup_info = mock.Mock(backup_id=self.backup_id, server_name=self.server_name)
        # AND a mock InstancesClient which returns an instance with the required disks
        # attached
        mock_instances_client = self._get_mock_instances_client(
            self.gcp_project, self.gcp_zone, self.gcp_instance_name, disks
        )
        mock_google_cloud_compute.InstancesClient.return_value = mock_instances_client
        # AND a mock DisksClient which returns the required disks
        mock_disks_client = self._get_mock_disks_client(
            self.gcp_project, self.gcp_zone, disks
        )
        mock_google_cloud_compute.DisksClient.return_value = mock_disks_client
        # AND a mock SnapshotsClient which returns successful responses
        mock_google_cloud_compute.SnapshotsClient.return_value = (
            self._get_mock_snapshots_client()
        )
        # AND a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project, self.gcp_zone)

        # WHEN take_snapshot_backup is called for multiple disks
        snapshot_interface.take_snapshot_backup(
            backup_info,
            self.gcp_instance_name,
            (disk["name"] for disk in disks),
        )

        # THEN the backup_info is updated with the expected snapshot metadata
        snapshots_info = backup_info.snapshots_info
        assert snapshots_info.project == self.gcp_project
        assert snapshots_info.provider == "gcp"
        assert len(snapshots_info.snapshots) == len(disks)
        for disk in disks:
            snapshot_name = self._get_snapshot_name(disk["name"])
            snapshot = next(
                snapshot
                for snapshot in snapshots_info.snapshots
                if snapshot.snapshot_name == snapshot_name
            )
            assert snapshot.identifier == snapshot_name
            assert snapshot.device == self._get_device_path(disk["device_name"])
            assert snapshot.snapshot_name == snapshot_name
            assert snapshot.snapshot_project == self.gcp_project
            assert snapshot.device_name == disk["device_name"]

    def test_take_snapshot_backup_instance_not_found(self, mock_google_cloud_compute):
        """
        Verify that a SnapshotBackupException is raised if the instance cannot be
        found.
        """
        # GIVEN a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project, self.gcp_zone)
        # AND a mock InstancesClient which cannot find the instance
        mock_instances_client = mock_google_cloud_compute.InstancesClient.return_value
        mock_instances_client.get.side_effect = NotFound("instance not found")

        # WHEN take_snapshot_backup is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(SnapshotBackupException) as exc:
            snapshot_interface.take_snapshot_backup(
                mock.Mock(), self.gcp_instance_name, self.gcp_disks
            )

        # AND the exception contains the expected message
        assert str(
            exc.value
        ) == "Cannot find instance with name {} in zone {} for project {}".format(
            self.gcp_instance_name, self.gcp_zone, self.gcp_project
        )

    def test_take_snapshot_backup_disk_not_found(
        self,
        mock_google_cloud_compute,
    ):
        """
        Verify that a SnapshotBackupException is raised if a disk cannot be found.
        """
        # GIVEN a set of disks
        disks = self.gcp_disks
        # AND a mock InstancesClient which returns an instance with a subset of the
        # required disks attached
        mock_instances_client = self._get_mock_instances_client(
            self.gcp_project, self.gcp_zone, self.gcp_instance_name, disks[:-1]
        )
        mock_google_cloud_compute.InstancesClient.return_value = mock_instances_client
        # AND a mock DisksClient which returns only that same subset of disks
        mock_disks_client = self._get_mock_disks_client(
            self.gcp_project, self.gcp_zone, disks[:-1]
        )
        mock_google_cloud_compute.DisksClient.return_value = mock_disks_client
        # AND a mock SnapshotsClient which returns successful responses
        mock_google_cloud_compute.SnapshotsClient.return_value = (
            self._get_mock_snapshots_client()
        )
        # AND a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project, self.gcp_zone)

        # WHEN take snapshot_backup is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(SnapshotBackupException) as exc:
            snapshot_interface.take_snapshot_backup(
                mock.Mock(),
                self.gcp_instance_name,
                (disk["name"] for disk in disks),
            )

        # AND the exception contains the expected message
        assert str(
            exc.value
        ) == "Cannot find disk with name {} in zone {} for project {}".format(
            disks[-1]["name"], self.gcp_zone, self.gcp_project
        )

    def test_take_snapshot_backup_disk_not_attached(
        self,
        mock_google_cloud_compute,
    ):
        """
        Verify that a SnapshotBackupException is raised if a disk is not attached.
        """
        # GIVEN a set of disks
        disks = self.gcp_disks
        # AND a mock InstancesClient which returns an instance with a subset of the
        # required disks attached
        mock_instances_client = self._get_mock_instances_client(
            self.gcp_project, self.gcp_zone, self.gcp_instance_name, disks[:-1]
        )
        mock_google_cloud_compute.InstancesClient.return_value = mock_instances_client
        # AND a mock DisksClient which returns all required disks
        mock_disks_client = self._get_mock_disks_client(
            self.gcp_project, self.gcp_zone, disks
        )
        mock_google_cloud_compute.DisksClient.return_value = mock_disks_client
        # AND a mock SnapshotsClient which returns successful responses
        mock_google_cloud_compute.SnapshotsClient.return_value = (
            self._get_mock_snapshots_client()
        )
        # AND a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project, self.gcp_zone)

        # WHEN take snapshot_backup is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(SnapshotBackupException) as exc:
            snapshot_interface.take_snapshot_backup(
                mock.Mock(),
                self.gcp_instance_name,
                (disk["name"] for disk in disks),
            )

        # AND the exception contains the expected message
        assert str(exc.value) == "Disk {} not attached to instance {}".format(
            disks[-1]["name"], self.gcp_instance_name
        )

    def test_take_snapshot_backup_disk_attached_multiple_times(
        self,
        mock_google_cloud_compute,
    ):
        """
        Verify that a SnapshotBackupException is raised if a disk appears to be
        attached more than once.
        """
        # GIVEN a set of disks
        disks = self.gcp_disks
        # AND a mock InstancesClient which returns an instance where one named disk is
        # attached twice
        mock_instances_client = self._get_mock_instances_client(
            self.gcp_project, self.gcp_zone, self.gcp_instance_name, disks + disks[-1:]
        )
        mock_google_cloud_compute.InstancesClient.return_value = mock_instances_client
        # AND a mock DisksClient which returns all required disks
        mock_disks_client = self._get_mock_disks_client(
            self.gcp_project, self.gcp_zone, disks
        )
        mock_google_cloud_compute.DisksClient.return_value = mock_disks_client
        # AND a mock SnapshotsClient which returns successful responses
        mock_google_cloud_compute.SnapshotsClient.return_value = (
            self._get_mock_snapshots_client()
        )
        # AND a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project, self.gcp_zone)

        # WHEN take snapshot_backup is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(AssertionError):
            snapshot_interface.take_snapshot_backup(
                mock.Mock(),
                self.gcp_instance_name,
                (disk["name"] for disk in disks),
            )

    def test_delete_snapshot(self, mock_google_cloud_compute, caplog):
        """Verify that a snapshot can be deleted successfully."""
        # GIVEN the snapshots client deletes successfully
        mock_google_cloud_compute.SnapshotsClient.return_value = (
            self._get_mock_snapshots_client()
        )
        # AND a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project)
        # AND log level is info
        caplog.set_level(logging.INFO)

        # WHEN a snapshot is deleted
        snapshot_name = self._get_snapshot_name(self.gcp_disks[0])
        snapshot_interface._delete_snapshot(snapshot_name)

        # THEN delete was called on the SnapshotsClient for that project/snapshot
        mock_snapshots_client = mock_google_cloud_compute.SnapshotsClient.return_value
        mock_snapshots_client.delete.assert_called_once_with(
            {"project": self.gcp_project, "snapshot": snapshot_name}
        )
        # AND result was called on the response
        resp = mock_snapshots_client.delete.return_value
        resp.result.assert_called_once()
        # AND a success message was logged
        assert "Snapshot {} deleted".format(snapshot_name) in caplog.text

    def test_delete_snapshot_not_found(self, mock_google_cloud_compute, caplog):
        """
        Verify that a snapshot deletion which fails with NotFound is considered
        successful.
        """
        # GIVEN a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project)
        # AND a snapshots client which will fail with a NotFound error
        mock_snapshots_client = mock_google_cloud_compute.SnapshotsClient.return_value
        mock_snapshots_client.delete.side_effect = NotFound("snapshot not found")

        # WHEN a snapshot is deleted
        snapshot_name = self._get_snapshot_name(self.gcp_disks[0])
        snapshot_interface._delete_snapshot(snapshot_name)

        # THEN delete was called on the SnapshotsClient for that project/snapshot
        mock_snapshots_client = mock_google_cloud_compute.SnapshotsClient.return_value
        mock_snapshots_client.delete.assert_called_once_with(
            {"project": self.gcp_project, "snapshot": snapshot_name}
        )
        # AND result was not called on the response
        resp = mock_snapshots_client.delete.return_value
        resp.result.assert_not_called()

    def test_delete_snapshot_warnings(self, mock_google_cloud_compute, caplog):
        """Verify that warnings are logged if present in the snapshots response."""
        # GIVEN a snapshots client which will delete a snapshot
        mock_snapshots_client = self._get_mock_snapshots_client()
        mock_google_cloud_compute.SnapshotsClient.return_value = mock_snapshots_client
        # AND the response has warnings
        mock_snapshots_client.delete.return_value.warnings = [
            mock.Mock(code="123", message="warning message")
        ]
        # AND a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project)

        # WHEN a snapshot is deleted
        snapshot_name = self._get_snapshot_name(self.gcp_disks[0])
        snapshot_interface._delete_snapshot(snapshot_name)

        # THEN the warning is included in the log output
        assert (
            "Warnings encountered during deletion of {}: 123:warning message".format(
                snapshot_name
            )
            in caplog.text
        )

    def test_delete_snapshot_failed(self, mock_google_cloud_compute, caplog):
        """Verify that a snapshot can be deleted successfully."""
        # GIVEN a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project)
        # AND a snapshots client which will fail to delete a snapshot
        mock_snapshots_client = mock_google_cloud_compute.SnapshotsClient.return_value
        mock_resp = mock_snapshots_client.delete.return_value
        mock_resp.result.return_value = True
        mock_resp.error_code = "503"
        mock_resp.error_message = "test error message"

        # WHEN a snapshot is deleted
        # THEN a CloudProviderError is raised
        snapshot_name = self._get_snapshot_name(self.gcp_disks[0])
        with pytest.raises(CloudProviderError) as exc:
            snapshot_interface._delete_snapshot(snapshot_name)

        # AND the exception message contains the snapshot name, error code and error
        # message
        expected_message = (
            "Deletion of snapshot {} failed with error code {}: {}".format(
                snapshot_name,
                mock_resp.error_code,
                mock_resp.error_message,
            )
        )
        assert str(exc.value) == expected_message

    @pytest.mark.parametrize(
        "snapshots_list",
        (
            [],
            [mock.Mock(identifier="snapshot0")],
            [mock.Mock(identifier="snapshot0"), mock.Mock(identifier="snapshot1")],
        ),
    )
    def test_delete_snapshot_backup(
        self, snapshots_list, mock_google_cloud_compute, caplog
    ):
        """Verfiy that all snapshots for a backup are deleted."""
        # GIVEN a backup_info specifying zero or more snapshots
        backup_info = mock.Mock(
            backup_id=self.backup_id, snapshots_info=mock.Mock(snapshots=snapshots_list)
        )
        # AND log level is info
        caplog.set_level(logging.INFO)
        # AND the snapshots client deletes successfully
        mock_google_cloud_compute.SnapshotsClient.return_value = (
            self._get_mock_snapshots_client()
        )
        # AND a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project, zone=None)

        # WHEN delete_snapshot_backup is called
        snapshot_interface.delete_snapshot_backup(backup_info)

        # THEN delete was called on the SnapshotsClient for each snapshot
        mock_snapshots_client = mock_google_cloud_compute.SnapshotsClient.return_value
        assert mock_snapshots_client.delete.call_count == len(snapshots_list)
        for snapshot in snapshots_list:
            assert (
                ({"project": self.gcp_project, "snapshot": snapshot.identifier},),
                {},
            ) in mock_snapshots_client.delete.call_args_list
            # AND the expected log message was logged for each snapshot
            assert (
                "Deleting snapshot '{}' for backup {}".format(
                    snapshot.identifier, self.backup_id
                )
                in caplog.text
            )

    @pytest.mark.parametrize(
        ("mock_disks", "expected_disk_names", "expected_device_names"),
        (
            ([], [], []),
            (
                [
                    mock.Mock(
                        source="projects/test_project/zones/us-east1-b/disks/disk0",
                        device_name="dev0",
                    )
                ],
                ["disk0"],
                ["dev0"],
            ),
            (
                [
                    mock.Mock(
                        source="projects/test_project/zones/us-east1-b/disks/disk0",
                        device_name="dev0",
                    ),
                    mock.Mock(
                        source="projects/test_project/zones/us-east1-b/disks/disk1",
                        device_name="dev1",
                    ),
                ],
                ["disk0", "disk1"],
                ["dev0", "dev1"],
            ),
        ),
    )
    def test_get_attached_devices(
        self,
        mock_disks,
        expected_disk_names,
        expected_device_names,
        mock_google_cloud_compute,
    ):
        """Verify that attached devices are returned as a dict keyed by disk name."""
        # GIVEN a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project, self.gcp_zone)
        # AND a mock InstancesClient which returns metadata listing the devices
        mock_instances_client = mock_google_cloud_compute.InstancesClient.return_value
        mock_instance_metadata = mock.Mock(disks=mock_disks)
        mock_instances_client.get.return_value = mock_instance_metadata

        # WHEN get_attached_devices is called
        attached_devices = snapshot_interface.get_attached_devices(
            self.gcp_instance_name
        )

        # THEN a dict of devices returned by the instance metadata is returned, keyed
        # by disk name
        assert len(attached_devices) == len(expected_disk_names)
        for expected_disk_name, expected_device_name in zip(
            expected_disk_names, expected_device_names
        ):
            assert expected_disk_name in attached_devices
            # AND the device name matches that returned by the instance metadata
            assert attached_devices[expected_disk_name] == self._get_device_path(
                expected_device_name
            )

    @pytest.mark.parametrize(
        "mock_disks",
        (
            [mock.Mock(source="", device_name="dev0")],
            [mock.Mock(source="/", device_name="dev0")],
            [mock.Mock(source="foo/", device_name="dev0")],
        ),
    )
    def test_get_attached_devices_bad_disk_name(
        self,
        mock_disks,
        mock_google_cloud_compute,
    ):
        """Verify that unparseable disk names are handled."""
        # GIVEN a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project, self.gcp_zone)
        # AND a mock InstancesClient which returns metadata listing the devices
        mock_instances_client = mock_google_cloud_compute.InstancesClient.return_value
        mock_instance_metadata = mock.Mock(disks=mock_disks)
        mock_instances_client.get.return_value = mock_instance_metadata

        # WHEN get_attached_devices is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(SnapshotBackupException) as exc:
            snapshot_interface.get_attached_devices(self.gcp_instance_name)
        # AND the expected message is included
        assert str(
            exc.value
        ) == "Could not parse disk name for source {} attached to instance {}".format(
            mock_disks[0].source, self.gcp_instance_name
        )

    @pytest.mark.parametrize(
        "mock_disks",
        (
            [
                mock.Mock(
                    source="projects/test_project/zones/us-east1-b/disks/disk0",
                    device_name="dev0",
                ),
                mock.Mock(
                    source="projects/test_project/zones/us-east1-b/disks/disk0",
                    device_name="dev1",
                ),
            ],
            [
                mock.Mock(
                    source="projects/test_project/zones/us-east1-b/disks/disk1",
                    device_name="dev2",
                ),
                mock.Mock(
                    source="projects/test_project/zones/us-east1-b/disks/disk0",
                    device_name="dev0",
                ),
                mock.Mock(
                    source="projects/test_project/zones/us-east1-b/disks/disk0",
                    device_name="dev1",
                ),
            ],
        ),
    )
    def test_get_attached_devices_multiple_names(
        self,
        mock_disks,
        mock_google_cloud_compute,
    ):
        """
        Verify that an exception is raised if a disk appears to be attached more than
        once.
        """
        # GIVEN a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project, self.gcp_zone)
        # AND a mock InstancesClient which returns metadata listing the devices
        mock_instances_client = mock_google_cloud_compute.InstancesClient.return_value
        mock_instance_metadata = mock.Mock(disks=mock_disks)
        mock_instances_client.get.return_value = mock_instance_metadata

        # WHEN get_attached_devices is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(SnapshotBackupException) as exc:
            snapshot_interface.get_attached_devices(self.gcp_instance_name)
        # AND the expected message is included
        assert str(exc.value) == (
            "Disk projects/test_project/zones/us-east1-b/disks/disk0 appears to be "
            "attached with name disk0 as devices {} and {}".format(
                self._get_device_path("dev1"), self._get_device_path("dev0")
            )
        )

    def test_get_attached_devices_instance_not_found(self, mock_google_cloud_compute):
        """
        Verify that a SnapshotBackupException is raised if the instance cannot be
        found.
        """
        # GIVEN a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project, self.gcp_zone)
        # AND a mock InstancesClient which cannot find the instance
        mock_instances_client = mock_google_cloud_compute.InstancesClient.return_value
        mock_instances_client.get.side_effect = NotFound("instance not found")

        # WHEN get_attached_devices is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(SnapshotBackupException) as exc:
            snapshot_interface.get_attached_devices(self.gcp_instance_name)

        # AND the exception contains the expected message
        assert str(
            exc.value
        ) == "Cannot find instance with name {} in zone {} for project {}".format(
            self.gcp_instance_name, self.gcp_zone, self.gcp_project
        )

    @pytest.mark.parametrize(
        "attached_devices",
        ({}, {"test_disk_0": "dev0"}, {"test_disk_0": "dev0", "test_disk_1": "dev1"}),
    )
    @mock.patch(
        "barman.cloud_providers.google_cloud_storage.GcpCloudSnapshotInterface.get_attached_devices"
    )
    def test_get_attached_snapshots(
        self, mock_get_attached_devices, attached_devices, mock_google_cloud_compute
    ):
        """Verify a dict of devices keyed by snapshot name is returned."""
        # GIVEN a dict of attached devices
        mock_get_attached_devices.return_value = attached_devices
        # AND a disks client which returns metadata including the source snapshot
        disks = []
        for disk_metadata in self.gcp_disks:
            updated_disk_metadata = disk_metadata.copy()
            updated_disk_metadata["source_snapshot"] = self._get_snapshot_name(
                disk_metadata["name"]
            )
            disks.append(updated_disk_metadata)
        mock_disks_client = self._get_mock_disks_client(
            self.gcp_project, self.gcp_zone, disks
        )
        mock_google_cloud_compute.DisksClient.return_value = mock_disks_client
        # AND a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project, self.gcp_zone)

        # WHEN get_attached_snapshots is called
        attached_snapshots = snapshot_interface.get_attached_snapshots(
            self.gcp_instance_name
        )

        # THEN the correct device is returned for each disk
        assert len(attached_snapshots) == len(attached_devices)
        for disk, device in attached_devices.items():
            assert attached_snapshots[self._get_snapshot_name(disk)] == device

    @pytest.mark.parametrize(
        "attached_devices",
        (
            {"test_disk_0": "dev0"},
            {"test_disk_0": "dev0", "test_disk_1": "dev1"},
            {"test_disk_0": "dev0", "test_disk_1": "dev1", "test_disk_2": "dev2"},
        ),
    )
    @mock.patch(
        "barman.cloud_providers.google_cloud_storage.GcpCloudSnapshotInterface.get_attached_devices"
    )
    def test_get_attached_snapshots_missing_snapshot(
        self, mock_get_attached_devices, attached_devices, mock_google_cloud_compute
    ):
        """Verify a missing snapshot is simply not included in the return value."""
        # GIVEN a dict of attached devices
        mock_get_attached_devices.return_value = attached_devices
        # AND a disks client which returns metadata including the source snapshot but
        # only for one of the disks
        disk_with_missing_snapshot = "test_disk_0"
        disks = []
        for disk_metadata in self.gcp_disks:
            updated_disk_metadata = disk_metadata.copy()
            if disk_metadata["name"] != disk_with_missing_snapshot:
                updated_disk_metadata["source_snapshot"] = self._get_snapshot_name(
                    disk_metadata["name"]
                )
            disks.append(updated_disk_metadata)
        mock_disks_client = self._get_mock_disks_client(
            self.gcp_project, self.gcp_zone, disks
        )
        mock_google_cloud_compute.DisksClient.return_value = mock_disks_client
        # AND a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project, self.gcp_zone)

        # WHEN get_attached_snapshots is called
        attached_snapshots = snapshot_interface.get_attached_snapshots(
            self.gcp_instance_name
        )

        # THEN only the disk cloned from a snapshot is included in the response
        assert len(attached_snapshots) == len(attached_devices) - 1
        for disk, device in attached_devices.items():
            if disk != disk_with_missing_snapshot:
                assert attached_snapshots[self._get_snapshot_name(disk)] == device

    @mock.patch(
        "barman.cloud_providers.google_cloud_storage.GcpCloudSnapshotInterface.get_attached_devices"
    )
    def test_get_attached_snapshots_disk_not_found(
        self, mock_get_attached_devices, mock_google_cloud_compute
    ):
        """
        Verify that a SnapshotBackupException is raised if the disk cannot be found.
        """
        # GIVEN a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project, self.gcp_zone)
        # AND a set of attached devices
        mock_get_attached_devices.return_value = {self.gcp_disks[0]["name"]: "dev0"}
        # AND a mock DisksClient which cannot find a disk
        mock_disks_client = mock_google_cloud_compute.DisksClient.return_value
        mock_disks_client.get.side_effect = NotFound("instance not found")

        # WHEN get_attached_snapshots is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(SnapshotBackupException) as exc:
            snapshot_interface.get_attached_snapshots(self.gcp_instance_name)

        # AND the exception contains the expected message
        assert str(
            exc.value
        ) == "Cannot find disk with name {} in zone {} for project {}".format(
            self.gcp_disks[0]["name"], self.gcp_zone, self.gcp_project
        )

    def test_instance_exists(self, mock_google_cloud_compute):
        """Verify successfully retrieving the instance results in a True response."""
        # GIVEN a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project)

        # WHEN instance_exists is called for an instance which exists
        result = snapshot_interface.instance_exists(self.gcp_instance_name)

        # THEN it returns True
        assert result is True

    def test_instance_exists_not_found(self, mock_google_cloud_compute):
        """Verify a NotFound error results in a False response."""
        # GIVEN a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project)
        # AND a mock InstancesClient which cannot find the instance
        mock_instances_client = mock_google_cloud_compute.InstancesClient.return_value
        mock_instances_client.get.side_effect = NotFound("instance not found")

        # WHEN instance_exists is called
        result = snapshot_interface.instance_exists(self.gcp_instance_name)

        # THEN it returns False
        assert result is False
