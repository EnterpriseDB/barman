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

from azure.core.exceptions import ResourceNotFoundError
from google.api_core.exceptions import NotFound

from barman.cloud import CloudProviderError

from barman.cloud_providers import (
    CloudProviderUnsupported,
    get_snapshot_interface,
    get_snapshot_interface_from_backup_info,
    get_snapshot_interface_from_server_config,
)
from barman.cloud_providers.azure_blob_storage import (
    AzureCloudSnapshotInterface,
    AzureVolumeMetadata,
)
from barman.cloud_providers.google_cloud_storage import (
    GcpCloudSnapshotInterface,
    GcpVolumeMetadata,
)
from barman.exceptions import (
    BarmanException,
    CommandException,
    ConfigurationException,
    SnapshotBackupException,
)


class TestGetSnapshotInterface(object):
    """
    Verify get_snapshot_interface creates the required CloudSnapshotInterface
    """

    @pytest.mark.parametrize(
        ("snapshot_provider", "interface_cls"),
        [
            ("aws", None),
            ("azure", AzureCloudSnapshotInterface),
            ("gcp", GcpCloudSnapshotInterface),
        ],
    )
    @mock.patch("barman.cloud_providers._get_azure_credential")
    @mock.patch("barman.cloud_providers.azure_blob_storage.import_azure_mgmt_compute")
    @mock.patch(
        "barman.cloud_providers.google_cloud_storage.import_google_cloud_compute"
    )
    def test_from_config_cloud_provider(
        self,
        _mock_google_cloud_compute,
        _mock_azure_mgmt_compute,
        _mock_get_azure_credential,
        snapshot_provider,
        interface_cls,
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
        # GIVEN a server config with the gcp snapshot provider and neither the
        # gcp_project option nor the deprecated snapshot_gcp_project option
        mock_config = mock.Mock(
            snapshot_provider="gcp", gcp_project=None, snapshot_gcp_project=None
        )
        # WHEN get snapshot_interface_from_server_config is called
        with pytest.raises(ConfigurationException) as exc:
            get_snapshot_interface_from_server_config(mock_config)
        # THEN the expected exception is raised
        assert "gcp_project option must be set when snapshot_provider is gcp" in str(
            exc.value
        )

    def test_from_config_azure_no_subscription_id(self):
        """
        Verify an exception is raised for azure snapshots with no azure_subscription_id
        in server config.
        """
        # GIVEN a server config with the azure snapshot provider and the
        # azure_subscription_id
        mock_config = mock.Mock(snapshot_provider="azure", azure_subscription_id=None)
        # WHEN get snapshot_interface_from_server_config is called
        with pytest.raises(ConfigurationException) as exc:
            get_snapshot_interface_from_server_config(mock_config)
        # THEN the expected exception is raised
        assert (
            "azure_subscription_id option must be set when snapshot_provider is azure"
            in str(exc.value)
        )

    @pytest.mark.parametrize(
        ("snapshot_provider", "interface_cls"),
        [
            ("aws", None),
            ("azure", AzureCloudSnapshotInterface),
            ("gcp", GcpCloudSnapshotInterface),
        ],
    )
    @mock.patch("barman.cloud_providers._get_azure_credential")
    @mock.patch("barman.cloud_providers.azure_blob_storage.import_azure_mgmt_compute")
    @mock.patch(
        "barman.cloud_providers.google_cloud_storage.import_google_cloud_compute"
    )
    def test_from_backup_info_cloud_provider(
        self,
        _mock_google_cloud_compute,
        _mock_azure_mgmt_compute,
        _mock_get_azure_credential,
        snapshot_provider,
        interface_cls,
    ):
        """Verify supported and unsupported cloud providers with backup_info."""
        # GIVEN a backup_info with snapshots_info containing the specified snapshot
        # provider
        mock_backup_info = mock.Mock(
            snapshots_info=mock.Mock(provider=snapshot_provider)
        )
        # AND a mock server config
        mock_config = mock.Mock()

        # WHEN get_snapshot_interface_from_server_config is called
        if interface_cls:
            # THEN supported providers return the expected interface
            assert isinstance(
                get_snapshot_interface_from_backup_info(mock_backup_info, mock_config),
                interface_cls,
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
        # GIVEN a server config with the gcp snapshot provider and no gcp_project
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

    def test_from_backup_info_azure_no_subscription_id(self):
        """
        Verify an exception is raised for azure snapshots with no azure_subscription_id
        in backup info.
        """
        # GIVEN a server config with the azure snapshot provider and no
        # azure_subscription_id
        mock_config = mock.Mock(snapshot_provider="azure", azure_subscription_id=None)
        # AND a backup info with no azure_subscription_id
        mock_backup_info = mock.Mock(
            snapshots_info=mock.Mock(provider="azure", subscription_id=None)
        )
        # WHEN get snapshot_interface_from_server_config is called
        with pytest.raises(ConfigurationException) as exc:
            get_snapshot_interface_from_backup_info(mock_backup_info, mock_config)
        # THEN the expected exception is raised
        assert (
            "backup_info has snapshot provider 'azure' but subscription_id is not set"
            in str(exc.value)
        )

    @pytest.mark.parametrize(
        ("cloud_provider", "interface_cls"),
        [
            ("aws-s3", None),
            (
                "azure-blob-storage",
                AzureCloudSnapshotInterface,
            ),
            ("google-cloud-storage", GcpCloudSnapshotInterface),
        ],
    )
    @mock.patch("barman.cloud_providers._get_azure_credential")
    @mock.patch("barman.cloud_providers.azure_blob_storage.import_azure_mgmt_compute")
    @mock.patch(
        "barman.cloud_providers.google_cloud_storage.import_google_cloud_compute"
    )
    def test_from_args_cloud_provider(
        self,
        _mock_google_cloud_compute,
        _mock_azure_mgmt_compute,
        _mock_get_azure_credential,
        cloud_provider,
        interface_cls,
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
        # GIVEN a cloud config with the specified snapshot provider where the
        # gcp_project and deprecated snapshot_gcp_project arguments are missing
        mock_config = mock.Mock(
            cloud_provider="google-cloud-storage",
            gcp_project=None,
            snapshot_gcp_project=None,
        )

        # WHEN get snapshot_interface_from_backup_info is called
        with pytest.raises(ConfigurationException) as exc:
            get_snapshot_interface(mock_config)
        # AND the exception has the expected message
        assert (
            "--gcp-project option must be set for snapshot backups when "
            "cloud provider is google-cloud-storage"
        ) == str(exc.value)

    def test_from_args_azure_no_subscription(self):
        """
        Verify an exception is raised for azure snapshots with no azure_subscription_id
        in args.
        """
        # GIVEN a cloud config with the azure snapshot provider where the
        # azure_subscription_id argument is missing
        mock_config = mock.Mock(
            cloud_provider="azure-blob-storage",
            azure_subscription_id=None,
        )

        # WHEN get snapshot_interface_from_backup_info is called
        with pytest.raises(ConfigurationException) as exc:
            get_snapshot_interface(mock_config)
        # AND the exception has the expected message
        assert (
            "--azure-subscription-id option must be set for snapshot backups when "
            "cloud provider is azure-blob-storage"
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
            "mount_options": "rw,noatime",
            "mount_point": "/opt/disk0",
        },
        {
            "name": "test_disk_1",
            "device_name": "dev1",
            "physical_block_size": 2048,
            "size_gb": 10,
            "mount_options": "rw",
            "mount_point": "/opt/disk1",
        },
        {
            "name": "test_disk_2",
            "device_name": "dev2",
            "physical_block_size": 4096,
            "size_gb": 100,
            "mount_options": "rw,relatime",
            "mount_point": "/opt/disk2",
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

    def _get_mock_volumes(self, disks):
        return dict(
            (
                disk["name"],
                mock.Mock(
                    mount_point=disk["mount_point"], mount_options=disk["mount_options"]
                ),
            )
            for disk in disks
        )

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
        # GIVEN a set of disks, represented as VolumeMetadata
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
            backup_info, self.gcp_instance_name, self._get_mock_volumes(disks)
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
            assert snapshot.snapshot_name == snapshot_name
            assert snapshot.snapshot_project == self.gcp_project
            assert snapshot.device_name == disk["device_name"]
            assert snapshot.mount_options == disk["mount_options"]
            assert snapshot.mount_point == disk["mount_point"]

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
                mock.Mock(),
                self.gcp_instance_name,
                self._get_mock_volumes(self.gcp_disks),
            )

        # AND the exception contains the expected message
        assert str(
            exc.value
        ) == "Cannot find instance with name {} in zone {} for project {}".format(
            self.gcp_instance_name, self.gcp_zone, self.gcp_project
        )

    def test_get_attached_volumes_disk_not_found(
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

        # WHEN get_attached_volumes is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(SnapshotBackupException) as exc:
            snapshot_interface.get_attached_volumes(
                self.gcp_instance_name, [disk["name"] for disk in disks]
            )

        # AND the exception contains the expected message
        assert str(
            exc.value
        ) == "Cannot find disk with name {} in zone {} for project {}".format(
            disks[-1]["name"], self.gcp_zone, self.gcp_project
        )

        # WHEN get_attached_volumes is called with fail_on_missing=False
        # THEN no exception is raised
        attached_volumes = snapshot_interface.get_attached_volumes(
            self.gcp_instance_name,
            [disk["name"] for disk in disks],
            fail_on_missing=False,
        )
        # AND the attached volumes contains only those disks which were present
        expected_volumes = [d["name"] for d in disks[:-1]]
        assert set(attached_volumes.keys()) == set(expected_volumes)

    def test_get_attached_volumes_disk_not_attached(
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

        # WHEN get_attached_volumes is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(SnapshotBackupException) as exc:
            snapshot_interface.get_attached_volumes(
                self.gcp_instance_name, [disk["name"] for disk in disks]
            )

        # AND the exception contains the expected message
        assert str(exc.value) == "Disks not attached to instance {}: {}".format(
            self.gcp_instance_name, disks[-1]["name"]
        )

        # WHEN get_attached_volumes is called with fail_on_missing=False
        # THEN no exception is raised
        attached_volumes = snapshot_interface.get_attached_volumes(
            self.gcp_instance_name,
            [disk["name"] for disk in disks],
            fail_on_missing=False,
        )
        # AND the attached volumes contains only those disks which were present
        expected_volumes = [d["name"] for d in disks[:-1]]
        assert set(attached_volumes.keys()) == set(expected_volumes)

    def test_get_attached_volumes_disk_attached_multiple_times(
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

        # WHEN get_attached_volumes is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(AssertionError):
            snapshot_interface.get_attached_volumes(
                self.gcp_instance_name,
                [disk["name"] for disk in disks],
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
        (
            "mock_disks",
            "mock_disk_metadata",
            "expected_disk_names",
            "expected_device_names",
            "expected_source_snapshots",
        ),
        (
            ([], [], [], [], []),
            (
                [
                    mock.Mock(
                        source="projects/test_project/zones/us-east1-b/disks/disk0",
                        device_name="dev0",
                    )
                ],
                [
                    mock.Mock(
                        source_snapshot=None,
                    ),
                ],
                ["disk0"],
                ["dev0"],
                [None],
            ),
            (
                [
                    mock.Mock(
                        source="projects/test_project/zones/us-east1-b/disks/disk0",
                        device_name="dev0",
                    )
                ],
                [
                    mock.Mock(
                        source_snapshot="snap0",
                    ),
                ],
                ["disk0"],
                ["dev0"],
                ["snap0"],
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
                [
                    mock.Mock(
                        source_snapshot="snap0",
                    ),
                    mock.Mock(
                        source_snapshot="snap1",
                    ),
                ],
                ["disk0", "disk1"],
                ["dev0", "dev1"],
                ["snap0", "snap1"],
            ),
        ),
    )
    def test_get_attached_volumes(
        self,
        mock_disks,
        mock_disk_metadata,
        expected_disk_names,
        expected_device_names,
        expected_source_snapshots,
        mock_google_cloud_compute,
    ):
        """Verify that attached volumes are returned as a dict keyed by disk name."""
        # GIVEN a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project, self.gcp_zone)
        # AND a mock InstancesClient which returns metadata listing the devices
        mock_instances_client = mock_google_cloud_compute.InstancesClient.return_value
        mock_instance_metadata = mock.Mock(disks=mock_disks)
        mock_instances_client.get.return_value = mock_instance_metadata
        # AND a mock DisksClient which returns the specified source snapshots
        mock_disks_client = mock_google_cloud_compute.DisksClient.return_value
        mock_disks_client.get.side_effect = mock_disk_metadata

        # WHEN get_attached_volumes is called
        attached_volumes = snapshot_interface.get_attached_volumes(
            self.gcp_instance_name
        )

        # THEN a dict of devices returned by the instance metadata is returned, keyed
        # by disk name
        assert len(attached_volumes) == len(expected_disk_names)
        for expected_disk_name, expected_device_name, expected_source_snapshot in zip(
            expected_disk_names, expected_device_names, expected_source_snapshots
        ):
            assert expected_disk_name in attached_volumes
            # AND the device name matches that returned by the instance metadata
            assert attached_volumes[
                expected_disk_name
            ]._device_path == self._get_device_path(expected_device_name)
            # AND the source snapshot matches that returned by the disk metadata
            assert (
                attached_volumes[expected_disk_name].source_snapshot
                == expected_source_snapshot
            )

    def test_get_attached_volumes_for_disks(self, mock_google_cloud_compute):
        """
        Verifies that only the requested disks are returned when the disks parameter is
        passed to get_attached_volumes.
        """
        # GIVEN a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project, self.gcp_zone)
        # AND a mock InstancesClient which returns metadata listing two disks
        mock_instances_client = mock_google_cloud_compute.InstancesClient.return_value
        mock_instance_metadata = mock.Mock(
            disks=[
                mock.Mock(
                    source="projects/test_project/zones/us-east1-b/disks/disk0",
                    device_name="dev0",
                ),
                mock.Mock(
                    source="projects/test_project/zones/us-east1-b/disks/disk1",
                    device_name="dev1",
                ),
            ]
        )
        mock_instances_client.get.return_value = mock_instance_metadata
        # AND a mock DisksClient which returns some arbitrary metadata
        mock_disks_client = mock_google_cloud_compute.DisksClient.return_value
        mock_disks_client.get.return_value = mock.Mock(source_snapshot=None)

        # WHEN get_attached_volumes is called requesting only disk1
        attached_volumes = snapshot_interface.get_attached_volumes(
            self.gcp_instance_name, disks=["disk1"]
        )

        # THEN only "disk1" is included in the resulting dict
        assert len(attached_volumes) == 1
        assert "disk1" in attached_volumes

    @pytest.mark.parametrize(
        "mock_disks",
        (
            [mock.Mock(source="", device_name="dev0")],
            [mock.Mock(source="/", device_name="dev0")],
            [mock.Mock(source="foo/", device_name="dev0")],
        ),
    )
    def test_get_attached_volumes_bad_disk_name(
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

        # WHEN get_attached_volumes is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(SnapshotBackupException) as exc:
            snapshot_interface.get_attached_volumes(self.gcp_instance_name)
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
    def test_get_attached_volumes_multiple_names(
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
        # AND a mock DisksClient which returns minimal information
        mock_disks_client = mock_google_cloud_compute.DisksClient.return_value
        mock_disks_client.get.return_value = mock.Mock(source_snapshot=None)

        # WHEN get_attached_volumes is called
        # THEN an AssertionError is raised
        with pytest.raises(AssertionError):
            snapshot_interface.get_attached_volumes(self.gcp_instance_name)

    def test_get_attached_volumes_instance_not_found(self, mock_google_cloud_compute):
        """
        Verify that a SnapshotBackupException is raised if the instance cannot be
        found.
        """
        # GIVEN a new GcpCloudSnapshotInterface
        snapshot_interface = GcpCloudSnapshotInterface(self.gcp_project, self.gcp_zone)
        # AND a mock InstancesClient which cannot find the instance
        mock_instances_client = mock_google_cloud_compute.InstancesClient.return_value
        mock_instances_client.get.side_effect = NotFound("instance not found")

        # WHEN get_attached_volumes is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(SnapshotBackupException) as exc:
            snapshot_interface.get_attached_volumes(self.gcp_instance_name)

        # AND the exception contains the expected message
        assert str(
            exc.value
        ) == "Cannot find instance with name {} in zone {} for project {}".format(
            self.gcp_instance_name, self.gcp_zone, self.gcp_project
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


class TestGcpVolumeMetadata(object):
    """Verify behaviour of GcpVolumeMetadata."""

    @pytest.mark.parametrize(
        (
            "attachment_metadata",
            "disk_metadata",
            "expected_device_path",
            "expected_source_snapshot",
        ),
        (
            (None, None, None, None),
            (
                mock.Mock(device_name="pgdata"),
                None,
                "/dev/disk/by-id/google-pgdata",
                None,
            ),
            (None, mock.Mock(source_snapshot="prefix/snap0"), None, "snap0"),
            (
                mock.Mock(device_name="pgdata"),
                mock.Mock(source_snapshot="prefix/snap0"),
                "/dev/disk/by-id/google-pgdata",
                "snap0",
            ),
        ),
    )
    def test_init(
        self,
        attachment_metadata,
        disk_metadata,
        expected_device_path,
        expected_source_snapshot,
    ):
        """Verify GcpVolumeMetadata is created from supplied metadata."""
        # WHEN volume metadata is created from the specified attachment_metadata and
        # disk_metadata
        volume = GcpVolumeMetadata(attachment_metadata, disk_metadata)

        # THEN the metadata has the expected source snapshot
        assert volume.source_snapshot == expected_source_snapshot
        # AND the internal _device_path has the expected value
        assert volume._device_path == expected_device_path

    def test_resolve_mounted_volume(self):
        """Verify resolve_mounted_volume sets mount info from findmnt output."""
        # GIVEN a GcpVolumeMetadata for device `pgdata`
        attachment_metadata = mock.Mock(device_name="pgdata")
        volume = GcpVolumeMetadata(attachment_metadata)
        # AND the specified findmnt response
        mock_cmd = mock.Mock()
        mock_cmd.findmnt.return_value = ("/opt/disk0", "rw,noatime")

        # WHEN resolve_mounted_volume is called
        volume.resolve_mounted_volume(mock_cmd)

        # THEN findmnt was called with the expected arguments
        mock_cmd.findmnt.assert_called_once_with("/dev/disk/by-id/google-pgdata")

        # AND the expected mount point and options are set on the volume metadata
        assert volume.mount_point == "/opt/disk0"
        assert volume.mount_options == "rw,noatime"

    @pytest.mark.parametrize(
        ("findmnt_fun", "device_name", "expected_exception_msg"),
        (
            (
                lambda x: (None, None),
                "pgdata",
                "Could not find device /dev/disk/by-id/google-pgdata at any mount point",
            ),
            (
                CommandException("error doing findmnt"),
                "pgdata",
                "Error finding mount point for device /dev/disk/by-id/google-pgdata: error doing findmnt",
            ),
            (
                lambda x: (None, None),
                None,
                "Cannot resolve mounted volume: Device path unknown",
            ),
        ),
    )
    def test_resolve_mounted_volume_failure(
        self, findmnt_fun, device_name, expected_exception_msg
    ):
        """Verify the failure modes of resolve_mounted_volume."""
        # GIVEN a GcpVolumeMetadata for device `pgdata`
        attachment_metadata = mock.Mock(device_name=device_name)
        volume = GcpVolumeMetadata(attachment_metadata)
        # AND the specified findmnt response
        mock_cmd = mock.Mock()
        mock_cmd.findmnt.side_effect = findmnt_fun

        # WHEN resolve_mounted_volume is called
        # THEN the expected exception occurs
        with pytest.raises(SnapshotBackupException) as exc:
            volume.resolve_mounted_volume(mock_cmd)

        # AND the exception has the expected error message
        assert str(exc.value) == expected_exception_msg


class TestAzureCloudSnapshotInterface(object):
    """
    Verify behaviour of the AzureCloudSnapshotInterface class.
    """

    azure_disks = (
        {
            "id": "disk_0",
            "location": "uksouth",
            "lun": 10,
            "managed_disk_id": "disk_id_0",
            "mount_options": "rw,noatime",
            "mount_point": "/opt/disk0",
            "name": "test_disk_0",
        },
        {
            "id": "disk_1",
            "location": "uksouth",
            "lun": 11,
            "managed_disk_id": "disk_id_1",
            "mount_options": "rw",
            "mount_point": "/opt/disk1",
            "name": "test_disk_1",
        },
        {
            "id": "disk_2",
            "location": "uksouth",
            "lun": 12,
            "managed_disk_id": "disk_id_2",
            "mount_options": "rw,relatime",
            "mount_point": "/opt/disk2",
            "name": "test_disk_2",
        },
    )
    azure_instance_name = "azure_vm"
    azure_resource_group = "test_resource_group"
    azure_subscription_id = "test_subscription_id"
    backup_id = "20380119T031407"
    server_name = "test_server"

    def _get_snapshot_name(self, disk_name):
        """Helper which forges the expected snapshot name for the given disk name."""
        return "{}-{}".format(disk_name, self.backup_id.lower())

    def _get_mock_snapshot_operations(self):
        """
        Helper which creates a mock SnapshotOperations client that always succeeds.
        """
        mock_resp = mock.Mock()
        mock_resp.status.return_value = "Succeeded"
        mock_resp.result.return_value.provisioning_state = "Succeeded"
        mock_snapshot_operations = mock.Mock()
        mock_snapshot_operations.begin_create_or_update.return_value = mock_resp
        mock_snapshot_operations.begin_delete.return_value = mock_resp
        return mock_snapshot_operations

    def _get_mock_instances_client(self, resource_group_name, instance_name, disks):
        """
        Helper which create a mock instances client for the given
        resource_group/instance with the specified disks attached as the specified
        device.
        """

        def get_fun(resource_group, instance):
            if instance == instance_name and resource_group == resource_group_name:
                mock_data_disks = []
                for disk in disks:
                    data_disk = mock.Mock(
                        lun=disk["lun"],
                        managed_disk=mock.Mock(id=disk["managed_disk_id"]),
                    )
                    data_disk.name = disk["name"]
                    mock_data_disks.append(data_disk)
                mock_storage_profile = mock.Mock(data_disks=mock_data_disks)
                return mock.Mock(storage_profile=mock_storage_profile)
            else:
                raise ResourceNotFoundError("instance not found")

        return mock.Mock(get=get_fun)

    def _get_mock_disks_client(self, resource_group_name, disks):
        """
        Helper which creates a mock disks client for the given resource group with the
        specified disks available.
        """
        disk_metadata = dict(
            (
                disk["name"],
                mock.Mock(location=disk["location"]),
            )
            for disk in disks
        )

        def get_fun(resource_group, disk):
            if resource_group == resource_group_name:
                try:
                    return disk_metadata[disk]
                except KeyError:
                    raise ResourceNotFoundError("disk not found")

        return mock.Mock(get=get_fun)

    def _get_mock_volumes(self, disks):
        """Helper which returns mock VolumeMetadata objects for the named disks."""
        return dict(
            (
                disk["name"],
                mock.Mock(
                    mount_point=disk["mount_point"],
                    mount_options=disk["mount_options"],
                    location=disk["location"],
                ),
            )
            for disk in disks
        )

    @pytest.fixture(autouse=True)
    def mock_azure_mgmt_compute(self):
        with mock.patch(
            "barman.cloud_providers.azure_blob_storage.import_azure_mgmt_compute"
        ) as mock_import_azure_mgmt_compute:
            self._mock_azure_mgmt_compute = mock_import_azure_mgmt_compute.return_value
            yield mock_import_azure_mgmt_compute.return_value

    @pytest.fixture(autouse=True)
    def mock_azure_identity(self):
        with mock.patch(
            "barman.cloud_providers.azure_blob_storage.import_azure_identity"
        ) as mock_import_azure_identity:
            self._mock_azure_identity = mock_import_azure_identity.return_value
            yield mock_import_azure_identity.return_value

    def test_init_with_null_subscription_id(self):
        """
        Verify creating AzureCloudSnapshotInterface fails if subscription_id is not set.
        """
        # GIVEN a null subscription ID
        azure_subscription_id = None

        # WHEN an AzureCloudSnapshotInterface is created
        # THEN a TypeError is raised
        with pytest.raises(TypeError) as exc:
            AzureCloudSnapshotInterface(azure_subscription_id)

        # AND the expected message is included
        assert str(exc.value) == "subscription_id cannot be None"

    def test_init(self):
        """
        Verify creating AzureCloudSnapshotInterface creates the necessary Azure client.
        """
        # GIVEN a non-null Azure subscription
        subscription_id = self.azure_subscription_id
        # WHEN an AzureCloudSnapshotInterface is created
        snapshot_interface = AzureCloudSnapshotInterface(subscription_id)
        # THEN an azure.mgmt.compute.ComputeManagementClient is created
        assert (
            snapshot_interface.client
            == self._mock_azure_mgmt_compute.ComputeManagementClient.return_value
        )
        # AND the DefaultAzureCredential was used
        self._mock_azure_mgmt_compute.ComputeManagementClient.assert_called_once_with(
            self._mock_azure_identity.DefaultAzureCredential.return_value,
            subscription_id,
        )

    def test_init_with_credential(self):
        """
        Verify creation of AzureCloudSnapshotInterface with provided credential.
        """
        # GIVEN a non-null Azure subscription
        subscription_id = self.azure_subscription_id
        # AND a user-specified credential
        credential = mock.Mock()
        # WHEN an AzureCloudSnapshotInterface is created
        AzureCloudSnapshotInterface(subscription_id, credential=credential)
        # THEN the user-specified credential was used to create the
        # ComputeManagementClient
        self._mock_azure_mgmt_compute.ComputeManagementClient.assert_called_once_with(
            credential.return_value, subscription_id
        )

    def test_take_snapshot(self, caplog):
        """
        Verify that _take_snapshot calls the Azure library and waits for the result.
        """
        # GIVEN a new AzureCloudSnapshotInterface
        snapshot_interface = AzureCloudSnapshotInterface(self.azure_subscription_id)
        # AND a backup_info for a given server name and backup ID
        backup_info = mock.Mock(backup_id=self.backup_id, server_name=self.server_name)
        # AND a mock SnapshotsOperations which returns a successful response to
        # begin_create_or_update
        mock_snapshot_operations = self._get_mock_snapshot_operations()
        mock_resp = mock_snapshot_operations.begin_create_or_update.return_value
        self._mock_azure_mgmt_compute.ComputeManagementClient.return_value.snapshots = (
            mock_snapshot_operations
        )
        # AND log level is INFO
        caplog.set_level(logging.INFO)

        # WHEN _take_snapshot is called
        snapshot_name = snapshot_interface._take_snapshot(
            backup_info,
            self.azure_resource_group,
            self.azure_disks[0]["location"],
            self.azure_disks[0]["name"],
            self.azure_disks[0]["id"],
        )

        # THEN begin_create_or_update is called on the SnapshotsOperations with the
        # expected args
        expected_disk_id = self.azure_disks[0]["id"]
        expected_disk_name = self.azure_disks[0]["name"]
        expected_location = self.azure_disks[0]["location"]
        expected_snapshot_name = self._get_snapshot_name(expected_disk_name)
        mock_snapshot_operations.begin_create_or_update.assert_called_once_with(
            self.azure_resource_group,
            expected_snapshot_name,
            {
                "location": expected_location,
                "incremental": True,
                "creation_data": {
                    "create_option": "Copy",
                    "source_uri": expected_disk_id,
                },
            },
        )
        # AND wait() was called on the response to await completion of the snapshot
        mock_resp.wait.assert_called_once()
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

    @pytest.mark.parametrize(
        ("mock_status", "mock_provisioning_state"),
        (("Succeeded", "Failed"), ("123", "Succeeded"), ("123", "Failed")),
    )
    def test_take_snapshot_failed(self, mock_status, mock_provisioning_state):
        """
        Verify that _take_snapshot raises an exception on failure.
        """
        # GIVEN a new AzureCloudSnapshotInterface
        snapshot_interface = AzureCloudSnapshotInterface(self.azure_subscription_id)
        # AND a backup_info for a given server name and backup ID
        backup_info = mock.Mock(backup_id=self.backup_id, server_name=self.server_name)
        # AND a mock SnapshotsOperations which returns a failed response to
        # begin_create_or_update
        mock_snapshot_operations = (
            self._mock_azure_mgmt_compute.ComputeManagementClient.return_value.snapshots
        )
        mock_resp = mock_snapshot_operations.begin_create_or_update.return_value
        mock_resp.status.return_value = mock_status
        mock_resp.result.return_value.provisioning_state = mock_provisioning_state
        mock_resp.result.return_value.__str__.return_value = "result_string"

        # WHEN _take_snapshot is called
        # THEN a CloudProviderError is raised
        with pytest.raises(CloudProviderError) as exc:
            snapshot_interface._take_snapshot(
                backup_info,
                self.azure_resource_group,
                self.azure_disks[0]["location"],
                self.azure_disks[0]["name"],
                self.azure_disks[0]["id"],
            )
        # AND the exception message contains the snapshot name, error code and error
        # message
        expected_snapshot_name = self._get_snapshot_name(self.azure_disks[0]["name"])
        expected_message = "Snapshot '{}' failed with error code {}: {}".format(
            expected_snapshot_name, mock_status, "result_string"
        )
        assert str(exc.value) == expected_message

    @pytest.mark.parametrize("number_of_disks", (1, 2, 3))
    def test_take_snapshot_backup(self, number_of_disks):
        """
        Verify that take_snapshot_backup takes the required snapshots and updates the
        backup_info when prerequisites are met.
        """
        # GIVEN a set of disks, represented as VolumeMetadata
        disks = self.azure_disks[:number_of_disks]
        assert len(disks) == number_of_disks
        # AND a backup_info for a given server name and backup ID
        backup_info = mock.Mock(backup_id=self.backup_id, server_name=self.server_name)
        # AND a mock VirtualMachinesOperations which returns an instance with the
        # required disks attached
        mock_compute_client = (
            self._mock_azure_mgmt_compute.ComputeManagementClient.return_value
        )
        mock_compute_client.virtual_machines = self._get_mock_instances_client(
            self.azure_resource_group, self.azure_instance_name, disks
        )
        # AND a mock SnapshotsOperation which returns a successful response to
        # begin_create_or_update
        mock_compute_client.snapshots = self._get_mock_snapshot_operations()
        # AND a new AzureCloudSnapshotInterface
        snapshot_interface = AzureCloudSnapshotInterface(
            self.azure_subscription_id, self.azure_resource_group
        )

        # WHEN take_snapshot_backup is called for multiple disks
        snapshot_interface.take_snapshot_backup(
            backup_info, self.azure_instance_name, self._get_mock_volumes(disks)
        )

        # THEN the backup_info is updated with the expected snapshot metadata
        snapshots_info = backup_info.snapshots_info
        assert snapshots_info.subscription_id == self.azure_subscription_id
        assert snapshots_info.resource_group == self.azure_resource_group
        assert snapshots_info.provider == "azure"
        assert len(snapshots_info.snapshots) == len(disks)
        for disk in disks:
            snapshot_name = self._get_snapshot_name(disk["name"])
            snapshot = next(
                snapshot
                for snapshot in snapshots_info.snapshots
                if snapshot.snapshot_name == snapshot_name
            )
            assert snapshot.identifier == snapshot_name
            assert snapshot.snapshot_name == snapshot_name
            assert snapshot.location == disk["location"]
            assert snapshot.lun == disk["lun"]
            assert snapshot.mount_options == disk["mount_options"]
            assert snapshot.mount_point == disk["mount_point"]

    def test_take_snapshot_backup_instance_not_found(self):
        """
        Verify that a SnapshotBackupException is raised if the instance cannot be
        found.
        """
        # GIVEN a new AzureCloudSnapshotInterface
        snapshot_interface = AzureCloudSnapshotInterface(
            self.azure_subscription_id, self.azure_resource_group
        )
        # AND a mock VirtualMachinesOperations which cannot find the instance
        mock_compute_client = (
            self._mock_azure_mgmt_compute.ComputeManagementClient.return_value
        )
        mock_compute_client.virtual_machines.get.side_effect = ResourceNotFoundError(
            "instance not found"
        )

        # WHEN take_snapshot_backup is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(SnapshotBackupException) as exc:
            snapshot_interface.take_snapshot_backup(
                mock.Mock(),
                self.azure_instance_name,
                self._get_mock_volumes(self.azure_disks),
            )

        # AND the exception contains the expected message
        assert str(exc.value) == (
            "Cannot find instance with name {} in resource group {} "
            "in subscription {}"
        ).format(
            self.azure_instance_name,
            self.azure_resource_group,
            self.azure_subscription_id,
        )

    def test_take_snapshot_backup_disks_not_attached(self):
        """
        Verify that a SnapshotBackupException is raised if the expected disks are not
        attached.
        """
        # GIVEN a new AzureCloudSnapshotInterface
        snapshot_interface = AzureCloudSnapshotInterface(
            self.azure_subscription_id, self.azure_resource_group
        )
        # AND a mock VirtualMachinesOperations which returns an instance with no disks
        # attached
        mock_compute_client = (
            self._mock_azure_mgmt_compute.ComputeManagementClient.return_value
        )
        mock_compute_client.virtual_machines = self._get_mock_instances_client(
            self.azure_resource_group, self.azure_instance_name, {}
        )

        # WHEN take_snapshot_backup is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(SnapshotBackupException) as exc:
            snapshot_interface.take_snapshot_backup(
                mock.Mock(),
                self.azure_instance_name,
                self._get_mock_volumes(self.azure_disks),
            )

        # AND the exception contains the expected message
        assert str(exc.value) == (
            "Disk {} not attached to instance {}".format(
                self.azure_disks[0]["name"], self.azure_instance_name
            )
        )

    def test_delete_snapshot(self, caplog):
        """Verify that a snapshot can be deleted successfully."""
        # GIVEN a successful response from the delete snapshot request
        mock_compute_client = (
            self._mock_azure_mgmt_compute.ComputeManagementClient.return_value
        )
        mock_compute_client.snapshots = self._get_mock_snapshot_operations()
        # AND a new AzureCloudSnapshotInterface
        snapshot_interface = AzureCloudSnapshotInterface(
            self.azure_subscription_id, self.azure_resource_group
        )
        # AND log level is info
        caplog.set_level(logging.INFO)

        # WHEN a snapshot is deleted
        snapshot_name = "test_snapshot"
        resource_group = "test_resource_group"
        snapshot_interface._delete_snapshot(snapshot_name, resource_group)

        # THEN delete was called on the client with the expected arguments
        mock_compute_client.snapshots.begin_delete.assert_called_once_with(
            resource_group, snapshot_name
        )
        # AND wait was called on the response
        mock_compute_client.snapshots.begin_delete.return_value.wait.assert_called_once()
        # AND a success message was logged
        assert "Snapshot {} deleted".format(snapshot_name) in caplog.text

    def test_delete_snapshot_not_found(self):
        """
        Verify that a ResourceNotFound error is propagated rather than being considered
        a successful deletion. This is because a ResourceNotFoundError is raised if the
        resource group cannot be found - this is an error condition.
        """
        # GIVEN a delete snapshot request which will raise a ResourceNotFoundError
        mock_compute_client = (
            self._mock_azure_mgmt_compute.ComputeManagementClient.return_value
        )
        mock_compute_client.snapshots = self._get_mock_snapshot_operations()
        mock_compute_client.snapshots.begin_delete.side_effect = ResourceNotFoundError
        # AND a new AzureCloudSnapshotInterface
        snapshot_interface = AzureCloudSnapshotInterface(
            self.azure_subscription_id, self.azure_resource_group
        )

        # WHEN a snapshot is deleted
        # THEN a ResourceNotFoundError is raised
        snapshot_name = "test_snapshot"
        resource_group = "test_resource_group"
        with pytest.raises(ResourceNotFoundError):
            snapshot_interface._delete_snapshot(snapshot_name, resource_group)

    def test_delete_snapshot_failed(self):
        """Verify that an unsuccessful response results in a CloudProviderError."""
        # GIVEN a delete snapshot request which will return an unsuccessful response
        mock_compute_client = (
            self._mock_azure_mgmt_compute.ComputeManagementClient.return_value
        )
        mock_compute_client.snapshots = self._get_mock_snapshot_operations()
        mock_resp = mock_compute_client.snapshots.begin_delete.return_value
        mock_resp.status.return_value = "failed"
        mock_resp.result.return_value = "failure message"
        # AND a new AzureCloudSnapshotInterface
        snapshot_interface = AzureCloudSnapshotInterface(
            self.azure_subscription_id, self.azure_resource_group
        )

        # WHEN a snapshot is deleted and a failure response is received
        # THEN a CloudProviderError is raised
        snapshot_name = "snapshot_name"
        with pytest.raises(CloudProviderError) as exc:
            snapshot_interface._delete_snapshot(snapshot_name, "resource group")
        # AND the exception has the expected message
        expected_message = (
            "Deletion of snapshot {} failed with error code {}: {}".format(
                snapshot_name, "failed", "failure message"
            )
        )
        assert expected_message in str(exc.value)

    @pytest.mark.parametrize(
        "snapshots_list",
        (
            [],
            [mock.Mock(identifier="snapshot0")],
            [mock.Mock(identifier="snapshot0"), mock.Mock(identifier="snapshot1")],
        ),
    )
    def test_delete_snapshot_backup(
        self,
        snapshots_list,
        caplog,
    ):
        """Verify that all snapshots for a backup are deleted."""
        # GIVEN a backup_info specifying zero or more snapshots in a given resource group
        resource_group = "resource group"
        backup_info = mock.Mock(
            backup_id=self.backup_id,
            snapshots_info=mock.Mock(
                resource_group=resource_group, snapshots=snapshots_list
            ),
        )
        # AND log level is info
        caplog.set_level(logging.INFO)
        # AND the snapshot delete requests are successful
        mock_compute_client = (
            self._mock_azure_mgmt_compute.ComputeManagementClient.return_value
        )
        mock_compute_client.snapshots = self._get_mock_snapshot_operations()
        # AND a new AzureCloudSnapshotInterface
        snapshot_interface = AzureCloudSnapshotInterface(self.azure_subscription_id)

        # WHEN delete_snapshot_backup is called
        snapshot_interface.delete_snapshot_backup(backup_info)

        # THEN begin_delete was called for each snapshot
        mock_snapshots_operation = mock_compute_client.snapshots
        assert mock_snapshots_operation.begin_delete.call_count == len(snapshots_list)
        for snapshot in snapshots_list:
            assert (
                (
                    resource_group,
                    snapshot.identifier,
                ),
            ) in mock_snapshots_operation.begin_delete.call_args_list
            # AND the expected log message was logged for each snapshot
            assert (
                "Deleting snapshot '{}' for backup {}".format(
                    snapshot.identifier, self.backup_id
                )
                in caplog.text
            )

    @pytest.mark.parametrize(
        (
            "disks_metadata",
            "expected_disk_names",
            "expected_luns",
            "expected_locations",
        ),
        (
            ([], [], [], []),
            (
                [
                    {
                        "location": "uksouth",
                        "lun": "10",
                        "managed_disk_id": "disk_id_0",
                        "name": "disk0",
                    }
                ],
                ["disk0"],
                ["10"],
                ["uksouth"],
            ),
            (
                [
                    {
                        "location": "uksouth",
                        "lun": "10",
                        "managed_disk_id": "disk_id_0",
                        "name": "disk0",
                    },
                    {
                        "location": "ukwest",
                        "lun": "11",
                        "managed_disk_id": "disk_id_1",
                        "name": "disk1",
                    },
                ],
                ["disk0", "disk1"],
                ["10", "11"],
                ["uksouth", "ukwest"],
            ),
        ),
    )
    def test_get_attached_volumes(
        self,
        disks_metadata,
        expected_disk_names,
        expected_luns,
        expected_locations,
    ):
        """Verify that attached volumes are returned as a dict keyed by disk name."""
        # GIVEN a new AzureCloudSnapshotInterface
        snapshot_interface = AzureCloudSnapshotInterface(
            self.azure_subscription_id, self.azure_resource_group
        )
        # AND a mock VirtualMachinesOperations which returns an instance with the
        # required disks attached
        mock_compute_client = (
            self._mock_azure_mgmt_compute.ComputeManagementClient.return_value
        )
        mock_compute_client.virtual_machines = self._get_mock_instances_client(
            self.azure_resource_group, self.azure_instance_name, disks_metadata
        )
        # AND a mock DisksOperation which returns the specified metadata
        mock_compute_client.disks = self._get_mock_disks_client(
            self.azure_resource_group, disks_metadata
        )

        # WHEN get_attached_volumes is called
        attached_volumes = snapshot_interface.get_attached_volumes(
            self.azure_instance_name
        )

        # THEN a dict of VolumeMetadata is returned, keyed by disk name
        assert len(attached_volumes) == len(expected_disk_names)
        for expected_disk_name, expected_lun, expected_location in zip(
            expected_disk_names, expected_luns, expected_locations
        ):
            assert expected_disk_name in attached_volumes
            # AND the lun matches that returned by the instance metadata
            assert attached_volumes[expected_disk_name]._lun == expected_lun
            # AND the location matches that returned by the disk metadata
            assert attached_volumes[expected_disk_name].location == expected_location

    def test_get_attached_volumes_for_disks(self):
        """
        Verifies that only the requested disks are returned when the disks parameter is
        passed to get_attached_volumes.
        """
        # GIVEN a new AzureCloudSnapshotInterface
        snapshot_interface = AzureCloudSnapshotInterface(
            self.azure_subscription_id, self.azure_resource_group
        )
        # AND a mock VirtualMachinesOperations which returns an instance with the
        # required disks attached
        mock_compute_client = (
            self._mock_azure_mgmt_compute.ComputeManagementClient.return_value
        )
        disks_metadata = [
            {
                "location": "uksouth",
                "lun": "10",
                "managed_disk_id": "disk_id_0",
                "name": "disk0",
            },
            {
                "location": "ukwest",
                "lun": "11",
                "managed_disk_id": "disk_id_1",
                "name": "disk1",
            },
        ]
        mock_compute_client.virtual_machines = self._get_mock_instances_client(
            self.azure_resource_group, self.azure_instance_name, disks_metadata
        )
        # AND a mock DisksOperation which returns the specified metadata
        mock_compute_client.disks = self._get_mock_disks_client(
            self.azure_resource_group, disks_metadata
        )

        # WHEN get_attached_volumes is called
        attached_volumes = snapshot_interface.get_attached_volumes(
            self.azure_instance_name, disks=["disk1"]
        )

        # THEN only "disk1" is included in the resulting dict
        assert len(attached_volumes) == 1
        assert "disk1" in attached_volumes

    def test_get_attached_volumes_disk_not_found(self):
        """
        Verify that a SnapshotBackupException is raised if a disk cannot be found.
        """
        # GIVEN a set of disks
        disks = self.azure_disks
        # AND a mock VirtualMachinesOperations which returns an instance with a
        # subset of the required disks attached
        mock_compute_client = (
            self._mock_azure_mgmt_compute.ComputeManagementClient.return_value
        )
        mock_compute_client.virtual_machines = self._get_mock_instances_client(
            self.azure_resource_group, self.azure_instance_name, disks[:-1]
        )
        # AND a mock DisksOperation which returns the specified metadata for the
        # same subset of disks
        mock_compute_client.disks = self._get_mock_disks_client(
            self.azure_resource_group, disks[:-1]
        )
        # AND a new AzureCloudSnapshotInterface
        snapshot_interface = AzureCloudSnapshotInterface(
            self.azure_subscription_id, self.azure_resource_group
        )

        # WHEN get_attached_volumes is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(SnapshotBackupException) as exc:
            snapshot_interface.get_attached_volumes(
                self.azure_instance_name, [disk["name"] for disk in disks]
            )

        # AND the exception contains the expected message
        assert str(exc.value) == (
            "Cannot find disk with name {} in resource group {} in subscription {}"
        ).format(
            disks[-1]["name"], self.azure_resource_group, self.azure_subscription_id
        )

        # WHEN get_attached_volumes is called with fail_on_missing=False
        # THEN no exception is raised
        attached_volumes = snapshot_interface.get_attached_volumes(
            self.azure_instance_name,
            [disk["name"] for disk in disks],
            fail_on_missing=False,
        )
        # AND the attached volumes contains only those disks which were present
        expected_volumes = [d["name"] for d in disks[:-1]]
        assert set(attached_volumes.keys()) == set(expected_volumes)

    def test_get_attached_volumes_disk_not_attaached(self):
        """
        Verify that a SnapshotBackupException is raised if a disk is not attached
        to the instance.
        """
        # GIVEN a set of disks
        disks = self.azure_disks
        # AND a mock VirtualMachinesOperations which returns an instance with a
        # subset of the required disks attached
        mock_compute_client = (
            self._mock_azure_mgmt_compute.ComputeManagementClient.return_value
        )
        mock_compute_client.virtual_machines = self._get_mock_instances_client(
            self.azure_resource_group, self.azure_instance_name, disks[:-1]
        )
        # AND a mock DisksOperation which returns the specified metadata for all
        # disks
        mock_compute_client.disks = self._get_mock_disks_client(
            self.azure_resource_group, disks
        )
        # AND a new AzureCloudSnapshotInterface
        snapshot_interface = AzureCloudSnapshotInterface(
            self.azure_subscription_id, self.azure_resource_group
        )

        # WHEN get_attached_volumes is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(SnapshotBackupException) as exc:
            snapshot_interface.get_attached_volumes(
                self.azure_instance_name, [disk["name"] for disk in disks]
            )

        # AND the exception contains the expected message
        assert str(exc.value) == "Disks not attached to instance {}: {}".format(
            self.azure_instance_name, disks[-1]["name"]
        )

        # WHEN get_attached_volumes is called with fail_on_missing=False
        # THEN no exception is raised
        attached_volumes = snapshot_interface.get_attached_volumes(
            self.azure_instance_name,
            [disk["name"] for disk in disks],
            fail_on_missing=False,
        )
        # AND the attached volumes contains only those disks which were present
        expected_volumes = [d["name"] for d in disks[:-1]]
        assert set(attached_volumes.keys()) == set(expected_volumes)

    @pytest.mark.parametrize(
        "mock_disks",
        (
            [
                {
                    "lun": "10",
                    "managed_disk_id": "disk_id_0",
                    "name": "disk0",
                },
                {
                    "lun": "11",
                    "managed_disk_id": "disk_id_0",
                    "name": "disk0",
                },
            ],
            [
                {
                    "lun": "10",
                    "managed_disk_id": "disk_id_0",
                    "name": "disk0",
                },
                {
                    "lun": "11",
                    "managed_disk_id": "disk_id_1",
                    "name": "disk1",
                },
                {
                    "lun": "11",
                    "managed_disk_id": "disk_id_1",
                    "name": "disk1",
                },
            ],
        ),
    )
    def test_get_attached_volumes_duplicate_names(self, mock_disks):
        """
        Verify that an exception is raised if a disk appears to be attached more than
        once.
        """
        # GIVEN a mock VirtualMachinesOperations which returns an instance with the
        # specified disks attached
        mock_compute_client = (
            self._mock_azure_mgmt_compute.ComputeManagementClient.return_value
        )
        mock_compute_client.virtual_machines = self._get_mock_instances_client(
            self.azure_resource_group, self.azure_instance_name, mock_disks
        )
        # AND a new AzureCloudSnapshotInterface
        snapshot_interface = AzureCloudSnapshotInterface(
            self.azure_subscription_id, self.azure_resource_group
        )

        # WHEN get_attached_volumes is called
        # THEN an AssertionError is raised
        with pytest.raises(AssertionError):
            snapshot_interface.get_attached_volumes(self.azure_instance_name)

    def test_get_attached_volumes_instance_not_found(self):
        """
        Verify that a SnapshotBackupException is raised if the instance cannot be
        found.
        """
        # GIVEN a mock VirtualMachinesOperations which cannot find the instance
        mock_compute_client = (
            self._mock_azure_mgmt_compute.ComputeManagementClient.return_value
        )
        mock_compute_client.virtual_machines.get.side_effect = ResourceNotFoundError(
            "instance_not_found"
        )
        # AND a new AzureCloudSnapshotInterface
        snapshot_interface = AzureCloudSnapshotInterface(
            self.azure_subscription_id, self.azure_resource_group
        )

        # WHEN get_attached_volumes is called
        # THEN a SnapshotBackupException is raised
        with pytest.raises(SnapshotBackupException) as exc:
            snapshot_interface.get_attached_volumes(self.azure_instance_name)

        # AND the exception contains the expected message
        assert str(exc.value) == (
            "Cannot find instance with name {} in resource group {} "
            "in subscription {}"
        ).format(
            self.azure_instance_name,
            self.azure_resource_group,
            self.azure_subscription_id,
        )

    def test_instance_exists(self):
        """Verify successfully retrieving the instance results in a True response."""
        # GIVEN a new AzureCloudSnapshotInterface
        snapshot_interface = AzureCloudSnapshotInterface(
            self.azure_subscription_id, self.azure_resource_group
        )
        # WHEN instance exists is called for an instance which exists
        result = snapshot_interface.instance_exists(self.azure_instance_name)

        # THEN it returns True
        assert result is True

    def test_instance_exists_not_found(self):
        """Verify a NotFound error results in a False response."""
        # GIVEN a new AzureCloudSnapshotInterface
        snapshot_interface = AzureCloudSnapshotInterface(
            self.azure_subscription_id, self.azure_resource_group
        )
        # AND a mock VirtualMachinesOperations which cannot find the instance
        mock_compute_client = (
            self._mock_azure_mgmt_compute.ComputeManagementClient.return_value
        )
        mock_compute_client.virtual_machines.get.side_effect = ResourceNotFoundError(
            "instance_not_found"
        )

        # WHEN instance exists is called
        result = snapshot_interface.instance_exists(self.azure_instance_name)

        # THEN it returns False
        assert result is False


class TestAzureVolumeMetadata(object):
    """Verify behaviour of AzureVolumeMetadata."""

    @pytest.mark.parametrize(
        [
            "attachment_metadata",
            "disk_metadata",
            "expected_lun",
            "expected_location",
        ],
        (
            # If no attachment metadata or disk metadata is passed then we expect init
            # to succeed but the lun and location to be None.
            (None, None, None, None),
            # If the lun is set in the attachment metadata then we eexpect it to be
            # set in the VolumeMetadata instance.
            (
                mock.Mock(lun="10"),
                None,
                "10",
                None,
            ),
            # If the location is set in the attachment metadata then we eexpect it to
            # be set in the VolumeMetadata instance.
            (None, mock.Mock(location="uksouth"), None, "uksouth"),
            # If lun and location are set in the attachment metadata then we expect
            # them to be set in the VolumeMetadata instance.
            (
                mock.Mock(lun="10"),
                mock.Mock(location="uksouth"),
                "10",
                "uksouth",
            ),
        ),
    )
    def test_init(
        self,
        attachment_metadata,
        disk_metadata,
        expected_lun,
        expected_location,
    ):
        """Verify AzureVolumeMetadata is created from supplied metadata"""
        # WHEN volume metadata is created from the specified attachment_metadata and
        # disk_metadata
        volume = AzureVolumeMetadata(attachment_metadata, disk_metadata)

        # THEN the metadata has the expected location
        assert volume.location == expected_location
        # AND the internal _lun has the expected value
        assert volume._lun == expected_lun

    @pytest.mark.parametrize(
        ("create_option", "source_resource_id", "expected_source_snapshot"),
        (
            # If there is no create_option or source_resource_id then we expect
            # source_snapshot to be None
            (None, None, None),
            # If there is no create_option and source_resource_id is set to
            # a snapshot name we expect source_snapshot to be None
            (
                None,
                "/subscriptions/id/resourceGroups/group/providers/Microsoft.Compute/snapshots/snapshot_name",
                None,
            ),
            # If the create_option is not Copy we expect source_snapshot to be None
            (
                "NotCopy",
                "/subscriptions/id/resourceGroups/group/providers/Microsoft.Compute/snapshots/snapshot_name",
                None,
            ),
            # If the create_option is Copy but the source_resource_id is not a snapshot
            # then we expect source_snapshot to be None
            (
                "Copy",
                "/subscriptions/id/resourceGroups/group/providers/Microsoft.Compute/disks/disk_name",
                None,
            ),
            # If create_option is Copy and source_resource_id is a snapshot ID then we
            # expect source_snapshot to be set to the snapshot name
            (
                "Copy",
                "/subscriptions/id/resourceGroups/group/providers/Microsoft.Compute/snapshots/snapshot_name",
                "snapshot_name",
            ),
        ),
    )
    def test_init_source_snapshot(
        self, create_option, source_resource_id, expected_source_snapshot
    ):
        """
        Verify that the source snapshot is set correctly when available in the disk
        metadata.
        """
        # GIVEN disk metadata with the specified create_option and source_resource_id
        disk_metadata = mock.Mock(
            creation_data=mock.Mock(
                create_option=create_option,
                source_resource_id=source_resource_id,
            )
        )

        # WHEN volume metadata is created from the disk metadata
        volume = AzureVolumeMetadata(None, disk_metadata)

        # THEN the metadata has the expected source_snapshot value
        assert volume.source_snapshot == expected_source_snapshot

    @pytest.mark.parametrize(
        "source_resource_id",
        (
            # Not a valid fully qualified resource ID
            "providers/Microsoft.Compute/snapshots/foo",
            # Missing snapshot name
            "/subscriptions/id/resourceGroups/group/providers/Microsoft.Compute/snapshots/",
            # Missing subscription ID
            "/subscriptions//resourceGroups/group/providers/Microsoft.Compute/snapshots/foo",
            # Missing resource group
            "/subscriptions/id/resourceGroups//providers/Microsoft.Compute/snapshots/foo",
        ),
    )
    def test_source_snapshot_parsing_error(self, source_resource_id):
        """
        Verify that an exception is raised when the source_resource_id cannot be
        parsed.
        """
        # GIVEN disk metadata with create_option "Copy" and the specified
        # source_resource_id
        disk_name = "disk0"
        disk_metadata = mock.Mock(
            creation_data=mock.Mock(
                create_option="Copy",
                source_resource_id=source_resource_id,
            )
        )
        disk_metadata.name = disk_name
        # AND some arbitrary attachment metadata
        attachment_metadata = mock.Mock()

        # WHEN volume metadata is created from the disk metadata
        # THEN a SnapshotBackupException is raised
        with pytest.raises(SnapshotBackupException) as exc:
            AzureVolumeMetadata(attachment_metadata, disk_metadata)

        # AND the exception has the expected message
        expected_msg = (
            "Could not determine source snapshot for disk {} with source resource ID "
            "{}"
        ).format(disk_name, source_resource_id)
        assert expected_msg in str(exc.value)

    def test_resolve_mounted_volume(self):
        """Verify resolve_mounted_volume sets mount info from findmnt output."""
        # GIVEN a AzureVolumeMetadata for lun `10`
        attachment_metadata = mock.Mock(lun="10")
        volume = AzureVolumeMetadata(attachment_metadata)
        # AND the specified findmnt response
        mock_cmd = mock.Mock()
        mock_cmd.findmnt.return_value = ("/opt/disk0", "rw,noatime")

        # WHEN resolve_mounted_volume is called
        volume.resolve_mounted_volume(mock_cmd)

        # THEN findmnt was called with the expected arguments
        mock_cmd.findmnt.assert_called_once_with("/dev/disk/azure/scsi1/lun10")

        # AND the expected mount point and options are set on the volume metadata
        assert volume.mount_point == "/opt/disk0"
        assert volume.mount_options == "rw,noatime"

    @pytest.mark.parametrize(
        ("findmnt_fun", "lun", "expected_exception_msg"),
        (
            (
                lambda x: (None, None),
                "10",
                "Could not find volume with lun 10 at any mount point",
            ),
            (
                CommandException("error doing findmnt"),
                "10",
                "Error finding mount point for volume with lun 10: error doing findmnt",
            ),
            (
                lambda x: (None, None),
                None,
                "Cannot resolve mounted volume: LUN unknown",
            ),
        ),
    )
    def test_resolve_mounted_volume_failure(
        self, findmnt_fun, lun, expected_exception_msg
    ):
        """Verify the failure modes of resolve_mounted_volume."""
        # GIVEN a AzureVolumeMetadata for lun `10`
        attachment_metadata = mock.Mock(lun=lun)
        volume = AzureVolumeMetadata(attachment_metadata)
        # AND the specified findmnt response
        mock_cmd = mock.Mock()
        mock_cmd.findmnt.side_effect = findmnt_fun

        # WHEN resolve_mounted_volume is called
        # THEN the expected exception occurs
        with pytest.raises(SnapshotBackupException) as exc:
            volume.resolve_mounted_volume(mock_cmd)

        # AND the exception has the expected error message
        assert str(exc.value) == expected_exception_msg
