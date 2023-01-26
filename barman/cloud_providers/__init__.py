# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2023
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

from barman.exceptions import BarmanException, ConfigurationException


class CloudProviderUnsupported(BarmanException):
    """
    Exception raised when an unsupported cloud provider is requested
    """


class CloudProviderOptionUnsupported(BarmanException):
    """
    Exception raised when a supported cloud provider is given an unsupported
    option
    """


def _update_kwargs(kwargs, config, args):
    """
    Helper which adds the attributes of config specified in args to the supplied
    kwargs dict if they exist.
    """
    for arg in args:
        if arg in config:
            kwargs[arg] = getattr(config, arg)


def _make_s3_cloud_interface(config, cloud_interface_kwargs):
    from barman.cloud_providers.aws_s3 import S3CloudInterface

    cloud_interface_kwargs.update(
        {
            "profile_name": config.profile,
            "endpoint_url": config.endpoint_url,
            "read_timeout": config.read_timeout,
        }
    )
    if "encryption" in config:
        cloud_interface_kwargs["encryption"] = config.encryption
    return S3CloudInterface(**cloud_interface_kwargs)


def _make_azure_cloud_interface(config, cloud_interface_kwargs):
    from barman.cloud_providers.azure_blob_storage import AzureCloudInterface

    _update_kwargs(
        cloud_interface_kwargs,
        config,
        (
            "encryption_scope",
            "max_block_size",
            "max_concurrency",
            "max_single_put_size",
        ),
    )

    if "credential" in config and config.credential is not None:
        try:
            from azure.identity import AzureCliCredential, ManagedIdentityCredential
        except ImportError:
            raise SystemExit("Missing required python module: azure-identity")

        supported_credentials = {
            "azure-cli": AzureCliCredential,
            "managed-identity": ManagedIdentityCredential,
        }
        try:
            cloud_interface_kwargs["credential"] = supported_credentials[
                config.credential
            ]()
        except KeyError:
            raise CloudProviderOptionUnsupported(
                "Unsupported credential: %s" % config.credential
            )

    return AzureCloudInterface(**cloud_interface_kwargs)


def _make_google_cloud_interface(config, cloud_interface_kwargs):
    """
    :param config: Not used yet
    :param cloud_interface_kwargs: common parameters
    :return: GoogleCloudInterface
    """
    from barman.cloud_providers.google_cloud_storage import GoogleCloudInterface

    cloud_interface_kwargs["jobs"] = 1
    return GoogleCloudInterface(**cloud_interface_kwargs)


def get_cloud_interface(config):
    """
    Factory function that creates CloudInterface for the specified cloud_provider

    :param: argparse.Namespace config
    :returns: A CloudInterface for the specified cloud_provider
    :rtype: CloudInterface
    """
    cloud_interface_kwargs = {
        "url": config.source_url if "source_url" in config else config.destination_url
    }
    _update_kwargs(
        cloud_interface_kwargs, config, ("jobs", "tags", "delete_batch_size")
    )

    if config.cloud_provider == "aws-s3":
        return _make_s3_cloud_interface(config, cloud_interface_kwargs)
    elif config.cloud_provider == "azure-blob-storage":
        return _make_azure_cloud_interface(config, cloud_interface_kwargs)
    elif config.cloud_provider == "google-cloud-storage":

        return _make_google_cloud_interface(config, cloud_interface_kwargs)
    else:
        raise CloudProviderUnsupported(
            "Unsupported cloud provider: %s" % config.cloud_provider
        )


def get_snapshot_interface(config):
    """
    Factory function that creates CloudSnapshotInterface for the cloud provider
    specified in the supplied config.

    :param argparse.Namespace config: The backup options provided at the command line.
    :rtype: CloudSnapshotInterface
    :returns: A CloudSnapshotInterface for the specified snapshot_provider.
    """
    if config.cloud_provider == "google-cloud-storage":
        from barman.cloud_providers.google_cloud_storage import (
            GcpCloudSnapshotInterface,
        )

        if config.snapshot_gcp_project is None:
            raise ConfigurationException(
                "--snapshot-gcp-project option must be set for snapshot backups "
                "when cloud provider is google-cloud-storage"
            )
        return GcpCloudSnapshotInterface(config.snapshot_gcp_project)
    else:
        raise CloudProviderUnsupported(
            "No snapshot provider for cloud provider: %s" % config.cloud_provider
        )


def get_snapshot_interface_from_server_config(server_config):
    """
    Factory function that creates CloudSnapshotInterface for the snapshot provider
    specified in the supplied config.

    :param barman.config.Config server_config: The barman configuration object for a
        specific server.
    :rtype: CloudSnapshotInterface
    :returns: A CloudSnapshotInterface for the specified snapshot_provider.
    """
    if server_config.snapshot_provider == "gcp":
        from barman.cloud_providers.google_cloud_storage import (
            GcpCloudSnapshotInterface,
        )

        if server_config.snapshot_gcp_project is None:
            raise ConfigurationException(
                "snapshot_gcp_project option must be set when snapshot_provider is gcp"
            )
        return GcpCloudSnapshotInterface(server_config.snapshot_gcp_project)
    else:
        raise CloudProviderUnsupported(
            "Unsupported snapshot provider: %s" % server_config.snapshot_provider
        )


def get_snapshot_interface_from_backup_info(backup_info):
    """
    Factory function that creates CloudSnapshotInterface for the snapshot provider
    specified in the supplied backup info.

    :param barman.infofile.BackupInfo backup_info: The metadata for a specific backup.
    :rtype: CloudSnapshotInterface
    :returns: A CloudSnapshotInterface for the specified snapshot provider.
    """
    if backup_info.snapshots_info.provider == "gcp":
        from barman.cloud_providers.google_cloud_storage import (
            GcpCloudSnapshotInterface,
        )

        if backup_info.snapshots_info.project is None:
            raise BarmanException(
                "backup_info has snapshot provider 'gcp' but project is not set"
            )
        return GcpCloudSnapshotInterface(backup_info.snapshots_info.project)
    else:
        raise CloudProviderUnsupported(
            "Unsupported snapshot provider in backup info: %s"
            % backup_info.snapshots_info.provider
        )


def snapshots_info_from_dict(snapshots_info):
    """
    Factory function which creates a SnapshotInfo object for the supplied dict of
    snapshot backup metadata.

    :param dict snapshots_info: Dictionary of snapshots info from a backup.info
    :rtype: SnapshotsInfo
    :return: A SnapshotInfo subclass for the snapshots provider listed in the
        `provider` field of the snapshots_info.
    """
    if "provider" in snapshots_info and snapshots_info["provider"] == "gcp":
        from barman.cloud_providers.google_cloud_storage import GcpSnapshotsInfo

        return GcpSnapshotsInfo.from_dict(snapshots_info)
    else:
        raise CloudProviderUnsupported(
            "Unsupported snapshot provider in backup info: %s"
            % snapshots_info["provider"]
        )
