# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2025
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

from urllib.parse import urlparse

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


class ObjectKeyAlreadyExists(BarmanException):
    """
    Exception raised when trying to insert an object with a key that already exists
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
            "profile_name": config.aws_profile,
            "endpoint_url": config.endpoint_url,
            "read_timeout": config.read_timeout,
        }
    )
    if "encryption" in config:
        cloud_interface_kwargs["encryption"] = config.encryption
    if "sse_kms_key_id" in config:
        if (
            config.sse_kms_key_id is not None
            and "encryption" in config
            and config.encryption != "aws:kms"
        ):
            raise CloudProviderOptionUnsupported(
                'Encryption type must be "aws:kms" if SSE KMS Key ID is specified'
            )
        cloud_interface_kwargs["sse_kms_key_id"] = config.sse_kms_key_id
    if "addressing_style" in config:
        cloud_interface_kwargs["addressing_style"] = config.addressing_style
    return S3CloudInterface(**cloud_interface_kwargs)


def _get_azure_credential(credential_type):
    if credential_type is None:
        return None

    try:
        from azure.identity import (
            AzureCliCredential,
            DefaultAzureCredential,
            ManagedIdentityCredential,
        )
    except ImportError:
        raise SystemExit("Missing required python module: azure-identity")

    supported_credentials = {
        "azure-cli": AzureCliCredential,
        "default": DefaultAzureCredential,
        "managed-identity": ManagedIdentityCredential,
    }
    try:
        return supported_credentials[credential_type]
    except KeyError:
        raise CloudProviderOptionUnsupported(
            "Unsupported credential: %s" % credential_type
        )


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

    if "azure_credential" in config:
        credential = _get_azure_credential(config.azure_credential)
        if credential is not None:
            cloud_interface_kwargs["credential"] = credential()

    return AzureCloudInterface(**cloud_interface_kwargs)


def _make_google_cloud_interface(config, cloud_interface_kwargs):
    """
    :param config: Not used yet
    :param cloud_interface_kwargs: common parameters
    :return: GoogleCloudInterface
    """
    from barman.cloud_providers.google_cloud_storage import GoogleCloudInterface

    cloud_interface_kwargs["jobs"] = 1
    if "kms_key_name" in config:
        if (
            config.kms_key_name is not None
            and "snapshot_instance" in config
            and config.snapshot_instance is not None
        ):
            raise CloudProviderOptionUnsupported(
                "KMS key cannot be specified for snapshot backups"
            )
        cloud_interface_kwargs["kms_key_name"] = config.kms_key_name
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


def get_cloud_interface_from_server_config(config, cloud_provider, base_url):
    """
    Factory function that creates a CloudInterface for the specified *cloud_provider*
    based on the *base_url*.

    :param barman.config.Config config: The barman configuration object for a
        specific server.
    :param str cloud_provider: The cloud provider to create the interface for.
    :param str base_url: The base URL to use for the cloud interface.
    :rtype: CloudInterface
    :returns: A CloudInterface for the specified cloud_provider.
    """
    cloud_interface_kwargs = {
        "url": base_url,
        "jobs": config.parallel_jobs,
        "delete_batch_size": config.cloud_delete_batch_size,
    }

    if cloud_provider == "aws-s3":
        from barman.cloud_providers.aws_s3 import S3CloudInterface

        if config.aws_sse_kms_key_id and config.aws_encryption != "aws:kms":
            raise ConfigurationException(
                'aws_encryption must be "aws:kms" if aws_sse_kms_key_id is specified'
            )

        cloud_interface_kwargs.update(
            {
                "profile_name": config.aws_profile,
                "region": config.aws_region,
                "read_timeout": config.aws_read_timeout,
                "encryption": config.aws_encryption,
                "sse_kms_key_id": config.aws_sse_kms_key_id,
            }
        )
        return S3CloudInterface(**cloud_interface_kwargs)
    elif cloud_provider == "azure-blob-storage":
        from barman.cloud_providers.azure_blob_storage import AzureCloudInterface

        return AzureCloudInterface(**cloud_interface_kwargs)
    elif cloud_provider == "google-cloud-storage":
        from barman.cloud_providers.google_cloud_storage import GoogleCloudInterface

        return GoogleCloudInterface(**cloud_interface_kwargs)
    else:
        raise CloudProviderUnsupported(
            "Unsupported cloud provider: %s" % cloud_provider
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

        if config.gcp_project is None:
            raise ConfigurationException(
                "--gcp-project option must be set for snapshot backups "
                "when cloud provider is google-cloud-storage"
            )
        return GcpCloudSnapshotInterface(config.gcp_project, config.gcp_zone)
    elif config.cloud_provider == "azure-blob-storage":
        from barman.cloud_providers.azure_blob_storage import (
            AzureCloudSnapshotInterface,
        )

        if config.azure_subscription_id is None:
            raise ConfigurationException(
                "--azure-subscription-id option must be set for snapshot "
                "backups when cloud provider is azure-blob-storage"
            )
        return AzureCloudSnapshotInterface(
            config.azure_subscription_id,
            resource_group=config.azure_resource_group,
            credential=_get_azure_credential(config.azure_credential),
        )
    elif config.cloud_provider == "aws-s3":
        from barman.cloud_providers.aws_s3 import AwsCloudSnapshotInterface

        args = [
            config.aws_profile,
            config.aws_region,
            config.aws_await_snapshots_timeout,
            config.aws_snapshot_lock_mode,
            config.aws_snapshot_lock_duration,
            config.aws_snapshot_lock_cool_off_period,
            config.aws_snapshot_lock_expiration_date,
            config.tags,
        ]
        return AwsCloudSnapshotInterface(*args)
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

        gcp_project = server_config.gcp_project or server_config.snapshot_gcp_project
        if gcp_project is None:
            raise ConfigurationException(
                "gcp_project option must be set when snapshot_provider is gcp"
            )
        gcp_zone = server_config.gcp_zone or server_config.snapshot_zone
        return GcpCloudSnapshotInterface(gcp_project, gcp_zone)
    elif server_config.snapshot_provider == "azure":
        from barman.cloud_providers.azure_blob_storage import (
            AzureCloudSnapshotInterface,
        )

        if server_config.azure_subscription_id is None:
            raise ConfigurationException(
                "azure_subscription_id option must be set when snapshot_provider "
                "is azure"
            )
        return AzureCloudSnapshotInterface(
            server_config.azure_subscription_id,
            resource_group=server_config.azure_resource_group,
            credential=_get_azure_credential(server_config.azure_credential),
        )
    elif server_config.snapshot_provider == "aws":
        from barman.cloud_providers.aws_s3 import AwsCloudSnapshotInterface

        return AwsCloudSnapshotInterface(
            server_config.aws_profile,
            server_config.aws_region,
            server_config.aws_await_snapshots_timeout,
            server_config.aws_snapshot_lock_mode,
            server_config.aws_snapshot_lock_duration,
            server_config.aws_snapshot_lock_cool_off_period,
            server_config.aws_snapshot_lock_expiration_date,
        )
    else:
        raise CloudProviderUnsupported(
            "Unsupported snapshot provider: %s" % server_config.snapshot_provider
        )


def get_snapshot_interface_from_backup_info(backup_info, config=None):
    """
    Factory function that creates CloudSnapshotInterface for the snapshot provider
    specified in the supplied backup info.

    :param barman.infofile.BackupInfo backup_info: The metadata for a specific backup.
        cloud provider.
    :param argparse.Namespace|barman.config.Config config: The backup options provided
        by the command line or the Barman configuration.
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
        gcp_zone = config is not None and config.gcp_zone or None
        return GcpCloudSnapshotInterface(
            backup_info.snapshots_info.project,
            gcp_zone,
        )
    elif backup_info.snapshots_info.provider == "azure":
        from barman.cloud_providers.azure_blob_storage import (
            AzureCloudSnapshotInterface,
        )

        # When creating a snapshot interface for dealing with existing backups we use
        # the subscription ID from that backup and the resource group specified in
        # provider_args. This means that:
        #   1. Resources will always belong to the same subscription.
        #   2. Recovery resources can be in a different resource group to the one used
        #      to create the backup.
        if backup_info.snapshots_info.subscription_id is None:
            raise ConfigurationException(
                "backup_info has snapshot provider 'azure' but "
                "subscription_id is not set"
            )
        resource_group = None
        azure_credential = None
        if config is not None:
            if hasattr(config, "azure_resource_group"):
                resource_group = config.azure_resource_group
            if hasattr(config, "azure_credential"):
                azure_credential = config.azure_credential
        return AzureCloudSnapshotInterface(
            backup_info.snapshots_info.subscription_id,
            resource_group=resource_group,
            credential=_get_azure_credential(azure_credential),
        )
    elif backup_info.snapshots_info.provider == "aws":
        from barman.cloud_providers.aws_s3 import AwsCloudSnapshotInterface

        # When creating a snapshot interface for existing backups we use the region
        # from the backup_info, unless a region is set in the config in which case the
        # config region takes precedence.
        region = None
        profile = None
        if config is not None:
            if hasattr(config, "aws_region"):
                region = config.aws_region
            if hasattr(config, "aws_profile"):
                profile = config.aws_profile
        if region is None:
            region = backup_info.snapshots_info.region
        return AwsCloudSnapshotInterface(profile, region)
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
    elif "provider" in snapshots_info and snapshots_info["provider"] == "azure":
        from barman.cloud_providers.azure_blob_storage import AzureSnapshotsInfo

        return AzureSnapshotsInfo.from_dict(snapshots_info)
    elif "provider" in snapshots_info and snapshots_info["provider"] == "aws":
        from barman.cloud_providers.aws_s3 import AwsSnapshotsInfo

        return AwsSnapshotsInfo.from_dict(snapshots_info)
    else:
        raise CloudProviderUnsupported(
            "Unsupported snapshot provider in backup info: %s"
            % snapshots_info["provider"]
        )


def validate_s3_url(url):
    """
    Validate an S3 or S3-compatible URL.

    :param str url: The URL to validate
    :rtype: bool
    :return: ``True`` if the URL is valid, ``False`` otherwise
    """
    parsed_url = urlparse(url)
    if parsed_url.netloc == "" or parsed_url.scheme != "s3":
        return False
    return True


def validate_google_cloud_url(url):
    """
    Validate a Google Cloud Storage URL.

    :param str url: The URL to validate
    :rtype: bool
    :return: ``True`` if the URL is valid, ``False`` otherwise
    """
    # It must start with either the console URL or the gs:// scheme
    GOOGLE_BASE_URL = "https://console.cloud.google.com/storage/browser/"
    if not url.startswith(GOOGLE_BASE_URL) and not url.startswith("gs://"):
        return False
    gs_url = url.replace(GOOGLE_BASE_URL, "gs://")
    parsed_url = urlparse(gs_url)
    if not parsed_url.netloc:
        # The bucket name is missing
        return False
    return True


def validate_azure_blob_storage_url(url):
    """
    Validate an Azure Blob Storage URL.

    :param str url: The URL to validate
    :rtype: bool
    :return: ``True`` if the URL is valid, ``False`` otherwise
    """
    # The URL must point to the Azure Blob Storage domain
    AZURE_BLOB_STORAGE_DOMAIN = "blob.core.windows.net"
    parsed_url = urlparse(url)
    if parsed_url.netloc.endswith(AZURE_BLOB_STORAGE_DOMAIN):
        try:
            # try to get the bucket name
            parsed_url.path.split("/")[1]
            return True
        except IndexError:
            pass
    return False


def recognize_cloud_provider(url):
    """
    Check if the given URL is a valid cloud storage URL and return the corresponding
    storage provider.

    :param str url: The URL to validate
    :rtype: string|None
    :return: The storage provider, or None if the URL is invalid

    .. note::
        The strings follow the same naming used by ``barman-cloud-backup``.
    """
    if validate_s3_url(url):
        return "aws-s3"
    elif validate_google_cloud_url(url):
        return "google-cloud-storage"
    elif validate_azure_blob_storage_url(url):
        return "azure-blob-storage"
    else:
        return None
