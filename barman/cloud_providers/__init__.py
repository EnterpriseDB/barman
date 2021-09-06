# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2022
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

from barman.exceptions import BarmanException


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
    _update_kwargs(cloud_interface_kwargs, config, ("jobs", "tags"))

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
