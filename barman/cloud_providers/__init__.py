# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2021
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


def get_cloud_interface(config):
    """
    Create a CloudInterface for the specified cloud_provider

    :returns: A CloudInterface for the specified cloud_provider
    :rtype: CloudInterface
    """
    cloud_interface_kwargs = {
        "url": config.source_url if "source_url" in config else config.destination_url
    }
    if "jobs" in config:
        cloud_interface_kwargs["jobs"] = config.jobs

    if config.cloud_provider == "aws-s3":
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

    elif config.cloud_provider == "azure-blob-storage":
        from barman.cloud_providers.azure_blob_storage import AzureCloudInterface

        if "encryption_scope" in config:
            cloud_interface_kwargs["encryption_scope"] = config.encryption_scope
        return AzureCloudInterface(**cloud_interface_kwargs)
