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
# along with Barman.  If not, see <http://www.gnu.org/licenses/>.

import argparse

from enum import Enum

import barman


class UrlArgumentType(Enum):
    source = "source"
    destination = "destination"


def create_argument_parser(description, source_or_destination=UrlArgumentType.source):
    """
    Create a barman-cloud argument parser with the given description.

    Returns an `argparse.ArgumentParser` object which parses the core arguments
    and options for barman-cloud commands.
    """
    parser = argparse.ArgumentParser(
        description=description,
        add_help=False,
    )
    parser.add_argument(
        "%s_url" % source_or_destination.value,
        help=(
            "URL of the cloud %s, such as a bucket in AWS S3."
            " For example: `s3://bucket/path/to/folder`."
        )
        % source_or_destination.value,
    )
    parser.add_argument(
        "server_name", help="the name of the server as configured in Barman."
    )
    parser.add_argument(
        "-V", "--version", action="version", version="%%(prog)s %s" % barman.__version__
    )
    parser.add_argument("--help", action="help", help="show this help message and exit")
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="increase output verbosity (e.g., -vv is more than -v)",
    )
    verbosity.add_argument(
        "-q",
        "--quiet",
        action="count",
        default=0,
        help="decrease output verbosity (e.g., -qq is less than -q)",
    )
    parser.add_argument(
        "-t",
        "--test",
        help="Test cloud connectivity and exit",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--cloud-provider",
        help="The cloud provider to use as a storage backend",
        choices=["aws-s3", "azure-blob-storage"],
        default="aws-s3",
    )
    s3_arguments = parser.add_argument_group(
        "Extra options for the aws-s3 cloud provider"
    )
    s3_arguments.add_argument(
        "--endpoint-url",
        help="Override default S3 endpoint URL with the given one",
    )
    s3_arguments.add_argument(
        "-P",
        "--profile",
        help="profile name (e.g. INI section in AWS credentials file)",
    )
    azure_arguments = parser.add_argument_group(
        "Extra options for the azure-blob-storage cloud provider"
    )
    azure_arguments.add_argument(
        "--credential",
        choices=["azure-cli", "managed-identity"],
        help="Optionally specify the type of credential to use when "
        "authenticating with Azure Blob Storage. If omitted then "
        "the credential will be obtained from the environment. If no "
        "credentials can be found in the environment then the default "
        "Azure authentication flow will be used",
    )
    return parser, s3_arguments, azure_arguments


azure = [
    (
        "--credential",
        {
            "choices": ["azure-cli", "managed-identity"],
            "help": (
                "Optionally specify the type of credential to use when "
                "authenticating with Azure Blob Storage. If omitted then "
                "the credential will be obtained from the environment. If no "
                "credentials can be found in the environment then the default "
                "Azure authentication flow will be used"
            ),
        },
    ),
]
