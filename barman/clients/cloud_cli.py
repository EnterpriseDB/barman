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
# along with Barman.  If not, see <http://www.gnu.org/licenses/>.

import argparse
import csv
import logging

import barman
from barman.utils import force_str


class OperationErrorExit(SystemExit):
    """
    Dedicated exit code for errors where connectivity to the cloud provider was ok
    but the operation still failed.
    """

    def __init__(self):
        super(OperationErrorExit, self).__init__(1)


class NetworkErrorExit(SystemExit):
    """Dedicated exit code for network related errors."""

    def __init__(self):
        super(NetworkErrorExit, self).__init__(2)


class CLIErrorExit(SystemExit):
    """Dedicated exit code for CLI level errors."""

    def __init__(self):
        super(CLIErrorExit, self).__init__(3)


class GeneralErrorExit(SystemExit):
    """Dedicated exit code for general barman cloud errors."""

    def __init__(self):
        super(GeneralErrorExit, self).__init__(4)


class UrlArgumentType(object):
    source = "source"
    destination = "destination"


def get_missing_attrs(config, attrs):
    """
    Returns list of each attr not found in config.

    :param argparse.Namespace config: The backup options provided at the command line.
    :param list[str] attrs: List of attribute names to be searched for in the config.
    :rtype: list[str]
    :return: List of all items in attrs which were not found as attributes of config.
    """
    missing_options = []
    for attr in attrs:
        if not getattr(config, attr):
            missing_options.append(attr)
    return missing_options


def __parse_tag(tag):
    """Parse key,value tag with csv reader"""
    try:
        rows = list(csv.reader([tag], delimiter=","))
    except csv.Error as exc:
        logging.error(
            "Error parsing tag %s: %s",
            tag,
            force_str(exc),
        )
        raise CLIErrorExit()
    if len(rows) != 1 or len(rows[0]) != 2:
        logging.error(
            "Invalid tag format: %s",
            tag,
        )
        raise CLIErrorExit()

    return tuple(rows[0])


def add_tag_argument(parser, name, help):
    parser.add_argument(
        "--%s" % name,
        type=__parse_tag,
        nargs="*",
        help=help,
    )


class CloudArgumentParser(argparse.ArgumentParser):
    """ArgumentParser which exits with CLIErrorExit on errors."""

    def error(self, message):
        try:
            super(CloudArgumentParser, self).error(message)
        except SystemExit:
            raise CLIErrorExit()


def create_argument_parser(description, source_or_destination=UrlArgumentType.source):
    """
    Create a barman-cloud argument parser with the given description.

    Returns an `argparse.ArgumentParser` object which parses the core arguments
    and options for barman-cloud commands.
    """
    parser = CloudArgumentParser(
        description=description,
        add_help=False,
    )
    parser.add_argument(
        "%s_url" % source_or_destination,
        help=(
            "URL of the cloud %s, such as a bucket in AWS S3."
            " For example: `s3://bucket/path/to/folder`."
        )
        % source_or_destination,
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
        choices=["aws-s3", "azure-blob-storage", "google-cloud-storage"],
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
        "--aws-profile",
        help="profile name (e.g. INI section in AWS credentials file)",
    )
    s3_arguments.add_argument(
        "--profile",
        help="profile name (deprecated: replaced by --aws-profile)",
        dest="aws_profile",
    )
    s3_arguments.add_argument(
        "--read-timeout",
        type=int,
        help="the time in seconds until a timeout is raised when waiting to "
        "read from a connection (defaults to 60 seconds)",
    )
    azure_arguments = parser.add_argument_group(
        "Extra options for the azure-blob-storage cloud provider"
    )
    azure_arguments.add_argument(
        "--azure-credential",
        "--credential",
        "--default",
        choices=["azure-cli", "managed-identity", "default"],
        help="Optionally specify the type of credential to use when authenticating "
        "with Azure. If omitted then Azure Blob Storage credentials will be obtained "
        "from the environment and the default Azure authentication flow will be used "
        "for authenticating with all other Azure services. If no credentials can be "
        "found in the environment then the default Azure authentication flow will "
        "also be used for Azure Blob Storage.",
        dest="azure_credential",
    )
    return parser, s3_arguments, azure_arguments
