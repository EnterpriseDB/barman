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
import logging

import barman
from barman.cloud import configure_logging, CloudBackupCatalog
from barman.cloud_providers import get_cloud_interface
from barman.exceptions import WalArchiveContentError
from barman.utils import force_str, check_positive
from barman.xlog import check_archive_usable


def main(args=None):
    """
    The main script entry point

    :param list[str] args: the raw arguments list. When not provided
        it defaults to sys.args[1:]
    """
    config = parse_arguments(args)
    configure_logging(config)

    try:
        cloud_interface = get_cloud_interface(config)
        catalog = CloudBackupCatalog(cloud_interface, config.server_name)
        wals = list(catalog.get_wal_paths().keys())
        check_archive_usable(
            wals,
            timeline=config.timeline,
        )
    except WalArchiveContentError as err:
        logging.error(
            "WAL archive check failed for server %s: %s",
            config.server_name,
            force_str(err),
        )
        raise SystemExit(1)
    except Exception as exc:
        logging.error("Barman cloud WAL archive check exception: %s", force_str(exc))
        logging.debug("Exception details:", exc_info=exc)
        raise SystemExit(2)


def parse_arguments(args=None):
    """
    Parse command line arguments

    :return: The options parsed
    """

    parser = argparse.ArgumentParser(
        description="Checks that the WAL archive on the specified cloud storage "
        "can be safely used for a new PostgreSQL server.",
        add_help=False,
    )
    parser.add_argument(
        "destination_url",
        help="URL of the cloud destination, such as a bucket in AWS S3."
        " For example: `s3://bucket/path/to/folder`.",
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
        "--timeline",
        help="The earliest timeline whose WALs should cause the check to fail",
        type=check_positive,
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
    return parser.parse_args(args=args)


if __name__ == "__main__":
    main()
