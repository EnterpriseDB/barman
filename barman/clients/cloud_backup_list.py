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
import json
import logging
from contextlib import closing

import barman
from barman.cloud import CloudBackupCatalog, configure_logging
from barman.cloud_providers import get_cloud_interface
from barman.infofile import BackupInfo
from barman.utils import force_str


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

        with closing(cloud_interface):
            catalog = CloudBackupCatalog(
                cloud_interface=cloud_interface, server_name=config.server_name
            )

            if not cloud_interface.test_connectivity():
                raise SystemExit(1)
            # If test is requested, just exit after connectivity test
            elif config.test:
                raise SystemExit(0)

            if not cloud_interface.bucket_exists:
                logging.error("Bucket %s does not exist", cloud_interface.bucket_name)
                raise SystemExit(1)

            backup_list = catalog.get_backup_list()

            # Output
            if config.format == "console":
                COLUMNS = "{:<20}{:<25}{:<30}{:<16}"
                print(
                    COLUMNS.format(
                        "Backup ID", "End Time", "Begin Wal", "Archival Status"
                    )
                )
                for backup_id in sorted(backup_list):
                    item = backup_list[backup_id]
                    if item and item.status == BackupInfo.DONE:
                        keep_target = catalog.get_keep_target(item.backup_id)
                        keep_status = (
                            keep_target and "KEEP:%s" % keep_target.upper() or ""
                        )
                        print(
                            COLUMNS.format(
                                item.backup_id,
                                item.end_time.strftime("%Y-%m-%d %H:%M:%S"),
                                item.begin_wal,
                                keep_status,
                            )
                        )
            else:
                print(
                    json.dumps(
                        {
                            "backups_list": [
                                backup_list[backup_id].to_json()
                                for backup_id in sorted(backup_list)
                            ]
                        }
                    )
                )

    except Exception as exc:
        logging.error("Barman cloud backup list exception: %s", force_str(exc))
        logging.debug("Exception details:", exc_info=exc)
        raise SystemExit(1)


def parse_arguments(args=None):
    """
    Parse command line arguments

    :return: The options parsed
    """

    parser = argparse.ArgumentParser(
        description="This script can be used to list backups "
        "made with barman-cloud-backup command. "
        "Currently AWS S3 and Azure Blob Storage are supported.",
        add_help=False,
    )

    parser.add_argument(
        "source_url",
        help="URL of the cloud source, such as a bucket in AWS S3."
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
        "--format",
        default="console",
        help="Output format (console or json). Default console.",
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
