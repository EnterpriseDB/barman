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
import os
import sys
from contextlib import closing

import barman
from barman.cloud import configure_logging, ALLOWED_COMPRESSIONS
from barman.cloud_providers import get_cloud_interface
from barman.exceptions import BarmanException
from barman.utils import force_str
from barman.xlog import hash_dir, is_any_xlog_file, is_backup_file


def main(args=None):
    """
    The main script entry point

    :param list[str] args: the raw arguments list. When not provided
        it defaults to sys.args[1:]
    """
    config = parse_arguments(args)
    configure_logging(config)

    # Validate the WAL file name before downloading it
    if not is_any_xlog_file(config.wal_name):
        logging.error("%s is an invalid name for a WAL file" % config.wal_name)
        raise SystemExit(1)

    try:
        cloud_interface = get_cloud_interface(config)

        with closing(cloud_interface):
            downloader = CloudWalDownloader(
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

            downloader.download_wal(config.wal_name, config.wal_dest)

    except Exception as exc:
        logging.error("Barman cloud WAL restore exception: %s", force_str(exc))
        logging.debug("Exception details:", exc_info=exc)
        raise SystemExit(1)


def parse_arguments(args=None):
    """
    Parse command line arguments

    :return: The options parsed
    """

    parser = argparse.ArgumentParser(
        description="This script can be used as a `restore_command` "
        "to download WAL files previously archived with "
        "barman-cloud-wal-archive command. "
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
        "wal_name",
        help="The value of the '%%f' keyword (according to 'restore_command').",
    )
    parser.add_argument(
        "wal_dest",
        help="The value of the '%%p' keyword (according to 'restore_command').",
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
    return parser.parse_args(args=args)


class CloudWalDownloader(object):
    """
    Cloud storage download client
    """

    def __init__(self, cloud_interface, server_name):
        """
        Object responsible for handling interactions with cloud storage

        :param CloudInterface cloud_interface: The interface to use to
          upload the backup
        :param str server_name: The name of the server as configured in Barman
        """

        self.cloud_interface = cloud_interface
        self.server_name = server_name

    def download_wal(self, wal_name, wal_dest):
        """
        Download a WAL file from cloud storage

        :param str wal_name: Name of the WAL file
        :param str wal_dest: Full path of the destination WAL file
        """

        # Correctly format the source path on s3
        source_dir = os.path.join(
            self.cloud_interface.path, self.server_name, "wals", hash_dir(wal_name)
        )
        # Add a path separator if needed
        if not source_dir.endswith(os.path.sep):
            source_dir += os.path.sep

        wal_path = os.path.join(source_dir, wal_name)

        remote_name = None
        # Automatically detect compression based on the file extension
        compression = None
        for item in self.cloud_interface.list_bucket(source_dir):
            # perfect match (uncompressed file)
            if item == wal_path:
                remote_name = item
            # look for compressed files or .partial files
            elif item.startswith(wal_path):
                # Detect compression
                basename = item
                for e, c in ALLOWED_COMPRESSIONS.items():
                    if item[-len(e) :] == e:
                        # Strip extension
                        basename = basename[: -len(e)]
                        compression = c
                        break

                # Check basename is a known xlog file (.partial?)
                if not is_any_xlog_file(basename):
                    logging.warning("Unknown WAL file: %s", item)
                    continue
                # Exclude backup informative files (not needed in recovery)
                elif is_backup_file(basename):
                    logging.info("Skipping backup file: %s", item)
                    continue

                # Found candidate
                remote_name = item
                logging.info(
                    "Found WAL %s for server %s as %s",
                    wal_name,
                    self.server_name,
                    remote_name,
                )
                break

        if not remote_name:
            logging.info(
                "WAL file %s for server %s does not exists", wal_name, self.server_name
            )
            raise SystemExit(1)

        if compression and sys.version_info < (3, 0, 0):
            raise BarmanException(
                "Compressed WALs cannot be restored with Python 2.x - "
                "please upgrade to a supported version of Python 3"
            )

        # Download the file
        logging.debug(
            "Downloading %s to %s (%s)",
            remote_name,
            wal_dest,
            "decompressing " + compression if compression else "no compression",
        )
        self.cloud_interface.download_file(remote_name, wal_dest, compression)


if __name__ == "__main__":
    main()
