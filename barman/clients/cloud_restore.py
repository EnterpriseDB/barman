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
import logging
import os
from contextlib import closing

import barman
from barman.cloud import CloudBackupCatalog, configure_logging
from barman.cloud_providers import get_cloud_interface
from barman.utils import force_str

try:
    import argparse
except ImportError:
    raise SystemExit("Missing required python module: argparse")


def main(args=None):
    """
    The main script entry point

    :param list[str] args: the raw arguments list. When not provided
        it defaults to sys.args[1:]
    """
    config = parse_arguments(args)
    configure_logging(config)

    # Validate the destination directory before starting recovery
    if os.path.exists(config.recovery_dir) and os.listdir(config.recovery_dir):
        logging.error(
            "Destination %s already exists and it is not empty", config.recovery_dir
        )
        raise SystemExit(1)

    try:
        cloud_interface = get_cloud_interface(
            url=config.source_url,
            encryption=config.encryption,
            profile_name=config.profile,
            endpoint_url=config.endpoint_url,
            cloud_provider=config.cloud_provider,
        )

        with closing(cloud_interface):
            downloader = CloudBackupDownloader(
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

            downloader.download_backup(config.backup_id, config.recovery_dir)

    except KeyboardInterrupt as exc:
        logging.error("Barman cloud restore was interrupted by the user")
        logging.debug("Exception details:", exc_info=exc)
        raise SystemExit(1)
    except Exception as exc:
        logging.error("Barman cloud restore exception: %s", force_str(exc))
        logging.debug("Exception details:", exc_info=exc)
        raise SystemExit(1)


def parse_arguments(args=None):
    """
    Parse command line arguments

    :return: The options parsed
    """

    parser = argparse.ArgumentParser(
        description="This script can be used to download a backup "
        "previously made with barman-cloud-backup command."
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
    parser.add_argument("backup_id", help="the backup ID")
    parser.add_argument("recovery_dir", help="the path to a directory for recovery.")
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
    s3_arguments.add_argument(
        "-e",
        "--encryption",
        help="Enable server-side encryption for the transfer. "
        "Allowed values: 'AES256', 'aws:kms'",
        choices=["AES256", "aws:kms"],
        metavar="ENCRYPTION",
    )
    return parser.parse_args(args=args)


class CloudBackupDownloader(object):
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
        self.catalog = CloudBackupCatalog(cloud_interface, server_name)

    def download_backup(self, backup_id, destination_dir):
        """
        Download a backup from cloud storage

        :param str wal_name: Name of the WAL file
        :param str wal_dest: Full path of the destination WAL file
        """

        backup_info = self.catalog.get_backup_info(backup_id)

        if not backup_info:
            logging.error(
                "Backup %s for server %s does not exists", backup_id, self.server_name
            )
            raise SystemExit(1)

        backup_files = self.catalog.get_backup_files(backup_info)

        # Check that everything is ok
        copy_jobs = []
        for oid in backup_files:
            file_info = backup_files[oid]
            # PGDATA is restored where requested (destination_dir)
            if oid is None:
                target_dir = destination_dir
            else:
                # Tablespaces are restored in the original location
                # TODO: implement tablespace remapping
                for tblspc in backup_info.tablespaces:
                    if oid == tblspc.oid:
                        target_dir = tblspc.location
                        break
                else:
                    raise AssertionError(
                        "The backup file oid '%s' must be present "
                        "in backupinfo.tablespaces list"
                    )

            # Validate the destination directory before starting recovery
            if os.path.exists(target_dir) and os.listdir(target_dir):
                logging.error(
                    "Destination %s already exists and it is not empty", target_dir
                )
                raise SystemExit(1)
            copy_jobs.append([file_info, target_dir])
            for additional_file in file_info.additional_files:
                copy_jobs.append([additional_file, target_dir])

        # Now it's time to download the files
        for file_info, target_dir in copy_jobs:
            # Download the file
            logging.debug(
                "Extracting %s to %s (%s)",
                file_info.path,
                target_dir,
                "decompressing " + file_info.compression
                if file_info.compression
                else "no compression",
            )
            self.cloud_interface.extract_tar(file_info.path, target_dir)


if __name__ == "__main__":
    main()
