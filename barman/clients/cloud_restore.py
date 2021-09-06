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
# along with Barman.  If not, see <http://www.gnu.org/licenses/>.
import logging
import os
from contextlib import closing

from barman.clients.cloud_cli import (
    CLIErrorExit,
    create_argument_parser,
    GeneralErrorExit,
    NetworkErrorExit,
    OperationErrorExit,
)
from barman.cloud import CloudBackupCatalog, configure_logging
from barman.cloud_providers import get_cloud_interface
from barman.utils import force_str


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
        raise OperationErrorExit()

    try:
        cloud_interface = get_cloud_interface(config)

        with closing(cloud_interface):
            downloader = CloudBackupDownloader(
                cloud_interface=cloud_interface, server_name=config.server_name
            )

            if not cloud_interface.test_connectivity():
                raise NetworkErrorExit()
            # If test is requested, just exit after connectivity test
            elif config.test:
                raise SystemExit(0)

            if not cloud_interface.bucket_exists:
                logging.error("Bucket %s does not exist", cloud_interface.bucket_name)
                raise OperationErrorExit()

            downloader.download_backup(
                config.backup_id,
                config.recovery_dir,
                tablespace_map(config.tablespace),
            )

    except KeyboardInterrupt as exc:
        logging.error("Barman cloud restore was interrupted by the user")
        logging.debug("Exception details:", exc_info=exc)
        raise OperationErrorExit()
    except Exception as exc:
        logging.error("Barman cloud restore exception: %s", force_str(exc))
        logging.debug("Exception details:", exc_info=exc)
        raise GeneralErrorExit()


def parse_arguments(args=None):
    """
    Parse command line arguments

    :return: The options parsed
    """

    parser, _, _ = create_argument_parser(
        description="This script can be used to download a backup "
        "previously made with barman-cloud-backup command."
        "Currently AWS S3, Azure Blob Storage and Google Cloud Storage are supported.",
    )
    parser.add_argument("backup_id", help="the backup ID")
    parser.add_argument("recovery_dir", help="the path to a directory for recovery.")
    parser.add_argument(
        "--tablespace",
        help="tablespace relocation rule",
        metavar="NAME:LOCATION",
        action="append",
        default=[],
    )
    return parser.parse_args(args=args)


def tablespace_map(rules):
    """
    Return a mapping from tablespace names to locations built from any
    `--tablespace name:/loc/ation` rules specified.
    """
    tablespaces = {}
    for rule in rules:
        try:
            tablespaces.update([rule.split(":", 1)])
        except ValueError:
            logging.error(
                "Invalid tablespace relocation rule '%s'\n"
                "HINT: The valid syntax for a relocation rule is "
                "NAME:LOCATION",
                rule,
            )
            raise CLIErrorExit()
    return tablespaces


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

    def download_backup(self, backup_id, destination_dir, tablespaces):
        """
        Download a backup from cloud storage

        :param str backup_id: The backup id to restore
        :param str destination_dir: Path to the destination directory
        """

        backup_info = self.catalog.get_backup_info(backup_id)

        if not backup_info:
            logging.error(
                "Backup %s for server %s does not exists", backup_id, self.server_name
            )
            raise OperationErrorExit()

        backup_files = self.catalog.get_backup_files(backup_info)

        # We must download and restore a bunch of .tar files that contain PGDATA
        # and each tablespace. First, we determine a target directory to extract
        # each tar file into and record these in copy_jobs. For each tablespace,
        # the location may be overriden by `--tablespace name:/new/location` on
        # the command-line; and we must also add an entry to link_jobs to create
        # a symlink from $PGDATA/pg_tblspc/oid to the correct location after the
        # downloads.

        copy_jobs = []
        link_jobs = []
        for oid in backup_files:
            file_info = backup_files[oid]
            # PGDATA is restored where requested (destination_dir)
            if oid is None:
                target_dir = destination_dir
            else:
                for tblspc in backup_info.tablespaces:
                    if oid == tblspc.oid:
                        target_dir = tblspc.location
                        if tblspc.name in tablespaces:
                            target_dir = os.path.realpath(tablespaces[tblspc.name])
                        logging.debug(
                            "Tablespace %s (oid=%s) will be located at %s",
                            tblspc.name,
                            oid,
                            target_dir,
                        )
                        link_jobs.append(
                            ["%s/pg_tblspc/%s" % (destination_dir, oid), target_dir]
                        )
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
                raise OperationErrorExit()
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

        for link, target in link_jobs:
            os.symlink(target, link)

        # If we did not restore the pg_wal directory from one of the uploaded
        # backup files, we must recreate it here. (If pg_wal was originally a
        # symlink, it would not have been uploaded.)

        wal_path = os.path.join(destination_dir, backup_info.wal_directory())
        if not os.path.exists(wal_path):
            os.mkdir(wal_path)


if __name__ == "__main__":
    main()
