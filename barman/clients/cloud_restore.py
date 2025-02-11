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
import logging
import os
from abc import ABCMeta, abstractmethod
from contextlib import closing

from barman.clients.cloud_cli import (
    CLIErrorExit,
    GeneralErrorExit,
    NetworkErrorExit,
    OperationErrorExit,
    create_argument_parser,
)
from barman.cloud import CloudBackupCatalog, configure_logging
from barman.cloud_providers import (
    get_cloud_interface,
    get_snapshot_interface_from_backup_info,
)
from barman.exceptions import ConfigurationException
from barman.fs import UnixLocalCommand
from barman.recovery_executor import SnapshotRecoveryExecutor
from barman.utils import (
    check_tli,
    force_str,
    get_backup_id_from_target_lsn,
    get_backup_id_from_target_time,
    get_backup_id_from_target_tli,
    get_last_backup_id,
    parse_target_tli,
    with_metaclass,
)


def _validate_config(config, backup_info):
    """
    Additional validation for config such as mutually inclusive options.

    Raises a ConfigurationException if any options are missing or incompatible.

    :param argparse.Namespace config: The backup options provided at the command line.
    :param BackupInfo backup_info: The backup info for the backup to restore
    """
    if backup_info.snapshots_info:
        if config.tablespace != []:
            raise ConfigurationException(
                "Backup %s is a snapshot backup therefore tablespace relocation rules "
                "cannot be used." % backup_info.backup_id,
            )


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
            if not cloud_interface.test_connectivity():
                raise NetworkErrorExit()
            # If test is requested, just exit after connectivity test
            elif config.test:
                raise SystemExit(0)

            if not cloud_interface.bucket_exists:
                logging.error("Bucket %s does not exist", cloud_interface.bucket_name)
                raise OperationErrorExit()

            catalog = CloudBackupCatalog(cloud_interface, config.server_name)
            backup_id = None
            if config.backup_id != "auto":
                backup_id = catalog.parse_backup_id(config.backup_id)
            else:
                target_options = ["target_time", "target_lsn"]
                target_option = None
                for option in target_options:
                    target = getattr(config, option, None)
                    if target is not None:
                        target_option = option
                        break

                # "Parse" the string value to integer for `target_tli` if passed as a
                # string ("current", "latest")
                target_tli = parse_target_tli(obj=catalog, target_tli=config.target_tli)

                available_backups = catalog.get_backup_list().values()
                if target_option is None:
                    if target_tli is not None:
                        backup_id = get_backup_id_from_target_tli(
                            available_backups, target_tli
                        )
                    else:
                        backup_id = get_last_backup_id(available_backups)
                elif target_option == "target_time":
                    backup_id = get_backup_id_from_target_time(
                        available_backups, target, target_tli
                    )
                elif target_option == "target_lsn":
                    backup_id = get_backup_id_from_target_lsn(
                        available_backups, target, target_tli
                    )
                # If no candidate backup_id is found, error out.
                if backup_id is None:
                    logging.error("Cannot find any candidate backup for recovery.")
                    raise OperationErrorExit()

            backup_info = catalog.get_backup_info(backup_id)
            logging.info("Restoring from backup_id: %s" % backup_id)
            if not backup_info:
                logging.error(
                    "Backup %s for server %s does not exists",
                    backup_id,
                    config.server_name,
                )
                raise OperationErrorExit()

            _validate_config(config, backup_info)

            if backup_info.snapshots_info:
                snapshot_interface = get_snapshot_interface_from_backup_info(
                    backup_info, config
                )
                snapshot_interface.validate_restore_config(config)
                downloader = CloudBackupDownloaderSnapshot(
                    cloud_interface, catalog, snapshot_interface
                )
                downloader.download_backup(
                    backup_info,
                    config.recovery_dir,
                    config.snapshot_recovery_instance,
                )
            else:
                downloader = CloudBackupDownloaderObjectStore(cloud_interface, catalog)
                downloader.download_backup(
                    backup_info,
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

    parser, s3_arguments, azure_arguments = create_argument_parser(
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
    parser.add_argument(
        "--snapshot-recovery-instance",
        help="Instance where the disks recovered from the snapshots are attached",
    )
    parser.add_argument(
        "--snapshot-recovery-zone",
        help=(
            "Zone containing the instance and disks for the snapshot recovery "
            "(deprecated: replaced by --gcp-zone)"
        ),
        dest="gcp_zone",
    )
    s3_arguments.add_argument(
        "--aws-region",
        help=(
            "Name of the AWS region where the instance and disks for snapshot "
            "recovery are located"
        ),
    )
    gcs_arguments = parser.add_argument_group(
        "Extra options for google-cloud-storage cloud provider"
    )
    gcs_arguments.add_argument(
        "--gcp-zone",
        help="Zone containing the instance and disks for the snapshot recovery",
    )
    azure_arguments.add_argument(
        "--azure-resource-group",
        help="Resource group containing the instance and disks for the snapshot recovery",
    )
    parser.add_argument("--target-tli", help="target timeline", type=check_tli)
    target_args = parser.add_mutually_exclusive_group()
    target_args.add_argument("--target-lsn", help="target LSN (Log Sequence Number)")
    target_args.add_argument(
        "--target-time",
        help="target time. You can use any valid unambiguous representation. "
        'e.g: "YYYY-MM-DD HH:MM:SS.mmm"',
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


class CloudBackupDownloader(with_metaclass(ABCMeta)):
    """
    Restore a backup from cloud storage.
    """

    def __init__(self, cloud_interface, catalog):
        """
        Object responsible for handling interactions with cloud storage

        :param CloudInterface cloud_interface: The interface to use to
          upload the backup
        :param str server_name: The name of the server as configured in Barman
        :param CloudBackupCatalog catalog: The cloud backup catalog
        """
        self.cloud_interface = cloud_interface
        self.catalog = catalog

    @abstractmethod
    def download_backup(self, backup_id, destination_dir):
        """
        Download a backup from cloud storage

        :param str backup_id: The backup id to restore
        :param str destination_dir: Path to the destination directory
        """


class CloudBackupDownloaderObjectStore(CloudBackupDownloader):
    """
    Cloud storage download client for an object store backup
    """

    def download_backup(self, backup_info, destination_dir, tablespaces):
        """
        Download a backup from cloud storage

        :param BackupInfo backup_info: The backup info for the backup to restore
        :param str destination_dir: Path to the destination directory
        """
        # Validate the destination directory before starting recovery
        if os.path.exists(destination_dir) and os.listdir(destination_dir):
            logging.error(
                "Destination %s already exists and it is not empty", destination_dir
            )
            raise OperationErrorExit()

        backup_files = self.catalog.get_backup_files(backup_info)

        # We must download and restore a bunch of .tar files that contain PGDATA
        # and each tablespace. First, we determine a target directory to extract
        # each tar file into and record these in copy_jobs. For each tablespace,
        # the location may be overridden by `--tablespace name:/new/location` on
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
                (
                    "decompressing " + file_info.compression
                    if file_info.compression
                    else "no compression"
                ),
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


class CloudBackupDownloaderSnapshot(CloudBackupDownloader):
    """A minimal downloader for cloud backups which just retrieves the backup label."""

    def __init__(self, cloud_interface, catalog, snapshot_interface):
        """
        Object responsible for handling interactions with cloud storage

        :param CloudInterface cloud_interface: The interface to use to
          upload the backup
        :param str server_name: The name of the server as configured in Barman
        :param CloudBackupCatalog catalog: The cloud backup catalog
        :param CloudSnapshotInterface snapshot_interface: Interface for managing
            snapshots via a cloud provider API.
        """
        super(CloudBackupDownloaderSnapshot, self).__init__(cloud_interface, catalog)
        self.snapshot_interface = snapshot_interface

    def download_backup(
        self,
        backup_info,
        destination_dir,
        recovery_instance,
    ):
        """
        Download a backup from cloud storage

        :param BackupInfo backup_info: The backup info for the backup to restore
        :param str destination_dir: Path to the destination directory
        :param str recovery_instance: The name of the VM instance to which the disks
            cloned from the backup snapshots are attached.
        """
        attached_volumes = SnapshotRecoveryExecutor.get_attached_volumes_for_backup(
            self.snapshot_interface,
            backup_info,
            recovery_instance,
        )
        cmd = UnixLocalCommand()
        SnapshotRecoveryExecutor.check_mount_points(backup_info, attached_volumes, cmd)
        SnapshotRecoveryExecutor.check_recovery_dir_exists(destination_dir, cmd)

        # If the target directory does not exist then we will fail here because
        # it tells us the snapshot has not been restored.
        return self.cloud_interface.download_file(
            "/".join((self.catalog.prefix, backup_info.backup_id, "backup_label")),
            os.path.join(destination_dir, "backup_label"),
            decompress=None,
        )


if __name__ == "__main__":
    main()
