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
from contextlib import closing

from barman.annotations import KeepManager
from barman.clients.cloud_cli import (
    GeneralErrorExit,
    NetworkErrorExit,
    OperationErrorExit,
    create_argument_parser,
)
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
            if not cloud_interface.test_connectivity():
                raise NetworkErrorExit()
            # If test is requested, just exit after connectivity test
            elif config.test:
                raise SystemExit(0)

            if not cloud_interface.bucket_exists:
                logging.error("Bucket %s does not exist", cloud_interface.bucket_name)
                raise OperationErrorExit()

            catalog = CloudBackupCatalog(cloud_interface, config.server_name)
            backup_id = catalog.parse_backup_id(config.backup_id)
            if config.release:
                catalog.release_keep(backup_id)
            elif config.status:
                target = catalog.get_keep_target(backup_id)
                if target:
                    print("Keep: %s" % target)
                else:
                    print("Keep: nokeep")
            else:
                backup_info = catalog.get_backup_info(backup_id)
                if backup_info.status == BackupInfo.DONE:
                    catalog.keep_backup(backup_id, config.target)
                else:
                    logging.error(
                        "Cannot add keep to backup %s because it has status %s. "
                        "Only backups with status DONE can be kept.",
                        backup_id,
                        backup_info.status,
                    )
                    raise OperationErrorExit()

    except Exception as exc:
        logging.error("Barman cloud keep exception: %s", force_str(exc))
        logging.debug("Exception details:", exc_info=exc)
        raise GeneralErrorExit()


def parse_arguments(args=None):
    """
    Parse command line arguments
    :return: The options parsed
    """
    parser, _, _ = create_argument_parser(
        description="This script can be used to tag backups in cloud storage as "
        "archival backups such that they will not be deleted. "
        "Currently AWS S3, Azure Blob Storage and Google Cloud Storage are supported.",
    )
    parser.add_argument(
        "backup_id",
        help="the backup ID of the backup to be kept",
    )
    keep_options = parser.add_mutually_exclusive_group(required=True)
    keep_options.add_argument(
        "-r",
        "--release",
        help="If specified, the command will remove the keep annotation and the "
        "backup will be eligible for deletion",
        action="store_true",
    )
    keep_options.add_argument(
        "-s",
        "--status",
        help="Print the keep status of the backup",
        action="store_true",
    )
    keep_options.add_argument(
        "--target",
        help="Specify the recovery target for this backup",
        choices=[KeepManager.TARGET_FULL, KeepManager.TARGET_STANDALONE],
    )
    return parser.parse_args(args=args)


if __name__ == "__main__":
    main()
