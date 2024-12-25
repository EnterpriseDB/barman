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

from __future__ import print_function

import json
import logging
from contextlib import closing

from barman.clients.cloud_cli import (
    GeneralErrorExit,
    NetworkErrorExit,
    OperationErrorExit,
    create_argument_parser,
)
from barman.cloud import CloudBackupCatalog, configure_logging
from barman.cloud_providers import get_cloud_interface
from barman.output import ConsoleOutputWriter
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
                raise NetworkErrorExit()
            # If test is requested, just exit after connectivity test
            elif config.test:
                raise SystemExit(0)

            if not cloud_interface.bucket_exists:
                logging.error("Bucket %s does not exist", cloud_interface.bucket_name)
                raise OperationErrorExit()

            backup_id = catalog.parse_backup_id(config.backup_id)
            backup_info = catalog.get_backup_info(backup_id)

            if not backup_info:
                logging.error(
                    "Backup %s for server %s does not exist",
                    backup_id,
                    config.server_name,
                )
                raise OperationErrorExit()

            # Output
            if config.format == "console":
                ConsoleOutputWriter.render_show_backup(backup_info.to_dict(), print)
            else:
                # Match the `barman show-backup` top level structure
                json_output = {backup_info.server_name: backup_info.to_json()}
                print(json.dumps(json_output))

    except Exception as exc:
        logging.error("Barman cloud backup show exception: %s", force_str(exc))
        logging.debug("Exception details:", exc_info=exc)
        raise GeneralErrorExit()


def parse_arguments(args=None):
    """
    Parse command line arguments

    :param list[str] args: The raw arguments list
    :return: The options parsed
    """

    parser, _, _ = create_argument_parser(
        description="This script can be used to show metadata for backups "
        "made with barman-cloud-backup command. "
        "Currently AWS S3, Azure Blob Storage and Google Cloud Storage are supported.",
    )
    parser.add_argument("backup_id", help="the backup ID")

    parser.add_argument(
        "--format",
        default="console",
        help="Output format (console or json). Default console.",
    )
    return parser.parse_args(args=args)


if __name__ == "__main__":
    main()
