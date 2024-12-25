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

from barman.clients.cloud_cli import (
    GeneralErrorExit,
    NetworkErrorExit,
    OperationErrorExit,
    UrlArgumentType,
    create_argument_parser,
)
from barman.cloud import CloudBackupCatalog, configure_logging
from barman.cloud_providers import get_cloud_interface
from barman.exceptions import WalArchiveContentError
from barman.utils import check_positive, force_str
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
        if not cloud_interface.test_connectivity():
            # Deliberately raise an error if we cannot connect
            raise NetworkErrorExit()
        # If test is requested, just exit after connectivity test
        elif config.test:
            raise SystemExit(0)
        if not cloud_interface.bucket_exists:
            # If the bucket does not exist then the check should pass
            return
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
        raise OperationErrorExit()
    except Exception as exc:
        logging.error("Barman cloud WAL archive check exception: %s", force_str(exc))
        logging.debug("Exception details:", exc_info=exc)
        raise GeneralErrorExit()


def parse_arguments(args=None):
    """
    Parse command line arguments

    :return: The options parsed
    """

    parser, _, _ = create_argument_parser(
        description="Checks that the WAL archive on the specified cloud storage "
        "can be safely used for a new PostgreSQL server.",
        source_or_destination=UrlArgumentType.destination,
    )
    parser.add_argument(
        "--timeline",
        help="The earliest timeline whose WALs should cause the check to fail",
        type=check_positive,
    )
    return parser.parse_args(args=args)


if __name__ == "__main__":
    main()
