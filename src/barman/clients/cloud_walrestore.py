# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2026
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

from barman.clients.cloud_cli import (
    CLIErrorExit,
    GeneralErrorExit,
    NetworkErrorExit,
    create_argument_parser,
)
from barman.cloud import CloudWalDownloader, configure_logging
from barman.cloud_providers import get_cloud_interface
from barman.utils import force_str
from barman.xlog import is_any_xlog_file

_logger = logging.getLogger(__name__)


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
        _logger.error("%s is an invalid name for a WAL file" % config.wal_name)
        raise CLIErrorExit()

    cloud_interface = None
    try:
        cloud_interface = get_cloud_interface(config)

        with closing(cloud_interface):
            # Do connectivity test if requested
            if config.test:
                cloud_interface.verify_cloud_connectivity_and_bucket_existence()
                raise SystemExit(0)

            downloader = CloudWalDownloader(
                cloud_interface=cloud_interface,
                server_name=config.server_name,
                spool_dir=config.spool_dir,
            )
            downloader.download_wal(
                config.wal_name, config.wal_dest, config.no_partial, config.parallel
            )

    except Exception as exc:
        _logger.error("Barman cloud WAL restore exception: %s", force_str(exc))
        _logger.debug("Exception details:", exc_info=exc)
        # If the failure was caused by loss of connectivity to the cloud
        # provider, surface it as a dedicated network error (exit code 2)
        # rather than a generic one.
        if cloud_interface is not None and cloud_interface.is_connectivity_error(exc):
            raise NetworkErrorExit()
        raise GeneralErrorExit()


def parse_arguments(args=None):
    """
    Parse command line arguments

    .. note::
        The ``--no-partial`` option addresses a specific Postgres behavior
        with partial WAL segments. If a standby server is archiving and gets
        promoted to primary, it will attempt to archive the incomplete WAL
        segment as a .partial file, and then the WALs from the new timeline.
        When executing a ``barman-cloud-wal-restore`` through ``restore_command``,
        it might find the .partial file instead of the complete WAL file and
        unintentionally apply those changes.
        Using ``--no-partial`` allows ``barman-cloud-wal-restore`` to intentionally
        skip partial files, forcing Postgres to continue replay on the new
        timeline instead.

    :param list[str] args: Command line arguments to parse
    :return: The options parsed
    """

    parser, s3_arguments, _ = create_argument_parser(
        description="This script can be used as a `restore_command` "
        "to download WAL files previously archived with "
        "barman-cloud-wal-archive command. "
        "Currently AWS S3, Azure Blob Storage and Google Cloud Storage are supported.",
    )

    s3_arguments.add_argument(
        "--sse-customer-key",
        help="Path to a file containing the customer-provided encryption key (SSE-C) "
        "to use for decrypting data in S3, specified as a file:// URI "
        "(e.g. file:///path/to/key.b64). The file must contain a base64-encoded "
        "256-bit key.",
    )

    parser.add_argument(
        "--no-partial",
        help="Do not download partial WAL files",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-p",
        "--parallel",
        default=0,
        type=int,
        help="Specifies the number of files to peek and download in parallel. "
        "Default is 0 (disabled)",
    )
    parser.add_argument(
        "--spool-dir",
        default=CloudWalDownloader.DEFAULT_SPOOL_DIR,
        metavar="SPOOL_DIR",
        help="Specifies a spool directory for extra WAL files that are downloaded in "
        "parallel. Defaults to '{0}'.".format(CloudWalDownloader.DEFAULT_SPOOL_DIR),
    )
    parser.add_argument(
        "wal_name",
        help="The value of the '%%f' keyword (according to 'restore_command').",
    )
    parser.add_argument(
        "wal_dest",
        help="The value of the '%%p' keyword (according to 'restore_command').",
    )
    return parser.parse_args(args=args)


if __name__ == "__main__":
    main()
