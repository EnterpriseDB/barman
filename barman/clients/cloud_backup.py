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
import re
import tempfile
from contextlib import closing
from shutil import rmtree

import barman
from barman.cloud import CloudBackupUploader, configure_logging
from barman.cloud_providers import get_cloud_interface
from barman.exceptions import PostgresConnectionError
from barman.postgres import PostgreSQLConnection
from barman.utils import check_positive, check_size, force_str

try:
    import argparse
except ImportError:
    raise SystemExit("Missing required python module: argparse")

_find_space = re.compile(r"[\s]").search


def quote_conninfo(value):
    """
    Quote a connection info parameter

    :param str value:
    :rtype: str
    """
    if not value:
        return "''"
    if not _find_space(value):
        return value
    return "'%s'" % value.replace("\\", "\\\\").replace("'", "\\'")


def build_conninfo(config):
    """
    Build a DSN to connect to postgres using command-line arguments
    """
    conn_parts = []
    if config.host:
        conn_parts.append("host=%s" % quote_conninfo(config.host))
    if config.port:
        conn_parts.append("port=%s" % quote_conninfo(config.port))
    if config.user:
        conn_parts.append("user=%s" % quote_conninfo(config.user))
    return " ".join(conn_parts)


def main(args=None):
    """
    The main script entry point

    :param list[str] args: the raw arguments list. When not provided
        it defaults to sys.args[1:]
    """
    config = parse_arguments(args)
    configure_logging(config)
    tempdir = tempfile.mkdtemp(prefix="barman-cloud-backup-")
    try:
        # Create any temporary file in the `tempdir` subdirectory
        tempfile.tempdir = tempdir

        conninfo = build_conninfo(config)
        postgres = PostgreSQLConnection(
            conninfo,
            config.immediate_checkpoint,
            application_name="barman_cloud_backup",
        )
        try:
            postgres.connect()
        except PostgresConnectionError as exc:
            logging.error("Cannot connect to postgres: %s", force_str(exc))
            logging.debug("Exception details:", exc_info=exc)
            raise SystemExit(1)

        with closing(postgres):
            cloud_interface = get_cloud_interface(
                url=config.destination_url,
                encryption=config.encryption,
                jobs=config.jobs,
                profile_name=config.profile,
                endpoint_url=config.endpoint_url,
                cloud_provider=config.cloud_provider,
            )

            if not cloud_interface.test_connectivity():
                raise SystemExit(1)
            # If test is requested, just exit after connectivity test
            elif config.test:
                raise SystemExit(0)

            with closing(cloud_interface):

                # TODO: Should the setup be optional?
                cloud_interface.setup_bucket()

                uploader = CloudBackupUploader(
                    server_name=config.server_name,
                    compression=config.compression,
                    postgres=postgres,
                    max_archive_size=config.max_archive_size,
                    cloud_interface=cloud_interface,
                )

                # Perform the backup
                uploader.backup()
    except KeyboardInterrupt as exc:
        logging.error("Barman cloud backup was interrupted by the user")
        logging.debug("Exception details:", exc_info=exc)
        raise SystemExit(1)
    except Exception as exc:
        logging.error("Barman cloud backup exception: %s", force_str(exc))
        logging.debug("Exception details:", exc_info=exc)
        raise SystemExit(1)
    finally:
        # Remove the temporary directory and all the contained files
        rmtree(tempdir, ignore_errors=True)


def parse_arguments(args=None):
    """
    Parse command line arguments

    :return: The options parsed
    """

    parser = argparse.ArgumentParser(
        description="This script can be used to perform a backup "
        "of a local PostgreSQL instance and ship "
        "the resulting tarball(s) to the Cloud. "
        "Currently AWS S3 and Azure Blob Storage are supported.",
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
    compression = parser.add_mutually_exclusive_group()
    compression.add_argument(
        "-z",
        "--gzip",
        help="gzip-compress the WAL while uploading to the cloud",
        action="store_const",
        const="gz",
        dest="compression",
    )
    compression.add_argument(
        "-j",
        "--bzip2",
        help="bzip2-compress the WAL while uploading to the cloud",
        action="store_const",
        const="bz2",
        dest="compression",
    )
    parser.add_argument(
        "-t",
        "--test",
        help="Test cloud connectivity and exit",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-h",
        "--host",
        help="host or Unix socket for PostgreSQL connection "
        "(default: libpq settings)",
    )
    parser.add_argument(
        "-p",
        "--port",
        help="port for PostgreSQL connection (default: libpq settings)",
    )
    parser.add_argument(
        "-U",
        "--user",
        help="user name for PostgreSQL connection (default: libpq settings)",
    )
    parser.add_argument(
        "--immediate-checkpoint",
        help="forces the initial checkpoint to be done as quickly as possible",
        action="store_true",
    )
    parser.add_argument(
        "-J",
        "--jobs",
        type=check_positive,
        help="number of subprocesses to upload data to cloud storage " "(default: 2)",
        default=2,
    )
    parser.add_argument(
        "-S",
        "--max-archive-size",
        type=check_size,
        help="maximum size of an archive when uploading to cloud storage "
        "(default: 100GB)",
        default="100GB",
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
        "Allowed values: 'AES256'|'aws:kms'.",
        choices=["AES256", "aws:kms"],
    )
    return parser.parse_args(args=args)


if __name__ == "__main__":
    main()
