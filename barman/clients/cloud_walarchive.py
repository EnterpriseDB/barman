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
import os.path
from contextlib import closing

from barman.clients.cloud_cli import (
    CLIErrorExit,
    GeneralErrorExit,
    NetworkErrorExit,
    UrlArgumentType,
    add_tag_argument,
    create_argument_parser,
)
from barman.clients.cloud_compression import compress
from barman.cloud import configure_logging
from barman.cloud_providers import get_cloud_interface
from barman.config import parse_compression_level
from barman.exceptions import BarmanException
from barman.utils import check_positive, check_size, force_str
from barman.xlog import hash_dir, is_any_xlog_file, is_history_file


def __is_hook_script():
    """Check the environment and determine if we are running as a hook script"""
    if "BARMAN_HOOK" in os.environ and "BARMAN_PHASE" in os.environ:
        if (
            os.getenv("BARMAN_HOOK") in ("archive_script", "archive_retry_script")
            and os.getenv("BARMAN_PHASE") == "pre"
        ):
            return True
        else:
            raise BarmanException(
                "barman-cloud-wal-archive called as unsupported hook script: %s_%s"
                % (os.getenv("BARMAN_PHASE"), os.getenv("BARMAN_HOOK"))
            )
    else:
        return False


def main(args=None):
    """
    The main script entry point

    :param list[str] args: the raw arguments list. When not provided
        it defaults to sys.args[1:]
    """
    config = parse_arguments(args)
    configure_logging(config)

    # Read wal_path from environment if we're a hook script
    if __is_hook_script():
        if "BARMAN_FILE" not in os.environ:
            raise BarmanException("Expected environment variable BARMAN_FILE not set")
        config.wal_path = os.getenv("BARMAN_FILE")
    else:
        if config.wal_path is None:
            raise BarmanException("the following arguments are required: wal_path")

    # Validate the WAL file name before uploading it
    if not is_any_xlog_file(config.wal_path):
        logging.error("%s is an invalid name for a WAL file" % config.wal_path)
        raise CLIErrorExit()

    try:
        cloud_interface = get_cloud_interface(config)

        with closing(cloud_interface):
            uploader = CloudWalUploader(
                cloud_interface=cloud_interface,
                server_name=config.server_name,
                compression=config.compression,
                compression_level=config.compression_level,
            )

            if not cloud_interface.test_connectivity():
                raise NetworkErrorExit()
            # If test is requested, just exit after connectivity test
            elif config.test:
                raise SystemExit(0)

            # TODO: Should the setup be optional?
            cloud_interface.setup_bucket()

            upload_kwargs = {}
            if is_history_file(config.wal_path):
                upload_kwargs["override_tags"] = config.history_tags

            uploader.upload_wal(config.wal_path, **upload_kwargs)

    except Exception as exc:
        logging.error("Barman cloud WAL archiver exception: %s", force_str(exc))
        logging.debug("Exception details:", exc_info=exc)
        raise GeneralErrorExit()


def parse_arguments(args=None):
    """
    Parse command line arguments

    :return: The options parsed
    """

    parser, s3_arguments, azure_arguments = create_argument_parser(
        description="This script can be used in the `archive_command` "
        "of a PostgreSQL server to ship WAL files to the Cloud. "
        "Currently AWS S3, Azure Blob Storage and Google Cloud Storage are supported.",
        source_or_destination=UrlArgumentType.destination,
    )
    parser.add_argument(
        "wal_path",
        nargs="?",
        help="the value of the '%%p' keyword (according to 'archive_command').",
        default=None,
    )
    compression = parser.add_mutually_exclusive_group()
    compression.add_argument(
        "-z",
        "--gzip",
        help="gzip-compress the WAL while uploading to the cloud",
        action="store_const",
        const="gzip",
        dest="compression",
    )
    compression.add_argument(
        "-j",
        "--bzip2",
        help="bzip2-compress the WAL while uploading to the cloud",
        action="store_const",
        const="bzip2",
        dest="compression",
    )
    compression.add_argument(
        "--xz",
        help="xz-compress the WAL while uploading to the cloud",
        action="store_const",
        const="xz",
        dest="compression",
    )
    compression.add_argument(
        "--snappy",
        help="snappy-compress the WAL while uploading to the cloud "
        "(requires optional python-snappy library)",
        action="store_const",
        const="snappy",
        dest="compression",
    )
    compression.add_argument(
        "--zstd",
        help="zstd-compress the WAL while uploading to the cloud "
        "(requires optional zstandard library)",
        action="store_const",
        const="zstd",
        dest="compression",
    )
    compression.add_argument(
        "--lz4",
        help="lz4-compress the WAL while uploading to the cloud "
        "(requires optional lz4 library)",
        action="store_const",
        const="lz4",
        dest="compression",
    )
    parser.add_argument(
        "--compression-level",
        help="A compression level for the specified compression algorithm",
        dest="compression_level",
        type=parse_compression_level,
        default=None,
    )
    add_tag_argument(
        parser,
        name="tags",
        help="Tags to be added to archived WAL files in cloud storage",
    )
    add_tag_argument(
        parser,
        name="history-tags",
        help="Tags to be added to archived history files in cloud storage",
    )
    gcs_arguments = parser.add_argument_group(
        "Extra options for google-cloud-storage cloud provider"
    )
    gcs_arguments.add_argument(
        "--kms-key-name",
        help="The name of the GCP KMS key which should be used for encrypting the "
        "uploaded data in GCS.",
    )
    s3_arguments.add_argument(
        "-e",
        "--encryption",
        help="The encryption algorithm used when storing the uploaded data in S3. "
        "Allowed values: 'AES256'|'aws:kms'.",
        choices=["AES256", "aws:kms"],
        metavar="ENCRYPTION",
    )
    s3_arguments.add_argument(
        "--sse-kms-key-id",
        help="The AWS KMS key ID that should be used for encrypting the uploaded data "
        "in S3. Can be specified using the key ID on its own or using the full ARN for "
        "the key. Only allowed if `-e/--encryption` is set to `aws:kms`.",
    )
    azure_arguments.add_argument(
        "--encryption-scope",
        help="The name of an encryption scope defined in the Azure Blob Storage "
        "service which is to be used to encrypt the data in Azure",
    )
    azure_arguments.add_argument(
        "--max-block-size",
        help="The chunk size to be used when uploading an object via the "
        "concurrent chunk method (default: 4MB).",
        type=check_size,
        default="4MB",
    )
    azure_arguments.add_argument(
        "--max-concurrency",
        help="The maximum number of chunks to be uploaded concurrently (default: 1).",
        type=check_positive,
        default=1,
    )
    azure_arguments.add_argument(
        "--max-single-put-size",
        help="Maximum size for which the Azure client will upload an object in a "
        "single request (default: 64MB). If this is set lower than the PostgreSQL "
        "WAL segment size after any applied compression then the concurrent chunk "
        "upload method for WAL archiving will be used.",
        default="64MB",
        type=check_size,
    )
    return parser.parse_args(args=args)


class CloudWalUploader(object):
    """
    Cloud storage upload client
    """

    def __init__(
        self,
        cloud_interface,
        server_name,
        compression=None,
        compression_level=None,
    ):
        """
        Object responsible for handling interactions with cloud storage

        :param CloudInterface cloud_interface: The interface to use to
          upload the backup
        :param str server_name: The name of the server as configured in Barman
        :param str|None compression: Compression algorithm to use
        :param str|int|None compression_level: Compression level for the specified
            algorithm
        """

        self.cloud_interface = cloud_interface
        self.compression = compression
        self.compression_level = compression_level
        self.server_name = server_name

    def upload_wal(self, wal_path, override_tags=None):
        """
        Upload a WAL file from postgres to cloud storage

        :param str wal_path: Full path of the WAL file
        :param List[tuple] override_tags: List of k,v tuples which should override any
          tags already defined in the cloud interface
        """
        # Extract the WAL file
        wal_name = self.retrieve_wal_name(wal_path)
        # Use the correct file object for the upload (simple|gzip|bz2)
        file_object = self.retrieve_file_obj(wal_path)
        # Correctly format the destination path
        destination = os.path.join(
            self.cloud_interface.path,
            self.server_name,
            "wals",
            hash_dir(wal_path),
            wal_name,
        )

        # Put the file in the correct bucket.
        # The put method will handle automatically multipart upload
        self.cloud_interface.upload_fileobj(
            fileobj=file_object, key=destination, override_tags=override_tags
        )

    def retrieve_file_obj(self, wal_path):
        """
        Create the correct type of file object necessary for the file transfer.

        If no compression is required a simple File object is returned.

        In case of compression, a BytesIO object is returned, containing the
        result of the compression.

        NOTE: the Wal files are actually compressed straight into memory,
        thanks to the usual small dimension of the WAL.
        This could change in the future because the WAL files dimension could
        be more than 16MB on some postgres install.

        TODO: Evaluate using tempfile if the WAL is bigger than 16MB

        :param str wal_path:
        :return File: simple or compressed file object
        """
        # Read the wal_file in binary mode
        wal_file = open(wal_path, "rb")
        # return the opened file if is uncompressed
        if not self.compression:
            return wal_file

        return compress(wal_file, self.compression, self.compression_level)

    def retrieve_wal_name(self, wal_path):
        """
        Extract the name of the WAL file from the complete path.

        If no compression is specified, then the simple file name is returned.

        In case of compression, the correct file extension is applied to the
        WAL file name.

        :param str wal_path: the WAL file complete path
        :return str: WAL file name
        """
        # Extract the WAL name
        wal_name = os.path.basename(wal_path)
        # return the plain file name if no compression is specified
        if not self.compression:
            return wal_name

        if self.compression == "gzip":
            # add gz extension
            return "%s.gz" % wal_name

        elif self.compression == "bzip2":
            # add bz2 extension
            return "%s.bz2" % wal_name

        elif self.compression == "xz":
            # add xz extension
            return "%s.xz" % wal_name

        elif self.compression == "snappy":
            # add snappy extension
            return "%s.snappy" % wal_name

        elif self.compression == "zstd":
            # add zst extension
            return "%s.zst" % wal_name

        elif self.compression == "lz4":
            # add lz4 extension
            return "%s.lz4" % wal_name
        else:
            raise ValueError("Unknown compression type: %s" % self.compression)


if __name__ == "__main__":
    main()
