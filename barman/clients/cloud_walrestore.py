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
import io
import sys
import shutil
from contextlib import closing

from barman.clients.cloud_cli import (
    CLIErrorExit,
    GeneralErrorExit,
    OperationErrorExit,
    create_argument_parser,
)
from barman.clients.cloud_compression import decompress_to_file
from barman.clients.cloud_encryption import EncryptionConfiguration
from barman.cloud import ALLOWED_COMPRESSIONS, configure_logging, DecryptingReadableStreamIO
from barman.cloud_providers import get_cloud_interface
from barman.exceptions import BarmanException
from barman.utils import force_str
from barman.xlog import hash_dir, is_any_xlog_file, is_backup_file, is_partial_file

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

    try:
        cloud_interface = get_cloud_interface(config)

        # get the client-encryption config
        encryption_config = EncryptionConfiguration(filename=config.client_encryption)

        with closing(cloud_interface):
            # Do connectivity test if requested
            if config.test:
                cloud_interface.verify_cloud_connectivity_and_bucket_existence()
                raise SystemExit(0)

            downloader = CloudWalDownloader(
                cloud_interface=cloud_interface, 
                server_name=config.server_name,
                encryption_config=encryption_config
            )

            downloader.download_wal(config.wal_name, config.wal_dest, config.no_partial)

    except Exception as exc:
        _logger.error("Barman cloud WAL restore exception: %s", force_str(exc))
        _logger.debug("Exception details:", exc_info=exc)
        raise GeneralErrorExit()


def parse_arguments(args=None):
    """
    Parse command line arguments

    :return: The options parsed
    """

    parser, _, _ = create_argument_parser(
        description="This script can be used as a `restore_command` "
        "to download WAL files previously archived with "
        "barman-cloud-wal-archive command. "
        "Currently AWS S3, Azure Blob Storage and Google Cloud Storage are supported.",
    )

    parser.add_argument(
        "--no-partial",
        help="Do not download partial WAL files",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--client-encryption",
        help="path to the client-encryption config file"
        "(default: /etc/barman/client-encryption.json)",
        default='/etc/barman/client-encryption.json',
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


class CloudWalDownloader(object):
    """
    Cloud storage download client
    """

    def __init__(self, cloud_interface, server_name, encryption_config):
        """
        Object responsible for handling interactions with cloud storage

        :param CloudInterface cloud_interface: The interface to use to
          upload the backup
        :param str server_name: The name of the server as configured in Barman
        :param EncryptionConfiguration encryption_config: encryption config
        """

        self.cloud_interface = cloud_interface
        self.server_name = server_name
        self.encryption_config = encryption_config

    def download_wal(self, wal_name, wal_dest, no_partial):
        """
        Download a WAL file from cloud storage

        :param str wal_name: Name of the WAL file
        :param str wal_dest: Full path of the destination WAL file
        :param bool no_partial: Do not download partial WAL files
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
        # Automatically detect compression and encryption based on the file extension
        compression = None
        is_encrypted = False
        for item in self.cloud_interface.list_bucket(wal_path):
            # perfect match (uncompressed file)
            if item == wal_path:
                remote_name = item
                continue
            # look for encrypted, compressed files or .partial files
            basename = item

            # Detect encryption
            if basename.endswith('.enc'):
                is_encrypted = True
                basename = basename[:-len('.enc')]

            # Detect compression
            for e, c in ALLOWED_COMPRESSIONS.items():
                if basename[-len(e) :] == e:
                    # Strip extension
                    basename = basename[: -len(e)]
                    compression = c
                    break

            # Check basename is a known xlog file (.partial?)
            if not is_any_xlog_file(basename):
                _logger.warning("Unknown WAL file: %s", item)
                continue
            # Exclude backup informative files (not needed in recovery)
            elif is_backup_file(basename):
                _logger.info("Skipping backup file: %s", item)
                continue
            # Exclude partial files if required
            elif no_partial and is_partial_file(basename):
                _logger.info("Skipping partial file: %s", item)
                continue

            # Found candidate
            remote_name = item
            _logger.info(
                "Found WAL %s for server %s as %s",
                wal_name,
                self.server_name,
                remote_name,
            )
            break

        if not remote_name:
            _logger.info(
                "WAL file %s for server %s does not exists", wal_name, self.server_name
            )
            raise OperationErrorExit()

        if compression and sys.version_info < (3, 0, 0):
            raise BarmanException(
                "Compressed WALs cannot be restored with Python 2.x - "
                "please upgrade to a supported version of Python 3"
            )
        
        # Download the file
        _logger.debug(
            "Downloading %s to %s (%s/%s)",
            remote_name,
            wal_dest,
            "decompressing " + compression if compression else "no compression",
            "decrypting" if is_encrypted else "no encryption"
        )
        # no idea why decompression got pushed to the individual cloud implementations
        #self.cloud_interface.download_file(remote_name, wal_dest, compression)

        # the aim was to use DecryptingReadableStreamIO and decompress_to_file in tandem to
        # stream the data. However, the decompressor first does a read(2) to determine the magic
        # this breaks decryption. TODO -- this must be fixed
        # since wal files are rather small in size, decrypt to memory ( not clean ... 😞 )
        with self.cloud_interface.remote_open(remote_name) as rf, open(wal_dest, 'wb') as lf:
            if is_encrypted:
                unencryptedFileObj = io.BytesIO(
                    DecryptingReadableStreamIO(rf, self.encryption_config).read())
            else:
                unencryptedFileObj = rf

            decompress_to_file(unencryptedFileObj, lf, compression=compression)

if __name__ == "__main__":
    main()
