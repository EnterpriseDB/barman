# Copyright (C) 2018-2019 2ndQuadrant Limited
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

import bz2
import gzip
import logging
import os
import os.path
import shutil
from contextlib import closing
from io import BytesIO

import barman
from barman.cloud import CloudInterface
from barman.utils import force_str
from barman.xlog import hash_dir, is_any_xlog_file

try:
    import argparse
except ImportError:
    raise SystemExit("Missing required python module: argparse")

LOGGING_FORMAT = "%(asctime)s %(levelname)s %(message)s"


def main(args=None):
    """
    The main script entry point

    :param list[str] args: the raw arguments list. When not provided
        it defaults to sys.args[1:]
    """
    config = parse_arguments(args)
    configure_logging()

    # Validate the WAL file name before uploading it
    if not is_any_xlog_file(config.wal_path):
        logging.error('%s is an invalid name for a WAL file' % config.wal_path)
        raise SystemExit(1)

    try:
        cloud_interface = CloudInterface(
            destination_url=config.destination_url,
            encryption=config.encryption,
            profile_name=config.profile)

        with closing(cloud_interface):
            uploader = S3WalUploader(
                cloud_interface=cloud_interface,
                server_name=config.server_name,
                compression=config.compression)

            if not cloud_interface.test_connectivity():
                raise SystemExit(1)
            # If test is requested, just exit after connectivity test
            elif config.test:
                raise SystemExit(0)

            # TODO: Should the setup be optional?
            cloud_interface.setup_bucket()

            uploader.upload_wal(config.wal_path)
    except Exception as exc:
        logging.error("Barman cloud WAL archiver exception: %s",
                      force_str(exc))
        logging.debug('Exception details:', exc_info=exc)
        raise SystemExit(1)


def parse_arguments(args=None):
    """
    Parse command line arguments

    :return: The options parsed
    """

    parser = argparse.ArgumentParser(
        description='This script can be used in the `archive_command` '
                    'of a PostgreSQL server to ship WAL files to the Cloud. '
                    'Currently only AWS S3 is supported.'
    )
    parser.add_argument(
        'destination_url',
        help='URL of the cloud destination, such as a bucket in AWS S3.'
             ' For example: `s3://bucket/path/to/folder`.'
    )
    parser.add_argument(
        'server_name',
        help='the name of the server as configured in Barman.'
    )
    parser.add_argument(
        'wal_path',
        help="the value of the '%%p' keyword"
             " (according to 'archive_command')."
    )
    parser.add_argument(
        '-V', '--version',
        action='version', version='%%(prog)s %s' % barman.__version__
    )
    parser.add_argument(
        '-P', '--profile',
        help='profile name (e.g. INI section in AWS credentials file)',
    )
    compression = parser.add_mutually_exclusive_group()
    compression.add_argument(
        "-z", "--gzip",
        help="gzip-compress the WAL while uploading to the cloud",
        action='store_const',
        const='gzip',
        dest='compression',
    )
    compression.add_argument(
        "-j", "--bzip2",
        help="bzip2-compress the WAL while uploading to the cloud",
        action='store_const',
        const='bzip2',
        dest='compression',
    )
    parser.add_argument(
        "-e", "--encryption",
        help="Enable server-side encryption for the transfer. "
             "Allowed values: 'AES256', 'aws:kms'",
        choices=['AES256', 'aws:kms'],
        metavar="ENCRYPTION",
    )
    parser.add_argument(
        "-t", "--test",
        help="Test cloud connectivity and exit",
        action="store_true",
        default=False
    )
    return parser.parse_args(args=args)


def configure_logging():
    """
    Get a nicer output from the Python logging package
    """
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.ERROR)


class S3WalUploader(object):
    """
    S3 upload client
    """
    def __init__(self, cloud_interface,
                 server_name, compression=None):
        """
        Object responsible for handling interactions with S3

        :param CloudInterface cloud_interface: The interface to use to
          upload the backup
        :param str server_name: The name of the server as configured in Barman
        :param str compression: Compression algorithm to use
        """

        self.cloud_interface = cloud_interface
        # If netloc is not present, the s3 url is badly formatted.
        self.compression = compression
        self.server_name = server_name

    def upload_wal(self, wal_path):
        """
        Upload a WAL file from postgres to S3

        :param str wal_path: Full path of the WAL file
        """
        # Extract the WAL file
        wal_name = self.retrieve_wal_name(wal_path)
        # Use the correct file object for the upload (simple|gzip|bz2)
        file_object = self.retrieve_file_obj(wal_path)
        # Correctly format the destination path on s3
        destination = os.path.join(
            self.cloud_interface.path,
            self.server_name,
            'wals',
            hash_dir(wal_path),
            wal_name
        )

        # Remove initial "/", otherwise we will create a folder with an empty
        # name.
        if destination[0] == '/':
            destination = destination[1:]

        # Put the file in the correct bucket.
        # The put method will handle automatically multipart upload
        self.cloud_interface.upload_fileobj(
            fileobj=file_object,
            key=destination)

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
        wal_file = open(wal_path, 'rb')
        # return the opened file if is uncompressed
        if not self.compression:
            return wal_file

        if self.compression == 'gzip':
            # Create a BytesIO for in memory compression
            in_mem_gzip = BytesIO()
            # TODO: closing is redundant with python >= 2.7
            with closing(gzip.GzipFile(fileobj=in_mem_gzip, mode='wb')) as gz:
                # copy the gzipped data in memory
                shutil.copyfileobj(wal_file, gz)
            in_mem_gzip.seek(0)
            return in_mem_gzip

        elif self.compression == 'bzip2':
            # Create a BytesIO for in memory compression
            in_mem_bz2 = BytesIO(bz2.compress(wal_file.read()))
            in_mem_bz2.seek(0)
            return in_mem_bz2
        else:
            raise ValueError("Unknown compression type: %s" % self.compression)

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

        if self.compression == 'gzip':
            # add gz extension
            return "%s.gz" % wal_name

        elif self.compression == 'bzip2':
            # add bz2 extension
            return "%s.bz2" % wal_name
        else:
            raise ValueError("Unknown compression type: %s" % self.compression)


if __name__ == '__main__':
    main()
