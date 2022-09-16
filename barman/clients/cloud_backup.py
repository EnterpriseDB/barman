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

import datetime
from io import BytesIO
import logging
import os
import re
import tempfile
from contextlib import closing
from shutil import rmtree

from barman.backup_executor import ConcurrentBackupStrategy
from barman.clients.cloud_cli import (
    add_tag_argument,
    create_argument_parser,
    GeneralErrorExit,
    NetworkErrorExit,
    OperationErrorExit,
    UrlArgumentType,
)
from barman.cloud import (
    CloudBackupUploaderBarman,
    CloudBackupUploaderPostgres,
    configure_logging,
)
from barman.cloud_providers import get_cloud_interface, get_snapshot_interface
from barman.exceptions import (
    BarmanException,
    PostgresConnectionError,
    UnrecoverableHookScriptError,
)
from barman.infofile import BackupInfo
from barman.postgres import PostgreSQLConnection
from barman.utils import check_positive, check_size, force_str

_find_space = re.compile(r"[\s]").search


def __is_hook_script():
    """Check the environment and determine if we are running as a hook script"""
    if "BARMAN_HOOK" in os.environ and "BARMAN_PHASE" in os.environ:
        if (
            os.getenv("BARMAN_HOOK") in ("backup_script", "backup_retry_script")
            and os.getenv("BARMAN_PHASE") == "post"
        ):
            return True
        else:
            raise BarmanException(
                "barman-cloud-backup called as unsupported hook script: %s_%s"
                % (os.getenv("BARMAN_PHASE"), os.getenv("BARMAN_HOOK"))
            )
    else:
        return False


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

    # If -d specified a conninfo string, just return it
    if config.dbname is not None:
        if config.dbname == "" or "=" in config.dbname:
            return config.dbname

    if config.host:
        conn_parts.append("host=%s" % quote_conninfo(config.host))
    if config.port:
        conn_parts.append("port=%s" % quote_conninfo(config.port))
    if config.user:
        conn_parts.append("user=%s" % quote_conninfo(config.user))
    if config.dbname:
        conn_parts.append("dbname=%s" % quote_conninfo(config.dbname))

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

        cloud_interface = get_cloud_interface(config)

        if not cloud_interface.test_connectivity():
            raise NetworkErrorExit()
        # If test is requested, just exit after connectivity test
        elif config.test:
            raise SystemExit(0)

        with closing(cloud_interface):

            # TODO: Should the setup be optional?
            cloud_interface.setup_bucket()

            # Perform the backup
            uploader_kwargs = {
                "server_name": config.server_name,
                "compression": config.compression,
                "max_archive_size": config.max_archive_size,
                "cloud_interface": cloud_interface,
            }
            if __is_hook_script():
                if "BARMAN_BACKUP_DIR" not in os.environ:
                    raise BarmanException(
                        "BARMAN_BACKUP_DIR environment variable not set"
                    )
                if "BARMAN_BACKUP_ID" not in os.environ:
                    raise BarmanException(
                        "BARMAN_BACKUP_ID environment variable not set"
                    )
                if os.getenv("BARMAN_STATUS") != "DONE":
                    raise UnrecoverableHookScriptError(
                        "backup in '%s' has status '%s' (status should be: DONE)"
                        % (os.getenv("BARMAN_BACKUP_DIR"), os.getenv("BARMAN_STATUS"))
                    )
                uploader = CloudBackupUploaderBarman(
                    backup_dir=os.getenv("BARMAN_BACKUP_DIR"),
                    backup_id=os.getenv("BARMAN_BACKUP_ID"),
                    **uploader_kwargs
                )
                uploader.backup()
            else:
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
                    raise OperationErrorExit()

                with closing(postgres):
                    # Do snapshot things if asked for
                    if config.snapshot_project is not None:
                        snapshot_interface = get_snapshot_interface(config)
                        # TODO here we want to create a CloudBackupUploaderSnapshot which
                        # uses snapshot_interface instead of the copying via the cloud
                        # interface
                        server_name = "cloud"
                        backup_info = BackupInfo(
                            backup_id=datetime.datetime.now().strftime("%Y%m%dT%H%M%S"),
                            server_name=server_name,
                        )
                        backup_info.set_attribute("systemid", postgres.get_systemid())
                        strategy = ConcurrentBackupStrategy(postgres, server_name)
                        logging.info("Starting backup '%s'", backup_info.backup_id)
                        strategy.start_backup(backup_info)
                        snapshot_interface.take_snapshot(
                            backup_info,
                            config.snapshot_disk_zone,
                            config.snapshot_disk_name,
                        )
                        logging.info("Stopping backup '%s'", backup_info.backup_id)
                        strategy.stop_backup(backup_info)

                        # Create a restore point after a backup
                        target_name = "barman_%s" % backup_info.backup_id
                        postgres.create_restore_point(target_name)
                        postgres.close()

                        # Set the backup status as DONE
                        backup_info.set_attribute("status", BackupInfo.DONE)

                        # TODO Now upload backup info and backup label
                        # Wheeeeee such duplication
                        if backup_info.backup_label:
                            backup_label_key = os.path.join(
                                cloud_interface.path,
                                config.server_name,
                                "base",
                                backup_info.backup_id,
                                "backup_label",
                            )
                            cloud_interface.upload_fileobj(
                                BytesIO(backup_info.backup_label.encode("UTF-8")),
                                backup_label_key,
                            )
                        with BytesIO() as backup_info_file:
                            backup_info_key = os.path.join(
                                cloud_interface.path,
                                config.server_name,
                                "base",
                                backup_info.backup_id,
                                "backup.info",
                            )
                            backup_info.save(file_object=backup_info_file)
                            backup_info_file.seek(0, os.SEEK_SET)
                            logging.info("Uploading '%s'", backup_info_key)
                            cloud_interface.upload_fileobj(
                                backup_info_file, backup_info_key
                            )
                    # Otherwise upload everything to the object store
                    else:
                        uploader = CloudBackupUploaderPostgres(
                            postgres=postgres, **uploader_kwargs
                        )
                        uploader.backup()

    except KeyboardInterrupt as exc:
        logging.error("Barman cloud backup was interrupted by the user")
        logging.debug("Exception details:", exc_info=exc)
        raise OperationErrorExit()
    except UnrecoverableHookScriptError as exc:
        logging.error("Barman cloud backup exception: %s", force_str(exc))
        logging.debug("Exception details:", exc_info=exc)
        raise SystemExit(63)
    except Exception as exc:
        logging.error("Barman cloud backup exception: %s", force_str(exc))
        logging.debug("Exception details:", exc_info=exc)
        raise GeneralErrorExit()
    finally:
        # Remove the temporary directory and all the contained files
        rmtree(tempdir, ignore_errors=True)


def parse_arguments(args=None):
    """
    Parse command line arguments

    :return: The options parsed
    """

    parser, s3_arguments, azure_arguments = create_argument_parser(
        description="This script can be used to perform a backup "
        "of a local PostgreSQL instance and ship "
        "the resulting tarball(s) to the Cloud. "
        "Currently AWS S3, Azure Blob Storage and Google Cloud Storage are supported.",
        source_or_destination=UrlArgumentType.destination,
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
    compression.add_argument(
        "--snappy",
        help="snappy-compress the WAL while uploading to the cloud ",
        action="store_const",
        const="snappy",
        dest="compression",
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
        help="number of subprocesses to upload data to cloud storage (default: 2)",
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
        "-d",
        "--dbname",
        help="Database name or conninfo string for Postgres connection (default: postgres)",
        default="postgres",
    )
    parser.add_argument(
        "--snapshot-project",
        help="Project under which disk snapshots should be stored",
    )
    parser.add_argument(
        "--snapshot-disk-zone",
        help="Zone of the disk from which snapshots should be taken",
    )
    parser.add_argument(
        "--snapshot-disk-name",
        help="Name of the disk from which snapshots should be taken",
    )
    add_tag_argument(
        parser,
        name="tags",
        help="Tags to be added to all uploaded files in cloud storage",
    )
    s3_arguments.add_argument(
        "-e",
        "--encryption",
        help="The encryption algorithm used when storing the uploaded data in S3. "
        "Allowed values: 'AES256'|'aws:kms'.",
        choices=["AES256", "aws:kms"],
    )
    azure_arguments.add_argument(
        "--encryption-scope",
        help="The name of an encryption scope defined in the Azure Blob Storage "
        "service which is to be used to encrypt the data in Azure",
    )
    return parser.parse_args(args=args)


if __name__ == "__main__":
    main()
