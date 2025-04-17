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
import re
import tempfile
from contextlib import closing
from shutil import rmtree

from barman.clients.cloud_cli import (
    GeneralErrorExit,
    NetworkErrorExit,
    OperationErrorExit,
    UrlArgumentType,
    add_tag_argument,
    create_argument_parser,
)
from barman.cloud import (
    CloudBackupSnapshot,
    CloudBackupUploader,
    CloudBackupUploaderBarman,
    configure_logging,
)
from barman.cloud_providers import get_cloud_interface, get_snapshot_interface
from barman.exceptions import (
    BarmanException,
    ConfigurationException,
    PostgresConnectionError,
    UnrecoverableHookScriptError,
)
from barman.postgres import PostgreSQLConnection
from barman.utils import (
    check_aws_expiration_date_format,
    check_aws_snapshot_lock_cool_off_period_range,
    check_aws_snapshot_lock_duration_range,
    check_backup_name,
    check_positive,
    check_size,
    force_str,
)

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


def _validate_config(config):
    """
    Additional validation for config such as mutually inclusive options.

    Raises a ConfigurationException if any options are missing or incompatible.

    :param argparse.Namespace config: The backup options provided at the command line.
    """
    required_snapshot_variables = (
        "snapshot_disks",
        "snapshot_instance",
    )
    is_snapshot_backup = any(
        [getattr(config, var) for var in required_snapshot_variables]
    )
    if is_snapshot_backup:
        if getattr(config, "compression"):
            raise ConfigurationException(
                "Compression options cannot be used with snapshot backups"
            )
    if getattr(config, "aws_snapshot_lock_mode", None) == "governance" and getattr(
        config, "aws_snapshot_lock_cool_off_period", None
    ):
        raise ConfigurationException(
            "'aws_snapshot_lock_mode' = 'governance' cannot be used with "
            "'aws_snapshot_lock_cool_off_period'"
        )


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
        _validate_config(config)
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
                "min_chunk_size": config.min_chunk_size,
                "max_bandwidth": config.max_bandwidth,
                "cloud_interface": cloud_interface,
            }
            if __is_hook_script():
                if config.backup_name:
                    raise BarmanException(
                        "Cannot set backup name when running as a hook script"
                    )
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
                    backup_info_path=os.getenv("BARMAN_BACKUP_INFO_PATH"),
                    **uploader_kwargs,
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
                    # Take snapshot backups if snapshot backups were specified
                    if config.snapshot_disks or config.snapshot_instance:
                        snapshot_interface = get_snapshot_interface(config)
                        snapshot_interface.validate_backup_config(config)
                        snapshot_backup = CloudBackupSnapshot(
                            config.server_name,
                            cloud_interface,
                            snapshot_interface,
                            postgres,
                            config.snapshot_instance,
                            config.snapshot_disks,
                            config.backup_name,
                        )
                        snapshot_backup.backup()
                    # Otherwise upload everything to the object store
                    else:
                        uploader = CloudBackupUploader(
                            postgres=postgres,
                            backup_name=config.backup_name,
                            **uploader_kwargs,
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
        help="gzip-compress the backup while uploading to the cloud",
        action="store_const",
        const="gz",
        dest="compression",
    )
    compression.add_argument(
        "-j",
        "--bzip2",
        help="bzip2-compress the backup while uploading to the cloud",
        action="store_const",
        const="bz2",
        dest="compression",
    )
    compression.add_argument(
        "--snappy",
        help="snappy-compress the backup while uploading to the cloud ",
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
        "--min-chunk-size",
        type=check_size,
        help="minimum size of an individual chunk when uploading to cloud storage "
        "(default: 5MB for aws-s3, 64KB for azure-blob-storage, not applicable for "
        "google-cloud-storage)",
        default=None,  # Defer to the cloud interface if nothing is specified
    )
    parser.add_argument(
        "--max-bandwidth",
        type=check_size,
        help="the maximum amount of data to be uploaded per second when backing up to "
        "either AWS S3 or Azure Blob Storage (default: no limit)",
        default=None,
    )
    parser.add_argument(
        "-d",
        "--dbname",
        help="Database name or conninfo string for Postgres connection (default: postgres)",
        default="postgres",
    )
    parser.add_argument(
        "-n",
        "--name",
        help="a name which can be used to reference this backup in commands "
        "such as barman-cloud-restore and barman-cloud-backup-delete",
        default=None,
        type=check_backup_name,
        dest="backup_name",
    )
    parser.add_argument(
        "--snapshot-instance",
        help="Instance where the disks to be backed up as snapshots are attached",
    )
    parser.add_argument(
        "--snapshot-disk",
        help="Name of a disk from which snapshots should be taken",
        metavar="NAME",
        action="append",
        default=[],
        dest="snapshot_disks",
    )
    parser.add_argument(
        "--snapshot-zone",
        help=(
            "Zone of the disks from which snapshots should be taken (deprecated: "
            "replaced by --gcp-zone)"
        ),
        dest="gcp_zone",
    )
    gcs_arguments = parser.add_argument_group(
        "Extra options for google-cloud-storage cloud provider"
    )
    gcs_arguments.add_argument(
        "--snapshot-gcp-project",
        help=(
            "GCP project under which disk snapshots should be stored (deprecated: "
            "replaced by --gcp-project)"
        ),
        dest="gcp_project",
    )
    gcs_arguments.add_argument(
        "--gcp-project",
        help="GCP project under which disk snapshots should be stored",
    )
    gcs_arguments.add_argument(
        "--kms-key-name",
        help="The name of the GCP KMS key which should be used for encrypting the "
        "uploaded data in GCS.",
    )
    gcs_arguments.add_argument(
        "--gcp-zone",
        help="Zone of the disks from which snapshots should be taken",
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
    s3_arguments.add_argument(
        "--sse-kms-key-id",
        help="The AWS KMS key ID that should be used for encrypting the uploaded data "
        "in S3. Can be specified using the key ID on its own or using the full ARN for "
        "the key. Only allowed if `-e/--encryption` is set to `aws:kms`.",
    )
    s3_arguments.add_argument(
        "--aws-region",
        help="The name of the AWS region containing the EC2 VM and storage volumes "
        "defined by the --snapshot-instance and --snapshot-disk arguments.",
    )
    s3_arguments.add_argument(
        "--aws-await-snapshots-timeout",
        default=3600,
        help="The length of time in seconds to wait for snapshots to be created in AWS before "
        "timing out (default: 3600 seconds)",
        type=check_positive,
    )
    s3_arguments.add_argument(
        "--aws-snapshot-lock-mode",
        help="The lock mode to apply to the snapshot. Allowed values: "
        "'governance'|'compliance'.",
        choices=["governance", "compliance"],
    )
    s3_arguments.add_argument(
        "--aws-snapshot-lock-cool-off-period",
        help="Specifies the cool-off period (in hours) for a snapshot locked in "
        "'compliance' mode, allowing you to unlock or modify lock settings after it is "
        "locked. Range must be from 1 to 72. To lock the snapshot immediately without "
        "a cool-off period, leave this option unset.",
        type=check_aws_snapshot_lock_cool_off_period_range,
    )
    s3_lock_target_group = s3_arguments.add_mutually_exclusive_group()
    s3_lock_target_group.add_argument(
        "--aws-snapshot-lock-expiration-date",
        help="The expiration date for a locked snapshot in the format "
        "YYYY-MM-DDThh:mm:ss.sssZ. To lock a snapshot, you must specify either this "
        "argument or --aws-snapshot-lock-duration, but not both.",
        type=check_aws_expiration_date_format,
    )
    s3_lock_target_group.add_argument(
        "--aws-snapshot-lock-duration",
        help="The duration (in days) for which the snapshot should be locked. Range "
        "must be from 1 to 36500. To lock a snapshopt, you must specify either this "
        "argument or --aws-snapshot-lock-expiration-date, but not both.",
        type=check_aws_snapshot_lock_duration_range,
    )
    azure_arguments.add_argument(
        "--encryption-scope",
        help="The name of an encryption scope defined in the Azure Blob Storage "
        "service which is to be used to encrypt the data in Azure",
    )
    azure_arguments.add_argument(
        "--azure-subscription-id",
        help="The ID of the Azure subscription which owns the instance and storage "
        "volumes defined by the --snapshot-instance and --snapshot-disk arguments.",
    )
    azure_arguments.add_argument(
        "--azure-resource-group",
        help="The name of the Azure resource group to which the compute instance and "
        "disks defined by the --snapshot-instance and --snapshot-disk arguments belong.",
    )

    parsed_args = parser.parse_args(args=args)
    return parsed_args


if __name__ == "__main__":
    main()
