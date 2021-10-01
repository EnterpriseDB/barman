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
import os
from contextlib import closing
from operator import attrgetter

import barman
from barman.backup import BackupManager
from barman.cloud import CloudBackupCatalog, configure_logging
from barman.cloud_providers import get_cloud_interface
from barman.retention_policies import RetentionPolicyFactory
from barman.utils import force_str
from barman import xlog

try:
    import argparse
except ImportError:
    raise SystemExit("Missing required python module: argparse")


def _get_files_for_backup(catalog, backup_info):
    backup_files = []
    # Sort the files by OID so that we always get a stable order. The PGDATA dir
    # has no OID so we use a -1 for sorting purposes, such that it always sorts
    # ahead of the tablespaces.
    for oid, backup_file in sorted(
        catalog.get_backup_files(backup_info, allow_missing=True).items(),
        key=lambda x: x[0] if x[0] else -1,
    ):
        key = oid or "PGDATA"
        for file_info in [backup_file] + sorted(
            backup_file.additional_files, key=attrgetter("path")
        ):
            # Silently skip files which could not be found - if they don't exist
            # then not being able to delete them is not an error condition here
            if file_info.path is not None:
                logging.debug(
                    "Will delete archive for %s at %s" % (key, file_info.path)
                )
                backup_files.append(file_info.path)

    return backup_files


def _remove_wals_for_backup(
    cloud_interface,
    catalog,
    deleted_backup,
    dry_run,
    skip_wal_cleanup_if_standalone=True,
):
    # An implementation of BackupManager.remove_wal_before_backup which does not
    # use xlogdb, since xlogdb is not available to barman-cloud
    should_remove_wals, wal_ranges_to_protect = BackupManager.should_remove_wals(
        deleted_backup,
        catalog.get_backup_list(),
        keep_manager=catalog,
        skip_wal_cleanup_if_standalone=skip_wal_cleanup_if_standalone,
    )
    next_backup = BackupManager.find_next_backup_in(
        catalog.get_backup_list(), deleted_backup.backup_id
    )
    wals_to_delete = {}
    if should_remove_wals:
        # There is no previous backup or all previous backups are archival
        # standalone backups, so we can remove unused WALs (those WALs not
        # required by standalone archival backups).
        # If there is a next backup then all unused WALs up to the begin_wal
        # of the next backup can be removed.
        # If there is no next backup then there are no remaining backups,
        # because we must assume non-exclusive backups are taken, we can only
        # safely delete unused WALs up to begin_wal of the deleted backup.
        # See comments in barman.backup.BackupManager.delete_backup.
        if next_backup:
            remove_until = next_backup
        else:
            remove_until = deleted_backup
        # A WAL is only a candidate for deletion if it is on the same timeline so we
        # use BackupManager to get a set of all other timelines with backups so that
        # we can preserve all WALs on other timelines.
        timelines_to_protect = BackupManager.get_timelines_to_protect(
            remove_until=remove_until,
            deleted_backup=deleted_backup,
            available_backups=catalog.get_backup_list(),
        )
        try:
            wal_paths = catalog.get_wal_paths()
        except Exception as exc:
            logging.error(
                "Cannot clean up WALs for backup %s because an error occurred listing WALs: %s",
                deleted_backup.backup_id,
                force_str(exc),
            )
            return
        for wal_name, wal in wal_paths.items():
            if xlog.is_history_file(wal_name):
                continue
            if timelines_to_protect:
                tli, _, _ = xlog.decode_segment_name(wal_name)
                if tli in timelines_to_protect:
                    continue

            # Check if the WAL is in a protected range, required by an archival
            # standalone backup - so do not delete it
            if xlog.is_backup_file(wal_name):
                # If we have a backup file, truncate the name for the range check
                range_check_wal_name = wal_name[:24]
            else:
                range_check_wal_name = wal_name
            if any(
                range_check_wal_name >= begin_wal and range_check_wal_name <= end_wal
                for begin_wal, end_wal in wal_ranges_to_protect
            ):
                continue

            if wal_name < remove_until.begin_wal:
                wals_to_delete[wal_name] = wal
    # Explicitly sort because dicts are not ordered in python < 3.6
    wal_paths_to_delete = sorted(wals_to_delete.values())
    if len(wal_paths_to_delete) > 0:
        if not dry_run:
            try:
                cloud_interface.delete_objects(wal_paths_to_delete)
            except Exception as exc:
                logging.error(
                    "Could not delete the following WALs for backup %s: %s, Reason: %s",
                    deleted_backup.backup_id,
                    wal_paths_to_delete,
                    force_str(exc),
                )
                # Return early so that we leave the WALs in the local cache so they
                # can be cleaned up should there be a subsequent backup deletion.
                return
        else:
            print(
                "Skipping deletion of objects %s due to --dry-run option"
                % wal_paths_to_delete
            )
        for wal_name in wals_to_delete.keys():
            catalog.remove_wal_from_cache(wal_name)


def _delete_backup(
    cloud_interface,
    catalog,
    backup_id,
    dry_run=True,
    skip_wal_cleanup_if_standalone=True,
):
    backup_info = catalog.get_backup_info(backup_id)
    if not backup_info:
        logging.warning("Backup %s does not exist", backup_id)
        return
    objects_to_delete = _get_files_for_backup(catalog, backup_info)
    backup_info_path = os.path.join(
        catalog.prefix, backup_info.backup_id, "backup.info"
    )
    logging.debug("Will delete backup.info file at %s" % backup_info_path)
    if not dry_run:
        try:
            cloud_interface.delete_objects(objects_to_delete)
            # Do not try to delete backup.info until we have successfully deleted
            # everything else so that it is possible to retry the operation should
            # we fail to delete any backup file
            cloud_interface.delete_objects([backup_info_path])
        except Exception as exc:
            logging.error("Could not delete backup %s: %s", backup_id, force_str(exc))
            raise SystemExit(2)
    else:
        print(
            "Skipping deletion of objects %s due to --dry-run option"
            % (objects_to_delete + [backup_info_path])
        )

    _remove_wals_for_backup(
        cloud_interface, catalog, backup_info, dry_run, skip_wal_cleanup_if_standalone
    )
    # It is important that the backup is removed from the catalog after cleaning
    # up the WALs because the code in _remove_wals_for_backup depends on the
    # deleted backup existing in the backup catalog
    catalog.remove_backup_from_cache(backup_id)


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
                raise SystemExit(1)
            # If test is requested, just exit after connectivity test
            elif config.test:
                raise SystemExit(0)

            if not cloud_interface.bucket_exists:
                logging.error("Bucket %s does not exist", cloud_interface.bucket_name)
                raise SystemExit(1)

            catalog = CloudBackupCatalog(
                cloud_interface=cloud_interface, server_name=config.server_name
            )
            # Call catalog.get_backup_list now so we know we can read the whole catalog
            # (the results are cached so this does not result in extra calls to cloud
            # storage)
            catalog.get_backup_list()
            if len(catalog.unreadable_backups) > 0:
                logging.error(
                    "Cannot read the following backups: %s\n"
                    "Unsafe to proceed with deletion due to failure reading backup catalog"
                    % catalog.unreadable_backups
                )
                raise SystemExit(1)

            if config.backup_id:
                # Because we only care about one backup, skip the annotation cache
                # because it is only helpful when dealing with multiple backups
                if catalog.should_keep_backup(config.backup_id, use_cache=False):
                    logging.error(
                        "Skipping delete of backup %s for server %s "
                        "as it has a current keep request. If you really "
                        "want to delete this backup please remove the keep "
                        "and try again.",
                        config.backup_id,
                        config.server_name,
                    )
                    raise SystemExit(1)
                _delete_backup(
                    cloud_interface, catalog, config.backup_id, config.dry_run
                )
            elif config.retention_policy:
                retention_policy = RetentionPolicyFactory.create(
                    "retention_policy",
                    config.retention_policy,
                    server_name=config.server_name,
                    catalog=catalog,
                )
                # Sort to ensure that we delete the backups in ascending order, that is
                # from oldest to newest. This ensures that the relevant WALs will be cleaned
                # up after each backup is deleted.
                backups_to_delete = sorted(
                    [
                        backup_id
                        for backup_id, status in retention_policy.report().items()
                        if status == "OBSOLETE"
                    ]
                )
                for backup_id in backups_to_delete:
                    _delete_backup(
                        cloud_interface,
                        catalog,
                        backup_id,
                        config.dry_run,
                        skip_wal_cleanup_if_standalone=False,
                    )
    except Exception as exc:
        logging.error("Barman cloud backup delete exception: %s", force_str(exc))
        logging.debug("Exception details:", exc_info=exc)
        raise SystemExit(1)


def parse_arguments(args=None):
    """
    Parse command line arguments

    :return: The options parsed
    """
    parser = argparse.ArgumentParser(
        description="This script can be used to delete backups "
        "made with barman-cloud-backup command. "
        "Currently AWS S3 and Azure Blob Storage are supported.",
        add_help=False,
    )
    parser.add_argument(
        "source_url",
        help="URL of the cloud source, such as a bucket in AWS S3."
        " For example: `s3://bucket/path/to/folder`.",
    )
    parser.add_argument(
        "server_name", help="the name of the server as configured in Barman."
    )
    delete_arguments = parser.add_mutually_exclusive_group(required=True)
    delete_arguments.add_argument(
        "-b",
        "--backup-id",
        help="Backup ID of the backup to be deleted",
    )
    delete_arguments.add_argument(
        "-r",
        "--retention-policy",
        help="If specified, delete all backups eligible for deletion according to the "
        "supplied retention policy. Syntax: REDUNDANCY value | RECOVERY WINDOW OF "
        "value {DAYS | WEEKS | MONTHS}",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Find the objects which need to be deleted but do not delete them",
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
    parser.add_argument(
        "-t",
        "--test",
        help="Test cloud connectivity and exit",
        action="store_true",
        default=False,
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
    return parser.parse_args(args=args)


if __name__ == "__main__":
    main()
