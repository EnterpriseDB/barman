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
from contextlib import closing
from operator import attrgetter

from barman import xlog
from barman.backup import BackupManager
from barman.clients.cloud_cli import (
    CLIErrorExit,
    GeneralErrorExit,
    NetworkErrorExit,
    OperationErrorExit,
    create_argument_parser,
)
from barman.cloud import CloudBackupCatalog, configure_logging
from barman.cloud_providers import (
    get_cloud_interface,
    get_snapshot_interface_from_backup_info,
)
from barman.exceptions import BadXlogPrefix, InvalidRetentionPolicy
from barman.retention_policies import RetentionPolicyFactory
from barman.utils import check_non_negative, force_str


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
        # Identify any prefixes under which all WALs are no longer needed.
        # This is a shortcut which allows us to delete all WALs under a prefix without
        # checking each individual WAL.
        try:
            wal_prefixes = catalog.get_wal_prefixes()
        except NotImplementedError:
            # If fetching WAL prefixes isn't supported by the cloud provider then
            # the old method of checking each WAL must be used for all WALs.
            wal_prefixes = []
        deletable_prefixes = []
        for wal_prefix in wal_prefixes:
            try:
                tli_and_log = wal_prefix.split("/")[-2]
                tli, log = xlog.decode_hash_dir(tli_and_log)
            except (BadXlogPrefix, IndexError):
                # If the prefix does not appear to be a tli and log we output a warning
                # and move on to the next prefix rather than error out.
                logging.warning(
                    "Ignoring malformed WAL object prefix: {}".format(wal_prefix)
                )
                continue
            # If this prefix contains a timeline which should be protected then we
            # cannot delete the WALS under it so advance to the next prefix.
            if tli in timelines_to_protect:
                continue

            # If the tli and log fall are inclusively between the tli and log for the
            # begin and end WAL of any protected WAL range then this prefix cannot be
            # deleted outright.
            for begin_wal, end_wal in wal_ranges_to_protect:
                begin_tli, begin_log, _ = xlog.decode_segment_name(begin_wal)
                end_tli, end_log, _ = xlog.decode_segment_name(end_wal)
                if (
                    tli >= begin_tli
                    and log >= begin_log
                    and tli <= end_tli
                    and log <= end_log
                ):
                    break
            else:
                # The prefix tli and log do not match any protected timelines or
                # protected WAL ranges so all WALs are eligible for deletion if the tli
                # is the same timeline and the log is below the begin_wal log of the
                # backup being deleted.
                until_begin_tli, until_begin_log, _ = xlog.decode_segment_name(
                    remove_until.begin_wal
                )
                if tli == until_begin_tli and log < until_begin_log:
                    # All WALs under this prefix pre-date the backup being deleted so they
                    # can be deleted in one request.
                    deletable_prefixes.append(wal_prefix)
        for wal_prefix in deletable_prefixes:
            if not dry_run:
                cloud_interface.delete_under_prefix(wal_prefix)
            else:
                print(
                    "Skipping deletion of all objects under prefix %s "
                    "due to --dry-run option" % wal_prefix
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
            # If the wal starts with a prefix we deleted then ignore it so that the
            # dry-run output is accurate
            if any(wal.startswith(prefix) for prefix in deletable_prefixes):
                continue
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
    config,
    skip_wal_cleanup_if_standalone=True,
):
    backup_info = catalog.get_backup_info(backup_id)
    if not backup_info:
        logging.warning("Backup %s does not exist", backup_id)
        return

    if backup_info.snapshots_info:
        logging.debug(
            "Will delete the following snapshots: %s",
            ", ".join(
                snapshot.identifier for snapshot in backup_info.snapshots_info.snapshots
            ),
        )
        if not config.dry_run:
            snapshot_interface = get_snapshot_interface_from_backup_info(
                backup_info, config
            )
            snapshot_interface.delete_snapshot_backup(backup_info)
        else:
            print("Skipping deletion of snapshots due to --dry-run option")
        # Delete the backup_label for snapshots backups as this is not stored in the
        # same format used by the non-snapshot backups.
        backup_label_path = os.path.join(
            catalog.prefix, backup_info.backup_id, "backup_label"
        )
        if not config.dry_run:
            cloud_interface.delete_objects([backup_label_path])
        else:
            print("Skipping deletion of %s due to --dry-run option" % backup_label_path)

    objects_to_delete = _get_files_for_backup(catalog, backup_info)
    backup_info_path = os.path.join(
        catalog.prefix, backup_info.backup_id, "backup.info"
    )
    logging.debug("Will delete backup.info file at %s" % backup_info_path)
    if not config.dry_run:
        try:
            cloud_interface.delete_objects(objects_to_delete)
            # Do not try to delete backup.info until we have successfully deleted
            # everything else so that it is possible to retry the operation should
            # we fail to delete any backup file
            cloud_interface.delete_objects([backup_info_path])
        except Exception as exc:
            logging.error("Could not delete backup %s: %s", backup_id, force_str(exc))
            raise OperationErrorExit()
    else:
        print(
            "Skipping deletion of objects %s due to --dry-run option"
            % (objects_to_delete + [backup_info_path])
        )

    _remove_wals_for_backup(
        cloud_interface,
        catalog,
        backup_info,
        config.dry_run,
        skip_wal_cleanup_if_standalone,
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
                raise NetworkErrorExit()
            # If test is requested, just exit after connectivity test
            elif config.test:
                raise SystemExit(0)

            if not cloud_interface.bucket_exists:
                logging.error("Bucket %s does not exist", cloud_interface.bucket_name)
                raise OperationErrorExit()

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
                raise OperationErrorExit()
            if config.backup_id:
                backup_id = catalog.parse_backup_id(config.backup_id)
                # Because we only care about one backup, skip the annotation cache
                # because it is only helpful when dealing with multiple backups
                if catalog.should_keep_backup(backup_id, use_cache=False):
                    logging.error(
                        "Skipping delete of backup %s for server %s "
                        "as it has a current keep request. If you really "
                        "want to delete this backup please remove the keep "
                        "and try again.",
                        backup_id,
                        config.server_name,
                    )
                    raise OperationErrorExit()
                if config.minimum_redundancy > 0:
                    if config.minimum_redundancy >= len(catalog.get_backup_list()):
                        logging.error(
                            "Skipping delete of backup %s for server %s "
                            "due to minimum redundancy requirements "
                            "(minimum redundancy = %s, "
                            "current redundancy = %s)",
                            backup_id,
                            config.server_name,
                            config.minimum_redundancy,
                            len(catalog.get_backup_list()),
                        )
                        raise OperationErrorExit()
                _delete_backup(cloud_interface, catalog, backup_id, config)
            elif config.retention_policy:
                try:
                    retention_policy = RetentionPolicyFactory.create(
                        "retention_policy",
                        config.retention_policy,
                        server_name=config.server_name,
                        catalog=catalog,
                        minimum_redundancy=config.minimum_redundancy,
                    )
                except InvalidRetentionPolicy as exc:
                    logging.error(
                        "Could not create retention policy %s: %s",
                        config.retention_policy,
                        force_str(exc),
                    )
                    raise CLIErrorExit()
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
                        config,
                        skip_wal_cleanup_if_standalone=False,
                    )
    except Exception as exc:
        logging.error("Barman cloud backup delete exception: %s", force_str(exc))
        logging.debug("Exception details:", exc_info=exc)
        raise GeneralErrorExit()


def parse_arguments(args=None):
    """
    Parse command line arguments

    :return: The options parsed
    """
    parser, _, _ = create_argument_parser(
        description="This script can be used to delete backups "
        "made with barman-cloud-backup command. "
        "Currently AWS S3, Azure Blob Storage and Google Cloud Storage are supported.",
    )
    delete_arguments = parser.add_mutually_exclusive_group(required=True)
    delete_arguments.add_argument(
        "-b",
        "--backup-id",
        help="Backup ID of the backup to be deleted",
    )
    parser.add_argument(
        "-m",
        "--minimum-redundancy",
        type=check_non_negative,
        help="The minimum number of backups that should always be available.",
        default=0,
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
        "--batch-size",
        dest="delete_batch_size",
        type=int,
        help="The maximum number of objects to be deleted in a single request to the "
        "cloud provider. If unset then the maximum allowed batch size for the "
        "specified cloud provider will be used (1000 for aws-s3, 256 for "
        "azure-blob-storage and 100 for google-cloud-storage).",
    )
    return parser.parse_args(args=args)


if __name__ == "__main__":
    main()
