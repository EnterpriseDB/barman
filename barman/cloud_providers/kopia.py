import io
import json
import os
import tempfile
from barman.cloud import CloudBackup
from barman.command_wrappers import Command
from barman.infofile import BackupInfo


class KopiaCloudInterface(object):
    def __init__(self, *args, **kwargs):
        pass

    def test_connectivity(self):
        return True

    def close(self):
        pass

    def setup_bucket(self):
        pass

    def bucket_exists(self):
        pass


class CloudBackupKopia(CloudBackup):
    def __init__(
        self,
        server_name,
        cloud_interface,
        max_archive_size,
        postgres,
        compression=None,
        backup_name=None,
        **kwargs,
    ):
        super(CloudBackupKopia, self).__init__(
            server_name,
            cloud_interface,
            postgres,
            backup_name=backup_name,
        )

    def _take_backup(self):
        """
        Take a kopia snapshot including PGDATA and all tablespaces.
        """
        tablespaces = []
        for tablespace in self.backup_info.tablespaces:
            if tablespace.location.startswith(self.backup_info.pgdata + "/"):
                # We can't exclude the tablespace from the copy if they're in PGDATA
                # but we can avoid adding it to the snapshot a second time.
                continue
            # The symlinks in pg_tblspc will be copied as symlinks so we don't
            # need to exclude them.
            tablespaces.append(tablespace.location)

        kopia_cmd = Kopia(
            "kopia",
            "snapshot",
            [
                "create",
                self.backup_info.pgdata,
                *(self._tags_args + ["--tags", "type:pgdata"]),
            ],
        )
        kopia_cmd()

        kopia_cmd = Kopia(
            "kopia",
            "snapshot",
            [
                "create",
                *tablespaces,
                *(self._tags_args + ["--tags", "type:tablespace"]),
            ],
        )
        kopia_cmd()

    def _upload_backup_label(self):
        """No-op because backup label gets added to a snapshot with backup.info."""
        pass

    def _add_stats_to_backup_info(self):
        """Maybe add some useful stuff here?"""
        pass

    def _finalise_copy(self):
        """Probably nothing to do here."""
        pass

    def _upload_backup_info(self):
        """Create a separate kopia snapshot with just the metadata."""
        # Write both the backup label and backup info to a staging location
        tempdir = tempfile.mkdtemp(prefix="backup-metadata")
        if self.backup_info.backup_label is not None:
            with open(os.path.join(tempdir, "backup_label"), "w") as backup_label:
                backup_label.write(self.backup_info.backup_label)
        self.backup_info.save(filename=os.path.join(tempdir, "backup.info"))
        # Create a kopia snapshot of that location and tag accordingly
        kopia_cmd = Kopia(
            "kopia",
            "snapshot",
            [
                "create",
                tempdir,
                *(self._tags_args + ["--tags", "type:metadata"]),
            ],
        )
        kopia_cmd()

    def backup(self):
        """
        Upload a Backup via Kopia, probably
        """
        server_name = "cloud"
        self.backup_info = self._get_backup_info(server_name)

        self._check_postgres_version()

        # Figure out the tags for use later in the process
        self._tags_args = [
            "--tags",
            f"backup_id:{self.backup_info.backup_id}",
            "--tags",
            f"server:{self.server_name}",
        ]
        if self.backup_name is not None:
            self._tags_args.extend(["--tags", f"backup_name:{self.backup_name}"])

        self._coordinate_backup()


class Kopia(Command):
    """
    Wrapper for the kopia command.
    """

    def __init__(
        self,
        kopia="kopia",
        subcommand=None,
        args=None,
        path=None,
        json=True,
    ):
        options = []
        if subcommand is not None:
            options += [subcommand]
        if args is not None:
            options += args
        if json:
            options.append("--json")
        super(Kopia, self).__init__(kopia, args=options, path=path)


class KopiaBackupCatalog(object):
    """Reimplmentation of barman.cloud.CloudBackupCatalog but for kopia."""

    def __init__(self, server_name):
        self.server_name = server_name
        self._backup_list = None

    def get_backup_list(self):
        if self._backup_list is None:
            # Get all snapshots of type:metadata for this server
            kopia_cmd = Kopia(
                "kopia",
                "snapshot",
                [
                    "list",
                    "--tags",
                    "type:metadata",
                    "--tags",
                    f"server:{self.server_name}",
                ],
            )
            kopia_cmd()
            out, _err = kopia_cmd.get_output()
            backups = json.loads(out)
            backup_list = {}
            for backup in backups:
                backup_id = backup["tags"]["tag:backup_id"]
                snapshot_id = backup["rootEntry"]["obj"]  # *not* "id"
                backup_info = self.get_backup_info(backup_id, snapshot_id)
                backup_list[backup_id] = backup_info
            self._backup_list = backup_list
        return self._backup_list

    def get_backup_info(self, backup_id, snapshot_id=None):
        if not snapshot_id:
            # TODO Find backup metadata from its tag
            pass
        kopia_cmd = Kopia("kopia", "show", [f"{snapshot_id}/backup.info"], json=False)
        kopia_cmd()
        out, _err = kopia_cmd.get_output()
        backup_info = BackupInfo(backup_id)
        backup_info.load(file_object=io.BytesIO(bytes(out, "utf-8")))
        return backup_info

    # TODO probably need another mixin to support keep annotations in kopia but for now here be stubs
    def get_keep_target(self, backup_id, use_cache=True):
        return
