from barman.cloud import CloudBackup
from barman.command_wrappers import Command


class KopiaCloudInterface(object):
    def __init__(self, *args, **kwargs):
        pass

    def test_connectivity(self):
        return True

    def close(self):
        pass

    def setup_bucket(self):
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
        # TODO add tablespaces
        kopia_cmd = Kopia(
            "kopia",
            "snapshot",
            [
                "create",
                self.backup_info.pgdata,
                "--tags",
                f"backup_id:{self.backup_info.backup_id}",
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
        # Create a kopia snapshot of that location and tag accordingly
        # TODO create a new snapshot with backup label and backup.info here

    def backup(self):
        """
        Upload a Backup via Kopia, probably
        """
        server_name = "cloud"
        self.backup_info = self._get_backup_info(server_name)

        self._check_postgres_version()

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
    ):
        options = []
        if subcommand is not None:
            options += [subcommand]
        if args is not None:
            options += args
        super(Kopia, self).__init__(kopia, args=options, path=path)
