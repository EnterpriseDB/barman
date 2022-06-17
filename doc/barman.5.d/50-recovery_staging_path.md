recovery_staging_path
:   A path to a location on the recovery host (either the barman server
    or a remote host if --remote-ssh-command is also used) where files
    for a compressed backup will be staged before being uncompressed to
    the destination directory. Backups will be staged in their own directory
    within the staging path according to the following naming convention:
    "barman-staging-SERVER_NAME-BACKUP_ID". The staging directory within
    the staging path will be removed upon successful recovery, otherwise
    it will be left in place so that subsequent recovery attempts can avoid
    transferring files which have already been transferred. This option is
    *required* when recovering from compressed backups and has no effect
    otherwise. Global/Server.
