recovery_staging_path
:   A path to a location on the recovery host (either the barman server
    or a remote host if --remote-ssh-command is also used) where files
    for a compressed backup will be staged before being uncompressed to
    the destination directory. Backups will be staged in their own directory
    within the staging path according to the following naming convention:
    "barman-staging-SERVER_NAME-BACKUP_ID". The staging directory within
    the staging path will be removed at the end of the recovery process.
    This option is *required* when recovering from compressed backups and
    has no effect otherwise. Global/Server.
