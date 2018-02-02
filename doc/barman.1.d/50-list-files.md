list-files *\[OPTIONS\]* *SERVER_NAME* *BACKUP_ID*
:   List all the files in a particular backup, identified by the server
    name and the backup ID. See the [Backup ID shortcuts](#shortcuts) section below
    for available shortcuts.

    --target *TARGET_TYPE*
    :    Possible values for TARGET_TYPE are:

         - *data*: lists just the data files;
         - *standalone*: lists the base backup files, including required
           WAL files;
         - *wal*: lists all the WAL files between the start of the base
           backup and the end of the log / the start of the following base
           backup (depending on whether the specified base backup is the most
           recent one available);
         - *full*: same as data + wal.

		The default value is `standalone`.
