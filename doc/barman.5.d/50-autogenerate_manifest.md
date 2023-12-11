autogenerate_manifest
:   This option enables the auto-generation of backup manifest files
    for rsync based backups and strategies.
    The manifest file is a JSON file containing the list of files contained in
    the backup.
    It is generated at the end of the backup process and stored in the backup
    directory.
    The manifest file generated follows the format described in the postgesql
    documentation, and is compatible with the `pg_verifybackup` tool.
    The option is ignored if the backup method is not rsync.

    Scope: Global/Server/Model.
