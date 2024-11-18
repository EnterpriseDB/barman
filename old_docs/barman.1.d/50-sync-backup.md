sync-backup *SERVER_NAME* *BACKUP_ID*
:   Command used for the synchronisation of a passive node with its primary.
    Executes a copy of all the files of a `BACKUP_ID` that is present on
    `SERVER_NAME` node. This command is available only for passive nodes,
    and uses the `primary_ssh_command` option to establish a secure connection
    with the primary node.
