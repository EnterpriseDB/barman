sync-wals *SERVER_NAME*
:   Command used for the synchronisation of a passive node with its primary.
    Executes a copy of all the archived WAL files that are present on
    `SERVER_NAME` node. This command is available only for passive nodes,
    and uses the `primary_ssh_command` option to establish a secure connection
    with the primary node.
