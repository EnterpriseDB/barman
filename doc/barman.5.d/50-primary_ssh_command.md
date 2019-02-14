primary_ssh_command
:   Parameter that identifies a Barman server as `passive`.
    In a passive node, the source of a backup server is a Barman installation
    rather than a PostgreSQL server.
    If `primary_ssh_command` is specified, Barman uses it to establish a
    connection with the primary server.
    Empty by default, it can also be set globally.
