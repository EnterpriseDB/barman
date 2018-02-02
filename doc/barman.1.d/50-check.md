check *SERVER_NAME*
:   Show diagnostic information about `SERVER_NAME`, including:
    Ssh connection check, PostgreSQL version, configuration and backup
    directories, archiving process, streaming process, replication slots, etc.
    Specify `all` as `SERVER_NAME` to show diagnostic information
    about all the configured servers.

    --nagios
    :    Nagios plugin compatible output
