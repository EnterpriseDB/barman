archiver
:   This option allows you to activate log file shipping through PostgreSQL's
    `archive_command` for a server. If set to `true` (default), Barman expects
    that continuous archiving for a server is in place and will activate
    checks as well as management (including compression) of WAL files that
    Postgres deposits in the *incoming* directory. Setting it to `false`,
    will disable standard continuous archiving for a server. Global/Server.
