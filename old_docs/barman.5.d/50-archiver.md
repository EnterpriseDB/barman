archiver
:   This option allows you to activate log file shipping through PostgreSQL's
    `archive_command` for a server. If set to `true`, Barman expects that
    continuous archiving for a server is in place and will activate checks as
    well as management (including compression) of WAL files that Postgres
    deposits in the *incoming* directory. Setting it to `false` (default),
    will disable standard continuous archiving for a server. Note: If neither
    `archiver` nor `streaming_archiver` are set, Barman will automatically set
    this option to `true`. This is in order to maintain parity with deprecated
    behaviour where `archiver` would be enabled by default. This behaviour will
    be removed from the next major Barman version.

    Scope: Global/Server/Model.
