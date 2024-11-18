primary_checkpoint_timeout
:   This defines the amount of seconds that Barman will wait at the end of a
    backup if no new WAL files are produced, before forcing a checkpoint on
    the primary server.

    If not set or set to 0, Barman will not force a checkpoint on the primary,
    and wait indefinitely for new WAL files to be produced.

    The value of this option should be greater of the value of the 
    `archive_timeout` set on the primary server.

    This option works only if `primary_conninfo` option is set, and it is
    ignored otherwise.

    Scope: Server/Model.
