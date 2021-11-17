last_wal_maximum_age
:   This option identifies a time frame that must contain the latest WAL file archived.
    If the latest WAL file is older than the time frame, barman check command
    will report an error to the user.
    If empty (default), the age of the WAL files is not checked.
    Syntax is the same as last_backup_maximum_age (above).
    Global/Server.