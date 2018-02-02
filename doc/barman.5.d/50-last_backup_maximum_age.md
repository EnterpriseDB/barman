last_backup_maximum_age
:   This option identifies a time frame that must contain the latest backup.
    If the latest backup is older than the time frame, barman check command
    will report an error to the user.
    If empty (default), latest backup is always considered valid.
    Syntax for this option is: "i (DAYS | WEEKS | MONTHS)" where i is a integer
    greater than zero, representing the number of days | weeks | months
    of the time frame. Global/Server.
