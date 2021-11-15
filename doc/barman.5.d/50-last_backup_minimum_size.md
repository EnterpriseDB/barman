last_backup_minimum_size
:   This option identifies lower limit to the acceptable size of the latest successful backup.
    If the latest backup is smaller than the specified size, barman check command will
    report an error to the user.
    If empty (default), latest backup is always considered valid.
    Syntax for this option is: "i (k|Ki|M|Mi|G|Gi|T|Ti)" where i is an integer
    greater than zero, with an optional SI or IEC suffix. k=kilo=1000, Ki=Kibi=1024 and so forth.
    Note that the suffix is case-sensitive.
    Global/Server.
