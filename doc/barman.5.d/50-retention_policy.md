retention_policy
:   Policy for retention of periodic backups and archive logs. If left empty,
    retention policies are not enforced. For redundancy based retention policy
    use "REDUNDANCY i" (where i is an integer > 0 and defines the number
    of backups to retain). For recovery window retention policy use
    "RECOVERY WINDOW OF i DAYS" or "RECOVERY WINDOW OF i WEEKS" or
    "RECOVERY WINDOW OF i MONTHS" where i is a positive integer representing,
    specifically, the number of days, weeks or months to retain your backups.
    For more detailed information, refer to the official documentation.
    Default value is empty. Global/Server.
