backup_options
:   This option allows you to control the way Barman interacts with PostgreSQL
    for backups. It is a comma-separated list of values that accepts the
    following options:

    * `exclusive_backup` (default when `backup_method = rsync`):
      `barman backup` executes backup operations using the standard
      exclusive backup approach (technically through `pg_start_backup`
      and `pg_stop_backup`)
    * `concurrent_backup` (default when `backup_method = postgres`):
      if using PostgreSQL 9.2, 9.3, 9.4, and 9.5, Barman requires the
      `pgespresso` module to be installed on the PostgreSQL server
      and can be used to perform a backup from a standby server.
      Starting from PostgreSQL 9.6, Barman uses the new PostgreSQL API to
      perform backups from a standby server.
    * `external_configuration`: if present, any warning regarding
      external configuration files is suppressed during the execution
      of a backup.

    Note that `exclusive_backup` and `concurrent_backup` are mutually
    exclusive. Global/Server.
