backup_options
:   This option allows you to control the way Barman interacts with PostgreSQL
    for backups. It is a comma-separated list of values that accepts the
    following options:

    * `concurrent_backup` (default):
      `barman backup` executes backup operations using concurrent
      backup which is the recommended backup approach for PostgreSQL
      versions >= 9.6 and uses the PostgreSQL API. `concurrent_backup` can
      also be used to perform a backup from a standby server.
    * `exclusive_backup` (PostgreSQL versions older than 15 only):
      `barman backup` executes backup operations using the deprecated
      exclusive backup approach (technically through `pg_start_backup`
      and `pg_stop_backup`)
    * `external_configuration`: if present, any warning regarding
      external configuration files is suppressed during the execution
      of a backup.

    Note that `exclusive_backup` and `concurrent_backup` are mutually
    exclusive. Global/Server.
