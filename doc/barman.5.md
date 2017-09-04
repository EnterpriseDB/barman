% BARMAN(5) Barman User manuals | Version 2.3
% 2ndQuadrant Limited <https://www.2ndQuadrant.com>
% September 05, 2017

# NAME
barman - Backup and Recovery Manager for PostgreSQL

# DESCRIPTION

Barman is an administration tool for disaster recovery of PostgreSQL
servers written in Python and maintained by 2ndQuadrant.
Barman can perform remote backups of multiple servers in business critical
environments and helps DBAs during the recovery phase.

# CONFIGURATION FILE LOCATIONS

The system-level Barman configuration file is located at

    /etc/barman.conf

or

    /etc/barman/barman.conf

and is overridden on a per-user level by

    $HOME/.barman.conf

# CONFIGURATION FILE SYNTAX

The Barman configuration file is a plain `INI` file.
There is a general section called `[barman]` and a
section `[servername]` for each server you want to backup.
Rows starting with `;` are comments.

# CONFIGURATION FILE DIRECTORY

Barman supports the inclusion of multiple configuration files, through
the `configuration_files_directory` option. Included files must contain
only server specifications, not global configurations.
If the value of `configuration_files_directory` is a directory, Barman reads
all files with `.conf` extension that exist in that folder.
For example, if you set it to `/etc/barman.d`, you can
specify your PostgreSQL servers placing each section in a separate `.conf`
file inside the `/etc/barman.d` folder.

# OPTIONS

active
:   When set to `true` (default), the server is in full operational state.
    When set to `false`, the server can be used for diagnostics, but any
    operational command such as backup execution or WAL archiving is
    temporarily disabled. Setting `active=false` is a good practice
    when adding a new node to Barman. Server.

archiver
:   This option allows you to activate log file shipping through PostgreSQL's
    `archive_command` for a server. If set to `true` (default), Barman expects
    that continuous archiving for a server is in place and will activate
    checks as well as management (including compression) of WAL files that
    Postgres deposits in the *incoming* directory. Setting it to `false`,
    will disable standard continuous archiving for a server. Global/Server.

archiver_batch_size
:   This option allows you to activate batch processing of WAL files
    for the `archiver` process, by setting it to a value > 0. Otherwise,
    the traditional unlimited processing of the WAL queue is enabled.
    When batch processing is activated, the `archive-wal` process would
    limit itself to maximum `archiver_batch_size` WAL segments per single
    run. Integer. Global/Server.

backup_directory
:   Directory where backup data for a server will be placed. Server.

backup_method
:   Configure the method barman used for backup execution.
    If set to `rsync` (default), barman will execute backup using the `rsync`
    command. If set to `postgres` barman will use the `pg_basebackup` command
    to execute the backup. Global/Server.

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

bandwidth_limit
:   This  option  allows  you  to specify a maximum transfer rate in
    kilobytes per second. A value of zero specifies no limit (default).
    Global/Server.

barman_home
:   Main data directory for Barman. Global.

barman_lock_directory
:   Directory for locks. Default: `%(barman_home)s`. Global.

basebackups_directory
:   Directory where base backups will be placed. Server.

basebackup_retry_sleep
:   Number of seconds of wait after a failed copy, before retrying
    Used during both backup and recovery operations.
    Positive integer, default 30. Global/Server.

basebackup_retry_times
:   Number of retries of base backup copy, after an error.
    Used during both backup and recovery operations.
    Positive integer, default 0. Global/Server.

check_timeout
:   Maximum execution time, in seconds per server, for a barman check
    command. Set to 0 to disable the timeout.
    Positive integer, default 30. Global/Server.

compression
:   Standard compression algorithm applied to WAL files. Possible values
    are: `gzip` (requires `gzip` to be installed on the system),
    `bzip2` (requires `bzip2`), `pigz` (requires `pigz`), `pygzip`
    (Python's internal gzip compressor) and `pybzip2` (Python's internal
    bzip2 compressor). Global/Server.

conninfo
:   Connection string used by Barman to connect to the Postgres server.
    This is a libpq connection string, consult the
    [PostgreSQL manual][conninfo] for more information. Commonly used
    keys are: host, hostaddr, port, dbname, user, password. Server.

[conninfo]: https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING

custom_compression_filter
:   Customised compression algorithm applied to WAL files. Global/Server.

custom_decompression_filter
:   Customised decompression algorithm applied to compressed WAL files;
    this must match the compression algorithm. Global/Server.

description
:   A human readable description of a server. Server.

errors_directory
:   Directory that contains WAL files that contain an error; usually
    this is related to a conflict with an existing WAL file (e.g. a WAL
    file that has been archived after a streamed one).

immediate_checkpoint
:   This option allows you to control the way PostgreSQL handles
    checkpoint at the start of the backup.
    If set to `false` (default), the I/O workload for the checkpoint
    will be limited, according to the `checkpoint_completion_target`
    setting on the PostgreSQL server. If set to `true`, an immediate
    checkpoint will be requested, meaning that PostgreSQL will complete
    the checkpoint as soon as possible. Global/Server.

incoming_wals_directory
:   Directory where incoming WAL files are archived into.
    Requires `archiver` to be enabled. Server.

last_backup_maximum_age
:   This option identifies a time frame that must contain the latest backup.
    If the latest backup is older than the time frame, barman check command
    will report an error to the user.
    If empty (default), latest backup is always considered valid.
    Syntax for this option is: "i (DAYS | WEEKS | MONTHS)" where i is a integer
    greater than zero, representing the number of days | weeks | months
    of the time frame. Global/Server.

log_file
:   Location of Barman's log file. Global.

log_level
:   Level of logging (DEBUG, INFO, WARNING, ERROR, CRITICAL). Global.

max_incoming_wals_queue
:   Maximum number of WAL files in the incoming queue (in both streaming and
    archiving pools) that are allowed before barman check returns an error
    (that does not block backups). Global/Server. Default: None (disabled).

minimum_redundancy
:   Minimum number of backups to be retained. Default 0. Global/Server.

network_compression
:   This option allows you to enable data compression for network
    transfers.
    If set to `false` (default), no compression is used.
    If set to `true`, compression is enabled, reducing network usage.
    Global/Server.

parallel_jobs
:   This option controls how many parallel workers will copy files during a
    backup or recovery command. Default 1. Global/Server. For backup purposes,
    it works only when `backup_method` is `rsync`.

path_prefix
:   One or more absolute paths, separated by colon, where Barman looks for
    executable files. The paths specified in `path_prefix` are tried before
    the ones specified in `PATH` environment variable. Global/server.

post_archive_retry_script
:   Hook script launched after a WAL file is archived by maintenance.
    Being this a _retry_ hook script, Barman will retry the execution of the
    script until this either returns a SUCCESS (0), an ABORT_CONTINUE (62) or
    an ABORT_STOP (63) code. In a post archive scenario, ABORT_STOP
    has currently the same effects as ABORT_CONTINUE. Global/Server.

post_archive_script
:   Hook script launched after a WAL file is archived by maintenance,
    after 'post_archive_retry_script'. Global/Server.

post_backup_retry_script
:   Hook script launched after a base backup.
    Being this a _retry_ hook script, Barman will retry the execution of the
    script until this either returns a SUCCESS (0), an ABORT_CONTINUE (62) or
    an ABORT_STOP (63) code. In a post backup scenario, ABORT_STOP
    has currently the same effects as ABORT_CONTINUE. Global/Server.

post_backup_script
:   Hook script launched after a base backup, after 'post_backup_retry_script'.
    Global/Server.

pre_archive_retry_script
:   Hook script launched before a WAL file is archived by maintenance,
    after 'pre_archive_script'.
    Being this a _retry_ hook script, Barman will retry the execution of the
    script until this either returns a SUCCESS (0), an ABORT_CONTINUE (62) or
    an ABORT_STOP (63) code. Returning ABORT_STOP will propagate the failure at
    a higher level and interrupt the WAL archiving operation. Global/Server.

pre_archive_script
:   Hook script launched before a WAL file is archived by maintenance.
    Global/Server.

pre_backup_retry_script
:   Hook script launched before a base backup, after 'pre_backup_script'.
    Being this a _retry_ hook script, Barman will retry the execution of the
    script until this either returns a SUCCESS (0), an ABORT_CONTINUE (62) or
    an ABORT_STOP (63) code. Returning ABORT_STOP will propagate the failure at
    a higher level and interrupt the backup operation. Global/Server.

pre_backup_script
:   Hook script launched before a base backup. Global/Server.

recovery_options
:   Options for recovery operations. Currently only supports `get-wal`.
    `get-wal` activates generation of a basic `restore_command` in
    the resulting `recovery.conf` file that uses the `barman get-wal`
    command to fetch WAL files directly from Barman's archive of WALs.
    Comma separated list of values, default empty. Global/Server.

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

retention_policy_mode
:   Currently only "auto" is implemented. Global/Server.

reuse_backup
:   This option controls incremental backup support. Global/Server.
    Possible values are:
    * `off`: disabled (default);
    * `copy`: reuse the last available backup for a server and
    create a copy of the unchanged files (reduce backup time);
    * `link`: reuse the last available backup for a server and
      create a hard link of the unchanged files (reduce backup time
      and space). Requires operating system and file system support
      for hard links.

slot_name
:   Physical replication slot to be used by the `receive-wal`
    command when `streaming_archiver` is set to `on`. Requires
    PostgreSQL >= 9.4. Global/Server. Default: None (disabled).

streaming_archiver
:   This option allows you to use the PostgreSQL's streaming protocol to
    receive transaction logs from a server. If set to `on`, Barman expects
    to find `pg_receivexlog` in the PATH (see `path_prefix` option) and that
    streaming connection for the server is working. This activates connection
    checks as well as management (including compression) of WAL files.
    If set to `off` (default) barman will rely only on continuous archiving
    for a server WAL archive operations, eventually terminating any running
    `pg_receivexlog` for the server. Global/Server.

streaming_archiver_batch_size
:   This option allows you to activate batch processing of WAL files
    for the `streaming_archiver` process, by setting it to a value > 0.
    Otherwise, the traditional unlimited processing of the WAL queue
    is enabled. When batch processing is activated, the `archive-wal`
    process would limit itself to maximum `streaming_archiver_batch_size`
    WAL segments per single run. Integer. Global/Server.

streaming_archiver_name
:   Identifier to be used as `application_name` by the `receive-wal` command.
    Only available with `pg_receivexlog` >= 9.3. By default it is set to
    `barman_receive_wal`. Global/Server.

streaming_backup_name
:   Identifier to be used as `application_name` by the `pg_basebackup` command.
    Only available with `pg_basebackup` >= 9.3. By default it is set to
    `barman_streaming_backup`. Global/Server.

streaming_conninfo
:   Connection string used by Barman to connect to the Postgres server via
    streaming replication protocol. By default it is set to `conninfo`. Server.

streaming_wals_directory
:   Directory where WAL files are streamed from the PostgreSQL server
    to Barman. Requires `streaming_archiver` to be enabled. Server.

ssh_command
:   Command used by Barman to login to the Postgres server via ssh. Server.

tablespace_bandwidth_limit
:   This  option  allows  you  to specify a maximum transfer rate in
    kilobytes per second, by specifying a comma separated list of
    tablespaces (pairs TBNAME:BWLIMIT). A value of zero specifies no limit
    (default). Global/Server.

wal_retention_policy
:   Policy for retention of archive logs (WAL files). Currently only "MAIN"
    is available. Global/Server.

wals_directory
:   Directory which contains WAL files. Server.

# HOOK SCRIPTS

The script definition is passed to a shell and can return any exit code.

The shell environment will contain the following variables:

`BARMAN_CONFIGURATION`
:   configuration file used by barman

`BARMAN_ERROR`
:   error message, if any (only for the 'post' phase)

`BARMAN_PHASE`
:   'pre' or 'post'

`BARMAN_RETRY`
:   `1` if it is a _retry script_ (from 1.5.0), `0` if not

`BARMAN_SERVER`
:   name of the server

Backup scripts specific variables:

`BARMAN_BACKUP_DIR`
:   backup destination directory

`BARMAN_BACKUP_ID`
:   ID of the backup

`BARMAN_PREVIOUS_ID`
:   ID of the previous backup (if present)

`BARMAN_STATUS`
:   status of the backup

`BARMAN_VERSION`
:   version of Barman

Archive scripts specific variables:

`BARMAN_SEGMENT`
:   name of the WAL file

`BARMAN_FILE`
:   full path of the WAL file

`BARMAN_SIZE`
:   size of the WAL file

`BARMAN_TIMESTAMP`
:   WAL file timestamp

`BARMAN_COMPRESSION`
:   type of compression used for the WAL file

Only in case of retry hook scripts, the exit code of the script
is checked by Barman. Output of hook scripts is simply written
in the log file.

# EXAMPLE

Here is an example of configuration file:

```
[barman]
; Main directory
barman_home = /var/lib/barman

; System user
barman_user = barman

; Log location
log_file = /var/log/barman/barman.log

; Default compression level
;compression = gzip

; Incremental backup
reuse_backup = link

; 'main' PostgreSQL Server configuration
[main]
; Human readable description
description =  "Main PostgreSQL Database"

; SSH options
ssh_command = ssh postgres@pg

; PostgreSQL connection string
conninfo = host=pg user=postgres

; PostgreSQL streaming connection string
streaming_conninfo = host=pg user=postgres

; Minimum number of required backups (redundancy)
minimum_redundancy = 1

; Retention policy (based on redundancy)
retention_policy = REDUNDANCY 2
```

# SEE ALSO

`barman` (1).

# AUTHORS

In alphabetical order:

* Gabriele Bartolini <gabriele.bartolini@2ndquadrant.it> (architect)
* Jonathan Battiato <jonathan.battiato@2ndquadrant.it> (QA/testing)
* Giulio Calacoci <giulio.calacoci@2ndquadrant.it> (developer)
* Francesco Canovai <francesco.canovai@2ndquadrant.it> (QA/testing)
* Leonardo Cecchi <leonardo.cecchi@2ndquadrant.it> (developer)
* Gianni Ciolli <gianni.ciolli@2ndquadrant.it> (QA/testing)
* Britt Cole <britt.cole@2ndquadrant.com> (documentation)
* Marco Nenciarini <marco.nenciarini@2ndquadrant.it> (project leader)
* Rubens Souza <rubens.souza@2ndquadrant.it> (QA/testing)

Past contributors:

* Carlo Ascani
* Stefano Bianucci
* Giuseppe Broccolo

# RESOURCES

* Homepage: <http://www.pgbarman.org/>
* Documentation: <http://docs.pgbarman.org/>
* Professional support: <https://www.2ndQuadrant.com/>

# COPYING

Barman is the property of 2ndQuadrant Limited
and its code is distributed under GNU General Public License v3.

Copyright (C) 2011-2017 2ndQuadrant Limited - https://www.2ndQuadrant.com/.
