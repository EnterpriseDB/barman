\newpage

# Features in detail

In this section we present several Barman features and discuss their
applicability and the configuration required to use them.

This list is not exhaustive, as many scenarios can be created working
on the Barman configuration. Nevertheless, it is useful to discuss
common patterns.

## Backup features
### Incremental backup

Barman implements **file-level incremental backup**. Incremental
backup is a type of full periodic backup which only saves data changes
from the latest full backup available in the catalog for a specific
PostgreSQL server. It must not be confused with differential backup,
which is implemented by _WAL continuous archiving_.

> **NOTE:** Block level incremental backup will be available in
> future versions.

> **IMPORTANT:** The `reuse_backup` option can't be used with the
> `postgres` backup method at this time.

The main goals of incremental backups in Barman are:

- Reduce the time taken for the full backup process
- Reduce the disk space occupied by several periodic backups (**data
  deduplication**)

This feature heavily relies on `rsync` and [hard links][8], which
must therefore be supported by both the underlying operating system
and the file system where the backup data resides.

The main concept is that a subsequent base backup will share those
files that have not changed since the previous backup, leading to
relevant savings in disk usage. This is particularly true of VLDB
contexts and of those databases containing a high percentage of
_read-only historical tables_.

Barman implements incremental backup through a global/server option
called `reuse_backup`, that transparently manages the `barman backup`
command. It accepts three values:

- `off`: standard full backup (default)
- `link`: incremental backup, by reusing the last backup for a server
  and creating a hard link of the unchanged files (for backup space
  and time reduction)
- `copy`: incremental backup, by reusing the last backup for a server
  and creating a copy of the unchanged files (just for backup time
  reduction)

The most common scenario is to set `reuse_backup` to `link`, as
follows:

``` ini
reuse_backup = link
```

Setting this at global level will automatically enable incremental
backup for all your servers.

As a final note, users can override the setting of the `reuse_backup`
option through the `--reuse-backup` runtime option for the `barman
backup` command. Similarly, the runtime option accepts three values:
`off`, `link` and `copy`. For example, you can run a one-off
incremental backup as follows:

``` bash
barman backup --reuse-backup=link <server_name>
```

### Limiting bandwidth usage

It is possible to limit the usage of I/O bandwidth through the
`bandwidth_limit` option (global/per server), by specifying the
maximum number of kilobytes per second. By default it is set to 0,
meaning no limit.

> **IMPORTANT:** the `bandwidth_limit` and the
> `tablespace_bandwidth_limit` options are not supported with the
> `postgres` backup method

In case you have several tablespaces and you prefer to limit the I/O
workload of your backup procedures on one or more tablespaces, you can
use the `tablespace_bandwidth_limit` option (global/per server):

``` ini
tablespace_bandwidth_limit = tbname:bwlimit[, tbname:bwlimit, ...]
```

The option accepts a comma separated list of pairs made up of the
tablespace name and the bandwidth limit (in kilobytes per second).

When backing up a server, Barman will try and locate any existing
tablespace in the above option. If found, the specified bandwidth
limit will be enforced. If not, the default bandwidth limit for that
server will be applied.


### Network Compression

It is possible to reduce the size of transferred data using
compression. It can be enabled using the `network_compression` option
(global/per server):

> **IMPORTANT:** the `network_compression` option is not available
> with the `postgres` backup method.

``` ini
network_compression = true|false
```

Setting this option to `true` will enable data compression during
network transfers (for both backup and recovery). By default it is set
to `false`.


### Concurrent Backup and backup from a standby

Normally, during backup operations, Barman uses PostgreSQL native
functions `pg_start_backup` and `pg_stop_backup` for _exclusive
backup_. These operations are not allowed on a read-only standby
server.

Barman is also capable of performing backups of PostgreSQL from 9.2 or
greater database servers in a **concurrent way**, primarily through
the `backup_options` configuration
parameter.[^ABOUT_CONCURRENT_BACKUP]

[^ABOUT_CONCURRENT_BACKUP]:
  Concurrent backup is a technology that has been available in
  PostgreSQL since version 9.2, through the _streaming replication
  protocol_ (for example, using a tool like `pg_basebackup`).

This introduces a new architecture scenario with Barman: **backup from
a standby server**, using `rsync`.

> **IMPORTANT:** **Concurrent backup** requires users of PostgreSQL
> 9.2, 9.3, 9.4, and 9.5 to install the `pgespresso` open source
> extension on every PostgreSQL server of the cluster. For more
> detailed information and the source code, please visit the
> [pgespresso extension website][9].  Barman supports the new API
> introduced in PostgreSQL 9.6. This removes the requirement of the
> `pgespresso` extension to perform concurrent backups from this
> version of PostgreSQL.

By default, `backup_options` is transparently set to
`exclusive_backup` for back compatibility reasons.
Users of PostgreSQL 9.6 should set `backup_options` to `concurrent_backup`.

When `backup_options` is set to `concurrent_backup`, Barman activates
the _concurrent backup mode_ for a server and follows these two simple
rules:

- `ssh_command` must point to the destination Postgres server
- `conninfo` must point to a database on the destination Postgres
  database.  Using PostgreSQL 9.2, 9.3, 9.4, and 9.5, `pgespresso`
  must be correctly installed through `CREATE EXTENSION`. Using 9.6 or
  greater, concurrent backups are executed through the Postgres native
  API.

The destination Postgres server can be either the master or a
streaming replicated standby server.

> **NOTE:**
> When backing up from a standby server, continuous archiving of WAL
> files must be configured on the master to ship files to the Barman
> server (as outlined in the _"WAL archiving via archive_command"_ section
> above)[^CONCURRENT_ARCHIVING].

[^CONCURRENT_ARCHIVING]:
  In case of a concurrent backup, currently Barman has no way
  to determine that the closing WAL file of a full backup has
  actually been shipped - opposite of an exclusive backup
  where PostgreSQL itself makes sure that the WAL file is correctly
  archived. Be aware that the full backup cannot be considered
  consistent until that WAL file has been received and archived by
  Barman.


## Archiving features
### WAL compression

The `barman cron` command will compress WAL files if the `compression`
option is set in the configuration file. This option allows five
values:

- `bzip2`: for Bzip2 compression (requires the `bzip2` utility)
- `gzip`: for Gzip compression (requires the `gzip` utility)
- `pybzip2`: for Bzip2 compression (uses Python's internal compression module)
- `pygzip`: for Gzip compression (uses Python's internal compression module)
- `pigz`: for Pigz compression (requires the `pigz` utility)
- `custom`: for custom compression, which requires you to set the
  following options as well:
      - `custom_compression_filter`: a compression filter
      - `custom_decompression_filter`: a decompression filter

> *NOTE:* All methods but `pybzip2` and `pygzip` require `barman
> archive-wal` to fork a new process.

### Synchronous WAL streaming

> **IMPORTANT:** This feature is available only from PostgreSQL 9.5
> and above.

Barman can also reduce the Recovery Point Objective to zero, by
collecting the transaction WAL files like a synchronous standby server
would.

To configure such a scenario, the Barman server must be configured to
archive WALs via the [streaming connection](#streaming_connection),
and the `receive-wal` process should figure as a synchronous standby
of the PostgreSQL server.

First of all, you need to retrieve the application name of the Barman
`receive-wal` process with the `show-server` command:

``` bash
barman@backup$ barman show-server pg|grep streaming_archiver_name
	streaming_archiver_name: barman_receive_wal
```

Then the application name should be added to the `postgresql.conf`
file as a synchronous standby:

``` ini
synchronous_standby_names = 'barman_receive_wal'
```

> **IMPORTANT:** this is only an example of configuration, to show you that
> Barman is eligible to be a synchronous standby node.
> We are not suggesting to use ONLY Barman.
> You can read _["Synchronous Replication"][synch]_ from the PostgreSQL
> documentation for further information on this topic.

The PostgreSQL server needs to be restarted for the configuration to
be reloaded.

If the server has been configured correctly, the `replication-status`
command should show the `receive_wal` process as a synchronous
streaming client:

``` bash
[root@backup ~]# barman replication-status pg
Status of streaming clients for server 'pg':
  Current xlog location on master: 0/9000098
  Number of streaming clients: 1

  1. #1 Sync WAL streamer
     Application name: barman_receive_wal
     Sync stage      : 3/3 Remote write
     Communication   : TCP/IP
     IP Address      : 139.59.135.32 / Port: 58262 / Host: -
     User name       : streaming_barman
     Current state   : streaming (sync)
     Replication slot: barman
     WAL sender PID  : 2501
     Started at      : 2016-09-16 10:33:01.725883+00:00
     Sent location   : 0/9000098 (diff: 0 B)
     Write location  : 0/9000098 (diff: 0 B)
     Flush location  : 0/9000098 (diff: 0 B)
```


## Catalog management features
### Minimum redundancy safety

You can define the minimum number of periodic backups for a PostgreSQL
server, using the global/per server configuration option called
`minimum_redundancy`, by default set to 0.

By setting this value to any number greater than 0, Barman makes sure
that at any time you will have at least that number of backups in a
server catalog.

This will protect you from accidental `barman delete` operations.

> **IMPORTANT:**
> Make sure that your retention policy settings do not collide with
> minimum redundancy requirements. Regularly check Barman's log for
> messages on this topic.


### Retention policies

Barman supports **retention policies** for backups.

A backup retention policy is a user-defined policy that determines how
long backups and related archive logs (Write Ahead Log segments) need
to be retained for recovery procedures.

Based on the user's request, Barman retains the periodic backups
required to satisfy the current retention policy and any archived WAL
files required for the complete recovery of those backups.

Barman users can define a retention policy in terms of **backup
redundancy** (how many periodic backups) or a **recovery window** (how
long).

Retention policy based on redundancy

  : In a redundancy based retention policy, the user determines how
    many periodic backups to keep. A redundancy-based retention policy
    is contrasted with retention policies that use a recovery window.

Retention policy based on recovery window

  : A recovery window is one type of Barman backup retention policy,
    in which the DBA specifies a period of time and Barman ensures
    retention of backups and/or archived WAL files required for
    point-in-time recovery to any time during the recovery window. The
    interval always ends with the current time and extends back in
    time for the number of days specified by the user. For example, if
    the retention policy is set for a recovery window of seven days,
    and the current time is 9:30 AM on Friday, Barman retains the
    backups required to allow point-in-time recovery back to 9:30 AM
    on the previous Friday.

#### Scope

Retention policies can be defined for:

- **PostgreSQL periodic base backups**: through the `retention_policy`
  configuration option
- **Archive logs**, for Point-In-Time-Recovery: through the
  `wal_retention_policy` configuration option

> **IMPORTANT:**
> In a temporal dimension, archive logs must be included in the time
> window of periodic backups.

There are two typical use cases here: full or partial point-in-time
recovery.

Full point in time recovery scenario:

  : Base backups and archive logs share the same retention policy,
    allowing you to recover at any point in time from the first
    available backup.

Partial point in time recovery scenario:

  : Base backup retention policy is wider than that of archive logs,
    for example allowing users to keep full, weekly backups of the
    last 6 months, but archive logs for the last 4 weeks (granting to
    recover at any point in time starting from the last 4 periodic
    weekly backups).

> **IMPORTANT:**
> Currently, Barman implements only the **full point in time
> recovery** scenario, by constraining the `wal_retention_policy`
> option to `main`.

#### How they work

Retention policies in Barman can be:

- **automated**: enforced by `barman cron`
- **manual**: Barman simply reports obsolete backups and allows you
  to delete them

> **IMPORTANT:**
> Currently Barman does not implement manual enforcement. This feature
> will be available in future versions.

#### Configuration and syntax

Retention policies can be defined through the following configuration
options:

- `retention_policy`: for base backup retention
- `wal_retention_policy`: for archive logs retention
- `retention_policy_mode`: can only be set to `auto` (retention
  policies are automatically enforced by the `barman cron` command)

These configuration options can be defined both at a global level and
a server level, allowing users maximum flexibility on a multi-server
environment.

##### Syntax for `retention_policy`

The general syntax for a base backup retention policy through
`retention_policy` is the following:

``` ini
retention_policy = {REDUNDANCY value | RECOVERY WINDOW OF value {DAYS | WEEKS | MONTHS}}
```

Where:

- syntax is case insensitive
- `value` is an integer and is > 0
- in case of **redundancy retention policy**:
      - `value` must be greater than or equal to the server minimum
        redundancy level (if that value is not assigned,
        a warning is generated)
      - the first valid backup is the value-th backup in a reverse
        ordered time series
- in case of **recovery window policy**:
      - the point of recoverability is: current time - window
      - the first valid backup is the first available backup before
        the point of recoverability; its value in a reverse ordered
        time series must be greater than or equal to the server
        minimum redundancy level (if it is not assigned to that value
        and a warning is generated)

By default, `retention_policy` is empty (no retention enforced).

##### Syntax for `wal_retention_policy`

Currently, the only allowed value for `wal_retention_policy` is the
special value `main`, that maps the retention policy of archive logs
to that of base backups.


## Hook scripts

Barman allows a database administrator to run hook scripts on these
two events:

- before and after a backup
- before and after the deletion of a backup
- before and after a WAL file is archived
- before and after a WAL file is deleted

There are two types of hook scripts that Barman can manage:

- standard hook scripts
- retry hook scripts

The only difference between these two types of hook scripts is that
Barman executes a standard hook script only once, without checking its
return code, whereas a retry hook script may be executed more than
once, depending on its return code.

Specifically, when executing a retry hook script, Barman checks the
return code and retries indefinitely until the script returns either
`SUCCESS` (with standard return code `0`), or `ABORT_CONTINUE` (return
code `62`), or `ABORT_STOP` (return code `63`). Barman treats any
other return code as a transient failure to be retried. Users are
given more power: a hook script can control its workflow by specifying
whether a failure is transient. Also, in case of a 'pre' hook script,
by returning `ABORT_STOP`, users can request Barman to interrupt the
main operation with a failure.

Hook scripts are executed in the following order:

1. The standard 'pre' hook script (if present)
2. The retry 'pre' hook script (if present)
3. The actual event (i.e. backup operation, or WAL archiving), if
   retry 'pre' hook script was not aborted with `ABORT_STOP`
4. The retry 'post' hook script (if present)
5. The standard 'post' hook script (if present)

The output generated by any hook script is written in the log file of
Barman.

> **NOTE:**
> Currently, `ABORT_STOP` is ignored by retry 'post' hook scripts. In
> these cases, apart from logging an additional warning, `ABORT_STOP`
> will behave like `ABORT_CONTINUE`.

### Backup scripts

These scripts can be configured with the following global
configuration options (which can be overridden on a per server basis):

- `pre_backup_script`: _hook script_ executed _before_ a base backup,
  only once, with no check on the exit code
- `pre_backup_retry_script`: _retry hook script_ executed _before_ a
  base backup, repeatedly until success or abort
- `post_backup_retry_script`: _retry hook script_ executed _after_ a
  base backup, repeatedly until success or abort
- `post_backup_script`: _hook script_ executed _after_ a base backup,
  only once, with no check on the exit code

The script definition is passed to a shell and can return any exit
code. Only in case of a _retry_ script, Barman checks the return code
(see the [hook script section](#hook_scripts)).

The shell environment will contain the following variables:

- `BARMAN_BACKUP_DIR`: backup destination directory
- `BARMAN_BACKUP_ID`: ID of the backup
- `BARMAN_CONFIGURATION`: configuration file used by Barman
- `BARMAN_ERROR`: error message, if any (only for the `post` phase)
- `BARMAN_PHASE`: phase of the script, either `pre` or `post`
- `BARMAN_PREVIOUS_ID`: ID of the previous backup (if present)
- `BARMAN_RETRY`: `1` if it is a retry script, `0` if not
- `BARMAN_SERVER`: name of the server
- `BARMAN_STATUS`: status of the backup
- `BARMAN_VERSION`: version of Barman

### Backup delete scripts

Version **2.4** introduces pre and post backup delete scripts.

As previous scripts, bakup delete scripts can be configured within global
configuration options, and it is possible to override them on a per server
basis:

- `pre_delete_script`: _hook script_ launched _before_ the deletion
  of a backup, only once, with no check on the exit code
- `pre_delete_retry_script`: _retry hook script_ executed _before_
  the deletion of a backup, repeatedly until success or abort
- `post_delete_retry_script`: _retry hook script_ executed _after_
  the deletion of a backup, repeatedly until success or abort
- `post_delete_script`: _hook script_ launched _after_ the deletion
  of a backup, only once, with no check on the exit code

The script is executed through a shell and can return any exit code.
Only in case of a _retry_ script, Barman checks the return code (see
the upper section).

Delete scripts uses the same environmental variables of a backup script,
plus:

- `BARMAN_NEXT_ID`: ID of the next backup (if present)
 
### WAL archive scripts

Similar to backup scripts, archive scripts can be configured with
global configuration options (which can be overridden on a per server
basis):

- `pre_archive_script`: _hook script_ executed _before_ a WAL file is
  archived by maintenance (usually `barman cron`), only once, with no
  check on the exit code
- `pre_archive_retry_script`: _retry hook script_ executed _before_ a
  WAL file is archived by maintenance (usually `barman cron`),
  repeatedly until it is successful or aborted
- `post_archive_retry_script`: _retry hook script_ executed _after_ a
  WAL file is archived by maintenance, repeatedly until it is
  successful or aborted
- `post_archive_script`: _hook script_ executed _after_ a WAL file is
  archived by maintenance, only once, with no check on the exit code

The script is executed through a shell and can return any exit code.
Only in case of a _retry_ script, Barman checks the return code (see
the upper section).

Archive scripts share with backup scripts some environmental
variables:

- `BARMAN_CONFIGURATION`: configuration file used by Barman
- `BARMAN_ERROR`: error message, if any (only for the `post` phase)
- `BARMAN_PHASE`: phase of the script, either `pre` or `post`
- `BARMAN_SERVER`: name of the server

Following variables are specific to archive scripts:

- `BARMAN_SEGMENT`: name of the WAL file
- `BARMAN_FILE`: full path of the WAL file
- `BARMAN_SIZE`: size of the WAL file
- `BARMAN_TIMESTAMP`: WAL file timestamp
- `BARMAN_COMPRESSION`: type of compression used for the WAL file

### WAL delete scripts

Version **2.4** introduces pre and post WAL delete scripts.

Similarly to the other hook scripts, wal delete scripts can be
configured with global configuration options, and is possible to
override them on a per server basis:

- `pre_wal_delete_script`: _hook script_ executed _before_
  the deletion of a WAL file
- `pre_wal_delete_retry_script`: _retry hook script_ executed _before_
  the deletion of a WAL file, repeatedly until it is successful
  or aborted
- `post_wal_delete_retry_script`: _retry hook script_ executed _after_
  the deletion of a WAL file, repeatedly until it is successful
  or aborted
- `post_wal_delete_script`: _hook script_ executed _after_
  the deletion of a WAL file

The script is executed through a shell and can return any exit code.
Only in case of a _retry_ script, Barman checks the return code (see
the upper section).

WAL delete scripts use the same environmental variables as WAL archive
scripts.

### Recovery scripts

Version **2.4** introduces pre and post recovery scripts.

As previous scripts, recovery scripts can be configured within global
configuration options, and is possible to override them on a per server
basis:

- `pre_recovery_script`: _hook script_ launched _before_ the recovery
  of a backup, only once, with no check on the exit code
- `pre_recovery_retry_script`: _retry hook script_ executed _before_
  the recovery of a backup, repeatedly until success or abort
- `post_recovery_retry_script`: _retry hook script_ executed _after_
  the recovery of a backup, repeatedly until success or abort
- `post_recovery_script`: _hook script_ launched _after_ the recovery
  of a backup, only once, with no check on the exit code

The script is executed through a shell and can return any exit code.
Only in case of a _retry_ script, Barman checks the return code (see
the upper section).

Recovery scripts uses the same environmental variables of a backup
script, plus:

- `BARMAN_DESTINATION_DIRECTORY`: the directory where the new instance
  is recovered

- `BARMAN_TABLESPACES`: tablespace relocation map (JSON, if present)

- `BARMAN_REMOTE_COMMAND`: secure shell command used
  by the recovery (if present)

- `BARMAN_RECOVER_OPTIONS`: recovery additional options (JSON, if present)

## Customization

### Lock file directory

Barman allows you to specify a directory for lock files through the
`barman_lock_directory` global option.

Lock files are used to coordinate concurrent work at global and server
level (for example, cron operations, backup operations, access to the
WAL archive, and so on.).

By default (for backward compatibility reasons),
`barman_lock_directory` is set to `barman_home`.

> **TIP:**
> Users are encouraged to use a directory in a volatile partition,
> such as the one dedicated to run-time variable data (e.g.
> `/var/run/barman`).

### Binary paths

As of version 1.6.0, Barman allows users to specify one or more directories
where Barman looks for executable files, using the global/server
option `path_prefix`.

If a `path_prefix` is provided, it must contain a list of one or more
directories separated by colon. Barman will search inside these directories
first, then in those specified by the `PATH` environment variable.

By default the `path_prefix` option is empty.

## Integration with cluster management systems

Barman has been designed for integration with standby servers (with
streaming replication or traditional file based log shipping) and high
availability tools like [repmgr][repmgr].

From an architectural point of view, PostgreSQL must be configured to
archive WAL files directly to the Barman server.
Barman, thanks to the `get-wal` framework, can also be used as a WAL hub.
For this purpose, you can use the `barman-wal-restore` script, part
of the `barman-cli` package, with all your standby servers.

The `replication-status` command allows
you to get information about any streaming client attached to the
managed server, in particular hot standby servers and WAL streamers.

## Parallel jobs

By default, Barman uses only one worker for file copy during both backup and
recover operations. Starting from version 2.2, it is possible to customize the
number of workers that will perform file copy. In this case, the
files to be copied will be equally distributed among all parallel workers.

It can be configured in global and server scopes, adding these in the
corresponding configuration file:

``` ini
parallel_jobs = n
```

where `n` is the desired number of parallel workers to be used in file copy
operations. The default value is 1.

In any case, users can override this value at run-time when executing
`backup` or `recover` commands. For example, you can use 4 parallel workers
as follows:

``` bash
barman backup --jobs 4 server1 
```

Or, alternatively:

``` bash
barman backup --j 4 server1 
```

Please note that this parallel jobs feature is only available for servers
configured through `rsync`/SSH. For servers configured through streaming
protocol, Barman will rely on `pg_basebackup` which is currently limited
to only one worker.

