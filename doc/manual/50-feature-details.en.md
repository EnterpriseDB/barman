\newpage

# Features in detail

<!--

TODO: Review and rewrite everything in a suitable form for a reference guide, including ordering

-->

## Incremental backup

From version 1.4.0, Barman implements **file-level incremental
backup**. Incremental backup is a kind of full periodic backup which
saves only data changes from the latest full backup available in the
catalogue for a specific PostgreSQL server. It must not be confused
with differential backup, which is implemented by _WAL continuous
archiving_.

The main goals of incremental backup in Barman are:

- Reduce the time taken for the full backup process
- Reduce the disk space occupied by several periodic backups (**data
  deduplication**)

This feature heavily relies on `rsync` and [hard links] [8], which
must be therefore supported by both the underlying operating system
and the file system where the backup data resides.

The main concept is that a subsequent base backup will share those
files that have not changed since the previous backup, leading to
relevant savings in disk usage. This is particularly true of VLDB
contexts and, more in general, of those databases containing a high
percentage of _read-only historical tables_.

Barman implements incremental backup through a global/server option,
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

## WAL compression

The `barman cron` command (see below) will compress WAL files if the
`compression` option is set in the configuration file. This option
allows five values:

- `bzip2`: for Bzip2 compression (requires the `bzip2` utility)
- `gzip`: for Gzip compression (requires the `gzip` utility)
- `pybzip2`: for Bzip2 compression (uses Python's internal compression module)
- `pygzip`: for Gzip compression (uses Python's internal compression module)
- `pigz`: for Pigz compression (requires the `pigz` utility)
- `custom`: for custom compression, which requires you to set the
  following options as well:
      - `custom_compression_filter`: a compression filter
      - `custom_decompression_filter`: a decompression filter

> *NOTE:* The `pybzip2`, `pygzip` and `pigz` options for standard
> compression have been introduced in Barman 1.6.0. All methods but
> `pybzip2` and `pygzip` require `barman archive-wal` to fork a new
> process.

## Limiting bandwidth usage

From version 1.2.1, it is possible to limit the usage of I/O bandwidth
through the `bandwidth_limit` option (global/per server), by
specifying the maximum number of kilobytes per second. By default it
is set to 0, meaning no limit.

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

## Network Compression

From version 1.3.0 it is possible to reduce the size of transferred
data using compression. It can be enabled using the
`network_compression` option (global/per server):

``` ini
network_compression = true|false
```

Setting this option to `true` will enable data compression during
network transfers (for both backup and recovery). By default it is set
to `false`.

## Backup ID shortcuts

As of version 1.1.2, you can use any of the following **shortcuts** to
identify a particular backup for a given server:

- `latest`: the latest available backup for that server, in
  chronological order. You can also use the `last` synonym.
- `oldest`: the oldest available backup for that server, in
  chronological order. You can also use the `first` synonym.

These aliases can be used with any of the following commands:
`show-backup`, `delete`, `list-files` and `recover`.

## Minimum redundancy safety

From version 1.2.0, you can define the minimum number of periodic
backups for a PostgreSQL server.

You can use the global/per server configuration option called
`minimum_redundancy` for this purpose, by default set to 0.

By setting this value to any number greater than 0, Barman makes sure
that at any time you will have at least that number of backups in a
server catalogue.

This will protect you from accidental `barman delete` operations.

> **Important:**
> Make sure that your policy retention settings do not collide with
> minimum redundancy requirements. Regularly check Barman's log for
> messages on this topic.

## Retention policies

From version 1.2.0, Barman supports **retention policies** for
backups.

A backup retention policy is an user-defined policy that determines
how long backups and related archive logs (Write Ahead Log segments)
need to be retained for recovery procedures.

Based on the user's request, Barman retains the periodic backups
required to satisfy the current retention policy, and any archived WAL
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

### Scope

Retention policies can be defined for:

- **PostgreSQL periodic base backups**: through the `retention_policy`
  configuration option;
- **Archive logs**, for Point-In-Time-Recovery: through the
  `wal_retention_policy` configuration option.

> **Important:**
> In a temporal dimension, archive logs must be included in the time
> window of periodic backups.

There are two typical use cases here: full or partial point-in-time
recovery.

Full point in time recovery scenario

  : Base backups and archive logs share the same retention policy,
    allowing DBAs to recover at any point in time from the first
    available backup.

Partial point in time recovery scenario

  : Base backup retention policy is wider than that of archive logs,
    allowing users for example to keep full weekly backups of the last
    6 months, but archive logs for the last 4 weeks (granting to
    recover at any point in time starting from the last 4 periodic
    weekly backups).

> **Important:**
> Currently, Barman implements only the **full point in time
> recovery** scenario, by constraining the `wal_retention_policy`
> option to `main`.

### How they work

Retention policies in Barman can be:

- **automated**: enforced by `barman cron`;
- **manual**: Barman simply reports obsolete backups and allows DBAs
  to delete them.

> **Important:**
> Currently Barman does not implement manual enforcement. This feature
> will be available in future versions.

### Configuration and syntax

Retention policies can be defined through the following configuration
options:

- `retention_policy`: for base backup retention;
- `wal_retention_policy`: for archive logs retention;
- `retention_policy_mode`: can only be set to `auto` (retention
  policies are automatically enforced by the `barman cron` command).

These configuration options can be defined both at a global level and
a server level, allowing users maximum flexibility on a multi-server
environment.

#### Syntax for `retention_policy`

The general syntax for a base backup retention policy through
`retention_policy` is the following:

``` ini
retention_policy = {REDUNDANCY value | RECOVERY WINDOW OF value {DAYS | WEEKS | MONTHS}}
```

Where:

- syntax is case insensitive;
- `value` is an integer and is > 0;
- in case of **redundancy retention policy**:
      - `value` must be greater than or equal to the server minimum
        redundancy level (if not is is assigned to that value and a
        warning is generated);
      - the first valid backup is the value-th backup in a reverse
        ordered time series;
- in case of **recovery window policy**:
      - the point of recoverability is: current time - window;
      - the first valid backup is the first available backup before
        the point of recoverability; its value in a reverse ordered
        time series must be greater than or equal to the server
        minimum redundancy level (if not is is assigned to that value
        and a warning is generated).

By default, `retention_policy` is empty (no retention enforced).

#### Syntax for `wal_retention_policy`

Currently, the only allowed value for `wal_retention_policy` is the
special value `main`, that maps the retention policy of archive logs
to that of base backups.

## Concurrent Backup and backup from a standby

Normally, during backup operations, Barman uses PostgreSQL native
functions `pg_start_backup` and `pg_stop_backup` for _exclusive
backup_. These operations are not allowed on a read-only standby
server.

As of version 1.3.1, Barman is also capable of performing backups of
PostgreSQL from 9.2 or greater database servers in a **concurrent way**,
primarily through the `backup_options` configuration
parameter.[^ABOUT_CONCURRENT_BACKUP]

[^ABOUT_CONCURRENT_BACKUP]:
  Concurrent backup is a technology that has been available in
  PostgreSQL since version 9.1, through the _streaming replication
  protocol_ (using, for example, a tool like `pg_basebackup`).

This introduces a new architecture scenario with Barman: **backup from
a standby server**, using `rsync`.

> **Important:**
> **Concurrent backup** requires users of PostgreSQL 9.2, 9.3, 9.4,
> and 9.5 to install the `pgespresso` open source extension
> on every PostgreSQL server of the cluster. For more detailed information
> and the source code, please visit the [pgespresso extension website] [9].
> As of version 2.0, Barman adds support to the new API introduced in
> PostgreSQL 9.6. This removes the requirement of the `pgespresso`
> extension to perform concurrent backups altogether.

By default, `backup_options` is transparently set to
`exclusive_backup` (the only supported method by any Barman version
prior to 1.3.1).

When `backup_options` is set to `concurrent_backup`, Barman activates
the _concurrent backup mode_ for a server and follows these two simple
rules:

- `ssh_command` must point to the destination Postgres server;
- `conninfo` must point to a database on the destination Postgres database.
  Using PostgreSQL 9.2, 9.3, 9.4, and 9.5 `pgespresso` must be correctly
  installed through `CREATE EXTENSION`. Using 9.6 or greater, concurrent
  backups are executed through the Postgres native API.

The destination Postgres server can be either the master or a
streaming replicated standby server.

> **Note:**
> When backing up from a standby server, continuous archiving of WAL
> files must be configured on the master to ship files to the Barman
> server (as outlined in the "Continuous WAL archiving" section
> above)[^CONCURRENT_ARCHIVING].

[^CONCURRENT_ARCHIVING]:
  In case of concurrent backup, currently Barman does not have a way
  to determine that the closing WAL file of a full backup has actually
  been shipped - opposite to the case of an exclusive backup where it
  is Postgres itself that makes sure that the WAL file is correctly
  archived. Be aware that the full backup cannot be considered
  consistent until that WAL file has been received and archived by
  Barman. We encourage Barman users to wait to delete the previous
  backup - at least until that moment.

## Hook scripts

Barman allows a database administrator to run _hook scripts_ on these
two events:

- before and after a backup
- before and after a WAL file is archived

There are two types of hook scripts that Barman can manage:

- standard hook scripts (already present in Barman since version
  1.1.0)
- retry hook scripts, introduced in version 1.5.0

The only difference between these two types of hook scripts is that
Barman executes a standard hook script only once, without checking its
return code, whereas a retry hook script may be executed more than
once depending on its return code.

Precisely, when executing a retry hook script, Barman checks the
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

> **Note:**
> Currently, `ABORT_STOP` is ignored by retry 'post' hook scripts. In
> these cases, apart from lodging an additional warning, `ABORT_STOP`
> will behave like `ABORT_CONTINUE`.

### Backup scripts

Version 1.1.0 introduced backup scripts.

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
(see the upper section).

The shell environment will contain the following variables:

- `BARMAN_BACKUP_DIR`: backup destination directory
- `BARMAN_BACKUP_ID`: ID of the backup
- `BARMAN_CONFIGURATION`: configuration file used by barman
- `BARMAN_ERROR`: error message, if any (only for the `post` phase)
- `BARMAN_PHASE`: phase of the script, either `pre` or `post`
- `BARMAN_PREVIOUS_ID`: ID of the previous backup (if present)
- `BARMAN_RETRY`: `1` if it is a retry script (from 1.5.0), `0` if not
- `BARMAN_SERVER`: name of the server
- `BARMAN_STATUS`: status of the backup
- `BARMAN_VERSION`: version of Barman (from 1.2.1)

### WAL archive scripts

Version 1.3.0 introduced WAL archive hook scripts.

Similarly to backup scripts, archive scripts can be configured with
global configuration options (which can be overridden on a per server
basis):

- `pre_archive_script`: _hook script_ executed _before_ a WAL file is
  archived by maintenance (usually `barman cron`), only once, with no
  check on the exit code
- `pre_archive_retry_script`: _retry hook script_ executed _before_ a
  WAL file is archived by maintenance (usually `barman cron`),
  repeatedly until success or abort
- `post_archive_retry_script`: _retry hook script_ executed _after_ a
  WAL file is archived by maintenance, repeatedly until success or
  abort
- `post_archive_script`: _hook script_ executed _after_ a WAL file is
  archived by maintenance, only once, with no check on the exit code

The script is executed through a shell and can return any exit code.
Only in case of a _retry_ script, Barman checks the return code (see
the upper section).

Archive scripts share with backup scripts some environmental
variables:

- `BARMAN_CONFIGURATION`: configuration file used by barman
- `BARMAN_ERROR`: error message, if any (only for the `post` phase)
- `BARMAN_PHASE`: phase of the script, either `pre` or `post`
- `BARMAN_SERVER`: name of the server

Following variables are specific to archive scripts:

- `BARMAN_SEGMENT`: name of the WAL file
- `BARMAN_FILE`: full path of the WAL file
- `BARMAN_SIZE`: size of the WAL file
- `BARMAN_TIMESTAMP`: WAL file timestamp
- `BARMAN_COMPRESSION`: type of compression used for the WAL file


## Customisation of lock file directory

Since version 1.5.0, Barman allows DBAs to specify a directory for
lock files through the `barman_lock_directory` global option.

Lock files are used to coordinate concurrent work at global and server
level (for example, cron operations, backup operations, access to the
WAL archive, etc.).

By default (for backward compatibility reasons),
`barman_lock_directory` is set to `barman_home`.

> **Important:**
> This change won't affect users upgrading from a version of Barman
> older than 1.5.0, unless you have written applications that depend
> on the names of the lock files. However, this is not a typical and
> common case for Barman and most of users do not fall into this
> category.

> **Tip:**
> Users are encouraged to use a directory in a volatile partition,
> such as the one dedicated to run-time variable data (e.g.
> `/var/run/barman`).

## Customisation of binary paths

As of version 1.6.0, Barman allows users to specify one or more directories
where Barman looks for executable files, using the global/server
option `path_prefix`.

If a `path_prefix` is provided, it must contain a list of one or more
directories separated by colon. Barman will search inside these directories
first, then in those specified by the `PATH` environment variable.

By default the `path_prefix` option is empty.

## Integration with standby servers

Barman has been designed for integration with standby servers (with
streaming replication or traditional file based log shipping) and high
availability tools like [repmgr] [repmgr].

From an architectural point of view, PostgreSQL must be configured to
archive WAL files directly to the Barman server.

Version 1.6.1 introduces the `replication-status` command which allows
users to get information about any streaming client attached to the
managed server, in particular hot standby servers and WAL streamers.

## Synchronous WAL streaming

TODO - Explain how to get RPO=0
