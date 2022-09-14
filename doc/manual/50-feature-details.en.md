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

> **IMPORTANT:** the `bandwidth_limit` option is supported with the
> `postgres` backup method for Postgres 9.4 and above, but the
> `tablespace_bandwidth_limit` option is available only if you use
> `rsync`

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


### Backup Compression
Barman can use the compression features of pg_basebackup in order to
compress the backup data during the backup process. This can be enabled
using the `backup_compression` config option (global/per server):

> **IMPORTANT:** the `backup_compression` and other options discussed
> in this section are not available with the `rsync` or `local-rsync`
> backup methods. Only with `postgres` backup method.

#### Compression algorithms 
Setting this option will cause pg_basebackup to compress the backup
using the specified compression algorithm. Currently, supported 
algorithm in Barman are: `gzip` `lz4` and `zstd`.

``` ini
backup_compression = gzip|lz4|zstd
```

Barman requires the CLI utility for the selected compression algorithm
to be available on both the Barman server *and* the PostgreSQL server.
The CLI utility is used to extract the backup label from the compressed
backup and to decompress the backup on the PostgreSQL server during
recovery. These can be installed through system packages named `gzip`,
`lz4` and `zstd` on Debian, Ubuntu, RedHat, CentOS and SLES systems.

> **Note:** On Ubuntu 18.04 (bionic) the `lz4` utility is available in
> the `liblz4-tool` pacakge.

> **Note:** `zstd` version must be 1.4.4 or higher. The system packages
> for `zstd` on Debian 10 (buster), Ubuntu 18.04 (bionic) and SLES 12
> install an earlier version - `backup_compression = zstd` will not
> work with these packages.

> **Note:** `lz4` and `zstd` are only available with PostgreSQL version
> 15 or higher.

> **IMPORTANT:** If you are using `backup_compression` you must also
> set `recovery_staging_path` so that `barman recover` is able to
> recover the compressed backups. See the [Recovering compressed backups](#recovering-compressed-backups)
> section for more information.

#### Compression workers
This optional parameter allows compression using multiple threads to increase compression speed (default being 0). 

```ini
backup_compression_workers = 2
```

> **Note:** This option is only available with `zstd` compression.
 
> **Note:** `zstd` version must be 1.5.0 or higher. Or 1.4.4 or higher compiled with multithreading option.

#### Compression level
The compression level can be specified using the
`backup_compression_level` option. This should be set to an integer
value supported by the compression algorithm specified in
`backup_compression`.

#### Compression location
When using Barman with PostgreSQL version 15 or higher it is possible
to specify for compression to happen on the server (i.e. PostgreSQL
will compress the backup) or on the client (i.e. pg_basebackup
will compress the backup). This can be achieved using the
`backup_compression_location` option:

> **IMPORTANT:** the `backup_compression_location` option is only
> available when running against PostgreSQL 15 or later.

``` ini
backup_compression_location = server|client
```

Using `backup_compression_location = server` should reduce the network
bandwidth required by the backup at the cost of moving the compression
work onto the PostgreSQL server.

When `backup_compression_location` is set to `server` then an
additional option, `backup_compression_format`, can be set to `plain`
in order to have pg_basebackup uncompress the data before writing it
to disk:

#### Compression format
``` ini
backup_compression_format = plain|tar
```

If `backup_compression_format` is unset or has the value `tar` then
the backup will be written to disk as compressed tarballs. A description
of both the `plain` and `tar` formats can be found in the [pg_basebackup
documentation][pg_basebackup-documentation].
   
> **IMPORTANT:** Barman uses external tools to manage compressed backups.
> Depending on the `backup_compression` and `backup_compression_format`
> You may need to install one or more tools on the Postgres server and 
> the Barman server.
> The following table will help you choose according to your configuration.

| **backup_compression** | **backup_compression_format** | **Postgres server** | **Barman server** | 
|:---------:|:---------------------:|:-------------------------:|:----------------------:|
| gzip | plain | **tar** | None | 
| gzip |  tar  | **tar** | **tar** | 
| lz4 | plain | **tar, lz4** | None | 
| lz4 |  tar  | **tar, lz4** | **tar, lz4** |
| zstd | plain | **tar, zstd** | None | 
| zstd |  tar  | **tar, zstd** | **tar, zstd** |

### Concurrent backup

Normally, during backup operations, Barman uses PostgreSQL native
functions `pg_start_backup` and `pg_stop_backup` for _concurrent
backup_.[^ABOUT_CONCURRENT_BACKUP] This is the recommended way of
taking backups for PostgreSQL 9.6 and above (though note the
functions have been renamed to `pg_backup_start` and `pg_backup_stop`
in the PostgreSQL 15 beta).

[^ABOUT_CONCURRENT_BACKUP]:
  Concurrent backup is a technology that has been available in
  PostgreSQL since version 9.2, through the _streaming replication
  protocol_ (for example, using a tool like `pg_basebackup`).

As well as being the recommended backup approach, concurrent backup
also allows the following architecture scenario with Barman: **backup from
a standby server**, using `rsync`.

By default, `backup_options` is set to `concurrent_backup`. If exclusive
backup is required for PostgreSQL servers older than version 15 then users
should set `backup_options` to `exclusive_backup`.

When `backup_options` is set to `concurrent_backup`, Barman activates
the _concurrent backup mode_ for a server and follows these two simple
rules:

- `ssh_command` must point to the destination Postgres server
- `conninfo` must point to a database on the destination Postgres
  database.

> **IMPORTANT:** In case of a concurrent backup, currently Barman
> cannot determine whether the closing WAL file of a full backup has
> actually been shipped - opposite of an exclusive backup
> where PostgreSQL itself makes sure that the WAL file is correctly
> archived. Be aware that the full backup cannot be considered
> consistent until that WAL file has been received and archived by
> Barman. Barman 2.5 introduces a new state, called `WAITING_FOR_WALS`,
> which is managed by the `check-backup` command (part of the
> ordinary maintenance job performed by the `cron` command).
> From Barman 2.10, you can use the `--wait` option with `barman backup`
> command.

### Concurrent backup of a standby

If backing up a standby then the following [configuration options][config-options]
should point to the standby server:

- `conninfo`
- `streaming_conninfo` (when using `backup_method = postgres` or `streaming_archiver = on`)
- `ssh_command` (when using `backup_method = rsync`)

The following config option should point to the primary server:

- `primary_conninfo`

Barman will use `primary_conninfo` to switch to a new WAL on the primary
so that the concurrent backup against the standby can complete without
having to wait for a WAL switch to occur naturally.

> **NOTE:** It is especially important that `primary_conninfo` is
> set if the standby is to be backed up when there is little or no write
> traffic on the primary. If `primary_conninfo` is not set then the
> backup will still run however it will wait at the stop backup stage
> until the current WAL semgent on the primary is newer than the latest
> WAL required by the backup.

Barman currently requires that WAL files and backup data come from the
same PostgreSQL server. In the case that the standby is promoted to primary
the backups and WALs will continue to be valid however you may wish to update
the Barman configuration so that it uses the new standby for taking backups and
receiving WALs.

WALs can be obtained from the standby using either WAL streaming or WAL
archiving. To use WAL streaming follow the instructions in the
[WAL streaming](#wal-streaming) section.

To use WAL archiving from the standby follow the instructions in the [WAL archiving via archive_command](#wal-archiving-via-archive_command) section
*and additionally* set `archive_mode = always` in the PostgreSQL config on
the standby server.

> **NOTE:** With PostgreSQL 10 and earlier Barman cannot handle WAL streaming
> and WAL archiving being enabled at the same time on a standby. You must therefore
> disable WAL archiving if using WAL streaming and vice versa. This is because it is
> possible for WALs produced by PostgreSQL 10 and earlier to be logically equivalent
> but differ at the binary level, causing Barman to fail to detect that two WALs are
> identical.

### Immediate checkpoint

Before starting a backup, Barman requests a checkpoint, which
generates additional workload. Normally that checkpoint is throttled
according to the settings for workload control on the PostgreSQL
server, which means that the backup could be delayed.

This default behaviour can be changed through the `immediate_checkpoint`
configuration global/server option (set to `false` by default).

If `immediate_checkpoint` is set to `true`, PostgreSQL will not try to
limit the workload, and the checkpoint will happen at maximum speed,
starting the backup as soon as possible.

At any time, you can override the configuration option behaviour, by
issuing `barman backup` with any of these two options:

- `--immediate-checkpoint`, which forces an immediate checkpoint;
- `--no-immediate-checkpoint`, which forces to wait for the checkpoint
  to happen.

### Local backup

> **DISCLAIMER:** This feature is not recommended for production usage,
> as Barman and PostgreSQL reside on the same server and are part of
> the same single point of failure.
> Some EnterpriseDB customers have requested to add support for
> local backup to Barman to be used under specific circumstances
> and, most importantly, under the 24/7 production service delivered
> by the company. Using this feature currently requires installation
> from sources, or to customise the environment for the `postgres`
> user in terms of permissions as well as logging and cron configurations.

Under special circumstances, Barman can be installed on the same
server where the PostgreSQL instance resides, with backup data stored
on a separate volume from PGDATA and, where applicable, tablespaces.
Usually, these volumes reside on network storage appliances, with
filesystems like NFS.

This architecture is not endorsed by EnterpriseDB.
For an enhanced business continuity experience of PostgreSQL, with better
results in terms of RPO and RTO, EnterpriseDB still recommends the
shared nothing architecture with a remote installation of Barman, capable
of acting like a witness server for replication and monitoring purposes.

The only requirement for local backup is that Barman runs with the
same user as the PostgreSQL server, which is normally `postgres`.
Given that the Community packages by default install Barman under the `barman`
user, this use case requires manual installation procedures that include:

- cron configurations
- log configurations, including logrotate

In order to use local backup for a given server in Barman, you need to
set `backup_method` to `local-rsync`. The feature is essentially identical
to its `rsync` equivalent, which relies on SSH instead and operates remotely.
With `local-rsync` file system copy is performed issuing `rsync` commands locally
(for this reason it is required that Barman runs with the same user as PostgreSQL).

An excerpt of configuration for local backup for a server named `local-pg13` is:

```ini
[local-pg13]
description = "Local PostgreSQL 13"
backup_method = local-rsync
...
```

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
      - `custom_compression_magic`: a hex string to identify a custom compressed wal file

> *NOTE:* All methods but `pybzip2` and `pygzip` require `barman
> archive-wal` to fork a new process.

### Synchronous WAL streaming

> **IMPORTANT:** This feature is available only from PostgreSQL 9.5
> and above.

Barman can also reduce the Recovery Point Objective to zero, by
collecting the transaction WAL files like a synchronous standby server
would.

To configure such a scenario, the Barman server must be configured to
archive WALs via the [streaming connection](#postgresql-streaming-connection),
and the `receive-wal` process should figure as a synchronous standby
of the PostgreSQL server.

First of all, you need to retrieve the application name of the Barman
`receive-wal` process with the `show-servers` command:

``` bash
barman@backup$ barman show-servers pg|grep streaming_archiver_name
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

As previous scripts, backup delete scripts can be configured within global
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

## Geographical redundancy

It is possible to set up **cascading backup architectures** with Barman,
where the source of a backup server
is a Barman installation rather than a PostgreSQL server.

This feature allows users to transparently keep _geographically distributed_
copies of PostgreSQL backups.

In Barman jargon, a backup server that is connected to a Barman installation
rather than a PostgreSQL server is defined **passive node**.
A passive node is configured through the `primary_ssh_command` option, available
both at global (for a full replica of a primary Barman installation) and server
level (for mixed scenarios, having both _direct_ and _passive_ servers).

### Sync information

The `barman sync-info` command is used to collect information regarding the
current status of a Barman server that is useful for synchronisation purposes.
The available syntax is the following:

``` bash
barman sync-info [--primary] <server_name> [<last_wal> [<last_position>]]
```

The command returns a JSON object containing:

- A map with all the backups having status `DONE` for that server
- A list with all the archived WAL files
- The configuration for the server
- The last read position (in the _xlog database file_)
- the name of the last read WAL file

The JSON response contains all the required information for the synchronisation
between the `master` and a `passive` node.

If `--primary` is specified, the command is executed on the defined
primary node, rather than locally.

### Configuration

Configuring a server as `passive node` is a quick operation.
Simply add to the server configuration the following option:

``` ini
primary_ssh_command = ssh barman@primary_barman
```

This option specifies the SSH connection parameters to the primary server,
identifying the source of the backup data for the passive server.

If you are invoking barman with the `-c/--config` option and you want to use
the same option when the passive node invokes barman on the primary node then
add the following option:

``` ini
forward_config_path = true
```

### Node synchronisation

When a node is marked as `passive` it is treated in a special way by Barman:

- it is excluded from standard maintenance operations
- direct operations to PostgreSQL are forbidden, including `barman backup`

Synchronisation between a passive server and its primary is automatically
managed by `barman cron` which will transparently invoke:

1. `barman sync-info --primary`, in order to collect synchronisation information
2. `barman sync-backup`, in order to create a local copy of every backup that is available on the primary node
3. `barman sync-wals`, in order to copy locally all the WAL files available on the primary node

### Manual synchronisation

Although `barman cron` automatically manages passive/primary node
synchronisation, it is possible to manually trigger synchronisation
of a backup through:

``` bash
barman sync-backup <server_name> <backup_id>
```

Launching `sync-backup` barman will use the primary_ssh_command to connect to the master server, then
if the backup is present on the remote machine, will begin to copy all the files using rsync.
Only one single synchronisation process per backup is allowed.

WAL files also can be synchronised, through:

``` bash
barman sync-wals <server_name>
```
