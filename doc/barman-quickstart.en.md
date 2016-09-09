% Quickstart
% 2ndQuadrant Italia
% September 9, 2016 (v2.0a1)

> **IMPORTANT:** This quickstart guide needs to be rewritten
> for Barman 2.0 before the first stable release is out.
> The main goal is to guide a user from start to execute
> a backup and a recovery.

<!-- Previous content

# Choose a backup method

The first important decision you are called to make is which backup method
to choose for your PostgreSQL server.

For versions prior to PostgreSQL 9.1 you can only use `rsync` over Ssh.
For PostgreSQL 9.1 or higher, starting from Barman 2.0, you have an
alternative: rely on PostgreSQL streaming replication only, by using
`pg_basebackup` as backup application.

This decision will have an impact at architecture, security and operations
level, bringing along a set of different requirements and actions to take.

Mechanically, this choice depends on the value that you assign to a
global/server configuration option called `backup_method`. Currently,
it only accepts two possible values:

* `rsync`: instruct Barman to use `rsync` over Ssh for taking base backups,
  thus requiring a Ssh connection with the PostgreSQL server;
* `postgres`: use `pg_basebackup` to take base backups, thus requiring a
  valid streaming connection to the PostgreSQL server.

Below is a list of requirements and steps to follow for each case.

## Backup with rsync over Ssh

**TODO**: Move to the manual

In case your backup method is `rsync`, you need to allow the `barman` user
on the `backup` server to connect via Ssh as the `postgres` user on the
PostgreSQL server. Ssh must be configured so that no password prompt
is presented when connecting.

As the `barman` user on the `backup` server, generate an Ssh key with
an empty password, and append the public key to the `authorized_keys`
file of the `postgres` user on the `pg` server.

The `barman` user on the `backup` server should then be able to
perform the following operation without typing a password:

``` bash
barman@backup$ ssh postgres@pg
```

For further information, refer to OpenSSH documentation.

## Backup through PostgreSQL streaming replication

In case your backup method is `postgres`, the following requirements apply:

- PostgreSQL database server:
    - version 9.3 or higher, **if you have tablespaces**
    - version 9.1 or higher, if you do not have tablespaces
- `pg_basebackup` installed on the Barman server:
    - `pg_basebackup` version 9.2 is required when connecting to
      PostgreSQL servers 9.1 and 9.2
    - the latest version of `pg_basebackup` is recommended when connecting to
      PostgreSQL servers 9.3 or higher (as it is back-compatible down to 9.3);
      **if you have tablespaces**, `pg_basebackup` version 9.4 or higher is
      required

> **Note:**
> In case you manage different versions of PostgreSQL, you can install
> different versions of `pg_basebackup`, then set the `path_prefix`
> specifically for each server to point to the proper version.

The `postgres` backup method requires to set up a streaming connection.
See the section "Setup the Streaming connection" below for further information.

Current limitations of the `postgres` method are:

* tablespaces support is available only for PostgreSQL servers 9.3 or
  higher using `pg_basebackup` 9.4 or higher
* `bandwidth_limit` is available with `pg_basebackup` >= 9.4 only
* `network_compression` is not supported
* `reuse_backup` is not supported
* `tablespace_bandwidth_limit` is not supported
* `retry-times` will erase the destination folder in subsequent attempts 

# Choose an archiving method

As of Barman 2.0, you can choose any of the following scenarios
for continuous WAL archiving from the master:

1. **traditional WAL file archiving**, also known as _log shipping_, implemented
   through PostgreSQL's `archive_command` from the master
2. **WAL streaming-only archiving**, through streaming replication protocol
   and replication slots (PostgreSQL 9.4 or higher is required) from the master
3. **hybrid WAL archiving** from the master (both traditional and streaming
   archiving, at the same time, for maximum robustness and reliability).
   Replication slots are not needed (PostgreSQL 9.2 or higher is required)

Once you have made your decision, depending also on the available PostgreSQL
versions, you can continue with setting up your preferred method (or methods in
case of hybrid archiving).

See the sections below for detailed information:

- "Setup traditional continuous WAL archiving"
- 

# Setup the PostgreSQL connection

You need to make sure that the `backup` server can connect to
the PostgreSQL server on `pg` as superuser (e.g. `postgres`).
This connection is required by Barman in order to coordinate its
activities with the server, as well as for monitoring purposes.

You can choose your favourite client authentication method among those
offered by PostgreSQL. More information can be found in the
[PostgreSQL Documentation] [6].

``` bash
barman@backup$ psql -c 'SELECT version()' -U postgres -h pg
```

> **Note:**
> As of version 1.1.2, Barman honours the `application_name`
> connection option for PostgreSQL servers 9.0 or higher.

# Setup the Streaming connection

As of version 1.6.0 Barman enables the connection to a PostgreSQL
server using its native [streaming replication protocol] [22].
In order to set up a streaming connection, you need to:

- Properly configure PostgreSQL to accept streaming replication
  connections from the Barman server. We encourage users to
  read the PostgreSQL documentation, in particular:

    - [Role attributes] [23]
    - [The pg_hba.conf file] [23]
    - [Setting up standby servers using streaming replication] [24]
- Set the `streaming_conninfo` parameter in the Barman server configuration
  accordingly.

  > **IMPORTANT**: Setting up streaming replication is not a task
  > that is strictly related to Barman configuration. Please refer
  > to PostgreSQL documentation, mailing lists, and books for this activity.

The `streaming_conninfo` option can be used by both the backup process
(when `backup_method=postgres`) and the streaming archiver process
(when `streaming_archiver=on`).

# Choose the main backup directory

Barman needs a main backup directory to store all the backups. Even
though you can define a separate folder for each server you want to
back up and for each type of resource (backup or WAL segments, for
instance), we suggest that you adhere to the default rules and stick
with the conventions that Barman chooses for you.

You will see that the configuration file (as explained below) defines
a `barman_home` variable, which is the directory where Barman will
store all your backups by default. We choose `/var/lib/barman` as home
directory for Barman:

``` bash
barman@backup$ sudo mkdir /var/lib/barman
barman@backup$ sudo chown barman:barman /var/lib/barman
```

> **Important:**
> We assume that you have enough space, and that you have already
> thought about redundancy and safety of your disks.

# Write a basic configuration

In the `docs` directory you will find a minimal configuration file.
Use it as a template, and copy it to `/etc/barman.conf`, or to
`~/.barman.conf`. In general, the former applies to all the users on
the `backup` server, while the latter applies only to the `barman`
user; for the purpose of this tutorial there is no difference in using
one or the other.

From version 1.2.1, you can use `/etc/barman/barman.conf` as default
system configuration file.

The configuration file follows the standard INI format, and is split
in:

- a section for general configuration (identified by the `barman`
  label)
- a section for each PostgreSQL server to be backed up (identified by
  the server label, e.g. `main` or `pg`)[^RESERVED_SECTIONS]

[^RESERVED_SECTIONS]:
  `all` and `barman` are reserved words and cannot be used as server
  labels.

## Global/server options

Every option in the configuration file has a _scope_:

- global
- server
- global/server

Global options can be present in the _general section_ (identified by
`barman`). Server options can only be specified in a _server section_.

Some options can be defined at global level and overridden at server
level, allowing users to specify a generic behaviour and refine it for
one or more servers. For a list of all the available configurations
and their scope, please refer to [section 5 of the man page] [7].

``` bash
man 5 barman
```

## Configuration files directory

As of version 1.1.2, you can now specify a directory for configuration
files similarly to other Linux applications, using the
`configuration_files_directory` option (empty by default). If the
value of `configuration_files_directory` is a directory, Barman will
read all the files with `.conf` extension that exist in that folder.
For example, if you set it to `/etc/barman.d`, you can specify your
PostgreSQL servers placing each section in a separate `.conf` file
inside the `/etc/barman.d` folder.

Otherwise, you can use Barman's standard way of specifying sections
within the main configuration file.

> **Tip:**
> This is the recommended way to configure servers in Barman.

## Example of configuration

Here follows a basic example of PostgreSQL configuration:

``` ini
[barman]
barman_home = /var/lib/barman
barman_user = barman
log_file = /var/log/barman/barman.log
compression = gzip
reuse_backup = link
minimum_redundancy = 1

[main]
description = "Main PostgreSQL Database"
ssh_command = ssh postgres@pg
conninfo = host=pg user=postgres
```

For more detailed information, please refer to the distributed
`barman.conf` file.

## Initial checks

Once you have created your configuration file (or files), you can now
test Barman's configuration by executing:

``` bash
barman@backup$ barman show-server main
barman@backup$ barman check main
```

Write down the `incoming_wals_directory`, as printed by the `barman
show-server main` command, because you will need it to setup
continuous WAL archiving.

> **Important:**
> The `barman check main` command automatically creates all the
> directories for the continuous backup of the `main` server.

> ** VERY IMPORTANT:**
> If the check for the WAL archive fails and you receive the hint
> 'please make sure WAL shipping is setup', rightly do so and set
> continuous WAL archiving and WAL streaming (see next sections).

## Setup traditional continuous WAL archiving

In case you want to setup the traditional WAL file archiving process,
Barman requires that PostgreSQL's `archive_command` is properly
configured on the master.

It also require that the `postgres` user on the `pg` server
can connect to the `backup` server as the `barman` user without typing
a password.

As the `postgres` user on the `pg` server, generate an Ssh key with
an empty password, and append the public key to the `authorized_keys`
file of the `barman` user on the `backup` server.

Then, manually verify that this works:

``` bash
postgres@pg$ ssh barman@backup
```

Edit the `postgresql.conf` file of the PostgreSQL instance on the `pg`
database and activate the archive mode:

``` ini
wal_level = 'archive' # For PostgreSQL >= 9.0
archive_mode = on
archive_command = 'rsync -a %p barman@backup:INCOMING_WALS_DIRECTORY/%f'
```

Make sure you change the `INCOMING_WALS_DIRECTORY` placeholder with
the value returned by the `barman show-server main` command above.

In case you use Hot Standby, `wal_level` must be set to `hot_standby`
or, for PostgreSQL 9.6 or higher, `replica`.

Restart the PostgreSQL server.

In order to test that continuous archiving is on and properly working,
you need to check both the PostgreSQL server and the `backup` server
(in particular, that WAL files are correctly collected in the
destination directory).

> **Warning:**
> It is currently a requirement that WAL files from PostgreSQL are
> shipped to the Barman server. Without `archive_command` being
> properly set in PostgreSQL to send WAL files to Barman, **full
> backups cannot be taken**.

> **Important:**
> PostgreSQL 9.5 introduces support for WAL file archiving using
> `archive_command` from a standby. This feature is not yet implemented
> in Barman.

From version 1.6.1, in order to improve the verification of the WAL
archiving process, the `switch-xlog` command has been developed:

``` bash
barman@backup$ barman switch-xlog main
```

## Setup WAL streaming

**TODO: Change for 2.0**

From version 1.6.0, Barman improves its Recovery Point Objective (RPO)
performance by allowing users to add, on top of the standard `archive_command`
strategy, continuous WAL streaming from a PostgreSQL server.

Barman relies on [`pg_receivexlog`] [25], a utility that is available
from PostgreSQL 9.2 which exploits the native streaming replication protocol
and continuously receives transaction logs from a PostgreSQL
server (be it a master or a standby).

> **Important:**
> Barman requires that `pg_receivexlog` is installed in the same server.
> For PostgreSQL 9.2 servers, you need `pg_receivexlog` of version 9.2
> installed alongside with Barman. For PostgreSQL 9.3 and above, it is
> recommended to install the latest available version of `pg_receivexlog`,
> as it is back compatible.
> Otherwise, users can install multiple versions of `pg_receivexlog` in the
> Barman server and properly point to the specific version for a server,
> using the `path` option in the configuration file.

In order to enable streaming of transaction logs, you need to:

1. setup a streaming connection, as previously described;
2. set the `streaming_archiver` option to `on`.

The `cron` command, if the aforementioned requirements are met,
transparently manages log streaming through the execution of the
`receive-wal` command. This is the recommended scenario.

However, users can manually execute the `receive-wal` command:

``` bash
barman receive-wal <server_name>
```

> **Note:**
> The `receive-wal` command is a foreground process.

Transaction logs are streamed directly in the directory specified by the
`streaming_wals_directory` configuration option and are then archived
by the `archive-wal` command.

Unless otherwise specified in the `streaming_archiver_name` parameter,
and only for PostgreSQL 9.3 or above, Barman will set `application_name`
of the WAL streamer process to `barman_receive_wal`, allowing you to
monitor its status in the `pg_stat_replication` system view of the
PostgreSQL server.

#### Stopping a receive-wal process for a server

If a `receive-wal` process is running in background (e.g. started by
the cron command), it is possible to ask barman to stop it by invoking
the `receive-wal` command with the `--stop` option:

``` bash
barman receive-wal --stop <server_name>
```
#### Reset location of receive-wal

In some cases, mainly due to the current lack of support for replication
slots in Barman, it may be necessary to reset the location of the streaming
WAL archiver (e.g.: a prolonged interruption of the `receive-wal` process
might cause Barman to go out of sync with the master).

You can reset the location using `--reset` option of the `receive-wal`
command, as follows:

``` bash
barman receive-wal --reset <server_name>
```

> **Note:**
> The `--reset` option requires that no `receive-wal` is running.

# Listing the servers

The following command displays the list of all the available servers:

``` bash
barman@backup$ barman list-server
```

# Executing a full backup

To take a backup for the `main` server, issue the following command:

``` bash
barman@backup$ barman backup main
```

As of version 1.1.0, you can serialise the backup of your managed
servers by using the `all` target for the server:

``` bash
barman@backup$ barman backup all
```

This will iterate through your available servers and sequentially take
a backup for each of them.

## Implicit restore point

**TODO**: Move to the manual

As of version 1.5.1, at the end of a successful backup Barman
automatically creates a restore point that can be used jointly
with `--target-name` during recovery.

By default, the restore point name uses the following convention:
`barman_<backup_id>`.

Barman internally uses the PostgreSQL function called `pg_create_restore_point`:
for further information, please refer to the
[PostgreSQL documentation on system administration functions] [20].

> **Important:**
> This feature is only available for PostgreSQL 9.1 or above.

## Immediate Checkpoint

**TODO**: Move to the manual

As of version 1.3.0, it is possible to use the `immediate_checkpoint`
configuration global/server option (set to `false` by default).

Before starting a backup, Barman requests a checkpoint, which
generates additional workload. Normally that checkpoint is throttled
according to the settings for workload control on the PostgreSQL
server, which means that the backup could be delayed.

If `immediate_checkpoint` is set to `true`, PostgreSQL will not try to
limit the workload, and the checkpoint will happen at maximum speed,
starting the backup as soon as possible.

At any time, you can override the configuration option behaviour, by
issuing `barman backup` with any of these two options:

- `--immediate-checkpoint`, which forces an immediate checkpoint;
- `--no-immediate-checkpoint`, which forces to wait for the checkpoint
  to happen.

# Viewing the list of backups for a server

To list all the available backups for a given server, issue:

``` bash
barman@backup$ barman list-backup main
```

the format of the output is as in:

``` bash
main - 20120529T092136 - Wed May 30 15:20:25 2012 - Size: 5.0 TiB
 - WAL Size: 845.0 GiB (tablespaces: a:/disk1/a, t:/disk2/t)
```

where `20120529T092136` is the ID of the backup and `Wed May 30
15:20:25 2012` is the start time of the operation, `Size` is the size
of the base backup and `WAL Size` is the size of the archived WAL files.

As of version 1.1.2, you can get a listing of the available backups
for all your servers, using the `all` target for the server:

``` bash
barman@backup$ barman list-backup all
```

# Restoring a whole server

To restore a whole server issue the following command:

``` bash
barman@backup$ barman recover main 20110920T185953 /path/to/recover/dir
```

where `20110920T185953` is the ID of the backup to be restored. When
this command completes successfully, `/path/to/recover/dir`
contains a complete data directory ready to be started as a PostgreSQL
database server.

Here is an example of a command that starts the server:

``` bash
barman@backup$ pg_ctl -D /path/to/recover/dir start
```

> **Important:**
> If you run this command as user `barman`, it will become the
> database superuser.

You can retrieve a list of backup IDs for a specific server with:

``` bash
barman@backup$ barman list-backup srvpgsql
```

> **Important:**
> Barman does not currently keep track of symbolic links inside PGDATA
> (except for tablespaces inside `pg_tblspc`). We encourage system
> administrators to keep track of symbolic links and to add them to the
> disaster recovery plans/procedures in case they need to be restored
> in their original location.

## Remote recovery

Barman is able to recover a backup on a remote server through the
`--remote-ssh-command COMMAND` option for the `recover` command.

If this option is specified, barman uses `COMMAND` to connect to a
remote host.

Here is an example of a command that starts recovery on a remote server:

``` bash
barman@backup$ barman recover --remote-ssh-command="ssh user@remotehost" \
main 20110920T185953 /path/to/recover/dir
```

> **Note:**
> The `postgres` user is normally used to recover on a remote host.

There are some limitations when using remote recovery. It is important
to be aware that:

- unless `get-wal` is specified in the `recovery_options` (available
  from version 1.5.0), Barman requires at least 4GB of free space in
  the system's temporary directory (usually `/tmp`);
- the Ssh connection between Barman and the remote host **must** use
  public key exchange authentication method;
- the remote user must be able to create the required destination
  directories for `PGDATA` and, where applicable, tablespaces;
- there must be enough free space on the remote server to contain the
  base backup and the WAL files needed for recovery.

## Relocating one or more tablespaces

**TODO**: Move to the manual

> **Important:**
> As of version 1.3.0, it is possible to relocate a tablespace both
> with local and remote recovery.

Barman is able to automatically relocate one or more tablespaces using
the `recover` command with the `--tablespace` option. The option
accepts a pair of values as arguments using the `NAME:DIRECTORY`
format:

- name/identifier of the tablespace (`NAME`);
- destination directory (`DIRECTORY`).

If the destination directory does not exists, Barman will try to
create it (assuming you have enough privileges).

## Restoring to a given point in time

**TODO**: Move to the manual

Barman employs PostgreSQL's Point-in-Time Recovery (PITR) by allowing
DBAs to specify a recovery target, either as a timestamp or as a
transaction ID; you can also specify whether the recovery target
should be included or not in the recovery.

The recovery target can be specified using one of three mutually
exclusive options:

- `--target-time TARGET_TIME`: to specify a timestamp
- `--target-xid TARGET_XID`: to specify a transaction ID
- `--target-name TARGET_NAME`: to specify a named restore point -
  previously created with the `pg_create_restore_point(name)`
  function[^POSTGRESQL0901]

[^POSTGRESQL0901]: Only available on PostgreSQL 9.1 and above.

You can use the `--exclusive` option to specify whether to stop
immediately before or immediately after the recovery target.

Barman allows you to specify a target timeline for recovery, using the
`target-tli` option. The notion of timeline goes beyond the scope of
this document; you can find more details in the PostgreSQL
documentation, or in one of 2ndQuadrant's Recovery training courses.

## Limitations of partial WAL files with recovery

**TODO**: Move to the manual

Version 1.6.0 introduces support for WAL streaming, by integrating PostgreSQL's
`pg_receivexlog` utility with Barman. The standard behaviour of `pg_receivexlog`
is to write transactional information in a file with `.partial` suffix after
the WAL segment name.

Barman expects a partial file to be in the `streaming_wals_directory` of
a server. When completed, `pg_receivexlog` removes the `.partial` suffix
and opens the following one, delivering the file to the `archive-wal` command
of Barman for permanent storage and compression.

In case of a sudden and unrecoverable failure of the master PostgreSQL server,
the `.partial` file that has been streamed to Barman contains very important
information that the standard archiver (through PostgreSQL's `archive_command`)
has not been able to deliver to Barman.

> **Important:**
> A current limitation of Barman is that the `recover` command is not yet able
> to transparently manage `.partial` files. In such situations, users will need
> to manually copy the latest partial file from the server's
> `streaming_wals_directory` of their Barman installation to the destination
> for recovery, making sure that the `.partial` suffix is removed.
> Restoring a server using the last partial file, reduces data loss, by bringing
> down _recovery point objective_ to values around 0.

# Retry of copy in backup/recovery operations

**TODO**: Move to the manual

As of version 1.3.3, it is possible to take advantage of two new
options in Barman:

- `basebackup_retry_times` (set to 0 by default)
- `basebackup_retry_sleep` (set to 30 by default)

When issuing a backup or a recovery, Barman normally tries to copy the
base backup once. If the copy fails (e.g. due to network problems),
Barman terminates the operation with a failure.

By setting `basebackup_retry_times`, Barman will try to re-execute a
copy operation as many times as requested by the user. The
`basebackup_retry_sleep` option specifies the number of seconds that
Barman will wait between each attempt.

At any time you can override the configuration option behaviour from
the command line, when issuing `barman backup` or `barman recover`,
using:

- `--retry-times <retry_number>` (same logic as
  `basebackup_retry_times`)
- `--no-retry` (same as `--retry-times 0`)
- `--retry-sleep <number_of_seconds>` (same logic as
  `basebackup_retry_sleep`)


# Troubleshooting

**TODO**

# License and Contributions

Barman is the exclusive property of 2ndQuadrant Italia and its code is
distributed under GNU General Public License 3.

Copyright (C) 2011-2016 [2ndQuadrant.it S.r.l.] [19].

Barman has been partially funded through [4CaaSt] [18], a research
project funded by the European Commission's Seventh Framework
programme.

Contributions to Barman are welcome, and will be listed in the
`AUTHORS` file. 2ndQuadrant Italia requires that any contributions
provide a copyright assignment and a disclaimer of any work-for-hire
ownership claims from the employer of the developer. This lets us make
sure that all of the Barman distribution remains free code. Please
contact info@2ndQuadrant.it for a copy of the relevant Copyright
Assignment Form.

-->



<!-- Reference links -->

  [1]: http://fedoraproject.org/wiki/EPEL
  [2]: http://yum.postgresql.org/
  [3]: https://sourceforge.net/projects/pgbarman/files/
  [4]: http://apt.postgresql.org/
  [5]: https://wiki.postgresql.org/wiki/Apt
  [6]: http://www.postgresql.org/docs/current/static/client-authentication.html
  [7]: http://docs.pgbarman.org/barman.5.html
  [8]: http://en.wikipedia.org/wiki/Hard_link
  [9]: https://github.com/2ndquadrant-it/pgespresso
  [10]: http://www.repmgr.org/
  [11]: http://www.pgbarman.org/
  [12]: http://www.pgbarman.org/support/
  [13]: http://www.2ndquadrant.com/
  [14]: http://www.pgbarman.org/faq/
  [15]: http://blog.2ndquadrant.com/tag/barman/
  [16]: https://github.com/hamann/check-barman
  [17]: https://github.com/2ndquadrant-it/puppet-barman
  [18]: http://4caast.morfeo-project.org/
  [19]: http://www.2ndquadrant.it/
  [20]: http://www.postgresql.org/docs/current/static/functions-admin.html
  [21]: http://www.postgresql.org/docs/current/static/auth-pg-hba-conf.html
  [22]: http://www.postgresql.org/docs/current/static/protocol-replication.html
  [23]: http://www.postgresql.org/docs/current/static/role-attributes.html
  [24]: http://www.postgresql.org/docs/current/static/warm-standby.html#STREAMING-REPLICATION
  [25]: http://www.postgresql.org/docs/current/static/app-pgreceivexlog.html
  [26]: https://goo.gl/218Ghl
  [27]: https://github.com/emin100/barmanapi
  [28]: https://www.postgresql.org/docs/current/static/backup-file.html
  [29]: https://www.postgresql.org/docs/current/static/continuous-archiving.html
  [30]: http://www.2ndquadrant.com/en/books/postgresql-9-administration-cookbook/
  [31]: http://www.postgresql.org/
