% BARMAN(1) Barman User manuals | Version 1.6.1
% 2ndQuadrant Italy <http://www.2ndQuadrant.it>
% May 23, 2016

# NAME

barman - Backup and Recovery Manager for PostgreSQL


# SYNOPSIS

barman [*OPTIONS*] *COMMAND*

# DESCRIPTION

barman is an administration tool for disaster recovery of PostgreSQL
servers written in Python. barman can perform remote backups of multiple
servers in business critical environments and helps DBAs during the
recovery phase.

# OPTIONS

-v, --version
:   Show program version number and exit.

-q, --quiet
:   Do not output anything. Useful for cron scripts.

-h, --help
:   Show a help message and exit.

-c *CONFIG*, --config *CONFIG*
:   Use the specified configuration file.


# COMMANDS

Important: every command has a help option

archive-wal *SERVER_NAME*
:   Get any incoming xlog file (both through standard `archive_command`
    and streaming replication, where applicable) and moves them in the
    WAL archive for that server. If necessary, apply compression when
    requested by the user.

cron
:   Perform maintenance tasks, such as enforcing retention policies or
    WAL files management.

list-server
:   Show all the configured servers, and their descriptions.

show-server *SERVER_NAME*
:   Show information about `SERVER_NAME`, including: `conninfo`,
    `backup_directory`, `wals_directory` and many more.
    Specify `all` as `SERVER_NAME` to show information about all
    the configured servers.

status *SERVER_NAME*
:   Show information about the status of a server, including: number of
    available backups, `archive_command`, `archive_status` and many more.
    For example:

```
Server quagmire:
  Description: The Giggity database
  Passive node: False
  PostgreSQL version: 9.3.9
  pgespresso extension: Not available
  PostgreSQL Data directory: /srv/postgresql/9.3/data
  PostgreSQL 'archive_command' setting: rsync -a %p barman@backup:/var/lib/barman/quagmire/incoming
  Last archived WAL: 0000000100003103000000AD
  Current WAL segment: 0000000100003103000000AE
  Retention policies: enforced (mode: auto, retention: REDUNDANCY 2, WAL retention: MAIN)
  No. of available backups: 2
  First available backup: 20150908T003001
  Last available backup: 20150909T003001
  Minimum redundancy requirements: satisfied (2/1)
```

check *SERVER_NAME*
:   Show diagnostic information about `SERVER_NAME`, including:
    ssh connection check, PostgreSQL version, configuration and backup
    directories. Specify `all` as `SERVER_NAME` to show diagnostic information
    about all the configured servers.

    --nagios
    :    Nagios plugin compatible output

diagnose
:   Collect diagnostic information about the server where barman is installed
    and all the configured servers, including: global configuration, SSH version,
    Python version, `rsync` version, as well as current configuration and status
    of all servers.

backup *SERVER_NAME*
:   Perform a backup of `SERVER_NAME` using parameters specified in the
    configuration file. Specify `all` as `SERVER_NAME` to perform a backup
    of all the configured servers.

    --immediate-checkpoint
    :   forces the initial checkpoint to be done as quickly as possible.
        Overrides value of the parameter `immediate_checkpoint`, if present
        in the configuration file.

    --no-immediate-checkpoint
    :   forces to wait for the checkpoint.
        Overrides value of the parameter `immediate_checkpoint`, if present
        in the configuration file.

    --reuse-backup [INCREMENTAL_TYPE]
    :   Overrides `reuse_backup` option behaviour. Possible values for
        `INCREMENTAL_TYPE` are:

        - *off*: do not reuse the last available backup;
        - *copy*: reuse the last available backup for a server and
           create a copy of the unchanged files (reduce backup time);
        - *link*: reuse the last available backup for a server and
           create a hard link of the unchanged files (reduce backup time
           and space);

        `link` is the default target if `--reuse-backup` is used and
        `INCREMENTAL_TYPE` is not explicited.

    --retry-times
    :   Number of retries of base backup copy, after an error.
        Used during both backup and recovery operations.
        Overrides value of the parameter `basebackup_retry_times`,
        if present in the configuration file.

    --no-retry
    :   Same as `--retry-times 0`

    --retry-sleep
    :   Number of seconds of wait after a failed copy, before retrying.
        Used during both backup and recovery operations.
        Overrides value of the parameter `basebackup_retry_sleep`,
        if present in the configuration file.

list-backup *SERVER_NAME*
:   Show available backups for `SERVER_NAME`. This command is useful to
    retrieve a backup ID. For example:

```
servername 20111104T102647 - Fri Nov  4 10:26:48 2011 - Size: 17.0 MiB - WAL Size: 100 B
```

    In this case, *20111104T102647* is the backup ID.

show-backup *SERVER_NAME* *BACKUP_ID*
:   Show detailed information about a particular backup, identified by
    the server name and the backup ID. See the [Backup ID shortcuts](#shortcuts)
    section below for available shortcuts. For example:

```
Backup 20150828T130001:
  Server Name            : quagmire
  Status                 : DONE
  PostgreSQL Version     : 90402
  PGDATA directory       : /srv/postgresql/9.4/main/data

  Base backup information:
    Disk usage           : 12.4 TiB (12.4 TiB with WALs)
    Incremental size     : 4.9 TiB (-60.02%)
    Timeline             : 1
    Begin WAL            : 0000000100000CFD000000AD
    End WAL              : 0000000100000D0D00000008
    WAL number           : 3932
    WAL compression ratio: 79.51%
    Begin time           : 2015-08-28 13:00:01.633925+00:00
    End time             : 2015-08-29 10:27:06.522846+00:00
    Begin Offset         : 1575048
    End Offset           : 13853016
    Begin XLOG           : CFD/AD180888
    End XLOG             : D0D/8D36158

  WAL information:
    No of files          : 35039
    Disk usage           : 121.5 GiB
    WAL rate             : 275.50/hour
    Compression ratio    : 77.81%
    Last available       : 0000000100000D95000000E7

  Catalog information:
    Retention Policy     : not enforced
    Previous Backup      : 20150821T130001
    Next Backup          : - (this is the latest base backup)
```

list-files *\[OPTIONS\]* *SERVER_NAME* *BACKUP_ID*
:   List all the files in a particular backup, identified by the server
    name and the backup ID. See the [Backup ID shortcuts](#shortcuts) section below
    for available shortcuts.

    --target *TARGET_TYPE*
    :    Possible values for TARGET_TYPE are:

         - *data*: lists just the data files;
         - *standalone*: lists the base backup files, including required
           WAL files;
         - *wal*: lists all the WAL files between the start of the base
           backup and the end of the log / the start of the following base
           backup (depending on whether the specified base backup is the most
           recent one available);
         - *full*: same as data + wal.

		The default value is `standalone`.

rebuild-xlogdb *SERVER_NAME*
:   Perform a rebuild of the WAL file metadata for `SERVER_NAME`
    (or every server, using the `all` shortcut) guessing it from
    the disk content. The metadata of the WAL archive is contained
    in the `xlog.db` file, and every Barman server has its own copy.

recover *\[OPTIONS\]* *SERVER_NAME* *BACKUP_ID* *DESTINATION_DIRECTORY*
:   Recover a backup in a given directory (local or remote, depending
    on the `--remote-ssh-command` option settings).
    See the [Backup ID shortcuts](#shortcuts) section below for available shortcuts.

    --target-tli *TARGET_TLI*
    :   Recover the specified timeline.

    --target-time *TARGET_TIME*
    :   Recover to the specified time.

        You can use any valid unambiguous representation
        (e.g: "YYYY-MM-DD HH:MM:SS.mmm").

    --target-xid *TARGET_XID*
    :   Recover to the specified transaction ID.

    --target-name *TARGET_NAME*
    :   Recover to the named restore point previously created with
        the `pg_create_restore_point(name)` (for PostgreSQL 9.1 and above users).

    --exclusive
    :   Set target xid to be non inclusive.

    --tablespace *NAME:LOCATION*
    :   Specify tablespace relocation rule.

    --remote-ssh-command *SSH_COMMAND*
    :   This options activates remote recovery, by specifying the
        secure shell command to be launched on a remote host.
        This is the equivalent of the "ssh_command" server option
        in the configuration file for remote recovery.
        Example: 'ssh postgres@db2'.

    --retry-times *RETRY_TIMES*
    :   Number of retries of data copy during base backup after
        an error. Overrides value of the parameter `basebackup_retry_times`,
        if present in the configuration file.

    --no-retry
    :   Same as `--retry-times 0`

    --retry-sleep
    :   Number of seconds of wait after a failed copy, before retrying.
        Overrides value of the parameter `basebackup_retry_sleep`,
        if present in the configuration file.

get-wal *\[OPTIONS\]* *SERVER_NAME* *WAL_ID*
:   Retrieve a WAL file from the `xlog` archive of a given server.
    By default, the requested WAL file, if found, is returned as
    uncompressed content to `STDOUT`. The following options allow
    users to change this behaviour:

    -o *OUTPUT_DIRECTORY*
    :   destination directory where the `get-wal` will deposit the requested WAL

    -z
    :   output will be compressed using gzip

    -j
    :   output will be compressed using bzip2

    -p *SIZE*
    :   peek from the WAL archive up to *SIZE* WAL files, starting
        from the requested one. 'SIZE' must be an integer >= 1.
        When invoked with this option, get-wal returns a
        list of zero to 'SIZE' WAL segment names, one per row.

switch-xlog *SERVER_NAME*
:   Execute pg_switch_xlog() on the target server

    --force
    :   Forces the switch by executing CHECKPOINT before pg_switch_xlog().
        *IMPORTANT:* executing a CHECKPOINT might increase I/O load on
        a PostgreSQL server. Use this option with care.

receive-wal *SERVER_NAME*
:   Start the stream of transaction logs for a server.
    The process relies on `pg_receivexlog` to receive WAL files
    from the PostgreSQL servers through the streaming protocol.

    --stop
    :   stop the receive-wal process for the server

    --reset
    :   reset the status of receive-wal, restarting the streaming
        from the current WAL file of the server

delete *SERVER_NAME* *BACKUP_ID*
:   Delete the specified backup. [Backup ID shortcuts](#shortcuts)
    section below for available shortcuts.

replication-status *\[OPTIONS\]* *SERVER_NAME*
:   Shows live information and status of any streaming client attached
    to the given server (or servers). Default behaviour can be changed
    through the following options:

    --minimal
    :   machine readable output (default: False)

    --target *TARGET_TYPE*
    :    Possible values for TARGET_TYPE are:

         - *hot-standby*: lists only hot standby servers
         - *wal-streamer*: lists only WAL streaming clients, such as
                          pg_receivexlog
         - *all*: any streaming client (default)

# BACKUP ID SHORTCUTS {#shortcuts}

Rather than using the timestamp backup ID, you can use any of the
following shortcuts/aliases to identity a backup for a given server:

first
:   Oldest available backup for that server, in chronological order.

last
:   Latest available backup for that server, in chronological order.

latest
:   same ast *last*.

oldest
:   same ast *first*.


# EXIT STATUS

0
:   Success

Not zero
:   Failure


# SEE ALSO

`barman` (5).

# BUGS

Barman has been extensively tested, and is currently being used in several
production environments. However, we cannot exclude the presence of bugs.

Any bug can be reported via the Sourceforge bug tracker. Along the bug submission,
users can provide developers with diagnostics information obtained through
the `barman diagnose` command.

# AUTHORS

In alphabetical order:

- Gabriele Bartolini <gabriele.bartolini@2ndquadrant.it> (project leader)
- Stefano Bianucci <stefano.bianucci@2ndquadrant.it> (developer)
- Giuseppe Broccolo <giuseppe.broccolo@2ndquadrant.it> (QA/testing)
- Giulio Calacoci <giulio.calacoci@2ndquadrant.it> (developer)
- Francesco Canovai <francesco.canovai@2ndquadrant.it> (QA/testing)
- Leonardo Cecchi <leonardo.cecchi@2ndquadrant.it> (developer)
- Gianni Ciolli <gianni.ciolli@2ndquadrant.it> (QA/testing)
- Marco Nenciarini <marco.nenciarini@2ndquadrant.it> (lead developer)

Past contributors:

* Carlo Ascani

# RESOURCES

* Homepage: <http://www.pgbarman.org/>
* Documentation: <http://docs.pgbarman.org/>

# COPYING

Barman is the exclusive property of 2ndQuadrant Italia
and its code is distributed under GNU General Public License v3.

Copyright (C) 2011-2016 2ndQuadrant Italia Srl - <http://www.2ndQuadrant.it/>.
