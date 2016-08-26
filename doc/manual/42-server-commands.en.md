\newpage

# Server commands

**TODO:**

- Define server commands
- do an updated inventory of server commands
- order commands alphabetically
- Remove all 'From version ...' paragraph. Assume that we start from scratch with 2.0

## `backup`

You can perform a full backup (base backup) for a given server with:

``` bash
barman backup [--immediate-checkpoint] <server_name>
```

> **Tip:**
> You can use `barman backup all` to sequentially backup all your
> configured servers.


## `check`

You can check if the connection to a given server is properly working
with:

``` bash
barman check <server_name>
```

> **Tip:**
> You can use `barman check all` to check all your configured servers.

From version 1.3.3, you can automatically be notified if the latest
backup of a given server is older than, for example, _7
days_.[^SMELLY_BACKUP]

[^SMELLY_BACKUP]:
  This feature is commonly known among the development team members as
  _smelly backup check_.

Barman introduces the option named `last_backup_maximum_age` having
the following syntax:

``` ini
last_backup_maximum_age = {value {DAYS | WEEKS | MONTHS}}
```

where `value` is a positive integer representing the number of days,
weeks or months of the time frame.

## `get-wal`

From version 1.5.0, Barman allows users to request any _xlog_ file
from its WAL archive through the `get-wal` command:

``` bash
barman get-wal [-o OUTPUT_DIRECTORY] [-j|-x] <server_name> <wal_id>
```

If the requested WAL file is found in the server archive, the
uncompressed content will be returned to `STDOUT`, unless otherwise
specified.

The following options are available for the `get-wal` command:

- `-o` allows users to specify a destination directory where Barman
  will deposit the requested WAL file
- `-j` will compress the output using `bzip2` algorithm
- `-x` will compress the output using `gzip` algorithm
- `-p SIZE` peeks from the archive up to WAL files, starting from
  the requested one.

It is possible to use `get-wal` during a recovery operation,
transforming the Barman server in a _WAL hub_ for your servers. This
can be automatically achieved by adding the `get-wal` value to the
`recovery_options` global/server configuration option:

``` ini
recovery_options = 'get-wal'
```

TODO: Rewrite this with barman-wal-restore

`recovery_options` is a global/server option that accepts a list of
comma separated values. If the keyword `get-wal` is present, during a
recovery operation Barman will prepare the `recovery.conf` file by
setting the `restore_command` so that `barman get-wal` is used to
fetch the required WAL files.

This is an example of a `restore_command` for a remote recovery:

``` ini
restore_command = 'ssh barman@pgbackup barman get-wal SERVER %f > %p'
```

This is an example of a `restore_command` for a local recovery:

``` ini
restore_command = 'barman get-wal SERVER %f > %p'
```

> **Important:**
> Even though `recovery_options` aims to automate the process, using
> the `get-wal` facility requires manual intervention and proper
> testing.

## `list-backup`

You can list the catalogue of available backups for a given server
with:

``` bash
barman list-backup <server_name>
```

## `rebuild-xlogdb`

At any time, you can regenerate the content of the WAL archive for a
specific server (or every server, using the `all` shortcut). The WAL
archive is contained in the `xlog.db` file, and every Barman server
has its own copy. From version 1.2.4 you can now rebuild the `xlog.db`
file with the `rebuild-xlogdb` command. This will scan all the
archived WAL files and regenerate the metadata for the archive.

> **Important:**
> Users of Barman < 1.2.3 might have suffered from a bug due to bad
> locking in highly concurrent environments. You can now regenerate
> the WAL archive using the `rebuild-xlogdb` command.

``` bash
barman rebuild-xlogdb <server_name>
```

## `show-server`

You can show the configuration parameters for a given server with:

``` bash
barman show-server <server_name>
```
