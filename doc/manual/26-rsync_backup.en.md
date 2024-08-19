## Backup with `rsync`/SSH

The backup over `rsync` was the only method for backups in Barman before
version 2.0, and before 3.11 it was the only method that supported incremental
backups. Current Barman supports file-level as well as block-level incremental backups.
Backups using `rsync` implements the file-level backup feature. Please consult the
_"Features in detail"_ section for more information.

To take a backup using `rsync` you need to put these parameters inside
the Barman server configuration file:

``` ini
backup_method = rsync
ssh_command = ssh postgres@pg
```

The `backup_method` option activates the `rsync` backup method, and
the `ssh_command` option is needed to correctly create an SSH
connection from the Barman server to the PostgreSQL server.

> **IMPORTANT:** You will not be able to start a backup if WAL is not
> being correctly archived to Barman, either through the `archiver` or
> the `streaming_archiver`

To check if the server configuration is valid you can use the `barman
check` command:

``` bash
barman@backup$ barman check pg
```

To take a backup use the `barman backup` command:

``` bash
barman@backup$ barman backup pg
```

> **NOTE:**
> Starting with Barman 3.11.0, Barman uses a keep-alive mechanism when taking
> rsync-based backups. It keeps sending a simple `SELECT 1` query over the
> libpq connection where Barman runs `pg_backup_start`/`pg_backup_stop`
> low-level API functions, and it's in place to reduce the probability of a firewall or
> a router dropping that connection as it can be idle for a long time while the base
> backup is being copied. You can control the interval of the hearbeats, or even
> disable the mechanism, through the `keepalive_interval` configuration option.
