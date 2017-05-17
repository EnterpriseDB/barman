## Backup with `rsync`/SSH

The backup over `rsync` was the only available method before 2.0, and
is currently the only backup method that supports the incremental
backup feature. Please consult the _"Features in detail"_ section for
more information.

To take a backup using `rsync` you need to put these parameters inside
the Barman server configuration file:

``` ini
backup_method = rsync
ssh_command = ssh postgres@pg
```

The `backup_method` option activates the `rsync` backup method, and
the `ssh_command` option is needed to correctly create an SSH
connection from the Barman server to the PostgreSQL server.

> **IMPORTANT:** Keep in mind that if the WAL archiving is not
> currently configured, you will not be able to start a backup.

To check if the server configuration is valid you can use the `barman
check` command:

``` bash
barman@backup$ barman check pg
```

To take a backup use the `barman backup` command:

``` bash
barman@backup$ barman backup pg
```

