## The server configuration file

Create a new file, called `pg.conf`, in `/etc/barman.d` directory, with the following content:

``` ini
[pg]
active = false
description =  "Our main PostgreSQL server"
conninfo = host=pg user=barman database=postgres
```

The `active = false` line temporarily disables this server during maintenance operations triggered by the `barman cron` command, allowing you to continue with the configuration.

The `conninfo` option is set accordingly to the section _"Preliminary steps: PostgreSQL connection"_.

<!-- TODO

- Execute barman check to create dirs?
- Execute barman show-server to get information about incoming directory for WAL archiving

-->