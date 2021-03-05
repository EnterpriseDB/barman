## The server configuration file

Create a new file, called `pg.conf`, in `/etc/barman.d` directory, with the following content:

``` ini
[pg]
description =  "Our main PostgreSQL server"
conninfo = host=pg user=barman dbname=postgres
backup_method = postgres
# backup_method = rsync
```

The `conninfo` option is set accordingly to the section _"Preliminary
steps: PostgreSQL connection"_.

The meaning of the `backup_method` option will be covered in the
backup section of this guide.

If you plan to use the streaming connection for WAL archiving or to
create a backup of your server, you also need a `streaming_conninfo`
parameter in your server configuration file:

``` ini
streaming_conninfo = host=pg user=streaming_barman dbname=postgres
```

This value must be choosen accordingly as described in the section
_"Preliminary steps: PostgreSQL connection"_.
