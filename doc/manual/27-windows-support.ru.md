## How to setup a Windows based server

You can backup a PostgreSQL server running on Windows using the
streaming connection for both WAL archiving and for backups.

> **IMPORTANT:** This feature is still experimental because it is not
> yet part of our continuous integration system.

Follow every step discussed previously for a streaming connection
setup.

> **WARNING:**: At this moment, `pg_basebackup` interoperability from
> Windows to Linux is still experimental. If you are having issues
> taking a backup from a Windows server and your PostgreSQL locale is
> not in English, a possible workaround for the issue is instructing
> your PostgreSQL to emit messages in English. You can do this by
> putting the following parameter in your `postgresql.conf` file:
>
> ``` ini
> lc_messages = 'English'
> ```
>
> This has been reported to fix the issue.

You can backup your server as usual.

Remote recovery is not supported for Windows servers, so you must
recover your cluster locally in the Barman server and then copy all
the files on a Windows server or use a folder shared between the
PostgreSQL server and the Barman server.

Additionally, make sure that the system user chosen to run PostgreSQL
has the permission needed to access the restored data. Basically, it
must have full control over the PostgreSQL data directory.
