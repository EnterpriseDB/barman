## Preliminary steps

This section contains some preliminary steps that you need to
undertake before setting up your PostgreSQL server in Barman.

> **IMPORTANT:**
> Before you proceed, it is important that you have made your decision
> in terms of WAL archiving and backup strategies, as outlined in the
> _"Design and architecture"_ section. In particular, you should
> decide which WAL archiving methods to use, as well as the backup
> method.

### PostgreSQL connection

You need to make sure that the `backup` server can connect to
the PostgreSQL server on `pg` as superuser. This operation is mandatory.

We recommend creating a specific user in PostgreSQL, named `barman`,
as follows:

``` bash
postgres@pg$ createuser -s -P barman
```

> **IMPORTANT:** The above command will prompt for a password,
> which you are then advised to add to the `~barman/.pgpass` file
> on the `backup` server. For further information, please refer to
> ["The Password File" section in the PostgreSQL Documentation][pgpass].

This connection is required by Barman in order to coordinate its
activities with the server, as well as for monitoring purposes.

You can choose your favourite client authentication method among those
offered by PostgreSQL. More information can be found in the
["Client Authentication" section of the PostgreSQL Documentation][pghba].

Make sure you test the following command before proceeding:

``` bash
barman@backup$ psql -c 'SELECT version()' -U barman -h pg postgres
```

Write down the above information (user name, host name and database
name) and keep it for later.  You will need it with in the `conninfo`
option for your server configuration, like in this example:

``` ini
[pg]
; ...
conninfo = host=pg user=barman dbname=postgres
```

> **NOTE:** Barman honours the `application_name` connection option
> for PostgreSQL servers 9.0 or higher.


### PostgreSQL WAL archiving and replication

Before you proceed, you need to properly configure PostgreSQL on `pg`
to accept streaming replication connections from the Barman
server. Please read the following sections in the PostgreSQL
documentation:

- [Role attributes][roles]
- [The pg_hba.conf file][authpghba]
- [Setting up standby servers using streaming replication][streamprot]


One configuration parameter that is crucially important is the
`wal_level` parameter. This parameter must be configured to ensure
that all the useful information necessary for a backup to be coherent
are included in the transaction log file.

``` ini
wal_level = 'replica'
```

For PostgreSQL versions older than 9.6, `wal_level` must be set to
`hot_standby`.

Restart the PostgreSQL server for the configuration to be refreshed.


### PostgreSQL streaming connection

If you plan to use WAL streaming or streaming backup, you need to
setup a streaming connection. We recommend creating a specific user in
PostgreSQL, named `streaming_barman`, as follows:

``` bash
postgres@pg$ createuser -P --replication streaming_barman
```

> **IMPORTANT:** The above command will prompt for a password,
> which you are then advised to add to the `~barman/.pgpass` file
> on the `backup` server. For further information, please refer to
> ["The Password File" section in the PostgreSQL Documentation][pgpass].

You can manually verify that the streaming connection works through
the following command:

``` bash
barman@backup$ psql -U streaming_barman -h pg \
  -c "IDENTIFY_SYSTEM" \
  replication=1
```

> **IMPORTANT:**
> Please make sure you are able to connect via streaming replication
> before going any further.

You also need to configure the `max_wal_senders` parameter in the
PostgreSQL configuration file. The number of WAL senders depends
on the PostgreSQL architecture you have implemented.
In this example, we are setting it to `2`:

``` ini
max_wal_senders = 2
```

This option represents the maximum number of concurrent streaming
connections that the server will be allowed to manage.

Another important parameter is `max_replication_slots`, which
represents the maximum number of replication slots [^replslot94]
that the server will be allowed to manage.
This parameter is needed if you are planning to
use the streaming connection to receive WAL files over the streaming
connection:

``` ini
max_replication_slots = 2
```

  [^replslot94]: Replication slots have been introduced in PostgreSQL 9.4.
                 See section _"WAL Streaming / Replication slots"_ for
                 details.

The values proposed for `max_replication_slots` and `max_wal_senders`
must be considered as examples, and the values you will use in your
actual setup must be choosen after a careful evaluation of the
architecture. Please consult the PostgreSQL documentation for
guidelines and clarifications.


### SSH connections

SSH is a protocol and a set of tools that allows you to open a remote
shell to a remote server and copy files between the server and the local
system. You can find more documentation about SSH usage in the article
["SSH Essentials"][ssh_essentials] by Digital Ocean.

SSH key exchange is a very common practice that is used to implement
secure passwordless connections between users on different machines,
and it's needed to use `rsync` for WAL archiving and for backups.

> **NOTE:**
> This procedure is not needed if you plan to use the streaming
> connection only to archive transaction logs and backup your PostgreSQL
> server.

[ssh_essentials]: https://www.digitalocean.com/community/tutorials/ssh-essentials-working-with-ssh-servers-clients-and-keys

#### SSH configuration of postgres user

Unless you have done it before, you need to create an SSH key for the
PostgreSQL user. Log in as `postgres`, in the `pg` host and type:

``` bash
postgres@pg$ ssh-keygen -t rsa
```

As this key must be used to connect from hosts without providing a
password, no passphrase should be entered during the key pair
creation.


#### SSH configuration of barman user

As in the previous paragraph, you need to create an SSH key for the
Barman user. Log in as `barman` in the `backup` host and type:

``` bash
barman@backup$ ssh-keygen -t rsa
```

For the same reason, no passphrase should be entered.

#### From PostgreSQL to Barman

The SSH connection from the PostgreSQL server to the backup server is
needed to correctly archive WAL files using the `archive_command`
setting.

To successfully connect from the PostgreSQL server to the backup
server, the PostgreSQL public key has to be configured into the
authorized keys of the backup server for the `barman` user.

The public key to be authorized is stored inside the `postgres` user
home directory in a file named `.ssh/id_rsa.pub`, and its content
should be included in a file named `.ssh/authorized_keys` inside the
home directory of the `barman` user in the backup server. If the
`authorized_keys` file doesn't exist, create it using `600` as
permissions.

The following command should succeed without any output if the SSH key
pair exchange has been completed successfully:

``` bash
postgres@pg$ ssh barman@backup -C true
```

The value of the `archive_command` configuration parameter will be
discussed in the _"WAL archiving via archive_command section"_.


#### From Barman to PostgreSQL

The SSH connection between the backup server and the PostgreSQL server
is used for the traditional backup over rsync. Just as with the
connection from the PostgreSQL server to the backup server, we should
authorize the public key of the backup server in the PostgreSQL server
for the `postgres` user.

The content of the file `.ssh/id_rsa.pub` in the `barman` server should
be put in the file named `.ssh/authorized_keys` in the PostgreSQL
server. The permissions of that file should be `600`.

The following command should succeed without any output if the key
pair exchange has been completed successfully.

``` bash
barman@backup$ ssh postgres@pg -C true
```
