## Preliminary steps

This section contains some preliminary steps that you need to
undertake before setting up your PostgreSQL server in Barman.

It is important that you have decided which WAL archiving methods
to use, as well as the backup method.

> **IMPORTANT:**
> Before you proceed, it is important that you have made your decision in terms of WAL archiving and backup strategies, as outlined in the _"Design and architecture"_ section.

### PostgreSQL connection

You need to make sure that the `backup` server can connect to
the PostgreSQL server on `pg` as superuser. This operation is mandatory.

We recommend to create a specific user in PostgreSQL, named `barman`, as follows:

``` bash
postgres@pg$ createuser -s -W barman
```

> **IMPORTANT:** The above command will prompt for a password,
> which you are then advised to add to the `~barman/.pgpass` file
> on the `backup` server. For further information, please refer to
> ["The Password File" section in the PostgreSQL Documentation] [pgpass].

This connection is required by Barman in order to coordinate its
activities with the server, as well as for monitoring purposes.

You can choose your favourite client authentication method among those
offered by PostgreSQL. More information can be found in the
["Client Authentication" section of the PostgreSQL Documentation] [pghba].

Make sure you test the following command before proceeding:

``` bash
barman@backup$ psql -c 'SELECT version()' -U barman -h pg postgres
```

> **NOTE:**
> As of version 1.1.2, Barman honours the `application_name`
> connection option for PostgreSQL servers 9.0 or higher.

Write down the above information and keep it for later.
You will need it with in the `conninfo` option for your server
configuration, like in this example:

``` ini
[pg]
; ...
conninfo = host=pg user=barman database=postgres
```

### PostgreSQL streaming connection

In case you plan to use WAL streaming or streaming backup, you need to setup a streaming connection. We recommend to create a specific user in PostgreSQL, named `streaming_barman`, as follows:

``` bash
postgres@pg$ createuser -S -W --replication streaming_barman
```
> **IMPORTANT:** The above command will prompt for a password,
> which you are then advised to add to the `~barman/.pgpass` file
> on the `backup` server. For further information, please refer to
> ["The Password File" section in the PostgreSQL Documentation] [pgpass].

Before you proceed, you need to properly configure PostgreSQL on `pg` to accept streaming replication connections from the Barman server. Please read the following sections in the PostgreSQL documentation:

- [Role attributes] [roles]
- [The pg_hba.conf file] [authpghba]
- [Setting up standby servers using streaming replication] [streamprot]

You can manually verify that the streaming connection works through the following command:

``` bash
barman@backup$ psql -U streaming_barman -h pg \
  -c "IDENTIFY_SYSTEM" \
  replication=1
```

Please make sure you are able to connect via streaming replication before going any further.

WWrite down the above information and keep it for later.
You will need it with in the `streaming_conninfo` option for your server
configuration, like in this example:

``` ini
[pg]
; ...
streaming_conninfo = host=pg user=streaming_barman
```

### SSH connections

SSH key exchange is a very common practice that is used to implement
secure passwordless connections between users on different machines.

<!--

TODO:

Add more info, like a link to https://www.digitalocean.com/community/tutorials/ssh-essentials-working-with-ssh-servers-clients-and-keys

-->

#### PostgreSQL user's key

Unless you have done if before, you need to create a SSH key for the PostgreSQL user. Log in the `pg` host as `postgres` user and type:

``` bash
postgres@pg$ ssh-keygen -t rsa
```

#### Barman user's key

Unless you have done if before, you need to create a SSH key for the Barman user. Log in the `backup` host as `barman` user and type:

``` bash
barman@backup$ ssh-keygen -t rsa
```

#### From PostgreSQL to Barman

**TODO:**

- Explain it is needed for WAL archiving
- Explain the steps
- Manual verification

#### From Barman to PostgreSQL

**TODO:**

- Explain it is needed for traditional rsync backup
- Explain the steps
- Manual verification
