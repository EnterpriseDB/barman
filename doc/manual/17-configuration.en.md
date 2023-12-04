\newpage

# Configuration

There are two types of configuration files in Barman:

- **global/general configuration**
- **server configuration**
- **model configuration**

The main configuration file (set to `/etc/barman.conf` by default) contains general options such as main directory, system user, log file, and so on.

Server configuration files, one for each server to be backed up by Barman, are located in the `/etc/barman.d` directory and must have a `.conf` suffix.

Similarly, model configuration files, one for each model that can be applied to a server which is part of a cluster, are located in the `/etc/barman.d` directory and must have a `.conf` suffix.

> *NOTE*: models define a set of configuration overrides which can be applied on top of the configuration of Barman servers that are part of the same cluster as the model, through the `barman config-switch` command.

> **IMPORTANT**: For historical reasons, you can still have one single
> configuration file containing both global as well as server and model options.
> However, for maintenance reasons, this approach is deprecated.

Configuration files in Barman follow the _INI_ format.

Configuration files accept distinct types of parameters:

- string
- enum
- integer
- boolean, `on/true/1` are accepted as well are `off/false/0`.

None of them requires to be quoted.

> *NOTE*: some `enum` allows `off` but not `false`.

## Options scope

Every configuration option has a _scope_:

- global
- server
- model
- global/server: server options that can be generally set at global level

Global options are allowed in the _general section_, which is identified in the INI file by the `[barman]` label:

``` ini
[barman]
; ... global and global/server options go here
```

Server options can only be specified in a _server section_, which is identified by a line in the configuration file, in square brackets (`[` and `]`). The server section represents the ID of that server in Barman. The following example specifies a section for the server named `pg`, which belongs to the `my-cluster` cluster:

``` ini
[pg]
cluster=my-cluster
; Configuration options for the
; server named 'pg' go here
```

Model options can only be specified in a _model section_, which is identified the same way as a _server section_. There can be no conflicts among the identifier of _server sections_ and _model sections_. The following example specifies a section for the model named `pg:switchover`, which belongs to the `my-cluster` cluster:

```ini
[pg:switchover]
cluster=my-cluster
model=true
; Configuration options for the model named 'pg:switchover', which belongs to
; the server which is configured with the option 'cluster=pg', go here
```

There are two reserved words that cannot be used neither as server names nor as model names in Barman:

- `barman`: identifier of the global section
- `all`: a handy shortcut that allows you to execute some commands on every server managed by Barman in sequence

Barman implements the **convention over configuration** design paradigm, which attempts to reduce the number of options that you are required to configure without losing flexibility. Therefore, some server options can be defined at global level and overridden at server level, allowing users to specify a generic behavior and refine it for one or more servers. These options have a global/server scope.

For a list of all the available configurations
and their scope, please refer to [section 5 of the 'man' page][man5].

``` bash
man 5 barman
```

## Examples of configuration

The following is a basic example of main configuration file:

``` ini
[barman]
barman_user = barman
configuration_files_directory = /etc/barman.d
barman_home = /var/lib/barman
log_file = /var/log/barman/barman.log
log_level = INFO
compression = gzip
```

The example below, on the other hand, is a server configuration file that uses streaming backup:

``` ini
[streaming-pg]
description =  "Example of PostgreSQL Database (Streaming-Only)"
conninfo = host=pg user=barman dbname=postgres
streaming_conninfo = host=pg user=streaming_barman
backup_method = postgres
streaming_archiver = on
slot_name = barman
```

The following example defines a configuration model with a set of overrides that can be applied to the server which cluster is `streaming-pg`:

```ini
[streaming-pg:switchover]
cluster=streaming-pg
model=true
conninfo = host=pg-2 user=barman dbname=postgres
streaming_conninfo = host=pg-2 user=streaming_barman
```

The following code shows a basic example of traditional backup using `rsync`/SSH:

``` ini
[ssh-pg]
description =  "Example of PostgreSQL Database (via Ssh)"
ssh_command = ssh postgres@pg
conninfo = host=pg user=barman dbname=postgres
backup_method = rsync
parallel_jobs = 1
reuse_backup = link
archiver = on
```

For more detailed information, please refer to the distributed
`barman.conf` file, as well as the `ssh-server.conf-template` and  `streaming-server.conf-template` template files.
