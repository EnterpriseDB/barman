\newpage

# General commands

**TODO:**

- Define general commands
- do an updated inventory of general commands
- order commands alphabetically
- Remove all 'From version ...' paragraph. Assume that we start from scratch with 2.0


## `cron`

You can perform maintenance operations, on both WAL files and backups,
using the command:


``` bash
barman cron
```

As of version 1.5.1 `barman cron` executes WAL archiving operations
concurrently on a server basis.

This also enforces retention policies on those servers that have:


- `retention_policy` not empty and valid;
- `retention_policy_mode` set to `auto`.


> **Note:**
> This command should be executed in a _cron script_. Our
> recommendation is to schedule `barman cron` to run every minute.

## `diagnose`

TODO

## `list-server`

You can display the list of active servers that have been configured
for your backup system with:

``` bash
barman list-server
```
