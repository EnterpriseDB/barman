\newpage

# Barman client utilities (`barman-cli`)

Formerly a separate open-source project, `barman-cli` has been
merged into Barman's core since version 2.8, and is distributed
as an RPM/Debian package. `barman-cli` contains a set of recommended
client utilities to be installed alongside the PostgreSQL server:

- `barman-wal-archive`: archiving script to be used as `archive_command`
  as described in the "WAL archiving via `barman-wal-archive`" section;
- `barman-wal-restore`: WAL restore script to be used as part of the
  `restore_command` recovery option on standby and recovery servers,
  as described in the "`get-wal`" section above;
- `barman-cloud-wal-archive`: archiving script to be used as `archive_command`
  to directly ship WAL files to an S3 object store, bypassing the Barman server;
  alternatively, as a hook script for WAL archiving (`pre_archive_retry_script`);
- `barman-cloud-backup`: backup script to be used to take a local backup
  directly on the PostgreSQL server and to ship it to an S3 object store,
  bypassing the Barman server.

For more detailed information, please refer to the specific man pages
or the `--help` option. For information on how to setup credentials
for the Cloud utilities, please refer to the
["Credentials" section in Boto 3 documentation][boto3creds].

> **WARNING:** `barman-cloud-wal-archive` and `barman-cloud-backup` have been
> introduced in Barman 2.10. The corresponding utilities for restore
> (`barman-cloud-wal-restore` and `barman-cloud-recover`) will be included
> in the next 2.11 release. For the moment, restore of WAL files and backups
> requires manual intervention (using for example third-party utilities like
> `aws-cli`). Cloud utilities require boto3 library installed in your system.

## Installation

Barman client utilities are normally installed where PostgreSQL is installed.
Our recommendation is to install the `barman-cli` package on every PostgreSQL
server, being that primary or standby.

Please refer to the main "Installation" section to install the repositories.

In case you want to use `barman-cloud-wal-archive` as a hook script, install
the package on the Barman server also.

To install the package on RedHat/CentOS system, as `root` type:

``` bash
yum install barman-cli
```

On Debian/Ubuntu, as `root` user type:

``` bash
apt-get install barman-cli
```

