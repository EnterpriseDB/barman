\newpage

# Installation

Official packages for Barman are distributed by EnterpriseDB through repositories
listed on the [Barman downloads page][barman-downloads].

> **IMPORTANT:**
> The recommended way to install Barman is by using the available
> packages for your GNU/Linux distribution.

## Installation on Red Hat Enterprise Linux (RHEL) and RHEL-based systems using RPM packages

Barman can be installed using RPM packages on RHEL8 and RHEL7 systems and the identical versions of RHEL derivatives AlmaLinux, Oracle Linux, and Rocky Linux. It is required to install the Extra Packages Enterprise Linux (EPEL) repository and the
[PostgreSQL Global Development Group RPM repository][yumpgdg] beforehand.

Official RPM packages for Barman are distributed by EnterpriseDB
via Yum through the [public RPM repository][2ndqrpmrepo],
by following the instructions you find on that website.

Then, as `root` simply type:

``` bash
yum install barman
```

In addition to the Barman packages available in the EDB and PGDG repositories, Barman RPMs published by the Fedora project can be found in EPEL.
These RPMs are not maintained by the Barman developers and use a different configuration layout to the packages available in the PGDG and EDB repositories:

- EDB and PGDG packages use `/etc/barman.conf` as the main configuration file and `/etc/barman.d` for additional configuration files.
- The Fedora packages use `/etc/barman/barman.conf` as the main configuration file and `/etc/barman/conf.d` for additional configuration files.

The difference in configuration file layout means that upgrades between the EPEL and non-EPEL Barman packages can break existing Barman installations until configuration files are manually updated.
We therefore recommend that you use a single source repository for Barman packages.
This can be achieved by adding the following line to the definition of the repositories from which you do not want to obtain Barman packages:

```ini
exclude=barman* python*-barman
```

Specifically:

- To use only Barman packages from the EDB repositories, add the exclude directive from above to repository definitions in `/etc/yum.repos.d/epel.repo` and `/etc/yum.repos.d/pgdg-*.repo`.
- To use only Barman packages from the PGDG repositories, add the exclude directive from above to repository definitions in `/etc/yum.repos.d/epel.repo` and `/etc/yum.repos.d/enterprisedb*.repo`.
- To use only Barman packages from the EPEL repositories, add the exclude directive from above to repository definitions in `/etc/yum.repos.d/pgdg-*.repo` and `/etc/yum.repos.d/enterprisedb*.repo`.

## Installation on Debian/Ubuntu using packages

Barman can be installed on Debian and Ubuntu Linux systems using
packages.

It is directly available in the official repository for Debian and Ubuntu, however, these repositories might not contain the latest available version.
If you want to have the latest version of Barman, the recommended method is to install both these repositories:

* [Public APT repository][2ndqdebrepo], directly maintained by
  Barman developers
* the [PostgreSQL Community APT repository][aptpgdg], by following instructions in the [APT section of the PostgreSQL Wiki][aptpgdgwiki]

> **NOTE:**
> Thanks to the direct involvement of Barman developers in the
> PostgreSQL Community APT repository project, you will always have access
> to the most updated versions of Barman.

Installing Barman is as easy. As `root` user simply type:

``` bash
apt-get install barman
```

## Installation on SLES using packages

Barman can be installed on SLES systems using packages available in the
[PGDG SLES repositories](https://zypp.postgresql.org/). Install the
necessary repository by following the instructions available on the
[PGDG site](https://zypp.postgresql.org/howtozypp/).

Supported SLES version: SLES 15 SP3.

Once the necessary repositories have been installed you can install Barman
as the `root` user:

``` bash
zypper install barman
```

## Installation from sources

> **WARNING:**
> Manual installation of Barman from sources should only be performed
> by expert GNU/Linux users. Installing Barman this way requires
> system administration activities such as dependencies management,
> `barman` user creation, configuration of the `barman.conf` file,
> cron setup for the `barman cron` command, log management, and so on.

Create a system user called `barman` on the `backup` server.
As `barman` user, download the sources and uncompress them.

For a system-wide installation, type:

``` bash
barman@backup$ ./setup.py build
# run this command with root privileges or through sudo
barman@backup# ./setup.py install
```

For a local installation, type:

``` bash
barman@backup$ ./setup.py install --user
```

The `barman` application will be installed in your user directory ([make sure that your `PATH` environment variable is set properly][setup_user]).

[Barman is also available on the Python Package Index (PyPI)][pypi] and can be installed through `pip`.

## PostgreSQL client/server binaries

The following Barman features depend on PostgreSQL binaries:

* [Streaming backup](#streaming-backup) with `backup_method = postgres` (requires `pg_basebackup`)
* [Streaming WAL archiving](#wal-streaming) with `streaming_archiver = on` (requires
  `pg_receivewal` or `pg_receivexlog`)
* [Verifying backups](#verify-backup) with `barman verify-backup` (requires `pg_verifybackup`)

Depending on the target OS these binaries are installed with either the PostgreSQL client or server packages:

* On RedHat/CentOS and SLES:
  * The `pg_basebackup` and `pg_receivewal`/`pg_receivexlog` binaries are installed with the PostgreSQL client packages.
  * The `pg_verifybackup` binary is installed with the PostgreSQL server packages.
  * All binaries are installed in `/usr/pgsql-${PG_MAJOR_VERSION}/bin`.
* On Debian/Ubuntu:
  * All binaries are installed with the PostgreSQL client packages.
  * The binaries are installed in `/usr/lib/postgresql/${PG_MAJOR_VERSION}/bin`.

You must ensure that either:

1. The Barman user has the `bin` directory for the appropriate `PG_MAJOR_VERSION`
   on its path, or:
2. The [path_prefix](#binary-paths) option is set in the Barman configuration for each
   server and points to the `bin` directory for the appropriate
   `PG_MAJOR_VERSION`.

The [psql][psql] program is recommended in addition to the above binaries.
While Barman does not use it directly the documentation provides examples of how it can be used to verify PostgreSQL connections are working as intended.
The `psql` binary can be found in the PostgreSQL client packages.

### Third party PostgreSQL variants

If you are using Barman for the backup and recovery of third-party
PostgreSQL variants then you will need to check whether the PGDG
client/server binaries described above are compatible with your variant.
If they are incompatible then you will need to install compatible
alternatives from appropriate packages.

# Upgrading Barman

Barman follows the trunk-based development paradigm, and as such
there is only one stable version, the latest. After every commit,
Barman goes through thousands of automated tests for each
supported PostgreSQL version and on each supported Linux distribution.

Also, **every version is back compatible** with previous ones.
Therefore, upgrading Barman normally requires a simple update of packages
using `yum update` or `apt update`.

There have been, however, the following exceptions in our development
history, which required some small changes to the configuration.

## Upgrading to Barman 3.0.0

### Default backup approach for Rsync backups is now concurrent

Barman will now use concurrent backups if neither `concurrent_backup`
nor `exclusive_backup` are specified in `backup_options`. This
differs from previous Barman versions where the default was to
use exclusive backup.

If you require exclusive backups you will now need to add
`exclusive_backup` to `backup_options` in the Barman configuration.

Note that exclusive backups are not supported at all when running
against PostgreSQL 15.

### Metadata changes

A new field named `compression` will be added to the metadata stored
in the `backup.info` file for all backups taken with version 3.0.0.
This is used when recovering from backups taken using the built-in
compression functionality of `pg_basebackup`.

The presence of this field means that earlier versions of Barman are
not able to read backups taken with Barman 3.0.0. This means that if
you downgrade from Barman 3.0.0 to an earlier version you will have
to either manually remove any backups taken with 3.0.0 or edit the
`backup.info` file of each backup to remove the `compression` field.

The same metadata change affects [pg-backup-api][pg-backup-api] so
if you are using pg-backup-api you will need to update it to version
0.2.0.

## Upgrading from Barman 2.10

If you are using `barman-cloud-wal-archive` or `barman-cloud-backup`
you need to be aware that from version 2.11 all cloud utilities
have been moved into the new `barman-cli-cloud` package.
Therefore, you need to ensure that the `barman-cli-cloud` package
is properly installed as part of the upgrade to the latest version.
If you are not using the above tools, you can upgrade to the latest
version as usual.

## Upgrading from Barman 2.X (prior to 2.8)

Before upgrading from a version of Barman 2.7 or older
users of `rsync` backup method on a primary server should explicitly
set `backup_options` to either `concurrent_backup` (recommended for
PostgreSQL 9.6 or higher) or `exclusive_backup` (current default),
otherwise Barman emits a warning every time it runs.

## Upgrading from Barman 1.X

If your Barman installation is 1.X, you need to explicitly configure
the archiving strategy. Before, the file based archiver, controlled by
`archiver`, was enabled by default.

Before you upgrade your Barman installation to the latest version,
make sure you add the following line either globally or for any server
that requires it:

``` ini
archiver = on
```

Additionally, for a few releases, Barman will transparently set
`archiver = on` with any server that has not explicitly set
an archiving strategy and emit a warning.
