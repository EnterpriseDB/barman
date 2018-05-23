\newpage

# Installation

> **IMPORTANT:**
> The recommended way to install Barman is by using the available
> packages for your GNU/Linux distribution.

## Installation on RedHat/CentOS using RPM packages

Barman can be installed on RHEL7 and RHEL6 Linux systems using
RPM packages. It is required to install the Extra Packages Enterprise
Linux (EPEL) repository and the
[PostgreSQL Global Development Group RPM repository][yumpgdg] beforehand.

Official RPM packages for Barman are distributed by 2ndQuadrant
via Yum through [2ndQuadrant Public RPM repository][2ndqrpmrepo],
by following the instructions you find on that website.

Then, as `root` simply type:

``` bash
yum install barman
```

2ndQuadrant also maintains RPM packages for Barman and distributes
them through [Sourceforge.net][3].

## Installation on Debian/Ubuntu using packages

Barman can be installed on Debian and Ubuntu Linux systems using
packages.

It is directly available in the official repository for Debian and Ubuntu, however, these repositories might not contain the latest available version.
If you want to have the latest version of Barman, the recommended method is to install both these repositories:

* [2ndQuadrant Public APT repository][2ndqdebrepo], directly maintained by
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

## Upgrading from Barman 1.X

Version 2.0 requires that users explicitly configure
their archiving strategy. Before, the file based
archiver, controlled by `archiver`, was enabled by default.

When you upgrade your Barman installation to 2.0, make sure
you add the following line either globally or for any server
that requires it:

``` ini
archiver = on
```

Additionally, for a few releases, Barman will transparently set
`archiver = on` with any server that has not explicitly set
an archiving strategy and emit a warning.

Besides that, version 2.0 is fully compatible with older ones.
