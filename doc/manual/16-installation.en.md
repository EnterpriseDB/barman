\newpage

# Installation

> **Important:**
> The recommended way to install Barman is by using the available
> packages for your GNU/Linux distribution.

## Installation on RedHat/CentOS using RPM packages

Barman can be installed on RHEL7, RHEL6 and RHEL5 Linux systems using
RPM packages. It is required to install the Extra Packages Enterprise
Linux (EPEL) repository beforehand.

RPM packages for Barman are available via Yum through the
[PostgreSQL Global Development Group RPM repository] [yumpgdg].
You need to follow the instructions for your distribution (for example RedHat,
CentOS, or Fedora) and architecture as detailed at
[yum.postgresql.org] [yumpgdg].

Then, as `root` simply type:

``` bash
yum install barman
```

2ndQuadrant also maintains RPM packages for Barman and distributes
them through [Sourceforge.net] [3].

## Installation on Debian/Ubuntu using packages

Barman can be installed on Debian and Ubuntu Linux systems using
packages.

It is directly available in the official repository for Debian and Ubuntu, however, these repositories might not contain the latest available version.
If you want to have the latest version of Barman, the recommended method is to install it through the [PostgreSQL Community APT repository] [aptpgdg].
Instructions can be found in the [APT section of the PostgreSQL Wiki] [aptpgdgwiki].

> **Note:**
> Thanks to the direct involvement of Barman developers in the
> PostgreSQL Community APT repository project, you will have access to
> the most updated versions of Barman.

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

The `barman` application will be installed in your user directory ([make sure that your `PATH` environment variable is set properly] [setup_user]).

[Barman is also available on the Python Package Index (PyPI)] [pypi] and can be installed through `pip`.