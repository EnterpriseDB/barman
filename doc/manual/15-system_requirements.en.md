\newpage

# System requirements

- Linux/Unix
- Python 2.6 or 2.7
- Python modules:
    - argcomplete
    - argh >= 0.21.2 <= 0.26.2
    - argparse (Python 2.6 only)
    - psycopg2 >= 2.4.2
    - python-dateutil <> 2.0
    - setuptools
- PostgreSQL >= 8.3
- rsync >= 3.0.4 (optional for PostgreSQL >= 9.2)

> **IMPORTANT:**
> Users of RedHat Enterprise Linux, CentOS and Scientific Linux are
> required to install the
> [Extra Packages Enterprise Linux (EPEL) repository][epel].

> **NOTE:**
> Python 3 support is experimental. Report any bug through
> the ticketing system on Github or the mailing list.

## Requirements for backup

The most critical requirement for a Barman server is the amount of disk space available.
You are recommended to plan the required disk space based on the size of the cluster, number of WAL files generated per day, frequency of backups, and retention policies.

Although the only file systems that we officially support are XFS and Ext4, we are aware of users that deploy Barman on different file systems including ZFS and NFS.

## Requirements for recovery

Barman allows you to recover a PostgreSQL instance either
locally (where Barman resides) or remotely (on a separate server).

Remote recovery is definitely the most common way to restore a PostgreSQL
server with Barman.

Either way, the same [requirements for PostgreSQL's Log shipping and Point-In-Time-Recovery apply][requirements_recovery]:

- identical hardware architecture
- identical major version of PostgreSQL

In general, it is **highly recommended** to create recovery environments that are as similar as possible, if not identical, to the original server, because they are easier to maintain. For example, we suggest that you use the same operating system, the same PostgreSQL version, the same disk layouts, and so on.

Additionally, dedicated recovery environments for each PostgreSQL server, even on demand, allows you to nurture the disaster recovery culture in your team. You can be prepared for when something unexpected happens by practising
recovery operations and becoming familiar with them.

Based on our experience, designated recovery environments reduce the impact of stress in real failure situations, and therefore increase the effectiveness of recovery operations.

Finally, it is important that time is synchronised between the servers, using NTP for example.
