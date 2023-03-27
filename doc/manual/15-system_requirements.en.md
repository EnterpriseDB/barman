\newpage

# System requirements

- Linux/Unix
- Python >= 3.6
- Python modules:
    - argcomplete
    - psycopg2 >= 2.4.2
    - python-dateutil
    - setuptools
- PostgreSQL >= 10 (next version will require PostgreSQL >= 11)
- rsync >= 3.0.4 (optional)

> **IMPORTANT:**
> Users of RedHat Enterprise Linux, CentOS and Scientific Linux are
> required to install the
> [Extra Packages Enterprise Linux (EPEL) repository][epel].

> **NOTE:**
> Support for Python 2.6 and 3.5 are discontinued.
> Support for Python 2.7 is limited to Barman 3.4.X version and will receive only bugfixes. It will be discontinued in 
> the near future.
> Support for Python 3.6 will be discontinued in future releases.
> Support for PostgreSQL < 10 is discontinued since Barman 3.0.0.
> Support for PostgreSQL 10 will be discontinued after Barman 3.5.0.

## Requirements for backup

The most critical requirement for a Barman server is the amount of disk space available.
You are recommended to plan the required disk space based on the size of the cluster, number of WAL files generated per day, frequency of backups, and retention policies.

Barman developers regularly test Barman with XFS and ext4. Like [PostgreSQL](https://www.postgresql.org/docs/current/creating-cluster.html#CREATING-CLUSTER-FILESYSTEM), Barman does nothing special for NFS. The following points are required for safely using Barman with NFS: 

* The `barman_lock_directory` should be on a non-network filesystem. 
* Use version 4 of the NFS protocol. 
* The file system must be mounted using the hard and synchronous options (`hard,sync`). 

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
