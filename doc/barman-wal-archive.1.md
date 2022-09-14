% BARMAN-WAL-ARCHIVE(1) Barman User manuals | Version 3.1.0
% EnterpriseDB <https://www.enterprisedb.com>
% September 14, 2022

# NAME

barman-wal-archive - `archive_command` based on Barman's put-wal


# SYNOPSIS

barman-wal-archive [*OPTIONS*] *BARMAN_HOST* *SERVER_NAME* *WAL_PATH*


# DESCRIPTION

This script can be used in the `archive_command` of a PostgreSQL
server to ship WAL files to a Barman host using the 'put-wal' command
(introduced in Barman 2.6).
An SSH connection will be opened to the Barman host.
`barman-wal-archive` allows the integration of Barman in PostgreSQL
clusters for better business continuity results.

This script and Barman are administration tools for disaster recovery
of PostgreSQL servers written in Python and maintained by EnterpriseDB.


# POSITIONAL ARGUMENTS

BARMAN_HOST
:    the host of the Barman server.

SERVER_NAME
:    the server name configured in Barman from which WALs are taken.

WAL_PATH
:    the value of the '%p' keyword (according to 'archive_command').

# OPTIONS

-h, --help
:    show a help message and exit

-V, --version
:    show program's version number and exit

-U *USER*, --user *USER*
:    the user used for the ssh connection to the Barman server. Defaults
     to 'barman'.

--port *PORT*
:    the port used for the ssh connection to the Barman server.

-c *CONFIG*, --config *CONFIG*
:    configuration file on the Barman server

-t, --test
:    test both the connection and the configuration of the
     requested PostgreSQL server in Barman for WAL retrieval.
     With this option, the 'WAL_PATH' mandatory argument is ignored.

# EXIT STATUS

0
:   Success

Not zero
:   Failure


# SEE ALSO

`barman` (1), `barman` (5).


# BUGS

Barman has been extensively tested, and is currently being used in several
production environments. However, we cannot exclude the presence of bugs.

Any bug can be reported via the GitHub issue tracker.


# RESOURCES

* Homepage: <https://www.pgbarman.org/>
* Documentation: <https://docs.pgbarman.org/>
* Professional support: <https://www.enterprisedb.com/>


# COPYING

Barman is the property of EnterpriseDB UK Limited
and its code is distributed under GNU General Public License v3.

© Copyright EnterpriseDB UK Limited 2011-2022
