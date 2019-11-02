% BARMAN-CLOUD-BACKUP(1) Barman User manuals | Version 2.8a1
% 2ndQuadrant <http://www.2ndQuadrant.com>
% Month DD, 2019

# NAME

barman-cloud-backup - Backup a PostgreSQL instance and stores it in the Cloud


# SYNOPSIS

barman-cloud-backup [*OPTIONS*] *DESTINATION\_URL* *SERVER\_NAME*


# DESCRIPTION

This script can be used to perform a backup of a local PostgreSQL instance
and ship the resulting tarball(s) to the Cloud. It requires read access
to PGDATA and tablespaces (normally run as `postgres` user).
Currently only AWS S3 is supported.

This script and Barman are administration tools for disaster recovery
of PostgreSQL servers written in Python and maintained by 2ndQuadrant.


# POSITIONAL ARGUMENTS

DESTINATION\_URL
:    URL of the cloud destination, such as a bucket in AWS S3.
     For example: `s3://bucket/path/to/folder`.

SERVER\_NAME
:    the name of the server as configured in Barman.

# OPTIONS

-h, --help
:    show a help message and exit

-V, --version
:    show program's version number and exit

-v, --verbose
:    increase output verbosity (e.g., -vv is more than -v)

-q, --quiet
:    decrease output verbosity (e.g., -qq is less than -q)
 
-t, --test
:    test connectivity to the cloud destination and exit

-P, --profile
:    profile name (e.g. INI section in AWS credentials file)

-z, --gzip
:    gzip-compress the tar files when uploading to the cloud

-j, --bzip2
:    bzip2-compress the tar files when uploading to the cloud

-e, --encrypt
:    enable server-side encryption for the transfer

-h, --host
:    host or Unix socket for PostgreSQL connection (default: libpq settings)

-p, --port
:    port for PostgreSQL connection (default: libpq settings)

-U, --user
:    user name for PostgreSQL connection (default: libpq settings)

--immediate-checkpoint
:    forces the initial checkpoint to be done as quickly as possible

# REFERENCES

For AWS:

* http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-set-up.html
* http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html.

For libpq settings information:

* https://www.postgresql.org/docs/current/libpq-envars.html

# DEPENDENCIES

* boto3

# EXIT STATUS

0
:   Success

Not zero
:   Failure


# BUGS

Barman has been extensively tested, and is currently being used in several
production environments. However, we cannot exclude the presence of bugs.

Any bug can be reported via the Github issue tracker.

# RESOURCES

* Homepage: <http://www.pgbarman.org/>
* Documentation: <http://docs.pgbarman.org/>
* Professional support: <http://www.2ndQuadrant.com/>


# COPYING

Barman is the property of 2ndQuadrant Limited
and its code is distributed under GNU General Public License v3.

Copyright (C) 2011-2019 2ndQuadrant Ltd - <http://www.2ndQuadrant.com/>.
