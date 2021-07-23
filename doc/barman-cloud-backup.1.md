% BARMAN-CLOUD-BACKUP(1) Barman User manuals | Version 2.12.1
% EnterpriseDB <http://www.enterprisedb.com>
% June 30, 2021

# NAME

barman-cloud-backup - Backup a PostgreSQL instance and stores it in the Cloud


# SYNOPSIS

barman-cloud-backup [*OPTIONS*] *DESTINATION_URL* *SERVER_NAME*


# DESCRIPTION

This script can be used to perform a backup of a local PostgreSQL instance
and ship the resulting tarball(s) to the Cloud. It requires read access
to PGDATA and tablespaces (normally run as `postgres` user).
Currently only AWS S3 is supported.

This script and Barman are administration tools for disaster recovery
of PostgreSQL servers written in Python and maintained by EnterpriseDB.

**IMPORTANT:** the Cloud upload process may fail if any file with a
size greater than the configured `--max-archive-size` is present
either in the data directory or in any tablespaces.
However, PostgreSQL creates files with a maximum size of 1GB,
and that size is always allowed, regardless of the `max-archive-size`
parameter.


# POSITIONAL ARGUMENTS

DESTINATION_URL
:    URL of the cloud destination, such as a bucket in AWS S3.
     For example: `s3://BUCKET_NAME/path/to/folder` (where `BUCKET_NAME`
     is the bucket you have created in AWS).

SERVER_NAME
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

-e, --encryption
:    The encryption algorithm used when storing the uploaded data in S3.
     Allowed values: 'AES256'|'aws:kms'.

-d, --dbname
:    database name or conninfo string for Postgres connection (default: postgres)

-h, --host
:    host or Unix socket for PostgreSQL connection (default: libpq settings)

-p, --port
:    port for PostgreSQL connection (default: libpq settings)

-U, --user
:    user name for PostgreSQL connection (default: libpq settings)

--immediate-checkpoint
:    forces the initial checkpoint to be done as quickly as possible

-J JOBS, --jobs JOBS
:    number of subprocesses to upload data to S3 (default: 2)

-S MAX_ARCHIVE_SIZE, --max-archive-size MAX_ARCHIVE_SIZE
:    maximum size of an archive when uploading to S3 (default: 100GB)

--endpoint-url
: override the default S3 URL construction mechanism by specifying an endpoint.

# REFERENCES

For Boto:

* https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html

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
* Professional support: <http://www.enterprisedb.com/>

# COPYING

Barman is the property of EnterpriseDB UK Limited
and its code is distributed under GNU General Public License v3.

© Copyright EnterpriseDB UK Limited 2011-2021
