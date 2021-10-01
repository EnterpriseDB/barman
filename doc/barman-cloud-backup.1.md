% BARMAN-CLOUD-BACKUP(1) Barman User manuals | Version 2.14
% EnterpriseDB <http://www.enterprisedb.com>
% September 22, 2020

# NAME

barman-cloud-backup - Backup a PostgreSQL instance and stores it in the Cloud


# SYNOPSIS

barman-cloud-backup [*OPTIONS*] *DESTINATION_URL* *SERVER_NAME*


# DESCRIPTION

This script can be used to perform a backup of a local PostgreSQL instance
and ship the resulting tarball(s) to the Cloud. Currently AWS S3 and Azure
Blob Storage are supported.

It requires read access to PGDATA and tablespaces (normally run as `postgres`
user). It can also be used as a hook script on a barman server, in which
case it requires read access to the directory where barman backups are stored.

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

-z, --gzip
:    gzip-compress the tar files when uploading to the cloud

-j, --bzip2
:    bzip2-compress the tar files when uploading to the cloud

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
:    number of subprocesses to upload data to cloud storage (default: 2)

-S MAX_ARCHIVE_SIZE, --max-archive-size MAX_ARCHIVE_SIZE
:    maximum size of an archive when uploading to cloud storage (default: 100GB)

--cloud-provider {aws-s3,azure-blob-storage}
:    the cloud provider to which the backup should be uploaded

-P, --profile
:    profile name (e.g. INI section in AWS credentials file)

--endpoint-url
:    override the default S3 URL construction mechanism by specifying an endpoint

-e, --encryption
:    the encryption algorithm used when storing the uploaded data in S3
     Allowed values: 'AES256'|'aws:kms'

--encryption-scope
:    the name of an encryption scope defined in the Azure Blob Storage
     service which is to be used to encrypt the data in Azure

# REFERENCES

For Boto:

* https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html

For AWS:

* http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-set-up.html
* http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html.

For Azure Blob Storage:

* https://docs.microsoft.com/en-us/azure/storage/blobs/authorize-data-operations-cli#set-environment-variables-for-authorization-parameters
* https://docs.microsoft.com/en-us/python/api/azure-storage-blob/?view=azure-python

For libpq settings information:

* https://www.postgresql.org/docs/current/libpq-envars.html

# DEPENDENCIES

If using `--cloud-provider=aws-s3`:

* boto3

If using `--cloud-provider=azure-blob-storage`:

* azure-storage-blob
* azure-identity (optional, if you wish to use DefaultAzureCredential)

# EXIT STATUS

0
:   Success

Not zero
:   Failure


# SEE ALSO

This script can be used in conjunction with `post_backup_script` or
`post_backup_retry_script` to relay barman backups to cloud storage as follows:

```
post_backup_retry_script = 'barman-cloud-backup [*OPTIONS*] *DESTINATION_URL* ${BARMAN_SERVER}'
```

When running as a hook script, barman-cloud-backup will read the location of
the backup directory and the backup ID from BACKUP_DIR and BACKUP_ID environment
variables set by barman.


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
