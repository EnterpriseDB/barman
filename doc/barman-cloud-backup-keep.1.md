% BARMAN-CLOUD-BACKUP-DELETE(1) Barman User manuals | Version 2.13
% EnterpriseDB <http://www.enterprisedb.com>
% July 26, 2021

# NAME

barman-cloud-backup-keep - Flag backups which should be kept forever


# SYNOPSIS

barman-cloud-backup-keep [*OPTIONS*] *SOURCE_URL* *SERVER_NAME* *BACKUP_ID*


# DESCRIPTION

This script can be used to flag backups previously made with
`barman-cloud-backup` as archival backups. Archival backups are kept forever
regardless of any retention policies applied.

This script and Barman are administration tools for disaster recovery
of PostgreSQL servers written in Python and maintained by EnterpriseDB.


# POSITIONAL ARGUMENTS

SOURCE_URL
:    URL of the cloud source, such as a bucket in AWS S3.
     For example: `s3://BUCKET_NAME/path/to/folder` (where `BUCKET_NAME`
     is the bucket you have created in AWS).

SERVER_NAME
:    the name of the server as configured in Barman.

BACKUP_ID
:    a valid Backup ID for a backup in cloud storage

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

--target *RECOVERY_TARGET*
:   Specify the recovery target for the archival backup.
    Possible values for *RECOVERY_TARGET* are:

      - *full*: The backup can always be used to recover to the latest point
        in time. To achieve this, Barman will retain all WALs needed to
        ensure consistency of the backup and all subsequent WALs.
      - *standalone*: The backup can only be used to recover the server to
        its state at the time the backup was taken. Barman will only retain
        the WALs needed to ensure consistency of the backup.

-s, --status
:   Report the archival status of the backup. This will either be the
    recovery target of *full* or *standalone* for archival backups or
    *nokeep* for backups which have not been flagged as archival.

-r, --release
:   Release the keep flag from this backup. This will remove its archival
    status and make it available for deletion, either directly or by
    retention policy.

--cloud-provider {aws-s3,azure-blob-storage}
:    the cloud provider to which the backup should be uploaded

-P, --profile
:    profile name (e.g. INI section in AWS credentials file)

--endpoint-url
:    override the default S3 URL construction mechanism by specifying an endpoint.


# REFERENCES

For Boto:

* https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html

For AWS:

* http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-set-up.html
* http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html.

For Azure Blob Storage:

* https://docs.microsoft.com/en-us/azure/storage/blobs/authorize-data-operations-cli#set-environment-variables-for-authorization-parameters
* https://docs.microsoft.com/en-us/python/api/azure-storage-blob/?view=azure-python


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