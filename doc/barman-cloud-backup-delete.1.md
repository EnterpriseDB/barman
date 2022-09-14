% BARMAN-CLOUD-BACKUP-DELETE(1) Barman User manuals | Version 3.1.0
% EnterpriseDB <https://www.enterprisedb.com>
% September 14, 2022

# NAME

barman-cloud-backup-delete - Delete backups stored in the Cloud


# SYNOPSIS

barman-cloud-backup-delete [*OPTIONS*] *SOURCE_URL* *SERVER_NAME*


# DESCRIPTION

This script can be used to delete backups previously made with the
`barman-cloud-backup` command. Currently AWS S3, Azure Blob Storage and 
Google Cloud Storage are supported.

The target backups can be specified either using the backup ID (as
returned by barman-cloud-backup-list) or by retention policy. Retention
policies are the same as those for Barman server and work as described in
the Barman manual: all backups not required to meet the specified policy
will be deleted.

When a backup is successfully deleted any unused WALs associated with that
backup are removed. WALs are only considered unused if:

 1. There are no older backups than the deleted backup *or* all older backups
    are archival backups.
 2. The WALs pre-date the begin_wal value of the oldest remaining backup.
 3. The WALs are not required by any archival backups present in cloud storage.

Note: The deletion of each backup involves three separate delete requests
to the cloud provider (once for the backup files, once for the backup.info
file and once for any associated WALs). If you have a significant number of
backups accumulated in cloud storage then deleting by retention policy could
result in a large number of delete requests.

This script and Barman are administration tools for disaster recovery
of PostgreSQL servers written in Python and maintained by EnterpriseDB.


# POSITIONAL ARGUMENTS

SOURCE_URL
:    URL of the cloud source, such as a bucket in AWS S3.
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

-b *BACKUP_ID*, --backup-id *BACKUP_ID*
:    a valid Backup ID for a backup in cloud storage which is to be deleted

-r *RETENTION_POLICY*, --retention-policy *RETENTION_POLICY*
:    used instead of --backup-id, a retention policy for selecting the backups
     to be deleted, e.g. "REDUNDANCY 3" or "RECOVERY WINDOW OF 2 WEEKS"

--dry-run
:    run without actually deleting any objects while printing information
     about the objects which would be deleted to stdout

--cloud-provider {aws-s3,azure-blob-storage,google-cloud-storage}
:    the cloud provider to which the backup should be uploaded

-P, --profile
:    profile name (e.g. INI section in AWS credentials file)

--endpoint-url
:    override the default S3 URL construction mechanism by specifying an endpoint.

--credential {azure-cli,managed-identity}
:    optionally specify the type of credential to use when authenticating with
     Azure Blob Storage. If omitted then the credential will be obtained from the
     environment. If no credentials can be found in the environment then the default
     Azure authentication flow will be used.

# REFERENCES

For Boto:

* https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html

For AWS:

* https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-set-up.html
* https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html.

For Azure Blob Storage:

* https://docs.microsoft.com/en-us/azure/storage/blobs/authorize-data-operations-cli#set-environment-variables-for-authorization-parameters
* https://docs.microsoft.com/en-us/python/api/azure-storage-blob/?view=azure-python

For Google Cloud Storage:
* Credentials: https://cloud.google.com/docs/authentication/getting-started#setting_the_environment_variable

  Only authentication with `GOOGLE_APPLICATION_CREDENTIALS` env is supported at the moment.

# DEPENDENCIES

If using `--cloud-provider=aws-s3`:

* boto3

If using `--cloud-provider=azure-blob-storage`:

* azure-storage-blob
* azure-identity (optional, if you wish to use DefaultAzureCredential)

If using `--cloud-provider=google-cloud-storage`
* google-cloud-storage 

# EXIT STATUS

0
:   Success

1
:   The delete operation was not successful

2
:   The connection to the cloud provider failed

3
:   There was an error in the command input

Other non-zero codes
:   Failure


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
