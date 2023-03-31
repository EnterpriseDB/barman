% BARMAN-CLOUD-BACKUP-DELETE(1) Barman User manuals | Version 3.4.1
% EnterpriseDB <https://www.enterprisedb.com>
% March 31, 2023

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


# Usage
```
usage: barman-cloud-backup-delete [-V] [--help] [-v | -q] [-t]
                                  [--cloud-provider {aws-s3,azure-blob-storage,google-cloud-storage}]
                                  [--endpoint-url ENDPOINT_URL] [-P PROFILE]
                                  [--read-timeout READ_TIMEOUT]
                                  [--credential {azure-cli,managed-identity}]
                                  (-b BACKUP_ID | -r RETENTION_POLICY)
                                  [--dry-run] [--batch-size DELETE_BATCH_SIZE]
                                  source_url server_name

This script can be used to delete backups made with barman-cloud-backup
command. Currently AWS S3, Azure Blob Storage and Google Cloud Storage are
supported.

positional arguments:
  source_url            URL of the cloud source, such as a bucket in AWS S3.
                        For example: `s3://bucket/path/to/folder`.
  server_name           the name of the server as configured in Barman.

optional arguments:
  -V, --version         show program's version number and exit
  --help                show this help message and exit
  -v, --verbose         increase output verbosity (e.g., -vv is more than -v)
  -q, --quiet           decrease output verbosity (e.g., -qq is less than -q)
  -t, --test            Test cloud connectivity and exit
  --cloud-provider {aws-s3,azure-blob-storage,google-cloud-storage}
                        The cloud provider to use as a storage backend
  -b BACKUP_ID, --backup-id BACKUP_ID
                        Backup ID of the backup to be deleted
  -r RETENTION_POLICY, --retention-policy RETENTION_POLICY
                        If specified, delete all backups eligible for deletion
                        according to the supplied retention policy. Syntax:
                        REDUNDANCY value | RECOVERY WINDOW OF value {DAYS |
                        WEEKS | MONTHS}
  --dry-run             Find the objects which need to be deleted but do not
                        delete them
  --batch-size DELETE_BATCH_SIZE
                        The maximum number of objects to be deleted in a
                        single request to the cloud provider. If unset then
                        the maximum allowed batch size for the specified cloud
                        provider will be used (1000 for aws-s3, 256 for azure-
                        blob-storage and 100 for google-cloud-storage).

Extra options for the aws-s3 cloud provider:
  --endpoint-url ENDPOINT_URL
                        Override default S3 endpoint URL with the given one
  -P PROFILE, --profile PROFILE
                        profile name (e.g. INI section in AWS credentials
                        file)
  --read-timeout READ_TIMEOUT
                        the time in seconds until a timeout is raised when
                        waiting to read from a connection (defaults to 60
                        seconds)

Extra options for the azure-blob-storage cloud provider:
  --credential {azure-cli,managed-identity}
                        Optionally specify the type of credential to use when
                        authenticating with Azure Blob Storage. If omitted
                        then the credential will be obtained from the
                        environment. If no credentials can be found in the
                        environment then the default Azure authentication flow
                        will be used
```
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

© Copyright EnterpriseDB UK Limited 2011-2023
