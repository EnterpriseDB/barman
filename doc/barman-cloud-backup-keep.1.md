% BARMAN-CLOUD-BACKUP-DELETE(1) Barman User manuals | Version 3.7.0
% EnterpriseDB <https://www.enterprisedb.com>
% July 25, 2023

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


# Usage
```
usage: barman-cloud-backup-keep [-V] [--help] [-v | -q] [-t]
                                [--cloud-provider {aws-s3,azure-blob-storage,google-cloud-storage}]
                                [--endpoint-url ENDPOINT_URL]
                                [-P AWS_PROFILE] [--profile AWS_PROFILE]
                                [--read-timeout READ_TIMEOUT]
                                [--azure-credential {azure-cli,managed-identity}]
                                (-r | -s | --target {full,standalone})
                                source_url server_name backup_id

This script can be used to tag backups in cloud storage as archival backups
such that they will not be deleted. Currently AWS S3, Azure Blob Storage and
Google Cloud Storage are supported.

positional arguments:
  source_url            URL of the cloud source, such as a bucket in AWS S3.
                        For example: `s3://bucket/path/to/folder`.
  server_name           the name of the server as configured in Barman.
  backup_id             the backup ID of the backup to be kept

optional arguments:
  -V, --version         show program's version number and exit
  --help                show this help message and exit
  -v, --verbose         increase output verbosity (e.g., -vv is more than -v)
  -q, --quiet           decrease output verbosity (e.g., -qq is less than -q)
  -t, --test            Test cloud connectivity and exit
  --cloud-provider {aws-s3,azure-blob-storage,google-cloud-storage}
                        The cloud provider to use as a storage backend
  -r, --release         If specified, the command will remove the keep
                        annotation and the backup will be eligible for
                        deletion
  -s, --status          Print the keep status of the backup
  --target {full,standalone}
                        Specify the recovery target for this backup

Extra options for the aws-s3 cloud provider:
  --endpoint-url ENDPOINT_URL
                        Override default S3 endpoint URL with the given one
  -P AWS_PROFILE, --aws-profile AWS_PROFILE
                        profile name (e.g. INI section in AWS credentials
                        file)
  --profile AWS_PROFILE
                        profile name (deprecated: replaced by --aws-profile)
  --read-timeout READ_TIMEOUT
                        the time in seconds until a timeout is raised when
                        waiting to read from a connection (defaults to 60
                        seconds)

Extra options for the azure-blob-storage cloud provider:
  --azure-credential {azure-cli,managed-identity}, --credential {azure-cli,managed-identity}
                        Optionally specify the type of credential to use when
                        authenticating with Azure. If omitted then Azure Blob
                        Storage credentials will be obtained from the
                        environment and the default Azure authentication flow
                        will be used for authenticating with all other Azure
                        services. If no credentials can be found in the
                        environment then the default Azure authentication flow
                        will also be used for Azure Blob Storage.
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
:   The keep command was not successful

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
