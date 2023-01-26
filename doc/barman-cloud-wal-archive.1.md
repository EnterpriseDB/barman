% BARMAN-CLOUD-WAL-ARCHIVE(1) Barman User manuals | Version 3.4.0
% EnterpriseDB <https://www.enterprisedb.com>
% January 26, 2023

# NAME

barman-cloud-wal-archive - Archive PostgreSQL WAL files in the Cloud using `archive_command`


# SYNOPSIS

barman-cloud-wal-archive [*OPTIONS*] *DESTINATION_URL* *SERVER_NAME* *WAL_PATH*


# DESCRIPTION

This script can be used in the `archive_command` of a PostgreSQL
server to ship WAL files to the Cloud. Currently AWS S3, Azure Blob
Storage and Google Cloud Storage are supported.

Note: If you are running python 2 or older unsupported versions of
python 3 then avoid the compression options `--gzip` or `--bzip2` as
barman-cloud-wal-restore is unable to restore gzip-compressed WALs
on python < 3.2 or bzip2-compressed WALs on python < 3.3.

This script and Barman are administration tools for disaster recovery
of PostgreSQL servers written in Python and maintained by EnterpriseDB.


# Usage
```
usage: barman-cloud-wal-archive [-V] [--help] [-v | -q] [-t]
                                [--cloud-provider {aws-s3,azure-blob-storage,google-cloud-storage}]
                                [--endpoint-url ENDPOINT_URL] [-P PROFILE]
                                [--read-timeout READ_TIMEOUT]
                                [--credential {azure-cli,managed-identity}]
                                [-z | -j | --snappy]
                                [--tags [TAGS [TAGS ...]]]
                                [--history-tags [HISTORY_TAGS [HISTORY_TAGS ...]]]
                                [-e ENCRYPTION]
                                [--encryption-scope ENCRYPTION_SCOPE]
                                [--max-block-size MAX_BLOCK_SIZE]
                                [--max-concurrency MAX_CONCURRENCY]
                                [--max-single-put-size MAX_SINGLE_PUT_SIZE]
                                destination_url server_name [wal_path]

This script can be used in the `archive_command` of a PostgreSQL server to
ship WAL files to the Cloud. Currently AWS S3, Azure Blob Storage and Google
Cloud Storage are supported.

positional arguments:
  destination_url       URL of the cloud destination, such as a bucket in AWS
                        S3. For example: `s3://bucket/path/to/folder`.
  server_name           the name of the server as configured in Barman.
  wal_path              the value of the '%p' keyword (according to
                        'archive_command').

optional arguments:
  -V, --version         show program's version number and exit
  --help                show this help message and exit
  -v, --verbose         increase output verbosity (e.g., -vv is more than -v)
  -q, --quiet           decrease output verbosity (e.g., -qq is less than -q)
  -t, --test            Test cloud connectivity and exit
  --cloud-provider {aws-s3,azure-blob-storage,google-cloud-storage}
                        The cloud provider to use as a storage backend
  -z, --gzip            gzip-compress the WAL while uploading to the cloud
                        (should not be used with python < 3.2)
  -j, --bzip2           bzip2-compress the WAL while uploading to the cloud
                        (should not be used with python < 3.3)
  --snappy              snappy-compress the WAL while uploading to the cloud
                        (requires optional python-snappy library)
  --tags [TAGS [TAGS ...]]
                        Tags to be added to archived WAL files in cloud
                        storage
  --history-tags [HISTORY_TAGS [HISTORY_TAGS ...]]
                        Tags to be added to archived history files in cloud
                        storage

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
  -e ENCRYPTION, --encryption ENCRYPTION
                        The encryption algorithm used when storing the
                        uploaded data in S3. Allowed values:
                        'AES256'|'aws:kms'.

Extra options for the azure-blob-storage cloud provider:
  --credential {azure-cli,managed-identity}
                        Optionally specify the type of credential to use when
                        authenticating with Azure Blob Storage. If omitted
                        then the credential will be obtained from the
                        environment. If no credentials can be found in the
                        environment then the default Azure authentication flow
                        will be used
  --encryption-scope ENCRYPTION_SCOPE
                        The name of an encryption scope defined in the Azure
                        Blob Storage service which is to be used to encrypt
                        the data in Azure
  --max-block-size MAX_BLOCK_SIZE
                        The chunk size to be used when uploading an object via
                        the concurrent chunk method (default: 4MB).
  --max-concurrency MAX_CONCURRENCY
                        The maximum number of chunks to be uploaded
                        concurrently (default: 1).
  --max-single-put-size MAX_SINGLE_PUT_SIZE
                        Maximum size for which the Azure client will upload an
                        object in a single request (default: 64MB). If this is
                        set lower than the PostgreSQL WAL segment size after
                        any applied compression then the concurrent chunk
                        upload method for WAL archiving will be used.
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
:   The WAL archive operation was not successful

2
:   The connection to the cloud provider failed

3
:   There was an error in the command input

Other non-zero codes
:   Failure


# SEE ALSO

This script can be used in conjunction with `pre_archive_retry_script` to relay WAL
files to S3, as follows:

```
pre_archive_retry_script = 'barman-cloud-wal-archive [*OPTIONS*] *DESTINATION_URL* ${BARMAN_SERVER}'
```


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
