% BARMAN-CLOUD-CHECK-WAL-ARCHIVE(1) Barman User manuals | Version 3.5.0
% EnterpriseDB <https://www.enterprisedb.com>
% March 29, 2023

# NAME

barman-cloud-check-wal-archive - Check a WAL archive destination for a new PostgreSQL cluster


# SYNOPSIS

barman-cloud-check-wal-archive [*OPTIONS*] *SOURCE_URL* *SERVER_NAME*


# DESCRIPTION

Check that the WAL archive destination for *SERVER_NAME* is safe to use
for a new PostgreSQL cluster. With no optional args (the default) this
check will pass if the WAL archive is empty or if the target bucket cannot
be found. All other conditions will result in failure.

This script and Barman are administration tools for disaster recovery
of PostgreSQL servers written in Python and maintained by EnterpriseDB.


# Usage
```
usage: barman-cloud-check-wal-archive [-V] [--help] [-v | -q] [-t]
                                      [--cloud-provider {aws-s3,azure-blob-storage,google-cloud-storage}]
                                      [--endpoint-url ENDPOINT_URL]
                                      [-P PROFILE]
                                      [--read-timeout READ_TIMEOUT]
                                      [--azure-credential {azure-cli,managed-identity}]
                                      [--timeline TIMELINE]
                                      destination_url server_name

Checks that the WAL archive on the specified cloud storage can be safely used
for a new PostgreSQL server.

positional arguments:
  destination_url       URL of the cloud destination, such as a bucket in AWS
                        S3. For example: `s3://bucket/path/to/folder`.
  server_name           the name of the server as configured in Barman.

optional arguments:
  -V, --version         show program's version number and exit
  --help                show this help message and exit
  -v, --verbose         increase output verbosity (e.g., -vv is more than -v)
  -q, --quiet           decrease output verbosity (e.g., -qq is less than -q)
  -t, --test            Test cloud connectivity and exit
  --cloud-provider {aws-s3,azure-blob-storage,google-cloud-storage}
                        The cloud provider to use as a storage backend
  --timeline TIMELINE   The earliest timeline whose WALs should cause the
                        check to fail

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
  --azure-credential {azure-cli,managed-identity}, --credential {azure-cli,managed-identity}
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
:   Failure

2
:   The connection to the cloud provider failed

3
:   There was an error in the command input

Other non-zero codes
:   Error running the check


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
