% BARMAN-CLOUD-CHECK-WAL-ARCHIVE(1) Barman User manuals | Version 3.2.0
% EnterpriseDB <https://www.enterprisedb.com>
% October 20, 2022

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

--timeline
:    A positive integer specifying the earliest timeline for which
     associated WALs should cause the check to fail.
     The check will pass if all WAL content in the archive relates
     to earlier timelines. If any WAL files are on this timeline or
     greater then the check will fail.

--cloud-provider {aws-s3,azure-blob-storage,google-cloud-storage}
:    the cloud provider to which the backup should be uploaded

-P, --profile
:    profile name (e.g. INI section in AWS credentials file)

--endpoint-url
:    override the default S3 URL construction mechanism by specifying an endpoint.

--read-timeout *TIMEOUT*
:    the time in seconds until a timeout is raised when waiting to read from a
     connection to AWS S3 (defaults to 60 seconds)

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

© Copyright EnterpriseDB UK Limited 2011-2022
