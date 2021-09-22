% BARMAN-CLOUD-WAL-RESTORE(1) Barman User manuals | Version 2.14
% EnterpriseDB <http://www.enterprisedb.com>
% September 22, 2020

# NAME

barman-cloud-wal-restore - Restore PostgreSQL WAL files from the Cloud using `restore_command`


# SYNOPSIS

barman-cloud-wal-restore [*OPTIONS*] *SOURCE_URL* *SERVER_NAME* *WAL_NAME* *WAL_PATH*


# DESCRIPTION

This script can be used as a `restore_command` to download WAL files
previously archived with `barman-cloud-wal-archive` command.
Currently AWS S3 and Azure Blob Storage are supported.

This script and Barman are administration tools for disaster recovery
of PostgreSQL servers written in Python and maintained by EnterpriseDB.


# POSITIONAL ARGUMENTS

SOURCE_URL
:    URL of the cloud source, such as a bucket in AWS S3.
     For example: `s3://BUCKET_NAME/path/to/folder` (where `BUCKET_NAME`
     is the bucket you have created in AWS).

SERVER_NAME
:    the name of the server as configured in Barman.

WAL_NAME
:    the name of the WAL file, equivalent of '%f' keyword (according to 'restore_command').

WAL_PATH
:    the value of the '%p' keyword (according to 'restore_command').

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
