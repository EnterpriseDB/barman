% BARMAN-CLOUD-WAL-ARCHIVE(1) Barman User manuals | Version 2.12.1
% EnterpriseDB <http://www.enterprisedb.com>
% June 30, 2021

# NAME

barman-cloud-wal-archive - Archive PostgreSQL WAL files in the Cloud using `archive_command`


# SYNOPSIS

barman-cloud-wal-archive [*OPTIONS*] *DESTINATION_URL* *SERVER_NAME* *WAL_PATH*


# DESCRIPTION

This script can be used in the `archive_command` of a PostgreSQL
server to ship WAL files to the Cloud. Currently AWS S3 and Azure Blob
Storage are supported.

This script and Barman are administration tools for disaster recovery
of PostgreSQL servers written in Python and maintained by EnterpriseDB.


# POSITIONAL ARGUMENTS

DESTINATION_URL
:    URL of the cloud destination, such as a bucket in AWS S3.
     For example: `s3://BUCKET_NAME/path/to/folder` (where `BUCKET_NAME`
     is the bucket you have created in AWS).


SERVER_NAME
:    the name of the server as configured in Barman.

WAL_PATH
:    the value of the '%p' keyword (according to 'archive_command').

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
:    gzip-compress the WAL while uploading to the cloud

-j, --bzip2
:    bzip2-compress the WAL while uploading to the cloud

--cloud-provider {aws-s3,azure-blob-storage}
:    the cloud provider to which the backup should be uploaded

-P, --profile
:    profile name (e.g. INI section in AWS credentials file)

--endpoint-url
:    override the default S3 URL construction mechanism by specifying an endpoint.

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

This script can be used in conjunction with `pre_archive_retry_script` to relay WAL
files to S3, as follows:

```
pre_archive_retry_script = 'barman-cloud-wal-archive [*OPTIONS*] *DESTINATION_URL* ${BARMAN_SERVER} ${BARMAN_FILE}'
```


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
