% BARMAN-CLOUD-WAL-ARCHIVE(1) Barman User manuals | Version 2.12
% 2ndQuadrant <http://www.2ndQuadrant.com>
% November 5, 2020

# NAME

barman-cloud-wal-archive - Archive PostgreSQL WAL files in the Cloud using `archive_command`


# SYNOPSIS

barman-cloud-wal-archive [*OPTIONS*] *DESTINATION_URL* *SERVER_NAME* *WAL_PATH*


# DESCRIPTION

This script can be used in the `archive_command` of a PostgreSQL
server to ship WAL files to the Cloud. Currently only AWS S3 is supported.

This script and Barman are administration tools for disaster recovery
of PostgreSQL servers written in Python and maintained by 2ndQuadrant.


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

-t, --test
: test connectivity to the cloud destination and exit

-P, --profile
: profile name (e.g. INI section in AWS credentials file)

-z, --gzip
: gzip-compress the WAL while uploading to the cloud

-j, --bzip2
: bzip2-compress the WAL while uploading to the cloud

-e ENCRYPT, --encrypt ENCRYPT
: enable server-side encryption with the given method for the transfer.
  Allowed methods: `AES256` and `aws:kms`.

--endpoint-url
: override the default S3 URL construction mechanism by specifying an endpoint.

# REFERENCES

For Boto:

* https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html

For AWS:

* http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-set-up.html
* http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html.

# DEPENDENCIES

* boto3

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
* Professional support: <http://www.2ndQuadrant.com/>


# COPYING

Barman is the property of 2ndQuadrant Limited
and its code is distributed under GNU General Public License v3.

© Copyright EnterpriseDB UK Limited 2011-2021
