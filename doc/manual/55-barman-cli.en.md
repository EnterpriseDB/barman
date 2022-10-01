\newpage

# Barman client utilities (`barman-cli`)

Formerly a separate open-source project, `barman-cli` has been
merged into Barman's core since version 2.8, and is distributed
as an RPM/Debian package. `barman-cli` contains a set of recommended
client utilities to be installed alongside the PostgreSQL server:

- `barman-wal-archive`: archiving script to be used as `archive_command`
  as described in the "WAL archiving via `barman-wal-archive`" section;
- `barman-wal-restore`: WAL restore script to be used as part of the
  `restore_command` recovery option on standby and recovery servers,
  as described in the "`get-wal`" section above;

For more detailed information, please refer to the specific man pages
or the `--help` option.

## Installation

Barman client utilities are normally installed where PostgreSQL is installed.
Our recommendation is to install the `barman-cli` package on every PostgreSQL
server, being that primary or standby.

Please refer to the main "Installation" section to install the repositories.

To install the package on RedHat/CentOS system, as `root` type:

``` bash
yum install barman-cli
```

On Debian/Ubuntu, as `root` user type:

``` bash
apt-get install barman-cli
```


# Barman client utilities for the Cloud (`barman-cli-cloud`)

Barman client utilities have been extended to support object storage
integration and enhance disaster recovery capabilities of your PostgreSQL
databases by relaying WAL files and backups to a supported cloud provider.

Supported cloud providers are:

* AWS S3 (or any S3 compatible object store)
* Azure Blob Storage
* Google Cloud Storage (Rest API)
* Networker Backup Software

These utilities are distributed in the `barman-cli-cloud` RPM/Debian package,
and can be installed alongside the PostgreSQL server:

- `barman-cloud-wal-archive`: archiving script to be used as `archive_command`
  to directly ship WAL files to cloud storage, bypassing the Barman server;
  alternatively, as a hook script for WAL archiving (`pre_archive_retry_script`);
- `barman-cloud-wal-restore`: script to be used as `restore_command`
  to fetch WAL files from cloud storage, bypassing the Barman server, and
  store them directly in the PostgreSQL standby;
- `barman-cloud-backup`: backup script to be used to take a local backup
  directly on the PostgreSQL server and to ship it to a supported cloud provider,
  bypassing the Barman server; alternatively, as a hook script for copying barman
  backups to the cloud (`post_backup_retry_script)`
- `barman-cloud-backup-delete`: script to be used to delete one or more backups
  taken with `barman-cloud-backup` from cloud storage and remove associated
  WALs;
- `barman-cloud-backup-keep`: script to be used to flag backups in cloud storage
  as archival backups - such backups will be kept forever regardless of any
  retention policies applied;
- `barman-cloud-backup-list`: script to be used to list the content of
  Barman backups taken with `barman-cloud-backup` from cloud storage;
- `barman-cloud-restore`: script to be used to restore a backup directly
  taken with `barman-cloud-backup` from cloud storage;

These commands require the appropriate library for the cloud provider you wish to
use:

* AWS S3: [boto3][boto3]
* Azure Blob Storage: [azure-storage-blob][azure-storage-blob] and (optionally)
  [azure-identity][azure-identity]
* Google Cloud Storage: [google-cloud-storage][google-cloud-storage]

**NOTE:** The latest versions of these libraries do not support python 2 due to it
being [end-of-lfe][python-2-sunset] since Januaray 2020. If you are using the
Barman cloud utilities on a python 2 system it is recommended you upgrade to python 3.
If you still want to use the Barman cloud utilities with python 2 then you will need
to ensure the following version requirements are met for each library:

* `boto3<1.18.0`
* `azure-storage-blob<12.10.0` and `azure-identity<1.8.0`
* `google-cloud-storage<2.0.0`

For information on how to setup credentials for the aws-s3 cloud provider
please refer to the ["Credentials" section in Boto 3 documentation][boto3creds].

For credentials for the azure-blob-storage cloud provider see the
["Environment variables for authorization parameters" section in the Azure documentation][azure-storage-auth].
The following environment variables are supported: `AZURE_STORAGE_CONNECTION_STRING`,
`AZURE_STORAGE_KEY` and `AZURE_STORAGE_SAS_TOKEN`. You can also use the
`--credential` option to specify either `azure-cli` or `managed-identity` credentials
in order to authenticate via Azure Active Directory.

> **WARNING:** Cloud utilities require the appropriate library for the cloud
> provider you wish to use - either: [boto3][boto3] or
> [azure-storage-blob][azure-storage-blob] and (optionally)
> [azure-identity][azure-identity]

For Networker the PostgreSQL server has to be configured as a backup client.
The Networker Client and Extended Software packages have to be installed.

## Installation

Barman client utilities for the Cloud need to be installed on those PostgreSQL
servers that you want to direcly backup to a cloud provider, bypassing Barman.

In case you want to use `barman-cloud-backup` and/or `barman-cloud-wal-archive`
as hook scripts, you can install the `barman-cli-cloud` package on the Barman
server also.

Please refer to the main "Installation" section to install the repositories.

To install the package on RedHat/CentOS system, as `root` type:

``` bash
yum install barman-cli-cloud
```

On Debian/Ubuntu, as `root` user type:

``` bash
apt-get install barman-cli-cloud
```

## barman-cloud hook scripts

Install the `barman-cli-cloud` package on the Barman server as described above.

Configure `barman-cloud-backup` as a post backup script by adding the following
to the Barman configuration for a PostgreSQL server:

```
post_backup_retry_script = 'barman-cloud-backup [*OPTIONS*] *DESTINATION_URL* ${BARMAN_SERVER}
```

> **WARNING:** When running as a hook script barman-cloud-backup requires that
> the status of the backup is DONE and it will fail if the backup has any other
> status. For this reason it is recommended backups are run with the
> `-w / --wait` option so that the hook script is not executed while a
> backup has status `WAITING_FOR_WALS`.

Configure `barman-cloud-wal-archive` as a pre WAL archive script by adding the
following to the Barman configuration for a PostgreSQL server:

```
pre_archive_retry_script = 'barman-cloud-wal-archive [*OPTIONS*] *DESTINATION_URL* ${BARMAN_SERVER}'
```

## Selecting a cloud provider

Use the `--cloud-provider` option to choose the cloud provider for your backups
and WALs. This can be set to one of the following:

* `aws-s3` [DEFAULT]: AWS S3 or S3-compatible object store.
* `azure-blob-storage`: Azure Blob Storage service.
* `google-cloud-storage`: Google Cloud Storage service.
* `networker-storage`: Networker Backup Software


## Specificity by provider

### Google Cloud Storage

#### set up
It will need google_storage_client dependency:
```bash
pip3 install google-cloud-storage 
```

To set credentials:

* [Create a service account](https://cloud.google.com/docs/authentication/getting-started#setting_the_environment_variable)
  And create a service account key.


* Set bucket access rights:

  We suggest to give [Storage Admin Role](https://cloud.google.com/storage/docs/access-control/iam-roles) 
to the service account on the bucket.
    

* When using barman_cloud, If the bucket does not exist, it will be created. Default options will be used to create 
the bucket. If you need the bucket to have specific options (region, storage class, labels), it is advised to create 
and set the bucket to match all you needs. 

* Set [env variable](https://cloud.google.com/docs/authentication/getting-started#setting_the_environment_variable) 
  `GOOGLE_APPLICATION_CREDENTIALS` to the service account key file path. 

  If running barman cloud from postgres (archive_command or restore_command), do not forget to set 
  `GOOGLE_APPLICATION_CREDENTIALS` in postgres environment file.

#### Usage
Some details are specific to all barman cloud commands:
* Select Google Cloud Storage`--cloud-provider=google-cloud-storage`
* `SOURCE_URL` support both gs and https format.
  ex:
  ```
  gs://BUCKET_NAME/path
  or
  https://console.cloud.google.com/storage/browser/BUCKET_NAME/path
  ```
### Networker Storage

#### Setup
Copy the necessary software packages to the PostgreSQL Server. Install the Client Software as follows
```bash
dnf -y install lgtoclnt lgtoxtdclnt lgtoman sudo
```

The barman storage module needs the **mminfo**, **recover** and **nsrmm** networker commands.

The system user for the PostgreSQL Database has to be enabled in sudoers. Because some of the
networker functions can only be performed as `root`. This restriction is hard-coded into the software.
In General, all recover operations need `root` permissions. So listing or restoring data operations
are best run as `root`. When run as a normal user, the module tries to elevate itself to `root` by
using sudo.

This is especially important, when configuring the PostgreSQL `restore_command`. e.g.
```
restore_command = "sudo barman-cloud-wal-restore --cloud-provider=networker-storage nw://..."
```

The module uses the directory `/nsr/cache/cloudboost/barman` as a local staging location. It will
create this directory if it doesn't exist. Assuming that `/nsr/cache/cloudboost` was created by
the networker client. Older clients may not. So check for it's existence and create it yourself
if necessary. e.g. by
```bash
mkdir -m 0777 -p /nsr/cache/cloudboost/barman
```

If you have an `/etc/sudoers.d` directory, create a file barman.conf in it. With the following content.
If not append the line to the `/etc/sudoers` file. This assumes that your user is named `postgres`.
```
postgres   ALL=(ALL)       NOPASSWD: ALL
```

#### Usage
Specific Parameters for all of the barman cloud commands:
* Select the Networker Storage Provider by `--cloud-provider=networker-storage`
* `SOURCE_URL` has to be in the following format.
  ```
  nw://<Networker Server Name>/<Media Pool>
  ```
Specific Parameters for the barman backup cloud commands:
* Parameters for the Networker `save` command can be specified through the barman `--tags "nwargs, ..."`
  Parameter. e.g.
  ```
  --tags "nwargs,-y ${RETENTION_TIME} -w ${BROWSE_TIME} -L"
  ```
  This can be useful because networker has it's own retention policies and management. So although
  old backups can be removed by `barman-cloud-backup-delete`, networker will do the same and may even
  already have done it.

`barman-cloud-backup-keep` has no impact when using networker as a storage provider. Regular Networker
backups will always expire. Networker uses a complete separate command set and storage pools for this.
The decision for archiving has to be made when creating the backup and cannot be reversed later. In
addition, networker archivals are intentionally left out of the browse index. The barman module uses
the index as storage for the backup keys. Therefore archival is not supported at all with networker.
