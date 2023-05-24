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
- `barman-cloud-backup-show`: script to be used to display the metadata for
  a Barman backup taken with `barman-cloud-backup`;
- `barman-cloud-restore`: script to be used to restore a backup directly
  taken with `barman-cloud-backup` from cloud storage;

These commands require the appropriate library for the cloud provider you wish to
use:

* AWS S3: [boto3][boto3]
* Azure Blob Storage: [azure-storage-blob][azure-storage-blob] and (optionally)
  [azure-identity][azure-identity]
* Google Cloud Storage: [google-cloud-storage][google-cloud-storage]

For information on how to setup credentials for the aws-s3 cloud provider
please refer to the ["Credentials" section in Boto 3 documentation][boto3creds].

For credentials for the azure-blob-storage cloud provider see the
["Environment variables for authorization parameters" section in the Azure documentation][azure-storage-auth].
The following environment variables are supported: `AZURE_STORAGE_CONNECTION_STRING`,
`AZURE_STORAGE_KEY` and `AZURE_STORAGE_SAS_TOKEN`. You can also use the
`--credential` option to specify either `azure-cli` or `managed-identity` credentials
in order to authenticate via Azure Active Directory.

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

## barman-cloud and snapshot backups

The barman-cloud client utilities can also be used to create and manage backups using cloud snapshots as an alternative to uploading to a cloud object store.

When using barman-cloud in this manner the backup data is stored by the cloud provider as volume snapshots and the WALs and backup metadata, including the backup_label, are stored in cloud object storage.

The prerequisites are the [same as for snapshot backups using Barman](#prerequisites-for-cloud-snapshots) with the added requirement that the credentials used by barman-cloud must be able to perform read/write/update operations against an object store.

### barman-cloud-backup for snapshots

To take a snapshot backup with barman-cloud, use `barman-cloud-backup` with the following additional arguments:

- `--snapshot-disk` (can be used multiple times for multiple disks)
- `--snapshot-instance`

If the `--cloud-provider` is `google-cloud-storage` then the following arguments are also required:

- `--gcp-project`
- `--gcp-zone`

If the `--cloud-provider` is `azure-blob-storage` then the following arguments are also required:

- `--azure-subscription-id`
- `--azure-resource-group`

The following options cannot be used with `barman-cloud-backup` when cloud snapshots are requested:

- `--bzip2`, `--gzip` or `--snappy`
- `--jobs`

Once a backup has been taken it can be managed using the standard barman-cloud commands such as `barman-cloud-backup-delete` and `barman-cloud-backup-keep`.

### barman-cloud-restore for snapshots

The process for recovering from a snapshot backup with barman-cloud is very similar to the process for [barman backups](#recovering-from-a-snapshot-backup) except that `barman-cloud-restore` should be run instead of `barman recover` once a recovery instance has been provisioned.
This carries out the same pre-recovery checks as `barman recover` and copies the backup label into place on the recovery instance.

The snapshot metadata required to provision the recovery instance can be queried using `barman-cloud-backup-show`.

Note that, just like when using `barman-cloud-restore` with an object stored backup, the command will not prepare PostgreSQL for the recovery.
Any PITR options, custom `restore_command` values or WAL files required before PostgreSQL starts must be handled manually or by external tooling.

The following additional argument must be used with `barman-cloud-restore` when restoring a backup made with cloud snapshots:

- `--snapshot-recovery-instance`

The following additional arguments are required with the `gcp` provider:

- `--gcp-zone`

The following additional arguments are required with the `azure` provider:

- `--azure-resource-group`

The `--tablespace` option cannot be used with `barman-cloud-restore` when restoring a cloud snapshot backup:
