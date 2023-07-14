## Backup with cloud snapshots

Barman is able to create backups of PostgreSQL servers deployed within certain cloud environments by taking snapshots of storage volumes.
When configured in this manner the physical backups of PostgreSQL files are volume snapshots stored in the cloud while Barman acts as a storage server for WALs and the backup catalog.
These backups can then be managed by Barman just like traditional backups taken with the `rsync` or `postgres` backup methods even though the backup data itself is stored in the cloud.

It is also possible to create snapshot backups without a Barman server using the [barman-cloud-backup](#barman-cloud-and-snapshot-backups) command directly on a suitable PostgreSQL server.

### Prerequisites for cloud snapshots

In order to use the snapshot backup method with Barman, deployments must meet the following prerequisites:

- PostgreSQL must be deployed on a compute instance within a supported cloud provider.
- PostgreSQL must be configured such that all critical data, such as PGDATA and any tablespace data, is stored on storage volumes which support snapshots.
- The `findmnt` command must be available on the PostgreSQL host.

> **IMPORTANT:** Any configuration files stored outside of PGDATA will not be
> included in the snapshots. The management of such files must be carried out
> using another mechanism such as a configuration management system.

#### Google Cloud Platform snapshot prerequisites

The google-cloud-compute and grpcio libraries must be available to the Python distribution used by Barman.
These libraries are an optional dependency and are not installed as standard by any of the Barman packages.
They can be installed as follows using `pip`:

``` bash
pip3 install grpcio google-cloud-compute
```

> **NOTE:** The minimum version of Python required by the google-cloud-compute
> library is 3.7. GCP snapshots cannot be used with earlier versions of Python.

The following additional prerequisites apply to snapshot backups on Google Cloud Platform:

- All disks included in the snapshot backup must be zonal persistent disks. Regional persistent disks are not currently supported.
- A service account with the required set of permissions must be available to Barman. This can be achieved by attaching such an account to the compute instance running Barman (recommended) or by using the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to a credentials file.

The required permissions are:

- `compute.disks.createSnapshot`
- `compute.disks.get`
- `compute.globalOperations.get`
- `compute.instances.get`
- `compute.snapshots.create`
- `compute.snapshots.delete`
- `compute.snapshots.list`

#### Azure snapshot prerequisites

The azure-mgmt-compute and azure-identity libraries must be available to the Python distribution used by Barman.

These libraries are an optional dependency and are not installed as standard by any of the Barman packages.
They can be installed as follows using `pip`:

``` bash
pip3 install azure-mgmt-compute azure-identity
```

> **NOTE:** The minimum version of Python required by the azure-mgmt-compute
> library is 3.7. Azure snapshots cannot be used with earlier versions of Python.

The following additional prerequisites apply to snapshot backups on Azure:

- All disks included in the snapshot backup must be managed disks which are attached to the VM instance as data disks.
- Barman must be able to use a credential obtained either using managed identity or CLI login and this must grant access to Azure with the required set of permissions.

The following permissions are required:

- `Microsoft.Compute/disks/read`
- `Microsoft.Compute/virtualMachines/read`
- `Microsoft.Compute/snapshots/read`
- `Microsoft.Compute/snapshots/write`
- `Microsoft.Compute/snapshots/delete`

#### AWS snapshot prerequisites

The boto3 library must be available to the Python distribution used by Barman.

This library is an optional dependency and not installed as standard by any of the Barman packages.
It can be installed as follows using `pip`:

```bash
pip3 install boto3
```

The following additional prerequisites apply to snapshot backups on AWS:

- All disks included in the snapshot backup must be non-root EBS volumes and must be attached to the same VM instance.
- NVMe volumes are not currently supported.

The following permissions are required:

- `ec2:CreateSnapshot`
- `ec2:CreateTags`
- `ec2:DeleteSnapshot`
- `ec2:DescribeSnapshots`
- `ec2:DescribeInstances`
- `ec2:DescribeVolumes`

### Configuration for snapshot backups

To configure Barman for backup via cloud snapshots, set the `backup_method` parameter to `snapshot` and set `snapshot_provider` to a supported cloud provider:

``` ini
backup_method = snapshot
snapshot_provider = gcp
```

Currently Google Cloud Platform (`gcp`), Microsoft Azure (`azure`) and AWS (`aws`) are supported.

The following parameters must be set regardless of cloud provider:

``` ini
snapshot_instance = INSTANCE_NAME
snapshot_disks = DISK_NAME,DISK2_NAME,...
```

Where `snapshot_instance` is set to the name of the VM or compute instance where the storage volumes are attached and `snapshot_disks` is a comma-separated list of the disks which should be included in the backup.

> **IMPORTANT:** You must ensure that `snapshot_disks` includes every disk
> which stores data required by PostgreSQL. Any data which is not stored
> on a storage volume listed in `snapshot_disks` will not be included in the
> backup and therefore will not be available at recovery time.

#### Configuration for Google Cloud Platform snapshots

The following additional parameters must be set when using GCP:

``` ini
gcp_project = GCP_PROJECT_ID
gcp_zone = ZONE
```

`gcp_project` should be set to the ID of the GCP project which owns the instance and storage volumes defined by `snapshot_instance` and `snapshot_disks`. `gcp_zone` should be set to the availability zone in which the instance is located.

#### Configuration for Azure snapshots

The following additional parameters must be set when using Azure:

``` ini
azure_subscription_id = AZURE_SUBSCRIPTION_ID
azure_resource_group = AZURE_RESOURCE_GROUP
```

`azure_subscription_id` should be set to the ID of the Azure subscription ID which owns the instance and storage volumes defined by `snapshot_instance` and `snapshot_disks`.
`azure_resource_group` should be set to the resource group to which the instance and disks belong.

#### Configuration for AWS snapshots

The following optional parameters can be set when using AWS:

``` ini
aws_region = AWS_REGION
aws_profile = AWS_PROFILE_NAME
```

If `aws_profile` is used it should be set to the name of a section in the AWS credentials file.
If `aws_profile` is not used then the default profile will be used.
If no credentials file exists then credentials will be sourced from the environment.

If `aws_region` is specified it will override any region that may be defined in the AWS profile.

### Taking a snapshot backup

Once the configuration options are set and appropriate credentials are available to Barman, backups can be taken using the [barman backup](#backup) command.

Barman will validate the configuration parameters for snapshot backups during the `barman check` command and also when starting a backup.

Note that the following arguments / config variables are unavailable when using `backup_method = snapshot`:

| **Command argument** | **Config variable**   |
|:--------------------:|:---------------------:|
| N/A                  | `backup_compression`  |
| `--bwlimit`          | `bandwidth_limit`     |
| `--jobs`             | `parallel_jobs`       |
| N/A                  | `network_compression` |
| `--reuse-backup`     | `reuse_backup`        |

For a more in-depth discussion of snapshot backups, including considerations around management and recovery of snapshot backups, see the [cloud snapshots section in feature details](#cloud-snapshot-backups).
